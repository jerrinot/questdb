/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package org.questdb.experimental.codegen;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.groupby.vect.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Bounded dictionary-indexed partial states, using the existing vector aggregate ABI. */
final class ResolvedSymbolAggregate implements CompiledVectorAggregate {
    private final int keyColumn;
    private final int[] indexes;
    private final AggregateUpdates updates;
    private final int stride;
    private final long maxSlotBytes;
    private final long[] slots;
    private final long[] nativeRows; // separate cache lines for exclusive worker slots
    private final CompiledKernel code;
    private final CompiledKernel mergeCode;
    private final String name;
    private MemoryTracker tracker;
    private int symbolCount;
    private long slotBytes;
    private long seenOffset;
    private long keysOffset;
    private long argsOffset;

    static ResolvedSymbolAggregate bind(RecordMetadata inputs, int keyColumn,
            ObjList<VectorAggregateFunction> functions, long rosti, int workers, Backend backend, Path artifacts) {
        if (inputs.getColumnType(keyColumn) != ColumnType.SYMBOL || !inputs.isSymbolTableStatic(keyColumn)) return null;
        var resolved = new ResolvedInputs(inputs);
        var operations = new ArrayList<AggregateUpdates.Update>();
        long offsets = Rosti.getValueOffsets(rosti);
        for (int i = 0; i < functions.size(); i++) {
            var function = functions.getQuick(i);
            Class<?> type = function.getClass();
            long offset = Unsafe.getInt(offsets + function.getValueOffset() * 4L);
            if (type == CountVectorAggregateFunction.class) {
                operations.add(new AggregateUpdates.ScalarUpdate(new Pipeline.Count(), offset));
                continue;
            }
            boolean min = type == MinShortVectorAggregateFunction.class || type == MinIntVectorAggregateFunction.class
                    || type == MinLongVectorAggregateFunction.class;
            boolean max = type == MaxShortVectorAggregateFunction.class || type == MaxIntVectorAggregateFunction.class
                    || type == MaxLongVectorAggregateFunction.class;
            boolean sum = type == SumShortVectorAggregateFunction.class || type == SumIntVectorAggregateFunction.class
                    || type == SumLongVectorAggregateFunction.class || type == AvgShortVectorAggregateFunction.class
                    || type == AvgIntVectorAggregateFunction.class;
            boolean wideSum = type == AvgLongVectorAggregateFunction.class;
            boolean count = type == CountIntVectorAggregateFunction.class || type == CountLongVectorAggregateFunction.class;
            if (!min && !max && !sum && !wideSum && !count) return null;
            int index = function.getColumnIndex();
            int inputType = inputs.getColumnType(index);
            if (inputType != ColumnType.SHORT && inputType != ColumnType.INT && inputType != ColumnType.LONG) return null;
            var column = resolved.column(index, inputType);
            if (min || max) {
                var width = inputType == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
                operations.add(new AggregateUpdates.IntegerExtremum(column, offset, width, min));
            } else if (wideSum) operations.add(new AggregateUpdates.VectorLongAverageUpdate(column, offset));
            else if (sum) operations.add(new AggregateUpdates.VectorSumUpdate(column, offset));
            else operations.add(new AggregateUpdates.ScalarUpdate(new Pipeline.CountValue(column), offset));
        }
        if (operations.isEmpty()) return null;
        var updates = new AggregateUpdates(new ResolvedVarchar.Binding(resolved.indexes(), resolved.columns(),
                new int[0], new byte[0]), List.copyOf(operations));
        return new ResolvedSymbolAggregate(keyColumn, resolved.indexes(), updates, rosti, workers, backend, artifacts);
    }

    private ResolvedSymbolAggregate(int keyColumn, int[] indexes, AggregateUpdates updates,
            long rosti, int workers, Backend backend, Path artifacts) {
        this.keyColumn = keyColumn;
        this.indexes = indexes;
        this.updates = updates;
        stride = Math.toIntExact(Rosti.getSlotSize(rosti));
        // Bound array replication by the slot storage the ordinary factory already allocated.
        maxSlotBytes = Math.multiplyExact(Rosti.getCapacity(rosti) + 1, stride);
        slots = new long[workers];
        nativeRows = new long[workers * 8];
        name = backend.name();
        code = CompiledKernel.defer(backend, this::program, artifacts);
        mergeCode = CompiledKernel.defer(backend, this::mergeProgram, artifacts);
    }

    @Override
    public boolean open(int count, MemoryTracker tracker) {
        clear();
        if (count < 0) return false;
        long entries = (long) count + 1;
        seenOffset = entries * stride;
        keysOffset = (seenOffset + entries + 7) & ~7L;
        argsOffset = (keysOffset + entries * 4 + 7) & ~7L;
        slotBytes = argsOffset + (indexes.length + 3L) * 8;
        if (slotBytes > maxSlotBytes) return false;
        symbolCount = count;
        this.tracker = tracker;
        try {
            for (int i = 0; i < slots.length; i++) {
                slots[i] = Unsafe.malloc(slotBytes, MemoryTag.NATIVE_JIT, tracker);
                Vect.memset(slots[i] + seenOffset, entries, 0);
                Vect.memset(slots[i] + argsOffset, (indexes.length + 3L) * 8, 0);
            }
            // Prepare deferred code on the owner before publication to workers.
            code.run(slots[0] + argsOffset, 0, slots[0]);
            return true;
        } catch (Throwable failure) {
            clear();
            throw failure;
        }
    }

    @Override
    public void aggregate(PageFrameMemory frame, long rows, int slot, ExecutionCircuitBreaker breaker) {
        long state = slots[slot], args = state + argsOffset;
        long keys = frame.getPageAddress(keyColumn);
        Unsafe.putLong(args + (indexes.length + 1L) * 8, state + seenOffset);
        Unsafe.putLong(args + (indexes.length + 2L) * 8, symbolCount);
        for (long lo = 0; lo < rows; lo += 65536) {
            if (breaker.checkIfTripped()) throw CairoException.queryCancelled();
            Unsafe.putLong(args, keys == 0 ? 0 : keys + lo * 4);
            for (int i = 0; i < indexes.length; i++) {
                long address = frame.getPageAddress(indexes[i]);
                Unsafe.putLong(args + (i + 1L) * 8, address == 0 ? 0 : address + lo * updates.columns().get(i).width().bytes);
            }
            long n = Math.min(65536, rows - lo);
            if (code.run(args, n, state) != 0) throw CairoException.critical(0).put("symbol ID outside reader snapshot");
            nativeRows[slot * 8] += n;
        }
    }

    @Override
    public boolean mergeAndExport(long rosti, ExecutionCircuitBreaker breaker) {
        long state = slots[0], seen = state + seenOffset, keys = state + keysOffset;
        long args = state + argsOffset;
        for (int slot = 1; slot < slots.length; slot++) {
            if (breaker.checkIfTripped()) throw CairoException.queryCancelled();
            Unsafe.putLong(args, slots[slot]);
            Unsafe.putLong(args + 8, slots[slot] + seenOffset);
            Unsafe.putLong(args + 16, seen);
            mergeCode.run(args, symbolCount + 1L, state);
        }
        int groups = 0;
        for (int id = 0; id <= symbolCount; id++) {
            if (Unsafe.getByte(seen + id) != 0) {
                Unsafe.putInt(keys + groups++ * 4L, id == symbolCount ? SymbolTable.VALUE_IS_NULL : id);
            }
        }
        if (groups == 0) return true;
        if (!Rosti.keyedIntDistinct(rosti, keys, groups)) return false;
        long ctrl = Rosti.getCtrl(rosti), destination = Rosti.getSlots(rosti);
        for (long slotIndex = 0, capacity = Rosti.getCapacity(rosti); slotIndex < capacity; slotIndex++) {
            if ((Unsafe.getByte(ctrl + slotIndex) & 0x80) == 0) {
                long entry = destination + slotIndex * stride;
                int key = Unsafe.getInt(entry);
                long source = state + (key == SymbolTable.VALUE_IS_NULL ? symbolCount : key) * (long) stride;
                Vect.memcpy(entry + 4, source + 4, stride - 4);
            }
        }
        return true;
    }

    @Override
    public void clear() {
        for (int i = 0; i < slots.length; i++) {
            slots[i] = Unsafe.free(slots[i], slotBytes, MemoryTag.NATIVE_JIT, tracker);
        }
        tracker = null;
    }

    @Override public void close() {
        try (mergeCode; code) {
            clear();
        }
    }
    @Override public CharSequence getName() { return name; }
    @Override public long getNativeRows() {
        long rows = 0;
        for (int i = 0; i < nativeRows.length; i += 8) rows += nativeRows[i];
        return rows;
    }

    private Program mergeProgram() {
        Builder b = new Builder();
        Var source = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var sourceSeen = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var destinationSeen = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 16)));
        Var id = b.let(i64(0));
        b.loop(binary(Op.LT, id, ROWS), loop -> {
            loop.when(ne(new Load(MemoryType.I8, new Offset(sourceSeen, id)), i64(0)), present -> {
                Var destination = present.let(new Offset(STATE, binary(Op.MUL, id, i64(stride))));
                Expr visited = new Offset(destinationSeen, id);
                present.when(eq(new Load(MemoryType.I8, visited), i64(0)), first -> {
                    updates.initialize(first, destination);
                    first.store(MemoryType.I8, visited, i64(1));
                });
                updates.merge(present, new Offset(source, binary(Op.MUL, id, i64(stride))), destination);
            });
            loop.assign(id, add(id, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    private Program program() {
        Builder b = new Builder();
        Var keys = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < indexes.length; i++) addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, (i + 1L) * 8))));
        Var seen = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, (indexes.length + 1L) * 8)));
        Var count = b.let(load(at(COLUMNS, (indexes.length + 2L) * 8)));
        Var row = b.let(i64(0));
        b.loop(binary(Op.LT, row, ROWS), loop -> {
            Var key = loop.let(i64(SymbolTable.VALUE_IS_NULL));
            loop.when(ne(keys, nil()), present -> present.assign(key,
                    new Load(MemoryType.I32, new Offset(keys, binary(Op.MUL, row, i64(4))))));
            Var id = loop.let(new Select(eq(key, i64(SymbolTable.VALUE_IS_NULL)), count, key));
            loop.when(binary(Op.BOTH, ne(key, i64(SymbolTable.VALUE_IS_NULL)),
                    binary(Op.EITHER, binary(Op.LT, key, i64(0)), binary(Op.GE, key, count))), invalid -> invalid.exit(1));
            Var destination = loop.let(new Offset(STATE, binary(Op.MUL, id, i64(stride))));
            Expr visited = new Offset(seen, id);
            loop.when(eq(new Load(MemoryType.I8, visited), i64(0)), first -> {
                updates.initialize(first, destination);
                first.store(MemoryType.I8, visited, i64(1));
            });
            var values = new ArrayList<Pipeline.Value>();
            for (int i = 0; i < indexes.length; i++) {
                var column = updates.columns().get(i);
                Var value = loop.let(i64(column.nullValue()));
                Var address = addresses.get(i);
                loop.when(ne(address, nil()), present -> present.assign(value,
                        new Load(column.width(), new Offset(address, binary(Op.MUL, row, i64(column.width().bytes))))));
                // SHORT has no NULL representation: a column top reads as zero.
                Expr isNull = column.nullable()
                        ? binary(Op.EITHER, eq(address, nil()), eq(value, i64(column.nullValue()))) : bool(false);
                values.add(new Pipeline.Value(value, isNull));
            }
            updates.emit(loop, new Pipeline.Row(updates.columns(), values), destination, nil());
            loop.assign(row, add(row, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }
}
