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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompiledScalarAggregate;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.questdb.experimental.codegen.Ir.*;

/** Register reductions over the engine's existing unkeyed state layout. */
final class ResolvedScalarAggregate implements CompiledScalarAggregate {
    private final CompiledKernel code;
    private final CompiledKernel mergeCode;
    private final ResolvedVarchar.Binding inputs;
    private final String name;

    private ResolvedScalarAggregate(CompiledKernel code, CompiledKernel mergeCode, ResolvedVarchar.Binding inputs, String name) {
        this.code = code;
        this.mergeCode = mergeCode;
        this.inputs = inputs;
        this.name = name;
    }

    static ResolvedScalarAggregate compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
            MapValue layout, Backend backend, Path artifacts) throws IOException {
        long base = layout.getAddress(0);
        var updates = AggregateUpdates.bind(inputs, values, functions, index -> layout.getAddress(index) - base, false);
        if (updates == null) return null;
        // Only single-slot, nonallocating aggregates can use this register reduction.
        for (var update : updates.updates()) {
            if (!(update instanceof AggregateUpdates.ScalarUpdate || update instanceof AggregateUpdates.IntegerExtremum)) return null;
        }
        CompiledKernel code = backend.compile(scan(updates), artifacts);
        try {
            return new ResolvedScalarAggregate(code, backend.compile(merge(updates), artifacts), updates.inputs(), backend.name());
        } catch (Throwable failure) {
            try { code.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static List<Load> stateLoads(AggregateUpdates updates, Expr base) {
        var result = new ArrayList<Load>();
        for (var update : updates.updates()) {
            var width = update instanceof AggregateUpdates.IntegerExtremum extrema ? extrema.width() : MemoryType.I64;
            result.add(new Load(width, at(base, update.offset())));
        }
        return result;
    }

    private static Program scan(AggregateUpdates updates) {
        Builder b = new Builder();
        var registers = new LinkedHashMap<Load, Var>();
        for (var load : stateLoads(updates, STATE)) registers.put(load, b.let(load));
        Var rowIds = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < updates.columns().size(); i++) {
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, (i + 1L) * 8))));
        }
        // Choose once per frame, leaving both inner loops free of the selection-mode branch.
        b.when(eq(rowIds, nil()), contiguous -> scan(contiguous, updates, addresses, registers, null));
        b.when(ne(rowIds, nil()), selected -> scan(selected, updates, addresses, registers, rowIds));
        for (var entry : registers.entrySet()) b.store(entry.getKey().width(), entry.getKey().address(), entry.getValue());
        b.exit(0);
        return new Program(b.block());
    }

    private static void scan(Builder b, AggregateUpdates updates, List<Var> addresses, Map<Load, Var> registers, Var rowIds) {
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), chunk -> {
            chunk.poll();
            Var end = chunk.let(new Select(binary(Op.LT, ROWS, add(index, i64(1024))), ROWS, add(index, i64(1024))));
            chunk.loop(binary(Op.LT, index, end), loop -> {
                Var row = rowIds == null ? index : loop.let(load(new Offset(rowIds, binary(Op.MUL, index, i64(8)))));
                var values = updates.loadFrameRow(loop, row, addresses);
                Builder emitted = loop.fork();
                updates.emit(emitted, values, STATE, nil());
                loop.append(promote(emitted.block(), registers));
                loop.assign(index, add(index, i64(1)));
            });
        });
    }

    // Reuse the existing update definitions. Only their known state loads/stores become registers;
    // unexpected effects fail compilation instead of silently changing an aggregate's semantics.
    private static Block promote(Block block, Map<Load, Var> registers) {
        var result = new ArrayList<Statement>();
        for (Statement statement : block.statements()) result.add(switch (statement) {
            case Define d -> new Define(d.variable(), promote(d.value(), registers));
            case Store s -> new Assign(register(registers, new Load(s.width(), s.address())), promote(s.value(), registers));
            case If branch -> new If(promote(branch.condition(), registers), promote(branch.body(), registers));
            default -> throw new IllegalArgumentException("Unsupported scalar aggregate effect: " + statement);
        });
        return new Block(result);
    }

    private static Expr promote(Expr expression, Map<Load, Var> registers) {
        return switch (expression) {
            case Var v -> v;
            case Literal literal -> literal;
            case Load load -> register(registers, load);
            case Binary binary -> new Binary(binary.op(), promote(binary.left(), registers), promote(binary.right(), registers));
            case Select select -> new Select(promote(select.condition(), registers), promote(select.yes(), registers), promote(select.no(), registers));
            default -> throw new IllegalArgumentException("Unsupported scalar aggregate expression: " + expression);
        };
    }

    private static Var register(Map<Load, Var> registers, Load load) {
        Var value = registers.get(load);
        if (value == null) throw new IllegalArgumentException("Unbound scalar aggregate state: " + load);
        return value;
    }

    private static Program merge(AggregateUpdates updates) {
        Builder b = new Builder();
        b.when(eq(ROWS, i64(0)), first -> {
            for (var load : stateLoads(updates, STATE)) {
                var offset = ((Offset) load.address()).bytes();
                first.store(load.width(), load.address(), new Load(load.width(), new Offset(COLUMNS, offset)));
            }
        });
        b.when(ne(ROWS, i64(0)), existing -> updates.merge(existing, COLUMNS, STATE));
        b.exit(0);
        return new Program(b.block());
    }

    @Override public CharSequence getName() { return name; }
    @Override public Updater newUpdater() { return new Slot(); }
    @Override public void merge(MapValue source, MapValue destination) {
        long status = mergeCode.run(source.getAddress(0), destination.isNew() ? 0 : 1, destination.getAddress(0));
        if (status != 0) throw new IllegalStateException("Scalar merge failed: " + status);
    }
    @Override public void close() { try (code; mergeCode) { } }

    private final class Slot implements Updater {
        private final long bytes = 8 + inputs.bytes();
        private final KernelMemory memory = KernelMemory.reusable((Runnable) null);
        private long addresses = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT);
        private long nativeRows;

        @Override public long getNativeRows() { return nativeRows; }
        @Override public boolean update(PageFrameMemoryRecord record, long rows, long rowIdsAddress,
                                        MapValue value, SqlExecutionCircuitBreaker circuitBreaker) {
            if (addresses == 0) throw new IllegalStateException("Scalar aggregate slot is closed");
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (rows == 0) return true;
            if (record instanceof PageFrameFilteredMemoryRecord || !inputs.prepare(record, addresses + 8)) return false;
            Unsafe.putLong(addresses, rowIdsAddress);
            memory.enter(circuitBreaker);
            try {
                long status = code.run(addresses, rows, value.getAddress(0));
                memory.throwIfFailed();
                if (status != 0) throw new IllegalStateException("Scalar aggregate failed: " + status);
                value.setNew(false);
                nativeRows += rows;
                return true;
            } finally {
                memory.close();
            }
        }
        @Override public void close() {
            if (addresses != 0) addresses = Unsafe.free(addresses, bytes, MemoryTag.NATIVE_JIT);
        }
    }
}
