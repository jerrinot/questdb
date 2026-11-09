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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompiledScalarAggregate;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
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
    private final CompiledKernel filteredCode;
    private final ResolvedFilter.Binding filter;
    private final ResolvedVarchar.Binding inputs;
    private final String name;
    private final Descriptor descriptor;

    private ResolvedScalarAggregate(CompiledKernel code, CompiledKernel mergeCode, CompiledKernel filteredCode,
                                    ResolvedFilter.Binding filter, ResolvedVarchar.Binding inputs, Descriptor descriptor, String name) {
        this.code = code;
        this.mergeCode = mergeCode;
        this.filteredCode = filteredCode;
        this.filter = filter;
        this.inputs = inputs;
        this.name = name;
        this.descriptor = descriptor;
    }

    static ResolvedScalarAggregate compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
            MapValue layout, Function predicate, Backend backend, Path artifacts) throws IOException {
        long base = layout.getAddress(0);
        var updates = AggregateUpdates.bind(values, functions, index -> layout.getAddress(index) - base, true,
                new ResolvedInputs(inputs, new ArrayList<>(), backend.options()));
        if (updates == null) return null;
        // Merge copies only explicitly described ordinary state cells.
        for (var update : updates.updates()) {
            if (update.scalarState().isEmpty()) return null;
        }
        var filter = predicate == null ? null : ResolvedFilter.bind(inputs, predicate, backend.options());
        var descriptor = new Descriptor(8 + updates.inputs().bytes(), filter == null ? 0 : filter.bytes());
        CompiledKernel code = filter == null ? backend.compile(scan(updates, descriptor), artifacts)
                : CompiledKernel.defer(backend, () -> scan(updates, descriptor), artifacts);
        CompiledKernel merged = null, filtered = null;
        try {
            merged = backend.compile(merge(updates), artifacts);
            if (filter != null) filtered = backend.compile(filteredScan(updates, filter, descriptor), artifacts);
            return new ResolvedScalarAggregate(code, merged, filtered, filter, updates.inputs(), descriptor, backend.name());
        } catch (Throwable failure) {
            try { code.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            if (merged != null) try { merged.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            if (filtered != null) try { filtered.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static List<Load> stateLoads(AggregateUpdates updates, Expr base) {
        var result = new ArrayList<Load>();
        for (var update : updates.updates()) {
            for (var cell : update.scalarState()) result.add(new Load(cell.width(), at(base, cell.offset())));
        }
        return result;
    }

    private static Program scan(AggregateUpdates updates, Descriptor descriptor) {
        Builder b = new Builder();
        var registers = new LinkedHashMap<Load, Var>();
        for (var update : updates.updates()) if (!update.allocates()) {
            for (var cell : update.scalarState()) {
                var load = new Load(cell.width(), at(STATE, cell.offset()));
                registers.put(load, b.let(load));
            }
        }
        initializeFresh(b, updates, registers, descriptor.freshOffset());
        Var first = updates.firstUpdateFlag(b, ne(load(at(COLUMNS, descriptor.freshOffset())), i64(0)));
        Var rowIds = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        var addresses = updates.loadFrameInputs(b);
        // Choose once per frame, leaving both inner loops free of the selection-mode branch.
        b.when(eq(rowIds, nil()), contiguous -> scan(contiguous, updates, addresses, registers, null, first));
        b.when(ne(rowIds, nil()), selected -> scan(selected, updates, addresses, registers, rowIds, first));
        for (var entry : registers.entrySet()) b.store(entry.getKey().width(), entry.getKey().address(), entry.getValue());
        b.exit(0);
        return new Program(b.block());
    }

    private static void scan(Builder b, AggregateUpdates updates, AggregateUpdates.FrameInputs addresses, Map<Load, Var> registers, Var rowIds, Var first) {
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), chunk -> {
            chunk.poll();
            Var end = chunk.let(new Select(binary(Op.LT, ROWS, add(index, i64(1024))), ROWS, add(index, i64(1024))));
            // A chunk is nonempty. Its first update establishes ordinary state even after
            // a NULL prefix; the remaining rows can use the same next-update contract as joins.
            Var firstRow = rowIds == null ? index : chunk.let(load(new Offset(rowIds, binary(Op.MUL, index, i64(8)))));
            updateRow(chunk, updates, addresses, registers, firstRow, false, first);
            if (first != null) chunk.assign(first, bool(false));
            chunk.assign(index, add(index, i64(1)));
            chunk.loop(binary(Op.LT, index, end), loop -> {
                Var row = rowIds == null ? index : loop.let(load(new Offset(rowIds, binary(Op.MUL, index, i64(8)))));
                updateRow(loop, updates, addresses, registers, row, true, null);
                loop.assign(index, add(index, i64(1)));
            });
        });
    }

    private static void updateRow(Builder loop, AggregateUpdates updates, AggregateUpdates.FrameInputs addresses,
                                  Map<Load, Var> registers, Var row, boolean next, Expr first) {
        var values = updates.loadFrameRow(loop, row, addresses);
        var frame = updates.inputs().frame(values, row, at(COLUMNS, 8));
        var strings = new LinkedHashMap<ResolvedVarchar.Expression, Pipeline.Utf8Value>();
        for (var update : updates.updates()) if (update instanceof AggregateUpdates.VarcharCount count
                && !strings.containsKey(count.argument())) {
            // Frame reads stay outside state promotion. COUNT needs no payload or length.
            Var isNull = loop.let(frame.isNull(loop, count.argument()));
            strings.put(count.argument(), new Pipeline.Utf8Value(nil(), i64(0), isNull, bool(false), bool(true)));
        }
        var source = new ResolvedVarchar.Frame(frame.numeric(loop), row, nil(), nil(), strings);
        for (var update : updates.updates()) {
            Builder emitted = loop.fork();
            AggregateUpdates.emitUpdate(emitted, update, source, STATE, nil(), first, next);
            // Set mutations and allocation effects remain ordinary memory operations.
            // Only fixed reductions have their state promoted into loop registers.
            loop.append(update.allocates() ? emitted.block() : promote(emitted.block(), registers));
        }
    }

    private static Program filteredScan(AggregateUpdates updates, ResolvedFilter.Binding filter, Descriptor descriptor) {
        Builder b = new Builder();
        var registers = new LinkedHashMap<Load, Var>();
        for (var update : updates.updates()) if (!update.allocates()) {
            for (var cell : update.scalarState()) {
                var load = new Load(cell.width(), at(STATE, cell.offset()));
                registers.put(load, b.let(load));
            }
        }
        initializeFresh(b, updates, registers, descriptor.freshOffset());
        var addresses = updates.loadFrameInputs(b);
        Var first = updates.firstUpdateFlag(b, ne(load(at(COLUMNS, descriptor.freshOffset())), i64(0)));
        Var count = b.let(i64(0));
        ScanLowering.filteredRange(filter.plan(), filter.residual(), b, at(COLUMNS, descriptor.filterOffset()), i64(0),
                (selected, row) -> {
                    updateRow(selected, updates, addresses, registers, selected.let(row), false, first);
                    if (first != null) selected.assign(first, bool(false));
                    selected.assign(count, add(count, i64(1)));
                });
        b.when(ne(count, i64(0)), nonempty -> {
            for (var entry : registers.entrySet()) nonempty.store(entry.getKey().width(), entry.getKey().address(), entry.getValue());
        });
        b.exit(count);
        return new Program(b.block());
    }

    private static void initializeFresh(Builder b, AggregateUpdates updates, Map<Load, Var> registers, long offset) {
        updates.initializeEmptyState(b, STATE, ne(load(at(COLUMNS, offset)), i64(0)),
                (fresh, seeds) -> fresh.append(promote(seeds, registers)));
    }

    // Reuse the existing update definitions. Only their known state loads/stores become registers;
    // unexpected effects fail compilation instead of silently changing an aggregate's semantics.
    private static Block promote(Block block, Map<Load, Var> registers) {
        var result = new ArrayList<Statement>();
        for (Statement statement : block.statements()) result.add(switch (statement) {
            case Define d -> new Define(d.variable(), promote(d.value(), registers));
            case Store s -> new Assign(register(registers, new Load(s.width(), s.address())),
                    promote(s.value(), registers));
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
            case ToDouble cast -> new ToDouble(promote(cast.value(), registers));
            case ToInteger cast -> new ToInteger(promote(cast.value(), registers), cast.bits());
            case ToFloat cast -> new ToFloat(promote(cast.value(), registers));
            case Sqrt cast -> new Sqrt(promote(cast.value(), registers));
            case BitCast cast -> new BitCast(promote(cast.value(), registers), cast.type());
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
        b.when(ne(ROWS, i64(0)), existing -> updates.merge(existing, COLUMNS, STATE, false));
        b.exit(0);
        return new Program(b.block());
    }

    @Override public CharSequence getName() { return name; }
    @Override public Updater newUpdater(GroupByAllocator allocator) { return new Slot(allocator); }
    @Override public void merge(MapValue source, MapValue destination, GroupByAllocator allocator,
                                 SqlExecutionCircuitBreaker circuitBreaker) {
        try (var memory = KernelMemory.bind(allocator, circuitBreaker::statefulThrowExceptionIfTrippedTimeThrottled)) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            long status = mergeCode.run(source.getAddress(0), destination.isNew() ? 0 : 1, destination.getAddress(0));
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Scalar merge failed: " + status);
        }
    }
    @Override public void prepareExecution(io.questdb.std.MemoryTracker tracker) {
        if (filter != null) filter.prepareExecution(tracker);
    }
    @Override public void clearExecution() { if (filter != null) filter.close(); }
    @Override public void close() { try (filter; code; mergeCode; filteredCode) { } }

    /** Shared by emission and every slot; the selection pointer occupies the first word. */
    private record Descriptor(long filterOffset, long filterBytes) {
        long freshOffset() { return filterOffset + filterBytes; }
        long bytes() { return freshOffset() + 8; }
    }

    private final class Slot implements Updater {
        private final long bytes = descriptor.bytes();
        private final KernelMemory memory;
        private long addresses = Unsafe.calloc(bytes, MemoryTag.NATIVE_JIT);
        private final ResolvedVarchar.Slot helpers;
        private long nativeRows;

        Slot(GroupByAllocator allocator) {
            memory = KernelMemory.reusable(allocator);
            try { helpers = inputs.newSlot(addresses + 8); }
            catch (Throwable failure) { addresses = Unsafe.free(addresses, bytes, MemoryTag.NATIVE_JIT); throw failure; }
        }

        @Override public long getNativeRows() { return nativeRows; }
        @Override public long filterAndUpdate(PageFrameMemoryRecord record, long rows,
                                              MapValue value, SqlExecutionCircuitBreaker circuitBreaker) {
            if (filteredCode == null) return -1;
            if (addresses == 0) throw new IllegalStateException("Scalar aggregate slot is closed");
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (rows == 0) return 0;
            if (record instanceof PageFrameFilteredMemoryRecord || !inputs.prepare(record, addresses + 8)
                    || !filter.prepare(record, addresses + descriptor.filterOffset())) return -1;
            Unsafe.putLong(addresses + descriptor.freshOffset(), value.isNew() ? 1 : 0);
            memory.enter(circuitBreaker);
            try {
                long selected = filteredCode.run(addresses, rows, value.getAddress(0));
                memory.throwIfFailed();
                if (selected < 0 || selected > rows) throw new IllegalStateException("Invalid filtered reduction count: " + selected);
                if (selected > 0) value.setNew(false);
                nativeRows += selected;
                return selected;
            } finally {
                memory.close();
            }
        }

        @Override public boolean update(PageFrameMemoryRecord record, long rows, long rowIdsAddress,
                                        MapValue value, SqlExecutionCircuitBreaker circuitBreaker) {
            if (addresses == 0) throw new IllegalStateException("Scalar aggregate slot is closed");
            circuitBreaker.statefulThrowExceptionIfTripped();
            if (rows == 0) return true;
            if (record instanceof PageFrameFilteredMemoryRecord || !inputs.prepare(record, addresses + 8)) return false;
            Unsafe.putLong(addresses, rowIdsAddress);
            Unsafe.putLong(addresses + descriptor.freshOffset(), value.isNew() ? 1 : 0);
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
            if (addresses != 0) {
                try { if (helpers != null) helpers.close(); }
                finally { addresses = Unsafe.free(addresses, bytes, MemoryTag.NATIVE_JIT); }
            }
        }
    }
}
