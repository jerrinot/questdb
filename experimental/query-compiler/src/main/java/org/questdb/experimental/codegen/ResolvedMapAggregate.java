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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.griffin.engine.groupby.CompiledMapAggregate;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.groupby.MapSymbolColumn;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Consumes finalized map rows in the producer's existing exclusive worker slots. */
final class ResolvedMapAggregate implements CompiledMapAggregate {
    private final CompiledKernel consume;
    private final ResolvedAggregateKernel.Prepared output;
    private final ArrayList<Slot> slots = new ArrayList<>();
    private boolean active;
    private AggregateSlot merger;

    private ResolvedMapAggregate(ResolvedAggregateKernel.Prepared output, CompiledKernel consume) {
        this.output = output;
        this.consume = consume;
    }

    static CompiledMapAggregate compile(
            CairoConfiguration configuration, ResolvedAggregateKernel.Prepared source,
            ColumnTypes sourceInputs, ObjList<Function> sourceFunctions, ObjList<GroupByKernel.Updater> sourceSlots,
            ObjList<RecordCursorFactory> projections, ColumnTypes keys, ListColumnFilter keyColumns,
            ObjList<Function> keyFunctions, ColumnTypes values, ObjList<GroupByFunction> functions, Map layout
    ) throws IOException {
        if (source.probe == null || !source.probe.mutable() || source.probe.variable() || source.probe.varchar()) return null;
        for (int i = 0; i < sourceSlots.size(); i++) {
            if (!(sourceSlots.getQuick(i) instanceof AggregateSlot slot) || slot.mapConsumer != null) return null;
        }
        ColumnTypes inputs = projections.size() == 0 ? sourceInputs : projections.getQuick(0).getMetadata();
        QuestFixedKey key = QuestFixedKey.bind(new ResolvedInputs(inputs, source.binding.options), keys, keyColumns, keyFunctions, layout);
        if (key == null) return null;
        QuestMapProbe probe = QuestMapProbe.fixed(key, layout instanceof OrderedMap ? -1 : keys.getColumnType(0),
                layout instanceof OrderedMap).mutating();
        var binding = ResolvedAggregateKernel.bind(inputs, values, functions, probe, source.binding.options);
        if (binding == null || binding.allocates) return null;

        Read[] sourceReads = new Read[sourceInputs.getColumnCount()];
        for (int i = 0; i < sourceReads.length; i++) sourceReads[i] = read(source, sourceFunctions.getQuick(i));
        for (int i = projections.size() - 1; i >= 0; i--) {
            sourceReads = project(projections.getQuick(i), sourceReads, source.binding.options);
            if (sourceReads == null) return null;
        }
        final Read[] reads = sourceReads;
        // Dictionary and string descriptors belong to frames; this consumer reads numeric map cells.
        if (!key.dictionaries().isEmpty() || !key.frameInputs().expressions().isEmpty()
                || !binding.updates.inputs().expressions().isEmpty()) return null;
        for (int i : key.columns()) if (reads[i] == null) return null;
        for (int i : binding.updates.sourceIndexes()) if (reads[i] == null) return null;

        var output = source.compileConsumer(binding);
        CompiledKernel code = null;
        ResolvedMapAggregate result = null;
        try {
            Builder b = new Builder();
            var parameters = binding.updates.loadMapParameters(b);
            Var position = b.let(i64(0)), begin = b.let(load(STATE));
            QuestMapScan.emit(b, source.probe, COLUMNS, (loop, entry) -> {
                // Growth resumes at the first uncommitted source row, in ordinary map cursor order.
                loop.when(binary(Op.GE, position, begin), emit -> {
                    Pipeline.Row keyRow = row(emit, entry, reads, key.columns(), key.inputs());
                    Var packed = probe.orderedNative()
                            ? key.stage(emit, keyRow, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)))
                            : key.scalar(emit, keyRow);
                    Var first = binding.updates.firstRowFlag(emit);
                    Var destination = probe.find(emit, packed, position, i64(0), null, binding.updates.initializer(first));
                    var arguments = row(emit, entry, reads, binding.updates.sourceIndexes(), binding.updates.columns());
                    binding.updates.emitInitialized(emit, new Pipeline.Row(arguments.columns(), arguments.values(), parameters),
                            destination, at(STATE, 40), first);
                });
                loop.assign(position, add(position, i64(1)));
            });
            b.exit(0);
            code = source.compileConsumer(new Program(b.block()));
            result = new ResolvedMapAggregate(output, code);
            for (int i = 0; i < sourceSlots.size(); i++) {
                result.slots.add(result.new Slot(configuration, keys, values, (AggregateSlot) sourceSlots.getQuick(i)));
            }
            // Publish only after every output slot is ready. The source retains map ownership.
            for (Slot slot : result.slots) slot.producer.mapConsumer = slot;
            return result;
        } catch (Throwable failure) {
            if (result != null) {
                try { result.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            } else {
                try { if (code != null) code.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                try { output.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            }
            throw failure;
        }
    }

    @Override
    public void clear() {
        active = false;
        if (merger != null) merger.clear();
        for (Slot slot : slots) {
            slot.updater.clear();
            slot.map.close();
            slot.allocator.close();
        }
    }

    @Override
    public void close() {
        active = false;
        Throwable failure = null;
        for (Slot slot : slots) {
            if (slot.producer.mapConsumer == slot) slot.producer.mapConsumer = null;
            failure = Misc.freeBestEffort(failure, slot.updater);
            failure = Misc.freeBestEffort(failure, slot.map);
            failure = Misc.freeBestEffort(failure, slot.allocator);
        }
        slots.clear();
        failure = Misc.freeBestEffort(failure, merger);
        merger = null;
        try { consume.close(); } catch (Throwable cleanup) { failure = Misc.foldCleanupFailure(failure, cleanup); }
        try { output.close(); } catch (Throwable cleanup) { failure = Misc.foldCleanupFailure(failure, cleanup); }
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void finish(Map destination, GroupByAllocator allocator, Runnable checkpoint) {
        if (!active) throw new IllegalStateException("Map consumer is not open");
        if (merger == null) merger = output.newSlot(allocator);
        merger.prepareParameters();
        for (Slot slot : slots) {
            if (!merger.merge(destination, slot.map, checkpoint)) throw new IllegalStateException("Map consumer merge declined");
        }
        merger.finalizeMap(destination, checkpoint);
    }

    @Override
    public CharSequence getName() {
        return "generated finalized maps";
    }

    @Override
    public ObjList<Function> outputFunctions(ObjList<Function> functions) {
        return output.outputFunctions(functions);
    }

    @Override
    public boolean longTopK(Map map, DirectLongLongSortedList list, Function function, Runnable checkpoint) {
        return merger.longTopK(map, list, function, checkpoint);
    }

    @Override
    public void prepareLongTopK(Function function) {
        try { output.prepareLongTopK(function); }
        catch (IOException failure) { throw CairoException.nonCritical().put("Map Top-K compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public void prepareLongTopK(Function function, boolean ascending, int limit) {
        try { output.prepareLongTopK(function, ascending ? 0 : 1, limit); }
        catch (IOException failure) { throw CairoException.nonCritical().put("Map Top-K compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public boolean supportsLongTopK(Function function) {
        return output.supportsLongTopK(function);
    }

    @Override
    public void reopen(MemoryTracker tracker) {
        clear();
        try {
            for (Slot slot : slots) {
                slot.map.setMemoryTracker(tracker);
                slot.map.reopen();
                slot.allocator.setMemoryTracker(tracker);
                slot.allocator.reopen();
                slot.updater.prepareParameters();
            }
            active = true;
        } catch (Throwable failure) {
            try { clear(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static Read[] project(RecordCursorFactory factory, Read[] source, CompilerOptions options) {
        Read[] result = new Read[factory.getMetadata().getColumnCount()];
        if (factory instanceof SelectedRecordCursorFactory selected) {
            for (int i = 0; i < result.length; i++) result[i] = source[selected.getColumnCrossIndex().getQuick(i)];
        } else if (factory instanceof VirtualRecordCursorFactory virtual) {
            int offset = virtual.getPriorityMetadata().getVirtualColumnReservedSlots();
            for (int i = 0; i < result.length; i++) {
                Function function = virtual.getFunctions().getQuick(i);
                while (function instanceof MemoizerFunction memo) function = memo.getArg();
                ResolvedInputs inputs = new ResolvedInputs(factory.getBaseFactory().getMetadata(), offset, options);
                Pipeline.Scalar expression = inputs.scalar(function, function.getType());
                if (expression == null) return null;
                int[] indexes = inputs.indexes();
                for (int j = 0; j < indexes.length; j++) {
                    indexes[j] -= offset;
                    if (source[indexes[j]] == null) return null;
                }
                result[i] = (b, entry) -> expression.emit(row(b, entry, source, indexes, inputs.columns()));
            }
        } else {
            return null;
        }
        return result;
    }

    private static Read read(ResolvedAggregateKernel.Prepared source, Function function) {
        int type = function.getType();
        MemoryType width = switch (type) {
            case ColumnType.BOOLEAN, ColumnType.BYTE -> MemoryType.I8;
            case ColumnType.SHORT, ColumnType.CHAR -> MemoryType.I16;
            case ColumnType.INT, ColumnType.SYMBOL, ColumnType.IPv4 -> MemoryType.I32;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> MemoryType.I64;
            case ColumnType.DOUBLE -> MemoryType.F64;
            case ColumnType.FLOAT -> MemoryType.F32;
            default -> null;
        };
        if (width == null) return null;
        if (function instanceof GroupByFunction) {
            long offset = source.numericResultOffset(function);
            return offset < 0 || (type != ColumnType.BYTE && type != ColumnType.SHORT && type != ColumnType.CHAR && type != ColumnType.BOOLEAN && type != ColumnType.IPv4 && type != ColumnType.INT && type != ColumnType.LONG && type != ColumnType.DOUBLE && type != ColumnType.FLOAT
                    && type != ColumnType.DATE && !ColumnType.isTimestamp(type))
                    ? null : new Stored(width, source.probe.keyBytes() + offset, type);
        }
        Class<?> implementation = function.getClass();
        int column;
        if (implementation == MapSymbolColumn.class && ((MapSymbolColumn) function).isSymbolTableStatic()) {
            // The ordinary function retains its dictionary cursor index and ownership.
            column = ((MapSymbolColumn) function).getMapColumnIndex();
        } else if (implementation == io.questdb.griffin.engine.functions.columns.BooleanColumn.class
                || implementation == io.questdb.griffin.engine.functions.columns.IPv4Column.class
                || implementation == io.questdb.griffin.engine.functions.columns.CharColumn.class
                || implementation == ByteColumn.class || implementation == ShortColumn.class
                || implementation == IntColumn.class || implementation == LongColumn.class || implementation == DoubleColumn.class || implementation == FloatColumn.class
                || implementation == DateColumn.class || implementation == TimestampColumn.class) {
            column = ((ColumnFunction) function).getColumnIndex();
        } else return null;
        int key = column - source.binding.valueColumnCount;
        if (key < 0) return null;
        QuestFixedKey fixed = source.probe.fixedKey();
        if (fixed == null) return key == 0 && width.bytes == source.probe.keyBytes() ? new Stored(width, 0, type) : null;
        MemoryType storedWidth = type == ColumnType.DOUBLE ? MemoryType.I64 : type == ColumnType.FLOAT ? MemoryType.I32 : width;
        if (key >= fixed.widths().length || storedWidth != fixed.widths()[key]) return null;
        long offset = 0;
        for (int i = 0; i < key; i++) offset += fixed.widths()[i].bytes;
        return new Stored(width, offset, type);
    }

    private static Pipeline.Row row(Builder b, Expr entry, Read[] reads, int[] indexes, List<Pipeline.Column> columns) {
        var values = new ArrayList<Pipeline.Value>();
        for (int index : indexes) values.add(reads[index].emit(b, entry));
        return new Pipeline.Row(columns, values);
    }

    private interface Read {
        Pipeline.Value emit(Builder b, Expr entry);
    }

    private record Stored(MemoryType width, long offset, int type) implements Read {
        @Override
        public Pipeline.Value emit(Builder b, Expr entry) {
            Var value = b.let(new Load(width, at(entry, offset)));
            Pipeline.Column column = new Pipeline.Column(0, "stored", type);
            return column.value(value);
        }
    }

    final class Slot {
        private final GroupByAllocator allocator;
        private final Map map;
        private final AggregateSlot producer;
        private final AggregateSlot updater;

        private Slot(CairoConfiguration configuration, ColumnTypes keys, ColumnTypes values, AggregateSlot producer) {
            this.producer = producer;
            Map map = MapFactory.createUnorderedMap(configuration, keys, values, true, false);
            GroupByAllocator allocator = null;
            try {
                allocator = GroupByAllocatorFactory.createAllocator(configuration, false);
                this.updater = output.newSlot(allocator);
                this.map = map;
                this.allocator = allocator;
            } catch (Throwable failure) {
                Throwable cleanup = Misc.freeBestEffort(null, map);
                cleanup = Misc.freeBestEffort(cleanup, allocator);
                if (cleanup != null) failure.addSuppressed(cleanup);
                throw failure;
            }
        }

        void consume(Map source, Runnable checkpoint) {
            if (active) updater.consumeMap(consume, source, map, checkpoint);
        }
    }
}
