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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.engine.CompiledCounter;
import io.questdb.griffin.engine.CompiledProjection;
import io.questdb.griffin.engine.CompiledScalarAggregate;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.CompiledMapAggregate;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByKernel.Updater;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.std.ObjList;

import java.io.IOException;
import java.nio.file.Path;

/** Explicit opt-in for ordinary SQL planning; no experimental SQL resolver is involved. */
public final class ResolvedQueryCompiler implements QueryCodeGenerator {
    private final Path artifacts;
    private final Backend backend;
    private final boolean fuseFilter;

    public ResolvedQueryCompiler(Backend backend, Path artifacts) {
        this(backend, artifacts, false);
    }

    /** Harness-only alternative; no global setting or automatic admission policy. */
    static ResolvedQueryCompiler filterFusionExperiment(Backend backend, Path artifacts) {
        return new ResolvedQueryCompiler(backend, artifacts, true);
    }

    private ResolvedQueryCompiler(Backend backend, Path artifacts, boolean fuseFilter) {
        this.fuseFilter = fuseFilter;
        this.backend = backend;
        this.artifacts = artifacts;
    }

    /** Read only after the owning frame sequence has joined; diagnostic coverage, not shared state. */
    static long updatedRows(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.updatedRows() : 0;
    }

    static long sharedRows(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.sharedRows() : 0;
    }

    static long limitedRows(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.limitedRows() : 0;
    }

    static long unionTasks(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.unionTasks() : 0;
    }

    static long sortedRows(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.sortedRows() : 0;
    }

    static long finalizedRows(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.finalizedRows() : 0;
    }

    static long partitionRows(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.partitionRows() : 0;
    }

    static long[] profileCounters(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.profileCounters() : new long[4];
    }

    static long[] filterCounters(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.filterCounters() : new long[2];
    }

    static long[] topKCounters(Updater updater) {
        return updater instanceof AggregateSlot slot ? slot.topKCounters() : new long[2];
    }

    @Override
    public io.questdb.griffin.engine.join.CompiledAsOfLookup compileAsOfLookup(
            ColumnTypes inputs, Function filter, int keyColumn, int timestampColumn,
            long timestampScale, boolean finiteTolerance) {
        return ResolvedAsOfLookup.compile(inputs, filter, keyColumn, timestampColumn,
                timestampScale, finiteTolerance, backend, artifacts);
    }

    @Override
    public io.questdb.griffin.engine.join.CompiledAsOfJoin compileAsOfJoin(
            io.questdb.griffin.engine.join.CompiledAsOfJoin.Input input, Map layout, Function filter,
            ColumnTypes values, ObjList<GroupByFunction> functions, io.questdb.cairo.map.MapValue value) {
        try {
            return ResolvedAsOfJoin.compile(input, layout, filter, values, functions, value, backend, artifacts);
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("ASOF aggregate compilation failed: ").put(failure.getMessage());
        }
    }

    @Override
    public io.questdb.griffin.engine.join.CompiledAsOfScan compileAsOfScan(
            ColumnTypes inputs, ColumnTypes keyTypes, io.questdb.cairo.ListColumnFilter columns,
            Map layout, Function filter, int timestampColumn, long timestampScale) {
        try {
            return ResolvedAsOfScan.compile(inputs, keyTypes, columns, layout, filter,
                    timestampColumn, timestampScale, backend, artifacts);
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("ASOF scan compilation failed: ").put(failure.getMessage());
        }
    }

    @Override
    public java.util.function.UnaryOperator<RecordCursorFactory> prepareGroupedTopK(
            RecordCursorFactory input, GroupedTopK plan, io.questdb.griffin.SqlExecutionContext context) {
        return ResolvedGroupedTopK.prepare(input, plan, context, this);
    }

    @Override
    public io.questdb.griffin.engine.groupby.vect.CompiledVectorAggregate compileVectorAggregate(
            io.questdb.cairo.sql.RecordMetadata inputs, int keyColumn,
            ObjList<io.questdb.griffin.engine.groupby.vect.VectorAggregateFunction> functions,
            long rosti, int workerCount) {
        return ResolvedSymbolAggregate.bind(inputs, keyColumn, functions, rosti, workerCount, backend, artifacts);
    }

    @Override
    public CompiledScalarAggregate compileScalarAggregate(ColumnTypes inputs, ColumnTypes values,
            ObjList<GroupByFunction> functions, MapValue layout, Function filter) {
        try { return ResolvedScalarAggregate.compile(inputs, values, functions, layout, filter, backend, artifacts); }
        catch (IOException failure) { throw CairoException.nonCritical().put("Scalar aggregate compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public CompiledCounter compileCounter(RecordCursorFactory input) {
        try { return ResolvedCounter.compile(input, backend, artifacts); }
        catch (IOException failure) { throw CairoException.nonCritical().put("COUNT compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public io.questdb.griffin.engine.orderby.CompiledTopK compileTopK(ColumnTypes inputs, io.questdb.cairo.ListColumnFilter order, Function filter) {
        try { return ResolvedTopK.compile(inputs, order, filter, backend, artifacts); }
        catch (IOException failure) { throw CairoException.nonCritical().put("Top-K build compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public CompiledFilters compileFilters(ColumnTypes inputs, Function predicate) {
        try { return ResolvedStandaloneFilter.compile(inputs, predicate, backend, artifacts); }
        catch (IOException failure) { throw CairoException.nonCritical().put("Filter compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public JoinFilter compileJoinFilter(ColumnTypes inputs, ObjList<IntJoinFilterSource> sources) {
        try { return ResolvedJoinFilter.compile(inputs, sources, backend, artifacts); }
        catch (IOException failure) { throw CairoException.nonCritical().put("Join filter compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public CompiledProjection compileProjection(ColumnTypes inputs, int columnOffset, ObjList<Function> functions) {
        try { return ResolvedProjection.compile(inputs, columnOffset, functions, backend, artifacts); }
        catch (IOException e) { throw new IllegalStateException("Could not compile projection", e); }
    }

    @Override
    public GroupByKernel compile(ColumnTypes inputTypes, ColumnTypes valueTypes, ObjList<GroupByFunction> functions) {
        return compileBound(ResolvedAggregateKernel.bind(inputTypes, valueTypes, functions));
    }

    @Override
    public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                            int keyColumn, int keyType, Map layout) {
        QuestMapProbe probe = QuestMapProbe.bind(keyColumn, keyType, layout);
        return compileBound(ResolvedAggregateKernel.bind(inputs, values, functions, probe));
    }

    @Override
    public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                            ColumnTypes keyTypes, io.questdb.cairo.ListColumnFilter keyColumns, Map layout) {
        return compile(inputs, values, functions, keyTypes, keyColumns, null, layout);
    }

    @Override
    public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                            ColumnTypes keyTypes, io.questdb.cairo.ListColumnFilter keyColumns,
                            ObjList<Function> keyFunctions, Map layout) {
        boolean computed = keyFunctions != null && keyFunctions.size() > 0;
        boolean ordered = layout instanceof io.questdb.cairo.map.OrderedMap;
        if (computed) {
            var key = QuestVarchar.Key.bind(inputs, keyTypes, keyColumns, keyFunctions, layout);
            if (key != null) return compileBound(ResolvedAggregateKernel.bind(inputs, values, functions, QuestMapProbe.varchar(key)));
        }
        if (ordered || computed) {
            var variable = QuestVariableKey.bind(inputs, keyTypes, keyColumns, keyFunctions, layout);
            if (variable != null) return compileBound(ResolvedAggregateKernel.bind(inputs, values, functions,
                    QuestMapProbe.variable(variable)));
            var key = QuestFixedKey.bind(inputs, keyTypes, keyColumns, keyFunctions, layout);
            if (key != null) return compileBound(ResolvedAggregateKernel.bind(inputs, values, functions,
                    QuestMapProbe.fixed(key, ordered ? -1 : keyTypes.getColumnType(0), ordered)));
        }
        if (computed) return compile(inputs, values, functions, -1, -1, layout);
        return QueryCodeGenerator.super.compile(inputs, values, functions, keyTypes, keyColumns, layout);
    }

    private GroupByKernel compileBound(ResolvedAggregateKernel.Binding binding) {
        if (binding == null) {
            return null;
        }
        try {
            var prepared = binding.compile(backend, artifacts, true);
            try {
                return new CompiledUpdates(prepared, backend.name(), fuseFilter);
            } catch (Throwable failure) {
                try { prepared.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("Aggregate compilation failed: ").put(failure.getMessage());
        }
    }

    @Override
    public CompiledJoin compileJoin(ColumnTypes joinedTypes, int probeColumnCount, ColumnTypes valueTypes,
                                    ObjList<GroupByFunction> functions, int probeKeyColumn, MapValue value,
                                    Function postJoinFilter, int groupKeyColumn, Map groupLayout) {
        try {
            return ResolvedJoinKernel.compile(joinedTypes, probeColumnCount, valueTypes, functions,
                    probeKeyColumn, value, postJoinFilter, groupKeyColumn, groupLayout, backend, artifacts);
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("Join compilation failed: ").put(failure.getMessage());
        }
    }

    @Override
    public CompiledJoin compileJoin(ColumnTypes joinedTypes, int probeColumnCount, ColumnTypes valueTypes,
                                    ObjList<GroupByFunction> functions, ObjList<CompiledJoin.Build> builds, MapValue value,
                                    Function postJoinFilter, int groupKeyColumn, Map groupLayout) {
        try {
            return ResolvedJoinKernel.compile(joinedTypes, probeColumnCount, valueTypes, functions,
                    builds, value, postJoinFilter, groupKeyColumn, groupLayout, backend, artifacts);
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("Join compilation failed: ").put(failure.getMessage());
        }
    }

    @Override
    public CompiledJoin compileJoin(ColumnTypes joinedTypes, int probeColumnCount, ColumnTypes valueTypes,
                                    ObjList<GroupByFunction> functions, ObjList<CompiledJoin.Build> builds, MapValue value,
                                    Function postJoinFilter, ColumnTypes keyTypes, io.questdb.cairo.ListColumnFilter keyColumns,
                                    ObjList<Function> keyFunctions, Map groupLayout) {
        try {
            return ResolvedJoinKernel.compile(joinedTypes, probeColumnCount, valueTypes, functions,
                    builds, value, postJoinFilter, keyTypes, keyColumns, keyFunctions, groupLayout, backend, artifacts);
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("Join compilation failed: ").put(failure.getMessage());
        }
    }

    record CompiledUpdates(ResolvedAggregateKernel.Prepared prepared, String name, boolean fuseFilter) implements GroupByKernel {
        @Override
        public CompiledMapAggregate compileMapAggregate(
                CairoConfiguration configuration, ColumnTypes inputs,
                ObjList<Function> sourceFunctions, ObjList<Updater> sourceSlots,
                ObjList<RecordCursorFactory> projections, ColumnTypes keys,
                io.questdb.cairo.ListColumnFilter keyColumns, ObjList<Function> keyFunctions,
                ColumnTypes values, ObjList<GroupByFunction> functions, Map layout) {
            try { return ResolvedMapAggregate.compile(configuration, prepared, inputs, sourceFunctions,
                    sourceSlots, projections, keys, keyColumns, keyFunctions, values, functions, layout); }
            catch (IOException failure) { throw CairoException.nonCritical().put("Map aggregate compilation failed: ").put(failure.getMessage()); }
        }

        @Override
        public void prepareFilter(ColumnTypes inputs, Function function) {
            try { prepared.prepareFilter(inputs, function, fuseFilter); }
            catch (IOException failure) { throw CairoException.nonCritical().put("Filter compilation failed: ").put(failure.getMessage()); }
        }

        @Override
        public void prepareExecution(io.questdb.std.MemoryTracker tracker, SqlExecutionCircuitBreaker breaker, long inputRows, int workerSlots) {
            try {
                if (prepared.filter != null) prepared.filter.prepareExecution(tracker);
                if (prepared.probe != null && prepared.probe.fixedKey() != null)
                    prepared.probe.fixedKey().prepareExecution(tracker, breaker, inputRows < 0 ? -1 : inputRows / workerSlots);
                if (prepared.dictionaryAggregate != null) {
                    prepared.dictionaryAggregate.prepare(inputRows < 0 ? -1 : inputRows / workerSlots);
                    if (prepared.dictionaryAggregate.entries() == 0) prepared.mapCode.prepare();
                }
            } catch (Throwable failure) {
                clearExecution();
                throw failure;
            }
        }

        @Override
        public void clearExecution() {
            if (prepared.dictionaryAggregate != null) prepared.dictionaryAggregate.clear();
            if (prepared.filter != null) prepared.filter.clearExecution();
            if (prepared.probe != null && prepared.probe.fixedKey() != null) prepared.probe.fixedKey().clearExecution();
        }

        @Override
        public void finishScan(java.util.function.Supplier<Map> destination, Runnable checkpoint) {
            if (prepared.dictionaryAggregate != null) prepared.dictionaryAggregate.finishScan(destination, checkpoint);
        }

        @Override
        public boolean hasCompiledFilter() { return prepared.hasCompiledFilter(); }

        @Override
        public ObjList<Function> outputFunctions(ObjList<Function> functions) { return prepared.outputFunctions(functions); }

        @Override
        public boolean needsWorkerAllocators() { return prepared.needsWorkerAllocators(); }

        @Override
        public boolean prepareSortedRowLimit() { return prepareRowLimit(); }

        @Override
        public boolean prepareGroupLimit(long requiredGroups) {
            try { return prepared.prepareGroupLimit(requiredGroups); }
            catch (IOException failure) { throw CairoException.nonCritical().put("Group admission compilation failed: ").put(failure.getMessage()); }
        }

        @Override
        public boolean limitGroups(long requiredGroups, UnorderedPageFrameSequence<?> frames,
                                   PageFrameMemoryPool pool, Function filter, SqlExecutionCircuitBreaker circuitBreaker) {
            return prepared.groupAdmission != null
                    && prepared.groupAdmission.discover(requiredGroups, frames, pool, filter, circuitBreaker);
        }

        @Override
        public void clearGroupLimit() {
            if (prepared.groupAdmission != null) prepared.groupAdmission.clear();
        }

        @Override
        public boolean prepareRowLimit() {
            try { return prepared.prepareRowLimit(); }
            catch (IOException failure) { throw CairoException.nonCritical().put("LIMIT compilation failed: ").put(failure.getMessage()); }
        }

        @Override
        public boolean prepareLongSort(Function function) {
            try { return prepared.prepareLongSort(function); }
            catch (IOException failure) { throw CairoException.nonCritical().put("Sort compilation failed: ").put(failure.getMessage()); }
        }

        @Override
        public boolean supportsLongTopK(Function function) { return prepared.supportsLongTopK(function); }

        @Override
        public void prepareLongTopK(Function function) {
            try { prepared.prepareLongTopK(function); }
            catch (IOException failure) { throw CairoException.nonCritical().put("Top-K compilation failed: ").put(failure.getMessage()); }
        }

        @Override
        public void prepareLongTopK(Function function, boolean ascending, int limit) {
            int order = ascending ? 0 : 1;
            // Only the small-capacity variant passed the execution regression gate.
            int capacity = QuestLongTopK.capacityClass(limit) == 0 ? limit : 0;
            try { prepared.prepareLongTopK(function, order, capacity); }
            catch (IOException failure) { throw CairoException.nonCritical().put("Top-K compilation failed: ").put(failure.getMessage()); }
        }

        @Override
        public void close() {
            prepared.close();
        }

        @Override
        public CharSequence getName() {
            return name;
        }

        @Override
        public Updater newUpdater(GroupByAllocator allocator) {
            return prepared.newSlot(allocator);
        }
    }
}
