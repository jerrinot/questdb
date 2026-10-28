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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.CompiledCounter;
import io.questdb.griffin.engine.CompiledProjection;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
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
    private final boolean handwritten;
    private final boolean fuseFilter;

    public ResolvedQueryCompiler(Backend backend, Path artifacts) {
        this(backend, artifacts, false);
    }

    ResolvedQueryCompiler(Backend backend, Path artifacts, boolean handwritten) {
        this(backend, artifacts, handwritten, false);
    }

    /** Harness-only alternative; no global setting or automatic admission policy. */
    static ResolvedQueryCompiler filterFusionExperiment(Backend backend, Path artifacts) {
        return new ResolvedQueryCompiler(backend, artifacts, false, true);
    }

    private ResolvedQueryCompiler(Backend backend, Path artifacts, boolean handwritten, boolean fuseFilter) {
        this.fuseFilter = fuseFilter;
        this.backend = backend;
        this.artifacts = artifacts;
        this.handwritten = handwritten;
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
    public CompiledCounter compileCounter(RecordCursorFactory input) {
        if (handwritten) return null;
        try { return ResolvedCounter.compile(input, backend, artifacts); }
        catch (IOException failure) { throw CairoException.nonCritical().put("COUNT compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public CompiledFilters compileFilters(ColumnTypes inputs, Function predicate) {
        if (handwritten) return null;
        try { return ResolvedStandaloneFilter.compile(inputs, predicate, backend, artifacts); }
        catch (IOException failure) { throw CairoException.nonCritical().put("Filter compilation failed: ").put(failure.getMessage()); }
    }

    @Override
    public CompiledProjection compileProjection(ColumnTypes inputs, int columnOffset, ObjList<Function> functions) {
        if (handwritten) return null;
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
        if (!handwritten && (ordered || computed)) {
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
            var prepared = handwritten ? binding.compileHandwritten(artifacts) : binding.compile(backend, artifacts, true);
            try {
                return new CompiledUpdates(prepared, handwritten ? "handwritten-c" : backend.name(), fuseFilter);
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
        if (handwritten) return null;
        try {
            return ResolvedJoinKernel.compile(joinedTypes, probeColumnCount, valueTypes, functions,
                    probeKeyColumn, value, postJoinFilter, groupKeyColumn, groupLayout, backend, artifacts);
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("Join compilation failed: ").put(failure.getMessage());
        }
    }

    private record CompiledUpdates(ResolvedAggregateKernel.Prepared prepared, String name, boolean fuseFilter) implements GroupByKernel {
        @Override
        public void prepareFilter(ColumnTypes inputs, Function function) {
            try { prepared.prepareFilter(inputs, function, fuseFilter); }
            catch (IOException failure) { throw CairoException.nonCritical().put("Filter compilation failed: ").put(failure.getMessage()); }
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
