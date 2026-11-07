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

package io.questdb.griffin.engine;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.CompiledProjection;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.griffin.engine.join.CompiledAsOfJoin;
import io.questdb.griffin.engine.join.CompiledAsOfLookup;
import io.questdb.griffin.engine.join.CompiledAsOfScan;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

/** Optional compilation of resolved operators. Core has no dependency on a backend. */
public interface QueryCodeGenerator {
    /** Stable RHS predicate and one raw INT/LONG/SYMBOL key; SYMBOL probes use the RHS dictionary. */
    default @Nullable CompiledAsOfLookup compileAsOfLookup(
            ColumnTypes inputs, Function filter, int keyColumn, int timestampColumn,
            long timestampScale, boolean finiteTolerance) {
        return null;
    }

    /** Borrows resolved ASOF inputs and the ordinary map and aggregate layouts during binding. */
    default @Nullable CompiledAsOfJoin compileAsOfJoin(
            CompiledAsOfJoin.Input input, Map layout, Function filter,
            ColumnTypes values, ObjList<GroupByFunction> functions, MapValue value) {
        return null;
    }

    /** Stable RHS predicate and raw column indexes in the existing ASOF map's key order. */
    default @Nullable CompiledAsOfScan compileAsOfScan(
            ColumnTypes inputs, ColumnTypes keyTypes, ListColumnFilter keyColumns,
            Map layout, Function filter, int timestampColumn, long timestampScale) {
        return null;
    }

    /** Planning-only preparation. A successful builder adopts its input, including on failure. */
    default @Nullable java.util.function.UnaryOperator<RecordCursorFactory> prepareGroupedTopK(
            RecordCursorFactory input, GroupedTopK plan, io.questdb.griffin.SqlExecutionContext context) { return null; }

    /** Direct base-column indexes; outputColumns maps window output columns to the base, or -1 for row number. */
    record GroupedTopK(io.questdb.cairo.sql.RecordMetadata outputMetadata, int[] partitionColumns,
                      int orderColumn, boolean ascending, int limit, int[] outputColumns) { }

    /** Borrows resolved vector functions and their initialized Rosti layout during binding. */
    default @Nullable io.questdb.griffin.engine.groupby.vect.CompiledVectorAggregate compileVectorAggregate(
            io.questdb.cairo.sql.RecordMetadata inputs, int keyColumn,
            ObjList<io.questdb.griffin.engine.groupby.vect.VectorAggregateFunction> functions,
            long rosti, int workerCount) { return null; }

    /** Borrows resolved operators and the actual unkeyed state layout; the factory retains function ownership. */
    default @Nullable CompiledScalarAggregate compileScalarAggregate(ColumnTypes inputs, ColumnTypes values,
            ObjList<GroupByFunction> functions, io.questdb.cairo.map.MapValue layout, @Nullable Function filter) { return null; }

    /** Only bind inputs whose calculateSize supplies bulk counts, without enumerating rows. */
    default @Nullable CompiledCounter compileCounter(@Transient RecordCursorFactory input) { return null; }

    /** Optional encoded Top-K; ordinary cursors and buffers retain scheduling and memory ownership. */
    default @Nullable io.questdb.griffin.engine.orderby.CompiledTopK compileTopK(
            ColumnTypes inputs, io.questdb.cairo.ListColumnFilter order, @Nullable Function filter) { return null; }

    /** Resolved selection/count kernels using the existing filter ABI and cursor ownership. */
    default @Nullable CompiledFilters compileFilters(ColumnTypes inputs, Function predicate) { return null; }

    record CompiledFilters(io.questdb.jit.CompiledFilter rows, io.questdb.jit.CompiledCountOnlyFilter count) { }

    /**
     * A completed ordinary join map, borrowed only during owner-thread preparation.
     * Immutable column/type arrays follow RecordSink key order. Keys use the resolved
     * comparison types and the probe columns' SYMBOL dictionaries.
     * A null map means pass-through (for example, the join swapped build/probe sides).
     * Generated filters must copy keys into their own tracked state before worker dispatch.
     */
    record JoinFilterSource(int[] probeColumns, int[] keyTypes, int mapKeyColumn,
                            java.util.function.Supplier<Map> completedBuild) { }

    /** The row kernel owns execution state; the Java fallback and count kernel borrow it. */
    record JoinFilter(Function fallback, CompiledFilters compiled) { }

    default @Nullable JoinFilter compileJoinFilter(ColumnTypes inputs, ObjList<JoinFilterSource> sources) { return null; }

    /**
     * Borrows planning inputs only for this call. Return null for unsupported implementations;
     * preparation failures propagate. Do not initialize or retain the resolved functions.
     * The caller adopts a successful result and closes it after all slot updaters finish.
     */
    @Nullable GroupByKernel compile(
            @Transient ColumnTypes inputTypes,
            @Transient ColumnTypes valueTypes,
            @Transient ObjList<GroupByFunction> functions
    );

    /** A direct fixed-width key, or keyColumn=-1 when ordinary key evaluation is required. */
    default @Nullable GroupByKernel compile(ColumnTypes inputTypes, ColumnTypes valueTypes,
                                      ObjList<GroupByFunction> functions, int keyColumn, int keyType, Map layout) {
        return compile(inputTypes, valueTypes, functions);
    }

    /** Direct physical keys in the same order used by the existing RecordSink. */
    default @Nullable GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                                      ColumnTypes keyTypes, io.questdb.cairo.ListColumnFilter keyColumns, Map layout) {
        int key = keyTypes.getColumnCount() == 1 && keyColumns.size() == 1 && keyColumns.getQuick(0) > 0
                ? keyColumns.getQuick(0) - 1 : -1;
        return compile(inputs, values, functions, key, key < 0 ? -1 : keyTypes.getColumnType(0), layout);
    }

    /** RecordSink writes direct columns first, then these computed keys. Planning-only borrow. */
    default @Nullable GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                                      ColumnTypes keyTypes, io.questdb.cairo.ListColumnFilter keyColumns,
                                      @Nullable ObjList<Function> keyFunctions, Map layout) {
        return keyFunctions == null || keyFunctions.size() == 0
                ? compile(inputs, values, functions, keyTypes, keyColumns, layout)
                : compile(inputs, values, functions, -1, -1, layout);
    }

    /**
     * Legacy single-INT INNER join pipeline. Joined columns place probe columns before build payloads.
     * A non-null groupLayout denotes keyed state; otherwise value supplies scalar state offsets.
     * The caller excludes SYMBOL translation and existing count-only specializations. Functions,
     * the resolved post-join predicate and layouts are borrowed only during compilation. Reject
     * unsupported pipelines before adoption; native payload views may decline before mutation.
     */
    default @Nullable CompiledJoin compileJoin(ColumnTypes joinedTypes, int probeColumnCount,
                                               ColumnTypes valueTypes, ObjList<GroupByFunction> functions,
                                               int probeKeyColumn, @Nullable MapValue value, @Nullable Function postJoinFilter,
                                               int groupKeyColumn, @Nullable Map groupLayout) {
        return null;
    }

    /** Common-probe INNER joins; payload columns follow the probe in build-list order. */
    default @Nullable CompiledJoin compileJoin(ColumnTypes joinedTypes, int probeColumnCount,
                                               ColumnTypes valueTypes, ObjList<GroupByFunction> functions,
                                               ObjList<CompiledJoin.Build> builds, @Nullable MapValue value,
                                               @Nullable Function postJoinFilter, int groupKeyColumn, @Nullable Map groupLayout) {
        // The legacy hook describes one INT key; never discard typed/composite components.
        if (builds.size() != 1 || builds.getQuick(0).keyColumns().length != 1
                || builds.getQuick(0).keyTypes().length != 1
                || builds.getQuick(0).keyTypes()[0] != io.questdb.cairo.ColumnType.INT) {
            return null;
        }
        return compileJoin(joinedTypes, probeColumnCount, valueTypes, functions,
                builds.getQuick(0).keyColumn(), value, postJoinFilter, groupKeyColumn, groupLayout);
    }

    /** Group keys use the same RecordSink layout as ordinary aggregation. Planning-only borrow. */
    default @Nullable CompiledJoin compileJoin(ColumnTypes joinedTypes, int probeColumnCount,
                                               ColumnTypes valueTypes, ObjList<GroupByFunction> functions,
                                               ObjList<CompiledJoin.Build> builds, @Nullable MapValue value,
                                               @Nullable Function postJoinFilter, ColumnTypes keyTypes,
                                               io.questdb.cairo.ListColumnFilter keyColumns,
                                               @Nullable ObjList<Function> keyFunctions, @Nullable Map groupLayout) {
        int key = -1;
        if (groupLayout != null) {
            if (keyTypes.getColumnCount() != 1 || keyColumns.size() != 1 || keyColumns.getQuick(0) <= 0
                    || keyFunctions != null && keyFunctions.size() != 0) {
                return null;
            }
            key = keyColumns.getQuick(0) - 1;
        }
        return compileJoin(joinedTypes, probeColumnCount, valueTypes, functions, builds, value,
                postJoinFilter, key, groupLayout);
    }

    /** Pure expressions over a random-access input. The factory owns the compiled buffers/code. */
    default @Nullable CompiledProjection compileProjection(ColumnTypes inputs, int columnOffset,
                                                           ObjList<Function> functions) { return null; }

}
