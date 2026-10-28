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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.CompiledProjection;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

/** Optional compilation of resolved operators. Core has no dependency on a backend. */
public interface QueryCodeGenerator {
    /** Only bind inputs whose calculateSize supplies bulk counts, without enumerating rows. */
    default @Nullable CompiledCounter compileCounter(@Transient RecordCursorFactory input) { return null; }

    /** Resolved selection/count kernels using the existing filter ABI and cursor ownership. */
    default @Nullable CompiledFilters compileFilters(ColumnTypes inputs, Function predicate) { return null; }

    record CompiledFilters(io.questdb.jit.CompiledFilter rows, io.questdb.jit.CompiledCountOnlyFilter count) { }

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
     * Optional INNER join pipeline. Joined columns place probe columns before build payloads.
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

    /** Pure expressions over a random-access input. The factory owns the compiled buffers/code. */
    default @Nullable CompiledProjection compileProjection(ColumnTypes inputs, int columnOffset,
                                                           ObjList<Function> functions) { return null; }

}
