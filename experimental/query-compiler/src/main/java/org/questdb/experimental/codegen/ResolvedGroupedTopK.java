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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.join.UnnestRecordCursorFactory;
import io.questdb.griffin.engine.join.UnnestSource;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.*;

import java.util.function.UnaryOperator;

/** Bounded numeric ranking lowered to the existing parallel group-by and expansion cursors. */
final class ResolvedGroupedTopK {
    private ResolvedGroupedTopK() { }

    static UnaryOperator<RecordCursorFactory> prepare(RecordCursorFactory input, QueryCodeGenerator.GroupedTopK plan,
                                                      SqlExecutionContext context, QueryCodeGenerator compiler) {
        if (plan.limit() <= 0 || QuestLongTopK.capacityClass(plan.limit()) != 0 || input.implementsLimit()) return null;
        if (!input.supportsPageFrameCursor()
                && (!input.supportsFilterStealing() || !input.getFilter().isThreadSafe()
                || !input.getBaseFactory().supportsPageFrameCursor())) return null;
        var metadata = input.getMetadata();
        int type = metadata.getColumnType(plan.orderColumn());
        if (type != ColumnType.INT && type != ColumnType.LONG && type != ColumnType.FLOAT && type != ColumnType.DOUBLE) return null;
        var used = new IntHashSet();
        for (int column : plan.partitionColumns()) {
            int keyType = metadata.getColumnType(column);
            if (keyType != ColumnType.INT && keyType != ColumnType.LONG && keyType != ColumnType.SYMBOL
                    && keyType != ColumnType.DOUBLE && keyType != ColumnType.VARCHAR || !used.add(column)) return null;
        }
        return base -> build(base, plan, context, compiler);
    }

    private static RecordCursorFactory build(RecordCursorFactory input, QueryCodeGenerator.GroupedTopK plan,
                                              SqlExecutionContext context, QueryCodeGenerator compiler) {
        RecordCursorFactory owned = input;
        ObjList<Function> records = new ObjList<>();
        CompiledFilter compiledFilter = null;
        MemoryCARW bindMemory = null;
        ObjList<Function> bindFunctions = null;
        Function filter = null;
        try {
            if (!input.supportsPageFrameCursor()) {
                compiledFilter = input.getCompiledFilter();
                bindMemory = input.getBindVarMemory();
                bindFunctions = input.getBindVarFunctions();
                filter = input.getFilter();
                owned = input.getBaseFactory();
                input.halfClose();
            }
            RecordMetadata source = owned.getMetadata();
            int type = source.getColumnType(plan.orderColumn());
            int keys = plan.partitionColumns().length;
            var function = new GroupedTopKAggregate(
                    GroupByUtils.createColumnFunction(source, plan.orderColumn() + 1, type, plan.orderColumn()),
                    plan.limit(), plan.ascending());
            var values = new ArrayColumnTypes();
            function.initValueTypes(values);
            var keyTypes = new ArrayColumnTypes();
            var keyColumns = new ListColumnFilter();
            var groupMetadata = new GenericRecordMetadata();
            var expandedMetadata = new GenericRecordMetadata();
            for (int i = 0; i < keys; i++) {
                int column = plan.partitionColumns()[i], keyType = source.getColumnType(column);
                keyTypes.add(keyType);
                keyColumns.add(column + 1);
                var metadata = new TableColumnMetadata("k" + i, keyType, IndexType.NONE, 0,
                        source.isSymbolTableStatic(column), null);
                groupMetadata.add(metadata);
                expandedMetadata.add(metadata);
                records.add(GroupByUtils.createColumnFunction(source, values.getColumnCount() + i + 1, keyType, column));
            }
            records.add(function);
            groupMetadata.add(new TableColumnMetadata("size", ColumnType.LONG));
            for (int i = 0; i < plan.limit(); i++) {
                records.add(LongColumn.newInstance(i + 1));
                groupMetadata.add(new TableColumnMetadata("value" + i, ColumnType.LONG));
            }
            var functions = new ObjList<GroupByFunction>();
            functions.add(function);
            // The existing filter-stealing contract transfers these owners to the group-by factory.
            RecordCursorFactory base = owned;
            var recordFunctions = records;
            CompiledFilter compiled = compiledFilter;
            MemoryCARW memory = bindMemory;
            ObjList<Function> binds = bindFunctions;
            Function predicate = filter;
            owned = null;
            records = null;
            compiledFilter = null;
            bindMemory = null;
            bindFunctions = null;
            filter = null;
            owned = new AsyncGroupByRecordCursorFactory(context.getCairoEngine(), new BytecodeAssembler(),
                    context.getCairoEngine().getConfiguration(), context.getMessageBus(), base, groupMetadata,
                    keyColumns, keyTypes, values, functions, null, new ObjList<>(), null, recordFunctions,
                    compiled, memory, binds, predicate, null, null, context.getSharedQueryWorkerCount(), null, compiler);

            expandedMetadata.add(new TableColumnMetadata("value", type));
            expandedMetadata.add(new TableColumnMetadata("row_number", ColumnType.LONG));
            var sources = new ObjList<UnnestSource>();
            sources.add(new Values(keys));
            owned = new Expanded(expandedMetadata, owned, sources, keys, plan.limit());
            var columns = new IntList();
            for (int column : plan.outputColumns()) {
                if (column < 0) columns.add(keys + 1);
                else if (column == plan.orderColumn()) columns.add(keys);
                else {
                    for (int i = 0; i < keys; i++) {
                        if (plan.partitionColumns()[i] == column) { columns.add(i); break; }
                    }
                }
            }
            var result = new SelectedRecordCursorFactory(plan.outputMetadata(), columns, owned);
            owned = null;
            return result;
        } catch (Throwable failure) {
            Misc.free(owned, failure);
            Misc.freeObjList(records, failure);
            Misc.free(compiledFilter, failure);
            Misc.free(bindMemory, failure);
            Misc.freeObjList(bindFunctions, failure);
            Misc.free(filter, failure);
            throw failure;
        }
    }

    private static final class Expanded extends UnnestRecordCursorFactory {
        private final RecordCursorFactory groups;
        private final int limit;

        Expanded(RecordMetadata metadata, RecordCursorFactory groups, ObjList<UnnestSource> sources, int keys, int limit) {
            super(metadata, groups, new ObjList<>(), sources, keys, true, new ObjList<>());
            this.groups = groups;
            this.limit = limit;
        }

        @Override public RecordCursorFactory getBaseFactory() { return groups; }
        @Override public void toPlan(PlanSink sink) {
            sink.type("Grouped Top K").attr("limit").val(limit).child(groups);
        }
    }

    private static final class Values implements UnnestSource {
        private final int countColumn;
        private Record record;

        Values(int countColumn) { this.countColumn = countColumn; }
        @Override public int getColumnCount() { return 1; }
        @Override public int init(Record record) {
            this.record = record;
            return Math.toIntExact(record.getLong(countColumn));
        }
        @Override public long getLong(int column, int index) { return record.getLong(countColumn + index + 1); }
        @Override public int getInt(int column, int index) {
            long key = getLong(column, index);
            return key == Numbers.LONG_NULL ? Numbers.INT_NULL : (int) key;
        }
        @Override public double getDouble(int column, int index) { return GroupedTopKAggregate.decode(getLong(column, index)); }
        @Override public float getFloat(int column, int index) { return (float) getDouble(column, index); }
    }
}
