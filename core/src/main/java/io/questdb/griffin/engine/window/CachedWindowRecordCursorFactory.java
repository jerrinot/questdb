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

package io.questdb.griffin.engine.window;


import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CachedWindowRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<WindowFunction> allFunctions;
    private final RecordCursorFactory base;
    private final GenericRecordMetadata chainMetadata;
    private final ObjList<RecordComparator> comparators;
    private final CachedWindowRecordCursor cursor;
    private final ObjList<ObjList<WindowFunction.CachedFunctionContext>> orderedFunctionContexts;
    private final ObjList<ObjList<WindowFunction>> orderedSecondaryPassFunctions;
    private final ObjList<ObjList<WindowFunction.CachedFunctionContext>> orderedSecondaryPassFunctionContexts;
    private final ObjList<ObjList<WindowFunction>> orderedFunctions;
    private final int orderedGroupCount;
    private final ObjList<IntList> sortKeys;
    private final ObjList<WindowFunction.CachedFunctionContext> unorderedFunctionContexts;
    private final ObjList<WindowFunction> unorderedSecondaryPassFunctions;
    private final ObjList<WindowFunction.CachedFunctionContext> unorderedSecondaryPassFunctionContexts;
    @Nullable
    private final ObjList<WindowFunction> unorderedFunctions;
    private boolean closed = false;

    public CachedWindowRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordSink recordSink,
            GenericRecordMetadata metadata,
            @Transient ColumnTypes chainTypes,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<WindowFunction>> orderedFunctions,
            @Nullable ObjList<WindowFunction> unorderedFunctions,
            @NotNull IntList columnIndexes,
            @NotNull final ObjList<IntList> sortKeys,
            @NotNull GenericRecordMetadata chainMetadata,
            @Nullable ObjObjHashMap<WindowFunction, WindowFunction.CachedFunctionLayout> functionLayouts
    ) {
        super(metadata);
        try {
            this.base = base;
            this.orderedGroupCount = comparators.size();
            assert orderedGroupCount == orderedFunctions.size();
            this.orderedFunctions = orderedFunctions;
            this.comparators = comparators;
            RecordArray recordChain = new RecordArray(
                    chainTypes,
                    recordSink,
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages()
            );
            this.sortKeys = sortKeys;
            this.chainMetadata = chainMetadata;

            ObjList<ObjList<DirectIntList>> perGroupRankMaps = new ObjList<>(orderedGroupCount);
            try {
                for (int i = 0; i < orderedGroupCount; i++) {
                    perGroupRankMaps.add(SortKeyEncoder.createRankMaps(chainMetadata, sortKeys.getQuick(i)));
                }
            } catch (Throwable t) {
                freePerGroupRankMaps(perGroupRankMaps);
                Misc.free(recordChain);
                throw t;
            }

            ObjList<LongTreeChain> orderedSources = new ObjList<>(orderedGroupCount);
            // red&black trees, one for each comparator where comparator is not null
            try {
                for (int i = 0; i < orderedGroupCount; i++) {
                    orderedSources.add(
                            new LongTreeChain(
                                    configuration.getSqlWindowTreeKeyPageSize(),
                                    configuration.getSqlWindowTreeKeyMaxPages(),
                                    configuration.getSqlWindowRowIdPageSize(),
                                    configuration.getSqlWindowRowIdMaxPages()
                            )
                    );
                }
            } catch (Throwable t) {
                Misc.freeObjList(orderedSources);
                freePerGroupRankMaps(perGroupRankMaps);
                recordChain.close();
                throw t;
            }

            this.cursor = new CachedWindowRecordCursor(columnIndexes, recordChain, orderedSources, perGroupRankMaps);
            this.allFunctions = new ObjList<>();
            this.orderedFunctionContexts = new ObjList<>(orderedGroupCount);

            ObjList<ObjList<WindowFunction>> orderedTmp = null;
            ObjList<ObjList<WindowFunction.CachedFunctionContext>> orderedSecondaryContextsTmp = null;
            for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                allFunctions.addAll(functions);
                final ObjList<WindowFunction.CachedFunctionContext> functionContexts = createFunctionContexts(functions, recordChain, functionLayouts);
                orderedFunctionContexts.extendAndSet(i, functionContexts);

                ObjList<WindowFunction> twoPassFunctions = null;
                ObjList<WindowFunction.CachedFunctionContext> twoPassFunctionContexts = null;
                for (int j = 0, k = functions.size(); j < k; j++) {
                    WindowFunction function = functions.getQuick(j);
                    if (function.needsSecondaryCachedPass()) {
                        if (twoPassFunctions == null) {
                            twoPassFunctions = new ObjList<>();
                            twoPassFunctionContexts = new ObjList<>();
                        }
                        twoPassFunctions.add(function);
                        twoPassFunctionContexts.add(functionContexts.getQuick(j));
                    }
                }
                if (twoPassFunctions != null) {
                    if (orderedTmp == null) {
                        orderedTmp = new ObjList<>();
                        orderedSecondaryContextsTmp = new ObjList<>();
                    }

                    orderedTmp.extendAndSet(i, twoPassFunctions);
                    orderedSecondaryContextsTmp.extendAndSet(i, twoPassFunctionContexts);
                }
            }

            orderedSecondaryPassFunctions = orderedTmp;
            orderedSecondaryPassFunctionContexts = orderedSecondaryContextsTmp;

            ObjList<WindowFunction> unorderedTmp = null;
            ObjList<WindowFunction.CachedFunctionContext> unorderedContextTmp = null;
            ObjList<WindowFunction.CachedFunctionContext> unorderedSecondaryContextTmp = null;
            if (unorderedFunctions != null) {
                allFunctions.addAll(unorderedFunctions);
                unorderedContextTmp = createFunctionContexts(unorderedFunctions, recordChain, functionLayouts);

                for (int i = 0, n = unorderedFunctions.size(); i < n; i++) {
                    WindowFunction function = unorderedFunctions.getQuick(i);
                    if (function.needsSecondaryCachedPass()) {
                        if (unorderedTmp == null) {
                            unorderedTmp = new ObjList<>();
                            unorderedSecondaryContextTmp = new ObjList<>();
                        }
                        unorderedTmp.add(function);
                        unorderedSecondaryContextTmp.add(unorderedContextTmp.getQuick(i));
                    }
                }
            }
            this.unorderedSecondaryPassFunctions = unorderedTmp;
            this.unorderedFunctionContexts = unorderedContextTmp;
            this.unorderedSecondaryPassFunctionContexts = unorderedSecondaryContextTmp;

            this.unorderedFunctions = unorderedFunctions;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return chainMetadata.getColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        cursor.of(baseCursor, executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CachedWindow");

        boolean oldVal = sink.getUseBaseMetadata();
        try {
            if (orderedFunctions.size() > 0) {
                sink.attr("orderedFunctions");
                sink.val("[");

                sink.useBaseMetadata(true);

                for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                    if (i > 0) {
                        sink.val(',');
                    }
                    sink.val('[');

                    addSortKeys(sink, sortKeys.getQuick(i));

                    sink.val("] => [");
                    ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        if (j > 0) {
                            sink.val(',');
                        }
                        sink.val(functions.getQuick(j));
                    }

                    sink.val("]");
                }
                sink.val(']');
            }

            sink.optAttr("unorderedFunctions", unorderedFunctions, true);
        } finally {
            sink.useBaseMetadata(oldVal);
        }

        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private static void freePerGroupRankMaps(ObjList<ObjList<DirectIntList>> perGroupRankMaps) {
        for (int i = 0, n = perGroupRankMaps.size(); i < n; i++) {
            Misc.freeObjList(perGroupRankMaps.getQuick(i));
        }
    }

    private static ObjList<WindowFunction.CachedFunctionContext> createFunctionContexts(
            ObjList<WindowFunction> functions,
            WindowSPI spi,
            @Nullable ObjObjHashMap<WindowFunction, WindowFunction.CachedFunctionLayout> functionLayouts
    ) {
        assert functionLayouts != null;
        final ObjList<WindowFunction.CachedFunctionContext> contexts = new ObjList<>(functions.size());
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            final WindowFunction.CachedFunctionLayout layout = functionLayouts.get(function);
            assert layout != null;
            final WindowSPI.FixedSizeColumn resultColumn = spi.getFixedSizeColumn(
                    layout.getResultColumnIndex(),
                    function.getType()
            );
            ObjList<WindowSPI.FixedSizeColumn> scratchColumns = null;
            if (layout.getScratchColumnCount() > 0) {
                scratchColumns = new ObjList<>(layout.getScratchColumnCount());
                for (int scratchIndex = 0, scratchCount = layout.getScratchColumnCount(); scratchIndex < scratchCount; scratchIndex++) {
                    scratchColumns.add(
                            spi.getFixedSizeColumn(
                                    layout.getScratchColumnIndex(scratchIndex),
                                    layout.getScratchColumnType(scratchIndex)
                            )
                    );
                }
            }
            contexts.add(new LegacyCachedFunctionContext(resultColumn, scratchColumns));
        }
        return contexts;
    }

    private void addSortKeys(PlanSink sink, IntList list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            int colIdx = list.get(i);
            int col = (colIdx > 0 ? colIdx : -colIdx) - 1;
            if (i > 0) {
                sink.val(", ");
            }
            sink.val(chainMetadata.getColumnName(col));
            if (colIdx < 0) {
                sink.val(" ").val("desc");
            }
        }
    }

    private void resetFunctions() {
        for (int i = 0, n = allFunctions.size(); i < n; i++) {
            allFunctions.getQuick(i).reset();
        }
    }

    @Override
    protected void _close() {
        if (closed) {
            return;
        }
        closed = true;
        Misc.free(base);
        Misc.free(cursor);
        Misc.freeObjList(allFunctions);
    }

    class CachedWindowRecordCursor implements RecordCursor {
        private final IntList columnIndexes; // Used for symbol table lookups.
        private final ObjList<LongTreeChain> orderedSources;
        private final ObjList<ObjList<DirectIntList>> perGroupRankMaps;
        private final RecordArray recordChain;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private boolean isRecordChainBuilt;
        private long recordChainOffset;

        public CachedWindowRecordCursor(IntList columnIndexes, RecordArray recordChain, ObjList<LongTreeChain> orderedSources, ObjList<ObjList<DirectIntList>> perGroupRankMaps) {
            this.columnIndexes = columnIndexes;
            this.recordChain = recordChain;
            this.recordChain.setSymbolTableResolver(this);
            this.isOpen = true;
            this.orderedSources = orderedSources;
            this.perGroupRankMaps = perGroupRankMaps;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            isRecordChainBuilt = true;
            recordChain.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                Misc.free(baseCursor);
                Misc.free(recordChain);
                for (int i = 0, n = orderedSources.size(); i < n; i++) {
                    Misc.free(orderedSources.getQuick(i));
                }
                for (int i = 0, n = perGroupRankMaps.size(); i < n; i++) {
                    Misc.freeObjListAndKeepObjects(perGroupRankMaps.getQuick(i));
                }
                resetFunctions();
                isOpen = false;
            }
        }

        @Override
        public Record getRecord() {
            return recordChain.getRecord();
        }

        @Override
        public Record getRecordB() {
            return recordChain.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            isRecordChainBuilt = true;
            return recordChain.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public long preComputedStateSize() {
            return recordChain.size();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return isRecordChainBuilt ? recordChain.size() : -1;// in case recordChain starts returning actual size
        }

        @Override
        public void toTop() {
            recordChain.toTop();
        }

        private void buildRecordChain() {
            // step #1: store source cursor in record list
            // - add record list's row ids to all trees, which will put these row ids in necessary order
            // for this we will be using out comparator, which helps tree compare long values
            // based on record these values are addressing
            final Record record = baseCursor.getRecord();
            final Record chainRecord = recordChain.getRecord();
            final Record chainRightRecord = recordChain.getRecordB();
            if (orderedGroupCount > 0) {
                while (baseCursor.hasNext()) {
                    recordChainOffset = recordChain.put(record);
                    recordChain.recordAt(chainRecord, recordChainOffset);
                    for (int i = 0; i < orderedGroupCount; i++) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        orderedSources.getQuick(i).put(chainRecord, recordChain, chainRightRecord, comparators.getQuick(i));
                    }
                }
            } else {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    recordChainOffset = recordChain.put(record);
                }
            }

            // step #2: populate all window functions with records in order of respective tree
            // run primary cached traversal for all ordered functions
            long offset;
            if (orderedGroupCount > 0) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final LongTreeChain tree = orderedSources.getQuick(i);
                    final ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    final ObjList<WindowFunction.CachedFunctionContext> functionContexts = orderedFunctionContexts.getQuick(i);
                    final LongTreeChain.TreeCursor cursor = tree.getCursor();
                    final int functionCount = functions.size();
                    while (cursor.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = cursor.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).processPrimaryCachedRow(chainRecord, offset, recordChain, functionContexts.getQuick(j));
                        }
                    }
                }
            }

            // run primary cached traversal for all unordered functions
            if (unorderedFunctions != null) {
                for (int j = 0, n = unorderedFunctions.size(); j < n; j++) {
                    final WindowFunction f = unorderedFunctions.getQuick(j);
                    final WindowFunction.CachedFunctionContext context = unorderedFunctionContexts.getQuick(j);
                    if (f.getPrimaryCachedTraversalDirection() == WindowFunction.PrimaryCachedTraversalDirection.FORWARD) {
                        recordChain.toTop();
                        while (recordChain.hasNext()) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            f.processPrimaryCachedRow(chainRecord, chainRecord.getRowId(), recordChain, context);
                        }
                    } else {
                        recordChain.toBottom();
                        while (recordChain.hasPrev()) {
                            circuitBreaker.statefulThrowExceptionIfTripped();
                            f.processPrimaryCachedRow(chainRecord, chainRecord.getRowId(), recordChain, context);
                        }
                    }
                }
            }

            // prepare secondary cached pass for ordered functions
            if (orderedSecondaryPassFunctions != null) {
                for (int i = 0, n = orderedSecondaryPassFunctions.size(); i < n; i++) {
                    final ObjList<WindowFunction> functions = orderedSecondaryPassFunctions.getQuick(i);
                    final ObjList<WindowFunction.CachedFunctionContext> functionContexts = orderedSecondaryPassFunctionContexts.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        functions.getQuick(j).prepareSecondaryCachedPass(functionContexts.getQuick(j));
                    }
                }
            }
            // prepare secondary cached pass for unordered functions
            if (unorderedSecondaryPassFunctions != null) {
                for (int j = 0, n = unorderedSecondaryPassFunctions.size(); j < n; j++) {
                    unorderedSecondaryPassFunctions.getQuick(j).prepareSecondaryCachedPass(unorderedSecondaryPassFunctionContexts.getQuick(j));
                }
            }

            // run secondary cached pass for all ordered functions
            if (orderedSecondaryPassFunctions != null) {
                for (int i = 0, n = orderedSecondaryPassFunctions.size(); i < n; i++) {
                    final LongTreeChain tree = orderedSources.getQuick(i);
                    final ObjList<WindowFunction> functions = orderedSecondaryPassFunctions.getQuick(i);
                    final ObjList<WindowFunction.CachedFunctionContext> functionContexts = orderedSecondaryPassFunctionContexts.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    final LongTreeChain.TreeCursor cursor = tree.getCursor();
                    final int functionCount = functions.size();
                    while (cursor.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = cursor.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).processSecondaryCachedRow(chainRecord, offset, recordChain, functionContexts.getQuick(j));
                        }
                    }
                }
            }

            // run secondary cached pass for all unordered functions
            if (unorderedSecondaryPassFunctions != null) {
                for (int j = 0, n = unorderedSecondaryPassFunctions.size(); j < n; j++) {
                    final WindowFunction f = unorderedSecondaryPassFunctions.getQuick(j);
                    final WindowFunction.CachedFunctionContext context = unorderedSecondaryPassFunctionContexts.getQuick(j);
                    recordChain.toTop();
                    while (recordChain.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        f.processSecondaryCachedRow(chainRecord, chainRecord.getRowId(), recordChain, context);
                    }
                }
            }

            recordChain.toTop();
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            isRecordChainBuilt = false;
            recordChainOffset = -1;
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                isOpen = true;
                recordChain.setSymbolTableResolver(this);
                reopenTrees();
                reopen(allFunctions);
            }
            Function.init(allFunctions, this, executionContext, null);
            for (int i = 0; i < orderedGroupCount; i++) {
                SortKeyEncoder.buildRankMaps(this, perGroupRankMaps.getQuick(i), comparators.getQuick(i));
            }
        }

        private void reopen(ObjList<?> list) {
            for (int i = 0, n = list.size(); i < n; i++) {
                if (list.getQuick(i) instanceof Reopenable) {
                    ((Reopenable) list.getQuick(i)).reopen();
                }
            }
        }

        private void reopenTrees() {
            for (int i = 0; i < orderedGroupCount; i++) {
                orderedSources.getQuick(i).reopen();
            }
        }
    }

    private static final class LegacyCachedFunctionContext implements WindowFunction.CachedFunctionContext {
        private final WindowSPI.FixedSizeColumn resultColumn;
        private final ObjList<WindowSPI.FixedSizeColumn> scratchColumns;

        private LegacyCachedFunctionContext(WindowSPI.FixedSizeColumn resultColumn, @Nullable ObjList<WindowSPI.FixedSizeColumn> scratchColumns) {
            this.resultColumn = resultColumn;
            this.scratchColumns = scratchColumns;
        }

        @Override
        public WindowSPI.FixedSizeColumn getResultColumn() {
            return resultColumn;
        }

        @Override
        public WindowSPI.FixedSizeColumn getScratchColumn(int index) {
            if (scratchColumns == null) {
                throw new IndexOutOfBoundsException("scratch column is not bound");
            }
            return scratchColumns.getQuick(index);
        }
    }
}
