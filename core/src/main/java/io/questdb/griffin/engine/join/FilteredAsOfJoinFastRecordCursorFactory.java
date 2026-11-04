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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.table.CompiledFilterBatch;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Specialized ASOF join cursor factory usable when the slave table supports TimeFrameRecordCursor.
 * <p>
 * Uses the shared bounded ASOF lookup/scan state for execution-stable predicates.
 * Stateful predicates retain lookup-only matching. Supports crossindex projection
 * of the slave records without changing the predicate's input column layout.
 * <p>
 * <strong>Important</strong>: the filter is tested <strong>before</strong> applying the crossindex projection!
 */
public final class FilteredAsOfJoinFastRecordCursorFactory extends AsOfJoinDenseRecordCursorFactoryBase {
    private final CompiledFilterBatch compiledFilter;
    private final IntList filterColumnTypes = new IntList();
    private CompiledAsOfScan compiledScan;
    private CompiledAsOfLookup compiledLookup;
    private CompiledAsOfJoin compiledJoin;
    private final QueryCodeGenerator codeGenerator;
    private final CompiledAsOfJoin.Input joinInput;
    private final boolean filterStable;
    private final RecordSink masterKeySink;
    private final Function scalarFilter;
    private final SelectedRecordCursorFactory.SelectedTimeFrameCursor selectedTimeFrameCursor;
    private final RecordSink slaveKeySink;
    private Function slaveRecordFilter;
    private @Nullable SymbolTranslatingRecord symbolTranslatingRecord;

    /**
     * Creates a new instance with filtered slave record support and optional crossindex projection.
     *
     * @param configuration         configuration
     * @param metadata              metadata of the resulting record factory
     * @param masterFactory         master record factory
     * @param masterKeySink         master key sink
     * @param slaveFactory          slave record factory
     * @param slaveKeySink          slave key sink
     * @param keyTypes              serialized join key types, including translated SYMBOL keys
     * @param slaveRecordFilter     filter function for slave records (applied before projection)
     * @param codeGenerator         optional compiler for bounded lookup and forward regions
     * @param slaveKeyColumns       slave key columns before undoing the optional projection
     * @param masterKeyColumns      master join key columns
     * @param columnSplit           number of columns to be split between master and slave
     * @param slaveNullRecord       null record to be used when the slave record is not found, projection is NOT applied on this record
     * @param slaveColumnCrossIndex optional crossindex for projecting the slave records, can be null
     * @param slaveTimestampIndex   timestamp column index in slave table after projection
     */
    public FilteredAsOfJoinFastRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull RecordSink masterKeySink,
            @NotNull RecordCursorFactory slaveFactory,
            @NotNull RecordSink slaveKeySink,
            @Transient @NotNull ColumnTypes keyTypes,
            @NotNull Function slaveRecordFilter,
            @Nullable QueryCodeGenerator codeGenerator,
            @Transient @NotNull ListColumnFilter slaveKeyColumns,
            @Transient @NotNull ListColumnFilter masterKeyColumns,
            int columnSplit,
            @NotNull Record slaveNullRecord,
            @Nullable IntList slaveColumnCrossIndex,
            int slaveTimestampIndex,
            long toleranceInterval,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices
    ) {
        super(configuration, metadata, masterFactory, slaveFactory, null, toleranceInterval, true, -1);
        assert slaveFactory.supportsTimeFrameCursor();
        this.codeGenerator = codeGenerator;
        int masterKey = masterKeyColumns.getColumnCount() == 1 ? masterKeyColumns.getColumnIndexFactored(0) : -1;
        int slaveKey = slaveKeyColumns.getColumnCount() == 1 ? slaveKeyColumns.getColumnIndexFactored(0) : -1;
        IntList projection = slaveColumnCrossIndex == null ? null : new IntList(slaveColumnCrossIndex);
        this.joinInput = new CompiledAsOfJoin.Input(masterFactory.getMetadata(), slaveFactory.getMetadata(),
                masterKey, slaveKey < 0 || projection == null ? slaveKey : projection.getQuick(slaveKey), projection, toleranceInterval, metadata, null);
        this.slaveRecordFilter = slaveRecordFilter;
        this.filterStable = slaveRecordFilter.isStableWithinExecution();
        this.compiledFilter = slaveRecordFilter instanceof CompiledFilterBatch batch ? batch : null;
        this.scalarFilter = compiledFilter == null ? slaveRecordFilter : compiledFilter.getArg();
        if (compiledFilter != null) {
            RecordMetadata rawMetadata = slaveFactory.getMetadata();
            for (int i = 0; i < rawMetadata.getColumnCount(); i++) {
                filterColumnTypes.add(rawMetadata.getColumnType(i));
            }
        }
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        if (slaveColumnCrossIndex != null && SelectedRecordCursorFactory.isCrossedIndex(slaveColumnCrossIndex)) {
            this.selectedTimeFrameCursor = new SelectedRecordCursorFactory.SelectedTimeFrameCursor(slaveColumnCrossIndex, slaveFactory.recordCursorSupportsRandomAccess(), slaveTimestampIndex);
        } else {
            this.selectedTimeFrameCursor = null;
        }
        this.symbolTranslatingRecord = masterSymbolKeyColumnIndices != null
                ? new SymbolTranslatingRecord(configuration, masterFactory.getMetadata().getColumnCount(), masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices)
                : null;
        long maxSinkTargetHeapSize = (long) configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
        Map forwardMap = null;
        Map backwardMap = null;
        try {
            forwardMap = createScanMap(configuration, keyTypes);
            backwardMap = createScanMap(configuration, keyTypes);
            if (filterStable && codeGenerator != null) {
                ListColumnFilter rawKeys = new ListColumnFilter();
                for (int i = 0; i < slaveKeyColumns.getColumnCount(); i++) {
                    int column = slaveKeyColumns.getColumnIndexFactored(i);
                    rawKeys.add((slaveColumnCrossIndex == null ? column : slaveColumnCrossIndex.getQuick(column)) + 1);
                }
                int rawTimestamp = slaveColumnCrossIndex == null ? slaveTimestampIndex : slaveColumnCrossIndex.getQuick(slaveTimestampIndex);
                long timestampScale = masterFactory.getMetadata().getTimestampType() == slaveFactory.getMetadata().getTimestampType()
                        ? 1 : ColumnType.getTimestampDriver(slaveFactory.getMetadata().getTimestampType()).toNanosScale();
                compiledScan = codeGenerator.compileAsOfScan(slaveFactory.getMetadata(), keyTypes, rawKeys,
                        forwardMap, scalarFilter, rawTimestamp, timestampScale);
                if (masterKey >= 0 && rawKeys.getColumnCount() == 1) {
                    int type = masterFactory.getMetadata().getColumnType(masterKey);
                    int rawKey = rawKeys.getColumnIndexFactored(0);
                    int encodedType = keyTypes.getColumnType(0);
                    if (type == slaveFactory.getMetadata().getColumnType(rawKey)
                            && (type == ColumnType.INT && encodedType == ColumnType.INT
                            || type == ColumnType.LONG && encodedType == ColumnType.LONG
                            || type == ColumnType.SYMBOL && encodedType == ColumnType.INT)) {
                        compiledLookup = codeGenerator.compileAsOfLookup(slaveFactory.getMetadata(), scalarFilter,
                                rawKey, rawTimestamp, timestampScale, toleranceInterval != io.questdb.std.Numbers.LONG_NULL);
                    }
                }
            }
            this.cursor = new FilteredAsOfJoinKeyedFastRecordCursor(
                    columnSplit,
                    forwardMap,
                    backwardMap,
                    slaveNullRecord,
                    masterFactory.getMetadata().getTimestampIndex(),
                    new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN, SingleRecordSink.OWNER_ASOF_JOIN,
                            SingleRecordSink.CONFIG_KEYS_ASOF_JOIN),
                    slaveTimestampIndex,
                    new SingleRecordSink(maxSinkTargetHeapSize, MemoryTag.NATIVE_RECORD_CHAIN, SingleRecordSink.OWNER_ASOF_JOIN,
                            SingleRecordSink.CONFIG_KEYS_ASOF_JOIN),
                    masterFactory.getMetadata().getTimestampType(),
                    slaveFactory.getMetadata().getTimestampType()
            );
        } catch (Throwable th) {
            Misc.free(backwardMap);
            Misc.free(forwardMap);
            close();
            throw th;
        }
    }

    public boolean bindAggregate(ColumnTypes inputs, IntList columns, ColumnTypes values, ObjList<GroupByFunction> functions, MapValue value) {
        if (codeGenerator != null && filterStable) {
            CompiledAsOfJoin.Input input = new CompiledAsOfJoin.Input(joinInput.master(), joinInput.slave(),
                    joinInput.masterKey(), joinInput.slaveKey(), joinInput.slaveProjection(), joinInput.tolerance(), inputs, columns);
            compiledJoin = codeGenerator.compileAsOfJoin(input,
                    ((FilteredAsOfJoinKeyedFastRecordCursor) cursor).getForwardMap(), scalarFilter, values, functions, value);
        }
        return compiledJoin != null;
    }

    public void aggregate(MapValue value, GroupByFunctionsUpdater updater, Record aggregateRecord) {
        cursor.aggregate(value, updater, aggregateRecord);
    }

    public long getNativeAggregateRows() {
        return compiledJoin == null ? 0 : compiledJoin.getNativeRows();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            TimeFrameCursor filterCursor = slaveCursor;
            Record filterRecord = slaveCursor.getRecordB();
            slaveRecordFilter.init(slaveCursor, executionContext);
            if (compiledLookup != null) {
                compiledLookup.prepareExecution(executionContext.getMemoryTracker());
            }
            if (compiledJoin != null) {
                compiledJoin.prepareExecution(executionContext.getMemoryTracker());
            }
            if (compiledScan != null) {
                compiledScan.prepareExecution(executionContext.getMemoryTracker());
            }
            if (selectedTimeFrameCursor != null) {
                slaveCursor = selectedTimeFrameCursor.of(slaveCursor);
            }
            // Bind the per-query tracker before of(); the cursor's of()
            // reopens its SingleRecordSinks, so the first malloc lands
            // under the bound tracker.
            cursor.setMemoryTracker(executionContext.getMemoryTracker());
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            ((FilteredAsOfJoinKeyedFastRecordCursor) cursor).of(masterCursor, slaveCursor, filterCursor, filterRecord, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() reopens the sinks and caches before adopting the cursors, so close() here frees
            // only the partial heap.
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        putFactoryType(sink);
        sink.attr("filter").val(slaveRecordFilter, slaveFactory);
        if (compiledLookup != null) {
            sink.attr("backward lookup").val(compiledLookup.getName());
        }
        if (compiledJoin != null) {
            sink.attr("aggregate region").val(compiledJoin.getName());
        }
        if (compiledScan != null) {
            sink.attr("forward scan").val(compiledScan.getName());
        }
        if (compiledFilter != null) {
            sink.attr("filter batches").val(compiledFilter.getBackendName());
        }
        if (symbolTranslatingRecord != null) {
            sink.attr("symbolKeyJoin").val(true);
        }
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    public long getNativeForwardRows() {
        return compiledScan == null ? 0 : compiledScan.getNativeRows();
    }

    public long getNativeLookupRows() {
        return compiledLookup == null ? 0 : compiledLookup.getNativeRows();
    }

    public long getCompiledFilterRows() {
        return compiledFilter == null ? 0 : compiledFilter.getInputRows();
    }

    @Override
    protected void _close() {
        final Function slaveRecordFilter = this.slaveRecordFilter;
        this.slaveRecordFilter = null;
        final SymbolTranslatingRecord symbolTranslatingRecord = this.symbolTranslatingRecord;
        this.symbolTranslatingRecord = null;
        Throwable failure = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeBestEffort(failure, slaveRecordFilter);
        failure = Misc.freeBestEffort(failure, compiledLookup);
        compiledLookup = null;
        failure = Misc.freeBestEffort(failure, compiledJoin);
        compiledJoin = null;
        failure = Misc.freeBestEffort(failure, compiledScan);
        compiledScan = null;
        failure = Misc.freeBestEffort(failure, symbolTranslatingRecord);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    protected void putFactoryType(PlanSink sink) {
        sink.type(filterStable ? "Filtered AsOf Join Adaptive" : "Filtered AsOf Join Fast");
    }

    private class FilteredAsOfJoinKeyedFastRecordCursor extends AsOfJoinDenseRecordCursorBase {
        private static final long FORWARD_TRIAL_ROWS = 64 * 1024;
        private final SingleRecordSink masterSinkTarget;
        private final SingleRecordSink slaveSinkTarget;
        private int batchFrame = -1;
        private long batchConsumedTo;
        private long batchHi;
        private long batchLo;
        private boolean batchPreferred;
        private TimeFrameCursor filterCursor;
        private Record filterRecord;
        private long nextMatch;
        private long fusedTrialNanos;
        private long fusedTrialRows;
        private long batchTrialNanos;
        private long batchTrialRows;
        private long rowsUntilTrial;
        private int trialCalls;
        // 0: fused trial, 1: batch trial, 2: use the winner without timing.
        private int forwardTrial;
        private DirectLongList selectedRows;
        // Record used for master key serialization. Set once in of() to either
        // masterRecord or SymbolTranslatingRecord wrapping it, so that getInt()
        // on symbol key columns returns slave symbol IDs.
        private Record masterKeyRecord;

        public FilteredAsOfJoinKeyedFastRecordCursor(
                int columnSplit,
                Map forwardMap,
                Map backwardMap,
                Record nullRecord,
                int masterTimestampIndex,
                SingleRecordSink masterSinkTarget,
                int slaveTimestampIndex,
                SingleRecordSink slaveSinkTarget,
                int masterTimestampType,
                int slaveTimestampType
        ) {
            super(columnSplit, forwardMap, backwardMap, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType);
            this.masterSinkTarget = masterSinkTarget;
            this.slaveSinkTarget = slaveSinkTarget;
        }

        @Override
        public void close() {
            try {
                super.close();
                masterSinkTarget.close();
                slaveSinkTarget.close();
                Misc.free(symbolTranslatingRecord);
            } finally {
                try {
                    if (compiledLookup != null) {
                        compiledLookup.clearExecution();
                    }
                    if (compiledJoin != null) {
                        compiledJoin.clearExecution();
                    }
                    if (compiledScan != null) {
                        compiledScan.clearExecution();
                    }
                } finally {
                    if (compiledFilter != null) {
                        compiledFilter.clearExecution();
                    }
                    batchFrame = -1;
                    selectedRows = null;
                    filterCursor = null;
                }
            }
        }

        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, TimeFrameCursor filterCursor, Record filterRecord, SqlExecutionCircuitBreaker circuitBreaker) {
            // Reopen the sinks and the translation caches before super.of() adopts the cursors
            // so an open-time breach frees each exactly once.
            masterSinkTarget.reopen();
            slaveSinkTarget.reopen();
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.initSources(masterCursor, slaveCursor);
            }
            super.of(masterCursor, slaveCursor, circuitBreaker);
            this.filterCursor = filterCursor;
            this.batchFrame = -1;
            this.selectedRows = null;
            resetForwardTrials();
            this.filterRecord = filterRecord;
            this.masterKeyRecord = masterRecord;
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.of(masterRecord);
                masterKeyRecord = symbolTranslatingRecord;
            }
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            super.setMemoryTracker(tracker);
            masterSinkTarget.setMemoryTracker(tracker);
            slaveSinkTarget.setMemoryTracker(tracker);
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.setMemoryTracker(tracker);
            }
        }

        @Override
        public void toTop() {
            super.toTop();
            slaveRecordFilter.toTop();
            batchFrame = -1;
            selectedRows = null;
            resetForwardTrials();
        }

        @Override
        protected void aggregateLookupRegion(PageFrameMemoryRecord master, MapValue value, CompiledAsOfJoin.State state) {
            if (compiledJoin == null) {
                return;
            }
            translateRegionKeys(master, state);
            compiledJoin.aggregateLookup(master, filterCursor, value, state, circuitBreaker);
        }

        private void translateRegionKeys(PageFrameMemoryRecord master, CompiledAsOfJoin.State state) {
            if (symbolTranslatingRecord != null) {
                long row = Rows.toLocalRowID(master.getRowId());
                try {
                    for (long i = state.masterLo; i < state.masterHi; i++) {
                        master.setRowIndex(i);
                        state.symbolKeys[(int) (i - state.masterLo)] = masterKeyRecord.getInt(joinInput.masterKey());
                    }
                } finally {
                    master.setRowIndex(row);
                }
            }
        }

        @Override
        protected void aggregateForwardRegion(PageFrameMemoryRecord master, MapValue value, CompiledAsOfJoin.State state) {
            if (compiledJoin == null) {
                return;
            }
            translateRegionKeys(master, state);
            // Native navigation may borrow record B for history. Discard the Java selection
            // cache: its row offsets belong to the previous forward cursor position.
            batchFrame = -1;
            selectedRows = null;
            compiledJoin.aggregate(master, filterCursor, getForwardMap(), value, state, circuitBreaker);
        }

        @Override
        protected int getSlaveJoinKey() {
            slaveSinkTarget.clear();
            slaveKeySink.copy(slaveRecB, slaveSinkTarget);
            return DUMMY_VALUE;
        }

        @Override
        protected CompiledAsOfLookup.Result scanBackwardFrame(long lo, long hi, long minTimestamp) {
            if (compiledLookup == null || !(filterRecord instanceof PageFrameMemoryRecord raw)) {
                return null;
            }
            int keyColumn = joinInput.masterKey();
            long key = joinInput.master().getColumnType(keyColumn) == ColumnType.LONG
                    ? masterKeyRecord.getLong(keyColumn) : masterKeyRecord.getInt(keyColumn);
            return compiledLookup.scan(raw, lo, hi, minTimestamp, key, circuitBreaker);
        }

        @Override
        protected boolean joinKeysMatch(int slaveKeyToFind, int slaveKey) {
            return masterSinkTarget.memeq(slaveSinkTarget);
        }

        @Override
        protected boolean matchesSlaveFilter() {
            return scalarFilter.getBool(filterRecord);
        }

        @Override
        protected void putSlaveJoinKey(MapKey key) {
            key.put(slaveRecB, slaveKeySink);
        }

        @Override
        protected void putSlaveKeyToFind(MapKey key, int slaveKeyToFind) {
            key.put(masterKeyRecord, masterKeySink);
        }

        @Override
        protected long scanForwardBatch(Map map, long lo, long hi, long minTimestamp, long maxTimestamp) {
            boolean trial = compiledFilter != null && compiledScan != null && forwardTrial < 2;
            boolean batch = compiledScan == null || (forwardTrial == 1 || forwardTrial == 2 && batchPreferred);
            long start = trial ? System.nanoTime() : 0;
            long next = -1;
            if (compiledFilter != null && batch && lo < hi) {
                next = scanFilteredBatch(map, lo, hi, minTimestamp, maxTimestamp);
                if (next < 0) {
                    batchPreferred = false;
                    finishForwardTrial();
                    trial = false;
                }
            }
            if (next < 0 && compiledScan != null && filterRecord instanceof PageFrameMemoryRecord frameRecord) {
                next = compiledScan.scan(frameRecord, map, lo, hi, minTimestamp, maxTimestamp, circuitBreaker);
                batch = false;
            }
            if (compiledFilter != null && compiledScan != null && next >= lo) {
                long rows = next - lo;
                if (trial) {
                    observeForwardTrial(batch, rows, System.nanoTime() - start);
                } else if ((rowsUntilTrial -= rows) <= 0) {
                    startForwardTrial();
                }
            }
            return next;
        }

        private void finishForwardTrial() {
            forwardTrial = 2;
            rowsUntilTrial = 64 * FORWARD_TRIAL_ROWS;
        }

        private void observeForwardTrial(boolean batch, long rows, long elapsed) {
            trialCalls++;
            if (batch) {
                batchTrialNanos += elapsed;
                batchTrialRows += rows;
                if (batchTrialRows >= FORWARD_TRIAL_ROWS || trialCalls >= 256) {
                    double fusedCost = (double) fusedTrialNanos / fusedTrialRows;
                    double batchCost = batchTrialRows > 0 ? (double) batchTrialNanos / batchTrialRows : Double.POSITIVE_INFINITY;
                    // Require a material advantage to change paths. Trials process real
                    // input and share the ordinary map, so no rows are replayed for timing.
                    if (batchCost < fusedCost * 0.8) {
                        batchPreferred = true;
                    } else if (fusedCost < batchCost * 0.8) {
                        batchPreferred = false;
                    }
                    finishForwardTrial();
                }
            } else {
                fusedTrialNanos += elapsed;
                fusedTrialRows += rows;
                if (fusedTrialRows >= FORWARD_TRIAL_ROWS || trialCalls >= 256) {
                    forwardTrial = fusedTrialRows > 0 ? 1 : 2;
                    rowsUntilTrial = 64 * FORWARD_TRIAL_ROWS;
                    trialCalls = 0;
                }
            }
        }

        private void resetForwardTrials() {
            batchPreferred = false;
            startForwardTrial();
        }

        private long scanFilteredBatch(Map map, long lo, long hi, long minTimestamp, long maxTimestamp) {
            final int frame = slaveTimeFrame.getFrameIndex();
            while (lo < hi) {
                if (batchFrame != frame || lo < batchConsumedTo || lo >= batchHi) {
                    PageFrameMemory memory = filterCursor.getFrameMemory();
                    if (memory == null) {
                        return -1;
                    }
                    // The buffer owns row offsets only; never retain borrowed frame addresses.
                    long end = Math.min(hi, lo + CompiledFilterBatch.BATCH_SIZE);
                    selectedRows = compiledFilter.apply(memory, filterColumnTypes, lo, end);
                    if (selectedRows == null) {
                        return -1; // frame eligibility is constant, so no preceding map mutation
                    }
                    batchFrame = frame;
                    batchLo = lo;
                    batchHi = end;
                    nextMatch = 0;
                }
                long end = Math.min(hi, batchHi);
                // A stable predicate makes rejected rows permanently irrelevant, even if
                // their timestamps are in the future. Stop at the first qualifying future
                // row. This is the same stream consumed by Light ASOF, with bounded lookahead.
                long frontier = end;
                for (; nextMatch < selectedRows.size(); nextMatch++) {
                    long row = batchLo + selectedRows.get(nextMatch);
                    if (row >= end) {
                        break;
                    }
                    if (row >= lo) {
                        slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, row);
                        long timestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (timestamp > maxTimestamp) {
                            frontier = row;
                            break;
                        }
                        if (timestamp >= minTimestamp) {
                            MapKey key = map.withKey();
                            putSlaveJoinKey(key);
                            key.createValue().putLong(0, Rows.toRowID(frame, row));
                        }
                    }
                }
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                batchConsumedTo = frontier;
                if (frontier < end) {
                    return frontier;
                }
                lo = end;
            }
            return lo;
        }

        private void startForwardTrial() {
            forwardTrial = 0;
            trialCalls = 0;
            fusedTrialRows = batchTrialRows = fusedTrialNanos = batchTrialNanos = 0;
        }

        @Override
        protected int setupSymbolKeyToFind() {
            if (symbolTranslatingRecord != null) {
                symbolTranslatingRecord.resetNonExistentKeyFlag();
            }
            masterSinkTarget.clear();
            masterKeySink.copy(masterKeyRecord, masterSinkTarget);
            return symbolTranslatingRecord != null && symbolTranslatingRecord.hadNonExistentKey()
                    ? SymbolTable.VALUE_NOT_FOUND : DUMMY_VALUE;
        }

        @Override
        protected boolean supportsForwardScan() {
            // Caching a row's eligibility changes predicate evaluation count/order.
            // Preserve lookup-only execution for random, sequence and other stateful functions.
            return filterStable;
        }
    }
}
