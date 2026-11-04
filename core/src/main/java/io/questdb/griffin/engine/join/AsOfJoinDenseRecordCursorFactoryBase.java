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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapMemoryLimitException;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.table.PageFrameRecordCursor;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
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
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Dense ASOF JOIN cursor is an improvement over the Light cursor for the case where
 * the slave cursor is a {@link TimeFrameCursor}. While the Light cursor uses a
 * forward-only scan of the slave cursor, the Dense cursor uses two scans: forward and
 * backward. They both start at the slave row that matches the first master row by
 * timestamp (as determined by {@link AbstractAsOfJoinFastRecordCursor#nextSlave
 * nextSlave()}).
 * <p>
 * When encountering another master row, we first resume the forward scan from the
 * previous position until the master timestamp. While scanning, we memorize the join
 * key at each row in a hashmap. Then we check whether the key is in the hashmap. If
 * yes, we're done.
 * <p>
 * Up to this point, the algorithm is identical to the Light cursor. The key difference
 * is, we didn't start the forward scan at the top of the slave cursor, and not finding
 * the key in the hashmap doesn't mean there's no match. We must continue with the
 * backward scan.
 * <p>
 * If we didn't find the join key in the hashmap of the forward scan, we check whether
 * it's in the backward scan's hashmap. If not, we resume the backward scan until we
 * find the key or exhaust the backward scan. In the backward scan, we memorize only new
 * keys (not already encountered in backward scan).
 * <p>
 * The Dense algorithm is the best choice when the master rows are densely interleaved
 * with slave rows. For each master row, we only need to scan a few slave rows. If the
 * interleaving is sparse, we'll still scan everything from the previous position, while
 * the matching row could be only a few rows behind the master.
 * <p>
 * The Fast/Memoized algos are better for sparse interleaving because they use binary
 * search to quickly zero in on the latest slave row ahead of master, and then search
 * backward. In a typical case, this means they are able to entirely ignore most of the
 * slave rows.
 */
public abstract class AsOfJoinDenseRecordCursorFactoryBase extends AbstractJoinRecordCursorFactory {
    protected static final ArrayColumnTypes TYPES_KEY = new ArrayColumnTypes();
    protected static final ArrayColumnTypes TYPES_VALUE = new ArrayColumnTypes();
    protected final boolean adaptive;
    private final int indexColumnIndex;
    private final int lookahead;
    private final long mapMemoryLimit;
    private final long toleranceInterval;
    protected AsOfJoinDenseRecordCursorBase cursor;

    public AsOfJoinDenseRecordCursorFactoryBase(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            JoinContext joinContext,
            long toleranceInterval,
            boolean adaptive,
            int indexColumnIndex
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        this.toleranceInterval = toleranceInterval;
        this.adaptive = adaptive;
        this.indexColumnIndex = indexColumnIndex;
        this.lookahead = adaptive ? configuration.getSqlAsOfJoinLookAhead() : 1;
        this.mapMemoryLimit = configuration.getSqlHashJoinValuePageSize();
    }

    @Override
    public boolean followedOrderByAdvice() {
        return masterFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = masterFactory.getCursor(executionContext);
        TimeFrameCursor slaveCursor = null;
        try {
            slaveCursor = slaveFactory.getTimeFrameCursor(executionContext);
            // Bind the per-query tracker before of(); the cursor's of()
            // reopens its SingleRecordSinks (in the keyed variants), so the
            // first malloc lands under the bound tracker.
            cursor.setMemoryTracker(executionContext.getMemoryTracker());
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() reopens the sinks/maps before adopting the cursors, so close() here frees only the partial heap.
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @TestOnly
    public boolean isForwardScanMode() {
        return cursor.forwardMode;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        putFactoryType(sink);
        sink.attr("condition").val(joinContext);
        if (indexColumnIndex >= 0) {
            sink.attr("indexed").val(true);
        }
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        final AsOfJoinDenseRecordCursorBase cursor = this.cursor;
        this.cursor = null;
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, cursor);
        CairoException.rethrowCleanupFailure(failure);
    }

    protected Map createScanMap(CairoConfiguration configuration, ColumnTypes keyTypes) {
        return adaptive
                ? MapFactory.createUnorderedMap(configuration, keyTypes, TYPES_VALUE, Long.MAX_VALUE)
                : MapFactory.createUnorderedMap(configuration, keyTypes, TYPES_VALUE, false, false);
    }

    protected abstract void putFactoryType(PlanSink sink);

    protected abstract class AsOfJoinDenseRecordCursorBase extends AbstractKeyedAsOfJoinRecordCursor {
        protected static final int DUMMY_VALUE = -10;

        // A work window amortizes the decision; the margins avoid switching on near ties.
        private static final long MAX_LOOKUP_ROWS = 65_536;
        private static final int WORK_WINDOW = 64;
        private final CompiledAsOfLookup.Result backwardResult = new CompiledAsOfLookup.Result();
        private final Map bwdScanKeyToRowId;
        private final Map fwdScanKeyToRowId;
        private boolean allowForwardScan;
        private long backwardRowId = -1;
        private boolean backwardScanExhausted;
        private boolean forwardMode;
        private long forwardRowId = -1;
        private long forwardRows;
        private boolean forwardScanExhausted;
        private long forwardScanLimit;
        private long forwardNanos;
        private int forwardSamples;
        private long forwardTrialRows;
        private long indexedNanos;
        private long latestRowId = -1;
        private boolean memoizedLookup;
        private long previousLookupFrameHi;
        private long previousLookupRowId = -1;
        private boolean slaveCursorReadyForForwardScan;
        private int sampleCountdown;
        private int trialCooldown;
        private long windowBackwardRows;
        private long windowForwardRows;
        private int windowIndexedLookups;
        private int windowProbes;

        protected AsOfJoinDenseRecordCursorBase(
                int columnSplit,
                Map fwdScanKeyToRowId,
                Map bwdScanKeyToRowId,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, AsOfJoinDenseRecordCursorFactoryBase.this.lookahead);
            this.fwdScanKeyToRowId = fwdScanKeyToRowId;
            this.bwdScanKeyToRowId = bwdScanKeyToRowId;
        }

        public void aggregate(MapValue value, GroupByFunctionsUpdater updater, Record aggregateRecord) {
            CompiledAsOfJoin.State state = new CompiledAsOfJoin.State();
            long rows = 0;
            while (hasNext()) {
                if (rows == 0) {
                    updater.updateNew(value, aggregateRecord, rows++);
                } else {
                    updater.updateExisting(value, aggregateRecord, rows++);
                }
                while (forwardMode && !forwardScanExhausted && forwardRowId >= 0 && indexColumnIndex < 0
                        && masterCursor instanceof PageFrameRecordCursor frames
                        && masterRecord instanceof PageFrameMemoryRecord master) {
                    circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    long hi = frames.getContiguousFrameRowHi();
                    long lo = Rows.toLocalRowID(master.getRowId()) + 1;
                    if (hi <= lo) {
                        break;
                    }
                    if (!slaveCursorReadyForForwardScan) {
                        slaveTimeFrameCursor.jumpTo(Rows.toPartitionIndex(forwardRowId));
                        slaveTimeFrameCursor.open();
                        slaveCursorReadyForForwardScan = true;
                    }
                    // The borrowed span proves the next row is in the same frame and within LIMIT.
                    if (!masterCursor.hasNext()) {
                        throw new IllegalStateException("ASOF master span ended early");
                    }
                    state.masterLo = state.nextMaster = lo;
                    state.masterHi = Math.min(hi, lo + WORK_WINDOW - windowProbes);
                    state.forwardRowId = forwardRowId;
                    state.latestRowId = latestRowId;
                    state.forwardRows = 0;
                    state.forwardLimit = forwardScanLimit;
                    state.slaveHi = Rows.toRowID(slaveTimeFrame.getFrameIndex(), slaveTimeFrame.getRowHi());
                    boolean refused = false;
                    try {
                        aggregateForwardRegion(master, value, state);
                    } catch (MapMemoryLimitException e) {
                        refused = true;
                    }
                    long completed = state.nextMaster - lo;
                    frames.consumeFrameRows(completed);
                    forwardRowId = state.forwardRowId;
                    latestRowId = state.latestRowId;
                    // recordAt() borrows records without changing the current TimeFrame.
                    for (int i = 0; i < completed; i++) {
                        if (adaptive && state.backwardWork[i] >= 0) {
                            observeWork(state.backwardWork[i], state.forwardWork[i], false);
                        }
                    }
                    rows += completed;
                    if (state.nextMaster == state.masterHi) {
                        continue;
                    }
                    if (completed > 0 && !masterCursor.hasNext()) {
                        throw new IllegalStateException("ASOF partial master span ended early");
                    }
                    // Retry only the unfinished probe. Completed rows already changed aggregate state.
                    if (refused) {
                        retryWithoutScanState();
                    } else {
                        long timestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
                        long minTimestamp = toleranceInterval == Numbers.LONG_NULL ? Long.MIN_VALUE : timestamp - toleranceInterval;
                        int key = setupSymbolKeyToFind();
                        if (key == SymbolTable.VALUE_NOT_FOUND) {
                            record.hasSlave(false);
                        } else {
                            try {
                                matchForward(timestamp, minTimestamp, key, state.forwardRows);
                            } catch (MapMemoryLimitException e) {
                                retryWithoutScanState();
                            }
                        }
                    }
                    updater.updateExisting(value, aggregateRecord, rows++);
                    break;
                }
            }
            if (rows == 0) {
                updater.updateEmpty(value);
            }
        }

        protected void aggregateForwardRegion(PageFrameMemoryRecord master, MapValue value, CompiledAsOfJoin.State state) {
        }

        protected Map getForwardMap() {
            return fwdScanKeyToRowId;
        }

        @Override
        public void close() {
            Misc.free(bwdScanKeyToRowId);
            Misc.free(fwdScanKeyToRowId);
            super.close();
        }

        @Override
        public boolean hasNext() {
            try {
                if (indexColumnIndex < 0 || --sampleCountdown > 0) {
                    return hasNext0();
                }
                // Index work has no useful row-count equivalent. Sample complete operator calls,
                // excluding the consumer, rather than time individual RHS rows or assume a CPU cost.
                // A prime sampling interval avoids locking onto common power-of-two key cycles.
                sampleCountdown = 67;
                boolean wasForward = forwardMode;
                long start = System.nanoTime();
                boolean result = hasNext0();
                long elapsed = System.nanoTime() - start;
                if (result && wasForward == forwardMode) {
                    if (wasForward) {
                        forwardNanos = forwardSamples++ == 0 ? elapsed : (forwardNanos * 3 + elapsed) / 4;
                        if ((forwardSamples >= 4 || forwardTrialRows >= MAX_LOOKUP_ROWS)
                                && forwardNanos > indexedNanos + indexedNanos / 4) {
                            switchToLookup();
                        }
                    } else {
                        indexedNanos = indexedNanos == 0 ? elapsed : (indexedNanos * 3 + elapsed) / 4;
                    }
                }
                return result;
            } catch (MapMemoryLimitException e) {
                return retryWithoutScanState();
            }
        }

        private boolean hasNext0() {
            if (adaptive && !forwardMode) {
                return super.hasNext();
            }
            // Consult the breaker at the top, so an empty master still observes cancellation.
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            if (!masterCursor.hasNext()) {
                return false;
            }
            final long masterTimestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
            final long minSlaveTimestamp = toleranceInterval == Numbers.LONG_NULL
                    ? Long.MIN_VALUE
                    : masterTimestamp - toleranceInterval;
            int slaveKeyToFind = setupSymbolKeyToFind();
            if (slaveKeyToFind == SymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                return true;
            }

            return matchForward(masterTimestamp, minSlaveTimestamp, slaveKeyToFind);
        }

        private boolean matchForward(long masterTimestamp, long minSlaveTimestamp, int slaveKeyToFind) {
            return matchForward(masterTimestamp, minSlaveTimestamp, slaveKeyToFind, 0);
        }

        private boolean matchForward(long masterTimestamp, long minSlaveTimestamp, int slaveKeyToFind, long alreadyScanned) {
            if (forwardRowId == -1) {
                // No scanning done yet, initialize state of forward and backward scans
                nextSlave(masterTimestamp);
                if (!record.hasSlave()) {
                    // There are no prevailing slave rows (all are more recent than master row)
                    return true;
                }
                long rowId = slaveRecB.getRowId();
                backwardRowId = rowId;
                forwardRowId = rowId;
            }

            if (!slaveCursorReadyForForwardScan) {
                slaveTimeFrameCursor.jumpTo(Rows.toPartitionIndex(forwardRowId));
                slaveTimeFrameCursor.open();
                slaveTimeFrameCursor.recordAt(slaveRecB, forwardRowId);
                slaveCursorReadyForForwardScan = true;
            }

            MapKey key;
            MapValue value;
            forwardRows = alreadyScanned;
            if (!forwardScanExhausted && !scanForward(masterTimestamp, minSlaveTimestamp)) {
                // The gap changed before the next work window ended. Discard the incomplete
                // maps and seek this same master row; do not advance or emit it twice.
                switchToLookup();
                findSlave(masterTimestamp);
                return true;
            }

            // Let's see if we saw a matching symbol in forward scan
            key = fwdScanKeyToRowId.withKey();
            putSlaveKeyToFind(key, slaveKeyToFind);
            value = key.findValue();
            if (value != null) {
                return setupSlaveRec(value.getLong(0), minSlaveTimestamp);
            }
            // Symbol not found, see if we already saw it in backward scan
            key = bwdScanKeyToRowId.withKey();
            putSlaveKeyToFind(key, slaveKeyToFind);
            value = key.findValue();
            if (value != null) {
                return setupSlaveRec(value.getLong(0), minSlaveTimestamp);
            }
            if (indexColumnIndex >= 0) {
                // Forward state covers the recent interval. Use the index for unresolved
                // history instead of building a map of a potentially enormous prefix.
                slaveCursorReadyForForwardScan = false;
                slaveTimeFrameCursor.jumpTo(Rows.toPartitionIndex(latestRowId));
                slaveTimeFrameCursor.open();
                slaveTimeFrameCursor.recordAt(slaveRecB, latestRowId);
                boolean found = findIndexedMatch(indexColumnIndex, TableUtils.toIndexKey(slaveKeyToFind), masterTimestamp, minSlaveTimestamp);
                long rowId = found ? slaveRecB.getRowId() : Numbers.LONG_NULL;
                key = bwdScanKeyToRowId.withKey();
                putSlaveKeyToFind(key, slaveKeyToFind);
                value = key.createValue();
                value.putLong(0, rowId);
                // A negative history entry remains valid: later occurrences enter the forward map.
                return setupSlaveRec(rowId, minSlaveTimestamp);
            }
            if (backwardScanExhausted) {
                // Symbol not found in backward scan, and the scan already reached the end, report no match
                record.hasSlave(false);
                return true;
            }

            // Resume the backward scan
            slaveCursorReadyForForwardScan = false;
            slaveTimeFrameCursor.jumpTo(Rows.toPartitionIndex(backwardRowId));
            slaveTimeFrameCursor.open();
            long frameRowLo = Rows.toRowID(slaveTimeFrame.getFrameIndex(), slaveTimeFrame.getRowLo());
            slaveTimeFrameCursor.recordAt(slaveRecB, backwardRowId);
            while (true) {
                long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                if (slaveTimestamp < minSlaveTimestamp) {
                    // minSlaveTimestamp will only get larger in later calls, it's safe to conclude backward scan now
                    backwardScanExhausted = true;
                    break;
                }
                if (matchesSlaveFilter()) {
                    key = bwdScanKeyToRowId.withKey();
                    putSlaveJoinKey(key);
                    value = key.createValue();
                    if (value.isNew()) {
                        value.putLong(0, backwardRowId);
                    }
                    int slaveKey = getSlaveJoinKey();
                    if (joinKeysMatch(slaveKeyToFind, slaveKey)) {
                        return setupSlaveRec(backwardRowId, minSlaveTimestamp);
                    }
                }
                if (backwardRowId > frameRowLo) {
                    backwardRowId--;
                    slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, Rows.toLocalRowID(backwardRowId));
                } else {
                    if (!slaveTimeFrameCursor.prev()) {
                        backwardScanExhausted = true;
                        break;
                    }
                    slaveTimeFrameCursor.open();
                    int frameIndex = slaveTimeFrame.getFrameIndex();
                    frameRowLo = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                    backwardRowId = Rows.toRowID(frameIndex, slaveTimeFrame.getRowHi() - 1);
                    slaveTimeFrameCursor.recordAt(slaveRecB, backwardRowId);
                }
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            }
            record.hasSlave(false);
            return true;
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            // Forced Dense opens state before adopting the cursors. Adaptive lookup opens it
            // only when switching, under the same tracker, with normal cursor-close ownership.
            if (!adaptive) {
                fwdScanKeyToRowId.reopen();
                fwdScanKeyToRowId.clear();
                bwdScanKeyToRowId.reopen();
                bwdScanKeyToRowId.clear();
            }
            resetScanState();
            resetTiming();
            super.of(masterCursor, slaveCursor, circuitBreaker);
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            // Bound lazily before of() reopens them; map malloc/free nets on the per-query counter.
            fwdScanKeyToRowId.setMemoryTracker(tracker);
            bwdScanKeyToRowId.setMemoryTracker(tracker);
            if (adaptive) {
                // At most one configured join-value page per map; finite query budgets
                // devote at most a quarter to both maps. Actual allocations remain tracked.
                long queryLimit = tracker == null ? 0 : tracker.getLimit();
                long limit = queryLimit > 0 ? Math.min(mapMemoryLimit, queryLimit / 8) : mapMemoryLimit;
                fwdScanKeyToRowId.setMemoryLimit(limit);
                bwdScanKeyToRowId.setMemoryLimit(limit);
            }
            allowForwardScan = supportsForwardScan();
        }

        @Override
        public void toTop() {
            super.toTop();
            allowForwardScan = supportsForwardScan();
            if (adaptive) {
                fwdScanKeyToRowId.close();
                bwdScanKeyToRowId.close();
            }
            if (fwdScanKeyToRowId.isOpen()) {
                fwdScanKeyToRowId.clear();
            }
            if (bwdScanKeyToRowId.isOpen()) {
                bwdScanKeyToRowId.clear();
            }
            resetScanState();
            resetTiming();
        }

        private boolean retryWithoutScanState() {
            // A full history map can be replaced by a smaller per-probe cache. An
            // oversized first key or a rejected initial allocation cannot benefit from it.
            boolean retryMemoized = indexColumnIndex < 0
                    && (bwdScanKeyToRowId.size() > 1 || fwdScanKeyToRowId.size() > 1);
            long timestamp = scaleTimestamp(masterRecord.getTimestamp(masterTimestampIndex), masterTimestampScale);
            for (;;) {
                switchToLookup();
                allowForwardScan = false;
                memoizedLookup = retryMemoized;
                try {
                    // Retry this master row, never advance or emit it twice. Only optional
                    // storage refusal gets here; CairoException/OOM/cancellation propagate.
                    findSlave(timestamp);
                    return true;
                } catch (MapMemoryLimitException e) {
                    if (!retryMemoized) {
                        throw e;
                    }
                    retryMemoized = false;
                }
            }
        }

        private void resetTiming() {
            indexedNanos = 0;
            forwardNanos = 0;
            forwardSamples = 0;
            sampleCountdown = 67;
            trialCooldown = 0;
        }

        private void resetScanState() {
            slaveCursorReadyForForwardScan = false;
            forwardScanExhausted = false;
            backwardScanExhausted = false;
            backwardRowId = -1;
            forwardRowId = -1;
            forwardMode = !adaptive;
            forwardScanLimit = Long.MAX_VALUE;
            latestRowId = -1;
            memoizedLookup = false;
            previousLookupRowId = -1;
            resetWorkWindow();
        }

        private void resetWorkWindow() {
            windowIndexedLookups = 0;
            windowProbes = 0;
            windowBackwardRows = 0;
            windowForwardRows = 0;
        }

        private boolean scanForward(long masterTimestamp, long minSlaveTimestamp) {
            MapValue value;
            MapKey key;
            long frameRowHi = Rows.toRowID(slaveTimeFrame.getFrameIndex(), slaveTimeFrame.getRowHi());
            // Returning a historical match can move record B away from the forward frame.
            // Rebind once on entry, then only adjust its row until crossing a frame boundary.
            slaveTimeFrameCursor.recordAt(slaveRecB, forwardRowId);
            while (true) {
                long lo = Rows.toLocalRowID(forwardRowId);
                long hi = lo + Math.min(frameRowHi - forwardRowId, forwardScanLimit - forwardRows);
                long next = scanForwardBatch(fwdScanKeyToRowId, lo, hi, minSlaveTimestamp, masterTimestamp);
                if (next >= 0) {
                    assert next >= lo && next <= hi;
                    if (next > lo) {
                        forwardRows += next - lo;
                        forwardRowId += next - lo;
                        latestRowId = forwardRowId - 1;
                    }
                    if (next < hi) {
                        return true; // first future timestamp, not a gap in qualifying rows
                    }
                    if (forwardRowId < frameRowHi) {
                        slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, next);
                        if (scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale) <= masterTimestamp) {
                            forwardRows++;
                            return false; // scan budget exhausted; retry this probe via lookup
                        }
                        return true;
                    }
                } else {
                    do {
                        long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
                        if (slaveTimestamp > masterTimestamp) {
                            return true;
                        }
                        if (forwardRows++ == forwardScanLimit) {
                            return false;
                        }
                        latestRowId = forwardRowId;
                        if (slaveTimestamp >= minSlaveTimestamp && matchesSlaveFilter()) {
                            key = fwdScanKeyToRowId.withKey();
                            putSlaveJoinKey(key);
                            value = key.createValue();
                            value.putLong(0, slaveRecB.getRowId());
                        }
                        forwardRowId++;
                        if (forwardRowId == frameRowHi) {
                            break;
                        }
                        slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, Rows.toLocalRowID(forwardRowId));
                        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                    } while (true);
                }
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (!slaveTimeFrameCursor.next()) {
                    forwardScanExhausted = true;
                    return true;
                }
                slaveTimeFrameCursor.open();
                int frameIndex = slaveTimeFrame.getFrameIndex();
                frameRowHi = Rows.toRowID(frameIndex, slaveTimeFrame.getRowHi());
                forwardRowId = Rows.toRowID(frameIndex, slaveTimeFrame.getRowLo());
                slaveTimeFrameCursor.recordAt(slaveRecB, forwardRowId);
            }
        }

        /** Optional borrowed-frame region. A negative result declines before map mutation. */
        protected long scanForwardBatch(Map map, long lo, long hi, long minTimestamp, long maxTimestamp) {
            return -1;
        }

        protected CompiledAsOfLookup.Result scanBackwardFrame(long lo, long hi, long minTimestamp) {
            return null;
        }

        private boolean setupSlaveRec(long slaveRowId, long minSlaveTimestamp) {
            if (slaveRowId == Numbers.LONG_NULL) {
                record.hasSlave(false);
                observeWork(0, forwardRows, true);
                return true;
            }
            slaveTimeFrameCursor.recordAt(slaveRecB, slaveRowId);
            long slaveTimestamp = scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale);
            record.hasSlave(slaveTimestamp >= minSlaveTimestamp);
            if (indexColumnIndex >= 0) {
                observeWork(0, forwardRows, latestRowId != slaveRowId);
            } else if (adaptive && Rows.toPartitionIndex(latestRowId) == Rows.toPartitionIndex(slaveRowId)) {
                // The map's answer tells us the exact alternative backward distance for free.
                observeWork(latestRowId - slaveRowId + 1, forwardRows, false);
            }
            return true;
        }

        protected abstract int getSlaveJoinKey();

        protected abstract boolean joinKeysMatch(int slaveKeyToFind, int slaveKey);

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            int keyToFind = setupSymbolKeyToFind();
            if (keyToFind == SymbolTable.VALUE_NOT_FOUND) {
                record.hasSlave(false);
                return;
            }
            final long position = Rows.toRowID(origSlaveFrameIndex, origSlaveRowId);
            long gap = -1;
            if (previousLookupRowId != -1) {
                int previousFrame = Rows.toPartitionIndex(previousLookupRowId);
                if (previousFrame == origSlaveFrameIndex) {
                    gap = position - previousLookupRowId;
                } else if (previousFrame + 1 == origSlaveFrameIndex) {
                    gap = previousLookupFrameHi - Rows.toLocalRowID(previousLookupRowId)
                            + origSlaveRowId - slaveTimeFrame.getRowLo();
                }
            }
            previousLookupRowId = position;
            previousLookupFrameHi = slaveTimeFrame.getRowHi();
            final long minTimestamp = toleranceInterval == Numbers.LONG_NULL
                    ? Long.MIN_VALUE : masterTimestamp - toleranceInterval;
            long row = origSlaveRowId;
            int frame = origSlaveFrameIndex;
            long rowLo = slaveTimeFrame.getRowLo();
            long comparisons = 0;
            boolean usedIndex = false;
            boolean memoChecked = false;
            long cachedThrough = -1;
            long cachedRow = Numbers.LONG_NULL;
            while (true) {
                if (scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale) < minTimestamp) {
                    record.hasSlave(false);
                    break;
                }
                comparisons++;
                if (matchesSlaveFilter() && joinKeysMatch(keyToFind, getSlaveJoinKey())) {
                    record.hasSlave(true);
                    break;
                }
                if (memoizedLookup) {
                    if (!memoChecked) {
                        memoChecked = true;
                        if (bwdScanKeyToRowId.isOpen()) {
                            MapKey key = bwdScanKeyToRowId.withKey();
                            putSlaveKeyToFind(key, keyToFind);
                            MapValue value = key.findValue();
                            if (value != null) {
                                cachedRow = value.getLong(0);
                                key = fwdScanKeyToRowId.withKey();
                                putSlaveKeyToFind(key, keyToFind);
                                value = key.findValue();
                                assert value != null;
                                cachedThrough = value.getLong(0);
                            }
                        }
                    }
                    // Like Memoized ASOF, only reuse a result after examining all newer
                    // rows. Row IDs are compared for order, never subtracted across frames.
                    if (Rows.toRowID(frame, row) <= cachedThrough) {
                        if (cachedRow == Numbers.LONG_NULL) {
                            record.hasSlave(false);
                        } else {
                            slaveTimeFrameCursor.recordAt(slaveRecB, cachedRow);
                            record.hasSlave(scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale) >= minTimestamp);
                        }
                        break;
                    }
                }
                if (indexColumnIndex >= 0) {
                    // A local match needs no index opening. After it fails, reuse the existing
                    // indexed predecessor lookup rather than speculate on a long key walk.
                    record.hasSlave(findIndexedMatch(indexColumnIndex, TableUtils.toIndexKey(keyToFind), masterTimestamp, minTimestamp));
                    // Only an exact distance can justify a forward trial. An index result in
                    // another frame does not give a row count, and no scan is done to obtain one.
                    if (record.hasSlave() && Rows.toPartitionIndex(slaveRecB.getRowId()) == origSlaveFrameIndex) {
                        comparisons = position - slaveRecB.getRowId() + 1;
                    }
                    usedIndex = true;
                    break;
                }
                if (allowForwardScan && !memoizedLookup && comparisons == MAX_LOOKUP_ROWS) {
                    // Bound a single unsuccessful exploration. Dense will finish this lookup
                    // and retain its history, instead of repeating the entire prefix next time.
                    switchToForward(comparisons);
                    matchForward(masterTimestamp, minTimestamp, keyToFind);
                    return;
                }
                // Preserve the cheap first candidate. Search the remaining frame as one
                // span, using the same memo and work boundaries for Java and generated code.
                long lookupLo = rowLo;
                if (memoizedLookup && cachedThrough >= 0 && Rows.toPartitionIndex(cachedThrough) == frame) {
                    lookupLo = Math.max(lookupLo, Rows.toLocalRowID(cachedThrough) + 1);
                }
                if (allowForwardScan && !memoizedLookup) {
                    lookupLo = Math.max(lookupLo, row - (MAX_LOOKUP_ROWS - comparisons - 1));
                }
                if (row > lookupLo) {
                    CompiledAsOfLookup.Result result = scanBackwardFrame(lookupLo, row, minTimestamp);
                    if (result == null) {
                        result = scanBackwardFrameJava(lookupLo, row, minTimestamp, keyToFind);
                    }
                    comparisons += result.examined;
                    if (result.row >= lookupLo) {
                        slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, result.row);
                        record.hasSlave(result.matched);
                        break;
                    }
                    row = lookupLo;
                }
                if (--row < rowLo) {
                    if (!slaveTimeFrameCursor.prev()) {
                        record.hasSlave(false);
                        break;
                    }
                    slaveTimeFrameCursor.open();
                    frame = slaveTimeFrame.getFrameIndex();
                    row = slaveTimeFrame.getRowHi() - 1;
                    rowLo = slaveTimeFrame.getRowLo();
                    slaveTimeFrameCursor.recordAt(slaveRecB, Rows.toRowID(frame, row));
                } else {
                    slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, row);
                }
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            }
            if (memoChecked) {
                bwdScanKeyToRowId.reopen();
                MapKey key = bwdScanKeyToRowId.withKey();
                putSlaveKeyToFind(key, keyToFind);
                MapValue value = key.createValue();
                value.putLong(0, record.hasSlave() ? slaveRecB.getRowId() : Numbers.LONG_NULL);
                // In lookup mode the two maps hold a result and its validity boundary.
                // Keep their original value width: widening it changes MapFactory's choice
                // of map implementation and penalizes the dense path.
                fwdScanKeyToRowId.reopen();
                key = fwdScanKeyToRowId.withKey();
                putSlaveKeyToFind(key, keyToFind);
                value = key.createValue();
                value.putLong(0, position);
            }
            // A nonadjacent frame jump has unknown distance. Never subtract encoded row IDs
            // across frames or open the skipped history just to estimate a selection cost.
            if (gap >= 0) {
                observeWork(comparisons, gap, usedIndex);
            }
        }

        private CompiledAsOfLookup.Result scanBackwardFrameJava(long lo, long hi, long minTimestamp, int keyToFind) {
            long row = hi;
            boolean matched = false;
            while (--row >= lo) {
                slaveTimeFrameCursor.recordAtRowIndex(slaveRecB, row);
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (scaleTimestamp(slaveRecB.getTimestamp(slaveTimestampIndex), slaveTimestampScale) < minTimestamp) {
                    break;
                }
                if (matchesSlaveFilter() && joinKeysMatch(keyToFind, getSlaveJoinKey())) {
                    matched = true;
                    break;
                }
            }
            backwardResult.row = row;
            backwardResult.matched = matched;
            backwardResult.examined = hi - row - (matched ? 0 : 1);
            return backwardResult;
        }

        private void observeWork(long backwardRows, long forwardRows, boolean usedIndex) {
            if (indexColumnIndex >= 0 && forwardMode && forwardSamples < 4) {
                forwardTrialRows += forwardRows;
                if (forwardTrialRows >= MAX_LOOKUP_ROWS) {
                    sampleCountdown = 0;
                }
            }
            // Saturation keeps a very large interval from overflowing the window arithmetic.
            windowBackwardRows += Math.min(backwardRows, MAX_LOOKUP_ROWS);
            windowForwardRows += Math.min(forwardRows, MAX_LOOKUP_ROWS);
            if (usedIndex) {
                windowIndexedLookups++;
            }
            if (++windowProbes == WORK_WINDOW) {
                if (indexColumnIndex >= 0) {
                    if (forwardMode) {
                        if (windowIndexedLookups < WORK_WINDOW / 4) {
                            switchToLookup();
                        }
                    } else if (trialCooldown > 0) {
                        trialCooldown--;
                    } else if (indexedNanos > 0 && windowIndexedLookups >= WORK_WINDOW / 2
                            && (windowForwardRows <= WORK_WINDOW || windowBackwardRows > 2 * windowForwardRows)) {
                        // Row work admits a bounded trial; sampled costs decide whether to
                        // retain it. A rejected trial waits before exploring again.
                        switchToForward(windowBackwardRows / WORK_WINDOW);
                    }
                } else if (forwardMode) {
                    forwardScanLimit = Math.max(WORK_WINDOW, 4 * windowBackwardRows / WORK_WINDOW);
                    if (windowForwardRows > 4 * windowBackwardRows) {
                        switchToLookup();
                    }
                } else if (windowBackwardRows > 2 * windowForwardRows && windowBackwardRows >= WORK_WINDOW * 2L) {
                    switchToForward(windowBackwardRows / WORK_WINDOW);
                }
                resetWorkWindow();
            }
        }

        private void switchToForward(long backwardRowsPerProbe) {
            if (!allowForwardScan) {
                return;
            }
            if (memoizedLookup) {
                // Lookup entries have per-key validity. They cannot seed a contiguous
                // forward interval; discard them before establishing a new scan frontier.
                bwdScanKeyToRowId.close();
                fwdScanKeyToRowId.close();
                memoizedLookup = false;
            }
            fwdScanKeyToRowId.reopen();
            bwdScanKeyToRowId.reopen();
            forwardMode = true;
            forwardScanLimit = Math.max(WORK_WINDOW, 4 * backwardRowsPerProbe);
            forwardSamples = 0;
            forwardTrialRows = 0;
            sampleCountdown = 0;
            forwardRowId = backwardRowId = Rows.toRowID(origSlaveFrameIndex, origSlaveRowId);
            forwardScanExhausted = backwardScanExhausted = false;
            slaveCursorReadyForForwardScan = false;
            latestRowId = -1;
            resetWorkWindow();
        }

        private void switchToLookup() {
            // A partial map is not evidence that a key is absent. Release it at the mode
            // boundary, including any oversized capacity, and restart timestamp navigation.
            fwdScanKeyToRowId.close();
            bwdScanKeyToRowId.close();
            resetScanState();
            memoizedLookup = indexColumnIndex < 0 && allowForwardScan;
            trialCooldown = WORK_WINDOW;
            origSlaveRowId = -1;
            origSlaveFrameIndex = -1;
            origHasSlave = false;
            slaveFrameIndex = -1;
            slaveFrameRow = Long.MIN_VALUE;
            lookaheadTimestamp = Long.MIN_VALUE;
            isSlaveOpenPending = false;
            isSlaveForwardScan = true;
        }

        protected abstract void putSlaveJoinKey(MapKey key);

        protected abstract void putSlaveKeyToFind(MapKey key, int slaveKeyToFind);

        protected boolean matchesSlaveFilter() {
            return true;
        }

        protected boolean supportsForwardScan() {
            return true;
        }

        protected abstract int setupSymbolKeyToFind();
    }

    static {
        TYPES_KEY.add(ColumnType.INT);
        TYPES_VALUE.add(ColumnType.LONG);
    }
}
