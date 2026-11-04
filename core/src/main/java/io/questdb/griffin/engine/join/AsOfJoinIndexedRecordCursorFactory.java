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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.Nullable;

/**
 * AsOf Join factory that leverages symbol bitmap indexes for efficient row lookup.
 * This implementation uses the bitmap index to quickly find matching symbol values
 * instead of linear scanning through time-ordered records.
 */
public final class AsOfJoinIndexedRecordCursorFactory extends AbstractJoinRecordCursorFactory {
    private final AsOfJoinIndexedRecordCursor cursor;
    private final int slaveSymbolColumnIndex;
    private final SymbolJoinKeyMapping symbolJoinKeyMapping;
    private final long toleranceInterval;

    public AsOfJoinIndexedRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            int columnSplit,
            int slaveSymbolColumnIndex,
            SymbolJoinKeyMapping symbolJoinKeyMapping,
            JoinContext joinContext,
            long toleranceInterval
    ) {
        super(metadata, joinContext, masterFactory, slaveFactory);
        assert slaveFactory.supportsTimeFrameCursor();
        this.symbolJoinKeyMapping = symbolJoinKeyMapping;
        this.slaveSymbolColumnIndex = slaveSymbolColumnIndex;
        RecordMetadata masterMeta = masterFactory.getMetadata();
        RecordMetadata slaveMeta = slaveFactory.getMetadata();
        this.cursor = new AsOfJoinIndexedRecordCursor(
                columnSplit,
                NullRecordFactory.getInstance(slaveMeta),
                masterMeta.getTimestampIndex(),
                masterMeta.getTimestampType(),
                slaveMeta.getTimestampIndex(),
                slaveMeta.getTimestampType(),
                configuration.getSqlAsOfJoinLookAhead()
        );
        this.toleranceInterval = toleranceInterval;
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
            // Bind before of(), which reopens the symbol key cache before adopting the cursors.
            cursor.setMemoryTracker(executionContext.getMemoryTracker());
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            // of() reopens the symbol key cache before adopting the cursors, so close() here frees only the cache.
            Misc.free(cursor);
            throw e;
        }
    }

    @Override
    public int getScanDirection() {
        return masterFactory.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("AsOf Join Indexed Scan");
        sink.attr("condition").val(joinContext);
        sink.child(masterFactory);
        sink.child(slaveFactory);
    }

    @Override
    protected void _close() {
        Throwable failure = closeJoinOwnersBestEffort();
        failure = Misc.freeBestEffort(failure, symbolJoinKeyMapping);
        CairoException.rethrowCleanupFailure(failure);
    }

    private class AsOfJoinIndexedRecordCursor extends AbstractKeyedAsOfJoinRecordCursor {

        public AsOfJoinIndexedRecordCursor(
                int columnSplit,
                Record nullRecord,
                int masterTimestampIndex,
                int masterTimestampType,
                int slaveTimestampIndex,
                int slaveTimestampType,
                int lookahead
        ) {
            super(columnSplit, nullRecord, masterTimestampIndex, masterTimestampType, slaveTimestampIndex, slaveTimestampType, lookahead);
        }

        @Override
        public void close() {
            symbolJoinKeyMapping.close();
            super.close();
        }

        @Override
        public void of(RecordCursor masterCursor, TimeFrameCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            // Reopen the symbol key cache before super.of() adopts the cursors so an open-time breach frees it exactly once.
            symbolJoinKeyMapping.reopen();
            super.of(masterCursor, slaveCursor, circuitBreaker);
            symbolJoinKeyMapping.of(slaveCursor);
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            symbolJoinKeyMapping.setMemoryTracker(tracker);
        }

        @Override
        protected void performKeyMatching(long masterTimestamp) {
            int symbolKey = TableUtils.toIndexKey(symbolJoinKeyMapping.getSlaveKey(masterRecord));
            if (symbolKey < 0) {
                record.hasSlave(false);
                return;
            }

            record.hasSlave(findIndexedMatch(
                    slaveSymbolColumnIndex,
                    symbolKey,
                    masterTimestamp,
                    toleranceInterval == Numbers.LONG_NULL ? Long.MIN_VALUE : masterTimestamp - toleranceInterval
            ));
        }
    }
}
