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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;

/** A bounded forward ASOF region with an aggregate consumer. The cursor owns strategy changes. */
public interface CompiledAsOfJoin extends QuietCloseable {
    void clearExecution();

    CharSequence getName();

    long getNativeRows();

    void prepareExecution(MemoryTracker tracker);

    /**
     * Consume a prefix of the borrowed master span. The latest-row map and aggregate state are
     * the ordinary operator states. Publish progress even on map refusal; only completed master
     * rows have contributed to the aggregate. No master navigation is allowed during the call.
     * Historical RHS payload frames may be borrowed with recordAt() between native calls;
     * the current TimeFrame must remain unchanged.
     */
    void aggregate(PageFrameMemoryRecord master, TimeFrameCursor slave, Map map, MapValue value,
                   State state, SqlExecutionCircuitBreaker breaker);

    record Input(RecordMetadata master, RecordMetadata slave,
                 int masterKey, int slaveKey, IntList slaveProjection, long tolerance,
                 ColumnTypes aggregateInputs, IntList aggregateColumns) {
    }

    final class State {
        public static final int CAPACITY = 64;
        // Negative backward work means the ordinary resolver would not observe this probe.
        public final long[] backwardWork = new long[CAPACITY];
        public final long[] forwardWork = new long[CAPACITY];
        public final int[] symbolKeys = new int[CAPACITY];
        public long masterLo;
        public long masterHi;
        public long nextMaster;
        public long forwardRowId;
        public long latestRowId;
        public long forwardRows;
        public long forwardLimit;
        public long slaveHi;
    }
}
