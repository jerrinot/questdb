/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | |/ _ \/ __| __| | | |  _ \
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

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/** Query-owned descending search. The ASOF owner retains navigation, history and work limits. */
public interface CompiledAsOfLookup extends QuietCloseable {
    void clearExecution();

    CharSequence getName();

    long getNativeRows();

    void prepareExecution(MemoryTracker tracker);

    /**
     * Borrows the positioned, pinned raw RHS frame; no navigation occurs during this call.
     * Searches [lo, hi) backwards for the translated key and predicate, stopping below minTimestamp.
     * Returns null before examining candidates if this frame is unsupported. The reused result is valid until
     * the next call. The caller bounds the span by memo and work limits and positions the output record.
     */
    @Nullable Result scan(PageFrameMemoryRecord record, long lo, long hi, long minTimestamp,
                          long key, SqlExecutionCircuitBreaker circuitBreaker);

    final class Result {
        public long examined;
        public boolean matched;
        // lo - 1 means span exhausted; otherwise the matching or first expired row in [lo, hi).
        public long row;
    }
}
