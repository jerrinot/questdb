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

import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;

/** Query-owned forward region; algorithm selection and historical lookup stay with ASOF. */
public interface CompiledAsOfScan extends QuietCloseable {
    void clearExecution();

    CharSequence getName();

    long getNativeRows();

    void prepareExecution(MemoryTracker tracker);

    /**
     * Borrows the positioned, pinned raw RHS record and an exclusively owned latest-row map.
     * No frame navigation may occur during this call. Rows use the record's frame coordinates.
     * Returns the first unconsumed row in [lo, hi], stopping before a timestamp above maxTimestamp,
     * or -1 to decline before mutation. Predicate-eligible rows at/above minTimestamp update
     * their key's row ID. The caller bounds hi by its scan budget and retains all mode transitions.
     * A failure may leave partial map updates; the existing ASOF retry/close owns their disposal.
     */
    long scan(PageFrameMemoryRecord record, Map map, long lo, long hi, long minTimestamp,
              long maxTimestamp, SqlExecutionCircuitBreaker circuitBreaker);
}
