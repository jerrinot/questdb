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

import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.QuietCloseable;

/** Query-owned unkeyed reduction code; existing acquired slots own state and scratch. */
public interface CompiledScalarAggregate extends QuietCloseable {
    CharSequence getName();

    Updater newUpdater();

    /** Source is nonempty. Copy on first contribution, otherwise merge into the destination. */
    void merge(MapValue source, MapValue destination);

    interface Updater extends QuietCloseable {
        /** Read after the frame sequence drains. */
        long getNativeRows();

        /**
         * Zero rowIdsAddress means contiguous rows. Otherwise IDs use original frame coordinates.
         * False is permitted only before mutation. Empty batches must leave isNew unchanged.
         * Input pointers are borrowed for this call only; failure cancels the query, never replays it.
         */
        boolean update(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                       MapValue value, SqlExecutionCircuitBreaker circuitBreaker);
    }
}
