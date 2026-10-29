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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;

/** A consumer of finalized maps in the producer's acquired worker slots.
 * The cursor arms it before building the producer and drains that producer before clear/close.
 * Input maps remain owned by the producer. Only finish publishes the final result.
 */
public interface CompiledMapAggregate extends QuietCloseable {
    CharSequence getName();
    void reopen(MemoryTracker tracker);
    void finish(Map destination, GroupByAllocator allocator, Runnable checkpoint);
    void clear();

    boolean supportsLongTopK(Function function);

    void prepareLongTopK(Function function);

    void prepareLongTopK(Function function, boolean ascending, int limit);

    boolean longTopK(Map map, DirectLongLongSortedList list, Function function, Runnable checkpoint);
}
