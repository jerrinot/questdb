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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/** Query-owned native join pipeline; acquired slots borrow the pinned build and input. */
public interface CompiledJoin extends QuietCloseable {
    CharSequence getName();

    /** A cached slot-local check of the existing sequence's cancellation and breaker state. */
    Updater newUpdater(Runnable checkpoint);

    interface Updater extends QuietCloseable {
        /** Diagnostics read only after the frame sequence has drained. */
        long getNativeRows();

        /** Native keyed misses serviced by the ordinary map insertion path. */
        default long getNativeMapMisses() {
            return 0;
        }

        /**
         * Borrow the pinned frame, frozen build and slot-owned scalar value for this call only.
         * False is permitted only before mutation. Success must preserve the value's isNew flag:
         * a slot with no joined rows must not participate in the scalar merge.
         */
        boolean update(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                       FrozenHashJoinBuild.IntKeyed build, MapValue value);

        /** Ordinary maps own insertion/growth; a sharded frame supplies its existing shard list. */
        default boolean updateKeyed(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                    FrozenHashJoinBuild.IntKeyed build, Map map, @Nullable ObjList<Map> shards,
                                    GroupByFunctionsUpdater functions) {
            return false;
        }
    }
}
