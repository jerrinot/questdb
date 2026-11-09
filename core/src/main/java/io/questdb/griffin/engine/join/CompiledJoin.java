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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/** Query-owned native join pipeline; acquired slots borrow the pinned build and input. */
public interface CompiledJoin extends QuietCloseable {
    /** Releases execution-owned state after every probe worker has drained. */
    default void clearExecution() {
    }

    /** Prepares shared state before probe publication; probeRows is -1 when filtering makes it unknown. */
    default void prepareBuilds(FrozenHashJoinBuild[] builds, long probeRows, MemoryTracker tracker,
                              SqlExecutionCircuitBreaker circuitBreaker) {
    }

    /**
     * Resolved keys in RecordSink order. Column indexes address the scan followed by earlier
     * builds' payloads; this build appends its payload to that prefix after selecting a match.
     */
    record Build(int[] keyColumns, int[] keyTypes, int payloadColumnCount) {
        public Build(int keyColumn, int payloadColumnCount) {
            this(new int[]{keyColumn}, new int[]{io.questdb.cairo.ColumnType.INT}, payloadColumnCount);
        }

        public int keyColumn() {
            return keyColumns[0];
        }
    }

    /** Final preparation over ready payload copies, before worker publication. */
    default void preparePayloads(FrozenHashJoinBuild[] builds, long probeRows, MemoryTracker tracker,
                                 SqlExecutionCircuitBreaker breaker) {
    }

    CharSequence getName();

    /** A cached slot-local check of the existing sequence's cancellation and breaker state. */
    Updater newUpdater(io.questdb.griffin.engine.groupby.GroupByAllocator allocator, Runnable checkpoint);

    interface Updater extends QuietCloseable {
        /** Releases slot scratch after the owning frame sequence has drained; the updater can reopen. */
        default void clearExecution() {
        }

        /** Owner-only preparation using this execution's bound dictionary views, before worker publication. */
        default void prepareDependencies(FrozenHashJoinBuild[] builds, long probeRows, MemoryTracker tracker,
                                         SqlExecutionCircuitBreaker breaker) {
        }

        /** Borrows a slot's dictionary translator; its tables/cache refresh on every reopen. */
        default void bindSymbol(int build, int key, SymbolKeyTranslator.View translator) {
        }

        /** Diagnostics read only after the frame sequence has drained. */
        long getNativeRows();

        /** Distinct-state growth in the last completed keyed batch, for ordinary sharding policy. */
        default long getFunctionCardinality() { return 0; }

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
                       FrozenHashJoinBuild build, MapValue value);

        /** All builds are immutable and pinned until the caller's frame work drains. */
        default boolean updateMulti(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                    FrozenHashJoinBuild[] builds, MapValue value) {
            return builds.length == 1 && update(record, rowCount, rowIdsAddress, builds[0], value);
        }

        /** Same ownership as updateMulti; a retry must preserve every duplicate position. */
        default boolean updateKeyedMulti(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                         FrozenHashJoinBuild[] builds, Map map, @Nullable ObjList<Map> shards,
                                         GroupByFunctionsUpdater functions) {
            return builds.length == 1 && updateKeyed(record, rowCount, rowIdsAddress, builds[0], map, shards, functions);
        }

        /** Ordinary maps own insertion/growth; a sharded frame supplies its existing shard list. */
        default boolean updateKeyed(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                    FrozenHashJoinBuild build, Map map, @Nullable ObjList<Map> shards,
                                    GroupByFunctionsUpdater functions) {
            return false;
        }
    }
}
