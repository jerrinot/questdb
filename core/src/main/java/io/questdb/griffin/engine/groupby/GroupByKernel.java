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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongList;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/** Query-owned aggregate code. Slots borrow it until the existing frame sequence drains. */
public interface GroupByKernel extends QuietCloseable {
    /** Borrowed output readers for finalized state; the original list retains function ownership. */
    default ObjList<Function> outputFunctions(ObjList<Function> functions) { return functions; }

    /** Generated state may allocate even when the resolved Java functions do not. */
    default boolean needsWorkerAllocators() { return false; }

    /** Planning-only borrow. Unsupported predicates leave the ordinary filter intact. */
    default void prepareFilter(ColumnTypes inputs, Function filter) {
    }

    default boolean hasCompiledFilter() {
        return false;
    }

    default boolean prepareRowLimit() { return false; }

    default boolean prepareSortedRowLimit() { return false; }

    default boolean prepareLongSort(Function function) { return false; }

    default boolean supportsLongTopK(Function function) { return false; }

    default void prepareLongTopK(Function function) {
    }

    default void prepareLongTopK(Function function, boolean ascending, int limit) {
        prepareLongTopK(function);
    }

    CharSequence getName();

    /** The allocator is borrowed from the owning atom and must never be closed here. */
    Updater newUpdater(GroupByAllocator allocator);

    /** Opaque work descriptor; its query allocator outlives the post-merge phase. */
    record MergeTask(long address, long index, int phase) { }

    interface Updater extends QuietCloseable {
        /**
         * Opt into shared final maps before frame dispatch. The supplier opens a map under
         * the existing query tracker; callers serialize initialization of each shard.
         * Maps retain their ordinary owner. False leaves worker-local aggregation intact.
         */
        default boolean initSharedMaps(java.util.function.IntFunction<Map> maps) { return false; }

        /** All scans have joined. Publish native state before merging ordinary fallback rows. */
        default void finishSharedMap(int shard, Runnable checkpoint) { throw new UnsupportedOperationException(); }

        /** All scan/sharding work has drained. Opt into a second phase for indivisible inner states. */
        default boolean beginParallelMerge() { return false; }

        /** Number of independently mergeable states after outer-map tasks drain. */
        default int getPendingMergeCount() { return 0; }

        /** Append one dependency stage. Return true when another stage follows after this one drains.
         * parallelism is the available worker budget per independent state, not an extra pool.
         */
        default boolean prepareMergeTasks(ObjList<MergeTask> tasks, int parallelism, Runnable checkpoint) { return false; }

        /** Runs in an acquired existing worker slot, with that slot's allocator and scratch. */
        default void runMergeTask(MergeTask task, Runnable checkpoint) { throw new UnsupportedOperationException(); }

        /** All inner-state tasks have drained; ordinary map finalization is now safe. */
        default void endParallelMerge(boolean hasDeferredStates) { }

        /** Called exactly once for each final map, after every partial state has merged. */
        default void finalizeMap(Map map, Runnable checkpoint) { }

        /** The prepared consumer needs only a fixed ordered prefix of complete groups. */
        default boolean hasPartitionTopK() { return false; }

        /** Merge complete disjoint groups, retaining the prepared ordered prefix in destination.
         * Working is owned by this acquired slot and may be reused; no row address may escape it.
         */
        default void mergeTopKPartition(Map working, Map destination, ObjList<Updater> sources,
                                       int partition, Runnable checkpoint) {
            throw new UnsupportedOperationException();
        }

        /** Nonzero opts into bounded local preaggregation and the existing partition merge. */
        default int localMapCapacity() { return 0; }

        /** Called after all scan tasks drain, before choosing the ordinary merge path. */
        default boolean hasPartitions() { return false; }

        /** Append remaining local state; storage belongs to the query's slot allocator. */
        default void partition(Map source, Runnable checkpoint) { throw new UnsupportedOperationException(); }

        /** Source is read-only after scan completion; destination belongs to this acquired slot. */
        default void mergePartition(Map destination, Updater source, int partition, Runnable checkpoint) {
            throw new UnsupportedOperationException();
        }

        /** All tasks have drained. Forget borrowed allocation addresses before their owner clears. */
        default void clear() { }

        /** Whether a generated filter can feed aggregation without a frame selection vector. */
        default boolean hasFusedFilter() { return false; }

        /** Return accepted rows, or -1 to decline before aggregate mutation.
         * Input, selection scratch and acquired maps are borrowed for this frame.
         */
        default long updateFiltered(PageFrameMemoryRecord record, long rowCount, @Nullable Map map,
                                    @Nullable ObjList<Map> shards, int batchSize, DirectLongList rows,
                                    SqlExecutionCircuitBreaker circuitBreaker) { return -1; }

        /** Fill the existing selection vector; false is permitted only before changing it. */
        default boolean filter(PageFrameMemoryRecord record, long rowCount, DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
            return false;
        }

        /** Collect all finalized rows and sort the existing encoded-sort pair buffer. */
        default boolean selectRows(Map owner, @Nullable ObjList<Map> shards, DirectLongList rows,
                                   long skip, long take, Runnable checkpoint) { return false; }

        /** Borrow finalized sorted pairs; copy only the requested row-ID window. */
        default boolean selectSortedRows(DirectLongLongSortedList sorted, DirectLongList rows,
                                         long skip, long take, Runnable checkpoint) { return false; }

        default boolean sortLong(Map owner, @Nullable ObjList<Map> shards, DirectLongList entries,
                                 DirectLongList scratch, Function function, boolean ascending,
                                 long maxEntries, Runnable checkpoint) { return false; }

        /** Finalized map and list are exclusively borrowed in the acquired slot. */
        default boolean longTopK(Map map, DirectLongLongSortedList list, Function function, Runnable checkpoint) {
            return false;
        }

        /** Borrow all finalized shards for one bounded collection/final-sort boundary. */
        default boolean longTopK(ObjList<Map> maps, DirectLongLongSortedList list, Function function, Runnable checkpoint) {
            return false;
        }

        /** Merge bounded worker results after all collection tasks have drained. */
        default boolean mergeLongTopK(DirectLongLongSortedList destination, DirectLongLongSortedList source, Runnable checkpoint) {
            return false;
        }

        default boolean updateMap(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress, Map map, int batchSize,
                                  SqlExecutionCircuitBreaker circuitBreaker) {
            return false;
        }

        /** Borrow source state; source allocators outlive the final result, including adopted sets. */
        default boolean merge(Map destination, Map source, Runnable checkpoint) {
            return false;
        }

        default boolean shard(Map source, ObjList<Map> shards, Runnable checkpoint) {
            return false;
        }

        /** Optional fused lookup/update path. False is permitted only before any mutation. */
        default boolean updateSharded(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                      ObjList<Map> shards, RecordSink mapSink, GroupByFunctionsUpdater functions,
                                      SqlExecutionCircuitBreaker circuitBreaker) {
            return false;
        }

        default long getNativeMapRows() {
            return 0;
        }

        /** New distinct values since resetStats, additional to the ordinary functions' stats. */
        default long getCardinalityStat() {
            return 0;
        }

        default void resetStats() {
        }

        /**
         * Runs in an acquired reducer slot, with its frame and map pinned. Entries use the
         * Map packed-batch ABI and the map has already initialized new values with setEmpty.
         * Return false only before any mutation; the caller then applies the Java batch once.
         * A failure after mutation must propagate, never request replay or fallback.
         * No frame, record, circuit breaker or batch address may be retained after returning.
         */
        boolean update(
                PageFrameMemoryRecord record,
                long baseValueAddress,
                long batchAddress,
                long rowCount,
                SqlExecutionCircuitBreaker circuitBreaker
        );
    }
}
