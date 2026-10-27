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

package org.questdb.experimental.codegen;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.NativeMapView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.DirectLongList;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.questdb.experimental.codegen.ResolvedAggregateKernel.Prepared;

import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.ResolvedAggregateKernel.NEED_CAPACITY;
import static org.questdb.experimental.codegen.ResolvedAggregateKernel.prefetchScratch;

/** Slot-local scratch, reused across batches. The caller owns map and frame lifetimes. */
public final class AggregateSlot implements GroupByKernel.Updater {
    private static final boolean PROFILE = Boolean.getBoolean("query.codegen.profile");
    private final Prepared prepared;
    private final long addressBytes;
    private final KernelMemory memory;
    private final long addresses;
    private final long columnBytes;
    private boolean closed;
    private long updatedRows;
    private long fusedInputRows;
    private long fusedSelectedRows;
    private long sharedRows;
    private long finalizedRows;
    private long sortedRows;
    private long limitedRows;
    private long unionTasks;
    private long rankScratch;
    private long rankScratchCapacity;
    private long mapRows;
    private long mapCalls;
    private long newGroups;
    private long partitionRows;
    private long topKRows;
    private long topKMergeRows;
    private final Map.NativeRehash rehash;
    private final long relocationScratch;
    private final ResolvedFilter.Slot filter;
    private final long partitions;
    private final long sharedScratch;
    private boolean partitioned;
    private boolean parallelMerge;
    private int mergePreparationStage;
    private boolean finalizeDeferred;

    AggregateSlot(Prepared prepared, GroupByAllocator allocator) {
        this.prepared = prepared;
        // Factory-owned scratch is shared by successive tasks in this acquired slot.
        if ((prepared.binding.allocates || prepared.localCapacity > 0) && allocator == null) {
            throw new IllegalArgumentException("Allocated aggregate state requires the owning slot allocator");
        }
        memory = prepared.binding.allocates || prepared.mapCode != null ? KernelMemory.reusable(allocator) : null;
        // Cardinality and deferred-merge words precede STATE; fusion adds an accepted-row count.
        columnBytes = (prepared.binding.sourceIndexes.length + 4L + (prepared.fusedFilterCode != null ? 1 : 0)) * 8;
        relocationScratch = columnBytes + (prepared.mapCode != null
                ? prefetchScratch(prepared.probe) + (long) Math.max(1, prepared.binding.prefetchRows) * prepared.probe.prefetchRowBytes() : 0);
        addressBytes = relocationScratch + 128 + (prepared.probe != null && prepared.probe.variable() ? prepared.probe.variableKey().descriptorBytes() : 0) + (prepared.probe != null && prepared.probe.varchar() ? 48 : 0) + (prepared.probe != null && prepared.probe.fixedKey() != null ? prepared.probe.fixedKey().columns().length * 8L : 0)
                + (prepared.localCapacity > 0 ? QuestPartitions.STATE_BYTES : 0)
                + (prepared.sharedMaps != null ? QuestSharedVarchar.SCRATCH_BYTES : 0);
        addresses = Unsafe.malloc(addressBytes, MemoryTag.NATIVE_JIT);
        long sharedBytes = prepared.sharedMaps != null ? QuestSharedVarchar.SCRATCH_BYTES : 0;
        sharedScratch = sharedBytes != 0 ? addresses + addressBytes - sharedBytes : 0;
        partitions = prepared.localCapacity > 0 ? addresses + addressBytes - sharedBytes - QuestPartitions.STATE_BYTES : 0;
        clear();
        try { filter = prepared.filter != null ? prepared.filter.newSlot() : null; }
        catch (Throwable failure) { Unsafe.free(addresses, addressBytes, MemoryTag.NATIVE_JIT); throw failure; }
        if (prepared.mapCode != null) {
            Unsafe.putLong(addresses + columnBytes + 40, 0);
            Unsafe.putLong(addresses + columnBytes + 48, 0);
        }
        rehash = (oldStart, oldCapacity, newStart, newCapacity, entryStride) -> {
            long scratch = addresses + relocationScratch;
            Unsafe.putLong(scratch, oldStart);
            Unsafe.putLong(scratch + 8, newStart);
            Unsafe.putLong(scratch + 16, newCapacity - 1);
            Unsafe.putLong(scratch + 24, entryStride);
            long status = prepared.rehashCode.run(scratch, oldCapacity, 0);
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Map rehash failed: " + status);
        };
        resetStats();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try { if (filter != null) filter.close(); }
            finally { Unsafe.free(addresses, addressBytes, MemoryTag.NATIVE_JIT); }
        }
    }

    @Override
    public int localMapCapacity() { return prepared.localCapacity; }
    @Override
    public boolean hasPartitions() { return partitioned; }

    @Override
    public boolean initSharedMaps(java.util.function.IntFunction<Map> maps) {
        if (prepared.sharedMaps == null) return false;
        prepared.sharedMaps.init(maps);
        return true;
    }

    @Override
    public void finishSharedMap(int shard, Runnable checkpoint) {
        memory.enterCheckpoint(checkpoint);
        try { prepared.sharedMaps.finish(shard, rehash, checkpoint); }
        finally { memory.close(); }
    }

    @Override
    public void clear() {
        if (prepared.sharedMaps != null) prepared.sharedMaps.clear();
        rankScratch = 0;
        rankScratchCapacity = 0;
        partitioned = false;
        parallelMerge = false;
        mergePreparationStage = 0;
        finalizeDeferred = false;
        Unsafe.putLong(addresses + columnBytes + DeferredDistinct.CONTROL, 0);
        Unsafe.putLong(addresses + columnBytes + DeferredDistinct.CONTROL + 8, 0);
        if (partitions != 0) io.questdb.std.Vect.memset(partitions, QuestPartitions.STATE_BYTES, 0);
    }

    @Override
    public void partition(Map source, Runnable checkpoint) {
        if (source.size() == 0) return;
        memory.enterCheckpoint(checkpoint);
        try { flush(source); } finally { memory.close(); }
    }

    private void flush(Map source) {
        if (source.size() == 0) return;
        long descriptor = addresses + relocationScratch + 32;
        source.writeNativeMutationView(descriptor);
        long rows = Unsafe.getLong(descriptor + NativeMapView.MASK) + (prepared.probe.varchar() ? 1 : 2);
        long status = prepared.flushCode.run(descriptor, rows, partitions);
        memory.throwIfFailed();
        if (status != 0) throw new IllegalStateException("Partial-state partition failed: " + status);
        partitioned = true;
        partitionRows += source.size();
        source.clear();
    }

    @Override
    public void mergePartition(Map destination, GroupByKernel.Updater sourceUpdater, int partition, Runnable checkpoint) {
        AggregateSlot source = (AggregateSlot) sourceUpdater;
        long page = Unsafe.getLong(source.partitions + 16L * partition);
        if (page == 0) return;
        writeViews(destination, null);
        memory.enterCheckpoint(checkpoint);
        try {
            while (page != 0) {
                runMutable(prepared.partitionMergeCode, page + 16, Unsafe.getLong(page + 8), destination, null, false);
                page = Unsafe.getLong(page);
            }
        } finally {
            try { publishViews(destination, null); } finally { memory.close(); }
        }
    }

    @Override
    public boolean beginParallelMerge() {
        parallelMerge = prepared.unionCode != null;
        return parallelMerge;
    }

    @Override
    public void endParallelMerge(boolean hasDeferredStates) { parallelMerge = false; finalizeDeferred = hasDeferredStates; }

    @Override
    public int getPendingMergeCount() {
        int count = 0;
        long context = Unsafe.getLong(addresses + columnBytes + DeferredDistinct.CONTROL + 8);
        while (context != 0) { count++; context = Unsafe.getLong(context); }
        return count;
    }

    @Override
    public boolean prepareMergeTasks(ObjList<GroupByKernel.MergeTask> tasks,
                              int parallelism, Runnable checkpoint) {
        int stage = mergePreparationStage++;
        if (stage > 2) return false;
        long context = Unsafe.getLong(addresses + columnBytes + DeferredDistinct.CONTROL + 8);
        boolean present = context != 0;
        while (context != 0) {
            checkpoint.run();
            if (stage == 0) {
                int parts = Integer.highestOneBit(Math.min(DeferredDistinct.PARTS, parallelism));
                tasks.add(new GroupByKernel.MergeTask(context, parts, 0));
            } else {
                boolean serial = Unsafe.getLong(context + 96) != 0;
                int parts = serial ? 1 : 1 << Unsafe.getLong(context + 72);
                if (stage == 2 || (!serial && Unsafe.getLong(context + 56) == 0)) {
                    for (int i = 0; i < parts; i++) tasks.add(new GroupByKernel.MergeTask(context, i, stage));
                }
            }
            context = Unsafe.getLong(context);
        }
        return present && stage < 2;
    }

    @Override
    public void runMergeTask(GroupByKernel.MergeTask task, Runnable checkpoint) {
        CompiledKernel code = switch (task.phase()) {
            case 0 -> prepared.unionPrepareCode;
            case 1 -> prepared.unionZeroCode;
            case 2 -> prepared.unionCode;
            default -> throw new IllegalArgumentException("Unknown compiled merge phase");
        };
        memory.enterCheckpoint(checkpoint);
        try {
            long status = code.run(task.address(), task.index(), 0);
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("DISTINCT union phase " + task.phase() + " failed: " + status);
            if (task.phase() == 2) unionTasks++;
        } finally { memory.close(); }
    }

    @Override
    public void finalizeMap(Map map, Runnable checkpoint) {
        if (parallelMerge || (!prepared.finalizeResults && !finalizeDeferred) || prepared.finalizeCode == null || map.size() == 0) return;
        long descriptor = addresses + relocationScratch;
        if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptor);
        else map.writeNativeProbeView(descriptor);
        memory.enterCheckpoint(checkpoint);
        try {
            long status = prepared.finalizeCode.run(descriptor, map.size(), 0);
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Aggregate finalization failed: " + status);
            finalizedRows += map.size();
        } finally { memory.close(); }
    }

    @Override
    public boolean filter(PageFrameMemoryRecord record, long rowCount, DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
        return filter != null && filter.filter(record, rowCount, rows, breaker);
    }

    long[] filterCounters() {
        long[] counters = filter != null ? filter.counters() : new long[2];
        counters[0] += fusedInputRows;
        counters[1] += fusedSelectedRows;
        return counters;
    }

    long updatedRows() { return updatedRows; }
    long sharedRows() { return sharedRows; }
    long finalizedRows() { return finalizedRows; }
    long limitedRows() { return limitedRows; }
    long unionTasks() { return unionTasks; }
    long sortedRows() { return sortedRows; }

    @Override
    public long getNativeMapRows() { return mapRows; }
    long partitionRows() { return partitionRows; }

    long[] profileCounters() {
        return new long[]{mapCalls, newGroups, prepared.mapCode == null ? 0 : Unsafe.getLong(addresses + columnBytes + 40),
                prepared.mapCode == null ? 0 : Unsafe.getLong(addresses + columnBytes + 48)};
    }

    long[] topKCounters() { return new long[]{topKRows, topKMergeRows}; }

    @Override
    public boolean selectSortedRows(io.questdb.std.DirectLongLongSortedList sorted, DirectLongList rows,
                             long skip, long take, Runnable checkpoint) {
        if (prepared.rowLimitCode == null || sorted.getNativeAddress() == 0) return false;
        if (closed || prepared.closed) throw new IllegalStateException("Aggregate slot is closed");
        if (skip < 0 || take < 0) throw new IllegalArgumentException("Positive LIMIT bounds required");
        rows.clear();
        long count = Math.min(take, Math.max(0, sorted.size() - skip));
        if (count == 0) return true;
        rows.setCapacity(count);
        long state = addresses + relocationScratch + 64;
        Unsafe.putLong(state, rows.getAddress());
        Unsafe.putLong(state + 8, 0);
        Unsafe.putLong(state + 16, skip);
        Unsafe.putLong(state + 24, count);
        memory.enterCheckpoint(checkpoint);
        try {
            long status = prepared.rowLimitCode.run(sorted.getNativeAddress(), sorted.size(), state);
            memory.throwIfFailed();
            if (status != 0 || Unsafe.getLong(state + 8) != count) throw new IllegalStateException("Sorted LIMIT kernel failed");
            rows.setPos(count);
            limitedRows += count;
            return true;
        } finally { memory.close(); }
    }

    @Override
    public boolean selectRows(Map owner, ObjList<Map> shards, DirectLongList rows, long skip, long take, Runnable checkpoint) {
        if (prepared.rowLimitCode == null) return false;
        if (closed || prepared.closed) throw new IllegalStateException("Aggregate slot is closed");
        if (skip < 0 || take < 0) throw new IllegalArgumentException("Positive LIMIT bounds required");
        rows.clear();
        if (take == 0) return true;
        long total = 0;
        int mapCount = shards == null ? 1 : shards.size();
        for (int i = 0; i < mapCount; i++) total = Math.addExact(total, (shards == null ? owner : shards.getQuick(i)).size());
        long count = Math.min(take, Math.max(0, total - skip));
        if (count == 0) return true;
        rows.setCapacity(count);
        long descriptor = addresses + relocationScratch, state = descriptor + NativeMapView.MUTATION_BYTES;
        Unsafe.putLong(state, rows.getAddress());
        Unsafe.putLong(state + 8, 0);
        Unsafe.putLong(state + 16, skip);
        Unsafe.putLong(state + 24, count);
        memory.enterCheckpoint(checkpoint);
        try {
            for (int i = 0; i < mapCount && Unsafe.getLong(state + 8) < count; i++) {
                Map map = shards == null ? owner : shards.getQuick(i);
                long remainingSkip = Unsafe.getLong(state + 16);
                if (remainingSkip >= map.size()) {
                    Unsafe.putLong(state + 16, remainingSkip - map.size());
                    continue;
                }
                if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptor);
                else map.writeNativeProbeView(descriptor);
                Unsafe.putLong(state + 32, shards == null ? 0 : (long) i << 48);
                long status = prepared.rowLimitCode.run(descriptor, 0, state);
                memory.throwIfFailed();
                if (status != 0) throw new IllegalStateException("LIMIT kernel failed: " + status);
            }
            if (Unsafe.getLong(state + 8) != count) throw new IllegalStateException("LIMIT cardinality changed");
            rows.setPos(count);
            limitedRows += count;
            return true;
        } finally { memory.close(); }
    }

    @Override
    public boolean sortLong(Map owner, ObjList<Map> shards, DirectLongList entries, DirectLongList scratch,
                     Function function, boolean ascending, long maxEntries, Runnable checkpoint) {
        if (prepared.longSortCode == null) return false;
        if (closed || prepared.closed) throw new IllegalStateException("Aggregate slot is closed");
        long offset = prepared.resultOffset(function);
        if (offset < 0) return false;
        long count = 0;
        int mapCount = shards == null ? 1 : shards.size();
        for (int i = 0; i < mapCount; i++) count = Math.addExact(count, (shards == null ? owner : shards.getQuick(i)).size());
        if (count > maxEntries || count > (Long.MAX_VALUE - QuestRadixSort.HISTOGRAM_BYTES) / 16) {
            io.questdb.griffin.engine.orderby.SortKeyEncoder.throwSortHeapOverflow(maxEntries * 16);
        }
        entries.clear();
        if (count == 0) return true;
        entries.setCapacity(count * 2);
        scratch.setCapacity(count * 2 + QuestRadixSort.HISTOGRAM_BYTES / 8);
        long descriptor = addresses + relocationScratch, state = descriptor + NativeMapView.MUTATION_BYTES;
        Unsafe.putLong(state, entries.getAddress());
        Unsafe.putLong(state + 8, 0);
        Unsafe.putLong(state + 16, 0);
        Unsafe.putLong(state + 24, ascending ? 0 : 1);
        Unsafe.putLong(state + 32, offset + prepared.probe.keyBytes());
        Unsafe.putLong(state + 40, scratch.getAddress());
        Unsafe.putLong(state + 48, scratch.getAddress() + count * 16);
        memory.enterCheckpoint(checkpoint);
        try {
            for (int i = 0; i < mapCount; i++) {
                Map map = shards == null ? owner : shards.getQuick(i);
                if (map.size() == 0) continue;
                if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptor);
                else map.writeNativeProbeView(descriptor);
                runLongSort(descriptor, state);
            }
            if (Unsafe.getLong(state + 8) != count) throw new IllegalStateException("Map sort cardinality changed");
            Unsafe.putLong(state + 16, 1);
            runLongSort(0, state);
            entries.setPos(count * 2);
            sortedRows += count;
            return true;
        } finally { memory.close(); }
    }

    private void runLongSort(long descriptor, long state) {
        long status = prepared.longSortCode.run(descriptor, 0, state);
        memory.throwIfFailed();
        if (status != 0) throw new IllegalStateException("Full-sort kernel failed: " + status);
    }

    @Override
    public boolean longTopK(Map map, DirectLongLongSortedList list, Function function, Runnable checkpoint) {
        if (closed || prepared.closed) throw new IllegalStateException("Aggregate slot is closed");
        if (!canRunTopK(list)) return false;
        long offset = prepared.resultOffset(function);
        if (offset < 0) return false;
        if (map.size() == 0) return true;
        long descriptor = addresses + relocationScratch;
        if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptor);
        else map.writeNativeProbeView(descriptor);
        runTopK(descriptor, 0, list, 0, offset + prepared.probe.keyBytes(), checkpoint);
        topKRows += map.size();
        return true;
    }

    @Override
    public boolean longTopK(ObjList<Map> maps, DirectLongLongSortedList list, Function function, Runnable checkpoint) {
        if (maps.size() > GroupByMapFragment.NUM_SHARDS || list.getCapacity() <= 64) return false;
        if (!canRunTopK(list)) return false;
        long offset = prepared.resultOffset(function);
        if (offset < 0) return false;
        long descriptors = addresses + columnBytes + prepared.probe.controlBytes();
        long rows = 0;
        for (int i = 0; i < maps.size(); i++) {
            Map map = maps.getQuick(i);
            if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptors + i * 64L);
            else map.writeNativeProbeView(descriptors + i * 64L);
            rows += map.size();
        }
        if (rows == 0) return true;
        runTopK(descriptors, maps.size(), list, 2, offset + prepared.probe.keyBytes(), checkpoint);
        topKRows += rows;
        return true;
    }

    @Override
    public boolean mergeLongTopK(DirectLongLongSortedList destination, DirectLongLongSortedList source, Runnable checkpoint) {
        if (closed || prepared.closed) throw new IllegalStateException("Aggregate slot is closed");
        if (!canRunTopK(destination) || source.getNativeAddress() == 0) return false;
        if (destination == source) throw new IllegalArgumentException("Top-K source aliases destination");
        runTopK(source.getNativeAddress(), source.size(), destination, 1, 0, checkpoint);
        topKMergeRows += source.size();
        return true;
    }

    private boolean canRunTopK(DirectLongLongSortedList list) {
        return prepared.topKCode != null && list.getNativeAddress() != 0
                && (prepared.topKOrder == -1 || prepared.topKOrder == list.getOrder())
                && (prepared.topKCapacityClass == -1
                || prepared.topKCapacityClass == QuestLongTopK.capacityClass(list.getCapacity()));
    }

    private void runTopK(long source, long rows, DirectLongLongSortedList destination, long kind, long offset, Runnable checkpoint) {
        if (closed || prepared.closed) throw new IllegalStateException("Aggregate slot is closed");
        long state = addresses + relocationScratch + 64;
        Unsafe.putLong(state, destination.getNativeAddress());
        Unsafe.putLong(state + 8, destination.size());
        Unsafe.putLong(state + 16, destination.getCapacity());
        Unsafe.putLong(state + 24, kind);
        Unsafe.putLong(state + 32, destination.getOrder());
        Unsafe.putLong(state + 40, offset);
        Unsafe.putLong(state + 48, rankScratch);
        Unsafe.putLong(state + 56, rankScratchCapacity);
        memory.enterCheckpoint(checkpoint);
        try {
            long status = prepared.topKCode.run(source, rows, state);
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Top-K kernel failed: " + status);
        } finally {
            rankScratch = Unsafe.getLong(state + 48);
            rankScratchCapacity = Unsafe.getLong(state + 56);
            try { destination.publishNativeSize((int) Unsafe.getLong(state + 8)); }
            finally { memory.close(); }
        }
    }

    @Override
    public boolean updateSharded(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                          ObjList<Map> shards, RecordSink mapSink, GroupByFunctionsUpdater functions,
                          SqlExecutionCircuitBreaker circuitBreaker) {
        QuestMapProbe probe = prepared.probe;
        if (probe != null && probe.mutable()) {
            return updateMaps(record, rowCount, rowIdsAddress, null, shards, 0, circuitBreaker);
        }
        if (probe == null || !canUpdate(record) || record.getPageAddress(probe.column()) == 0) return false;
        if (shards.size() != GroupByMapFragment.NUM_SHARDS) return false;
        final long state = addresses + columnBytes;
        Unsafe.putLong(state + 8, rowIdsAddress);
        Unsafe.putLong(state + 16, record.getPageAddress(probe.column()));
        for (int i = 0; i < prepared.binding.sourceIndexes.length; i++) {
            Unsafe.putLong(addresses + (i + 1L) * 8, record.getPageAddress(prepared.binding.sourceIndexes[i]));
        }
        for (int i = 0; i < shards.size(); i++) {
            shards.getQuick(i).writeNativeProbeView(state + QuestMapProbe.CONTROL_BYTES + (long) i * QuestMapProbe.VIEW_BYTES);
        }
        long position = 0;
        final Map lookup = shards.getQuick(0);
        memory.enter(circuitBreaker);
        try {
            while (position < rowCount) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                Unsafe.putLong(state, position);
                long status = prepared.mapCode.run(addresses, rowCount, state);
                if (PROFILE) mapCalls++;
                memory.throwIfFailed();
                if (status != 0 && status != QuestMapProbe.NEED_KEY) {
                    throw new IllegalStateException("Map/update kernel failed: " + status);
                }
                long end = status == 0 ? rowCount : Unsafe.getLong(state + 32);
                updatedRows += end - position;
                mapRows += end - position;
                position = end;
                if (status == 0) break;
                if (PROFILE) newGroups++;
                // Resume before the missing row: only the ordinary map owns insertion,
                // growth, size/free counters and tracking. No aggregate update is replayed.
                long row = rowIdsAddress == 0 ? position : Unsafe.getLong(rowIdsAddress + 8 * position);
                record.setRowIndex(row);
                MapKey key = lookup.withKey();
                mapSink.copy(record, key);
                key.commit();
                long hash = key.hash();
                int shardIndex = (int) (hash >>> GroupByMapFragment.NUM_SHARDS_SHR);
                if (shardIndex != Unsafe.getLong(state + 24)) throw new IllegalStateException("Map hash ABI mismatch");
                Map shard = shards.getQuick(shardIndex);
                if (shard != lookup) {
                    MapKey target = shard.withKey();
                    target.copyFrom(key);
                    key = target;
                }
                var value = key.createValue(hash);
                if (!value.isNew()) throw new IllegalStateException("Native probe missed an existing key");
                functions.updateEmpty(value);
                // Staging a key can also move OrderedMap's shard-zero heap.
                lookup.writeNativeProbeView(state + QuestMapProbe.CONTROL_BYTES);
                shard.writeNativeProbeView(state + QuestMapProbe.CONTROL_BYTES + (long) shardIndex * QuestMapProbe.VIEW_BYTES);
            }
        } finally {
            memory.close();
        }
        return true;
    }

    long fusedInputRows() { return fusedInputRows; }

    @Override
    public boolean hasFusedFilter() { return prepared.fusedFilterCode != null; }

    @Override
    public long updateFiltered(PageFrameMemoryRecord record, long rowCount, Map map, ObjList<Map> shards,
                                  int batchSize, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!hasFusedFilter() || !canUpdate(record) || !prepareKeyColumns(record)) return -1;
        long predicates = filter.prepare(record);
        if (predicates == 0) return -1;
        long state = addresses + columnBytes;
        Unsafe.putLong(state + 8, predicates);
        long before = fusedSelectedRows;
        runMap(record, rowCount, map, shards, batchSize, circuitBreaker, prepared.fusedFilterCode);
        fusedInputRows += rowCount;
        return fusedSelectedRows - before;
    }

    @Override
    public boolean updateMap(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress, Map map, int batchSize,
                      SqlExecutionCircuitBreaker circuitBreaker) {
        return prepared.probe != null && prepared.probe.mutable()
                && updateMaps(record, rowCount, rowIdsAddress, map, null, batchSize, circuitBreaker);
    }

    private boolean updateMaps(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                               Map map, ObjList<Map> shards, int batchSize, SqlExecutionCircuitBreaker circuitBreaker) {
        QuestMapProbe probe = prepared.probe;
        if (!canUpdate(record) || !prepareKeyColumns(record)) return false;
        long state = addresses + columnBytes;
        Unsafe.putLong(state + 8, rowIdsAddress);
        if (prepared.sharedMaps != null && prepared.sharedMaps.isActive()
                && Unsafe.getLong(Unsafe.getLong(state + 16) + 40) != 0) {
            memory.enter(circuitBreaker);
            try {
                Unsafe.putLong(addresses, batchSize);
                for (long position = 0; position < rowCount;) {
                    long end = Math.min(rowCount, position + SharedVarcharMaps.QUANTUM);
                    if (map != null && map.size() <= SharedVarcharMaps.LOCAL_GROUPS && prepared.sharedMaps.prefersLocal()) {
                        writeViews(map, null);
                        try { runMutable(prepared.mapCode, addresses, end, map, null, true, position); }
                        finally { publishViews(map, null); }
                    } else {
                        prepared.sharedMaps.scan(prepared.sharedCode, Unsafe.getLong(state + 16), rowIdsAddress,
                                position, end, sharedScratch, rehash, circuitBreaker);
                        updatedRows += end - position;
                        sharedRows += end - position;
                        mapRows += end - position;
                    }
                    position = end;
                }
                memory.throwIfFailed();
            } finally { memory.close(); }
            return true;
        }
        runMap(record, rowCount, map, shards, batchSize, circuitBreaker, prepared.mapCode);
        return true;
    }

    private void runMap(PageFrameMemoryRecord record, long rowCount, Map map, ObjList<Map> shards,
                        int batchSize, SqlExecutionCircuitBreaker circuitBreaker, CompiledKernel kernel) {
        long state = addresses + columnBytes;
        QuestMapProbe probe = prepared.probe;
        for (int i = 0; i < prepared.binding.sourceIndexes.length; i++) {
            Unsafe.putLong(addresses + (i + 1L) * 8, record.getPageAddress(prepared.binding.sourceIndexes[i]));
        }
        Unsafe.putLong(addresses, batchSize);
        long groupsBefore = PROFILE ? groupCount(map, shards) + partitionRows : 0;
        writeViews(map, shards);
        if (probe.variable()) ((io.questdb.cairo.map.OrderedMap) (shards == null ? map : shards.getQuick(0)))
                .writeNativeKeyStagingView(state + 64, 64);
        memory.enter(circuitBreaker);
        try {
            runMutable(kernel, addresses, rowCount, map, shards, true);
        } finally {
            try {
                publishViews(map, shards);
                if (PROFILE) newGroups += groupCount(map, shards) + partitionRows - groupsBefore;
            } finally { memory.close(); }
        }
    }

    private boolean prepareKeyColumns(PageFrameMemoryRecord record) {
        var probe = prepared.probe;
        long state = addresses + columnBytes;
        if (probe.variable()) {
            long descriptor = addresses + relocationScratch + 128;
            if (!probe.variableKey().prepare(record, descriptor)) return false;
            Unsafe.putLong(state + 16, descriptor);
        } else if (probe.varchar()) {
            long descriptor = addresses + relocationScratch + 128;
            if (!record.writeNativeVarcharView(probe.column(), descriptor)) return false;
            Unsafe.putLong(state + 16, descriptor);
        } else if (probe.fixedKey() == null) {
            long page = record.getPageAddress(probe.column());
            if (page == 0) return false;
            Unsafe.putLong(state + 16, page);
        } else {
            long pointers = addresses + relocationScratch + 128;
            int[] keys = probe.fixedKey().columns();
            for (int i = 0; i < keys.length; i++) {
                long page = record.getPageAddress(keys[i]);
                if (page == 0) return false;
                Unsafe.putLong(pointers + i * 8L, page);
            }
            Unsafe.putLong(state + 16, pointers);
        }
        return true;
    }

    private static long groupCount(Map map, ObjList<Map> shards) {
        if (shards == null) return map.size();
        long count = 0;
        for (int i = 0; i < shards.size(); i++) count += shards.getQuick(i).size();
        return count;
    }

    @Override
    public boolean merge(Map destination, Map source, Runnable checkpoint) {
        return merge(destination, source, null, checkpoint);
    }

    @Override
    public boolean shard(Map source, ObjList<Map> shards, Runnable checkpoint) {
        return merge(null, source, shards, checkpoint);
    }

    boolean merge(Map destination, Map source, ObjList<Map> shards, Runnable checkpoint) {
        if (closed) throw new IllegalStateException("Aggregate slot is closed");
        if (prepared.mergeCode == null) return false;
        if (source.size() == 0) return true;
        // Separate from the rehash arguments: a destination growth must not clobber this source.
        long descriptor = addresses + relocationScratch + 32;
        source.writeNativeMutationView(descriptor);
        long capacity = Unsafe.getLong(descriptor + NativeMapView.MASK) + 1;
        long rows;
        if (prepared.probe.varchar()) {
            rows = capacity;
        } else if (prepared.probe.orderedNative()) {
            long size = Unsafe.getLong(descriptor + NativeMapView.SIZE);
            Unsafe.putLong(descriptor + 16, capacity);
            Unsafe.putLong(descriptor + 32, size);
            Unsafe.putLong(descriptor + 40, shards == null ? 1 : 0);
            rows = shards == null ? capacity : size;
        } else {
            Unsafe.putLong(descriptor + 8, capacity);
            Unsafe.putLong(descriptor + 16, Unsafe.getLong(descriptor + NativeMapView.STRIDE));
            Unsafe.putLong(descriptor + 24, Unsafe.getLong(descriptor + NativeMapView.ZERO_ENTRY));
            Unsafe.putLong(descriptor + 32, Unsafe.getLong(descriptor + NativeMapView.HAS_ZERO));
            Unsafe.putLong(descriptor + 40, shards == null ? 1 : 0);
            rows = capacity + 1;
        }
        writeViews(destination, shards);
        memory.enterCheckpoint(checkpoint);
        try {
            runMutable(prepared.mergeCode, descriptor, rows, destination, shards, false);
        } finally {
            try { publishViews(destination, shards); } finally { memory.close(); }
        }
        return true;
    }

    private void writeViews(Map map, ObjList<Map> shards) {
        long state = addresses + columnBytes;
        Unsafe.putLong(state + DeferredDistinct.CONTROL, parallelMerge && shards == null ? 1 : 0);
        Unsafe.putLong(state + 56, shards == null ? 0 : GroupByMapFragment.NUM_SHARDS - 1);
        int count = shards == null ? 1 : shards.size();
        for (int i = 0; i < count; i++) {
            (shards == null ? map : shards.getQuick(i)).writeNativeMutationView(view(i));
        }
    }

    private long view(int index) {
        return addresses + columnBytes + prepared.probe.controlBytes() + (long) index * prepared.probe.viewBytes();
    }

    private void publishViews(Map map, ObjList<Map> shards) {
        int count = shards == null ? 1 : shards.size();
        for (int i = 0; i < count; i++) {
            (shards == null ? map : shards.getQuick(i)).publishNativeMutationView(view(i));
        }
    }

    private void runMutable(CompiledKernel kernel, long input, long rows, Map map, ObjList<Map> shards, boolean update) {
        runMutable(kernel, input, rows, map, shards, update, 0);
    }

    private void runMutable(CompiledKernel kernel, long input, long rows, Map map, ObjList<Map> shards, boolean update, long begin) {
        long state = addresses + columnBytes;
        long position = begin;
        if (prepared.probe.varchar()) Unsafe.putLong(state + 80, -1);
        if (prepared.probe.variable()) Unsafe.putLong(state + 96, 0);
        while (position < rows) {
            Unsafe.putLong(state, position);
            long status = kernel.run(input, rows, state);
            if (PROFILE && update) mapCalls++;
            memory.throwIfFailed();
            if (status != 0 && status != QuestMapProbe.NEED_KEY && status != NEED_CAPACITY && status != QuestVarchar.NEED_COPY && status != QuestVariableKey.NEED_STAGING && status != QuestPartitions.NEED_FLUSH) {
                throw new IllegalStateException("Map mutation failed: " + status);
            }
            long end = status == 0 ? rows : Unsafe.getLong(state + 32);
            if (update) {
                long accepted = kernel == prepared.fusedFilterCode
                        ? Unsafe.getLong(state + ResolvedAggregateKernel.FILTERED_ROWS) : end - position;
                updatedRows += accepted;
                mapRows += accepted;
                if (kernel == prepared.fusedFilterCode) fusedSelectedRows += accepted;
            }
            position = end;
            if (status == 0) return;
            if (status == QuestPartitions.NEED_FLUSH) {
                map.publishNativeMutationView(view(0));
                flush(map);
                writeViews(map, null);
                continue;
            }
            if (status == QuestVariableKey.NEED_STAGING) {
                ((io.questdb.cairo.map.OrderedMap) (shards == null ? map : shards.getQuick(0)))
                        .writeNativeKeyStagingView(state + 64, Unsafe.getLong(state + 88));
                continue;
            }
            int shard = (int) Unsafe.getLong(state + 24);
            Map target = shards == null ? map : shards.getQuick(shard);
            target.publishNativeMutationView(view(shard));
            try {
                if (status == QuestVarchar.NEED_COPY) {
                    long copied = ((io.questdb.cairo.map.UnorderedVarcharMap) target).copyNativeKey(
                            Unsafe.getLong(state + 64), (int) Unsafe.getLong(state + 72));
                    Unsafe.putLong(state + 64, copied);
                    Unsafe.putLong(state + 80, position);
                } else if (status == NEED_CAPACITY) {
                    target.reserveNative(Math.min(rows - position, Unsafe.getLong(addresses)), rehash);
                } else if (prepared.probe.variable()) {
                    ((io.questdb.cairo.map.OrderedMap) target).growNative(Unsafe.getLong(state + 88), rehash);
                } else {
                    target.growNative(rehash);
                }
            } finally {
                // OrderedMap may grow its heap before offset-table growth fails.
                target.writeNativeMutationView(view(shard));
            }
        }
    }

    @Override
    public long getCardinalityStat() {
        if (closed) throw new IllegalStateException("Aggregate slot is closed");
        return Unsafe.getLong(addresses + (prepared.binding.sourceIndexes.length + 1L) * 8);
    }

    @Override
    public void resetStats() {
        if (closed) throw new IllegalStateException("Aggregate slot is closed");
        Unsafe.putLong(addresses + (prepared.binding.sourceIndexes.length + 1L) * 8, 0);
    }

    public boolean canUpdate(PageFrameMemoryRecord record) {
        if (closed) throw new IllegalStateException("Aggregate slot is closed");
        if (record instanceof PageFrameFilteredMemoryRecord) return false;
        for (int index : prepared.binding.sourceIndexes) {
            if (record.getPageAddress(index) == 0) return false;
        }
        return true;
    }

    public boolean update(PageFrameMemoryRecord record, long baseValueAddress, long batchAddress, long rows) {
        return update(record, baseValueAddress, batchAddress, rows, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    @Override
    public boolean update(PageFrameMemoryRecord record, long baseValueAddress, long batchAddress, long rows,
                          SqlExecutionCircuitBreaker circuitBreaker) {
        circuitBreaker.statefulThrowExceptionIfTripped();
        if (closed) {
            throw new IllegalStateException("Aggregate slot is closed");
        }
        // Late materialization mixes compacted and original row coordinates. The
        // normal factory routes it through scalar accessors; never infer an ABI here.
        if (record instanceof PageFrameFilteredMemoryRecord) {
            return false;
        }
        Unsafe.putLong(addresses, batchAddress);
        int[] indexes = prepared.binding.sourceIndexes;
        for (int i = 0; i < indexes.length; i++) {
            long address = record.getPageAddress(indexes[i]);
            // Zero also denotes lazy type conversion, not just a column top. Let the
            // original record accessors handle the whole batch before mutating state.
            if (address == 0) {
                return false;
            }
            Unsafe.putLong(addresses + (i + 1L) * 8, address);
        }
        long status;
        if (memory == null) {
            status = prepared.code.run(addresses, rows, baseValueAddress);
        } else {
            memory.enter(circuitBreaker);
            try {
                status = prepared.code.run(addresses, rows, baseValueAddress);
                memory.throwIfFailed();
            } finally {
                memory.close();
            }
        }
        if (status != 0) {
            throw new IllegalStateException("Aggregate kernel failed: " + status);
        }
        updatedRows += rows;
        return true;
    }
}
