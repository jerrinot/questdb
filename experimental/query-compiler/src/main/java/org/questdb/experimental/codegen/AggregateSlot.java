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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.NativeMapView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.DirectLongList;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.questdb.experimental.codegen.ResolvedAggregateKernel.Prepared;

import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.MapKernelState.*;
import static org.questdb.experimental.codegen.ResolvedAggregateKernel.prefetchScratch;

/** Slot-local scratch, reused across batches. The caller owns map and frame lifetimes. */
public final class AggregateSlot implements GroupByKernel.Updater {
    private final ResolvedVarchar.Slot argumentHelpers;
    private final ResolvedVarchar.Slot keyHelpers;
    // 128KiB of row IDs bounds the stage independently of frame/task scheduling.
    private static final int FILTER_STAGE_ROWS = 16 * 1024;
    // Rehash arguments, map views and sort/selection controls reuse this region by phase.
    private static final int PHASE_SCRATCH_BYTES = Math.max(128, GroupAdmission.SCRATCH_BYTES);
    private static final boolean PROFILE = Boolean.getBoolean("query.codegen.profile");
    private final Prepared prepared;
    private final long addressBytes;
    private final KernelMemory memory;
    private final long addresses;
    private final long stateAddress;
    ResolvedMapAggregate.Slot mapConsumer;
    private boolean closed;
    private long updatedRows;
    private long fusedInputRows;
    private long fusedSelectedRows;
    private long admissionInputRows;
    private long admissionSelectedRows;
    private long sharedRows;
    private long finalizedRows;
    private int sampledGroupLowerBound;
    private long sampledGroupBytes;
    private long sortedRows;
    private long limitedRows;
    private long unionTasks;
    private long keyFilterAddress;
    private long rankScratch;
    private long partitionTopK;
    private long partitionSketch;
    private long mergedSketch;
    private long estimatedGroups;
    private long splitStates;
    private long splitInputs;
    private final io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLogDenseRepresentation estimator =
            new io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLogDenseRepresentation(QuestPartitionSketch.PRECISION);
    private long rankScratchCapacity;
    private long mapRows;
    private long mapCalls;
    private long newGroups;
    private long partitionRows;
    private long inputPartitionRows;
    private long inputPartitionRemaining;
    private long inputPartitionWindow;
    private long localRows;
    private final DictionaryAggregate.Slot dictionary;
    private long topKRows;
    private long topKMergeRows;
    private final Map.NativeRehash rehash;
    private final long scratchAddress;
    private final long keyAddress;
    private final ResolvedFilter.Slot filter;
    private final long partitions;
    private final long inputPartitions;
    private final long sharedScratch;
    private boolean partitioned;
    private boolean parallelMerge;
    private int mergePreparationStage;
    private boolean finalizeDeferred;
    private FastGroupByAllocator partitionAllocator;
    private KernelMemory partitionMemory;

    AggregateSlot(Prepared prepared, GroupByAllocator allocator) {
        this.prepared = prepared;
        // Factory-owned scratch is shared by successive tasks in this acquired slot.
        if ((prepared.binding.allocates || prepared.localCapacity > 0) && allocator == null) {
            throw new IllegalArgumentException("Allocated aggregate state requires the owning slot allocator");
        }
        memory = prepared.binding.allocates || prepared.mapCode != null ? KernelMemory.reusable(allocator) : null;
        // Standalone kernels borrow the caller's arena. SQL execution binds a page arena below.
        partitionMemory = KernelMemory.reusable(allocator);
        // Cardinality and deferred-merge words precede STATE; fusion adds an accepted-row count.
        long columnBytes = prepared.binding.inputs.bytes() + 32 + (prepared.fusedFilterCode != null ? 8 : 0);
        long scratchOffset = columnBytes + (prepared.mapCode != null
                ? prefetchScratch(prepared.probe) + (long) Math.max(1, prepared.binding.prefetchRows) * prepared.probe.prefetchRowBytes() : 0);
        long keyBytes = 0;
        if (prepared.probe != null) {
            if (prepared.probe.variable()) keyBytes = prepared.probe.variableKey().descriptorBytes();
            else if (prepared.probe.varchar()) keyBytes = prepared.probe.varcharKey() != null ? prepared.probe.varcharKey().inputs().bytes() : 48;
            else if (prepared.probe.fixedKey() != null) keyBytes = prepared.probe.fixedKey().descriptorBytes();
        }
        long sharedBytes = prepared.sharedMaps != null ? QuestSharedVarchar.SCRATCH_BYTES : 0;
        addressBytes = scratchOffset + PHASE_SCRATCH_BYTES + keyBytes
                + (prepared.localCapacity > 0 ? QuestPartitions.STATE_BYTES : 0)
                + (prepared.inputPartitionCode != null ? QuestPartitions.STATE_BYTES : 0) + sharedBytes;
        addresses = Unsafe.malloc(addressBytes, MemoryTag.NATIVE_JIT);
        stateAddress = addresses + columnBytes;
        scratchAddress = addresses + scratchOffset;
        keyAddress = scratchAddress + PHASE_SCRATCH_BYTES;
        sharedScratch = sharedBytes != 0 ? addresses + addressBytes - sharedBytes : 0;
        partitions = prepared.localCapacity > 0 ? addresses + addressBytes - sharedBytes - QuestPartitions.STATE_BYTES : 0;
        inputPartitions = prepared.inputPartitionCode != null ? partitions - QuestPartitions.STATE_BYTES : 0;
        clear();
        ResolvedFilter.Slot slotFilter = null;
        ResolvedVarchar.Slot arguments = null, keys = null;
        try {
            arguments = prepared.binding.inputs.newSlot(addresses + 8);
            var keyInputs = prepared.probe == null ? null : prepared.probe.keyInputs();
            keys = keyInputs == null ? null : keyInputs.newSlot(keyAddress);
            slotFilter = prepared.filter != null ? prepared.filter.newSlot() : null;
            dictionary = prepared.dictionaryAggregate != null ? prepared.dictionaryAggregate.newSlot(memory, scratchAddress) : null;
        } catch (Throwable failure) {
            try (var closeArguments = arguments; var closeKeys = keys; var closeFilter = slotFilter) { }
            catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            finally { Unsafe.free(addresses, addressBytes, MemoryTag.NATIVE_JIT); }
            throw failure;
        }
        argumentHelpers = arguments;
        keyHelpers = keys;
        filter = slotFilter;
        if (prepared.mapCode != null) {
            Unsafe.putLong(stateAddress + PROBE_COUNT, 0);
            Unsafe.putLong(stateAddress + KEY_COMPARISONS, 0);
        }
        rehash = (oldStart, oldCapacity, newStart, newCapacity, entryStride) -> {
            long scratch = scratchAddress;
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
            try (argumentHelpers; keyHelpers; filter) { if (dictionary != null) dictionary.close(); }
            finally {
                try { if (partitionAllocator != null) partitionAllocator.close(); }
                finally { Unsafe.free(addresses, addressBytes, MemoryTag.NATIVE_JIT); }
            }
        }
    }

    @Override
    public void prepareExecution(CairoConfiguration configuration, MemoryTracker tracker) {
        if (prepared.localCapacity == 0) return;
        if (partitionAllocator == null) {
            // Each page must be an entire allocation: the ordinary slot arena can pack
            // multiple pages into a chunk that cannot be freed independently.
            partitionAllocator = new FastGroupByAllocator(QuestPartitions.PAGE_BYTES,
                    configuration.getGroupByAllocatorMaxChunkSize(), true, false);
            partitionMemory = KernelMemory.reusable(partitionAllocator);
        }
        partitionAllocator.setMemoryTracker(tracker);
        partitionAllocator.reopen();
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
        if (partitionAllocator != null) {
            partitionAllocator.clear();
            partitionAllocator.setMemoryTracker(null);
        }
        if (prepared.sharedMaps != null) prepared.sharedMaps.clear();
        keyFilterAddress = 0;
        rankScratch = 0;
        partitionTopK = 0;
        partitionSketch = mergedSketch = splitStates = splitInputs = 0;
        rankScratchCapacity = 0;
        partitioned = false;
        localRows = 0;
        inputPartitionRemaining = 0;
        inputPartitionWindow = 4L * prepared.localCapacity;
        parallelMerge = false;
        mergePreparationStage = 0;
        finalizeDeferred = false;
        Unsafe.putLong(stateAddress + DeferredDistinct.CONTROL, 0);
        Unsafe.putLong(stateAddress + DeferredDistinct.CONTROL + 8, 0);
        if (partitions != 0) io.questdb.std.Vect.memset(partitions, QuestPartitions.STATE_BYTES, 0);
        if (inputPartitions != 0) io.questdb.std.Vect.memset(inputPartitions, QuestPartitions.STATE_BYTES, 0);
    }

    long dictionaryRows() { return dictionary != null ? dictionary.rows() : 0; }

    @Override
    public void partition(Map source, Runnable checkpoint) {
        if (source.size() == 0) return;
        memory.enterCheckpoint(checkpoint);
        try { flush(source); } finally { memory.close(); }
    }

    private void flush(Map source) {
        if (source.size() == 0) return;
        if (prepared.partitionSelectCode != null && partitionSketch == 0) {
            long bytes = prepared.probe.variable() ? QuestPartitionSketch.VARIABLE_BYTES : QuestPartitionSketch.BYTES;
            partitionSketch = KernelMemory.allocator().malloc(bytes);
            io.questdb.std.Vect.memset(partitionSketch, bytes, 0);
            Unsafe.putLong(partitions + QuestPartitions.SKETCH_OFFSET, partitionSketch);
            if (inputPartitions != 0) Unsafe.putLong(inputPartitions + QuestPartitions.SKETCH_OFFSET, partitionSketch);
        }
        // The growth callback owns scratch[0..31]; its rehash must not overwrite the source view.
        long descriptor = scratchAddress + 32;
        source.writeNativeMutationView(descriptor);
        long rows = Unsafe.getLong(descriptor + NativeMapView.MASK) + (prepared.probe.varchar() ? 1 : 2);
        partitionMemory.enter(null);
        try {
            long status = prepared.flushCode.run(descriptor, rows, partitions);
            partitionMemory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Partial-state partition failed: " + status);
        } finally { partitionMemory.close(); }
        partitioned = true;
        partitionRows += source.size();
        source.clear();
        localRows = 0;
    }

    @Override
    public void mergePartition(Map destination, GroupByKernel.Updater sourceUpdater, int partition, Runnable checkpoint) {
        mergePartition(destination, sourceUpdater, partition, checkpoint, prepared.partitionMergeCode, prepared.inputPartitionMergeCode);
    }

    @Override
    public void releasePartition(int partition) {
        if (partitionAllocator == null) return;
        // Scan allocation has finished. Shard consumers are disjoint, but the allocator's
        // chunk index is shared; serialize only page reclamation, never row processing.
        synchronized (partitionAllocator) {
            releasePages(partitions, partition);
            if (inputPartitions != 0) releasePages(inputPartitions, partition);
        }
    }

    private void releasePages(long states, int partition) {
        long bucket = states + (long) QuestPartitions.BUCKET_BYTES * partition;
        long page = Unsafe.getLong(bucket);
        Unsafe.putLong(bucket, 0);
        Unsafe.putLong(bucket + QuestPartitions.BUCKET_TAIL_OFFSET, 0);
        while (page != 0) {
            long next = Unsafe.getLong(page);
            long bytes = prepared.probe.variable()
                    ? QuestPartitions.VARIABLE_HEADER_BYTES + Unsafe.getLong(page + QuestPartitions.PAGE_CAPACITY_OFFSET)
                    : QuestPartitions.PAGE_BYTES;
            partitionAllocator.free(page, bytes);
            page = next;
        }
        // Separate VARCHAR payload blobs can still be referenced by final keys/states.
        // Their arena remains alive until clear(), after the result cursor closes.
    }

    private void mergePartition(Map destination, GroupByKernel.Updater sourceUpdater, int partition, Runnable checkpoint,
                                CompiledKernel stateCode, CompiledKernel inputCode) {
        AggregateSlot source = (AggregateSlot) sourceUpdater;
        long page = Unsafe.getLong(source.partitions + (long) QuestPartitions.BUCKET_BYTES * partition);
        long inputPage = source.inputPartitions != 0
                ? Unsafe.getLong(source.inputPartitions + (long) QuestPartitions.BUCKET_BYTES * partition) : 0;
        if (page == 0 && inputPage == 0) return;
        writeViews(destination, null);
        memory.enterCheckpoint(checkpoint);
        try {
            while (page != 0) {
                runMutable(stateCode, page + (prepared.probe.variable()
                        ? QuestPartitions.VARIABLE_HEADER_BYTES : QuestPartitions.PAGE_HEADER_BYTES),
                        Unsafe.getLong(page + QuestPartitions.PAGE_COUNT_OFFSET), destination, null, false);
                page = Unsafe.getLong(page);
            }
            while (inputPage != 0) {
                runMutable(inputCode, inputPage + (prepared.probe.variable()
                        ? QuestPartitions.VARIABLE_HEADER_BYTES : QuestPartitions.PAGE_HEADER_BYTES),
                        Unsafe.getLong(inputPage + QuestPartitions.PAGE_COUNT_OFFSET), destination, null, false);
                inputPage = Unsafe.getLong(inputPage);
            }
        } finally {
            try { publishViews(destination, null); } finally { memory.close(); }
        }
    }

    @Override
    public boolean hasPartitionTopK() { return prepared.partitionSelectCode != null; }

    private void selectPartitionTopK(Map source, Map destination, Runnable checkpoint) {
        if (source.size() == 0) return;
        long descriptor = scratchAddress, state = descriptor + NativeMapView.MUTATION_BYTES;
        memory.enterCheckpoint(checkpoint);
        try {
            if (partitionTopK == 0) partitionTopK = KernelMemory.allocator().malloc(prepared.partitionTopKLimit * 16L);
            source.writeNativeMutationView(descriptor);
            Unsafe.putLong(state, partitionTopK);
            Unsafe.putLong(state + QuestLongTopK.SIZE_OFFSET, 0);
            Unsafe.putLong(state + QuestLongTopK.CAPACITY_OFFSET, prepared.partitionTopKLimit);
            Unsafe.putLong(state + QuestLongTopK.SOURCE_KIND_OFFSET, QuestLongTopK.SOURCE_MAP);
            Unsafe.putLong(state + QuestLongTopK.ORDER_OFFSET, prepared.topKOrder);
            Unsafe.putLong(state + QuestLongTopK.VALUE_OFFSET, prepared.partitionTopKOffset + prepared.probe.keyBytes());
            long status = prepared.topKCode.run(descriptor, 0, state);
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Partition Top-K failed: " + status);
            topKRows += source.size();
            long count = Unsafe.getLong(state + QuestLongTopK.SIZE_OFFSET);
            writeViews(destination, null);
            try { runMutable(prepared.partitionSelectCode, partitionTopK, count, destination, null, false); }
            finally { publishViews(destination, null); }
        } finally { memory.close(); }
    }

    long estimatedGroups() { return estimatedGroups; }

    private long estimatePartition(ObjList<GroupByKernel.Updater> sources, int partition, Runnable checkpoint) {
        memory.enterCheckpoint(checkpoint);
        try {
            if (mergedSketch == 0) mergedSketch = KernelMemory.allocator().malloc(QuestPartitionSketch.HEADER_BYTES + QuestPartitionSketch.REGISTERS);
            sampledGroupLowerBound = 0;
            long entryBytes = 0;
            int keySamples = 0;
            for (int r = 0; r < QuestPartitionSketch.REGISTERS; r++) {
                byte maximum = 0;
                long priority = -1, width = 0;
                for (int i = 0; i < sources.size(); i++) {
                    long sketch = ((AggregateSlot) sources.getQuick(i)).partitionSketch;
                    if (sketch != 0) maximum = (byte) Math.max(maximum,
                            Unsafe.getByte(sketch + (long) partition * QuestPartitionSketch.REGISTERS + r));
                    if (sketch != 0 && prepared.probe.variable()) {
                        long sample = sketch + QuestPartitionSketch.BYTES
                                + ((long) partition * QuestPartitionSketch.REGISTERS + r) * QuestPartitionSketch.KEY_SAMPLE_BYTES;
                        long candidate = Unsafe.getLong(sample), candidateWidth = Unsafe.getLong(sample + 8);
                        if (candidate > priority || (candidate == priority && candidateWidth > width)) {
                            priority = candidate;
                            width = candidateWidth;
                        }
                    }
                }
                Unsafe.putByte(mergedSketch + QuestPartitionSketch.HEADER_BYTES + r, maximum);
                if (maximum != 0) sampledGroupLowerBound++;
                if (width > 0) {
                    entryBytes += (width + prepared.binding.valueBytes + 7) & ~7L;
                    keySamples++;
                }
            }
            sampledGroupBytes = keySamples == 0 ? 0 : (entryBytes + keySamples - 1) / keySamples;
            return estimator.of(mergedSketch).computeCardinality() * QuestPartitionSketch.SAMPLE_RATE;
        } finally { memory.close(); }
    }

    @Override
    public void mergeTopKPartition(Map working, Map destination, ObjList<GroupByKernel.Updater> sources,
                                  int partition, Runnable checkpoint) {
        mergeTopKPartition(working, destination, sources, partition, CacheSizing.perWorker(sources.size()), checkpoint);
    }

    // The resource budget is explicit here so focused tests can exercise subdivision on small inputs.
    void mergeTopKPartition(Map working, Map destination, ObjList<GroupByKernel.Updater> sources,
                           int partition, long cacheBytes, Runnable checkpoint) {
        long groups = estimatePartition(sources, partition, checkpoint);
        // Each occupied sample register proves at least one distinct group. Only
        // replay payloads when even the sampled subset exceeds the retained prefix.
        boolean late = prepared.lateStateCode != null && sampledGroupLowerBound > prepared.partitionTopKLimit;
        CompiledKernel stateCode = late ? prepared.rankStateCode : prepared.partitionMergeCode;
        CompiledKernel inputCode = late ? prepared.rankInputCode : prepared.inputPartitionMergeCode;
        estimatedGroups += groups;
        long groupBytes = prepared.probe.variable() ? sampledGroupBytes : (prepared.partitionRowBytes + 7) & ~7L;
        // An empty hash sample needs no subdivision, and supplies no width estimate.
        if (groupBytes == 0) groupBytes = 8;
        long groupLimit = cacheBytes / (2 * groupBytes);
        double loadFactor = 0;
        if (prepared.probe.variable()) {
            working.writeNativeMutationView(scratchAddress);
            loadFactor = (Unsafe.getLong(scratchAddress + NativeMapView.FREE)
                    + Unsafe.getLong(scratchAddress + NativeMapView.SIZE))
                    / (double) (Unsafe.getLong(scratchAddress + NativeMapView.MASK) + 1);
            groupLimit = variableGroupLimit(cacheBytes, groupBytes, loadFactor);
        }
        // Leave a margin for estimation error and the extra partition pass. A mildly
        // oversized map can be cheaper than copying every record again.
        if (groupLimit == 0 || groups <= 4 * groupLimit) {
            for (int i = 0; i < sources.size(); i++) mergePartition(working, sources.getQuick(i), partition, checkpoint, stateCode, inputCode);
            selectPartitionTopK(working, destination, checkpoint);
            if (late) {
                for (int i = 0; i < sources.size(); i++) {
                    mergePartition(destination, sources.getQuick(i), partition, checkpoint, prepared.lateStateCode, prepared.lateInputCode);
                }
            }
            return;
        }
        int parts = 2;
        while (parts < QuestRepartition.PARTS && groupLimit * parts < groups) parts *= 2;
        if (prepared.probe.variable() && working.getKeyCapacity()
                > io.questdb.std.Numbers.ceilPow2((long) Math.ceil((groupLimit + 1) / loadFactor))) {
            // clear() keeps a previous large offset table, which would defeat subdivision.
            working.restoreInitialCapacity();
        }
        long shift = io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS_SHR - Integer.numberOfTrailingZeros(parts);
        memory.enterCheckpoint(checkpoint);
        try {
            if (splitStates == 0) {
                splitStates = KernelMemory.allocator().malloc(2L * QuestRepartition.STATE_BYTES);
                splitInputs = splitStates + QuestRepartition.STATE_BYTES;
                io.questdb.std.Vect.memset(splitStates, 2L * QuestRepartition.STATE_BYTES, 0);
            }
            Unsafe.putLong(splitStates + QuestRepartition.MASK_OFFSET, parts - 1);
            Unsafe.putLong(splitInputs + QuestRepartition.MASK_OFFSET, parts - 1);
            Unsafe.putLong(splitStates + QuestRepartition.SHIFT_OFFSET, shift);
            Unsafe.putLong(splitInputs + QuestRepartition.SHIFT_OFFSET, shift);
            for (int i = 0; i < parts; i++) {
                Unsafe.putLong(splitStates + i * (long) QuestPartitions.BUCKET_BYTES + QuestPartitions.BUCKET_TAIL_OFFSET, 0);
                Unsafe.putLong(splitInputs + i * (long) QuestPartitions.BUCKET_BYTES + QuestPartitions.BUCKET_TAIL_OFFSET, 0);
            }
            for (int i = 0; i < sources.size(); i++) {
                AggregateSlot source = (AggregateSlot) sources.getQuick(i);
                splitPages(prepared.splitStateCode, Unsafe.getLong(source.partitions + (long) QuestPartitions.BUCKET_BYTES * partition), splitStates);
                if (source.inputPartitions != 0) splitPages(prepared.splitInputCode,
                        Unsafe.getLong(source.inputPartitions + (long) QuestPartitions.BUCKET_BYTES * partition), splitInputs);
            }
        } finally { memory.close(); }
        for (int i = 0; i < parts; i++) {
            mergeSplitPages(working, stateCode, splitStates + i * (long) QuestPartitions.BUCKET_BYTES, checkpoint);
            if (inputCode != null) mergeSplitPages(working,
                    inputCode, splitInputs + i * (long) QuestPartitions.BUCKET_BYTES, checkpoint);
            selectPartitionTopK(working, destination, checkpoint);
            if (late) {
                mergeSplitPages(destination, prepared.lateStateCode, splitStates + i * (long) QuestPartitions.BUCKET_BYTES, checkpoint);
                if (prepared.lateInputCode != null) mergeSplitPages(destination,
                        prepared.lateInputCode, splitInputs + i * (long) QuestPartitions.BUCKET_BYTES, checkpoint);
            }
            if (i + 1 < parts) working.clear();
        }
    }

    private static long variableGroupLimit(long cacheBytes, long groupBytes, double loadFactor) {
        long groups = 0;
        // OrderedMap touches its used entry heap and the entire power-of-two offset table.
        // Unused heap capacity contributes allocation, but not cache working data.
        for (long slots = 1; slots <= cacheBytes / Long.BYTES; slots *= 2) {
            groups = Math.max(groups, Math.min((long) (slots * loadFactor) - 1,
                    (cacheBytes - slots * Long.BYTES) / groupBytes));
        }
        return groups;
    }

    private void splitPages(CompiledKernel code, long page, long state) {
        while (page != 0) {
            long status = code.run(page + (prepared.probe.variable() ? QuestPartitions.VARIABLE_HEADER_BYTES
                    : QuestPartitions.PAGE_HEADER_BYTES), Unsafe.getLong(page + QuestPartitions.PAGE_COUNT_OFFSET), state);
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Split records failed: " + status);
            page = Unsafe.getLong(page);
        }
    }

    private void mergeSplitPages(Map destination, CompiledKernel code, long bucket, Runnable checkpoint) {
        long tail = Unsafe.getLong(bucket + QuestPartitions.BUCKET_TAIL_OFFSET);
        if (tail == 0) return;
        long page = Unsafe.getLong(bucket);
        writeViews(destination, null);
        memory.enterCheckpoint(checkpoint);
        try {
            while (true) {
                runMutable(code, page + (prepared.probe.variable() ? QuestPartitions.VARIABLE_HEADER_BYTES : QuestPartitions.PAGE_HEADER_BYTES),
                        Unsafe.getLong(page + QuestPartitions.PAGE_COUNT_OFFSET), destination, null, false);
                if (page == tail) break;
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
    public long getPendingMergeWork() {
        long work = 0;
        long context = Unsafe.getLong(stateAddress + DeferredDistinct.CONTROL + 8);
        while (context != 0) { work += Unsafe.getLong(context + 16); context = Unsafe.getLong(context); }
        return work;
    }

    @Override
    public boolean prepareMergeTasks(ObjList<GroupByKernel.MergeTask> tasks,
                              long targetTaskWork, Runnable checkpoint) {
        int stage = mergePreparationStage++;
        if (stage > 2) return false;
        long context = Unsafe.getLong(stateAddress + DeferredDistinct.CONTROL + 8);
        boolean present = context != 0;
        while (context != 0) {
            checkpoint.run();
            if (stage == 0) {
                // Unequal sets need unequal ranges; counting states alone serializes large outliers.
                long work = Unsafe.getLong(context + 16);
                int parts = Numbers.ceilPow2((int) Math.min(DeferredDistinct.PARTS, 1 + (work - 1) / targetTaskWork));
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
        if (parallelMerge) return;
        finalizeState(map, checkpoint);
        if (mapConsumer != null) mapConsumer.consume(map, checkpoint);
    }

    private void finalizeState(Map map, Runnable checkpoint) {
        if ((!prepared.finalizeResults && !finalizeDeferred) || prepared.finalizeCode == null || map.size() == 0) return;
        long descriptor = scratchAddress;
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
    long inputPartitionRows() { return inputPartitionRows; }

    long[] profileCounters() {
        return new long[]{mapCalls, newGroups, prepared.mapCode == null ? 0 : Unsafe.getLong(stateAddress + PROBE_COUNT),
                prepared.mapCode == null ? 0 : Unsafe.getLong(stateAddress + KEY_COMPARISONS)};
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
        long state = scratchAddress + 64;
        Unsafe.putLong(state, rows.getAddress());
        Unsafe.putLong(state + QuestRowLimit.SELECTED_OFFSET, 0);
        Unsafe.putLong(state + QuestRowLimit.SKIP_OFFSET, skip);
        Unsafe.putLong(state + QuestRowLimit.TAKE_OFFSET, count);
        memory.enterCheckpoint(checkpoint);
        try {
            long status = prepared.rowLimitCode.run(sorted.getNativeAddress(), sorted.size(), state);
            memory.throwIfFailed();
            if (status != 0 || Unsafe.getLong(state + QuestRowLimit.SELECTED_OFFSET) != count) throw new IllegalStateException("Sorted LIMIT kernel failed");
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
        long descriptor = scratchAddress, state = descriptor + NativeMapView.MUTATION_BYTES;
        Unsafe.putLong(state, rows.getAddress());
        Unsafe.putLong(state + QuestRowLimit.SELECTED_OFFSET, 0);
        Unsafe.putLong(state + QuestRowLimit.SKIP_OFFSET, skip);
        Unsafe.putLong(state + QuestRowLimit.TAKE_OFFSET, count);
        memory.enterCheckpoint(checkpoint);
        try {
            for (int i = 0; i < mapCount && Unsafe.getLong(state + QuestRowLimit.SELECTED_OFFSET) < count; i++) {
                Map map = shards == null ? owner : shards.getQuick(i);
                long remainingSkip = Unsafe.getLong(state + QuestRowLimit.SKIP_OFFSET);
                if (remainingSkip >= map.size()) {
                    Unsafe.putLong(state + QuestRowLimit.SKIP_OFFSET, remainingSkip - map.size());
                    continue;
                }
                if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptor);
                else map.writeNativeProbeView(descriptor);
                Unsafe.putLong(state + QuestRowLimit.SHARD_OFFSET, shards == null ? 0 : (long) i << 48);
                long status = prepared.rowLimitCode.run(descriptor, 0, state);
                memory.throwIfFailed();
                if (status != 0) throw new IllegalStateException("LIMIT kernel failed: " + status);
            }
            if (Unsafe.getLong(state + QuestRowLimit.SELECTED_OFFSET) != count) throw new IllegalStateException("LIMIT cardinality changed");
            rows.setPos(count);
            limitedRows += count;
            return true;
        } finally { memory.close(); }
    }

    @Override
    public boolean sortLong(Map owner, ObjList<Map> shards, DirectLongList entries, DirectLongList scratch,
                     Function function, boolean ascending, long maxEntries, Runnable checkpoint) {
        if (prepared.longSortCode == null || prepared.longSortType != function.getType()) return false;
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
        long descriptor = scratchAddress, state = descriptor + NativeMapView.MUTATION_BYTES;
        Unsafe.putLong(state, entries.getAddress());
        Unsafe.putLong(state + QuestLongSort.COUNT_OFFSET, 0);
        Unsafe.putLong(state + QuestLongSort.MODE_OFFSET, QuestLongSort.COLLECT);
        Unsafe.putLong(state + QuestLongSort.ORDER_OFFSET, ascending ? 0 : 1);
        Unsafe.putLong(state + QuestLongSort.VALUE_OFFSET, offset + prepared.probe.keyBytes());
        Unsafe.putLong(state + QuestLongSort.SCRATCH_OFFSET, scratch.getAddress());
        Unsafe.putLong(state + QuestLongSort.HISTOGRAMS_OFFSET, scratch.getAddress() + count * 16);
        memory.enterCheckpoint(checkpoint);
        try {
            for (int i = 0; i < mapCount; i++) {
                Map map = shards == null ? owner : shards.getQuick(i);
                if (map.size() == 0) continue;
                if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptor);
                else map.writeNativeProbeView(descriptor);
                runLongSort(descriptor, state);
            }
            if (Unsafe.getLong(state + QuestLongSort.COUNT_OFFSET) != count) throw new IllegalStateException("Map sort cardinality changed");
            Unsafe.putLong(state + QuestLongSort.MODE_OFFSET, QuestLongSort.SORT);
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
        if (!canRunTopK(list) || prepared.topKType != function.getType()) return false;
        long offset = prepared.resultOffset(function);
        if (offset < 0) return false;
        if (map.size() == 0) return true;
        long descriptor = scratchAddress;
        if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptor);
        else map.writeNativeProbeView(descriptor);
        runTopK(descriptor, 0, list, QuestLongTopK.SOURCE_MAP, offset + prepared.probe.keyBytes(), checkpoint);
        topKRows += map.size();
        return true;
    }

    @Override
    public boolean longTopK(ObjList<Map> maps, DirectLongLongSortedList list, Function function, Runnable checkpoint) {
        if (maps.size() > GroupByMapFragment.NUM_SHARDS || list.getCapacity() <= 64) return false;
        if (!canRunTopK(list) || prepared.topKType != function.getType()) return false;
        long offset = prepared.resultOffset(function);
        if (offset < 0) return false;
        long descriptors = stateAddress + prepared.probe.controlBytes();
        long rows = 0;
        for (int i = 0; i < maps.size(); i++) {
            Map map = maps.getQuick(i);
            if (prepared.probe.orderedNative()) map.writeNativeMutationView(descriptors + i * 64L);
            else map.writeNativeProbeView(descriptors + i * 64L);
            rows += map.size();
        }
        if (rows == 0) return true;
        runTopK(descriptors, maps.size(), list, QuestLongTopK.SOURCE_MAPS, offset + prepared.probe.keyBytes(), checkpoint);
        topKRows += rows;
        return true;
    }

    @Override
    public boolean mergeLongTopK(DirectLongLongSortedList destination, DirectLongLongSortedList source, Runnable checkpoint) {
        if (closed || prepared.closed) throw new IllegalStateException("Aggregate slot is closed");
        if (!canRunTopK(destination) || source.getNativeAddress() == 0) return false;
        if (destination == source) throw new IllegalArgumentException("Top-K source aliases destination");
        runTopK(source.getNativeAddress(), source.size(), destination, QuestLongTopK.SOURCE_PAIRS, 0, checkpoint);
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
        long state = scratchAddress + 64;
        Unsafe.putLong(state, destination.getNativeAddress());
        Unsafe.putLong(state + QuestLongTopK.SIZE_OFFSET, destination.size());
        Unsafe.putLong(state + QuestLongTopK.CAPACITY_OFFSET, destination.getCapacity());
        Unsafe.putLong(state + QuestLongTopK.SOURCE_KIND_OFFSET, kind);
        Unsafe.putLong(state + QuestLongTopK.ORDER_OFFSET, destination.getOrder());
        Unsafe.putLong(state + QuestLongTopK.VALUE_OFFSET, offset);
        Unsafe.putLong(state + QuestLongTopK.SCRATCH_OFFSET, rankScratch);
        Unsafe.putLong(state + QuestLongTopK.SCRATCH_CAPACITY_OFFSET, rankScratchCapacity);
        memory.enterCheckpoint(checkpoint);
        try {
            long status = prepared.topKCode.run(source, rows, state);
            memory.throwIfFailed();
            if (status != 0) throw new IllegalStateException("Top-K kernel failed: " + status);
        } finally {
            rankScratch = Unsafe.getLong(state + QuestLongTopK.SCRATCH_OFFSET);
            rankScratchCapacity = Unsafe.getLong(state + QuestLongTopK.SCRATCH_CAPACITY_OFFSET);
            try { destination.publishNativeSize((int) Unsafe.getLong(state + QuestLongTopK.SIZE_OFFSET)); }
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
        final long state = stateAddress;
        Unsafe.putLong(state + SELECTION, rowIdsAddress);
        Unsafe.putLong(state + KEY_COLUMNS, record.getPageAddress(probe.column()));
        for (int i = 0; i < shards.size(); i++) {
            shards.getQuick(i).writeNativeProbeView(state + CONTROL_BYTES + (long) i * QuestMapProbe.VIEW_BYTES);
        }
        long position = 0;
        final Map lookup = shards.getQuick(0);
        memory.enter(circuitBreaker);
        try {
            while (position < rowCount) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                Unsafe.putLong(state + START_ROW, position);
                long status = prepared.mapCode.run(addresses, rowCount, state);
                if (PROFILE) mapCalls++;
                memory.throwIfFailed();
                if (status != 0 && status != NEED_KEY) {
                    throw new IllegalStateException("Map/update kernel failed: " + status);
                }
                long end = status == 0 ? rowCount : Unsafe.getLong(state + RESUME_ROW);
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
                if (shardIndex != Unsafe.getLong(state + SHARD)) throw new IllegalStateException("Map hash ABI mismatch");
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
                lookup.writeNativeProbeView(state + CONTROL_BYTES);
                shard.writeNativeProbeView(state + CONTROL_BYTES + (long) shardIndex * QuestMapProbe.VIEW_BYTES);
            }
        } finally {
            memory.close();
        }
        return true;
    }

    long fusedInputRows() { return fusedInputRows; }

    long[] groupAdmissionCounters() { return new long[]{admissionInputRows, admissionSelectedRows}; }

    @Override
    public long filterGroups(PageFrameMemoryRecord record, long rowCount, boolean filtered, DirectLongList rows,
                             SqlExecutionCircuitBreaker circuitBreaker) {
        var admission = prepared.groupAdmission;
        if (admission == null || !admission.active()) return -1;
        long selected = admission.select(record, rowCount, filtered, rows, scratchAddress, memory, circuitBreaker);
        admissionInputRows += rowCount;
        admissionSelectedRows += selected < 0 ? rowCount : selected;
        return selected;
    }

    @Override
    public boolean hasFusedFilter() {
        if (prepared.groupAdmission != null && prepared.groupAdmission.active()) return false;
        return prepared.fusedFilterCode != null || (filter != null && prepared.filter.fusedPlan() != null
                && prepared.localCapacity > 0 && prepared.sharedMaps == null && prepared.probe.mutable()
                && !prepared.probe.varchar() && !prepared.probe.variable());
    }

    @Override
    public long updateFiltered(PageFrameMemoryRecord record, long rowCount, Map map, ObjList<Map> shards,
                               int batchSize, DirectLongList rows, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!hasFusedFilter() || !canUpdate(record) || !prepareKeyColumns(record)) return -1;
        long predicates = filter.prepare(record);
        if (predicates == 0) return -1;
        long state = stateAddress;
        if (prepared.fusedFilterCode != null) {
            Unsafe.putLong(state + SELECTION, predicates);
            long before = fusedSelectedRows;
            runMap(rowCount, map, shards, batchSize, circuitBreaker, prepared.fusedFilterCode);
            fusedInputRows += rowCount;
            return fusedSelectedRows - before;
        }
        prepareMap(map, shards, batchSize);
        long groupsBefore = PROFILE ? groupCount(map, shards) + partitionRows : 0;
        long accepted = 0;
        inputPartitionRemaining = 0;
        inputPartitionWindow = 4L * prepared.localCapacity;
        memory.enter(circuitBreaker);
        try {
            for (long begin = 0; begin < rowCount;) {
                long end = Math.min(rowCount, begin + FILTER_STAGE_ROWS);
                long selected = filter.filterRange(begin, end, rows, circuitBreaker);
                if (selected > 0) {
                    Unsafe.putLong(state + SELECTION, rows.getAddress());
                    runMutable(prepared.mapCode, addresses, selected, map, shards, true, 0, accepted > 0);
                    accepted += selected;
                }
                begin = end;
            }
            return accepted;
        } finally {
            try {
                publishViews(map, shards);
                if (PROFILE) newGroups += groupCount(map, shards) + partitionRows - groupsBefore;
            } finally { memory.close(); }
        }
    }

    @Override
    public boolean updateMap(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress, Map map, int batchSize,
                      SqlExecutionCircuitBreaker circuitBreaker) {
        return prepared.probe != null && prepared.probe.mutable()
                && updateMaps(record, rowCount, rowIdsAddress, map, null, batchSize, circuitBreaker);
    }

    private boolean updateMaps(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                               Map map, ObjList<Map> shards, int batchSize, SqlExecutionCircuitBreaker circuitBreaker) {
        if (!canUpdate(record) || !prepareKeyColumns(record)) return false;
        if (shards == null && rowIdsAddress == 0 && dictionary != null
                && dictionary.update(addresses, rowCount, stateAddress, circuitBreaker)) {
            updatedRows += rowCount;
            mapRows += rowCount;
            return true;
        }
        long state = stateAddress;
        Unsafe.putLong(state + SELECTION, rowIdsAddress);
        if (prepared.sharedMaps != null && prepared.sharedMaps.isActive()
                && Unsafe.getLong(Unsafe.getLong(state + KEY_COLUMNS) + 40) != 0) {
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
                        prepared.sharedMaps.scan(prepared.sharedCode, Unsafe.getLong(state + KEY_COLUMNS), rowIdsAddress,
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
        runMap(rowCount, map, shards, batchSize, circuitBreaker, prepared.mapCode);
        return true;
    }

    private void prepareMap(Map map, ObjList<Map> shards, int batchSize) {
        long state = stateAddress;
        QuestMapProbe probe = prepared.probe;
        Unsafe.putLong(addresses, batchSize);
        writeViews(map, shards);
        if (probe.variable()) ((io.questdb.cairo.map.OrderedMap) (shards == null ? map : shards.getQuick(0)))
                .writeNativeKeyStagingView(state + VariableKey.STAGING_ADDRESS, 64);
    }

    private void runMap(long rowCount, Map map, ObjList<Map> shards,
                        int batchSize, SqlExecutionCircuitBreaker circuitBreaker, CompiledKernel kernel) {
        prepareMap(map, shards, batchSize);
        inputPartitionRemaining = 0;
        inputPartitionWindow = 4L * prepared.localCapacity;
        long groupsBefore = PROFILE ? groupCount(map, shards) + partitionRows : 0;
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

    private void partitionInputs(long begin, long rows, Map map) {
        long batchSize = Unsafe.getLong(addresses);
        Unsafe.putLong(addresses, inputPartitions);
        partitionMemory.enter(null);
        try {
            long position = begin;
            while (position < rows) {
                Unsafe.putLong(stateAddress + START_ROW, position);
                long status = prepared.inputPartitionCode.run(addresses, rows, stateAddress);
                partitionMemory.throwIfFailed();
                if (status == 0) break;
                if (status != NEED_STAGING) throw new IllegalStateException("Input partition failed: " + status);
                position = Unsafe.getLong(stateAddress + RESUME_ROW);
                ((io.questdb.cairo.map.OrderedMap) map).writeNativeKeyStagingView(stateAddress + VariableKey.STAGING_ADDRESS, Unsafe.getLong(stateAddress + VariableKey.REQUIRED_BYTES));
            }
            long accepted = rows - begin;
            updatedRows += accepted;
            mapRows += accepted;
            partitionRows += accepted;
            inputPartitionRows += accepted;
            partitioned = true;
            if (PROFILE) mapCalls++;
        } finally {
            Unsafe.putLong(addresses, batchSize);
            partitionMemory.close();
        }
    }

    private boolean prepareKeyColumns(PageFrameMemoryRecord record) {
        var probe = prepared.probe;
        long state = stateAddress;
        if (probe.variable()) {
            long descriptor = keyAddress;
            if (!probe.variableKey().prepare(record, descriptor)) return false;
            Unsafe.putLong(state + KEY_COLUMNS, descriptor);
        } else if (probe.varchar()) {
            long descriptor = keyAddress;
            if (probe.varcharKey() != null) {
                if (!probe.varcharKey().inputs().prepare(record, descriptor)) return false;
            } else if (!record.writeNativeVarcharView(probe.column(), descriptor)) return false;
            Unsafe.putLong(state + KEY_COLUMNS, descriptor);
        } else if (probe.fixedKey() == null) {
            long page = record.getPageAddress(probe.column());
            if (page == 0) return false;
            Unsafe.putLong(state + KEY_COLUMNS, page);
        } else {
            long pointers = keyAddress;
            if (!probe.fixedKey().prepare(record, pointers)) return false;
            Unsafe.putLong(state + KEY_COLUMNS, pointers);
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
        long descriptor = scratchAddress + 32;
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

    void consumeMap(CompiledKernel kernel, Map source, Map destination, Runnable checkpoint) {
        if (source.size() == 0) return;
        // The growth callback owns scratch[0..31]; its rehash must not overwrite the source view.
        long descriptor = scratchAddress + 32;
        source.writeNativeMutationView(descriptor);
        writeViews(destination, null);
        memory.enterCheckpoint(checkpoint);
        try { runMutable(kernel, descriptor, source.size(), destination, null, false); }
        finally { try { publishViews(destination, null); } finally { memory.close(); } }
    }

    private void runKeyFilter(CompiledKernel kernel, long begin, long rows, boolean filtered) {
        if (begin == rows) return;
        if (keyFilterAddress == 0) {
            long bytes = prepared.localCapacity * QuestKeyFilter.stride(prepared.probe);
            keyFilterAddress = KernelMemory.allocator().malloc(bytes);
            io.questdb.std.Vect.memset(keyFilterAddress, bytes, 0);
        }
        long scratch = ResolvedAggregateKernel.prefetchScratch(prepared.probe);
        Unsafe.putLong(stateAddress + START_ROW, begin);
        Unsafe.putLong(stateAddress + scratch, keyFilterAddress);
        Unsafe.putLong(stateAddress + scratch + 8, partitions);
        long status = kernel.run(addresses, rows, stateAddress);
        if (PROFILE) mapCalls++;
        memory.throwIfFailed();
        if (status != 0) throw new IllegalStateException("Key filter failed: " + status);
        partitionRows += Unsafe.getLong(stateAddress + scratch + 16);
        long accepted = filtered ? Unsafe.getLong(stateAddress + FILTERED_ROWS) : rows - begin;
        updatedRows += accepted;
        mapRows += accepted;
        if (filtered) fusedSelectedRows += accepted;
    }

    private void writeViews(Map map, ObjList<Map> shards) {
        long state = stateAddress;
        Unsafe.putLong(state + DeferredDistinct.CONTROL, parallelMerge && shards == null ? 1 : 0);
        Unsafe.putLong(state + SHARD_MASK, shards == null ? 0 : GroupByMapFragment.NUM_SHARDS - 1);
        int count = shards == null ? 1 : shards.size();
        for (int i = 0; i < count; i++) {
            (shards == null ? map : shards.getQuick(i)).writeNativeMutationView(view(i));
        }
    }

    private long view(int index) {
        return stateAddress + prepared.probe.controlBytes() + (long) index * prepared.probe.viewBytes();
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
        runMutable(kernel, input, rows, map, shards, update, begin, false);
    }

    private void runMutable(CompiledKernel kernel, long input, long rows, Map map, ObjList<Map> shards,
                               boolean update, long begin, boolean frameHasRows) {
        // Preserve the scalar merge while its used heap and offset table fit in L2.
        if (!update && kernel == prepared.partitionMergeCode && prepared.stagedPartitionMergeCode != null
                && CacheSizing.L2_DATA_BYTES > 0) {
            long descriptor = view(0);
            long bytes = Unsafe.getLong(descriptor + NativeMapView.Ordered.APPEND)
                    - Unsafe.getLong(descriptor + NativeMapView.ENTRIES)
                    + (Unsafe.getLong(descriptor + NativeMapView.MASK) + 1) * Long.BYTES;
            if (bytes >= CacheSizing.L2_DATA_BYTES) {
                runMutableLoop(kernel, prepared.stagedPartitionMergeCode,
                        input, rows, map, shards, update, begin, frameHasRows);
                return;
            }
        }
        // A large allocated state makes latency hiding plausible, but does not prove it:
        // skew and phase changes can leave a tiny hot working set in a large map.
        if (update && prepared.mapChoice != null && CacheSizing.L2_DATA_BYTES > 0
                && memory.peakBytes() >= CacheSizing.L2_DATA_BYTES
                && (kernel == prepared.mapCode || kernel == prepared.fusedFilterCode)) {
            long footprint = 0;
            for (int i = 0, n = shards == null ? 1 : shards.size(); i < n; i++) {
                long descriptor = view(i);
                footprint += (Unsafe.getLong(descriptor + NativeMapView.MASK) + 1)
                        * Unsafe.getLong(descriptor + NativeMapView.STRIDE);
            }
            if (footprint >= CacheSizing.L2_DATA_BYTES) {
                AdaptiveKernel choice = kernel == prepared.mapCode ? prepared.mapChoice : prepared.filterChoice;
                // Allocated DISTINCT states never use bounded partition collection. Time the
                // complete invocation, including growth, rather than train on partial retries.
                choice.runBatch(rows - begin, selected ->
                        runMutableLoop(kernel, selected, input, rows, map, shards, update, begin, frameHasRows));
                return;
            }
        }
        runMutableLoop(kernel, kernel, input, rows, map, shards, update, begin, frameHasRows);
    }

    private void runMutableLoop(CompiledKernel kernel, Kernel execution, long input, long rows, Map map, ObjList<Map> shards,
                               boolean update, long begin, boolean frameHasRows) {
        CompiledKernel keyFilter = !update ? null : kernel == prepared.mapCode ? prepared.keyFilterCode
                : kernel == prepared.fusedFilterCode ? prepared.filteredKeyFilterCode : null;
        if (keyFilterAddress != 0 && keyFilter != null) {
            runKeyFilter(keyFilter, begin, rows, kernel == prepared.fusedFilterCode);
            return;
        }
        long state = stateAddress;
        long position = begin;
        if (prepared.probe.varchar()) Unsafe.putLong(state + VarcharKey.COPIED_ROW, -1);
        if (prepared.probe.variable()) Unsafe.putLong(state + VariableKey.SOURCE_OFFSET, 0);
        while (position < rows) {
            if (inputPartitionRemaining > 0 && update && kernel == prepared.mapCode) {
                long end = position + Math.min(rows - position, inputPartitionRemaining);
                partitionInputs(position, end, map);
                inputPartitionRemaining -= end - position;
                position = end;
                if (position == rows) return;
            }
            Unsafe.putLong(state + START_ROW, position);
            long status = execution.run(input, rows, state);
            if (PROFILE && update) mapCalls++;
            memory.throwIfFailed();
            if (status != 0 && status != NEED_KEY && status != NEED_CAPACITY && status != NEED_COPY && status != NEED_STAGING && status != NEED_FLUSH) {
                throw new IllegalStateException("Map mutation failed: " + status);
            }
            long end = status == 0 ? rows : Unsafe.getLong(state + RESUME_ROW);
            if (update) {
                long accepted = kernel == prepared.fusedFilterCode
                        ? Unsafe.getLong(state + FILTERED_ROWS) : end - position;
                updatedRows += accepted;
                mapRows += accepted;
                if (kernel == prepared.fusedFilterCode) fusedSelectedRows += accepted;
                if (kernel == prepared.mapCode) localRows += accepted;
            }
            position = end;
            if (status == 0) return;
            if (status == NEED_FLUSH) {
                map.publishNativeMutationView(view(0));
                // A full map supplies actual combining effectiveness. Revisit local
                // aggregation within this frame: a distinct prefix can precede hot keys.
                boolean compact = inputPartitions != 0 && kernel == prepared.mapCode && update
                        && (position > begin || frameHasRows) && localRows >= map.size()
                        // With variable keys, zero combining proves that smaller input payloads
                        // save bytes without estimating a skewed distribution of key lengths.
                        && (prepared.probe.variable() ? localRows == map.size()
                        : map.size() * prepared.partitionRowBytes > localRows * prepared.inputPartitionRowBytes
                        // Equal-width input avoids a second aggregation pass at at most
                        // five percent extra partition bytes in the observed window.
                        || prepared.inputPartitionRowBytes == prepared.partitionRowBytes
                        && localRows - map.size() <= map.size() / 20);
                flush(map);
                writeViews(map, null);
                if (compact) {
                    // Start cautiously after each frame/reuse change. Repeated complete
                    // windows with poor combining amortize lookup checks by backing off.
                    inputPartitionRemaining = inputPartitionWindow;
                    inputPartitionWindow *= 2;
                    continue;
                }
                inputPartitionWindow = 4L * prepared.localCapacity;
                if (keyFilter != null) {
                    // A complete window proved that the key set exceeds the local map.
                    // Remember emitted keys instead of probing and flushing that map again.
                    runKeyFilter(keyFilter, position, rows, kernel == prepared.fusedFilterCode);
                    return;
                }
                continue;
            }
            if (status == NEED_STAGING) {
                ((io.questdb.cairo.map.OrderedMap) (shards == null ? map : shards.getQuick(0)))
                        .writeNativeKeyStagingView(state + VariableKey.STAGING_ADDRESS, Unsafe.getLong(state + VariableKey.REQUIRED_BYTES));
                continue;
            }
            int shard = (int) Unsafe.getLong(state + SHARD);
            Map target = shards == null ? map : shards.getQuick(shard);
            target.publishNativeMutationView(view(shard));
            try {
                if (status == NEED_COPY) {
                    long copied = ((io.questdb.cairo.map.UnorderedVarcharMap) target).copyNativeKey(
                            Unsafe.getLong(state + VarcharKey.COPY_ADDRESS), (int) Unsafe.getLong(state + VarcharKey.COPY_BYTES));
                    Unsafe.putLong(state + VarcharKey.COPY_ADDRESS, copied);
                    Unsafe.putLong(state + VarcharKey.COPIED_ROW, position);
                } else if (status == NEED_CAPACITY) {
                    target.reserveNative(Math.min(rows - position, Unsafe.getLong(addresses)), rehash);
                } else if (prepared.probe.variable()) {
                    ((io.questdb.cairo.map.OrderedMap) target).growNative(Unsafe.getLong(state + VariableKey.REQUIRED_BYTES), rehash);
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
        return Unsafe.getLong(addresses + 8 + prepared.binding.inputs.bytes());
    }

    @Override
    public void resetStats() {
        if (closed) throw new IllegalStateException("Aggregate slot is closed");
        Unsafe.putLong(addresses + 8 + prepared.binding.inputs.bytes(), 0);
    }

    public boolean canUpdate(PageFrameMemoryRecord record) {
        if (closed) throw new IllegalStateException("Aggregate slot is closed");
        if (record instanceof PageFrameFilteredMemoryRecord) return false;
        return prepared.binding.inputs.prepare(record, addresses + 8);
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
        if (!prepared.binding.inputs.prepare(record, addresses + 8)) return false;
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
