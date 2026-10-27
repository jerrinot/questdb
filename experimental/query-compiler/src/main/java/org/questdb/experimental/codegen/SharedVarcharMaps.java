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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.NativeMapView;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntFunction;

/** Borrows the final maps. Existing map owners retain allocation, tracking and cleanup. */
final class SharedVarcharMaps implements AutoCloseable {
    private static final int SHARDS = 256;
    private static final long VIEW_BYTES = NativeMapView.MUTATION_BYTES;
    static final long QUANTUM = 8192;
    // 8192 entries need a 16384-slot, 384KiB ordinary map. Keep this small
    // working set private; it fits comfortably within the measured 1MiB/core L2.
    static final long LOCAL_GROUPS = 8192;
    private final AtomicLong scannedRows = new AtomicLong();
    private volatile boolean large;
    private final long views;
    private final long allocation;
    private final Object[] initializers = new Object[SHARDS];
    private final ReentrantReadWriteLock resizeLock = new ReentrantReadWriteLock();
    private IntFunction<Map> maps;
    private boolean closed;

    SharedVarcharMaps() {
        for (int i = 0; i < SHARDS; i++) initializers[i] = new Object();
        allocation = Unsafe.malloc(SHARDS * VIEW_BYTES + 63, MemoryTag.NATIVE_JIT);
        views = (allocation + 63) & -64L;
        Vect.memset(views, SHARDS * VIEW_BYTES, 0);
    }

    /** Called before frame publication, after the owning context binds its query tracker. */
    void init(IntFunction<Map> maps) {
        if (closed || this.maps != null) throw new IllegalStateException("Shared maps already active or closed");
        this.maps = maps;
    }

    boolean isActive() { return maps != null; }

    /** All frame, merge and output work has drained. The ordinary maps close separately. */
    void clear() {
        if (maps != null) {
            maps = null;
            scannedRows.set(0);
            large = false;
            Vect.memset(views, SHARDS * VIEW_BYTES, 0);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            maps = null;
            Unsafe.free(allocation, SHARDS * VIEW_BYTES + 63, MemoryTag.NATIVE_JIT);
        }
    }

    void scan(CompiledKernel code, long descriptor, long rowIds, long begin, long rows, long scratch,
              Map.NativeRehash rehash, SqlExecutionCircuitBreaker breaker) {
        if (closed || maps == null) throw new IllegalStateException("Shared maps are not active");
        Unsafe.putLong(scratch, begin);
        Unsafe.putLong(scratch + 8, rowIds);
        Unsafe.putLong(scratch + 16, descriptor);
        Unsafe.putLong(scratch + 24, views);
        long position = begin;
        while (position < rows) {
            breaker.statefulThrowExceptionIfTrippedTimeThrottled();
            long end = Math.min(rows, position + QUANTUM);
            long status;
            acquire(resizeLock.readLock(), breaker);
            try {
                status = code.run(0, end, scratch);
                KernelMemory.checkFailure();
            } finally { resizeLock.readLock().unlock(); }
            long next = Unsafe.getLong(scratch);
            if (status != 0 && status != QuestSharedVarchar.NEED_MAP) {
                throw new IllegalStateException("Shared aggregation failed: " + status);
            }
            if (next < position || next > end || (status == 0 && next != end)) {
                throw new IllegalStateException("Shared aggregation did not preserve its input position");
            }
            position = next;
            if (status == QuestSharedVarchar.NEED_MAP) {
                int shard = Math.toIntExact(Unsafe.getLong(scratch + 32));
                long observedBase = Unsafe.getLong(scratch + 40), observedMask = Unsafe.getLong(scratch + 48);
                if (shard < 0 || shard >= SHARDS) throw new IllegalStateException("Invalid shared shard");
                if (observedBase == 0) initialize(shard, scratch + QuestSharedVarchar.COUNTS, breaker);
                else {
                    acquire(resizeLock.writeLock(), breaker);
                    try {
                        long view = views + shard * VIEW_BYTES;
                        // A waiting worker may have already grown this shard. Include the
                        // mask to distinguish allocator address reuse after multiple grows.
                        if (Unsafe.getLong(view) == observedBase && Unsafe.getLong(view + NativeMapView.MASK) == observedMask) {
                            grow(maps.apply(shard), view, Unsafe.getLong(view + NativeMapView.SIZE), rehash);
                        }
                    } finally { resizeLock.writeLock().unlock(); }
                }
            }
        }
        scannedRows.addAndGet(rows - begin);
    }

    // This is an online choice over already aggregated rows, not a preliminary
    // cardinality scan. A private map that exceeds the budget returns to sharing.
    boolean prefersLocal() {
        if (large || scannedRows.get() < 65536) return false;
        long groups = 0;
        for (int shard = 0; shard < SHARDS; shard++) {
            groups += Unsafe.getLongVolatile(views + shard * VIEW_BYTES + NativeMapView.SIZE);
            if (groups > LOCAL_GROUPS) { large = true; return false; }
        }
        return true;
    }

    /** Called by an existing merge worker after every scan task has joined. */
    void finish(int shard, Map.NativeRehash rehash, Runnable checkpoint) {
        checkpoint.run();
        Map map = maps.apply(shard);
        long view = views + shard * VIEW_BYTES;
        if (Unsafe.getLong(view) == 0) {
            map.writeNativeMutationView(view);
            return;
        }
        long count = Unsafe.getLong(view + NativeMapView.SIZE);
        long free = Unsafe.getLong(view + NativeMapView.FREE) - (count - map.size());
        if (free <= 0) grow(map, view, count, rehash);
        else {
            Unsafe.putLong(view + NativeMapView.FREE, free);
            map.publishNativeMutationView(view);
        }
    }

    private static void acquire(Lock lock, SqlExecutionCircuitBreaker breaker) {
        try {
            while (!lock.tryLock(1, TimeUnit.MILLISECONDS)) breaker.statefulThrowExceptionIfTrippedTimeThrottled();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw CairoException.queryCancelled();
        }
    }

    private static void grow(Map map, long view, long count, Map.NativeRehash rehash) {
        // Native counts are authoritative during scanning; ordinary map metadata is
        // published only at a quiescent boundary. Rehash scans entries independently
        // of size. Grow before publishing a temporarily saturated free counter.
        long publishedCount = map.size();
        long free;
        do {
            map.growNative(rehash);
            map.writeNativeMutationView(view);
            free = Unsafe.getLong(view + NativeMapView.FREE) - (count - publishedCount);
        } while (free <= 0);
        Unsafe.putLong(view + NativeMapView.FREE, free);
        Unsafe.putLong(view + NativeMapView.SIZE, count);
        map.publishNativeMutationView(view);
    }

    private void initialize(int shard, long staging, SqlExecutionCircuitBreaker breaker) {
        acquire(resizeLock.readLock(), breaker);
        try {
            synchronized (initializers[shard]) {
                long view = views + shard * VIEW_BYTES;
                if (Unsafe.getLong(view) == 0) {
                    Map map = maps.apply(shard);
                    ((UnorderedVarcharMap) map).preferLargePages();
                    map.writeNativeMutationView(staging);
                    // Independent maps initialize concurrently. Publish the base last:
                    // generated readers acquire it before reading the remaining fields.
                    for (int i = 1; i < 8; i++) Unsafe.putLong(view + i * 8L, Unsafe.getLong(staging + i * 8L));
                    Unsafe.putLongVolatile(view, Unsafe.getLong(staging));
                }
            }
        } finally { resizeLock.readLock().unlock(); }
    }
}
