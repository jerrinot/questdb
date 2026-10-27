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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;

import java.lang.foreign.Arena;

/** AsyncGroupByAtom's allocation model: one ordinary allocator per slot, one query tracker.
 * Source allocations survive pointer adoption until all result rows have been consumed.
 */
final class ParallelMemory implements AutoCloseable {
    final Arena arena = Arena.ofShared();
    private final long limit;
    private final GroupByAllocator[] allocators;
    private final BudgetTracker owned;
    private long observedPeak;

    ParallelMemory(SqlExecutionContext context, long limit, int states) {
        this.limit = limit;
        allocators = new GroupByAllocator[states];
        owned = context.getMemoryTracker() == null ? new BudgetTracker(limit) : null;
        var tracker = owned == null ? context.getMemoryTracker() : owned;
        try {
            for (int i = 0; i < states; i++) {
                var allocator = GroupByAllocatorFactory.createAllocator(context.getCairoEngine().getConfiguration(), false);
                allocators[i] = allocator;
                allocator.setMemoryTracker(tracker);
                allocator.reopen();
            }
        } catch (RuntimeException | Error failure) {
            try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    GroupByAllocator allocator(int index) { return allocators[index]; }

    /** Called only at joined task barriers. No hot-path locks or replacement allocator. */
    void observe() {
        long used = 0;
        for (var allocator : allocators) { used += allocator.allocated(); }
        observedPeak = Math.max(observedPeak, used);
        if (used > limit) {
            throw CairoException.nonCritical().put("Compiled query memory budget exceeded: ").put(limit);
        }
    }

    long peak() { return observedPeak; }

    @Override
    public void close() {
        Throwable failure = null;
        for (var allocator : allocators) {
            if (allocator == null) { continue; }
            try { allocator.close(); }
            catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } }
        }
        try { arena.close(); }
        catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } }
        try { if (owned != null) { owned.close(); } }
        catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } }
        if (failure instanceof Error e) { throw e; }
        if (failure instanceof RuntimeException e) { throw e; }
    }

    private static final class BudgetTracker extends MemoryTracker {
        BudgetTracker(long limit) {
            Unsafe.getUnsafe().putLong(nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limit);
        }
        @Override public long getQueryId() { return -1; }
        @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
        @Override public void close() { destroyNativeBlock(); }
    }
}
