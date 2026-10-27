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
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;

/** Bounds retained data chunks, including both old and new tables during rehash. */
final class QueryAllocator extends FastGroupByAllocator {
    private final long chunkSize;
    private final long budget;
    private final BudgetTracker owned;
    private long peak;

    QueryAllocator(SqlExecutionContext context, long budget) {
        super(context.getCairoEngine().getConfiguration().getGroupByAllocatorDefaultChunkSize(),
                context.getCairoEngine().getConfiguration().getGroupByAllocatorMaxChunkSize(), true, false);
        this.budget = budget;
        chunkSize = context.getCairoEngine().getConfiguration().getGroupByAllocatorDefaultChunkSize();
        owned = context.getMemoryTracker() == null ? new BudgetTracker(budget) : null;
        try {
            setMemoryTracker(owned == null ? context.getMemoryTracker() : owned);
            reopen();
        } catch (RuntimeException | Error e) {
            close();
            throw e;
        }
    }

    @Override
    public long malloc(long bytes) {
        checkAdditional(bytes);
        long ptr = super.malloc(bytes);
        peak = Math.max(peak, allocated());
        return ptr;
    }

    @Override
    public long realloc(long ptr, long oldBytes, long newBytes) {
        checkAdditional(newBytes);
        long address = super.realloc(ptr, oldBytes, newBytes);
        peak = Math.max(peak, allocated());
        return address;
    }

    long peak() { return peak; }

    @Override
    public void close() {
        try { super.close(); } finally { if (owned != null) { owned.close(); } }
    }

    private void checkAdditional(long bytes) {
        // An existing query tracker remains authoritative. Additionally reserve
        // room for a worst-case new chunk under the prototype's explicit budget.
        if (owned == null && Math.max(bytes, chunkSize) > budget - allocated()) {
            throw CairoException.nonCritical().put("Compiled query memory budget exceeded: ").put(budget);
        }
    }

    private static final class BudgetTracker extends MemoryTracker {
        BudgetTracker(long limit) {
            Unsafe.getUnsafe().putLong(nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limit);
        }
        @Override
        public long getQueryId() { return -1; }
        @Override
        public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
        @Override
        public void close() { destroyNativeBlock(); }
    }
}
