/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.IOURing;
import io.questdb.std.IOURingFacadeImpl;
import io.questdb.std.IOURingImpl;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;

/**
 * Manages async prefetch operations using io_uring on Linux.
 * Falls back to synchronous madvise on other platforms or when io_uring is unavailable.
 *
 * <p>This class is NOT thread-safe. Each worker thread should have its own instance,
 * or access must be externally synchronized.</p>
 */
public class AsyncPrefetchManager implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(AsyncPrefetchManager.class);
    private static final int MADV_WILLNEED = 3;

    private final IOURing ring;
    private final boolean asyncEnabled;
    private final long maxChunkBytes;

    // Track pending operations for this manager instance
    private int pendingCount;

    public AsyncPrefetchManager(CairoConfiguration configuration) {
        this.maxChunkBytes = configuration.getSqlJitPrefetchMaxChunkBytes();

        boolean useAsync = configuration.isSqlJitPrefetchAsync()
                && Os.isLinux()
                && IOURingFacadeImpl.INSTANCE.isAvailable()
                && IOURingFacadeImpl.INSTANCE.isMadviseSupported();

        if (useAsync) {
            IOURing localRing = null;
            boolean localAsyncEnabled = false;
            try {
                localRing = new IOURingImpl(
                        IOURingFacadeImpl.INSTANCE,
                        configuration.getSqlJitPrefetchRingCapacity()
                );
                localAsyncEnabled = true;
                LOG.info().$("async prefetch enabled via io_uring").$();
            } catch (Exception e) {
                LOG.info().$("io_uring init failed, falling back to sync prefetch [error=").$(e.getMessage()).I$();
            }
            this.ring = localRing;
            this.asyncEnabled = localAsyncEnabled;
        } else {
            this.ring = null;
            this.asyncEnabled = false;
        }
    }

    /**
     * Prefetch specified columns for the given frame.
     *
     * @param frameIndex            Frame to prefetch
     * @param cache                 Page frame address cache
     * @param filterTableColIndexes Table column indexes to prefetch
     */
    public void prefetch(
            int frameIndex,
            PageFrameAddressCache cache,
            IntList filterTableColIndexes
    ) {
        if (filterTableColIndexes.size() == 0) {
            return;
        }

        // Check frame format - only NATIVE frames benefit from prefetch
        byte format = cache.getFrameFormat(frameIndex);
        if (format != PartitionFormat.NATIVE) {
            return;
        }

        LongList addresses = cache.getPageAddresses(frameIndex);
        LongList sizes = cache.getPageSizes(frameIndex);

        if (addresses == null || sizes == null) {
            return;
        }

        for (int i = 0, n = filterTableColIndexes.size(); i < n; i++) {
            int tableColIdx = filterTableColIndexes.getQuick(i);
            int queryColIdx = cache.tableToQueryColumnIndex(tableColIdx);

            if (queryColIdx < 0 || queryColIdx >= addresses.size()) {
                continue;
            }

            long addr = addresses.getQuick(queryColIdx);
            long size = sizes.getQuick(queryColIdx);

            if (addr != 0 && size > 0) {
                if (asyncEnabled) {
                    prefetchAsync(addr, size);
                } else {
                    prefetchSync(addr, size);
                }
            }

            // Also prefetch aux vectors for variable-size columns
            if (cache.isVarSizeColumn(queryColIdx)) {
                LongList auxAddresses = cache.getAuxPageAddresses(frameIndex);
                LongList auxSizes = cache.getAuxPageSizes(frameIndex);

                if (auxAddresses != null && auxSizes != null
                        && queryColIdx < auxAddresses.size()) {
                    long auxAddr = auxAddresses.getQuick(queryColIdx);
                    long auxSize = auxSizes.getQuick(queryColIdx);

                    if (auxAddr != 0 && auxSize > 0) {
                        if (asyncEnabled) {
                            prefetchAsync(auxAddr, auxSize);
                        } else {
                            prefetchSync(auxAddr, auxSize);
                        }
                    }
                }
            }
        }

        // Submit any pending async operations
        if (asyncEnabled && pendingCount > 0) {
            ring.submit();
            pendingCount = 0;
        }
    }

    /**
     * Check if async prefetch is enabled.
     *
     * @return true if using io_uring for async prefetch
     */
    public boolean isAsyncEnabled() {
        return asyncEnabled;
    }

    /**
     * Drain completion queue. Should be called periodically to prevent CQ overflow.
     * For prefetch operations, we don't care about results - just drain.
     */
    public void drainCompletions() {
        if (asyncEnabled && ring != null) {
            while (ring.nextCqe()) {
                // Discard results; prefetch is advisory
            }
        }
    }

    @Override
    public void close() {
        if (ring != null) {
            Misc.free(ring);
        }
    }

    private void prefetchAsync(long addr, long size) {
        long remaining = size;
        long currentAddr = addr;

        while (remaining > 0) {
            // io_uring madvise uses 32-bit length
            int chunkLen = (int) Math.min(remaining, Math.min(maxChunkBytes, Integer.MAX_VALUE));

            long id = ring.enqueueMadvise(currentAddr, chunkLen, MADV_WILLNEED);
            if (id >= 0) {
                pendingCount++;
            } else {
                // Queue full, submit current batch and retry once
                if (pendingCount > 0) {
                    ring.submit();
                    drainCompletions();
                    pendingCount = 0;

                    id = ring.enqueueMadvise(currentAddr, chunkLen, MADV_WILLNEED);
                    if (id >= 0) {
                        pendingCount++;
                    } else {
                        // Still full, fall back to sync for this chunk
                        Files.prefetch(currentAddr, chunkLen);
                    }
                } else {
                    Files.prefetch(currentAddr, chunkLen);
                }
            }

            currentAddr += chunkLen;
            remaining -= chunkLen;
        }
    }

    private void prefetchSync(long addr, long size) {
        long remaining = size;
        long currentAddr = addr;

        while (remaining > 0) {
            int chunkLen = (int) Math.min(remaining, maxChunkBytes);
            Files.prefetch(currentAddr, chunkLen);
            currentAddr += chunkLen;
            remaining -= chunkLen;
        }
    }
}
