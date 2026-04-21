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

package io.questdb.cutlass.http2;

import io.questdb.std.IntIntHashMap;
import io.questdb.std.Misc;

import java.io.Closeable;

/**
 * Fixed-size pool of HTTP/2 stream slots (see {@code STREAM_STATE_MACHINE.md} §4
 * and §5). Each slot is a tagged union of three kinds: LIVE (a full
 * {@link Http2Stream} counted against {@code activeStreamCount}),
 * DISCARDING_BLOCK (HPACK-decoded-but-suppressed entry for a refused HEADERS
 * block), and TOMBSTONE (recently-closed stream kept around for the grace
 * window in §5).
 * <p>
 * Post-warmup every request finds a pre-allocated slot; the pool itself does
 * not allocate on the hot path. Tombstones roll off in FIFO order once
 * {@link #getTombstoneCap} is reached; the DISCARDING_BLOCK kind has its own
 * small reserve because {@code HTTP2_FRAME_CODEC.md} §7 guarantees at most
 * one header block is in-flight across the whole connection (CONTINUATION
 * sequencing), but we size the reserve to 2 to absorb a pipelined
 * promotion race without allocation.
 * <p>
 * Stream-id uniqueness is enforced upstream: the connection context advances
 * {@code highestPeerStreamIdSeen} on every HEADERS and rejects re-used ids
 * with {@code PROTOCOL_ERROR} before the pool sees them. The pool therefore
 * assumes every {@link #allocateLive} and {@link #allocateDiscardingBlock}
 * call carries a fresh id.
 */
public final class Http2StreamPool implements Closeable {

    public static final byte CLOSE_CLEAN = 0;
    public static final byte CLOSE_LOCAL_RESET = 1;
    public static final int SLOT_NOT_FOUND = -1;
    public static final byte SLOT_DISCARDING_BLOCK = 2;
    public static final byte SLOT_FREE = 0;
    public static final byte SLOT_LIVE = 1;
    public static final byte SLOT_TOMBSTONE = 3;

    private static final int DEFAULT_DISCARDING_RESERVE = 2;
    private final int[] freeList;
    private final IntIntHashMap idToSlot;
    private final int maxConcurrentStreams;
    private final int[] slotBlockBytes;
    private final byte[] slotCloseKind;
    private final long[] slotCloseTick;
    private final byte[] slotKind;
    private final int[] slotReason;
    private final Http2Stream[] slotStream;
    private final int[] slotStreamId;
    private final int slotCount;
    private final int tombstoneCap;
    private int activeStreamCount;
    private boolean closed;
    // Monotonic counter used to pick the oldest tombstone on roll-off.
    // Incremented on every tombstone install so ordering survives even
    // though slot indices are handed out from a free-list in arbitrary
    // order.
    private long closeTickCounter;
    private int discardingCount;
    private int freeCount;
    private int tombstoneCount;

    public Http2StreamPool(int maxConcurrentStreams, int tombstoneCap) {
        this(maxConcurrentStreams, tombstoneCap, DEFAULT_DISCARDING_RESERVE, 0, 0, 0);
    }

    public Http2StreamPool(int maxConcurrentStreams, int tombstoneCap, int discardingReserve) {
        this(maxConcurrentStreams, tombstoneCap, discardingReserve, 0, 0, 0);
    }

    public Http2StreamPool(int maxConcurrentStreams, int tombstoneCap, int discardingReserve,
                           int outboundArenaBytesPerStream, int outboundTupleQueueCap) {
        this(maxConcurrentStreams, tombstoneCap, discardingReserve,
                outboundArenaBytesPerStream, outboundTupleQueueCap, 0);
    }

    /**
     * @param maxConcurrentStreams         {@code SETTINGS_MAX_CONCURRENT_STREAMS}
     *                                     policy ceiling (§8).
     * @param tombstoneCap                 recently-closed-stream grace window
     *                                     bound (§5 closed rules).
     * @param discardingReserve            extra slots reserved for
     *                                     {@link #SLOT_DISCARDING_BLOCK} promotions.
     * @param outboundArenaBytesPerStream  native copy-on-enqueue arena size
     *                                     on each {@link Http2Stream}. {@code 0}
     *                                     disables outbound staging.
     * @param outboundTupleQueueCap        per-stream outbound tuple ring
     *                                     capacity; must be {@code 0} iff
     *                                     {@code outboundArenaBytesPerStream}
     *                                     is {@code 0}.
     * @param headerStagingBytesPerStream  native pseudo-header staging buffer
     *                                     size on each {@link Http2Stream}.
     *                                     {@code 0} disables pseudo-header
     *                                     capture.
     */
    public Http2StreamPool(int maxConcurrentStreams, int tombstoneCap, int discardingReserve,
                           int outboundArenaBytesPerStream, int outboundTupleQueueCap,
                           int headerStagingBytesPerStream) {
        if (maxConcurrentStreams < 1) {
            throw new IllegalArgumentException("maxConcurrentStreams must be >= 1: " + maxConcurrentStreams);
        }
        if (tombstoneCap < 0) {
            throw new IllegalArgumentException("tombstoneCap must be >= 0: " + tombstoneCap);
        }
        if (discardingReserve < 1) {
            throw new IllegalArgumentException("discardingReserve must be >= 1: " + discardingReserve);
        }
        this.maxConcurrentStreams = maxConcurrentStreams;
        this.tombstoneCap = tombstoneCap;
        this.slotCount = maxConcurrentStreams + tombstoneCap + discardingReserve;
        this.slotKind = new byte[slotCount];
        this.slotStreamId = new int[slotCount];
        this.slotReason = new int[slotCount];
        this.slotBlockBytes = new int[slotCount];
        this.slotCloseTick = new long[slotCount];
        this.slotCloseKind = new byte[slotCount];
        this.slotStream = new Http2Stream[slotCount];
        // Allocate each per-slot Http2Stream up-front so the hot path never
        // allocates. If one allocation throws (native OOM), roll back all
        // prior slots so the partially-constructed pool doesn't leak the
        // native arenas.
        int allocated = 0;
        try {
            for (int i = 0; i < slotCount; i++) {
                this.slotStream[i] = new Http2Stream(
                        outboundArenaBytesPerStream,
                        outboundTupleQueueCap,
                        headerStagingBytesPerStream);
                this.slotStreamId[i] = -1;
                allocated++;
            }
        } catch (Throwable t) {
            for (int i = 0; i < allocated; i++) {
                Misc.free(slotStream[i]);
            }
            throw t;
        }
        this.freeList = new int[slotCount];
        for (int i = 0; i < slotCount; i++) {
            // Push indices 0..N-1 onto the free-list; pop in LIFO order so
            // hot slots stay hot across tests and warm allocations.
            this.freeList[i] = slotCount - 1 - i;
        }
        this.freeCount = slotCount;
        this.idToSlot = new IntIntHashMap(slotCount);
    }

    /**
     * Converts the DISCARDING_BLOCK at {@code slotIndex} into a
     * {@link #SLOT_TOMBSTONE} with {@link #CLOSE_LOCAL_RESET} kind once the
     * refused block reaches END_HEADERS. Evicts the oldest existing
     * tombstone first if the pool is at {@link #getTombstoneCap}.
     */
    public void promoteDiscardingToTombstone(int slotIndex) {
        if (slotKind[slotIndex] != SLOT_DISCARDING_BLOCK) {
            throw new IllegalStateException("promoteDiscardingToTombstone on slot kind=" + slotKind[slotIndex]);
        }
        discardingCount--;
        slotBlockBytes[slotIndex] = 0;
        slotReason[slotIndex] = 0;
        if (tombstoneCap == 0) {
            releaseSlot(slotIndex);
            return;
        }
        if (tombstoneCount >= tombstoneCap) {
            evictOldestTombstone();
        }
        slotKind[slotIndex] = SLOT_TOMBSTONE;
        slotCloseKind[slotIndex] = CLOSE_LOCAL_RESET;
        slotCloseTick[slotIndex] = ++closeTickCounter;
        tombstoneCount++;
    }

    /**
     * Attempts to take a free slot for a newly-refused header block (§8
     * concurrency refusal, §12 post-GOAWAY draining). When the free-list
     * is empty the pool evicts the oldest TOMBSTONE before failing;
     * callers therefore observe an implicit tombstone roll-off on
     * allocation, not just on {@link #closeLiveSlot} /
     * {@link #promoteDiscardingToTombstone}. Returns
     * {@link #SLOT_NOT_FOUND} if no free slot remains after the eviction
     * attempt — the caller converts that into a connection
     * {@code INTERNAL_ERROR} per §12 (pool exhaustion is a server bug at
     * this point in the design, not a recoverable state).
     */
    public int allocateDiscardingBlock(int streamId, int reason) {
        if (freeCount == 0) {
            if (tombstoneCount > 0) {
                evictOldestTombstone();
            }
            if (freeCount == 0) {
                return SLOT_NOT_FOUND;
            }
        }
        int slot = takeSlot();
        slotKind[slot] = SLOT_DISCARDING_BLOCK;
        slotStreamId[slot] = streamId;
        slotReason[slot] = reason;
        slotBlockBytes[slot] = 0;
        idToSlot.put(streamId, slot);
        discardingCount++;
        return slot;
    }

    /**
     * Attempts to allocate a LIVE slot for a newly-admitted stream. Returns
     * {@link #SLOT_NOT_FOUND} if {@link #getActiveStreamCount} already
     * equals {@link #getMaxConcurrentStreams} — the caller emits
     * {@code RST_STREAM(REFUSED_STREAM)} per §8. When the LIVE cap is not
     * yet reached but the free-list is empty (every slot is a tombstone),
     * the pool evicts the oldest tombstone to make room before failing, so
     * tombstone roll-off can happen on allocation as well as on close.
     * <p>
     * The caller supplies the initial inbound / outbound per-stream windows
     * computed from the connection context's tracked SETTINGS at the moment
     * of admission (§7 initial-window rules). The pool does not depend on
     * SETTINGS state directly.
     *
     * @param streamId              peer-initiated stream id being admitted
     * @param initialInboundWindow  {@code ourApplied[INITIAL_WINDOW_SIZE]}
     * @param initialOutboundWindow {@code peerAdvertised[INITIAL_WINDOW_SIZE]}
     * @return the slot index, or {@link #SLOT_NOT_FOUND} if the LIVE cap is
     * reached
     */
    public int allocateLive(int streamId, long initialInboundWindow, long initialOutboundWindow) {
        if (activeStreamCount >= maxConcurrentStreams) {
            return SLOT_NOT_FOUND;
        }
        if (freeCount == 0) {
            if (tombstoneCount > 0) {
                evictOldestTombstone();
            }
            if (freeCount == 0) {
                return SLOT_NOT_FOUND;
            }
        }
        int slot = takeSlot();
        slotKind[slot] = SLOT_LIVE;
        slotStreamId[slot] = streamId;
        idToSlot.put(streamId, slot);
        slotStream[slot].recycle(streamId, initialInboundWindow, initialOutboundWindow);
        activeStreamCount++;
        return slot;
    }

    /**
     * Appends {@code delta} bytes to the discarding-block accumulator for
     * the slot. Used while CONTINUATION frames feed the block-assembly
     * scratch and we want to track how much we've accepted on the refused
     * id without maintaining a separate ledger.
     */
    public void appendDiscardingBlockBytes(int slotIndex, int delta) {
        if (slotKind[slotIndex] != SLOT_DISCARDING_BLOCK) {
            throw new IllegalStateException("appendDiscardingBlockBytes on slot kind=" + slotKind[slotIndex]);
        }
        slotBlockBytes[slotIndex] += delta;
    }

    /**
     * Resets the pool to its empty state. Called when returning the pool
     * instance to a connection pool for reuse. Every LIVE slot's
     * {@link Http2Stream#unassign} is invoked so stale state does not
     * leak into the next connection.
     */
    public void clear() {
        for (int i = 0; i < slotCount; i++) {
            if (slotKind[i] != SLOT_FREE) {
                slotStream[i].unassign();
                slotKind[i] = SLOT_FREE;
                slotStreamId[i] = -1;
                slotReason[i] = 0;
                slotBlockBytes[i] = 0;
                slotCloseTick[i] = 0;
                slotCloseKind[i] = 0;
            }
            freeList[i] = slotCount - 1 - i;
        }
        freeCount = slotCount;
        activeStreamCount = 0;
        tombstoneCount = 0;
        discardingCount = 0;
        closeTickCounter = 0;
        idToSlot.clear();
    }

    /**
     * Releases the per-stream native arenas held by every pool slot.
     * Idempotent. Called by {@link Http2ConnectionContext#close} at
     * connection teardown. A subsequent {@link #allocateLive} or
     * {@link #allocateDiscardingBlock} call after {@link #close} is a
     * programming error — the stream objects have given their native
     * memory back to the allocator.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (int i = 0; i < slotCount; i++) {
            Misc.free(slotStream[i]);
        }
    }

    /**
     * Transitions the LIVE slot at {@code slotIndex} to
     * {@link #SLOT_TOMBSTONE}. Bumps the stream's
     * {@link Http2Stream#getGeneration} so any deferred-credit ack the
     * handler may still emit against the prior generation no-ops per §7
     * step 6. Evicts the oldest tombstone first if the pool is at its
     * {@link #getTombstoneCap}.
     *
     * @param slotIndex slot returned by {@link #allocateLive}
     * @param closeKind {@link #CLOSE_CLEAN} or {@link #CLOSE_LOCAL_RESET}
     */
    public void closeLiveSlot(int slotIndex, byte closeKind) {
        if (slotKind[slotIndex] != SLOT_LIVE) {
            throw new IllegalStateException("closeLiveSlot on slot kind=" + slotKind[slotIndex]);
        }
        if (closeKind != CLOSE_CLEAN && closeKind != CLOSE_LOCAL_RESET) {
            throw new IllegalArgumentException("closeKind must be CLEAN or LOCAL_RESET: " + closeKind);
        }
        activeStreamCount--;
        // LIVE → TOMBSTONE should not carry response bytes the peer can no
        // longer act on (RST races, local reset, unclean connection
        // teardown). A clean close via send-END_STREAM already drained the
        // queue during the final emission; this is a defensive idempotent
        // reset for the RST / abort paths where the queue might still
        // contain tuples. Catches the "arena bytes leak into a recycled
        // slot" ownership bug that HTTP2_INTEGRATION.md §15.4 A.7 test 9
        // is designed to surface.
        slotStream[slotIndex].clearOutboundQueue();
        if (tombstoneCap == 0) {
            // Grace window disabled; skip TOMBSTONE entirely and return the
            // slot to the free-list so subsequent LIVE allocations see it
            // immediately. No generation bump here — §4 ties the LIVE→
            // TOMBSTONE bump to the promotion event, and without a tombstone
            // phase the only bump is the subsequent recycle.
            releaseSlot(slotIndex);
            return;
        }
        // §4: bump generation on LIVE → TOMBSTONE so a stale handler token
        // from the prior occupant fails the late-ack guard in §7 step 6.
        slotStream[slotIndex].bumpGeneration();
        if (tombstoneCount >= tombstoneCap) {
            evictOldestTombstone();
        }
        slotKind[slotIndex] = SLOT_TOMBSTONE;
        slotCloseKind[slotIndex] = closeKind;
        slotCloseTick[slotIndex] = ++closeTickCounter;
        tombstoneCount++;
    }

    public int getActiveStreamCount() {
        return activeStreamCount;
    }

    public int getDiscardingBlockBytes(int slotIndex) {
        if (slotKind[slotIndex] != SLOT_DISCARDING_BLOCK) {
            throw new IllegalStateException("getDiscardingBlockBytes on slot kind=" + slotKind[slotIndex]);
        }
        return slotBlockBytes[slotIndex];
    }

    public int getDiscardingCount() {
        return discardingCount;
    }

    public int getDiscardingReason(int slotIndex) {
        if (slotKind[slotIndex] != SLOT_DISCARDING_BLOCK) {
            throw new IllegalStateException("getDiscardingReason on slot kind=" + slotKind[slotIndex]);
        }
        return slotReason[slotIndex];
    }

    /**
     * Returns the {@link Http2Stream} backing the slot. The caller must
     * already have verified {@link #getSlotKind} is {@link #SLOT_LIVE};
     * the pool does not re-check to keep the dispatch path branchless.
     */
    public Http2Stream getLiveStream(int slotIndex) {
        return slotStream[slotIndex];
    }

    public int getMaxConcurrentStreams() {
        return maxConcurrentStreams;
    }

    public int getSlotCount() {
        return slotCount;
    }

    public byte getSlotKind(int slotIndex) {
        return slotKind[slotIndex];
    }

    public int getSlotStreamId(int slotIndex) {
        return slotStreamId[slotIndex];
    }

    public byte getTombstoneCloseKind(int slotIndex) {
        if (slotKind[slotIndex] != SLOT_TOMBSTONE) {
            throw new IllegalStateException("getTombstoneCloseKind on slot kind=" + slotKind[slotIndex]);
        }
        return slotCloseKind[slotIndex];
    }

    public int getTombstoneCap() {
        return tombstoneCap;
    }

    public int getTombstoneCount() {
        return tombstoneCount;
    }

    /**
     * Returns the slot index for {@code streamId} across all kinds
     * (LIVE, DISCARDING_BLOCK, TOMBSTONE), or {@link #SLOT_NOT_FOUND}
     * if the id has rolled off. The caller branches on
     * {@link #getSlotKind} per §5 dispatch precedence.
     */
    public int lookup(int streamId) {
        return idToSlot.get(streamId);
    }

    private void evictOldestTombstone() {
        if (tombstoneCount == 0) {
            return;
        }
        // Linear scan over `slotCount` entries; default tombstoneCap is 100
        // and the scan runs at most once per close, so the constant factor
        // is negligible against the work of a request round-trip.
        int oldest = -1;
        long minTick = Long.MAX_VALUE;
        for (int i = 0; i < slotCount; i++) {
            if (slotKind[i] == SLOT_TOMBSTONE && slotCloseTick[i] < minTick) {
                minTick = slotCloseTick[i];
                oldest = i;
            }
        }
        if (oldest >= 0) {
            releaseSlot(oldest);
            tombstoneCount--;
        }
    }

    private void releaseSlot(int slotIndex) {
        int id = slotStreamId[slotIndex];
        if (id >= 0) {
            idToSlot.remove(id);
        }
        slotKind[slotIndex] = SLOT_FREE;
        slotStreamId[slotIndex] = -1;
        slotReason[slotIndex] = 0;
        slotBlockBytes[slotIndex] = 0;
        slotCloseTick[slotIndex] = 0;
        slotCloseKind[slotIndex] = 0;
        slotStream[slotIndex].unassign();
        freeList[freeCount++] = slotIndex;
    }

    private int takeSlot() {
        return freeList[--freeCount];
    }
}
