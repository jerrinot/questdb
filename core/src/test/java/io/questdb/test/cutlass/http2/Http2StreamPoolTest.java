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

package io.questdb.test.cutlass.http2;

import io.questdb.cutlass.http2.Http2ErrorCode;
import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamPool;
import org.junit.Assert;
import org.junit.Test;

public class Http2StreamPoolTest {

    @Test
    public void testAllocateDiscardingBlock() {
        Http2StreamPool pool = new Http2StreamPool(4, 4);
        int slot = pool.allocateDiscardingBlock(1, Http2ErrorCode.REFUSED_STREAM);
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, slot);
        Assert.assertEquals(Http2StreamPool.SLOT_DISCARDING_BLOCK, pool.getSlotKind(slot));
        Assert.assertEquals(1, pool.getSlotStreamId(slot));
        Assert.assertEquals(Http2ErrorCode.REFUSED_STREAM, pool.getDiscardingReason(slot));
        Assert.assertEquals(0, pool.getActiveStreamCount()); // DISCARDING doesn't count
        Assert.assertEquals(1, pool.getDiscardingCount());
        Assert.assertEquals(slot, pool.lookup(1));

        pool.appendDiscardingBlockBytes(slot, 128);
        pool.appendDiscardingBlockBytes(slot, 64);
        Assert.assertEquals(192, pool.getDiscardingBlockBytes(slot));
    }

    @Test
    public void testCleanCloseProducesCleanTombstone() {
        Http2StreamPool pool = new Http2StreamPool(2, 2);
        int slot = pool.allocateLive(1, 65_535, 65_535);
        pool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
        Assert.assertEquals(Http2StreamPool.SLOT_TOMBSTONE, pool.getSlotKind(slot));
        Assert.assertEquals(Http2StreamPool.CLOSE_CLEAN, pool.getTombstoneCloseKind(slot));
    }

    @Test
    public void testClear() {
        Http2StreamPool pool = new Http2StreamPool(2, 2);
        pool.allocateLive(1, 65_535, 65_535);
        pool.allocateLive(3, 65_535, 65_535);
        pool.clear();
        Assert.assertEquals(0, pool.getActiveStreamCount());
        Assert.assertEquals(0, pool.getTombstoneCount());
        Assert.assertEquals(0, pool.getDiscardingCount());
        Assert.assertEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.lookup(1));
        Assert.assertEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.lookup(3));
        // Can allocate fresh after clear.
        int slot = pool.allocateLive(5, 65_535, 65_535);
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, slot);
    }

    @Test
    public void testCloseLiveTransitionsToTombstoneAndBumpsGeneration() {
        Http2StreamPool pool = new Http2StreamPool(4, 4);
        int slot = pool.allocateLive(1, 65_535, 65_535);
        int genBeforeClose = pool.getLiveStream(slot).getGeneration();
        pool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
        Assert.assertEquals(Http2StreamPool.SLOT_TOMBSTONE, pool.getSlotKind(slot));
        Assert.assertEquals(Http2StreamPool.CLOSE_CLEAN, pool.getTombstoneCloseKind(slot));
        Assert.assertEquals(0, pool.getActiveStreamCount());
        Assert.assertEquals(1, pool.getTombstoneCount());
        Assert.assertEquals(slot, pool.lookup(1));
        // Tombstone LIVE→TOMBSTONE bump: §4 invariant "generation incremented
        // on every LIVE → tombstone promotion".
        Http2Stream s = pool.getLiveStream(slot);
        Assert.assertTrue(s.getGeneration() > genBeforeClose);
    }

    @Test
    public void testCloseLiveWithZeroTombstoneCapReleasesImmediately() {
        Http2StreamPool pool = new Http2StreamPool(2, 0);
        int slot = pool.allocateLive(1, 65_535, 65_535);
        pool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
        Assert.assertEquals(Http2StreamPool.SLOT_FREE, pool.getSlotKind(slot));
        Assert.assertEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.lookup(1));
        Assert.assertEquals(0, pool.getTombstoneCount());
        // Slot reusable.
        int slot2 = pool.allocateLive(3, 65_535, 65_535);
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, slot2);
    }

    @Test
    public void testLiveSlotAllocationAndLookup() {
        Http2StreamPool pool = new Http2StreamPool(4, 4);
        int slot = pool.allocateLive(1, 65_535, 65_535);
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, slot);
        Assert.assertEquals(Http2StreamPool.SLOT_LIVE, pool.getSlotKind(slot));
        Assert.assertEquals(1, pool.getSlotStreamId(slot));
        Assert.assertEquals(1, pool.getActiveStreamCount());
        Assert.assertEquals(slot, pool.lookup(1));

        Http2Stream s = pool.getLiveStream(slot);
        Assert.assertEquals(1, s.getStreamId());
        Assert.assertEquals(65_535L, s.getInboundStreamWindow());
        Assert.assertEquals(65_535L, s.getOutboundStreamWindow());
    }

    @Test
    public void testMultipleStreamsAndCloses() {
        Http2StreamPool pool = new Http2StreamPool(4, 4);
        int s1 = pool.allocateLive(1, 65_535, 65_535);
        int s3 = pool.allocateLive(3, 65_535, 65_535);
        int s5 = pool.allocateLive(5, 65_535, 65_535);
        Assert.assertEquals(3, pool.getActiveStreamCount());
        Assert.assertEquals(s1, pool.lookup(1));
        Assert.assertEquals(s3, pool.lookup(3));
        Assert.assertEquals(s5, pool.lookup(5));

        pool.closeLiveSlot(s1, Http2StreamPool.CLOSE_CLEAN);
        Assert.assertEquals(2, pool.getActiveStreamCount());
        Assert.assertEquals(1, pool.getTombstoneCount());
        Assert.assertEquals(s1, pool.lookup(1)); // tombstone still resolvable

        pool.closeLiveSlot(s5, Http2StreamPool.CLOSE_LOCAL_RESET);
        Assert.assertEquals(1, pool.getActiveStreamCount());
        Assert.assertEquals(2, pool.getTombstoneCount());
    }

    @Test
    public void testPromoteDiscardingToTombstone() {
        Http2StreamPool pool = new Http2StreamPool(2, 2);
        int slot = pool.allocateDiscardingBlock(9, Http2ErrorCode.REFUSED_STREAM);
        Assert.assertEquals(1, pool.getDiscardingCount());
        Assert.assertEquals(0, pool.getTombstoneCount());
        pool.promoteDiscardingToTombstone(slot);
        Assert.assertEquals(0, pool.getDiscardingCount());
        Assert.assertEquals(1, pool.getTombstoneCount());
        Assert.assertEquals(Http2StreamPool.SLOT_TOMBSTONE, pool.getSlotKind(slot));
        Assert.assertEquals(Http2StreamPool.CLOSE_LOCAL_RESET, pool.getTombstoneCloseKind(slot));
        Assert.assertEquals(slot, pool.lookup(9));
    }

    @Test
    public void testRefuseOverMaxConcurrentStreams() {
        Http2StreamPool pool = new Http2StreamPool(3, 2);
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.allocateLive(1, 1, 1));
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.allocateLive(3, 1, 1));
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.allocateLive(5, 1, 1));
        Assert.assertEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.allocateLive(7, 1, 1));
        Assert.assertEquals(3, pool.getActiveStreamCount());
    }

    @Test
    public void testReuseSlotAfterCleanClose() {
        // tombstoneCap=0 skips the grace window; the closed slot returns to
        // the free-list immediately. With maxConcurrentStreams=1 the
        // subsequent allocate must land in the same slot, proving the
        // generation bump is observable through a recycle.
        Http2StreamPool pool = new Http2StreamPool(1, 0);
        int slotA = pool.allocateLive(1, 65_535, 65_535);
        int genA = pool.getLiveStream(slotA).getGeneration();
        pool.closeLiveSlot(slotA, Http2StreamPool.CLOSE_CLEAN);
        int slotB = pool.allocateLive(3, 65_535, 65_535);
        Assert.assertEquals("pool with maxConcurrentStreams=1 and tombstoneCap=0 must reuse the slot",
                slotA, slotB);
        Http2Stream sB = pool.getLiveStream(slotB);
        Assert.assertEquals(3, sB.getStreamId());
        // recycle() alone bumps once. No LIVE→TOMBSTONE bump on tombstoneCap=0
        // (we skip the tombstone phase), so the delta is exactly 1.
        Assert.assertEquals(genA + 1, sB.getGeneration());
    }

    @Test
    public void testRollOffOldestTombstoneWhenCapReached() {
        Http2StreamPool pool = new Http2StreamPool(4, 2);
        int s1 = pool.allocateLive(1, 65_535, 65_535);
        int s3 = pool.allocateLive(3, 65_535, 65_535);
        int s5 = pool.allocateLive(5, 65_535, 65_535);
        pool.closeLiveSlot(s1, Http2StreamPool.CLOSE_CLEAN); // oldest tombstone
        pool.closeLiveSlot(s3, Http2StreamPool.CLOSE_CLEAN);
        Assert.assertEquals(2, pool.getTombstoneCount());
        // Third close must evict the oldest (stream 1).
        pool.closeLiveSlot(s5, Http2StreamPool.CLOSE_CLEAN);
        Assert.assertEquals(2, pool.getTombstoneCount());
        Assert.assertEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.lookup(1));
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.lookup(3));
        Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, pool.lookup(5));
    }
}
