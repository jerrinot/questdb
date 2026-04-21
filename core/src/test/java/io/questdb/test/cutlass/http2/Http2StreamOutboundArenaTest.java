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

import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for the per-stream outbound response arena and tuple ring
 * added in {@code HTTP2_INTEGRATION.md} §15.4 step A.1. Drives
 * {@link Http2Stream} directly; the scheduler / enqueue-sentinel surface
 * in {@code Http2ConnectionContext} lands in PR2 / PR3.
 */
public class Http2StreamOutboundArenaTest {

    private static final int ARENA_CAP = 1024;
    private static final int TUPLE_CAP = 8;

    @Test
    public void testArenaAllocatedWithPositiveCaps() {
        long beforeNative = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        try {
            Assert.assertEquals(ARENA_CAP, s.getOutboundArenaCap());
            Assert.assertEquals(TUPLE_CAP, s.getOutboundTupleCap());
            Assert.assertEquals(0, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());
            Assert.assertFalse(s.isOutboundEndStreamStaged());
            Assert.assertFalse(s.isOutboundParked());
            Assert.assertNotEquals(0L, s.getOutboundArenaAddr());
            // Arena allocation is visible in the native counter.
            long afterNative = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            Assert.assertEquals(ARENA_CAP, afterNative - beforeNative);
        } finally {
            s.close();
        }
        // Arena freed back to the allocator.
        Assert.assertEquals(beforeNative, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
    }

    @Test
    public void testArenaNotAllocatedForZeroCapStream() {
        Http2Stream s = new Http2Stream();
        try {
            Assert.assertEquals(0, s.getOutboundArenaCap());
            Assert.assertEquals(0, s.getOutboundTupleCap());
            Assert.assertEquals(0L, s.getOutboundArenaAddr());
            // Enqueue on a zero-arena stream returns false without mutating state.
            long src = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putInt(src, 0xCAFEBABE);
                Assert.assertFalse(s.tryEnqueueOutbound(
                        Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 4));
            } finally {
                Unsafe.free(src, 4, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, s.getOutboundTupleCount());
        } finally {
            s.close();
        }
    }

    @Test
    public void testClearOutboundQueueResetsEverything() {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        long src = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            fillIncreasing(src, 32);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_HEADERS,
                    (byte) (Http2Stream.TUPLE_FLAG_END_HEADERS | Http2Stream.TUPLE_FLAG_END_STREAM),
                    src, 16));
            s.setOutboundParked(true);
            Assert.assertTrue(s.isOutboundParked());
            Assert.assertTrue(s.isOutboundEndStreamStaged());

            s.clearOutboundQueue();
            Assert.assertEquals(0, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());
            Assert.assertFalse(s.isOutboundEndStreamStaged());
            Assert.assertFalse(s.isOutboundParked());
            // Cursor reset lets a subsequent large enqueue succeed again.
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 32));
        } finally {
            Unsafe.free(src, 32, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
    }

    @Test
    public void testCloseIsIdempotent() {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        s.close();
        s.close(); // second close must not double-free.
        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
    }

    @Test
    public void testConstructorRejectsMixedCaps() {
        try {
            new Http2Stream(0, 4).close();
            Assert.fail();
        } catch (IllegalArgumentException expected) {
        }
        try {
            new Http2Stream(ARENA_CAP, 0).close();
            Assert.fail();
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testConstructorRejectsNegativeCaps() {
        try {
            new Http2Stream(-1, 0).close();
            Assert.fail();
        } catch (IllegalArgumentException expected) {
        }
        try {
            new Http2Stream(0, -1).close();
            Assert.fail();
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testDequeueEmptyThrows() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        try {
            try {
                s.dequeueOutbound();
                Assert.fail();
            } catch (IllegalStateException expected) {
            }
        } finally {
            s.close();
        }
    }

    @Test
    public void testDequeueFifoOrder() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        long src = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(src, (byte) 0x11);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_HEADERS, Http2Stream.TUPLE_FLAG_END_HEADERS, src, 1));
            Unsafe.getUnsafe().putByte(src, (byte) 0x22);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 1));
            Unsafe.getUnsafe().putByte(src, (byte) 0x33);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, Http2Stream.TUPLE_FLAG_END_STREAM, src, 1));

            Assert.assertEquals(3, s.getOutboundTupleCount());
            Assert.assertEquals(Http2Stream.TUPLE_KIND_HEADERS, s.peekOutboundKind());
            Assert.assertEquals(0x11, Unsafe.getUnsafe().getByte(s.peekOutboundPayloadAddr()));
            s.dequeueOutbound();

            Assert.assertEquals(Http2Stream.TUPLE_KIND_DATA, s.peekOutboundKind());
            Assert.assertEquals((byte) 0, s.peekOutboundFlags());
            Assert.assertEquals(0x22, Unsafe.getUnsafe().getByte(s.peekOutboundPayloadAddr()));
            s.dequeueOutbound();

            Assert.assertEquals(Http2Stream.TUPLE_KIND_DATA, s.peekOutboundKind());
            Assert.assertEquals(Http2Stream.TUPLE_FLAG_END_STREAM, s.peekOutboundFlags());
            Assert.assertEquals(0x33, Unsafe.getUnsafe().getByte(s.peekOutboundPayloadAddr()));
            s.dequeueOutbound();

            Assert.assertEquals(0, s.getOutboundTupleCount());
        } finally {
            Unsafe.free(src, 8, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testDequeueResetsBumpCursorOnEmpty() {
        // 100 bytes arena / 2-tuple ring. First enqueue fills the tail; a
        // second enqueue would fail against the tail, but after draining
        // the queue the bump cursor resets and the same-size write fits.
        Http2Stream s = new Http2Stream(100, 2);
        long src = Unsafe.malloc(80, MemoryTag.NATIVE_DEFAULT);
        try {
            fillIncreasing(src, 80);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 80));
            // Not enough tail room for another 80-byte write while queue non-empty.
            Assert.assertFalse(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 80));
            Assert.assertEquals(1, s.getOutboundTupleCount());

            s.dequeueOutbound();
            Assert.assertEquals(0, s.getOutboundTupleCount());
            // Cursor reset — the same-size write now fits again.
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 80));
        } finally {
            Unsafe.free(src, 80, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testEnqueueAfterCloseReturnsFalse() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        long src = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putLong(src, 0xDEADBEEFL);
            s.close();
            Assert.assertFalse(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 8));
        } finally {
            Unsafe.free(src, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEnqueueCopiesBytesAndAdvancesState() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            fillIncreasing(src, 64);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 64));
            Assert.assertEquals(1, s.getOutboundTupleCount());
            Assert.assertEquals(64, s.getOutboundQueuedPayloadBytes());
            Assert.assertEquals(Http2Stream.TUPLE_KIND_DATA, s.peekOutboundKind());
            Assert.assertEquals((byte) 0, s.peekOutboundFlags());
            Assert.assertEquals(64, s.peekOutboundPayloadLen());

            // The arena owns its own copy — mutating the caller's buffer
            // must not disturb the queued tuple. §4.4 copy-on-enqueue
            // ownership contract.
            Unsafe.getUnsafe().putByte(src, (byte) 0xFF);
            long peekAddr = s.peekOutboundPayloadAddr();
            for (int i = 0; i < 64; i++) {
                Assert.assertEquals(
                        "byte " + i + " mutated in arena after caller overwrote src",
                        (byte) i, Unsafe.getUnsafe().getByte(peekAddr + i));
            }
        } finally {
            Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testEnqueueEndStreamBlocksSubsequentEnqueues() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        long src = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putLong(src, 0xC0FFEEL);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, Http2Stream.TUPLE_FLAG_END_STREAM, src, 8));
            Assert.assertTrue(s.isOutboundEndStreamStaged());
            // Second enqueue rejected silently — no state change, no bytes copied.
            int countBefore = s.getOutboundTupleCount();
            int queuedBefore = s.getOutboundQueuedPayloadBytes();
            Assert.assertFalse(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 8));
            Assert.assertEquals(countBefore, s.getOutboundTupleCount());
            Assert.assertEquals(queuedBefore, s.getOutboundQueuedPayloadBytes());
        } finally {
            Unsafe.free(src, 8, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testEnqueueFlagBitsPreserved() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        long src = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            byte flags = (byte) (Http2Stream.TUPLE_FLAG_END_HEADERS | Http2Stream.TUPLE_FLAG_END_STREAM);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_HEADERS, flags, src, 0));
            Assert.assertEquals(flags, s.peekOutboundFlags());
            Assert.assertTrue(s.isOutboundEndStreamStaged());
        } finally {
            Unsafe.free(src, 4, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testEnqueueRejectsFragmentedTail() {
        // arena = 100. Two 40-byte writes leave writeOffset at 80; 20 bytes
        // free at tail. A 30-byte write would need to wrap the arena, but
        // the queue is non-empty so wrap is forbidden and the enqueue parks.
        Http2Stream s = new Http2Stream(100, TUPLE_CAP);
        long src = Unsafe.malloc(40, MemoryTag.NATIVE_DEFAULT);
        try {
            fillIncreasing(src, 40);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 40));
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 40));
            // queuedPayloadBytes == 80, 20 bytes free at tail.
            Assert.assertFalse(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 30));
            Assert.assertEquals(2, s.getOutboundTupleCount());
            Assert.assertEquals(80, s.getOutboundQueuedPayloadBytes());
        } finally {
            Unsafe.free(src, 40, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testEnqueueRejectsInvalidKind() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        try {
            try {
                s.tryEnqueueOutbound((byte) 0, (byte) 0, 0L, 0);
                Assert.fail();
            } catch (IllegalArgumentException expected) {
            }
            try {
                s.tryEnqueueOutbound((byte) 42, (byte) 0, 0L, 0);
                Assert.fail();
            } catch (IllegalArgumentException expected) {
            }
        } finally {
            s.close();
        }
    }

    @Test
    public void testEnqueueRejectsNegativePayloadLen() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        try {
            try {
                s.tryEnqueueOutbound(Http2Stream.TUPLE_KIND_DATA, (byte) 0, 0L, -1);
                Assert.fail();
            } catch (IllegalArgumentException expected) {
            }
        } finally {
            s.close();
        }
    }

    @Test
    public void testEnqueueReturnsFalseWhenPayloadExceedsArenaCap() {
        Http2Stream s = new Http2Stream(64, TUPLE_CAP);
        long src = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        try {
            fillIncreasing(src, 128);
            Assert.assertFalse(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 128));
            Assert.assertEquals(0, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());
        } finally {
            Unsafe.free(src, 128, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testEnqueueReturnsFalseWhenRingFull() {
        Http2Stream s = new Http2Stream(ARENA_CAP, 2);
        long src = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(src, 0);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 4));
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 4));
            // Third enqueue rejected by the 2-entry ring.
            Assert.assertFalse(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 4));
            Assert.assertEquals(2, s.getOutboundTupleCount());
        } finally {
            Unsafe.free(src, 4, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testEnqueueZeroLengthAllowed() {
        // A zero-length DATA with END_STREAM is the canonical "close after
        // window drained" case (RFC 9113 §6.9.1 — not flow-controlled).
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        try {
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, Http2Stream.TUPLE_FLAG_END_STREAM, 0L, 0));
            Assert.assertEquals(1, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());
            Assert.assertEquals(0, s.peekOutboundPayloadLen());
            Assert.assertTrue(s.isOutboundEndStreamStaged());
        } finally {
            s.close();
        }
    }

    @Test
    public void testPeekEmptyThrows() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        try {
            try {
                s.peekOutboundKind();
                Assert.fail();
            } catch (IllegalStateException expected) {
            }
            try {
                s.peekOutboundFlags();
                Assert.fail();
            } catch (IllegalStateException expected) {
            }
            try {
                s.peekOutboundPayloadAddr();
                Assert.fail();
            } catch (IllegalStateException expected) {
            }
            try {
                s.peekOutboundPayloadLen();
                Assert.fail();
            } catch (IllegalStateException expected) {
            }
        } finally {
            s.close();
        }
    }

    @Test
    public void testPoolCloseFreesEveryStreamArena() {
        // Pool with 4 LIVE slots + 2 tombstones + 2 discarding reserve = 8 streams
        // each holding a 1 KiB arena. After pool.close() the NATIVE_DEFAULT
        // tag returns to baseline regardless of which slots were LIVE.
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        Http2StreamPool pool = new Http2StreamPool(4, 2, 2, 1024, TUPLE_CAP);
        try {
            long afterCtor = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            Assert.assertEquals(8L * 1024, afterCtor - before);
            pool.allocateLive(1, 65_535, 65_535);
            pool.allocateLive(3, 65_535, 65_535);
        } finally {
            pool.close();
        }
        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
    }

    @Test
    public void testPoolCloseIsIdempotent() {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        Http2StreamPool pool = new Http2StreamPool(2, 0, 1, 512, TUPLE_CAP);
        pool.close();
        pool.close(); // no double-free.
        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
    }

    @Test
    public void testRecycleClearsOutboundQueue() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        long src = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            fillIncreasing(src, 16);
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, Http2Stream.TUPLE_FLAG_END_STREAM, src, 16));
            s.setOutboundParked(true);
            Assert.assertTrue(s.isOutboundParked());
            Assert.assertTrue(s.isOutboundEndStreamStaged());

            s.recycle(5, 1_000L, 2_000L);
            Assert.assertEquals(0, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());
            Assert.assertFalse(s.isOutboundEndStreamStaged());
            Assert.assertFalse(s.isOutboundParked());
            // Fresh enqueue still works on the recycled stream.
            Assert.assertTrue(s.tryEnqueueOutbound(
                    Http2Stream.TUPLE_KIND_DATA, (byte) 0, src, 16));
        } finally {
            Unsafe.free(src, 16, MemoryTag.NATIVE_DEFAULT);
            s.close();
        }
    }

    @Test
    public void testSetOutboundParkedIsObservable() {
        Http2Stream s = new Http2Stream(ARENA_CAP, TUPLE_CAP);
        try {
            Assert.assertFalse(s.isOutboundParked());
            s.setOutboundParked(true);
            Assert.assertTrue(s.isOutboundParked());
            s.setOutboundParked(false);
            Assert.assertFalse(s.isOutboundParked());
        } finally {
            s.close();
        }
    }

    private static void fillIncreasing(long addr, int len) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(addr + i, (byte) i);
        }
    }
}
