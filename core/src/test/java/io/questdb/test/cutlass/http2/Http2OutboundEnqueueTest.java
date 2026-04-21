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

import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2ConnectionConfig;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2HeadersWriter;
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.cutlass.http2.Http2StreamPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Phase A PR2 coverage: {@code Http2ConnectionContext#enqueueData} and
 * {@code emitResponseHeaders} plus the three-sentinel return surface
 * ({@code ENQUEUE_STALE_GENERATION}, {@code ENQUEUE_STREAM_CLOSED},
 * {@code ENQUEUE_PARK}). Exercises Test 7 (per-stream cap overrun →
 * {@code ENQUEUE_PARK}) and Test 8 (stale generation is a silent no-op
 * for {@code enqueueData}, {@code onBytesConsumed}, and
 * {@code emitResponseHeaders}) from {@code HTTP2_INTEGRATION.md} §15.4.
 * <p>
 * The {@code onBytesConsumed} arm of Test 8 lives in
 * {@code Http2ConnectionContextTest.testOnBytesConsumedStaleGenerationIsNoOp}.
 */
public class Http2OutboundEnqueueTest {

    // :status: 200 via the static table. Index 8 encodes to a single
    // 0x88 byte per RFC 7541 sec. 6.1.
    private static final int STATUS_200 = HpackEncoder.HINT_STATIC_INDEX | 8;

    @Test
    public void testEmitResponseHeadersHeadersOnlyEndStream() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            int r = f.ctx.emitResponseHeaders(1, gen, smallStatus200(), true);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK, r);
            // Single HEADERS tuple carrying both END_HEADERS and END_STREAM.
            Assert.assertEquals(1, s.getOutboundTupleCount());
            Assert.assertEquals(Http2Stream.TUPLE_KIND_HEADERS, s.peekOutboundKind());
            byte flags = s.peekOutboundFlags();
            Assert.assertEquals(Http2Stream.TUPLE_FLAG_END_HEADERS,
                    flags & Http2Stream.TUPLE_FLAG_END_HEADERS);
            Assert.assertEquals(Http2Stream.TUPLE_FLAG_END_STREAM,
                    flags & Http2Stream.TUPLE_FLAG_END_STREAM);
            Assert.assertTrue(s.isOutboundEndStreamStaged());
            // Encoded body: HPACK M1 block starts with pin-to-0 size update (0x20)
            // plus the 0x88 indexed :status: 200 = 2 bytes.
            Assert.assertEquals(2, s.peekOutboundPayloadLen());
        }
    }

    @Test
    public void testEmitResponseHeadersParkedOnArenaOverrun() {
        // Tight arena: 16 bytes can hold the pin-to-0 prefix + :status: 200
        // but not a second block until the first tuple drains.
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOutboundArenaBytesPerStream(16)
                .withOutboundTupleQueueCap(4)
                .build();
        try (Fixture f = new Fixture(cfg)) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    f.ctx.emitResponseHeaders(1, gen, smallStatus200(), false));
            int tupleCountAfterFirst = s.getOutboundTupleCount();
            int queuedAfterFirst = s.getOutboundQueuedPayloadBytes();
            // Fill the rest of the arena with a DATA enqueue so the next
            // emit has no room for the pin-cleared second block.
            int remaining = s.getOutboundArenaCap() - queuedAfterFirst;
            long payload = Unsafe.malloc(remaining, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, remaining, false));
                // Now the arena is full; a second emit parks.
                int r = f.ctx.emitResponseHeaders(1, gen, smallStatus200(), true);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK, r);
                Assert.assertTrue(s.isOutboundParked());
                // No new tuple appended.
                Assert.assertEquals(tupleCountAfterFirst + 1, s.getOutboundTupleCount());
                Assert.assertFalse(s.isOutboundEndStreamStaged());
            } finally {
                Unsafe.free(payload, remaining, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEmitResponseHeadersReentrancyThrows() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            final int streamIdHolder = 1;
            final Http2ConnectionContext ctxHolder = f.ctx;
            try {
                ctxHolder.emitResponseHeaders(streamIdHolder, gen, (encoder, cursor, limit) -> {
                    // Illegal reentrancy from within the writer callback.
                    ctxHolder.emitResponseHeaders(streamIdHolder, gen, smallStatus200(), true);
                    return cursor;
                }, true);
                Assert.fail("expected IllegalStateException on reentrant emitResponseHeaders");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("re-entered"));
            }
            // Stream's encoder state is restored — a subsequent normal
            // emit still succeeds.
            Assert.assertFalse(s.isOutboundEndStreamStaged());
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    f.ctx.emitResponseHeaders(1, gen, smallStatus200(), true));
        }
    }

    @Test
    public void testEmitResponseHeadersStaleGenerationIsSilentNoOp() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            int tupleCountBefore = s.getOutboundTupleCount();
            int r = f.ctx.emitResponseHeaders(1, gen - 1, smallStatus200(), true);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STALE_GENERATION, r);
            Assert.assertEquals(tupleCountBefore, s.getOutboundTupleCount());
            Assert.assertFalse(s.isOutboundEndStreamStaged());
            Assert.assertFalse(s.isOutboundParked());
        }
    }

    @Test
    public void testEmitResponseHeadersStreamClosedAfterTerminator() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    f.ctx.emitResponseHeaders(1, gen, smallStatus200(), true));
            int tupleCountAfter = s.getOutboundTupleCount();
            // Second call is rejected — terminator already staged.
            int r = f.ctx.emitResponseHeaders(1, gen, smallStatus200(), false);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STREAM_CLOSED, r);
            Assert.assertEquals(tupleCountAfter, s.getOutboundTupleCount());
        }
    }

    @Test
    public void testEmitResponseHeadersUnknownStreamStale() {
        try (Fixture f = new Fixture()) {
            int r = f.ctx.emitResponseHeaders(999, 0, smallStatus200(), true);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STALE_GENERATION, r);
        }
    }

    @Test
    public void testEnqueueDataHappyPathQueuesDataTuple() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 32; i++) {
                    Unsafe.getUnsafe().putByte(payload + i, (byte) i);
                }
                int r = f.ctx.enqueueData(1, gen, payload, 32, false);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK, r);
                Assert.assertEquals(1, s.getOutboundTupleCount());
                Assert.assertEquals(Http2Stream.TUPLE_KIND_DATA, s.peekOutboundKind());
                Assert.assertEquals((byte) 0, s.peekOutboundFlags());
                Assert.assertEquals(32, s.peekOutboundPayloadLen());
                Assert.assertFalse(s.isOutboundEndStreamStaged());
                // Mutating caller buffer must not disturb the queued bytes.
                Unsafe.getUnsafe().putByte(payload, (byte) 0xFF);
                long addr = s.peekOutboundPayloadAddr();
                Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(addr));
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataParkOnArenaOverrun() {
        // This is Test 7 from §15.4 A.7: per-stream cap overrun returns
        // ENQUEUE_PARK and does not accept more bytes.
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOutboundArenaBytesPerStream(64)
                .withOutboundTupleQueueCap(4)
                .build();
        try (Fixture f = new Fixture(cfg)) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            long payload = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 64; i++) {
                    Unsafe.getUnsafe().putByte(payload + i, (byte) i);
                }
                // First enqueue fills the arena exactly.
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, 64, false));
                Assert.assertFalse(s.isOutboundParked());
                // Second enqueue would overrun the 64-byte arena → PARK.
                int r = f.ctx.enqueueData(1, gen, payload, 1, false);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK, r);
                Assert.assertTrue(s.isOutboundParked());
                // Queue state unchanged by the parked call.
                Assert.assertEquals(1, s.getOutboundTupleCount());
                Assert.assertEquals(64, s.getOutboundQueuedPayloadBytes());
                // Further parked enqueues don't corrupt state either.
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK,
                        f.ctx.enqueueData(1, gen, payload, 1, false));
                Assert.assertEquals(1, s.getOutboundTupleCount());
                Assert.assertEquals(64, s.getOutboundQueuedPayloadBytes());
            } finally {
                Unsafe.free(payload, 64, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataParkOnTupleRingFull() {
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOutboundArenaBytesPerStream(1024)
                .withOutboundTupleQueueCap(2)
                .build();
        try (Fixture f = new Fixture(cfg)) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            long payload = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, 4, false));
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, 4, false));
                // Ring full — third enqueue parks.
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK,
                        f.ctx.enqueueData(1, gen, payload, 4, false));
                Assert.assertTrue(s.isOutboundParked());
                Assert.assertEquals(2, s.getOutboundTupleCount());
            } finally {
                Unsafe.free(payload, 4, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataRecycledSlotGenerationIsStale() {
        // After the state machine closes a LIVE slot (clean close) the
        // tombstone promotion bumps the generation, so the handler's
        // captured token no longer matches.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int staleGen = s.getGeneration();
            Http2StreamPool pool = f.ctx.getStreamPool();
            int slot = pool.lookup(1);
            // Drive the FSM past the close edge via a send-HEADERS
            // END_STREAM on HALF_CLOSED_REMOTE is awkward from here, so
            // just RST locally.
            pool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
            long payload = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                // Stale token now: slot is TOMBSTONE, gen bumped.
                int r = f.ctx.enqueueData(1, staleGen, payload, 4, false);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STALE_GENERATION, r);
            } finally {
                Unsafe.free(payload, 4, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataRejectsNegativePayloadLen() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            try {
                f.ctx.enqueueData(1, gen, 0L, -1, false);
                Assert.fail();
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    @Test
    public void testEnqueueDataStaleGenerationIsSilentNoOp() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            int tupleCountBefore = s.getOutboundTupleCount();
            int queuedBefore = s.getOutboundQueuedPayloadBytes();
            long payload = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                // Stale token → silent no-op per §7 step 6.
                int r = f.ctx.enqueueData(1, gen - 1, payload, 4, false);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STALE_GENERATION, r);
                Assert.assertEquals(tupleCountBefore, s.getOutboundTupleCount());
                Assert.assertEquals(queuedBefore, s.getOutboundQueuedPayloadBytes());
                Assert.assertFalse(s.isOutboundParked());
            } finally {
                Unsafe.free(payload, 4, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataStreamClosedAfterTerminator() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            long payload = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, 4, true));
                Assert.assertTrue(s.isOutboundEndStreamStaged());
                int r = f.ctx.enqueueData(1, gen, payload, 4, false);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STREAM_CLOSED, r);
                Assert.assertEquals(1, s.getOutboundTupleCount());
            } finally {
                Unsafe.free(payload, 4, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataStreamClosedOnFrozenOutbound() {
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            // Drive the stream's outbound direction closed: our send END_STREAM
            // on OPEN → HALF_CLOSED_LOCAL (outbound frozen).
            Assert.assertTrue(s.onSendDataEndStream());
            Assert.assertFalse(s.isOutboundDirectionActive());
            long payload = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                int r = f.ctx.enqueueData(1, gen, payload, 4, false);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STREAM_CLOSED, r);
            } finally {
                Unsafe.free(payload, 4, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataUnknownStreamStale() {
        try (Fixture f = new Fixture()) {
            long payload = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                int r = f.ctx.enqueueData(999, 0, payload, 4, false);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_STALE_GENERATION, r);
            } finally {
                Unsafe.free(payload, 4, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEnqueueDataZeroLengthWithEndStreamLegal() {
        // Zero-length DATA with END_STREAM is the canonical "response body
        // done" terminator under a drained window (RFC 9113 §6.9.1).
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            int r = f.ctx.enqueueData(1, gen, 0L, 0, true);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK, r);
            Assert.assertEquals(1, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.peekOutboundPayloadLen());
            Assert.assertTrue(s.isOutboundEndStreamStaged());
        }
    }

    private static Http2HeadersWriter smallStatus200() {
        return (encoder, cursor, limit) -> encoder.encode(cursor, limit, 0L, 0, 0L, 0, STATUS_200);
    }

    /**
     * Swallow-all stream listener that lets a fixture exercise the engine
     * without hand-rolling wire frames.
     */
    private static final class NoopListener implements Http2StreamListener {
        @Override
        public boolean onData(int streamId, long addr, int dataLen, boolean endStream, int generationToken) {
            return true;
        }

        @Override
        public void onRequestHeader(int streamId, long nameAddr, int nameLen,
                                    long valueAddr, int valueLen, boolean neverIndexed) {
        }

        @Override
        public void onRequestHeaders(int streamId, Http2RequestHeadersView view, boolean endStream) {
        }

        @Override
        public void onStreamClosed(int streamId, int cause) {
        }

        @Override
        public void onStreamWritable(int streamId) {
        }

        @Override
        public void onTrailers(int streamId, boolean endStream) {
        }
    }

    private static final class Fixture implements AutoCloseable {

        final Http2ConnectionContext ctx;

        Fixture() {
            this(Http2ConnectionConfig.defaults());
        }

        Fixture(Http2ConnectionConfig cfg) {
            this.ctx = new Http2ConnectionContext(new NoopListener(), cfg);
        }

        @Override
        public void close() {
            ctx.close();
        }

        Http2Stream openStream(int streamId) {
            Http2StreamPool pool = ctx.getStreamPool();
            int slot = pool.allocateLive(streamId, 65_535, 65_535);
            Http2Stream s = pool.getLiveStream(slot);
            // Advance past IDLE so enqueueData / emitResponseHeaders can see
            // OPEN outbound-direction-active.
            Assert.assertTrue(s.onRecvHeaders(false));
            return s;
        }
    }
}
