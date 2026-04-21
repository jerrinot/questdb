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
import io.questdb.cutlass.http2.Http2ErrorCode;
import io.questdb.cutlass.http2.Http2Flags;
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameReader;
import io.questdb.cutlass.http2.Http2FrameType;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.http2.Http2HeadersWriter;
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2Settings;
import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.cutlass.http2.Http2StreamPool;
import io.questdb.cutlass.http2.Http2StreamState;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Phase A PR4 coverage:
 * <ul>
 *   <li>{@code HTTP2_INTEGRATION.md} §15.4 A.7 Test 9 — peer
 *       {@code RST_STREAM(CANCEL)} races queued HEADERS / DATA tuples;
 *       scheduler drops the queue, the per-stream arena bookkeeping
 *       resets, no further frames emit for the stream, and the slot
 *       cleanly recycles for a subsequent stream id.</li>
 *   <li>{@code onStreamWritable} triggers 3 (SETTINGS_INITIAL_WINDOW_SIZE
 *       increase) and 4 (scheduler-drain cap-free) that PR3 wired but did
 *       not test explicitly.</li>
 *   <li>Scheduler cursor resilience when it lands on a freshly-tombstoned
 *       slot mid-sweep.</li>
 * </ul>
 * Every test wraps the fixture lifecycle in a native-memory leak check
 * via {@link Unsafe#getMemUsedByTag}, confirming the pool-lifetime
 * arena decision (PR1) survives the RST path.
 */
public class Http2OutboundResetTest {

    private static final int BUF = 1 << 20;
    private static final int STATUS_200 = HpackEncoder.HINT_STATIC_INDEX | 8;

    @Test
    public void testOnStreamWritableFiresOnSchedulerDrainCapFree() {
        // Trigger 4: park the stream via cap-overrun, then let the
        // scheduler drain the queue back below cap. The emission path
        // fires onStreamWritable and clears the park flag.
        assertNoNativeLeak(() -> {
            Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                    .withOutboundArenaBytesPerStream(32)
                    .withOutboundTupleQueueCap(2)
                    .build();
            try (Fixture f = new Fixture(cfg)) {
                Http2Stream s = f.openStream(1);
                int gen = s.getGeneration();
                long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.enqueueData(1, gen, payload, 32, false));
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK,
                            f.ctx.enqueueData(1, gen, payload, 1, false));
                    Assert.assertTrue(s.isOutboundParked());
                    f.listener.events.clear();
                    long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long end = f.ctx.writePending(send, send + BUF);
                        Assert.assertTrue(end > send);
                    } finally {
                        Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                    }
                    Assert.assertFalse(s.isOutboundParked());
                    Assert.assertTrue(f.listener.events.contains("writable:1"));
                    Assert.assertEquals(0, s.getOutboundTupleCount());
                    Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());
                } finally {
                    Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testOnStreamWritableFiresOnSettingsInitialWindowIncrease() {
        // Trigger 3: park the stream via cap-overrun. Peer bumps
        // SETTINGS_INITIAL_WINDOW_SIZE (positive delta). Fan-out fires
        // onStreamWritable for the parked outbound-active stream.
        assertNoNativeLeak(() -> {
            Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                    .withOutboundArenaBytesPerStream(32)
                    .withOutboundTupleQueueCap(2)
                    .build();
            try (Fixture f = new Fixture(cfg)) {
                Http2Stream s = f.openStream(1);
                int gen = s.getGeneration();
                long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.enqueueData(1, gen, payload, 32, false));
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK,
                            f.ctx.enqueueData(1, gen, payload, 1, false));
                    Assert.assertTrue(s.isOutboundParked());
                    f.listener.events.clear();
                    f.sendPeerSettingsInitialWindowSize(100_000);
                    Assert.assertFalse(s.isOutboundParked());
                    Assert.assertTrue(f.listener.events.contains("writable:1"));
                } finally {
                    Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPeerRstStreamDropsQueuedTuplesAndRecyclesSlotClean() {
        // Test 9: RST_STREAM(CANCEL) lands while HEADERS + DATA tuples
        // are still in the stream's outbound queue. Assertions:
        //  - listener sees onStreamClosed with CANCEL cause;
        //  - the stream FSM reached CLOSED;
        //  - the outbound tuple ring and arena counter are reset;
        //  - writePending emits nothing for the dead stream;
        //  - the same slot recycles cleanly for a fresh stream id
        //    (generation bumped, outbound state zeroed);
        //  - no native memory leaks across the full open / RST / reuse cycle.
        assertNoNativeLeak(() -> {
            // tombstoneCap=0 makes the RST'd slot immediately reusable via
            // the free-list LIFO, so the slot-reuse arm of the test runs on
            // the same slot index as stream 1.
            Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                    .withTombstoneCap(0)
                    .build();
            try (Fixture f = new Fixture(cfg)) {
                Http2StreamPool pool = f.ctx.getStreamPool();
                Http2Stream s1 = f.openStream(1);
                int slot1 = pool.lookup(1);
                int gen1 = s1.getGeneration();

                // Queue a response: HEADERS (not END_STREAM) + a large DATA
                // chunk. Neither is yet emitted; both sit in the arena.
                long data = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < 1024; i++) {
                        Unsafe.getUnsafe().putByte(data + i, (byte) (i & 0xFF));
                    }
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.emitResponseHeaders(1, gen1, smallStatus200(), false));
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.enqueueData(1, gen1, data, 1024, false));
                    Assert.assertTrue(s1.getOutboundTupleCount() >= 2);
                    Assert.assertTrue(s1.getOutboundQueuedPayloadBytes() > 0);

                    // Peer sends RST_STREAM(CANCEL).
                    long recv = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long end = Http2FrameWriter.writeRstStream(
                                recv, recv + 64, 1, Http2ErrorCode.CANCEL);
                        f.ctx.processReceivedBytes(recv, end);
                    } finally {
                        Unsafe.free(recv, 64, MemoryTag.NATIVE_DEFAULT);
                    }

                    Assert.assertTrue("listener must receive onStreamClosed with CANCEL",
                            f.listener.events.contains("closed:1:" + Http2ErrorCode.CANCEL));
                    Assert.assertEquals(0, f.ctx.getActiveStreamCount());
                    // Outbound queue ownership is released: arena bytes and
                    // tuple count reset to 0 on the RST path. With
                    // tombstoneCap=0 the slot is fully released (IDLE
                    // state, streamId=-1), so we only read the counters
                    // that are still well-defined in that state.
                    Assert.assertEquals("queued payload must be released on RST",
                            0, s1.getOutboundQueuedPayloadBytes());
                    Assert.assertEquals("tuple ring must be emptied on RST",
                            0, s1.getOutboundTupleCount());
                    // Slot returned to free-list because tombstoneCap=0.
                    Assert.assertEquals(Http2StreamPool.SLOT_FREE, pool.getSlotKind(slot1));

                    // writePending must emit nothing for stream 1.
                    long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long sendEnd = f.ctx.writePending(send, send + BUF);
                        List<DecodedFrame> frames = decode(send, sendEnd);
                        for (DecodedFrame fr : frames) {
                            Assert.assertNotEquals("dead stream produced a frame after RST",
                                    1, fr.streamId);
                        }
                    } finally {
                        Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                    }

                    // Slot reuse: open stream id 3 — LIFO free-list hands
                    // back the same slot index. Arena state must be fully
                    // reset (queued=0, tuples=0, endStreamStaged=false,
                    // parked=false); generation must be bumped.
                    Http2Stream s2 = f.openStream(3);
                    int slot2 = pool.lookup(3);
                    Assert.assertEquals("slot LIFO reuse", slot1, slot2);
                    Assert.assertSame("per-slot Http2Stream object is pool-reused",
                            s1, s2);
                    Assert.assertTrue("generation must bump across recycle",
                            s2.getGeneration() > gen1);
                    Assert.assertEquals(0, s2.getOutboundTupleCount());
                    Assert.assertEquals(0, s2.getOutboundQueuedPayloadBytes());
                    Assert.assertFalse(s2.isOutboundEndStreamStaged());
                    Assert.assertFalse(s2.isOutboundParked());
                    // Run the same scenario end-to-end on the recycled slot:
                    // enqueue, emit, stream cleanly closes.
                    int gen2 = s2.getGeneration();
                    Assert.assertTrue(s2.onRecvDataEndStream());
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.emitResponseHeaders(3, gen2, smallStatus200(), true));
                    long sendR = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long endR = f.ctx.writePending(sendR, sendR + BUF);
                        List<DecodedFrame> recycledFrames = decode(sendR, endR);
                        int headersOnStream3 = 0;
                        for (DecodedFrame fr : recycledFrames) {
                            if (fr.streamId == 3 && fr.type == Http2FrameType.HEADERS) {
                                Assert.assertTrue(fr.endStream);
                                headersOnStream3++;
                            }
                        }
                        Assert.assertEquals("recycled slot emits a fresh response",
                                1, headersOnStream3);
                    } finally {
                        Unsafe.free(sendR, BUF, MemoryTag.NATIVE_DEFAULT);
                    }
                    // HEADERS+END_STREAM on HALF_CLOSED_REMOTE drove s2 to
                    // CLOSED, then closeLiveSlot+tombstoneCap=0 released
                    // the slot back to FREE (and unassign() reset state to
                    // IDLE — the slot is ready for a third reuse).
                    Assert.assertTrue("listener must receive onStreamClosed for recycled stream",
                            f.listener.events.contains("closed:3:" + Http2ErrorCode.NO_ERROR));
                    Assert.assertEquals(Http2StreamPool.SLOT_FREE, pool.getSlotKind(slot2));
                    Assert.assertEquals(0, f.ctx.getActiveStreamCount());
                } finally {
                    Unsafe.free(data, 1024, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPeerRstStreamWithTombstoneCapClearsQueueOnLiveToTombstone() {
        // Same RST scenario but with the default tombstoneCap > 0 path.
        // The slot becomes TOMBSTONE rather than FREE, but the outbound
        // queue is still fully cleared on LIVE → TOMBSTONE per the
        // closeLiveSlot invariant. Catches the regression where the
        // tombstone phase would hold on to stale arena bytes.
        assertNoNativeLeak(() -> {
            try (Fixture f = new Fixture()) {
                Http2Stream s = f.openStream(1);
                int gen = s.getGeneration();
                long data = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.emitResponseHeaders(1, gen, smallStatus200(), false));
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.enqueueData(1, gen, data, 256, false));
                    Assert.assertTrue(s.getOutboundQueuedPayloadBytes() > 0);
                    long recv = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long end = Http2FrameWriter.writeRstStream(
                                recv, recv + 64, 1, Http2ErrorCode.CANCEL);
                        f.ctx.processReceivedBytes(recv, end);
                    } finally {
                        Unsafe.free(recv, 64, MemoryTag.NATIVE_DEFAULT);
                    }
                    // Slot is now TOMBSTONE (tombstoneCap > 0 = default 100).
                    Http2StreamPool pool = f.ctx.getStreamPool();
                    int slot = pool.lookup(1);
                    Assert.assertEquals(Http2StreamPool.SLOT_TOMBSTONE, pool.getSlotKind(slot));
                    // Stream object's queue must be reset regardless.
                    Assert.assertEquals(0, s.getOutboundTupleCount());
                    Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());
                } finally {
                    Unsafe.free(data, 256, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSchedulerCursorSkipsRstStreamMidSweep() {
        // Bonus: scheduler cursor lands on a slot that was just RST'd.
        // The scheduler must skip it cleanly to the next eligible stream
        // in the same writePending call, no infinite loop.
        assertNoNativeLeak(() -> {
            try (Fixture f = new Fixture()) {
                Http2Stream a = f.openStream(1);
                Http2Stream b = f.openStream(3);
                int genA = a.getGeneration();
                int genB = b.getGeneration();
                long payload = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.enqueueData(1, genA, payload, 64, false));
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.enqueueData(3, genB, payload, 64, false));
                    // Peer RST of A — B still has work.
                    long recv = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long end = Http2FrameWriter.writeRstStream(
                                recv, recv + 64, 1, Http2ErrorCode.CANCEL);
                        f.ctx.processReceivedBytes(recv, end);
                    } finally {
                        Unsafe.free(recv, 64, MemoryTag.NATIVE_DEFAULT);
                    }
                    long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long end = f.ctx.writePending(send, send + BUF);
                        Assert.assertTrue(end > send);
                        List<DecodedFrame> frames = decode(send, end);
                        int aCount = 0, bCount = 0;
                        for (DecodedFrame fr : frames) {
                            if (fr.type == Http2FrameType.DATA) {
                                if (fr.streamId == 1) {
                                    aCount++;
                                } else if (fr.streamId == 3) {
                                    bCount++;
                                }
                            }
                        }
                        Assert.assertEquals("RST'd stream A must emit nothing", 0, aCount);
                        Assert.assertEquals("stream B must emit its queued DATA", 1, bCount);
                    } finally {
                        Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(payload, 64, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static void assertNoNativeLeak(Runnable body) {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        body.run();
        long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        Assert.assertEquals("native memory leak, delta=" + (after - before), before, after);
    }

    private static List<DecodedFrame> decode(long start, long end) {
        List<DecodedFrame> result = new ArrayList<>();
        Http2FrameReader reader = new Http2FrameReader();
        Http2FrameHeader header = new Http2FrameHeader();
        long cursor = start;
        while (cursor < end) {
            int n = reader.tryReadNext(cursor, end, header, Http2Settings.MAX_FRAME_SIZE_UPPER);
            if (n == 0) {
                break;
            }
            DecodedFrame fr = new DecodedFrame();
            fr.type = header.getType();
            fr.streamId = header.getStreamId();
            fr.payloadLen = header.getPayloadLength();
            fr.endStream = Http2Flags.hasEndStream(header.getFlags());
            fr.endHeaders = Http2Flags.hasEndHeaders(header.getFlags());
            result.add(fr);
            cursor += n;
        }
        return result;
    }

    private static Http2HeadersWriter smallStatus200() {
        return (encoder, cursor, limit) -> encoder.encode(cursor, limit, 0L, 0, 0L, 0, STATUS_200);
    }

    private static final class DecodedFrame {

        boolean endHeaders;
        boolean endStream;
        int payloadLen;
        int streamId;
        byte type;
    }

    private static final class Fixture implements AutoCloseable {

        final Http2ConnectionContext ctx;
        final RecordingListener listener;

        Fixture() {
            this(Http2ConnectionConfig.defaults());
        }

        Fixture(Http2ConnectionConfig cfg) {
            this.listener = new RecordingListener();
            this.ctx = new Http2ConnectionContext(listener, cfg);
        }

        @Override
        public void close() {
            ctx.close();
        }

        Http2Stream openStream(int streamId) {
            Http2StreamPool pool = ctx.getStreamPool();
            int slot = pool.allocateLive(streamId, 65_535, 65_535);
            Http2Stream s = pool.getLiveStream(slot);
            Assert.assertTrue(s.onRecvHeaders(false));
            return s;
        }

        void sendPeerSettingsInitialWindowSize(int value) {
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                long end = Http2FrameWriter.writeSettings(
                        buf, buf + 64,
                        new short[]{Http2Settings.INITIAL_WINDOW_SIZE},
                        new int[]{value},
                        1);
                ctx.processReceivedBytes(buf, end);
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static final class RecordingListener implements Http2StreamListener {

        final List<String> events = new ArrayList<>();

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
            events.add("closed:" + streamId + ":" + cause);
        }

        @Override
        public void onStreamWritable(int streamId) {
            events.add("writable:" + streamId);
        }

        @Override
        public void onTrailers(int streamId, boolean endStream) {
        }
    }
}
