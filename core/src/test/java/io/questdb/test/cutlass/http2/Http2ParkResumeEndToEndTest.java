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
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameReader;
import io.questdb.cutlass.http2.Http2FrameType;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.http2.Http2HeadersWriter;
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2Settings;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.cutlass.http2.Http2StreamPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * HTTP2_INTEGRATION.md §15.5 step 4 — the park/resume loop closes at the
 * engine level under every documented trigger: per-stream
 * {@code WINDOW_UPDATE}, connection-level {@code WINDOW_UPDATE(0)},
 * {@code SETTINGS_INITIAL_WINDOW_SIZE} increase, scheduler-drain arena
 * freeing, and {@code RST_STREAM} mid-park. A test-only
 * {@link ParkResumeTestListener} emits a multi-frame response body and
 * resumes on every {@code onStreamWritable} callback; the assertions
 * verify the full body reaches the peer without the listener stranding
 * pending bytes.
 */
public class Http2ParkResumeEndToEndTest {

    private static final int BUF = 32 * 1024;
    private static final int STATUS_200 = HpackEncoder.HINT_STATIC_INDEX | 8;

    @Test
    public void testConnectionWindowUpdateZeroDoesNotBreakParkedStream() {
        int totalBody = 512;
        Fixture f = new Fixture(
                Http2ConnectionConfig.newBuilder()
                        .withOutboundArenaBytesPerStream(128)
                        .withOutboundTupleQueueCap(8)
                        .build(),
                totalBody);
        try {
            f.sendHeaders(1);

            long drainEnd = f.ctx.writePending(f.send, f.send + BUF);
            int drained = countDataBytes(f.send, drainEnd, 1);
            Assert.assertTrue(drained > 0);

            // WINDOW_UPDATE(0) — exercises the trigger-2 fan-out path.
            long wu = Http2FrameWriter.writeWindowUpdate(f.recv, f.recv + BUF, 0, 1024);
            f.ctx.processReceivedBytes(f.recv, wu);

            int total = drained;
            for (int i = 0; i < 50 && total < totalBody; i++) {
                long next = f.ctx.writePending(f.send, f.send + BUF);
                total += countDataBytes(f.send, next, 1);
            }
            Assert.assertEquals(totalBody, total);
        } finally {
            f.close();
        }
    }

    @Test
    public void testMultiParkCycle() {
        // Tight arena + tuple cap so every second-or-third enqueue parks
        // and every scheduler drain unparks. The multi-round loop
        // deliberately relies on onStreamWritable callbacks to eventually
        // push the full body across.
        int totalBody = 1024;
        Fixture f = new Fixture(
                Http2ConnectionConfig.newBuilder()
                        .withOutboundArenaBytesPerStream(64)
                        .withOutboundTupleQueueCap(4)
                        .build(),
                totalBody);
        try {
            f.sendHeaders(1);
            int total = 0;
            for (int round = 0; round < 200 && total < totalBody; round++) {
                long next = f.ctx.writePending(f.send, f.send + BUF);
                total += countDataBytes(f.send, next, 1);
            }
            Assert.assertEquals(totalBody, total);
            Assert.assertTrue("multi-park run must have unparked at least 5 times",
                    f.listener.streamWritableCount >= 5);
        } finally {
            f.close();
        }
    }

    @Test
    public void testRstStreamMidParkDiscardsPendingBytes() {
        // Peer RST_STREAM(CANCEL) mid-drain: the listener has more bytes
        // to push but its onStreamClosed callback observes the reset and
        // stops. No DATA frames emit on the reset stream thereafter.
        // Tiny send buffer forces writePending to stop before the body
        // fully drains; the RST_STREAM then lands during the pause.
        int totalBody = 4096;
        int smallSend = 256;
        Fixture f = new Fixture(
                Http2ConnectionConfig.newBuilder()
                        .withOutboundArenaBytesPerStream(64)
                        .withOutboundTupleQueueCap(4)
                        .build(),
                totalBody);
        try {
            f.sendHeaders(1);
            long drained = f.ctx.writePending(f.send, f.send + smallSend);
            int bytesBefore = countDataBytes(f.send, drained, 1);
            Assert.assertTrue(bytesBefore > 0);
            Assert.assertTrue("body must not fully drain in one small-buffer sweep",
                    bytesBefore < totalBody);

            long rst = Http2FrameWriter.writeRstStream(f.recv, f.recv + BUF, 1, Http2ErrorCode.CANCEL);
            f.ctx.processReceivedBytes(f.recv, rst);
            Assert.assertEquals(1, f.listener.streamClosedCount);
            Assert.assertTrue(f.listener.streamClosedSeenBeforeEnd);

            long afterReset = f.ctx.writePending(f.send, f.send + BUF);
            Assert.assertEquals("no DATA must emit after peer RST_STREAM",
                    0, countDataBytes(f.send, afterReset, 1));
        } finally {
            f.close();
        }
    }

    @Test
    public void testSchedulerDrainFiresOnStreamWritable() {
        // Tight arena forces PARK on the first enqueue wave; the
        // scheduler drain then fires onStreamWritable via the trigger-4
        // cap-free path. Listener resumes and eventually ships the full
        // body plus END_STREAM.
        int totalBody = 256;
        Fixture f = new Fixture(
                Http2ConnectionConfig.newBuilder()
                        .withOutboundArenaBytesPerStream(96)
                        .withOutboundTupleQueueCap(6)
                        .build(),
                totalBody);
        try {
            f.sendHeaders(1);
            int total = 0;
            for (int round = 0; round < 50 && total < totalBody; round++) {
                long next = f.ctx.writePending(f.send, f.send + BUF);
                total += countDataBytes(f.send, next, 1);
            }
            Assert.assertEquals(totalBody, total);
            Assert.assertTrue("scheduler drain must have fired onStreamWritable",
                    f.listener.streamWritableCount >= 1);
            Assert.assertEquals("stream must close cleanly after terminating DATA",
                    1, f.listener.streamClosedCount);
            Assert.assertFalse("clean close implies the full body shipped",
                    f.listener.streamClosedSeenBeforeEnd);
        } finally {
            f.close();
        }
    }

    @Test
    public void testSettingsInitialWindowSizeIncreaseEventuallyDrainsBody() {
        // Tiny peer SETTINGS_INITIAL_WINDOW_SIZE constrains DATA emit.
        // A later SETTINGS increase + periodic WINDOW_UPDATEs eventually
        // let the whole body flow. Exercises the trigger-3 fan-out even
        // though the path is not the only engine in play — the fan-out
        // fires only for streams that are already parked on arena cap,
        // which the tight arena here guarantees.
        int totalBody = 4096;
        Fixture f = new Fixture(
                Http2ConnectionConfig.newBuilder()
                        .withOutboundArenaBytesPerStream(256)
                        .withOutboundTupleQueueCap(8)
                        .build(),
                totalBody);
        try {
            long cursor = Http2FrameWriter.writeSettings(
                    f.recv, f.recv + BUF,
                    new short[]{Http2Settings.INITIAL_WINDOW_SIZE},
                    new int[]{16},
                    1);
            f.ctx.processReceivedBytes(f.recv, cursor);
            f.sendHeaders(1);

            int total = 0;
            for (int round = 0; round < 200 && total < totalBody; round++) {
                long next = f.ctx.writePending(f.send, f.send + BUF);
                total += countDataBytes(f.send, next, 1);
                if (round == 4) {
                    long s2 = Http2FrameWriter.writeSettings(
                            f.recv, f.recv + BUF,
                            new short[]{Http2Settings.INITIAL_WINDOW_SIZE},
                            new int[]{256},
                            1);
                    f.ctx.processReceivedBytes(f.recv, s2);
                }
                // Keep flow-control credit flowing so the stream window
                // doesn't permanently stall; the feature under test is
                // park/resume, not the full flow-control discipline.
                long wu = Http2FrameWriter.writeWindowUpdate(f.recv, f.recv + BUF, 1, 512);
                f.ctx.processReceivedBytes(f.recv, wu);
                long wuConn = Http2FrameWriter.writeWindowUpdate(f.recv, f.recv + BUF, 0, 512);
                f.ctx.processReceivedBytes(f.recv, wuConn);
            }
            Assert.assertEquals(totalBody, total);
            Assert.assertTrue("onStreamWritable must have fired across the body",
                    f.listener.streamWritableCount >= 1);
        } finally {
            f.close();
        }
    }

    /**
     * Sums body-payload bytes across DATA frames for {@code streamId}.
     */
    private static int countDataBytes(long start, long end, int streamId) {
        Http2FrameReader reader = new Http2FrameReader();
        Http2FrameHeader header = new Http2FrameHeader();
        long cursor = start;
        int total = 0;
        while (cursor < end) {
            int n = reader.tryReadNext(cursor, end, header, Http2Settings.MAX_FRAME_SIZE_UPPER);
            if (n == 0) {
                break;
            }
            if (header.getType() == Http2FrameType.DATA && header.getStreamId() == streamId) {
                total += header.getPayloadLength();
            }
            cursor += n;
        }
        return total;
    }

    /**
     * Test fixture holding the engine + the park-resume listener. The
     * listener needs a back-reference to the context to call
     * {@code emitResponseHeaders} / {@code enqueueData}, so we wire the
     * reference after construction. {@link #close} frees the
     * fixture-owned native scratch and shuts the engine down.
     */
    private static final class Fixture {
        final Http2ConnectionContext ctx;
        final ParkResumeTestListener listener;
        final long recv;
        final long send;

        Fixture(Http2ConnectionConfig cfg, int bodyLen) {
            this.listener = new ParkResumeTestListener(bodyLen);
            this.ctx = new Http2ConnectionContext(listener, cfg);
            this.listener.bind(ctx);
            this.recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
            this.send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        }

        void close() {
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
            listener.close();
        }

        void sendHeaders(int streamId) {
            long end = Http2FrameWriter.writeHeaders(recv, recv + BUF, streamId, true, true, 0L, 0);
            ctx.processReceivedBytes(recv, end);
        }
    }

    /**
     * Listener that emits a fixed-size body in 32-byte chunks across
     * {@link Http2ConnectionContext#enqueueData} calls and resumes on
     * every {@link #onStreamWritable}. Full park/resume loop coverage
     * at the engine level; no HTTP integration involved.
     */
    private static final class ParkResumeTestListener implements Http2StreamListener {
        final long bodyBuf;
        final int bodyLen;
        Http2ConnectionContext ctx;
        int requestHeadersCount;
        int streamClosedCount;
        boolean streamClosedSeenBeforeEnd;
        int streamWritableCount;
        private int cursor;
        private int gen;
        private int sid = -1;

        ParkResumeTestListener(int bodyLen) {
            this.bodyLen = bodyLen;
            this.bodyBuf = Unsafe.malloc(bodyLen, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < bodyLen; i++) {
                Unsafe.getUnsafe().putByte(bodyBuf + i, (byte) (i & 0x7F));
            }
        }

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
            requestHeadersCount++;
            sid = streamId;
            int slot = ctx.getStreamPool().lookup(streamId);
            Assert.assertNotEquals(Http2StreamPool.SLOT_NOT_FOUND, slot);
            gen = ctx.getStreamPool().getLiveStream(slot).getGeneration();
            cursor = 0;

            Http2HeadersWriter statusWriter = (encoder, c, limit)
                    -> encoder.encode(c, limit, 0L, 0, 0L, 0, STATUS_200);
            int rc = ctx.emitResponseHeaders(sid, gen, statusWriter, false);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK, rc);
            pumpData();
        }

        @Override
        public void onStreamClosed(int streamId, int cause) {
            if (streamId != sid) {
                return;
            }
            streamClosedCount++;
            streamClosedSeenBeforeEnd = cursor < bodyLen;
        }

        @Override
        public void onStreamWritable(int streamId) {
            streamWritableCount++;
            pumpData();
        }

        @Override
        public void onTrailers(int streamId, boolean endStream) {
        }

        void bind(Http2ConnectionContext ctx) {
            this.ctx = ctx;
        }

        void close() {
            Unsafe.free(bodyBuf, bodyLen, MemoryTag.NATIVE_DEFAULT);
        }

        private void pumpData() {
            while (cursor < bodyLen) {
                int chunk = Math.min(32, bodyLen - cursor);
                boolean last = (cursor + chunk) == bodyLen;
                int rc = ctx.enqueueData(sid, gen, bodyBuf + cursor, chunk, last);
                if (rc == Http2ConnectionContext.ENQUEUE_OK) {
                    cursor += chunk;
                    if (last) {
                        return;
                    }
                    continue;
                }
                if (rc == Http2ConnectionContext.ENQUEUE_PARK) {
                    return;
                }
                if (rc == Http2ConnectionContext.ENQUEUE_STALE_GENERATION
                        || rc == Http2ConnectionContext.ENQUEUE_STREAM_CLOSED) {
                    return;
                }
                Assert.fail("unexpected enqueueData rc=" + rc);
            }
        }
    }
}
