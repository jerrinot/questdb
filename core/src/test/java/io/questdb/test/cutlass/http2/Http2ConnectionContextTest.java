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

import io.questdb.cutlass.http2.Http2ConnectionConfig;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2ErrorCode;
import io.questdb.cutlass.http2.Http2Flags;
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameReader;
import io.questdb.cutlass.http2.Http2FrameType;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2Settings;
import io.questdb.cutlass.http2.Http2FlowController;
import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.cutlass.http2.Http2StreamPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class Http2ConnectionContextTest {

    private static final int BUF = 64 * 1024;

    @Test
    public void testAbortingGoAwayCancelsActiveStreams() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        // Seed a LIVE stream via the pool so GOAWAY has something to cancel.
        Http2StreamPool pool = ctx.getStreamPool();
        int slot = pool.allocateLive(1, 65_535, 65_535);
        pool.getLiveStream(slot).onRecvHeaders(false); // advance to OPEN
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            // Peer GOAWAY with non-NO_ERROR triggers §12 cancellation.
            long end = Http2FrameWriter.writeGoAway(recv, recv + BUF, 1,
                    Http2ErrorCode.INTERNAL_ERROR, 0L, 0);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_DRAINING, ctx.getState());
            // Stream should be closed and a RST_STREAM queued.
            Assert.assertTrue(listener.events.contains("closed:1:" + Http2ErrorCode.CANCEL));
            Assert.assertEquals(0, pool.getActiveStreamCount());
            Assert.assertEquals(1, ctx.getPendingCount());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testConstructAndClose() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        Assert.assertEquals(Http2ConnectionContext.STATE_ACTIVE, ctx.getState());
        Assert.assertEquals(Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE, ctx.getInboundConnectionWindow());
        Assert.assertEquals(Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE, ctx.getOutboundConnectionWindow());
        Assert.assertEquals(0, ctx.getActiveStreamCount());
        ctx.close();
        Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
        // Idempotent close.
        ctx.close();
    }

    @Test
    public void testEmitInitialSettings() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOurMaxConcurrentStreams(100)
                .withOurInitialWindowSize(1 << 16)
                .withOurMaxFrameSize(32 * 1024)
                .build();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, cfg);
        long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = ctx.emitInitialSettings(send, send + BUF);
            Assert.assertTrue(end > send);
            Assert.assertTrue(ctx.isSettingsFrameOutstanding());
            // Every identifier we carried is now in ourAdvertised.
            Assert.assertEquals(100, ctx.getOurAdvertised(Http2Settings.MAX_CONCURRENT_STREAMS));
            Assert.assertEquals(0, ctx.getOurAdvertised(Http2Settings.ENABLE_PUSH));
            Assert.assertEquals(1 << 16, ctx.getOurAdvertised(Http2Settings.INITIAL_WINDOW_SIZE));
            Assert.assertEquals(32 * 1024, ctx.getOurAdvertised(Http2Settings.MAX_FRAME_SIZE));
            // Pure-immediate and widen-immediate identifiers commit to ourApplied.
            Assert.assertEquals(100, ctx.getOurApplied(Http2Settings.MAX_CONCURRENT_STREAMS));
            Assert.assertEquals(0, ctx.getOurApplied(Http2Settings.ENABLE_PUSH));
            Assert.assertEquals(32 * 1024, ctx.getOurApplied(Http2Settings.MAX_FRAME_SIZE));
            // ACK-gated identifiers keep the old ourApplied until the peer acks.
            Assert.assertEquals(Http2Settings.DEFAULT_HEADER_TABLE_SIZE, ctx.getOurApplied(Http2Settings.HEADER_TABLE_SIZE));
            Assert.assertEquals(Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE, ctx.getOurApplied(Http2Settings.INITIAL_WINDOW_SIZE));
        } finally {
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testEmitInitialSettingsTwiceThrows() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long first = ctx.emitInitialSettings(send, send + BUF);
            Assert.assertTrue(first > send);
            try {
                ctx.emitInitialSettings(send, send + BUF);
                Assert.fail("expected IllegalStateException on second emitInitialSettings");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage().contains("already"));
            }
        } finally {
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testGoAwayReceivedTransitionsToDraining() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            // GOAWAY(lastStreamId=1, errorCode=NO_ERROR, no debug).
            long end = Http2FrameWriter.writeGoAway(recv, recv + BUF, 1, Http2ErrorCode.NO_ERROR, 0L, 0);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_DRAINING, ctx.getState());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testPartialFrameBufferingHaltsAtBoundary() {
        // processReceivedBytes must stop at an incomplete frame rather than
        // mis-parse the header. Send half of a SETTINGS frame, confirm zero
        // bytes are consumed, then supply the rest.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeSettings(recv, recv + BUF,
                    new short[]{Http2Settings.INITIAL_WINDOW_SIZE}, new int[]{65_535}, 1);
            long halfway = recv + ((end - recv) / 2);
            long consumed = ctx.processReceivedBytes(recv, halfway);
            Assert.assertEquals(0, consumed);
            Assert.assertEquals(0, ctx.getPendingCount());
            // Now supply the rest.
            consumed = ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(end - recv, consumed);
            Assert.assertEquals(1, ctx.getPendingCount());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testPeerSettingsInitialWindowOverflowTearsDownConnection() {
        // Seed a LIVE stream with outbound window near the RFC cap; a peer
        // SETTINGS INITIAL_WINDOW_SIZE change that would push it past 2^31-1
        // is a connection FLOW_CONTROL_ERROR per §7.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        Http2StreamPool pool = ctx.getStreamPool();
        int slot = pool.allocateLive(1, 65_535, Http2FlowController.WINDOW_MAX - 100);
        Http2Stream s = pool.getLiveStream(slot);
        s.onRecvHeaders(false); // drive to OPEN so outbound direction is active
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            // Peer's initial window is 65_535; new advertised value 200_000 →
            // delta +134_465. Added to the stream's 2^31-1-100 window, that
            // overflows.
            long end = Http2FrameWriter.writeSettings(recv, recv + BUF,
                    new short[]{Http2Settings.INITIAL_WINDOW_SIZE},
                    new int[]{200_000}, 1);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
            Assert.assertEquals(1, ctx.getPendingCount()); // GOAWAY queued
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testPingAckIsSilentlyAccepted() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writePing(recv, recv + BUF, true, 0x1234L);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(0, ctx.getPendingCount()); // no echo back
            Assert.assertEquals(Http2ConnectionContext.STATE_ACTIVE, ctx.getState());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testPingRoundtrip() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long opaque = 0xDEADBEEF_01234567L;
            long end = Http2FrameWriter.writePing(recv, recv + BUF, false, opaque);
            long consumed = ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(end - recv, consumed);
            Assert.assertEquals(1, ctx.getPendingCount());
            long written = ctx.writePending(send, send + BUF);
            Assert.assertTrue(written > send);
            Assert.assertEquals(0, ctx.getPendingCount());
            // Inspect the emitted PING_ACK.
            Http2FrameReader r = new Http2FrameReader();
            Http2FrameHeader h = new Http2FrameHeader();
            int n = r.tryReadNext(send, written, h, 16_384);
            Assert.assertEquals(Http2FrameType.PING, h.getType());
            Assert.assertTrue(Http2Flags.hasAck(h.getFlags()));
            Assert.assertEquals(8, h.getPayloadLength());
            Assert.assertEquals(opaque, Unsafe.getUnsafe().getLong(h.getPayloadAddr()));
            Assert.assertEquals(Http2FrameHeader.SIZE + 8, n);
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testPeerSettingsAppliedAndAcked() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            short[] ids = {
                    Http2Settings.HEADER_TABLE_SIZE,
                    Http2Settings.INITIAL_WINDOW_SIZE,
                    Http2Settings.MAX_FRAME_SIZE,
            };
            int[] values = {8192, 131_072, 32_768};
            long end = Http2FrameWriter.writeSettings(recv, recv + BUF, ids, values, ids.length);
            long consumed = ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(end - recv, consumed);
            Assert.assertEquals(8192, ctx.getPeerAdvertised(Http2Settings.HEADER_TABLE_SIZE));
            Assert.assertEquals(131_072, ctx.getPeerAdvertised(Http2Settings.INITIAL_WINDOW_SIZE));
            Assert.assertEquals(32_768, ctx.getPeerAdvertised(Http2Settings.MAX_FRAME_SIZE));
            Assert.assertEquals(1, ctx.getPendingCount());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testPeerSettingsUnknownIdIgnored() {
        // Known INITIAL_WINDOW_SIZE + unknown id 0x0f (value 42). The unknown
        // id must not cause a protocol error or any array access (regression
        // guard for the off-by-one §10 long[7] indexing noted in §13.1).
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            short[] ids = {Http2Settings.INITIAL_WINDOW_SIZE, (short) 0x0F};
            int[] values = {131_072, 42};
            long end = Http2FrameWriter.writeSettings(recv, recv + BUF, ids, values, ids.length);
            long consumed = ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(end - recv, consumed);
            Assert.assertEquals(Http2ConnectionContext.STATE_ACTIVE, ctx.getState());
            Assert.assertEquals(131_072, ctx.getPeerAdvertised(Http2Settings.INITIAL_WINDOW_SIZE));
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testRstStreamOnIdleStreamTearsDownConnection() {
        // RST_STREAM on an id never admitted to the pool is a connection
        // PROTOCOL_ERROR per RFC 9113 sec. 5.1 / §5 rule 4.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeRstStream(recv, recv + BUF, 7, Http2ErrorCode.CANCEL);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
            Assert.assertEquals(1, ctx.getPendingCount()); // GOAWAY
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testSettingsAckCommitsOurApplied() {
        RecordingListener listener = new RecordingListener();
        // Emit our initial settings, then receive an ACK.
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOurInitialWindowSize(131_072) // ACK-gated
                .withOurHeaderTableSize(8192)       // ACK-gated
                .build();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, cfg);
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long emitted = ctx.emitInitialSettings(send, send + BUF);
            Assert.assertTrue(emitted > send);
            Assert.assertTrue(ctx.isSettingsFrameOutstanding());
            Assert.assertEquals(Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE,
                    ctx.getOurApplied(Http2Settings.INITIAL_WINDOW_SIZE));
            // Peer sends SETTINGS ACK (empty payload).
            long end = Http2FrameWriter.writeSettingsAck(recv, recv + BUF);
            ctx.processReceivedBytes(recv, end);
            Assert.assertFalse(ctx.isSettingsFrameOutstanding());
            // ACK-gated identifiers now reflect the advertised value.
            Assert.assertEquals(131_072, ctx.getOurApplied(Http2Settings.INITIAL_WINDOW_SIZE));
            Assert.assertEquals(8192, ctx.getOurApplied(Http2Settings.HEADER_TABLE_SIZE));
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testDataFlowControlAcceptedPath() {
        // DATA on a LIVE inbound-active stream: connection + stream windows
        // debit by payloadLen, handler accepts, coalesced WINDOW_UPDATE(n)
        // credits back both and re-emits as wire frames. Padding overhead is
        // credited back immediately in its own WINDOW_UPDATE emissions.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        seedLiveStream(ctx, 1);
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(payload, (byte) 'a');
            long end = Http2FrameWriter.writeData(recv, recv + BUF, 1, false, payload, 16);
            long inboundBefore = ctx.getInboundConnectionWindow();
            ctx.processReceivedBytes(recv, end);
            // 16-byte payload, no padding → net connection window unchanged
            // after handler accepts (debit 16, credit 16).
            Assert.assertEquals(inboundBefore, ctx.getInboundConnectionWindow());
            Assert.assertTrue(listener.events.contains("data:1:16:false"));
            // Wire path: 2 WINDOW_UPDATEs (connection + stream) for the 16
            // bytes credited back.
            Assert.assertEquals(2, ctx.getPendingCount());
        } finally {
            Unsafe.free(payload, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testDataFlowControlDeferredAndSettledOnReset() {
        // Handler defers (onData returns false) → outstandingInboundCredit
        // accumulates. A local RST_STREAM must settle that obligation back
        // to the connection window; otherwise §7 step 6 leak occurs.
        RecordingListener listener = new RecordingListener();
        listener.onDataReturnsAccepted = false;
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        Http2StreamPool pool = ctx.getStreamPool();
        int slot = seedLiveStream(ctx, 1);
        Http2Stream s = pool.getLiveStream(slot);
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeData(recv, recv + BUF, 1, false, payload, 16);
            long inboundBefore = ctx.getInboundConnectionWindow();
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(16, s.getOutstandingInboundCredit());
            Assert.assertEquals(inboundBefore - 16, ctx.getInboundConnectionWindow());
            // Local reset via stream exception pathway: drain the queue first
            // and then trigger a reset by sending an unsolicited SETTINGS ACK
            // would kill the whole connection. Instead drive resetStreamLocally
            // via a peer RST_STREAM — that goes through dispatchRstStream
            // which also settles.
            ctx.writePending(payload, payload + 64); // drain WINDOW_UPDATEs for padding (none in this test)
            int preResetPending = ctx.getPendingCount();
            long end2 = Http2FrameWriter.writeRstStream(recv, recv + BUF, 1, Http2ErrorCode.CANCEL);
            ctx.processReceivedBytes(recv, end2);
            // §7 step 6: 16 bytes credited back to connection window.
            Assert.assertEquals(inboundBefore, ctx.getInboundConnectionWindow());
            Assert.assertEquals(0, s.getOutstandingInboundCredit());
            // One WINDOW_UPDATE(0, 16) queued for the settlement credit.
            Assert.assertTrue(ctx.getPendingCount() > preResetPending);
        } finally {
            Unsafe.free(payload, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testHeadersAdmitsNewStreamAndFiresListener() {
        // Smallest viable HEADERS admission: empty field block with
        // END_HEADERS + END_STREAM. The HPACK decoder accepts a zero-byte
        // block; we just need to exercise the state-machine edge
        // idle → half-closed-remote and the onRequestHeaders callback.
        // The stream stays LIVE after onRequestHeaders — we are half-closed-
        // remote but the server's response side is still open.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeHeaders(recv, recv + BUF, 1,
                    /*endStream*/ true, /*endHeaders*/ true, 0L, 0);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(1, ctx.getHighestPeerStreamIdSeen());
            Assert.assertEquals(1, ctx.getLastAcceptedPeerStreamId());
            Assert.assertTrue(listener.events.contains("reqHeaders:1:true"));
            // Stream is HALF_CLOSED_REMOTE — still LIVE, awaits response.
            Assert.assertEquals(1, ctx.getActiveStreamCount());
            Assert.assertFalse(listener.events.contains("closed:1:0"));
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testHeadersEvenStreamIdRejected() {
        // Client-initiated streams use odd ids (RFC 9113 sec. 5.1.1).
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeHeaders(recv, recv + BUF, 2, true, true, 0L, 0);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testHeadersMonotonicIdViolationClosesConnection() {
        // Two HEADERS frames with the second using a lower stream id →
        // connection PROTOCOL_ERROR per §6.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end1 = Http2FrameWriter.writeHeaders(recv, recv + BUF, 5, true, true, 0L, 0);
            long end2 = Http2FrameWriter.writeHeaders(end1, recv + BUF, 3, true, true, 0L, 0);
            ctx.processReceivedBytes(recv, end2);
            Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
            // First stream accepted, second rejected.
            Assert.assertEquals(5, ctx.getHighestPeerStreamIdSeen());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testHeadersPostGoAwayRefusesStream() {
        // After receiving peer GOAWAY we transition to DRAINING. New HEADERS
        // arrive → REFUSED_STREAM (queued as RST_STREAM), no LIVE allocation.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeGoAway(recv, recv + BUF, 0, Http2ErrorCode.NO_ERROR, 0L, 0);
            end = Http2FrameWriter.writeHeaders(end, recv + BUF, 1, true, true, 0L, 0);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_DRAINING, ctx.getState());
            // lastAcceptedPeerStreamId stays at 0 because we never admitted
            // the stream to LIVE — it was refused.
            Assert.assertEquals(0, ctx.getLastAcceptedPeerStreamId());
            Assert.assertEquals(0, ctx.getActiveStreamCount());
            // Drain: there should be a RST_STREAM(REFUSED_STREAM) on stream 1.
            long written = ctx.writePending(send, send + BUF);
            Assert.assertTrue(written > send);
            // Scan wire bytes for RST_STREAM on stream 1.
            Http2FrameReader r = new Http2FrameReader();
            Http2FrameHeader h = new Http2FrameHeader();
            boolean foundRst = false;
            long cur = send;
            while (cur < written) {
                int n = r.tryReadNext(cur, written, h, 16_384);
                if (n == 0) break;
                if (h.getType() == Http2FrameType.RST_STREAM && h.getStreamId() == 1) {
                    Assert.assertEquals(Http2ErrorCode.REFUSED_STREAM,
                            Http2FrameReader.readRstStreamErrorCode(h.getPayloadAddr()));
                    foundRst = true;
                }
                cur += n;
            }
            Assert.assertTrue("expected RST_STREAM(REFUSED_STREAM) on stream 1", foundRst);
        } finally {
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testOnBytesConsumedStaleGenerationIsNoOp() {
        RecordingListener listener = new RecordingListener();
        listener.onDataReturnsAccepted = false;
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        int slot = seedLiveStream(ctx, 1);
        Http2Stream s = ctx.getStreamPool().getLiveStream(slot);
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeData(recv, recv + BUF, 1, false, payload, 8);
            ctx.processReceivedBytes(recv, end);
            int token = listener.lastDataGenerationToken;
            // Stale token (one less): silent no-op.
            long inboundBefore = ctx.getInboundConnectionWindow();
            long outstandingBefore = s.getOutstandingInboundCredit();
            ctx.onBytesConsumed(1, token - 1, 8);
            Assert.assertEquals(inboundBefore, ctx.getInboundConnectionWindow());
            Assert.assertEquals(outstandingBefore, s.getOutstandingInboundCredit());
            // Valid token + valid n: applies.
            ctx.onBytesConsumed(1, token, 8);
            Assert.assertEquals(inboundBefore + 8, ctx.getInboundConnectionWindow());
            Assert.assertEquals(0, s.getOutstandingInboundCredit());
            // Over-ack: silent no-op.
            ctx.onBytesConsumed(1, token, 100);
            Assert.assertEquals(inboundBefore + 8, ctx.getInboundConnectionWindow());
        } finally {
            Unsafe.free(payload, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testSettingsAckDoesNotClobberOmittedMaxHeaderListSize() {
        // Regression: the M1 initial SETTINGS omits MAX_HEADER_LIST_SIZE.
        // An earlier bug treated ourAdvertised[MAX_HEADER_LIST_SIZE]=0 as
        // "advertised 0" and zeroed ourApplied on ACK. The pendingSettingsBits
        // bitset fix ensures only emitted ids are applied.
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            ctx.emitInitialSettings(send, send + BUF);
            long before = ctx.getOurApplied(Http2Settings.MAX_HEADER_LIST_SIZE);
            Assert.assertEquals(Http2Settings.DEFAULT_MAX_HEADER_LIST_SIZE, before);
            long end = Http2FrameWriter.writeSettingsAck(recv, recv + BUF);
            ctx.processReceivedBytes(recv, end);
            long after = ctx.getOurApplied(Http2Settings.MAX_HEADER_LIST_SIZE);
            Assert.assertEquals("MAX_HEADER_LIST_SIZE must survive ACK unchanged", before, after);
        } finally {
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testUnsolicitedSettingsAckClosesConnection() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeSettingsAck(recv, recv + BUF);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    /**
     * Seeds a LIVE stream in OPEN state by driving the pool + FSM directly.
     * Used by tests that need flow-control debit behaviour without having
     * to craft and parse HEADERS frames (pseudo-header capture + §11
     * validators are out of scope for the current commit).
     */
    private static int seedLiveStream(Http2ConnectionContext ctx, int streamId) {
        Http2StreamPool pool = ctx.getStreamPool();
        int slot = pool.allocateLive(streamId, 65_535, 65_535);
        pool.getLiveStream(slot).onRecvHeaders(false);
        return slot;
    }

    @Test
    public void testWindowUpdateConnectionCredits() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long initial = ctx.getOutboundConnectionWindow();
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeWindowUpdate(recv, recv + BUF, 0, 10_000);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(initial + 10_000, ctx.getOutboundConnectionWindow());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testWindowUpdateConnectionOverflowGoAway() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            // Push the connection window past 2^31 - 1 with a single increment.
            long end = Http2FrameWriter.writeWindowUpdate(recv, recv + BUF, 0, Integer.MAX_VALUE);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
            // GOAWAY enqueued.
            Assert.assertEquals(1, ctx.getPendingCount());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testWindowUpdateOnIdleStreamGoAway() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeWindowUpdate(recv, recv + BUF, 7, 100);
            ctx.processReceivedBytes(recv, end);
            Assert.assertEquals(Http2ConnectionContext.STATE_CLOSED, ctx.getState());
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testWritePendingDrainsQueue() {
        RecordingListener listener = new RecordingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long recv = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            // Queue a SETTINGS_ACK + a PING_ACK.
            long emitCursor = Http2FrameWriter.writeSettings(recv, recv + BUF,
                    new short[]{Http2Settings.INITIAL_WINDOW_SIZE}, new int[]{65_535}, 1);
            emitCursor = Http2FrameWriter.writePing(emitCursor, recv + BUF, false, 0xABCDL);
            ctx.processReceivedBytes(recv, emitCursor);
            Assert.assertEquals(2, ctx.getPendingCount());
            long written = ctx.writePending(send, send + BUF);
            Assert.assertTrue(written > send);
            Assert.assertEquals(0, ctx.getPendingCount());
            // First frame: SETTINGS ACK.
            Http2FrameReader r = new Http2FrameReader();
            Http2FrameHeader h = new Http2FrameHeader();
            int n1 = r.tryReadNext(send, written, h, 16_384);
            Assert.assertEquals(Http2FrameType.SETTINGS, h.getType());
            Assert.assertTrue(Http2Flags.hasAck(h.getFlags()));
            Assert.assertEquals(0, h.getPayloadLength());
            // Second frame: PING ACK.
            int n2 = r.tryReadNext(send + n1, written, h, 16_384);
            Assert.assertEquals(Http2FrameType.PING, h.getType());
            Assert.assertTrue(Http2Flags.hasAck(h.getFlags()));
            Assert.assertEquals(0xABCDL, Unsafe.getUnsafe().getLong(h.getPayloadAddr()));
            Assert.assertEquals(written - send, n1 + n2);
        } finally {
            Unsafe.free(recv, BUF, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    private static final class RecordingListener implements Http2StreamListener {

        final List<String> events = new ArrayList<>();
        int lastDataGenerationToken;
        int lastDataStreamId;
        boolean onDataReturnsAccepted = true;

        @Override
        public boolean onData(int streamId, long addr, int dataLen, boolean endStream, int generationToken) {
            events.add("data:" + streamId + ":" + dataLen + ":" + endStream);
            lastDataStreamId = streamId;
            lastDataGenerationToken = generationToken;
            return onDataReturnsAccepted;
        }

        @Override
        public void onRequestHeader(int streamId, long nameAddr, int nameLen,
                                    long valueAddr, int valueLen, boolean neverIndexed) {
            events.add("reqHeader:" + streamId);
        }

        @Override
        public void onRequestHeaders(int streamId, Http2RequestHeadersView view, boolean endStream) {
            events.add("reqHeaders:" + streamId + ":" + endStream);
        }

        @Override
        public void onStreamClosed(int streamId, int cause) {
            events.add("closed:" + streamId + ":" + cause);
        }

        @Override
        public void onTrailers(int streamId, boolean endStream) {
            events.add("trailers:" + streamId);
        }
    }
}
