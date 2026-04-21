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
 * Phase A PR3 coverage: round-robin scheduler inside
 * {@link Http2ConnectionContext#writePending} and the
 * {@link Http2StreamListener#onStreamWritable} callback.
 * <p>
 * Exercises the HTTP2_INTEGRATION.md §15.4 A.7 tests 1, 2, 3, 4, 5, 6, 10,
 * plus an ENQUEUE_HEADER_LIST_TOO_LARGE path covering the defensive
 * sentinel from PR2.
 */
public class Http2OutboundSchedulerTest {

    private static final int BUF = 1 << 20;
    private static final int STATUS_200 = HpackEncoder.HINT_STATIC_INDEX | 8;

    @Test
    public void testDataPayloadSplitsAcrossFramesByPeerMaxFrameSize() {
        // Test 3: enqueueData with payload > peer MAX_FRAME_SIZE (default
        // 16384) emits a sequence of DATA frames each ≤ 16384 bytes.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            // Drive the request to HALF_CLOSED_REMOTE so a response-side
            // END_STREAM completes the stream cleanly.
            Assert.assertTrue(s.onRecvDataEndStream());

            int gen = s.getGeneration();
            int totalLen = 40_000; // > 2 × default MAX_FRAME_SIZE (16384).
            long payload = Unsafe.malloc(totalLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < totalLen; i++) {
                    Unsafe.getUnsafe().putByte(payload + i, (byte) (i & 0xFF));
                }
                // Headers + DATA + END_STREAM.
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.emitResponseHeaders(1, gen, smallStatus200(), false));
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, totalLen, true));

                long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = f.ctx.writePending(send, send + BUF);
                    Assert.assertTrue(end > send);
                    List<DecodedFrame> frames = decode(send, end);
                    // Expected: 1 HEADERS + 3 DATA (16384 + 16384 + 7232).
                    int dataCount = 0;
                    int totalDataBytes = 0;
                    int maxDataLen = 0;
                    int lastEndStreamIdx = -1;
                    for (int i = 0; i < frames.size(); i++) {
                        DecodedFrame fr = frames.get(i);
                        if (fr.type == Http2FrameType.DATA) {
                            dataCount++;
                            totalDataBytes += fr.payloadLen;
                            maxDataLen = Math.max(maxDataLen, fr.payloadLen);
                            if (fr.endStream) {
                                lastEndStreamIdx = i;
                            }
                        }
                    }
                    Assert.assertEquals(3, dataCount);
                    Assert.assertEquals(totalLen, totalDataBytes);
                    Assert.assertTrue("frame size exceeds peer MAX_FRAME_SIZE",
                            maxDataLen <= Http2Settings.DEFAULT_MAX_FRAME_SIZE);
                    Assert.assertEquals("END_STREAM must ride the final DATA",
                            frames.size() - 1, lastEndStreamIdx);
                    // Stream is closed after the final END_STREAM.
                    Assert.assertTrue(f.listener.events.contains("closed:1:" + Http2ErrorCode.NO_ERROR));
                } finally {
                    Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(payload, totalLen, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testEmitResponseHeadersHeaderListTooLargeOnWriterReturnMinus1() {
        // PR2 deviation #2: a writer that overflows the engine scratch
        // returns -1; the engine translates that to
        // ENQUEUE_HEADER_LIST_TOO_LARGE. We force it by having the writer
        // return -1 unconditionally.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            Http2HeadersWriter overflowWriter = (encoder, cursor, limit) -> -1L;
            int r = f.ctx.emitResponseHeaders(1, gen, overflowWriter, false);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_HEADER_LIST_TOO_LARGE, r);
            Assert.assertEquals(0, s.getOutboundTupleCount());
            Assert.assertFalse(s.isOutboundParked());
            // Subsequent emit with a well-formed writer still succeeds —
            // the scratch path is stateless per call.
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    f.ctx.emitResponseHeaders(1, gen, smallStatus200(), true));
        }
    }

    @Test
    public void testHeadersDataEndStreamHappyPath() {
        // Test 2: HEADERS + DATA + END_STREAM; final frame carries
        // END_STREAM and the stream transitions to CLOSED.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            // Close the inbound side first so the server-side END_STREAM
            // drives CLOSED (half-closed-remote → closed).
            Assert.assertTrue(s.onRecvDataEndStream());

            int gen = s.getGeneration();
            long body = Unsafe.malloc(11, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 11; i++) {
                    Unsafe.getUnsafe().putByte(body + i, (byte) ('a' + i));
                }
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.emitResponseHeaders(1, gen, smallStatus200(), false));
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, body, 11, true));

                long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = f.ctx.writePending(send, send + BUF);
                    Assert.assertTrue(end > send);
                    List<DecodedFrame> frames = decode(send, end);
                    Assert.assertEquals(2, frames.size());
                    Assert.assertEquals(Http2FrameType.HEADERS, frames.get(0).type);
                    Assert.assertTrue(frames.get(0).endHeaders);
                    Assert.assertFalse(frames.get(0).endStream);
                    Assert.assertEquals(Http2FrameType.DATA, frames.get(1).type);
                    Assert.assertTrue(frames.get(1).endStream);
                    Assert.assertEquals(11, frames.get(1).payloadLen);
                    Assert.assertEquals(Http2StreamState.CLOSED, s.getState());
                    Assert.assertTrue(f.listener.events.contains("closed:1:" + Http2ErrorCode.NO_ERROR));
                } finally {
                    Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(body, 11, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testHeadersOnlyResponseWithEndStreamEmitsNoData() {
        // Test 1: HEADERS-only response with END_STREAM flag, no DATA frame.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStream(1);
            Assert.assertTrue(s.onRecvDataEndStream()); // HALF_CLOSED_REMOTE
            int gen = s.getGeneration();
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    f.ctx.emitResponseHeaders(1, gen, smallStatus200(), true));
            long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
            try {
                long end = f.ctx.writePending(send, send + BUF);
                Assert.assertTrue(end > send);
                List<DecodedFrame> frames = decode(send, end);
                Assert.assertEquals(1, frames.size());
                Assert.assertEquals(Http2FrameType.HEADERS, frames.get(0).type);
                Assert.assertTrue(frames.get(0).endStream);
                Assert.assertTrue(frames.get(0).endHeaders);
                Assert.assertEquals(Http2StreamState.CLOSED, s.getState());
                Assert.assertTrue(f.listener.events.contains("closed:1:" + Http2ErrorCode.NO_ERROR));
            } finally {
                Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testOnStreamWritableFiresOnConnectionWindowUpdateFanOut() {
        // PR feedback trigger 2: WU(0) fans out to parked streams with
        // non-zero stream window.
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOutboundArenaBytesPerStream(64)
                .withOutboundTupleQueueCap(2)
                .build();
        try (Fixture f = new Fixture(cfg)) {
            Http2Stream s = f.openStream(1);
            int gen = s.getGeneration();
            // Fill cap → PARK.
            long payload = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, 64, false));
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK,
                        f.ctx.enqueueData(1, gen, payload, 1, false));
                Assert.assertTrue(s.isOutboundParked());
                f.listener.events.clear();
                // WU(0) arrives. Stream window is positive (default 65535),
                // so fan-out fires onStreamWritable.
                long recv = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = Http2FrameWriter.writeWindowUpdate(recv, recv + 64, 0, 100);
                    f.ctx.processReceivedBytes(recv, end);
                    Assert.assertFalse(s.isOutboundParked());
                    Assert.assertTrue(f.listener.events.contains("writable:1"));
                } finally {
                    Unsafe.free(recv, 64, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(payload, 64, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testOnStreamWritableFiresOnStreamWindowUpdate() {
        // PR feedback trigger 1: WINDOW_UPDATE(streamId>0) on parked
        // stream fires onStreamWritable.
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
                long recv = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = Http2FrameWriter.writeWindowUpdate(recv, recv + 64, 1, 10_000);
                    f.ctx.processReceivedBytes(recv, end);
                    Assert.assertFalse(s.isOutboundParked());
                    Assert.assertTrue(f.listener.events.contains("writable:1"));
                } finally {
                    Unsafe.free(recv, 64, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testRoundRobinFairnessAlternatesStreamsAcrossCalls() {
        // Test 10: two streams A (id 1) and B (id 3), each with 2 DATA
        // frames. Send buffer sized to hold exactly 2 data frames so each
        // writePending emits one from each stream. Cursor state persists
        // across calls — the second call emits each stream's second frame.
        try (Fixture f = new Fixture()) {
            Http2Stream a = f.openStream(1);
            Http2Stream b = f.openStream(3);
            int genA = a.getGeneration();
            int genB = b.getGeneration();

            long payloadA = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
            long payloadB = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 10; i++) {
                    Unsafe.getUnsafe().putByte(payloadA + i, (byte) ('A'));
                    Unsafe.getUnsafe().putByte(payloadB + i, (byte) ('B'));
                }
                // Queue 2 DATA frames each, no END_STREAM so the stream
                // stays OPEN for later calls.
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, genA, payloadA, 10, false));
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, genA, payloadA, 10, false));
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(3, genB, payloadB, 10, false));
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(3, genB, payloadB, 10, false));

                // 2 frames × (9-byte header + 10-byte payload) = 38 bytes.
                int bufSize = 38;
                long send = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = f.ctx.writePending(send, send + bufSize);
                    Assert.assertEquals(bufSize, end - send);
                    List<DecodedFrame> firstCall = decode(send, end);
                    Assert.assertEquals(2, firstCall.size());
                    Assert.assertEquals(1, firstCall.get(0).streamId);
                    Assert.assertEquals(3, firstCall.get(1).streamId);
                    // Stream A kept its second frame; stream B kept its second.
                    Assert.assertEquals(1, a.getOutboundTupleCount());
                    Assert.assertEquals(1, b.getOutboundTupleCount());

                    // Second call (caller drained send buffer, reuses it).
                    long end2 = f.ctx.writePending(send, send + bufSize);
                    Assert.assertEquals(bufSize, end2 - send);
                    List<DecodedFrame> secondCall = decode(send, end2);
                    Assert.assertEquals(2, secondCall.size());
                    Assert.assertEquals(1, secondCall.get(0).streamId);
                    Assert.assertEquals(3, secondCall.get(1).streamId);
                    Assert.assertEquals(0, a.getOutboundTupleCount());
                    Assert.assertEquals(0, b.getOutboundTupleCount());
                } finally {
                    Unsafe.free(send, bufSize, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(payloadA, 10, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(payloadB, 10, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testSettingsInitialWindowSizeIncreaseUnparksStreamsBlockedOnStreamWindow() {
        // Test 6: stream with zero outbound stream window, conn window
        // healthy. Peer SETTINGS_INITIAL_WINDOW_SIZE bump injects positive
        // delta into every outbound-active stream; scheduler then emits.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStreamWithWindows(1, 65_535, 0);
            int gen = s.getGeneration();
            long payload = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(payload, 0xDEADBEEFL);
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, 8, false));
                long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(send, f.ctx.writePending(send, send + BUF));
                    Assert.assertEquals(1, s.getOutboundTupleCount());
                    // Peer raises INITIAL_WINDOW_SIZE: delta = 100000 - 65535 = +34465.
                    f.sendPeerSettingsInitialWindowSize(100_000);
                    // Stream window moved from 0 to 34465. writePending drains
                    // both the SETTINGS_ACK the engine queued in response and
                    // the newly-unblocked DATA frame.
                    Assert.assertTrue(s.getOutboundStreamWindow() > 0);
                    long end = f.ctx.writePending(send, send + BUF);
                    Assert.assertTrue(end > send);
                    List<DecodedFrame> frames = decode(send, end);
                    int dataIdx = -1;
                    for (int i = 0; i < frames.size(); i++) {
                        if (frames.get(i).type == Http2FrameType.DATA) {
                            dataIdx = i;
                            break;
                        }
                    }
                    Assert.assertTrue("expected a DATA frame", dataIdx >= 0);
                    Assert.assertEquals(1, frames.get(dataIdx).streamId);
                    Assert.assertEquals(8, frames.get(dataIdx).payloadLen);
                } finally {
                    Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(payload, 8, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testZeroConnectionWindowBlocksDataUntilConnectionWindowUpdate() {
        // Test 5: zero connection outbound window with stream window > 0.
        // DATA queued, not emitted. WINDOW_UPDATE(0) unparks.
        try (Fixture f = new Fixture()) {
            Http2Stream a = f.openStream(1);
            int genA = a.getGeneration();
            // Drain the connection window by emitting 65535 bytes on A.
            int drainLen = Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE;
            long drain = Unsafe.malloc(drainLen, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, genA, drain, drainLen, false));
                long bigSend = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                try {
                    f.ctx.writePending(bigSend, bigSend + BUF);
                } finally {
                    Unsafe.free(bigSend, BUF, MemoryTag.NATIVE_DEFAULT);
                }
                Assert.assertEquals(0L, f.ctx.getOutboundConnectionWindow());
                Assert.assertEquals(0L, a.getOutboundStreamWindow());
                // Grow A's stream window only (via WU on stream 1). Conn
                // window stays 0.
                long recv = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = Http2FrameWriter.writeWindowUpdate(recv, recv + 64, 1, 1000);
                    f.ctx.processReceivedBytes(recv, end);
                } finally {
                    Unsafe.free(recv, 64, MemoryTag.NATIVE_DEFAULT);
                }
                Assert.assertEquals(1000L, a.getOutboundStreamWindow());
                Assert.assertEquals(0L, f.ctx.getOutboundConnectionWindow());

                // Enqueue another DATA frame; should not emit (conn=0).
                long more = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                            f.ctx.enqueueData(1, genA, more, 10, false));
                    long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Assert.assertEquals(send, f.ctx.writePending(send, send + BUF));
                        Assert.assertEquals(1, a.getOutboundTupleCount());
                        // WU(0) credits conn window.
                        long recv2 = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                        try {
                            long end = Http2FrameWriter.writeWindowUpdate(recv2, recv2 + 64, 0, 500);
                            f.ctx.processReceivedBytes(recv2, end);
                        } finally {
                            Unsafe.free(recv2, 64, MemoryTag.NATIVE_DEFAULT);
                        }
                        long end = f.ctx.writePending(send, send + BUF);
                        Assert.assertTrue(end > send);
                        List<DecodedFrame> frames = decode(send, end);
                        Assert.assertEquals(1, frames.size());
                        Assert.assertEquals(Http2FrameType.DATA, frames.get(0).type);
                        Assert.assertEquals(10, frames.get(0).payloadLen);
                    } finally {
                        Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(more, 10, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(drain, drainLen, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testZeroLengthEndStreamEmitsUnderZeroWindow() {
        // RFC 9113 §6.9.1: zero-length DATA with END_STREAM is exempt
        // from flow control and must emit even when both windows are 0.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStreamWithWindows(1, 65_535, 0);
            Assert.assertTrue(s.onRecvDataEndStream()); // HALF_CLOSED_REMOTE
            int gen = s.getGeneration();
            // Also zero the connection window by enqueueing + draining a
            // big chunk on another stream. Actually, simpler: bump inbound
            // via headers-only response first, then terminator.
            // Use a stream + conn window of 0 via allocateLive's stream=0
            // alone; conn window is still 65535. Drain conn window.
            Http2Stream filler = f.openStream(3);
            int genFiller = filler.getGeneration();
            int drainLen = Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE;
            long drain = Unsafe.malloc(drainLen, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(3, genFiller, drain, drainLen, true));
                long bigSend = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                try {
                    f.ctx.writePending(bigSend, bigSend + BUF);
                } finally {
                    Unsafe.free(bigSend, BUF, MemoryTag.NATIVE_DEFAULT);
                }
                Assert.assertEquals(0L, f.ctx.getOutboundConnectionWindow());
                Assert.assertEquals(0L, s.getOutboundStreamWindow());

                // Zero-length END_STREAM DATA on stream 1 under zero windows.
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, 0L, 0, true));
                long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = f.ctx.writePending(send, send + BUF);
                    Assert.assertTrue(end > send);
                    List<DecodedFrame> frames = decode(send, end);
                    // One DATA frame on stream 1 with payloadLen=0 and END_STREAM.
                    int found = 0;
                    for (DecodedFrame fr : frames) {
                        if (fr.type == Http2FrameType.DATA && fr.streamId == 1) {
                            Assert.assertEquals(0, fr.payloadLen);
                            Assert.assertTrue(fr.endStream);
                            found++;
                        }
                    }
                    Assert.assertEquals(1, found);
                    // Stream transitioned to CLOSED.
                    Assert.assertEquals(Http2StreamState.CLOSED, s.getState());
                } finally {
                    Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(drain, drainLen, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    @Test
    public void testZeroStreamWindowBlocksDataUntilStreamWindowUpdate() {
        // Test 4: zero stream outbound window → DATA queued, no emission.
        // WINDOW_UPDATE on the stream unparks and DATA flows.
        try (Fixture f = new Fixture()) {
            Http2Stream s = f.openStreamWithWindows(1, 65_535, 0);
            int gen = s.getGeneration();
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 16; i++) {
                    Unsafe.getUnsafe().putByte(payload + i, (byte) (0xA0 + i));
                }
                Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                        f.ctx.enqueueData(1, gen, payload, 16, false));
                // Scheduler cannot emit (stream window = 0).
                long send = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
                try {
                    Assert.assertEquals(send, f.ctx.writePending(send, send + BUF));
                    Assert.assertEquals(1, s.getOutboundTupleCount());
                    // WU on stream.
                    long recv = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long end = Http2FrameWriter.writeWindowUpdate(recv, recv + 64, 1, 1000);
                        f.ctx.processReceivedBytes(recv, end);
                    } finally {
                        Unsafe.free(recv, 64, MemoryTag.NATIVE_DEFAULT);
                    }
                    Assert.assertEquals(1000L, s.getOutboundStreamWindow());
                    // Scheduler now emits.
                    long end = f.ctx.writePending(send, send + BUF);
                    Assert.assertTrue(end > send);
                    List<DecodedFrame> frames = decode(send, end);
                    Assert.assertEquals(1, frames.size());
                    Assert.assertEquals(Http2FrameType.DATA, frames.get(0).type);
                    Assert.assertEquals(16, frames.get(0).payloadLen);
                } finally {
                    Unsafe.free(send, BUF, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
            }
        }
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
            return openStreamWithWindows(streamId, 65_535, 65_535);
        }

        Http2Stream openStreamWithWindows(int streamId, long inboundWin, long outboundWin) {
            Http2StreamPool pool = ctx.getStreamPool();
            int slot = pool.allocateLive(streamId, inboundWin, outboundWin);
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
            events.add("data:" + streamId + ":" + dataLen + ":" + endStream);
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
