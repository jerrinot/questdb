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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2ConnectionConfig;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2Flags;
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameReader;
import io.questdb.cutlass.http2.Http2FrameType;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP2_INTEGRATION.md §15.5 step 3 coverage for
 * {@link Http2ConnectionContext#emitTrailers}. The path handles the gRPC
 * trailer shape: HEADERS + DATA + trailer HEADERS, with END_STREAM landing
 * on the trailer HEADERS frame and the stream transitioning to CLOSED.
 * The PARK / restore discipline mirrors
 * {@link Http2ConnectionContext#emitResponseHeaders}: a parked trailer
 * block retries identically and never corrupts peer decoder state.
 */
public class Http2TrailerEmissionTest {

    private static final int SCRATCH = 256;
    private static final int SEND_CAP = 16 * 1024;
    private static final int STATUS_200 = HpackEncoder.HINT_STATIC_INDEX | 8;

    @Test
    public void testEndStreamFalseRejectedWithIllegalArgument() {
        Http2ConnectionContext ctx = new Http2ConnectionContext(
                new NoopListener(), Http2ConnectionConfig.defaults());
        try {
            Http2StreamPool pool = ctx.getStreamPool();
            int slot = pool.allocateLive(1, 65_535, 65_535);
            Http2Stream s = pool.getLiveStream(slot);
            Assert.assertTrue(s.onRecvHeaders(/*endStream*/ true));
            int gen = s.getGeneration();

            try {
                ctx.emitTrailers(1, gen, (encoder, cursor, limit) -> cursor, /*endStream*/ false);
                Assert.fail("emitTrailers must reject endStream=false per RFC 9113 sec. 8.1");
            } catch (IllegalArgumentException expected) {
                // expected
            }
        } finally {
            ctx.close();
        }
    }

    @Test
    public void testHappyPathHeadersDataTrailersCloseStream() throws Http2Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN);
        Http2ConnectionContext ctx = new Http2ConnectionContext(
                new NoopListener(), Http2ConnectionConfig.defaults());
        long nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long body = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2StreamPool pool = ctx.getStreamPool();
            int slot = pool.allocateLive(1, 65_535, 65_535);
            Http2Stream s = pool.getLiveStream(slot);
            // Simulate peer HEADERS+END_STREAM already received so the
            // stream sits in HALF_CLOSED_REMOTE; response-side writes can
            // then advance it to CLOSED.
            Assert.assertTrue(s.onRecvHeaders(true));
            Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());
            int gen = s.getGeneration();

            Http2HeadersWriter respHeaders = new StableHeadersWriter(new String[][]{
                    {":status", "200"},
                    {"content-type", "application/grpc"},
            }, nameBuf, valueBuf);
            Http2HeadersWriter trailers = new StableHeadersWriter(new String[][]{
                    {"grpc-status", "0"},
                    {"grpc-message", "ok"},
            }, nameBuf, valueBuf);

            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.emitResponseHeaders(1, gen, respHeaders, /*endStream*/ false));

            // Small body — fits a single DATA frame.
            byte[] payload = "hello".getBytes();
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(body + i, payload[i]);
            }
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.enqueueData(1, gen, body, payload.length, /*endStream*/ false));

            // Trailer HEADERS closes the stream. Must carry END_STREAM
            // per RFC 9113 §8.1.
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.emitTrailers(1, gen, trailers, /*endStream*/ true));

            // Stream should still be HALF_CLOSED_REMOTE until the trailer
            // frame actually ships — FSM transition happens on emit, not
            // on enqueue.
            Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());

            long cursor = ctx.writePending(send, send + SEND_CAP);
            Assert.assertTrue(cursor > send);

            FrameTrace trace = classifyFrames(send, cursor);
            Assert.assertEquals("expected 3 stream frames: HEADERS, DATA, trailer HEADERS",
                    3, trace.frames.size());
            Assert.assertEquals(Http2FrameType.HEADERS, trace.frames.get(0).type);
            Assert.assertFalse("response HEADERS must not carry END_STREAM",
                    trace.frames.get(0).endStream);
            Assert.assertEquals(Http2FrameType.DATA, trace.frames.get(1).type);
            Assert.assertFalse("response DATA must not carry END_STREAM",
                    trace.frames.get(1).endStream);
            Assert.assertEquals(Http2FrameType.HEADERS, trace.frames.get(2).type);
            Assert.assertTrue("trailer HEADERS must carry END_STREAM",
                    trace.frames.get(2).endStream);

            // After trailers emit the stream is CLOSED and the pool slot
            // has rolled off to TOMBSTONE.
            Assert.assertEquals(0, pool.getActiveStreamCount());

            // Feed both HEADERS blocks through Netty's decoder to confirm
            // no HPACK state drift between response and trailer blocks.
            DefaultHttp2HeadersDecoder nettyDec = new DefaultHttp2HeadersDecoder(true);
            Http2Headers decodedResponseHeaders = nettyDec.decodeHeaders(1, trace.frames.get(0).payloadAsByteBuf());
            assertHeadersEqual(new String[][]{
                    {":status", "200"},
                    {"content-type", "application/grpc"},
            }, decodedResponseHeaders);
            Http2Headers decodedTrailers = nettyDec.decodeHeaders(1, trace.frames.get(2).payloadAsByteBuf());
            assertHeadersEqual(new String[][]{
                    {"grpc-status", "0"},
                    {"grpc-message", "ok"},
            }, decodedTrailers);
        } finally {
            Unsafe.free(send, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(body, 32, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
        Assert.assertEquals("no NATIVE_HTTP_CONN leak",
                before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN));
    }

    @Test
    public void testParkedTrailerRetriesCleanlyAfterArenaDrain() throws Http2Exception {
        // Tight arena pins the trailer emit into PARK: response HEADERS
        // + a DATA filler occupy the whole outbound arena. When the
        // scheduler drains that backlog to the send buffer the arena
        // becomes free and the trailer retry must succeed with
        // identical encoded bytes — the HPACK snapshot/restore must
        // roll back any queued size-update or dynamic-table change.
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOutboundArenaBytesPerStream(64)
                .withOutboundTupleQueueCap(4)
                .build();
        Http2ConnectionContext ctx = new Http2ConnectionContext(new NoopListener(), cfg);
        long nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long filler = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2StreamPool pool = ctx.getStreamPool();
            int slot = pool.allocateLive(1, 65_535, 65_535);
            Http2Stream s = pool.getLiveStream(slot);
            Assert.assertTrue(s.onRecvHeaders(true));
            int gen = s.getGeneration();

            Http2HeadersWriter trailers = new StableHeadersWriter(new String[][]{
                    {"grpc-status", "0"},
            }, nameBuf, valueBuf);

            // Queue a status:200 HEADERS so the HPACK encoder has
            // written a queued size-update prefix. Its snapshot is what
            // the trailer PARK must roll back to.
            Http2HeadersWriter firstHeaders = new StableHeadersWriter(new String[][]{
                    {":status", "200"},
            }, nameBuf, valueBuf);
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.emitResponseHeaders(1, gen, firstHeaders, /*endStream*/ false));

            int queued = s.getOutboundQueuedPayloadBytes();
            int remaining = s.getOutboundArenaCap() - queued;
            Assert.assertTrue("fixture expects arena slack for a filler DATA", remaining > 0);
            for (int i = 0; i < remaining; i++) {
                Unsafe.getUnsafe().putByte(filler + i, (byte) i);
            }
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.enqueueData(1, gen, filler, remaining, /*endStream*/ false));
            Assert.assertEquals(s.getOutboundArenaCap(), s.getOutboundQueuedPayloadBytes());

            // Trailer emit parks — arena has no room.
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK,
                    ctx.emitTrailers(1, gen, trailers, /*endStream*/ true));
            Assert.assertTrue(s.isOutboundParked());

            // Drain: HEADERS + DATA hit the wire, arena cursor resets.
            List<byte[]> drained = new ArrayList<>();
            long cursor = ctx.writePending(send, send + SEND_CAP);
            drained.addAll(collectHeaderBlocks(send, cursor));
            Assert.assertEquals(0, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());

            // Retry: same trailer writer, now fits. END_STREAM on the
            // trailer HEADERS closes the stream.
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.emitTrailers(1, gen, trailers, /*endStream*/ true));

            long cursor2 = ctx.writePending(send, send + SEND_CAP);
            drained.addAll(collectHeaderBlocks(send, cursor2));
            Assert.assertEquals(2, drained.size());

            DefaultHttp2HeadersDecoder nettyDec = new DefaultHttp2HeadersDecoder(true);
            Http2Headers decoded0 = nettyDec.decodeHeaders(1, Unpooled.wrappedBuffer(drained.get(0)));
            assertHeadersEqual(new String[][]{{":status", "200"}}, decoded0);
            Http2Headers decoded1 = nettyDec.decodeHeaders(1, Unpooled.wrappedBuffer(drained.get(1)));
            assertHeadersEqual(new String[][]{{"grpc-status", "0"}}, decoded1);
            Assert.assertEquals(0, pool.getActiveStreamCount());
        } finally {
            Unsafe.free(send, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(filler, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    private static void assertHeadersEqual(String[][] expected, Http2Headers actual) {
        Map<String, String> got = new HashMap<>();
        for (Map.Entry<CharSequence, CharSequence> e : actual) {
            got.put(e.getKey().toString(), e.getValue().toString());
        }
        for (String[] kv : expected) {
            Assert.assertEquals("header " + kv[0] + " mismatch", kv[1], got.get(kv[0]));
        }
        Assert.assertEquals("extra or missing headers after decode",
                expected.length, got.size());
    }

    private static FrameTrace classifyFrames(long start, long end) {
        FrameTrace trace = new FrameTrace();
        Http2FrameReader reader = new Http2FrameReader();
        Http2FrameHeader header = new Http2FrameHeader();
        long cursor = start;
        while (cursor < end) {
            int n = reader.tryReadNext(cursor, end, header, Http2Settings.MAX_FRAME_SIZE_UPPER);
            if (n == 0) {
                break;
            }
            if (header.getStreamId() == 0) {
                // connection-scoped (SETTINGS, PING, etc.) — skip
                cursor += n;
                continue;
            }
            FrameInfo info = new FrameInfo();
            info.type = header.getType();
            info.endStream = (header.getFlags() & Http2Flags.END_STREAM) != 0;
            int payloadLen = header.getPayloadLength();
            info.payload = new byte[payloadLen];
            for (int i = 0; i < payloadLen; i++) {
                info.payload[i] = Unsafe.getUnsafe().getByte(header.getPayloadAddr() + i);
            }
            trace.frames.add(info);
            cursor += n;
        }
        return trace;
    }

    private static List<byte[]> collectHeaderBlocks(long start, long end) {
        List<byte[]> blocks = new ArrayList<>();
        Http2FrameReader reader = new Http2FrameReader();
        Http2FrameHeader header = new Http2FrameHeader();
        long cursor = start;
        byte[] pending = null;
        while (cursor < end) {
            int n = reader.tryReadNext(cursor, end, header, Http2Settings.MAX_FRAME_SIZE_UPPER);
            if (n == 0) {
                break;
            }
            if (header.getType() == Http2FrameType.HEADERS
                    || header.getType() == Http2FrameType.CONTINUATION) {
                int payloadLen = header.getPayloadLength();
                byte[] chunk = new byte[payloadLen];
                for (int i = 0; i < payloadLen; i++) {
                    chunk[i] = Unsafe.getUnsafe().getByte(header.getPayloadAddr() + i);
                }
                pending = pending == null ? chunk : concat(pending, chunk);
                if ((header.getFlags() & Http2Flags.END_HEADERS) != 0) {
                    blocks.add(pending);
                    pending = null;
                }
            }
            cursor += n;
        }
        return blocks;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static final class FrameInfo {
        boolean endStream;
        byte[] payload;
        byte type;

        ByteBuf payloadAsByteBuf() {
            return Unpooled.wrappedBuffer(payload);
        }
    }

    private static final class FrameTrace {
        final List<FrameInfo> frames = new ArrayList<>();
    }

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

    /**
     * Writer that emits a fixed list of {@code (name, value)} pairs every
     * time it is invoked. Uses the static table for {@code :status} and a
     * literal-with-static-name form for recognised regular headers.
     */
    private static final class StableHeadersWriter implements Http2HeadersWriter {
        private final String[][] headers;
        private final long nameScratch;
        private final long valueScratch;

        StableHeadersWriter(String[][] headers, long nameScratch, long valueScratch) {
            this.headers = headers;
            this.nameScratch = nameScratch;
            this.valueScratch = valueScratch;
        }

        @Override
        public long write(HpackEncoder encoder, long cursor, long limit) {
            for (String[] kv : headers) {
                if (":status".equals(kv[0]) && "200".equals(kv[1])) {
                    cursor = encoder.encode(cursor, limit, 0L, 0, 0L, 0, STATUS_200);
                    if (cursor < 0) {
                        return cursor;
                    }
                    continue;
                }
                byte[] name = kv[0].getBytes();
                byte[] value = kv[1].getBytes();
                for (int i = 0; i < name.length; i++) {
                    Unsafe.getUnsafe().putByte(nameScratch + i, name[i]);
                }
                for (int i = 0; i < value.length; i++) {
                    Unsafe.getUnsafe().putByte(valueScratch + i, value[i]);
                }
                int hint = HpackEncoder.HINT_NONE;
                if ("content-type".equals(kv[0])) {
                    hint = HpackEncoder.HINT_STATIC_NAME | 31;
                }
                cursor = encoder.encode(cursor, limit,
                        nameScratch, name.length,
                        valueScratch, value.length,
                        hint);
                if (cursor < 0) {
                    return cursor;
                }
            }
            return cursor;
        }
    }
}
