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
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2Settings;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * HTTP2_INTEGRATION.md §15.5 step 2 coverage. The engine captures
 * {@code :method}, {@code :scheme}, {@code :path}, {@code :authority}, and
 * {@code content-type} into the per-stream staging buffer; every other
 * field is consumed and dropped. Staging overflow resets the stream with
 * {@code PROTOCOL_ERROR} and no {@code onRequestHeaders} fires.
 */
public class Http2PseudoHeaderCaptureTest {

    private static final int SCRATCH = 256;
    private static final int SEND_CAP = 16 * 1024;

    @Test
    public void testAllFiveSlotsCaptured() {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN);
        CapturingListener listener = new CapturingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long scratch = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long frame = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        try {
            String[][] headers = new String[][]{
                    {":method", "POST"},
                    {":scheme", "https"},
                    {":path", "/arrow.flight.protocol.FlightService/DoGet"},
                    {":authority", "flight.example.com"},
                    {"content-type", "application/grpc"},
                    {"user-agent", "grpc-java-netty/1.60.0"},
            };
            int blockLen = encodeBlock(scratch, nameBuf, valueBuf, headers);
            long end = Http2FrameWriter.writeHeaders(frame, frame + SEND_CAP, 1,
                    /*endStream*/ true, /*endHeaders*/ true, scratch, blockLen);
            ctx.processReceivedBytes(frame, end);

            Assert.assertEquals(1, listener.requestHeadersCount);
            Assert.assertEquals(1, listener.lastStreamId);
            Assert.assertTrue(listener.lastEndStream);
            Assert.assertEquals("POST", listener.capturedMethod);
            Assert.assertEquals("https", listener.capturedScheme);
            Assert.assertEquals("/arrow.flight.protocol.FlightService/DoGet", listener.capturedPath);
            Assert.assertEquals("flight.example.com", listener.capturedAuthority);
            Assert.assertEquals("application/grpc", listener.capturedContentType);
            Assert.assertEquals("regular headers outside the capture set must be dropped",
                    0, listener.perFieldCallCount);
        } finally {
            Unsafe.free(frame, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
        Assert.assertEquals("no NATIVE_HTTP_CONN leak across full cycle",
                before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN));
    }

    @Test
    public void testAuthorityAbsent() {
        CapturingListener listener = new CapturingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long scratch = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long frame = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        try {
            String[][] headers = new String[][]{
                    {":method", "GET"},
                    {":scheme", "http"},
                    {":path", "/"},
                    {"content-type", "application/grpc"},
            };
            int blockLen = encodeBlock(scratch, nameBuf, valueBuf, headers);
            long end = Http2FrameWriter.writeHeaders(frame, frame + SEND_CAP, 1,
                    true, true, scratch, blockLen);
            ctx.processReceivedBytes(frame, end);

            Assert.assertEquals(1, listener.requestHeadersCount);
            Assert.assertEquals("GET", listener.capturedMethod);
            Assert.assertEquals("/", listener.capturedPath);
            Assert.assertEquals("http", listener.capturedScheme);
            Assert.assertNull("absent :authority slot reports null via view", listener.capturedAuthority);
            Assert.assertEquals("application/grpc", listener.capturedContentType);
        } finally {
            Unsafe.free(frame, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testContentTypeAbsent() {
        CapturingListener listener = new CapturingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long scratch = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long frame = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        try {
            String[][] headers = new String[][]{
                    {":method", "GET"},
                    {":scheme", "https"},
                    {":path", "/"},
                    {":authority", "localhost"},
            };
            int blockLen = encodeBlock(scratch, nameBuf, valueBuf, headers);
            long end = Http2FrameWriter.writeHeaders(frame, frame + SEND_CAP, 1,
                    true, true, scratch, blockLen);
            ctx.processReceivedBytes(frame, end);

            Assert.assertEquals(1, listener.requestHeadersCount);
            Assert.assertEquals("GET", listener.capturedMethod);
            Assert.assertEquals("https", listener.capturedScheme);
            Assert.assertEquals("/", listener.capturedPath);
            Assert.assertEquals("localhost", listener.capturedAuthority);
            Assert.assertNull(listener.capturedContentType);
        } finally {
            Unsafe.free(frame, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testSequentialRequestsOverwriteCleanly() {
        // Two streams (3 then 5) on the same connection. The second
        // request's slots must fully overwrite the first so a router
        // never reads cross-stream mixed state.
        CapturingListener listener = new CapturingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, Http2ConnectionConfig.defaults());
        long scratch = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long frame = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        try {
            String[][] firstHeaders = new String[][]{
                    {":method", "POST"},
                    {":scheme", "https"},
                    {":path", "/first"},
                    {":authority", "a.example.com"},
                    {"content-type", "application/grpc+proto"},
            };
            int firstBlockLen = encodeBlock(scratch, nameBuf, valueBuf, firstHeaders);
            long cursor = Http2FrameWriter.writeHeaders(frame, frame + SEND_CAP, 3,
                    true, true, scratch, firstBlockLen);
            ctx.processReceivedBytes(frame, cursor);

            Assert.assertEquals(1, listener.requestHeadersCount);
            Assert.assertEquals("/first", listener.capturedPath);
            Assert.assertEquals("application/grpc+proto", listener.capturedContentType);
            Assert.assertEquals(3, listener.lastStreamId);

            String[][] secondHeaders = new String[][]{
                    {":method", "GET"},
                    {":scheme", "http"},
                    {":path", "/second"},
                    {":authority", "b.example.com"},
                    {"content-type", "application/grpc"},
            };
            int secondBlockLen = encodeBlock(scratch, nameBuf, valueBuf, secondHeaders);
            cursor = Http2FrameWriter.writeHeaders(frame, frame + SEND_CAP, 5,
                    true, true, scratch, secondBlockLen);
            ctx.processReceivedBytes(frame, cursor);

            Assert.assertEquals(2, listener.requestHeadersCount);
            Assert.assertEquals(5, listener.lastStreamId);
            Assert.assertEquals("GET", listener.capturedMethod);
            Assert.assertEquals("http", listener.capturedScheme);
            Assert.assertEquals("/second", listener.capturedPath);
            Assert.assertEquals("b.example.com", listener.capturedAuthority);
            Assert.assertEquals("application/grpc", listener.capturedContentType);
        } finally {
            Unsafe.free(frame, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    @Test
    public void testStagingOverflowResetsStreamAndSuppressesListener() {
        // Tiny staging budget (64 bytes) forces overflow on a long
        // :path. No onRequestHeaders must fire; instead the engine
        // enqueues RST_STREAM(PROTOCOL_ERROR) on the outbound queue.
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withHeaderStagingBytesPerStream(64)
                .build();
        CapturingListener listener = new CapturingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, cfg);
        long scratch = Unsafe.malloc(SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
        long nameBuf = Unsafe.malloc(SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
        long frame = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
        try {
            StringBuilder longPath = new StringBuilder("/");
            for (int i = 0; i < 500; i++) {
                longPath.append('x');
            }
            String[][] headers = new String[][]{
                    {":method", "GET"},
                    {":scheme", "https"},
                    {":path", longPath.toString()},
                    {":authority", "localhost"},
                    {"content-type", "application/grpc"},
            };
            int blockLen = encodeBlock(scratch, nameBuf, valueBuf, headers);
            long end = Http2FrameWriter.writeHeaders(frame, frame + SEND_CAP, 1,
                    true, true, scratch, blockLen);
            ctx.processReceivedBytes(frame, end);

            Assert.assertEquals("listener must NOT fire on overflow",
                    0, listener.requestHeadersCount);
            Assert.assertEquals(1, ctx.getPendingCount());
            long cursor = ctx.writePending(send, send + SEND_CAP);
            Assert.assertTrue(cursor > send);
            Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR,
                    firstRstStreamErrorCode(send, cursor, 1));
        } finally {
            Unsafe.free(send, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(frame, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
    }

    /**
     * Encodes {@code headers} into {@code scratch} using an independent
     * {@link HpackEncoder} so the engine's shared decoder is driven
     * through the normal decode path. Returns the encoded block length.
     */
    private static int encodeBlock(long scratch, long nameBuf, long valueBuf, String[][] headers) {
        HpackEncoder encoder = new HpackEncoder(
                Http2Settings.DEFAULT_HEADER_TABLE_SIZE,
                0,
                Math.max(4096, Http2Settings.DEFAULT_HEADER_TABLE_SIZE),
                16 * 1024,
                64 * 1024);
        try {
            long limit = scratch + 64 * 1024;
            long cursor = encoder.beginBlock(scratch, limit);
            Assert.assertTrue("beginBlock must not fail in the test fixture", cursor >= 0);
            for (String[] kv : headers) {
                byte[] name = kv[0].getBytes();
                byte[] value = kv[1].getBytes();
                for (int i = 0; i < name.length; i++) {
                    Unsafe.getUnsafe().putByte(nameBuf + i, name[i]);
                }
                for (int i = 0; i < value.length; i++) {
                    Unsafe.getUnsafe().putByte(valueBuf + i, value[i]);
                }
                cursor = encoder.encode(cursor, limit,
                        nameBuf, name.length,
                        valueBuf, value.length,
                        HpackEncoder.HINT_NONE);
                Assert.assertTrue("encode must not overflow the scratch", cursor >= 0);
            }
            encoder.endBlock();
            return (int) (cursor - scratch);
        } finally {
            encoder.close();
        }
    }

    /**
     * Scans {@code [start, end)} for the first {@code RST_STREAM} frame
     * targeting {@code streamId} and returns the error code. Asserts
     * fail if no such frame is found.
     */
    private static int firstRstStreamErrorCode(long start, long end, int streamId) {
        Http2FrameReader reader = new Http2FrameReader();
        Http2FrameHeader header = new Http2FrameHeader();
        long cursor = start;
        while (cursor < end) {
            int n = reader.tryReadNext(cursor, end, header, Http2Settings.MAX_FRAME_SIZE_UPPER);
            if (n == 0) {
                break;
            }
            if (header.getType() == Http2FrameType.RST_STREAM
                    && header.getStreamId() == streamId) {
                long payload = header.getPayloadAddr();
                int b0 = Unsafe.getUnsafe().getByte(payload) & 0xFF;
                int b1 = Unsafe.getUnsafe().getByte(payload + 1) & 0xFF;
                int b2 = Unsafe.getUnsafe().getByte(payload + 2) & 0xFF;
                int b3 = Unsafe.getUnsafe().getByte(payload + 3) & 0xFF;
                return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
            }
            cursor += n;
        }
        Assert.fail("no RST_STREAM frame for stream " + streamId);
        return -1;
    }

    private static final class CapturingListener implements Http2StreamListener {
        String capturedAuthority;
        String capturedContentType;
        String capturedMethod;
        String capturedPath;
        String capturedScheme;
        boolean lastEndStream;
        int lastStreamId = -1;
        int perFieldCallCount;
        int requestHeadersCount;

        @Override
        public boolean onData(int streamId, long addr, int dataLen, boolean endStream, int generationToken) {
            return true;
        }

        @Override
        public void onRequestHeader(int streamId, long nameAddr, int nameLen,
                                    long valueAddr, int valueLen, boolean neverIndexed) {
            perFieldCallCount++;
        }

        @Override
        public void onRequestHeaders(int streamId, Http2RequestHeadersView view, boolean endStream) {
            requestHeadersCount++;
            lastStreamId = streamId;
            lastEndStream = endStream;
            capturedMethod = read(view.getMethodAddr(), view.getMethodLen());
            capturedScheme = read(view.getSchemeAddr(), view.getSchemeLen());
            capturedPath = read(view.getPathAddr(), view.getPathLen());
            capturedAuthority = read(view.getAuthorityAddr(), view.getAuthorityLen());
            capturedContentType = read(view.getContentTypeAddr(), view.getContentTypeLen());
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

        private static String read(long addr, int len) {
            if (len <= 0) {
                return null;
            }
            byte[] out = new byte[len];
            for (int i = 0; i < len; i++) {
                out[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            return new String(out);
        }
    }
}
