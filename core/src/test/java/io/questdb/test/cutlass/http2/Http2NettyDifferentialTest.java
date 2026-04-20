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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.questdb.cutlass.http2.Http2ErrorCode;
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tier 1 differential against Netty's reference HTTP/2 codec.
 * <p>
 * Our {@link Http2FrameWriter} emits into a native buffer; the test copies the
 * bytes into a Netty {@link ByteBuf}, runs {@link DefaultHttp2FrameReader}
 * over them, and asserts Netty decodes back the same values. The reverse
 * direction (Netty writer -> our reader) also runs for symmetric coverage.
 */
public class Http2NettyDifferentialTest {

    private static final int BUF_SIZE = 64 * 1024;

    @Test
    public void testDataFrameParsedByNetty() throws Http2Exception {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] expected = new byte[4096];
            for (int i = 0; i < expected.length; i++) {
                expected[i] = (byte) (i * 31 + 7);
                Unsafe.getUnsafe().putByte(payload + i, expected[i]);
            }
            long end = Http2FrameWriter.writeData(buf, buf + BUF_SIZE, 7, true, payload, expected.length);
            Assert.assertTrue(end > 0);

            ByteBuf bb = copyToByteBuf(buf, (int) (end - buf));
            try {
                CapturingListener cap = new CapturingListener();
                DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
                reader.readFrame(null, bb, cap);
                Assert.assertEquals("DATA", cap.lastEvent);
                Assert.assertEquals(7, cap.streamId);
                Assert.assertTrue(cap.endStream);
                Assert.assertArrayEquals(expected, cap.payload);
            } finally {
                bb.release();
            }
        } finally {
            Unsafe.free(payload, 4096, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGoAwayParsedByNetty() throws Http2Exception {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long debug = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] debugBytes = "protocol_error!!".getBytes();
            for (int i = 0; i < debugBytes.length; i++) {
                Unsafe.getUnsafe().putByte(debug + i, debugBytes[i]);
            }
            long end = Http2FrameWriter.writeGoAway(buf, buf + BUF_SIZE, 42,
                    Http2ErrorCode.PROTOCOL_ERROR, debug, debugBytes.length);

            ByteBuf bb = copyToByteBuf(buf, (int) (end - buf));
            try {
                CapturingListener cap = new CapturingListener();
                DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
                reader.readFrame(null, bb, cap);
                Assert.assertEquals("GOAWAY", cap.lastEvent);
                Assert.assertEquals(42, cap.lastStreamId);
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, cap.errorCode);
                Assert.assertArrayEquals(debugBytes, cap.payload);
            } finally {
                bb.release();
            }
        } finally {
            Unsafe.free(debug, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNettyDataFrameParsedByUs() {
        DefaultHttp2FrameWriter writer = new DefaultHttp2FrameWriter();
        EmbeddedChannel ch = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        try {
            ByteBuf data = Unpooled.buffer(256);
            for (int i = 0; i < 256; i++) {
                data.writeByte(i);
            }
            writer.writeData(ch.pipeline().firstContext(), 9, data, 0, false, ch.newPromise());
            ch.flushOutbound();

            byte[] frame = drainOutbound(ch);
            long buf = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < frame.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, frame[i]);
                }
                Http2FrameHeader h = new Http2FrameHeader();
                int consumed = new io.questdb.cutlass.http2.Http2FrameReader().tryReadNext(
                        buf, buf + frame.length, h, io.questdb.cutlass.http2.Http2Settings.MAX_FRAME_SIZE_UPPER);
                Assert.assertEquals(frame.length, consumed);
                Assert.assertEquals(io.questdb.cutlass.http2.Http2FrameType.DATA, h.getType());
                Assert.assertEquals(9, h.getStreamId());
                Assert.assertEquals(256, h.getPayloadLength());
                Assert.assertFalse(io.questdb.cutlass.http2.Http2Flags.hasEndStream(h.getFlags()));
                for (int i = 0; i < 256; i++) {
                    Assert.assertEquals((byte) i, Unsafe.getUnsafe().getByte(h.getPayloadAddr() + i));
                }
            } finally {
                Unsafe.free(buf, frame.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void testNettyPingParsedByUs() {
        DefaultHttp2FrameWriter writer = new DefaultHttp2FrameWriter();
        EmbeddedChannel ch = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        try {
            long opaque = 0x0102030405060708L;
            writer.writePing(ch.pipeline().firstContext(), false, opaque, ch.newPromise());
            ch.flushOutbound();

            byte[] frame = drainOutbound(ch);
            long buf = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < frame.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, frame[i]);
                }
                Http2FrameHeader h = new Http2FrameHeader();
                int consumed = new io.questdb.cutlass.http2.Http2FrameReader().tryReadNext(
                        buf, buf + frame.length, h, io.questdb.cutlass.http2.Http2Settings.MAX_FRAME_SIZE_UPPER);
                Assert.assertEquals(frame.length, consumed);
                Assert.assertEquals(io.questdb.cutlass.http2.Http2FrameType.PING, h.getType());
                Assert.assertEquals(8, h.getPayloadLength());
                for (int i = 0; i < 8; i++) {
                    Assert.assertEquals((byte) ((opaque >>> (56 - 8 * i)) & 0xFF),
                            Unsafe.getUnsafe().getByte(h.getPayloadAddr() + i));
                }
            } finally {
                Unsafe.free(buf, frame.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ch.finishAndReleaseAll();
        }
    }

    @Test
    public void testPingParsedByNetty() throws Http2Exception {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long opaque = 0xFEEDFACECAFEBEEFL;
            long end = Http2FrameWriter.writePing(buf, buf + BUF_SIZE, false, opaque);

            ByteBuf bb = copyToByteBuf(buf, (int) (end - buf));
            try {
                CapturingListener cap = new CapturingListener();
                DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
                reader.readFrame(null, bb, cap);
                Assert.assertEquals("PING", cap.lastEvent);
                Assert.assertEquals(opaque, cap.pingData);
                Assert.assertFalse(cap.pingAck);
            } finally {
                bb.release();
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRstStreamParsedByNetty() throws Http2Exception {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeRstStream(buf, buf + BUF_SIZE, 21, Http2ErrorCode.CANCEL);

            ByteBuf bb = copyToByteBuf(buf, (int) (end - buf));
            try {
                CapturingListener cap = new CapturingListener();
                DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
                reader.readFrame(null, bb, cap);
                Assert.assertEquals("RST_STREAM", cap.lastEvent);
                Assert.assertEquals(21, cap.streamId);
                Assert.assertEquals(Http2ErrorCode.CANCEL, cap.errorCode);
            } finally {
                bb.release();
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSettingsParsedByNetty() throws Http2Exception {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            short[] ids = {
                    io.questdb.cutlass.http2.Http2Settings.MAX_FRAME_SIZE,
                    io.questdb.cutlass.http2.Http2Settings.INITIAL_WINDOW_SIZE
            };
            int[] values = {131_072, 1_048_576};
            long end = Http2FrameWriter.writeSettings(buf, buf + BUF_SIZE, ids, values, 2);

            ByteBuf bb = copyToByteBuf(buf, (int) (end - buf));
            try {
                CapturingListener cap = new CapturingListener();
                DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
                reader.readFrame(null, bb, cap);
                Assert.assertEquals("SETTINGS", cap.lastEvent);
                Assert.assertFalse(cap.settingsAck);
                Assert.assertEquals(Integer.valueOf(131_072), cap.settings.maxFrameSize());
                Assert.assertEquals(Integer.valueOf(1_048_576), cap.settings.initialWindowSize());
            } finally {
                bb.release();
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWindowUpdateParsedByNetty() throws Http2Exception {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeWindowUpdate(buf, buf + BUF_SIZE, 13, 524_288);

            ByteBuf bb = copyToByteBuf(buf, (int) (end - buf));
            try {
                CapturingListener cap = new CapturingListener();
                DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
                reader.readFrame(null, bb, cap);
                Assert.assertEquals("WINDOW_UPDATE", cap.lastEvent);
                Assert.assertEquals(13, cap.streamId);
                Assert.assertEquals(524_288, cap.windowIncrement);
            } finally {
                bb.release();
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static ByteBuf copyToByteBuf(long addr, int len) {
        ByteBuf bb = Unpooled.directBuffer(len);
        for (int i = 0; i < len; i++) {
            bb.writeByte(Unsafe.getUnsafe().getByte(addr + i));
        }
        return bb;
    }

    /**
     * Netty may split a single frame across multiple outbound messages (the
     * 9-byte header and the payload ByteBuf are commonly written separately).
     * Drain them all into a single contiguous byte[] so the test sees one frame.
     */
    private static byte[] drainOutbound(EmbeddedChannel ch) {
        int total = 0;
        java.util.List<byte[]> chunks = new java.util.ArrayList<>();
        ByteBuf part;
        while ((part = ch.readOutbound()) != null) {
            byte[] chunk = new byte[part.readableBytes()];
            part.readBytes(chunk);
            part.release();
            chunks.add(chunk);
            total += chunk.length;
        }
        byte[] out = new byte[total];
        int off = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, out, off, chunk.length);
            off += chunk.length;
        }
        return out;
    }

    private static final class CapturingListener implements Http2FrameListener {
        final DefaultHttp2HeadersDecoder headersDecoder = new DefaultHttp2HeadersDecoder();
        int errorCode;
        boolean endStream;
        int lastStreamId;
        String lastEvent;
        byte[] payload;
        boolean pingAck;
        long pingData;
        boolean settingsAck;
        Http2Settings settings;
        int streamId;
        int windowIncrement;

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream) {
            this.lastEvent = "DATA";
            this.streamId = streamId;
            this.endStream = endOfStream;
            int readable = data.readableBytes();
            this.payload = new byte[readable];
            data.readBytes(this.payload);
            return readable + padding;
        }

        @Override
        public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
            this.lastEvent = "GOAWAY";
            this.lastStreamId = lastStreamId;
            this.errorCode = (int) errorCode;
            this.payload = new byte[debugData.readableBytes()];
            debugData.readBytes(this.payload);
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding, boolean endOfStream) {
            this.lastEvent = "HEADERS";
            this.streamId = streamId;
            this.endStream = endOfStream;
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency, short weight, boolean exclusive, int padding, boolean endOfStream) {
            this.lastEvent = "HEADERS";
            this.streamId = streamId;
            this.endStream = endOfStream;
        }

        @Override
        public void onPingAckRead(ChannelHandlerContext ctx, long data) {
            this.lastEvent = "PING";
            this.pingAck = true;
            this.pingData = data;
        }

        @Override
        public void onPingRead(ChannelHandlerContext ctx, long data) {
            this.lastEvent = "PING";
            this.pingAck = false;
            this.pingData = data;
        }

        @Override
        public void onPriorityRead(ChannelHandlerContext ctx, int streamId, int streamDependency, short weight, boolean exclusive) {
            this.lastEvent = "PRIORITY";
            this.streamId = streamId;
        }

        @Override
        public void onPushPromiseRead(ChannelHandlerContext ctx, int streamId, int promisedStreamId, Http2Headers headers, int padding) {
            this.lastEvent = "PUSH_PROMISE";
        }

        @Override
        public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
            this.lastEvent = "RST_STREAM";
            this.streamId = streamId;
            this.errorCode = (int) errorCode;
        }

        @Override
        public void onSettingsAckRead(ChannelHandlerContext ctx) {
            this.lastEvent = "SETTINGS";
            this.settingsAck = true;
        }

        @Override
        public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
            this.lastEvent = "SETTINGS";
            this.settingsAck = false;
            this.settings = settings;
        }

        @Override
        public void onUnknownFrame(ChannelHandlerContext ctx, byte frameType, int streamId, io.netty.handler.codec.http2.Http2Flags flags, ByteBuf payload) {
            this.lastEvent = "UNKNOWN";
        }

        @Override
        public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
            this.lastEvent = "WINDOW_UPDATE";
            this.streamId = streamId;
            this.windowIncrement = windowSizeIncrement;
        }
    }
}
