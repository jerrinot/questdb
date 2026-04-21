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

package io.questdb.test.cutlass.flightsql;

import com.google.protobuf.CodedInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.questdb.PropertyKey;
import io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.protobuf.HandshakeCodec;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HandshakeEndToEndTest extends AbstractBootstrapTest {

    private static final int BUF_SIZE = 16 * 1024;
    private static final byte[] PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final int SCRATCH = 1024;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testHandshakeRoundTrip() throws Exception {
        try (final TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.HTTP_H2_ENABLED.getEnvVarName(), "true",
                PropertyKey.FLIGHT_SQL_ENABLED.getEnvVarName(), "true"
        )) {
            serverMain.start();
            int port = serverMain.getConfiguration().getHttpServerConfiguration().getBindPort();
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(5_000);
                OutputStream out = socket.getOutputStream();
                out.write(PREFACE);
                out.write(buildEmptySettings());
                out.write(buildHeadersFrame(1,
                        FlightSqlDispatchListener.HANDSHAKE_PATH,
                        "POST",
                        "application/grpc+proto",
                        /*endStream*/ false));
                // 5-byte gRPC prefix + zero-length HandshakeRequest, END_STREAM.
                out.write(buildDataFrame(1, new byte[]{0, 0, 0, 0, 0}, /*endStream*/ true));
                out.flush();

                Collected collected = readUntilStreamClosed(socket.getInputStream(), 1);
                List<ReceivedFrame> stream1 = collected.streamFrames.getOrDefault(1, new ArrayList<>());
                Assert.assertTrue("expected 3 stream-1 frames, got " + stream1.size(), stream1.size() >= 3);

                ReceivedFrame first = stream1.get(0);
                Assert.assertEquals("HEADERS", first.kind);
                Assert.assertFalse("initial HEADERS should not carry END_STREAM", first.endStream);
                Assert.assertEquals("200", first.headers.get(":status"));
                Assert.assertEquals("application/grpc+proto", first.headers.get("content-type"));

                ReceivedFrame data = stream1.get(1);
                Assert.assertEquals("DATA", data.kind);
                Assert.assertEquals(0, data.payload[0]);
                int bodyLen = ((data.payload[1] & 0xFF) << 24)
                        | ((data.payload[2] & 0xFF) << 16)
                        | ((data.payload[3] & 0xFF) << 8)
                        | (data.payload[4] & 0xFF);
                Assert.assertEquals(data.payload.length - 5, bodyLen);
                byte[] body = new byte[bodyLen];
                System.arraycopy(data.payload, 5, body, 0, bodyLen);
                // Decode via Google protobuf-java as oracle.
                CodedInputStream in = CodedInputStream.newInstance(body);
                long protocolVersion = -1;
                byte[] payload = null;
                while (!in.isAtEnd()) {
                    int tag = in.readTag();
                    int fn = tag >>> 3;
                    switch (fn) {
                        case HandshakeCodec.FIELD_PROTOCOL_VERSION:
                            protocolVersion = in.readUInt64();
                            break;
                        case HandshakeCodec.FIELD_PAYLOAD:
                            payload = in.readByteArray();
                            break;
                        default:
                            in.skipField(tag);
                    }
                }
                Assert.assertEquals(0L, protocolVersion);
                Assert.assertNotNull(payload);
                Assert.assertEquals(0, payload.length);

                ReceivedFrame trailers = stream1.get(2);
                Assert.assertEquals("HEADERS", trailers.kind);
                Assert.assertTrue("trailer HEADERS must carry END_STREAM", trailers.endStream);
                Assert.assertEquals("0", trailers.headers.get("grpc-status"));
            }
        }
    }

    @Test
    public void testUnknownPathReturnsUnimplementedTrailersOnly() throws Exception {
        try (final TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.HTTP_H2_ENABLED.getEnvVarName(), "true",
                PropertyKey.FLIGHT_SQL_ENABLED.getEnvVarName(), "true"
        )) {
            serverMain.start();
            int port = serverMain.getConfiguration().getHttpServerConfiguration().getBindPort();
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(5_000);
                OutputStream out = socket.getOutputStream();
                out.write(PREFACE);
                out.write(buildEmptySettings());
                out.write(buildHeadersFrame(1,
                        "/arrow.flight.protocol.FlightService/ListFlights",
                        "POST",
                        "application/grpc+proto",
                        /*endStream*/ true));
                out.flush();

                Collected collected = readUntilStreamClosed(socket.getInputStream(), 1);
                List<ReceivedFrame> stream1 = collected.streamFrames.getOrDefault(1, new ArrayList<>());
                Assert.assertFalse("no stream-1 frames received", stream1.isEmpty());
                ReceivedFrame only = stream1.get(0);
                Assert.assertEquals("HEADERS", only.kind);
                Assert.assertTrue("trailers-only must set END_STREAM", only.endStream);
                Assert.assertEquals("unknown path must not produce DATA", 1, stream1.size());
                Assert.assertEquals("200", only.headers.get(":status"));
                Assert.assertEquals("application/grpc+proto", only.headers.get("content-type"));
                Assert.assertEquals("12", only.headers.get("grpc-status"));
            }
        }
    }

    private static byte[] buildDataFrame(int streamId, byte[] payload, boolean endStream) {
        long frame = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long payloadAddr = Unsafe.malloc(Math.max(1, payload.length), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(payloadAddr + i, payload[i]);
            }
            long end = Http2FrameWriter.writeData(frame, frame + BUF_SIZE, streamId, endStream,
                    payloadAddr, payload.length);
            Assert.assertTrue(end > 0);
            int len = (int) (end - frame);
            byte[] out = new byte[len];
            for (int i = 0; i < len; i++) {
                out[i] = Unsafe.getUnsafe().getByte(frame + i);
            }
            return out;
        } finally {
            Unsafe.free(payloadAddr, Math.max(1, payload.length), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(frame, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] buildEmptySettings() {
        long frame = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeSettings(frame, frame + BUF_SIZE, new short[0], new int[0], 0);
            Assert.assertTrue(end > 0);
            int len = (int) (end - frame);
            byte[] out = new byte[len];
            for (int i = 0; i < len; i++) {
                out[i] = Unsafe.getUnsafe().getByte(frame + i);
            }
            return out;
        } finally {
            Unsafe.free(frame, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] buildHeadersFrame(int streamId, String path, String method, String contentType, boolean endStream) {
        long scratch = Unsafe.malloc(SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
        long nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
        long frame = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, 8192, 32 * 1024);
        try {
            String[][] headers = new String[][]{
                    {":method", method},
                    {":scheme", "http"},
                    {":path", path},
                    {":authority", "localhost"},
                    {"content-type", contentType},
            };
            long cursor = encoder.beginBlock(scratch, scratch + SCRATCH * 4);
            Assert.assertTrue(cursor > 0);
            for (String[] kv : headers) {
                byte[] n = kv[0].getBytes();
                byte[] v = kv[1].getBytes();
                for (int i = 0; i < n.length; i++) {
                    Unsafe.getUnsafe().putByte(nameBuf + i, n[i]);
                }
                for (int i = 0; i < v.length; i++) {
                    Unsafe.getUnsafe().putByte(valueBuf + i, v[i]);
                }
                cursor = encoder.encode(cursor, scratch + SCRATCH * 4,
                        nameBuf, n.length, valueBuf, v.length, HpackEncoder.HINT_NONE);
                Assert.assertTrue(cursor > 0);
            }
            encoder.endBlock();
            int blockLen = (int) (cursor - scratch);
            long end = Http2FrameWriter.writeHeaders(frame, frame + BUF_SIZE, streamId,
                    endStream, true, scratch, blockLen);
            Assert.assertTrue(end > 0);
            int len = (int) (end - frame);
            byte[] out = new byte[len];
            for (int i = 0; i < len; i++) {
                out[i] = Unsafe.getUnsafe().getByte(frame + i);
            }
            return out;
        } finally {
            encoder.close();
            Unsafe.free(frame, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratch, SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Collected readUntilStreamClosed(InputStream in, int watchStreamId) throws IOException, Http2Exception {
        Collected collected = new Collected();
        DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
        ByteBuf accumulated = Unpooled.buffer();
        // EmbeddedChannel just so we get a real ChannelHandlerContext with
        // a ByteBufAllocator — DefaultHttp2FrameReader uses ctx.alloc()
        // to stage HEADERS-fragment payloads across CONTINUATION frames.
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        ChannelHandlerContext ctx = channel.pipeline().firstContext();
        try {
            byte[] chunk = new byte[4096];
            while (!collected.closedStreams.contains(watchStreamId)) {
                int n = in.read(chunk);
                if (n < 0) {
                    break;
                }
                accumulated.writeBytes(chunk, 0, n);
                while (accumulated.readableBytes() >= 9) {
                    int prefixPayloadLen = (accumulated.getUnsignedByte(accumulated.readerIndex()) << 16)
                            | (accumulated.getUnsignedByte(accumulated.readerIndex() + 1) << 8)
                            | accumulated.getUnsignedByte(accumulated.readerIndex() + 2);
                    int frameTotal = 9 + prefixPayloadLen;
                    if (accumulated.readableBytes() < frameTotal) {
                        break;
                    }
                    ByteBuf oneFrame = accumulated.readRetainedSlice(frameTotal);
                    try {
                        reader.readFrame(ctx, oneFrame, new FrameCollector(collected));
                    } finally {
                        oneFrame.release();
                    }
                }
            }
        } finally {
            accumulated.release();
            channel.finishAndReleaseAll();
        }
        return collected;
    }

    private static Map<String, String> flatten(Http2Headers headers) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<CharSequence, CharSequence> e : headers) {
            out.put(e.getKey().toString(), e.getValue().toString());
        }
        return out;
    }

    private static final class Collected {
        final Set<Integer> closedStreams = new HashSet<>();
        final Map<Integer, List<ReceivedFrame>> streamFrames = new HashMap<>();

        void add(int streamId, ReceivedFrame f) {
            streamFrames.computeIfAbsent(streamId, k -> new ArrayList<>()).add(f);
        }
    }

    private static final class FrameCollector implements Http2FrameListener {
        private final Collected collected;

        FrameCollector(Collected collected) {
            this.collected = collected;
        }

        @Override
        public int onDataRead(io.netty.channel.ChannelHandlerContext ctx, int streamId,
                              ByteBuf data, int padding, boolean endOfStream) {
            byte[] payload = new byte[data.readableBytes()];
            data.readBytes(payload);
            ReceivedFrame f = new ReceivedFrame();
            f.kind = "DATA";
            f.endStream = endOfStream;
            f.payload = payload;
            collected.add(streamId, f);
            if (endOfStream) {
                collected.closedStreams.add(streamId);
            }
            return payload.length + padding;
        }

        @Override
        public void onGoAwayRead(io.netty.channel.ChannelHandlerContext ctx, int lastStreamId,
                                 long errorCode, ByteBuf debugData) {
        }

        @Override
        public void onHeadersRead(io.netty.channel.ChannelHandlerContext ctx, int streamId,
                                  Http2Headers headers, int padding, boolean endOfStream) {
            ReceivedFrame f = new ReceivedFrame();
            f.kind = "HEADERS";
            f.endStream = endOfStream;
            f.headers = flatten(headers);
            collected.add(streamId, f);
            if (endOfStream) {
                collected.closedStreams.add(streamId);
            }
        }

        @Override
        public void onHeadersRead(io.netty.channel.ChannelHandlerContext ctx, int streamId,
                                  Http2Headers headers, int streamDependency, short weight,
                                  boolean exclusive, int padding, boolean endOfStream) {
            onHeadersRead(ctx, streamId, headers, padding, endOfStream);
        }

        @Override
        public void onPingAckRead(io.netty.channel.ChannelHandlerContext ctx, long data) {
        }

        @Override
        public void onPingRead(io.netty.channel.ChannelHandlerContext ctx, long data) {
        }

        @Override
        public void onPriorityRead(io.netty.channel.ChannelHandlerContext ctx, int streamId,
                                   int streamDependency, short weight, boolean exclusive) {
        }

        @Override
        public void onPushPromiseRead(io.netty.channel.ChannelHandlerContext ctx, int streamId,
                                      int promisedStreamId, Http2Headers headers, int padding) {
        }

        @Override
        public void onRstStreamRead(io.netty.channel.ChannelHandlerContext ctx, int streamId, long errorCode) {
            collected.closedStreams.add(streamId);
        }

        @Override
        public void onSettingsAckRead(io.netty.channel.ChannelHandlerContext ctx) {
        }

        @Override
        public void onSettingsRead(io.netty.channel.ChannelHandlerContext ctx, Http2Settings settings) {
        }

        @Override
        public void onUnknownFrame(io.netty.channel.ChannelHandlerContext ctx, byte frameType,
                                   int streamId, io.netty.handler.codec.http2.Http2Flags flags,
                                   ByteBuf payload) {
        }

        @Override
        public void onWindowUpdateRead(io.netty.channel.ChannelHandlerContext ctx, int streamId,
                                       int windowSizeIncrement) {
        }
    }

    private static final class ReceivedFrame {
        boolean endStream;
        Map<String, String> headers;
        String kind;
        byte[] payload;
    }
}
