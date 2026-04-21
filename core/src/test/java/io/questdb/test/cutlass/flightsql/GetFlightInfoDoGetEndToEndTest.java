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
import com.google.protobuf.CodedOutputStream;
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
import io.questdb.cutlass.flightsql.proto.FlightDataCodec;
import io.questdb.cutlass.flightsql.proto.FlightDescriptorCodec;
import io.questdb.cutlass.flightsql.proto.FlightEndpointCodec;
import io.questdb.cutlass.flightsql.proto.FlightInfoCodec;
import io.questdb.cutlass.flightsql.proto.LocationCodec;
import io.questdb.cutlass.flightsql.proto.TicketCodec;
import io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.protobuf.ProtobufWireFormat;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetFlightInfoDoGetEndToEndTest extends AbstractBootstrapTest {

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
    public void testDoGetWithUnknownTicketReturnsInvalidArgument() throws Exception {
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
                        FlightSqlDispatchListener.DO_GET_PATH,
                        "POST",
                        "application/grpc+proto",
                        /*endStream*/ false));
                byte[] forgedTicketBytes = new byte[]{0, 0, 0, 0, 0, 0, 0, 99};
                byte[] ticketProto = encodeTicketProto(forgedTicketBytes);
                byte[] grpcPayload = new byte[5 + ticketProto.length];
                grpcPayload[0] = 0;
                grpcPayload[1] = (byte) ((ticketProto.length >>> 24) & 0xFF);
                grpcPayload[2] = (byte) ((ticketProto.length >>> 16) & 0xFF);
                grpcPayload[3] = (byte) ((ticketProto.length >>> 8) & 0xFF);
                grpcPayload[4] = (byte) (ticketProto.length & 0xFF);
                System.arraycopy(ticketProto, 0, grpcPayload, 5, ticketProto.length);
                out.write(buildDataFrame(1, grpcPayload, true));
                out.flush();
                Collected collected = readUntilStreamClosed(socket.getInputStream(), 1);
                List<ReceivedFrame> stream1 = collected.streamFrames.get(1);
                Assert.assertNotNull(stream1);
                Assert.assertFalse(stream1.isEmpty());
                ReceivedFrame only = stream1.get(0);
                Assert.assertEquals("HEADERS", only.kind);
                Assert.assertTrue("trailers-only must set END_STREAM", only.endStream);
                Assert.assertEquals("3", only.headers.get("grpc-status"));
            }
        }
    }

    @Test
    public void testGetFlightInfoThenDoGetReturnsThreeRows() throws Exception {
        try (final TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.HTTP_H2_ENABLED.getEnvVarName(), "true",
                PropertyKey.FLIGHT_SQL_ENABLED.getEnvVarName(), "true"
        )) {
            serverMain.start();
            int port = serverMain.getConfiguration().getHttpServerConfiguration().getBindPort();
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(10_000);
                OutputStream out = socket.getOutputStream();
                out.write(PREFACE);
                out.write(buildEmptySettings());

                // Stream 1: GetFlightInfo.
                out.write(buildHeadersFrame(1,
                        FlightSqlDispatchListener.GET_FLIGHT_INFO_PATH,
                        "POST",
                        "application/grpc+proto",
                        /*endStream*/ false));
                byte[] descriptorBytes = encodeFlightDescriptor("SELECT 1");
                out.write(buildGrpcDataFrame(1, descriptorBytes, true));
                out.flush();

                Collected c1 = readUntilStreamClosed(socket.getInputStream(), 1);
                List<ReceivedFrame> s1 = c1.streamFrames.get(1);
                Assert.assertNotNull("stream 1 frames missing", s1);
                Assert.assertTrue("expected HEADERS+DATA+TRAILERS, got " + s1.size(), s1.size() >= 3);

                Assert.assertEquals("HEADERS", s1.get(0).kind);
                Assert.assertEquals("200", s1.get(0).headers.get(":status"));
                Assert.assertEquals("DATA", s1.get(1).kind);
                // Extract gRPC framed message body (1-byte compressed flag + 4-byte len).
                byte[] flightInfoProto = extractGrpcBody(s1.get(1).payload);
                Assert.assertEquals("HEADERS", s1.get(2).kind);
                Assert.assertEquals("0", s1.get(2).headers.get("grpc-status"));

                // Decode FlightInfo to recover schema bytes and ticket.
                byte[] schemaBytes;
                byte[] ticketBytes;
                CodedInputStream in = CodedInputStream.newInstance(flightInfoProto);
                byte[] endpointBytes = null;
                schemaBytes = null;
                while (!in.isAtEnd()) {
                    int tag = in.readTag();
                    int fn = ProtobufWireFormat.fieldNumberOf(tag);
                    switch (fn) {
                        case FlightInfoCodec.FIELD_SCHEMA:
                            schemaBytes = in.readByteArray();
                            break;
                        case FlightInfoCodec.FIELD_ENDPOINT:
                            if (endpointBytes == null) {
                                endpointBytes = in.readByteArray();
                            } else {
                                in.skipField(tag);
                            }
                            break;
                        default:
                            in.skipField(tag);
                    }
                }
                Assert.assertNotNull("schema missing", schemaBytes);
                Assert.assertNotNull("endpoint missing", endpointBytes);

                ticketBytes = null;
                String gotUri = null;
                CodedInputStream ein = CodedInputStream.newInstance(endpointBytes);
                while (!ein.isAtEnd()) {
                    int tag = ein.readTag();
                    int fn = ProtobufWireFormat.fieldNumberOf(tag);
                    if (fn == FlightEndpointCodec.FIELD_TICKET) {
                        byte[] inner = ein.readByteArray();
                        CodedInputStream tin = CodedInputStream.newInstance(inner);
                        while (!tin.isAtEnd()) {
                            int t = tin.readTag();
                            if (ProtobufWireFormat.fieldNumberOf(t) == TicketCodec.FIELD_TICKET) {
                                ticketBytes = tin.readByteArray();
                            } else {
                                tin.skipField(t);
                            }
                        }
                    } else if (fn == FlightEndpointCodec.FIELD_LOCATION) {
                        byte[] inner = ein.readByteArray();
                        CodedInputStream lin = CodedInputStream.newInstance(inner);
                        while (!lin.isAtEnd()) {
                            int t = lin.readTag();
                            if (ProtobufWireFormat.fieldNumberOf(t) == LocationCodec.FIELD_URI) {
                                gotUri = lin.readString();
                            } else {
                                lin.skipField(t);
                            }
                        }
                    } else {
                        ein.skipField(tag);
                    }
                }
                Assert.assertEquals(8, ticketBytes.length);
                Assert.assertEquals("arrow-flight-reuse-connection://", gotUri);

                // Sanity check: decode schema bytes with Arrow Java so we
                // know the handler produced a valid IPC message.
                Message schemaMsg = Message.getRootAsMessage(ByteBuffer.wrap(schemaBytes));
                Schema schemaFb = (Schema) schemaMsg.header(new Schema());
                Assert.assertNotNull(schemaFb);
                Assert.assertEquals(1, schemaFb.fieldsLength());
                Assert.assertEquals("col1", schemaFb.fields(0).name());

                // Stream 3: DoGet carrying the ticket.
                out.write(buildHeadersFrame(3,
                        FlightSqlDispatchListener.DO_GET_PATH,
                        "POST",
                        "application/grpc+proto",
                        /*endStream*/ false));
                byte[] ticketProto = encodeTicketProto(ticketBytes);
                out.write(buildGrpcDataFrame(3, ticketProto, true));
                out.flush();

                Collected c2 = readUntilStreamClosed(socket.getInputStream(), 3);
                List<ReceivedFrame> s3 = c2.streamFrames.get(3);
                Assert.assertNotNull("stream 3 frames missing", s3);
                Assert.assertTrue("expected HEADERS + 2 DATA + TRAILERS, got " + s3.size(),
                        s3.size() >= 4);

                Assert.assertEquals("HEADERS", s3.get(0).kind);
                Assert.assertEquals("200", s3.get(0).headers.get(":status"));
                Assert.assertEquals("DATA", s3.get(1).kind);
                Assert.assertEquals("DATA", s3.get(2).kind);
                Assert.assertEquals("HEADERS", s3.get(3).kind);
                Assert.assertEquals("0", s3.get(3).headers.get("grpc-status"));

                // Message 1: FlightData{data_header = schema, no body}.
                byte[] msg1 = extractGrpcBody(s3.get(1).payload);
                FlightDataParts p1 = parseFlightData(msg1);
                Assert.assertNotNull("message 1 header missing", p1.header);
                Assert.assertNull("message 1 body must be empty", p1.body);
                // Schema bytes must match what GetFlightInfo returned.
                Assert.assertArrayEquals(schemaBytes, p1.header);

                // Message 2: FlightData{data_header = RecordBatch, data_body = values}.
                byte[] msg2 = extractGrpcBody(s3.get(2).payload);
                FlightDataParts p2 = parseFlightData(msg2);
                Assert.assertNotNull("message 2 header missing", p2.header);
                Assert.assertNotNull("message 2 body missing", p2.body);
                Assert.assertEquals(24, p2.body.length);

                // Decode the record batch via Arrow Java.
                org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                        org.apache.arrow.vector.types.pojo.Schema.convertSchema(schemaFb);
                try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
                     VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, alloc)) {
                    ArrowBuf body = alloc.buffer(p2.body.length);
                    body.setBytes(0, p2.body);
                    Message rbMsg = Message.getRootAsMessage(ByteBuffer.wrap(p2.header));
                    ArrowRecordBatch arb = MessageSerializer.deserializeRecordBatch(rbMsg, body);
                    try {
                        VectorLoader loader = new VectorLoader(root);
                        loader.load(arb);
                    } finally {
                        arb.close();
                    }
                    BigIntVector col = (BigIntVector) root.getVector("col1");
                    Assert.assertEquals(3, col.getValueCount());
                    Assert.assertEquals(1L, col.get(0));
                    Assert.assertEquals(2L, col.get(1));
                    Assert.assertEquals(3L, col.get(2));
                }
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

    private static byte[] buildGrpcDataFrame(int streamId, byte[] message, boolean endStream) {
        byte[] framed = new byte[5 + message.length];
        framed[0] = 0;
        framed[1] = (byte) ((message.length >>> 24) & 0xFF);
        framed[2] = (byte) ((message.length >>> 16) & 0xFF);
        framed[3] = (byte) ((message.length >>> 8) & 0xFF);
        framed[4] = (byte) (message.length & 0xFF);
        System.arraycopy(message, 0, framed, 5, message.length);
        return buildDataFrame(streamId, framed, endStream);
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

    private static byte[] encodeFlightDescriptor(String sql) throws IOException {
        byte[] cmd = sql.getBytes(StandardCharsets.UTF_8);
        byte[] buf = new byte[64 + cmd.length];
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        out.writeEnum(FlightDescriptorCodec.FIELD_TYPE, FlightDescriptorCodec.TYPE_CMD);
        out.writeByteArray(FlightDescriptorCodec.FIELD_CMD, cmd);
        int len = buf.length - out.spaceLeft();
        byte[] res = new byte[len];
        System.arraycopy(buf, 0, res, 0, len);
        return res;
    }

    private static byte[] encodeTicketProto(byte[] ticketBytes) throws IOException {
        byte[] buf = new byte[32 + ticketBytes.length];
        CodedOutputStream out = CodedOutputStream.newInstance(buf);
        out.writeByteArray(TicketCodec.FIELD_TICKET, ticketBytes);
        int len = buf.length - out.spaceLeft();
        byte[] res = new byte[len];
        System.arraycopy(buf, 0, res, 0, len);
        return res;
    }

    private static byte[] extractGrpcBody(byte[] dataPayload) {
        Assert.assertTrue("gRPC framed DATA frame too short: " + dataPayload.length,
                dataPayload.length >= 5);
        Assert.assertEquals("unexpected compression flag", 0, dataPayload[0]);
        int bodyLen = ((dataPayload[1] & 0xFF) << 24)
                | ((dataPayload[2] & 0xFF) << 16)
                | ((dataPayload[3] & 0xFF) << 8)
                | (dataPayload[4] & 0xFF);
        Assert.assertEquals(dataPayload.length - 5, bodyLen);
        byte[] body = new byte[bodyLen];
        System.arraycopy(dataPayload, 5, body, 0, bodyLen);
        return body;
    }

    private static Map<String, String> flatten(Http2Headers headers) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<CharSequence, CharSequence> e : headers) {
            out.put(e.getKey().toString(), e.getValue().toString());
        }
        return out;
    }

    private static FlightDataParts parseFlightData(byte[] bytes) throws IOException {
        CodedInputStream in = CodedInputStream.newInstance(bytes);
        FlightDataParts parts = new FlightDataParts();
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            int fn = ProtobufWireFormat.fieldNumberOf(tag);
            if (fn == FlightDataCodec.FIELD_DATA_HEADER) {
                parts.header = in.readByteArray();
            } else if (fn == FlightDataCodec.FIELD_DATA_BODY) {
                parts.body = in.readByteArray();
            } else {
                in.skipField(tag);
            }
        }
        return parts;
    }

    private static Collected readUntilStreamClosed(InputStream in, int watchStreamId) throws IOException, Http2Exception {
        Collected collected = new Collected();
        DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
        ByteBuf accumulated = Unpooled.buffer();
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
                    int payloadLen = (accumulated.getUnsignedByte(accumulated.readerIndex()) << 16)
                            | (accumulated.getUnsignedByte(accumulated.readerIndex() + 1) << 8)
                            | accumulated.getUnsignedByte(accumulated.readerIndex() + 2);
                    int frameTotal = 9 + payloadLen;
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

    private static final class Collected {
        final Set<Integer> closedStreams = new HashSet<>();
        final Map<Integer, List<ReceivedFrame>> streamFrames = new HashMap<>();

        void add(int streamId, ReceivedFrame f) {
            streamFrames.computeIfAbsent(streamId, k -> new ArrayList<>()).add(f);
        }
    }

    private static final class FlightDataParts {
        byte[] body;
        byte[] header;
    }

    private static final class FrameCollector implements Http2FrameListener {
        private final Collected collected;

        FrameCollector(Collected collected) {
            this.collected = collected;
        }

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId,
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
        public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding, boolean endOfStream) {
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
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers,
                                  int streamDependency, short weight, boolean exclusive, int padding, boolean endOfStream) {
            onHeadersRead(ctx, streamId, headers, padding, endOfStream);
        }

        @Override
        public void onPingAckRead(ChannelHandlerContext ctx, long data) {
        }

        @Override
        public void onPingRead(ChannelHandlerContext ctx, long data) {
        }

        @Override
        public void onPriorityRead(ChannelHandlerContext ctx, int streamId, int streamDependency, short weight, boolean exclusive) {
        }

        @Override
        public void onPushPromiseRead(ChannelHandlerContext ctx, int streamId, int promisedStreamId, Http2Headers headers, int padding) {
        }

        @Override
        public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
            collected.closedStreams.add(streamId);
        }

        @Override
        public void onSettingsAckRead(ChannelHandlerContext ctx) {
        }

        @Override
        public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
        }

        @Override
        public void onUnknownFrame(ChannelHandlerContext ctx, byte frameType, int streamId,
                                   io.netty.handler.codec.http2.Http2Flags flags, ByteBuf payload) {
        }

        @Override
        public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
        }
    }

    private static final class ReceivedFrame {
        boolean endStream;
        Map<String, String> headers;
        String kind;
        byte[] payload;
    }
}
