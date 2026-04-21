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
import io.questdb.cutlass.flightsql.proto.CommandStatementQueryCodec;
import io.questdb.cutlass.flightsql.proto.FlightDataCodec;
import io.questdb.cutlass.flightsql.proto.FlightDescriptorCodec;
import io.questdb.cutlass.flightsql.proto.FlightEndpointCodec;
import io.questdb.cutlass.flightsql.proto.FlightInfoCodec;
import io.questdb.cutlass.flightsql.proto.TicketCodec;
import io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.protobuf.AnyCodec;
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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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

/**
 * Wave 6b end-to-end tests: drive the Flight SQL server through real
 * {@code SELECT} statements over a single H2 connection and validate the
 * round-tripped {@code FlightData} via Arrow Java.
 */
public class FlightSqlQueryEndToEndTest extends AbstractBootstrapTest {

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
    public void testMalformedSqlReturnsInvalidArgument() throws Exception {
        try (final TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.start();
            try (Socket socket = openH2(serverMain)) {
                QueryResult r = runGetFlightInfo(socket, "SELECT FROM nowhere");
                Assert.assertFalse("expected trailers-only error response", r.ok);
                Assert.assertEquals("3", r.grpcStatus); // INVALID_ARGUMENT
            }
        }
    }

    @Test
    public void testSelectDoubleColumn() throws Exception {
        try (final TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.start();
            try (Socket socket = openH2(serverMain)) {
                QueryResult r = runGetFlightInfo(socket, "SELECT cast(x AS double) / 2 AS v FROM long_sequence(4)");
                Assert.assertTrue("GetFlightInfo failed: " + r.grpcStatus, r.ok);
                BatchResult batch = runDoGet(socket, 3, r);
                Assert.assertEquals(1, batch.columnCount);
                Float8Vector v = (Float8Vector) batch.root.getVector("v");
                Assert.assertEquals(4, v.getValueCount());
                Assert.assertEquals(0.5, v.get(0), 0.0);
                Assert.assertEquals(1.0, v.get(1), 0.0);
                Assert.assertEquals(1.5, v.get(2), 0.0);
                Assert.assertEquals(2.0, v.get(3), 0.0);
                batch.close();
            }
        }
    }

    @Test
    public void testSelectLongSequenceThree() throws Exception {
        try (final TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.start();
            try (Socket socket = openH2(serverMain)) {
                QueryResult r = runGetFlightInfo(socket, "SELECT x FROM long_sequence(3)");
                Assert.assertTrue("GetFlightInfo failed: " + r.grpcStatus, r.ok);
                BatchResult batch = runDoGet(socket, 3, r);
                Assert.assertEquals(1, batch.columnCount);
                BigIntVector col = (BigIntVector) batch.root.getVector("x");
                Assert.assertEquals(3, col.getValueCount());
                Assert.assertEquals(1L, col.get(0));
                Assert.assertEquals(2L, col.get(1));
                Assert.assertEquals(3L, col.get(2));
                batch.close();
            }
        }
    }

    @Test
    public void testSelectMultiColumn() throws Exception {
        try (final TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.start();
            try (Socket socket = openH2(serverMain)) {
                QueryResult r = runGetFlightInfo(socket, "SELECT x, x * 2 AS y, cast(x AS int) AS z FROM long_sequence(5)");
                Assert.assertTrue("GetFlightInfo failed: " + r.grpcStatus, r.ok);
                BatchResult batch = runDoGet(socket, 3, r);
                Assert.assertEquals(3, batch.columnCount);
                BigIntVector x = (BigIntVector) batch.root.getVector("x");
                BigIntVector y = (BigIntVector) batch.root.getVector("y");
                IntVector z = (IntVector) batch.root.getVector("z");
                Assert.assertEquals(5, x.getValueCount());
                for (int i = 0; i < 5; i++) {
                    Assert.assertEquals(i + 1L, x.get(i));
                    Assert.assertEquals(2L * (i + 1), y.get(i));
                    Assert.assertEquals(i + 1, z.get(i));
                }
                batch.close();
            }
        }
    }

    @Test
    public void testStringColumnReturnsUnimplemented() throws Exception {
        try (final TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.start();
            try (Socket socket = openH2(serverMain)) {
                QueryResult r = runGetFlightInfo(socket, "SELECT 'hello' AS greeting");
                Assert.assertFalse("expected trailers-only unimplemented error", r.ok);
                Assert.assertEquals("12", r.grpcStatus); // UNIMPLEMENTED
            }
        }
    }

    @Test
    public void testUnknownTicketReturnsInvalidArgument() throws Exception {
        try (final TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.start();
            try (Socket socket = openH2(serverMain)) {
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
                out.write(buildGrpcDataFrame(1, ticketProto, true));
                out.flush();
                Collected collected = readUntilStreamClosed(socket.getInputStream(), 1);
                List<ReceivedFrame> stream1 = collected.streamFrames.get(1);
                Assert.assertNotNull(stream1);
                ReceivedFrame only = stream1.get(0);
                Assert.assertEquals("HEADERS", only.kind);
                Assert.assertTrue("trailers-only must set END_STREAM", only.endStream);
                Assert.assertEquals("3", only.headers.get("grpc-status"));
            }
        }
    }

    /**
     * Large scan that must spill across multiple RecordBatches. Uses a
     * BATCH_SIZE of 4096 on the server, so a SELECT of 10_000 rows gives
     * three batches: 4096, 4096, 1808.
     */
    @Test
    public void testSelect10kRowsAcrossMultipleBatches() throws Exception {
        try (final TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.start();
            try (Socket socket = openH2(serverMain)) {
                QueryResult r = runGetFlightInfo(socket, "SELECT x FROM long_sequence(10000)");
                Assert.assertTrue("GetFlightInfo failed: " + r.grpcStatus, r.ok);
                BatchResult batch = runDoGet(socket, 3, r);
                BigIntVector col = (BigIntVector) batch.root.getVector("x");
                Assert.assertEquals(10_000, col.getValueCount());
                Assert.assertEquals(1L, col.get(0));
                Assert.assertEquals(10_000L, col.get(9_999));
                Assert.assertTrue("expected more than one RecordBatch frame, got " + batch.batchCount,
                        batch.batchCount >= 2);
                batch.close();
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

    /**
     * Connection-level {@code WINDOW_UPDATE(streamId=0)} with a huge
     * increment so the server does not park on the connection window
     * while streaming multi-batch responses. Paired with
     * {@link #buildEmptySettings}'s per-stream window bump.
     */
    private static byte[] buildConnectionWindowUpdate() {
        long frame = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeWindowUpdate(frame, frame + BUF_SIZE, 0, 0x7FFF_0000);
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

    private static byte[] buildEmptySettings() {
        // Advertise a large initial-window so the server can stream many
        // RecordBatches without waiting on WINDOW_UPDATEs the netty frame
        // reader never sends. MAX_FRAME_SIZE stays at the default 16KiB
        // so the scheduler chunks > 16 KiB payloads into frames the netty
        // reader accepts.
        short[] ids = new short[]{
                io.questdb.cutlass.http2.Http2Settings.INITIAL_WINDOW_SIZE,
        };
        int[] vals = new int[]{
                io.questdb.cutlass.http2.Http2Settings.INITIAL_WINDOW_SIZE_MAX,
        };
        long frame = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeSettings(frame, frame + BUF_SIZE, ids, vals, ids.length);
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

    private static byte[] encodeAnyCommandStatementQuery(String sql) throws IOException {
        // Inner: CommandStatementQuery { query = sql }
        byte[] queryBytes = sql.getBytes(StandardCharsets.UTF_8);
        byte[] innerBuf = new byte[64 + queryBytes.length];
        CodedOutputStream inner = CodedOutputStream.newInstance(innerBuf);
        inner.writeByteArray(CommandStatementQueryCodec.FIELD_QUERY, queryBytes);
        int innerLen = innerBuf.length - inner.spaceLeft();
        byte[] innerMsg = new byte[innerLen];
        System.arraycopy(innerBuf, 0, innerMsg, 0, innerLen);

        // Outer: Any { type_url, value = innerMsg }
        byte[] typeUrl = CommandStatementQueryCodec.TYPE_URL.getBytes(StandardCharsets.US_ASCII);
        byte[] outerBuf = new byte[128 + typeUrl.length + innerLen];
        CodedOutputStream outer = CodedOutputStream.newInstance(outerBuf);
        outer.writeByteArray(AnyCodec.FIELD_TYPE_URL, typeUrl);
        outer.writeByteArray(AnyCodec.FIELD_VALUE, innerMsg);
        int outerLen = outerBuf.length - outer.spaceLeft();
        byte[] outerMsg = new byte[outerLen];
        System.arraycopy(outerBuf, 0, outerMsg, 0, outerLen);
        return outerMsg;
    }

    private static byte[] encodeFlightDescriptorCmd(byte[] cmd) throws IOException {
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

    /**
     * Concatenates all DATA frame payloads on {@code streamFrames} and
     * splits the resulting byte stream into one or more gRPC messages.
     * Each message has the shape {@code [compression flag 1B][big-endian len 4B][body]};
     * H2 is free to chunk a single gRPC message across multiple DATA
     * frames and to pack multiple gRPC messages into one frame, so the
     * naive per-frame split the WAVE 6a tests used does not survive
     * long Flight SQL responses.
     */
    private static List<byte[]> extractGrpcMessages(List<ReceivedFrame> streamFrames) {
        int totalLen = 0;
        for (ReceivedFrame f : streamFrames) {
            if ("DATA".equals(f.kind)) {
                totalLen += f.payload.length;
            }
        }
        byte[] combined = new byte[totalLen];
        int cursor = 0;
        for (ReceivedFrame f : streamFrames) {
            if ("DATA".equals(f.kind)) {
                System.arraycopy(f.payload, 0, combined, cursor, f.payload.length);
                cursor += f.payload.length;
            }
        }
        List<byte[]> out = new ArrayList<>();
        int pos = 0;
        while (pos < combined.length) {
            Assert.assertTrue("not enough bytes for gRPC prefix", combined.length - pos >= 5);
            Assert.assertEquals("unexpected compression flag", 0, combined[pos]);
            int bodyLen = ((combined[pos + 1] & 0xFF) << 24)
                    | ((combined[pos + 2] & 0xFF) << 16)
                    | ((combined[pos + 3] & 0xFF) << 8)
                    | (combined[pos + 4] & 0xFF);
            Assert.assertTrue("truncated gRPC body", combined.length - pos - 5 >= bodyLen);
            byte[] msg = new byte[bodyLen];
            System.arraycopy(combined, pos + 5, msg, 0, bodyLen);
            out.add(msg);
            pos += 5 + bodyLen;
        }
        return out;
    }

    private static Map<String, String> flatten(Http2Headers headers) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<CharSequence, CharSequence> e : headers) {
            out.put(e.getKey().toString(), e.getValue().toString());
        }
        return out;
    }

    private static Socket openH2(TestServerMain server) throws IOException {
        int port = server.getConfiguration().getHttpServerConfiguration().getBindPort();
        Socket socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(30_000);
        return socket;
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

    private TestServerMain startFlightSqlServer() throws Exception {
        return startWithEnvVariables(
                PropertyKey.HTTP_H2_ENABLED.getEnvVarName(), "true",
                PropertyKey.FLIGHT_SQL_ENABLED.getEnvVarName(), "true"
        );
    }

    /**
     * Sends a GetFlightInfo request for {@code sql} over stream 1 and
     * returns the parsed ticket + schema + a status code. On error the
     * returned {@code ok} is {@code false} and {@code grpcStatus} is the
     * observed trailers-only value.
     */
    private QueryResult runGetFlightInfo(Socket socket, String sql) throws Exception {
        OutputStream out = socket.getOutputStream();
        out.write(PREFACE);
        out.write(buildEmptySettings());
        out.write(buildConnectionWindowUpdate());
        out.write(buildHeadersFrame(1,
                FlightSqlDispatchListener.GET_FLIGHT_INFO_PATH,
                "POST",
                "application/grpc+proto",
                /*endStream*/ false));
        byte[] anyCmd = encodeAnyCommandStatementQuery(sql);
        byte[] descriptorBytes = encodeFlightDescriptorCmd(anyCmd);
        out.write(buildGrpcDataFrame(1, descriptorBytes, true));
        out.flush();
        Collected c = readUntilStreamClosed(socket.getInputStream(), 1);
        List<ReceivedFrame> s1 = c.streamFrames.get(1);
        Assert.assertNotNull(s1);
        ReceivedFrame first = s1.get(0);
        QueryResult r = new QueryResult();
        if (first.endStream && s1.size() == 1) {
            r.ok = false;
            r.grpcStatus = first.headers.get("grpc-status");
            return r;
        }
        // HEADERS (status 200) + DATA + HEADERS (trailers)
        Assert.assertEquals("HEADERS", first.kind);
        Assert.assertEquals("200", first.headers.get(":status"));
        Assert.assertEquals("DATA", s1.get(1).kind);
        byte[] flightInfoProto = extractGrpcBody(s1.get(1).payload);
        Assert.assertEquals("HEADERS", s1.get(2).kind);
        r.ok = "0".equals(s1.get(2).headers.get("grpc-status"));
        r.grpcStatus = s1.get(2).headers.get("grpc-status");

        CodedInputStream in = CodedInputStream.newInstance(flightInfoProto);
        while (!in.isAtEnd()) {
            int tag = in.readTag();
            int fn = ProtobufWireFormat.fieldNumberOf(tag);
            switch (fn) {
                case FlightInfoCodec.FIELD_SCHEMA:
                    r.schemaBytes = in.readByteArray();
                    break;
                case FlightInfoCodec.FIELD_ENDPOINT:
                    byte[] endpointBytes = in.readByteArray();
                    CodedInputStream ein = CodedInputStream.newInstance(endpointBytes);
                    while (!ein.isAtEnd()) {
                        int etag = ein.readTag();
                        if (ProtobufWireFormat.fieldNumberOf(etag) == FlightEndpointCodec.FIELD_TICKET) {
                            byte[] inner = ein.readByteArray();
                            CodedInputStream tin = CodedInputStream.newInstance(inner);
                            while (!tin.isAtEnd()) {
                                int t = tin.readTag();
                                if (ProtobufWireFormat.fieldNumberOf(t) == TicketCodec.FIELD_TICKET) {
                                    r.ticketBytes = tin.readByteArray();
                                } else {
                                    tin.skipField(t);
                                }
                            }
                        } else {
                            ein.skipField(etag);
                        }
                    }
                    break;
                default:
                    in.skipField(tag);
            }
        }
        return r;
    }

    /**
     * Sends a DoGet on {@code streamId} and returns decoded Arrow data.
     */
    private BatchResult runDoGet(Socket socket, int streamId, QueryResult q) throws Exception {
        OutputStream out = socket.getOutputStream();
        out.write(buildHeadersFrame(streamId,
                FlightSqlDispatchListener.DO_GET_PATH,
                "POST",
                "application/grpc+proto",
                /*endStream*/ false));
        byte[] ticketProto = encodeTicketProto(q.ticketBytes);
        out.write(buildGrpcDataFrame(streamId, ticketProto, true));
        out.flush();

        Collected c = readUntilStreamClosed(socket.getInputStream(), streamId);
        List<ReceivedFrame> s = c.streamFrames.get(streamId);
        Assert.assertNotNull("stream frames missing", s);
        Assert.assertEquals("HEADERS", s.get(0).kind);
        Assert.assertEquals("200", s.get(0).headers.get(":status"));
        // Reassemble gRPC messages across potentially many DATA frames.
        // Frame 0 is HEADERS (:status 200); the trailing frame is HEADERS
        // (trailers). Everything in between is DATA.
        ReceivedFrame trailers = s.get(s.size() - 1);
        Assert.assertEquals("HEADERS", trailers.kind);
        Assert.assertEquals("0", trailers.headers.get("grpc-status"));
        List<ReceivedFrame> dataFrames = s.subList(1, s.size() - 1);
        List<byte[]> grpcMessages = extractGrpcMessages(dataFrames);
        Assert.assertFalse("no gRPC messages on DoGet response", grpcMessages.isEmpty());

        byte[] schemaMsg = grpcMessages.get(0);
        FlightDataParts schemaPart = parseFlightData(schemaMsg);
        Assert.assertArrayEquals(q.schemaBytes, schemaPart.header);
        Assert.assertNull(schemaPart.body);

        Schema schemaFb = (Schema) Message.getRootAsMessage(ByteBuffer.wrap(q.schemaBytes)).header(new Schema());
        org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                org.apache.arrow.vector.types.pojo.Schema.convertSchema(schemaFb);
        BatchResult result = new BatchResult();
        result.alloc = new RootAllocator(Long.MAX_VALUE);
        result.root = VectorSchemaRoot.create(pojoSchema, result.alloc);
        result.columnCount = schemaFb.fieldsLength();
        result.batchCount = 0;

        int accumulatedRows = 0;
        for (int i = 1; i < grpcMessages.size(); i++) {
            byte[] bytes = grpcMessages.get(i);
            FlightDataParts parts = parseFlightData(bytes);
            Assert.assertNotNull(parts.header);
            Assert.assertNotNull(parts.body);
            ArrowBuf body = result.alloc.buffer(parts.body.length);
            body.setBytes(0, parts.body);
            Message rbMsg = Message.getRootAsMessage(ByteBuffer.wrap(parts.header));
            ArrowRecordBatch arb = MessageSerializer.deserializeRecordBatch(rbMsg, body);
            try {
                VectorSchemaRoot tmp = VectorSchemaRoot.create(pojoSchema, result.alloc);
                VectorLoader loader = new VectorLoader(tmp);
                loader.load(arb);
                int rows = tmp.getRowCount();
                // Copy rows into result.root.
                if (accumulatedRows == 0) {
                    result.root.setRowCount(rows);
                } else {
                    result.root.setRowCount(accumulatedRows + rows);
                }
                for (int ci = 0; ci < schemaFb.fieldsLength(); ci++) {
                    org.apache.arrow.vector.FieldVector src = tmp.getVector(ci);
                    org.apache.arrow.vector.FieldVector dst = result.root.getVector(ci);
                    for (int r = 0; r < rows; r++) {
                        dst.copyFromSafe(r, accumulatedRows + r, src);
                    }
                }
                accumulatedRows += rows;
                tmp.close();
            } finally {
                arb.close();
            }
            result.batchCount++;
        }
        result.root.setRowCount(accumulatedRows);
        return result;
    }

    private static final class BatchResult implements AutoCloseable {
        RootAllocator alloc;
        int batchCount;
        int columnCount;
        VectorSchemaRoot root;

        @Override
        public void close() {
            if (root != null) root.close();
            if (alloc != null) alloc.close();
        }
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

    private static final class QueryResult {
        String grpcStatus;
        boolean ok;
        byte[] schemaBytes;
        byte[] ticketBytes;
    }

    private static final class ReceivedFrame {
        boolean endStream;
        Map<String, String> headers;
        String kind;
        byte[] payload;
    }
}
