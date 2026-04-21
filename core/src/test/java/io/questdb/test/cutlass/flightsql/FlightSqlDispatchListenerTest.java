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

import io.questdb.cutlass.flightsql.server.FlightSqlCallContextPool;
import io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener;
import io.questdb.cutlass.flightsql.server.HandshakeHandler;
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.hpack.HpackDecoder;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2ConnectionConfig;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2Flags;
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameReader;
import io.questdb.cutlass.http2.Http2FrameType;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.http2.Http2Settings;
import io.questdb.cutlass.protobuf.HandshakeCodec;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlightSqlDispatchListenerTest {

    private static final int MAX_MESSAGE_BYTES = 64 * 1024;
    private static final int SCRATCH = 1024;
    private static final int SEND_CAP = 32 * 1024;

    @Test
    public void testHandshakeRoundTrip() {
        Fixture f = new Fixture();
        try {
            byte[] handshakeBody = new byte[0];
            f.sendRequest(
                    FlightSqlDispatchListener.HANDSHAKE_PATH,
                    "POST",
                    "application/grpc+proto",
                    handshakeBody,
                    1);

            Map<String, String> responseHeaders = f.decodeHeadersForStream(1, /*trailers*/ false);
            Assert.assertEquals("200", responseHeaders.get(":status"));
            Assert.assertEquals("application/grpc+proto", responseHeaders.get("content-type"));

            byte[] dataPayload = f.readDataForStream(1);
            // HandshakeResponse with protocol_version=0 + empty payload
            // encodes to 4 bytes; with the 5-byte gRPC prefix the DATA
            // frame carries 9 bytes total.
            Assert.assertTrue("expected non-empty DATA", dataPayload.length >= 9);
            Assert.assertEquals((byte) 0, dataPayload[0]);
            int bodyLen = ((dataPayload[1] & 0xFF) << 24)
                    | ((dataPayload[2] & 0xFF) << 16)
                    | ((dataPayload[3] & 0xFF) << 8)
                    | (dataPayload[4] & 0xFF);
            Assert.assertEquals(dataPayload.length - 5, bodyLen);
            // Decode via our reader.
            long bodyAddr = Unsafe.malloc(bodyLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < bodyLen; i++) {
                    Unsafe.getUnsafe().putByte(bodyAddr + i, dataPayload[5 + i]);
                }
                HandshakeCodec.Fields fields = new HandshakeCodec.Fields();
                HandshakeCodec.decodeHandshakeRequest(bodyAddr, bodyAddr + bodyLen, fields);
                Assert.assertEquals(0L, fields.protocolVersion);
                Assert.assertEquals(0, fields.payloadLen);
            } finally {
                Unsafe.free(bodyAddr, bodyLen, MemoryTag.NATIVE_DEFAULT);
            }

            Map<String, String> trailers = f.decodeHeadersForStream(1, /*trailers*/ true);
            Assert.assertEquals(Integer.toString(GrpcStatus.OK), trailers.get("grpc-status"));
            Assert.assertNull("OK response must not carry grpc-message", trailers.get("grpc-message"));
        } finally {
            f.close();
        }
    }

    @Test
    public void testMalformedGrpcFrameReturnsInternal() {
        Fixture f = new Fixture();
        try {
            // Compressed flag set — reader rejects as UNIMPLEMENTED.
            byte[] payload = new byte[]{
                    1,                                      // compressed = true
                    0, 0, 0, 3,
                    'a', 'b', 'c'
            };
            f.sendRequest(
                    FlightSqlDispatchListener.HANDSHAKE_PATH,
                    "POST",
                    "application/grpc+proto",
                    payload,
                    1);
            Map<String, String> trailers = f.decodeHeadersForStream(1, /*trailers*/ true);
            String status = trailers.get("grpc-status");
            Assert.assertTrue("expected non-OK grpc-status, got " + status,
                    !"0".equals(status));
            int code = Integer.parseInt(status);
            Assert.assertEquals(GrpcStatus.UNIMPLEMENTED, code);
        } finally {
            f.close();
        }
    }

    @Test
    public void testNonPostReturnsInternal() {
        Fixture f = new Fixture();
        try {
            f.sendRequest(
                    FlightSqlDispatchListener.HANDSHAKE_PATH,
                    "GET",
                    "application/grpc+proto",
                    new byte[0],
                    1);
            Map<String, String> resp = f.decodeHeadersForStream(1, /*trailers*/ false);
            Assert.assertEquals("200", resp.get(":status"));
            Assert.assertEquals(Integer.toString(GrpcStatus.INTERNAL), resp.get("grpc-status"));
            Assert.assertNotNull("INTERNAL must carry grpc-message", resp.get("grpc-message"));
        } finally {
            f.close();
        }
    }

    @Test
    public void testUnknownContentTypeReturnsInternal() {
        Fixture f = new Fixture();
        try {
            f.sendRequest(
                    FlightSqlDispatchListener.HANDSHAKE_PATH,
                    "POST",
                    "application/json",
                    new byte[0],
                    1);
            Map<String, String> resp = f.decodeHeadersForStream(1, /*trailers*/ false);
            Assert.assertEquals("200", resp.get(":status"));
            Assert.assertEquals(Integer.toString(GrpcStatus.INTERNAL), resp.get("grpc-status"));
        } finally {
            f.close();
        }
    }

    @Test
    public void testUnknownPathReturnsUnimplemented() {
        Fixture f = new Fixture();
        try {
            f.sendRequest(
                    "/arrow.flight.protocol.FlightService/DoGet",
                    "POST",
                    "application/grpc+proto",
                    new byte[0],
                    1);
            Map<String, String> resp = f.decodeHeadersForStream(1, /*trailers*/ false);
            Assert.assertEquals("200", resp.get(":status"));
            Assert.assertEquals("application/grpc+proto", resp.get("content-type"));
            Assert.assertEquals(Integer.toString(GrpcStatus.UNIMPLEMENTED), resp.get("grpc-status"));
        } finally {
            f.close();
        }
    }

    private static int encodeHeaderBlock(long scratch, long nameBuf, long valueBuf, String[][] headers) {
        HpackEncoder encoder = new HpackEncoder(
                Http2Settings.DEFAULT_HEADER_TABLE_SIZE,
                0,
                Math.max(4096, Http2Settings.DEFAULT_HEADER_TABLE_SIZE),
                16 * 1024,
                64 * 1024);
        try {
            long limit = scratch + SCRATCH * 4;
            long cursor = encoder.beginBlock(scratch, limit);
            Assert.assertTrue("beginBlock must succeed", cursor >= 0);
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
                Assert.assertTrue("encode must fit scratch", cursor >= 0);
            }
            encoder.endBlock();
            return (int) (cursor - scratch);
        } finally {
            encoder.close();
        }
    }

    private static final class Fixture {
        final Http2ConnectionContext ctx;
        final FlightSqlDispatchListener dispatcher;
        final long inputFrame;
        final long nameBuf;
        final FlightSqlCallContextPool pool;
        final long responseBuf;
        final long scratchBuf;
        long responseEnd;
        final long valueBuf;

        Fixture() {
            pool = new FlightSqlCallContextPool(8, MAX_MESSAGE_BYTES, MemoryTag.NATIVE_DEFAULT);
            dispatcher = new FlightSqlDispatchListener(pool, new HandshakeHandler(),
                    null, null, null, null);
            ctx = new Http2ConnectionContext(dispatcher, Http2ConnectionConfig.defaults());
            dispatcher.bind(ctx);
            scratchBuf = Unsafe.malloc(SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
            nameBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
            valueBuf = Unsafe.malloc(SCRATCH, MemoryTag.NATIVE_DEFAULT);
            inputFrame = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            responseBuf = Unsafe.malloc(SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            responseEnd = responseBuf;
        }

        void close() {
            Unsafe.free(responseBuf, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(inputFrame, SEND_CAP, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameBuf, SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(scratchBuf, SCRATCH * 4, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
            dispatcher.close();
        }

        Map<String, String> decodeHeadersForStream(int streamId, boolean trailers) {
            List<byte[]> blocks = collectHeadersForStream(streamId);
            Assert.assertTrue("no HEADERS frames for stream " + streamId, !blocks.isEmpty());
            int index;
            if (trailers && blocks.size() >= 2) {
                index = blocks.size() - 1;
            } else {
                index = 0;
            }
            byte[] block = blocks.get(index);
            long blockAddr = Unsafe.malloc(block.length, MemoryTag.NATIVE_DEFAULT);
            HpackDecoder decoder = new HpackDecoder(4096, 4096);
            try {
                for (int i = 0; i < block.length; i++) {
                    Unsafe.getUnsafe().putByte(blockAddr + i, block[i]);
                }
                Map<String, String> out = new HashMap<>();
                decoder.decodeBlock(blockAddr, blockAddr + block.length,
                        (nameAddr, nameLen, valueAddr, valueLen, neverIndexed) ->
                                out.put(bytesToString(nameAddr, nameLen),
                                        bytesToString(valueAddr, valueLen)));
                return out;
            } finally {
                decoder.close();
                Unsafe.free(blockAddr, block.length, MemoryTag.NATIVE_DEFAULT);
            }
        }

        byte[] readDataForStream(int streamId) {
            Http2FrameReader reader = new Http2FrameReader();
            Http2FrameHeader header = new Http2FrameHeader();
            long cursor = responseBuf;
            while (cursor < responseEnd) {
                int n = reader.tryReadNext(cursor, responseEnd, header,
                        Http2Settings.MAX_FRAME_SIZE_UPPER);
                if (n == 0) {
                    break;
                }
                if (header.getStreamId() == streamId && header.getType() == Http2FrameType.DATA) {
                    int payloadLen = header.getPayloadLength();
                    byte[] out = new byte[payloadLen];
                    long p = header.getPayloadAddr();
                    for (int i = 0; i < payloadLen; i++) {
                        out[i] = Unsafe.getUnsafe().getByte(p + i);
                    }
                    return out;
                }
                cursor += n;
            }
            Assert.fail("no DATA frame for stream " + streamId);
            return null;
        }

        void sendRequest(String path, String method, String contentType, byte[] body, int streamId) {
            String[][] headers = new String[][]{
                    {":method", method},
                    {":scheme", "http"},
                    {":path", path},
                    {":authority", "test"},
                    {"content-type", contentType},
            };
            int blockLen = encodeHeaderBlock(scratchBuf, nameBuf, valueBuf, headers);
            boolean endStream = body == null || body.length == 0;
            long end = Http2FrameWriter.writeHeaders(inputFrame, inputFrame + SEND_CAP,
                    streamId, endStream, true, scratchBuf, blockLen);
            Assert.assertTrue(end > 0);
            if (body != null && body.length > 0) {
                // Write DATA containing 5-byte-prefixed body.
                long dataBuf = Unsafe.malloc(body.length + 5, MemoryTag.NATIVE_DEFAULT);
                try {
                    // Caller-supplied body is expected to already include the
                    // gRPC prefix when we want to test the malformed-flag
                    // path; otherwise synthesize the prefix here.
                    if (body.length >= 5 && body[0] != 0) {
                        // Pass through as-is.
                        for (int i = 0; i < body.length; i++) {
                            Unsafe.getUnsafe().putByte(dataBuf + i, body[i]);
                        }
                        end = Http2FrameWriter.writeData(end, inputFrame + SEND_CAP,
                                streamId, /*endStream*/ true, dataBuf, body.length);
                    } else {
                        // Synthesize gRPC prefix + body.
                        Unsafe.getUnsafe().putByte(dataBuf, (byte) 0);
                        Unsafe.getUnsafe().putByte(dataBuf + 1, (byte) ((body.length >>> 24) & 0xFF));
                        Unsafe.getUnsafe().putByte(dataBuf + 2, (byte) ((body.length >>> 16) & 0xFF));
                        Unsafe.getUnsafe().putByte(dataBuf + 3, (byte) ((body.length >>> 8) & 0xFF));
                        Unsafe.getUnsafe().putByte(dataBuf + 4, (byte) (body.length & 0xFF));
                        for (int i = 0; i < body.length; i++) {
                            Unsafe.getUnsafe().putByte(dataBuf + 5 + i, body[i]);
                        }
                        end = Http2FrameWriter.writeData(end, inputFrame + SEND_CAP,
                                streamId, /*endStream*/ true, dataBuf, body.length + 5);
                    }
                } finally {
                    Unsafe.free(dataBuf, body.length + 5, MemoryTag.NATIVE_DEFAULT);
                }
                Assert.assertTrue(end > 0);
            }
            long consumed = ctx.processReceivedBytes(inputFrame, end);
            Assert.assertEquals("engine must consume all request bytes",
                    end - inputFrame, consumed);
            responseEnd = ctx.writePending(responseBuf, responseBuf + SEND_CAP);
            Assert.assertTrue("expected some response bytes [state=" + ctx.getState()
                            + ", activeStreams=" + ctx.getActiveStreamCount()
                            + ", pending=" + ctx.getPendingCount() + "]",
                    responseEnd > responseBuf);
        }

        private static String bytesToString(long addr, int len) {
            byte[] a = new byte[len];
            for (int i = 0; i < len; i++) {
                a[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            return new String(a);
        }

        private List<byte[]> collectHeadersForStream(int streamId) {
            List<byte[]> blocks = new ArrayList<>();
            Http2FrameReader reader = new Http2FrameReader();
            Http2FrameHeader header = new Http2FrameHeader();
            long cursor = responseBuf;
            byte[] pending = null;
            while (cursor < responseEnd) {
                int n = reader.tryReadNext(cursor, responseEnd, header,
                        Http2Settings.MAX_FRAME_SIZE_UPPER);
                if (n == 0) {
                    break;
                }
                if (header.getStreamId() != streamId) {
                    cursor += n;
                    continue;
                }
                int type = header.getType();
                if (type == Http2FrameType.HEADERS || type == Http2FrameType.CONTINUATION) {
                    int payloadLen = header.getPayloadLength();
                    byte[] chunk = new byte[payloadLen];
                    long p = header.getPayloadAddr();
                    for (int i = 0; i < payloadLen; i++) {
                        chunk[i] = Unsafe.getUnsafe().getByte(p + i);
                    }
                    if (pending == null) {
                        pending = chunk;
                    } else {
                        byte[] merged = new byte[pending.length + chunk.length];
                        System.arraycopy(pending, 0, merged, 0, pending.length);
                        System.arraycopy(chunk, 0, merged, pending.length, chunk.length);
                        pending = merged;
                    }
                    if ((header.getFlags() & Http2Flags.END_HEADERS) != 0) {
                        blocks.add(pending);
                        pending = null;
                    }
                }
                cursor += n;
            }
            return blocks;
        }
    }
}
