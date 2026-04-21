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

package io.questdb.test.cutlass.flightsql.proto;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.questdb.cutlass.flightsql.proto.CommandStatementQueryCodec;
import io.questdb.cutlass.flightsql.proto.FlightDataCodec;
import io.questdb.cutlass.flightsql.proto.FlightDescriptorCodec;
import io.questdb.cutlass.flightsql.proto.FlightEndpointCodec;
import io.questdb.cutlass.flightsql.proto.FlightInfoCodec;
import io.questdb.cutlass.flightsql.proto.LocationCodec;
import io.questdb.cutlass.flightsql.proto.TicketCodec;
import io.questdb.cutlass.protobuf.AnyCodec;
import io.questdb.cutlass.protobuf.ProtobufWireFormat;
import io.questdb.cutlass.protobuf.ProtobufWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FlightMessageCodecTest {

    private static final int BUF = 4096;

    @Test
    public void testAnyDecodeCommandStatementQuery() throws IOException {
        String typeUrl = CommandStatementQueryCodec.TYPE_URL;
        byte[] innerValue = "inner-bytes".getBytes(StandardCharsets.UTF_8);
        byte[] wire = new byte[256];
        CodedOutputStream cos = CodedOutputStream.newInstance(wire);
        cos.writeString(AnyCodec.FIELD_TYPE_URL, typeUrl);
        cos.writeByteArray(AnyCodec.FIELD_VALUE, innerValue);
        int len = wire.length - cos.spaceLeft();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            AnyCodec.Fields f = new AnyCodec.Fields();
            AnyCodec.decode(buf, buf + len, f);
            byte[] typeUrlBytes = typeUrl.getBytes(StandardCharsets.UTF_8);
            Assert.assertEquals(typeUrlBytes.length, f.typeUrlLen);
            for (int i = 0; i < typeUrlBytes.length; i++) {
                Assert.assertEquals(typeUrlBytes[i], Unsafe.getUnsafe().getByte(f.typeUrlAddr + i));
            }
            Assert.assertEquals(innerValue.length, f.valueLen);
            for (int i = 0; i < innerValue.length; i++) {
                Assert.assertEquals(innerValue[i], Unsafe.getUnsafe().getByte(f.valueAddr + i));
            }
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAnyDecodeEmptyFields() throws IOException {
        byte[] wire = new byte[16];
        CodedOutputStream cos = CodedOutputStream.newInstance(wire);
        cos.writeString(AnyCodec.FIELD_TYPE_URL, "");
        cos.writeByteArray(AnyCodec.FIELD_VALUE, new byte[0]);
        int len = wire.length - cos.spaceLeft();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            AnyCodec.Fields f = new AnyCodec.Fields();
            AnyCodec.decode(buf, buf + len, f);
            Assert.assertEquals(0, f.typeUrlLen);
            Assert.assertEquals(0, f.valueLen);
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAnyDecodeSkipsUnknownField() throws IOException {
        byte[] wire = new byte[64];
        CodedOutputStream cos = CodedOutputStream.newInstance(wire);
        cos.writeString(AnyCodec.FIELD_TYPE_URL, "type.googleapis.com/x.Y");
        cos.writeInt32(42, 9999);
        cos.writeByteArray(AnyCodec.FIELD_VALUE, "v".getBytes());
        int len = wire.length - cos.spaceLeft();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            AnyCodec.Fields f = new AnyCodec.Fields();
            AnyCodec.decode(buf, buf + len, f);
            Assert.assertTrue(f.typeUrlLen > 0);
            Assert.assertEquals(1, f.valueLen);
            Assert.assertEquals((byte) 'v', Unsafe.getUnsafe().getByte(f.valueAddr));
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCommandStatementQueryDecode() throws IOException {
        String sql = "SELECT x FROM long_sequence(3)";
        byte[] wire = new byte[128];
        CodedOutputStream cos = CodedOutputStream.newInstance(wire);
        cos.writeString(CommandStatementQueryCodec.FIELD_QUERY, sql);
        int len = wire.length - cos.spaceLeft();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            CommandStatementQueryCodec.Fields f = new CommandStatementQueryCodec.Fields();
            CommandStatementQueryCodec.decode(buf, buf + len, f);
            byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
            Assert.assertEquals(sqlBytes.length, f.queryLen);
            for (int i = 0; i < sqlBytes.length; i++) {
                Assert.assertEquals(sqlBytes[i], Unsafe.getUnsafe().getByte(f.queryAddr + i));
            }
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCommandStatementQueryDecodeSkipsTransactionId() throws IOException {
        String sql = "SELECT 1";
        byte[] txn = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        byte[] wire = new byte[64];
        CodedOutputStream cos = CodedOutputStream.newInstance(wire);
        cos.writeString(CommandStatementQueryCodec.FIELD_QUERY, sql);
        cos.writeByteArray(CommandStatementQueryCodec.FIELD_TRANSACTION_ID, txn);
        int len = wire.length - cos.spaceLeft();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            CommandStatementQueryCodec.Fields f = new CommandStatementQueryCodec.Fields();
            CommandStatementQueryCodec.decode(buf, buf + len, f);
            Assert.assertEquals(sql.length(), f.queryLen);
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFlightDataEncodeRoundTripViaGoogle() throws IOException {
        byte[] header = "schema-header-bytes".getBytes();
        byte[] body = new byte[32];
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte) i;
        }
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long headerAddr = Unsafe.malloc(header.length, MemoryTag.NATIVE_DEFAULT);
        long bodyAddr = Unsafe.malloc(body.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < header.length; i++) {
                Unsafe.getUnsafe().putByte(headerAddr + i, header[i]);
            }
            for (int i = 0; i < body.length; i++) {
                Unsafe.getUnsafe().putByte(bodyAddr + i, body[i]);
            }
            ProtobufWriter w = new ProtobufWriter();
            w.of(buf, buf + BUF);
            long c = FlightDataCodec.encode(w, headerAddr, header.length, bodyAddr, body.length);
            Assert.assertTrue(c > 0);
            int len = (int) (c - buf);
            byte[] wire = new byte[len];
            for (int i = 0; i < len; i++) {
                wire[i] = Unsafe.getUnsafe().getByte(buf + i);
            }
            CodedInputStream in = CodedInputStream.newInstance(wire);
            byte[] gotHeader = null;
            byte[] gotBody = null;
            while (!in.isAtEnd()) {
                int tag = in.readTag();
                int fn = ProtobufWireFormat.fieldNumberOf(tag);
                if (fn == FlightDataCodec.FIELD_DATA_HEADER) {
                    gotHeader = in.readByteArray();
                } else if (fn == FlightDataCodec.FIELD_DATA_BODY) {
                    gotBody = in.readByteArray();
                } else {
                    in.skipField(tag);
                }
            }
            Assert.assertArrayEquals(header, gotHeader);
            Assert.assertArrayEquals(body, gotBody);
        } finally {
            Unsafe.free(bodyAddr, body.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(headerAddr, header.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFlightDataHeaderOnly() throws IOException {
        byte[] header = "header-only".getBytes();
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long headerAddr = Unsafe.malloc(header.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < header.length; i++) {
                Unsafe.getUnsafe().putByte(headerAddr + i, header[i]);
            }
            ProtobufWriter w = new ProtobufWriter();
            w.of(buf, buf + BUF);
            long c = FlightDataCodec.encode(w, headerAddr, header.length, 0, 0);
            Assert.assertTrue(c > 0);
            int len = (int) (c - buf);
            byte[] wire = new byte[len];
            for (int i = 0; i < len; i++) {
                wire[i] = Unsafe.getUnsafe().getByte(buf + i);
            }
            CodedInputStream in = CodedInputStream.newInstance(wire);
            byte[] gotHeader = null;
            byte[] gotBody = null;
            while (!in.isAtEnd()) {
                int tag = in.readTag();
                int fn = ProtobufWireFormat.fieldNumberOf(tag);
                if (fn == FlightDataCodec.FIELD_DATA_HEADER) {
                    gotHeader = in.readByteArray();
                } else if (fn == FlightDataCodec.FIELD_DATA_BODY) {
                    gotBody = in.readByteArray();
                } else {
                    in.skipField(tag);
                }
            }
            Assert.assertArrayEquals(header, gotHeader);
            Assert.assertNull(gotBody);
        } finally {
            Unsafe.free(headerAddr, header.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFlightDescriptorDecodeMatchesGoogle() throws IOException {
        byte[] cmd = "SELECT 1".getBytes();
        byte[] pathA = "foo".getBytes();
        byte[] pathB = "bar".getBytes();
        byte[] wire = new byte[128];
        CodedOutputStream out = CodedOutputStream.newInstance(wire);
        out.writeEnum(FlightDescriptorCodec.FIELD_TYPE, FlightDescriptorCodec.TYPE_CMD);
        out.writeByteArray(FlightDescriptorCodec.FIELD_CMD, cmd);
        out.writeByteArray(FlightDescriptorCodec.FIELD_PATH, pathA);
        out.writeByteArray(FlightDescriptorCodec.FIELD_PATH, pathB);
        int len = wire.length - out.spaceLeft();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            FlightDescriptorCodec.Fields f = new FlightDescriptorCodec.Fields();
            FlightDescriptorCodec.decode(buf, buf + len, f);
            Assert.assertEquals(FlightDescriptorCodec.TYPE_CMD, f.type);
            Assert.assertEquals(cmd.length, f.cmdLen);
            for (int i = 0; i < cmd.length; i++) {
                Assert.assertEquals(cmd[i], Unsafe.getUnsafe().getByte(f.cmdAddr + i));
            }
            Assert.assertEquals(2, f.pathCount);
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFlightDescriptorDecodeOnlyCmd() throws IOException {
        byte[] cmd = "pong".getBytes();
        byte[] wire = new byte[64];
        CodedOutputStream out = CodedOutputStream.newInstance(wire);
        out.writeEnum(FlightDescriptorCodec.FIELD_TYPE, FlightDescriptorCodec.TYPE_CMD);
        out.writeByteArray(FlightDescriptorCodec.FIELD_CMD, cmd);
        int len = wire.length - out.spaceLeft();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            FlightDescriptorCodec.Fields f = new FlightDescriptorCodec.Fields();
            FlightDescriptorCodec.decode(buf, buf + len, f);
            Assert.assertEquals(FlightDescriptorCodec.TYPE_CMD, f.type);
            Assert.assertEquals(cmd.length, f.cmdLen);
            Assert.assertEquals(0, f.pathCount);
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFlightInfoEncodeRoundTripViaGoogle() throws IOException {
        byte[] schema = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00};
        byte[] ticket = {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] uri = "arrow-flight-reuse-connection://?".getBytes();

        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long schemaAddr = Unsafe.malloc(schema.length, MemoryTag.NATIVE_DEFAULT);
        long ticketAddr = Unsafe.malloc(ticket.length, MemoryTag.NATIVE_DEFAULT);
        long uriAddr = Unsafe.malloc(uri.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < schema.length; i++) {
                Unsafe.getUnsafe().putByte(schemaAddr + i, schema[i]);
            }
            for (int i = 0; i < ticket.length; i++) {
                Unsafe.getUnsafe().putByte(ticketAddr + i, ticket[i]);
            }
            for (int i = 0; i < uri.length; i++) {
                Unsafe.getUnsafe().putByte(uriAddr + i, uri[i]);
            }
            ProtobufWriter w = new ProtobufWriter();
            w.of(buf, buf + BUF);
            long c = FlightInfoCodec.encodeSingleEndpoint(w,
                    schemaAddr, schema.length,
                    0, 0,
                    ticketAddr, ticket.length,
                    uriAddr, uri.length);
            Assert.assertTrue(c > 0);
            int len = (int) (c - buf);
            byte[] wire = new byte[len];
            for (int i = 0; i < len; i++) {
                wire[i] = Unsafe.getUnsafe().getByte(buf + i);
            }
            CodedInputStream in = CodedInputStream.newInstance(wire);
            byte[] gotSchema = null;
            List<byte[]> endpoints = new ArrayList<>();
            long totalRecords = 0;
            long totalBytes = 0;
            boolean sawRecords = false;
            boolean sawBytes = false;
            while (!in.isAtEnd()) {
                int tag = in.readTag();
                int fn = ProtobufWireFormat.fieldNumberOf(tag);
                switch (fn) {
                    case FlightInfoCodec.FIELD_SCHEMA:
                        gotSchema = in.readByteArray();
                        break;
                    case FlightInfoCodec.FIELD_ENDPOINT:
                        endpoints.add(in.readByteArray());
                        break;
                    case FlightInfoCodec.FIELD_TOTAL_RECORDS:
                        totalRecords = in.readInt64();
                        sawRecords = true;
                        break;
                    case FlightInfoCodec.FIELD_TOTAL_BYTES:
                        totalBytes = in.readInt64();
                        sawBytes = true;
                        break;
                    default:
                        in.skipField(tag);
                }
            }
            Assert.assertArrayEquals(schema, gotSchema);
            Assert.assertEquals(1, endpoints.size());
            Assert.assertTrue(sawRecords);
            Assert.assertTrue(sawBytes);
            Assert.assertEquals(-1L, totalRecords);
            Assert.assertEquals(-1L, totalBytes);
            // decode endpoint: nested ticket + nested location
            CodedInputStream ep = CodedInputStream.newInstance(endpoints.get(0));
            byte[] gotTicket = null;
            List<byte[]> gotUris = new ArrayList<>();
            while (!ep.isAtEnd()) {
                int tag = ep.readTag();
                int fn = ProtobufWireFormat.fieldNumberOf(tag);
                if (fn == FlightEndpointCodec.FIELD_TICKET) {
                    byte[] inner = ep.readByteArray();
                    CodedInputStream tin = CodedInputStream.newInstance(inner);
                    while (!tin.isAtEnd()) {
                        int t = tin.readTag();
                        if (ProtobufWireFormat.fieldNumberOf(t) == TicketCodec.FIELD_TICKET) {
                            gotTicket = tin.readByteArray();
                        } else {
                            tin.skipField(t);
                        }
                    }
                } else if (fn == FlightEndpointCodec.FIELD_LOCATION) {
                    byte[] inner = ep.readByteArray();
                    CodedInputStream lin = CodedInputStream.newInstance(inner);
                    while (!lin.isAtEnd()) {
                        int t = lin.readTag();
                        if (ProtobufWireFormat.fieldNumberOf(t) == LocationCodec.FIELD_URI) {
                            gotUris.add(lin.readByteArray());
                        } else {
                            lin.skipField(t);
                        }
                    }
                } else {
                    ep.skipField(tag);
                }
            }
            Assert.assertArrayEquals(ticket, gotTicket);
            Assert.assertEquals(1, gotUris.size());
            Assert.assertArrayEquals(uri, gotUris.get(0));
        } finally {
            Unsafe.free(uriAddr, uri.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(ticketAddr, ticket.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(schemaAddr, schema.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTicketEncodeDecodeRoundTrip() throws IOException {
        byte[] ticket = {8, 7, 6, 5, 4, 3, 2, 1};
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long ticketAddr = Unsafe.malloc(ticket.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < ticket.length; i++) {
                Unsafe.getUnsafe().putByte(ticketAddr + i, ticket[i]);
            }
            // Encode via our codec.
            ProtobufWriter w = new ProtobufWriter();
            w.of(buf, buf + BUF);
            long c = TicketCodec.encode(w, ticketAddr, ticket.length);
            Assert.assertTrue(c > 0);
            int len = (int) (c - buf);
            byte[] wire = new byte[len];
            for (int i = 0; i < len; i++) {
                wire[i] = Unsafe.getUnsafe().getByte(buf + i);
            }
            // Decode via Google as oracle.
            CodedInputStream in = CodedInputStream.newInstance(wire);
            byte[] gotTicket = null;
            while (!in.isAtEnd()) {
                int tag = in.readTag();
                if (ProtobufWireFormat.fieldNumberOf(tag) == TicketCodec.FIELD_TICKET) {
                    gotTicket = in.readByteArray();
                } else {
                    in.skipField(tag);
                }
            }
            Assert.assertArrayEquals(ticket, gotTicket);
            // Decode via our codec.
            TicketCodec.Fields f = new TicketCodec.Fields();
            TicketCodec.decode(buf, buf + len, f);
            Assert.assertEquals(ticket.length, f.ticketLen);
            for (int i = 0; i < ticket.length; i++) {
                Assert.assertEquals(ticket[i], Unsafe.getUnsafe().getByte(f.ticketAddr + i));
            }
        } finally {
            Unsafe.free(ticketAddr, ticket.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
