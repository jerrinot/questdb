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

package io.questdb.test.cutlass.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.questdb.cutlass.protobuf.HandshakeCodec;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class HandshakeCodecTest {

    private static final int BUF_SIZE = 4096;

    @Test
    public void testDecodeEmptyRequest() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            HandshakeCodec.Fields f = new HandshakeCodec.Fields();
            HandshakeCodec.decodeHandshakeRequest(buf, buf, f);
            Assert.assertEquals(0L, f.protocolVersion);
            Assert.assertEquals(0, f.payloadLen);
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRequestWrittenByGoogle() throws IOException {
        byte[] payload = "ticket-blob".getBytes();
        byte[] wire = new byte[payload.length + 32];
        CodedOutputStream out = CodedOutputStream.newInstance(wire);
        out.writeUInt64(HandshakeCodec.FIELD_PROTOCOL_VERSION, 17);
        out.writeByteArray(HandshakeCodec.FIELD_PAYLOAD, payload);
        int len = wire.length - out.spaceLeft();

        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            HandshakeCodec.Fields f = new HandshakeCodec.Fields();
            HandshakeCodec.decodeHandshakeRequest(buf, buf + len, f);
            Assert.assertEquals(17L, f.protocolVersion);
            Assert.assertEquals(payload.length, f.payloadLen);
            for (int i = 0; i < payload.length; i++) {
                Assert.assertEquals(payload[i], Unsafe.getUnsafe().getByte(f.payloadAddr + i));
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeSkipsUnknownField() throws IOException {
        byte[] wire = new byte[64];
        CodedOutputStream out = CodedOutputStream.newInstance(wire);
        out.writeUInt64(HandshakeCodec.FIELD_PROTOCOL_VERSION, 3);
        out.writeByteArray(42, new byte[]{'x'});
        out.writeByteArray(HandshakeCodec.FIELD_PAYLOAD, new byte[]{'P'});
        int len = wire.length - out.spaceLeft();

        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            HandshakeCodec.Fields f = new HandshakeCodec.Fields();
            HandshakeCodec.decodeHandshakeRequest(buf, buf + len, f);
            Assert.assertEquals(3L, f.protocolVersion);
            Assert.assertEquals(1, f.payloadLen);
            Assert.assertEquals((byte) 'P', Unsafe.getUnsafe().getByte(f.payloadAddr));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeResponseEmptyPayload() throws IOException {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long c = HandshakeCodec.encodeHandshakeResponse(buf, buf + BUF_SIZE, 0, 0, 0);
            Assert.assertTrue(c > buf);
            int len = (int) (c - buf);
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(buf + i);
            }
            // Decode via Google.
            CodedInputStream in = CodedInputStream.newInstance(bytes);
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
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeResponseOverflowAtomic() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Preload poison bytes so we can detect partial writes.
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0xAB);
            }
            long c = HandshakeCodec.encodeHandshakeResponse(buf, buf + 2, 0, 0, 0);
            Assert.assertEquals(-1L, c);
            // No bytes should have been written; poison is intact.
            for (int i = 0; i < 2; i++) {
                Assert.assertEquals((byte) 0xAB, Unsafe.getUnsafe().getByte(buf + i));
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeResponseWithPayload() throws IOException {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] payload = "token".getBytes();
            long payloadAddr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(payloadAddr + i, payload[i]);
                }
                long c = HandshakeCodec.encodeHandshakeResponse(
                        buf, buf + BUF_SIZE, 1, payloadAddr, payload.length);
                Assert.assertTrue(c > buf);
                int len = (int) (c - buf);
                byte[] bytes = new byte[len];
                for (int i = 0; i < len; i++) {
                    bytes[i] = Unsafe.getUnsafe().getByte(buf + i);
                }
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                long protocolVersion = -1;
                byte[] decoded = null;
                while (!in.isAtEnd()) {
                    int tag = in.readTag();
                    int fn = tag >>> 3;
                    switch (fn) {
                        case HandshakeCodec.FIELD_PROTOCOL_VERSION:
                            protocolVersion = in.readUInt64();
                            break;
                        case HandshakeCodec.FIELD_PAYLOAD:
                            decoded = in.readByteArray();
                            break;
                        default:
                            in.skipField(tag);
                    }
                }
                Assert.assertEquals(1L, protocolVersion);
                Assert.assertArrayEquals(payload, decoded);
            } finally {
                Unsafe.free(payloadAddr, payload.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRoundTripOurEncodeOurDecode() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] payload = "hello handshake".getBytes();
            long payloadAddr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(payloadAddr + i, payload[i]);
                }
                long c = HandshakeCodec.encodeHandshakeResponse(
                        buf, buf + BUF_SIZE, 5, payloadAddr, payload.length);
                Assert.assertTrue(c > buf);
                HandshakeCodec.Fields f = new HandshakeCodec.Fields();
                HandshakeCodec.decodeHandshakeRequest(buf, c, f);
                Assert.assertEquals(5L, f.protocolVersion);
                Assert.assertEquals(payload.length, f.payloadLen);
                for (int i = 0; i < payload.length; i++) {
                    Assert.assertEquals(payload[i], Unsafe.getUnsafe().getByte(f.payloadAddr + i));
                }
            } finally {
                Unsafe.free(payloadAddr, payload.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
