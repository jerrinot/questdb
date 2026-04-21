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
import io.questdb.cutlass.protobuf.ProtobufWireFormat;
import io.questdb.cutlass.protobuf.ProtobufWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ProtobufWriterTest {

    private static final int BUF_SIZE = 4096;

    @Test
    public void testLengthDelimitedFieldOverflowAtomic() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long payload = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 8; i++) {
                    Unsafe.getUnsafe().putByte(payload + i, (byte) (0x41 + i));
                }
                ProtobufWriter w = new ProtobufWriter();
                // Budget: enough for tag + length but not the 8 payload bytes.
                long narrowLimit = buf + 2;
                w.of(buf, narrowLimit);
                long pre = w.cursor();
                long c = w.writeLengthDelimitedField(2, payload, 8);
                Assert.assertEquals(-1L, c);
                Assert.assertEquals(pre, w.cursor());
            } finally {
                Unsafe.free(payload, 8, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testLengthDelimitedFieldRoundTripViaGoogle() throws IOException {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] payload = "hello, flight sql".getBytes();
            long payloadAddr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(payloadAddr + i, payload[i]);
                }
                ProtobufWriter w = new ProtobufWriter();
                w.of(buf, buf + BUF_SIZE);
                long c = w.writeLengthDelimitedField(2, payloadAddr, payload.length);
                Assert.assertTrue(c > 0);

                int encodedLen = (int) (c - buf);
                byte[] bytes = new byte[encodedLen];
                for (int i = 0; i < encodedLen; i++) {
                    bytes[i] = Unsafe.getUnsafe().getByte(buf + i);
                }
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                int tag = in.readTag();
                Assert.assertEquals(2, ProtobufWireFormat.fieldNumberOf(tag));
                Assert.assertEquals(
                        ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED,
                        ProtobufWireFormat.wireTypeOf(tag));
                byte[] decoded = in.readByteArray();
                Assert.assertArrayEquals(payload, decoded);
                Assert.assertTrue(in.isAtEnd());
            } finally {
                Unsafe.free(payloadAddr, payload.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTagOverflowAtomic() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            ProtobufWriter w = new ProtobufWriter();
            w.of(buf, buf);
            long c = w.writeTag(1, ProtobufWireFormat.WIRE_TYPE_VARINT);
            Assert.assertEquals(-1L, c);
            Assert.assertEquals(buf, w.cursor());
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTagViaGoogle() throws IOException {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            ProtobufWriter w = new ProtobufWriter();
            w.of(buf, buf + BUF_SIZE);
            long c = w.writeTag(15, ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED);
            int encodedLen = (int) (c - buf);
            byte[] bytes = new byte[encodedLen];
            for (int i = 0; i < encodedLen; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(buf + i);
            }
            CodedInputStream in = CodedInputStream.newInstance(bytes);
            int tag = in.readTag();
            Assert.assertEquals(15, ProtobufWireFormat.fieldNumberOf(tag));
            Assert.assertEquals(ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED, ProtobufWireFormat.wireTypeOf(tag));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testVarint64BoundaryValues() throws IOException {
        long[] values = {
                0L, 1L, 127L, 128L, 16_383L, 16_384L,
                (1L << 28) - 1, 1L << 28,
                Integer.MAX_VALUE, 1L << 32,
                Long.MAX_VALUE, -1L, Long.MIN_VALUE
        };
        int[] expectedSizes = {
                1, 1, 1, 2, 2, 3,
                4, 5,
                5, 5,
                9, 10, 10
        };
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            ProtobufWriter w = new ProtobufWriter();
            for (int i = 0; i < values.length; i++) {
                long v = values[i];
                w.of(buf, buf + BUF_SIZE);
                long c = w.writeVarint64Raw(v);
                int encodedLen = (int) (c - buf);
                Assert.assertEquals("size for " + v, expectedSizes[i], encodedLen);

                byte[] bytes = new byte[encodedLen];
                for (int j = 0; j < encodedLen; j++) {
                    bytes[j] = Unsafe.getUnsafe().getByte(buf + j);
                }
                CodedInputStream in = CodedInputStream.newInstance(bytes);
                long decoded = in.readRawVarint64();
                Assert.assertEquals("round-trip " + v, v, decoded);
                Assert.assertTrue(in.isAtEnd());
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testVarint64FieldOverflowAtomic() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Poison the first 5 bytes so we can detect a partial write.
            for (int i = 0; i < 5; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0xAB);
            }
            ProtobufWriter w = new ProtobufWriter();
            // Tag for field 1 (1 byte) fits; value -1L (10 bytes) does not.
            w.of(buf, buf + 5);
            long pre = w.cursor();
            long c = w.writeVarint64Field(1, -1L);
            Assert.assertEquals(-1L, c);
            Assert.assertEquals(pre, w.cursor());
            for (int i = 0; i < 5; i++) {
                Assert.assertEquals((byte) 0xAB, Unsafe.getUnsafe().getByte(buf + i));
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testVarint64RawOverflowAtomic() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            ProtobufWriter w = new ProtobufWriter();
            w.of(buf, buf + 1);
            long pre = w.cursor();
            long c = w.writeVarint64Raw(128L);
            Assert.assertEquals(-1L, c);
            Assert.assertEquals(pre, w.cursor());
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testVarintSize() {
        Assert.assertEquals(1, ProtobufWireFormat.varintSize(0));
        Assert.assertEquals(1, ProtobufWireFormat.varintSize(127));
        Assert.assertEquals(2, ProtobufWireFormat.varintSize(128));
        Assert.assertEquals(5, ProtobufWireFormat.varintSize(1L << 32));
        Assert.assertEquals(10, ProtobufWireFormat.varintSize(-1L));
        Assert.assertEquals(10, ProtobufWireFormat.varintSize(Long.MIN_VALUE));
    }
}
