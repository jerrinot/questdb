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

import com.google.protobuf.CodedOutputStream;
import io.questdb.cutlass.protobuf.ProtobufException;
import io.questdb.cutlass.protobuf.ProtobufReader;
import io.questdb.cutlass.protobuf.ProtobufWireFormat;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ProtobufReaderTest {

    private static final int BUF_SIZE = 4096;

    @Test
    public void testReadLengthDelimitedFromGoogle() throws IOException {
        byte[] payload = "Arrow Flight SQL handshake payload".getBytes();
        byte[] wire = new byte[payload.length + 10];
        CodedOutputStream out = CodedOutputStream.newInstance(wire);
        out.writeByteArray(2, payload);
        int len = wire.length - out.spaceLeft();
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            ProtobufReader r = new ProtobufReader();
            r.of(buf, buf + len);
            int tag = r.readTag();
            Assert.assertEquals(2, ProtobufWireFormat.fieldNumberOf(tag));
            Assert.assertEquals(ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED, ProtobufWireFormat.wireTypeOf(tag));
            r.readLengthDelimited();
            Assert.assertEquals(payload.length, r.lastValueLen());
            for (int i = 0; i < payload.length; i++) {
                Assert.assertEquals(payload[i], Unsafe.getUnsafe().getByte(r.lastValueAddr() + i));
            }
            Assert.assertFalse(r.hasMore());
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadTruncatedLengthDelimited() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // tag (field 2, LEN) + length 5, but only 2 bytes of payload provided.
            Unsafe.getUnsafe().putByte(buf, (byte) 0x12);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x05);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 'a');
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 'b');
            ProtobufReader r = new ProtobufReader();
            r.of(buf, buf + 4);
            r.readTag();
            try {
                r.readLengthDelimited();
                Assert.fail();
            } catch (ProtobufException expected) {
                // ok
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadTruncatedVarint() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Continuation bit set but no further byte.
            Unsafe.getUnsafe().putByte(buf, (byte) 0x80);
            ProtobufReader r = new ProtobufReader();
            r.of(buf, buf + 1);
            try {
                r.readVarint64();
                Assert.fail();
            } catch (ProtobufException expected) {
                // ok
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReadVarint64FromGoogle() throws IOException {
        long[] values = {
                0L, 1L, 127L, 128L, 16_383L, 16_384L,
                (1L << 28) - 1, 1L << 28,
                Integer.MAX_VALUE, 1L << 32,
                Long.MAX_VALUE, -1L, Long.MIN_VALUE
        };
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            ProtobufReader r = new ProtobufReader();
            for (long v : values) {
                byte[] wire = new byte[10];
                CodedOutputStream out = CodedOutputStream.newInstance(wire);
                out.writeUInt64NoTag(v);
                int len = wire.length - out.spaceLeft();
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, wire[i]);
                }
                r.of(buf, buf + len);
                long decoded = r.readVarint64();
                Assert.assertEquals("round-trip " + v, v, decoded);
                Assert.assertFalse(r.hasMore());
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSkipUnknownField() throws IOException {
        // Stream: field 7 (varint) = 42, field 99 (len-delimited) = "xyz", field 3 (varint) = 9.
        // Reader should skip 7 and 99 and read 3.
        byte[] wire = new byte[32];
        CodedOutputStream out = CodedOutputStream.newInstance(wire);
        out.writeUInt64(7, 42);
        out.writeByteArray(99, new byte[]{'x', 'y', 'z'});
        out.writeUInt64(3, 9);
        int len = wire.length - out.spaceLeft();
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(buf + i, wire[i]);
            }
            ProtobufReader r = new ProtobufReader();
            r.of(buf, buf + len);
            long expected3 = -1;
            while (r.hasMore()) {
                int tag = r.readTag();
                int fn = ProtobufWireFormat.fieldNumberOf(tag);
                int wt = ProtobufWireFormat.wireTypeOf(tag);
                if (fn == 3) {
                    expected3 = r.readVarint64();
                } else {
                    r.skipField(wt);
                }
            }
            Assert.assertEquals(9L, expected3);
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
