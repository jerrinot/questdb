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

package io.questdb.test.cutlass.hpack;

import io.questdb.cutlass.hpack.HpackException;
import io.questdb.cutlass.hpack.HpackIntCodec;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class HpackIntCodecTest {

    private static final int BUF_SIZE = 64;

    @Test
    public void testDecodeOverflowBeyondMaxInt() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // 7-bit prefix saturated (0x7F), then 5 continuation bytes pushing past Integer.MAX_VALUE.
            Unsafe.getUnsafe().putByte(buf, (byte) 0x7F);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(buf + 4, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(buf + 5, (byte) 0x7F);
            try {
                HpackIntCodec.decode(buf, buf + 6, 0x7F);
                Assert.fail("expected overflow");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRfc7541ExampleC11() {
        // RFC 7541 C.1.1: encoding of 10 with 5-bit prefix -> 0x0A single byte.
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0x0A);
            long packed = HpackIntCodec.decode(buf, buf + 1, 0x1F);
            Assert.assertEquals(10, Numbers.decodeLowInt(packed));
            Assert.assertEquals(1, Numbers.decodeHighInt(packed));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRfc7541ExampleC12() {
        // RFC 7541 C.1.2: encoding of 1337 with 5-bit prefix.
        // Wire: 0x1F 0x9A 0x0A (prefix saturates to 31, then 1337 - 31 = 1306 base-128).
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0x1F);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x9A);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x0A);
            long packed = HpackIntCodec.decode(buf, buf + 3, 0x1F);
            Assert.assertEquals(1337, Numbers.decodeLowInt(packed));
            Assert.assertEquals(3, Numbers.decodeHighInt(packed));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRfc7541ExampleC13() {
        // RFC 7541 C.1.3: encoding of 42 with 8-bit prefix (i.e. fresh start, no pattern bits).
        // We use 7-bit prefix which matches Appendix C.1.3's context for a starting byte: 0x2A.
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0x2A);
            long packed = HpackIntCodec.decode(buf, buf + 1, 0x7F);
            Assert.assertEquals(42, Numbers.decodeLowInt(packed));
            Assert.assertEquals(1, Numbers.decodeHighInt(packed));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeTruncatedEmpty() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            try {
                HpackIntCodec.decode(buf, buf, 0x7F);
                Assert.fail("expected truncation");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeTruncatedMidContinuation() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Saturated 5-bit prefix says "continue", but no continuation follows.
            Unsafe.getUnsafe().putByte(buf, (byte) 0x1F);
            try {
                HpackIntCodec.decode(buf, buf + 1, 0x1F);
                Assert.fail("expected truncation");
            } catch (HpackException expected) {
                // ok
            }
            // Continuation byte has top bit set (more bytes expected), then limit runs out.
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x80);
            try {
                HpackIntCodec.decode(buf, buf + 2, 0x1F);
                Assert.fail("expected truncation");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeBufferTooSmallReturnsMinusOne() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encoding 1337 with 5-bit prefix needs 3 bytes; give it 2.
            long r = HpackIntCodec.encode(buf, buf + 2, 0x1F, 0x20, 1337);
            Assert.assertEquals(-1L, r);
            // Buffer untouched on the -1 path (preflight atomicity).
            Unsafe.getUnsafe().putByte(buf, (byte) 0xCC);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0xCC);
            r = HpackIntCodec.encode(buf, buf + 2, 0x1F, 0x20, 1337);
            Assert.assertEquals(-1L, r);
            Assert.assertEquals((byte) 0xCC, Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) 0xCC, Unsafe.getUnsafe().getByte(buf + 1));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeRfc7541ExampleC11() {
        // 10 with 5-bit prefix, first-byte flags 0x20 (size update pattern).
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = HpackIntCodec.encode(buf, buf + BUF_SIZE, 0x1F, 0x20, 10);
            Assert.assertEquals(1L, end - buf);
            Assert.assertEquals((byte) 0x2A, Unsafe.getUnsafe().getByte(buf));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeRfc7541ExampleC12() {
        // 1337 with 5-bit prefix, pattern 0x20 (size update).
        // Expect 0x3F 0x9A 0x0A.
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = HpackIntCodec.encode(buf, buf + BUF_SIZE, 0x1F, 0x20, 1337);
            Assert.assertEquals(3L, end - buf);
            Assert.assertEquals((byte) 0x3F, Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) 0x9A, Unsafe.getUnsafe().getByte(buf + 1));
            Assert.assertEquals((byte) 0x0A, Unsafe.getUnsafe().getByte(buf + 2));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodedLengthMatchesEncode() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            int[] widths = {4, 5, 6, 7};
            int[] values = {0, 1, 14, 15, 30, 31, 62, 63, 126, 127, 128, 1337, 65_535, 1_000_000, Integer.MAX_VALUE};
            for (int bits : widths) {
                int mask = (1 << bits) - 1;
                for (int v : values) {
                    int predicted = HpackIntCodec.encodedLength(bits, v);
                    long end = HpackIntCodec.encode(buf, buf + BUF_SIZE, mask, 0, v);
                    Assert.assertTrue("encode must succeed at prefixBits=" + bits + " value=" + v, end > 0);
                    Assert.assertEquals("prefixBits=" + bits + " value=" + v, predicted, (int) (end - buf));
                }
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeRejectsNegativeValue() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            try {
                HpackIntCodec.encode(buf, buf + BUF_SIZE, 0x7F, 0x80, -1);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            try {
                HpackIntCodec.encodedLength(7, -1);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            try {
                HpackIntCodec.encodedLength(5, Integer.MIN_VALUE);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRoundtripFuzz() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Rnd rnd = new Rnd(0xDEADBEEFL, 0xC0FFEEL);
            int[] widths = {4, 5, 6, 7};
            for (int i = 0; i < 10_000; i++) {
                int bits = widths[rnd.nextPositiveInt() % 4];
                int mask = (1 << bits) - 1;
                int flags = (rnd.nextInt() & ~mask) & 0xFF;
                int value = rnd.nextPositiveInt();
                long end = HpackIntCodec.encode(buf, buf + BUF_SIZE, mask, flags, value);
                Assert.assertTrue(end > 0);
                long packed = HpackIntCodec.decode(buf, end, mask);
                Assert.assertEquals("value bits=" + bits + " flags=" + flags + " v=" + value,
                        value, Numbers.decodeLowInt(packed));
                Assert.assertEquals(end - buf, Numbers.decodeHighInt(packed));
                // Pattern flags survived into the first byte.
                int firstByte = Unsafe.getUnsafe().getByte(buf) & 0xFF;
                Assert.assertEquals(flags, firstByte & ~mask);
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
