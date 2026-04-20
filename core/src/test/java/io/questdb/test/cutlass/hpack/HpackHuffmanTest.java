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
import io.questdb.cutlass.hpack.HpackHuffman;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class HpackHuffmanTest {

    @Test
    public void testDecodeAcceptsEosPrefixPadding() {
        long src = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            // Encoding "0" (symbol 48, code 0x00, 5 bits): the 5-bit code is all 0s,
            // padded with 3 trailing 1s yields 0x07.
            Unsafe.getUnsafe().putByte(src, (byte) 0x07);
            int n = HpackHuffman.decode(src, 1, dst, dst + 32);
            Assert.assertEquals(1, n);
            Assert.assertEquals((byte) '0', Unsafe.getUnsafe().getByte(dst));
        } finally {
            Unsafe.free(dst, 32, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeBufferTooSmallReturnsMinusOne() {
        long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        long srcIn = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] msg = "www.example.com".getBytes();
            for (int i = 0; i < msg.length; i++) {
                Unsafe.getUnsafe().putByte(srcIn + i, msg[i]);
            }
            long end = HpackHuffman.encode(src, src + 64, srcIn, msg.length);
            int encLen = (int) (end - src);
            Assert.assertEquals(-1, HpackHuffman.decode(src, encLen, dst, dst + 5));
        } finally {
            Unsafe.free(dst, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(srcIn, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRejectsEosAsProperSymbol() {
        long src = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            // EOS code is 30 bits of 1s: 0x3fffffff. Pack into 4 bytes with 2 padding bits.
            // MSB-first: 11111111_11111111_11111111_11111100 → 0xFF 0xFF 0xFF 0xFC.
            Unsafe.getUnsafe().putByte(src, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(src + 1, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(src + 2, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(src + 3, (byte) 0xFC);
            try {
                HpackHuffman.decode(src, 4, dst, dst + 8);
                Assert.fail("expected EOS rejection");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            Unsafe.free(dst, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRejectsNonEosPrefixPadding() {
        long src = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            // Symbol '0' (code 0x00, 5 bits) followed by 3 bits of padding that are NOT all 1s.
            // Valid padding is 0x07 (binary 000 + 111). Invalid: 0x04 = 000 + 100.
            Unsafe.getUnsafe().putByte(src, (byte) 0x04);
            try {
                HpackHuffman.decode(src, 1, dst, dst + 8);
                Assert.fail("expected padding rejection");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            Unsafe.free(dst, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRejectsOverlongPadding() {
        long src = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            // 'A' (code 0x21 = binary 100001, 6 bits) + 10 bits of all-1 padding across two bytes.
            // A complete '0' (code 0, 5 bits) or similar cannot have more than 7 padding bits.
            // Constructed: 100001_11 11111111 — first byte 0x87, second byte 0xFF.
            Unsafe.getUnsafe().putByte(src, (byte) 0x87);
            Unsafe.getUnsafe().putByte(src + 1, (byte) 0xFF);
            try {
                HpackHuffman.decode(src, 2, dst, dst + 8);
                Assert.fail("expected overlong padding rejection");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            Unsafe.free(dst, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDecodeRfc7541C41WwwExampleCom() {
        // RFC 7541 C.4.1 encodes "www.example.com" as 12 Huffman bytes.
        long src = Unsafe.malloc(12, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] wire = {
                    (byte) 0xf1, (byte) 0xe3, (byte) 0xc2, (byte) 0xe5,
                    (byte) 0xf2, (byte) 0x3a, (byte) 0x6b, (byte) 0xa0,
                    (byte) 0xab, (byte) 0x90, (byte) 0xf4, (byte) 0xff
            };
            for (int i = 0; i < wire.length; i++) {
                Unsafe.getUnsafe().putByte(src + i, wire[i]);
            }
            int n = HpackHuffman.decode(src, wire.length, dst, dst + 32);
            Assert.assertEquals(15, n);
            byte[] expected = "www.example.com".getBytes();
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("byte " + i, expected[i], Unsafe.getUnsafe().getByte(dst + i));
            }
        } finally {
            Unsafe.free(dst, 32, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 12, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeDecodeAllSingleBytes() {
        long src = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        long enc = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int sym = 0; sym < 256; sym++) {
                Unsafe.getUnsafe().putByte(src, (byte) sym);
                long end = HpackHuffman.encode(enc, enc + 8, src, 1);
                Assert.assertTrue("sym=" + sym, end > 0);
                int encLen = (int) (end - enc);
                int n = HpackHuffman.decode(enc, encLen, dst, dst + 8);
                Assert.assertEquals("sym=" + sym, 1, n);
                Assert.assertEquals("sym=" + sym, (byte) sym, Unsafe.getUnsafe().getByte(dst));
            }
        } finally {
            Unsafe.free(dst, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(enc, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 1, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeRfc7541C41WwwExampleCom() {
        byte[] msg = "www.example.com".getBytes();
        long src = Unsafe.malloc(msg.length, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < msg.length; i++) {
                Unsafe.getUnsafe().putByte(src + i, msg[i]);
            }
            long end = HpackHuffman.encode(dst, dst + 32, src, msg.length);
            int encLen = (int) (end - dst);
            Assert.assertEquals(12, encLen);
            byte[] expected = {
                    (byte) 0xf1, (byte) 0xe3, (byte) 0xc2, (byte) 0xe5,
                    (byte) 0xf2, (byte) 0x3a, (byte) 0x6b, (byte) 0xa0,
                    (byte) 0xab, (byte) 0x90, (byte) 0xf4, (byte) 0xff
            };
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("byte " + i, expected[i], Unsafe.getUnsafe().getByte(dst + i));
            }
        } finally {
            Unsafe.free(dst, 32, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, msg.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodedLengthMatchesEncode() {
        long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
        try {
            Rnd rnd = new Rnd(0x12345678L, 0x87654321L);
            for (int i = 0; i < 64; i++) {
                Unsafe.getUnsafe().putByte(src + i, (byte) rnd.nextInt());
            }
            for (int len = 0; len <= 64; len++) {
                int predicted = HpackHuffman.encodedLength(src, len);
                long end = HpackHuffman.encode(dst, dst + 512, src, len);
                Assert.assertTrue("len=" + len, end >= dst);
                Assert.assertEquals("len=" + len, predicted, (int) (end - dst));
            }
        } finally {
            Unsafe.free(dst, 512, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRoundtripFuzz() {
        long src = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
        long enc = Unsafe.malloc(2048, MemoryTag.NATIVE_DEFAULT);
        long dst = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
        try {
            Rnd rnd = new Rnd(0xDEADBEEFL, 0xCAFEBABEL);
            for (int trial = 0; trial < 200; trial++) {
                int len = rnd.nextPositiveInt() % 256;
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(src + i, (byte) rnd.nextInt());
                }
                long end = HpackHuffman.encode(enc, enc + 2048, src, len);
                Assert.assertTrue(end > 0);
                int encLen = (int) (end - enc);
                int n = HpackHuffman.decode(enc, encLen, dst, dst + 256);
                Assert.assertEquals("trial=" + trial + " len=" + len, len, n);
                for (int i = 0; i < len; i++) {
                    Assert.assertEquals("trial=" + trial + " i=" + i,
                            Unsafe.getUnsafe().getByte(src + i),
                            Unsafe.getUnsafe().getByte(dst + i));
                }
            }
        } finally {
            Unsafe.free(dst, 256, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(enc, 2048, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(src, 256, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
