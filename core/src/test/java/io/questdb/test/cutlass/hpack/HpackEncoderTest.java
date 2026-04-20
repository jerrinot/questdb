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

import io.questdb.cutlass.hpack.HpackDecoder;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.hpack.HpackListener;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HpackEncoderTest {

    private static final int BUF_SIZE = 64 * 1024;
    private static final int FIELD_CAP = 8192;
    private static final int OUTPUT_CAP = 32 * 1024;

    @Test
    public void testBufferTooSmallReturnsMinusOneAtomically() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long scratch = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long afterBegin = encoder.beginBlock(buf, buf + BUF_SIZE);
            Assert.assertTrue(afterBegin > 0);

            byte[] name = "x-custom-long-header-name".getBytes();
            byte[] value = "x-custom-long-header-value".getBytes();
            copyToNative(name, scratch);
            copyToNative(value, scratch + name.length);

            // Offer a buffer that's clearly too small for a literal (at most 10 bytes available).
            long miniLimit = afterBegin + 10;
            long r = encoder.encode(afterBegin, miniLimit, scratch, name.length,
                    scratch + name.length, value.length, HpackEncoder.HINT_NONE);
            Assert.assertEquals(-1L, r);
            // Preflight atomicity: no bytes written past afterBegin.
            byte marker = (byte) 0xAA;
            for (long p = afterBegin; p < miniLimit; p++) {
                Unsafe.getUnsafe().putByte(p, marker);
            }
            r = encoder.encode(afterBegin, miniLimit, scratch, name.length,
                    scratch + name.length, value.length, HpackEncoder.HINT_NONE);
            Assert.assertEquals(-1L, r);
            for (long p = afterBegin; p < miniLimit; p++) {
                Assert.assertEquals(marker, Unsafe.getUnsafe().getByte(p));
            }
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(scratch, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testBeginBlockPreflightAtomicity() {
        // The encoder is constructed with queuedSizeUpdate=0, so beginBlock must emit
        // exactly one byte (0x20) on the first call. Offering a buffer smaller than the
        // queued prelude must return -1 without writing any bytes, so a retry after drain
        // sees the queue still armed and the caller buffer untouched.
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            // Sentinel the first byte to detect any write.
            Unsafe.getUnsafe().putByte(buf, (byte) 0xFE);
            long r = encoder.beginBlock(buf, buf);
            Assert.assertEquals(-1L, r);
            Assert.assertEquals((byte) 0xFE, Unsafe.getUnsafe().getByte(buf));

            // Retry with room: the queue is still armed, and we get the pin.
            long end = encoder.beginBlock(buf, buf + BUF_SIZE);
            Assert.assertEquals(1L, end - buf);
            Assert.assertEquals((byte) 0x20, Unsafe.getUnsafe().getByte(buf));
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testConstructorRejectsPoolBelowLocalPreferredCap() {
        // Design doc §11: encoder pool must cover every selectedMax the connection can
        // ever operate at, bounded by localPreferredCap. Mismatched config must fail at
        // construction, not at the first Milestone 2 cap raise.
        try {
            new HpackEncoder(4096, 4096, 1024, FIELD_CAP, OUTPUT_CAP);
            Assert.fail("expected rejection");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void testFirstBlockEmitsSizeUpdateZero() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long end = encoder.beginBlock(buf, buf + BUF_SIZE);
            Assert.assertEquals(1L, end - buf);
            // 0x20 = size update pattern with value 0 in the 5-bit prefix.
            Assert.assertEquals((byte) 0x20, Unsafe.getUnsafe().getByte(buf));
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHintNoneProbesStaticTable() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long scratch = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long cursor = encoder.beginBlock(buf, buf + BUF_SIZE);

            // (A) Exact match in static table: :status 200 (idx 8).
            byte[] name = ":status".getBytes();
            byte[] value = "200".getBytes();
            copyToNative(name, scratch);
            copyToNative(value, scratch + name.length);
            cursor = encoder.encode(cursor, buf + BUF_SIZE, scratch, name.length,
                    scratch + name.length, value.length, HpackEncoder.HINT_NONE);
            Assert.assertTrue(cursor > 0);

            // (B) Name-only match: :status with a non-standard value "299".
            byte[] nameB = ":status".getBytes();
            byte[] valueB = "299".getBytes();
            copyToNative(nameB, scratch);
            copyToNative(valueB, scratch + nameB.length);
            cursor = encoder.encode(cursor, buf + BUF_SIZE, scratch, nameB.length,
                    scratch + nameB.length, valueB.length, HpackEncoder.HINT_NONE);

            // (C) Neither in static table: custom-name / custom-value.
            byte[] nameC = "x-custom".getBytes();
            byte[] valueC = "custom-value".getBytes();
            copyToNative(nameC, scratch);
            copyToNative(valueC, scratch + nameC.length);
            cursor = encoder.encode(cursor, buf + BUF_SIZE, scratch, nameC.length,
                    scratch + nameC.length, valueC.length, HpackEncoder.HINT_NONE);
            encoder.endBlock();

            // Round-trip through our own decoder.
            List<String[]> headers = decodeAll(buf, cursor, 4096, 4096);
            Assert.assertEquals(3, headers.size());
            Assert.assertArrayEquals(new String[]{":status", "200"}, headers.get(0));
            Assert.assertArrayEquals(new String[]{":status", "299"}, headers.get(1));
            Assert.assertArrayEquals(new String[]{"x-custom", "custom-value"}, headers.get(2));
        } finally {
            encoder.close();
            Unsafe.free(scratch, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHintStaticIndexEmitsIndexedHeader() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long cursor = encoder.beginBlock(buf, buf + BUF_SIZE);
            // :status 200 is static idx 8 -> wire 0x88.
            cursor = encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0,
                    HpackEncoder.HINT_STATIC_INDEX | 8);
            encoder.endBlock();

            List<String[]> headers = decodeAll(buf, cursor, 4096, 4096);
            Assert.assertEquals(1, headers.size());
            Assert.assertArrayEquals(new String[]{":status", "200"}, headers.get(0));
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHintStaticNameEmitsLiteralWithIndexedName() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long value = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long cursor = encoder.beginBlock(buf, buf + BUF_SIZE);
            // content-type is static idx 31. Value: application/grpc.
            byte[] valueBytes = "application/grpc".getBytes();
            copyToNative(valueBytes, value);
            cursor = encoder.encode(cursor, buf + BUF_SIZE, 0, 0, value, valueBytes.length,
                    HpackEncoder.HINT_STATIC_NAME | 31);
            encoder.endBlock();

            List<String[]> headers = decodeAll(buf, cursor, 4096, 4096);
            Assert.assertEquals(1, headers.size());
            Assert.assertArrayEquals(new String[]{"content-type", "application/grpc"}, headers.get(0));
        } finally {
            encoder.close();
            Unsafe.free(value, 32, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOnPeerAdvertisedCapChangedAcceptsUnsigned32Bit() {
        // SETTINGS_HEADER_TABLE_SIZE is unsigned 32-bit on the wire; a top-bit-set peer
        // setting must not surface as a negative int here. The argument is typed long and
        // clamped to localPreferredCap internally.
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            encoder.onPeerAdvertisedCapChanged(0x80000000L);
            Assert.assertEquals(4096, encoder.peerAdvertisedCap());
            encoder.onPeerAdvertisedCapChanged(0xFFFFFFFFL);
            Assert.assertEquals(4096, encoder.peerAdvertisedCap());
            // Below localPreferredCap: stored as-is.
            encoder.onPeerAdvertisedCapChanged(1024L);
            Assert.assertEquals(1024, encoder.peerAdvertisedCap());
            // Out-of-range values rejected.
            try {
                encoder.onPeerAdvertisedCapChanged(-1L);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            try {
                encoder.onPeerAdvertisedCapChanged(0x100000000L);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
        } finally {
            encoder.close();
        }
    }

    @Test
    public void testOnPeerAdvertisedCapChangedDoesNotEmitOnM1() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            // First block: size-update pin.
            long c1 = encoder.beginBlock(buf, buf + BUF_SIZE);
            Assert.assertEquals(1L, c1 - buf);
            encoder.endBlock();

            // Peer lowers cap to 0 between blocks: Milestone 1 records but takes no action.
            encoder.onPeerAdvertisedCapChanged(0);

            // Second block: nothing queued, beginBlock is a no-op.
            long c2 = encoder.beginBlock(c1, buf + BUF_SIZE);
            Assert.assertEquals(c1, c2);
            encoder.endBlock();

            // Peer raises cap to 16 KiB: still no action.
            encoder.onPeerAdvertisedCapChanged(16_384);

            long c3 = encoder.beginBlock(c2, buf + BUF_SIZE);
            Assert.assertEquals(c2, c3);
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEncodeRejectsUnrecognizedHintBits() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long cursor = encoder.beginBlock(buf, buf + BUF_SIZE);
            // Both kind bits set simultaneously — not a recognized hint.
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0,
                        HpackEncoder.HINT_STATIC_INDEX | HpackEncoder.HINT_STATIC_NAME | 5);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            // Stray bit outside the kind / index masks — not a recognized hint, not HINT_NONE.
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0, 0x10000000);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            // High bit set — neither kind bit and not HINT_NONE.
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0, 0x80000000);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            // Stray bit combined with a recognized kind — must fail BEFORE dispatching
            // on kind, not silently emit as static-indexed / static-named.
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0,
                        HpackEncoder.HINT_STATIC_INDEX | 0x10000000 | 8);
                Assert.fail("expected rejection (stray bit + HINT_STATIC_INDEX)");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0,
                        HpackEncoder.HINT_STATIC_NAME | 0x80000000 | 31);
                Assert.fail("expected rejection (high bit + HINT_STATIC_NAME)");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHintStaticIndexRejectsZero() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long cursor = encoder.beginBlock(buf, buf + BUF_SIZE);
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0, HpackEncoder.HINT_STATIC_INDEX | 0);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, 0, 0, HpackEncoder.HINT_STATIC_INDEX | 62);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHintStaticNameRejectsZero() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long value = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long cursor = encoder.beginBlock(buf, buf + BUF_SIZE);
            try {
                encoder.encode(cursor, buf + BUF_SIZE, 0, 0, value, 1, HpackEncoder.HINT_STATIC_NAME | 0);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(value, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testResetAllowsPoolReuseAcrossConnections() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            // "Connection 1": consume the initial pin + emit one indexed header.
            long c = encoder.beginBlock(buf, buf + BUF_SIZE);
            c = encoder.encode(c, buf + BUF_SIZE, 0, 0, 0, 0, HpackEncoder.HINT_STATIC_INDEX | 2);
            encoder.endBlock();
            encoder.onPeerAdvertisedCapChanged(0);

            // Hand the instance to "connection 2": reset without reallocating.
            encoder.reset(4096);
            Assert.assertEquals(0, encoder.selectedMax());
            Assert.assertEquals(4096, encoder.peerAdvertisedCap());

            // The new connection's first block must emit the size-update pin again.
            long start = encoder.beginBlock(buf, buf + BUF_SIZE);
            Assert.assertEquals(1L, start - buf);
            Assert.assertEquals((byte) 0x20, Unsafe.getUnsafe().getByte(buf));
            encoder.endBlock();

            // And the second block of the reused instance must emit nothing (pin already consumed).
            long again = encoder.beginBlock(start, buf + BUF_SIZE);
            Assert.assertEquals(start, again);
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSecondBlockEmitsNothing() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long c1 = encoder.beginBlock(buf, buf + BUF_SIZE);
            encoder.endBlock();
            long c2 = encoder.beginBlock(c1, buf + BUF_SIZE);
            Assert.assertEquals(c1, c2);
            encoder.endBlock();
        } finally {
            encoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void copyToNative(byte[] src, long addr) {
        for (int i = 0; i < src.length; i++) {
            Unsafe.getUnsafe().putByte(addr + i, src[i]);
        }
    }

    private static List<String[]> decodeAll(long buf, long end, int initialCap, int poolCap) {
        HpackDecoder decoder = new HpackDecoder(initialCap, poolCap);
        try {
            List<String[]> out = new ArrayList<>();
            HpackListener listener = (n, nl, v, vl, ni) -> {
                byte[] nb = new byte[nl];
                for (int i = 0; i < nl; i++) {
                    nb[i] = Unsafe.getUnsafe().getByte(n + i);
                }
                byte[] vb = new byte[vl];
                for (int i = 0; i < vl; i++) {
                    vb[i] = Unsafe.getUnsafe().getByte(v + i);
                }
                out.add(new String[]{new String(nb), new String(vb)});
            };
            decoder.decodeBlock(buf, end, listener);
            return out;
        } finally {
            decoder.close();
        }
    }
}
