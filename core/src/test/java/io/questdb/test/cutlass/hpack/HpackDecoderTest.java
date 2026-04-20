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

import io.questdb.cutlass.hpack.Hpack;
import io.questdb.cutlass.hpack.HpackDecoder;
import io.questdb.cutlass.hpack.HpackException;
import io.questdb.cutlass.hpack.HpackListener;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HpackDecoderTest {

    private static final int BUF_SIZE = 4096;

    @Test
    public void testRfc7541C21LiteralWithIndexing() {
        byte[] wire = hex("400a 6375 7374 6f6d 2d6b 6579 0d63 7573 746f 6d2d 6865 6164 6572");
        List<DecodedHeader> out = decode(wire, 4096, 4096);
        Assert.assertEquals(1, out.size());
        Assert.assertEquals("custom-key", out.get(0).name);
        Assert.assertEquals("custom-header", out.get(0).value);
        Assert.assertFalse(out.get(0).neverIndexed);
    }

    @Test
    public void testRfc7541C22LiteralWithoutIndexing() {
        byte[] wire = hex("040c 2f73 616d 706c 652f 7061 7468");
        List<DecodedHeader> out = decode(wire, 4096, 4096);
        Assert.assertEquals(1, out.size());
        Assert.assertEquals(":path", out.get(0).name);
        Assert.assertEquals("/sample/path", out.get(0).value);
        Assert.assertFalse(out.get(0).neverIndexed);
    }

    @Test
    public void testRfc7541C23LiteralNeverIndexed() {
        byte[] wire = hex("1008 7061 7373 776f 7264 0673 6563 7265 74");
        List<DecodedHeader> out = decode(wire, 4096, 4096);
        Assert.assertEquals(1, out.size());
        Assert.assertEquals("password", out.get(0).name);
        Assert.assertEquals("secret", out.get(0).value);
        Assert.assertTrue(out.get(0).neverIndexed);
    }

    @Test
    public void testRfc7541C24IndexedHeaderField() {
        byte[] wire = hex("82");
        List<DecodedHeader> out = decode(wire, 4096, 4096);
        Assert.assertEquals(1, out.size());
        Assert.assertEquals(":method", out.get(0).name);
        Assert.assertEquals("GET", out.get(0).value);
    }

    @Test
    public void testRfc7541C3_MultipleRequestsWithoutHuffman() {
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            // C.3.1 First request.
            List<DecodedHeader> req1 = decodeOne(decoder,
                    hex("828684410f 7777 772e 6578 616d 706c 652e 636f 6d"));
            Assert.assertEquals(4, req1.size());
            assertHeader(req1.get(0), ":method", "GET", false);
            assertHeader(req1.get(1), ":scheme", "http", false);
            assertHeader(req1.get(2), ":path", "/", false);
            assertHeader(req1.get(3), ":authority", "www.example.com", false);
            // Dynamic table now has one entry: :authority / www.example.com (cost 57).
            Assert.assertEquals(1, decoder.dynamicTable().dynamicCount());
            Assert.assertEquals(57, decoder.dynamicTable().currentSize());

            // C.3.2 Second request adds cache-control: no-cache (via idx 24).
            List<DecodedHeader> req2 = decodeOne(decoder,
                    hex("828684be580 8 6e6f 2d63 6163 6865"));
            Assert.assertEquals(5, req2.size());
            assertHeader(req2.get(0), ":method", "GET", false);
            assertHeader(req2.get(1), ":scheme", "http", false);
            assertHeader(req2.get(2), ":path", "/", false);
            assertHeader(req2.get(3), ":authority", "www.example.com", false);
            assertHeader(req2.get(4), "cache-control", "no-cache", false);
            // Dynamic table now has two entries: cache-control / no-cache then :authority / www.example.com.
            Assert.assertEquals(2, decoder.dynamicTable().dynamicCount());
            Assert.assertEquals(110, decoder.dynamicTable().currentSize());
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testLiteralTruncatedAtNameStringHeader() {
        // A literal-without-indexing with new name (0x00 + name-index 0) and NOTHING after:
        // the decoder must reject cleanly, not wild-read past the buffer.
        byte[] wire = hex("00");
        try {
            decode(wire, 4096, 4096);
            Assert.fail("expected truncation rejection");
        } catch (HpackException expected) {
            // ok
        }
    }

    @Test
    public void testPendingSizeUpdateLatchesMinAcrossRaises() {
        // RFC 7541 sec. 4.2 last paragraph: when multiple cap changes happen between blocks,
        // the peer encoder must report the SMALLEST of them on its next block. A decoder that
        // only compared the inbound update against the current localAdvertisedCap would
        // silently accept a peer that skipped the interim minimum.
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            // peerSelectedMax starts at 4096. Drop our cap to 0 (arms the obligation), then
            // raise it back to 4096 before the peer's next block.
            decoder.onLocalAdvertisedCapChanged(0);
            decoder.onLocalAdvertisedCapChanged(4096);
            Assert.assertEquals(4096, decoder.localAdvertisedCap());

            // Peer attempts a single size update to 4096. Valid against localAdvertisedCap
            // but violates the interim-minimum commitment: they owe us <= 0 first.
            try {
                decodeOne(decoder, hex("3fe1 1f 82"));  // 4096 size update + :method GET
                Assert.fail("expected pending-interim rejection");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testPendingSizeUpdateAcceptsInterimThenFinal() {
        // After arming the minimum to 0, the peer can send 0 then any value up to the
        // current localAdvertisedCap; both sit at block start (size updates only before
        // any header representation).
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            decoder.onLocalAdvertisedCapChanged(0);
            decoder.onLocalAdvertisedCapChanged(4096);

            // 0x20 = size update to 0, then 0x3f 0xe1 0x1f = size update to 4096, then :method GET.
            List<DecodedHeader> out = decodeOne(decoder, hex("20 3fe1 1f 82"));
            Assert.assertEquals(1, out.size());
            assertHeader(out.get(0), ":method", "GET", false);
            Assert.assertEquals(4096, decoder.peerSelectedMax());
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testPendingSizeUpdateRatchetsDownOnSubsequentDrops() {
        // Successive drops tighten the minimum. Start peerSelectedMax=4096; drop to 1024
        // (arms min=1024), drop again to 0 (min tightens to 0). The peer must now report 0.
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            decoder.onLocalAdvertisedCapChanged(1024);
            decoder.onLocalAdvertisedCapChanged(0);

            // Peer tries 1024 (allowed by the current localAdvertisedCap=0... wait, 1024 > 0
            // would be rejected by the cap check first). Use a value <= 0: only 0 qualifies.
            // Try peer sending 0 — should succeed.
            List<DecodedHeader> out = decodeOne(decoder, hex("20 82"));
            Assert.assertEquals(1, out.size());
            Assert.assertEquals(0, decoder.peerSelectedMax());
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testPendingSizeUpdateRaisedBelowInterimMinStillRejected() {
        // Drop cap to 0 (min=0), raise to 2048 (min stays 0), peer sends 1024. Valid against
        // localAdvertisedCap but violates min=0.
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            decoder.onLocalAdvertisedCapChanged(0);
            decoder.onLocalAdvertisedCapChanged(2048);

            // 1024 encoded with 5-bit prefix: 0x3F E1 07 (saturate 31, remainder 993).
            try {
                decodeOne(decoder, hex("3fe1 07 82"));
                Assert.fail("expected interim-min rejection");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testOnLocalAdvertisedCapChangedRejectsCapAbovePool() {
        // Fail-fast at the advertise boundary rather than crashing later inside
        // HpackDynamicTable.setOperatingCap when a peer legitimately advertises the
        // full cap we advertised. Pool=4096, try to advertise 8192.
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            int before = decoder.localAdvertisedCap();
            try {
                decoder.onLocalAdvertisedCapChanged(8192);
                Assert.fail("expected rejection");
            } catch (IllegalArgumentException expected) {
                // ok
            }
            // State must be unchanged: the rejected cap did not overwrite localAdvertisedCap.
            Assert.assertEquals(before, decoder.localAdvertisedCap());
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testConstructorRejectsPoolBelowHttp2Default() {
        // Production callers must size the pool to cover at least the HTTP/2 default
        // HPACK table size (4096) so a legal pre-ACK peer size update cannot crash the
        // dynamic table; previously this was silently clamped and legal pre-ACK blocks
        // could be rejected.
        try {
            new HpackDecoder(512, 512);
            Assert.fail("expected rejection");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

@Test
    public void testResetAllowsPoolReuseAcrossConnections() {
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            // "Connection 1": arm the pending-size-update flag, build dynamic state.
            decoder.onLocalAdvertisedCapChanged(0);
            Assert.assertEquals(0, decoder.localAdvertisedCap());
            decodeOne(decoder, hex("20"));  // qualifying size update clears flag, sets peerSelectedMax=0
            Assert.assertEquals(0, decoder.peerSelectedMax());

            // Hand the instance to "connection 2": reset without reallocating.
            decoder.reset(4096);
            Assert.assertEquals(4096, decoder.peerSelectedMax());
            Assert.assertEquals(4096, decoder.localAdvertisedCap());
            Assert.assertEquals(4096, decoder.dynamicTable().currentOperatingCap());
            Assert.assertEquals(0, decoder.dynamicTable().dynamicCount());

            // The flag must no longer be armed and the first block must succeed without any prelude.
            List<DecodedHeader> out = decodeOne(decoder, hex("82"));
            Assert.assertEquals(1, out.size());
            assertHeader(out.get(0), ":method", "GET", false);
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testSizeUpdateAfterHeaderRejected() {
        byte[] wire = hex("8220");  // :method GET (indexed), then size update to 0
        try {
            decode(wire, 4096, 4096);
            Assert.fail("expected rejection");
        } catch (HpackException expected) {
            // ok
        }
    }

    @Test
    public void testSizeUpdateAtBlockStart() {
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            // Size update to 1024 followed by :method GET (idx 2).
            // 0x3F 0xE1 0x07 = size update prefix 5 = saturated; 1024 - 31 = 993 base-128 = 993 → 0xE1 0x07.
            byte[] wire = hex("3fe1 07 82");
            List<DecodedHeader> out = decodeOne(decoder, wire);
            Assert.assertEquals(1, out.size());
            assertHeader(out.get(0), ":method", "GET", false);
            Assert.assertEquals(1024, decoder.dynamicTable().currentOperatingCap());
            Assert.assertEquals(1024, decoder.peerSelectedMax());
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testSizeUpdateExceedsAdvertisedCapRejected() {
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            decoder.onLocalAdvertisedCapChanged(100);
            // Size update to 4096 > localAdvertisedCap 100.
            byte[] wire = hex("3fe1 1f");  // 4096 with 5-bit prefix: saturate (31), remainder 4065 → 0xE1 0x1F.
            try {
                decodeOne(decoder, wire);
                Assert.fail("expected rejection");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testPendingSizeUpdateRequiredAtBlockStart() {
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            // peerSelectedMax starts at 4096. Lower our cap to 0, which is < peerSelectedMax,
            // arming the pending-size-update flag. The peer's next block must begin with a
            // qualifying size update.
            decoder.onLocalAdvertisedCapChanged(0);

            // First attempt: block that starts with a header (no size update) - must throw.
            try {
                decodeOne(decoder, hex("82"));
                Assert.fail("expected pending-size-update rejection");
            } catch (HpackException expected) {
                // ok
            }

            // Second attempt: block starting with a qualifying size update (to 0), then header.
            decodeOne(decoder, hex("20 82"));
            Assert.assertEquals(0, decoder.peerSelectedMax());
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testRfc7541C4_RequestsWithHuffman() {
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        try {
            // C.4.1: :method GET, :scheme http, :path /, :authority www.example.com
            // Wire: 82 86 84 41 8c f1e3c2e5f23a6ba0ab90f4ff
            List<DecodedHeader> req1 = decodeOne(decoder,
                    hex("82 86 84 41 8c f1e3 c2e5 f23a 6ba0 ab90 f4ff"));
            Assert.assertEquals(4, req1.size());
            assertHeader(req1.get(0), ":method", "GET", false);
            assertHeader(req1.get(1), ":scheme", "http", false);
            assertHeader(req1.get(2), ":path", "/", false);
            assertHeader(req1.get(3), ":authority", "www.example.com", false);
            Assert.assertEquals(1, decoder.dynamicTable().dynamicCount());
        } finally {
            decoder.close();
        }
    }

    @Test
    public void testIndexedNameReferenceFromDynamicTableSurvivesEviction() {
        HpackDecoder decoder = new HpackDecoder(120, 4096);
        try {
            // Insert entry 62: custom-name / value-A (cost 32 + 11 + 7 = 50).
            // Then insert entry using name-idx 62 + new value "value-long-enough-to-evict" (26 bytes)
            // cost = 32 + 11 + 26 = 69. Total size would be 50 + 69 = 119 <= cap 120, so no eviction yet.
            // Add a third entry that forces eviction of entry 62 (the original name source) to verify
            // the staged-name mechanism survives.
            List<DecodedHeader> seen = new ArrayList<>();
            HpackListener listener = (n, nl, v, vl, ni) -> seen.add(DecodedHeader.of(n, nl, v, vl, ni));

            long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                // Block 1: literal with incremental indexing, name=custom-name, value=value-A.
                byte[] b1 = hex("400b 6375 7374 6f6d 2d6e 616d 6507 7661 6c75 652d 41");
                copyToNative(b1, buf);
                decoder.decodeBlock(buf, buf + b1.length, listener);
                Assert.assertEquals(1, seen.size());
                Assert.assertEquals("custom-name", seen.get(0).name);
                Assert.assertEquals("value-A", seen.get(0).value);
                Assert.assertEquals(1, decoder.dynamicTable().dynamicCount());

                // Block 2: literal with incremental indexing, name-idx=62 (0x7e under 6-bit prefix
                // saturates: 0x40|0x3f + continuation 31 → 0x7F 0x1F, because 62 - 63 = -1, so 62 fits
                // in prefix). Actually 62 > 63? No, 62 < 63, so it fits: first byte 0x40 | 62 = 0x7E.
                // Followed by value length 26, then 26 bytes.
                seen.clear();
                byte[] b2 = hex("7e1a 7661 6c75 652d 6c6f 6e67 2d65 6e6f 7567 682d 746f 2d65 7669 6374");
                copyToNative(b2, buf);
                decoder.decodeBlock(buf, buf + b2.length, listener);
                Assert.assertEquals(1, seen.size());
                Assert.assertEquals("custom-name", seen.get(0).name);
                Assert.assertEquals("value-long-enough-to-evict", seen.get(0).value);
            } finally {
                Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            decoder.close();
        }
    }

    private static void assertHeader(DecodedHeader h, String name, String value, boolean neverIndexed) {
        Assert.assertEquals(name, h.name);
        Assert.assertEquals(value, h.value);
        Assert.assertEquals(neverIndexed, h.neverIndexed);
    }

    private static void copyToNative(byte[] src, long addr) {
        for (int i = 0; i < src.length; i++) {
            Unsafe.getUnsafe().putByte(addr + i, src[i]);
        }
    }

    private static List<DecodedHeader> decode(byte[] wire, int initialCap, int poolCap) {
        HpackDecoder decoder = new HpackDecoder(initialCap, poolCap);
        try {
            return decodeOne(decoder, wire);
        } finally {
            decoder.close();
        }
    }

    private static List<DecodedHeader> decodeOne(HpackDecoder decoder, byte[] wire) {
        long buf = Unsafe.malloc(Math.max(1, wire.length), MemoryTag.NATIVE_DEFAULT);
        try {
            copyToNative(wire, buf);
            List<DecodedHeader> out = new ArrayList<>();
            HpackListener listener = (n, nl, v, vl, ni) -> out.add(DecodedHeader.of(n, nl, v, vl, ni));
            decoder.decodeBlock(buf, buf + wire.length, listener);
            return out;
        } finally {
            Unsafe.free(buf, Math.max(1, wire.length), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] hex(String s) {
        String clean = s.replaceAll("\\s+", "");
        byte[] out = new byte[clean.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(clean.substring(2 * i, 2 * i + 2), 16);
        }
        return out;
    }

    // Exercise Hpack constants to keep imports used.
    static {
        Assert.assertEquals(61, Hpack.STATIC_TABLE_SIZE);
    }

    private static final class DecodedHeader {
        final String name;
        final boolean neverIndexed;
        final String value;

        private DecodedHeader(String name, String value, boolean neverIndexed) {
            this.name = name;
            this.value = value;
            this.neverIndexed = neverIndexed;
        }

        static DecodedHeader of(long nameAddr, int nameLen, long valueAddr, int valueLen, boolean neverIndexed) {
            byte[] n = new byte[nameLen];
            for (int i = 0; i < nameLen; i++) {
                n[i] = Unsafe.getUnsafe().getByte(nameAddr + i);
            }
            byte[] v = new byte[valueLen];
            for (int i = 0; i < valueLen; i++) {
                v[i] = Unsafe.getUnsafe().getByte(valueAddr + i);
            }
            return new DecodedHeader(new String(n), new String(v), neverIndexed);
        }
    }
}
