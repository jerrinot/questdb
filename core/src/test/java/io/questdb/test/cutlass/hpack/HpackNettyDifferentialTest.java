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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.questdb.cutlass.hpack.HpackDecoder;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.hpack.HpackListener;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HPACK Tier 3 differential: roundtrip between our {@link HpackEncoder} /
 * {@link HpackDecoder} and Netty's {@link DefaultHttp2HeadersEncoder} /
 * {@link DefaultHttp2HeadersDecoder}. Validates our wire output against the
 * JVM reference implementation in both directions.
 * <p>
 * Milestone 1 encoder posture: static-indexed + literal-without-indexing
 * with plain-octet strings, and a size-update-to-0 pin on every first block.
 * Netty's decoder accepts both shapes.
 */
public class HpackNettyDifferentialTest {

    private static final int BUF_SIZE = 64 * 1024;
    private static final int FIELD_CAP = 8192;
    private static final int OUTPUT_CAP = 32 * 1024;

    @Test
    public void testNettyEncoderToOurDecoder_CustomHeaders() throws Http2Exception {
        Http2Headers headers = new DefaultHttp2Headers()
                .method("POST")
                .scheme("http")
                .path("/service/Method")
                .authority("grpc.example.com");
        headers.add("content-type", "application/grpc");
        headers.add("te", "trailers");
        headers.add("user-agent", "grpc-go/1.60");
        headers.add("x-request-id", "abc-123-xyz-456");
        headers.add("x-trace-id", "0123456789abcdef0123456789abcdef");
        assertNettyToOurRoundtrip(headers);
    }

    @Test
    public void testNettyEncoderToOurDecoder_GrpcResponse() throws Http2Exception {
        Http2Headers headers = new DefaultHttp2Headers()
                .status("200");
        headers.add("content-type", "application/grpc");
        headers.add("grpc-status", "0");
        headers.add("grpc-message", "ok");
        assertNettyToOurRoundtrip(headers);
    }

    @Test
    public void testNettyEncoderToOurDecoder_PseudoHeaders() throws Http2Exception {
        Http2Headers headers = new DefaultHttp2Headers()
                .method("GET")
                .scheme("https")
                .path("/")
                .authority("www.example.com");
        assertNettyToOurRoundtrip(headers);
    }

    @Test
    public void testOurEncoderToNettyDecoder_CustomHeaders() throws Http2Exception {
        List<String[]> input = new ArrayList<>();
        input.add(new String[]{":method", "POST"});
        input.add(new String[]{":scheme", "http"});
        input.add(new String[]{":path", "/service/Method"});
        input.add(new String[]{":authority", "grpc.example.com"});
        input.add(new String[]{"content-type", "application/grpc"});
        input.add(new String[]{"te", "trailers"});
        input.add(new String[]{"user-agent", "grpc-go/1.60"});
        input.add(new String[]{"x-request-id", "abc-123-xyz-456"});
        input.add(new String[]{"x-trace-id", "0123456789abcdef0123456789abcdef"});
        assertOurToNettyRoundtrip(input);
    }

    @Test
    public void testOurEncoderToNettyDecoder_GrpcResponse() throws Http2Exception {
        List<String[]> input = new ArrayList<>();
        input.add(new String[]{":status", "200"});
        input.add(new String[]{"content-type", "application/grpc"});
        input.add(new String[]{"grpc-status", "0"});
        input.add(new String[]{"grpc-message", "ok"});
        assertOurToNettyRoundtrip(input);
    }

    @Test
    public void testOurEncoderToNettyDecoder_PseudoHeaders() throws Http2Exception {
        List<String[]> input = new ArrayList<>();
        input.add(new String[]{":method", "GET"});
        input.add(new String[]{":scheme", "https"});
        input.add(new String[]{":path", "/"});
        input.add(new String[]{":authority", "www.example.com"});
        assertOurToNettyRoundtrip(input);
    }

    @Test
    public void testRoundtripFuzz() throws Http2Exception {
        Rnd rnd = new Rnd(0xA1B2C3D4L, 0xE5F6A7B8L);
        for (int trial = 0; trial < 50; trial++) {
            int numHeaders = 1 + rnd.nextPositiveInt() % 8;
            List<String[]> input = new ArrayList<>();
            // Every block must start with valid pseudo-headers in the normal request order.
            input.add(new String[]{":method", (rnd.nextPositiveInt() & 1) == 0 ? "GET" : "POST"});
            input.add(new String[]{":scheme", (rnd.nextPositiveInt() & 1) == 0 ? "https" : "http"});
            input.add(new String[]{":path", "/r/" + rnd.nextPositiveInt()});
            input.add(new String[]{":authority", "host-" + rnd.nextPositiveInt() + ".example.com"});
            for (int i = 0; i < numHeaders; i++) {
                String name = "x-custom-" + (i + 1);
                String value = randomAsciiLower(rnd, 1 + rnd.nextPositiveInt() % 48);
                input.add(new String[]{name, value});
            }
            assertOurToNettyRoundtrip(input);

            Http2Headers nettyHeaders = new DefaultHttp2Headers();
            for (String[] kv : input) {
                nettyHeaders.add(kv[0], kv[1]);
            }
            assertNettyToOurRoundtrip(nettyHeaders);
        }
    }

    private static void assertNettyToOurRoundtrip(Http2Headers headers) throws Http2Exception {
        DefaultHttp2HeadersEncoder nettyEnc = new DefaultHttp2HeadersEncoder();
        ByteBuf out = Unpooled.buffer(BUF_SIZE);
        try {
            nettyEnc.encodeHeaders(1, headers, out);
            int blockLen = out.readableBytes();

            long nativeBlock = Unsafe.malloc(Math.max(1, blockLen), MemoryTag.NATIVE_DEFAULT);
            HpackDecoder decoder = new HpackDecoder(4096, 4096);
            try {
                for (int i = 0; i < blockLen; i++) {
                    Unsafe.getUnsafe().putByte(nativeBlock + i, out.getByte(out.readerIndex() + i));
                }
                List<String[]> decoded = new ArrayList<>();
                HpackListener listener = (nameAddr, nameLen, valueAddr, valueLen, ni) -> {
                    decoded.add(new String[]{readString(nameAddr, nameLen), readString(valueAddr, valueLen)});
                };
                decoder.decodeBlock(nativeBlock, nativeBlock + blockLen, listener);

                List<String[]> expected = new ArrayList<>();
                for (Map.Entry<CharSequence, CharSequence> e : headers) {
                    expected.add(new String[]{e.getKey().toString(), e.getValue().toString()});
                }
                assertHeaderListsEqual(expected, decoded);
            } finally {
                decoder.close();
                Unsafe.free(nativeBlock, Math.max(1, blockLen), MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            out.release();
        }
    }

    private static void assertOurToNettyRoundtrip(List<String[]> input) throws Http2Exception {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long scratch = Unsafe.malloc(FIELD_CAP * 2, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, FIELD_CAP, OUTPUT_CAP);
        try {
            long cursor = encoder.beginBlock(buf, buf + BUF_SIZE);
            Assert.assertTrue(cursor > 0);
            for (String[] kv : input) {
                byte[] n = kv[0].getBytes();
                byte[] v = kv[1].getBytes();
                for (int i = 0; i < n.length; i++) {
                    Unsafe.getUnsafe().putByte(scratch + i, n[i]);
                }
                for (int i = 0; i < v.length; i++) {
                    Unsafe.getUnsafe().putByte(scratch + n.length + i, v[i]);
                }
                cursor = encoder.encode(cursor, buf + BUF_SIZE,
                        scratch, n.length, scratch + n.length, v.length, HpackEncoder.HINT_NONE);
                Assert.assertTrue(cursor > 0);
            }
            encoder.endBlock();
            int blockLen = (int) (cursor - buf);

            ByteBuf bb = Unpooled.buffer(blockLen);
            try {
                for (int i = 0; i < blockLen; i++) {
                    bb.writeByte(Unsafe.getUnsafe().getByte(buf + i));
                }
                DefaultHttp2HeadersDecoder nettyDec = new DefaultHttp2HeadersDecoder(true);
                Http2Headers decoded = nettyDec.decodeHeaders(1, bb);

                List<String[]> got = new ArrayList<>();
                for (Map.Entry<CharSequence, CharSequence> e : decoded) {
                    got.add(new String[]{e.getKey().toString(), e.getValue().toString()});
                }
                assertHeaderListsEqual(input, got);
            } finally {
                bb.release();
            }
        } finally {
            encoder.close();
            Unsafe.free(scratch, FIELD_CAP * 2, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static String randomAsciiLower(Rnd rnd, int len) {
        char[] c = new char[len];
        for (int i = 0; i < len; i++) {
            c[i] = (char) ('a' + rnd.nextPositiveInt() % 26);
        }
        return new String(c);
    }

    private static String readString(long addr, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return new String(b);
    }

    private static void assertHeaderListsEqual(List<String[]> expected, List<String[]> actual) {
        Assert.assertEquals("header count", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals("name at " + i, expected.get(i)[0], actual.get(i)[0]);
            Assert.assertEquals("value at " + i, expected.get(i)[1], actual.get(i)[1]);
        }
    }
}
