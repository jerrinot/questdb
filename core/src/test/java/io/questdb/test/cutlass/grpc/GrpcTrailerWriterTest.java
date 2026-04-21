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

package io.questdb.test.cutlass.grpc;

import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.grpc.GrpcTrailerWriter;
import io.questdb.cutlass.hpack.HpackDecoder;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GrpcTrailerWriterTest {

    private static final int BUF_SIZE = 4096;

    @Test
    public void testEncodeStatusDigits() {
        long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 8; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0xFF);
            }
            Assert.assertEquals(1, GrpcTrailerWriter.encodeStatusDigits(0, buf));
            Assert.assertEquals((byte) '0', Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals(2, GrpcTrailerWriter.encodeStatusDigits(13, buf));
            Assert.assertEquals((byte) '1', Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) '3', Unsafe.getUnsafe().getByte(buf + 1));
            Assert.assertEquals(2, GrpcTrailerWriter.encodeStatusDigits(16, buf));
            Assert.assertEquals((byte) '1', Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) '6', Unsafe.getUnsafe().getByte(buf + 1));
            Assert.assertEquals(3, GrpcTrailerWriter.encodeStatusDigits(404, buf));
            Assert.assertEquals((byte) '4', Unsafe.getUnsafe().getByte(buf));
            Assert.assertEquals((byte) '0', Unsafe.getUnsafe().getByte(buf + 1));
            Assert.assertEquals((byte) '4', Unsafe.getUnsafe().getByte(buf + 2));
        } finally {
            Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPercentEncodeAsciiPassThrough() {
        byte[] expected = "plain-ascii:foo/bar".getBytes();
        long dst = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        try {
            int len = GrpcTrailerWriter.percentEncode("plain-ascii:foo/bar", dst, 128);
            Assert.assertEquals(expected.length, len);
            for (int i = 0; i < len; i++) {
                Assert.assertEquals(expected[i], Unsafe.getUnsafe().getByte(dst + i));
            }
        } finally {
            Unsafe.free(dst, 128, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPercentEncodeEscapesNonAscii() {
        // U+00E9 (é) encodes in UTF-8 as 0xC3 0xA9 → "%C3%A9".
        long dst = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        try {
            int len = GrpcTrailerWriter.percentEncode("caf\u00E9", dst, 128);
            byte[] expected = "caf%C3%A9".getBytes();
            Assert.assertEquals(expected.length, len);
            for (int i = 0; i < len; i++) {
                Assert.assertEquals(expected[i], Unsafe.getUnsafe().getByte(dst + i));
            }
        } finally {
            Unsafe.free(dst, 128, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPercentEncodeEscapesPercent() {
        long dst = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            int len = GrpcTrailerWriter.percentEncode("50%off", dst, 32);
            byte[] expected = "50%25off".getBytes();
            Assert.assertEquals(expected.length, len);
            for (int i = 0; i < len; i++) {
                Assert.assertEquals(expected[i], Unsafe.getUnsafe().getByte(dst + i));
            }
        } finally {
            Unsafe.free(dst, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPercentEncodeOverflow() {
        long dst = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(-1, GrpcTrailerWriter.percentEncode("ABCDE", dst, 4));
        } finally {
            Unsafe.free(dst, 4, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTrailersOkNoMessageRoundTrip() {
        assertRoundTrip(GrpcStatus.OK, null, false);
    }

    @Test
    public void testTrailersOkSuppressesGivenMessage() {
        assertRoundTrip(GrpcStatus.OK, "ignored", false);
    }

    @Test
    public void testTrailersInternalWithAsciiMessage() {
        assertRoundTrip(GrpcStatus.INTERNAL, "unexpected failure", true);
    }

    @Test
    public void testTrailersUnimplementedNoMessage() {
        assertRoundTrip(GrpcStatus.UNIMPLEMENTED, null, false);
    }

    @Test
    public void testTrailersWithNonAsciiMessage() {
        assertRoundTrip(GrpcStatus.INTERNAL, "caf\u00E9 closed", true);
    }

    private void assertRoundTrip(int status, CharSequence message, boolean expectMessage) {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        HpackEncoder encoder = new HpackEncoder(4096, 4096, 4096, 8192, 32 * 1024);
        HpackDecoder decoder = new HpackDecoder(4096, 4096);
        GrpcTrailerWriter trailerWriter = GrpcTrailerWriter.newInstanceForTesting(512);
        try {
            long begin = encoder.beginBlock(buf, buf + BUF_SIZE);
            Assert.assertTrue(begin > 0);
            long end = trailerWriter.writeTrailers(encoder, begin, buf + BUF_SIZE, status, message);
            Assert.assertTrue(end > 0);
            encoder.endBlock();

            List<String[]> received = new ArrayList<>();
            decoder.decodeBlock(buf, end, (nameAddr, nameLen, valueAddr, valueLen, neverIndexed) -> {
                received.add(new String[]{
                        readBytes(nameAddr, nameLen),
                        readBytes(valueAddr, valueLen)
                });
            });

            int expectedFields = expectMessage ? 2 : 1;
            Assert.assertEquals(expectedFields, received.size());
            Assert.assertEquals("grpc-status", received.get(0)[0]);
            Assert.assertEquals(Integer.toString(status), received.get(0)[1]);
            if (expectMessage) {
                Assert.assertEquals("grpc-message", received.get(1)[0]);
                // Percent-encoding is deterministic; round-trip by applying the same
                // encoding to the expected input.
                long scratch = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
                try {
                    int len = GrpcTrailerWriter.percentEncode(message, scratch, 512);
                    String expected = readBytes(scratch, len);
                    Assert.assertEquals(expected, received.get(1)[1]);
                } finally {
                    Unsafe.free(scratch, 512, MemoryTag.NATIVE_DEFAULT);
                }
            }
        } finally {
            trailerWriter.close();
            encoder.close();
            decoder.close();
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static String readBytes(long addr, int len) {
        byte[] arr = new byte[len];
        for (int i = 0; i < len; i++) {
            arr[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return new String(arr);
    }
}
