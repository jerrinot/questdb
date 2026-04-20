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

package io.questdb.test.cutlass.http2;

import io.questdb.cutlass.http2.Http2ConnectionException;
import io.questdb.cutlass.http2.Http2ErrorCode;
import io.questdb.cutlass.http2.Http2Flags;
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameReader;
import io.questdb.cutlass.http2.Http2FrameType;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.http2.Http2Preface;
import io.questdb.cutlass.http2.Http2Settings;
import io.questdb.cutlass.http2.Http2StreamException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class Http2FrameCodecTest {

    private static final int BUF_SIZE = 64 * 1024;
    private static final int INBOUND_MAX = Http2Settings.MAX_FRAME_SIZE_LOWER;

    @Test
    public void testContinuationRoundtripAfterOpenHeaders() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long block = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 32; i++) {
                Unsafe.getUnsafe().putByte(block + i, (byte) (0x40 + i));
            }
            // Open a HEADERS block on stream 7 (no END_HEADERS), then CONTINUATION.
            long afterHeaders = Http2FrameWriter.writeHeaders(buf, buf + BUF_SIZE, 7, false, false, block, 16);
            long end = Http2FrameWriter.writeContinuation(afterHeaders, buf + BUF_SIZE, 7, true, block + 16, 16);
            Assert.assertTrue(end > 0);

            Http2FrameReader reader = new Http2FrameReader();
            Http2FrameHeader h = new Http2FrameHeader();
            int n = reader.tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(Http2FrameType.HEADERS, h.getType());
            int n2 = reader.tryReadNext(buf + n, end, h, INBOUND_MAX);
            Assert.assertEquals(Http2FrameType.CONTINUATION, h.getType());
            Assert.assertEquals(7, h.getStreamId());
            Assert.assertTrue(Http2Flags.hasEndHeaders(h.getFlags()));
            Assert.assertEquals(16, h.getPayloadLength());
            Assert.assertEquals(end - buf, (long) n + n2);
        } finally {
            Unsafe.free(block, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testDataRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 128; i++) {
                Unsafe.getUnsafe().putByte(payload + i, (byte) (i + 1));
            }
            long end = Http2FrameWriter.writeData(buf, buf + BUF_SIZE, 3, true, payload, 128);
            Assert.assertTrue(end > 0);
            Assert.assertEquals(Http2FrameHeader.SIZE + 128, end - buf);

            // Wire-format byte-level check.
            Assert.assertEquals(0, Unsafe.getUnsafe().getByte(buf));         // length hi
            Assert.assertEquals(0, Unsafe.getUnsafe().getByte(buf + 1));     // length mid
            Assert.assertEquals((byte) 128, Unsafe.getUnsafe().getByte(buf + 2)); // length lo
            Assert.assertEquals(Http2FrameType.DATA, Unsafe.getUnsafe().getByte(buf + 3));
            Assert.assertEquals(Http2Flags.END_STREAM, Unsafe.getUnsafe().getByte(buf + 4));
            Assert.assertEquals(0, Unsafe.getUnsafe().getByte(buf + 5));     // stream id hi (reserved bit + 0)
            Assert.assertEquals(0, Unsafe.getUnsafe().getByte(buf + 6));
            Assert.assertEquals(0, Unsafe.getUnsafe().getByte(buf + 7));
            Assert.assertEquals(3, Unsafe.getUnsafe().getByte(buf + 8));     // stream id lo

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.DATA, h.getType());
            Assert.assertEquals(3, h.getStreamId());
            Assert.assertEquals(128, h.getPayloadLength());
            Assert.assertTrue(Http2Flags.hasEndStream(h.getFlags()));
            for (int i = 0; i < 128; i++) {
                Assert.assertEquals((byte) (i + 1), Unsafe.getUnsafe().getByte(h.getPayloadAddr() + i));
            }
        } finally {
            Unsafe.free(payload, 128, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFrameHeaderLengthEncoding() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Length 0x123456 crosses all three bytes of the 24-bit length field.
            int len = 0x123456;
            Http2FrameHeader.write(buf, len, Http2FrameType.DATA, Http2Flags.NONE, 1);
            Assert.assertEquals(0x12, Unsafe.getUnsafe().getByte(buf) & 0xFF);
            Assert.assertEquals(0x34, Unsafe.getUnsafe().getByte(buf + 1) & 0xFF);
            Assert.assertEquals(0x56, Unsafe.getUnsafe().getByte(buf + 2) & 0xFF);

            Http2FrameHeader h = new Http2FrameHeader();
            h.readFrom(buf);
            Assert.assertEquals(len, h.getPayloadLength());
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGoAwayRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long debug = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(debug + i, (byte) ('a' + i));
            }
            long end = Http2FrameWriter.writeGoAway(buf, buf + BUF_SIZE, 99, Http2ErrorCode.INTERNAL_ERROR, debug, 16);
            Assert.assertTrue(end > 0);

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.GOAWAY, h.getType());
            Assert.assertEquals(0, h.getStreamId());
            Assert.assertEquals(24, h.getPayloadLength());
            Assert.assertEquals(99, Http2FrameReader.readGoAwayLastStreamId(h.getPayloadAddr()));
            Assert.assertEquals(Http2ErrorCode.INTERNAL_ERROR, Http2FrameReader.readGoAwayErrorCode(h.getPayloadAddr()));
            for (int i = 0; i < 16; i++) {
                Assert.assertEquals((byte) ('a' + i), Unsafe.getUnsafe().getByte(h.getPayloadAddr() + 8 + i));
            }
        } finally {
            Unsafe.free(debug, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHeadersRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long block = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 40; i++) {
                Unsafe.getUnsafe().putByte(block + i, (byte) (0x80 | i));
            }
            long end = Http2FrameWriter.writeHeaders(buf, buf + BUF_SIZE, 5, false, true, block, 40);
            Assert.assertTrue(end > 0);

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.HEADERS, h.getType());
            Assert.assertEquals(5, h.getStreamId());
            Assert.assertTrue(Http2Flags.hasEndHeaders(h.getFlags()));
            Assert.assertFalse(Http2Flags.hasEndStream(h.getFlags()));
            Assert.assertFalse(Http2Flags.hasPadded(h.getFlags()));
            Assert.assertFalse(Http2Flags.hasPriority(h.getFlags()));
            Assert.assertEquals(40, h.getPayloadLength());
        } finally {
            Unsafe.free(block, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMaxStreamIdReservedBitIgnored() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Manually plant a frame header with the reserved high bit set in the
            // stream id; the reader must strip it per RFC 7540 sec. 4.1.
            Unsafe.getUnsafe().putByte(buf, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 3, Http2FrameType.PING);
            Unsafe.getUnsafe().putByte(buf + 4, Http2Flags.NONE);
            Unsafe.getUnsafe().putByte(buf + 5, (byte) 0xFF); // R=1, top 7 bits = 0x7F
            Unsafe.getUnsafe().putByte(buf + 6, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(buf + 7, (byte) 0xFF);
            Unsafe.getUnsafe().putByte(buf + 8, (byte) 0xFF);
            // PING needs 8 bytes payload; plant zeros. Also PING on stream 0 is required,
            // so this is expected to fail validation with PROTOCOL_ERROR. Read the header
            // directly to check the reserved-bit behaviour in isolation.
            Http2FrameHeader h = new Http2FrameHeader();
            h.readFrom(buf);
            Assert.assertEquals(0x7FFFFFFF, h.getStreamId());
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPingRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long opaque = 0xDEADBEEFCAFEBABEL;
            long end = Http2FrameWriter.writePing(buf, buf + BUF_SIZE, false, opaque);
            Assert.assertEquals(Http2FrameHeader.SIZE + 8, end - buf);

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.PING, h.getType());
            Assert.assertEquals(0, h.getStreamId());
            Assert.assertEquals(8, h.getPayloadLength());
            Assert.assertFalse(Http2Flags.hasAck(h.getFlags()));

            // Verify big-endian encoding of the 8 opaque bytes.
            Assert.assertEquals((byte) 0xDE, Unsafe.getUnsafe().getByte(h.getPayloadAddr()));
            Assert.assertEquals((byte) 0xAD, Unsafe.getUnsafe().getByte(h.getPayloadAddr() + 1));
            Assert.assertEquals((byte) 0xBE, Unsafe.getUnsafe().getByte(h.getPayloadAddr() + 7));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPingAckRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writePing(buf, buf + BUF_SIZE, true, 1L);
            Assert.assertTrue(end > 0);

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertTrue(Http2Flags.hasAck(h.getFlags()));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPrefaceDetect() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // Correct preface.
            Http2Preface.write(buf);
            Assert.assertEquals(Http2Preface.MATCH, Http2Preface.detect(buf, 24));

            // Partial match with fewer bytes.
            Assert.assertEquals(Http2Preface.INCOMPLETE, Http2Preface.detect(buf, 10));
            Assert.assertEquals(Http2Preface.INCOMPLETE, Http2Preface.detect(buf, 0));

            // Over-long buffer — still matches on first 24.
            Unsafe.getUnsafe().putByte(buf + 24, (byte) 'X');
            Assert.assertEquals(Http2Preface.MATCH, Http2Preface.detect(buf, 25));

            // Flip a byte — now no match.
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 'Z');
            Assert.assertEquals(Http2Preface.NO_MATCH, Http2Preface.detect(buf, 24));

            // HTTP/1.1 verbs — GET, POST — must not match.
            Unsafe.getUnsafe().putByte(buf, (byte) 'G');
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 'E');
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 'T');
            Unsafe.getUnsafe().putByte(buf + 3, (byte) ' ');
            Assert.assertEquals(Http2Preface.NO_MATCH, Http2Preface.detect(buf, 4));
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReaderReturnsZeroOnIncompleteHeader() {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2FrameHeader h = new Http2FrameHeader();
            for (int available = 0; available < Http2FrameHeader.SIZE; available++) {
                Assert.assertEquals("available=" + available, 0,
                        new Http2FrameReader().tryReadNext(buf, buf + available, h, INBOUND_MAX));
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testReaderReturnsZeroOnIncompletePayload() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // Write a DATA frame with 20-byte payload, but hand the reader only
            // the header + 5 payload bytes.
            long end = Http2FrameWriter.writeData(buf, buf + 64, 1, false, buf + 64 - 20, 20);
            Assert.assertTrue(end > 0);
            Http2FrameHeader h = new Http2FrameHeader();
            Assert.assertEquals(0, new Http2FrameReader().tryReadNext(buf, buf + Http2FrameHeader.SIZE + 5, h, INBOUND_MAX));
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsDataOnStream0() {
        assertRejectedConnection(Http2FrameType.DATA, Http2Flags.NONE, 0, 0, Http2ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    public void testRejectsFrameTooLarge() {
        long buf = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
        try {
            // Plant a header advertising a 17 KiB payload, exceeding the 16 KiB inbound bound.
            Http2FrameHeader.write(buf, 17 * 1024, Http2FrameType.DATA, Http2Flags.NONE, 1);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 256, h, INBOUND_MAX);
                Assert.fail("expected FRAME_SIZE_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.FRAME_SIZE_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, 256, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsGoAwayOnNonZeroStream() {
        assertRejectedConnection(Http2FrameType.GOAWAY, Http2Flags.NONE, 8, 1, Http2ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    public void testRejectsGoAwayShort() {
        assertRejectedConnection(Http2FrameType.GOAWAY, Http2Flags.NONE, 7, 0, Http2ErrorCode.FRAME_SIZE_ERROR);
    }

    @Test
    public void testRejectsHeadersOnStream0() {
        assertRejectedConnection(Http2FrameType.HEADERS, Http2Flags.NONE, 0, 0, Http2ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    public void testRejectsPingNonZeroStream() {
        assertRejectedConnection(Http2FrameType.PING, Http2Flags.NONE, 8, 1, Http2ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    public void testRejectsPingWrongLength() {
        assertRejectedConnection(Http2FrameType.PING, Http2Flags.NONE, 7, 0, Http2ErrorCode.FRAME_SIZE_ERROR);
    }

    @Test
    public void testRejectsPriorityWrongLengthAsStream() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2FrameHeader.write(buf, 4, Http2FrameType.PRIORITY, Http2Flags.NONE, 3);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected stream-level FRAME_SIZE_ERROR");
            } catch (Http2StreamException e) {
                Assert.assertEquals(Http2ErrorCode.FRAME_SIZE_ERROR, e.getErrorCode());
                Assert.assertEquals(3, e.getStreamId());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsPushPromise() {
        assertRejectedConnection(Http2FrameType.PUSH_PROMISE, Http2Flags.NONE, 4, 1, Http2ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    public void testRejectsRstStreamOnStream0() {
        assertRejectedConnection(Http2FrameType.RST_STREAM, Http2Flags.NONE, 4, 0, Http2ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    public void testRejectsRstStreamWrongLength() {
        assertRejectedConnection(Http2FrameType.RST_STREAM, Http2Flags.NONE, 3, 1, Http2ErrorCode.FRAME_SIZE_ERROR);
    }

    @Test
    public void testRejectsSettingsAckWithPayload() {
        assertRejectedConnection(Http2FrameType.SETTINGS, Http2Flags.ACK, 6, 0, Http2ErrorCode.FRAME_SIZE_ERROR);
    }

    @Test
    public void testRejectsSettingsLengthNotMultipleOf6() {
        assertRejectedConnection(Http2FrameType.SETTINGS, Http2Flags.NONE, 7, 0, Http2ErrorCode.FRAME_SIZE_ERROR);
    }

    @Test
    public void testRejectsSettingsOnNonZeroStream() {
        assertRejectedConnection(Http2FrameType.SETTINGS, Http2Flags.NONE, 0, 1, Http2ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    public void testRejectsWindowUpdateWrongLength() {
        assertRejectedConnection(Http2FrameType.WINDOW_UPDATE, Http2Flags.NONE, 3, 1, Http2ErrorCode.FRAME_SIZE_ERROR);
    }

    @Test
    public void testRoundtripFuzz() {
        Rnd rnd = new Rnd();
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2FrameHeader h = new Http2FrameHeader();
            for (int iter = 0; iter < 1000; iter++) {
                int streamId = 1 + (rnd.nextInt() & 0x7FFFFFFE);
                int len = rnd.nextInt(4097);
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(payload + i, (byte) rnd.nextInt());
                }
                boolean endStream = rnd.nextBoolean();

                long end = Http2FrameWriter.writeData(buf, buf + BUF_SIZE, streamId, endStream, payload, len);
                Assert.assertTrue("iter=" + iter, end > 0);

                int n = new Http2FrameReader().tryReadNext(buf, end, h, Http2Settings.MAX_FRAME_SIZE_UPPER);
                Assert.assertEquals(end - buf, n);
                Assert.assertEquals(Http2FrameType.DATA, h.getType());
                Assert.assertEquals(streamId, h.getStreamId());
                Assert.assertEquals(len, h.getPayloadLength());
                Assert.assertEquals(endStream, Http2Flags.hasEndStream(h.getFlags()));
                for (int i = 0; i < len; i++) {
                    Assert.assertEquals("iter=" + iter + " i=" + i,
                            Unsafe.getUnsafe().getByte(payload + i),
                            Unsafe.getUnsafe().getByte(h.getPayloadAddr() + i));
                }
            }
        } finally {
            Unsafe.free(payload, 4096, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRstStreamRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeRstStream(buf, buf + BUF_SIZE, 1, Http2ErrorCode.CANCEL);
            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.RST_STREAM, h.getType());
            Assert.assertEquals(4, h.getPayloadLength());
            Assert.assertEquals(Http2ErrorCode.CANCEL, Http2FrameReader.readRstStreamErrorCode(h.getPayloadAddr()));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSettingsAckRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeSettingsAck(buf, buf + BUF_SIZE);
            Assert.assertEquals(Http2FrameHeader.SIZE, end - buf);

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.SETTINGS, h.getType());
            Assert.assertTrue(Http2Flags.hasAck(h.getFlags()));
            Assert.assertEquals(0, h.getPayloadLength());
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSettingsRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            short[] ids = {
                    Http2Settings.MAX_FRAME_SIZE,
                    Http2Settings.INITIAL_WINDOW_SIZE,
                    Http2Settings.MAX_CONCURRENT_STREAMS
            };
            int[] values = {65536, 1_048_576, 256};
            long end = Http2FrameWriter.writeSettings(buf, buf + BUF_SIZE, ids, values, 3);
            Assert.assertTrue(end > 0);

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.SETTINGS, h.getType());
            Assert.assertFalse(Http2Flags.hasAck(h.getFlags()));
            Assert.assertEquals(18, h.getPayloadLength());

            long p = h.getPayloadAddr();
            Assert.assertEquals(Http2Settings.MAX_FRAME_SIZE, Http2FrameReader.readSettingsId(p));
            Assert.assertEquals(65536, Http2FrameReader.readSettingsValue(p));
            Assert.assertEquals(Http2Settings.INITIAL_WINDOW_SIZE, Http2FrameReader.readSettingsId(p + 6));
            Assert.assertEquals(1_048_576, Http2FrameReader.readSettingsValue(p + 6));
            Assert.assertEquals(Http2Settings.MAX_CONCURRENT_STREAMS, Http2FrameReader.readSettingsId(p + 12));
            Assert.assertEquals(256, Http2FrameReader.readSettingsValue(p + 12));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSettingsValidate() {
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.MAX_FRAME_SIZE, 16384));
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.MAX_FRAME_SIZE, 16_777_215));
        Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR,
                Http2Settings.validate(Http2Settings.MAX_FRAME_SIZE, 16383));
        Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR,
                Http2Settings.validate(Http2Settings.MAX_FRAME_SIZE, 16_777_216));
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.ENABLE_PUSH, 0));
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.ENABLE_PUSH, 1));
        Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR,
                Http2Settings.validate(Http2Settings.ENABLE_PUSH, 2));
        Assert.assertEquals(Http2ErrorCode.FLOW_CONTROL_ERROR,
                Http2Settings.validate(Http2Settings.INITIAL_WINDOW_SIZE, -1));
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.INITIAL_WINDOW_SIZE, Integer.MAX_VALUE));
        // Unknown id -> ignored (NO_ERROR).
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate((short) 0x7FFF, 42));

        // Top-bit-set (unsigned) HEADER_TABLE_SIZE is syntactically legal per RFC 7540
        // sec. 6.5.2: unsigned 32-bit with no upper bound. Must pass validation rather
        // than fall through as a negative int.
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.HEADER_TABLE_SIZE, 0x80000000L));
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.HEADER_TABLE_SIZE, 0xFFFFFFFFL));
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.MAX_CONCURRENT_STREAMS, 0xFFFFFFFFL));
        Assert.assertEquals(Http2ErrorCode.NO_ERROR,
                Http2Settings.validate(Http2Settings.MAX_HEADER_LIST_SIZE, 0xFFFFFFFFL));
    }

    @Test
    public void testReadSettingsValueReturnsUnsignedLong() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Build a SETTINGS frame with HEADER_TABLE_SIZE = 0x80000000 (unsigned).
            short[] ids = {Http2Settings.HEADER_TABLE_SIZE};
            int[] values = {0x80000000};  // raw int bits, negative when signed
            long end = Http2FrameWriter.writeSettings(buf, buf + BUF_SIZE, ids, values, 1);
            Assert.assertTrue(end > 0);

            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.SETTINGS, h.getType());

            // Reader must return the positive unsigned value, not a negative int.
            long value = Http2FrameReader.readSettingsValue(h.getPayloadAddr());
            Assert.assertEquals(0x80000000L, value);
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWindowUpdateRoundtrip() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeWindowUpdate(buf, buf + BUF_SIZE, 11, 1_048_576);
            Http2FrameHeader h = new Http2FrameHeader();
            int n = new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(end - buf, n);
            Assert.assertEquals(Http2FrameType.WINDOW_UPDATE, h.getType());
            Assert.assertEquals(11, h.getStreamId());
            Assert.assertEquals(4, h.getPayloadLength());
            Assert.assertEquals(1_048_576, Http2FrameReader.readWindowUpdateIncrement(h.getPayloadAddr()));
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsContinuationMismatchedStream() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long afterHeaders = Http2FrameWriter.writeHeaders(buf, buf + BUF_SIZE, 3, false, false, 0, 0);
            long end = Http2FrameWriter.writeContinuation(afterHeaders, buf + BUF_SIZE, 5, true, 0, 0);
            Http2FrameReader reader = new Http2FrameReader();
            Http2FrameHeader h = new Http2FrameHeader();
            int n = reader.tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(Http2FrameType.HEADERS, h.getType());
            try {
                reader.tryReadNext(buf + n, end, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsContinuationWithoutOpenHeaders() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeContinuation(buf, buf + BUF_SIZE, 3, true, 0, 0);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, end, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testContinuationSequenceBeatsFrameSizeError() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            // Open a HEADERS block without END_HEADERS on stream 3.
            long afterHeaders = Http2FrameWriter.writeHeaders(buf, buf + BUF_SIZE, 3, false, false, 0, 0);
            // Then plant a DATA frame claiming an oversized payload: this breaks
            // both rules (sequence + max size), and the sequence error must win.
            int oversize = INBOUND_MAX + 1;
            Http2FrameHeader.write(afterHeaders, oversize, Http2FrameType.DATA, Http2Flags.NONE, 3);

            Http2FrameReader reader = new Http2FrameReader();
            Http2FrameHeader h = new Http2FrameHeader();
            int n = reader.tryReadNext(buf, buf + BUF_SIZE, h, INBOUND_MAX);
            Assert.assertEquals(Http2FrameType.HEADERS, h.getType());
            try {
                reader.tryReadNext(buf + n, buf + BUF_SIZE, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR (continuation sequence)");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHttp2FrameHeaderWriteRejectsNegativeStreamId() {
        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            try {
                Http2FrameHeader.write(buf, 0, Http2FrameType.DATA, Http2Flags.NONE, -1);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
            try {
                Http2FrameHeader.write(buf, 0, Http2FrameType.DATA, Http2Flags.NONE, Integer.MIN_VALUE);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
        } finally {
            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsDataAfterOpenHeaders() {
        long buf = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            long afterHeaders = Http2FrameWriter.writeHeaders(buf, buf + BUF_SIZE, 3, false, false, 0, 0);
            long end = Http2FrameWriter.writeData(afterHeaders, buf + BUF_SIZE, 3, false, payload, 4);
            Http2FrameReader reader = new Http2FrameReader();
            Http2FrameHeader h = new Http2FrameHeader();
            int n = reader.tryReadNext(buf, end, h, INBOUND_MAX);
            Assert.assertEquals(Http2FrameType.HEADERS, h.getType());
            try {
                reader.tryReadNext(buf + n, end, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsDataPadLengthOverflow() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // 4-byte payload, pad-length byte claims 10 bytes of padding.
            Http2FrameHeader.write(buf, 4, Http2FrameType.DATA, Http2Flags.PADDED, 1);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE, (byte) 10);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 3, (byte) 0);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsHeadersPadLengthOverflow() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // 3-byte payload (pad-length byte + 2 block bytes); pad-length says 5.
            Http2FrameHeader.write(buf, 3, Http2FrameType.HEADERS, Http2Flags.PADDED, 1);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE, (byte) 5);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 2, (byte) 0);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsSettingsBadMaxFrameSize() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // Single SETTINGS entry with MAX_FRAME_SIZE below the 16384 floor.
            Http2FrameHeader.write(buf, 6, Http2FrameType.SETTINGS, Http2Flags.NONE, 0);
            long p = buf + Http2FrameHeader.SIZE;
            Unsafe.getUnsafe().putByte(p, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) Http2Settings.MAX_FRAME_SIZE);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 3, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 4, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 5, (byte) 100);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsHeadersPaddedPrioritySelfDependency() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // HEADERS on stream 11 with PADDED+PRIORITY, priority dependency = 11.
            byte flags = (byte) (Http2Flags.PADDED | Http2Flags.PRIORITY | Http2Flags.END_HEADERS);
            Http2FrameHeader.write(buf, 6, Http2FrameType.HEADERS, flags, 11);
            long p = buf + Http2FrameHeader.SIZE;
            Unsafe.getUnsafe().putByte(p, (byte) 0);          // pad length
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0);      // E bit + dep hi
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 3, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 4, (byte) 11);     // dep = 11
            Unsafe.getUnsafe().putByte(p + 5, (byte) 16);     // weight
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected stream-level PROTOCOL_ERROR");
            } catch (Http2StreamException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
                Assert.assertEquals(11, e.getStreamId());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsHeadersPrioritySelfDependency() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // HEADERS on stream 7 with PRIORITY flag pointing at itself.
            byte flags = (byte) (Http2Flags.PRIORITY | Http2Flags.END_HEADERS);
            Http2FrameHeader.write(buf, 5, Http2FrameType.HEADERS, flags, 7);
            long p = buf + Http2FrameHeader.SIZE;
            Unsafe.getUnsafe().putByte(p, (byte) 0);      // E bit + dep high byte
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 3, (byte) 7);  // dep = 7
            Unsafe.getUnsafe().putByte(p + 4, (byte) 16); // weight
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected stream-level PROTOCOL_ERROR");
            } catch (Http2StreamException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
                Assert.assertEquals(7, e.getStreamId());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsPrioritySelfDependency() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // PRIORITY on stream 3 depending on stream 3 (E=0, dependency=3, weight=16).
            Http2FrameHeader.write(buf, 5, Http2FrameType.PRIORITY, Http2Flags.NONE, 3);
            long p = buf + Http2FrameHeader.SIZE;
            Unsafe.getUnsafe().putByte(p, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(p + 3, (byte) 3);
            Unsafe.getUnsafe().putByte(p + 4, (byte) 16);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected stream-level PROTOCOL_ERROR");
            } catch (Http2StreamException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
                Assert.assertEquals(3, e.getStreamId());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsWindowUpdateZeroIncrementConnection() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2FrameHeader.write(buf, 4, Http2FrameType.WINDOW_UPDATE, Http2Flags.NONE, 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 3, (byte) 0);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected PROTOCOL_ERROR");
            } catch (Http2ConnectionException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsWindowUpdateZeroIncrementStream() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2FrameHeader.write(buf, 4, Http2FrameType.WINDOW_UPDATE, Http2Flags.NONE, 5);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + 3, (byte) 0);
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 64, h, INBOUND_MAX);
                Assert.fail("expected stream-level PROTOCOL_ERROR");
            } catch (Http2StreamException e) {
                Assert.assertEquals(Http2ErrorCode.PROTOCOL_ERROR, e.getErrorCode());
                Assert.assertEquals(5, e.getStreamId());
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriterRejectsInvalidSettingsValues() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            // MAX_FRAME_SIZE below the 16384 floor -> PROTOCOL_ERROR on peer.
            try {
                Http2FrameWriter.writeSettings(buf, buf + 64,
                        new short[]{Http2Settings.MAX_FRAME_SIZE}, new int[]{100}, 1);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
            // ENABLE_PUSH outside {0, 1}.
            try {
                Http2FrameWriter.writeSettings(buf, buf + 64,
                        new short[]{Http2Settings.ENABLE_PUSH}, new int[]{2}, 1);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
            // INITIAL_WINDOW_SIZE negative -> FLOW_CONTROL_ERROR on peer.
            try {
                Http2FrameWriter.writeSettings(buf, buf + 64,
                        new short[]{Http2Settings.INITIAL_WINDOW_SIZE}, new int[]{-1}, 1);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriterRejectsInvalidStreamIds() {
        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            // Stream-scoped writers require streamId > 0.
            assertRejectsStreamZero(() -> Http2FrameWriter.writeData(buf, buf + 32, 0, false, buf, 0));
            assertRejectsStreamZero(() -> Http2FrameWriter.writeHeaders(buf, buf + 32, 0, false, true, buf, 0));
            assertRejectsStreamZero(() -> Http2FrameWriter.writeContinuation(buf, buf + 32, 0, true, buf, 0));
            assertRejectsStreamZero(() -> Http2FrameWriter.writeRstStream(buf, buf + 32, 0, Http2ErrorCode.CANCEL));
            assertRejectsStreamZero(() -> Http2FrameWriter.writeData(buf, buf + 32, -1, false, buf, 0));

            // WINDOW_UPDATE allows 0 (connection) or positive; negative is invalid.
            try {
                Http2FrameWriter.writeWindowUpdate(buf, buf + 32, -1, 1);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
            // Connection-level WINDOW_UPDATE (streamId 0) is valid.
            Assert.assertTrue(Http2FrameWriter.writeWindowUpdate(buf, buf + 32, 0, 1) > 0);

            // GOAWAY lastStreamId must be >= 0.
            try {
                Http2FrameWriter.writeGoAway(buf, buf + 32, -1, Http2ErrorCode.NO_ERROR, 0, 0);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
        } finally {
            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriterRejectsOversizedPayload() {
        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try {
            int over = Http2Settings.MAX_FRAME_SIZE_UPPER + 1;
            try {
                Http2FrameWriter.writeData(buf, buf + 32, 1, false, buf, over);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
            try {
                Http2FrameWriter.writeHeaders(buf, buf + 32, 1, false, true, buf, over);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
            try {
                Http2FrameWriter.writeContinuation(buf, buf + 32, 1, true, buf, over);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
            try {
                Http2FrameHeader.write(buf, over, Http2FrameType.DATA, Http2Flags.NONE, 1);
                Assert.fail("expected IllegalArgumentException");
            } catch (IllegalArgumentException ignore) {
            }
        } finally {
            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriterReturnsMinus1OnInsufficientSpace() {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            // DATA with 8 payload bytes needs 17 total; buffer is 16.
            Assert.assertEquals(-1L, Http2FrameWriter.writeData(buf, buf + 16, 1, false, payload, 8));
            // SETTINGS ACK needs exactly 9 bytes; 8-byte budget fails.
            Assert.assertEquals(-1L, Http2FrameWriter.writeSettingsAck(buf, buf + 8));
        } finally {
            Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertRejectsStreamZero(Runnable r) {
        try {
            r.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ignore) {
        }
    }

    private static void assertRejectedConnection(byte type, byte flags, int payloadLen, int streamId, int expectedCode) {
        long buf = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2FrameHeader.write(buf, payloadLen, type, flags, streamId);
            // Zero the payload so the reader never reads uninitialised memory when it
            // validates past the header.
            for (int i = 0; i < payloadLen; i++) {
                Unsafe.getUnsafe().putByte(buf + Http2FrameHeader.SIZE + i, (byte) 0);
            }
            Http2FrameHeader h = new Http2FrameHeader();
            try {
                new Http2FrameReader().tryReadNext(buf, buf + 256, h, INBOUND_MAX);
                Assert.fail("expected connection-level error " + Http2ErrorCode.nameOf(expectedCode));
            } catch (Http2ConnectionException e) {
                Assert.assertEquals("expected " + Http2ErrorCode.nameOf(expectedCode)
                                + " got " + Http2ErrorCode.nameOf(e.getErrorCode()),
                        expectedCode, e.getErrorCode());
            }
        } finally {
            Unsafe.free(buf, 256, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
