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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2ConnectionConfig;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2FrameHeader;
import io.questdb.cutlass.http2.Http2FrameReader;
import io.questdb.cutlass.http2.Http2FrameType;
import io.questdb.cutlass.http2.Http2HeadersWriter;
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2Settings;
import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.cutlass.http2.Http2StreamPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Regression coverage for the PR2/PR3 encoder-rollback hot-patch: when
 * {@code emitResponseHeaders} returns non-{@code OK}, the HPACK encoder's
 * queued size updates, {@code blockOpen} flag, and dynamic-table pointers
 * are rolled back to match the last committed block. Without this, a
 * PARK'd emit would drop the Milestone-1 pin-to-0 size update (or, in
 * M2, an incremental-indexing admission) and the peer's decoder would
 * observe an inconsistent dynamic table on the next successful block.
 * <p>
 * The test feeds all emitted HEADERS + CONTINUATION bytes — across a
 * successful emit, a PARK'd retry, and the eventual successful retry —
 * through Netty's {@link DefaultHttp2HeadersDecoder}. Netty's decoder is
 * the peer-simulation oracle; any HpackException (or decoded header
 * drift) proves the rollback is incorrect.
 */
public class Http2EncoderRollbackTest {

    // :status: 200 is static-table index 8 (RFC 7541 Appendix A). HEADERS
    // frame payload for an indexed lookup fits in one byte post-beginBlock
    // queued-update prefix.
    private static final int STATUS_200 = HpackEncoder.HINT_STATIC_INDEX | 8;

    @Test
    public void testHeadersRoundTripAfterParkedEmitRetry() throws Http2Exception {
        // Tight arena: 64 bytes per stream. First emit of tiny headers
        // fits (~6-10 bytes with the pin-to-0 prefix + :status index).
        // After filling the rest of the arena with a DATA tuple, a
        // second emit parks (cap overrun). Draining the scheduler
        // returns arena headroom; the retry of the exact same writer
        // must succeed and the peer's decoder must round-trip cleanly
        // across every emission.
        Http2ConnectionConfig cfg = Http2ConnectionConfig.newBuilder()
                .withOutboundArenaBytesPerStream(64)
                .withOutboundTupleQueueCap(4)
                .build();

        // Every header line in this fixed set is either static-indexed
        // or literal-with-static-name; decoding via Netty round-trips
        // the exact name/value pairs.
        String[][] expected = new String[][]{
                {":status", "200"},
                {"content-type", "application/json"},
        };

        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        CapturingListener listener = new CapturingListener();
        Http2ConnectionContext ctx = new Http2ConnectionContext(listener, cfg);
        long nameScratch = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
        long valueScratch = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
        long filler = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        long send = Unsafe.malloc(16 * 1024, MemoryTag.NATIVE_DEFAULT);

        List<byte[]> emittedHeaderBlocks = new ArrayList<>();
        try {
            Http2StreamPool pool = ctx.getStreamPool();
            int slot = pool.allocateLive(1, 65_535, 65_535);
            Http2Stream s = pool.getLiveStream(slot);
            Assert.assertTrue(s.onRecvHeaders(false));
            int gen = s.getGeneration();

            Http2HeadersWriter writer = new StableHeadersWriter(expected, nameScratch, valueScratch);

            // 1) First emit: succeeds. Arena now has roughly ~12 bytes
            //    of encoded HEADERS payload queued.
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.emitResponseHeaders(1, gen, writer, false));
            int headersAfterFirst = s.getOutboundTupleCount();
            int queuedAfterFirst = s.getOutboundQueuedPayloadBytes();
            Assert.assertTrue(queuedAfterFirst > 0);

            // 2) Fill the rest of the arena with a DATA tuple so the
            //    next emit would overrun.
            int remaining = s.getOutboundArenaCap() - queuedAfterFirst;
            Assert.assertTrue("test fixture requires some arena left to fill", remaining > 0);
            for (int i = 0; i < remaining; i++) {
                Unsafe.getUnsafe().putByte(filler + i, (byte) i);
            }
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.enqueueData(1, gen, filler, remaining, false));
            Assert.assertEquals(s.getOutboundArenaCap(), s.getOutboundQueuedPayloadBytes());

            // 3) Second emit attempts the same headers again. Cap is
            //    full → PARK. Crucially, the HPACK encoder's state must
            //    be rolled back so the next successful retry encodes an
            //    identical block (same queued size-update prefix, same
            //    dynamic-table pointers). Without rollback the queued
            //    pin-to-0 would have been consumed by the first emit on
            //    this connection (the first beginBlock drains it); this
            //    PARK'd attempt has nothing queued to drop, so the
            //    rollback is specifically guarding against future M2
            //    admissions and the block-open bit. It must still be
            //    invariant-preserving today.
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_PARK,
                    ctx.emitResponseHeaders(1, gen, writer, false));
            Assert.assertTrue(s.isOutboundParked());
            Assert.assertEquals("PARK'd emit must leave tuple count unchanged",
                    headersAfterFirst + 1 /* the DATA tuple */, s.getOutboundTupleCount());

            // 4) Drain: writePending emits HEADERS + DATA frames. The
            //    drained HEADERS frames land in the send buffer; we
            //    collect them for Netty decoding.
            long cursor = ctx.writePending(send, send + 16 * 1024);
            emittedHeaderBlocks.addAll(collectHeaderBlocks(send, cursor));
            Assert.assertEquals(0, s.getOutboundTupleCount());
            Assert.assertEquals(0, s.getOutboundQueuedPayloadBytes());

            // 5) Retry emit: should succeed now that the arena is free.
            Assert.assertEquals(Http2ConnectionContext.ENQUEUE_OK,
                    ctx.emitResponseHeaders(1, gen, writer, true));
            // Drain retry's HEADERS frame.
            long cursor2 = ctx.writePending(send, send + 16 * 1024);
            emittedHeaderBlocks.addAll(collectHeaderBlocks(send, cursor2));

            // 6) Feed every emitted header block through Netty's
            //    decoder. Netty tracks dynamic-table state across
            //    decodeHeaders calls, so a rollback bug would manifest
            //    as an HpackException on the second decode.
            Assert.assertFalse("expected at least one HEADERS block emitted",
                    emittedHeaderBlocks.isEmpty());
            DefaultHttp2HeadersDecoder nettyDec = new DefaultHttp2HeadersDecoder(true);
            for (int i = 0; i < emittedHeaderBlocks.size(); i++) {
                byte[] block = emittedHeaderBlocks.get(i);
                ByteBuf bb = Unpooled.wrappedBuffer(block);
                try {
                    Http2Headers decoded = nettyDec.decodeHeaders(1 + 2 * i, bb);
                    assertHeadersEqual(expected, decoded);
                } finally {
                    bb.release();
                }
            }
        } finally {
            Unsafe.free(send, 16 * 1024, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(filler, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueScratch, 256, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameScratch, 256, MemoryTag.NATIVE_DEFAULT);
            ctx.close();
        }
        Assert.assertEquals("native memory leak across the full test cycle",
                before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
    }

    private static void assertHeadersEqual(String[][] expected, Http2Headers actual) {
        Map<String, String> got = new HashMap<>();
        for (Map.Entry<CharSequence, CharSequence> e : actual) {
            got.put(e.getKey().toString(), e.getValue().toString());
        }
        for (String[] kv : expected) {
            Assert.assertEquals("header " + kv[0] + " mismatch", kv[1], got.get(kv[0]));
        }
        Assert.assertEquals("extra or missing headers after decode",
                expected.length, got.size());
    }

    /**
     * Reads every HEADERS frame between {@code [start, end)} and returns
     * the concatenated payload bytes per HEADERS-plus-CONTINUATION
     * sequence. For this test the writer always fits in a single HEADERS
     * frame (no CONTINUATION), so each HEADERS frame becomes one block.
     */
    private static List<byte[]> collectHeaderBlocks(long start, long end) {
        List<byte[]> blocks = new ArrayList<>();
        Http2FrameReader reader = new Http2FrameReader();
        Http2FrameHeader header = new Http2FrameHeader();
        long cursor = start;
        byte[] pending = null;
        while (cursor < end) {
            int n = reader.tryReadNext(cursor, end, header, Http2Settings.MAX_FRAME_SIZE_UPPER);
            if (n == 0) {
                break;
            }
            if (header.getType() == Http2FrameType.HEADERS
                    || header.getType() == Http2FrameType.CONTINUATION) {
                int payloadLen = header.getPayloadLength();
                byte[] chunk = new byte[payloadLen];
                for (int i = 0; i < payloadLen; i++) {
                    chunk[i] = Unsafe.getUnsafe().getByte(header.getPayloadAddr() + i);
                }
                pending = pending == null ? chunk : concat(pending, chunk);
                if ((header.getFlags() & 0x04 /* END_HEADERS */) != 0) {
                    blocks.add(pending);
                    pending = null;
                }
            }
            cursor += n;
        }
        return blocks;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static final class CapturingListener implements Http2StreamListener {
        @Override
        public boolean onData(int streamId, long addr, int dataLen, boolean endStream, int generationToken) {
            return true;
        }

        @Override
        public void onRequestHeader(int streamId, long nameAddr, int nameLen,
                                    long valueAddr, int valueLen, boolean neverIndexed) {
        }

        @Override
        public void onRequestHeaders(int streamId, Http2RequestHeadersView view, boolean endStream) {
        }

        @Override
        public void onStreamClosed(int streamId, int cause) {
        }

        @Override
        public void onStreamWritable(int streamId) {
        }

        @Override
        public void onTrailers(int streamId, boolean endStream) {
        }
    }

    /**
     * Writer that emits a fixed list of {@code (name, value)} pairs
     * every time it is invoked. Uses the static table for {@code :status}
     * and a literal-with-static-name form for other fields.
     */
    private static final class StableHeadersWriter implements Http2HeadersWriter {
        private final String[][] headers;
        private final long nameScratch;
        private final long valueScratch;

        StableHeadersWriter(String[][] headers, long nameScratch, long valueScratch) {
            this.headers = headers;
            this.nameScratch = nameScratch;
            this.valueScratch = valueScratch;
        }

        @Override
        public long write(HpackEncoder encoder, long cursor, long limit) {
            for (String[] kv : headers) {
                if (":status".equals(kv[0]) && "200".equals(kv[1])) {
                    cursor = encoder.encode(cursor, limit, 0L, 0, 0L, 0, STATUS_200);
                    if (cursor < 0) {
                        return cursor;
                    }
                    continue;
                }
                byte[] name = kv[0].getBytes();
                byte[] value = kv[1].getBytes();
                for (int i = 0; i < name.length; i++) {
                    Unsafe.getUnsafe().putByte(nameScratch + i, name[i]);
                }
                for (int i = 0; i < value.length; i++) {
                    Unsafe.getUnsafe().putByte(valueScratch + i, value[i]);
                }
                int hint = HpackEncoder.HINT_NONE;
                // Recognised static-name-only fields use HINT_STATIC_NAME.
                // content-type sits at static index 31 (RFC 7541 Appendix A).
                if ("content-type".equals(kv[0])) {
                    hint = HpackEncoder.HINT_STATIC_NAME | 31;
                }
                cursor = encoder.encode(cursor, limit,
                        nameScratch, name.length,
                        valueScratch, value.length,
                        hint);
                if (cursor < 0) {
                    return cursor;
                }
            }
            return cursor;
        }
    }
}
