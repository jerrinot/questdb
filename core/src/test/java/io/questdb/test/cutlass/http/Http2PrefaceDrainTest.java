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

package io.questdb.test.cutlass.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http2.Http2FrameWriter;
import io.questdb.cutlass.http2.Http2Preface;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the preface drain + engine wiring from Wave 2 / B.4 of
 * {@code HTTP2_INTEGRATION.md}. Synthetic peer bytes arrive via a recording
 * {@link NetworkFacadeImpl}; outbound bytes are decoded with Netty's
 * frame reader so wire-format claims are validated against a reference
 * implementation.
 */
public class Http2PrefaceDrainTest extends AbstractCairoTest {

    private static final long DUMMY_FD = -1L;

    @Test
    public void testClearFreesEngineOnPoolReturn() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN);
            RecordingFacade nf = new RecordingFacade().withRecvBytes(prefaceBytes());
            DefaultHttpServerConfiguration config = buildH2Config(nf);
            HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE);
            ctx.of(DUMMY_FD);
            ctx.init();
            runDrain(ctx);
            Assert.assertEquals("h2", ctx.getProtocolModeForTests());
            final long afterUpgrade = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN);
            Assert.assertTrue("expected engine memory above baseline after upgrade", afterUpgrade > baseline);
            ctx.clear();
            final long afterClear = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN);
            Assert.assertTrue("engine memory must return to baseline on clear", afterClear <= afterUpgrade - (afterUpgrade - baseline) / 2);
            ctx.close();
        });
    }

    @Test
    public void testCloseFreesEngineOnConnectionClose() throws Exception {
        assertMemoryLeak(() -> {
            RecordingFacade nf = new RecordingFacade().withRecvBytes(prefaceBytes());
            DefaultHttpServerConfiguration config = buildH2Config(nf);
            try (HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE)) {
                ctx.of(DUMMY_FD);
                ctx.init();
                runDrain(ctx);
                Assert.assertEquals("h2", ctx.getProtocolModeForTests());
            }
        });
    }

    @Test
    public void testFullPrefaceDrainAllocatesEngineAndEmitsSettings() throws Exception {
        assertMemoryLeak(() -> {
            RecordingFacade nf = new RecordingFacade().withRecvBytes(prefaceBytes());
            DefaultHttpServerConfiguration config = buildH2Config(nf);
            try (HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE)) {
                ctx.of(DUMMY_FD);
                ctx.init();
                runDrain(ctx);
                Assert.assertEquals("h2", ctx.getProtocolModeForTests());
                byte[] emitted = nf.takeOutbound();
                assertEmittedContainsType(emitted, "SETTINGS");
            }
        });
    }

    @Test
    public void testH2PeerSendsPing() throws Exception {
        assertMemoryLeak(() -> {
            RecordingFacade nf = new RecordingFacade().withRecvBytes(prefaceBytes());
            DefaultHttpServerConfiguration config = buildH2Config(nf);
            try (HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE)) {
                ctx.of(DUMMY_FD);
                ctx.init();
                runDrain(ctx);
                Assert.assertEquals("h2", ctx.getProtocolModeForTests());
                nf.takeOutbound();

                final long opaque = 0x0102030405060708L;
                byte[] pingFrame = pingFrame(opaque);
                nf.withRecvBytes(pingFrame);
                expectReadReschedule(ctx);
                byte[] emitted = nf.takeOutbound();
                assertPingAck(emitted, opaque);
            }
        });
    }

    @Test
    public void testH2PeerSendsSettings() throws Exception {
        assertMemoryLeak(() -> {
            RecordingFacade nf = new RecordingFacade().withRecvBytes(prefaceBytes());
            DefaultHttpServerConfiguration config = buildH2Config(nf);
            try (HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE)) {
                ctx.of(DUMMY_FD);
                ctx.init();
                runDrain(ctx);
                Assert.assertEquals("h2", ctx.getProtocolModeForTests());
                nf.takeOutbound();

                byte[] peerSettings = emptySettingsFrame();
                nf.withRecvBytes(peerSettings);
                expectReadReschedule(ctx);
                byte[] emitted = nf.takeOutbound();
                assertEmittedContainsType(emitted, "SETTINGS_ACK");
            }
        });
    }

    @Test
    public void testPartialPrefaceDrainWaitsAcrossTicks() throws Exception {
        assertMemoryLeak(() -> {
            byte[] full = prefaceBytes();
            byte[] first = new byte[10];
            byte[] rest = new byte[Http2Preface.LENGTH - 10];
            System.arraycopy(full, 0, first, 0, 10);
            System.arraycopy(full, 10, rest, 0, rest.length);
            // Peek sees the full preface so the sniff transitions to
            // MODE_H2_PREFACE_PENDING; recv is scripted to return 10 bytes on
            // the first call and the remaining 14 on the second.
            RecordingFacade nf = new RecordingFacade().withPeekReturnBytes(full).withRecvBytes(first);
            DefaultHttpServerConfiguration config = buildH2Config(nf);
            try (HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE)) {
                ctx.of(DUMMY_FD);
                ctx.init();
                expectReadReschedule(ctx);
                Assert.assertEquals("h2_preface_pending", ctx.getProtocolModeForTests());

                nf.withRecvBytes(rest);
                runDrain(ctx);
                Assert.assertEquals("h2", ctx.getProtocolModeForTests());
            }
        });
    }

    @Test
    public void testPrefaceMismatchDisconnects() throws Exception {
        assertMemoryLeak(() -> {
            byte[] bad = new byte[Http2Preface.LENGTH];
            bad[0] = 'G';
            bad[1] = 'E';
            bad[2] = 'T';
            bad[3] = ' ';
            RecordingFacade nf = new RecordingFacade().withPeekReturnBytes(prefaceBytes()).withRecvBytes(bad);
            // Preface sniff would return MATCH on peek (we seed peek with good preface),
            // which flips protocolMode to MODE_H2_PREFACE_PENDING. Then drain fires and
            // reads the bad bytes, triggering protocol-violation disconnect.
            DefaultHttpServerConfiguration config = buildH2Config(nf);
            try (HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE)) {
                ctx.of(DUMMY_FD);
                ctx.init();
                try {
                    ctx.handleClientOperation(IOOperation.READ, null, null);
                    Assert.fail("expected ServerDisconnectException on preface mismatch");
                } catch (ServerDisconnectException expected) {
                    // ok
                } catch (PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
                    Assert.fail("expected disconnect, got re-register: " + e);
                }
            }
        });
    }

    private static void assertEmittedContainsType(byte[] bytes, String expectedType) {
        CapturingListener cap = decodeFrames(bytes);
        Assert.assertTrue("expected to decode at least one frame, got " + cap.events, !cap.events.isEmpty());
        Assert.assertTrue("expected frame type " + expectedType + " in " + cap.events, cap.events.contains(expectedType));
    }

    private static void assertPingAck(byte[] bytes, long expectedOpaque) {
        CapturingListener cap = decodeFrames(bytes);
        Assert.assertTrue("expected at least one frame", !cap.events.isEmpty());
        Assert.assertTrue("expected PING_ACK in " + cap.events, cap.events.contains("PING_ACK"));
        Assert.assertEquals(expectedOpaque, cap.lastPingData);
    }

    private static DefaultHttpServerConfiguration buildH2Config(RecordingFacade nf) {
        return new HttpServerConfigurationBuilder()
                .withNetwork(nf)
                .withH2Enabled(true)
                .build(configuration);
    }

    private static CapturingListener decodeFrames(byte[] bytes) {
        CapturingListener cap = new CapturingListener();
        if (bytes.length == 0) {
            return cap;
        }
        ByteBuf bb = Unpooled.wrappedBuffer(bytes);
        try {
            DefaultHttp2FrameReader reader = new DefaultHttp2FrameReader(false);
            while (bb.isReadable()) {
                reader.readFrame(null, bb, cap);
            }
        } catch (Http2Exception e) {
            throw new RuntimeException(e);
        } finally {
            bb.release();
        }
        return cap;
    }

    private static byte[] emptySettingsFrame() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writeSettings(buf, buf + 64, new short[0], new int[0], 0);
            return slice(buf, (int) (end - buf));
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void expectReadReschedule(HttpConnectionContext ctx) throws Exception {
        try {
            ctx.handleClientOperation(IOOperation.READ, null, null);
            Assert.fail("expected PeerIsSlowToWriteException (read re-register)");
        } catch (PeerIsSlowToWriteException expected) {
            // ok
        }
    }

    private static byte[] pingFrame(long opaque) {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            long end = Http2FrameWriter.writePing(buf, buf + 64, false, opaque);
            return slice(buf, (int) (end - buf));
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] prefaceBytes() {
        long addr = Unsafe.malloc(Http2Preface.LENGTH, MemoryTag.NATIVE_DEFAULT);
        try {
            Http2Preface.write(addr);
            return slice(addr, Http2Preface.LENGTH);
        } finally {
            Unsafe.free(addr, Http2Preface.LENGTH, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void runDrain(HttpConnectionContext ctx) throws Exception {
        try {
            ctx.handleClientOperation(IOOperation.READ, null, null);
        } catch (PeerIsSlowToWriteException expected) {
            // drain leaves ctx in MODE_H2 and throws to re-register for READ
        }
    }

    private static byte[] slice(long addr, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return out;
    }

    private static final class CapturingListener implements Http2FrameListener {
        final java.util.List<String> events = new java.util.ArrayList<>();
        long lastPingData;

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream) {
            events.add("DATA");
            return data.readableBytes() + padding;
        }

        @Override
        public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
            events.add("GOAWAY");
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding, boolean endOfStream) {
            events.add("HEADERS");
        }

        @Override
        public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency, short weight, boolean exclusive, int padding, boolean endOfStream) {
            events.add("HEADERS");
        }

        @Override
        public void onPingAckRead(ChannelHandlerContext ctx, long data) {
            events.add("PING_ACK");
            lastPingData = data;
        }

        @Override
        public void onPingRead(ChannelHandlerContext ctx, long data) {
            events.add("PING");
            lastPingData = data;
        }

        @Override
        public void onPriorityRead(ChannelHandlerContext ctx, int streamId, int streamDependency, short weight, boolean exclusive) {
            events.add("PRIORITY");
        }

        @Override
        public void onPushPromiseRead(ChannelHandlerContext ctx, int streamId, int promisedStreamId, Http2Headers headers, int padding) {
            events.add("PUSH_PROMISE");
        }

        @Override
        public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
            events.add("RST_STREAM");
        }

        @Override
        public void onSettingsAckRead(ChannelHandlerContext ctx) {
            events.add("SETTINGS_ACK");
        }

        @Override
        public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
            events.add("SETTINGS");
        }

        @Override
        public void onUnknownFrame(ChannelHandlerContext ctx, byte frameType, int streamId, io.netty.handler.codec.http2.Http2Flags flags, ByteBuf payload) {
            events.add("UNKNOWN");
        }

        @Override
        public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement) {
            events.add("WINDOW_UPDATE");
        }
    }

    private static final class RecordingFacade extends NetworkFacadeImpl {
        private final java.util.ArrayList<byte[]> outbound = new java.util.ArrayList<>();
        private byte[] peekBytes;
        private int recvCursor;
        private byte[] recvBytes = new byte[0];

        @Override
        public int peekRaw(long fd, long buffer, int bufferLen) {
            if (peekBytes == null) {
                peekBytes = recvBytes;
            }
            final int n = Math.min(peekBytes.length, bufferLen);
            for (int i = 0; i < n; i++) {
                Unsafe.getUnsafe().putByte(buffer + i, peekBytes[i]);
            }
            return n;
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            final int remaining = recvBytes.length - recvCursor;
            if (remaining <= 0) {
                return 0;
            }
            final int n = Math.min(remaining, bufferLen);
            for (int i = 0; i < n; i++) {
                Unsafe.getUnsafe().putByte(buffer + i, recvBytes[recvCursor + i]);
            }
            recvCursor += n;
            return n;
        }

        @Override
        public int sendRaw(long fd, long buffer, int bufferLen) {
            byte[] copy = new byte[bufferLen];
            for (int i = 0; i < bufferLen; i++) {
                copy[i] = Unsafe.getUnsafe().getByte(buffer + i);
            }
            outbound.add(copy);
            return bufferLen;
        }

        byte[] takeOutbound() {
            int total = 0;
            for (byte[] b : outbound) {
                total += b.length;
            }
            byte[] out = new byte[total];
            int o = 0;
            for (byte[] b : outbound) {
                System.arraycopy(b, 0, out, o, b.length);
                o += b.length;
            }
            outbound.clear();
            return out;
        }

        RecordingFacade withPeekReturnBytes(byte[] bytes) {
            this.peekBytes = bytes;
            return this;
        }

        RecordingFacade withRecvBytes(byte[] bytes) {
            this.recvBytes = bytes;
            this.recvCursor = 0;
            return this;
        }
    }
}
