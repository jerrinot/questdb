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

import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http2.Http2Preface;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HTTP/2 preface sniff added to {@link HttpConnectionContext}
 * per {@code HTTP2_INTEGRATION.md} §6. Covers the six cases in the peek-result
 * table plus the config-off fast path.
 */
public class Http2PrefaceSniffTest extends AbstractCairoTest {

    private static final long DUMMY_FD = -1L;

    @Test
    public void testFullPrefaceTriggersH2() throws Exception {
        RecordingNetworkFacade nf = new RecordingNetworkFacade().withPayload(Http2Preface.LENGTH, Http2Preface.LENGTH);
        runSniff(nf, true, ctx -> {
            ctx.sniffAndSelectModeForTests();
            Assert.assertEquals("h2_preface_pending", ctx.getProtocolModeForTests());
            Assert.assertEquals(1, nf.peekRawCount);
        });
    }

    @Test
    public void testH1RequestGoesH1() throws Exception {
        byte[] h1 = "POST /exec HTTP/1.1\r\nHost: x\r\n\r\n".getBytes();
        RecordingNetworkFacade nf = new RecordingNetworkFacade().withBytes(h1);
        runSniff(nf, true, ctx -> {
            ctx.sniffAndSelectModeForTests();
            Assert.assertEquals("h1", ctx.getProtocolModeForTests());
            Assert.assertEquals(1, nf.peekRawCount);
        });
    }

    @Test
    public void testH2DisabledSkipsSniff() throws Exception {
        RecordingNetworkFacade nf = new RecordingNetworkFacade();
        runSniff(nf, false, ctx -> {
            ctx.sniffAndSelectModeForTests();
            Assert.assertEquals("h1", ctx.getProtocolModeForTests());
            Assert.assertEquals(0, nf.peekRawCount);
        });
    }

    @Test
    public void testPeekDisconnect() throws Exception {
        RecordingNetworkFacade nf = new RecordingNetworkFacade().withPeekReturn(-1);
        runSniff(nf, true, ctx -> {
            try {
                ctx.sniffAndSelectModeForTests();
                Assert.fail("expected ServerDisconnectException");
            } catch (ServerDisconnectException expected) {
                // ok
            } catch (PeerIsSlowToWriteException e) {
                Assert.fail("expected disconnect, got re-register: " + e);
            }
        });
    }

    @Test
    public void testPeekEmptyReschedulesRead() throws Exception {
        RecordingNetworkFacade nf = new RecordingNetworkFacade();
        runSniff(nf, true, ctx -> {
            try {
                ctx.sniffAndSelectModeForTests();
                Assert.fail("expected PeerIsSlowToWriteException");
            } catch (PeerIsSlowToWriteException expected) {
                // ok
            } catch (ServerDisconnectException e) {
                Assert.fail("expected re-register, got disconnect: " + e);
            }
            Assert.assertEquals("sniffing", ctx.getProtocolModeForTests());
        });
    }

    @Test
    public void testPrefacePrefixWaitsForMore() throws Exception {
        RecordingNetworkFacade nf = new RecordingNetworkFacade().withPayload(Http2Preface.LENGTH, 10);
        runSniff(nf, true, ctx -> {
            try {
                ctx.sniffAndSelectModeForTests();
                Assert.fail("expected PeerIsSlowToWriteException to re-register read");
            } catch (PeerIsSlowToWriteException expected) {
                // ok
            } catch (ServerDisconnectException e) {
                Assert.fail("expected re-register, got disconnect: " + e);
            }
            Assert.assertEquals("sniffing", ctx.getProtocolModeForTests());
            // Deliver remaining 14 preface bytes; next sniff should transition
            nf.withPayload(Http2Preface.LENGTH, Http2Preface.LENGTH);
            try {
                ctx.sniffAndSelectModeForTests();
            } catch (ServerDisconnectException | PeerIsSlowToWriteException e) {
                Assert.fail("second sniff should succeed, got: " + e);
            }
            Assert.assertEquals("h2_preface_pending", ctx.getProtocolModeForTests());
        });
    }

    private void runSniff(RecordingNetworkFacade nf, boolean h2Enabled, SniffAction body) throws Exception {
        assertMemoryLeak(() -> {
            DefaultHttpServerConfiguration config = new HttpServerConfigurationBuilder()
                    .withNetwork(nf)
                    .withH2Enabled(h2Enabled)
                    .build(configuration);
            try (HttpConnectionContext ctx = new HttpConnectionContext(config, PlainSocketFactory.INSTANCE)) {
                ctx.of(DUMMY_FD);
                ctx.init();
                body.run(ctx);
            }
        });
    }

    @FunctionalInterface
    private interface SniffAction {
        void run(HttpConnectionContext ctx) throws Exception;
    }

    private static final class RecordingNetworkFacade extends NetworkFacadeImpl {
        int peekRawCount;
        private byte[] peekBytes = new byte[0];
        private int peekReturn = 0;

        @Override
        public int peekRaw(long fd, long buffer, int bufferLen) {
            peekRawCount++;
            if (peekReturn < 0) {
                return peekReturn;
            }
            final int n = Math.min(peekBytes.length, bufferLen);
            for (int i = 0; i < n; i++) {
                Unsafe.getUnsafe().putByte(buffer + i, peekBytes[i]);
            }
            return n;
        }

        RecordingNetworkFacade withBytes(byte[] bytes) {
            this.peekBytes = bytes;
            this.peekReturn = 0;
            return this;
        }

        RecordingNetworkFacade withPayload(int prefaceSize, int bytesToDeliver) {
            byte[] buf = new byte[prefaceSize];
            long addr = Unsafe.malloc(prefaceSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            try {
                Http2Preface.write(addr);
                for (int i = 0; i < prefaceSize; i++) {
                    buf[i] = Unsafe.getUnsafe().getByte(addr + i);
                }
            } finally {
                Unsafe.free(addr, prefaceSize, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            }
            final byte[] delivered = new byte[bytesToDeliver];
            System.arraycopy(buf, 0, delivered, 0, bytesToDeliver);
            this.peekBytes = delivered;
            this.peekReturn = 0;
            return this;
        }

        RecordingNetworkFacade withPeekReturn(int peekReturn) {
            this.peekReturn = peekReturn;
            return this;
        }
    }
}
