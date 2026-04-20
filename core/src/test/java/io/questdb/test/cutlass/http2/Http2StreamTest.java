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

import io.questdb.cutlass.http2.Http2FlowController;
import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamState;
import org.junit.Assert;
import org.junit.Test;

public class Http2StreamTest {

    @Test
    public void testGenerationBumpsOnRecycle() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 65_535, 65_535);
        int g1 = s.getGeneration();
        s.recycle(3, 65_535, 65_535);
        int g2 = s.getGeneration();
        Assert.assertTrue(g2 > g1);
    }

    @Test
    public void testIdleRecvHeadersEndStreamToHalfClosedRemote() {
        Http2Stream s = new Http2Stream();
        s.recycle(3, 65_535, 65_535);
        Assert.assertEquals(Http2StreamState.IDLE, s.getState());
        Assert.assertTrue(s.onRecvHeaders(true));
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());
        Assert.assertTrue(s.isInitialHeadersSeen());
    }

    @Test
    public void testIdleRecvHeadersToOpen() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 65_535, 65_535);
        Assert.assertEquals(Http2StreamState.IDLE, s.getState());
        Assert.assertTrue(s.onRecvHeaders(false));
        Assert.assertEquals(Http2StreamState.OPEN, s.getState());
        Assert.assertTrue(s.isInitialHeadersSeen());
    }

    @Test
    public void testInboundDirectionActive() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 1024, 1024);
        Assert.assertFalse(s.isInboundDirectionActive()); // IDLE
        s.onRecvHeaders(false);
        Assert.assertTrue(s.isInboundDirectionActive()); // OPEN
        s.onRecvDataEndStream();
        Assert.assertFalse(s.isInboundDirectionActive()); // HALF_CLOSED_REMOTE
    }

    @Test
    public void testLocalResetFromAnyState() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 65_535, 65_535);
        s.onRecvHeaders(false);
        Assert.assertEquals(Http2StreamState.OPEN, s.getState());
        s.onRst();
        Assert.assertEquals(Http2StreamState.CLOSED, s.getState());
    }

    @Test
    public void testOpenRecvDataEndStreamToHalfClosedRemote() {
        Http2Stream s = openStream(1);
        Assert.assertTrue(s.onRecvDataEndStream());
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());
    }

    @Test
    public void testOpenRecvTrailerHeadersRequiresEndStream() {
        Http2Stream s = openStream(1);
        // Trailer HEADERS without END_STREAM is malformed.
        Assert.assertFalse(s.onRecvHeaders(false));
        Assert.assertEquals(Http2StreamState.OPEN, s.getState()); // unchanged
        // With END_STREAM: legal trailer.
        Assert.assertTrue(s.onRecvHeaders(true));
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());
    }

    @Test
    public void testOpenSendDataEndStreamToHalfClosedLocal() {
        Http2Stream s = openStream(1);
        Assert.assertTrue(s.onSendDataEndStream());
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_LOCAL, s.getState());
    }

    @Test
    public void testOutboundDirectionActive() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 1024, 1024);
        Assert.assertFalse(s.isOutboundDirectionActive()); // IDLE
        s.onRecvHeaders(false);
        Assert.assertTrue(s.isOutboundDirectionActive()); // OPEN
        s.onSendDataEndStream();
        Assert.assertFalse(s.isOutboundDirectionActive()); // HALF_CLOSED_LOCAL
    }

    @Test
    public void testOutboundInitialWindowDeltaOverflow() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 65_535L, Http2FlowController.WINDOW_MAX - 100L);
        s.onRecvHeaders(false); // OPEN → outbound active
        Assert.assertFalse(s.onOutboundInitialWindowDelta(200L));
        // Window unchanged on overflow.
        Assert.assertEquals(Http2FlowController.WINDOW_MAX - 100L, s.getOutboundStreamWindow());
    }

    @Test
    public void testOutboundInitialWindowDeltaSkipsFrozenDirection() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 65_535L, 1_000L);
        s.onRecvHeaders(false); // OPEN
        s.onSendDataEndStream(); // HALF_CLOSED_LOCAL → outbound frozen
        Assert.assertTrue(s.onOutboundInitialWindowDelta(500L));
        Assert.assertEquals(1_000L, s.getOutboundStreamWindow());
    }

    @Test
    public void testRecvEndStreamFromHalfClosedLocalClosesStream() {
        Http2Stream s = openStream(1);
        Assert.assertTrue(s.onSendDataEndStream());
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_LOCAL, s.getState());
        Assert.assertTrue(s.onRecvDataEndStream());
        Assert.assertEquals(Http2StreamState.CLOSED, s.getState());
    }

    @Test
    public void testRecvEndStreamFromIdleIsRejected() {
        // onRecvDataEndStream is only legal after onRecvHeaders put us in
        // OPEN or HALF_CLOSED_LOCAL; IDLE must not accept it.
        Http2Stream s = new Http2Stream();
        s.recycle(1, 65_535, 65_535);
        Assert.assertFalse(s.onRecvDataEndStream());
        Assert.assertEquals(Http2StreamState.IDLE, s.getState());
    }

    @Test
    public void testRecycleResetsState() {
        Http2Stream s = openStream(1);
        s.onSendDataEndStream(); // HALF_CLOSED_LOCAL
        int gen1 = s.getGeneration();
        s.recycle(5, 1_000L, 2_000L);
        Assert.assertEquals(5, s.getStreamId());
        Assert.assertEquals(Http2StreamState.IDLE, s.getState());
        Assert.assertEquals(1_000L, s.getInboundStreamWindow());
        Assert.assertEquals(2_000L, s.getOutboundStreamWindow());
        Assert.assertEquals(0L, s.getOutstandingInboundCredit());
        Assert.assertFalse(s.isInitialHeadersSeen());
        Assert.assertEquals(gen1 + 1, s.getGeneration());
    }

    @Test
    public void testRecycleRejectsInvalidStreamId() {
        Http2Stream s = new Http2Stream();
        try {
            s.recycle(0, 65_535, 65_535);
            Assert.fail();
        } catch (IllegalArgumentException expected) {
        }
        try {
            s.recycle(-1, 65_535, 65_535);
            Assert.fail();
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testSendHeadersEndStreamOnHalfClosedRemoteClosesStream() {
        Http2Stream s = openStream(1);
        Assert.assertTrue(s.onRecvDataEndStream());
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());
        // Headers-only response with END_STREAM (e.g., 204 No Content).
        Assert.assertTrue(s.onSendHeaders(true));
        Assert.assertEquals(Http2StreamState.CLOSED, s.getState());
    }

    @Test
    public void testSendHeadersFromHalfClosedRemoteStaysInState() {
        Http2Stream s = openStream(1);
        s.onRecvDataEndStream();
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());
        // Response headers (no END_STREAM because body will follow).
        Assert.assertTrue(s.onSendHeaders(false));
        Assert.assertEquals(Http2StreamState.HALF_CLOSED_REMOTE, s.getState());
    }

    @Test
    public void testSendHeadersFromIdleRejected() {
        Http2Stream s = new Http2Stream();
        s.recycle(1, 65_535, 65_535);
        // Server cannot send response HEADERS before the request HEADERS
        // have been received (RFC 9113 sec. 5.1 forbids idle → send HEADERS
        // on the server side).
        Assert.assertFalse(s.onSendHeaders(false));
    }

    private static Http2Stream openStream(int id) {
        Http2Stream s = new Http2Stream();
        s.recycle(id, 65_535L, 65_535L);
        s.onRecvHeaders(false);
        return s;
    }
}
