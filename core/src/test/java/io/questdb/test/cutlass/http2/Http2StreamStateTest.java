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

import io.questdb.cutlass.http2.Http2FrameType;
import io.questdb.cutlass.http2.Http2StreamState;
import org.junit.Assert;
import org.junit.Test;

public class Http2StreamStateTest {

    @Test
    public void testClosedPermitsGracefulRaceFrames() {
        Http2StreamState s = Http2StreamState.CLOSED;
        Assert.assertTrue(s.permitsInbound(Http2FrameType.WINDOW_UPDATE));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.RST_STREAM));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.PRIORITY));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.HEADERS));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.CONTINUATION));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.DATA));
    }

    @Test
    public void testHalfClosedLocalPermitsInboundDataAndTrailers() {
        // We sent END_STREAM; the peer can still send body + trailers on the
        // request side (§5 table row half-closed-local).
        Http2StreamState s = Http2StreamState.HALF_CLOSED_LOCAL;
        Assert.assertTrue(s.permitsInbound(Http2FrameType.HEADERS));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.CONTINUATION));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.DATA));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.RST_STREAM));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.WINDOW_UPDATE));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.PRIORITY));
    }

    @Test
    public void testHalfClosedRemotePermitsOnlyControlFrames() {
        Http2StreamState s = Http2StreamState.HALF_CLOSED_REMOTE;
        Assert.assertFalse(s.permitsInbound(Http2FrameType.HEADERS));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.CONTINUATION));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.DATA));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.WINDOW_UPDATE));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.RST_STREAM));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.PRIORITY));
    }

    @Test
    public void testIdlePermitsHeadersAndPriority() {
        Http2StreamState s = Http2StreamState.IDLE;
        Assert.assertTrue(s.permitsInbound(Http2FrameType.HEADERS));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.PRIORITY));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.DATA));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.CONTINUATION));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.WINDOW_UPDATE));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.RST_STREAM));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.SETTINGS));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.PING));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.GOAWAY));
    }

    @Test
    public void testOpenPermitsAllStreamScopedFrames() {
        Http2StreamState s = Http2StreamState.OPEN;
        Assert.assertTrue(s.permitsInbound(Http2FrameType.HEADERS));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.CONTINUATION));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.DATA));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.RST_STREAM));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.WINDOW_UPDATE));
        Assert.assertTrue(s.permitsInbound(Http2FrameType.PRIORITY));
        // Connection-scoped frames never reach a stream-scoped matrix.
        Assert.assertFalse(s.permitsInbound(Http2FrameType.SETTINGS));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.PING));
        Assert.assertFalse(s.permitsInbound(Http2FrameType.GOAWAY));
    }

    @Test
    public void testReservedStatesRejectEverything() {
        // Server never pushes and peer is a client, so RESERVED_LOCAL /
        // RESERVED_REMOTE are unreachable. Matrix must reflect that.
        Assert.assertFalse(Http2StreamState.RESERVED_LOCAL.permitsInbound(Http2FrameType.HEADERS));
        Assert.assertFalse(Http2StreamState.RESERVED_LOCAL.permitsInbound(Http2FrameType.DATA));
        Assert.assertFalse(Http2StreamState.RESERVED_LOCAL.permitsInbound(Http2FrameType.RST_STREAM));
        Assert.assertFalse(Http2StreamState.RESERVED_REMOTE.permitsInbound(Http2FrameType.HEADERS));
        Assert.assertFalse(Http2StreamState.RESERVED_REMOTE.permitsInbound(Http2FrameType.DATA));
    }

    @Test
    public void testUnknownFrameTypeRejected() {
        // Frame types >= 32 (outside the bitmask range) must always return
        // false; the frame reader's unknown-type discipline handles discard.
        Assert.assertFalse(Http2StreamState.OPEN.permitsInbound((byte) 0x20));
        Assert.assertFalse(Http2StreamState.OPEN.permitsInbound((byte) 0xFF));
    }
}
