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

package io.questdb.cutlass.http2;

/**
 * HTTP/2 per-stream state machine states (RFC 7540 sec. 5.1 / RFC 9113 sec. 5.1).
 * <p>
 * The server-only code path exercises {@link #IDLE}, {@link #OPEN},
 * {@link #HALF_CLOSED_LOCAL}, {@link #HALF_CLOSED_REMOTE}, and {@link #CLOSED}.
 * {@link #RESERVED_LOCAL} and {@link #RESERVED_REMOTE} are defined for
 * completeness — a server that never emits {@code PUSH_PROMISE} (this one
 * advertises {@code SETTINGS_ENABLE_PUSH = 0}) cannot enter either.
 * <p>
 * The permitted-inbound-frames matrix from {@code STREAM_STATE_MACHINE.md} §5
 * is encoded here as a per-state bitmask of frame-type codes. The two
 * "closed" flavours (clean vs locally-reset) are distinguished by the pool's
 * tombstone {@code closeKind}, not by separate enum values — the FSM itself
 * only has one {@code CLOSED} state.
 */
public enum Http2StreamState {
    IDLE,
    RESERVED_LOCAL,
    RESERVED_REMOTE,
    OPEN,
    HALF_CLOSED_LOCAL,
    HALF_CLOSED_REMOTE,
    CLOSED;

    // Bitmask of frame-type bits permitted on the inbound side in each state.
    // Bit `frameType` is set iff the frame type is structurally legal on a
    // LIVE stream in that state. The dispatch layer (§5 precedence) decides
    // the error scope (stream vs connection) for disallowed frames.
    private static final int[] INBOUND_MASKS = buildInboundMasks();

    /**
     * Returns {@code true} if the bytes with frame-type code {@code frameType}
     * are structurally legal on a LIVE stream in this state. A {@code false}
     * result indicates the dispatch layer must raise a stream- or connection-
     * level error per §5 / §12.
     * <p>
     * Unknown frame-type codes ({@code frameType >= 16}) always return
     * {@code false} here; the frame reader's unknown-frame discipline is
     * the correct place to accept-and-ignore them, not the state machine.
     */
    public boolean permitsInbound(byte frameType) {
        int unsigned = frameType & 0xFF;
        if (unsigned >= 32) {
            return false;
        }
        return (INBOUND_MASKS[ordinal()] & (1 << unsigned)) != 0;
    }

    private static int bit(byte frameType) {
        return 1 << (frameType & 0xFF);
    }

    private static int[] buildInboundMasks() {
        int[] m = new int[values().length];
        m[IDLE.ordinal()] =
                bit(Http2FrameType.HEADERS)
                        | bit(Http2FrameType.PRIORITY);
        m[OPEN.ordinal()] =
                bit(Http2FrameType.HEADERS)
                        | bit(Http2FrameType.CONTINUATION)
                        | bit(Http2FrameType.DATA)
                        | bit(Http2FrameType.RST_STREAM)
                        | bit(Http2FrameType.WINDOW_UPDATE)
                        | bit(Http2FrameType.PRIORITY);
        m[HALF_CLOSED_LOCAL.ordinal()] =
                bit(Http2FrameType.HEADERS)
                        | bit(Http2FrameType.CONTINUATION)
                        | bit(Http2FrameType.DATA)
                        | bit(Http2FrameType.RST_STREAM)
                        | bit(Http2FrameType.WINDOW_UPDATE)
                        | bit(Http2FrameType.PRIORITY);
        m[HALF_CLOSED_REMOTE.ordinal()] =
                bit(Http2FrameType.WINDOW_UPDATE)
                        | bit(Http2FrameType.RST_STREAM)
                        | bit(Http2FrameType.PRIORITY);
        // CLEAN-close flavour. The locally-reset flavour permits every frame
        // type (minimally processed) and is selected by the tombstone's
        // closeKind at dispatch time, not by this enum.
        m[CLOSED.ordinal()] =
                bit(Http2FrameType.WINDOW_UPDATE)
                        | bit(Http2FrameType.RST_STREAM)
                        | bit(Http2FrameType.PRIORITY);
        // RESERVED_LOCAL / RESERVED_REMOTE: unreachable on the server-only
        // code path (no push, no incoming push). Masks left at 0.
        return m;
    }
}
