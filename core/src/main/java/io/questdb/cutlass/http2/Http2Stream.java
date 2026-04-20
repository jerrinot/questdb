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
 * Per-stream state carried by the HTTP/2 state machine (see
 * {@code STREAM_STATE_MACHINE.md} §4 and §5).
 * <p>
 * One long-lived instance per pool slot, reused across requests. The pool
 * calls {@link #recycle} when it hands the slot to a new stream; the
 * {@link #generation} counter bumps on every recycle to make stale handler
 * references (see §7 step 6 late-ack guard) observable at the connection
 * context's {@code onBytesConsumed} ack path.
 * <p>
 * This class owns only the FSM + flow-control + content-length-tracking
 * surface that is independent of HPACK decoding. The header-staging buffer,
 * pseudo-header slots, policy counter, and sticky-error flag documented in
 * §4 / §11 land in a follow-up commit alongside the listener wiring in
 * §14.3 step 7. Keeping the state-machine commit focused on transitions
 * matches the build order in §14.3.
 */
public final class Http2Stream {

    private int generation;
    private long inboundStreamWindow;
    // A second HEADERS on the same stream carries trailers (RFC 9113 sec. 8.1);
    // the first HEADERS carries the request pseudo-headers. The §11 dispatch
    // branches on this, and the pool preserves it only for the lifetime of
    // one LIVE assignment — recycle() clears it.
    private boolean initialHeadersSeen;
    private long outboundStreamWindow;
    // §8 M1 trailer refusal: set when a second HEADERS arrives on a LIVE
    // stream. HPACK decoding of the trailer block still runs (to keep the
    // dynamic table coherent) but per-field callbacks and state transitions
    // are suppressed, and the stream is reset at END_HEADERS. Cleared on
    // {@link #beginHeaderBlock} so the flag is strictly per-block.
    private boolean refusingCurrentBlock;
    // Deferred-credit owed back to the peer. Bumped on every `onData` the
    // handler defers (returns false), reduced on `onBytesConsumed`. The
    // stream-close settlement in §7 step 6 credits the residual to the
    // connection window and zeroes this field.
    private long outstandingInboundCredit;
    private Http2StreamState state = Http2StreamState.IDLE;
    private int streamId = -1;

    /**
     * Returns the monotonically-increasing generation counter, advanced by
     * the pool on every LIVE slot recycle. The handler holds a
     * {@code (streamId, generationToken)} pair captured at dispatch time and
     * echoes the token on every deferred-credit ack; a stale token (from a
     * prior generation in the same slot) causes
     * {@code Http2ConnectionContext.onBytesConsumed} to silently no-op per
     * §7 step 6.
     */
    /**
     * Resets per-block staging state (currently only {@link #refusingCurrentBlock})
     * at the start of a new HEADERS / trailer block. The connection context
     * calls this exactly once per inbound HEADERS frame admission before
     * HPACK fragment bytes accumulate.
     */
    public void beginHeaderBlock() {
        refusingCurrentBlock = false;
    }

    public int getGeneration() {
        return generation;
    }

    public long getInboundStreamWindow() {
        return inboundStreamWindow;
    }

    public long getOutboundStreamWindow() {
        return outboundStreamWindow;
    }

    public long getOutstandingInboundCredit() {
        return outstandingInboundCredit;
    }

    public Http2StreamState getState() {
        return state;
    }

    public int getStreamId() {
        return streamId;
    }

    public boolean isClosed() {
        return state == Http2StreamState.CLOSED;
    }

    /**
     * Returns {@code true} if the inbound direction of this stream can still
     * carry {@code DATA} or trailer {@code HEADERS} from the peer — i.e.
     * {@link Http2StreamState#OPEN} or
     * {@link Http2StreamState#HALF_CLOSED_LOCAL}. The inbound-window
     * direction is live in exactly these two states; a debit or credit from
     * any other state must skip the stream window per §7 step 3.
     */
    public boolean isInboundDirectionActive() {
        return state == Http2StreamState.OPEN
                || state == Http2StreamState.HALF_CLOSED_LOCAL;
    }

    public boolean isInitialHeadersSeen() {
        return initialHeadersSeen;
    }

    /**
     * Returns {@code true} if the outbound direction of this stream can
     * still carry {@code DATA} or response {@code HEADERS} to the peer —
     * i.e. {@link Http2StreamState#OPEN} or
     * {@link Http2StreamState#HALF_CLOSED_REMOTE}. Symmetric with
     * {@link #isInboundDirectionActive}.
     */
    public boolean isOutboundDirectionActive() {
        return state == Http2StreamState.OPEN
                || state == Http2StreamState.HALF_CLOSED_REMOTE;
    }

    /**
     * Returns {@code true} if the currently-assembling HEADERS block is
     * being HPACK-decoded with per-field output suppressed. Set by
     * {@link #markCurrentBlockRefused} when §8's M1 trailer-refusal path
     * applies; cleared by {@link #beginHeaderBlock} on the next block.
     */
    public boolean isRefusingCurrentBlock() {
        return refusingCurrentBlock;
    }

    /**
     * Marks the block currently assembling on this LIVE stream as refused
     * (§8 M1 trailer refusal, §11 sticky-error-equivalent paths). HPACK
     * decode still runs so the dynamic table stays coherent; the
     * connection context emits {@code RST_STREAM} at the block's
     * END_HEADERS. One-way flip — cleared only by
     * {@link #beginHeaderBlock} on the next block.
     */
    public void markCurrentBlockRefused() {
        refusingCurrentBlock = true;
    }

    /**
     * Applies a signed delta to {@link #outboundStreamWindow} after a peer
     * {@code SETTINGS_INITIAL_WINDOW_SIZE} change. No-op on streams whose
     * outbound direction is closed (§7 direction-active rule). Returns
     * {@code true} on success, {@code false} if the adjustment would push
     * the window past {@code 2^31 - 1}; the caller escalates to connection
     * {@code FLOW_CONTROL_ERROR} per §7.
     */
    public boolean onOutboundInitialWindowDelta(long delta) {
        if (!isOutboundDirectionActive()) {
            return true;
        }
        long adjusted = Http2FlowController.adjustOnInitialWindowChange(outboundStreamWindow, delta);
        if (adjusted == Http2FlowController.OVERFLOW) {
            return false;
        }
        outboundStreamWindow = adjusted;
        return true;
    }

    /**
     * Transition the stream in response to the peer sending {@code DATA} with
     * the {@code END_STREAM} flag set. Legal from {@link Http2StreamState#OPEN}
     * (→ {@link Http2StreamState#HALF_CLOSED_REMOTE}) or
     * {@link Http2StreamState#HALF_CLOSED_LOCAL} (→
     * {@link Http2StreamState#CLOSED}). Any other source state is a bug in
     * the dispatcher (the permitted-frames matrix should have caught it
     * first) — this method returns {@code false} rather than throwing so the
     * dispatcher can surface the right error-scope via its state-table
     * lookup.
     */
    public boolean onRecvDataEndStream() {
        switch (state) {
            case OPEN:
                state = Http2StreamState.HALF_CLOSED_REMOTE;
                return true;
            case HALF_CLOSED_LOCAL:
                state = Http2StreamState.CLOSED;
                return true;
            default:
                return false;
        }
    }

    /**
     * Transition the stream in response to the peer sending a {@code HEADERS}
     * frame (initial request headers or trailers). The {@code endStream}
     * flag distinguishes the two sub-cases per the §5 table:
     * <ul>
     *   <li>IDLE → OPEN (no END_STREAM) or HALF_CLOSED_REMOTE (END_STREAM).
     *       This is the idle-to-live admission edge. After the transition,
     *       {@link #initialHeadersSeen} flips to {@code true} so the next
     *       HEADERS arrival is recognised as trailers.</li>
     *   <li>OPEN + END_STREAM → HALF_CLOSED_REMOTE (trailers path; RFC 9113
     *       sec. 8.1 requires END_STREAM on trailer HEADERS).</li>
     *   <li>HALF_CLOSED_LOCAL + END_STREAM → CLOSED (trailers path after
     *       response-side close).</li>
     * </ul>
     * An {@code OPEN} or {@code HALF_CLOSED_LOCAL} HEADERS without END_STREAM
     * is a malformed trailer and returns {@code false}; the dispatcher
     * translates that into a sticky stream error per §11. Any other source
     * state returns {@code false}.
     */
    public boolean onRecvHeaders(boolean endStream) {
        switch (state) {
            case IDLE:
                state = endStream ? Http2StreamState.HALF_CLOSED_REMOTE : Http2StreamState.OPEN;
                initialHeadersSeen = true;
                return true;
            case OPEN:
                if (!endStream) {
                    return false;
                }
                state = Http2StreamState.HALF_CLOSED_REMOTE;
                return true;
            case HALF_CLOSED_LOCAL:
                if (!endStream) {
                    return false;
                }
                state = Http2StreamState.CLOSED;
                return true;
            default:
                return false;
        }
    }

    /**
     * Transition the stream when an {@code RST_STREAM} is sent or received.
     * Legal from any non-idle, non-closed state per §5. An RST on an idle
     * stream is a connection-level protocol error per RFC 9113 sec. 5.1; the
     * dispatcher catches that before reaching this method.
     */
    public void onRst() {
        state = Http2StreamState.CLOSED;
    }

    /**
     * Transition the stream in response to us sending {@code DATA} with
     * {@code END_STREAM}. Symmetric with {@link #onRecvDataEndStream}.
     */
    public boolean onSendDataEndStream() {
        switch (state) {
            case OPEN:
                state = Http2StreamState.HALF_CLOSED_LOCAL;
                return true;
            case HALF_CLOSED_REMOTE:
                state = Http2StreamState.CLOSED;
                return true;
            default:
                return false;
        }
    }

    /**
     * Transition the stream in response to us sending {@code HEADERS}
     * (response headers or trailers). Response headers may legally be sent
     * from {@link Http2StreamState#OPEN} (if we choose to respond before the
     * request body finishes) or {@link Http2StreamState#HALF_CLOSED_REMOTE}
     * (the typical case; request body already closed). The {@code endStream}
     * flag can drive the stream to HALF_CLOSED_LOCAL or CLOSED respectively.
     */
    public boolean onSendHeaders(boolean endStream) {
        switch (state) {
            case OPEN:
                if (endStream) {
                    state = Http2StreamState.HALF_CLOSED_LOCAL;
                }
                return true;
            case HALF_CLOSED_REMOTE:
                if (endStream) {
                    state = Http2StreamState.CLOSED;
                }
                return true;
            default:
                return false;
        }
    }

    /**
     * Reinitialise the stream for a new LIVE assignment. Called by
     * {@link Http2StreamPool} when reusing a free-listed slot. The
     * {@link #generation} counter is incremented so stale handler
     * {@code (streamId, generationToken)} references from the previous
     * occupant fail the late-ack guard in §7 step 6.
     *
     * @param newStreamId         peer-initiated stream id being admitted
     * @param initialInboundWindow  {@code ourApplied[INITIAL_WINDOW_SIZE]} at
     *                              the moment of transition-to-OPEN
     *                              (§7 initial-window rule)
     * @param initialOutboundWindow {@code peerAdvertised[INITIAL_WINDOW_SIZE]}
     *                              at the moment of transition-to-OPEN
     */
    public void recycle(int newStreamId, long initialInboundWindow, long initialOutboundWindow) {
        if (newStreamId <= 0 || newStreamId > 0x7FFFFFFF) {
            throw new IllegalArgumentException("streamId out of range: " + newStreamId);
        }
        this.streamId = newStreamId;
        this.state = Http2StreamState.IDLE;
        this.inboundStreamWindow = initialInboundWindow;
        this.outboundStreamWindow = initialOutboundWindow;
        this.outstandingInboundCredit = 0;
        this.initialHeadersSeen = false;
        this.refusingCurrentBlock = false;
        this.generation++;
    }

    /**
     * Bumps {@link #generation} without re-initialising any other fields.
     * Called by the pool when this slot is promoted to a tombstone so a
     * subsequent handler ack against the promoted occupant's token is
     * observed as stale. State-reset belongs in {@link #recycle}.
     */
    void bumpGeneration() {
        this.generation++;
    }

    /**
     * Decrements {@link #outstandingInboundCredit} by {@code n}. Package-
     * private because the credit lifecycle is managed by the connection
     * context's deferred-credit path (§7 step 6), which owns the bounds
     * check ({@code 0 < n <= outstandingInboundCredit}).
     */
    void consumeDeferredCredit(long n) {
        outstandingInboundCredit -= n;
    }

    /**
     * Accumulates a deferred-credit obligation. Package-private: only the
     * connection context (on handler {@code onData} returning {@code false})
     * or the pool (when stashing an in-progress obligation across a slot
     * recycle) should mutate this counter.
     */
    void recordDeferredCredit(long n) {
        outstandingInboundCredit += n;
    }

    void setInboundStreamWindow(long window) {
        this.inboundStreamWindow = window;
    }

    void setOutboundStreamWindow(long window) {
        this.outboundStreamWindow = window;
    }

    /**
     * Restores the stream to a never-assigned state. Called when the pool
     * retires a slot to its free-list; the FSM returns to IDLE and the
     * stream id to its sentinel so any lookup against the retired id
     * unambiguously misses.
     */
    void unassign() {
        this.streamId = -1;
        this.state = Http2StreamState.IDLE;
        this.inboundStreamWindow = 0;
        this.outboundStreamWindow = 0;
        this.outstandingInboundCredit = 0;
        this.initialHeadersSeen = false;
        this.refusingCurrentBlock = false;
    }
}
