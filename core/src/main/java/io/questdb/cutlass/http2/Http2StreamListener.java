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
 * Visitor implemented by the request-dispatch layer above the HTTP/2 state
 * machine. The connection context drives these callbacks as request frames
 * arrive and validate.
 * <p>
 * Callback lifecycle per stream ({@code STREAM_STATE_MACHINE.md} §4 / §11):
 * <ul>
 *   <li>Initial HEADERS block, validation passed:
 *       {@link #onRequestHeader} fires once per staged regular-header field,
 *       then {@link #onRequestHeaders} fires once. If the block fails
 *       validation (sticky error in §11), zero per-field callbacks fire and
 *       the per-block callback is suppressed.</li>
 *   <li>Request body: {@link #onData} fires for every DATA frame that
 *       survives the pre-dispatch content-length check.</li>
 *   <li>Trailers (M2): {@link #onTrailers} fires at trailer END_HEADERS if
 *       validation passed.</li>
 *   <li>Teardown: {@link #onStreamClosed} fires once per stream on close,
 *       regardless of clean or reset. {@link #onStreamClosed} is the only
 *       callback that always fires; every other callback may be suppressed
 *       by validation.</li>
 * </ul>
 * <p>
 * The terminal edge of a request is carried by the {@code endStream} flag
 * on exactly one of {@link #onRequestHeaders}, {@link #onData}, or
 * {@link #onTrailers} — whichever callback carries the last byte. There is
 * no separate "request complete" callback.
 */
public interface Http2StreamListener {

    /**
     * Relayed DATA payload for stream {@code streamId}.
     * <p>
     * The {@code (addr, dataLen)} pair points into the frame reader's
     * receive buffer and is stable only for the duration of this call; the
     * handler must copy any bytes it wants to retain. The {@code dataLen}
     * excludes padding — the flow-control accounting for padding has
     * already happened per §7 and is not relayed to the handler.
     * <p>
     * Return value contract per §7 step 6:
     * <ul>
     *   <li>{@code true} — synchronous consumption. The context credits
     *       {@code dataLen} bytes to the coalesced {@code WINDOW_UPDATE}
     *       path immediately.</li>
     *   <li>{@code false} — deferred consumption. The context adds
     *       {@code dataLen} to the stream's
     *       {@code outstandingInboundCredit} and waits for the handler to
     *       call
     *       {@code Http2ConnectionContext.onBytesConsumed(streamId,
     *       generationToken, n)} to credit the consumed portion. The
     *       {@code generationToken} is the current stream generation at
     *       the moment of this call; the handler echoes it on every ack
     *       so a stale reference after reset / recycle no-ops per §7
     *       step 6.</li>
     * </ul>
     */
    boolean onData(int streamId, long addr, int dataLen, boolean endStream, int generationToken);

    /**
     * Relayed per-field regular-header emission during an initial HEADERS
     * block that has passed §11 validation. Pseudo-header slots are
     * delivered via the {@link Http2RequestHeadersView} in
     * {@link #onRequestHeaders}, not through this callback.
     * <p>
     * {@code (nameAddr, nameLen)} and {@code (valueAddr, valueLen)} point
     * into the stream's per-stream header-staging buffer; they are stable
     * for the duration of this call.
     */
    void onRequestHeader(int streamId, long nameAddr, int nameLen,
                         long valueAddr, int valueLen, boolean neverIndexed);

    /**
     * Per-block emission fired once after {@link #onRequestHeader} has been
     * invoked for every staged regular-header field. The {@code view}
     * exposes the captured pseudo-header slots; the {@code endStream} flag
     * indicates whether the HEADERS frame's {@code END_STREAM} was set
     * (i.e., a request with no body and no trailers to follow).
     */
    void onRequestHeaders(int streamId, Http2RequestHeadersView view, boolean endStream);

    /**
     * Fires once per stream when the state machine closes the stream,
     * clean or reset. The {@code cause} is a {@link Http2ErrorCode}
     * value, with {@link Http2ErrorCode#NO_ERROR} indicating a clean
     * close. Every handler resource bound to the stream (the slot in
     * {@code onRequestHeaders}'s view, deferred-credit obligations, etc.)
     * must be released by the handler inside this callback.
     */
    void onStreamClosed(int streamId, int cause);

    /**
     * Fires when an engine event makes {@code streamId}'s outbound side
     * more writable — i.e. a prior
     * {@link Http2ConnectionContext#enqueueData} or
     * {@link Http2ConnectionContext#emitResponseHeaders} that returned
     * {@link Http2ConnectionContext#ENQUEUE_PARK} could now succeed, or
     * the scheduler unblocked a stream whose DATA was window-stuck.
     * <p>
     * Fires synchronously inside
     * {@link Http2ConnectionContext#processReceivedBytes} or
     * {@link Http2ConnectionContext#writePending}. Triggers:
     * <ul>
     *   <li>peer {@code WINDOW_UPDATE(streamId)} on a parked stream;</li>
     *   <li>peer {@code WINDOW_UPDATE(0)} on the connection, fanned out
     *       to every parked stream whose per-stream outbound window is
     *       non-zero;</li>
     *   <li>peer {@code SETTINGS_INITIAL_WINDOW_SIZE} increase that
     *       raises a parked stream's outbound window;</li>
     *   <li>scheduler emission that brings a parked stream's queued-
     *       payload and tuple-ring occupancy below the per-stream caps.</li>
     * </ul>
     * The callback may run more than once per stream across the stream
     * lifetime (every time it parks and then un-parks). The integration
     * layer is expected to dispatch {@code processor.resumeSend} once
     * per invocation.
     */
    void onStreamWritable(int streamId);

    /**
     * Relayed trailer block after validation (M2). The {@code endStream}
     * flag is always {@code true} on a legal trailer block per RFC 9113
     * sec. 8.1; the layer surfaces it for symmetry with the other two
     * terminal-edge callbacks.
     */
    void onTrailers(int streamId, boolean endStream);
}
