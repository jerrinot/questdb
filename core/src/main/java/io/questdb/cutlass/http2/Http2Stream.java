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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

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
 * Owns the FSM + flow-control + content-length-tracking surface and the
 * per-stream outbound response state documented in
 * {@code HTTP2_INTEGRATION.md} §4.4 / §10: a copy-on-enqueue native arena
 * plus a fixed-size ring of frame tuples
 * {@code (kind, flags, arenaOffset, payloadLen)}. The scheduler in
 * {@code Http2ConnectionContext.writePending} drains the tuple queues in
 * round-robin order; this class only stores and exposes them. The header-
 * staging buffer, pseudo-header slots, policy counter, and sticky-error
 * flag documented in §4 / §11 still land in a follow-up commit alongside
 * the listener wiring.
 * <p>
 * Implements {@link Closeable} so the pool can reclaim the native arena
 * at connection teardown. Callers that construct a stand-alone stream
 * with a non-zero arena (tests, benchmarks) must pair it with
 * {@link #close}.
 */
public final class Http2Stream implements Closeable {

    // Pseudo-header / content-type staging slot indices. Five fixed slots
    // matching the narrow capture set from §15.5 step 2: :method, :path,
    // :scheme, :authority plus the {@code content-type} regular header that
    // Flight SQL routers need for gRPC content negotiation. {@code Host}
    // is deliberately not captured — H1-era mechanism that Flight SQL
    // clients never use.
    public static final int STAGING_SLOT_AUTHORITY = 3;
    public static final int STAGING_SLOT_CONTENT_TYPE = 4;
    public static final int STAGING_SLOT_COUNT = 5;
    public static final int STAGING_SLOT_METHOD = 0;
    public static final int STAGING_SLOT_PATH = 1;
    public static final int STAGING_SLOT_SCHEME = 2;
    public static final byte TUPLE_FLAG_END_HEADERS = 0x02;
    public static final byte TUPLE_FLAG_END_STREAM = 0x01;
    public static final byte TUPLE_KIND_CONTINUATION = 2;
    public static final byte TUPLE_KIND_DATA = 3;
    public static final byte TUPLE_KIND_HEADERS = 1;
    // Trailer HEADERS emit path (§15.5 step 3). Wire-frame-identical to
    // {@link #TUPLE_KIND_HEADERS} — the scheduler encodes it as a HEADERS
    // frame plus optional CONTINUATION — but semantically distinct: the
    // engine enforces that the caller set {@code endStream=true} at
    // enqueue time, so every trailer tuple closes the stream.
    public static final byte TUPLE_KIND_TRAILERS = 4;

    // Outbound arena (native, copy-on-enqueue). Size fixed at construction;
    // zero-capacity streams are legal for unit tests that only exercise the
    // inbound / FSM surface. The bump cursor resets to 0 every time the
    // tuple queue fully drains; partial-drain + refill attempts that would
    // run off the tail return false from tryEnqueueOutbound without a
    // compaction memmove. That keeps PR1 semantics obvious; a wrap-around
    // variant can land later if workloads justify it.
    private final long outboundArenaAddr;
    private final int outboundArenaCap;
    private final byte[] outboundTupleFlags;
    // Tuple ring. Kind / flags / arena offset / payload len are parallel
    // primitive arrays indexed by a head pointer + live count; arenas and
    // tuple caps are zero on streams built via the default constructor.
    private final byte[] outboundTupleKinds;
    private final int[] outboundTuplePayloadLens;
    private final int outboundTupleCap;
    private final int[] outboundTupleOffsets;
    // Per-stream pseudo-header staging. Native buffer owns the captured
    // bytes for :method / :path / :scheme / :authority / Host; the two
    // primitive arrays carry offsets and lengths into that buffer. A slot
    // with {@code stagingSlotLen[i] == 0} is absent. {@link #stagingOverflow}
    // latches when {@code captureSlot} cannot fit a value — the engine
    // converts that into a stream-level {@code PROTOCOL_ERROR} reset, so the
    // overflow is always observed at end-of-block, never mid-capture.
    private final long stagingAddr;
    private final int stagingCap;
    private final int[] stagingSlotLens = new int[STAGING_SLOT_COUNT];
    private final int[] stagingSlotOffsets = new int[STAGING_SLOT_COUNT];
    private int generation;
    private long inboundStreamWindow;
    // A second HEADERS on the same stream carries trailers (RFC 9113 sec. 8.1);
    // the first HEADERS carries the request pseudo-headers. The §11 dispatch
    // branches on this, and the pool preserves it only for the lifetime of
    // one LIVE assignment — recycle() clears it.
    private boolean initialHeadersSeen;
    private boolean outboundArenaClosed;
    private int outboundArenaWriteOffset;
    // §4.4 END_STREAM carry: once a tuple with the END_STREAM flag is
    // enqueued, further enqueues are rejected. Cleared on {@link #recycle}
    // and {@link #clearOutboundQueue}.
    private boolean outboundEndStreamStaged;
    // Bytes already emitted from the head tuple's payload. Lets the
    // scheduler split a DATA tuple that exceeds peer MAX_FRAME_SIZE
    // (or is larger than the available flow-control window) across
    // multiple wire frames without fragmenting the stored tuple. Resets
    // to 0 on {@link #dequeueOutbound} and {@link #clearOutboundQueue}.
    private int outboundHeadOffset;
    // §4.5 park marker used by the `onStreamWritable` callback. Set by the
    // engine when enqueueData / emitResponseHeaders return PARK, cleared
    // by the scheduler after a successful DATA emission drains the arena
    // back below the cap.
    private boolean outboundParked;
    private int outboundQueuedPayloadBytes;
    private long outboundStreamWindow;
    private int outboundTupleCount;
    private int outboundTupleHead;
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
    // Staging buffer bump cursor (bytes written) and overflow latch. Both
    // reset on {@link #beginInitialHeaders}.
    private boolean stagingOverflow;
    private int stagingWritePos;
    private Http2StreamState state = Http2StreamState.IDLE;
    private int streamId = -1;

    /**
     * Constructs a stream with no outbound arena and no header-staging
     * buffer. Exposed for unit tests that only exercise the inbound / FSM
     * surface — attempting to enqueue an outbound tuple or capture a
     * pseudo-header on such a stream returns {@code false}.
     * Production code uses {@link #Http2Stream(int, int, int)}.
     */
    public Http2Stream() {
        this(0, 0, 0);
    }

    /**
     * Constructs a stream with an outbound arena but no header-staging
     * buffer. Retained for backward compatibility with callers that were
     * written before staging capture landed; those streams cannot route
     * inbound HEADERS through the capturing listener (captures fail
     * silently). Production code uses {@link #Http2Stream(int, int, int)}.
     *
     * @param outboundArenaCap size in bytes of the native copy-on-enqueue
     *                         arena, or {@code 0} to disable outbound
     *                         staging entirely
     * @param outboundTupleCap maximum live outbound tuples, or {@code 0}
     *                         to disable outbound staging entirely
     */
    public Http2Stream(int outboundArenaCap, int outboundTupleCap) {
        this(outboundArenaCap, outboundTupleCap, 0);
    }

    /**
     * Constructs a stream whose outbound response arena spans
     * {@code outboundArenaCap} bytes, whose outbound tuple ring admits
     * {@code outboundTupleCap} frame tuples at once, and whose pseudo-
     * header staging buffer spans {@code stagingCap} bytes. All three caps
     * are fixed at construction and reused across {@link #recycle} calls;
     * hitting any of them causes the corresponding enqueue / capture path
     * to fail fast.
     *
     * @param outboundArenaCap size in bytes of the native copy-on-enqueue
     *                         arena, or {@code 0} to disable outbound
     *                         staging entirely
     * @param outboundTupleCap maximum live outbound tuples, or {@code 0}
     *                         to disable outbound staging entirely
     * @param stagingCap       size in bytes of the pseudo-header staging
     *                         buffer, or {@code 0} to disable capture
     */
    public Http2Stream(int outboundArenaCap, int outboundTupleCap, int stagingCap) {
        if (outboundArenaCap < 0) {
            throw new IllegalArgumentException("outboundArenaCap must be >= 0: " + outboundArenaCap);
        }
        if (outboundTupleCap < 0) {
            throw new IllegalArgumentException("outboundTupleCap must be >= 0: " + outboundTupleCap);
        }
        if (stagingCap < 0) {
            throw new IllegalArgumentException("stagingCap must be >= 0: " + stagingCap);
        }
        // Both outbound caps must either be zero (outbound disabled) or
        // both non-zero (outbound enabled). A single-sided configuration
        // would leave the queue unable to emit usefully.
        if ((outboundArenaCap == 0) != (outboundTupleCap == 0)) {
            throw new IllegalArgumentException(
                    "outboundArenaCap and outboundTupleCap must both be zero or both be positive");
        }
        this.outboundArenaCap = outboundArenaCap;
        this.outboundTupleCap = outboundTupleCap;
        this.stagingCap = stagingCap;
        long arenaAddr = 0L;
        long stagingBuf = 0L;
        byte[] kinds = null;
        byte[] flags = null;
        int[] offsets = null;
        int[] payloadLens = null;
        if (outboundArenaCap > 0) {
            arenaAddr = Unsafe.malloc(outboundArenaCap, MemoryTag.NATIVE_DEFAULT);
            try {
                kinds = new byte[outboundTupleCap];
                flags = new byte[outboundTupleCap];
                offsets = new int[outboundTupleCap];
                payloadLens = new int[outboundTupleCap];
            } catch (Throwable t) {
                Unsafe.free(arenaAddr, outboundArenaCap, MemoryTag.NATIVE_DEFAULT);
                throw t;
            }
        }
        if (stagingCap > 0) {
            try {
                stagingBuf = Unsafe.malloc(stagingCap, MemoryTag.NATIVE_HTTP_CONN);
            } catch (Throwable t) {
                if (arenaAddr != 0L) {
                    Unsafe.free(arenaAddr, outboundArenaCap, MemoryTag.NATIVE_DEFAULT);
                }
                throw t;
            }
        }
        this.outboundArenaAddr = arenaAddr;
        this.outboundTupleKinds = kinds;
        this.outboundTupleFlags = flags;
        this.outboundTupleOffsets = offsets;
        this.outboundTuplePayloadLens = payloadLens;
        this.stagingAddr = stagingBuf;
        for (int i = 0; i < STAGING_SLOT_COUNT; i++) {
            this.stagingSlotOffsets[i] = -1;
        }
    }

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
    /**
     * Consumes {@code n} bytes from the head tuple's payload. If the
     * advance reaches the tuple's {@code payloadLen} (including the
     * {@code n == 0} case on a zero-length tuple) the head is fully
     * dequeued via {@link #dequeueOutbound}; otherwise the internal
     * {@code outboundHeadOffset} advances and the next
     * {@link #peekOutboundPayloadAddr} / {@link #peekOutboundPayloadLen}
     * reflects the remaining bytes.
     *
     * @throws IllegalArgumentException if {@code n} is negative or would
     *                                  advance past the tuple's
     *                                  {@code payloadLen}
     * @throws IllegalStateException    if the queue is empty
     */
    public void advanceOutboundHead(int n) {
        requireOutboundNonEmpty();
        int payloadLen = outboundTuplePayloadLens[outboundTupleHead];
        if (n < 0 || outboundHeadOffset + n > payloadLen) {
            throw new IllegalArgumentException("advanceOutboundHead out of range: n=" + n
                    + " headOffset=" + outboundHeadOffset + " payloadLen=" + payloadLen);
        }
        int newOffset = outboundHeadOffset + n;
        if (newOffset == payloadLen) {
            dequeueOutbound();
        } else {
            outboundHeadOffset = newOffset;
        }
    }

    public void beginHeaderBlock() {
        refusingCurrentBlock = false;
    }

    /**
     * Resets the pseudo-header staging slots + bump cursor ahead of a new
     * request's initial HEADERS block. Trailer blocks do not call this —
     * the initial-HEADERS capture remains visible through
     * {@link Http2RequestHeadersView}-facing reads for the whole request
     * lifetime. The engine invokes this from {@code beginBlockAssembly}
     * when the block is the stream's first HEADERS (§5 IDLE → OPEN edge).
     */
    public void beginInitialHeaders() {
        stagingWritePos = 0;
        stagingOverflow = false;
        for (int i = 0; i < STAGING_SLOT_COUNT; i++) {
            stagingSlotOffsets[i] = -1;
            stagingSlotLens[i] = 0;
        }
    }

    /**
     * Captures {@code valueLen} bytes from {@code valueAddr} into the
     * pseudo-header staging buffer under the given {@link #STAGING_SLOT_METHOD}
     * / {@link #STAGING_SLOT_PATH} / {@link #STAGING_SLOT_SCHEME} /
     * {@link #STAGING_SLOT_AUTHORITY} / {@link #STAGING_SLOT_CONTENT_TYPE}
     * index.
     * A slot may be overwritten — later captures win, matching the RFC
     * 9113 §8.3 rule that the last instance of a pseudo-header wins after
     * decompression. Latches {@link #isStagingOverflow} and leaves the
     * slot unchanged on overflow; the engine then rejects the block.
     * <p>
     * Returns {@code true} when the value was captured, {@code false} on
     * overflow or if the stream has no staging buffer.
     */
    public boolean captureHeaderSlot(int slot, long valueAddr, int valueLen) {
        if (slot < 0 || slot >= STAGING_SLOT_COUNT) {
            throw new IllegalArgumentException("staging slot out of range: " + slot);
        }
        if (stagingCap == 0) {
            stagingOverflow = true;
            return false;
        }
        if (valueLen < 0) {
            throw new IllegalArgumentException("valueLen must be non-negative: " + valueLen);
        }
        if (stagingWritePos + valueLen > stagingCap) {
            stagingOverflow = true;
            return false;
        }
        if (valueLen > 0) {
            Unsafe.getUnsafe().copyMemory(valueAddr, stagingAddr + stagingWritePos, valueLen);
        }
        stagingSlotOffsets[slot] = stagingWritePos;
        stagingSlotLens[slot] = valueLen;
        stagingWritePos += valueLen;
        return true;
    }

    /**
     * Drops every queued outbound tuple and resets the arena bump cursor
     * back to the start of the native buffer. Called by the engine when
     * the stream is reset (peer or local {@code RST_STREAM}) so the
     * scheduler will not emit bytes that the peer has already abandoned,
     * and when the pool recycles the slot so a new occupant does not
     * inherit stale tuples from the prior request.
     * <p>
     * Leaves the arena allocation itself in place — only {@link #close}
     * returns native memory to the allocator.
     */
    public void clearOutboundQueue() {
        outboundTupleCount = 0;
        outboundTupleHead = 0;
        outboundQueuedPayloadBytes = 0;
        outboundArenaWriteOffset = 0;
        outboundHeadOffset = 0;
        outboundEndStreamStaged = false;
        outboundParked = false;
    }

    /**
     * Releases the per-stream native arena. Idempotent. Called by
     * {@link Http2StreamPool#close} at connection teardown; after
     * {@code close} returns every {@link #tryEnqueueOutbound} call on
     * this instance rejects with {@code false}.
     */
    @Override
    public void close() {
        if (outboundArenaClosed) {
            return;
        }
        outboundArenaClosed = true;
        clearOutboundQueue();
        if (outboundArenaAddr != 0L) {
            Unsafe.free(outboundArenaAddr, outboundArenaCap, MemoryTag.NATIVE_DEFAULT);
        }
        if (stagingAddr != 0L) {
            Unsafe.free(stagingAddr, stagingCap, MemoryTag.NATIVE_HTTP_CONN);
        }
    }

    /**
     * Removes the head tuple from the outbound queue. The scheduler calls
     * this after it has copied the tuple's payload into the send buffer
     * and committed the matching wire frame; subsequent {@link #peekOutboundKind}
     * and friends surface the next tuple (if any). When the last tuple
     * drains the arena bump cursor resets to {@code 0} so the next
     * enqueue starts fresh.
     */
    public void dequeueOutbound() {
        if (outboundTupleCount == 0) {
            throw new IllegalStateException("dequeue from empty outbound queue");
        }
        int slot = outboundTupleHead;
        int payloadLen = outboundTuplePayloadLens[slot];
        outboundQueuedPayloadBytes -= payloadLen;
        outboundTupleHead = (outboundTupleHead + 1) % outboundTupleCap;
        outboundTupleCount--;
        outboundHeadOffset = 0;
        if (outboundTupleCount == 0) {
            outboundArenaWriteOffset = 0;
        }
    }

    public int getGeneration() {
        return generation;
    }

    public long getInboundStreamWindow() {
        return inboundStreamWindow;
    }

    public long getOutboundArenaAddr() {
        return outboundArenaAddr;
    }

    public int getOutboundArenaCap() {
        return outboundArenaCap;
    }

    public int getOutboundHeadOffset() {
        return outboundHeadOffset;
    }

    public int getOutboundQueuedPayloadBytes() {
        return outboundQueuedPayloadBytes;
    }

    public long getOutboundStreamWindow() {
        return outboundStreamWindow;
    }

    public int getOutboundTupleCap() {
        return outboundTupleCap;
    }

    public int getOutboundTupleCount() {
        return outboundTupleCount;
    }

    public long getOutstandingInboundCredit() {
        return outstandingInboundCredit;
    }

    /**
     * Returns the native address of the captured pseudo-header value for
     * the given slot, or {@code 0} when the slot is absent. The returned
     * address is stable until the next {@link #beginInitialHeaders} —
     * typically that means the end of the current request's dispatch.
     */
    public long getStagingSlotAddr(int slot) {
        if (slot < 0 || slot >= STAGING_SLOT_COUNT) {
            throw new IllegalArgumentException("staging slot out of range: " + slot);
        }
        final int offset = stagingSlotOffsets[slot];
        return offset < 0 ? 0L : stagingAddr + offset;
    }

    public int getStagingSlotLen(int slot) {
        if (slot < 0 || slot >= STAGING_SLOT_COUNT) {
            throw new IllegalArgumentException("staging slot out of range: " + slot);
        }
        return stagingSlotLens[slot];
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
     * Returns {@code true} once a tuple carrying the {@code END_STREAM}
     * flag has been appended via {@link #tryEnqueueOutbound}. Further
     * enqueues are rejected until {@link #clearOutboundQueue} or
     * {@link #recycle} runs.
     */
    public boolean isOutboundEndStreamStaged() {
        return outboundEndStreamStaged;
    }

    /**
     * Returns {@code true} if the engine has marked this stream as
     * parked on the outbound side — i.e. a prior {@code enqueueData} /
     * {@code emitResponseHeaders} returned {@code ENQUEUE_PARK}. The flag
     * is cleared by the scheduler when the queue drains back under the
     * per-stream cap and the engine fires
     * {@link Http2StreamListener#onStreamWritable}.
     */
    public boolean isOutboundParked() {
        return outboundParked;
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
     * Returns {@code true} if the current request's pseudo-header capture
     * latched overflow. The engine checks this at end-of-block and resets
     * the stream with {@link Http2ErrorCode#PROTOCOL_ERROR} when set; the
     * adapter layer therefore never sees a capture that silently dropped a
     * slot. Cleared on the next {@link #beginInitialHeaders}.
     */
    public boolean isStagingOverflow() {
        return stagingOverflow;
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
     * Returns the flag byte of the head outbound tuple.
     * See {@link #TUPLE_FLAG_END_STREAM} / {@link #TUPLE_FLAG_END_HEADERS}.
     * Throws {@link IllegalStateException} when the queue is empty.
     */
    public byte peekOutboundFlags() {
        requireOutboundNonEmpty();
        return outboundTupleFlags[outboundTupleHead];
    }

    /**
     * Returns the kind of the head outbound tuple
     * ({@link #TUPLE_KIND_HEADERS}, {@link #TUPLE_KIND_CONTINUATION}, or
     * {@link #TUPLE_KIND_DATA}). Throws {@link IllegalStateException}
     * when the queue is empty.
     */
    public byte peekOutboundKind() {
        requireOutboundNonEmpty();
        return outboundTupleKinds[outboundTupleHead];
    }

    /**
     * Returns the native address of the next byte to emit from the head
     * outbound tuple — i.e. the tuple's arena address plus any bytes
     * already consumed by a prior {@link #advanceOutboundHead}. For a
     * freshly-enqueued tuple this equals the tuple's original arena
     * start; after a partial emit it points past the already-sent
     * bytes.
     */
    public long peekOutboundPayloadAddr() {
        requireOutboundNonEmpty();
        return outboundArenaAddr + outboundTupleOffsets[outboundTupleHead] + outboundHeadOffset;
    }

    /**
     * Returns the number of bytes remaining to emit from the head
     * outbound tuple — i.e. the tuple's {@code payloadLen} minus any
     * bytes already consumed via {@link #advanceOutboundHead}. For a
     * zero-length tuple this is always {@code 0}. Throws
     * {@link IllegalStateException} when the queue is empty.
     */
    public int peekOutboundPayloadLen() {
        requireOutboundNonEmpty();
        return outboundTuplePayloadLens[outboundTupleHead] - outboundHeadOffset;
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
        clearOutboundQueue();
        beginInitialHeaders();
        this.generation++;
    }

    /**
     * Toggles the park flag. Only the connection-context layer should
     * call this: set it on {@code enqueueData} / {@code emitResponseHeaders}
     * PARK return, clear it on the scheduler's first successful emission
     * after a window or cap unblock. Exposed as public so the scheduler
     * and unit tests in the {@code io.questdb.test.cutlass.http2} package
     * can assert on its observable state.
     */
    public void setOutboundParked(boolean parked) {
        this.outboundParked = parked;
    }

    /**
     * Attempts to append an outbound frame tuple whose payload bytes are
     * copied from {@code srcAddr} into this stream's arena. Returns
     * {@code true} on success; returns {@code false} without mutating
     * state when:
     * <ul>
     *   <li>the arena has been {@link #close closed};</li>
     *   <li>the arena is not configured on this stream (cap == 0);</li>
     *   <li>a prior tuple carrying {@code END_STREAM} has already been
     *       enqueued ({@link #isOutboundEndStreamStaged} is true);</li>
     *   <li>the tuple ring is full ({@code getOutboundTupleCount ==
     *       getOutboundTupleCap});</li>
     *   <li>appending {@code payloadLen} bytes would push
     *       {@code getOutboundQueuedPayloadBytes} past
     *       {@code getOutboundArenaCap};</li>
     *   <li>the arena bump cursor cannot fit {@code payloadLen} bytes
     *       without a reset and the queue is non-empty (bump-only
     *       allocator — wrap-around fragmentation support may land in a
     *       follow-up if workloads justify it).</li>
     * </ul>
     * The caller's buffer is free to be reused immediately on return —
     * §4.4 copy-on-enqueue ownership rule.
     *
     * @param kind       one of {@link #TUPLE_KIND_HEADERS},
     *                   {@link #TUPLE_KIND_CONTINUATION},
     *                   {@link #TUPLE_KIND_DATA}
     * @param flags      bitset of {@link #TUPLE_FLAG_END_STREAM} (valid
     *                   on HEADERS and DATA) and
     *                   {@link #TUPLE_FLAG_END_HEADERS} (valid on
     *                   HEADERS and CONTINUATION). Unknown bits are
     *                   stored verbatim; the scheduler interprets the
     *                   two defined bits only.
     * @param srcAddr    native address of the payload bytes; ignored
     *                   when {@code payloadLen == 0}
     * @param payloadLen payload byte count; must be non-negative
     */
    public boolean tryEnqueueOutbound(byte kind, byte flags, long srcAddr, int payloadLen) {
        if (payloadLen < 0) {
            throw new IllegalArgumentException("payloadLen must be non-negative: " + payloadLen);
        }
        if (kind != TUPLE_KIND_HEADERS && kind != TUPLE_KIND_CONTINUATION
                && kind != TUPLE_KIND_DATA && kind != TUPLE_KIND_TRAILERS) {
            throw new IllegalArgumentException("unknown outbound tuple kind: " + kind);
        }
        if (outboundArenaClosed || outboundArenaCap == 0) {
            return false;
        }
        if (outboundEndStreamStaged) {
            return false;
        }
        if (outboundTupleCount >= outboundTupleCap) {
            return false;
        }
        if (payloadLen > outboundArenaCap - outboundQueuedPayloadBytes) {
            return false;
        }
        if (payloadLen > outboundArenaCap - outboundArenaWriteOffset) {
            // The remaining arena tail is too small. Legal only when the
            // queue has drained, in which case the bump cursor resets. A
            // partial-drain state prevents reset and forces PARK.
            if (outboundTupleCount != 0) {
                return false;
            }
            outboundArenaWriteOffset = 0;
        }
        int offset = outboundArenaWriteOffset;
        if (payloadLen > 0) {
            Unsafe.getUnsafe().copyMemory(srcAddr, outboundArenaAddr + offset, payloadLen);
        }
        int slot = (outboundTupleHead + outboundTupleCount) % outboundTupleCap;
        outboundTupleKinds[slot] = kind;
        outboundTupleFlags[slot] = flags;
        outboundTupleOffsets[slot] = offset;
        outboundTuplePayloadLens[slot] = payloadLen;
        outboundTupleCount++;
        outboundArenaWriteOffset = offset + payloadLen;
        outboundQueuedPayloadBytes += payloadLen;
        if ((flags & TUPLE_FLAG_END_STREAM) != 0) {
            outboundEndStreamStaged = true;
        }
        return true;
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
        clearOutboundQueue();
        beginInitialHeaders();
    }

    private void requireOutboundNonEmpty() {
        if (outboundTupleCount == 0) {
            throw new IllegalStateException("outbound queue is empty");
        }
    }
}
