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

import io.questdb.cutlass.hpack.HpackDecoder;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.hpack.HpackException;
import io.questdb.cutlass.hpack.HpackListener;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Per-connection HTTP/2 state machine driver (see
 * {@code STREAM_STATE_MACHINE.md} §4 and §10).
 * <p>
 * Owns the frame reader, both HPACK codecs, the stream pool, connection-level
 * flow-control windows, SETTINGS tracking, the HEADERS / CONTINUATION
 * block-assembly scratch, and the peer-stream-id counters that enforce the
 * monotonic rule (§6) and supply the GOAWAY last-stream-id.
 * <p>
 * Not thread-safe; one instance per connection, driven by the owning I/O
 * worker. Callers read bytes from the socket into a caller-owned native
 * receive buffer and hand the address range to
 * {@link #processReceivedBytes}; outbound control frames queued by the
 * dispatch layer drain through {@link #writePending} into a caller-owned
 * send buffer, mirroring the backpressure model of
 * {@code Http2FrameWriter}.
 * <p>
 * Milestone 1 scope: connection-scoped frames (SETTINGS, PING,
 * WINDOW_UPDATE, RST_STREAM, GOAWAY) plus PRIORITY discard. HEADERS /
 * CONTINUATION / DATA routing with the HPACK listener and §11 validator
 * surface land in a follow-up per §14.3 steps 7 and 9.
 */
public final class Http2ConnectionContext implements Closeable {

    public static final byte STATE_ACTIVE = 0;
    public static final byte STATE_CLOSED = 2;
    public static final byte STATE_DRAINING = 1;

    // M1 listener that drives the HPACK decoder to END_HEADERS without
    // surfacing fields. The §11 validator surface (pseudo-header rules,
    // staging, policy counter, forbidden-header / content-length checks)
    // lands in a follow-up commit; for now the decoder still must run so
    // the shared dynamic table stays coherent.
    private static final HpackListener NOOP_HPACK_LISTENER =
            (nameAddr, nameLen, valueAddr, valueLen, neverIndexed) -> {
            };
    // Placeholder view handed to {@link Http2StreamListener#onRequestHeaders}
    // until pseudo-header capture lands. Every accessor returns the absent
    // sentinel {@code (0, 0)}; handler code should not rely on slot
    // presence in M1.
    private static final Http2RequestHeadersView EMPTY_REQUEST_HEADERS =
            new EmptyRequestHeadersView();

    private static final byte PENDING_GOAWAY = 3;
    private static final int PENDING_QUEUE_CAP = 16;
    private static final byte PENDING_PING_ACK = 2;
    private static final byte PENDING_RST_STREAM = 4;
    private static final byte PENDING_SETTINGS_ACK = 1;
    private static final byte PENDING_WINDOW_UPDATE = 5;
    // Slot 0 unused; indices 1..6 match the wire SETTINGS identifier codes
    // (HEADER_TABLE_SIZE..MAX_HEADER_LIST_SIZE). A flat array keyed by wire
    // id lets the SETTINGS dispatcher read-modify-write without a mapping
    // table, while reserving slot 0 keeps index arithmetic safe.
    private static final int SETTINGS_COUNT = 7;

    private final long blockAssemblyScratchAddr;
    private final int blockAssemblyScratchCap;
    private final Http2ConnectionConfig config;
    private final Http2FrameHeader frameHeader = new Http2FrameHeader();
    private final HpackDecoder hpackDecoder;
    private final HpackEncoder hpackEncoder;
    private final Http2StreamListener listener;
    private final long[] ourAdvertised = new long[SETTINGS_COUNT];
    private final long[] ourApplied = new long[SETTINGS_COUNT];
    private final long[] peerAdvertised = new long[SETTINGS_COUNT];
    // Pending outbound control frames queued by dispatch. Small FIFO; a
    // typical call to processReceivedBytes enqueues 0-2 entries.
    private final byte[] pendingKind = new byte[PENDING_QUEUE_CAP];
    private final long[] pendingOpaque = new long[PENDING_QUEUE_CAP];
    private final int[] pendingParam = new int[PENDING_QUEUE_CAP];
    private final int[] pendingStreamId = new int[PENDING_QUEUE_CAP];
    private final Http2FrameReader reader = new Http2FrameReader();
    private final Http2StreamPool streamPool;
    // HEADERS / CONTINUATION block assembly (§11 connection-scoped scratch).
    // blockAssemblyStreamId == -1 means no block is currently in flight;
    // during assembly it names the single stream whose block is being
    // accumulated (CONTINUATION sequencing guarantees one at a time).
    private boolean blockAssemblyEndStream;
    private int blockAssemblyLen;
    private int blockAssemblySlot = -1;
    private int blockAssemblyStreamId = -1;
    private boolean closed;
    private int highestPeerStreamIdSeen;
    private long inboundConnectionWindow;
    private boolean initialSettingsEmitted;
    private int lastAcceptedPeerStreamId;
    private long outboundConnectionWindow;
    private int pendingCount;
    // Bitset of SETTINGS identifier ids carried on our most-recent emitted
    // SETTINGS frame but not yet acknowledged by the peer. The ACK handler
    // applies changes only for bits set here — preventing settings we never
    // emitted (e.g. MAX_HEADER_LIST_SIZE in M1) from being clobbered to
    // zero via the ourAdvertised-vs-ourApplied diff. Cleared on ACK.
    private int pendingSettingsBits;
    private boolean settingsFrameOutstanding;
    private byte state = STATE_ACTIVE;

    public Http2ConnectionContext(Http2StreamListener listener, Http2ConnectionConfig config) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must be non-null");
        }
        if (config == null) {
            throw new IllegalArgumentException("config must be non-null");
        }
        this.listener = listener;
        this.config = config;
        this.streamPool = new Http2StreamPool(config.ourMaxConcurrentStreams, config.tombstoneCap);
        // Pool capacity must cover every cap we might advertise via
        // SETTINGS_HEADER_TABLE_SIZE plus the HTTP/2 default (peer's pre-ACK
        // encoder operates at the default 4 KiB regardless of our config).
        int hpackPool = Math.max(
                Math.max(Http2Settings.DEFAULT_HEADER_TABLE_SIZE, config.hpackPoolCapacityBytes),
                config.ourHeaderTableSize);
        this.hpackDecoder = new HpackDecoder(
                Http2Settings.DEFAULT_HEADER_TABLE_SIZE,
                hpackPool,
                config.hpackDecoderScratchBytes
        );
        this.hpackEncoder = new HpackEncoder(
                Http2Settings.DEFAULT_HEADER_TABLE_SIZE,
                0, // Milestone 1 posture: selectedMax pinned at 0 (HPACK_CODEC.md §16.1).
                hpackPool,
                config.headerListPolicyCap,
                config.hpackEncoderBufferBytes
        );
        this.blockAssemblyScratchCap = config.blockAssemblyScratchBytes;
        this.blockAssemblyScratchAddr = Unsafe.malloc(config.blockAssemblyScratchBytes, MemoryTag.NATIVE_DEFAULT);

        initSettings();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        streamPool.clear();
        hpackDecoder.close();
        hpackEncoder.close();
        Unsafe.free(blockAssemblyScratchAddr, blockAssemblyScratchCap, MemoryTag.NATIVE_DEFAULT);
        state = STATE_CLOSED;
    }

    /**
     * Emits our initial SETTINGS frame into the caller-owned send buffer.
     * Carries our advertised {@code HEADER_TABLE_SIZE},
     * {@code ENABLE_PUSH = 0}, {@code MAX_CONCURRENT_STREAMS},
     * {@code INITIAL_WINDOW_SIZE}, and {@code MAX_FRAME_SIZE} per §10. M1
     * does not advertise {@code MAX_HEADER_LIST_SIZE} on the wire (§11 M1
     * posture) but enforces its policy value internally.
     * <p>
     * Sets {@code settingsFrameOutstanding} so further SETTINGS changes
     * queue per §10's outstanding discipline until the ACK lands. Returns
     * {@code -1} if the caller buffer cannot hold the full frame; the
     * caller drains and retries.
     */
    public long emitInitialSettings(long addr, long limit) {
        if (initialSettingsEmitted) {
            throw new IllegalStateException("emitInitialSettings already called on this context");
        }
        short[] ids = new short[5];
        int[] values = new int[5];
        int n = 0;
        ids[n] = Http2Settings.HEADER_TABLE_SIZE;
        values[n++] = config.ourHeaderTableSize;
        ids[n] = Http2Settings.ENABLE_PUSH;
        values[n++] = 0;
        ids[n] = Http2Settings.MAX_CONCURRENT_STREAMS;
        values[n++] = config.ourMaxConcurrentStreams;
        ids[n] = Http2Settings.INITIAL_WINDOW_SIZE;
        values[n++] = config.ourInitialWindowSize;
        ids[n] = Http2Settings.MAX_FRAME_SIZE;
        values[n++] = config.ourMaxFrameSize;
        long written = Http2FrameWriter.writeSettings(addr, limit, ids, values, n);
        if (written < 0) {
            return -1;
        }
        // ourAdvertised updates per §10: every emitted SETTINGS sets
        // ourAdvertised[id] = newValue for every identifier it carries.
        // Pure-immediate identifiers also update ourApplied immediately.
        // Widen-immediately / tighten-on-ACK identifiers update ourApplied
        // on emit only when newValue >= current. ACK-gated identifiers
        // leave ourApplied at the old value until the peer ACKs.
        ourAdvertised[Http2Settings.HEADER_TABLE_SIZE] = config.ourHeaderTableSize;
        ourAdvertised[Http2Settings.ENABLE_PUSH] = 0;
        ourApplied[Http2Settings.ENABLE_PUSH] = 0;
        ourAdvertised[Http2Settings.MAX_CONCURRENT_STREAMS] = config.ourMaxConcurrentStreams;
        ourApplied[Http2Settings.MAX_CONCURRENT_STREAMS] = config.ourMaxConcurrentStreams;
        ourAdvertised[Http2Settings.INITIAL_WINDOW_SIZE] = config.ourInitialWindowSize;
        ourAdvertised[Http2Settings.MAX_FRAME_SIZE] = config.ourMaxFrameSize;
        if (config.ourMaxFrameSize >= ourApplied[Http2Settings.MAX_FRAME_SIZE]) {
            ourApplied[Http2Settings.MAX_FRAME_SIZE] = config.ourMaxFrameSize;
        }
        // Record every id carried on the wire so the ACK handler applies
        // changes only for those bits (avoids the "omitted MAX_HEADER_LIST_SIZE
        // diffs against ourAdvertised=0 and clobbers ourApplied" regression).
        pendingSettingsBits = (1 << Http2Settings.HEADER_TABLE_SIZE)
                | (1 << Http2Settings.ENABLE_PUSH)
                | (1 << Http2Settings.MAX_CONCURRENT_STREAMS)
                | (1 << Http2Settings.INITIAL_WINDOW_SIZE)
                | (1 << Http2Settings.MAX_FRAME_SIZE);
        settingsFrameOutstanding = true;
        initialSettingsEmitted = true;
        return written;
    }

    public int getActiveStreamCount() {
        return streamPool.getActiveStreamCount();
    }

    public int getHighestPeerStreamIdSeen() {
        return highestPeerStreamIdSeen;
    }

    public long getInboundConnectionWindow() {
        return inboundConnectionWindow;
    }

    public int getLastAcceptedPeerStreamId() {
        return lastAcceptedPeerStreamId;
    }

    public long getOurAdvertised(short settingId) {
        return ourAdvertised[settingId];
    }

    public long getOurApplied(short settingId) {
        return ourApplied[settingId];
    }

    public long getOutboundConnectionWindow() {
        return outboundConnectionWindow;
    }

    public long getPeerAdvertised(short settingId) {
        return peerAdvertised[settingId];
    }

    public int getPendingCount() {
        return pendingCount;
    }

    public byte getState() {
        return state;
    }

    public Http2StreamPool getStreamPool() {
        return streamPool;
    }

    public boolean isSettingsFrameOutstanding() {
        return settingsFrameOutstanding;
    }

    /**
     * Acks {@code n} bytes of deferred {@code DATA} previously delivered to
     * the handler on a {@code listener.onData(..., generationToken)} that
     * returned {@code false}. The context credits {@code n} to both the
     * connection inbound window and the per-stream inbound window (when
     * that direction is still active), and queues the matching coalesced
     * {@code WINDOW_UPDATE} frames for emission via {@link #writePending}.
     * <p>
     * §7 step 6 guards:
     * <ul>
     *   <li>{@code streamId} must resolve to a LIVE slot with
     *       {@code generation == generationToken}. A stale token (from a
     *       prior occupant, after recycle or tombstone promotion) is a
     *       silent no-op.</li>
     *   <li>{@code 0 < n && n <= outstandingInboundCredit}. Double-acks,
     *       zero / negative / over-acks are silent no-ops so a buggy
     *       handler cannot inflate the flow-control windows.</li>
     * </ul>
     */
    public void onBytesConsumed(int streamId, int generationToken, long n) {
        int slot = streamPool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            return;
        }
        if (streamPool.getSlotKind(slot) != Http2StreamPool.SLOT_LIVE) {
            return;
        }
        Http2Stream s = streamPool.getLiveStream(slot);
        if (s.getGeneration() != generationToken) {
            return;
        }
        long outstanding = s.getOutstandingInboundCredit();
        if (n <= 0 || n > outstanding) {
            return;
        }
        // n is bounded by outstanding, which in turn is bounded by the
        // cumulative DATA received (≤ 2^31 - 1 per RFC). Narrow safely.
        int increment = (int) n;
        s.consumeDeferredCredit(n);
        creditInboundConnectionWindow(increment);
        enqueueWindowUpdate(0, increment);
        if (s.isInboundDirectionActive()) {
            creditInboundStreamWindow(slot, increment);
            enqueueWindowUpdate(streamId, increment);
        }
    }

    /**
     * Processes as many fully-buffered frames as possible from
     * {@code [addr, limit)}. Returns the number of bytes consumed; the
     * caller compacts the receive buffer past that point before the next
     * read. Processing halts on connection-level errors, which enqueue a
     * {@code GOAWAY} and transition the context to {@link #STATE_CLOSED}
     * after the caller drains {@link #writePending}.
     */
    public long processReceivedBytes(long addr, long limit) {
        if (state == STATE_CLOSED) {
            return 0;
        }
        long cursor = addr;
        int inboundMaxFrameSize = (int) ourApplied[Http2Settings.MAX_FRAME_SIZE];
        if (inboundMaxFrameSize < Http2Settings.MAX_FRAME_SIZE_LOWER) {
            inboundMaxFrameSize = Http2Settings.MAX_FRAME_SIZE_LOWER;
        }
        while (cursor < limit && state != STATE_CLOSED) {
            int n;
            try {
                n = reader.tryReadNext(cursor, limit, frameHeader, inboundMaxFrameSize);
            } catch (Http2ConnectionException e) {
                enqueueGoAway(e.getErrorCode());
                state = STATE_CLOSED;
                break;
            } catch (Http2StreamException e) {
                // Stream-level structural error (e.g. self-dependent HEADERS,
                // PRIORITY length != 5). The reader only raises stream errors
                // after its buffered-check, so the full frame is guaranteed
                // to be in the caller's buffer and advancing by the header-
                // reported size is safe. `updateContinuationState` has also
                // already run before the reader throws, so a rejected
                // HEADERS frame with END_HEADERS=0 does not leave the
                // continuation tracker un-armed.
                resetStreamLocally(e.getStreamId(), e.getErrorCode());
                cursor += Http2FrameHeader.SIZE + frameHeader.getPayloadLength();
                continue;
            }
            if (n == 0) {
                break;
            }
            try {
                dispatch(frameHeader);
            } catch (Http2ConnectionException e) {
                enqueueGoAway(e.getErrorCode());
                state = STATE_CLOSED;
                break;
            } catch (Http2StreamException e) {
                resetStreamLocally(e.getStreamId(), e.getErrorCode());
            }
            cursor += n;
        }
        return cursor - addr;
    }

    /**
     * Drains the pending outbound control-frame queue into the caller-owned
     * send buffer {@code [addr, limit)}. Stops at the first frame that
     * cannot fit (buffer too small); the caller retries after the socket
     * write drains. Returns the new write pointer; unwritten frames remain
     * queued in FIFO order.
     */
    public long writePending(long addr, long limit) {
        long cursor = addr;
        int written = 0;
        for (int i = 0; i < pendingCount; i++) {
            long next = writeOne(cursor, limit, i);
            if (next < 0) {
                break;
            }
            cursor = next;
            written++;
        }
        if (written > 0) {
            int remaining = pendingCount - written;
            for (int i = 0; i < remaining; i++) {
                pendingKind[i] = pendingKind[i + written];
                pendingStreamId[i] = pendingStreamId[i + written];
                pendingParam[i] = pendingParam[i + written];
                pendingOpaque[i] = pendingOpaque[i + written];
            }
            pendingCount = remaining;
        }
        return cursor;
    }

    private int admitNewStream(int streamId) {
        // §6 monotonic-id validation. Client-initiated streams use odd ids
        // (RFC 9113 sec. 5.1.1); an even id from a client is a connection
        // protocol error. highestPeerStreamIdSeen only advances after this
        // admission path succeeds so a rejected HEADERS doesn't silently
        // reserve the id for later re-use.
        if (streamId <= highestPeerStreamIdSeen) {
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.PROTOCOL_ERROR, "HEADERS on non-monotonic stream id");
        }
        if ((streamId & 1) == 0) {
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.PROTOCOL_ERROR, "HEADERS on even (server-initiated) stream id");
        }
        highestPeerStreamIdSeen = streamId;
        // DRAINING: refuse new streams with RST_STREAM(REFUSED_STREAM) per §12.
        if (state == STATE_DRAINING) {
            int refusedSlot = streamPool.allocateDiscardingBlock(streamId, Http2ErrorCode.REFUSED_STREAM);
            if (refusedSlot == Http2StreamPool.SLOT_NOT_FOUND) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.INTERNAL_ERROR, "pool exhaustion on DRAINING HEADERS");
            }
            return refusedSlot;
        }
        long initialInbound = ourApplied[Http2Settings.INITIAL_WINDOW_SIZE];
        long initialOutbound = peerAdvertised[Http2Settings.INITIAL_WINDOW_SIZE];
        int slot = streamPool.allocateLive(streamId, initialInbound, initialOutbound);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            // Concurrency refused (§8): allocate a DISCARDING_BLOCK so the
            // HEADERS block HPACK-decodes for dynamic-table coherence; we
            // emit RST_STREAM(REFUSED_STREAM) at END_HEADERS via the
            // completeHeaderBlock DISCARDING branch.
            slot = streamPool.allocateDiscardingBlock(streamId, Http2ErrorCode.REFUSED_STREAM);
            if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.INTERNAL_ERROR, "pool exhaustion on concurrency-refused HEADERS");
            }
        } else {
            lastAcceptedPeerStreamId = streamId;
        }
        return slot;
    }

    private void appendBlockAssembly(long fragAddr, int fragLen) {
        if (fragLen < 0) {
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.PROTOCOL_ERROR, "negative HEADERS fragment length");
        }
        if (blockAssemblyLen + fragLen > blockAssemblyScratchCap) {
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.COMPRESSION_ERROR, "HEADERS block exceeds scratch capacity");
        }
        if (fragLen > 0) {
            Unsafe.getUnsafe().copyMemory(
                    fragAddr, blockAssemblyScratchAddr + blockAssemblyLen, fragLen);
            blockAssemblyLen += fragLen;
        }
    }

    private void appendFragmentFromHeaders(Http2FrameHeader header) {
        // Strips the wire wrappers (pad-length byte, PRIORITY bytes, trailing
        // padding) before appending the field-block fragment to the
        // block-assembly scratch. The frame reader has already verified that
        // pad-length + pad-flag/priority sizing does not overrun the payload.
        byte flags = header.getFlags();
        long payloadAddr = header.getPayloadAddr();
        int payloadLen = header.getPayloadLength();
        int offset = 0;
        int padLen = 0;
        if (Http2Flags.hasPadded(flags)) {
            padLen = Unsafe.getUnsafe().getByte(payloadAddr) & 0xFF;
            offset += 1;
        }
        if (Http2Flags.hasPriority(flags)) {
            offset += 5;
        }
        int fragmentLen = payloadLen - offset - padLen;
        appendBlockAssembly(payloadAddr + offset, fragmentLen);
    }

    private void applyPeerInitialWindowChange(long delta) {
        if (delta == 0) {
            return;
        }
        // §7: sweep outbound-direction-active streams (OPEN or HALF_CLOSED_REMOTE).
        // Pre-check for 2^31 - 1 overflow before any commit; a breach is
        // connection FLOW_CONTROL_ERROR.
        Http2StreamPool pool = streamPool;
        int slotCount = pool.getSlotCount();
        for (int i = 0; i < slotCount; i++) {
            if (pool.getSlotKind(i) != Http2StreamPool.SLOT_LIVE) {
                continue;
            }
            Http2Stream s = pool.getLiveStream(i);
            if (!s.isOutboundDirectionActive()) {
                continue;
            }
            long result = Http2FlowController.adjustOnInitialWindowChange(
                    s.getOutboundStreamWindow(), delta);
            if (result == Http2FlowController.OVERFLOW) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.FLOW_CONTROL_ERROR,
                        "INITIAL_WINDOW_SIZE delta overflows outbound stream window");
            }
        }
        // Second pass: commit.
        for (int i = 0; i < slotCount; i++) {
            if (pool.getSlotKind(i) != Http2StreamPool.SLOT_LIVE) {
                continue;
            }
            Http2Stream s = pool.getLiveStream(i);
            s.onOutboundInitialWindowDelta(delta);
        }
    }

    private void applyPeerSetting(short id, long value) {
        long previous = peerAdvertised[id];
        long delta = value - previous;
        switch (id) {
            case Http2Settings.HEADER_TABLE_SIZE:
                hpackEncoder.onPeerAdvertisedCapChanged(value);
                break;
            case Http2Settings.INITIAL_WINDOW_SIZE:
                applyPeerInitialWindowChange(delta);
                break;
            case Http2Settings.ENABLE_PUSH:
            case Http2Settings.MAX_CONCURRENT_STREAMS:
            case Http2Settings.MAX_FRAME_SIZE:
            case Http2Settings.MAX_HEADER_LIST_SIZE:
                // Plain field update; the per-setting effect is observed when
                // the peer's encoder / frame sizing / concurrency decisions
                // are next consulted.
                break;
            default:
                // Unreachable — caller filtered unknown ids.
        }
        peerAdvertised[id] = value;
    }

    private void beginBlockAssembly(int streamId, int slot, boolean endStream) {
        blockAssemblyStreamId = streamId;
        blockAssemblySlot = slot;
        blockAssemblyEndStream = endStream;
        blockAssemblyLen = 0;
        if (streamPool.getSlotKind(slot) == Http2StreamPool.SLOT_LIVE) {
            Http2Stream s = streamPool.getLiveStream(slot);
            // beginHeaderBlock() clears refusingCurrentBlock. A trailer
            // HEADERS sets that flag via markCurrentBlockRefused() AFTER
            // this call.
            s.beginHeaderBlock();
        }
    }

    private void cancelAllActiveStreams() {
        // §12: peer aborted rather than drained. RST_STREAM(CANCEL) each
        // LIVE stream so handler state releases now; the peer is about to
        // close the socket.
        Http2StreamPool pool = streamPool;
        int slotCount = pool.getSlotCount();
        for (int i = 0; i < slotCount; i++) {
            if (pool.getSlotKind(i) != Http2StreamPool.SLOT_LIVE) {
                continue;
            }
            int streamId = pool.getSlotStreamId(i);
            resetStreamLocally(streamId, Http2ErrorCode.CANCEL);
        }
    }

    private void completeHeaderBlock() {
        // Snapshot + reset assembly state before upward delivery so a
        // listener call re-entering dispatch (it shouldn't, but defensively)
        // would see a clean slate.
        int streamId = blockAssemblyStreamId;
        int slot = blockAssemblySlot;
        boolean endStream = blockAssemblyEndStream;
        long blockAddr = blockAssemblyScratchAddr;
        long blockLimit = blockAssemblyScratchAddr + blockAssemblyLen;
        blockAssemblyStreamId = -1;
        blockAssemblySlot = -1;
        blockAssemblyLen = 0;
        blockAssemblyEndStream = false;

        // HPACK decode always runs so the shared dynamic table stays
        // coherent even when the block is refused / discarded (§11
        // sticky-error rationale). A structural HPACK failure escalates
        // to a connection COMPRESSION_ERROR.
        try {
            hpackDecoder.decodeBlock(blockAddr, blockLimit, NOOP_HPACK_LISTENER);
        } catch (HpackException e) {
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.COMPRESSION_ERROR, e.getMessage());
        }

        byte kind = streamPool.getSlotKind(slot);
        if (kind == Http2StreamPool.SLOT_DISCARDING_BLOCK) {
            // Concurrency-refused or DRAINING-refused block. Promote to
            // LOCAL_RESET tombstone + emit RST_STREAM with the stashed
            // reason, then drop any in-flight DATA via §5 rule 2.
            int reason = streamPool.getDiscardingReason(slot);
            streamPool.promoteDiscardingToTombstone(slot);
            enqueueRstStream(streamId, reason);
            return;
        }
        if (kind != Http2StreamPool.SLOT_LIVE) {
            // TOMBSTONE / FREE — shouldn't reach completeHeaderBlock;
            // defensive no-op.
            return;
        }
        Http2Stream s = streamPool.getLiveStream(slot);
        if (s.isRefusingCurrentBlock()) {
            // §8 M1 trailer refusal. Reset the stream; the block decoded
            // above keeps HPACK state coherent.
            resetStreamLocally(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (!s.onRecvHeaders(endStream)) {
            // Malformed trailer (no END_STREAM) or bad state transition.
            resetStreamLocally(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        listener.onRequestHeaders(streamId, EMPTY_REQUEST_HEADERS, endStream);
        if (s.isClosed()) {
            // HEADERS+END_STREAM on HALF_CLOSED_LOCAL closes the stream
            // immediately. Settle any deferred credit (none expected on
            // a zero-body request, but defensive) and notify the handler.
            settleDeferredCredit(s);
            streamPool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
            listener.onStreamClosed(streamId, Http2ErrorCode.NO_ERROR);
        }
    }

    private void commitOurInitialWindowChange(long delta) {
        if (delta == 0) {
            return;
        }
        // §7: sweep inbound-direction-active streams (OPEN or HALF_CLOSED_LOCAL)
        // for the ACK of our SETTINGS change. Overflow → FLOW_CONTROL_ERROR.
        Http2StreamPool pool = streamPool;
        int slotCount = pool.getSlotCount();
        for (int i = 0; i < slotCount; i++) {
            if (pool.getSlotKind(i) != Http2StreamPool.SLOT_LIVE) {
                continue;
            }
            Http2Stream s = pool.getLiveStream(i);
            if (!s.isInboundDirectionActive()) {
                continue;
            }
            long result = Http2FlowController.adjustOnInitialWindowChange(
                    s.getInboundStreamWindow(), delta);
            if (result == Http2FlowController.OVERFLOW) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.FLOW_CONTROL_ERROR,
                        "INITIAL_WINDOW_SIZE delta overflows inbound stream window");
            }
        }
        for (int i = 0; i < slotCount; i++) {
            if (pool.getSlotKind(i) != Http2StreamPool.SLOT_LIVE) {
                continue;
            }
            Http2Stream s = pool.getLiveStream(i);
            if (!s.isInboundDirectionActive()) {
                continue;
            }
            s.setInboundStreamWindow(s.getInboundStreamWindow() + delta);
        }
    }

    private void creditInboundConnectionWindow(int increment) {
        long after = Http2FlowController.credit(inboundConnectionWindow, increment);
        // Overflow is theoretically impossible here — we only credit back
        // amounts we previously debited — but the flow-controller helper
        // returns its sentinel rather than throwing so we clamp defensively.
        if (after != Http2FlowController.OVERFLOW) {
            inboundConnectionWindow = after;
        }
    }

    private void creditInboundStreamWindow(int slot, int increment) {
        Http2Stream s = streamPool.getLiveStream(slot);
        long after = Http2FlowController.credit(s.getInboundStreamWindow(), increment);
        if (after != Http2FlowController.OVERFLOW) {
            s.setInboundStreamWindow(after);
        }
    }

    private void dispatch(Http2FrameHeader header) {
        switch (header.getType()) {
            case Http2FrameType.SETTINGS -> dispatchSettings(header);
            case Http2FrameType.PING -> dispatchPing(header);
            case Http2FrameType.WINDOW_UPDATE -> dispatchWindowUpdate(header);
            case Http2FrameType.RST_STREAM -> dispatchRstStream(header);
            case Http2FrameType.GOAWAY -> dispatchGoAway(header);
            case Http2FrameType.HEADERS -> dispatchHeaders(header);
            case Http2FrameType.CONTINUATION -> dispatchContinuation(header);
            case Http2FrameType.DATA -> dispatchData(header);
            case Http2FrameType.PRIORITY -> {
                // §9: parse + discard (RFC 9113 sec. 5.3.2 deprecates priority).
            }
            default -> {
                // RFC 7540 sec. 4.1: unknown frame types are discarded.
            }
        }
    }

    private void dispatchContinuation(Http2FrameHeader header) {
        int streamId = header.getStreamId();
        // Frame reader's continuation tracker has already enforced "same
        // stream id as the open HEADERS block"; the equality below is a
        // defence-in-depth assertion.
        if (streamId != blockAssemblyStreamId) {
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.PROTOCOL_ERROR, "CONTINUATION on unexpected stream id");
        }
        appendBlockAssembly(header.getPayloadAddr(), header.getPayloadLength());
        if (Http2Flags.hasEndHeaders(header.getFlags())) {
            completeHeaderBlock();
        }
    }

    private void dispatchData(Http2FrameHeader header) {
        int streamId = header.getStreamId();
        byte flags = header.getFlags();
        long payloadAddr = header.getPayloadAddr();
        int payloadLen = header.getPayloadLength();
        int dataOffset = 0;
        int padLen = 0;
        if (Http2Flags.hasPadded(flags)) {
            padLen = Unsafe.getUnsafe().getByte(payloadAddr) & 0xFF;
            dataOffset = 1;
        }
        int paddingOverhead = dataOffset + padLen;
        int dataLen = payloadLen - paddingOverhead;
        boolean endStream = Http2Flags.hasEndStream(flags);

        // §7 step 1: debit connection window against paddedLen, check
        // underflow immediately. paddedLen == payloadLen here — the reader
        // already rejected pad-length-exceeds-payload as a connection
        // PROTOCOL_ERROR.
        long afterDebit = Http2FlowController.debit(inboundConnectionWindow, payloadLen);
        inboundConnectionWindow = afterDebit;
        if (afterDebit < 0) {
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.FLOW_CONTROL_ERROR, "connection inbound window underflow");
        }
        // §7 step 2: credit padding back to connection window immediately.
        if (paddingOverhead > 0) {
            creditInboundConnectionWindow(paddingOverhead);
            enqueueWindowUpdate(0, paddingOverhead);
        }

        // §7 step 3: per-id lookup + stream-window accounting.
        int slot = streamPool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            if (streamId > highestPeerStreamIdSeen) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.PROTOCOL_ERROR, "DATA on idle stream");
            }
            // Rolled-off: credit dataLen back to connection, stream error.
            creditInboundConnectionWindow(dataLen);
            if (dataLen > 0) {
                enqueueWindowUpdate(0, dataLen);
            }
            throw Http2StreamException.instance(streamId, Http2ErrorCode.STREAM_CLOSED);
        }
        byte kind = streamPool.getSlotKind(slot);
        if (kind != Http2StreamPool.SLOT_LIVE) {
            // DISCARDING_BLOCK / TOMBSTONE: minimal processing per §5.
            // Credit dataLen back to connection so unrelated streams aren't
            // starved. Stream window is gone; the per-stream side is N/A.
            creditInboundConnectionWindow(dataLen);
            if (dataLen > 0) {
                enqueueWindowUpdate(0, dataLen);
            }
            if (kind == Http2StreamPool.SLOT_TOMBSTONE
                    && streamPool.getTombstoneCloseKind(slot) == Http2StreamPool.CLOSE_CLEAN) {
                throw Http2StreamException.instance(streamId, Http2ErrorCode.STREAM_CLOSED);
            }
            // LOCAL_RESET tombstone / DISCARDING_BLOCK: silently discard.
            return;
        }

        Http2Stream s = streamPool.getLiveStream(slot);
        if (!s.isInboundDirectionActive()) {
            // half-closed-remote or later — inbound direction frozen. §5
            // maps this to STREAM_CLOSED.
            creditInboundConnectionWindow(dataLen);
            if (dataLen > 0) {
                enqueueWindowUpdate(0, dataLen);
            }
            throw Http2StreamException.instance(streamId, Http2ErrorCode.STREAM_CLOSED);
        }

        // Debit stream window against paddedLen; underflow → stream reset
        // (FLOW_CONTROL_ERROR) with a dataLen credit back to the connection
        // so unrelated streams aren't starved (§7 step 5 table row
        // "stream-window underflow").
        long afterStreamDebit = Http2FlowController.debit(s.getInboundStreamWindow(), payloadLen);
        if (afterStreamDebit < 0) {
            s.setInboundStreamWindow(afterStreamDebit); // observable transient
            creditInboundConnectionWindow(dataLen);
            if (dataLen > 0) {
                enqueueWindowUpdate(0, dataLen);
            }
            throw Http2StreamException.instance(streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
        }
        // §7 step 3.iii: credit paddingOverhead back to the stream window.
        if (paddingOverhead > 0) {
            afterStreamDebit += paddingOverhead;
        }
        s.setInboundStreamWindow(afterStreamDebit);

        // FSM transition on endStream.
        if (endStream && !s.onRecvDataEndStream()) {
            // Unreachable if the pre-transition state was inbound-active;
            // defensive reset.
            throw Http2StreamException.instance(streamId, Http2ErrorCode.PROTOCOL_ERROR);
        }

        // Hand payload to the listener. Exclude the pad-length byte and
        // trailing padding — only dataLen is meaningful to the handler.
        int generation = s.getGeneration();
        boolean accepted = listener.onData(
                streamId, payloadAddr + dataOffset, dataLen, endStream, generation);

        if (dataLen > 0) {
            if (accepted) {
                // Synchronous consumption: credit dataLen to both windows
                // and emit WINDOW_UPDATE. §7 step 5 row "handler accepts".
                creditInboundConnectionWindow(dataLen);
                enqueueWindowUpdate(0, dataLen);
                if (s.isInboundDirectionActive()) {
                    creditInboundStreamWindow(slot, dataLen);
                    enqueueWindowUpdate(streamId, dataLen);
                }
            } else {
                // Deferred: accumulate obligation. §7 step 6 settles the
                // residual on close if the handler never acks.
                s.recordDeferredCredit(dataLen);
            }
        }

        if (s.isClosed()) {
            // endStream on HALF_CLOSED_LOCAL drove the FSM to CLOSED.
            // Settle any deferred credit before releasing the slot.
            settleDeferredCredit(s);
            streamPool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
            listener.onStreamClosed(streamId, Http2ErrorCode.NO_ERROR);
        }
    }

    private void dispatchGoAway(Http2FrameHeader header) {
        // Peer-initiated shutdown (§12). Because this server never pushes,
        // lastStreamId is advisory; we do not cancel in-flight client
        // requests on a graceful drain. If the peer reports a non-NO_ERROR
        // code they are aborting rather than draining — §12 allows us to
        // RST_STREAM(CANCEL) the active streams to release handler state
        // promptly, since the peer is about to close the socket anyway.
        if (state != STATE_ACTIVE) {
            return;
        }
        int errorCode = Http2FrameReader.readGoAwayErrorCode(header.getPayloadAddr());
        state = STATE_DRAINING;
        if (errorCode != Http2ErrorCode.NO_ERROR) {
            cancelAllActiveStreams();
        }
    }

    private void dispatchHeaders(Http2FrameHeader header) {
        int streamId = header.getStreamId();
        byte flags = header.getFlags();
        boolean endStream = Http2Flags.hasEndStream(flags);

        int slot = streamPool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            slot = admitNewStream(streamId);
        } else {
            byte kind = streamPool.getSlotKind(slot);
            if (kind == Http2StreamPool.SLOT_LIVE) {
                Http2Stream s = streamPool.getLiveStream(slot);
                if (!s.isInitialHeadersSeen()) {
                    // Internal invariant violation — LIVE slots always have
                    // the initial HEADERS flag set at admission.
                    throw Http2ConnectionException.instance(
                            Http2ErrorCode.INTERNAL_ERROR, "LIVE slot without initial HEADERS");
                }
                // Trailer HEADERS. M1 refuses trailers (§8); decode must still
                // run so the dynamic table stays coherent, so we assemble the
                // block with the LIVE slot flagged refusing.
                beginBlockAssembly(streamId, slot, endStream);
                s.markCurrentBlockRefused();
                appendFragmentFromHeaders(header);
                if (Http2Flags.hasEndHeaders(flags)) {
                    completeHeaderBlock();
                }
                return;
            }
            if (kind == Http2StreamPool.SLOT_DISCARDING_BLOCK) {
                // A DISCARDING_BLOCK on this id means a HEADERS block is
                // already being refused on it — HPACK decode is mid-flight
                // under the existing entry. A fresh HEADERS arriving here
                // would mean the peer closed END_HEADERS and is re-opening,
                // which the CONTINUATION sequencing invariant forbids.
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.PROTOCOL_ERROR, "HEADERS on discarding-block id");
            }
            // TOMBSTONE — re-use of a closed id.
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.PROTOCOL_ERROR, "HEADERS on tombstone id");
        }

        beginBlockAssembly(streamId, slot, endStream);
        appendFragmentFromHeaders(header);
        if (Http2Flags.hasEndHeaders(flags)) {
            completeHeaderBlock();
        }
    }

    private void dispatchPing(Http2FrameHeader header) {
        byte flags = header.getFlags();
        if (Http2Flags.hasAck(flags)) {
            // §9: correlate with an outstanding outbound PING. M1 does not
            // send outbound PINGs, so every ACK we see is unsolicited and
            // silently dropped.
            return;
        }
        // §9: echo the 8-byte opaque payload back with the ACK flag set.
        long opaque = Unsafe.getUnsafe().getLong(header.getPayloadAddr());
        enqueuePingAck(opaque);
    }

    private void dispatchRstStream(Http2FrameHeader header) {
        int streamId = header.getStreamId();
        int slot = streamPool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            // Per §5 rule 5: late RST on a rolled-off stream is silently
            // ignored.
            if (streamId > highestPeerStreamIdSeen) {
                // RFC 7540 sec. 5.1: RST on an idle (never-admitted) stream
                // is a connection error.
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.PROTOCOL_ERROR, "RST_STREAM on idle stream");
            }
            return;
        }
        byte kind = streamPool.getSlotKind(slot);
        if (kind == Http2StreamPool.SLOT_LIVE) {
            Http2Stream s = streamPool.getLiveStream(slot);
            int cause = Http2FrameReader.readRstStreamErrorCode(header.getPayloadAddr());
            settleDeferredCredit(s);
            s.onRst();
            // Peer-initiated reset: the peer will send nothing further, so
            // CLEAN-close is the correct flavour. Locally-initiated RSTs
            // (which stay open to in-flight peer frames) use LOCAL_RESET
            // via enqueueRstStream.
            streamPool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
            listener.onStreamClosed(streamId, cause);
        }
        // DISCARDING_BLOCK / TOMBSTONE: the peer's RST is informational;
        // we've already allocated the tombstone.
    }

    private void dispatchSettings(Http2FrameHeader header) {
        byte flags = header.getFlags();
        if (Http2Flags.hasAck(flags)) {
            // RFC 9113 sec. 6.5.3: an ACK with no outstanding SETTINGS is
            // a protocol violation. gRPC and other strict servers treat it
            // as a connection error rather than silently accepting — mirror
            // that posture here so misbehaving peers fail loudly.
            if (!settingsFrameOutstanding) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.PROTOCOL_ERROR, "unsolicited SETTINGS ACK");
            }
            settingsFrameOutstanding = false;
            // §10 compute-delta-then-commit: propagate ACKed identifiers only.
            // pendingSettingsBits is the bitset of ids carried on our last
            // SETTINGS frame; ids we never advertised (e.g. MAX_HEADER_LIST_SIZE
            // in M1) have ourAdvertised=0 and must not be applied.
            int bits = pendingSettingsBits;
            pendingSettingsBits = 0;
            for (short id = Http2Settings.HEADER_TABLE_SIZE;
                 id <= Http2Settings.MAX_HEADER_LIST_SIZE; id++) {
                if ((bits & (1 << id)) == 0) {
                    continue;
                }
                long adv = ourAdvertised[id];
                long app = ourApplied[id];
                if (adv == app) {
                    continue;
                }
                long delta = adv - app;
                switch (id) {
                    case Http2Settings.HEADER_TABLE_SIZE:
                        hpackDecoder.onLocalAdvertisedCapChanged((int) adv);
                        break;
                    case Http2Settings.INITIAL_WINDOW_SIZE:
                        commitOurInitialWindowChange(delta);
                        break;
                    default:
                        // MAX_FRAME_SIZE / MAX_HEADER_LIST_SIZE: tighten-on-ACK
                        // becomes a plain field update.
                        break;
                }
                ourApplied[id] = adv;
            }
            return;
        }
        // Peer SETTINGS (non-ACK). Apply on receipt per §10 peer-apply rule;
        // then emit our ACK.
        long payloadAddr = header.getPayloadAddr();
        int entries = header.getPayloadLength() / 6;
        for (int i = 0; i < entries; i++) {
            long entry = payloadAddr + (long) i * 6;
            short id = Http2FrameReader.readSettingsId(entry);
            long value = Http2FrameReader.readSettingsValue(entry);
            // RFC 9113 sec. 6.5.2: unknown ids MUST be ignored.
            if (id < Http2Settings.HEADER_TABLE_SIZE || id > Http2Settings.MAX_HEADER_LIST_SIZE) {
                continue;
            }
            applyPeerSetting(id, value);
        }
        enqueueSettingsAck();
    }

    private void dispatchWindowUpdate(Http2FrameHeader header) {
        int streamId = header.getStreamId();
        int increment = Http2FrameReader.readWindowUpdateIncrement(header.getPayloadAddr());
        // Zero-increment already rejected by the frame reader.
        if (streamId == 0) {
            long credited = Http2FlowController.credit(outboundConnectionWindow, increment);
            if (credited == Http2FlowController.OVERFLOW) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.FLOW_CONTROL_ERROR,
                        "connection outbound window overflow");
            }
            outboundConnectionWindow = credited;
            return;
        }
        int slot = streamPool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            // Late WINDOW_UPDATE on a rolled-off stream: silently ignored (§5 rule 5).
            if (streamId > highestPeerStreamIdSeen) {
                throw Http2ConnectionException.instance(
                        Http2ErrorCode.PROTOCOL_ERROR,
                        "WINDOW_UPDATE on idle stream");
            }
            return;
        }
        if (streamPool.getSlotKind(slot) != Http2StreamPool.SLOT_LIVE) {
            // DISCARDING_BLOCK / TOMBSTONE: accepted but no-op (§5 grace-window
            // tolerance for race frames).
            return;
        }
        Http2Stream s = streamPool.getLiveStream(slot);
        long credited = Http2FlowController.credit(s.getOutboundStreamWindow(), increment);
        if (credited == Http2FlowController.OVERFLOW) {
            throw Http2StreamException.instance(streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
        }
        s.setOutboundStreamWindow(credited);
    }

    private void enqueueGoAway(int errorCode) {
        if (pendingCount == PENDING_QUEUE_CAP) {
            // Last-resort: drop the oldest pending frame to make room for
            // the terminal GOAWAY. Pending queue overflow is not expected
            // under normal M1 traffic; a saturated queue means the caller
            // has not been draining writePending.
            shiftPendingLeft();
        }
        int idx = pendingCount++;
        pendingKind[idx] = PENDING_GOAWAY;
        pendingStreamId[idx] = lastAcceptedPeerStreamId;
        pendingParam[idx] = errorCode;
        pendingOpaque[idx] = 0L;
    }

    private void enqueuePingAck(long opaque) {
        if (pendingCount == PENDING_QUEUE_CAP) {
            // PING ACK is an RFC-required response; drop the oldest queued
            // frame rather than silently abandoning the ack. Under normal
            // traffic the queue holds 0-1 entries.
            shiftPendingLeft();
        }
        int idx = pendingCount++;
        pendingKind[idx] = PENDING_PING_ACK;
        pendingStreamId[idx] = 0;
        pendingParam[idx] = 0;
        pendingOpaque[idx] = opaque;
    }

    private void enqueueRstStream(int streamId, int errorCode) {
        if (pendingCount == PENDING_QUEUE_CAP) {
            shiftPendingLeft();
        }
        int idx = pendingCount++;
        pendingKind[idx] = PENDING_RST_STREAM;
        pendingStreamId[idx] = streamId;
        pendingParam[idx] = errorCode;
        pendingOpaque[idx] = 0L;
    }

    private void enqueueSettingsAck() {
        if (pendingCount == PENDING_QUEUE_CAP) {
            shiftPendingLeft();
        }
        int idx = pendingCount++;
        pendingKind[idx] = PENDING_SETTINGS_ACK;
        pendingStreamId[idx] = 0;
        pendingParam[idx] = 0;
        pendingOpaque[idx] = 0L;
    }

    private void enqueueWindowUpdate(int streamId, int increment) {
        if (increment <= 0) {
            return;
        }
        if (pendingCount == PENDING_QUEUE_CAP) {
            shiftPendingLeft();
        }
        int idx = pendingCount++;
        pendingKind[idx] = PENDING_WINDOW_UPDATE;
        pendingStreamId[idx] = streamId;
        pendingParam[idx] = increment;
        pendingOpaque[idx] = 0L;
    }

    private void initSettings() {
        // RFC 9113 sec. 6.5.2 defaults populate both ourApplied and
        // peerAdvertised before any SETTINGS exchange. ourAdvertised stays
        // at zero until our first SETTINGS frame is emitted.
        long[][] defaults = {ourApplied, peerAdvertised};
        for (long[] arr : defaults) {
            arr[Http2Settings.HEADER_TABLE_SIZE] = Http2Settings.DEFAULT_HEADER_TABLE_SIZE;
            arr[Http2Settings.ENABLE_PUSH] = Http2Settings.DEFAULT_ENABLE_PUSH;
            arr[Http2Settings.MAX_CONCURRENT_STREAMS] = Http2Settings.DEFAULT_MAX_CONCURRENT_STREAMS;
            arr[Http2Settings.INITIAL_WINDOW_SIZE] = Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE;
            arr[Http2Settings.MAX_FRAME_SIZE] = Http2Settings.DEFAULT_MAX_FRAME_SIZE;
            arr[Http2Settings.MAX_HEADER_LIST_SIZE] = Http2Settings.DEFAULT_MAX_HEADER_LIST_SIZE;
        }
        inboundConnectionWindow = Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE;
        outboundConnectionWindow = Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE;
    }

    private void resetStreamLocally(int streamId, int errorCode) {
        // Locally-initiated RST_STREAM path: emit the wire frame, transition
        // the LIVE slot to LOCAL_RESET tombstone, notify the listener.
        // Peer-initiated RST lands in dispatchRstStream with CLEAN close-kind.
        int slot = streamPool.lookup(streamId);
        if (slot != Http2StreamPool.SLOT_NOT_FOUND
                && streamPool.getSlotKind(slot) == Http2StreamPool.SLOT_LIVE) {
            Http2Stream s = streamPool.getLiveStream(slot);
            settleDeferredCredit(s);
            s.onRst();
            streamPool.closeLiveSlot(slot, Http2StreamPool.CLOSE_LOCAL_RESET);
            listener.onStreamClosed(streamId, errorCode);
        }
        enqueueRstStream(streamId, errorCode);
    }

    private void settleDeferredCredit(Http2Stream s) {
        // §7 step 6: credit any outstanding deferred obligation back to the
        // connection window and zero the counter. The per-stream window is
        // gone (or about to be) by the time settlement runs, so we don't
        // touch it. Skipping this step leaks connection-window capacity on
        // every reset/close that carried deferred credit.
        long outstanding = s.getOutstandingInboundCredit();
        if (outstanding <= 0) {
            return;
        }
        s.consumeDeferredCredit(outstanding);
        if (outstanding <= Integer.MAX_VALUE) {
            int increment = (int) outstanding;
            creditInboundConnectionWindow(increment);
            enqueueWindowUpdate(0, increment);
        }
    }

    private void shiftPendingLeft() {
        // Drop the head entry to free one slot.
        for (int i = 1; i < pendingCount; i++) {
            pendingKind[i - 1] = pendingKind[i];
            pendingStreamId[i - 1] = pendingStreamId[i];
            pendingParam[i - 1] = pendingParam[i];
            pendingOpaque[i - 1] = pendingOpaque[i];
        }
        pendingCount--;
    }

    private static final class EmptyRequestHeadersView implements Http2RequestHeadersView {

        @Override
        public long getAuthorityAddr() {
            return 0;
        }

        @Override
        public int getAuthorityLen() {
            return 0;
        }

        @Override
        public long getHostAddr() {
            return 0;
        }

        @Override
        public int getHostLen() {
            return 0;
        }

        @Override
        public long getMethodAddr() {
            return 0;
        }

        @Override
        public int getMethodLen() {
            return 0;
        }

        @Override
        public long getPathAddr() {
            return 0;
        }

        @Override
        public int getPathLen() {
            return 0;
        }

        @Override
        public long getSchemeAddr() {
            return 0;
        }

        @Override
        public int getSchemeLen() {
            return 0;
        }
    }

    private long writeOne(long addr, long limit, int i) {
        byte kind = pendingKind[i];
        return switch (kind) {
            case PENDING_SETTINGS_ACK -> Http2FrameWriter.writeSettingsAck(addr, limit);
            case PENDING_PING_ACK -> Http2FrameWriter.writePing(addr, limit, true, pendingOpaque[i]);
            case PENDING_GOAWAY -> Http2FrameWriter.writeGoAway(
                    addr, limit, pendingStreamId[i], pendingParam[i], 0L, 0);
            case PENDING_RST_STREAM -> Http2FrameWriter.writeRstStream(
                    addr, limit, pendingStreamId[i], pendingParam[i]);
            case PENDING_WINDOW_UPDATE -> Http2FrameWriter.writeWindowUpdate(
                    addr, limit, pendingStreamId[i], pendingParam[i]);
            default -> throw new IllegalStateException("unknown pending kind=" + kind);
        };
    }
}
