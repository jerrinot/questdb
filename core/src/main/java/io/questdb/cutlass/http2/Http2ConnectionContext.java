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
 * <p>
 * <b>Error-style split.</b> The class uses two distinct error-handling
 * styles for two distinct concerns. Inbound frame parsing and
 * control-path errors throw {@link Http2ConnectionException} or
 * {@link Http2StreamException} — the connection-level vs. stream-level
 * discrimination is encoded in the exception type, and these errors are
 * rare enough that per-call allocation is not a hot-path concern. The
 * response-emit surface ({@link #enqueueData}, {@link #emitResponseHeaders})
 * uses {@code int} sentinels ({@link #ENQUEUE_OK},
 * {@link #ENQUEUE_PARK}, {@link #ENQUEUE_STALE_GENERATION},
 * {@link #ENQUEUE_STREAM_CLOSED},
 * {@link #ENQUEUE_HEADER_LIST_TOO_LARGE}) because those methods run on
 * the hottest per-response path under sustained load; per-call exception
 * allocation there would violate the zero-GC discipline. The trade-off
 * is intentional — do not "normalise" one style to the other without
 * first measuring the allocation cost on the response-emit path.
 */
public final class Http2ConnectionContext implements Closeable {

    /**
     * {@link #emitResponseHeaders} only — the encoded block overflows the
     * engine's response-headers scratch. Caller treats as a programming
     * error (likely exceeds the peer's
     * {@code SETTINGS_MAX_HEADER_LIST_SIZE} advertisement or the per-stream
     * arena cap).
     */
    public static final int ENQUEUE_HEADER_LIST_TOO_LARGE = -4;
    /** Success; the tuple(s) have been copied into the stream's arena. */
    public static final int ENQUEUE_OK = 0;
    /**
     * Per-stream outbound cap would be exceeded. The engine flags the
     * stream as parked; the caller must suspend response production until
     * {@link Http2StreamListener#onStreamWritable} fires for this stream.
     */
    public static final int ENQUEUE_PARK = -3;
    /**
     * Generation mismatch — the stream referenced by
     * {@code (streamId, generation)} has been reset / recycled since the
     * handler captured the pair. The call no-ops; no state changes.
     */
    public static final int ENQUEUE_STALE_GENERATION = -1;
    /**
     * Outbound direction is closed for this stream (state machine moved
     * past {@code OPEN} / {@code HALF_CLOSED_REMOTE} on the send side, or
     * a prior enqueue staged {@code END_STREAM}). Caller has a
     * programming error or raced the stream's lifecycle; no state changes.
     */
    public static final int ENQUEUE_STREAM_CLOSED = -2;
    public static final byte STATE_ACTIVE = 0;
    public static final byte STATE_CLOSED = 2;
    public static final byte STATE_DRAINING = 1;

    // Pseudo-header name addresses in the HPACK static table. The HPACK
    // decoder surfaces indexed names by reference into that table, so a
    // pseudo-header captured via an indexed representation lands here with
    // the same native address each time. Byte-literal compare against these
    // addresses would be brittle, so {@link #matchPseudoName} does a value
    // compare, but caching them lets the hot path short-circuit.
    private static final byte[] HEADER_AUTHORITY = asciiBytes(":authority");
    private static final byte[] HEADER_CONTENT_TYPE = asciiBytes("content-type");
    private static final byte[] HEADER_METHOD = asciiBytes(":method");
    private static final byte[] HEADER_PATH = asciiBytes(":path");
    private static final byte[] HEADER_SCHEME = asciiBytes(":scheme");

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
    // Emit sentinels for emitOneFrame. Every positive return is a cursor
    // address inside the caller's send buffer; negative returns discriminate
    // the two non-success outcomes.
    private static final long EMIT_BUFFER_FULL = -1L;
    private static final long EMIT_WINDOW_BLOCKED = -2L;
    private static final int SETTINGS_COUNT = 7;

    private final long blockAssemblyScratchAddr;
    private final int blockAssemblyScratchCap;
    private final Http2ConnectionConfig config;
    private final Http2FrameHeader frameHeader = new Http2FrameHeader();
    // Engine-owned scratch for emitResponseHeaders: the writer callback
    // encodes into this buffer, the engine measures the result, and only
    // then commits tuples into the target stream's arena. Sized to the
    // per-stream arena cap so any block that will fit the arena also fits
    // the scratch. Allocated once at construction; freed on close.
    private final long headersScratchAddr;
    private final int headersScratchCap;
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
    // Per-stream capture target for the HPACK listener. Non-null only
    // during {@link #completeHeaderBlock} for an initial LIVE HEADERS
    // block; the captured pseudo-header slots belong to this stream and
    // live on until the next request on the same slot rebinds them.
    private Http2Stream captureTargetStream;
    private final Http2StreamHeaderCapturingListener capturingListener = new Http2StreamHeaderCapturingListener();
    private final Http2StreamHeadersView requestHeadersView = new Http2StreamHeadersView();
    private boolean closed;
    // Reentrancy guard for emitResponseHeaders. Set while the writer
    // callback runs; any nested emitResponseHeaders / enqueueData call
    // would corrupt the shared HPACK encoder's block state and throws
    // IllegalStateException instead.
    private boolean headersEncoderInUse;
    private int highestPeerStreamIdSeen;
    private long inboundConnectionWindow;
    private boolean initialSettingsEmitted;
    private int lastAcceptedPeerStreamId;
    // Connection-scoped round-robin cursor over stream-pool slots. Persists
    // across writePending calls so when a buffer-full early exit interrupts
    // mid-sweep the next call resumes where we left off — preserves fairness
    // under sustained backpressure. The cursor names the next slot to try;
    // advanced after each successful emission attempt (whether the slot
    // had a tuple or not).
    private int nextStreamCursor;
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
        this.streamPool = new Http2StreamPool(
                config.ourMaxConcurrentStreams,
                config.tombstoneCap,
                2, // DEFAULT_DISCARDING_RESERVE — tests cover the wider overload.
                config.outboundArenaBytesPerStream,
                config.outboundTupleQueueCap,
                config.headerStagingBytesPerStream);
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
        // Response-headers scratch matches the per-stream arena cap so the
        // engine can always trial-encode a block that would fit the arena
        // into the scratch first. outboundArenaBytesPerStream may be 0 if
        // the config disables outbound staging (tests); in that case we
        // still allocate at least the HPACK encoder's buffer floor so
        // beginBlock / encode have working space, because
        // emitResponseHeaders is still callable and will reject with
        // ENQUEUE_STREAM_CLOSED downstream.
        int headersScratchSize = Math.max(
                config.outboundArenaBytesPerStream,
                config.hpackEncoderBufferBytes);
        long headersScratch;
        try {
            headersScratch = Unsafe.malloc(headersScratchSize, MemoryTag.NATIVE_DEFAULT);
        } catch (Throwable t) {
            Unsafe.free(blockAssemblyScratchAddr, blockAssemblyScratchCap, MemoryTag.NATIVE_DEFAULT);
            throw t;
        }
        this.headersScratchAddr = headersScratch;
        this.headersScratchCap = headersScratchSize;

        initSettings();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        streamPool.clear();
        streamPool.close();
        hpackDecoder.close();
        hpackEncoder.close();
        Unsafe.free(blockAssemblyScratchAddr, blockAssemblyScratchCap, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(headersScratchAddr, headersScratchCap, MemoryTag.NATIVE_DEFAULT);
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

    /**
     * Encodes a response HEADERS block for {@code streamId} with the
     * connection's shared {@link HpackEncoder} and commits the encoded
     * bytes as one HEADERS frame plus zero-or-more CONTINUATION frames
     * on the stream's outbound tuple queue. All-or-nothing: if the
     * encoded block would push the stream's arena or tuple ring past
     * its cap the method returns {@link #ENQUEUE_PARK} without appending
     * any tuples.
     * <p>
     * Return values:
     * <ul>
     *   <li>{@link #ENQUEUE_OK} — one HEADERS + N CONTINUATION tuples
     *       are queued; the scheduler will emit them in order.</li>
     *   <li>{@link #ENQUEUE_STALE_GENERATION} — {@code generation} does
     *       not match the current {@link Http2Stream#getGeneration} on
     *       the stream id. No state changes.</li>
     *   <li>{@link #ENQUEUE_STREAM_CLOSED} — the stream's outbound
     *       direction is closed (FSM past {@code OPEN} /
     *       {@code HALF_CLOSED_REMOTE}), or a prior call staged
     *       {@code END_STREAM}. No state changes; the HPACK encoder's
     *       block is not opened.</li>
     *   <li>{@link #ENQUEUE_PARK} — the encoded block would exceed the
     *       stream's {@code outboundArenaBytesPerStream} or
     *       {@code outboundTupleQueueCap}. The stream's park flag is
     *       set so the engine fires
     *       {@link Http2StreamListener#onStreamWritable} once the queue
     *       drains enough to admit the block. The HPACK encoder's
     *       block state is clean (any queued size updates that were
     *       flushed into scratch are discarded along with the scratch
     *       bytes, but the encoder's dynamic-table state is unchanged
     *       — Milestone 1 pins {@code selectedMax} at 0 per
     *       {@code HPACK_CODEC.md} §16.1).</li>
     *   <li>{@link #ENQUEUE_HEADER_LIST_TOO_LARGE} — the writer could
     *       not finish encoding before the engine's response-headers
     *       scratch overflowed. The block is discarded; the caller
     *       must trim headers or reject the request.</li>
     * </ul>
     * <p>
     * Concurrency: the engine serialises access to the shared HPACK
     * encoder. A nested {@link #emitResponseHeaders} or
     * {@link #enqueueData} call from inside the writer throws
     * {@link IllegalStateException} — the contract on
     * {@link Http2HeadersWriter} forbids it, and the engine enforces it
     * with a reentrancy guard.
     */
    public int emitResponseHeaders(int streamId, int generation,
                                   Http2HeadersWriter writer, boolean endStream) {
        return emitHeaderBlock(streamId, generation, writer, endStream, Http2Stream.TUPLE_KIND_HEADERS);
    }

    /**
     * Encodes a trailer HEADERS block for {@code streamId} via the
     * shared {@link HpackEncoder} and commits it as one HEADERS-kind
     * frame plus zero-or-more CONTINUATION frames on the stream's
     * outbound tuple queue. Wire-frame-identical to
     * {@link #emitResponseHeaders}: the scheduler encodes the tuple as
     * a HEADERS frame on the wire (RFC 9113 §8.1 — trailers are a second
     * HEADERS block in the stream, not a distinct frame type). The tuple
     * is tagged {@link Http2Stream#TUPLE_KIND_TRAILERS} so the scheduler
     * can distinguish it for diagnostics; the first tuple always carries
     * {@code END_STREAM}.
     * <p>
     * Return values match {@link #emitResponseHeaders}:
     * {@link #ENQUEUE_OK}, {@link #ENQUEUE_STALE_GENERATION},
     * {@link #ENQUEUE_STREAM_CLOSED}, {@link #ENQUEUE_PARK},
     * {@link #ENQUEUE_HEADER_LIST_TOO_LARGE}. HPACK snapshot / restore
     * rolls the shared encoder back on every non-committing return —
     * the peer's decoder never sees a size-update that didn't make it
     * to the wire.
     *
     * @throws IllegalArgumentException if {@code endStream} is {@code
     *         false}. RFC 9113 §8.1 mandates {@code END_STREAM} on the
     *         terminating HEADERS of a trailer block; an omit here is a
     *         programming error, not a protocol variant.
     */
    public int emitTrailers(int streamId, int generation,
                            Http2HeadersWriter writer, boolean endStream) {
        if (!endStream) {
            throw new IllegalArgumentException(
                    "trailer HEADERS must carry END_STREAM per RFC 9113 sec. 8.1");
        }
        return emitHeaderBlock(streamId, generation, writer, true, Http2Stream.TUPLE_KIND_TRAILERS);
    }

    private int emitHeaderBlock(int streamId, int generation,
                                Http2HeadersWriter writer, boolean endStream,
                                byte leadTupleKind) {
        if (writer == null) {
            throw new IllegalArgumentException("writer must be non-null");
        }
        if (headersEncoderInUse) {
            throw new IllegalStateException("emitResponseHeaders re-entered from within a writer callback");
        }
        int validation = validateOutboundTarget(streamId, generation);
        if (validation != ENQUEUE_OK) {
            return validation;
        }
        int slot = streamPool.lookup(streamId);
        Http2Stream s = streamPool.getLiveStream(slot);

        int peerMaxFrameSize = (int) peerAdvertised[Http2Settings.MAX_FRAME_SIZE];
        if (peerMaxFrameSize < Http2Settings.MAX_FRAME_SIZE_LOWER) {
            peerMaxFrameSize = Http2Settings.MAX_FRAME_SIZE_LOWER;
        }

        // Snapshot the encoder's mutable state so any non-committing
        // return path below can roll it back. Without this a PARK'd emit
        // would drop a queued Dynamic Table Size Update (RFC 7541 sec.
        // 4.2) or (M2) incremental-indexing admission on the floor —
        // the peer's decoder would then observe an inconsistent dynamic
        // table on the next successful block. `committed` flips to true
        // only on the single ENQUEUE_OK return; every other exit path
        // (PARK, HEADER_LIST_TOO_LARGE, propagated exceptions) goes
        // through the finally's restore.
        long encoderSnapshot = hpackEncoder.snapshot();
        boolean committed = false;
        headersEncoderInUse = true;
        try {
            long scratchStart = hpackEncoder.beginBlock(headersScratchAddr, headersScratchAddr + headersScratchCap);
            if (scratchStart < 0) {
                return ENQUEUE_HEADER_LIST_TOO_LARGE;
            }
            long finalCursor;
            try {
                finalCursor = writer.write(hpackEncoder, scratchStart, headersScratchAddr + headersScratchCap);
            } catch (RuntimeException e) {
                // Ensure the encoder block state is cleared before the
                // exception propagates so the connection isn't left with
                // a half-open block; the outer finally then calls
                // restore() because committed is still false.
                hpackEncoder.endBlock();
                throw e;
            }
            hpackEncoder.endBlock();
            if (finalCursor < 0 || finalCursor < scratchStart
                    || finalCursor > headersScratchAddr + headersScratchCap) {
                return ENQUEUE_HEADER_LIST_TOO_LARGE;
            }
            long encodedLen = finalCursor - headersScratchAddr;

            // Cap check #1: per-stream arena can hold the encoded bytes.
            int remainingArena = s.getOutboundArenaCap() - s.getOutboundQueuedPayloadBytes();
            if (encodedLen > remainingArena) {
                s.setOutboundParked(true);
                return ENQUEUE_PARK;
            }
            // Cap check #2: per-stream tuple ring has room for 1 HEADERS
            // + (N-1) CONTINUATIONs. A zero-length encoded block still
            // needs exactly one HEADERS frame to carry END_HEADERS and
            // (if set) END_STREAM — the all-or-nothing reservation must
            // account for that floor.
            int frames = (int) ((encodedLen + peerMaxFrameSize - 1) / peerMaxFrameSize);
            if (frames == 0) {
                frames = 1;
            }
            if (frames > s.getOutboundTupleCap() - s.getOutboundTupleCount()) {
                s.setOutboundParked(true);
                return ENQUEUE_PARK;
            }

            // Commit. Each tryEnqueueOutbound below must succeed given
            // the two pre-checks above — any false return here would
            // mean the accounting invariants drifted.
            long emit = headersScratchAddr;
            long scratchEnd = headersScratchAddr + encodedLen;
            for (int i = 0; i < frames; i++) {
                long remaining = scratchEnd - emit;
                int chunk = (int) Math.min(remaining, (long) peerMaxFrameSize);
                boolean isLast = (i == frames - 1);
                byte kind = (i == 0) ? leadTupleKind : Http2Stream.TUPLE_KIND_CONTINUATION;
                // RFC 9113 sec. 6.2: END_STREAM lives on the HEADERS
                // frame (not CONTINUATION). END_HEADERS rides the last
                // frame of the sequence.
                byte tupleFlags = 0;
                if (isLast) {
                    tupleFlags |= Http2Stream.TUPLE_FLAG_END_HEADERS;
                }
                if (i == 0 && endStream) {
                    tupleFlags |= Http2Stream.TUPLE_FLAG_END_STREAM;
                }
                if (!s.tryEnqueueOutbound(kind, tupleFlags, emit, chunk)) {
                    // Pre-checks above should make this unreachable;
                    // defend anyway by surfacing PARK so the caller can
                    // retry. The partial appends that already happened
                    // in this loop are rolled back by clearing the ring
                    // entries we just wrote — but since tryEnqueueOutbound
                    // on a tuple only appends (no shared state with
                    // earlier tuples), dropping via clearOutboundQueue
                    // here would also wipe pre-existing tuples the caller
                    // legitimately owns. Leave the partial append in
                    // place and restore only the HPACK encoder state —
                    // the per-stream outbound queue is caller-owned and
                    // the caller is expected to retry the same block on
                    // the next onStreamWritable. Pre-checks ensure this
                    // branch is unreachable in practice.
                    s.setOutboundParked(true);
                    return ENQUEUE_PARK;
                }
                emit += chunk;
            }
            committed = true;
            return ENQUEUE_OK;
        } finally {
            headersEncoderInUse = false;
            if (!committed) {
                hpackEncoder.restore(encoderSnapshot);
            }
        }
    }

    /**
     * Appends a DATA frame tuple carrying {@code payloadLen} bytes copied
     * from {@code payloadAddr} to the outbound queue of {@code streamId}.
     * The caller's buffer is free to be reused immediately on return —
     * the bytes are memcpy'd into the stream's arena per §4.4
     * copy-on-enqueue ownership.
     * <p>
     * Return values:
     * <ul>
     *   <li>{@link #ENQUEUE_OK} — tuple queued.</li>
     *   <li>{@link #ENQUEUE_STALE_GENERATION} — generation mismatch;
     *       silent no-op.</li>
     *   <li>{@link #ENQUEUE_STREAM_CLOSED} — the stream's outbound
     *       direction is closed or a prior call staged
     *       {@code END_STREAM}.</li>
     *   <li>{@link #ENQUEUE_PARK} — per-stream arena or tuple ring cap
     *       would be exceeded. The stream's park flag is set; the
     *       engine will fire
     *       {@link Http2StreamListener#onStreamWritable} once the queue
     *       drains.</li>
     * </ul>
     * <p>
     * Zero-length payloads are legal: a {@code payloadLen == 0} call with
     * {@code endStream == true} queues an empty terminator DATA frame
     * that the scheduler emits even under a zero flow-control window
     * (RFC 9113 §6.9.1).
     */
    public int enqueueData(int streamId, int generation, long payloadAddr, int payloadLen, boolean endStream) {
        if (payloadLen < 0) {
            throw new IllegalArgumentException("payloadLen must be non-negative: " + payloadLen);
        }
        int validation = validateOutboundTarget(streamId, generation);
        if (validation != ENQUEUE_OK) {
            return validation;
        }
        int slot = streamPool.lookup(streamId);
        Http2Stream s = streamPool.getLiveStream(slot);
        byte flags = endStream ? Http2Stream.TUPLE_FLAG_END_STREAM : (byte) 0;
        if (!s.tryEnqueueOutbound(Http2Stream.TUPLE_KIND_DATA, flags, payloadAddr, payloadLen)) {
            s.setOutboundParked(true);
            return ENQUEUE_PARK;
        }
        return ENQUEUE_OK;
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
     * Drains outbound frames into the caller-owned send buffer
     * {@code [addr, limit)}. Two phases:
     * <ol>
     *   <li>Flush the control-frame FIFO (SETTINGS_ACK, PING_ACK, GOAWAY,
     *       RST_STREAM, WINDOW_UPDATE). Stops at the first frame that
     *       cannot fit.</li>
     *   <li>Round-robin over {@link Http2StreamPool} slots with queued
     *       outbound tuples. Each sweep emits at most one frame per
     *       eligible LIVE stream (HEADERS / CONTINUATION / DATA); DATA
     *       emission is gated on stream + connection outbound windows and
     *       capped at peer {@code MAX_FRAME_SIZE}. Sweeps repeat while any
     *       slot made progress; the loop exits when the send buffer fills
     *       or no slot is eligible. The next-slot cursor persists across
     *       {@link #writePending} calls so an interrupted sweep resumes
     *       fairly on the next call.</li>
     * </ol>
     * A parked stream (see {@link #ENQUEUE_PARK}) whose successful emission
     * frees arena / ring headroom triggers
     * {@link Http2StreamListener#onStreamWritable} synchronously from
     * inside this call.
     * <p>
     * A stream that reaches {@link Http2StreamState#CLOSED} as a result of
     * a send-side {@code END_STREAM} emission has its pool slot promoted
     * to a clean tombstone and
     * {@link Http2StreamListener#onStreamClosed} fired before
     * {@code writePending} returns.
     */
    public long writePending(long addr, long limit) {
        long cursor = addr;
        // Phase 1: drain control-frame FIFO in FIFO order.
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
        if (pendingCount > 0) {
            // Control FIFO did not fully drain — buffer is full. No room
            // for stream frames either; return and let the caller drain.
            return cursor;
        }

        // Phase 2: round-robin stream scheduler. Skip on closed connection.
        if (state == STATE_CLOSED) {
            return cursor;
        }
        int slotCount = streamPool.getSlotCount();
        if (slotCount == 0) {
            return cursor;
        }

        boolean sweptAnything;
        do {
            sweptAnything = false;
            int attempted = 0;
            while (attempted < slotCount) {
                int slot = nextStreamCursor;
                if (streamPool.getSlotKind(slot) != Http2StreamPool.SLOT_LIVE) {
                    nextStreamCursor = (nextStreamCursor + 1) % slotCount;
                    attempted++;
                    continue;
                }
                Http2Stream s = streamPool.getLiveStream(slot);
                if (s.getOutboundTupleCount() == 0) {
                    nextStreamCursor = (nextStreamCursor + 1) % slotCount;
                    attempted++;
                    continue;
                }
                long next = emitOneFrame(cursor, limit, s);
                if (next == EMIT_BUFFER_FULL) {
                    // Leave nextStreamCursor on this slot so the next
                    // writePending resumes here — round-robin fairness
                    // after buffer-full interruption demands we retry
                    // the same slot rather than skip it.
                    return cursor;
                }
                if (next == EMIT_WINDOW_BLOCKED) {
                    // Window-blocked stream: advance past it and try other
                    // streams in this sweep; a subsequent WINDOW_UPDATE
                    // will re-enable it.
                    nextStreamCursor = (nextStreamCursor + 1) % slotCount;
                    attempted++;
                    continue;
                }
                cursor = next;
                sweptAnything = true;
                // Check if this emission unblocked a parked stream.
                maybeFireWritableAfterEmission(s);
                nextStreamCursor = (nextStreamCursor + 1) % slotCount;
                attempted++;
            }
        } while (sweptAnything);

        return cursor;
    }

    private static byte[] asciiBytes(String s) {
        byte[] out = new byte[s.length()];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) s.charAt(i);
        }
        return out;
    }

    private static boolean equalsAsciiIgnoreCase(long addr, int len, byte[] expected) {
        if (len != expected.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            byte a = Unsafe.getUnsafe().getByte(addr + i);
            byte b = expected[i];
            if (a != b) {
                if ((a >= 'A' && a <= 'Z' ? (byte) (a + 32) : a)
                        != (b >= 'A' && b <= 'Z' ? (byte) (b + 32) : b)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int matchPseudoName(long nameAddr, int nameLen) {
        // Byte-literal match on the decoder-produced name. The HPACK decoder
        // already lowercased regular-header names per RFC 9113 §8.2.1, but we
        // still guard Host with a case-insensitive compare for indexed-literal
        // paths that surface mixed case. Pseudo-headers (:method, :path,
        // :scheme, :authority) are always lowercase on the wire per RFC 9113
        // §8.3 so the equalsAsciiIgnoreCase helper is overkill for them but
        // free — the four ASCII bytes compare in a tight loop.
        if (nameLen <= 0) {
            return -1;
        }
        byte first = Unsafe.getUnsafe().getByte(nameAddr);
        if (first == (byte) ':') {
            if (equalsAsciiIgnoreCase(nameAddr, nameLen, HEADER_METHOD)) {
                return Http2Stream.STAGING_SLOT_METHOD;
            }
            if (equalsAsciiIgnoreCase(nameAddr, nameLen, HEADER_PATH)) {
                return Http2Stream.STAGING_SLOT_PATH;
            }
            if (equalsAsciiIgnoreCase(nameAddr, nameLen, HEADER_SCHEME)) {
                return Http2Stream.STAGING_SLOT_SCHEME;
            }
            if (equalsAsciiIgnoreCase(nameAddr, nameLen, HEADER_AUTHORITY)) {
                return Http2Stream.STAGING_SLOT_AUTHORITY;
            }
            return -1;
        }
        if (equalsAsciiIgnoreCase(nameAddr, nameLen, HEADER_CONTENT_TYPE)) {
            return Http2Stream.STAGING_SLOT_CONTENT_TYPE;
        }
        return -1;
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
        // Trigger 3: a positive delta unparks every outbound-active
        // parked stream. No-op for negative / zero deltas.
        if (delta > 0) {
            fanOutWritableAfterSettingsWindowIncrease();
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
        captureTargetStream = null;
        if (streamPool.getSlotKind(slot) == Http2StreamPool.SLOT_LIVE) {
            Http2Stream s = streamPool.getLiveStream(slot);
            // beginHeaderBlock() clears refusingCurrentBlock. A trailer
            // HEADERS sets that flag via markCurrentBlockRefused() AFTER
            // this call.
            s.beginHeaderBlock();
            if (!s.isInitialHeadersSeen()) {
                // §11 capture scope: only the initial HEADERS block carries
                // pseudo-headers. Trailer blocks land on §8 M1 refusal above
                // and never need a staging reset.
                s.beginInitialHeaders();
                captureTargetStream = s;
            }
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
        // sticky-error rationale). The capturing listener writes target
        // pseudo-header / content-type bytes into captureTargetStream's
        // staging buffer; all other fields are consumed and dropped. A
        // structural HPACK failure escalates to a connection
        // COMPRESSION_ERROR.
        Http2Stream captureTargetSnapshot = captureTargetStream;
        try {
            hpackDecoder.decodeBlock(blockAddr, blockLimit, capturingListener);
        } catch (HpackException e) {
            captureTargetStream = null;
            throw Http2ConnectionException.instance(
                    Http2ErrorCode.COMPRESSION_ERROR, e.getMessage());
        }
        captureTargetStream = null;

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
        if (captureTargetSnapshot == s && s.isStagingOverflow()) {
            // §15.5 step 2 capture-overflow rule. A pseudo-header or
            // content-type value is too large to fit the per-stream
            // staging budget; reset with PROTOCOL_ERROR and do not fire
            // onRequestHeaders so the listener never observes a
            // half-populated view.
            resetStreamLocally(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (!s.onRecvHeaders(endStream)) {
            // Malformed trailer (no END_STREAM) or bad state transition.
            resetStreamLocally(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        listener.onRequestHeaders(streamId, requestHeadersView.of(s), endStream);
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
        // Must use big-endian read (readU64) symmetric with writePing's
        // big-endian putU64 so the wire-octet sequence round-trips exactly
        // as RFC 9113 sec. 6.7 requires.
        long opaque = Http2FrameReader.readU64(header.getPayloadAddr());
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
            // Trigger 2: connection window credit fans out to every
            // parked stream whose own outbound window is non-zero.
            fanOutWritableAfterConnectionWindowUpdate();
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
        // Trigger 1: per-stream window credit unparks this stream.
        maybeFireWritableAfterStreamWindowUpdate(slot);
    }

    /**
     * Emits at most one wire frame from {@code s}'s head outbound tuple
     * into {@code [addr, limit)}. Returns the new cursor on success, or
     * one of {@link #EMIT_BUFFER_FULL} / {@link #EMIT_WINDOW_BLOCKED} on
     * failure. HEADERS and CONTINUATION ignore flow control per RFC 9113
     * sec. 5.2.1; DATA debits both the stream and connection outbound
     * windows by the emitted payload length. Zero-length DATA with
     * {@code END_STREAM} is emitted even under a zero flow-control window
     * per RFC 9113 sec. 6.9.1.
     * <p>
     * On {@code END_STREAM} emission the stream's FSM advances per §5;
     * a transition to {@link Http2StreamState#CLOSED} causes the slot to
     * close-clean and fires {@link Http2StreamListener#onStreamClosed}.
     */
    private long emitOneFrame(long addr, long limit, Http2Stream s) {
        byte kind = s.peekOutboundKind();
        byte flags = s.peekOutboundFlags();
        long tupleAddr = s.peekOutboundPayloadAddr();
        int remaining = s.peekOutboundPayloadLen();
        boolean tupleEndStream = (flags & Http2Stream.TUPLE_FLAG_END_STREAM) != 0;
        boolean tupleEndHeaders = (flags & Http2Stream.TUPLE_FLAG_END_HEADERS) != 0;
        int peerMaxFrame = peerMaxFrameSize();
        int streamId = s.getStreamId();

        if (kind == Http2Stream.TUPLE_KIND_HEADERS || kind == Http2Stream.TUPLE_KIND_TRAILERS) {
            // §15.5 step 3: trailers ride the wire as HEADERS frames (RFC
            // 9113 §8.1 — a trailer block is a second HEADERS block, not
            // a distinct frame type). Only the tuple kind tracks the
            // distinction for diagnostics.
            int frameSize = Math.min(remaining, peerMaxFrame);
            boolean frameEndStream = tupleEndStream && frameSize == remaining;
            boolean frameEndHeaders = tupleEndHeaders && frameSize == remaining;
            long next = Http2FrameWriter.writeHeaders(addr, limit, streamId,
                    frameEndStream, frameEndHeaders, tupleAddr, frameSize);
            if (next < 0) {
                return EMIT_BUFFER_FULL;
            }
            s.advanceOutboundHead(frameSize);
            if (frameEndStream) {
                s.onSendHeaders(true);
                maybeCloseStream(s);
            }
            return next;
        }

        if (kind == Http2Stream.TUPLE_KIND_CONTINUATION) {
            int frameSize = Math.min(remaining, peerMaxFrame);
            boolean frameEndHeaders = tupleEndHeaders && frameSize == remaining;
            long next = Http2FrameWriter.writeContinuation(addr, limit, streamId,
                    frameEndHeaders, tupleAddr, frameSize);
            if (next < 0) {
                return EMIT_BUFFER_FULL;
            }
            s.advanceOutboundHead(frameSize);
            return next;
        }

        // DATA
        if (remaining == 0) {
            if (!tupleEndStream) {
                // Zero-length non-END_STREAM DATA is not a useful tuple;
                // enqueueData should have rejected or skipped it. Defensive.
                throw new IllegalStateException(
                        "zero-length DATA without END_STREAM on stream=" + streamId);
            }
            // RFC 9113 sec. 6.9.1: zero-length DATA with END_STREAM is not
            // flow-controlled and emits even under a zero window.
            long next = Http2FrameWriter.writeData(addr, limit, streamId, true, 0L, 0);
            if (next < 0) {
                return EMIT_BUFFER_FULL;
            }
            s.advanceOutboundHead(0);
            s.onSendDataEndStream();
            maybeCloseStream(s);
            return next;
        }
        long streamWindow = s.getOutboundStreamWindow();
        long connWindow = outboundConnectionWindow;
        if (streamWindow <= 0 || connWindow <= 0) {
            return EMIT_WINDOW_BLOCKED;
        }
        long budget = Math.min(Math.min(streamWindow, connWindow), (long) peerMaxFrame);
        int frameSize = (int) Math.min((long) remaining, budget);
        if (frameSize <= 0) {
            return EMIT_WINDOW_BLOCKED;
        }
        boolean frameEndStream = tupleEndStream && frameSize == remaining;
        long next = Http2FrameWriter.writeData(addr, limit, streamId,
                frameEndStream, tupleAddr, frameSize);
        if (next < 0) {
            return EMIT_BUFFER_FULL;
        }
        // RFC 9113 sec. 5.2.1: only DATA consumes outbound windows.
        outboundConnectionWindow -= frameSize;
        s.setOutboundStreamWindow(streamWindow - frameSize);
        s.advanceOutboundHead(frameSize);
        if (frameEndStream) {
            s.onSendDataEndStream();
            maybeCloseStream(s);
        }
        return next;
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

    /**
     * Trigger 2 fan-out: after a peer {@code WINDOW_UPDATE(0)} credits
     * the connection outbound window, every parked stream whose own
     * outbound window is non-zero becomes potentially writable.
     */
    private void fanOutWritableAfterConnectionWindowUpdate() {
        Http2StreamPool pool = streamPool;
        int slotCount = pool.getSlotCount();
        for (int i = 0; i < slotCount; i++) {
            if (pool.getSlotKind(i) != Http2StreamPool.SLOT_LIVE) {
                continue;
            }
            Http2Stream s = pool.getLiveStream(i);
            if (!s.isOutboundParked()) {
                continue;
            }
            if (s.getOutboundStreamWindow() <= 0) {
                continue;
            }
            s.setOutboundParked(false);
            listener.onStreamWritable(s.getStreamId());
        }
    }

    /**
     * Trigger 3 fan-out: a peer
     * {@code SETTINGS_INITIAL_WINDOW_SIZE} increase has bumped every
     * outbound-direction-active stream's window. Every parked stream
     * whose direction is still active becomes writable.
     */
    private void fanOutWritableAfterSettingsWindowIncrease() {
        Http2StreamPool pool = streamPool;
        int slotCount = pool.getSlotCount();
        for (int i = 0; i < slotCount; i++) {
            if (pool.getSlotKind(i) != Http2StreamPool.SLOT_LIVE) {
                continue;
            }
            Http2Stream s = pool.getLiveStream(i);
            if (!s.isOutboundParked()) {
                continue;
            }
            if (!s.isOutboundDirectionActive()) {
                continue;
            }
            s.setOutboundParked(false);
            listener.onStreamWritable(s.getStreamId());
        }
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

    /**
     * Called after a scheduler emission that may have driven the stream
     * into {@link Http2StreamState#CLOSED}. Promotes the LIVE slot to a
     * clean tombstone and fires {@link Http2StreamListener#onStreamClosed}.
     */
    private void maybeCloseStream(Http2Stream s) {
        if (!s.isClosed()) {
            return;
        }
        int streamId = s.getStreamId();
        int slot = streamPool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND
                || streamPool.getSlotKind(slot) != Http2StreamPool.SLOT_LIVE) {
            return;
        }
        streamPool.closeLiveSlot(slot, Http2StreamPool.CLOSE_CLEAN);
        listener.onStreamClosed(streamId, Http2ErrorCode.NO_ERROR);
    }

    /**
     * Trigger 4: a scheduler emission drained some of {@code s}'s queue.
     * If the stream was parked and now has arena + ring headroom for a
     * hypothetical next enqueue, clear the park flag and fire
     * {@link Http2StreamListener#onStreamWritable}. Stream-window
     * secondary condition is deliberately left out here — the integration
     * layer's next enqueue only cares about cap headroom, and the
     * scheduler drain is evidence that the stream's windows are at least
     * non-blocking for the bytes we just sent.
     * <p>
     * M2 tech debt: under sustained zero-window traffic this can produce
     * a spurious {@code resumeSend} cycle when the cap-freeing emission
     * did not actually unblock forward progress (stream window dropped
     * to zero on the same emission). Measure under a deferred-body-heavy
     * workload; if it shows up in profiles, tighten the precondition to
     * {@code capFree && outboundStreamWindow > 0}.
     */
    private void maybeFireWritableAfterEmission(Http2Stream s) {
        if (!s.isOutboundParked()) {
            return;
        }
        if (s.getOutboundQueuedPayloadBytes() >= s.getOutboundArenaCap()) {
            return;
        }
        if (s.getOutboundTupleCount() >= s.getOutboundTupleCap()) {
            return;
        }
        s.setOutboundParked(false);
        listener.onStreamWritable(s.getStreamId());
    }

    /**
     * Trigger 1: a peer {@code WINDOW_UPDATE(streamId>0)} credited this
     * stream's outbound window. If the stream was parked, it is now
     * writable.
     */
    private void maybeFireWritableAfterStreamWindowUpdate(int slot) {
        Http2Stream s = streamPool.getLiveStream(slot);
        if (!s.isOutboundParked()) {
            return;
        }
        s.setOutboundParked(false);
        listener.onStreamWritable(s.getStreamId());
    }

    /**
     * Returns the peer's current advertised {@code MAX_FRAME_SIZE}
     * clamped to {@link Http2Settings#MAX_FRAME_SIZE_LOWER}. The peer
     * setting is tracked as an unsigned 32-bit long; the cap never rises
     * above {@link Http2Settings#MAX_FRAME_SIZE_UPPER} (2^24 - 1), so
     * the int narrow is safe.
     */
    private int peerMaxFrameSize() {
        long peer = peerAdvertised[Http2Settings.MAX_FRAME_SIZE];
        if (peer < Http2Settings.MAX_FRAME_SIZE_LOWER) {
            return Http2Settings.MAX_FRAME_SIZE_LOWER;
        }
        if (peer > Http2Settings.MAX_FRAME_SIZE_UPPER) {
            return Http2Settings.MAX_FRAME_SIZE_UPPER;
        }
        return (int) peer;
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

    /**
     * Shared preflight for {@link #enqueueData} and
     * {@link #emitResponseHeaders}: classifies the target stream against
     * the engine's pool state + the handler's captured generation token.
     * Returns {@link #ENQUEUE_OK} if the stream is LIVE, the generation
     * matches, the outbound direction is still active, and no prior
     * call has staged {@code END_STREAM}; otherwise the appropriate
     * sentinel.
     */
    private int validateOutboundTarget(int streamId, int generation) {
        int slot = streamPool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            return ENQUEUE_STALE_GENERATION;
        }
        if (streamPool.getSlotKind(slot) != Http2StreamPool.SLOT_LIVE) {
            // DISCARDING_BLOCK / TOMBSTONE / FREE slot for this id: the
            // handler's token is stale by definition.
            return ENQUEUE_STALE_GENERATION;
        }
        Http2Stream s = streamPool.getLiveStream(slot);
        if (s.getGeneration() != generation) {
            return ENQUEUE_STALE_GENERATION;
        }
        if (!s.isOutboundDirectionActive()) {
            return ENQUEUE_STREAM_CLOSED;
        }
        if (s.isOutboundEndStreamStaged()) {
            return ENQUEUE_STREAM_CLOSED;
        }
        return ENQUEUE_OK;
    }

    /**
     * HPACK listener that captures the five pseudo-header / Host slots
     * into the current {@link #captureTargetStream}'s per-stream staging
     * buffer. Other fields are consumed and dropped so the shared HPACK
     * dynamic table stays coherent. Name matching is byte-literal over
     * the decoder-produced {@code (nameAddr, nameLen)} pair; the hot path
     * is a single {@code nameLen} compare plus one memory compare per
     * short name and no allocation.
     */
    private final class Http2StreamHeaderCapturingListener implements HpackListener {

        @Override
        public void onHeader(long nameAddr, int nameLen, long valueAddr, int valueLen, boolean neverIndexed) {
            Http2Stream target = captureTargetStream;
            if (target == null) {
                return;
            }
            int slot = matchPseudoName(nameAddr, nameLen);
            if (slot < 0) {
                return;
            }
            target.captureHeaderSlot(slot, valueAddr, valueLen);
        }
    }

    /**
     * Read-only view over a {@link Http2Stream}'s staged pseudo-header
     * slots plus the captured {@code content-type}. A single instance per
     * connection is rebound via {@link #of} immediately before each
     * {@link Http2StreamListener#onRequestHeaders} dispatch; the returned
     * addresses live in the stream's staging buffer and remain valid
     * until the next request's {@link Http2Stream#beginInitialHeaders}
     * clears them.
     */
    private static final class Http2StreamHeadersView implements Http2RequestHeadersView {
        private Http2Stream target;

        @Override
        public long getAuthorityAddr() {
            return target == null ? 0L : target.getStagingSlotAddr(Http2Stream.STAGING_SLOT_AUTHORITY);
        }

        @Override
        public int getAuthorityLen() {
            return target == null ? 0 : target.getStagingSlotLen(Http2Stream.STAGING_SLOT_AUTHORITY);
        }

        @Override
        public long getContentTypeAddr() {
            return target == null ? 0L : target.getStagingSlotAddr(Http2Stream.STAGING_SLOT_CONTENT_TYPE);
        }

        @Override
        public int getContentTypeLen() {
            return target == null ? 0 : target.getStagingSlotLen(Http2Stream.STAGING_SLOT_CONTENT_TYPE);
        }

        @Override
        public long getMethodAddr() {
            return target == null ? 0L : target.getStagingSlotAddr(Http2Stream.STAGING_SLOT_METHOD);
        }

        @Override
        public int getMethodLen() {
            return target == null ? 0 : target.getStagingSlotLen(Http2Stream.STAGING_SLOT_METHOD);
        }

        @Override
        public long getPathAddr() {
            return target == null ? 0L : target.getStagingSlotAddr(Http2Stream.STAGING_SLOT_PATH);
        }

        @Override
        public int getPathLen() {
            return target == null ? 0 : target.getStagingSlotLen(Http2Stream.STAGING_SLOT_PATH);
        }

        @Override
        public long getSchemeAddr() {
            return target == null ? 0L : target.getStagingSlotAddr(Http2Stream.STAGING_SLOT_SCHEME);
        }

        @Override
        public int getSchemeLen() {
            return target == null ? 0 : target.getStagingSlotLen(Http2Stream.STAGING_SLOT_SCHEME);
        }

        Http2StreamHeadersView of(Http2Stream stream) {
            this.target = stream;
            return this;
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
