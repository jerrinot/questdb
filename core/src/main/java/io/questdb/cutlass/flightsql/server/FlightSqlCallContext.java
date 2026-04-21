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

package io.questdb.cutlass.flightsql.server;

import io.questdb.cutlass.grpc.GrpcFrameReader;
import io.questdb.cutlass.grpc.GrpcFrameWriter;
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.grpc.GrpcTrailerWriter;
import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2HeadersWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

/**
 * Per-stream Flight SQL call state. Pooled by
 * {@link FlightSqlCallContextPool} and bound to a
 * {@code (streamId, generation)} pair for the lifetime of one RPC
 * invocation. Delegates response emission to the composed
 * {@link Http2ConnectionContext}; owns the per-stream
 * {@link GrpcFrameReader} for inbound message reassembly and a private
 * {@link GrpcTrailerWriter} for emitting {@code grpc-status} /
 * {@code grpc-message} on the trailer path.
 * <p>
 * Usage order from the handler:
 * <ol>
 *   <li>{@link #emitResponseHeaders()} — at most once, before the first
 *       data message.</li>
 *   <li>{@link #emitDataMessage(long, int)} — zero or more times.</li>
 *   <li>{@link #emitTrailers(int, CharSequence)} — exactly once to
 *       terminate the stream.</li>
 * </ol>
 * For errors detected before any DATA has been enqueued the handler /
 * dispatcher calls {@link #emitTrailersOnly(int, CharSequence)}
 * instead, which collapses all four header / trailer fields into a
 * single HEADERS frame with {@code END_STREAM}.
 */
public final class FlightSqlCallContext implements Closeable {

    public static final int EMIT_HEADER_LIST_TOO_LARGE = Http2ConnectionContext.ENQUEUE_HEADER_LIST_TOO_LARGE;
    public static final int EMIT_OK = Http2ConnectionContext.ENQUEUE_OK;
    public static final int EMIT_PARK = Http2ConnectionContext.ENQUEUE_PARK;
    public static final int EMIT_STALE_GENERATION = Http2ConnectionContext.ENQUEUE_STALE_GENERATION;
    public static final int EMIT_STREAM_CLOSED = Http2ConnectionContext.ENQUEUE_STREAM_CLOSED;
    /**
     * Default scratch size for a handler's outgoing message body.
     * Sized for Wave 6a's {@code FlightInfo} and {@code FlightData}
     * messages (the RecordBatch header pushes toward the upper end of
     * this budget); Wave 5's Handshake needed only a few bytes so the
     * extra headroom is cheap. The response scratch is allocated once
     * per pool slot, not per call.
     */
    public static final int DEFAULT_RESPONSE_SCRATCH_BYTES = 8192;
    private static final byte[] CONTENT_TYPE_BYTES = "application/grpc+proto".getBytes(StandardCharsets.US_ASCII);
    // HPACK static table index for ":status 200" (RFC 7541 Appendix A).
    private static final int HPACK_STATIC_CONTENT_TYPE_NAME = 31;
    private static final int HPACK_STATIC_STATUS_200 = 8;
    private final long contentTypeAddr;
    private final int contentTypeLen;
    private final int memoryTag;
    private final GrpcFrameReader reader;
    private final long responseBodyAddr;
    private final int responseBodyCap;
    private final Http2HeadersWriter responseHeadersWriter = this::writeResponseHeaders;
    private final GrpcTrailerWriter trailerWriter;
    private final Http2HeadersWriter trailersOnlyWriter = this::writeTrailersOnly;
    private final Http2HeadersWriter trailersWriter = this::writeTrailers;
    private int generation;
    private Http2ConnectionContext h2;
    private FlightSqlHandler handler;
    private boolean isClosed;
    private boolean isInUse;
    private boolean responseHeadersEmitted;
    private int streamId;
    /** For DoGet: ticket id attached to this stream (0 if none). */
    private long ticketId;
    private int trailerStatus;
    private CharSequence trailerMessage;
    private boolean trailersEmitted;

    public FlightSqlCallContext(int maxMessageBytes, int memoryTag) {
        this(maxMessageBytes, DEFAULT_RESPONSE_SCRATCH_BYTES, memoryTag);
    }

    public FlightSqlCallContext(int maxMessageBytes, int responseBodyCap, int memoryTag) {
        this.memoryTag = memoryTag;
        this.reader = new GrpcFrameReader(maxMessageBytes, memoryTag);
        this.trailerWriter = new GrpcTrailerWriter(GrpcTrailerWriter.DEFAULT_MESSAGE_SCRATCH_BYTES, memoryTag);
        this.contentTypeLen = CONTENT_TYPE_BYTES.length;
        this.contentTypeAddr = Unsafe.malloc(contentTypeLen, memoryTag);
        for (int i = 0; i < contentTypeLen; i++) {
            Unsafe.getUnsafe().putByte(contentTypeAddr + i, CONTENT_TYPE_BYTES[i]);
        }
        this.responseBodyCap = responseBodyCap;
        this.responseBodyAddr = Unsafe.malloc(responseBodyCap, memoryTag);
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        reader.close();
        trailerWriter.close();
        Unsafe.free(contentTypeAddr, contentTypeLen, memoryTag);
        Unsafe.free(responseBodyAddr, responseBodyCap, memoryTag);
    }

    /**
     * Enqueues a single gRPC DATA message (5-byte prefix + body) on the
     * stream. The caller may pass any native address; if the caller has
     * already composed the body into the context's response scratch at
     * {@code getResponseBodyAddr() + GrpcFrameWriter.PREFIX_LEN} it
     * passes that address and the method skips the body copy. The
     * prefix is written into the context's response scratch; the H2
     * engine memcpy's the combined bytes into its per-stream arena
     * before returning.
     */
    public int emitDataMessage(long msgAddr, int msgLen) {
        if (trailersEmitted) {
            throw new IllegalStateException("trailers already emitted");
        }
        if (msgLen < 0) {
            throw new IllegalArgumentException("msgLen must be non-negative: " + msgLen);
        }
        int totalLen = GrpcFrameWriter.PREFIX_LEN + msgLen;
        if (totalLen > responseBodyCap) {
            throw new IllegalArgumentException("msgLen exceeds response scratch capacity: " + msgLen);
        }
        GrpcFrameWriter.writePrefix(responseBodyAddr, msgLen);
        if (msgLen > 0 && msgAddr != responseBodyAddr + GrpcFrameWriter.PREFIX_LEN) {
            Unsafe.getUnsafe().copyMemory(msgAddr, responseBodyAddr + GrpcFrameWriter.PREFIX_LEN, msgLen);
        }
        return h2.enqueueData(streamId, generation, responseBodyAddr, totalLen, false);
    }

    /**
     * Enqueues a DATA message whose bytes already include the 5-byte
     * gRPC prefix. Used by server-streaming handlers that build large
     * RecordBatch payloads in their own scratch (the fixed response-body
     * scratch is too small for a 4096-row RecordBatch). Caller layout:
     * {@code [prefix 5 bytes][body payloadLen-5 bytes]}.
     */
    public int emitDataMessagePrefixed(long payloadAddr, int payloadLen) {
        if (trailersEmitted) {
            throw new IllegalStateException("trailers already emitted");
        }
        if (payloadLen < GrpcFrameWriter.PREFIX_LEN) {
            throw new IllegalArgumentException("payloadLen must include the 5-byte gRPC prefix: " + payloadLen);
        }
        return h2.enqueueData(streamId, generation, payloadAddr, payloadLen, false);
    }

    /**
     * Emits the initial response HEADERS block for a successful RPC:
     * {@code :status 200, content-type: application/grpc+proto}.
     */
    public int emitResponseHeaders() {
        if (responseHeadersEmitted) {
            throw new IllegalStateException("response headers already emitted");
        }
        int result = h2.emitResponseHeaders(streamId, generation, responseHeadersWriter, false);
        if (result == EMIT_OK) {
            responseHeadersEmitted = true;
        }
        return result;
    }

    /**
     * Emits the terminating trailer HEADERS block for a stream that has
     * already emitted response HEADERS + zero or more DATA messages.
     * Includes {@code grpc-status} and, for non-OK statuses with a
     * non-empty {@code message}, {@code grpc-message}.
     */
    public int emitTrailers(int status, CharSequence message) {
        if (!responseHeadersEmitted) {
            throw new IllegalStateException("response headers not emitted; use emitTrailersOnly");
        }
        if (trailersEmitted) {
            throw new IllegalStateException("trailers already emitted");
        }
        trailerStatus = status;
        trailerMessage = message;
        int result = h2.emitTrailers(streamId, generation, trailersWriter, true);
        if (result == EMIT_OK) {
            trailersEmitted = true;
        }
        return result;
    }

    /**
     * Emits a single HEADERS frame containing {@code :status 200},
     * {@code content-type}, {@code grpc-status}, and (for non-OK)
     * {@code grpc-message}, with {@code END_STREAM} set. Used when the
     * dispatcher detects a request-time error before any DATA has been
     * enqueued.
     */
    public int emitTrailersOnly(int status, CharSequence message) {
        if (responseHeadersEmitted) {
            throw new IllegalStateException("response headers already emitted; use emitTrailers");
        }
        if (trailersEmitted) {
            throw new IllegalStateException("trailers already emitted");
        }
        trailerStatus = status;
        trailerMessage = message;
        int result = h2.emitResponseHeaders(streamId, generation, trailersOnlyWriter, true);
        if (result == EMIT_OK) {
            responseHeadersEmitted = true;
            trailersEmitted = true;
        }
        return result;
    }

    public int getGeneration() {
        return generation;
    }

    public FlightSqlHandler getHandler() {
        return handler;
    }

    public long getResponseBodyAddr() {
        return responseBodyAddr;
    }

    public int getResponseBodyCap() {
        return responseBodyCap;
    }

    public int getStreamId() {
        return streamId;
    }

    public long getTicketId() {
        return ticketId;
    }

    public boolean isInUse() {
        return isInUse;
    }

    public void of(Http2ConnectionContext h2, int streamId, int generation) {
        if (isInUse) {
            throw new IllegalStateException("call context already in use");
        }
        this.h2 = h2;
        this.streamId = streamId;
        this.generation = generation;
        this.handler = null;
        this.ticketId = 0;
        this.responseHeadersEmitted = false;
        this.trailersEmitted = false;
        this.isInUse = true;
        reader.clear();
    }

    public void setHandler(FlightSqlHandler handler) {
        this.handler = handler;
    }

    public void setTicketId(long ticketId) {
        this.ticketId = ticketId;
    }

    public GrpcFrameReader reader() {
        return reader;
    }

    public void release() {
        isInUse = false;
        h2 = null;
        handler = null;
        streamId = 0;
        generation = 0;
        ticketId = 0;
        trailerMessage = null;
        reader.clear();
    }

    private long writeResponseHeaders(HpackEncoder encoder, long cursor, long limit) {
        long c = encoder.encode(cursor, limit, 0, 0, 0, 0, HpackEncoder.HINT_STATIC_INDEX | HPACK_STATIC_STATUS_200);
        if (c < 0) {
            return -1;
        }
        return encoder.encode(c, limit, 0, 0, contentTypeAddr, contentTypeLen,
                HpackEncoder.HINT_STATIC_NAME | HPACK_STATIC_CONTENT_TYPE_NAME);
    }

    private long writeTrailers(HpackEncoder encoder, long cursor, long limit) {
        return trailerWriter.writeTrailers(encoder, cursor, limit, trailerStatus, trailerMessage);
    }

    private long writeTrailersOnly(HpackEncoder encoder, long cursor, long limit) {
        long c = encoder.encode(cursor, limit, 0, 0, 0, 0, HpackEncoder.HINT_STATIC_INDEX | HPACK_STATIC_STATUS_200);
        if (c < 0) {
            return -1;
        }
        c = encoder.encode(c, limit, 0, 0, contentTypeAddr, contentTypeLen,
                HpackEncoder.HINT_STATIC_NAME | HPACK_STATIC_CONTENT_TYPE_NAME);
        if (c < 0) {
            return -1;
        }
        return trailerWriter.writeTrailers(encoder, c, limit,
                trailerStatus, trailerStatus == GrpcStatus.OK ? null : trailerMessage);
    }

    public static FlightSqlCallContext newInstanceForTesting(int maxMessageBytes) {
        return new FlightSqlCallContext(maxMessageBytes, MemoryTag.NATIVE_DEFAULT);
    }
}
