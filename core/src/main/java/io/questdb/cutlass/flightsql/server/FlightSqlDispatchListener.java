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
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2Stream;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.cutlass.http2.Http2StreamPool;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * {@link Http2StreamListener} implementation that terminates the gRPC
 * protocol for the Flight SQL service. Routes incoming RPCs by
 * {@code :path}; rejects non-conforming requests with trailers-only
 * {@code grpc-status} responses. Wave 5 only wires the
 * {@code /arrow.flight.protocol.FlightService/Handshake} route; every
 * other path returns {@code UNIMPLEMENTED}.
 */
public final class FlightSqlDispatchListener implements Http2StreamListener, Closeable {

    public static final int DEFAULT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024;
    public static final String HANDSHAKE_PATH = "/arrow.flight.protocol.FlightService/Handshake";
    private static final byte[] CONTENT_TYPE_GRPC_PREFIX = "application/grpc".getBytes();
    private static final Log LOG = LogFactory.getLog(FlightSqlDispatchListener.class);
    private static final byte[] METHOD_POST = "POST".getBytes();
    private final FlightSqlCallContextPool contextPool;
    private final HandshakeHandler handshakeHandler;
    private Http2ConnectionContext h2;
    // Route table is flat because Wave 5 only has one route. When more
    // routes land, promote to an IntObjHashMap keyed on a hash of the
    // path bytes (prompt §3).
    private final byte[] handshakePathBytes = HANDSHAKE_PATH.getBytes();
    private boolean isClosed;

    public FlightSqlDispatchListener(FlightSqlCallContextPool contextPool,
                                     HandshakeHandler handshakeHandler) {
        this.contextPool = contextPool;
        this.handshakeHandler = handshakeHandler;
    }

    /**
     * Binds the composed {@link Http2ConnectionContext}. Must be called
     * exactly once after construction, before any inbound traffic
     * reaches the listener — the circular reference between the engine
     * and this listener prevents constructor-time binding.
     */
    public void bind(Http2ConnectionContext h2) {
        if (this.h2 != null) {
            throw new IllegalStateException("already bound");
        }
        this.h2 = h2;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        contextPool.close();
    }

    @Override
    public boolean onData(int streamId, long addr, int dataLen, boolean endStream, int generationToken) {
        FlightSqlCallContext ctx = contextPool.lookup(streamId);
        if (ctx == null) {
            // Stream was rejected at headers time; drop any follow-up DATA.
            return true;
        }
        GrpcFrameReader reader = ctx.reader();
        if (!reader.feed(addr, dataLen)) {
            rejectWithError(ctx, GrpcStatus.RESOURCE_EXHAUSTED,
                    "request message exceeds max size");
            return true;
        }
        FlightSqlHandler handler = ctx.getHandler();
        while (true) {
            int r = reader.tryReadMessage();
            if (r == GrpcFrameReader.READ_NEED_MORE) {
                break;
            }
            if (r == GrpcFrameReader.READ_ERROR_COMPRESSED) {
                rejectWithError(ctx, GrpcStatus.UNIMPLEMENTED,
                        "per-message compression not supported");
                return true;
            }
            if (r == GrpcFrameReader.READ_ERROR_TOO_LARGE) {
                rejectWithError(ctx, GrpcStatus.RESOURCE_EXHAUSTED,
                        "request message exceeds max size");
                return true;
            }
            // MESSAGE_READY
            if (handler != null) {
                handler.onClientStreaming(ctx, reader.lastMessageAddr(), reader.lastMessageLen(), false);
            }
        }
        if (endStream && handler != null) {
            handler.onClientStreaming(ctx, 0, 0, true);
        }
        return true;
    }

    @Override
    public void onRequestHeader(int streamId, long nameAddr, int nameLen,
                                long valueAddr, int valueLen, boolean neverIndexed) {
        // Wave 5 ignores regular headers. The pseudo-headers captured
        // by the H2 engine cover the full dispatch surface.
    }

    @Override
    public void onRequestHeaders(int streamId, Http2RequestHeadersView view, boolean endStream) {
        FlightSqlCallContext ctx = contextPool.acquire(streamId);
        if (ctx == null) {
            LOG.error().$("flight sql dispatch pool exhausted [sid=").$(streamId).I$();
            // Without a ctx we cannot emit a per-stream trailers-only
            // response; relying on H2's RST_STREAM here would be nicer
            // but for Wave 5 we just drop the stream on the floor and
            // let it time out. This branch is unreachable at default
            // concurrency.
            return;
        }
        ctx.of(h2, streamId, lookupGeneration(streamId));
        // Method check.
        if (!bytesEqual(view.getMethodAddr(), view.getMethodLen(), METHOD_POST)) {
            rejectTrailersOnly(ctx, GrpcStatus.INTERNAL, "HTTP/2 method must be POST");
            return;
        }
        // Content-type must start with "application/grpc" (covers
        // application/grpc, application/grpc+proto, application/grpc+json, ...).
        if (!bytesStartsWith(view.getContentTypeAddr(), view.getContentTypeLen(), CONTENT_TYPE_GRPC_PREFIX)) {
            rejectTrailersOnly(ctx, GrpcStatus.INTERNAL, "content-type must be application/grpc*");
            return;
        }
        FlightSqlHandler handler = routeByPath(view.getPathAddr(), view.getPathLen());
        if (handler == null) {
            rejectTrailersOnly(ctx, GrpcStatus.UNIMPLEMENTED, "method not implemented");
            return;
        }
        ctx.setHandler(handler);
        if (endStream) {
            handler.onClientStreaming(ctx, 0, 0, true);
        }
    }

    @Override
    public void onStreamClosed(int streamId, int cause) {
        contextPool.release(streamId);
    }

    @Override
    public void onStreamWritable(int streamId) {
        // Wave 5 handlers emit their full response in one turn (the
        // Handshake response is ~8 bytes). Parking is theoretically
        // possible under tiny flow-control windows but is not exercised
        // by any Wave 5 test; full park / resume plumbing lands with
        // the server-streaming DoGet handler in a later wave.
    }

    @Override
    public void onTrailers(int streamId, boolean endStream) {
        // Clients that pack message bytes into trailers after DATA are
        // non-conformant for gRPC. For Wave 5 we just treat trailers
        // as the end-of-stream marker for the request side.
        FlightSqlCallContext ctx = contextPool.lookup(streamId);
        if (ctx == null) {
            return;
        }
        FlightSqlHandler handler = ctx.getHandler();
        if (handler != null) {
            handler.onClientStreaming(ctx, 0, 0, true);
        }
    }

    private static boolean bytesEqual(long addr, int len, byte[] expected) {
        if (addr == 0 || len != expected.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (Unsafe.getUnsafe().getByte(addr + i) != expected[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean bytesStartsWith(long addr, int len, byte[] prefix) {
        if (addr == 0 || len < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (Unsafe.getUnsafe().getByte(addr + i) != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private void rejectTrailersOnly(FlightSqlCallContext ctx, int status, CharSequence message) {
        int r = ctx.emitTrailersOnly(status, message);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("trailers-only emit failed [sid=").$(ctx.getStreamId())
                    .$(", rc=").$(r).I$();
        }
    }

    private void rejectWithError(FlightSqlCallContext ctx, int status, CharSequence message) {
        // If we've already sent response headers, we must use
        // emitTrailers; otherwise collapse into trailers-only.
        int r;
        try {
            r = ctx.emitTrailersOnly(status, message);
        } catch (IllegalStateException e) {
            r = ctx.emitTrailers(status, message);
        }
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("reject-with-error emit failed [sid=").$(ctx.getStreamId())
                    .$(", rc=").$(r).I$();
        }
    }

    private int lookupGeneration(int streamId) {
        Http2StreamPool pool = h2.getStreamPool();
        int slot = pool.lookup(streamId);
        if (slot == Http2StreamPool.SLOT_NOT_FOUND) {
            return 0;
        }
        Http2Stream s = pool.getLiveStream(slot);
        return s == null ? 0 : s.getGeneration();
    }

    private FlightSqlHandler routeByPath(long pathAddr, int pathLen) {
        if (bytesEqual(pathAddr, pathLen, handshakePathBytes)) {
            return handshakeHandler;
        }
        return null;
    }
}
