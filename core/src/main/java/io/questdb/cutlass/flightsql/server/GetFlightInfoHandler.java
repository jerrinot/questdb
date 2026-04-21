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

import io.questdb.cutlass.flightsql.proto.FlightDescriptorCodec;
import io.questdb.cutlass.flightsql.proto.FlightInfoCodec;
import io.questdb.cutlass.grpc.GrpcFrameWriter;
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.protobuf.ProtobufException;
import io.questdb.cutlass.protobuf.ProtobufWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

/**
 * Wave 6a {@code GetFlightInfo} handler. Ignores the incoming
 * {@code FlightDescriptor.cmd} content and always returns a
 * {@code FlightInfo} carrying the cached Int64 schema, a freshly-minted
 * ticket, and a single {@code arrow-flight-reuse-connection://}
 * endpoint so the client reuses the existing gRPC channel for
 * {@code DoGet}.
 */
public final class GetFlightInfoHandler implements FlightSqlHandler, Closeable {

    public static final long[] HARDCODED_ROW_VALUES = {1L, 2L, 3L};
    private static final byte[] REUSE_CONNECTION_URI = "arrow-flight-reuse-connection://"
            .getBytes(StandardCharsets.US_ASCII);
    private static final Log LOG = LogFactory.getLog(GetFlightInfoHandler.class);
    private final FlightDescriptorCodec.Fields descriptorFields = new FlightDescriptorCodec.Fields();
    private final int memoryTag;
    private final ArrowSchemaCache schemaCache;
    private final long ticketBuf;
    private final TicketRegistry ticketRegistry;
    private final long uriAddr;
    private boolean isClosed;

    public GetFlightInfoHandler(ArrowSchemaCache schemaCache, TicketRegistry ticketRegistry, int memoryTag) {
        this.schemaCache = schemaCache;
        this.ticketRegistry = ticketRegistry;
        this.memoryTag = memoryTag;
        this.ticketBuf = Unsafe.malloc(8, memoryTag);
        this.uriAddr = Unsafe.malloc(REUSE_CONNECTION_URI.length, memoryTag);
        for (int i = 0; i < REUSE_CONNECTION_URI.length; i++) {
            Unsafe.getUnsafe().putByte(uriAddr + i, REUSE_CONNECTION_URI[i]);
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        Unsafe.free(uriAddr, REUSE_CONNECTION_URI.length, memoryTag);
        Unsafe.free(ticketBuf, 8, memoryTag);
    }

    @Override
    public void onClientStreaming(FlightSqlCallContext ctx, long messageAddr, int messageLen, boolean endOfStream) {
        if (!endOfStream) {
            if (messageLen > 0) {
                // Validate structure but ignore the cmd content — Wave 6a
                // returns hardcoded data for any descriptor.
                try {
                    FlightDescriptorCodec.decode(messageAddr, messageAddr + messageLen, descriptorFields);
                } catch (ProtobufException e) {
                    LOG.error().$("GetFlightInfo descriptor decode failed [msg=").$(e.getDebug()).I$();
                    rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "malformed FlightDescriptor");
                    return;
                }
            }
            return;
        }

        long ticketId = ticketRegistry.acquire();
        if (ticketId < 0) {
            rejectWithStatus(ctx, GrpcStatus.RESOURCE_EXHAUSTED, "ticket registry exhausted");
            return;
        }
        TicketRegistry.TicketEntry entry = ticketRegistry.entryById(ticketId);
        if (entry == null) {
            rejectWithStatus(ctx, GrpcStatus.INTERNAL, "ticket entry missing after acquire");
            return;
        }
        // Wave 6a: every entry gets its own copy of the cached schema
        // bytes so the entry owns its allocation. Wave 6b / 7 swap this
        // for per-query schema when the schema varies per compiled SQL.
        int schemaLen = schemaCache.getSchemaLen();
        long entrySchema = Unsafe.malloc(schemaLen, memoryTag);
        Unsafe.getUnsafe().copyMemory(schemaCache.getSchemaAddr(), entrySchema, schemaLen);
        entry.setSchema(entrySchema, schemaLen, schemaLen, memoryTag);
        entry.setRowValues(HARDCODED_ROW_VALUES);

        // Encode ticket bytes: 8-byte big-endian ticket id.
        encodeTicketIdBigEndian(ticketBuf, ticketId);

        int r = ctx.emitResponseHeaders();
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("GetFlightInfo response headers emit failed [rc=").$(r).I$();
            ticketRegistry.release(ticketId);
            return;
        }

        long bodyWriteAddr = ctx.getResponseBodyAddr() + GrpcFrameWriter.PREFIX_LEN;
        long bodyLimit = ctx.getResponseBodyAddr() + ctx.getResponseBodyCap();
        ProtobufWriter writer = new ProtobufWriter();
        writer.of(bodyWriteAddr, bodyLimit);
        long end = FlightInfoCodec.encodeSingleEndpoint(writer,
                schemaCache.getSchemaAddr(), schemaCache.getSchemaLen(),
                ticketBuf, 8,
                uriAddr, REUSE_CONNECTION_URI.length);
        if (end < 0) {
            LOG.error().$("GetFlightInfo FlightInfo scratch overflow").$();
            ticketRegistry.release(ticketId);
            rejectAfterHeaders(ctx, GrpcStatus.INTERNAL, "FlightInfo scratch overflow");
            return;
        }
        int bodyLen = (int) (end - bodyWriteAddr);
        r = ctx.emitDataMessage(bodyWriteAddr, bodyLen);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("GetFlightInfo data emit failed [rc=").$(r).I$();
            ticketRegistry.release(ticketId);
            return;
        }
        r = ctx.emitTrailers(GrpcStatus.OK, null);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("GetFlightInfo trailers emit failed [rc=").$(r).I$();
        }
    }

    private static void encodeTicketIdBigEndian(long addr, long ticketId) {
        Unsafe.getUnsafe().putByte(addr, (byte) ((ticketId >>> 56) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 1, (byte) ((ticketId >>> 48) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 2, (byte) ((ticketId >>> 40) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 3, (byte) ((ticketId >>> 32) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 4, (byte) ((ticketId >>> 24) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 5, (byte) ((ticketId >>> 16) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 6, (byte) ((ticketId >>> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 7, (byte) (ticketId & 0xFF));
    }

    private static void rejectAfterHeaders(FlightSqlCallContext ctx, int status, CharSequence message) {
        int r = ctx.emitTrailers(status, message);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("reject-after-headers emit failed [rc=").$(r).I$();
        }
    }

    private static void rejectWithStatus(FlightSqlCallContext ctx, int status, CharSequence message) {
        int r = ctx.emitTrailersOnly(status, message);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("reject-with-status emit failed [rc=").$(r).I$();
        }
    }
}
