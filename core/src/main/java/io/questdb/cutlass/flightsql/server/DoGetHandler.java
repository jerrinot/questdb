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

import io.questdb.cutlass.arrow.column.Int64ColumnEmitter;
import io.questdb.cutlass.arrow.ipc.ArrowRecordBatchWriter;
import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.cutlass.flightsql.proto.FlightDataCodec;
import io.questdb.cutlass.flightsql.proto.TicketCodec;
import io.questdb.cutlass.grpc.GrpcFrameWriter;
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.protobuf.ProtobufException;
import io.questdb.cutlass.protobuf.ProtobufWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Wave 6a {@code DoGet} handler. Consumes a {@code Ticket} gRPC message,
 * looks it up in the {@link TicketRegistry}, and streams the two
 * {@code FlightData} messages Arrow Flight clients expect for a result
 * set: one carrying the schema (metadata only, no body) and one
 * carrying the single {@code RecordBatch}. Trailers close with
 * {@code grpc-status: 0}.
 * <p>
 * The metadata / body layout is fixed in Wave 6a: one {@code Int64}
 * column with three rows. Wave 6b replaces the hardcoded row set with
 * a real {@code RecordCursorFactory} + open cursor.
 */
public final class DoGetHandler implements FlightSqlHandler, Closeable {

    private static final int BODY_BUFFER_CAP = 64 * 1024;
    private static final int METADATA_BUFFER_CAP = 16 * 1024;
    private static final Log LOG = LogFactory.getLog(DoGetHandler.class);
    private final long bodyBuffer;
    private final FbWriter fbWriter = new FbWriter();
    private final int memoryTag;
    private final long metadataBuffer;
    private final ProtobufWriter protobufWriter = new ProtobufWriter();
    private final TicketCodec.Fields ticketFields = new TicketCodec.Fields();
    private final TicketRegistry ticketRegistry;
    private boolean isClosed;

    public DoGetHandler(TicketRegistry ticketRegistry, int memoryTag) {
        this.ticketRegistry = ticketRegistry;
        this.memoryTag = memoryTag;
        this.metadataBuffer = Unsafe.malloc(METADATA_BUFFER_CAP, memoryTag);
        this.bodyBuffer = Unsafe.malloc(BODY_BUFFER_CAP, memoryTag);
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        Unsafe.free(bodyBuffer, BODY_BUFFER_CAP, memoryTag);
        Unsafe.free(metadataBuffer, METADATA_BUFFER_CAP, memoryTag);
    }

    @Override
    public void onClientStreaming(FlightSqlCallContext ctx, long messageAddr, int messageLen, boolean endOfStream) {
        if (!endOfStream) {
            if (messageLen > 0) {
                // Stash the ticket payload for the end-of-stream branch.
                // The registry lookup happens on END_STREAM so that
                // clients that misbehave mid-stream do not allocate.
                try {
                    TicketCodec.decode(messageAddr, messageAddr + messageLen, ticketFields);
                } catch (ProtobufException e) {
                    LOG.error().$("DoGet ticket decode failed [msg=").$(e.getDebug()).I$();
                    rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "malformed Ticket");
                    // Zero out so the end-of-stream branch bails.
                    ticketFields.clear();
                }
            }
            return;
        }

        if (ticketFields.ticketLen != 8) {
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "ticket length must be 8");
            ticketFields.clear();
            return;
        }
        long ticketId = decodeTicketIdBigEndian(ticketFields.ticketAddr);
        ticketFields.clear();

        TicketRegistry.TicketEntry entry = ticketRegistry.entryById(ticketId);
        if (entry == null) {
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "unknown ticket");
            return;
        }

        int r = ctx.emitResponseHeaders();
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("DoGet response headers emit failed [rc=").$(r).I$();
            return;
        }

        // Message 1: FlightData{data_header = schema bytes}. No body.
        long schemaAddr = entry.getSchemaAddr();
        int schemaLen = entry.getSchemaLen();
        int wrote = writeFlightDataMessage(ctx, schemaAddr, schemaLen, 0, 0);
        if (wrote < 0) {
            rejectAfterHeaders(ctx, GrpcStatus.INTERNAL, "schema FlightData scratch overflow");
            return;
        }
        r = ctx.emitDataMessage(ctx.getResponseBodyAddr() + GrpcFrameWriter.PREFIX_LEN, wrote);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("DoGet schema data emit failed [rc=").$(r).I$();
            return;
        }

        // Message 2: FlightData{data_header = record batch bytes,
        // data_body = int64 values}.
        long[] values = entry.getRowValues();
        int rowCount = values == null ? 0 : values.length;
        // Emit values into the dedicated body buffer.
        long bodyEnd = Int64ColumnEmitter.INSTANCE.emit(bodyBuffer, values == null ? new long[0] : values,
                0, rowCount);
        int bodyLen = (int) (bodyEnd - bodyBuffer);

        fbWriter.of(metadataBuffer, metadataBuffer + METADATA_BUFFER_CAP);
        int metaLen = ArrowRecordBatchWriter.writeInt64RecordBatchMessage(fbWriter,
                rowCount, 0L, (long) bodyLen);
        if (metaLen <= 0) {
            rejectAfterHeaders(ctx, GrpcStatus.INTERNAL, "record batch metadata scratch overflow");
            return;
        }
        long metaAddr = fbWriter.finishedAddr();

        wrote = writeFlightDataMessage(ctx, metaAddr, metaLen, bodyBuffer, bodyLen);
        if (wrote < 0) {
            rejectAfterHeaders(ctx, GrpcStatus.INTERNAL, "record batch FlightData scratch overflow");
            return;
        }
        r = ctx.emitDataMessage(ctx.getResponseBodyAddr() + GrpcFrameWriter.PREFIX_LEN, wrote);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("DoGet batch data emit failed [rc=").$(r).I$();
            return;
        }
        r = ctx.emitTrailers(GrpcStatus.OK, null);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("DoGet trailers emit failed [rc=").$(r).I$();
        }
    }

    private static long decodeTicketIdBigEndian(long addr) {
        long v = 0L;
        v |= ((long) (Unsafe.getUnsafe().getByte(addr) & 0xFF)) << 56;
        v |= ((long) (Unsafe.getUnsafe().getByte(addr + 1) & 0xFF)) << 48;
        v |= ((long) (Unsafe.getUnsafe().getByte(addr + 2) & 0xFF)) << 40;
        v |= ((long) (Unsafe.getUnsafe().getByte(addr + 3) & 0xFF)) << 32;
        v |= ((long) (Unsafe.getUnsafe().getByte(addr + 4) & 0xFF)) << 24;
        v |= ((long) (Unsafe.getUnsafe().getByte(addr + 5) & 0xFF)) << 16;
        v |= ((long) (Unsafe.getUnsafe().getByte(addr + 6) & 0xFF)) << 8;
        v |= Unsafe.getUnsafe().getByte(addr + 7) & 0xFF;
        return v;
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

    /**
     * Encodes a FlightData protobuf message into the response scratch
     * starting at {@code gRPC prefix + 5}. Returns the byte length of
     * the encoded message or {@code -1} on overflow.
     */
    private int writeFlightDataMessage(FlightSqlCallContext ctx,
                                       long headerAddr, int headerLen,
                                       long bodyAddr, int bodyLen) {
        long bodyWriteAddr = ctx.getResponseBodyAddr() + GrpcFrameWriter.PREFIX_LEN;
        long bodyLimit = ctx.getResponseBodyAddr() + ctx.getResponseBodyCap();
        protobufWriter.of(bodyWriteAddr, bodyLimit);
        long end = FlightDataCodec.encode(protobufWriter, headerAddr, headerLen, bodyAddr, bodyLen);
        if (end < 0) {
            return -1;
        }
        return (int) (end - bodyWriteAddr);
    }
}
