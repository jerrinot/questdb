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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cutlass.arrow.column.ArrowColumnScratch;
import io.questdb.cutlass.arrow.ipc.ArrowBatchLayout;
import io.questdb.cutlass.arrow.ipc.ArrowRecordBatchWriter;
import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.cutlass.arrow.ipc.UnsupportedColumnTypeException;
import io.questdb.cutlass.flightsql.proto.FlightDataCodec;
import io.questdb.cutlass.flightsql.proto.TicketCodec;
import io.questdb.cutlass.flightsql.server.TicketRegistry.DoGetState;
import io.questdb.cutlass.flightsql.server.TicketRegistry.TicketEntry;
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.protobuf.ProtobufException;
import io.questdb.cutlass.protobuf.ProtobufWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Wave 6b {@code DoGet} handler. Consumes a {@code Ticket} gRPC message,
 * looks up the {@link TicketEntry} in the {@link TicketRegistry}, opens
 * the stashed {@code RecordCursor}, and streams RecordBatches packed as
 * {@code FlightData} messages. Terminates with {@code grpc-status} in
 * trailers.
 * <p>
 * The streaming state machine is re-entrant: when the underlying
 * {@code Http2ConnectionContext.enqueueData} returns {@code ENQUEUE_PARK}
 * (outbound flow-control window exhausted or the stream's egress queue
 * full), the handler records the current state on the ticket entry and
 * returns. The dispatcher's {@code onStreamWritable} callback re-enters
 * the handler to continue from the saved state. Built FlightData bytes
 * are retained across PARK so the retry does not rebuild metadata or
 * re-iterate the cursor.
 */
public final class DoGetHandler implements FlightSqlHandler, Closeable {

    /** Target row count per RecordBatch. */
    public static final int BATCH_SIZE_ROWS = 4096;
    private static final int INITIAL_BATCH_SCRATCH_CAP = 64 * 1024;
    private static final int METADATA_BUFFER_CAP = 16 * 1024;
    private static final Log LOG = LogFactory.getLog(DoGetHandler.class);
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
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        Unsafe.free(metadataBuffer, METADATA_BUFFER_CAP, memoryTag);
    }

    @Override
    public void onClientStreaming(FlightSqlCallContext ctx, long messageAddr, int messageLen, boolean endOfStream) {
        if (!endOfStream) {
            if (messageLen > 0) {
                try {
                    TicketCodec.decode(messageAddr, messageAddr + messageLen, ticketFields);
                } catch (ProtobufException e) {
                    LOG.error().$("DoGet ticket decode failed [msg=").$(e.getDebug()).I$();
                    rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "malformed Ticket");
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

        TicketEntry entry = ticketRegistry.entryById(ticketId);
        if (entry == null) {
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "unknown ticket");
            return;
        }
        ctx.setTicketId(ticketId);
        entry.setDoGetState(DoGetState.EMIT_HEADERS);
        drive(ctx, entry);
    }

    @Override
    public void onStreamWritable(FlightSqlCallContext ctx) {
        long ticketId = ctx.getTicketId();
        if (ticketId <= 0) {
            return;
        }
        TicketEntry entry = ticketRegistry.entryById(ticketId);
        if (entry == null) {
            return;
        }
        drive(ctx, entry);
    }

    private static boolean anyUtf8NearThreshold(ArrowColumnScratch[] scratches) {
        if (scratches == null) {
            return false;
        }
        for (int i = 0; i < scratches.length; i++) {
            if (scratches[i].isUtf8ValuesNearThreshold()) {
                return true;
            }
        }
        return false;
    }

    private static void appendCell(Record r, int ci, ArrowColumnScratch s, int qtype) {
        switch (ColumnType.tagOf(qtype)) {
            case ColumnType.LONG:
                s.appendLongOrNull(r.getLong(ci));
                break;
            case ColumnType.DOUBLE:
                s.appendDoubleOrNull(r.getDouble(ci));
                break;
            case ColumnType.INT:
                s.appendIntOrNull(r.getInt(ci));
                break;
            case ColumnType.FLOAT:
                s.appendFloatOrNull(r.getFloat(ci));
                break;
            case ColumnType.BYTE:
                s.appendByte(r.getByte(ci));
                break;
            case ColumnType.SHORT:
                s.appendShort(r.getShort(ci));
                break;
            case ColumnType.BOOLEAN:
                s.appendBool(r.getBool(ci));
                break;
            case ColumnType.DATE:
                s.appendLongOrNull(r.getDate(ci));
                break;
            case ColumnType.TIMESTAMP:
                s.appendLongOrNull(r.getTimestamp(ci));
                break;
            case ColumnType.STRING:
                s.appendStringOrNull(r.getStrA(ci));
                break;
            case ColumnType.VARCHAR:
                s.appendVarcharOrNull(r.getVarcharA(ci));
                break;
            case ColumnType.SYMBOL:
                s.appendStringOrNull(r.getSymA(ci));
                break;
            default:
                throw new UnsupportedColumnTypeException(qtype);
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

    private static void zeroRange(long start, long endExclusive) {
        long bytes = endExclusive - start;
        if (bytes > 0) {
            Unsafe.getUnsafe().setMemory(start, bytes, (byte) 0);
        }
    }

    private void appendRow(TicketEntry ticket, Record record) {
        int[] columnTypes = ticket.getColumnTypes();
        ArrowColumnScratch[] scratches = ticket.getScratches();
        for (int ci = 0; ci < columnTypes.length; ci++) {
            appendCell(record, ci, scratches[ci], columnTypes[ci]);
        }
        ticket.setRowsBuffered(ticket.getRowsBuffered() + 1);
    }

    /**
     * Ensures the ticket's batch scratch buffer holds at least
     * {@code required} bytes. Rounds first-time allocation up to
     * {@link #INITIAL_BATCH_SCRATCH_CAP} so small schema messages do
     * not incur many reallocations.
     */
    private void ensureBatchScratchCap(TicketEntry ticket, int required) {
        int floored = Math.max(required, INITIAL_BATCH_SCRATCH_CAP);
        ticket.ensureBatchScratchCap(floored, memoryTag);
    }

    /**
     * Drives the per-ticket state machine. Called from
     * {@link #onClientStreaming(FlightSqlCallContext, long, int, boolean)}
     * after the ticket is first resolved, and from {@link #onStreamWritable}
     * on every subsequent unpark.
     */
    private void drive(FlightSqlCallContext ctx, TicketEntry ticket) {
        boolean progressing = true;
        while (progressing) {
            switch (ticket.getDoGetState()) {
                case SETUP:
                    // Initial state set by onClientStreaming before the
                    // first drive call; should not re-enter here.
                    ticket.setDoGetState(DoGetState.EMIT_HEADERS);
                    break;
                case EMIT_HEADERS: {
                    int r = ctx.emitResponseHeaders();
                    if (r == FlightSqlCallContext.EMIT_PARK) {
                        return;
                    }
                    if (r != FlightSqlCallContext.EMIT_OK) {
                        LOG.error().$("DoGet response headers emit failed [rc=").$(r).I$();
                        releaseTicketAndDone(ctx, ticket);
                        return;
                    }
                    ticket.setDoGetState(DoGetState.EMIT_SCHEMA);
                    break;
                }
                case EMIT_SCHEMA: {
                    // If nothing is pending (fresh entry), build the schema FlightData now.
                    // FlightData.data_header carries the raw flatbuffer only -- skip the
                    // 8-byte IPC stream prefix stored on the ticket for FlightInfo.schema.
                    if (ticket.getBatchScratchLen() == 0) {
                        int encoded = writeFlightDataBytes(ticket,
                                ticket.getSchemaAddr() + 8, ticket.getRawSchemaLen(), 0, 0);
                        if (encoded < 0) {
                            rejectAfterHeaders(ctx, GrpcStatus.INTERNAL, "schema FlightData scratch overflow");
                            releaseTicketAndDone(ctx, ticket);
                            return;
                        }
                    }
                    int r = enqueueBatchData(ctx, ticket);
                    if (r == FlightSqlCallContext.EMIT_PARK) {
                        return;
                    }
                    if (r != FlightSqlCallContext.EMIT_OK) {
                        LOG.error().$("DoGet schema data emit failed [rc=").$(r).I$();
                        releaseTicketAndDone(ctx, ticket);
                        return;
                    }
                    ticket.setBatchScratchLen(0);
                    // Open cursor now; factory.getCursor may throw which
                    // maps to an error trailer.
                    if (ticket.getCursor() == null) {
                        try {
                            ticket.setCursor(ticket.getFactory().getCursor(ticket.getExecutionContext()));
                        } catch (io.questdb.griffin.SqlException e) {
                            LOG.error().$("DoGet cursor open failed [msg=").$(e.getFlyweightMessage()).I$();
                            ticket.setError(GrpcStatus.INVALID_ARGUMENT, e.getFlyweightMessage());
                            ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                            break;
                        } catch (CairoException e) {
                            LOG.error().$("DoGet cursor open failed [msg=").$(e.getFlyweightMessage()).I$();
                            int status = e.isAuthorizationError() ? GrpcStatus.PERMISSION_DENIED : GrpcStatus.INTERNAL;
                            ticket.setError(status, e.getFlyweightMessage());
                            ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                            break;
                        } catch (RuntimeException e) {
                            LOG.error().$("DoGet cursor open failed [msg=").$(e.getMessage()).I$();
                            ticket.setError(GrpcStatus.INTERNAL, "server error");
                            ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                            break;
                        }
                    }
                    ticket.setDoGetState(DoGetState.EMIT_BATCH);
                    break;
                }
                case EMIT_BATCH: {
                    // If the previous batch is still in-flight (PARK retry),
                    // re-enqueue its bytes from the ticket's scratch.
                    if (ticket.getBatchScratchLen() > 0) {
                        int r = enqueueBatchData(ctx, ticket);
                        if (r == FlightSqlCallContext.EMIT_PARK) {
                            return;
                        }
                        if (r != FlightSqlCallContext.EMIT_OK) {
                            LOG.error().$("DoGet batch data emit failed [rc=").$(r).I$();
                            releaseTicketAndDone(ctx, ticket);
                            return;
                        }
                        ticket.setBatchScratchLen(0);
                        resetScratches(ticket);
                    }
                    // Pull up to BATCH_SIZE_ROWS from the cursor. For Utf8
                    // columns also break out early if the accumulated
                    // values bytes approach the int32 ceiling, so the
                    // next append cannot overflow.
                    RecordCursor cursor = ticket.getCursor();
                    ArrowColumnScratch[] cursorScratches = ticket.getScratches();
                    try {
                        while (ticket.getRowsBuffered() < BATCH_SIZE_ROWS && cursor.hasNext()) {
                            appendRow(ticket, cursor.getRecord());
                            if (anyUtf8NearThreshold(cursorScratches)) {
                                break;
                            }
                        }
                    } catch (IllegalArgumentException e) {
                        LOG.error().$("DoGet Utf8 cell too large [msg=").$(e.getMessage()).I$();
                        ticket.setError(GrpcStatus.INTERNAL, "Utf8 cell exceeds per-cell cap");
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                        break;
                    } catch (CairoException e) {
                        LOG.error().$("DoGet cursor iteration failed [msg=").$(e.getFlyweightMessage()).I$();
                        int status = e.isAuthorizationError() ? GrpcStatus.PERMISSION_DENIED : GrpcStatus.INTERNAL;
                        ticket.setError(status, e.getFlyweightMessage());
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                        break;
                    } catch (UnsupportedColumnTypeException e) {
                        LOG.error().$("DoGet unsupported column type [type=").$(e.getColumnType()).I$();
                        ticket.setError(GrpcStatus.UNIMPLEMENTED, e.getMessage());
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                        break;
                    } catch (RuntimeException e) {
                        LOG.error().$("DoGet cursor iteration failed [msg=").$(e.getMessage()).I$();
                        ticket.setError(GrpcStatus.INTERNAL, "server error");
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                        break;
                    }

                    if (ticket.getRowsBuffered() == 0) {
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_OK);
                        break;
                    }

                    // Build FlightData{RecordBatch header, body = flushed scratches}.
                    int rowCount = ticket.getRowsBuffered();
                    int[] columnTypes = ticket.getColumnTypes();
                    ArrowColumnScratch[] scratches = ticket.getScratches();
                    ArrowBatchLayout layout = ticket.getBatchLayout();
                    layout.reset(scratches.length);
                    for (int ci = 0; ci < scratches.length; ci++) {
                        layout.set(
                                ci,
                                scratches[ci].getNullCount(),
                                scratches[ci].validityLengthBytes(),
                                scratches[ci].offsetsLengthBytes(),
                                scratches[ci].valuesLengthBytes()
                        );
                    }
                    layout.finish();
                    long bodyBytes = layout.bodyBytes();
                    if (bodyBytes > Integer.MAX_VALUE) {
                        ticket.setError(GrpcStatus.INTERNAL, "batch body exceeds 2 GiB");
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                        break;
                    }

                    fbWriter.of(metadataBuffer, metadataBuffer + METADATA_BUFFER_CAP);
                    int metaLen = ArrowRecordBatchWriter.writeRecordBatchMessage(fbWriter,
                            rowCount, columnTypes, layout);
                    if (metaLen <= 0) {
                        ticket.setError(GrpcStatus.INTERNAL, "record batch metadata scratch overflow");
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                        break;
                    }

                    // Layout: [PREFIX_LEN head-room][FlightData bytes][body staging].
                    // The body is staged at the tail of the scratch while
                    // the FlightData protobuf is serialised in front of it,
                    // then the tail is dropped.
                    int prefix = io.questdb.cutlass.grpc.GrpcFrameWriter.PREFIX_LEN;
                    int flightDataUpper = estimateFlightDataUpperBound(metaLen, (int) bodyBytes);
                    int totalScratchNeeded = prefix + flightDataUpper + (int) bodyBytes;
                    ensureBatchScratchCap(ticket, totalScratchNeeded);

                    long batchAddr = ticket.getBatchScratchAddr();
                    long bodyAddr = batchAddr + ticket.getBatchScratchCap() - (int) bodyBytes;
                    for (int ci = 0; ci < scratches.length; ci++) {
                        long validityStart = bodyAddr + layout.validityOffset(ci);
                        long afterValidity = scratches[ci].flushValidityTo(validityStart);
                        long validityAlignedEnd = bodyAddr
                                + ArrowRecordBatchWriter.alignTo8(afterValidity - bodyAddr);
                        zeroRange(afterValidity, validityAlignedEnd);

                        if (layout.hasOffsets(ci)) {
                            long offsetsStart = bodyAddr + layout.offsetsOffset(ci);
                            long afterOffsets = scratches[ci].flushOffsetsTo(offsetsStart);
                            long offsetsAlignedEnd = bodyAddr
                                    + ArrowRecordBatchWriter.alignTo8(afterOffsets - bodyAddr);
                            zeroRange(afterOffsets, offsetsAlignedEnd);
                        }

                        long valuesStart = bodyAddr + layout.valuesOffset(ci);
                        long afterValues = scratches[ci].flushValuesTo(valuesStart);
                        long valuesAlignedEnd = bodyAddr
                                + ArrowRecordBatchWriter.alignTo8(afterValues - bodyAddr);
                        zeroRange(afterValues, valuesAlignedEnd);
                    }

                    long metaAddr = fbWriter.finishedAddr();
                    long payloadStart = batchAddr + prefix;
                    long payloadLimit = bodyAddr;
                    protobufWriter.of(payloadStart, payloadLimit);
                    long end = FlightDataCodec.encode(protobufWriter, metaAddr, metaLen, bodyAddr, (int) bodyBytes);
                    if (end < 0) {
                        ticket.setError(GrpcStatus.INTERNAL, "record batch FlightData scratch overflow");
                        ticket.setDoGetState(DoGetState.EMIT_TRAILERS_ERR);
                        break;
                    }
                    int builtLen = (int) (end - payloadStart);
                    ticket.setBatchScratchLen(builtLen);
                    int r = enqueueBatchData(ctx, ticket);
                    if (r == FlightSqlCallContext.EMIT_PARK) {
                        return;
                    }
                    if (r != FlightSqlCallContext.EMIT_OK) {
                        LOG.error().$("DoGet batch data emit failed [rc=").$(r).I$();
                        releaseTicketAndDone(ctx, ticket);
                        return;
                    }
                    ticket.setBatchScratchLen(0);
                    resetScratches(ticket);
                    // Loop for the next batch.
                    break;
                }
                case EMIT_TRAILERS_OK: {
                    int r = ctx.emitTrailers(GrpcStatus.OK, null);
                    if (r == FlightSqlCallContext.EMIT_PARK) {
                        return;
                    }
                    if (r != FlightSqlCallContext.EMIT_OK) {
                        LOG.error().$("DoGet trailers emit failed [rc=").$(r).I$();
                    }
                    releaseTicketAndDone(ctx, ticket);
                    return;
                }
                case EMIT_TRAILERS_ERR: {
                    int r = ctx.emitTrailers(ticket.getErrStatus(), ticket.getErrMessage());
                    if (r == FlightSqlCallContext.EMIT_PARK) {
                        return;
                    }
                    if (r != FlightSqlCallContext.EMIT_OK) {
                        LOG.error().$("DoGet error trailers emit failed [rc=").$(r).I$();
                    }
                    releaseTicketAndDone(ctx, ticket);
                    return;
                }
                case DONE:
                default:
                    progressing = false;
                    break;
            }
        }
    }

    /**
     * Enqueues the ticket's currently-built FlightData bytes, which sit
     * at {@code batchScratchAddr + PREFIX_LEN} with {@code PREFIX_LEN}
     * bytes of headroom at the front for the 5-byte gRPC prefix.
     * Emits directly into the H2 outbound tuple queue via the call
     * context passthrough; returns the H2 enqueue status.
     */
    private int enqueueBatchData(FlightSqlCallContext ctx, TicketEntry ticket) {
        long batchAddr = ticket.getBatchScratchAddr();
        int flightDataLen = ticket.getBatchScratchLen();
        io.questdb.cutlass.grpc.GrpcFrameWriter.writePrefix(batchAddr, flightDataLen);
        int totalLen = io.questdb.cutlass.grpc.GrpcFrameWriter.PREFIX_LEN + flightDataLen;
        return ctx.emitDataMessagePrefixed(batchAddr, totalLen);
    }

    private int estimateFlightDataUpperBound(int metaLen, int bodyLen) {
        // tag (1) + length-delimited header (5 + metaLen) + tag (1) +
        // length-delimited body (5 + bodyLen) + small headroom.
        return 16 + metaLen + bodyLen;
    }

    private void releaseTicketAndDone(FlightSqlCallContext ctx, TicketEntry ticket) {
        ticket.setDoGetState(DoGetState.DONE);
        ticketRegistry.release(ticket.getTicketId());
        ctx.setTicketId(0);
    }

    /**
     * Resets per-column scratches between batches; keeps the native
     * buffers allocated for reuse.
     */
    private void resetScratches(TicketEntry ticket) {
        ArrowColumnScratch[] scratches = ticket.getScratches();
        if (scratches != null) {
            for (ArrowColumnScratch s : scratches) {
                if (s != null) {
                    s.reset();
                }
            }
        }
        ticket.setRowsBuffered(0);
    }

    /**
     * Encodes a FlightData message into the ticket's batch scratch
     * leaving the first {@code PREFIX_LEN} bytes reserved for the gRPC
     * frame prefix. Stores the encoded length (without prefix) on the
     * ticket. Returns {@code -1} on scratch overflow.
     */
    private int writeFlightDataBytes(TicketEntry ticket,
                                     long headerAddr, int headerLen,
                                     long bodyAddr, int bodyLen) {
        int estimated = estimateFlightDataUpperBound(headerLen, bodyLen)
                + io.questdb.cutlass.grpc.GrpcFrameWriter.PREFIX_LEN;
        ensureBatchScratchCap(ticket, estimated);
        long batchAddr = ticket.getBatchScratchAddr();
        long payloadStart = batchAddr + io.questdb.cutlass.grpc.GrpcFrameWriter.PREFIX_LEN;
        long payloadLimit = batchAddr + ticket.getBatchScratchCap();
        protobufWriter.of(payloadStart, payloadLimit);
        long end = FlightDataCodec.encode(protobufWriter, headerAddr, headerLen, bodyAddr, bodyLen);
        if (end < 0) {
            return -1;
        }
        int len = (int) (end - payloadStart);
        ticket.setBatchScratchLen(len);
        return len;
    }
}
