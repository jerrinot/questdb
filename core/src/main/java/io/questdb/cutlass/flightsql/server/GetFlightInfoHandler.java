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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.arrow.column.ArrowColumnScratch;
import io.questdb.cutlass.arrow.ipc.ArrowSchemaWriter;
import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.cutlass.arrow.ipc.UnsupportedColumnTypeException;
import io.questdb.cutlass.flightsql.proto.CommandStatementQueryCodec;
import io.questdb.cutlass.flightsql.proto.FlightDescriptorCodec;
import io.questdb.cutlass.flightsql.proto.FlightInfoCodec;
import io.questdb.cutlass.grpc.GrpcFrameWriter;
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.protobuf.AnyCodec;
import io.questdb.cutlass.protobuf.ProtobufException;
import io.questdb.cutlass.protobuf.ProtobufWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

/**
 * Wave 6b {@code GetFlightInfo} handler. Decodes the incoming
 * {@code FlightDescriptor.cmd} as {@code google.protobuf.Any} wrapping a
 * {@code CommandStatementQuery}, compiles the enclosed SQL against the
 * connection's {@link CairoEngine}, validates the resulting metadata
 * against the supported scalar types (LONG / DOUBLE / INT), builds the
 * Arrow IPC Schema bytes, and stashes the compiled
 * {@link RecordCursorFactory} on a new {@link TicketRegistry.TicketEntry}
 * alongside the per-ticket execution state. Replies with a
 * {@code FlightInfo} that points {@code DoGet} at the just-minted
 * ticket.
 * <p>
 * The SQL compiler is acquired from the engine pool only for the
 * duration of the compile call; execution happens later inside
 * {@code DoGetHandler}. Errors map to gRPC statuses per
 * {@code FLIGHT_SQL_DESIGN.md} -- {@code SqlException} /
 * {@code ImplicitCastException} -> {@code INVALID_ARGUMENT};
 * authorization -> {@code PERMISSION_DENIED};
 * {@code UnsupportedColumnTypeException} -> {@code UNIMPLEMENTED};
 * everything else -> {@code INTERNAL}.
 */
public final class GetFlightInfoHandler implements FlightSqlHandler, Closeable {

    private static final int NAME_SCRATCH_CAP = 256;
    private static final byte[] REUSE_CONNECTION_URI = "arrow-flight-reuse-connection://?"
            .getBytes(StandardCharsets.US_ASCII);
    private static final int SCHEMA_SCRATCH_CAP = 32 * 1024;
    private static final Log LOG = LogFactory.getLog(GetFlightInfoHandler.class);
    private final AnyCodec.Fields anyFields = new AnyCodec.Fields();
    private final CommandStatementQueryCodec.Fields cmdFields = new CommandStatementQueryCodec.Fields();
    private final FlightDescriptorCodec.Fields descriptorFields = new FlightDescriptorCodec.Fields();
    private final FbWriter fbWriter = new FbWriter();
    private final int memoryTag;
    private final long nameScratchAddr;
    private final FlightSqlResources resources;
    private final long schemaScratchAddr;
    private final long ticketBuf;
    private final TicketRegistry ticketRegistry;
    private final long uriAddr;
    private boolean isClosed;

    public GetFlightInfoHandler(FlightSqlResources resources, TicketRegistry ticketRegistry, int memoryTag) {
        this.resources = resources;
        this.ticketRegistry = ticketRegistry;
        this.memoryTag = memoryTag;
        this.ticketBuf = Unsafe.malloc(8, memoryTag);
        this.uriAddr = Unsafe.malloc(REUSE_CONNECTION_URI.length, memoryTag);
        for (int i = 0; i < REUSE_CONNECTION_URI.length; i++) {
            Unsafe.getUnsafe().putByte(uriAddr + i, REUSE_CONNECTION_URI[i]);
        }
        this.schemaScratchAddr = Unsafe.malloc(SCHEMA_SCRATCH_CAP, memoryTag);
        this.nameScratchAddr = Unsafe.malloc(NAME_SCRATCH_CAP, memoryTag);
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        Unsafe.free(nameScratchAddr, NAME_SCRATCH_CAP, memoryTag);
        Unsafe.free(schemaScratchAddr, SCHEMA_SCRATCH_CAP, memoryTag);
        Unsafe.free(uriAddr, REUSE_CONNECTION_URI.length, memoryTag);
        Unsafe.free(ticketBuf, 8, memoryTag);
    }

    @Override
    public void onClientStreaming(FlightSqlCallContext ctx, long messageAddr, int messageLen, boolean endOfStream) {
        if (!endOfStream) {
            if (messageLen > 0) {
                try {
                    FlightDescriptorCodec.decode(messageAddr, messageAddr + messageLen, descriptorFields);
                } catch (ProtobufException e) {
                    LOG.error().$("GetFlightInfo descriptor decode failed [msg=").$(e.getDebug()).I$();
                    rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "malformed FlightDescriptor");
                    descriptorFields.clear();
                }
            }
            return;
        }

        if (descriptorFields.cmdAddr == 0 || descriptorFields.cmdLen <= 0) {
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "FlightDescriptor.cmd missing");
            descriptorFields.clear();
            return;
        }

        // Decode Any(CommandStatementQuery) out of FlightDescriptor.cmd.
        long cmdAddr = descriptorFields.cmdAddr;
        int cmdLen = descriptorFields.cmdLen;
        try {
            AnyCodec.decode(cmdAddr, cmdAddr + cmdLen, anyFields);
        } catch (ProtobufException e) {
            LOG.error().$("GetFlightInfo Any decode failed [msg=").$(e.getDebug()).I$();
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "malformed Any in FlightDescriptor.cmd");
            descriptorFields.clear();
            return;
        }

        if (!matchesCommandStatementQuery(anyFields.typeUrlAddr, anyFields.typeUrlLen)) {
            rejectWithStatus(ctx, GrpcStatus.UNIMPLEMENTED, "only CommandStatementQuery is supported");
            descriptorFields.clear();
            anyFields.clear();
            return;
        }

        try {
            CommandStatementQueryCodec.decode(anyFields.valueAddr,
                    anyFields.valueAddr + anyFields.valueLen, cmdFields);
        } catch (ProtobufException e) {
            LOG.error().$("GetFlightInfo CommandStatementQuery decode failed [msg=").$(e.getDebug()).I$();
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "malformed CommandStatementQuery");
            descriptorFields.clear();
            anyFields.clear();
            return;
        }

        if (cmdFields.queryLen <= 0) {
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "query is empty");
            descriptorFields.clear();
            anyFields.clear();
            cmdFields.clear();
            return;
        }

        CharSequence sql = copyUtf8ToString(cmdFields.queryAddr, cmdFields.queryLen);

        // Clear the descriptor / any / cmd fields now; the decoded string
        // owns the SQL bytes from here on and the slices are free to be
        // reused on subsequent calls.
        descriptorFields.clear();
        anyFields.clear();
        cmdFields.clear();

        CairoEngine engine = resources.getCairoEngine();
        if (engine == null) {
            rejectWithStatus(ctx, GrpcStatus.INTERNAL, "Flight SQL not bootstrapped with CairoEngine");
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

        // Prepare the per-ticket execution context. Reuses the per-connection
        // SqlExecutionContextImpl (it carries only Wave 6b-scope state) and
        // rebinds it to the current fd + a fresh AllowAllSecurityContext.
        SqlExecutionContextImpl executionContext = resources.getOrCreateSqlExecutionContext();
        NetworkSqlExecutionCircuitBreaker circuitBreaker = resources.getOrCreateCircuitBreaker();
        circuitBreaker.setFd(resources.getFd());
        circuitBreaker.resetMaxTimeToDefault();
        circuitBreaker.resetTimer();
        executionContext.with(AllowAllSecurityContext.INSTANCE, null, null,
                resources.getFd(), circuitBreaker);
        executionContext.initNow();

        RecordCursorFactory factory = null;
        try {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cq = compiler.compile(sql, executionContext);
                if (cq.getType() != CompiledQuery.SELECT && cq.getType() != CompiledQuery.PSEUDO_SELECT) {
                    rejectWithStatus(ctx, GrpcStatus.UNIMPLEMENTED, "only SELECT is supported in Wave 6b");
                    ticketRegistry.release(ticketId);
                    return;
                }
                factory = cq.getRecordCursorFactory();
            }

            RecordMetadata metadata = factory.getMetadata();
            int columnCount = metadata.getColumnCount();
            if (columnCount <= 0) {
                rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, "query produces no columns");
                Misc.free(factory);
                ticketRegistry.release(ticketId);
                return;
            }

            // Validate each column type and capture the flat type array.
            int[] columnTypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                int t = metadata.getColumnType(i);
                if (!isSupportedColumnType(t)) {
                    rejectWithStatus(ctx, GrpcStatus.UNIMPLEMENTED,
                            "unsupported column type: " + ColumnType.nameOf(t));
                    Misc.free(factory);
                    ticketRegistry.release(ticketId);
                    return;
                }
                columnTypes[i] = t;
            }

            // Build Arrow schema bytes into the handler's scratch, then copy
            // into a per-ticket allocation wrapped in the Arrow IPC
            // encapsulated stream format so FlightInfo.schema parses with
            // MessageSerializer.deserializeSchema(ReadChannel) on the
            // client. Layout:
            //   [0xFFFFFFFF continuation, 4B]
            //   [metadata_size LE, 4B]
            //   [raw flatbuffer Message, rawSchemaLen B]
            //   [zero padding to 8-byte boundary]
            // metadata_size includes padding per Arrow spec. DoGet reads
            // (addr + 8, rawSchemaLen) to emit just the raw flatbuffer.
            fbWriter.of(schemaScratchAddr, schemaScratchAddr + SCHEMA_SCRATCH_CAP);
            int rawSchemaLen = ArrowSchemaWriter.writeSchemaMessage(fbWriter, nameScratchAddr,
                    NAME_SCRATCH_CAP, metadata);
            if (rawSchemaLen <= 0) {
                rejectWithStatus(ctx, GrpcStatus.INTERNAL, "schema scratch overflow");
                Misc.free(factory);
                ticketRegistry.release(ticketId);
                return;
            }
            int paddedMetaLen = (rawSchemaLen + 7) & ~7;
            int framedLen = 8 + paddedMetaLen;
            long finishedAddr = fbWriter.finishedAddr();
            long entrySchemaAddr = Unsafe.malloc(framedLen, memoryTag);
            Unsafe.getUnsafe().putInt(entrySchemaAddr, 0xFFFFFFFF);
            Unsafe.getUnsafe().putInt(entrySchemaAddr + 4, paddedMetaLen);
            Unsafe.getUnsafe().copyMemory(finishedAddr, entrySchemaAddr + 8, rawSchemaLen);
            int padBytes = paddedMetaLen - rawSchemaLen;
            if (padBytes > 0) {
                Unsafe.getUnsafe().setMemory(entrySchemaAddr + 8 + rawSchemaLen, padBytes, (byte) 0);
            }

            // Wire all the per-ticket state into the entry.
            entry.setFactory(factory);
            factory = null; // ownership transferred
            entry.setExecutionContext(executionContext);
            entry.setCircuitBreaker(circuitBreaker);
            entry.setColumnTypes(columnTypes);
            ArrowColumnScratch[] scratches = new ArrowColumnScratch[columnCount];
            for (int i = 0; i < columnCount; i++) {
                scratches[i] = new ArrowColumnScratch(memoryTag);
                scratches[i].initFor(columnTypes[i], 4096);
            }
            entry.setScratches(scratches);
            entry.setSchema(entrySchemaAddr, framedLen, framedLen, rawSchemaLen, memoryTag);

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
                    entry.getSchemaAddr(), entry.getSchemaLen(),
                    0, 0,
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
        } catch (SqlException e) {
            LOG.error().$("GetFlightInfo compile failed [msg=").$(e.getFlyweightMessage()).I$();
            Misc.free(factory);
            ticketRegistry.release(ticketId);
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, e.getFlyweightMessage());
        } catch (ImplicitCastException e) {
            LOG.error().$("GetFlightInfo implicit cast failed [msg=").$(e.getFlyweightMessage()).I$();
            Misc.free(factory);
            ticketRegistry.release(ticketId);
            rejectWithStatus(ctx, GrpcStatus.INVALID_ARGUMENT, e.getFlyweightMessage());
        } catch (UnsupportedColumnTypeException e) {
            LOG.error().$("GetFlightInfo unsupported column type [type=").$(e.getColumnType()).I$();
            Misc.free(factory);
            ticketRegistry.release(ticketId);
            rejectWithStatus(ctx, GrpcStatus.UNIMPLEMENTED, e.getMessage());
        } catch (CairoException e) {
            Misc.free(factory);
            ticketRegistry.release(ticketId);
            if (e.isAuthorizationError()) {
                LOG.error().$("GetFlightInfo authorization error [msg=").$(e.getFlyweightMessage()).I$();
                rejectWithStatus(ctx, GrpcStatus.PERMISSION_DENIED, e.getFlyweightMessage());
            } else {
                LOG.error().$("GetFlightInfo cairo error [msg=").$(e.getFlyweightMessage()).I$();
                rejectWithStatus(ctx, GrpcStatus.INTERNAL, e.getFlyweightMessage());
            }
        } catch (RuntimeException e) {
            LOG.error().$("GetFlightInfo unexpected error [msg=").$(e.getMessage()).I$();
            Misc.free(factory);
            ticketRegistry.release(ticketId);
            rejectWithStatus(ctx, GrpcStatus.INTERNAL, "server error");
        }
    }

    private static String copyUtf8ToString(long addr, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
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

    private static boolean isSupportedColumnType(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.INT:
            case ColumnType.FLOAT:
            case ColumnType.BYTE:
            case ColumnType.SHORT:
            case ColumnType.BOOLEAN:
                return true;
            default:
                return false;
        }
    }

    private static boolean matchesCommandStatementQuery(long addr, int len) {
        byte[] expected = CommandStatementQueryCodec.TYPE_URL.getBytes(StandardCharsets.US_ASCII);
        if (len != expected.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (Unsafe.getUnsafe().getByte(addr + i) != expected[i]) {
                return false;
            }
        }
        return true;
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
