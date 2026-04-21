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

import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.arrow.column.ArrowColumnScratch;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Per-connection registry of outstanding Flight SQL tickets. A ticket is
 * the server-minted opaque handle returned by {@code GetFlightInfo} and
 * consumed by {@code DoGet} to drive the streaming response.
 * <p>
 * Wave 6b uses a fixed-capacity slot array. Ticket ids are monotonic per
 * registry and never repeat -- registries are bound to a single TCP
 * connection, so the id space only needs to cover one client's
 * in-flight query count. {@link #acquire()} returns the newly-minted id
 * on success or {@code -1} when the cap is reached; callers convert
 * exhaustion into a {@code grpc-status: RESOURCE_EXHAUSTED} response.
 * <p>
 * Wave 7 will swap the flat scan for an HMAC-signed ticket and
 * introduce per-ticket expiration; the public surface is shaped so those
 * changes can happen behind the {@link TicketEntry} wall.
 */
public final class TicketRegistry implements Closeable {

    public static final int DEFAULT_CAPACITY = 64;
    private final ObjList<TicketEntry> entries;
    private boolean isClosed;
    private long nextTicketId = 1L;

    public TicketRegistry(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.entries = new ObjList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            entries.add(new TicketEntry());
        }
    }

    /**
     * Reserves a new slot. Returns a monotonic ticket id (always
     * positive) on success or {@code -1} if every slot is in use.
     * Callers that receive {@code -1} must emit
     * {@code RESOURCE_EXHAUSTED}.
     * <p>
     * The caller populates the returned slot via {@link #entryById(long)}
     * before emitting the containing {@code FlightInfo}.
     */
    public long acquire() {
        for (int i = 0, n = entries.size(); i < n; i++) {
            TicketEntry e = entries.getQuick(i);
            if (!e.isInUse) {
                long id = nextTicketId++;
                e.of(id);
                return id;
            }
        }
        return -1L;
    }

    public int capacity() {
        return entries.size();
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        for (int i = 0, n = entries.size(); i < n; i++) {
            entries.getQuick(i).close();
        }
        entries.clear();
    }

    /**
     * Returns the live entry for {@code ticketId} or {@code null} if
     * the ticket is unknown (never minted, already released, or from a
     * different connection).
     */
    public TicketEntry entryById(long ticketId) {
        if (ticketId <= 0) {
            return null;
        }
        for (int i = 0, n = entries.size(); i < n; i++) {
            TicketEntry e = entries.getQuick(i);
            if (e.isInUse && e.ticketId == ticketId) {
                return e;
            }
        }
        return null;
    }

    public int inUseCount() {
        int count = 0;
        for (int i = 0, n = entries.size(); i < n; i++) {
            if (entries.getQuick(i).isInUse) {
                count++;
            }
        }
        return count;
    }

    /**
     * Releases a slot previously reserved via {@link #acquire()}. Frees
     * all native-memory payloads (schema bytes, batch scratch) and
     * closes the {@link RecordCursorFactory}, its cursor, and the
     * per-ticket execution context. No-op if the ticket is not live.
     */
    public void release(long ticketId) {
        TicketEntry e = entryById(ticketId);
        if (e != null) {
            e.release();
        }
    }

    /**
     * Streaming state for a single Flight SQL DoGet response. The state
     * machine is driven re-entrantly from {@code DoGetHandler}; when an
     * {@code enqueueData} returns {@code ENQUEUE_PARK} the handler
     * records the current state and returns, resuming on the next
     * {@code onStreamWritable}.
     */
    public enum DoGetState {
        /** Handler has received the ticket but not emitted response HEADERS. */
        SETUP,
        /** Emit {@code :status 200} + {@code content-type}. */
        EMIT_HEADERS,
        /** Emit {@code FlightData} carrying the schema message. */
        EMIT_SCHEMA,
        /** Emit / continue emitting RecordBatch {@code FlightData} messages. */
        EMIT_BATCH,
        /** Stream finished cleanly; emit {@code grpc-status: 0} trailers. */
        EMIT_TRAILERS_OK,
        /** Stream errored mid-response; emit {@code grpc-status != 0} trailers. */
        EMIT_TRAILERS_ERR,
        /** Terminal: no more work. */
        DONE
    }

    /**
     * Per-ticket state. Holds the compiled {@link RecordCursorFactory},
     * the open {@link RecordCursor} (lazily populated on first DoGet),
     * the per-ticket {@link SqlExecutionContextImpl} and circuit breaker,
     * the column-type array that drives the append switch, the per-column
     * {@link ArrowColumnScratch} instances reused across batches, the
     * streaming state machine, and a native scratch buffer for the most
     * recent FlightData bytes so PARK retry can resume without rebuilding.
     */
    public static final class TicketEntry implements Closeable {
        long batchScratchAddr;
        int batchScratchCap;
        int batchScratchLen;
        NetworkSqlExecutionCircuitBreaker circuitBreaker;
        int[] columnTypes;
        RecordCursor cursor;
        DoGetState doGetState = DoGetState.SETUP;
        /** Error status captured on the EMIT_TRAILERS_ERR transition. */
        int errStatus;
        /** Error message captured on the EMIT_TRAILERS_ERR transition. */
        CharSequence errMessage;
        SqlExecutionContextImpl executionContext;
        RecordCursorFactory factory;
        boolean isInUse;
        int memoryTag;
        /** Per-column Arrow {@code null_count}. Reused across batches. */
        long[] nullCounts;
        /** Rows appended to {@link #scratches} but not yet flushed on the wire. */
        int rowsBuffered;
        ArrowColumnScratch[] scratches;
        int rawSchemaLen;
        long schemaAddr;
        int schemaCap;
        int schemaLen;
        long ticketId;
        /** Per-column validity buffer length for the current batch, in bytes. */
        long[] validityLengths;
        /** Per-column values buffer length for the current batch, in bytes. */
        long[] valuesLengths;

        @Override
        public void close() {
            release();
        }

        public NetworkSqlExecutionCircuitBreaker getCircuitBreaker() {
            return circuitBreaker;
        }

        public int[] getColumnTypes() {
            return columnTypes;
        }

        public RecordCursor getCursor() {
            return cursor;
        }

        public DoGetState getDoGetState() {
            return doGetState;
        }

        public SqlExecutionContextImpl getExecutionContext() {
            return executionContext;
        }

        public RecordCursorFactory getFactory() {
            return factory;
        }

        public int getRowsBuffered() {
            return rowsBuffered;
        }

        public long[] getNullCounts() {
            return nullCounts;
        }

        public ArrowColumnScratch[] getScratches() {
            return scratches;
        }

        public int getRawSchemaLen() {
            return rawSchemaLen;
        }

        public long getSchemaAddr() {
            return schemaAddr;
        }

        public int getSchemaLen() {
            return schemaLen;
        }

        public long getTicketId() {
            return ticketId;
        }

        public long[] getValidityLengths() {
            return validityLengths;
        }

        public long[] getValuesLengths() {
            return valuesLengths;
        }

        /**
         * Ensures the batch scratch buffer holds at least {@code required}
         * bytes. Grows via {@link Unsafe#realloc}. On first allocation
         * captures the memory tag so {@link #release()} frees under the
         * correct bucket.
         */
        public void ensureBatchScratchCap(int required, int memoryTag) {
            if (batchScratchCap >= required) {
                return;
            }
            int newCap = Math.max(batchScratchCap * 2, required);
            batchScratchAddr = Unsafe.realloc(batchScratchAddr, batchScratchCap, newCap, memoryTag);
            batchScratchCap = newCap;
            this.memoryTag = memoryTag;
        }

        public void setCircuitBreaker(NetworkSqlExecutionCircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        public void setColumnTypes(int[] columnTypes) {
            this.columnTypes = columnTypes;
        }

        public void setCursor(RecordCursor cursor) {
            this.cursor = cursor;
        }

        public void setDoGetState(DoGetState doGetState) {
            this.doGetState = doGetState;
        }

        public void setError(int status, CharSequence message) {
            this.errStatus = status;
            this.errMessage = message;
        }

        public void setExecutionContext(SqlExecutionContextImpl executionContext) {
            this.executionContext = executionContext;
        }

        public void setFactory(RecordCursorFactory factory) {
            this.factory = factory;
        }

        public void setRowsBuffered(int rowsBuffered) {
            this.rowsBuffered = rowsBuffered;
        }

        public void setScratches(ArrowColumnScratch[] scratches) {
            this.scratches = scratches;
            int n = scratches == null ? 0 : scratches.length;
            if (nullCounts == null || nullCounts.length != n) {
                nullCounts = new long[n];
                validityLengths = new long[n];
                valuesLengths = new long[n];
            }
        }

        /**
         * Attaches a schema payload in Arrow IPC encapsulated stream format:
         * {@code [0xFFFFFFFF continuation, 4B][metadata_size LE, 4B]
         * [raw flatbuffer Message, rawSchemaLen B][zero padding to 8B]}.
         * The {@code (addr, len)} pair is what {@code FlightInfo.schema}
         * sends on the wire; DoGet's {@code FlightData.data_header} uses
         * {@code (addr + 8, rawSchemaLen)} to emit just the raw flatbuffer.
         * Ownership of {@code addr} transfers to the entry:
         * {@link #release()} and {@link #close()} free it under the supplied
         * {@code memoryTag}. {@code cap} may exceed {@code len} to make the
         * free call symmetric with the original {@code Unsafe.malloc}.
         */
        public void setSchema(long addr, int len, int cap, int rawSchemaLen, int memoryTag) {
            if (addr == 0 && (len != 0 || cap != 0)) {
                throw new IllegalArgumentException("addr must be non-zero when len/cap non-zero");
            }
            if (len < 0 || cap < len) {
                throw new IllegalArgumentException("len/cap out of range");
            }
            if (rawSchemaLen < 0 || rawSchemaLen + 8 > len) {
                throw new IllegalArgumentException("rawSchemaLen out of range");
            }
            freeSchema();
            this.schemaAddr = addr;
            this.schemaLen = len;
            this.schemaCap = cap;
            this.rawSchemaLen = rawSchemaLen;
            this.memoryTag = memoryTag;
        }

        public int getErrStatus() {
            return errStatus;
        }

        public CharSequence getErrMessage() {
            return errMessage;
        }

        public int getBatchScratchCap() {
            return batchScratchCap;
        }

        public long getBatchScratchAddr() {
            return batchScratchAddr;
        }

        public int getBatchScratchLen() {
            return batchScratchLen;
        }

        public void setBatchScratchLen(int batchScratchLen) {
            this.batchScratchLen = batchScratchLen;
        }

        void of(long ticketId) {
            this.ticketId = ticketId;
            this.isInUse = true;
            this.schemaAddr = 0;
            this.schemaLen = 0;
            this.schemaCap = 0;
            this.rawSchemaLen = 0;
            this.doGetState = DoGetState.SETUP;
            this.rowsBuffered = 0;
            this.batchScratchLen = 0;
            this.errStatus = 0;
            this.errMessage = null;
        }

        void release() {
            freeSchema();
            cursor = Misc.free(cursor);
            factory = Misc.free(factory);
            // SqlExecutionContextImpl doesn't own native memory directly in
            // the Wave 6b shape; reset for reuse rather than free.
            executionContext = null;
            // The circuit breaker is shared with the connection ctx
            // (HttpConnectionContext.getOrCreateCircuitBreaker reuses a
            // single instance per connection) -- do not close.
            circuitBreaker = null;
            if (scratches != null) {
                for (int i = 0; i < scratches.length; i++) {
                    scratches[i] = Misc.free(scratches[i]);
                }
                scratches = null;
            }
            nullCounts = null;
            validityLengths = null;
            valuesLengths = null;
            columnTypes = null;
            if (batchScratchAddr != 0) {
                Unsafe.free(batchScratchAddr, batchScratchCap, memoryTag);
                batchScratchAddr = 0;
                batchScratchCap = 0;
                batchScratchLen = 0;
            }
            rowsBuffered = 0;
            doGetState = DoGetState.SETUP;
            errStatus = 0;
            errMessage = null;
            isInUse = false;
            ticketId = 0;
        }

        private void freeSchema() {
            if (schemaAddr != 0) {
                Unsafe.free(schemaAddr, schemaCap, memoryTag);
            }
            schemaAddr = 0;
            schemaLen = 0;
            schemaCap = 0;
            rawSchemaLen = 0;
        }
    }
}
