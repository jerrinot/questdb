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

import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Per-connection registry of outstanding Flight SQL tickets. A ticket
 * is the server-minted opaque handle returned by {@code GetFlightInfo}
 * and consumed by {@code DoGet} to drive the streaming response.
 * <p>
 * Wave 6a uses a fixed-capacity slot array. Ticket ids are monotonic
 * per registry and never repeat — registries are bound to a single
 * TCP connection, so the id space only needs to cover one client's
 * in-flight query count. {@link #acquire()} returns the newly-minted
 * id on success or {@code -1} when the cap is reached; callers convert
 * exhaustion into a {@code grpc-status: RESOURCE_EXHAUSTED} response.
 * <p>
 * Wave 7 will swap the flat scan for an HMAC-signed ticket and
 * introduce per-ticket expiration; the public surface is shaped so
 * those changes can happen behind the {@link TicketEntry} wall.
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
     * Releases a slot previously reserved via {@link #acquire()}. The
     * entry's native-memory payload (currently just the schema bytes)
     * is freed. No-op if the ticket is not live.
     */
    public void release(long ticketId) {
        TicketEntry e = entryById(ticketId);
        if (e != null) {
            e.release();
        }
    }

    /**
     * Per-ticket state. Wave 6a carries a pointer to a cached Arrow
     * Schema message and a hardcoded row set; Wave 6b replaces the row
     * set with a {@code RecordCursorFactory} + open cursor.
     */
    public static final class TicketEntry implements Closeable {
        boolean isInUse;
        int memoryTag;
        long[] rowValues;
        long schemaAddr;
        int schemaCap;
        int schemaLen;
        long ticketId;

        @Override
        public void close() {
            freeSchema();
            rowValues = null;
            isInUse = false;
            ticketId = 0;
        }

        public long[] getRowValues() {
            return rowValues;
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

        /**
         * Attaches a schema payload. Ownership of {@code addr} transfers
         * to the entry: {@link #release()} and {@link #close()} free it
         * under the supplied {@code memoryTag}. If {@code cap} is
         * greater than {@code len} the extra bytes are tracked so the
         * free call is symmetric with the original {@code Unsafe.malloc}.
         */
        public void setSchema(long addr, int len, int cap, int memoryTag) {
            if (addr == 0 && (len != 0 || cap != 0)) {
                throw new IllegalArgumentException("addr must be non-zero when len/cap non-zero");
            }
            if (len < 0 || cap < len) {
                throw new IllegalArgumentException("len/cap out of range");
            }
            freeSchema();
            this.schemaAddr = addr;
            this.schemaLen = len;
            this.schemaCap = cap;
            this.memoryTag = memoryTag;
        }

        public void setRowValues(long[] rowValues) {
            this.rowValues = rowValues;
        }

        void of(long ticketId) {
            this.ticketId = ticketId;
            this.isInUse = true;
            this.rowValues = null;
            this.schemaAddr = 0;
            this.schemaLen = 0;
            this.schemaCap = 0;
        }

        void release() {
            freeSchema();
            rowValues = null;
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
        }
    }
}
