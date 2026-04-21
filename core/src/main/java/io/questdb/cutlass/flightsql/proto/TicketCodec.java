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

package io.questdb.cutlass.flightsql.proto;

import io.questdb.cutlass.protobuf.ProtobufException;
import io.questdb.cutlass.protobuf.ProtobufReader;
import io.questdb.cutlass.protobuf.ProtobufWireFormat;
import io.questdb.cutlass.protobuf.ProtobufWriter;

/**
 * Encoder / decoder for the Flight {@code Ticket} message.
 * <pre>
 *   message Ticket {
 *     bytes ticket = 1;
 *   }
 * </pre>
 */
public final class TicketCodec {

    public static final int FIELD_TICKET = 1;

    private TicketCodec() {
    }

    /**
     * Decodes a {@code Ticket} at {@code [addr, limit)} into {@code out}.
     * Unknown fields are skipped. The ticket slice references into the
     * caller-owned input buffer.
     */
    public static void decode(long addr, long limit, Fields out) {
        if (out == null) {
            throw new IllegalArgumentException("out must be non-null");
        }
        out.clear();
        ProtobufReader r = out.reader;
        r.of(addr, limit);
        while (r.hasMore()) {
            int tag = r.readTag();
            int fieldNumber = ProtobufWireFormat.fieldNumberOf(tag);
            int wireType = ProtobufWireFormat.wireTypeOf(tag);
            switch (fieldNumber) {
                case FIELD_TICKET:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED) {
                        throw ProtobufException.instance("Ticket.ticket wrong wire type");
                    }
                    r.readLengthDelimited();
                    out.ticketAddr = r.lastValueAddr();
                    out.ticketLen = r.lastValueLen();
                    break;
                default:
                    r.skipField(wireType);
                    break;
            }
        }
    }

    /**
     * Encodes a {@code Ticket} message body (no outer length prefix) into
     * {@code writer}. Returns the writer cursor on success, {@code -1} on
     * overflow.
     */
    public static long encode(ProtobufWriter writer, long ticketAddr, int ticketLen) {
        return writer.writeLengthDelimitedField(FIELD_TICKET, ticketAddr, ticketLen);
    }

    public static final class Fields {
        final ProtobufReader reader = new ProtobufReader();
        public long ticketAddr;
        public int ticketLen;

        public void clear() {
            ticketAddr = 0;
            ticketLen = 0;
        }
    }
}
