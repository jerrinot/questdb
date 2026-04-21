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

import io.questdb.cutlass.protobuf.ProtobufWireFormat;
import io.questdb.cutlass.protobuf.ProtobufWriter;

/**
 * Encoder for the Flight {@code FlightInfo} message.
 * <pre>
 *   message FlightInfo {
 *     bytes                schema            = 1;
 *     FlightDescriptor     flight_descriptor = 2;  // skipped
 *     repeated FlightEndpoint endpoint       = 3;
 *     int64                total_records     = 4;
 *     int64                total_bytes       = 5;
 *     bool                 ordered           = 6;  // skipped
 *     bytes                app_metadata      = 7;  // skipped
 *   }
 * </pre>
 * Wave 6a emits {@code total_records = -1} and {@code total_bytes = -1}
 * (unknown), a single {@code FlightEndpoint} with a locally-minted ticket,
 * and a hardcoded {@code arrow-flight-reuse-connection://} URI.
 */
public final class FlightInfoCodec {

    public static final int FIELD_ENDPOINT = 3;
    public static final int FIELD_SCHEMA = 1;
    public static final int FIELD_TOTAL_BYTES = 5;
    public static final int FIELD_TOTAL_RECORDS = 4;

    private FlightInfoCodec() {
    }

    /**
     * Writes a {@code FlightInfo} body into {@code writer} with the
     * Wave 6a hardcoded shape: schema bytes + one endpoint + unknown
     * totals. Returns the writer cursor on success, {@code -1} on
     * overflow.
     */
    public static long encodeSingleEndpoint(ProtobufWriter writer,
                                            long schemaAddr, int schemaLen,
                                            long ticketAddr, int ticketLen,
                                            long uriAddr, int uriLen) {
        // schema bytes
        if (writer.writeLengthDelimitedField(FIELD_SCHEMA, schemaAddr, schemaLen) < 0) {
            return -1;
        }
        // endpoint (single)
        long endpointBodyStart = writer.beginNestedMessage(FIELD_ENDPOINT);
        if (endpointBodyStart < 0) {
            return -1;
        }
        if (FlightEndpointCodec.encode(writer, ticketAddr, ticketLen, uriAddr, uriLen) < 0) {
            return -1;
        }
        if (writer.endNestedMessage(endpointBodyStart) < 0) {
            return -1;
        }
        // total_records = -1 (encoded as signed-interpretation of uint64 long -1)
        if (writer.writeVarint64Field(FIELD_TOTAL_RECORDS, -1L) < 0) {
            return -1;
        }
        // total_bytes = -1
        return writer.writeVarint64Field(FIELD_TOTAL_BYTES, -1L);
    }

    /**
     * Returns a conservative upper bound on the encoded size for
     * {@link #encodeSingleEndpoint}. Useful for scratch sizing.
     */
    public static int upperBoundEncodedLen(int schemaLen, int ticketLen, int uriLen) {
        // schema field: tag (2) + length varint (5) + body
        int total = 2 + 5 + schemaLen;
        // endpoint field: tag (1) + length varint (5) + body
        // body = ticket sub-msg + location sub-msg
        //   ticket sub-msg: tag (1) + length varint (5) + (tag (1) + varint (5) + ticketLen)
        //   location sub-msg: tag (1) + length varint (5) + (tag (1) + varint (5) + uriLen)
        int ticketSubMsg = 1 + 5 + (1 + 5 + ticketLen);
        int locationSubMsg = 1 + 5 + (1 + 5 + uriLen);
        int endpointBody = ticketSubMsg + locationSubMsg;
        total += 1 + 5 + endpointBody;
        // total_records + total_bytes: tag (1) + 10-byte varint for -1
        total += 2 * (1 + ProtobufWireFormat.MAX_VARINT_BYTES);
        return total;
    }
}
