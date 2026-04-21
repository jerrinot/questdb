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

import io.questdb.cutlass.protobuf.ProtobufWriter;

/**
 * Encoder for the Flight {@code FlightEndpoint} message.
 * <pre>
 *   message FlightEndpoint {
 *     Ticket   ticket   = 1;
 *     repeated Location location = 2;
 *     // expiration_time = 3 and app_metadata = 4 skipped — Wave 6a
 *     // does not advertise either.
 *   }
 * </pre>
 * Wave 6a always writes a single location pointing at
 * {@code arrow-flight-reuse-connection://?} so the client reuses the
 * existing gRPC channel for {@code DoGet}.
 */
public final class FlightEndpointCodec {

    public static final int FIELD_LOCATION = 2;
    public static final int FIELD_TICKET = 1;

    private FlightEndpointCodec() {
    }

    /**
     * Writes one {@code FlightEndpoint} body into {@code writer} with the
     * given ticket bytes and a single {@code Location} URI. Returns the
     * writer cursor on success, {@code -1} on overflow.
     */
    public static long encode(ProtobufWriter writer, long ticketAddr, int ticketLen,
                              long uriAddr, int uriLen) {
        long ticketBodyStart = writer.beginNestedMessage(FIELD_TICKET);
        if (ticketBodyStart < 0) {
            return -1;
        }
        if (TicketCodec.encode(writer, ticketAddr, ticketLen) < 0) {
            return -1;
        }
        if (writer.endNestedMessage(ticketBodyStart) < 0) {
            return -1;
        }
        long locationBodyStart = writer.beginNestedMessage(FIELD_LOCATION);
        if (locationBodyStart < 0) {
            return -1;
        }
        if (LocationCodec.encode(writer, uriAddr, uriLen) < 0) {
            return -1;
        }
        return writer.endNestedMessage(locationBodyStart);
    }
}
