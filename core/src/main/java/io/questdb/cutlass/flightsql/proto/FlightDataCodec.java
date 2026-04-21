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
 * Encoder for the Flight {@code FlightData} message.
 * <pre>
 *   message FlightData {
 *     FlightDescriptor flight_descriptor = 1;     // skipped
 *     bytes            data_header       = 2;
 *     bytes            app_metadata      = 3;     // skipped
 *     bytes            data_body         = 1000;  // high field number
 *   }
 * </pre>
 * The high field number on {@code data_body} is deliberate in
 * {@code Flight.proto}: it keeps the varint tag for the large body at
 * three bytes while low-numbered fields stay at one byte each, so the
 * per-message overhead is constant regardless of body size.
 */
public final class FlightDataCodec {

    public static final int FIELD_DATA_BODY = 1000;
    public static final int FIELD_DATA_HEADER = 2;

    private FlightDataCodec() {
    }

    /**
     * Writes a {@code FlightData} body into {@code writer}. Either the
     * header, the body, or both may be empty: fields whose length is
     * zero are still emitted as empty length-delimited values (Flight
     * clients accept either representation; emitting is more predictable
     * for tests). Returns the writer cursor on success, {@code -1} on
     * overflow.
     */
    public static long encode(ProtobufWriter writer,
                              long dataHeaderAddr, int dataHeaderLen,
                              long dataBodyAddr, int dataBodyLen) {
        if (dataHeaderLen > 0) {
            long c = writer.writeLengthDelimitedField(FIELD_DATA_HEADER, dataHeaderAddr, dataHeaderLen);
            if (c < 0) {
                return -1;
            }
        }
        if (dataBodyLen > 0) {
            long c = writer.writeLengthDelimitedField(FIELD_DATA_BODY, dataBodyAddr, dataBodyLen);
            if (c < 0) {
                return -1;
            }
        }
        return writer.cursor();
    }

    /**
     * Returns a conservative upper bound on the encoded size for
     * {@link #encode}. Useful for scratch sizing.
     */
    public static int upperBoundEncodedLen(int dataHeaderLen, int dataBodyLen) {
        // Tag for field 2: 1 byte. Tag for field 1000: 3 bytes. Length
        // varints bounded at 5 bytes each.
        return 1 + 5 + dataHeaderLen
                + 3 + 5 + dataBodyLen
                + ProtobufWireFormat.MAX_VARINT_BYTES;
    }
}
