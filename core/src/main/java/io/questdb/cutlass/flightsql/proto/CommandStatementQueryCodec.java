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

/**
 * Decoder for Flight SQL's {@code CommandStatementQuery} message.
 * <pre>
 *   message CommandStatementQuery {
 *     string query          = 1;
 *     bytes  transaction_id = 2;
 *   }
 * </pre>
 * Wave 6b reads only the {@code query} field; transaction handling
 * lands in a later wave (Wave 6b is single-statement autocommit).
 * <p>
 * The {@code query} slice points into the caller-owned input buffer
 * and is only stable while that buffer stays pinned.
 */
public final class CommandStatementQueryCodec {

    public static final int FIELD_QUERY = 1;
    public static final int FIELD_TRANSACTION_ID = 2;
    public static final String TYPE_URL = "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery";

    private CommandStatementQueryCodec() {
    }

    /**
     * Decodes a {@code CommandStatementQuery} at {@code [addr, limit)}
     * into {@code out}. Unknown fields (including {@code transaction_id}
     * for Wave 6b) are skipped.
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
                case FIELD_QUERY:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED) {
                        throw ProtobufException.instance("CommandStatementQuery.query wrong wire type");
                    }
                    r.readLengthDelimited();
                    out.queryAddr = r.lastValueAddr();
                    out.queryLen = r.lastValueLen();
                    break;
                default:
                    r.skipField(wireType);
                    break;
            }
        }
    }

    /**
     * Reusable holder for decoded {@code CommandStatementQuery} fields.
     */
    public static final class Fields {
        final ProtobufReader reader = new ProtobufReader();
        public long queryAddr;
        public int queryLen;

        public void clear() {
            queryAddr = 0;
            queryLen = 0;
        }
    }
}
