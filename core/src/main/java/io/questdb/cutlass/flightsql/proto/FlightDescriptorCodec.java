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
 * Decoder for the Flight {@code FlightDescriptor} message.
 * <pre>
 *   message FlightDescriptor {
 *     DescriptorType type   = 1;  // UNKNOWN=0, PATH=1, CMD=2
 *     bytes          cmd    = 2;
 *     repeated string path  = 3;
 *   }
 * </pre>
 * Wave 6a only reads the {@code type} enum and the {@code cmd} bytes slice;
 * {@code path} entries are recognised but their contents are discarded
 * (the hardcoded handlers return the same data for any descriptor).
 */
public final class FlightDescriptorCodec {

    public static final int FIELD_CMD = 2;
    public static final int FIELD_PATH = 3;
    public static final int FIELD_TYPE = 1;
    public static final int TYPE_CMD = 2;
    public static final int TYPE_PATH = 1;
    public static final int TYPE_UNKNOWN = 0;

    private FlightDescriptorCodec() {
    }

    /**
     * Decodes a {@code FlightDescriptor} at {@code [addr, limit)} into
     * {@code out}. Unknown fields are skipped. The {@code cmd} slice
     * references into the caller-owned input buffer and is stable only
     * while that buffer stays pinned.
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
                case FIELD_TYPE:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_VARINT) {
                        throw ProtobufException.instance("FlightDescriptor.type wrong wire type");
                    }
                    long typeValue = r.readVarint64();
                    if (typeValue < 0 || typeValue > Integer.MAX_VALUE) {
                        throw ProtobufException.instance("FlightDescriptor.type out of range");
                    }
                    out.type = (int) typeValue;
                    break;
                case FIELD_CMD:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED) {
                        throw ProtobufException.instance("FlightDescriptor.cmd wrong wire type");
                    }
                    r.readLengthDelimited();
                    out.cmdAddr = r.lastValueAddr();
                    out.cmdLen = r.lastValueLen();
                    break;
                case FIELD_PATH:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED) {
                        throw ProtobufException.instance("FlightDescriptor.path wrong wire type");
                    }
                    r.readLengthDelimited();
                    out.pathCount++;
                    break;
                default:
                    r.skipField(wireType);
                    break;
            }
        }
    }

    /**
     * Reusable holder for decoded {@code FlightDescriptor} fields.
     */
    public static final class Fields {
        final ProtobufReader reader = new ProtobufReader();
        public long cmdAddr;
        public int cmdLen;
        public int pathCount;
        public int type;

        public void clear() {
            type = TYPE_UNKNOWN;
            cmdAddr = 0;
            cmdLen = 0;
            pathCount = 0;
        }
    }
}
