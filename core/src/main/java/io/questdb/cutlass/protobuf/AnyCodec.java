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

package io.questdb.cutlass.protobuf;

/**
 * Decoder for {@code google.protobuf.Any}.
 * <pre>
 *   message Any {
 *     string type_url = 1;
 *     bytes  value    = 2;
 *   }
 * </pre>
 * Wave 6b only needs to decode (Flight SQL commands arrive wrapped in
 * {@code Any} inside {@code FlightDescriptor.cmd}). The encode path will
 * land in Wave 7+ when server-side responses start wrapping outbound
 * Flight SQL messages or when tickets get signed.
 * <p>
 * Both {@code type_url} and {@code value} slices point back into the
 * caller-owned input buffer and are only stable while the buffer stays
 * pinned — the codec performs no copies.
 */
public final class AnyCodec {

    public static final int FIELD_TYPE_URL = 1;
    public static final int FIELD_VALUE = 2;

    private AnyCodec() {
    }

    /**
     * Decodes a {@code google.protobuf.Any} at {@code [addr, limit)} into
     * {@code out}. Unknown fields are skipped. The {@code type_url} and
     * {@code value} slices reference the caller's input buffer directly.
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
                case FIELD_TYPE_URL:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED) {
                        throw ProtobufException.instance("Any.type_url wrong wire type");
                    }
                    r.readLengthDelimited();
                    out.typeUrlAddr = r.lastValueAddr();
                    out.typeUrlLen = r.lastValueLen();
                    break;
                case FIELD_VALUE:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED) {
                        throw ProtobufException.instance("Any.value wrong wire type");
                    }
                    r.readLengthDelimited();
                    out.valueAddr = r.lastValueAddr();
                    out.valueLen = r.lastValueLen();
                    break;
                default:
                    r.skipField(wireType);
                    break;
            }
        }
    }

    /**
     * Reusable holder for decoded {@code google.protobuf.Any} fields.
     */
    public static final class Fields {
        final ProtobufReader reader = new ProtobufReader();
        public long typeUrlAddr;
        public int typeUrlLen;
        public long valueAddr;
        public int valueLen;

        public void clear() {
            typeUrlAddr = 0;
            typeUrlLen = 0;
            valueAddr = 0;
            valueLen = 0;
        }
    }
}
