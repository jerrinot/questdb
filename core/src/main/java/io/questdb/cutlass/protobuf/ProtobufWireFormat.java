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
 * Protobuf wire-format constants and small helpers shared by
 * {@link ProtobufWriter} and {@link ProtobufReader}.
 * <p>
 * Wave 5 supports only the subset needed by Flight SQL's
 * {@code Handshake} RPC: varint ({@link #WIRE_TYPE_VARINT}) and
 * length-delimited ({@link #WIRE_TYPE_LENGTH_DELIMITED}) wire types.
 * Fixed32 / fixed64 are recognised for skip-unknown purposes only;
 * {@code START_GROUP} / {@code END_GROUP} are not supported (groups are
 * deprecated and Flight / Flight SQL never use them).
 */
public final class ProtobufWireFormat {

    public static final int MAX_VARINT_BYTES = 10;
    public static final int WIRE_TYPE_FIXED32 = 5;
    public static final int WIRE_TYPE_FIXED64 = 1;
    public static final int WIRE_TYPE_LENGTH_DELIMITED = 2;
    public static final int WIRE_TYPE_MASK = 7;
    public static final int WIRE_TYPE_VARINT = 0;

    private ProtobufWireFormat() {
    }

    public static int fieldNumberOf(int tag) {
        return tag >>> 3;
    }

    public static long makeTag(int fieldNumber, int wireType) {
        if (fieldNumber <= 0) {
            throw new IllegalArgumentException("fieldNumber must be positive: " + fieldNumber);
        }
        if ((wireType & ~WIRE_TYPE_MASK) != 0) {
            throw new IllegalArgumentException("wireType out of range: " + wireType);
        }
        return ((long) fieldNumber << 3) | wireType;
    }

    /**
     * Byte length of the varint encoding of {@code value}, interpreting
     * {@code value} as an unsigned 64-bit integer (protobuf's uint64).
     * Negative {@code value}s encode as 10 bytes.
     */
    public static int varintSize(long value) {
        if (value < 0) {
            return 10;
        }
        int size = 1;
        while ((value & ~0x7FL) != 0L) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    public static int wireTypeOf(int tag) {
        return tag & WIRE_TYPE_MASK;
    }
}
