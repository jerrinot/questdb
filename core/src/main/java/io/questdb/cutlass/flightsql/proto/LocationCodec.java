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
 * Encoder for the Flight {@code Location} message.
 * <pre>
 *   message Location {
 *     string uri = 1;
 *   }
 * </pre>
 * Strings encode byte-identically to {@code bytes}: a length-delimited
 * UTF-8 payload.
 */
public final class LocationCodec {

    public static final int FIELD_URI = 1;

    private LocationCodec() {
    }

    /**
     * Encodes a {@code Location} body into {@code writer}. Returns the
     * writer cursor on success, {@code -1} on overflow.
     */
    public static long encode(ProtobufWriter writer, long uriAddr, int uriLen) {
        return writer.writeLengthDelimitedField(FIELD_URI, uriAddr, uriLen);
    }
}
