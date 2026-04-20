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

package io.questdb.cutlass.http2;

/**
 * HTTP/2 frame flag bits (RFC 7540 sec. 6). Flag semantics vary per frame type;
 * a bit that is defined on one frame is "reserved, send 0" on another.
 */
public final class Http2Flags {
    public static final byte ACK = 0x01;
    public static final byte END_HEADERS = 0x04;
    public static final byte END_STREAM = 0x01;
    public static final byte NONE = 0x00;
    public static final byte PADDED = 0x08;
    public static final byte PRIORITY = 0x20;

    private Http2Flags() {
    }

    public static boolean hasAck(byte flags) {
        return (flags & ACK) != 0;
    }

    public static boolean hasEndHeaders(byte flags) {
        return (flags & END_HEADERS) != 0;
    }

    public static boolean hasEndStream(byte flags) {
        return (flags & END_STREAM) != 0;
    }

    public static boolean hasPadded(byte flags) {
        return (flags & PADDED) != 0;
    }

    public static boolean hasPriority(byte flags) {
        return (flags & PRIORITY) != 0;
    }
}
