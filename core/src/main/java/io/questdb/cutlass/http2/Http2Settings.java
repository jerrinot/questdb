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
 * HTTP/2 SETTINGS identifiers, defaults, and validation (RFC 7540 sec. 6.5.2).
 * <p>
 * SETTINGS values travel on the wire as (u16 id, u32 value) pairs. An identifier
 * outside the registered range MUST be ignored (sec. 6.5.2). We still validate
 * the value range for registered identifiers so a peer that sends a bogus value
 * triggers a connection error instead of silently corrupting state.
 */
public final class Http2Settings {

    public static final int COUNT = 6;
    public static final int DEFAULT_ENABLE_PUSH = 0;
    public static final int DEFAULT_HEADER_TABLE_SIZE = 4096;
    public static final int DEFAULT_INITIAL_WINDOW_SIZE = 65535;
    public static final int DEFAULT_MAX_CONCURRENT_STREAMS = Integer.MAX_VALUE;
    public static final int DEFAULT_MAX_FRAME_SIZE = 16384;
    public static final int DEFAULT_MAX_HEADER_LIST_SIZE = Integer.MAX_VALUE;
    public static final short ENABLE_PUSH = 0x2;
    public static final short HEADER_TABLE_SIZE = 0x1;
    public static final short INITIAL_WINDOW_SIZE = 0x4;
    /**
     * Maximum value for {@code SETTINGS_INITIAL_WINDOW_SIZE} per RFC 7540 sec. 6.5.2.
     * Larger values trigger {@code FLOW_CONTROL_ERROR}.
     */
    public static final int INITIAL_WINDOW_SIZE_MAX = 0x7FFFFFFF;
    public static final short MAX_CONCURRENT_STREAMS = 0x3;
    public static final short MAX_FRAME_SIZE = 0x5;
    /**
     * Lower bound on {@code SETTINGS_MAX_FRAME_SIZE} per RFC 7540 sec. 4.2.
     */
    public static final int MAX_FRAME_SIZE_LOWER = 16384;
    /**
     * Upper bound on {@code SETTINGS_MAX_FRAME_SIZE} per RFC 7540 sec. 4.2 (2^24 - 1).
     */
    public static final int MAX_FRAME_SIZE_UPPER = 0x00FFFFFF;
    public static final short MAX_HEADER_LIST_SIZE = 0x6;

    private Http2Settings() {
    }

    /**
     * Returns {@link Http2ErrorCode#NO_ERROR} if the value is legal for the given
     * setting identifier, or the appropriate error code otherwise.
     * <p>
     * Values arrive on the wire as unsigned 32-bit; callers must pass the full
     * unsigned value as a {@code long} (e.g. via {@code raw & 0xFFFFFFFFL}) so
     * the top-bit-set range receives the correct numeric comparison instead of
     * wrapping through negative int space.
     * <p>
     * Unknown identifiers must be silently ignored per RFC 7540 sec. 6.5.2; this
     * method returns {@code NO_ERROR} for them so the caller can store or skip
     * as it chooses.
     */
    public static int validate(short id, long value) {
        switch (id) {
            case HEADER_TABLE_SIZE:
            case MAX_CONCURRENT_STREAMS:
            case MAX_HEADER_LIST_SIZE:
                // RFC 7540 sec. 6.5.2 defines these as unsigned 32-bit with no upper
                // bound; a peer is free to advertise any value in [0, 2^32 - 1]. We
                // accept them and leave clamping to downstream (where a value above
                // Integer.MAX_VALUE is impossible to actually honour).
                return (value >= 0 && value <= 0xFFFFFFFFL) ? Http2ErrorCode.NO_ERROR : Http2ErrorCode.PROTOCOL_ERROR;
            case ENABLE_PUSH:
                return (value == 0 || value == 1) ? Http2ErrorCode.NO_ERROR : Http2ErrorCode.PROTOCOL_ERROR;
            case INITIAL_WINDOW_SIZE:
                return (value >= 0 && value <= INITIAL_WINDOW_SIZE_MAX)
                        ? Http2ErrorCode.NO_ERROR
                        : Http2ErrorCode.FLOW_CONTROL_ERROR;
            case MAX_FRAME_SIZE:
                return (value >= MAX_FRAME_SIZE_LOWER && value <= MAX_FRAME_SIZE_UPPER)
                        ? Http2ErrorCode.NO_ERROR
                        : Http2ErrorCode.PROTOCOL_ERROR;
            default:
                return Http2ErrorCode.NO_ERROR;
        }
    }
}
