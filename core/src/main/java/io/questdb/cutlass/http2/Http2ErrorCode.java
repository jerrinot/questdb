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
 * HTTP/2 error codes (RFC 7540 sec. 7). Values are on-wire 32-bit unsigned
 * integers; we keep them as {@code int} since all registered codes fit in a
 * signed 32-bit range.
 */
public final class Http2ErrorCode {
    public static final int CANCEL = 0x8;
    public static final int COMPRESSION_ERROR = 0x9;
    public static final int CONNECT_ERROR = 0xA;
    public static final int ENHANCE_YOUR_CALM = 0xB;
    public static final int FLOW_CONTROL_ERROR = 0x3;
    public static final int FRAME_SIZE_ERROR = 0x6;
    public static final int HTTP_1_1_REQUIRED = 0xD;
    public static final int INADEQUATE_SECURITY = 0xC;
    public static final int INTERNAL_ERROR = 0x2;
    public static final int NO_ERROR = 0x0;
    public static final int PROTOCOL_ERROR = 0x1;
    public static final int REFUSED_STREAM = 0x7;
    public static final int SETTINGS_TIMEOUT = 0x4;
    public static final int STREAM_CLOSED = 0x5;

    private Http2ErrorCode() {
    }

    public static String nameOf(int code) {
        switch (code) {
            case NO_ERROR:
                return "NO_ERROR";
            case PROTOCOL_ERROR:
                return "PROTOCOL_ERROR";
            case INTERNAL_ERROR:
                return "INTERNAL_ERROR";
            case FLOW_CONTROL_ERROR:
                return "FLOW_CONTROL_ERROR";
            case SETTINGS_TIMEOUT:
                return "SETTINGS_TIMEOUT";
            case STREAM_CLOSED:
                return "STREAM_CLOSED";
            case FRAME_SIZE_ERROR:
                return "FRAME_SIZE_ERROR";
            case REFUSED_STREAM:
                return "REFUSED_STREAM";
            case CANCEL:
                return "CANCEL";
            case COMPRESSION_ERROR:
                return "COMPRESSION_ERROR";
            case CONNECT_ERROR:
                return "CONNECT_ERROR";
            case ENHANCE_YOUR_CALM:
                return "ENHANCE_YOUR_CALM";
            case INADEQUATE_SECURITY:
                return "INADEQUATE_SECURITY";
            case HTTP_1_1_REQUIRED:
                return "HTTP_1_1_REQUIRED";
            default:
                return "UNKNOWN";
        }
    }
}
