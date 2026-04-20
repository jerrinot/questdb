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
 * HTTP/2 frame type codes (RFC 7540 sec. 6).
 */
public final class Http2FrameType {
    public static final byte CONTINUATION = 0x09;
    public static final byte DATA = 0x00;
    public static final byte GOAWAY = 0x07;
    public static final byte HEADERS = 0x01;
    public static final byte PING = 0x06;
    public static final byte PRIORITY = 0x02;
    public static final byte PUSH_PROMISE = 0x05;
    public static final byte RST_STREAM = 0x03;
    public static final byte SETTINGS = 0x04;
    public static final byte WINDOW_UPDATE = 0x08;

    private Http2FrameType() {
    }
}
