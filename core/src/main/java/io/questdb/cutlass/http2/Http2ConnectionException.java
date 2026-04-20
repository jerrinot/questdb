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
 * Terminal connection-level error. Handler emits a {@code GOAWAY} frame with
 * the error code and closes the connection (RFC 7540 sec. 5.4.1).
 * <p>
 * The debug string is ASCII-only and is intended for the {@code GOAWAY}
 * debug-data payload. It may be empty.
 */
public class Http2ConnectionException extends RuntimeException {

    private static final ThreadLocal<Http2ConnectionException> TL_INSTANCE = ThreadLocal.withInitial(Http2ConnectionException::new);

    private CharSequence debug;
    private int errorCode;

    private Http2ConnectionException() {
        super(null, null, true, false);
    }

    public static Http2ConnectionException instance(int errorCode, CharSequence debug) {
        Http2ConnectionException e = TL_INSTANCE.get();
        e.errorCode = errorCode;
        e.debug = debug;
        return e;
    }

    public static Http2ConnectionException instance(int errorCode) {
        return instance(errorCode, "");
    }

    public CharSequence getDebug() {
        return debug;
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        CharSequence d = debug;
        String name = Http2ErrorCode.nameOf(errorCode);
        if (d == null || d.length() == 0) {
            return name;
        }
        return name + ": " + d;
    }
}
