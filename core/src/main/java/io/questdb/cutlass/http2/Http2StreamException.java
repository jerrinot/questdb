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
 * Per-stream error. Handler emits {@code RST_STREAM} on the affected stream id
 * and keeps the connection open (RFC 7540 sec. 5.4.2).
 */
public class Http2StreamException extends RuntimeException {

    private static final ThreadLocal<Http2StreamException> TL_INSTANCE = ThreadLocal.withInitial(Http2StreamException::new);

    private int errorCode;
    private int streamId;

    private Http2StreamException() {
        super(null, null, true, false);
    }

    public static Http2StreamException instance(int streamId, int errorCode) {
        Http2StreamException e = TL_INSTANCE.get();
        e.streamId = streamId;
        e.errorCode = errorCode;
        return e;
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return "stream=" + streamId + " " + Http2ErrorCode.nameOf(errorCode);
    }

    public int getStreamId() {
        return streamId;
    }
}
