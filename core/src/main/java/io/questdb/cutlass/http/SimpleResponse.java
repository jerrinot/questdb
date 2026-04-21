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

package io.questdb.cutlass.http;

import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Minimal response surface used by processors that emit single-shot status
 * replies (reject, ping, table-status check, settings, static 3xx/4xx).
 * Extracted from {@link HttpResponseSink.SimpleResponseImpl} so both the
 * HTTP/1.x response sink and the forthcoming HTTP/2 per-stream response
 * sink can satisfy it.
 */
public interface SimpleResponse {

    void sendStatusJsonContent(int code) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusJsonContent(int code, @NotNull Utf8Sequence message) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusJsonContent(int code, @NotNull Utf8Sequence message, boolean appendEOL) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusNoContent(int code) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusNoContent(int code, @Nullable CharSequence header) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusNoContent(int code, @NotNull Utf8Sequence header) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusTextContent(int code) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusTextContent(int code, CharSequence header) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusTextContent(int code, Utf8Sequence message, CharSequence header) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusTextContent(
            int code,
            @Nullable Utf8Sequence message,
            @Nullable CharSequence header,
            @Nullable ObjList<CharSequence> cookieNames,
            @Nullable ObjList<CharSequence> cookieValues
    ) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void sendStatusWithCookie(
            int code,
            Utf8Sequence message,
            @Nullable ObjList<CharSequence> cookieNames,
            @Nullable ObjList<CharSequence> cookieValues
    ) throws PeerDisconnectedException, PeerIsSlowToReadException;

    void shutdownWrite();
}
