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
 * Read-only view over the pseudo-header slots captured during an HTTP/2
 * initial HEADERS block. Passed to
 * {@link Http2StreamListener#onRequestHeaders} so the request-dispatch
 * layer can read {@code :method}, {@code :scheme}, {@code :path},
 * {@code :authority}, and {@code content-type} without taking a reference
 * to the mutable {@link Http2Stream}. {@code content-type} sits alongside
 * the four pseudo-headers because Flight SQL routers need it to
 * distinguish gRPC traffic from other H2 clients; no other regular header
 * is captured at this layer.
 * <p>
 * All {@code (addr, len)} pairs point into the owning stream's per-stream
 * header-staging buffer, which is stable for the duration of the
 * enclosing {@code onRequestHeaders} call but may be rewritten by a
 * subsequent request on the same stream. The handler must copy any bytes
 * it wants to retain. A {@code *Len} of {@code 0} with {@code *Addr} of
 * {@code 0} indicates the slot was absent in the request — this layer
 * does not defensively return an empty sentinel, because the
 * pseudo-header validation path in §11 has already rejected malformed
 * requests before the callback fires.
 */
public interface Http2RequestHeadersView {

    long getAuthorityAddr();

    int getAuthorityLen();

    long getContentTypeAddr();

    int getContentTypeLen();

    long getMethodAddr();

    int getMethodLen();

    long getPathAddr();

    int getPathLen();

    long getSchemeAddr();

    int getSchemeLen();
}
