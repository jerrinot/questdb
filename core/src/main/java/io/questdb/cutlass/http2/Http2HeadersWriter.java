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

import io.questdb.cutlass.hpack.HpackEncoder;

/**
 * Callback invoked by
 * {@link Http2ConnectionContext#emitResponseHeaders} with the connection's
 * shared {@link HpackEncoder} already positioned inside an open block on
 * engine-owned scratch memory. The callback encodes each response header
 * field (status pseudo-header first, then regular headers) by calling
 * {@link HpackEncoder#encode} and threading the returned cursor forward
 * on each successful emission. Returning {@code -1} signals that the
 * engine scratch overflowed before the block finished — the engine
 * translates that into
 * {@link Http2ConnectionContext#ENQUEUE_HEADER_LIST_TOO_LARGE} and
 * rolls its own commit decisions back.
 * <p>
 * Rules per {@code HTTP2_INTEGRATION.md} §4.4 / §10:
 * <ul>
 *   <li>must not retain the encoder reference or the cursor values past
 *       the callback return;</li>
 *   <li>must not re-enter the engine
 *       ({@link Http2ConnectionContext#emitResponseHeaders} or
 *       {@link Http2ConnectionContext#enqueueData}) from inside the
 *       callback — the engine serialises access to the shared encoder
 *       and nested calls would corrupt its block state;</li>
 *   <li>must not call {@link HpackEncoder#beginBlock} or
 *       {@link HpackEncoder#endBlock} — the engine owns the block
 *       boundary.</li>
 * </ul>
 * <p>
 * The spec's aspirational "void write(HpackEncoder)" signature in
 * {@code HTTP2_INTEGRATION.md} §4.4 omits the buffer context; the actual
 * {@code HpackEncoder.encode} API is cursor-returning and stateless, so
 * this interface threads {@code cursor} and {@code limit} through the
 * callback to match.
 */
@FunctionalInterface
public interface Http2HeadersWriter {

    /**
     * Encodes the response header block.
     *
     * @param encoder the connection's shared HPACK encoder, inside an
     *                open block on engine-owned scratch
     * @param cursor  the current write position inside the scratch, as
     *                returned by {@link HpackEncoder#beginBlock}
     * @param limit   the end of the scratch range
     * @return the new cursor after the last emitted field, or {@code -1}
     * on scratch overflow
     */
    long write(HpackEncoder encoder, long cursor, long limit);
}
