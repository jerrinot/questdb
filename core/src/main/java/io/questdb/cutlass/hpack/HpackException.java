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

package io.questdb.cutlass.hpack;

/**
 * Thrown for any RFC 7541 structural failure during HPACK encode / decode.
 * The caller (the HTTP/2 request parser) converts this into a connection-level
 * {@code COMPRESSION_ERROR} (RFC 9113 sec. 4.3) which the frame layer
 * translates into a {@code GOAWAY} and connection close.
 * <p>
 * Backed by a thread-local instance so decode / encode hot paths allocate
 * nothing on the error exit; the debug string is ASCII-only and may be empty.
 */
public class HpackException extends RuntimeException {

    private static final ThreadLocal<HpackException> TL_INSTANCE = ThreadLocal.withInitial(HpackException::new);

    private CharSequence debug;

    private HpackException() {
        super(null, null, true, false);
    }

    public static HpackException instance(CharSequence debug) {
        HpackException e = TL_INSTANCE.get();
        e.debug = debug;
        return e;
    }

    public CharSequence getDebug() {
        return debug;
    }

    @Override
    public String getMessage() {
        CharSequence d = debug;
        if (d == null || d.length() == 0) {
            return "HPACK error";
        }
        return "HPACK error: " + d;
    }
}
