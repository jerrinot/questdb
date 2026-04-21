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

import io.questdb.std.Unsafe;

/**
 * HTTP/2 connection preface (RFC 7540 sec. 3.5).
 * <p>
 * A client initiating HTTP/2 over clear text (h2c with prior knowledge) sends
 * exactly 24 bytes as its first transmission:
 * <pre>
 *   "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
 * </pre>
 * HTTP/1.1 request lines never begin with {@code "PRI "}, so the preface is
 * unambiguous with the HTTP/1.1 request grammar. The HTTP/1.1 listener can
 * sniff for it before handing the connection to the HTTP/2 context.
 */
public final class Http2Preface {

    /**
     * Buffer could still be the preface but fewer than {@link #LENGTH} bytes
     * have arrived. Caller should read more before deciding.
     */
    public static final int INCOMPLETE = 0;

    /**
     * Preface length in bytes.
     */
    public static final int LENGTH = 24;

    /**
     * Buffer matches the preface in full.
     */
    public static final int MATCH = 1;

    /**
     * Buffer definitely does not match the preface.
     */
    public static final int NO_MATCH = -1;

    private static final byte[] BYTES = {
            'P', 'R', 'I', ' ', '*', ' ', 'H', 'T', 'T', 'P', '/', '2', '.', '0',
            '\r', '\n', '\r', '\n',
            'S', 'M',
            '\r', '\n', '\r', '\n'
    };

    private Http2Preface() {
    }

    /**
     * Verifies that the range {@code [addr + fromInclusive, addr + toExclusive)}
     * matches the corresponding slice of the HTTP/2 preface. Used by the
     * preface drain in the connection layer, where the fixed 24-byte preface
     * arrives piecewise across READ ticks and each fresh slice must match
     * its position in the preface exactly.
     *
     * @param addr          buffer start (preface origin; callers pass the
     *                      fixed scratch base so offsets line up with the
     *                      preface)
     * @param fromInclusive offset of the first byte to check, inclusive
     * @param toExclusive   offset one past the last byte to check; must be
     *                      &lt;= {@link #LENGTH}
     * @return {@code true} if every byte in the range matches the preface
     */
    public static boolean matches(long addr, int fromInclusive, int toExclusive) {
        assert fromInclusive >= 0 && toExclusive <= LENGTH && fromInclusive <= toExclusive;
        for (int i = fromInclusive; i < toExclusive; i++) {
            if (Unsafe.getUnsafe().getByte(addr + i) != BYTES[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Classifies a native buffer against the 24-byte preface.
     *
     * @param addr buffer start
     * @param len  number of bytes available at {@code addr}
     * @return one of {@link #MATCH}, {@link #NO_MATCH}, {@link #INCOMPLETE}
     */
    public static int detect(long addr, int len) {
        int check = Math.min(len, LENGTH);
        for (int i = 0; i < check; i++) {
            if (Unsafe.getUnsafe().getByte(addr + i) != BYTES[i]) {
                return NO_MATCH;
            }
        }
        if (len >= LENGTH) {
            return MATCH;
        }
        return INCOMPLETE;
    }

    /**
     * Writes the 24-byte preface at {@code addr}. Used by the (future) HTTP/2
     * client role.
     *
     * @return {@code addr + LENGTH}
     */
    public static long write(long addr) {
        for (int i = 0; i < LENGTH; i++) {
            Unsafe.getUnsafe().putByte(addr + i, BYTES[i]);
        }
        return addr + LENGTH;
    }
}
