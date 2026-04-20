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

import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

/**
 * HPACK prefix-integer encode / decode (RFC 7541 sec. 5.1).
 * <p>
 * An HPACK integer occupies a variable-length tail after a prefix that fills
 * the low {@code N} bits of the first byte, where {@code N ∈ {4, 5, 6, 7}}
 * is determined by the representation kind. If the prefix value is
 * {@code < (1 << N) - 1}, the prefix alone is the integer. Otherwise the
 * prefix stores {@code (1 << N) - 1} and the remaining value is written as a
 * sequence of continuation bytes: low 7 bits of each byte are base-128
 * digits, the high bit is {@code 1} on all but the last byte.
 * <p>
 * Decoded integers are capped at {@link Integer#MAX_VALUE}; a continuation
 * sequence that would overflow throws {@link HpackException}. The encoder is
 * caller-driven and never sees values larger than header-field lengths or
 * table sizes, both of which fit comfortably in {@code int}.
 */
public final class HpackIntCodec {

    /**
     * Maximum encoded length of an HPACK prefix integer whose value fits in
     * {@code int}. One prefix byte plus up to five continuation bytes (the
     * fifth covers the remaining bits of {@link Integer#MAX_VALUE}).
     */
    public static final int MAX_ENCODED_LENGTH = 6;

    private HpackIntCodec() {
    }

    /**
     * Decodes an HPACK prefix integer starting at {@code addr}. The first
     * byte's low {@code N} bits (where {@code prefixMask == (1 << N) - 1})
     * carry the initial prefix; continuation bytes follow when the prefix is
     * saturated.
     *
     * @param addr       first byte of the integer
     * @param limit      one past the last readable byte of the field block
     * @param prefixMask mask covering the prefix bits ({@code 0x0F}, {@code 0x1F},
     *                   {@code 0x3F}, or {@code 0x7F})
     * @return packed {@code (value, bytesRead)} with value in the low 32 bits
     * and bytesRead in the high 32 bits; use {@link Numbers#decodeLowInt(long)}
     * and {@link Numbers#decodeHighInt(long)} to unpack
     * @throws HpackException if the buffer is truncated mid-integer or the
     *                        value would exceed {@link Integer#MAX_VALUE}
     */
    public static long decode(long addr, long limit, int prefixMask) {
        if (addr >= limit) {
            throw HpackException.instance("truncated integer: empty input");
        }
        int value = Unsafe.getUnsafe().getByte(addr) & prefixMask;
        long cursor = addr + 1;
        if (value < prefixMask) {
            return Numbers.encodeLowHighInts(value, (int) (cursor - addr));
        }
        // Saturated prefix: read continuation bytes (RFC 7541 sec. 5.1).
        int shift = 0;
        while (true) {
            if (cursor >= limit) {
                throw HpackException.instance("truncated integer: missing continuation byte");
            }
            int b = Unsafe.getUnsafe().getByte(cursor) & 0xFF;
            cursor++;
            // Shift cannot exceed 28 before overflow: five 7-bit groups cover
            // bits 0..34, and the combined value must fit in positive int.
            if (shift > 28) {
                throw HpackException.instance("integer overflow");
            }
            long add = ((long) (b & 0x7F)) << shift;
            long next = Integer.toUnsignedLong(value) + add;
            if (next > Integer.MAX_VALUE) {
                throw HpackException.instance("integer overflow");
            }
            value = (int) next;
            if ((b & 0x80) == 0) {
                return Numbers.encodeLowHighInts(value, (int) (cursor - addr));
            }
            shift += 7;
        }
    }

    /**
     * Encodes an HPACK prefix integer starting at {@code addr}. The first
     * byte is {@code firstByteFlags | min(value, prefixMask)}; continuation
     * bytes follow when {@code value >= prefixMask}.
     *
     * @param addr            write position
     * @param limit           one past the last writable byte
     * @param prefixMask      mask covering the prefix bits ({@code 0x0F},
     *                        {@code 0x1F}, {@code 0x3F}, or {@code 0x7F})
     * @param firstByteFlags  representation-kind pattern bits occupying the
     *                        high bits of the first byte (complement of
     *                        {@code prefixMask}). Must not overlap with
     *                        {@code prefixMask}; the caller supplies these.
     * @param value           non-negative integer to encode
     * @return the new write pointer (one past the last byte written), or
     * {@code -1} if the buffer cannot hold the encoded integer. Preflight
     * atomicity: no bytes are written on the {@code -1} path.
     */
    public static long encode(long addr, long limit, int prefixMask, int firstByteFlags, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative: " + value);
        }
        int needed = encodedLengthForMask(prefixMask, value);
        if (limit - addr < needed) {
            return -1;
        }
        if (value < prefixMask) {
            Unsafe.getUnsafe().putByte(addr, (byte) (firstByteFlags | value));
            return addr + 1;
        }
        Unsafe.getUnsafe().putByte(addr, (byte) (firstByteFlags | prefixMask));
        long cursor = addr + 1;
        int remaining = value - prefixMask;
        while ((remaining & ~0x7F) != 0) {
            Unsafe.getUnsafe().putByte(cursor, (byte) ((remaining & 0x7F) | 0x80));
            cursor++;
            remaining >>>= 7;
        }
        Unsafe.getUnsafe().putByte(cursor, (byte) remaining);
        return cursor + 1;
    }

    /**
     * Returns the exact number of bytes {@link #encode} would write for the
     * given prefix width and value, without touching any buffer.
     *
     * @param prefixBits prefix width in bits ({@code 4}, {@code 5}, {@code 6},
     *                   or {@code 7})
     * @param value      non-negative integer to encode
     */
    public static int encodedLength(int prefixBits, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative: " + value);
        }
        return encodedLengthForMask((1 << prefixBits) - 1, value);
    }

    private static int encodedLengthForMask(int prefixMask, int value) {
        if (value < prefixMask) {
            return 1;
        }
        int len = 2; // prefix byte + at least one continuation
        int remaining = value - prefixMask;
        while ((remaining & ~0x7F) != 0) {
            remaining >>>= 7;
            len++;
        }
        return len;
    }
}
