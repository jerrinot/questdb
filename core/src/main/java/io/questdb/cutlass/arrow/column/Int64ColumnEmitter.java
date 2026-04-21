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

package io.questdb.cutlass.arrow.column;

import io.questdb.std.Unsafe;

/**
 * Emits a dense, little-endian {@code int64} values buffer matching the
 * Arrow {@code Int(64, signed)} layout. QuestDB LONG columns already
 * store their backing bytes as little-endian int64 on disk, so a real
 * page-frame fast path will eventually memcpy directly — for Wave 6a
 * the source is a caller-owned {@code long[]} so we emit byte-by-byte.
 * <p>
 * No nulls; no validity bitmap byte flips. Wave 7 extends this with a
 * validity-bitmap pass over the source sentinel values.
 */
public final class Int64ColumnEmitter implements ColumnEmitter {

    public static final int BYTES_PER_ROW = 8;
    public static final Int64ColumnEmitter INSTANCE = new Int64ColumnEmitter();

    @Override
    public long emit(long dstAddr, long[] src, int offset, int count) {
        if (src == null) {
            throw new IllegalArgumentException("src must be non-null");
        }
        if (offset < 0 || count < 0 || offset + count > src.length) {
            throw new IllegalArgumentException("offset/count out of range");
        }
        long cursor = dstAddr;
        for (int i = 0; i < count; i++) {
            long v = src[offset + i];
            Unsafe.getUnsafe().putByte(cursor, (byte) (v & 0xFF));
            Unsafe.getUnsafe().putByte(cursor + 1, (byte) ((v >>> 8) & 0xFF));
            Unsafe.getUnsafe().putByte(cursor + 2, (byte) ((v >>> 16) & 0xFF));
            Unsafe.getUnsafe().putByte(cursor + 3, (byte) ((v >>> 24) & 0xFF));
            Unsafe.getUnsafe().putByte(cursor + 4, (byte) ((v >>> 32) & 0xFF));
            Unsafe.getUnsafe().putByte(cursor + 5, (byte) ((v >>> 40) & 0xFF));
            Unsafe.getUnsafe().putByte(cursor + 6, (byte) ((v >>> 48) & 0xFF));
            Unsafe.getUnsafe().putByte(cursor + 7, (byte) ((v >>> 56) & 0xFF));
            cursor += BYTES_PER_ROW;
        }
        return cursor;
    }
}
