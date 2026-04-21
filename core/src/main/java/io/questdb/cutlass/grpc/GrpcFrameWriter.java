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

package io.questdb.cutlass.grpc;

import io.questdb.std.Unsafe;

/**
 * Stateless helpers for writing the gRPC message prefix: 1-byte
 * compressed flag followed by a 4-byte big-endian message length. Wave 5
 * never emits compressed messages, so the prefix is always
 * {@code [0x00, b3, b2, b1, b0]}.
 */
public final class GrpcFrameWriter {

    /**
     * Length of the gRPC 5-byte message prefix.
     */
    public static final int PREFIX_LEN = 5;

    private GrpcFrameWriter() {
    }

    /**
     * Writes the 5-byte gRPC message prefix at {@code dstAddr}. Returns
     * the new cursor ({@code dstAddr + 5}). Caller is responsible for
     * ensuring {@code [dstAddr, dstAddr + 5)} is within a writable
     * buffer.
     *
     * @param dstAddr    write position
     * @param messageLen body length of the gRPC message (not including
     *                   the prefix itself)
     */
    public static long writePrefix(long dstAddr, int messageLen) {
        return writePrefix(dstAddr, messageLen, false);
    }

    public static long writePrefix(long dstAddr, int messageLen, boolean compressed) {
        if (messageLen < 0) {
            throw new IllegalArgumentException("messageLen must be non-negative: " + messageLen);
        }
        Unsafe.getUnsafe().putByte(dstAddr, compressed ? (byte) 1 : (byte) 0);
        // Big-endian length, explicit byte writes — same pattern as
        // Http2FrameWriter uses for the 24-bit length in the H2 frame
        // header; endianness-clean on every platform.
        Unsafe.getUnsafe().putByte(dstAddr + 1, (byte) ((messageLen >>> 24) & 0xFF));
        Unsafe.getUnsafe().putByte(dstAddr + 2, (byte) ((messageLen >>> 16) & 0xFF));
        Unsafe.getUnsafe().putByte(dstAddr + 3, (byte) ((messageLen >>> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(dstAddr + 4, (byte) (messageLen & 0xFF));
        return dstAddr + PREFIX_LEN;
    }
}
