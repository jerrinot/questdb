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

import io.questdb.cutlass.hpack.HpackEncoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

/**
 * Emits {@code grpc-status} (mandatory) and {@code grpc-message}
 * (optional, non-OK status only) as HPACK-encoded HEADERS-block fields.
 * Used both for the terminating trailer block of a successful stream
 * and, by a dispatcher wrapping this call in the initial-HEADERS
 * writer, for the "trailers-only" error path.
 * <p>
 * Percent-encoding of {@code grpc-message} follows the gRPC spec:
 * printable ASCII (0x20..0x7E excluding {@code %}) passes through;
 * every other byte is encoded as {@code %XX} with upper-case hex digits.
 * Non-ASCII {@code CharSequence} chars are first re-encoded as UTF-8
 * and each byte percent-encoded — Wave 5 callers pass ASCII-only
 * messages so the UTF-8 path is exercised only by the codec tests.
 * <p>
 * Native ownership: the writer allocates one scratch block at
 * construction time holding the fixed name bytes for
 * {@code grpc-status} / {@code grpc-message}, a small status-digit
 * area, and a percent-encoded message scratch sized by the caller.
 * {@link #close()} frees them.
 */
public final class GrpcTrailerWriter implements Closeable {

    public static final int DEFAULT_MESSAGE_SCRATCH_BYTES = 512;
    private static final byte[] GRPC_MESSAGE_NAME_BYTES = "grpc-message".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] GRPC_STATUS_NAME_BYTES = "grpc-status".getBytes(StandardCharsets.US_ASCII);
    // Max stringified gRPC status code length. Spec status codes are
    // 0..16; we reserve 4 bytes so future codes up to 9999 still fit.
    private static final int STATUS_VALUE_MAX = 4;
    private final long grpcMessageNameAddr;
    private final long grpcStatusNameAddr;
    private final int memoryTag;
    private final long messageScratchAddr;
    private final int messageScratchCap;
    private final long rootAddr;
    private final int rootSize;
    private final long statusValueAddr;
    private boolean isClosed;

    public GrpcTrailerWriter(int messageScratchCap, int memoryTag) {
        if (messageScratchCap < 0) {
            throw new IllegalArgumentException("messageScratchCap must be non-negative: " + messageScratchCap);
        }
        this.messageScratchCap = messageScratchCap;
        this.memoryTag = memoryTag;
        this.rootSize = GRPC_STATUS_NAME_BYTES.length
                + GRPC_MESSAGE_NAME_BYTES.length
                + STATUS_VALUE_MAX
                + messageScratchCap;
        this.rootAddr = Unsafe.malloc(rootSize, memoryTag);
        long cursor = rootAddr;
        this.grpcStatusNameAddr = cursor;
        copyBytes(GRPC_STATUS_NAME_BYTES, cursor);
        cursor += GRPC_STATUS_NAME_BYTES.length;
        this.grpcMessageNameAddr = cursor;
        copyBytes(GRPC_MESSAGE_NAME_BYTES, cursor);
        cursor += GRPC_MESSAGE_NAME_BYTES.length;
        this.statusValueAddr = cursor;
        cursor += STATUS_VALUE_MAX;
        this.messageScratchAddr = cursor;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        Unsafe.free(rootAddr, rootSize, memoryTag);
        isClosed = true;
    }

    public int getMessageScratchCap() {
        return messageScratchCap;
    }

    /**
     * Writes the {@code grpc-status} field (always) followed by a
     * {@code grpc-message} field (only when {@code status != OK} and
     * {@code message} is non-null and non-empty). Returns the new
     * cursor on success or {@code -1} if the caller's scratch overflows
     * at any point.
     */
    public long writeTrailers(HpackEncoder enc, long cursor, long limit, int status, CharSequence message) {
        if (isClosed) {
            throw new IllegalStateException("trailer writer closed");
        }
        int statusLen = encodeStatusDigits(status, statusValueAddr);
        long c = enc.encode(
                cursor, limit,
                grpcStatusNameAddr, GRPC_STATUS_NAME_BYTES.length,
                statusValueAddr, statusLen,
                HpackEncoder.HINT_NONE);
        if (c < 0) {
            return -1;
        }
        if (status != GrpcStatus.OK && message != null && message.length() > 0) {
            int encodedLen = percentEncode(message, messageScratchAddr, messageScratchCap);
            if (encodedLen < 0) {
                return -1;
            }
            c = enc.encode(
                    c, limit,
                    grpcMessageNameAddr, GRPC_MESSAGE_NAME_BYTES.length,
                    messageScratchAddr, encodedLen,
                    HpackEncoder.HINT_NONE);
            if (c < 0) {
                return -1;
            }
        }
        return c;
    }

    public static int encodeStatusDigits(int status, long dstAddr) {
        if (status < 0 || status > 9999) {
            throw new IllegalArgumentException("gRPC status out of range: " + status);
        }
        if (status < 10) {
            Unsafe.getUnsafe().putByte(dstAddr, (byte) ('0' + status));
            return 1;
        }
        if (status < 100) {
            Unsafe.getUnsafe().putByte(dstAddr, (byte) ('0' + status / 10));
            Unsafe.getUnsafe().putByte(dstAddr + 1, (byte) ('0' + status % 10));
            return 2;
        }
        if (status < 1000) {
            Unsafe.getUnsafe().putByte(dstAddr, (byte) ('0' + status / 100));
            Unsafe.getUnsafe().putByte(dstAddr + 1, (byte) ('0' + (status / 10) % 10));
            Unsafe.getUnsafe().putByte(dstAddr + 2, (byte) ('0' + status % 10));
            return 3;
        }
        Unsafe.getUnsafe().putByte(dstAddr, (byte) ('0' + status / 1000));
        Unsafe.getUnsafe().putByte(dstAddr + 1, (byte) ('0' + (status / 100) % 10));
        Unsafe.getUnsafe().putByte(dstAddr + 2, (byte) ('0' + (status / 10) % 10));
        Unsafe.getUnsafe().putByte(dstAddr + 3, (byte) ('0' + status % 10));
        return 4;
    }

    /**
     * Percent-encodes {@code s} into {@code [dstAddr, dstAddr + dstCap)}.
     * Returns the number of encoded bytes on success or {@code -1} if
     * the scratch is too small.
     */
    public static int percentEncode(CharSequence s, long dstAddr, int dstCap) {
        int pos = 0;
        int len = s.length();
        for (int i = 0; i < len; i++) {
            char ch = s.charAt(i);
            if (ch >= 0x20 && ch <= 0x7E && ch != '%') {
                if (pos + 1 > dstCap) {
                    return -1;
                }
                Unsafe.getUnsafe().putByte(dstAddr + pos, (byte) ch);
                pos++;
                continue;
            }
            // Re-encode non-printable / non-ASCII chars as UTF-8 bytes,
            // then percent-encode every byte.
            if (ch < 0x80) {
                if (pos + 3 > dstCap) {
                    return -1;
                }
                writePercent(dstAddr + pos, ch);
                pos += 3;
            } else if (ch < 0x800) {
                if (pos + 6 > dstCap) {
                    return -1;
                }
                writePercent(dstAddr + pos, 0xC0 | (ch >>> 6));
                pos += 3;
                writePercent(dstAddr + pos, 0x80 | (ch & 0x3F));
                pos += 3;
            } else if (ch < 0xD800 || ch >= 0xE000) {
                if (pos + 9 > dstCap) {
                    return -1;
                }
                writePercent(dstAddr + pos, 0xE0 | (ch >>> 12));
                pos += 3;
                writePercent(dstAddr + pos, 0x80 | ((ch >>> 6) & 0x3F));
                pos += 3;
                writePercent(dstAddr + pos, 0x80 | (ch & 0x3F));
                pos += 3;
            } else {
                // Surrogate — not expected in Wave 5 messages.
                return -1;
            }
        }
        return pos;
    }

    private static void copyBytes(byte[] src, long dst) {
        for (int i = 0; i < src.length; i++) {
            Unsafe.getUnsafe().putByte(dst + i, src[i]);
        }
    }

    private static void writePercent(long dstAddr, int byteVal) {
        Unsafe.getUnsafe().putByte(dstAddr, (byte) '%');
        Unsafe.getUnsafe().putByte(dstAddr + 1, hexDigit((byteVal >>> 4) & 0xF));
        Unsafe.getUnsafe().putByte(dstAddr + 2, hexDigit(byteVal & 0xF));
    }

    private static byte hexDigit(int v) {
        return (byte) (v < 10 ? '0' + v : 'A' + (v - 10));
    }

    public static GrpcTrailerWriter newInstanceForTesting(int scratchCap) {
        return new GrpcTrailerWriter(scratchCap, MemoryTag.NATIVE_DEFAULT);
    }
}
