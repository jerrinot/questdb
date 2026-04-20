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
 * Mutable view of a parsed HTTP/2 frame header (RFC 7540 sec. 4.1).
 * <p>
 * Instances are long-lived and are reused across reads on the same connection;
 * {@link Http2FrameReader#tryReadNext} rewrites the fields in place. The
 * caller must consume the header (including reading the payload at
 * {@link #getPayloadAddr()}) before triggering the next read.
 * <p>
 * Wire layout:
 * <pre>
 *   Length (24 bits, unsigned, big-endian)
 *   Type   (8 bits)
 *   Flags  (8 bits)
 *   R|Stream Identifier (1 + 31 bits, big-endian; R sent 0, ignored on read)
 * </pre>
 */
public final class Http2FrameHeader {

    /**
     * Fixed wire size of the frame header.
     */
    public static final int SIZE = 9;

    private byte flags;
    private int payloadAddrHi;
    private int payloadAddrLo;
    private int payloadLength;
    private int streamId;
    private byte type;

    public byte getFlags() {
        return flags;
    }

    /**
     * Native address of the payload's first byte. Valid only until the caller
     * advances past the frame.
     */
    public long getPayloadAddr() {
        return ((long) payloadAddrHi << 32) | (payloadAddrLo & 0xFFFFFFFFL);
    }

    public int getPayloadLength() {
        return payloadLength;
    }

    public int getStreamId() {
        return streamId;
    }

    public byte getType() {
        return type;
    }

    /**
     * Parses the 9-byte header starting at {@code addr} into this instance.
     * <p>
     * Does not validate the values; that is the reader's job. Does clear the
     * reserved high bit of the stream id per RFC 7540 sec. 4.1.
     */
    public void readFrom(long addr) {
        int hi = Unsafe.getUnsafe().getByte(addr) & 0xFF;
        int mid = Unsafe.getUnsafe().getByte(addr + 1) & 0xFF;
        int lo = Unsafe.getUnsafe().getByte(addr + 2) & 0xFF;
        this.payloadLength = (hi << 16) | (mid << 8) | lo;
        this.type = Unsafe.getUnsafe().getByte(addr + 3);
        this.flags = Unsafe.getUnsafe().getByte(addr + 4);
        int sb0 = Unsafe.getUnsafe().getByte(addr + 5) & 0xFF;
        int sb1 = Unsafe.getUnsafe().getByte(addr + 6) & 0xFF;
        int sb2 = Unsafe.getUnsafe().getByte(addr + 7) & 0xFF;
        int sb3 = Unsafe.getUnsafe().getByte(addr + 8) & 0xFF;
        this.streamId = ((sb0 & 0x7F) << 24) | (sb1 << 16) | (sb2 << 8) | sb3;
        long payloadAddr = addr + SIZE;
        this.payloadAddrHi = (int) (payloadAddr >>> 32);
        this.payloadAddrLo = (int) payloadAddr;
    }

    public void reset() {
        this.payloadLength = 0;
        this.type = 0;
        this.flags = 0;
        this.streamId = 0;
        this.payloadAddrHi = 0;
        this.payloadAddrLo = 0;
    }

    /**
     * Writes the 9-byte header at {@code addr}. The length field is 24 bits
     * wide (RFC 7540 sec. 4.1); callers are responsible for capping the chunk
     * at {@link Http2Settings#MAX_FRAME_SIZE_UPPER} and at the peer's advertised
     * {@code SETTINGS_MAX_FRAME_SIZE}. Passing an out-of-range length throws
     * rather than silently truncating and corrupting the wire stream.
     */
    public static void write(long addr, int payloadLength, byte type, byte flags, int streamId) {
        if (payloadLength < 0 || payloadLength > Http2Settings.MAX_FRAME_SIZE_UPPER) {
            throw new IllegalArgumentException("payloadLength out of range: " + payloadLength);
        }
        // Reserved high bit is sent 0 (RFC 7540 sec. 4.1); reject negative
        // stream ids so a wraparound doesn't silently remap onto the 31-bit
        // field. Frame-type-specific rules (e.g. DATA requires streamId > 0,
        // SETTINGS requires streamId == 0) live in Http2FrameWriter.
        if (streamId < 0) {
            throw new IllegalArgumentException("streamId must be non-negative: " + streamId);
        }
        Unsafe.getUnsafe().putByte(addr, (byte) ((payloadLength >>> 16) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 1, (byte) ((payloadLength >>> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 2, (byte) (payloadLength & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 3, type);
        Unsafe.getUnsafe().putByte(addr + 4, flags);
        Unsafe.getUnsafe().putByte(addr + 5, (byte) ((streamId >>> 24) & 0x7F));
        Unsafe.getUnsafe().putByte(addr + 6, (byte) ((streamId >>> 16) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 7, (byte) ((streamId >>> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 8, (byte) (streamId & 0xFF));
    }
}
