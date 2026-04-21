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

package io.questdb.cutlass.protobuf;

import io.questdb.std.Unsafe;

/**
 * Stateful protobuf encoder over a caller-owned native buffer. Mirrors
 * the HPACK encoder's {@code -1}-on-overflow convention: every write
 * method returns the post-write cursor on success, or {@code -1} if the
 * write would not fit, in which case the cursor is left unchanged and
 * no partial bytes are observable.
 * <p>
 * Zero-allocation: callers reuse one writer instance per thread /
 * connection and call {@link #of(long, long)} to rebind to a new buffer.
 * Callers that want a static / cursor-passing style can reach for the
 * {@code staticWrite*} helpers.
 * <p>
 * Wire subset: varint ({@link ProtobufWireFormat#WIRE_TYPE_VARINT}) and
 * length-delimited ({@link ProtobufWireFormat#WIRE_TYPE_LENGTH_DELIMITED}).
 * The {@code bytes} type covers everything Wave 5 needs; {@code string}
 * fields decode byte-identically so callers can reuse
 * {@link #writeLengthDelimitedField}.
 */
public final class ProtobufWriter {

    private long cursor;
    private long limit;

    /**
     * Static helper that writes a length-delimited field atomically.
     * See {@link #writeLengthDelimitedField(int, long, int)} for contract.
     */
    public static long staticWriteLengthDelimitedField(long addr, long limit, int fieldNumber,
                                                       long srcAddr, int srcLen) {
        if (srcLen < 0) {
            throw new IllegalArgumentException("srcLen must be non-negative: " + srcLen);
        }
        long tag = ProtobufWireFormat.makeTag(fieldNumber, ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED);
        long needed = ProtobufWireFormat.varintSize(tag)
                + ProtobufWireFormat.varintSize(srcLen)
                + srcLen;
        if (addr + needed > limit) {
            return -1;
        }
        long c = staticWriteVarint64Raw(addr, limit, tag);
        c = staticWriteVarint64Raw(c, limit, srcLen);
        if (srcLen > 0) {
            Unsafe.getUnsafe().copyMemory(srcAddr, c, srcLen);
            c += srcLen;
        }
        return c;
    }

    /**
     * Static helper that writes a tag varint. Returns the new cursor on
     * success, or {@code -1} on overflow.
     */
    public static long staticWriteTag(long addr, long limit, int fieldNumber, int wireType) {
        long tag = ProtobufWireFormat.makeTag(fieldNumber, wireType);
        return staticWriteVarint64Raw(addr, limit, tag);
    }

    /**
     * Static helper that writes a raw varint. Returns the new cursor on
     * success, or {@code -1} on overflow. Negative {@code value} encodes
     * as 10 bytes per protobuf uint64 semantics.
     */
    public static long staticWriteVarint64Raw(long addr, long limit, long value) {
        long c = addr;
        long v = value;
        while ((v & ~0x7FL) != 0L) {
            if (c >= limit) {
                return -1;
            }
            Unsafe.getUnsafe().putByte(c, (byte) ((v & 0x7FL) | 0x80L));
            c++;
            v >>>= 7;
        }
        if (c >= limit) {
            return -1;
        }
        Unsafe.getUnsafe().putByte(c, (byte) v);
        return c + 1;
    }

    /**
     * Static helper that writes a varint-wire-type field (tag + value)
     * atomically. See {@link #writeVarint64Field(int, long)} for the
     * contract.
     */
    public static long staticWriteVarint64Field(long addr, long limit, int fieldNumber, long value) {
        long tag = ProtobufWireFormat.makeTag(fieldNumber, ProtobufWireFormat.WIRE_TYPE_VARINT);
        long needed = ProtobufWireFormat.varintSize(tag) + ProtobufWireFormat.varintSize(value);
        if (addr + needed > limit) {
            return -1;
        }
        long c = staticWriteVarint64Raw(addr, limit, tag);
        return staticWriteVarint64Raw(c, limit, value);
    }

    public long cursor() {
        return cursor;
    }

    public long limit() {
        return limit;
    }

    public void of(long addr, long limit) {
        if (limit < addr) {
            throw new IllegalArgumentException("limit < addr");
        }
        this.cursor = addr;
        this.limit = limit;
    }

    public int remaining() {
        return (int) Math.min((long) Integer.MAX_VALUE, limit - cursor);
    }

    /**
     * Writes a length-delimited ({@code bytes}) field: tag + length
     * varint + {@code srcLen} bytes copied from {@code srcAddr}. Returns
     * the new cursor on success, or {@code -1} on buffer overflow (cursor
     * unchanged, no partial bytes written).
     */
    public long writeLengthDelimitedField(int fieldNumber, long srcAddr, int srcLen) {
        long c = staticWriteLengthDelimitedField(cursor, limit, fieldNumber, srcAddr, srcLen);
        if (c < 0) {
            return -1;
        }
        cursor = c;
        return c;
    }

    /**
     * Writes a tag ({@code (fieldNumber << 3) | wireType} as varint).
     * Returns the new cursor on success or {@code -1} on overflow
     * (cursor unchanged).
     */
    public long writeTag(int fieldNumber, int wireType) {
        long c = staticWriteTag(cursor, limit, fieldNumber, wireType);
        if (c < 0) {
            return -1;
        }
        cursor = c;
        return c;
    }

    /**
     * Writes a varint-wire-type field: tag + value varint. Returns the
     * new cursor on success, or {@code -1} on overflow (cursor unchanged,
     * no partial bytes written).
     */
    public long writeVarint64Field(int fieldNumber, long value) {
        long c = staticWriteVarint64Field(cursor, limit, fieldNumber, value);
        if (c < 0) {
            return -1;
        }
        cursor = c;
        return c;
    }

    /**
     * Writes a raw varint with no tag. Returns the new cursor on success
     * or {@code -1} on overflow (cursor unchanged). Negative {@code value}
     * encodes as 10 bytes per protobuf uint64 semantics.
     */
    public long writeVarint64Raw(long value) {
        long c = staticWriteVarint64Raw(cursor, limit, value);
        if (c < 0) {
            return -1;
        }
        cursor = c;
        return c;
    }
}
