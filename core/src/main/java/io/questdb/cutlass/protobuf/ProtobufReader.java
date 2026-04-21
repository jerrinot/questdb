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
 * Stateful protobuf decoder over a caller-owned native buffer. The
 * decoder holds a read cursor into {@code [addr, limit)} and, for the
 * length-delimited path, a last-read {@code (addr, len)} pair so callers
 * can retrieve the payload slice without allocation.
 * <p>
 * Zero-allocation: callers reuse one reader instance per thread /
 * connection and call {@link #of(long, long)} to rebind to a new
 * message. On structural failure the reader throws a thread-local
 * {@link ProtobufException} — the gRPC framing layer converts it into
 * a {@code grpc-status: INTERNAL} trailers-only response.
 */
public final class ProtobufReader {

    private long cursor;
    private long lastValueAddr;
    private int lastValueLen;
    private long limit;

    public long cursor() {
        return cursor;
    }

    public boolean hasMore() {
        return cursor < limit;
    }

    public long lastValueAddr() {
        return lastValueAddr;
    }

    public int lastValueLen() {
        return lastValueLen;
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
        this.lastValueAddr = 0;
        this.lastValueLen = 0;
    }

    /**
     * Reads a length-delimited field body. Advances the cursor past the
     * body and stages the {@code (addr, len)} pair on
     * {@link #lastValueAddr()} / {@link #lastValueLen()} for the caller.
     * The length prefix must already have been consumed via
     * {@link #readTag()}'s wire-type branch.
     */
    public void readLengthDelimited() {
        long len = readVarint64();
        if (len < 0 || len > Integer.MAX_VALUE) {
            throw ProtobufException.instance("length out of int range");
        }
        if (cursor + len > limit) {
            throw ProtobufException.instance("truncated length-delimited field");
        }
        lastValueAddr = cursor;
        lastValueLen = (int) len;
        cursor += len;
    }

    /**
     * Reads a tag varint. Returns the packed
     * {@code (fieldNumber << 3) | wireType}. Use
     * {@link ProtobufWireFormat#fieldNumberOf(int)} and
     * {@link ProtobufWireFormat#wireTypeOf(int)} to unpack.
     */
    public int readTag() {
        long v = readVarint64();
        if (v <= 0 || v > Integer.MAX_VALUE) {
            throw ProtobufException.instance("tag out of int range");
        }
        int tag = (int) v;
        if (ProtobufWireFormat.fieldNumberOf(tag) <= 0) {
            throw ProtobufException.instance("invalid field number");
        }
        return tag;
    }

    /**
     * Reads a raw varint. Throws {@link ProtobufException} on truncation
     * or on a sequence longer than {@link ProtobufWireFormat#MAX_VARINT_BYTES}.
     */
    public long readVarint64() {
        long result = 0;
        int shift = 0;
        int bytes = 0;
        while (true) {
            if (cursor >= limit) {
                throw ProtobufException.instance("truncated varint");
            }
            if (bytes >= ProtobufWireFormat.MAX_VARINT_BYTES) {
                throw ProtobufException.instance("varint too long");
            }
            int b = Unsafe.getUnsafe().getByte(cursor) & 0xFF;
            cursor++;
            bytes++;
            result |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }

    /**
     * Skips a field body given its wire type. {@code START_GROUP} /
     * {@code END_GROUP} are rejected as unsupported.
     */
    public void skipField(int wireType) {
        switch (wireType) {
            case ProtobufWireFormat.WIRE_TYPE_VARINT:
                readVarint64();
                break;
            case ProtobufWireFormat.WIRE_TYPE_FIXED64:
                if (cursor + 8 > limit) {
                    throw ProtobufException.instance("truncated fixed64");
                }
                cursor += 8;
                break;
            case ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED:
                readLengthDelimited();
                break;
            case ProtobufWireFormat.WIRE_TYPE_FIXED32:
                if (cursor + 4 > limit) {
                    throw ProtobufException.instance("truncated fixed32");
                }
                cursor += 4;
                break;
            default:
                throw ProtobufException.instance("unsupported wire type");
        }
    }
}
