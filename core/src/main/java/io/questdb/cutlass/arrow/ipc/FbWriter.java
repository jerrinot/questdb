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

package io.questdb.cutlass.arrow.ipc;

import io.questdb.std.Unsafe;

/**
 * Low-level Flatbuffers builder over a caller-owned native buffer.
 * <p>
 * The buffer is filled bottom-up: the writer cursor starts at
 * {@code limit} and moves toward {@code addr} as bytes are prepended.
 * Cursor position is tracked internally as {@code cursorFromEnd} — the
 * number of bytes written so far — which keeps child offsets stable
 * even while the buffer grows. When the caller is done, {@link #finish}
 * lays down a root uoffset and returns the low byte address of the
 * serialised buffer; the buffer length is {@link #finishedLen}.
 * <p>
 * Flatbuffers alignment works by ensuring each write leaves the cursor
 * at a boundary appropriate for the value just written, measured from
 * the final buffer origin. The implementation follows the canonical
 * {@code FlatBufferBuilder} approach from {@code flatbuffers-java}:
 * before a write of {@code size} bytes (optionally with
 * {@code additionalBytes} that follow), pad with enough zero bytes so
 * that the subsequent write finishes on a {@code size}-byte boundary.
 * <p>
 * Wave 6a does not reuse vtables: every {@link #endTable} emits a fresh
 * vtable even when two tables would serialise identically. Arrow's Java
 * reader decodes either form; the sharing optimisation is worth chasing
 * only once vtable traffic becomes measurable.
 */
public final class FbWriter {

    private static final int SIZEOF_INT = 4;
    private static final int SIZEOF_LONG = 8;
    private static final int SIZEOF_SHORT = 2;
    private long bufferAddr;
    private long bufferLimit;
    private int cursorFromEnd;
    private boolean isFinished;
    private int minAlign;
    private int objectStart;
    private int tableFieldCount;
    /**
     * Scratch storage for per-field offsets during
     * {@link #startTable(int)} / {@link #endTable()}. Sized once and
     * resized on demand; never shrunk.
     */
    private int[] vtableFieldOffsets = new int[16];

    /**
     * Returns the total capacity of the underlying buffer.
     */
    public int bufferCapacity() {
        return (int) (bufferLimit - bufferAddr);
    }

    /**
     * Current offset-from-end — the number of bytes written so far. Also
     * the value used as an identifier for child objects (e.g., a string
     * offset or table offset) when slotting them into a parent vtable.
     */
    public int cursorFromEnd() {
        return cursorFromEnd;
    }

    /**
     * Writes a uoffset_t at the current cursor pointing to
     * {@code referentOffsetFromEnd}, pads / aligns to make the final
     * buffer {@code minAlign}-aligned, and returns the low byte address
     * of the finished buffer. After a successful {@code finish} call
     * the finished buffer occupies {@code [finishedAddr(), bufferLimit)}
     * and its length is {@link #finishedLen()}.
     */
    public long finish(int rootOffsetFromEnd) {
        if (isFinished) {
            throw new IllegalStateException("buffer already finished");
        }
        prep(minAlign, SIZEOF_INT);
        prependUoffset(rootOffsetFromEnd);
        isFinished = true;
        return finishedAddr();
    }

    /**
     * Address of the first byte of the finished buffer.
     */
    public long finishedAddr() {
        return bufferLimit - cursorFromEnd;
    }

    /**
     * Total number of bytes in the finished buffer.
     */
    public int finishedLen() {
        return cursorFromEnd;
    }

    public boolean isFinished() {
        return isFinished;
    }

    /**
     * Binds the writer to {@code [addr, limit)} and resets state. The
     * caller is responsible for the underlying allocation and its
     * lifetime; the writer never frees it.
     */
    public void of(long addr, long limit) {
        if (limit < addr) {
            throw new IllegalArgumentException("limit < addr");
        }
        this.bufferAddr = addr;
        this.bufferLimit = limit;
        this.cursorFromEnd = 0;
        this.minAlign = 1;
        this.isFinished = false;
        this.objectStart = 0;
        this.tableFieldCount = 0;
    }

    /**
     * Prepends {@code count} zero bytes, growing the cursor by the same
     * amount.
     */
    public void pad(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative");
        }
        long endAddr = bufferLimit - cursorFromEnd - count;
        ensureInBounds(endAddr);
        cursorFromEnd += count;
        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putByte(endAddr + i, (byte) 0);
        }
    }

    /**
     * Ensures alignment for a subsequent write of {@code size} bytes
     * followed by {@code additionalBytes} more bytes. Mirrors
     * {@code FlatBufferBuilder.prep}: computes the padding needed so
     * that the total write of {@code size + additionalBytes} finishes
     * at a {@code size}-byte boundary from the eventual buffer origin.
     */
    public void prep(int size, int additionalBytes) {
        if (size > minAlign) {
            minAlign = size;
        }
        int alignSize = ((-(cursorFromEnd + additionalBytes)) & (size - 1));
        pad(alignSize);
    }

    /**
     * Prepends a little-endian int16. Aligns to 2 bytes first.
     */
    public void prependInt16(short v) {
        prep(SIZEOF_SHORT, 0);
        cursorFromEnd += SIZEOF_SHORT;
        long addr = bufferLimit - cursorFromEnd;
        Unsafe.getUnsafe().putByte(addr, (byte) (v & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 1, (byte) ((v >>> 8) & 0xFF));
    }

    /**
     * Prepends a little-endian int32. Aligns to 4 bytes first.
     */
    public void prependInt32(int v) {
        prep(SIZEOF_INT, 0);
        cursorFromEnd += SIZEOF_INT;
        long addr = bufferLimit - cursorFromEnd;
        putLittleEndianInt32(addr, v);
    }

    /**
     * Prepends a little-endian int64. Aligns to 8 bytes first.
     */
    public void prependInt64(long v) {
        prep(SIZEOF_LONG, 0);
        cursorFromEnd += SIZEOF_LONG;
        long addr = bufferLimit - cursorFromEnd;
        Unsafe.getUnsafe().putByte(addr, (byte) (v & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 1, (byte) ((v >>> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 2, (byte) ((v >>> 16) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 3, (byte) ((v >>> 24) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 4, (byte) ((v >>> 32) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 5, (byte) ((v >>> 40) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 6, (byte) ((v >>> 48) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 7, (byte) ((v >>> 56) & 0xFF));
    }

    /**
     * Prepends a signed int8.
     */
    public void prependInt8(byte v) {
        cursorFromEnd++;
        long addr = bufferLimit - cursorFromEnd;
        ensureInBounds(addr);
        Unsafe.getUnsafe().putByte(addr, v);
    }

    /**
     * Prepends a uoffset_t (uint32) pointing from the just-written slot
     * to {@code referentOffsetFromEnd}. The encoded value is the byte
     * distance from this slot forward to the referent, i.e.
     * {@code (cursorFromEnd_after_write) - referentOffsetFromEnd}.
     */
    public void prependUoffset(int referentOffsetFromEnd) {
        prep(SIZEOF_INT, 0);
        int encoded = (cursorFromEnd + SIZEOF_INT) - referentOffsetFromEnd;
        if (encoded <= 0) {
            throw new IllegalStateException("uoffset references at or behind the cursor");
        }
        cursorFromEnd += SIZEOF_INT;
        putLittleEndianInt32(bufferLimit - cursorFromEnd, encoded);
    }

    /**
     * Prepends a uint8. The value is truncated to eight bits.
     */
    public void prependUint8(int v) {
        prependInt8((byte) (v & 0xFF));
    }

    /**
     * Begins a table layout. Subsequent {@link #slot(int, int)} calls
     * attach field offsets to the vtable under construction. Call
     * {@link #endTable()} once all fields are written.
     */
    public void startTable(int fieldCount) {
        if (tableFieldCount != 0) {
            throw new IllegalStateException("nested startTable not supported");
        }
        if (fieldCount < 0) {
            throw new IllegalArgumentException("fieldCount must be non-negative");
        }
        if (fieldCount > vtableFieldOffsets.length) {
            vtableFieldOffsets = new int[Math.max(fieldCount, vtableFieldOffsets.length * 2)];
        }
        for (int i = 0; i < fieldCount; i++) {
            vtableFieldOffsets[i] = 0;
        }
        tableFieldCount = fieldCount;
        objectStart = cursorFromEnd;
    }

    /**
     * Records the current cursor position as the location of field
     * {@code fieldIndex}. Must be called immediately after prepending
     * the field's value. A later {@link #endTable()} uses these
     * positions to emit the vtable.
     */
    public void slot(int fieldIndex, int fieldOffsetFromEnd) {
        if (fieldIndex < 0 || fieldIndex >= tableFieldCount) {
            throw new IllegalArgumentException("fieldIndex out of range: " + fieldIndex);
        }
        vtableFieldOffsets[fieldIndex] = fieldOffsetFromEnd;
    }

    /**
     * Ends the current table. Writes a 4-byte soffset placeholder at
     * the table start, prepends the vtable (including
     * {@code vtable_length} and {@code table_length} headers), and
     * patches the soffset to point back at the vtable. Returns the
     * offset-from-end of the table (its soffset slot).
     */
    public int endTable() {
        if (tableFieldCount < 0) {
            throw new IllegalStateException("endTable without startTable");
        }
        prependInt32(0);
        int vtableloc = cursorFromEnd;

        for (int i = tableFieldCount - 1; i >= 0; i--) {
            int fieldOffset = vtableFieldOffsets[i];
            short entry = fieldOffset != 0 ? (short) (vtableloc - fieldOffset) : 0;
            prependInt16(entry);
        }
        // table_length: size of inline table body including soffset.
        prependInt16((short) (vtableloc - objectStart));
        // vt_length: size of the vtable in bytes.
        int vtSize = SIZEOF_SHORT + SIZEOF_SHORT + tableFieldCount * SIZEOF_SHORT;
        prependInt16((short) vtSize);

        // Patch soffset placeholder with the table-to-vtable distance.
        int soffset = cursorFromEnd - vtableloc;
        long soffsetAddr = bufferLimit - vtableloc;
        putLittleEndianInt32(soffsetAddr, soffset);

        tableFieldCount = 0;
        return vtableloc;
    }

    /**
     * Closes a vector previously opened via
     * {@link #startVector(int, int, int)}. Prepends the 4-byte length
     * prefix and returns the offset-from-end of the vector (pointing at
     * the length prefix's low byte).
     */
    public int endVector(int count) {
        prependInt32(count);
        return cursorFromEnd;
    }

    /**
     * Opens a vector of {@code count} fixed-size elements. Mirrors
     * {@code FlatBufferBuilder.startVector}: the caller prepends each
     * element between {@code startVector} and {@link #endVector(int)}.
     * The buffer is pre-aligned so that the final vector header is
     * 4-byte aligned and the element payload honours {@code alignment}.
     */
    public void startVector(int elementSize, int count, int alignment) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative");
        }
        int totalBytes = elementSize * count;
        prep(SIZEOF_INT, totalBytes);
        if (alignment > SIZEOF_INT) {
            prep(alignment, totalBytes);
        }
    }

    /**
     * Writes a UTF-8 string: int32 length prefix + bytes + trailing
     * null byte. Returns the offset-from-end of the length prefix.
     */
    public int writeString(long utf8Addr, int utf8Len) {
        if (utf8Len < 0) {
            throw new IllegalArgumentException("utf8Len must be non-negative");
        }
        prep(SIZEOF_INT, utf8Len + 1);
        // trailing NUL
        prependInt8((byte) 0);
        // payload, prepended byte-by-byte so the last source byte lands
        // at the highest memory address.
        for (int i = utf8Len - 1; i >= 0; i--) {
            prependInt8(Unsafe.getUnsafe().getByte(utf8Addr + i));
        }
        prependInt32(utf8Len);
        return cursorFromEnd;
    }

    private void ensureInBounds(long addr) {
        if (addr < bufferAddr) {
            throw new IllegalStateException("FbWriter buffer overflow");
        }
    }

    private void putLittleEndianInt32(long addr, int v) {
        Unsafe.getUnsafe().putByte(addr, (byte) (v & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 1, (byte) ((v >>> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 2, (byte) ((v >>> 16) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 3, (byte) ((v >>> 24) & 0xFF));
    }
}
