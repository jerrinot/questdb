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

import io.questdb.cairo.ColumnType;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;

/**
 * Per-column native scratch used by the Arrow Flight SQL DoGet path during
 * one RecordBatch. Modelled on QwpColumnScratch: stateful, type-switch
 * append, lazy native buffer allocation, per-batch reuse via {@link #reset()},
 * full free via {@link #close()}.
 * <p>
 * Wave 7c covers fixed-width Arrow layout for LONG (int64), DOUBLE (float64),
 * INT (int32), FLOAT (float32), BYTE (int8), SHORT (int16), BOOLEAN
 * (bit-packed), DATE (int64 ms) and TIMESTAMP (int64 us/ns). Wave 7d adds
 * variable-length Arrow Utf8 layout for STRING, VARCHAR and SYMBOL: each
 * Utf8 column emits THREE Arrow buffers (validity + int32 offsets + packed
 * UTF-8 bytes) and a dedicated {@link #appendStringOrNull(CharSequence)} /
 * {@link #appendVarcharOrNull(Utf8Sequence)} entry point. Nullable
 * fixed-width types (LONG/DOUBLE/INT/FLOAT/DATE/TIMESTAMP) expose dedicated
 * {@code appendXxxOrNull} entry points that detect QuestDB sentinels and
 * maintain an Arrow-shaped validity bitmap (LSB-first, {@code 1=valid},
 * {@code 0=null}; opposite polarity to QWP's {@code 1=null} bitmap).
 * BYTE / SHORT / BOOLEAN have no null sentinel in QuestDB and always mark
 * their rows valid, so their validity buffer stays empty.
 * <p>
 * The validity buffer is lazy-allocated on the first null encountered.
 * When a batch finishes with {@code nullCount == 0}, {@link #validityLengthBytes()}
 * returns 0 and the caller emits an empty validity Arrow buffer (Arrow
 * readers treat this as "all valid" without inspecting the absent bits).
 * <p>
 * For Utf8 columns the offsets buffer is eagerly allocated on
 * {@link #initFor(int, int)} and always carries {@code 4 * (rowCount + 1)}
 * bytes on the wire, with {@code offsets[0] = 0} and
 * {@code offsets[i+1] = offsets[i] + byteLen(row i)}. Null rows contribute
 * an empty byte run (the offset repeats). The per-cell hard cap of
 * {@link #MAX_UTF8_CELL_BYTES} guards against malformed inputs; callers may
 * check {@link #isUtf8ValuesNearThreshold()} to force an early batch flush
 * when the accumulated byte count approaches the int32 ceiling.
 */
public final class ArrowColumnScratch implements QuietCloseable {

    /**
     * Soft threshold for Utf8 values buffer size. Callers poll
     * {@link #isUtf8ValuesNearThreshold()} after each append and force an
     * early batch flush once any Utf8 column reaches this size. Chosen so
     * that one more max-sized cell (16 MiB) cannot tip the buffer past
     * {@link Integer#MAX_VALUE}.
     */
    public static final int UTF8_VALUES_SOFT_CAP = Integer.MAX_VALUE / 2;
    /**
     * Hard per-cell cap for Utf8 appends. Single STRING / VARCHAR cells
     * larger than this are rejected; QuestDB VARCHAR values never approach
     * this size in practice.
     */
    public static final int MAX_UTF8_CELL_BYTES = 16 * 1024 * 1024;
    private static final int INITIAL_BYTES = 4096;
    private static final int INITIAL_OFFSETS_BYTES = 64;
    private static final int INITIAL_VALIDITY_BYTES = 64;
    private final int memoryTag;
    private int columnType;
    private boolean isUtf8;
    private int nullCount;
    private long offsetsAddr;
    private int offsetsCap;
    private int rowCount;
    private long validityAddr;
    private int validityCap;
    private long valuesAddr;
    private int valuesCap;
    private int valuesPos;

    public ArrowColumnScratch() {
        this(MemoryTag.NATIVE_HTTP_CONN);
    }

    public ArrowColumnScratch(int memoryTag) {
        this.memoryTag = memoryTag;
    }

    public void appendBool(boolean v) {
        int bitIdx = rowCount;
        int byteIdx = bitIdx >>> 3;
        ensureValuesCap((byteIdx + 1) - valuesPos);
        long byteAddr = valuesAddr + byteIdx;
        if ((bitIdx & 7) == 0) {
            Unsafe.getUnsafe().putByte(byteAddr, (byte) 0);
        }
        if (v) {
            byte cur = Unsafe.getUnsafe().getByte(byteAddr);
            Unsafe.getUnsafe().putByte(byteAddr, (byte) (cur | (1 << (bitIdx & 7))));
        }
        // rowCount drives valuesPos for BOOLEAN; keep valuesPos in sync for
        // capacity computations above.
        valuesPos = (bitIdx + 1 + 7) >>> 3;
        markValid();
    }

    public void appendByte(byte v) {
        ensureValuesCap(1);
        Unsafe.getUnsafe().putByte(valuesAddr + valuesPos, v);
        valuesPos += 1;
        markValid();
    }

    public void appendDouble(double v) {
        ensureValuesCap(8);
        Unsafe.getUnsafe().putDouble(valuesAddr + valuesPos, v);
        valuesPos += 8;
        markValid();
    }

    public void appendDoubleOrNull(double v) {
        ensureValuesCap(8);
        if (Numbers.isNull(v)) {
            Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, 0L);
            valuesPos += 8;
            markNull();
        } else {
            Unsafe.getUnsafe().putDouble(valuesAddr + valuesPos, v);
            valuesPos += 8;
            markValid();
        }
    }

    public void appendFloat(float v) {
        ensureValuesCap(4);
        Unsafe.getUnsafe().putFloat(valuesAddr + valuesPos, v);
        valuesPos += 4;
        markValid();
    }

    public void appendFloatOrNull(float v) {
        ensureValuesCap(4);
        if (Numbers.isNull(v)) {
            Unsafe.getUnsafe().putInt(valuesAddr + valuesPos, 0);
            valuesPos += 4;
            markNull();
        } else {
            Unsafe.getUnsafe().putFloat(valuesAddr + valuesPos, v);
            valuesPos += 4;
            markValid();
        }
    }

    public void appendInt(int v) {
        ensureValuesCap(4);
        Unsafe.getUnsafe().putInt(valuesAddr + valuesPos, v);
        valuesPos += 4;
        markValid();
    }

    public void appendIntOrNull(int v) {
        ensureValuesCap(4);
        if (v == Numbers.INT_NULL) {
            Unsafe.getUnsafe().putInt(valuesAddr + valuesPos, 0);
            valuesPos += 4;
            markNull();
        } else {
            Unsafe.getUnsafe().putInt(valuesAddr + valuesPos, v);
            valuesPos += 4;
            markValid();
        }
    }

    public void appendLong(long v) {
        ensureValuesCap(8);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, v);
        valuesPos += 8;
        markValid();
    }

    public void appendLongOrNull(long v) {
        ensureValuesCap(8);
        if (v == Numbers.LONG_NULL) {
            Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, 0L);
            valuesPos += 8;
            markNull();
        } else {
            Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, v);
            valuesPos += 8;
            markValid();
        }
    }

    public void appendShort(short v) {
        ensureValuesCap(2);
        Unsafe.getUnsafe().putShort(valuesAddr + valuesPos, v);
        valuesPos += 2;
        markValid();
    }

    /**
     * Appends a STRING or SYMBOL value (both exposed as
     * {@link CharSequence} by QuestDB). A {@code null} CharSequence
     * contributes no data bytes and a flat offset; the validity bitmap is
     * updated accordingly. The encoded UTF-8 byte count must not exceed
     * {@link #MAX_UTF8_CELL_BYTES} -- larger inputs throw
     * {@link IllegalArgumentException}, letting the caller surface a
     * {@code grpc-status: INTERNAL} error.
     */
    public void appendStringOrNull(CharSequence cs) {
        if (cs == null) {
            writeOffsetForRow(valuesPos);
            markNull();
            return;
        }
        int charLen = cs.length();
        // Worst case: 4 bytes per BMP char after surrogate pairing.
        int worstCase = 4 * charLen;
        if (worstCase > MAX_UTF8_CELL_BYTES) {
            throw new IllegalArgumentException("Utf8 cell exceeds " + MAX_UTF8_CELL_BYTES + " bytes");
        }
        ensureValuesCap(worstCase);
        int start = valuesPos;
        valuesPos = encodeUtf8(cs, valuesAddr, start);
        int written = valuesPos - start;
        if (written > MAX_UTF8_CELL_BYTES) {
            throw new IllegalArgumentException("Utf8 cell exceeds " + MAX_UTF8_CELL_BYTES + " bytes");
        }
        writeOffsetForRow(valuesPos);
        markValid();
    }

    /**
     * Appends a VARCHAR value backed by a {@link Utf8Sequence}. Already
     * UTF-8, so on the direct-backed fast path the append resolves to a
     * single {@link Vect#memcpy(long, long, long)}; on-heap sequences fall
     * back to {@link Utf8Sequence#writeTo(long, int, int)}. Null sequences
     * mirror {@link #appendStringOrNull(CharSequence)}.
     */
    public void appendVarcharOrNull(Utf8Sequence us) {
        if (us == null) {
            writeOffsetForRow(valuesPos);
            markNull();
            return;
        }
        int n = us.size();
        if (n > MAX_UTF8_CELL_BYTES) {
            throw new IllegalArgumentException("Utf8 cell exceeds " + MAX_UTF8_CELL_BYTES + " bytes");
        }
        ensureValuesCap(n);
        long dst = valuesAddr + valuesPos;
        long src = us.ptr();
        if (src >= 0) {
            Vect.memcpy(dst, src, n);
        } else {
            us.writeTo(dst, 0, n);
        }
        valuesPos += n;
        writeOffsetForRow(valuesPos);
        markValid();
    }

    @Override
    public void close() {
        if (valuesAddr != 0) {
            Unsafe.free(valuesAddr, valuesCap, memoryTag);
            valuesAddr = 0;
            valuesCap = 0;
            valuesPos = 0;
        }
        if (validityAddr != 0) {
            Unsafe.free(validityAddr, validityCap, memoryTag);
            validityAddr = 0;
            validityCap = 0;
        }
        if (offsetsAddr != 0) {
            Unsafe.free(offsetsAddr, offsetsCap, memoryTag);
            offsetsAddr = 0;
            offsetsCap = 0;
        }
        rowCount = 0;
        nullCount = 0;
    }

    /**
     * Copies the int32 offsets buffer for a Utf8 column to {@code dst}.
     * Emits {@code 4 * (rowCount + 1)} bytes, including the implicit
     * {@code offsets[0] = 0}. Returns 0 for fixed-width columns (no
     * offsets buffer).
     */
    public long flushOffsetsTo(long dst) {
        int len = offsetsLengthBytes();
        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(offsetsAddr, dst, len);
        }
        return dst + len;
    }

    /**
     * Copies the current validity bitmap to {@code dst}. Only the bytes
     * returned by {@link #validityLengthBytes()} are copied. Caller pads
     * to 8 bytes to honour Arrow's per-buffer alignment.
     */
    public long flushValidityTo(long dst) {
        int len = validityLengthBytes();
        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(validityAddr, dst, len);
        }
        return dst + len;
    }

    /**
     * Copies the dense values buffer to {@code dst} and returns the address one
     * past the last written byte. Callers align the following column's start
     * to 8 bytes themselves (see {@code ArrowRecordBatchWriter.alignTo8}).
     */
    public long flushValuesTo(long dst) {
        int len = valuesLengthBytes();
        if (len > 0) {
            Unsafe.getUnsafe().copyMemory(valuesAddr, dst, len);
        }
        return dst + len;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getNullCount() {
        return nullCount;
    }

    public int getRowCount() {
        return rowCount;
    }

    /**
     * Prepares this scratch for a column of the given QuestDB type. Allocates
     * the dense values buffer up front sized for {@code initialRowCap} rows,
     * and for Utf8 columns also eagerly sizes the offsets buffer and plants
     * {@code offsets[0] = 0}. May be called repeatedly: capacity grows, it
     * never shrinks. The validity buffer stays unallocated until the first
     * null append.
     */
    public void initFor(int columnType, int initialRowCap) {
        if (validityAddr != 0 && rowCount > 0) {
            int usedBytes = (rowCount + 7) >>> 3;
            if (usedBytes > validityCap) {
                usedBytes = validityCap;
            }
            Unsafe.getUnsafe().setMemory(validityAddr, usedBytes, (byte) 0);
        }
        this.columnType = columnType;
        this.isUtf8 = isUtf8Type(columnType);
        this.rowCount = 0;
        this.valuesPos = 0;
        this.nullCount = 0;
        int required = bytesForRows(columnType, initialRowCap);
        if (required > 0) {
            ensureValuesCap(required);
        }
        if (isUtf8) {
            ensureOffsetsCap(initialRowCap);
            // Plant offsets[0] = 0 up front so writeOffsetForRow always writes
            // offsets[rowCount + 1] and leaves the leading slot untouched.
            Unsafe.getUnsafe().putInt(offsetsAddr, 0);
        }
    }

    /**
     * Returns true once this column's Utf8 values buffer has grown past
     * {@link #UTF8_VALUES_SOFT_CAP} and a batch flush should be forced before
     * another append pushes it past the int32 ceiling. Always false for
     * fixed-width columns.
     */
    public boolean isUtf8ValuesNearThreshold() {
        return isUtf8 && valuesPos >= UTF8_VALUES_SOFT_CAP;
    }

    public int offsetsLengthBytes() {
        if (!isUtf8) {
            return 0;
        }
        return 4 * (rowCount + 1);
    }

    /**
     * Clears per-batch counters without freeing backing buffers. Zeros any
     * previously-written validity bytes so the next batch starts clean
     * while still hitting the lazy-alloc fast path when no nulls appear.
     * For Utf8 columns resets {@code offsets[0] = 0} so the next append
     * starts from a known-clean state.
     */
    public void reset() {
        if (validityAddr != 0 && rowCount > 0) {
            int usedBytes = (rowCount + 7) >>> 3;
            if (usedBytes > validityCap) {
                usedBytes = validityCap;
            }
            Unsafe.getUnsafe().setMemory(validityAddr, usedBytes, (byte) 0);
        }
        rowCount = 0;
        valuesPos = 0;
        nullCount = 0;
        if (isUtf8 && offsetsAddr != 0) {
            Unsafe.getUnsafe().putInt(offsetsAddr, 0);
        }
    }

    public int validityLengthBytes() {
        return nullCount == 0 ? 0 : (rowCount + 7) >>> 3;
    }

    public int valuesLengthBytes() {
        if (ColumnType.tagOf(columnType) == ColumnType.BOOLEAN) {
            return (rowCount + 7) >>> 3;
        }
        return valuesPos;
    }

    private static int bytesForRows(int columnType, int rowCap) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return rowCap * 8;
            case ColumnType.INT:
            case ColumnType.FLOAT:
                return rowCap * 4;
            case ColumnType.SHORT:
                return rowCap * 2;
            case ColumnType.BYTE:
                return rowCap;
            case ColumnType.BOOLEAN:
                return (rowCap + 7) >>> 3;
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
            case ColumnType.SYMBOL:
                // A modest guess; grows on demand. Using 8 bytes/row keeps
                // the first allocation small while still covering most
                // short identifiers without a realloc.
                return rowCap * 8;
            default:
                return 0;
        }
    }

    /**
     * UTF-8 encode {@code cs} into {@code heapAddr} starting at {@code pos};
     * returns the position one past the last written byte. Caller must
     * pre-size the heap for the worst case ({@code 4 * cs.length()} bytes).
     * Kept in-module (copy of QwpColumnScratch.encodeUtf8) to avoid a
     * cross-module dependency on the QWP scratch class.
     */
    private static int encodeUtf8(CharSequence cs, long heapAddr, int pos) {
        final int charLen = cs.length();
        for (int i = 0; i < charLen; i++) {
            char c = cs.charAt(i);
            if (c < 0x80) {
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) c);
            } else if (c < 0x800) {
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0xC0 | (c >> 6)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            } else if (Character.isHighSurrogate(c) && i + 1 < charLen
                    && Character.isLowSurrogate(cs.charAt(i + 1))) {
                int cp = Character.toCodePoint(c, cs.charAt(i + 1));
                i++;
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0xF0 | (cp >> 18)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | ((cp >> 12) & 0x3F)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | ((cp >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | (cp & 0x3F)));
            } else {
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0xE0 | (c >> 12)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(heapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            }
        }
        return pos;
    }

    private static boolean isUtf8Type(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
            case ColumnType.SYMBOL:
                return true;
            default:
                return false;
        }
    }

    private void backfillValid(int upToRowExclusive) {
        int fullBytes = upToRowExclusive >>> 3;
        int tailBits = upToRowExclusive & 7;
        if (fullBytes > 0) {
            Unsafe.getUnsafe().setMemory(validityAddr, fullBytes, (byte) 0xFF);
        }
        if (tailBits > 0) {
            long tailAddr = validityAddr + fullBytes;
            byte mask = (byte) ((1 << tailBits) - 1);
            byte cur = Unsafe.getUnsafe().getByte(tailAddr);
            Unsafe.getUnsafe().putByte(tailAddr, (byte) (cur | mask));
        }
    }

    private void ensureOffsetsCap(int rowCap) {
        int required = 4 * (rowCap + 1);
        if (offsetsCap >= required) {
            return;
        }
        int oldCap = offsetsCap;
        int newCap = Math.max(oldCap * 2, Math.max(INITIAL_OFFSETS_BYTES, required));
        offsetsAddr = Unsafe.realloc(offsetsAddr, oldCap, newCap, memoryTag);
        offsetsCap = newCap;
    }

    private void ensureValidityCap(int rowIdx) {
        int neededBytes = (rowIdx >>> 3) + 1;
        if (validityCap >= neededBytes) {
            return;
        }
        int oldCap = validityCap;
        int newCap = Math.max(oldCap * 2, Math.max(INITIAL_VALIDITY_BYTES, neededBytes));
        validityAddr = Unsafe.realloc(validityAddr, oldCap, newCap, memoryTag);
        validityCap = newCap;
        Unsafe.getUnsafe().setMemory(validityAddr + oldCap, newCap - oldCap, (byte) 0);
    }

    private void ensureValuesCap(int addBytes) {
        int required = valuesPos + addBytes;
        if (valuesCap >= required) {
            return;
        }
        int newCap = Math.max(valuesCap * 2, Math.max(INITIAL_BYTES, required));
        valuesAddr = Unsafe.realloc(valuesAddr, valuesCap, newCap, memoryTag);
        valuesCap = newCap;
    }

    private void markNull() {
        // Arrow validity: 0 means null. Allocate the bitmap the first time a
        // null arrives and backfill bits for previously-valid rows so they
        // remain marked valid. The bit at the current index stays zero.
        boolean wasLazy = validityAddr == 0;
        ensureValidityCap(rowCount);
        if (wasLazy && rowCount > 0) {
            backfillValid(rowCount);
        }
        nullCount++;
        rowCount++;
    }

    private void markValid() {
        if (validityAddr != 0) {
            ensureValidityCap(rowCount);
            long byteAddr = validityAddr + (rowCount >>> 3);
            byte cur = Unsafe.getUnsafe().getByte(byteAddr);
            Unsafe.getUnsafe().putByte(byteAddr, (byte) (cur | (1 << (rowCount & 7))));
        }
        rowCount++;
    }

    /**
     * Writes {@code offsets[rowCount + 1] = endPos} for the Utf8 row the
     * caller is about to commit. Must be invoked before {@link #markValid()} /
     * {@link #markNull()} so {@code rowCount} still points at the current
     * row.
     */
    private void writeOffsetForRow(int endPos) {
        int slot = rowCount + 1;
        ensureOffsetsCap(slot);
        Unsafe.getUnsafe().putInt(offsetsAddr + 4L * slot, endPos);
    }
}
