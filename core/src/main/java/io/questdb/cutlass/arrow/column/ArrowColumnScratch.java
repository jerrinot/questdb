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

/**
 * Per-column native scratch used by the Arrow Flight SQL DoGet path during
 * one RecordBatch. Modelled on QwpColumnScratch: stateful, type-switch
 * append, lazy native buffer allocation, per-batch reuse via {@link #reset()},
 * full free via {@link #close()}.
 * <p>
 * Wave 7a covers fixed-width Arrow layout for LONG (int64), DOUBLE (float64),
 * INT (int32), FLOAT (float32), BYTE (int8), SHORT (int16) and BOOLEAN
 * (bit-packed). Nullable types (LONG/DOUBLE/INT/FLOAT) expose dedicated
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
 */
public final class ArrowColumnScratch implements QuietCloseable {

    private static final int INITIAL_BYTES = 4096;
    private static final int INITIAL_VALIDITY_BYTES = 64;
    private final int memoryTag;
    private int columnType;
    private int nullCount;
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
        rowCount = 0;
        nullCount = 0;
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
     * the dense values buffer up front sized for {@code initialRowCap} rows.
     * May be called repeatedly: capacity grows, it never shrinks. The
     * validity buffer stays unallocated until the first null append.
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
        this.rowCount = 0;
        this.valuesPos = 0;
        this.nullCount = 0;
        int required = bytesForRows(columnType, initialRowCap);
        if (required > 0) {
            ensureValuesCap(required);
        }
    }

    /**
     * Clears per-batch counters without freeing backing buffers. Zeros any
     * previously-written validity bytes so the next batch starts clean
     * while still hitting the lazy-alloc fast path when no nulls appear.
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
            default:
                return 0;
        }
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

    private void markValid() {
        if (validityAddr != 0) {
            ensureValidityCap(rowCount);
            long byteAddr = validityAddr + (rowCount >>> 3);
            byte cur = Unsafe.getUnsafe().getByte(byteAddr);
            Unsafe.getUnsafe().putByte(byteAddr, (byte) (cur | (1 << (rowCount & 7))));
        }
        rowCount++;
    }
}
