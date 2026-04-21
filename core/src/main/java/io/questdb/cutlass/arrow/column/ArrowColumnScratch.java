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
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * Per-column native scratch used by the Arrow Flight SQL DoGet path during
 * one RecordBatch. Modelled on QwpColumnScratch: stateful, type-switch
 * append, lazy native buffer allocation, per-batch reuse via {@link #reset()},
 * full free via {@link #close()}.
 * <p>
 * Wave 6b covers the fixed-width dense Arrow layout for LONG (int64),
 * DOUBLE (float64) and INT (int32) — each column writes a single dense
 * values buffer. No validity bitmap is emitted; QuestDB sentinel nulls
 * pass through raw. Wave 7 adds validity bitmaps, null detection inside
 * dedicated {@code appendLongOrNull} / {@code appendDoubleOrNull} /
 * {@code appendIntOrNull} methods, plus heap/offsets/symbol buffers for
 * variable-width column types. Their fields will live alongside
 * {@link #valuesAddr} following the QwpColumnScratch layout.
 */
public final class ArrowColumnScratch implements QuietCloseable {

    private static final int INITIAL_BYTES = 4096;
    private final int memoryTag;
    private int columnType;
    private int rowCount;
    private long valuesAddr;
    private int valuesCap;
    private int valuesPos;

    public ArrowColumnScratch() {
        this(MemoryTag.NATIVE_HTTP_CONN);
    }

    public ArrowColumnScratch(int memoryTag) {
        this.memoryTag = memoryTag;
    }

    public void appendDouble(double v) {
        ensureValuesCap(8);
        Unsafe.getUnsafe().putDouble(valuesAddr + valuesPos, v);
        valuesPos += 8;
        rowCount++;
    }

    public void appendInt(int v) {
        ensureValuesCap(4);
        Unsafe.getUnsafe().putInt(valuesAddr + valuesPos, v);
        valuesPos += 4;
        rowCount++;
    }

    public void appendLong(long v) {
        ensureValuesCap(8);
        Unsafe.getUnsafe().putLong(valuesAddr + valuesPos, v);
        valuesPos += 8;
        rowCount++;
    }

    @Override
    public void close() {
        if (valuesAddr != 0) {
            Unsafe.free(valuesAddr, valuesCap, memoryTag);
            valuesAddr = 0;
            valuesCap = 0;
            valuesPos = 0;
        }
        rowCount = 0;
    }

    public int getColumnType() {
        return columnType;
    }

    public int getRowCount() {
        return rowCount;
    }

    /**
     * Copies the dense values buffer to {@code dst} and returns the address one
     * past the last written byte. Callers align the following column's start
     * to 8 bytes themselves (see {@code ArrowRecordBatchWriter.alignTo8}).
     */
    public long flushValuesTo(long dst) {
        if (valuesPos > 0) {
            Unsafe.getUnsafe().copyMemory(valuesAddr, dst, valuesPos);
        }
        return dst + valuesPos;
    }

    /**
     * Prepares this scratch for a column of the given QuestDB type. Allocates
     * the dense values buffer up front sized for {@code initialRowCap} rows.
     * May be called repeatedly: capacity grows, it never shrinks. Wave 7 will
     * extend this to size validity/heap/offsets buckets based on the type.
     */
    public void initFor(int columnType, int initialRowCap) {
        this.columnType = columnType;
        this.rowCount = 0;
        this.valuesPos = 0;
        int required = initialRowCap * bytesPerRow(columnType);
        if (required > 0) {
            ensureValuesCap(required - valuesPos);
        }
    }

    /**
     * Clears per-batch counters without freeing the backing buffer. Call
     * between RecordBatches so the native memory is reused in place.
     */
    public void reset() {
        rowCount = 0;
        valuesPos = 0;
    }

    public int valuesLengthBytes() {
        return valuesPos;
    }

    private static int bytesPerRow(int columnType) {
        switch (columnType) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
                return 8;
            case ColumnType.INT:
                return 4;
            default:
                return 0;
        }
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
}
