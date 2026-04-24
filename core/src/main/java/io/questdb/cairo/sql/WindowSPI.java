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

package io.questdb.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.std.Unsafe;

public interface WindowSPI {
    long getAddress(long recordAddress, int columnIndex);

    Record getRecordAt(long recordOffset);

    /**
     * Returns a fixed-width view over a cached record-chain column.
     * <p>
     * This is used by window executors to bind result columns and private scratch columns declared through
     * {@code WindowFunction.CachedFunctionLayout}. The supplied {@code columnType} must be a fixed-width type
     * supported by {@link FixedSizeColumn#putValue(long, Function, Record)}.
     */
    default FixedSizeColumn getFixedSizeColumn(int columnIndex, int columnType) {
        return new FixedSizeColumn(this, columnIndex, columnType);
    }

    /**
     * Materializes the current value of {@code function} into a fixed-width cached column.
     * <p>
     * This is a low-level helper for cached window execution. It is valid only for function result types supported by
     * {@link FixedSizeColumn#putValue(long, Function, Record)}.
     */
    default void put(long recordOffset, int columnIndex, Function function, Record record) {
        getFixedSizeColumn(columnIndex, function.getType()).putValue(recordOffset, function, record);
    }

    /**
     * Bound accessor for a fixed-width cached record-chain column.
     * <p>
     * The cached window executor uses this for both result columns and per-function scratch columns. Variable-width
     * types are intentionally unsupported here.
     */
    final class FixedSizeColumn {
        private final int columnIndex;
        private final int columnType;
        private final WindowSPI spi;

        public FixedSizeColumn(WindowSPI spi, int columnIndex, int columnType) {
            this.spi = spi;
            this.columnIndex = columnIndex;
            this.columnType = columnType;
        }

        public boolean getBool(long recordOffset) {
            return Unsafe.getUnsafe().getByte(address(recordOffset)) == 1;
        }

        public byte getByte(long recordOffset) {
            return Unsafe.getUnsafe().getByte(address(recordOffset));
        }

        public char getChar(long recordOffset) {
            return Unsafe.getUnsafe().getChar(address(recordOffset));
        }

        public double getDouble(long recordOffset) {
            return Unsafe.getUnsafe().getDouble(address(recordOffset));
        }

        public float getFloat(long recordOffset) {
            return Unsafe.getUnsafe().getFloat(address(recordOffset));
        }

        public int getInt(long recordOffset) {
            return Unsafe.getUnsafe().getInt(address(recordOffset));
        }

        public long getLong(long recordOffset) {
            return Unsafe.getUnsafe().getLong(address(recordOffset));
        }

        public short getShort(long recordOffset) {
            return Unsafe.getUnsafe().getShort(address(recordOffset));
        }

        public void putBool(long recordOffset, boolean value) {
            Unsafe.getUnsafe().putByte(address(recordOffset), (byte) (value ? 1 : 0));
        }

        public void putByte(long recordOffset, byte value) {
            Unsafe.getUnsafe().putByte(address(recordOffset), value);
        }

        public void putChar(long recordOffset, char value) {
            Unsafe.getUnsafe().putChar(address(recordOffset), value);
        }

        public void putDouble(long recordOffset, double value) {
            Unsafe.getUnsafe().putDouble(address(recordOffset), value);
        }

        public void putFloat(long recordOffset, float value) {
            Unsafe.getUnsafe().putFloat(address(recordOffset), value);
        }

        public void putInt(long recordOffset, int value) {
            Unsafe.getUnsafe().putInt(address(recordOffset), value);
        }

        public void putLong(long recordOffset, long value) {
            Unsafe.getUnsafe().putLong(address(recordOffset), value);
        }

        public void putShort(long recordOffset, short value) {
            Unsafe.getUnsafe().putShort(address(recordOffset), value);
        }

        public void putValue(long recordOffset, Function function, Record record) {
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN:
                    putBool(recordOffset, function.getBool(record));
                    break;
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                case ColumnType.DECIMAL8:
                    putByte(recordOffset, function.getByte(record));
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                case ColumnType.DECIMAL16:
                    putShort(recordOffset, function.getShort(record));
                    break;
                case ColumnType.CHAR:
                    putChar(recordOffset, function.getChar(record));
                    break;
                case ColumnType.INT:
                case ColumnType.IPv4:
                case ColumnType.GEOINT:
                case ColumnType.DECIMAL32:
                    putInt(recordOffset, function.getInt(record));
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                case ColumnType.GEOLONG:
                case ColumnType.DECIMAL64:
                    putLong(recordOffset, function.getLong(record));
                    break;
                case ColumnType.FLOAT:
                    putFloat(recordOffset, function.getFloat(record));
                    break;
                case ColumnType.DOUBLE:
                    putDouble(recordOffset, function.getDouble(record));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "window result materialization is unsupported for type " + ColumnType.nameOf(ColumnType.tagOf(columnType))
                    );
            }
        }

        private long address(long recordOffset) {
            return spi.getAddress(recordOffset, columnIndex);
        }
    }
}
