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

import io.questdb.cairo.ColumnType;

/**
 * Emits an Arrow IPC {@code Message} wrapping a {@code RecordBatch} with
 * one or more columns. The body bytes (per-column validity + optional
 * offsets + values buffers, each 8-byte aligned) are composed by the
 * caller; this writer emits only the metadata Flatbuffers payload.
 * <p>
 * Each column contributes two or three Arrow {@code Buffer} descriptors
 * depending on whether it is fixed- or variable-width:
 * <ul>
 *   <li>Fixed-width (LONG, DOUBLE, INT, FLOAT, BYTE, SHORT, BOOLEAN,
 *       DATE, TIMESTAMP) emit two buffers: {@code validity}, {@code values}.</li>
 *   <li>Variable-width Utf8 (STRING, VARCHAR, SYMBOL) emit three buffers:
 *       {@code validity}, {@code offsets} (int32 * rowCount + 1),
 *       {@code values} (packed UTF-8 bytes).</li>
 * </ul>
 * An empty validity buffer combined with {@code null_count=0} is
 * interpreted by Arrow readers as "all valid".
 * <p>
 * {@code FieldNode} and {@code Buffer} are both structs in the Arrow
 * Flatbuffers schema (fixed 16-byte inline elements), not tables.
 * <p>
 * Column types outside the supported set (LONG, DOUBLE, INT, FLOAT,
 * BYTE, SHORT, BOOLEAN, DATE, TIMESTAMP, STRING, VARCHAR, SYMBOL) trigger
 * {@link UnsupportedColumnTypeException}.
 */
public final class ArrowRecordBatchWriter {

    private static final int BODY_ALIGNMENT = 8;
    private static final short METADATA_VERSION_V5 = 4;
    private static final int MESSAGE_HEADER_RECORD_BATCH = 3;

    private ArrowRecordBatchWriter() {
    }

    /**
     * Rounds {@code offset} up to the next 8-byte boundary. Each Arrow
     * buffer starts at an 8-byte-aligned offset within the body; an
     * INT32 column (4-byte natural alignment) therefore gets 4 bytes of
     * tail padding before the next buffer.
     */
    public static long alignTo8(long offset) {
        return (offset + BODY_ALIGNMENT - 1) & ~(long) (BODY_ALIGNMENT - 1);
    }

    /**
     * Byte width of the values buffer row for the given fixed-width column
     * type. BOOLEAN returns 0 since the caller uses bit-packed layout that
     * does not admit a whole-byte-per-row product (callers size BOOLEAN
     * values buffers via {@code (rowCount + 7) / 8}). Variable-width Utf8
     * columns also throw; the caller never needs a per-row byte width for
     * those because values buffer size is driven by the offsets buffer.
     */
    public static int bytesPerRowOf(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return 8;
            case ColumnType.INT:
            case ColumnType.FLOAT:
                return 4;
            case ColumnType.SHORT:
                return 2;
            case ColumnType.BYTE:
                return 1;
            case ColumnType.BOOLEAN:
                return 0;
            default:
                throw new UnsupportedColumnTypeException(columnType);
        }
    }

    /**
     * Computes the total aligned body size given per-column validity,
     * offsets and values buffer lengths. Each buffer starts at an
     * 8-byte-aligned offset. {@code offsetsLengthsPerColumn[i] == 0}
     * indicates a fixed-width column (no offsets buffer). The arrays must
     * have length equal to the column count.
     */
    public static long computeBodyBytes(long[] validityLengthsPerColumn,
                                        long[] offsetsLengthsPerColumn,
                                        long[] valuesLengthsPerColumn) {
        validateLengths(validityLengthsPerColumn, offsetsLengthsPerColumn, valuesLengthsPerColumn);
        long total = 0;
        for (int i = 0, n = valuesLengthsPerColumn.length; i < n; i++) {
            total = alignTo8(total + validityLengthsPerColumn[i]);
            if (offsetsLengthsPerColumn[i] > 0) {
                total = alignTo8(total + offsetsLengthsPerColumn[i]);
            }
            total = alignTo8(total + valuesLengthsPerColumn[i]);
        }
        return total;
    }

    /**
     * Computes the body offset of column {@code columnIndex}'s offsets
     * buffer. Valid only for variable-width Utf8 columns (those with
     * {@code offsetsLengthsPerColumn[columnIndex] > 0}); callers must
     * gate on the length array before invoking.
     */
    public static long computeColumnOffsetsOffset(long[] validityLengthsPerColumn,
                                                  long[] offsetsLengthsPerColumn,
                                                  long[] valuesLengthsPerColumn, int columnIndex) {
        long base = computeColumnValidityOffset(validityLengthsPerColumn, offsetsLengthsPerColumn,
                valuesLengthsPerColumn, columnIndex);
        return alignTo8(base + validityLengthsPerColumn[columnIndex]);
    }

    /**
     * Computes the body offset of column {@code columnIndex}'s validity
     * buffer. Offsets are 8-byte aligned; column 0 starts at offset 0.
     */
    public static long computeColumnValidityOffset(long[] validityLengthsPerColumn,
                                                   long[] offsetsLengthsPerColumn,
                                                   long[] valuesLengthsPerColumn, int columnIndex) {
        long offset = 0;
        for (int i = 0; i < columnIndex; i++) {
            offset = alignTo8(offset + validityLengthsPerColumn[i]);
            if (offsetsLengthsPerColumn[i] > 0) {
                offset = alignTo8(offset + offsetsLengthsPerColumn[i]);
            }
            offset = alignTo8(offset + valuesLengthsPerColumn[i]);
        }
        return offset;
    }

    /**
     * Computes the body offset of column {@code columnIndex}'s values
     * buffer. Starts after the column's validity (and, for Utf8 columns,
     * offsets) buffer(s) and their alignment padding.
     */
    public static long computeColumnValuesOffset(long[] validityLengthsPerColumn,
                                                 long[] offsetsLengthsPerColumn,
                                                 long[] valuesLengthsPerColumn, int columnIndex) {
        long base = computeColumnValidityOffset(validityLengthsPerColumn, offsetsLengthsPerColumn,
                valuesLengthsPerColumn, columnIndex);
        long afterValidity = alignTo8(base + validityLengthsPerColumn[columnIndex]);
        if (offsetsLengthsPerColumn[columnIndex] > 0) {
            return alignTo8(afterValidity + offsetsLengthsPerColumn[columnIndex]);
        }
        return afterValidity;
    }

    /**
     * Writes a multi-column RecordBatch message. The caller composes the
     * body as per-column validity + (optional) offsets + values buffer
     * groups, each 8-byte padded (see {@link #computeColumnValidityOffset},
     * {@link #computeColumnOffsetsOffset}, {@link #computeColumnValuesOffset});
     * this method emits only the Flatbuffers metadata. A column with
     * {@code offsetsLengthsPerColumn[i] > 0} is treated as variable-width
     * (three Buffer descriptors emitted); zero means fixed-width (two
     * descriptors). Returns the byte length of the emitted message, or
     * {@code -1} on FlatBuffer scratch overflow.
     *
     * @param writer                    destination FlatBuffer writer
     * @param rowCount                  rows in the batch
     * @param columnTypes               per-column QuestDB type codes (validated for caller convenience)
     * @param nullCountsPerColumn       per-column Arrow {@code null_count}
     * @param validityLengthsPerColumn  per-column validity buffer length in bytes (0 when all valid)
     * @param offsetsLengthsPerColumn   per-column offsets buffer length in bytes (0 for fixed-width)
     * @param valuesLengthsPerColumn    per-column values buffer length in bytes
     * @param totalBodyBytes            the aligned size of the body; must equal
     *                                  {@link #computeBodyBytes(long[], long[], long[])}
     */
    public static int writeRecordBatchMessage(FbWriter writer, long rowCount,
                                              int[] columnTypes,
                                              long[] nullCountsPerColumn,
                                              long[] validityLengthsPerColumn,
                                              long[] offsetsLengthsPerColumn,
                                              long[] valuesLengthsPerColumn,
                                              long totalBodyBytes) {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        if (columnTypes == null || columnTypes.length == 0) {
            throw new IllegalArgumentException("columnTypes must be non-empty");
        }
        if (nullCountsPerColumn == null) {
            throw new IllegalArgumentException("per-column arrays must be non-null");
        }
        validateLengths(validityLengthsPerColumn, offsetsLengthsPerColumn, valuesLengthsPerColumn);
        int columnCount = columnTypes.length;
        if (nullCountsPerColumn.length != columnCount
                || validityLengthsPerColumn.length != columnCount
                || offsetsLengthsPerColumn.length != columnCount
                || valuesLengthsPerColumn.length != columnCount) {
            throw new IllegalArgumentException("per-column array lengths must equal columnCount");
        }
        if (totalBodyBytes < 0) {
            throw new IllegalArgumentException("totalBodyBytes must be non-negative");
        }
        try {
            int totalBuffers = 0;
            for (int i = 0; i < columnCount; i++) {
                totalBuffers += offsetsLengthsPerColumn[i] > 0 ? 3 : 2;
            }
            // Buffers vector: 2 or 3 Buffer structs per column, 16 bytes each.
            // Elements emitted last-column-first, within a column values
            // last so column 0 / buffer 0 ends up at the lowest memory
            // address in the finished buffer.
            writer.startVector(16, totalBuffers, 8);
            for (int i = columnCount - 1; i >= 0; i--) {
                long validityOffset = computeColumnValidityOffset(validityLengthsPerColumn,
                        offsetsLengthsPerColumn, valuesLengthsPerColumn, i);
                long valuesOffset = computeColumnValuesOffset(validityLengthsPerColumn,
                        offsetsLengthsPerColumn, valuesLengthsPerColumn, i);
                // Buffer N-1 for column i: values.
                writer.prependInt64(valuesLengthsPerColumn[i]);
                writer.prependInt64(valuesOffset);
                if (offsetsLengthsPerColumn[i] > 0) {
                    long offsetsOffset = computeColumnOffsetsOffset(validityLengthsPerColumn,
                            offsetsLengthsPerColumn, valuesLengthsPerColumn, i);
                    // Buffer 1 for column i: offsets (Utf8 only).
                    writer.prependInt64(offsetsLengthsPerColumn[i]);
                    writer.prependInt64(offsetsOffset);
                }
                // Buffer 0 for column i: validity.
                writer.prependInt64(validityLengthsPerColumn[i]);
                writer.prependInt64(validityOffset);
            }
            int buffersVector = writer.endVector(totalBuffers);

            // FieldNode vector: one struct per column, 16 bytes each.
            writer.startVector(16, columnCount, 8);
            for (int i = columnCount - 1; i >= 0; i--) {
                writer.prependInt64(nullCountsPerColumn[i]);
                writer.prependInt64(rowCount);
            }
            int nodesVector = writer.endVector(columnCount);

            // RecordBatch table: length(0), nodes(1), buffers(2).
            writer.startTable(3);
            writer.prependUoffset(buffersVector);
            writer.slot(2, writer.cursorFromEnd());
            writer.prependUoffset(nodesVector);
            writer.slot(1, writer.cursorFromEnd());
            writer.prependInt64(rowCount);
            writer.slot(0, writer.cursorFromEnd());
            int recordBatchTable = writer.endTable();

            // Message table.
            writer.startTable(4);
            writer.prependInt64(totalBodyBytes);
            writer.slot(3, writer.cursorFromEnd());
            writer.prependUoffset(recordBatchTable);
            writer.slot(2, writer.cursorFromEnd());
            writer.prependUint8(MESSAGE_HEADER_RECORD_BATCH);
            writer.slot(1, writer.cursorFromEnd());
            writer.prependInt16(METADATA_VERSION_V5);
            writer.slot(0, writer.cursorFromEnd());
            int messageTable = writer.endTable();

            writer.finish(messageTable);
            return writer.finishedLen();
        } catch (IllegalStateException overflow) {
            return -1;
        }
    }

    private static void validateLengths(long[] validityLengthsPerColumn,
                                        long[] offsetsLengthsPerColumn,
                                        long[] valuesLengthsPerColumn) {
        if (validityLengthsPerColumn == null || offsetsLengthsPerColumn == null
                || valuesLengthsPerColumn == null) {
            throw new IllegalArgumentException("lengths arrays must be non-null");
        }
        int n = valuesLengthsPerColumn.length;
        if (validityLengthsPerColumn.length != n || offsetsLengthsPerColumn.length != n) {
            throw new IllegalArgumentException("lengths arrays must have equal length");
        }
    }
}
