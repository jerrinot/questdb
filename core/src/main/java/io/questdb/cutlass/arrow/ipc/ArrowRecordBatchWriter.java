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
 * one or more scalar columns. The body bytes (per-column validity +
 * values buffers, each 8-byte aligned) are composed by the caller; this
 * writer emits only the metadata Flatbuffers payload.
 * <p>
 * Each column contributes two Arrow {@code Buffer} descriptors:
 * <ul>
 *   <li>Buffer 0 &mdash; validity bitmap. When the column has no nulls
 *       in this batch the caller passes
 *       {@code validityLengthsPerColumn[i] == 0} and the writer emits
 *       {@code Buffer{offset, length=0}}. Arrow readers treat an empty
 *       validity buffer combined with {@code null_count=0} as "all
 *       valid".</li>
 *   <li>Buffer 1 &mdash; values. Dense little-endian packed values for
 *       fixed-width types; bit-packed for BOOLEAN. The per-column body
 *       offsets include 8-byte padding between consecutive buffers.</li>
 * </ul>
 * {@code FieldNode} and {@code Buffer} are both structs in the Arrow
 * Flatbuffers schema (fixed 16-byte inline elements), not tables.
 * <p>
 * Column types outside the Wave 7a supported set (LONG, DOUBLE, INT,
 * FLOAT, BYTE, SHORT, BOOLEAN) trigger
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
     * Byte width of the values buffer row for the given Wave 7a column
     * type. BOOLEAN returns 0 since the caller uses bit-packed layout
     * that does not admit a whole-byte-per-row product (callers size
     * BOOLEAN values buffers via {@code (rowCount + 7) / 8}).
     */
    public static int bytesPerRowOf(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
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
     * Computes the total aligned body size given per-column validity and
     * values buffer lengths. Each buffer starts at an 8-byte-aligned
     * offset. The arrays must have length equal to the column count.
     */
    public static long computeBodyBytes(long[] validityLengthsPerColumn,
                                        long[] valuesLengthsPerColumn) {
        if (validityLengthsPerColumn == null || valuesLengthsPerColumn == null) {
            throw new IllegalArgumentException("lengths arrays must be non-null");
        }
        if (validityLengthsPerColumn.length != valuesLengthsPerColumn.length) {
            throw new IllegalArgumentException("lengths arrays must have equal length");
        }
        long total = 0;
        for (int i = 0, n = valuesLengthsPerColumn.length; i < n; i++) {
            total = alignTo8(total + validityLengthsPerColumn[i]);
            total = alignTo8(total + valuesLengthsPerColumn[i]);
        }
        return total;
    }

    /**
     * Computes the body offset of column {@code columnIndex}'s validity
     * buffer. Offsets are 8-byte aligned; column 0 starts at offset 0.
     */
    public static long computeColumnValidityOffset(long[] validityLengthsPerColumn,
                                                   long[] valuesLengthsPerColumn, int columnIndex) {
        long offset = 0;
        for (int i = 0; i < columnIndex; i++) {
            offset = alignTo8(offset + validityLengthsPerColumn[i]);
            offset = alignTo8(offset + valuesLengthsPerColumn[i]);
        }
        return offset;
    }

    /**
     * Computes the body offset of column {@code columnIndex}'s values
     * buffer. Starts after the column's validity buffer and its
     * alignment padding.
     */
    public static long computeColumnValuesOffset(long[] validityLengthsPerColumn,
                                                 long[] valuesLengthsPerColumn, int columnIndex) {
        long base = computeColumnValidityOffset(validityLengthsPerColumn, valuesLengthsPerColumn, columnIndex);
        return alignTo8(base + validityLengthsPerColumn[columnIndex]);
    }

    /**
     * Writes a multi-column RecordBatch message. The caller composes the
     * body as per-column validity + values buffer pairs, each 8-byte
     * padded (see {@link #computeColumnValidityOffset} /
     * {@link #computeColumnValuesOffset}); this method emits only the
     * Flatbuffers metadata. Returns the byte length of the emitted
     * message, or {@code -1} on FlatBuffer scratch overflow.
     *
     * @param writer                    destination FlatBuffer writer
     * @param rowCount                  rows in the batch
     * @param columnTypes               per-column QuestDB type codes (validated for caller convenience)
     * @param nullCountsPerColumn       per-column Arrow {@code null_count}
     * @param validityLengthsPerColumn  per-column validity buffer length in bytes (0 when all valid)
     * @param valuesLengthsPerColumn    per-column values buffer length in bytes
     * @param totalBodyBytes            the aligned size of the body; must equal
     *                                  {@link #computeBodyBytes(long[], long[])}
     */
    public static int writeRecordBatchMessage(FbWriter writer, long rowCount,
                                              int[] columnTypes,
                                              long[] nullCountsPerColumn,
                                              long[] validityLengthsPerColumn,
                                              long[] valuesLengthsPerColumn,
                                              long totalBodyBytes) {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        if (columnTypes == null || columnTypes.length == 0) {
            throw new IllegalArgumentException("columnTypes must be non-empty");
        }
        if (nullCountsPerColumn == null || validityLengthsPerColumn == null || valuesLengthsPerColumn == null) {
            throw new IllegalArgumentException("per-column arrays must be non-null");
        }
        int columnCount = columnTypes.length;
        if (nullCountsPerColumn.length != columnCount
                || validityLengthsPerColumn.length != columnCount
                || valuesLengthsPerColumn.length != columnCount) {
            throw new IllegalArgumentException("per-column array lengths must equal columnCount");
        }
        if (totalBodyBytes < 0) {
            throw new IllegalArgumentException("totalBodyBytes must be non-negative");
        }
        try {
            // Buffers vector: 2 Buffer structs per column, 16 bytes each.
            // Elements emitted last-column-first so that column 0 ends up
            // at the lowest memory address in the finished buffer.
            writer.startVector(16, 2 * columnCount, 8);
            for (int i = columnCount - 1; i >= 0; i--) {
                long validityOffset = computeColumnValidityOffset(validityLengthsPerColumn, valuesLengthsPerColumn, i);
                long valuesOffset = computeColumnValuesOffset(validityLengthsPerColumn, valuesLengthsPerColumn, i);
                // Buffer 1 for column i: values.
                writer.prependInt64(valuesLengthsPerColumn[i]);
                writer.prependInt64(valuesOffset);
                // Buffer 0 for column i: validity.
                writer.prependInt64(validityLengthsPerColumn[i]);
                writer.prependInt64(validityOffset);
            }
            int buffersVector = writer.endVector(2 * columnCount);

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
}
