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
 * one or more scalar columns. The body bytes (per-column values buffers
 * concatenated with 8-byte padding between columns) are composed by the
 * caller; this writer emits only the metadata Flatbuffers payload.
 * <p>
 * For each column of types LONG / DOUBLE / INT Wave 6b emits two Arrow
 * {@code Buffer} structs:
 * <ul>
 *   <li>Buffer 0 — validity bitmap. Length zero is accepted by Arrow
 *       readers when {@code null_count} is zero ("all valid").</li>
 *   <li>Buffer 1 — values. Dense little-endian packed values. The
 *       caller-supplied {@code valueBufferLengths[i]} records the
 *       length of column {@code i}'s values buffer in bytes; the
 *       writer computes the per-column body offsets with 8-byte
 *       alignment padding between consecutive columns.</li>
 * </ul>
 * {@code FieldNode} and {@code Buffer} are both structs in the Arrow
 * Flatbuffers schema (fixed 16-byte inline elements), not tables.
 * <p>
 * Column types other than LONG / DOUBLE / INT trigger
 * {@link UnsupportedColumnTypeException}.
 */
public final class ArrowRecordBatchWriter {

    private static final int BODY_ALIGNMENT = 8;
    private static final short METADATA_VERSION_V5 = 4;
    private static final int MESSAGE_HEADER_RECORD_BATCH = 3;

    private ArrowRecordBatchWriter() {
    }

    /**
     * Rounds {@code offset} up to the next 8-byte boundary. Wave 6b's
     * body alignment: each per-column values buffer starts at an
     * 8-byte-aligned offset within the body, so INT32 columns (4-byte
     * natural alignment) get 4 bytes of tail padding before the next
     * column.
     */
    public static long alignTo8(long offset) {
        return (offset + BODY_ALIGNMENT - 1) & ~(long) (BODY_ALIGNMENT - 1);
    }

    /**
     * Byte width of the values buffer row for the given Wave 6b column
     * type (LONG / DOUBLE: 8 bytes, INT: 4 bytes). Throws
     * {@link UnsupportedColumnTypeException} for anything else.
     */
    public static int bytesPerRowOf(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.LONG:
            case ColumnType.DOUBLE:
                return 8;
            case ColumnType.INT:
                return 4;
            default:
                throw new UnsupportedColumnTypeException(columnType);
        }
    }

    /**
     * Computes the total aligned body size for a batch of {@code rowCount}
     * rows across columns of the given types. Each column's values
     * buffer starts at an 8-byte-aligned offset. Returns the required
     * body byte count.
     */
    public static long computeBodyBytes(int[] columnTypes, long rowCount) {
        if (columnTypes == null) {
            throw new IllegalArgumentException("columnTypes must be non-null");
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        long total = 0;
        for (int i = 0, n = columnTypes.length; i < n; i++) {
            long columnBytes = (long) bytesPerRowOf(columnTypes[i]) * rowCount;
            total = alignTo8(total + columnBytes);
        }
        return total;
    }

    /**
     * Computes the body offset of column {@code columnIndex}'s values
     * buffer given the column type array and the row count. Offsets are
     * 8-byte-aligned; column 0 starts at offset 0.
     */
    public static long computeColumnOffset(int[] columnTypes, long rowCount, int columnIndex) {
        long offset = 0;
        for (int i = 0; i < columnIndex; i++) {
            long columnBytes = (long) bytesPerRowOf(columnTypes[i]) * rowCount;
            offset = alignTo8(offset + columnBytes);
        }
        return offset;
    }

    /**
     * Writes a multi-column RecordBatch message. Caller is responsible
     * for the body bytes (concatenated per-column values, with 8-byte
     * padding between columns computed by
     * {@link #computeColumnOffset(int[], long, int)}); this method only
     * emits the Flatbuffers metadata. Returns the byte length of the
     * emitted message, or {@code -1} on FlatBuffer scratch overflow.
     *
     * @param writer         destination FlatBuffer writer
     * @param rowCount       rows in the batch
     * @param columnTypes    per-column QuestDB type codes (LONG / DOUBLE / INT only)
     * @param totalBodyBytes the aligned size of the body the caller will
     *                       attach to the FlightData message; this is
     *                       the value that lands in the Message's
     *                       {@code bodyLength} slot and must equal
     *                       {@link #computeBodyBytes(int[], long)}
     */
    public static int writeRecordBatchMessage(FbWriter writer, long rowCount,
                                              int[] columnTypes, long totalBodyBytes) {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        if (columnTypes == null || columnTypes.length == 0) {
            throw new IllegalArgumentException("columnTypes must be non-empty");
        }
        if (totalBodyBytes < 0) {
            throw new IllegalArgumentException("totalBodyBytes must be non-negative");
        }
        int columnCount = columnTypes.length;
        try {
            // Buffers vector: 2 Buffer structs per column, 16 bytes each.
            // Elements emitted last-column-first so that column 0 ends up
            // at the lowest memory address in the finished buffer.
            writer.startVector(16, 2 * columnCount, 8);
            for (int i = columnCount - 1; i >= 0; i--) {
                long columnOffset = computeColumnOffset(columnTypes, rowCount, i);
                long columnBytes = (long) bytesPerRowOf(columnTypes[i]) * rowCount;
                // Buffer 1 for column i: values.
                writer.prependInt64(columnBytes);
                writer.prependInt64(columnOffset);
                // Buffer 0 for column i: validity (empty, null_count = 0).
                writer.prependInt64(0L);
                writer.prependInt64(0L);
            }
            int buffersVector = writer.endVector(2 * columnCount);

            // FieldNode vector: one struct per column, 16 bytes each.
            writer.startVector(16, columnCount, 8);
            for (int i = columnCount - 1; i >= 0; i--) {
                writer.prependInt64(0L);       // null_count
                writer.prependInt64(rowCount); // length
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
