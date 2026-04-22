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
     * Writes a multi-column RecordBatch message. The caller composes the
     * body as per-column validity + (optional) offsets + values buffer
     * groups according to {@link ArrowBatchLayout}; this method emits
     * only the Flatbuffers metadata. A column with
     * {@link ArrowBatchLayout#hasOffsets(int)} is treated as
     * variable-width (three Buffer descriptors emitted); otherwise it is
     * fixed-width (two descriptors). Returns the byte length of the
     * emitted message, or {@code -1} on FlatBuffer scratch overflow.
     *
     * @param writer      destination FlatBuffer writer
     * @param rowCount    rows in the batch
     * @param columnTypes per-column QuestDB type codes
     * @param layout      finished per-column body layout
     */
    public static int writeRecordBatchMessage(FbWriter writer, long rowCount,
                                              int[] columnTypes,
                                              ArrowBatchLayout layout) {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        if (columnTypes == null || columnTypes.length == 0) {
            throw new IllegalArgumentException("columnTypes must be non-empty");
        }
        if (layout == null) {
            throw new IllegalArgumentException("layout must be non-null");
        }
        if (!layout.isFinished()) {
            throw new IllegalArgumentException("layout must be finished");
        }
        int columnCount = columnTypes.length;
        if (layout.columnCount() != columnCount) {
            throw new IllegalArgumentException("layout column count must equal columnCount");
        }
        long totalBodyBytes = layout.bodyBytes();
        try {
            // Buffers vector: 2 or 3 Buffer structs per column, 16 bytes each.
            // Elements emitted last-column-first, within a column values
            // last so column 0 / buffer 0 ends up at the lowest memory
            // address in the finished buffer.
            int totalBuffers = layout.bufferCount();
            writer.startVector(16, totalBuffers, 8);
            for (int i = columnCount - 1; i >= 0; i--) {
                // Buffer N-1 for column i: values.
                writer.prependInt64(layout.valuesLength(i));
                writer.prependInt64(layout.valuesOffset(i));
                if (layout.hasOffsets(i)) {
                    // Buffer 1 for column i: offsets (Utf8 only).
                    writer.prependInt64(layout.offsetsLength(i));
                    writer.prependInt64(layout.offsetsOffset(i));
                }
                // Buffer 0 for column i: validity.
                writer.prependInt64(layout.validityLength(i));
                writer.prependInt64(layout.validityOffset(i));
            }
            int buffersVector = writer.endVector(totalBuffers);

            // FieldNode vector: one struct per column, 16 bytes each.
            writer.startVector(16, columnCount, 8);
            for (int i = columnCount - 1; i >= 0; i--) {
                writer.prependInt64(layout.nullCount(i));
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
