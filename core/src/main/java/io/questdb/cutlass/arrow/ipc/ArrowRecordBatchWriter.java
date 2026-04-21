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

/**
 * Emits an Arrow IPC {@code Message} wrapping a {@code RecordBatch}
 * with a single Int64 column. Buffer body bytes are handed to the
 * caller separately — the {@code Message} here is only the metadata
 * Flatbuffers payload.
 * <p>
 * {@code FieldNode} and {@code Buffer} are both structs (fixed-size
 * inline elements) in Arrow's Flatbuffers schema, not tables:
 * <ul>
 *   <li>{@code FieldNode}: {@code int64 length}, {@code int64 null_count} — 16 bytes.</li>
 *   <li>{@code Buffer}: {@code int64 offset}, {@code int64 length} — 16 bytes.</li>
 * </ul>
 * Wave 6a emits two buffers per column in the Int64 layout:
 * <ul>
 *   <li>Buffer 0 — validity bitmap. Length zero is accepted by Arrow
 *       readers when {@code null_count} is zero ("all valid").</li>
 *   <li>Buffer 1 — values. Dense little-endian int64 array.</li>
 * </ul>
 * Table field indices used here (from {@code Message.fbs}):
 * <ul>
 *   <li>{@code RecordBatch}: {@code length: int64 = 0},
 *       {@code nodes: [FieldNode] = 1}, {@code buffers: [Buffer] = 2},
 *       (rest skipped).</li>
 *   <li>{@code Message}: same as the schema message writer.</li>
 * </ul>
 * Union discriminator value: {@code MessageHeader.RecordBatch = 3}.
 */
public final class ArrowRecordBatchWriter {

    private static final short METADATA_VERSION_V5 = 4;
    private static final int MESSAGE_HEADER_RECORD_BATCH = 3;

    private ArrowRecordBatchWriter() {
    }

    /**
     * Writes a RecordBatch message for a single Int64 column of
     * {@code rowCount} rows. {@code valuesBodyOffset} and
     * {@code valuesBodyLen} describe the values buffer's location
     * within the body payload (body is separate from the metadata
     * message). Buffer 0 (validity) is emitted as zero-length with
     * {@code null_count = 0}.
     */
    public static int writeInt64RecordBatchMessage(FbWriter writer,
                                                   long rowCount,
                                                   long valuesBodyOffset,
                                                   long valuesBodyLen) {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        // 1) Buffers vector (2 structs, 16 bytes each, 8-aligned).
        // Flatbuffers writes vectors of structs inline; each struct is
        // prepended with the last field written first.
        writer.startVector(16, 2, 8);
        // buffer 1: values
        writer.prependInt64(valuesBodyLen);   // length
        writer.prependInt64(valuesBodyOffset); // offset
        // buffer 0: validity (empty; null_count=0 means "all valid")
        writer.prependInt64(0L); // length = 0
        writer.prependInt64(0L); // offset = 0
        int buffersVector = writer.endVector(2);

        // 2) Field nodes vector (1 struct, 16 bytes, 8-aligned).
        writer.startVector(16, 1, 8);
        writer.prependInt64(0L);        // null_count
        writer.prependInt64(rowCount);  // length
        int nodesVector = writer.endVector(1);

        // 3) RecordBatch table
        writer.startTable(3);
        writer.prependUoffset(buffersVector);
        writer.slot(2, writer.cursorFromEnd());
        writer.prependUoffset(nodesVector);
        writer.slot(1, writer.cursorFromEnd());
        writer.prependInt64(rowCount);
        writer.slot(0, writer.cursorFromEnd());
        int recordBatchTable = writer.endTable();

        // 4) Message table
        writer.startTable(4);
        writer.prependInt64(valuesBodyLen); // bodyLength = values length (buffer 0 is empty)
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
    }
}
