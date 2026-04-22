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

import java.util.Arrays;

/**
 * Reusable primitive-array-backed description of a RecordBatch body.
 * Each column stores Arrow FieldNode and Buffer metadata without
 * allocating per-column objects. Callers set per-column buffer lengths,
 * then {@link #finish()} computes all buffer offsets and the total body
 * size in one pass. Length and null-count accessors expose caller-supplied
 * inputs immediately; offset, body-size and buffer-count accessors require
 * a finished layout.
 */
public final class ArrowBatchLayout {

    private static final int FIELD_COUNT = 7;
    private static final int NULL_COUNT = 0;
    private static final int VALIDITY_LENGTH = 1;
    private static final int VALIDITY_OFFSET = 2;
    private static final int OFFSETS_LENGTH = 3;
    private static final int OFFSETS_OFFSET = 4;
    private static final int VALUES_LENGTH = 5;
    private static final int VALUES_OFFSET = 6;

    private long bodyBytes;
    private int bufferCount;
    private int columnCount;
    private boolean finished;
    private long[] layout = new long[0];

    public long bodyBytes() {
        assertFinished();
        return bodyBytes;
    }

    public int bufferCount() {
        assertFinished();
        return bufferCount;
    }

    public int columnCount() {
        return columnCount;
    }

    public void finish() {
        long offset = 0;
        int buffers = 0;
        for (int columnIndex = 0, index = 0; columnIndex < columnCount; columnIndex++, index += FIELD_COUNT) {
            layout[index + VALIDITY_OFFSET] = offset;
            offset = ArrowRecordBatchWriter.alignTo8(offset + layout[index + VALIDITY_LENGTH]);

            layout[index + OFFSETS_OFFSET] = offset;
            if (layout[index + OFFSETS_LENGTH] > 0) {
                offset = ArrowRecordBatchWriter.alignTo8(offset + layout[index + OFFSETS_LENGTH]);
                buffers++;
            }

            layout[index + VALUES_OFFSET] = offset;
            offset = ArrowRecordBatchWriter.alignTo8(offset + layout[index + VALUES_LENGTH]);
            buffers += 2;
        }
        bodyBytes = offset;
        bufferCount = buffers;
        finished = true;
    }

    public boolean hasOffsets(int columnIndex) {
        return offsetsLength(columnIndex) > 0;
    }

    public long nullCount(int columnIndex) {
        return layout[indexOf(columnIndex) + NULL_COUNT];
    }

    public long offsetsLength(int columnIndex) {
        return layout[indexOf(columnIndex) + OFFSETS_LENGTH];
    }

    public long offsetsOffset(int columnIndex) {
        assertFinished();
        return layout[indexOf(columnIndex) + OFFSETS_OFFSET];
    }

    public void reset(int columnCount) {
        if (columnCount < 0) {
            throw new IllegalArgumentException("columnCount must be non-negative");
        }
        int required = columnCount * FIELD_COUNT;
        if (required / FIELD_COUNT != columnCount) {
            throw new IllegalArgumentException("columnCount is too large: " + columnCount);
        }
        if (layout.length < required) {
            layout = new long[required];
        } else {
            Arrays.fill(layout, 0, required, 0L);
        }
        this.columnCount = columnCount;
        this.bodyBytes = 0;
        this.bufferCount = 0;
        this.finished = false;
    }

    public void set(int columnIndex, long nullCount, long validityLength, long offsetsLength, long valuesLength) {
        if (nullCount < 0) {
            throw new IllegalArgumentException("nullCount must be non-negative");
        }
        if (validityLength < 0 || offsetsLength < 0 || valuesLength < 0) {
            throw new IllegalArgumentException("buffer lengths must be non-negative");
        }
        int index = indexOf(columnIndex);
        layout[index + NULL_COUNT] = nullCount;
        layout[index + VALIDITY_LENGTH] = validityLength;
        layout[index + OFFSETS_LENGTH] = offsetsLength;
        layout[index + VALUES_LENGTH] = valuesLength;
        finished = false;
    }

    public long validityLength(int columnIndex) {
        return layout[indexOf(columnIndex) + VALIDITY_LENGTH];
    }

    public long validityOffset(int columnIndex) {
        assertFinished();
        return layout[indexOf(columnIndex) + VALIDITY_OFFSET];
    }

    public long valuesLength(int columnIndex) {
        return layout[indexOf(columnIndex) + VALUES_LENGTH];
    }

    public long valuesOffset(int columnIndex) {
        assertFinished();
        return layout[indexOf(columnIndex) + VALUES_OFFSET];
    }

    boolean isFinished() {
        return finished;
    }

    private void assertFinished() {
        if (!finished) {
            throw new IllegalStateException("layout must be finished");
        }
    }

    private int indexOf(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnCount) {
            throw new IndexOutOfBoundsException("columnIndex out of range: " + columnIndex);
        }
        return columnIndex * FIELD_COUNT;
    }
}
