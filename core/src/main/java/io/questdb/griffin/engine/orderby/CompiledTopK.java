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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.DirectLongList;
import io.questdb.std.QuietCloseable;

/** Query-owned encoding, admission and buffer operations; the acquired slot owns the borrowed memory. */
public interface CompiledTopK extends QuietCloseable {
    // EncodedTopKBuffer view. Addresses expire on growth/compaction; only COUNT and HEAP_POS are writable.
    int ENTRIES = 0, COUNT = 8, STOP = 16, HEAP = 24, HEAP_POS = 32, HEAP_SIZE = 40;
    int HAS_THRESHOLD = 48, FIRST = 56, K1 = 64, K2 = 72, LENGTH = 80, ROW_ID = 88, TAIL = 96;
    int VIEW_BYTES = 104;

    /** False must leave the buffer unchanged. No record or frame address survives this call. */
    boolean encodeFrame(PageFrameMemoryRecord record, int frameIndex, DirectLongList rows, long frameRows,
                        EncodedTopKBuffer buffer, SqlExecutionCircuitBreaker breaker);

    /**
     * Packs sorted survivors, restoring heap offsets. Invoked inside encodeFrame/merge's execution
     * scope; the caller publishes count and threshold. Failure invalidates the execution's buffers.
     */
    void compact(long entries, long count, long limit, boolean first, MemoryCARW heap, MemoryCARW scratch);

    /** Source entries contain heap offsets and remain unchanged throughout the merge. */
    void merge(EncodedTopKBuffer source, EncodedTopKBuffer target, SqlExecutionCircuitBreaker breaker);

    /** Final sort and prefix selection after all workers join; returns the number to emit. */
    long sort(EncodedTopKBuffer buffer, SqlExecutionCircuitBreaker breaker);

    /**
     * Sorts normalized entries and rebases heap offsets to pointers. Invoked inside a bound
     * execution scope; scratch is buffer-owned and charged to its query memory tracker.
     */
    long sortEntries(long entries, long count, long heap, long limit, DirectLongList scratch);

    CharSequence getName();
    long getNativeRows();
}
