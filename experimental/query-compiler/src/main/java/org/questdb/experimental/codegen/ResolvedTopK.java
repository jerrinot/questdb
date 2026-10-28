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

package org.questdb.experimental.codegen;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.orderby.CompiledTopK;
import io.questdb.griffin.engine.orderby.EncodedTopKBuffer;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

/** Shared code; each call borrows one acquired slot's buffer and pinned frame. */
final class ResolvedTopK implements CompiledTopK {
    private static final long BYTES = QuestEncodedTopK.INPUT_BYTES + VIEW_BYTES;
    private final CompiledKernel code;
    private final CompiledKernel compactCode;
    private final CompiledKernel mergeCode;
    private final CompiledKernel sortCode;
    private final int column;
    private final String name;
    private final LongAdder nativeRows = new LongAdder();
    private final LongAdder mergedRows = new LongAdder();
    private final LongAdder compactions = new LongAdder();
    private final LongAdder sortedRows = new LongAdder();

    private ResolvedTopK(CompiledKernel code, int column, Backend backend, Path artifacts) {
        this.code = code; this.column = column; this.name = backend.name();
        compactCode = CompiledKernel.defer(backend, QuestEncodedTopK::compact, artifacts);
        mergeCode = CompiledKernel.defer(backend, QuestEncodedTopK::merge, artifacts);
        sortCode = CompiledKernel.defer(backend, QuestEncodedSort::program, artifacts);
    }

    static CompiledTopK compile(ColumnTypes inputs, ListColumnFilter order, Backend backend, Path artifacts) throws IOException {
        if (order.size() != 1) return null;
        int key = order.getQuick(0), column = Math.abs(key) - 1;
        if (column < 0 || column >= inputs.getColumnCount() || inputs.getColumnType(column) != ColumnType.VARCHAR) return null;
        var code = backend.compile(QuestVarcharTopK.program(key < 0), artifacts);
        try { return new ResolvedTopK(code, column, backend, artifacts); }
        catch (Throwable failure) { code.close(); throw failure; }
    }

    @Override public boolean encodeFrame(PageFrameMemoryRecord record, int frameIndex, DirectLongList rows, long frameRows,
                                          EncodedTopKBuffer buffer, SqlExecutionCircuitBreaker breaker) {
        long count = rows == null ? frameRows : rows.size();
        if (count == 0) return true;
        long scratch = Unsafe.malloc(BYTES, MemoryTag.NATIVE_JIT);
        long view = scratch + QuestEncodedTopK.INPUT_BYTES;
        try (var memory = KernelMemory.bind(null, breaker::statefulThrowExceptionIfTrippedTimeThrottled)) {
            if (!record.writeNativeVarcharView(column, scratch) || !buffer.writeCompiledView(view)) return false;
            Unsafe.putLong(scratch + QuestVarcharTopK.SELECTION, rows == null ? 0 : rows.getAddress());
            Unsafe.putLong(scratch + QuestVarcharTopK.ROW_BASE, Rows.toRowID(frameIndex, 0));
            Unsafe.putLong(scratch + QuestEncodedTopK.NEXT, 0);
            append(code, scratch, count, view, buffer, memory);
            nativeRows.add(count);
            return true;
        } finally { Unsafe.free(scratch, BYTES, MemoryTag.NATIVE_JIT); }
    }

    @Override public void merge(EncodedTopKBuffer source, EncodedTopKBuffer target, SqlExecutionCircuitBreaker breaker) {
        long count = source.getCount();
        if (count == 0) return;
        long scratch = Unsafe.malloc(BYTES, MemoryTag.NATIVE_JIT);
        long view = scratch + QuestEncodedTopK.INPUT_BYTES;
        try (var memory = KernelMemory.bind(null, breaker::statefulThrowExceptionIfTrippedTimeThrottled)) {
            if (!target.writeCompiledView(view)) throw new IllegalArgumentException("Expected variable Top-K entries");
            Unsafe.putLong(scratch + QuestEncodedTopK.SOURCE, source.getAddress());
            Unsafe.putLong(scratch + QuestEncodedTopK.SOURCE_HEAP, source.getKeyHeap().getAppendOffset() == 0 ? 0 : source.getKeyHeap().addressOf(0));
            Unsafe.putLong(scratch + QuestEncodedTopK.NEXT, 0);
            append(mergeCode, scratch, count, view, target, memory);
            mergedRows.add(count);
        } finally { Unsafe.free(scratch, BYTES, MemoryTag.NATIVE_JIT); }
    }

    @Override public void compact(long entries, long count, long limit, boolean first, MemoryCARW heap, MemoryCARW packed) {
        long scratch = Unsafe.malloc(BYTES, MemoryTag.NATIVE_JIT);
        long view = scratch + QuestEncodedTopK.INPUT_BYTES;
        try {
            Unsafe.putLong(scratch + QuestEncodedTopK.SOURCE, entries + (first ? 0 : count - limit) * 40);
            Unsafe.putLong(scratch + QuestEncodedTopK.SOURCE_HEAP, heap.getAppendOffset() == 0 ? 0 : heap.addressOf(0));
            Unsafe.putLong(scratch + QuestEncodedTopK.NEXT, 0);
            Unsafe.putLong(view + ENTRIES, entries);
            Unsafe.putLong(view + HEAP_POS, 0);
            for (;;) {
                // Scratch growth can move it; all generated entries retain offsets, never scratch pointers.
                Unsafe.putLong(view + HEAP, packed.size() == 0 ? 0 : packed.addressOf(0));
                Unsafe.putLong(view + HEAP_SIZE, packed.size());
                long status = compactCode.run(scratch, limit, view);
                KernelMemory.checkFailure();
                if (status == QuestEncodedTopK.DONE) break;
                if (status != QuestEncodedTopK.HEAP_SPACE) throw new IllegalStateException("Invalid compiled compaction status: " + status);
                packed.extend(Unsafe.getLong(scratch + QuestEncodedTopK.NEED));
            }
            long bytes = Unsafe.getLong(view + HEAP_POS);
            packed.jumpTo(bytes);
            heap.jumpTo(bytes);
            compactions.increment();
        } finally { Unsafe.free(scratch, BYTES, MemoryTag.NATIVE_JIT); }
    }

    @Override public long sort(EncodedTopKBuffer buffer, SqlExecutionCircuitBreaker breaker) {
        try (var memory = KernelMemory.bind(null, breaker::statefulThrowExceptionIfTrippedTimeThrottled)) {
            long selected = buffer.sort(this);
            memory.throwIfFailed();
            return selected;
        }
    }

    @Override public long sortEntries(long entries, long count, long heap, long limit, DirectLongList scratch) {
        // Pending radix ranges are disjoint and contain >=2 entries. Across 16 prefix bytes,
        // depth-first traversal retains at most 16*255+1 ranges, regardless of table size.
        long capacity = Math.min(QuestEncodedSort.MAX_RADIX_PENDING, Math.max(1, count / 2));
        long workBytes = count < 128 ? (64 - Long.numberOfLeadingZeros(count)) * 32L
                : QuestEncodedSort.RADIX_STACK_OFFSET + capacity * 24;
        long bytes = QuestEncodedSort.INPUT_BYTES + workBytes;
        if (scratch.getCapacity() < bytes / Long.BYTES) scratch.setCapacity(bytes / Long.BYTES);
        long input = scratch.getAddress();
        Unsafe.putLong(input + QuestEncodedSort.HEAP_BASE, heap);
        Unsafe.putLong(input + QuestEncodedSort.UNSTABLE_LIMIT, count < 2 ? 0 : count / (63 - Long.numberOfLeadingZeros(count)));
        Unsafe.putLong(input + QuestEncodedSort.STACK_CAPACITY, capacity);
        Unsafe.putLong(input + QuestEncodedSort.WORKSPACE, input + QuestEncodedSort.INPUT_BYTES);
        Unsafe.putLong(input + QuestEncodedSort.LIMIT, limit);
        long status = sortCode.run(entries, count, input);
        KernelMemory.checkFailure();
        if (status != 0) throw CairoException.nonCritical().put("Invalid compiled Top-K sort [status=").put(status).put(']');
        sortedRows.add(count);
        return Unsafe.getLong(input + QuestEncodedSort.SELECTED);
    }

    private void append(CompiledKernel operation, long input, long count, long view, EncodedTopKBuffer buffer, KernelMemory memory) {
        do {
            long status = operation.run(input, count, view);
            memory.throwIfFailed();
            if (status < 0) {
                if (status == QuestVarcharTopK.INVALID_UTF8) throw CairoException.nonCritical().put("cannot ORDER BY a VARCHAR with invalid UTF-8 (contains a 0xFF byte)");
                throw CairoException.nonCritical().put("Invalid compiled Top-K input [status=").put(status).put(']');
            }
            buffer.commitCompiledView(view, this);
            if (status == QuestEncodedTopK.DONE) break;
            if (status == QuestEncodedTopK.ENTRY_SPACE) buffer.beginAppend();
            else if (status == QuestEncodedTopK.HEAP_SPACE) buffer.getKeyHeap().extend(Unsafe.getLong(input + QuestEncodedTopK.NEED));
            else if (status != QuestEncodedTopK.BOUNDARY) throw new IllegalStateException("Invalid compiled Top-K status: " + status);
            buffer.writeCompiledView(view);
        } while (Unsafe.getLong(input + QuestEncodedTopK.NEXT) < count);
    }

    long mergedRows() { return mergedRows.sum(); }
    long compactions() { return compactions.sum(); }
    long sortedRows() { return sortedRows.sum(); }
    @Override public CharSequence getName() { return name; }
    @Override public long getNativeRows() { return nativeRows.sum(); }
    @Override public void close() {
        try { code.close(); }
        finally { try { mergeCode.close(); } finally { try { compactCode.close(); } finally { sortCode.close(); } } }
    }
}
