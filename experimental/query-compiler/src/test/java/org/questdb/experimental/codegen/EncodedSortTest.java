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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.griffin.engine.orderby.EncodedTopKBuffer;
import io.questdb.griffin.engine.orderby.SortKeyType;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class EncodedSortTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    public EncodedSortTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() { return List.of(
            new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"c", new NativeBackend()}, new Object[]{"asmjit", new AsmJitBackend()}); }

    @Test public void normalizedVariableKeysMatchStockAcrossDistributions() throws Throwable {
        var config = new DefaultCairoConfiguration(temporary.getRoot().getAbsolutePath());
        var fallback = new Ir.Builder();
        QuestPdqSort.heapSort(fallback, Ir.COLUMNS, Ir.i64(0), Ir.ROWS, false);
        fallback.exit(0);
        try (var code = backend.compile(QuestEncodedSort.program(), temporary.getRoot().toPath());
             var heapCode = backend.compile(new Ir.Program(fallback.block()), temporary.getRoot().toPath());
             var stock = new EncodedTopKBuffer(config, false); var candidate = new EncodedTopKBuffer(config, false);
             var arena = Arena.ofConfined(); var memory = KernelMemory.bind(null, () -> { })) {
            long state = arena.allocate(QuestEncodedSort.INPUT_BYTES, 8).address();
            for (int count : new int[]{0, 1, 2, 23, 24, 25, 127, 128, 129, 511, 4097}) {
                for (String pattern : List.of("random", "ascending", "descending", "organ-pipe", "equal-prefix", "equal-key", "sawtooth", "runs")) {
                    stock.of(SortKeyType.VARIABLE, true, 0); candidate.of(SortKeyType.VARIABLE, true, 0);
                    var random = new Random(72571);
                    for (int i = 0; i < count; i++) {
                        int length = pattern.equals("random") ? 1 + random.nextInt(273) : pattern.startsWith("equal") ? 97 : 16;
                        byte[] key = new byte[length];
                        random.nextBytes(key);
                        if (!pattern.equals("random")) {
                            java.util.Arrays.fill(key, (byte) 0x55);
                            long value = switch (pattern) {
                                case "ascending" -> i;
                                case "descending" -> count - i;
                                case "organ-pipe" -> Math.min(i, count - i);
                                case "equal-key" -> 1;
                                case "equal-prefix" -> random.nextInt(71);
                                case "sawtooth" -> i % 31;
                                default -> i / 137 * 137L + (i / 137 % 2 == 0 ? i % 137 : 136 - i % 137);
                            };
                            int offset = pattern.startsWith("equal") ? 89 : 0;
                            for (int j = 0; j < 8; j++) key[offset + j] = (byte) (value >>> ((7 - j) * 8));
                        }
                        long rowId = (i % 3 == 0 ? Long.MIN_VALUE : 0) + i;
                        append(stock, key, rowId); append(candidate, key, rowId);
                    }
                    stock.sort();
                    int heapBytes = Math.toIntExact(candidate.getKeyHeap().getAppendOffset());
                    int capacity = Math.min(QuestEncodedSort.MAX_RADIX_PENDING, Math.max(1, count / 2));
                    int workspaceBytes = count < 128 ? Math.max(1, 64 - Long.numberOfLeadingZeros(count)) * 32
                            : QuestEncodedSort.RADIX_STACK_OFFSET + capacity * 24;
                    for (boolean atEnd : new boolean[]{false, true}) try (
                            var entries = new ScanStageTest.GuardedPage(count * 40, atEnd);
                            var heap = new ScanStageTest.GuardedPage(heapBytes, atEnd);
                            var workspace = new ScanStageTest.GuardedPage(workspaceBytes, atEnd)) {
                        if (count > 0) Vect.memcpy(entries.address, candidate.getAddress(), count * 40L);
                        if (heapBytes > 0) Vect.memcpy(heap.address, candidate.getKeyHeap().addressOf(0), heapBytes);
                        Unsafe.putLong(state + QuestEncodedSort.HEAP_BASE, heapBytes == 0 ? 0 : heap.address);
                        Unsafe.putLong(state + QuestEncodedSort.UNSTABLE_LIMIT, count < 2 ? 0 : count / (63 - Long.numberOfLeadingZeros(count)));
                        Unsafe.putLong(state + QuestEncodedSort.STACK_CAPACITY, capacity);
                        Unsafe.putLong(state + QuestEncodedSort.WORKSPACE, workspace.address);
                        long limit = atEnd ? count / 2 : count + 7;
                        Unsafe.putLong(state + QuestEncodedSort.LIMIT, limit);
                        assertEquals(pattern + " count=" + count, 0, code.run(entries.address, count, state));
                        memory.throwIfFailed();
                        assertEquals(Math.min(count, limit), Unsafe.getLong(state + QuestEncodedSort.SELECTED));
                        assertSortedEntries(stock, entries.address, count, pattern);
                        // Exercise the worst-case fallback directly, without depending on one pivot-killer distribution.
                        if (count > 1) {
                            for (int i = 0; i < count / 2; i++) for (int offset = 0; offset < 40; offset += 8) {
                                long left = entries.address + i * 40L + offset, right = entries.address + (count - 1 - i) * 40L + offset;
                                long value = Unsafe.getLong(left); Unsafe.putLong(left, Unsafe.getLong(right)); Unsafe.putLong(right, value);
                            }
                            assertEquals(0, heapCode.run(entries.address, count, 0));
                            memory.throwIfFailed();
                            assertSortedEntries(stock, entries.address, count, pattern);
                        }
                    }
                }
            }
        }
    }

    @Test public void sortWorkspaceHonorsBudgetAndRecoversAfterCancellation() throws Exception {
        long jitMemory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var config = new DefaultCairoConfiguration(temporary.getRoot().getAbsolutePath());
        var metadata = new GenericRecordMetadata(); metadata.add(new TableColumnMetadata("s", ColumnType.VARCHAR));
        var order = new ListColumnFilter(); order.add(1);
        try (var tracker = new MemoryTracker() {
            @Override public long getQueryId() { return -1; }
            @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
            @Override public void close() { destroyNativeBlock(); }
        }; var code = (ResolvedTopK) ResolvedTopK.compile(metadata, order, backend, temporary.getRoot().toPath())) {
            try (var buffer = new EncodedTopKBuffer(config, false)) {
                buffer.setMemoryTracker(tracker);
                buffer.of(SortKeyType.VARIABLE, true, 0);
                fillDescending(buffer);
                long entryBytes = tracker.getUsed();
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, entryBytes);
                var failure = assertThrows(CairoException.class, () -> code.sort(buffer, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                assertTrue(failure.isOutOfMemory());
                assertEquals(entryBytes, tracker.getUsed());
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                var polls = new java.util.concurrent.atomic.AtomicInteger();
                try (var memory = KernelMemory.bind(null, () -> {
                    if (polls.incrementAndGet() == 2) throw CairoException.nonCritical().put("Injected sort cancellation");
                })) {
                    failure = assertThrows(CairoException.class, () -> buffer.sort(code));
                    assertTrue(failure.getFlyweightMessage().toString().contains("sort cancellation"));
                }
                buffer.close();
                assertEquals(0, tracker.getUsed());
                buffer.reopen(); buffer.of(SortKeyType.VARIABLE, true, 0);
                fillDescending(buffer);
                code.sort(buffer, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                assertEquals(257, code.sortedRows());
                for (int i = 0; i < 257; i++) assertEquals(256 - i, Unsafe.getLong(buffer.getAddress() + i * 40L + 32));
                assertTrue(tracker.getUsed() > entryBytes);
            }
            assertEquals(0, tracker.getUsed());
        }
        assertEquals(jitMemory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    private static void fillDescending(EncodedTopKBuffer buffer) {
        for (int i = 0; i < 257; i++) {
            byte[] key = new byte[33];
            for (int j = 0; j < 8; j++) key[j] = (byte) ((257L - i) >>> ((7 - j) * 8));
            append(buffer, key, i);
        }
    }

    private static void assertSortedEntries(EncodedTopKBuffer stock, long address, int count, String pattern) {
        for (long i = 0; i < count; i++) {
            long expected = stock.getAddress() + i * 40, actual = address + i * 40;
            for (int offset : new int[]{0, 8, 16, 32}) assertEquals(pattern + " count=" + count + " at=" + i + "+" + offset,
                    Unsafe.getLong(expected + offset), Unsafe.getLong(actual + offset));
            long length = Unsafe.getLong(actual + 16);
            if (length > 16 && count > 1) {
                long a = Unsafe.getLong(expected + 24), c = Unsafe.getLong(actual + 24);
                for (int j = 0; j < length; j++) assertEquals(Unsafe.getByte(a + j), Unsafe.getByte(c + j));
            }
        }
    }

    private static void append(EncodedTopKBuffer buffer, byte[] key, long rowId) {
        long entry = buffer.beginAppend(), k1 = 0, k2 = 0;
        for (int i = 0; i < 16; i++) {
            int value = i < key.length ? key[i] & 255 : 0;
            if (i < 8) k1 = k1 << 8 | value; else k2 = k2 << 8 | value;
        }
        long offset = buffer.getKeyHeap().getAppendOffset();
        if (key.length > 16) {
            long dest = buffer.getKeyHeap().appendAddressFor(key.length);
            for (int i = 0; i < key.length; i++) Unsafe.putByte(dest + i, key[i]);
        }
        Unsafe.putLong(entry, k1); Unsafe.putLong(entry + 8, k2); Unsafe.putLong(entry + 16, key.length);
        Unsafe.putLong(entry + 24, offset); Unsafe.putLong(entry + 32, rowId);
        buffer.endAppend();
    }
}
