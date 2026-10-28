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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.orderby.EncodedTopKBuffer;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.orderby.SortKeyType;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class EncodedTopKTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    public EncodedTopKTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() { return List.of(
            new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"c", new NativeBackend()}, new Object[]{"asmjit", new AsmJitBackend()}); }

    @Test public void mergeAndCompactionMatchStockBuffers() throws Exception {
        long memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var config = configuration();
        var metadata = metadata();
        for (boolean desc : new boolean[]{false, true}) {
            var order = new ListColumnFilter(); order.add(desc ? -1 : 1);
            try (var code = (ResolvedTopK) ResolvedTopK.compile(metadata, order, backend, temporary.getRoot().toPath());
                 var encoder = new SortKeyEncoder(metadata, order);
                 var source = new EncodedTopKBuffer(new DefaultCairoConfiguration(temporary.getRoot().getAbsolutePath()), false);
                 var stock = new EncodedTopKBuffer(config, false);
                 var candidate = new EncodedTopKBuffer(config, false)) {
                var type = encoder.init(null);
                for (boolean first : new boolean[]{true, false}) for (int limit : new int[]{10, 2500}) {
                    stock.of(type, first, limit);
                    candidate.of(type, first, limit);
                    for (int batch = 0; batch < 3; batch++) {
                        source.of(type, first, 0);
                        fill(encoder, source, batch * 7003, 7003);
                        byte[] inputEntries = entries(source), inputHeap = heap(source);
                        stock.mergeFrom(source);
                        code.merge(source, candidate, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                        assertArrayEquals(inputEntries, entries(source));
                        assertArrayEquals(inputHeap, heap(source));
                        assertArrayEquals("Unsorted entries and restored heap offsets", entries(stock), entries(candidate));
                        assertArrayEquals("Packed key bytes", heap(stock), heap(candidate));
                    }
                    stock.sort(); code.sort(candidate, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                    assertEquals(stock.getCount(), candidate.getCount());
                    for (long i = 0; i < stock.getCount(); i++) assertEquals(
                            Unsafe.getLong(stock.getAddress() + i * 40 + 32), Unsafe.getLong(candidate.getAddress() + i * 40 + 32));
                }
                assertTrue(code.compactions() > 0);
                assertEquals(3 * 7003 * 4, code.mergedRows());
            }
        }
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void deferredCompactionFailureAllowsReopen() throws Exception {
        long memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var created = new AtomicInteger();
        var closed = new AtomicInteger();
        var fail = new AtomicBoolean(true);
        Backend failing = new Backend() {
            @Override public String name() { return backend.name(); }
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path path) throws java.io.IOException {
                var kernel = backend.compile(program, path);
                int phase = created.incrementAndGet(); // deterministic here: build, merge, sort, compaction
                return new CompiledKernel((columns, rows, state) -> {
                    long status = kernel.run(columns, rows, state);
                    if (phase == 4 && status == QuestEncodedTopK.HEAP_SPACE
                            && Unsafe.getLong(columns + QuestEncodedTopK.NEXT) > 0 && fail.getAndSet(false)) {
                        throw CairoException.nonCritical().put("Injected failure after partial compaction");
                    }
                    return status;
                }, () -> { kernel.close(); closed.incrementAndGet(); }, kernel.artifacts(), kernel.compilationNanos());
            }
        };
        var config = configuration();
        var metadata = metadata();
        var order = new ListColumnFilter(); order.add(1);
        try (var code = (ResolvedTopK) ResolvedTopK.compile(metadata, order, failing, temporary.getRoot().toPath());
             var encoder = new SortKeyEncoder(metadata, order);
             var source = new EncodedTopKBuffer(new DefaultCairoConfiguration(temporary.getRoot().getAbsolutePath()), false);
             var target = new EncodedTopKBuffer(config, false);
             var stock = new EncodedTopKBuffer(config, false)) {
            var type = encoder.init(null);
            source.of(type, true, 0);
            target.of(type, false, 2500);
            stock.of(type, false, 2500);
            fill(encoder, source, 0, 7003);
            var failure = assertThrows(CairoException.class, () -> code.merge(source, target, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            assertTrue(failure.getFlyweightMessage().toString().contains("partial compaction"));
            assertFalse(fail.get());
            target.close(); target.reopen(); target.of(type, false, 2500);
            code.merge(source, target, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            stock.mergeFrom(source);
            assertArrayEquals(entries(stock), entries(target));
            assertArrayEquals(heap(stock), heap(target));
            assertTrue(code.compactions() > 0);
        }
        assertEquals(4, created.get());
        assertEquals(created.get(), closed.get());
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    private CairoConfiguration configuration() {
        return new DefaultCairoConfiguration(temporary.getRoot().getAbsolutePath()) {
            @Override public long getSqlSortKeyPageSize() { return 64; }
            // Compact at 4096 even for K=2500: LAST N moves overlapping entry ranges.
            @Override public long getSqlSortLightValueMaxBytes() { return 4096 * 8; }
        };
    }

    private static GenericRecordMetadata metadata() {
        var metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("s", ColumnType.VARCHAR));
        return metadata;
    }

    private static void fill(SortKeyEncoder encoder, EncodedTopKBuffer buffer, int start, int count) {
        encoder.setKeyHeap(buffer.getKeyHeap());
        var record = new Record() {
            Utf8Sequence value;
            @Override public Utf8Sequence getVarcharA(int col) { return value; }
            @Override public int getVarcharSize(int col) { return value == null ? -1 : value.size(); }
        };
        for (int i = start; i < start + count; i++) {
            // Include prefix ties, length ties, multibyte tails and keys spanning many growth boundaries.
            String text = switch (i % 7) {
                case 0 -> null;
                case 1 -> "";
                case 2 -> "abcdefghijklmno".substring(0, i % 16);
                case 3 -> "same-16-byte-prefix-ž-" + (i % 71);
                case 4 -> "same-16-byte-prefix-ž-" + (i % 71) + "-long-".repeat(i % 97);
                default -> "row-" + ((i * 7919) % 10007);
            };
            record.value = text == null ? null : new Utf8String(text);
            encoder.encodeTopK(record, i, buffer);
        }
    }

    private static byte[] entries(EncodedTopKBuffer buffer) { return bytes(buffer.getAddress(), buffer.getCount() * SortKeyType.VARIABLE.entrySize()); }
    private static byte[] heap(EncodedTopKBuffer buffer) {
        long count = buffer.getKeyHeap().getAppendOffset();
        return count == 0 ? new byte[0] : bytes(buffer.getKeyHeap().addressOf(0), count);
    }
    private static byte[] bytes(long address, long count) {
        byte[] result = new byte[Math.toIntExact(count)];
        for (int i = 0; i < result.length; i++) result[i] = Unsafe.getByte(address + i);
        return result;
    }
}
