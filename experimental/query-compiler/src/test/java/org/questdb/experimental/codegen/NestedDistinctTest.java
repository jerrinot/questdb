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
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Pipeline.*;

@RunWith(Parameterized.class)
public class NestedDistinctTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;

    public NestedDistinctTest(String name, Backend backend) { this.backend = backend; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"native", new NativeBackend()}, new Object[]{"asmjit", new AsmJitBackend()});
    }

    @Test
    public void matchesQuestDbSetLayoutAfterEveryRow() throws Exception {
        assertLayoutAfterEveryRow(false);
    }

    @Test
    public void mixedControlMatchesEveryAggregateAndSetAfterEveryRow() throws Exception {
        assertLayoutAfterEveryRow(true);
    }

    private void assertLayoutAfterEveryRow(boolean mixed) throws Exception {
        for (int requested : new int[]{3, 5}) {
            double loadFactor = requested == 3 ? 0.7 : 0.6;
            Pipeline pipeline = pipeline(requested, loadFactor);
            if (mixed) {
                Column adv = new Column(2, "AdvEngineID", ColumnType.SHORT);
                pipeline = new Pipeline(List.of(pipeline.columns().get(0), pipeline.columns().get(1), adv), new All(), pipeline.keys(),
                        List.of(new Sum(adv), new Count(), pipeline.aggregates().getFirst()));
            }
            Kernel control = mixed ? HandwrittenKernels.nestedMixed(requested, loadFactor)
                    : HandwrittenKernels.nestedDistinct(requested, loadFactor);
            try (var generated = backend.compile(pipeline.lower(), temporary.getRoot().toPath());
                 Arena arena = Arena.ofConfined()) {
                int capacity = 16;
                long output = TestTables.create(arena, capacity, pipeline.entryBytes()).address();
                long reference = TestTables.create(arena, capacity, pipeline.entryBytes()).address();
                put(output, capacity);
                put(reference, capacity);
                long columns = arena.allocate(24, 8).address();
                long key = arena.allocate(8, 8).address();
                long value = arena.allocate(8, 8).address();
                long adv = arena.allocate(8, 8).address();
                put(columns, key);
                put(columns + 8, value);
                put(columns + 16, adv);
                long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                var allocator = new FastGroupByAllocator(64, 1 << 20);
                try (var memory = KernelMemory.open(allocator)) {
                    Random random = new Random(817239);
                    long[] special = {Long.MIN_VALUE, Long.MIN_VALUE, 0, 0, -1, Long.MAX_VALUE, Long.MIN_VALUE + 1};
                    for (int row = 0; row < 1200; row++) {
                        int group = row < special.length ? 0 : random.nextInt(9) - 4;
                        Unsafe.getUnsafe().putInt(key, group == -4 ? Integer.MIN_VALUE : group);
                        put(value, row < special.length ? special[row] : random.nextInt(257) - 128);
                        Unsafe.getUnsafe().putShort(adv, (short) (row % 23 - 11));
                        assertEquals(0, generated.run(columns, 1, output));
                        memory.throwIfFailed();
                        assertEquals(0, control.run(columns, 1, reference));
                        assertSameState(output, reference, capacity, pipeline.entryBytes(), mixed ? 32 : 16);
                        if (row < 4) { assertEquals("Nulls and repeated singleton must stay inline", 0, allocator.allocated()); }
                    }
                    // Column tops: null group and null argument, without allocating a set.
                    put(columns, 0);
                    put(columns + 8, 0);
                    put(columns + 16, 0);
                    assertEquals(0, generated.run(columns, 10, output));
                    assertEquals(0, control.run(columns, 10, reference));
                    assertSameState(output, reference, capacity, pipeline.entryBytes(), mixed ? 32 : 16);
                }
                assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            }
        }
    }

    @Test
    public void allocatorFailureReturnsSafelyAndReleasesMemory() throws Exception {
        Pipeline pipeline = pipeline(3, 0.7);
        try (var generated = backend.compile(pipeline.lower(), temporary.getRoot().toPath());
             Arena arena = Arena.ofConfined()) {
            var output = TestTables.create(arena, 1, pipeline.entryBytes());
            long columns = arena.allocate(16, 8).address();
            long keys = arena.allocate(8, 8).address();
            long values = arena.allocate(16, 8).address();
            put(columns, keys);
            put(columns + 8, values);
            put(values, 0);
            put(values + 8, 1);
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            put(output.address(), 1);
            try (var memory = KernelMemory.open(new FastGroupByAllocator(64, 64))) {
                // The first 48-byte set fits; growth to 80 bytes must fail inside the callback.
                assertEquals(2, generated.run(columns, 2, output.address()));
                assertThrows(CairoException.class, memory::throwIfFailed);
            }
            assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            TestTables.clear(output, pipeline.entryBytes());
            put(output.address(), 1);
            try (var memory = KernelMemory.open(new FastGroupByAllocator(64, 1 << 20))) {
                assertEquals(0, generated.run(columns, 2, output.address()));
                memory.throwIfFailed();
                assertEquals(2, get(TableState.entries(output.address()) + 16));
            }
            assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
        }
    }

    private static void assertSameState(long actual, long expected, int capacity, long stride, long countOffset) {
        assertEquals(get(expected + 8), get(actual + 8));
        for (int i = 0; i < capacity; i++) {
            long a = TableState.entries(actual) + i * stride;
            long e = TableState.entries(expected) + i * stride;
            for (int offset = 0; offset < stride; offset += 8) {
                if (offset != countOffset + 8) { assertEquals(get(e + offset), get(a + offset)); }
            }
            long count = get(e + countOffset);
            if (count <= 1) {
                assertEquals(get(e + countOffset + 8), get(a + countOffset + 8));
            } else {
                long setA = get(a + countOffset + 8);
                long setE = get(e + countOffset + 8);
                // Addresses and unused padding differ; capacity, size, limit and every slot must match.
                for (int offset = 0; offset <= 8; offset += 4) {
                    assertEquals(Unsafe.getUnsafe().getInt(setE + offset), Unsafe.getUnsafe().getInt(setA + offset));
                }
                int setCapacity = Unsafe.getUnsafe().getInt(setE);
                for (int slot = 0; slot < setCapacity; slot++) {
                    assertEquals(get(setE + 16 + slot * 8L), get(setA + 16 + slot * 8L));
                }
            }
        }
    }

    private static Pipeline pipeline(int capacity, double loadFactor) {
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        return new Pipeline(List.of(k, v), new All(), List.of(k), List.of(new NestedCountDistinct(v, capacity, loadFactor)));
    }

    private static long get(long address) { return Unsafe.getUnsafe().getLong(address); }
    private static void put(long address, long value) { Unsafe.getUnsafe().putLong(address, value); }
}
