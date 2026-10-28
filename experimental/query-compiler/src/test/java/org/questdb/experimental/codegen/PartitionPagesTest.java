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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.hyperloglog.HyperLogLogDenseRepresentation;
import io.questdb.std.Hash;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

@RunWith(Parameterized.class)
public class PartitionPagesTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;

    public PartitionPagesTest(String name, Backend backend) { this.backend = backend; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()},
                new Object[]{"c", new NativeBackend()});
    }

    @Test
    public void sampledRegistersMatchQuestHllWithoutBiasOnDenseKeys() throws Exception {
        Builder b = new Builder();
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            QuestPartitionSketch.add(loop, STATE, load(new Offset(COLUMNS, binary(Op.MUL, index, i64(8)))));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofConfined()) {
            int count = 1_000_000;
            long data = arena.allocate(count * 8L, 8).address();
            long sketch = arena.allocate(QuestPartitionSketch.BYTES, 8).address();
            int stride = QuestPartitionSketch.HEADER_BYTES + QuestPartitionSketch.REGISTERS;
            long reference = arena.allocate(256L * stride, 8).address();
            for (boolean dense : new boolean[]{true, false}) {
                Vect.memset(sketch, QuestPartitionSketch.BYTES, 0);
                Vect.memset(reference, 256L * stride, 0);
                var estimators = new HyperLogLogDenseRepresentation[256];
                for (int p = 0; p < 256; p++) estimators[p] = new HyperLogLogDenseRepresentation(7).of(reference + (long) p * stride);
                var random = new Random(1747);
                for (int i = 0; i < count; i++) {
                    long hash = dense ? Hash.hashLong64(i) : random.nextLong();
                    Unsafe.putLong(data + i * 8L, hash);
                    if (((hash >>> 52) & 15) == 0) {
                        int partition = (int) (hash >>> 56);
                        long mixed = (hash ^ (hash >>> 33)) * 0xff51afd7ed558ccdL;
                        mixed = (mixed ^ (mixed >>> 33)) * 0xc4ceb9fe1a85ec53L;
                        mixed ^= mixed >>> 33;
                        // Reorient the same register/rank bits to QuestDB's leading-zero convention.
                        long hllHash = (((mixed >>> 45) & 127) << 57)
                                | (Long.reverse(mixed & ((1L << 45) - 1)) >>> 7) | (1L << 11);
                        estimators[partition].add(hllHash);
                    }
                }
                assertEquals(0, code.run(data, count, 0));
                for (int repeat = 0; repeat < 2; repeat++) {
                    assertEquals(0, code.run(data, count, sketch));
                    for (int p = 0; p < 256; p++) for (int register = 0; register < 128; register++) {
                        assertEquals(Unsafe.getByte(reference + (long) p * stride + QuestPartitionSketch.HEADER_BYTES + register),
                                Unsafe.getByte(sketch + p * 128L + register));
                    }
                }
                long estimate = 0;
                for (var estimator : estimators) estimate += estimator.computeCardinality() * QuestPartitionSketch.SAMPLE_RATE;
                assertTrue("Map hash conditioning must not bias cardinality: " + estimate, Math.abs(estimate - count) < count / 20);
            }
        }
    }

    @Test
    public void repartitionReusesChainsWithDifferentFanoutsAndShorterTails() throws Exception {
        var probe = QuestMapProbe.direct(0, ColumnType.LONG, false).mutating();
        try (var code = backend.compile(QuestRepartition.rows(probe, 16), temporary.getRoot().toPath());
             var allocator = new FastGroupByAllocator(64 * 1024, 1 << 20);
             Arena arena = Arena.ofConfined()) {
            long records = arena.allocate(100_000L * 16, 8).address();
            long state = arena.allocate(QuestRepartition.STATE_BYTES, 8).address();
            int[] fanouts = {2, 16, 4, 64, 2};
            int[] lengths = {100_000, 17, 60_000, 90_000, 9};
            var memory = KernelMemory.reusable(allocator);
            long firstHead = 0;
            for (int round = 0; round < fanouts.length; round++) {
                int parts = fanouts[round], count = lengths[round];
                int shift = 56 - Integer.numberOfTrailingZeros(parts);
                Unsafe.putLong(state + QuestRepartition.MASK_OFFSET, parts - 1);
                Unsafe.putLong(state + QuestRepartition.SHIFT_OFFSET, shift);
                for (int part = 0; part < parts; part++) {
                    Unsafe.putLong(state + part * (long) QuestPartitions.BUCKET_BYTES + QuestPartitions.BUCKET_TAIL_OFFSET, 0);
                }
                for (int row = 0; row < count; row++) {
                    Unsafe.putLong(records + row * 16L, key(row, round));
                    Unsafe.putLong(records + row * 16L + 8, row);
                }
                memory.enterCheckpoint(() -> { });
                try {
                    assertEquals(0, code.run(records, count, state));
                    memory.throwIfFailed();
                } finally { memory.close(); }
                if (round == 0) firstHead = Unsafe.getLong(state);
                else assertEquals("First chain is reused", firstHead, Unsafe.getLong(state));
                boolean[] seen = new boolean[count];
                int actual = 0;
                for (int part = 0; part < parts; part++) {
                    long tail = Unsafe.getLong(state + part * (long) QuestPartitions.BUCKET_BYTES + QuestPartitions.BUCKET_TAIL_OFFSET);
                    if (tail == 0) continue;
                    long page = Unsafe.getLong(state + part * (long) QuestPartitions.BUCKET_BYTES);
                    while (true) {
                        long rows = Unsafe.getLong(page + QuestPartitions.PAGE_COUNT_OFFSET);
                        assertTrue(rows > 0 && rows <= (QuestPartitions.PAGE_BYTES - QuestPartitions.PAGE_HEADER_BYTES) / 16);
                        for (int row = 0; row < rows; row++) {
                            long record = page + QuestPartitions.PAGE_HEADER_BYTES + row * 16L;
                            int input = Math.toIntExact(Unsafe.getLong(record + 8));
                            assertTrue(input >= 0 && input < count);
                            assertFalse("Stale or duplicate row", seen[input]);
                            seen[input] = true;
                            assertEquals(key(input, round), Unsafe.getLong(record));
                            assertEquals(part, (Hash.hashLong64(Unsafe.getLong(record)) >>> shift) & (parts - 1));
                            actual++;
                        }
                        if (page == tail) break;
                        page = Unsafe.getLong(page);
                        assertNotEquals(0, page);
                    }
                }
                assertEquals(count, actual);
            }
        }
    }

    private static long key(int row, int round) {
        return row == 0 ? 0 : row == 1 ? Long.MIN_VALUE : row * 198491317L + round;
    }
}
