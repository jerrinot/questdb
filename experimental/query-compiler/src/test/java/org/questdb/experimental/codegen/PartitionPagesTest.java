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
    public void inlineMapScanPreservesCursorOrderAndSourceAcrossOccupancies() throws Exception {
        var values = new io.questdb.cairo.ArrayColumnTypes().add(ColumnType.LONG);
        for (int type : new int[]{ColumnType.INT, ColumnType.LONG, ColumnType.VARCHAR}) {
            var probe = QuestMapProbe.direct(0, type, false);
            for (boolean zeroLast : new boolean[]{false, true}) {
                Builder b = new Builder();
                Var count = b.let(i64(0));
                QuestMapScan.emit(b, probe, COLUMNS, zeroLast, (next, entry) -> {
                    next.store(new Offset(STATE, binary(Op.MUL, count, i64(8))), load(probe.valueAddress(entry)));
                    next.assign(count, add(count, i64(1)));
                });
                b.exit(count);
                try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
                     io.questdb.cairo.map.Map map = type == ColumnType.INT
                             ? new io.questdb.cairo.map.Unordered4Map(type, values, 1, 0.9, 30)
                             : type == ColumnType.LONG ? new io.questdb.cairo.map.Unordered8Map(type, values, 1, 0.9, 30)
                             : new io.questdb.cairo.map.UnorderedVarcharMap(values, 1, 0.9, 30, 4096, 1 << 20);
                     var arena = Arena.ofConfined();
                     var memory = KernelMemory.bind(null, () -> { })) {
                    long view = arena.allocate(io.questdb.cairo.map.NativeMapView.MUTATION_BYTES, 8).address();
                    long output = arena.allocate(8192 * 8L, 8).address();
                    // Empty/minimum, densely occupied, grown and sparse-after-clear maps.
                    for (int rows : new int[]{0, 1, 14, 15, 900, 4097, 0, 3}) {
                        map.clear();
                        for (int i = 0; i < rows; i++) {
                            var key = map.withKey();
                            if (type == ColumnType.INT) key.putInt(i == 1 ? Integer.MIN_VALUE : i);
                            else if (type == ColumnType.LONG) key.putLong(i == 1 ? Long.MIN_VALUE : i);
                            else key.putVarchar(i == 0 ? null : new io.questdb.std.str.Utf8String(i == 1 ? "" : "ž-" + i));
                            key.createValue().putLong(0, i * 17L + 5);
                        }
                        var expected = new java.util.ArrayList<Long>();
                        try (var cursor = map.getCursor()) {
                            while (cursor.hasNext()) expected.add(cursor.getRecord().getLong(0));
                        }
                        if (!zeroLast && type != ColumnType.VARCHAR && rows > 0) {
                            expected.add(0, expected.remove(expected.size() - 1));
                        }
                        map.writeNativeMutationView(view);
                        long address = Unsafe.getLong(view), stride = Unsafe.getLong(view + io.questdb.cairo.map.NativeMapView.STRIDE);
                        long bytes = (Unsafe.getLong(view + io.questdb.cairo.map.NativeMapView.MASK) + 1
                                + (type == ColumnType.VARCHAR ? 0 : 1)) * stride;
                        byte[] before = new byte[(int) bytes];
                        for (int i = 0; i < bytes; i++) before[i] = Unsafe.getByte(address + i);
                        for (int repeat = 0; repeat < 2; repeat++) {
                            assertEquals(rows, code.run(view, rows, output));
                            memory.throwIfFailed();
                            for (int i = 0; i < rows; i++) assertEquals(expected.get(i).longValue(), Unsafe.getLong(output + i * 8L));
                            for (int i = 0; i < bytes; i++) assertEquals(before[i], Unsafe.getByte(address + i));
                        }
                    }
                }
            }
        }
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
    public void keyWidthSamplesIgnoreFrequencyAndInputOrder() throws Exception {
        Builder b = new Builder();
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(16))));
            QuestPartitionSketch.add(loop, STATE, load(row), load(at(row, 8)));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofConfined()) {
            int count = 32_768;
            long data = arena.allocate(count * 16L, 8).address();
            long reversed = arena.allocate(count * 16L, 8).address();
            long sketch = arena.allocate(QuestPartitionSketch.VARIABLE_BYTES, 8).address();
            long reference = arena.allocate(QuestPartitionSketch.VARIABLE_BYTES, 8).address();
            var random = new Random(1747);
            for (int i = 0; i < count; i++) {
                long hash = random.nextLong(), width = i % 31 == 0 ? 100_003 : 17 + i % 73;
                Unsafe.putLong(data + i * 16L, hash);
                Unsafe.putLong(data + i * 16L + 8, width);
                Unsafe.putLong(reversed + (count - 1L - i) * 16, hash);
                Unsafe.putLong(reversed + (count - 1L - i) * 16 + 8, width);
            }
            assertEquals(0, code.run(data, count, reference));
            assertEquals(0, code.run(reversed, count, sketch));
            for (int repeat = 0; repeat < 100; repeat++) assertEquals(0, code.run(data, 1024, sketch));
            for (long i = 0; i < QuestPartitionSketch.VARIABLE_BYTES; i++) {
                assertEquals("A repeated key must not gain sample weight", Unsafe.getByte(reference + i), Unsafe.getByte(sketch + i));
            }
        }
    }

    @Test
    public void variableRepartitionReusesPagesAndPreservesOversizedRecords() throws Exception {
        for (boolean inputs : new boolean[]{false, true}) {
            try (var code = backend.compile(QuestRepartition.variableRows(8, inputs), temporary.newFolder().toPath());
                 var allocator = new FastGroupByAllocator(64 * 1024, 1 << 20);
                 Arena arena = Arena.ofConfined()) {
                long records = arena.allocate(2_000_000, 8).address();
                long state = arena.allocate(QuestRepartition.STATE_BYTES, 8).address();
                var memory = KernelMemory.reusable(allocator);
                int[] lengths = {10_000, 17, 6_000, 9_000, 9}, fanouts = {2, 16, 4, 64, 2};
                for (int round = 0; round < lengths.length; round++) {
                    int count = lengths[round], parts = fanouts[round], shift = 56 - Integer.numberOfTrailingZeros(parts);
                    long row = records;
                    for (int i = 0; i < count; i++) {
                        long keyBytes = round == 1 && i == 0 ? 90_013 : 12 + i % 73;
                        long bytes = inputs ? keyBytes + 8 : (keyBytes + 15) & ~7L;
                        Unsafe.putLong(row, Hash.hashLong64(i));
                        Unsafe.putInt(row + 8, (int) keyBytes - 4);
                        Vect.memset(row + 12, keyBytes - 4, i & 255);
                        Unsafe.putLong(row + 8 + keyBytes, i);
                        row += 8 + bytes;
                    }
                    Unsafe.putLong(state + QuestRepartition.MASK_OFFSET, parts - 1);
                    Unsafe.putLong(state + QuestRepartition.SHIFT_OFFSET, shift);
                    long allocated = 0;
                    for (int repeat = 0; repeat < 2; repeat++) {
                        for (int part = 0; part < parts; part++) Unsafe.putLong(state + part * 16L + 8, 0);
                        memory.enterCheckpoint(() -> { });
                        try { assertEquals(0, code.run(records, count, state)); memory.throwIfFailed(); }
                        finally { memory.close(); }
                        if (repeat == 0) allocated = allocator.allocated();
                        else assertEquals("Replaying an outer partition must reuse its pages", allocated, allocator.allocated());
                    }
                    boolean[] seen = new boolean[count];
                    int actual = 0;
                    for (int part = 0; part < parts; part++) {
                        long tail = Unsafe.getLong(state + part * 16L + 8), page = Unsafe.getLong(state + part * 16L);
                        if (tail == 0) continue;
                        while (true) {
                            row = page + QuestPartitions.VARIABLE_HEADER_BYTES;
                            for (long n = 0; n < Unsafe.getLong(page + 8); n++) {
                                long keyBytes = 4L + Unsafe.getInt(row + 8);
                                int id = Math.toIntExact(Unsafe.getLong(row + 8 + keyBytes));
                                assertTrue(id >= 0 && id < count);
                                assertFalse("Stale or duplicate record", seen[id]); seen[id] = true;
                                assertEquals(Hash.hashLong64(id), Unsafe.getLong(row));
                                assertEquals(part, (Unsafe.getLong(row) >>> shift) & (parts - 1));
                                for (long j = 4; j < keyBytes; j++) assertEquals((byte) id, Unsafe.getByte(row + 8 + j));
                                row += 8 + (inputs ? keyBytes + 8 : (keyBytes + 15) & ~7L);
                                actual++;
                            }
                            assertEquals(page + QuestPartitions.VARIABLE_HEADER_BYTES + Unsafe.getLong(page + 16), row);
                            if (page == tail) break;
                            page = Unsafe.getLong(page);
                            assertNotEquals(0, page);
                        }
                    }
                    assertEquals(count, actual);
                }
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
