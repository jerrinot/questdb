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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
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
import static org.questdb.experimental.codegen.Ir.*;

@RunWith(Parameterized.class)
public class OptimizerBackendTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    public OptimizerBackendTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name="{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"native", new NativeBackend()}, new Object[]{"asmjit", new AsmJitBackend()});
    }

    @Test public void preservesCopiesAcrossWritesBranchesAndLoopBackedges() throws Exception {
        Builder b = new Builder();
        Var source = b.let(load(COLUMNS)), snapshot = b.let(source), running = b.let(snapshot);
        b.assign(source, i64(7));
        Var index = b.let(i64(0)), total = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            Var previous = loop.let(running);
            loop.when(eq(binary(Op.AND, index, i64(1)), i64(0)), even -> even.assign(running, add(running, i64(3))));
            loop.when(eq(binary(Op.AND, index, i64(3)), i64(1)), odd -> odd.assign(running, binary(Op.XOR, running, snapshot)));
            loop.assign(total, add(total, previous));
            loop.when(eq(index, i64(2)), third -> third.assign(snapshot, add(snapshot, i64(1))));
            loop.assign(index, add(index, i64(1)));
        });
        b.store(STATE, source); b.store(at(STATE, 8), snapshot);
        b.store(at(STATE, 16), running); b.store(at(STATE, 24), total);
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(8, 8).address(), output = arena.allocate(32, 8).address();
            for (long value : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, -19, 0, 8}) for (int n : new int[]{0, 1, 2, 3, 7, 64}) {
                Unsafe.getUnsafe().putLong(input, value);
                assertEquals(0, kernel.run(input, n, output));
                long snap = value, run = value, sum = 0;
                for (int i = 0; i < n; i++) {
                    long previous = run;
                    if ((i & 1) == 0) run += 3;
                    if ((i & 3) == 1) run ^= snap;
                    sum += previous;
                    if (i == 2) snap++;
                }
                assertEquals(7, Unsafe.getUnsafe().getLong(output));
                assertEquals(snap, Unsafe.getUnsafe().getLong(output + 8));
                assertEquals(run, Unsafe.getUnsafe().getLong(output + 16));
                assertEquals(sum, Unsafe.getUnsafe().getLong(output + 24));
            }
        }
    }

    @Test public void mergesConditionalConstantsAndPreservesAliasedMemoryLoads() throws Exception {
        Builder b = new Builder();
        Var x = b.let(i64(5)), old = b.let(x);
        b.when(eq(ROWS, i64(0)), taken -> taken.assign(x, i64(9)));
        b.store(STATE, x); b.store(at(STATE, 8), old);
        Var before = b.let(load(COLUMNS));
        b.store(COLUMNS, add(before, i64(1)));
        Var after = b.let(load(COLUMNS));
        b.store(at(STATE, 16), before); b.store(at(STATE, 24), after);
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(8, 8).address(), output = arena.allocate(32, 8).address();
            for (int rows : new int[]{0, 1}) {
                Unsafe.getUnsafe().putLong(input, Long.MAX_VALUE);
                assertEquals(0, kernel.run(input, rows, output));
                assertEquals(rows == 0 ? 9 : 5, Unsafe.getUnsafe().getLong(output));
                assertEquals(5, Unsafe.getUnsafe().getLong(output + 8));
                assertEquals(Long.MAX_VALUE, Unsafe.getUnsafe().getLong(output + 16));
                assertEquals(Long.MIN_VALUE, Unsafe.getUnsafe().getLong(output + 24));
            }
        }
    }

    @Test public void simplifiesComparisonCombinationsForAllSignedOutcomes() throws Exception {
        Op[] comparisons = {Op.EQ, Op.NE, Op.LT, Op.GE};
        Builder b = new Builder();
        Var x = b.let(load(COLUMNS)), y = b.let(load(at(COLUMNS, 8)));
        long offset = 0;
        for (Op a : comparisons) for (Op c : comparisons) for (Op logical : new Op[]{Op.BOTH, Op.EITHER}) {
            Expr value = binary(logical, binary(a, x, y), binary(c, x, y));
            b.store(at(STATE, offset), new Select(value, i64(1), i64(0))); offset += 8;
        }
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(16, 8).address(), output = arena.allocate(offset, 8).address();
            Random random = new Random(442);
            long[] values = {Long.MIN_VALUE, Long.MAX_VALUE, -1, 0, 1, random.nextLong(), random.nextLong()};
            for (long a : values) for (long c : values) {
                Unsafe.getUnsafe().putLong(input, a); Unsafe.getUnsafe().putLong(input + 8, c);
                assertEquals(0, kernel.run(input, 0, output)); offset = 0;
                for (Op op1 : comparisons) for (Op op2 : comparisons) for (Op logical : new Op[]{Op.BOTH, Op.EITHER}) {
                    boolean left = compare(op1, a, c), right = compare(op2, a, c);
                    boolean expected = logical == Op.BOTH ? left && right : left || right;
                    assertEquals(expected ? 1 : 0, Unsafe.getUnsafe().getLong(output + offset)); offset += 8;
                }
            }
        }
    }

    @Test public void preservesAllocationCountAndLazyArms() throws Exception {
        Builder b = new Builder();
        b.let(new Allocate(i64(64))); // Unused result must not erase the call.
        b.let(new Select(eq(ROWS, i64(0)), new Allocate(i64(64)), new Allocate(i64(128))));
        Expr allocation = new Allocate(i64(16));
        b.store(STATE, new Select(eq(allocation, allocation), i64(1), i64(0))); // Two distinct calls.
        b.when(binary(Op.EITHER, bool(true), eq(new Allocate(i64(256)), nil())), taken -> taken.poll());
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long output = arena.allocate(8, 8).address();
            for (int rows : new int[]{0, 1}) {
                long[] allocations = {0, 0};
                var allocator = new FastGroupByAllocator(1024, 1048576) {
                    @Override public long malloc(long bytes) {allocations[0]++; allocations[1] += bytes; return super.malloc(bytes);}
                };
                try (var memory = KernelMemory.open(allocator)) {
                    assertEquals(0, kernel.run(0, rows, output)); memory.throwIfFailed();
                    assertEquals(4, allocations[0]); assertEquals(rows == 0 ? 160 : 224, allocations[1]);
                    assertEquals(0, Unsafe.getUnsafe().getLong(output));
                }
            }
        }
    }
    @Test public void reallocatesInPlaceOrCopiesThroughTheQueryAllocator() throws Exception {
        Builder b = new Builder();
        Var original = b.let(new Allocate(i64(64)));
        b.when(eq(original, nil()), failed -> failed.exit(2));
        b.store(original, i64(0x123456789abcdefL));
        b.store(at(original, 56), i64(-37));
        Var grown = b.let(new Reallocate(original, i64(64), i64(128)));
        b.when(eq(grown, nil()), failed -> failed.exit(2));
        b.let(new Allocate(i64(8))); // Prevent the next growth from extending the latest allocation.
        Var copied = b.let(new Reallocate(grown, i64(128), i64(256)));
        b.when(eq(copied, nil()), failed -> failed.exit(2));
        b.let(new Reallocate(copied, i64(256), i64(128))); // Unused reallocation is still observable.
        b.store(MemoryType.ADDRESS, STATE, original);
        b.store(MemoryType.ADDRESS, at(STATE, 8), grown);
        b.store(MemoryType.ADDRESS, at(STATE, 16), copied);
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long output = arena.allocate(24, 8).address();
            var allocator = new FastGroupByAllocator(1024, 1048576) {
                int reallocations;
                @Override public long realloc(long address, long oldBytes, long newBytes) {
                    reallocations++;
                    return super.realloc(address, oldBytes, newBytes);
                }
            };
            try (var memory = KernelMemory.open(allocator)) {
                assertEquals(0, kernel.run(0, 0, output)); memory.throwIfFailed();
                long first = Unsafe.getUnsafe().getLong(output), grownAddress = Unsafe.getUnsafe().getLong(output + 8);
                long copiedAddress = Unsafe.getUnsafe().getLong(output + 16);
                assertEquals(first, grownAddress);
                assertNotEquals(grownAddress, copiedAddress);
                assertEquals(0x123456789abcdefL, Unsafe.getUnsafe().getLong(copiedAddress));
                assertEquals(-37, Unsafe.getUnsafe().getLong(copiedAddress + 56));
                assertEquals(3, allocator.reallocations);
            }
        }
    }

    @Test public void failedReallocationPreservesThePublishedPointer() throws Exception {
        Builder b = new Builder();
        Var old = b.let(new Load(MemoryType.ADDRESS, STATE)), grown = b.let(new Reallocate(old, i64(64), i64(128)));
        b.when(eq(grown, nil()), failed -> failed.exit(2));
        b.store(MemoryType.ADDRESS, STATE, grown);
        b.exit(0);
        Builder caller = new Builder();
        caller.call(new Helper("grow", List.of(COLUMNS, ROWS, STATE), b.block()), List.of(COLUMNS, ROWS, STATE));
        caller.exit(0);
        try (var kernel = backend.compile(new Program(caller.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long output = arena.allocate(8, 8).address();
            var injected = new OutOfMemoryError("reallocation test");
            var allocator = new FastGroupByAllocator(1024, 1048576) {
                @Override public long realloc(long address, long oldBytes, long newBytes) { throw injected; }
            };
            try (var memory = KernelMemory.open(allocator)) {
                long oldAddress = allocator.malloc(64);
                Unsafe.getUnsafe().putLong(oldAddress, 117);
                Unsafe.getUnsafe().putLong(output, oldAddress);
                assertEquals(2, kernel.run(0, 0, output));
                assertSame(injected, assertThrows(OutOfMemoryError.class, memory::throwIfFailed));
                assertEquals(oldAddress, Unsafe.getUnsafe().getLong(output));
                assertEquals(117, Unsafe.getUnsafe().getLong(oldAddress));
            }
        }
    }

    @Test public void preservesParallelPhiCopiesThroughNestedLoops() throws Exception {
        Builder b = new Builder();
        Var a = b.let(load(COLUMNS)), c = b.let(load(at(COLUMNS, 8)));
        Var index = b.let(i64(0)), sum = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            Var saved = loop.let(a); loop.assign(a, c); loop.assign(c, saved);
            loop.when(eq(binary(Op.AND, index, i64(1)), i64(0)), even -> even.assign(a, add(a, i64(7))));
            Var inner = loop.let(i64(0));
            loop.loop(binary(Op.LT, inner, binary(Op.AND, index, i64(3))), nested -> {
                Var prior = nested.let(a); nested.assign(a, c); nested.assign(c, prior);
                nested.assign(sum, add(sum, binary(Op.XOR, a, c)));
                nested.assign(inner, add(inner, i64(1)));
            });
            loop.assign(sum, add(sum, add(a, binary(Op.MUL, ROWS, i64(7)))));
            loop.assign(index, add(index, i64(1)));
        });
        b.store(STATE, a); b.store(at(STATE, 8), c); b.store(at(STATE, 16), sum); b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(16, 8).address(), output = arena.allocate(24, 8).address();
            Random random = new Random(917);
            for (int trial = 0; trial < 50; trial++) for (int n : new int[]{0, 1, 2, 3, 11, 65}) {
                long x = random.nextLong(), y = random.nextLong(), total = 0;
                Unsafe.getUnsafe().putLong(input, x); Unsafe.getUnsafe().putLong(input + 8, y);
                for (int i = 0; i < n; i++) {
                    long saved = x; x = y; y = saved;
                    if ((i & 1) == 0) x += 7;
                    for (int j = 0; j < (i & 3); j++) { saved = x; x = y; y = saved; total += x ^ y; }
                    total += x + n * 7L;
                }
                assertEquals(0, kernel.run(input, n, output));
                assertEquals(x, Unsafe.getUnsafe().getLong(output));
                assertEquals(y, Unsafe.getUnsafe().getLong(output + 8));
                assertEquals(total, Unsafe.getUnsafe().getLong(output + 16));
            }
        }
    }

    @Test public void observesMemoryChangesAcrossAllocationAndFreeCallbacks() throws Exception {
        Builder b = new Builder(); Var before = b.let(load(COLUMNS));
        Var allocated = b.let(new Allocate(i64(64))), afterAllocate = b.let(load(COLUMNS));
        b.free(allocated, i64(64)); Var afterFree = b.let(load(COLUMNS));
        b.store(STATE, before); b.store(at(STATE, 8), afterAllocate); b.store(at(STATE, 16), afterFree); b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(8, 8).address(), output = arena.allocate(24, 8).address();
            Unsafe.getUnsafe().putLong(input, 17);
            var allocator = new FastGroupByAllocator(1024, 1048576) {
                @Override public long malloc(long bytes) { Unsafe.getUnsafe().putLong(input, 29); return super.malloc(bytes); }
                @Override public void free(long address, long bytes) { Unsafe.getUnsafe().putLong(input, 41); super.free(address, bytes); }
            };
            try (var memory = KernelMemory.open(allocator)) {
                assertEquals(0, kernel.run(input, 0, output)); memory.throwIfFailed();
                assertEquals(17, Unsafe.getUnsafe().getLong(output));
                assertEquals(29, Unsafe.getUnsafe().getLong(output + 8));
                assertEquals(41, Unsafe.getUnsafe().getLong(output + 16));
            }
        }
    }

    @Test public void neverHoistsGuardedLoadsOutOfZeroTripLoops() throws Exception {
        Builder b = new Builder(); Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(COLUMNS, nil()), absent -> absent.exit(17));
            loop.store(STATE, add(load(COLUMNS), binary(Op.MUL, ROWS, i64(3))));
            loop.assign(index, add(index, i64(1)));
        }); b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            assertEquals(0, kernel.run(0, 0, 0));
            assertEquals(17, kernel.run(0, 1, 0));
            long input = arena.allocate(8, 8).address(), output = arena.allocate(8, 8).address();
            Unsafe.getUnsafe().putLong(input, Long.MAX_VALUE);
            assertEquals(0, kernel.run(input, 2, output));
            assertEquals(Long.MIN_VALUE + 5, Unsafe.getUnsafe().getLong(output));
        }
    }

    private static boolean compare(Op op, long a, long b) {
        return switch (op) {case EQ -> a == b; case NE -> a != b; case LT -> a < b; case GE -> a >= b; default -> throw new AssertionError();};
    }
}
