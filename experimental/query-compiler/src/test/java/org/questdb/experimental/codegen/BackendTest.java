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

import io.questdb.std.Unsafe;
import io.questdb.cairo.ColumnType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

@RunWith(Parameterized.class)
public class BackendTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;

    public BackendTest(String name, Backend backend) { this.backend = backend; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend(2, true)}, new Object[]{"native", new NativeBackend()}, new Object[]{"asmjit", new AsmJitBackend(true)});
    }

    @Test
    public void sharedHelperPreservesArgumentsEffectsAndFailureExits() throws Exception {
        var parameters = List.of(new Var(0, Type.ADDRESS), new Var(1, Type.I64), new Var(2, Type.I64));
        Builder body = new Builder(parameters);
        body.when(ne(parameters.get(2), i64(0)), failed -> failed.exit(7));
        body.poll();
        body.store(parameters.get(0), add(load(parameters.get(0)), parameters.get(1)));
        body.assign(parameters.get(1), i64(999)); // A helper parameter is local to the call.
        body.exit(0);
        Helper helper = new Helper("accumulate", parameters, body.block());

        Builder b = new Builder();
        b.store(STATE, i64(0));
        b.store(at(STATE, 8), i64(111));
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.call(helper, List.of(STATE, add(index, i64(1)), load(COLUMNS)));
            loop.call(helper, List.of(STATE, i64(10), load(COLUMNS)));
            loop.assign(index, add(index, i64(1)));
        });
        b.store(at(STATE, 8), i64(222));
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             var memory = KernelMemory.reusable(() -> { });
             Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(8, 8).address(), output = arena.allocate(16, 8).address();
            for (boolean fail : new boolean[]{false, true}) {
                Unsafe.putLong(input, fail ? 1 : 0);
                memory.enter(null);
                try {
                    assertEquals(fail ? 7 : 0, code.run(input, 3, output));
                    assertEquals(fail ? 0 : 36, Unsafe.getLong(output));
                    assertEquals(fail ? 111 : 222, Unsafe.getLong(output + 8));
                } finally {
                    memory.close();
                }
            }
            Unsafe.putLong(input, 0);
            RuntimeException cancelled = new RuntimeException("cancel inside helper");
            memory.enterCheckpoint(() -> { throw cancelled; });
            try {
                assertEquals(4, code.run(input, 3, output));
                assertEquals(0, Unsafe.getLong(output));
                assertEquals(111, Unsafe.getLong(output + 8));
                assertSame(cancelled, assertThrows(RuntimeException.class, memory::throwIfFailed));
            } finally {
                memory.close();
            }
            memory.enter(null);
            try {
                assertEquals(0, code.run(input, 3, output));
                assertEquals(36, Unsafe.getLong(output));
                assertEquals(222, Unsafe.getLong(output + 8));
            } finally {
                memory.close();
            }
        }
    }

    @Test
    public void atomicTicketsPublishPayloadsToConcurrentReader() throws Exception {
        Builder writer = new Builder();
        Var index = writer.let(i64(0));
        writer.loop(binary(Op.LT, index, ROWS), loop -> {
            Var ticket = loop.let(new AtomicAdd(STATE, i64(1)));
            Var cell = loop.let(new Offset(COLUMNS, binary(Op.MUL, ticket, i64(16))));
            loop.store(at(cell, 8), binary(Op.XOR, ticket, i64(0x123456789abcdefL)));
            loop.atomicStore(cell, i64(1));
            loop.assign(index, add(index, i64(1)));
        });
        writer.exit(0);

        Builder reader = new Builder();
        Var row = reader.let(i64(0));
        reader.loop(binary(Op.LT, row, ROWS), loop -> {
            Var cell = loop.let(new Offset(COLUMNS, binary(Op.MUL, row, i64(16))));
            Var spins = loop.let(i64(0));
            loop.loop(binary(Op.BOTH, eq(new AtomicLoad(cell, true), i64(0)),
                    binary(Op.LT, spins, i64(100_000_000))), wait -> wait.assign(spins, add(spins, i64(1))));
            loop.when(eq(new AtomicLoad(cell, true), i64(0)), failed -> failed.exit(-1));
            loop.when(ne(load(at(cell, 8)), binary(Op.XOR, row, i64(0x123456789abcdefL))), failed -> failed.exit(-2));
            loop.assign(row, add(row, i64(1)));
        });
        reader.exit(0);
        try (var write = backend.compile(new Program(writer.block()), temporary.getRoot().toPath());
             var read = backend.compile(new Program(reader.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofShared()) {
            long counter = arena.allocate(8, 8).address();
            long cells = arena.allocate(8_000 * 16L, 8).address();
            var start = new java.util.concurrent.CountDownLatch(1);
            var failures = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
            var threads = new java.util.ArrayList<Thread>();
            for (int i = 0; i < 9; i++) {
                boolean reading = i == 8;
                Thread thread = new Thread(() -> {
                    try {
                        start.await();
                        assertEquals(0, (reading ? read : write).run(cells, reading ? 8_000 : 1_000, counter));
                    } catch (Throwable failure) { failures.add(failure); }
                });
                threads.add(thread);
                thread.start();
            }
            start.countDown();
            for (Thread thread : threads) thread.join();
            assertTrue(failures.toString(), failures.isEmpty());
            assertEquals(8_000, Unsafe.getLong(counter));
        }
    }

    @Test
    public void atomicCompareExchangePreservesObservedValuesUnderContention() throws Exception {
        Builder b = new Builder();
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            Var observed = loop.let(new AtomicLoad(STATE));
            Var done = loop.let(bool(false));
            loop.loop(eq(done, bool(false)), retry -> {
                Var previous = retry.let(new CompareExchange(STATE, observed, add(observed, i64(1))));
                retry.assign(done, eq(previous, observed));
                retry.assign(observed, previous);
            });
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofShared()) {
            long address = arena.allocate(8, 8).address();
            Unsafe.putLong(address, 0);
            var start = new java.util.concurrent.CountDownLatch(1);
            var failures = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
            var threads = new java.util.ArrayList<Thread>();
            for (int i = 0; i < 8; i++) {
                Thread thread = new Thread(() -> {
                    try { start.await(); assertEquals(0, code.run(0, 10_000, address)); }
                    catch (Throwable failure) { failures.add(failure); }
                });
                threads.add(thread);
                thread.start();
            }
            start.countDown();
            for (Thread thread : threads) thread.join();
            assertTrue(failures.toString(), failures.isEmpty());
            assertEquals(80_000, Unsafe.getLong(address));
        }
    }

    @Test
    public void positiveConstantDivisionPreservesSignedExtremes() throws Exception {
        long[] divisors = {1, 2, 3, 60, 60_000_000L, 60_000_000_000L, Long.MAX_VALUE};
        Builder b = new Builder();
        Var value = b.let(load(COLUMNS));
        for (int i = 0; i < divisors.length; i++) b.store(at(STATE, i * 8L), binary(Op.SDIV, value, i64(divisors[i])));
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long in = arena.allocate(8, 8).address(), out = arena.allocate(divisors.length * 8L, 8).address();
            for (long v : new long[]{Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE, -60_000_000_001L,
                    -60_000_000_000L, -61, -60, -59, -1, 0, 1, 59, 60, 61, 60_000_000_001L}) {
                Unsafe.getUnsafe().putLong(in, v);
                assertEquals(0, code.run(in, 1, out));
                for (int i=0; i<divisors.length; i++) assertEquals(v / divisors[i], Unsafe.getUnsafe().getLong(out+i*8L));
            }
        }
        assertThrows(IllegalArgumentException.class, () -> binary(Op.SDIV, ROWS, i64(0)));
        assertThrows(IllegalArgumentException.class, () -> binary(Op.SDIV, ROWS, i64(-1)));
        assertThrows(IllegalArgumentException.class, () -> binary(Op.SDIV, ROWS, ROWS));
    }

    @Test
    public void floatingStatePreservesIEEEArithmeticAndLoopPhis() throws Exception {
        Builder b = new Builder();
        Var converted = b.let(new ToDouble(load(COLUMNS)));
        Var x = b.let(new Load(MemoryType.F64, at(COLUMNS, 8)));
        Var y = b.let(new Load(MemoryType.F64, at(COLUMNS, 16)));
        b.store(MemoryType.F64, STATE, converted);
        b.store(MemoryType.F64, at(STATE, 8), binary(Op.FDIV, converted, x));
        b.store(MemoryType.F64, at(STATE, 16), binary(Op.FADD, x, f64(-0.0)));
        b.store(MemoryType.F64, at(STATE, 24), binary(Op.FSUB, x, x));
        b.store(MemoryType.F64, at(STATE, 32), binary(Op.FMUL, x, y));
        Var accumulator = b.let(f64(-0.0)), index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.assign(accumulator, binary(Op.FADD, accumulator, x));
            loop.assign(index, add(index, i64(1)));
        });
        b.store(MemoryType.F64, at(STATE, 40), accumulator);
        b.store(MemoryType.F64, at(STATE, 48), binary(Op.FSUB, binary(Op.FMUL, x, x), y));
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long in = arena.allocate(24, 8).address(), out = arena.allocate(56, 8).address();
            for (long integer : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, -1, 0, 9007199254740993L}) {
                for (double xValue : new double[]{-0.0, 0.0, Double.NaN, Double.POSITIVE_INFINITY, -3.5, 1.0000000000000002}) {
                    double yValue = 1.0000000000000004;
                    Unsafe.getUnsafe().putLong(in, integer);
                    Unsafe.getUnsafe().putDouble(in + 8, xValue);
                    Unsafe.getUnsafe().putDouble(in + 16, yValue);
                    for (int rows : new int[]{0, 1, 7}) {
                        assertEquals(0, code.run(in, rows, out));
                        double sum = -0.0;
                        for (int i=0; i<rows; i++) sum += xValue;
                        double[] expected = {(double)integer, (double)integer/xValue, xValue+-0.0,
                                xValue-xValue, xValue*yValue, sum, xValue*xValue-yValue};
                        for (int i=0; i<expected.length; i++) assertEquals("slot="+i,
                                Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(Unsafe.getUnsafe().getDouble(out+8L*i)));
                    }
                }
            }
        }
    }

    @Test
    public void handwrittenControlsProduceIdenticalTableBytes() throws Exception {
        var key = new Pipeline.Column(0, "k", ColumnType.INT);
        var value = new Pipeline.Column(1, "v", ColumnType.LONG);
        var flag = new Pipeline.Column(2, "f", ColumnType.BYTE);
        Pipeline mixed = new Pipeline(List.of(key, value, flag),
                new Pipeline.Compare(Op.NE, flag, new Pipeline.Constant(0)), List.of(key),
                List.of(new Pipeline.Count(), new Pipeline.CountValue(value), new Pipeline.Sum(new Pipeline.Add(value, new Pipeline.Constant(3)))));
        Pipeline distinct = new Pipeline(List.of(key, value), new Pipeline.All(), List.of(key), List.of(new Pipeline.CountDistinct(value)));
        try (Arena arena = Arena.ofConfined()) {
            int rows = 300;
            long columns = arena.allocate(24, 8).address();
            long keys = arena.allocate(rows * 4L, 8).address();
            long values = arena.allocate(rows * 8L, 8).address();
            long flags = arena.allocate(rows, 8).address();
            Unsafe.getUnsafe().putLong(columns, keys);
            Unsafe.getUnsafe().putLong(columns + 8, values);
            Unsafe.getUnsafe().putLong(columns + 16, flags);
            for (int i = 0; i < rows; i++) {
                Unsafe.getUnsafe().putInt(keys + i * 4L, i % 7 == 0 ? Integer.MIN_VALUE : i % 13 - 6);
                Unsafe.getUnsafe().putLong(values + i * 8L, i % 11 == 0 ? Long.MIN_VALUE : i % 17 - 8);
                Unsafe.getUnsafe().putByte(flags + i, (byte) (i % 3));
            }
            for (Pipeline pipeline : List.of(mixed, distinct)) {
                int capacity = pipeline.distinct() ? 512 : 32;
                var generated = TestTables.create(arena, capacity, pipeline.inputEntryBytes());
                var control = TestTables.create(arena, capacity, pipeline.inputEntryBytes());
                Unsafe.getUnsafe().putLong(generated.address(), capacity);
                Unsafe.getUnsafe().putLong(control.address(), capacity);
                try (var kernel = backend.compile(pipeline.lower(), temporary.getRoot().toPath())) {
                    Kernel hand = pipeline.distinct() ? HandwrittenKernels::distinct : HandwrittenKernels::mixed;
                    assertEquals(0, kernel.run(columns, rows, generated.address()));
                    assertEquals(0, hand.run(columns, rows, control.address()));
                    assertArrayEquals(TestTables.bytes(control, pipeline.inputEntryBytes()), TestTables.bytes(generated, pipeline.inputEntryBytes()));
                    // Zero addresses represent column tops; exercise both controls that way too.
                    Unsafe.getUnsafe().putLong(columns, 0);
                    Unsafe.getUnsafe().putLong(columns + 8, 0);
                    assertEquals(0, kernel.run(columns, 10, generated.address()));
                    assertEquals(0, hand.run(columns, 10, control.address()));
                    assertArrayEquals(TestTables.bytes(control, pipeline.inputEntryBytes()), TestTables.bytes(generated, pipeline.inputEntryBytes()));
                    Unsafe.getUnsafe().putLong(columns, keys);
                    Unsafe.getUnsafe().putLong(columns + 8, values);
                }
                if (pipeline.distinct()) {
                    var generatedGroups = TestTables.create(arena, 32, pipeline.entryBytes());
                    var controlGroups = TestTables.create(arena, 32, pipeline.entryBytes());
                    Unsafe.getUnsafe().putLong(generatedGroups.address(), 32);
                    Unsafe.getUnsafe().putLong(controlGroups.address(), 32);
                    try (var finish = backend.compile(pipeline.lowerFinish(), temporary.getRoot().toPath())) {
                        assertEquals(0, finish.run(TableState.entries(generated.address()), capacity, generatedGroups.address()));
                        assertEquals(0, HandwrittenKernels.collapse(TableState.entries(control.address()), capacity, controlGroups.address()));
                        assertArrayEquals(TestTables.bytes(controlGroups, pipeline.entryBytes()), TestTables.bytes(generatedGroups, pipeline.entryBytes()));
                    }
                }
            }
        }
    }

    @Test
    public void preservesSignedLoadsAndWraparound() throws Exception {
        Builder b = new Builder();
        b.store(at(STATE, 0), new Load(MemoryType.I8, at(COLUMNS, 0)));
        b.store(at(STATE, 8), new Load(MemoryType.I16, at(COLUMNS, 1)));
        b.store(at(STATE, 16), new Load(MemoryType.I32, at(COLUMNS, 3)));
        Var value = b.let(load(at(COLUMNS, 7)));
        b.store(at(STATE, 24), add(value, i64(1)));
        b.store(at(STATE, 32), binary(Op.MUL, value, i64(3)));
        b.store(at(STATE, 40), binary(Op.USHR, i64(-1), value));
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(16, 8).address();
            long output = arena.allocate(48, 8).address();
            Unsafe.getUnsafe().putByte(input, Byte.MIN_VALUE);
            Unsafe.getUnsafe().putShort(input + 1, Short.MIN_VALUE);
            Unsafe.getUnsafe().putInt(input + 3, Integer.MIN_VALUE);
            for (long valueIn : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, -1, 0, 1, 63, 64, 65}) {
                Unsafe.getUnsafe().putLong(input + 7, valueIn);
                assertEquals(0, kernel.run(input, 0, output));
                assertEquals(Byte.MIN_VALUE, Unsafe.getUnsafe().getLong(output));
                assertEquals(Short.MIN_VALUE, Unsafe.getUnsafe().getLong(output + 8));
                assertEquals(Integer.MIN_VALUE, Unsafe.getUnsafe().getLong(output + 16));
                assertEquals(valueIn + 1, Unsafe.getUnsafe().getLong(output + 24));
                assertEquals(valueIn * 3, Unsafe.getUnsafe().getLong(output + 32));
                assertEquals(-1L >>> valueIn, Unsafe.getUnsafe().getLong(output + 40));
            }
            assertTrue(Files.size(kernel.artifacts().resolve("query.ir")) > 0);
        }
    }

    @Test
    public void preservesWideConstantsAndSignedComparisons() throws Exception {
        long[] constants = {Long.MIN_VALUE, Integer.MIN_VALUE - 1L, Integer.MIN_VALUE,
                -1, 0, 1, 63, 64, 65, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L, Long.MAX_VALUE};
        Builder b = new Builder();
        Var value = b.let(load(at(COLUMNS, -8)));
        for (int i = 0; i < constants.length; i++) {
            long c = constants[i];
            long offset = i * 56L;
            b.store(at(STATE, offset), add(value, i64(c)));
            b.store(at(STATE, offset + 8), binary(Op.MUL, value, i64(c)));
            b.store(at(STATE, offset + 16), binary(Op.AND, value, i64(c)));
            b.store(at(STATE, offset + 24), binary(Op.XOR, value, i64(c)));
            b.store(at(STATE, offset + 32), binary(Op.USHR, value, i64(c)));
            b.store(at(STATE, offset + 40), new Select(binary(Op.LT, value, i64(c)), i64(1), i64(0)));
            b.store(at(STATE, offset + 48), new Select(eq(value, i64(c)), i64(1), i64(0)));
        }
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofConfined()) {
            long input = arena.allocate(16, 8).address();
            long output = arena.allocate(constants.length * 56L, 8).address();
            for (long v : constants) {
                Unsafe.getUnsafe().putLong(input, v);
                assertEquals(0, kernel.run(input + 8, 0, output));
                for (int i = 0; i < constants.length; i++) {
                    long c = constants[i], at = output + i * 56L;
                    assertEquals(v + c, Unsafe.getUnsafe().getLong(at));
                    assertEquals(v * c, Unsafe.getUnsafe().getLong(at + 8));
                    assertEquals(v & c, Unsafe.getUnsafe().getLong(at + 16));
                    assertEquals(v ^ c, Unsafe.getUnsafe().getLong(at + 24));
                    assertEquals(v >>> c, Unsafe.getUnsafe().getLong(at + 32));
                    assertEquals(v < c ? 1 : 0, Unsafe.getUnsafe().getLong(at + 40));
                    assertEquals(v == c ? 1 : 0, Unsafe.getUnsafe().getLong(at + 48));
                }
            }
        }
    }

    @Test
    public void executesLoopsAndBranches() throws Exception {
        Builder b = new Builder();
        Var row = b.let(i64(0));
        Var sum = b.let(i64(0));
        b.loop(binary(Op.LT, row, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, row, i64(1)), i64(0)), even -> even.assign(sum, add(sum, row)));
            loop.assign(row, add(row, i64(1)));
        });
        b.store(STATE, sum);
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofConfined()) {
            long output = arena.allocate(8, 8).address();
            for (int count : new int[]{0, 1, 2, 10, 101}) {
                long expected = 0;
                for (int i = 0; i < count; i += 2) { expected += i; }
                assertEquals(0, kernel.run(0, count, output));
                assertEquals(expected, Unsafe.getUnsafe().getLong(output));
            }
        }
    }

    @Test
    public void supportsConstantLoopConditionsAndUnusedLocals() throws Exception {
        Builder b = new Builder();
        b.let(i64(123)); // Legal dead IR; a native warning must not reject it.
        b.loop(bool(false), dead -> dead.store(nil(), i64(1)));
        b.loop(bool(true), live -> live.exit(42));
        b.exit(0);
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath())) {
            assertEquals(42, kernel.run(0, 0, 0));
        }
    }

    @Test
    public void doesNotEvaluateUnselectedLoadAndRejectsUseAfterClose() throws Exception {
        Builder b = new Builder();
        b.store(STATE, new Select(eq(COLUMNS, nil()), i64(17), load(COLUMNS)));
        b.exit(0);
        CompiledKernel kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
        try (kernel; Arena arena = Arena.ofConfined()) {
            long output = arena.allocate(8, 8).address();
            assertEquals(0, kernel.run(0, 0, output));
            assertEquals(17, Unsafe.getUnsafe().getLong(output));
            assertThrows(IllegalArgumentException.class, () -> kernel.run(0, -1, output));
        }
        kernel.close();
        assertThrows(IllegalStateException.class, () -> kernel.run(0, 0, 0));
    }
}
