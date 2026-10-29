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
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

@RunWith(Parameterized.class)
public class FloatingIrTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    public FloatingIrTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()},
                new Object[]{"c", new NativeBackend()});
    }

    @Test public void sqlArithmeticAndIntegerWideningPreserveGetterSemantics() throws Exception {
        var left = new Pipeline.Column(0, "d", ColumnType.DOUBLE);
        var right = new Pipeline.Column(1, "e", ColumnType.DOUBLE);
        var integer = new Pipeline.Column(2, "i", ColumnType.INT);
        var columns = List.of(left, right, integer);
        var row = new Pipeline.Row(columns, List.of(left.value(new Load(MemoryType.F64, COLUMNS)),
                right.value(new Load(MemoryType.F64, at(COLUMNS, 8))), integer.value(new Load(MemoryType.I32, at(COLUMNS, 16)))));
        List<Pipeline.Scalar> expressions = List.of(new Pipeline.Add(left, right, ColumnType.DOUBLE),
                new Pipeline.Subtract(left, right, ColumnType.DOUBLE), new Pipeline.Multiply(left, right, ColumnType.DOUBLE),
                new Pipeline.Divide(left, right), new Pipeline.ToFloating(new Pipeline.Add(integer, new Pipeline.Constant(1), ColumnType.INT)));
        var b = new Builder();
        for (int i = 0; i < expressions.size(); i++) {
            var value = expressions.get(i).emit(row);
            b.store(MemoryType.F64, at(STATE, i * 16), value.data());
            b.store(at(STATE, i * 16 + 8), new Select(value.isNull(), i64(1), i64(0)));
        }
        b.exit(0);
        double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, -0.0,
                Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, 17.25, -1.5};
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath()); var arena = Arena.ofConfined()) {
            long input = arena.allocate(24, 8).address(), output = arena.allocate(80, 8).address();
            for (double a : values) for (double c : values) for (int integerValue : new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 17}) {
                Unsafe.putDouble(input, a); Unsafe.putDouble(input + 8, c); Unsafe.putInt(input + 16, integerValue);
                assertEquals(0, code.run(input, 1, output));
                double quotient = a / c;
                int sum = integerValue + 1;
                double widened = integerValue == Integer.MIN_VALUE || sum == Integer.MIN_VALUE ? Double.NaN : sum;
                double[] expected = {a + c, a - c, a * c, Double.isFinite(quotient) ? quotient : Double.NaN, widened};
                for (int i = 0; i < expected.length; i++) {
                    assertEquals(Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(Unsafe.getDouble(output + i * 16)));
                    assertEquals(Double.isFinite(expected[i]) ? 0 : 1, Unsafe.getLong(output + i * 16 + 8));
                }
            }
        }
    }

    @Test public void floatingSelectionKeepsSqlToleranceNullsAndGuardedTails() throws Throwable {
        var d = new Pipeline.Column(0, "d", ColumnType.DOUBLE);
        var i = new Pipeline.Column(1, "i", ColumnType.INT);
        var converted = new Pipeline.ToFloating(i);
        double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, 0.0,
                1, 1 + Numbers.DOUBLE_TOLERANCE / 2, 1 + Numbers.DOUBLE_TOLERANCE * 2, -17, Double.MIN_VALUE};
        for (Op op : new Op[]{Op.EQ, Op.NE, Op.LT, Op.GE}) {
            var plan = ScanPlan.of(List.of(d, i), new Pipeline.FloatingCompare(op, d, converted));
            try (var code = backend.compile(ScanLowering.selection(plan), temporary.newFolder().toPath());
                 var arena = Arena.ofConfined(); var memory = KernelMemory.bind(null, () -> { })) {
                long descriptor = arena.allocate(16, 8).address(), output = arena.allocate(130 * 8, 8).address();
                for (int rows : new int[]{0, 1, 7, 8, 9, 63, 64, 65, 129}) {
                    try (var doubles = new ScanStageTest.GuardedPage(rows * 8); var integers = new ScanStageTest.GuardedPage(rows * 4)) {
                        for (int n = 0; n < rows; n++) {
                            Unsafe.putDouble(doubles.address + n * 8L, values[n % values.length]);
                            Unsafe.putInt(integers.address + n * 4L, n % 11 == 0 ? Integer.MIN_VALUE : 1);
                        }
                        for (boolean top : new boolean[]{false, true}) {
                            Unsafe.putLong(descriptor, top ? 0 : doubles.address); Unsafe.putLong(descriptor + 8, integers.address);
                            Unsafe.putLong(output + rows * 8L, -7);
                            long count = code.run(descriptor, rows, output), expected = 0;
                            for (int n = 0; n < rows; n++) {
                                double a = top ? Double.NaN : values[n % values.length], c = n % 11 == 0 ? Double.NaN : 1;
                                boolean eq = Numbers.equals(a, c);
                                boolean selected = switch (op) { case EQ -> eq; case NE -> !eq; case LT -> !eq && a < c;
                                    case GE -> eq || a > c; default -> throw new AssertionError(); };
                                if (selected) { assertTrue(expected < count); assertEquals(n, Unsafe.getLong(output + expected++ * 8)); }
                            }
                            assertEquals(expected, count);
                            assertEquals(-7, Unsafe.getLong(output + rows * 8L));
                        }
                    }
                }
            }
        }
    }

    @Test public void ieeeComparisonsAndBitCastsPreserveExceptionalValues() throws Exception {
        var b = new Builder();
        Var left = b.let(new Load(MemoryType.F64, COLUMNS));
        Var right = b.let(new Load(MemoryType.F64, at(COLUMNS, 8)));
        Op[] operations = {Op.FEQ, Op.FNE, Op.FLT, Op.FGE};
        for (int i = 0; i < operations.length; i++) {
            b.store(at(STATE, i * 8), new Select(binary(operations[i], left, right), i64(1), i64(0)));
            b.store(at(STATE, 32 + i * 8), new Select(binary(operations[i], left, left), i64(1), i64(0)));
        }
        b.store(at(STATE, 64), new BitCast(left));
        b.store(MemoryType.F64, at(STATE, 72), new BitCast(new BitCast(right)));
        b.store(at(STATE, 80), new Select(binary(Op.FEQ, f64(-0.0), f64(0.0)), i64(1), i64(0)));
        b.store(at(STATE, 88), new Select(binary(Op.FNE, f64(Double.NaN), f64(Double.NaN)), i64(1), i64(0)));
        b.store(at(STATE, 96), new Select(binary(Op.FGE, f64(Double.NaN), f64(0)), i64(1), i64(0)));
        b.exit(0);
        long[] bits = {0, Long.MIN_VALUE, 1, Long.MIN_VALUE + 1, 0x0010000000000000L,
                Double.doubleToRawLongBits(-17.25), Double.doubleToRawLongBits(17.25),
                0x7fefffffffffffffL, 0xffefffffffffffffL, 0x7ff0000000000000L,
                0xfff0000000000000L, 0x7ff8000000000000L, 0x7ff0000000000001L, 0xfff8000000001234L};
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long input = arena.allocate(16, 8).address(), output = arena.allocate(104, 8).address();
            for (long x : bits) for (long y : bits) {
                Unsafe.putLong(input, x); Unsafe.putLong(input + 8, y);
                assertEquals(0, code.run(input, 1, output));
                double a = Double.longBitsToDouble(x), c = Double.longBitsToDouble(y);
                boolean[] expected = {a == c, a != c, a < c, a >= c, a == a, a != a, a < a, a >= a};
                for (int i = 0; i < expected.length; i++) assertEquals(expected[i] ? 1 : 0, Unsafe.getLong(output + i * 8));
                assertEquals(x, Unsafe.getLong(output + 64));
                assertEquals(y, Unsafe.getLong(output + 72));
                assertEquals(1, Unsafe.getLong(output + 80));
                assertEquals(1, Unsafe.getLong(output + 88));
                assertEquals(0, Unsafe.getLong(output + 96));
            }
        }
    }
}
