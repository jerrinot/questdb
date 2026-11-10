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

import java.lang.foreign.Arena;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class FloatingIrTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend = new LlvmBackend();

    @Test public void floatingRemainderMatchesJavaForConstantsScalarsAndLanes() throws Throwable {
        double[] edges={Double.NaN,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,-0.0,0,
                Double.MIN_VALUE,-Double.MIN_VALUE,Double.MIN_NORMAL,Double.MAX_VALUE,-Double.MAX_VALUE,
                Float.MIN_VALUE,-Float.MIN_VALUE,Float.MIN_NORMAL,Float.MAX_VALUE,
                1,-1,3,-3,17.25,-17.25,Math.nextUp(1.0),Math.nextDown(1.0),0x1p100,-0x1p100};
        var random=new java.util.Random(0x5a17f00dL);
        for(MemoryType width:List.of(MemoryType.F32,MemoryType.F64)) {
            var b=new Builder();
            Expr left=new Load(width,COLUMNS),right=new Load(width,at(COLUMNS,64));
            b.store(width,STATE,binary(Op.FREM,left,right));
            b.store(at(STATE,8),new VectorMask(8,binary(Op.FLT,binary(Op.FREM,left,right),width==MemoryType.F32?f32(0):f64(0))));
            for(int i=0;i<edges.length;i++) {
                double divisor=edges[(i+5)%edges.length];
                b.store(width,at(STATE,16+i*8L),binary(Op.FREM,
                        width==MemoryType.F32?f32((float)edges[i]):f64(edges[i]),
                        width==MemoryType.F32?f32((float)divisor):f64(divisor)));
            }
            b.exit(0);
            try(var code=backend.compile(new Program(b.block()),temporary.newFolder().toPath());var arena=Arena.ofConfined()) {
                long input=arena.allocate(128,8).address(),output=arena.allocate(16+edges.length*8L,8).address();
                for(int sample=0;sample<edges.length*edges.length+512;sample++) {
                    double a=sample<edges.length*edges.length?edges[sample/edges.length]:Double.longBitsToDouble(random.nextLong());
                    double c=sample<edges.length*edges.length?edges[sample%edges.length]:Double.longBitsToDouble(random.nextLong());
                    long mask=0;
                    for(int lane=0;lane<8;lane++) {
                        double x=lane==0?a:edges[(sample+lane)%edges.length],y=lane==0?c:edges[(sample/edges.length+lane)%edges.length];
                        if(width==MemoryType.F32) {
                            Unsafe.putFloat(input+lane*4L,(float)x);Unsafe.putFloat(input+64+lane*4L,(float)y);
                            if((float)x%(float)y<0)mask|=1L<<lane;
                        } else {
                            Unsafe.putDouble(input+lane*8L,x);Unsafe.putDouble(input+64+lane*8L,y);
                            if(x%y<0)mask|=1L<<lane;
                        }
                    }
                    assertEquals(0,code.run(input,8,output));
                    if(width==MemoryType.F32) assertEquals(Float.floatToIntBits((float)a%(float)c),Float.floatToIntBits(Unsafe.getFloat(output)));
                    else assertEquals(Double.doubleToLongBits(a%c),Double.doubleToLongBits(Unsafe.getDouble(output)));
                    assertEquals(mask,Unsafe.getLong(output+8));
                    for(int i=0;i<edges.length;i++) {
                        double divisor=edges[(i+5)%edges.length];
                        if(width==MemoryType.F32) assertEquals(Float.floatToIntBits((float)edges[i]%(float)divisor),Float.floatToIntBits(Unsafe.getFloat(output+16+i*8L)));
                        else assertEquals(Double.doubleToLongBits(edges[i]%divisor),Double.doubleToLongBits(Unsafe.getDouble(output+16+i*8L)));
                    }
                }
            }
        }
    }

    @Test public void floatingIntegerConversionMatchesJavaForScalarConstantsAndLanes() throws Throwable {
        double[] edges={Double.NaN,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,-0.0,0,
                Double.MIN_VALUE,-Double.MIN_VALUE,Double.MAX_VALUE,-Double.MAX_VALUE,1.9,-1.9,
                Math.nextDown(0x1p31),0x1p31,Math.nextUp(0x1p31),-0x1p31,Math.nextDown(-0x1p31),
                Math.nextDown(0x1p63),0x1p63,Math.nextUp(0x1p63),-0x1p63,Math.nextDown(-0x1p63),
                Math.nextDown(0x1p31f),Math.nextUp(0x1p31f),Math.nextDown(0x1p63f),Math.nextUp(0x1p63f),
                Double.longBitsToDouble(0x7ff0000000000001L),Double.longBitsToDouble(0xfff8000000001234L)};
        var random=new java.util.Random(0x5a17c0deL);
        double[] values=java.util.Arrays.copyOf(edges,edges.length+512);
        for(int i=edges.length;i<values.length;i++) values[i]=Double.longBitsToDouble(random.nextLong());
        for(MemoryType width:List.of(MemoryType.F32,MemoryType.F64)) for(int bits:new int[]{32,64}) {
            var b=new Builder();
            b.store(STATE,new ToInteger(new Load(width,COLUMNS),bits));
            b.store(at(STATE,8),new VectorMask(8,binary(Op.LT,new ToInteger(new Load(width,COLUMNS),bits),i64(0))));
            for(int i=0;i<edges.length;i++) b.store(at(STATE,16+i*8L),new ToInteger(
                    width==MemoryType.F32?f32((float)edges[i]):f64(edges[i]),bits));
            b.exit(0);
            try(var code=backend.compile(new Program(b.block()),temporary.newFolder().toPath());var arena=Arena.ofConfined()) {
                long input=arena.allocate(64,8).address(),output=arena.allocate(16+edges.length*8L,8).address();
                for(int i=0;i<values.length;i++) {
                    long mask=0;
                    for(int lane=0;lane<8;lane++) {
                        double value=values[(i+lane)%values.length];
                        if(width==MemoryType.F32) {value=(float)value;Unsafe.putFloat(input+lane*4L,(float)value);}
                        else Unsafe.putDouble(input+lane*8L,value);
                        if((bits==32?(int)value:(long)value)<0)mask|=1L<<lane;
                    }
                    assertEquals(0,code.run(input,8,output));
                    double value=width==MemoryType.F32?(float)values[i]:values[i];
                    assertEquals(width+"/"+bits+"/"+value,bits==32?(int)value:(long)value,Unsafe.getLong(output));
                    assertEquals(mask,Unsafe.getLong(output+8));
                    for(int j=0;j<edges.length;j++) {
                        value=width==MemoryType.F32?(float)edges[j]:edges[j];
                        assertEquals(bits==32?(int)value:(long)value,Unsafe.getLong(output+16+j*8L));
                    }
                }
            }
        }
    }

    @Test public void squareRootMatchesJavaForScalarConstantAndLaneInputs() throws Throwable {
        double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, 0,
                Double.MIN_VALUE, -Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE,
                -1, 1, 2, 4, Math.nextUp(4.0), Math.nextDown(4.0)};
        var b = new Builder();
        b.store(MemoryType.F64, STATE, new Sqrt(new Load(MemoryType.F64, COLUMNS)));
        b.store(at(STATE, 8), new VectorMask(8, binary(Op.FLT,
                new Sqrt(new Load(MemoryType.F64, COLUMNS)), f64(2))));
        for (int i = 0; i < values.length; i++)
            b.store(MemoryType.F64, at(STATE, 16 + i * 8L), new Sqrt(f64(values[i])));
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long input = arena.allocate(64, 8).address(), output = arena.allocate(16 + values.length * 8L, 8).address();
            for (int i = 0; i < values.length; i++) {
                long mask = 0;
                for (int lane = 0; lane < 8; lane++) {
                    double value = values[(i + lane) % values.length];
                    Unsafe.putDouble(input + lane * 8L, value);
                    if (Math.sqrt(value) < 2) mask |= 1L << lane;
                }
                assertEquals(0, code.run(input, 8, output));
                assertEquals(Double.doubleToLongBits(Math.sqrt(values[i])), Double.doubleToLongBits(Unsafe.getDouble(output)));
                assertEquals(mask, Unsafe.getLong(output + 8));
                for (int j = 0; j < values.length; j++)
                    assertEquals(Double.doubleToLongBits(Math.sqrt(values[j])), Double.doubleToLongBits(Unsafe.getDouble(output + 16 + j * 8L)));
            }
        }
    }

    @Test public void floatBitsSurviveLoadsSelectsRegistersStoresAndLanes() throws Throwable {
        int[] bits = {0, 0x80000000, 0x7f800000, 0xff800000, 0x7fc01234, 0xffc05678,
                0x7f800001, 0xff800001, 1, 0x80000001, 0x7f7fffff, 0x3f800000};
        var b = new Builder();
        Var value = b.let(new Load(MemoryType.F32, COLUMNS));
        Var selected = b.let(new Select(eq(ROWS, i64(0)), new Literal(0x7f800001, Type.F32), value));
        b.store(MemoryType.F32, STATE, selected);
        b.store(at(STATE, 8), new BitCast(selected));
        b.store(at(STATE, 16), new BitCast(new BitCast(load(at(COLUMNS, 32)), Type.F32)));
        b.store(MemoryType.F32, at(STATE, 24), new BitCast(new BitCast(selected), Type.F32));
        b.store(at(STATE, 32), new VectorMask(8, eq(new BitCast(new Load(MemoryType.F32, COLUMNS)), i64(0x7f800001))));
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long input = arena.allocate(40, 8).address(), output = arena.allocate(40, 8).address();
            for (int i = 0; i < bits.length; i++) for (int rows : new int[]{0, 1}) {
                long mask = 0;
                for (int lane = 0; lane < 8; lane++) {
                    int raw = bits[(i+lane)%bits.length]; Unsafe.putInt(input+lane*4L, raw);
                    if (raw == 0x7f800001) mask |= 1L<<lane;
                }
                Unsafe.putLong(input+32, 0x1234567800000000L | Integer.toUnsignedLong(bits[i]));
                assertEquals(0, code.run(input, rows, output));
                int expected = rows == 0 ? 0x7f800001 : bits[i];
                assertEquals(expected, Unsafe.getInt(output)); assertEquals((long) expected, Unsafe.getLong(output+8));
                assertEquals((long) bits[i], Unsafe.getLong(output+16)); assertEquals(expected, Unsafe.getInt(output+24));
                assertEquals(mask, Unsafe.getLong(output+32));
            }
        }
    }

    @Test public void binary32ArithmeticMatchesJavaIncludingSubnormalsAndOverflow() throws Throwable {
        var operations = List.of(Op.FADD, Op.FSUB, Op.FMUL, Op.FDIV);
        float[] values = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, -0.0f, 0,
                Float.MIN_VALUE, -Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE, -Float.MAX_VALUE,
                0x1p24f, 1, 3, Math.nextUp(1.0f), Math.nextDown(1.0f), -17.25f};
        var b = new Builder();
        for (int i = 0; i < operations.size(); i++) {
            b.store(MemoryType.F64, at(STATE, i * 8L), new ToDouble(binary(operations.get(i),
                    new ToFloat(new Load(MemoryType.F64, COLUMNS)), new ToFloat(new Load(MemoryType.F64, at(COLUMNS, 8))))));
            b.store(MemoryType.F64, at(STATE, 40 + i * 8L), new ToDouble(binary(operations.get(i), f32(0x1p24f), f32(3))));
        }
        b.store(at(STATE, 32), new VectorMask(8, binary(Op.FLT,
                new ToDouble(binary(Op.FDIV, new Load(MemoryType.F32, at(COLUMNS, 16)), f32(3))), f64(0.01))));
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long input = arena.allocate(48, 8).address(), output = arena.allocate(72, 8).address();
            float[] constantResults = {0x1p24f + 3, 0x1p24f - 3, 0x1p24f * 3, 0x1p24f / 3};
            for (int a = 0; a < values.length; a++) for (float right : values) {
                float left = values[a];
                Unsafe.putDouble(input, left); Unsafe.putDouble(input + 8, right);
                long mask = 0;
                for (int lane = 0; lane < 8; lane++) {
                    float value = values[(a + lane) % values.length];
                    Unsafe.putFloat(input + 16 + lane * 4L, value);
                    if (value / 3 < 0.01) mask |= 1L << lane;
                }
                assertEquals(0, code.run(input, 8, output));
                float[] results = {left + right, left - right, left * right, left / right};
                for (int i = 0; i < results.length; i++) {
                    assertEquals(operations.get(i).name(), Double.doubleToLongBits(results[i]),
                            Double.doubleToLongBits(Unsafe.getDouble(output + i * 8L)));
                    assertEquals(Double.doubleToLongBits(constantResults[i]), Double.doubleToLongBits(Unsafe.getDouble(output + 40 + i * 8L)));
                }
                assertEquals(mask, Unsafe.getLong(output + 32));
            }
        }
    }

    @Test public void floatConversionRoundsRegistersConstantsAndLanes() throws Throwable {
        double[] doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                -0.0, 0, Double.MIN_VALUE, -Double.MIN_VALUE, Float.MIN_VALUE / 2.0,
                Math.nextUp(Float.MIN_VALUE / 2.0), Float.MIN_NORMAL, Float.MAX_VALUE,
                Double.MAX_VALUE, 1.0 + 0x1p-24, Math.nextUp(1.0 + 0x1p-24)};
        long[] integers = {Long.MIN_VALUE, Long.MAX_VALUE, 0, 1, -1,
                (1L << 53) + (1L << 29) + 1, -((1L << 53) + (1L << 29) + 1)};
        var b = new Builder();
        b.store(MemoryType.F64, STATE, new ToDouble(new ToFloat(new Load(MemoryType.F64, COLUMNS))));
        b.store(MemoryType.F64, at(STATE, 8), new ToDouble(new ToFloat(load(at(COLUMNS, 8)))));
        for (int i = 0; i < doubles.length; i++)
            b.store(MemoryType.F64, at(STATE, 16 + 8L * i), new ToDouble(new ToFloat(f64(doubles[i]))));
        for (int i = 0; i < integers.length; i++)
            b.store(MemoryType.F64, at(STATE, 16 + 8L * (doubles.length + i)), new ToDouble(new ToFloat(i64(integers[i]))));
        b.store(at(STATE, 256), new VectorMask(8,
                binary(Op.FLT, new ToFloat(new Load(MemoryType.F64, COLUMNS)), f32(1))));
        b.store(at(STATE, 264), new VectorMask(8,
                binary(Op.FLT, new ToFloat(load(COLUMNS)), f32(1))));
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long input = arena.allocate(64, 8).address(), output = arena.allocate(272, 8).address();
            for (double value : doubles) for (long integer : integers) {
                for (int lane = 0; lane < 8; lane++) Unsafe.putDouble(input + lane * 8L, doubles[(lane + 3) % doubles.length]);
                Unsafe.putDouble(input, value); Unsafe.putLong(input + 8, integer);
                assertEquals(0, code.run(input, 8, output));
                assertEquals(Double.doubleToLongBits((float) value), Double.doubleToLongBits(Unsafe.getDouble(output)));
                assertEquals(Double.doubleToLongBits((float) integer), Double.doubleToLongBits(Unsafe.getDouble(output + 8)));
                for (int i = 0; i < doubles.length; i++)
                    assertEquals(Double.doubleToLongBits((float) doubles[i]),
                            Double.doubleToLongBits(Unsafe.getDouble(output + 16 + 8L * i)));
                for (int i = 0; i < integers.length; i++)
                    assertEquals(Double.doubleToLongBits((float) integers[i]),
                            Double.doubleToLongBits(Unsafe.getDouble(output + 16 + 8L * (doubles.length + i))));
                long doubleMask = 0, integerMask = 0;
                for (int lane = 0; lane < 8; lane++) {
                    if ((float) Unsafe.getDouble(input + lane * 8L) < 1) doubleMask |= 1L << lane;
                    if ((float) Unsafe.getLong(input + lane * 8L) < 1) integerMask |= 1L << lane;
                }
                assertEquals(doubleMask, Unsafe.getLong(output + 256));
                assertEquals(integerMask, Unsafe.getLong(output + 264));
            }
        }
    }

    @Test public void floatMemoryRetainsPrecisionAndConvertsWithoutOverreading() throws Throwable {
        var b = new Builder();
        b.store(MemoryType.F32, STATE, new Load(MemoryType.F32, COLUMNS));
        b.store(MemoryType.F64, at(STATE, 8), new ToDouble(new Load(MemoryType.F32, COLUMNS)));
        b.store(MemoryType.F32, at(STATE, 16), new ToFloat(new Load(MemoryType.F64, at(STATE, 24))));
        var floatColumn = new Pipeline.Column(0, "f", ColumnType.FLOAT);
        var floatRow = new Pipeline.Row(List.of(floatColumn), List.of(floatColumn.value(new Load(MemoryType.F32, COLUMNS))));
        b.store(MemoryType.F64, at(STATE, 32), new ToDouble(new Pipeline.Negate(floatColumn, ColumnType.FLOAT).emit(floatRow).data()));
        b.exit(0);
        float[] values = {Float.NaN, Float.intBitsToFloat(0xffc01234), Float.POSITIVE_INFINITY,
                Float.NEGATIVE_INFINITY, -0.0f, 0.0f, Float.MIN_VALUE, -Float.MIN_VALUE,
                Float.MIN_NORMAL, Float.MAX_VALUE, -17.25f, 1.0000001f};
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var input = new ScanStageTest.GuardedPage(4); var arena = Arena.ofConfined()) {
            long output = arena.allocate(40, 8).address();
            for (float value : values) {
                Unsafe.putFloat(input.address, value);
                Unsafe.putInt(output + 4, 0x12345678);
                Unsafe.putDouble(output + 24, (double) value / 3);
                assertEquals(0, code.run(input.address, 1, output));
                assertEquals(Float.floatToIntBits(value), Float.floatToIntBits(Unsafe.getFloat(output)));
                assertEquals(Double.doubleToLongBits(value), Double.doubleToLongBits(Unsafe.getDouble(output + 8)));
                assertEquals(Float.floatToIntBits((float) ((double) value / 3)), Float.floatToIntBits(Unsafe.getFloat(output + 16)));
                assertEquals(0x12345678, Unsafe.getInt(output + 4));
                assertEquals(Double.doubleToLongBits(-value), Double.doubleToLongBits(Unsafe.getDouble(output + 32)));
            }
        }
        var column = new Pipeline.Column(0, "f", io.questdb.cairo.ColumnType.FLOAT);
        var plan = ScanPlan.of(List.of(column), new Pipeline.FloatingCompare(Op.GE, new Pipeline.ToFloating(column), new Pipeline.FloatingConstant(0)));
        try (var code = backend.compile(ScanLowering.selection(plan), temporary.newFolder().toPath());
             var arena = Arena.ofConfined(); var memory = KernelMemory.bind(null, () -> { })) {
            long descriptor = arena.allocate(8, 8).address(), output = arena.allocate(130 * 8, 8).address();
            for (int rows : new int[]{0, 1, 7, 8, 9, 63, 64, 65, 129}) {
                try (var input = new ScanStageTest.GuardedPage(rows * 4)) {
                    for (int n = 0; n < rows; n++) Unsafe.putFloat(input.address + n * 4L, values[n % values.length]);
                    for (boolean top : new boolean[]{false, true}) {
                        Unsafe.putLong(descriptor, top ? 0 : input.address);
                        Unsafe.putLong(output + rows * 8L, -7);
                        long count = code.run(descriptor, rows, output), expected = 0;
                        for (int n = 0; n < rows; n++) {
                            double value = top ? Double.NaN : values[n % values.length];
                            if (Numbers.equals(value, 0) || value > 0) {
                                assertTrue(expected < count);
                                assertEquals(n, Unsafe.getLong(output + expected++ * 8));
                            }
                        }
                        assertEquals(expected, count);
                        assertEquals(-7, Unsafe.getLong(output + rows * 8L));
                    }
                }
            }
        }
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
                new Pipeline.Divide(left, right), new Pipeline.ToFloating(new Pipeline.Add(integer, new Pipeline.Constant(1), ColumnType.INT)),
                new Pipeline.Negate(left, ColumnType.DOUBLE), new Pipeline.ToFloating(new Pipeline.Negate(integer, ColumnType.INT)));
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
            long input = arena.allocate(24, 8).address(), output = arena.allocate(expressions.size() * 16L, 8).address();
            for (double a : values) for (double c : values) for (int integerValue : new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 17}) {
                Unsafe.putDouble(input, a); Unsafe.putDouble(input + 8, c); Unsafe.putInt(input + 16, integerValue);
                assertEquals(0, code.run(input, 1, output));
                double quotient = a / c;
                int sum = integerValue + 1;
                double widened = integerValue == Integer.MIN_VALUE || sum == Integer.MIN_VALUE ? Double.NaN : sum;
                double[] expected = {a + c, a - c, a * c, Double.isFinite(quotient) ? quotient : Double.NaN, widened,
                        -a, integerValue == Integer.MIN_VALUE ? Double.NaN : -integerValue};
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
