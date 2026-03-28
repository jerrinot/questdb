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

package io.questdb.test.jit;

import io.questdb.cairo.JitBackend;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.jit.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import jdk.incubator.vector.LongVector;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.jit.CompiledFilterIRSerializer.*;

/**
 * Tests VectorBytecodeFilterCompiler by comparing vectorized output
 * against the scalar bytecode compiler (oracle).
 */
public class VectorBytecodeFilterCompilerTest {

    // Options: log2(8)=3 in bits 1-3, single-size hint in bits 4-5, no null checks
    private static final int LONG_OPTIONS = (3 << 1) | (1 << 4);
    // Same but with null checks enabled (bit 6)
    private static final int LONG_NULL_OPTIONS = (3 << 1) | (1 << 4) | (1 << 6);

    // Number of rows: 3 full vectors + partial tail to exercise tail handling
    private static final int ROW_COUNT = LongVector.SPECIES_PREFERRED.length() * 3 + 2;

    @Test
    public void testLongGt() throws Exception {
        // col0 > 42
        assertParity(
                longCol(ROW_COUNT, i -> i * 10L),
                ir(
                        insn(IMM, I8_TYPE, 42, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(GT, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testLongEq() throws Exception {
        // col0 == 50
        assertParity(
                longCol(ROW_COUNT, i -> i * 10L),
                ir(
                        insn(IMM, I8_TYPE, 50, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(EQ, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testLongLt() throws Exception {
        // col0 < 30
        assertParity(
                longCol(ROW_COUNT, i -> i * 10L),
                ir(
                        insn(IMM, I8_TYPE, 30, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(LT, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testLongAndTwoCols() throws Exception {
        // col0 > 20 AND col1 < 80
        long[] data0 = longCol(ROW_COUNT, i -> i * 10L);
        long[] data1 = longCol(ROW_COUNT, i -> i * 5L);
        assertParityTwoCols(data0, data1, ir(
                insn(IMM, I8_TYPE, 20, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(IMM, I8_TYPE, 80, 0),
                insn(MEM, I8_TYPE, 1, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testLongCountOnly() throws Exception {
        // col0 > 42 (count-only)
        long[] data = longCol(ROW_COUNT, i -> i * 10L);
        assertCountParity(data, ir(
                insn(IMM, I8_TYPE, 42, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testLongIn5() throws Exception {
        long[] data = longCol(ROW_COUNT, i -> (i % 7) + 1L);
        assertParity(data, ir(
                insn(IMM, I8_TYPE, 1, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(IMM, I8_TYPE, 2, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(IMM, I8_TYPE, 3, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(IMM, I8_TYPE, 4, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(IMM, I8_TYPE, 5, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testLongIn5CountOnly() throws Exception {
        long[] data = longCol(ROW_COUNT, i -> (i % 7) + 1L);
        assertCountParity(data, ir(
                insn(IMM, I8_TYPE, 1, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(IMM, I8_TYPE, 2, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(IMM, I8_TYPE, 3, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(IMM, I8_TYPE, 4, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(IMM, I8_TYPE, 5, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testLongNe() throws Exception {
        // col0 != 50
        assertParity(
                longCol(ROW_COUNT, i -> i * 10L),
                ir(
                        insn(IMM, I8_TYPE, 50, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(NE, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testLongOr() throws Exception {
        // col0 < 20 OR col0 > 80
        assertParity(
                longCol(ROW_COUNT, i -> i * 10L),
                ir(
                        insn(IMM, I8_TYPE, 20, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(LT, 0, 0, 0),
                        insn(IMM, I8_TYPE, 80, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(GT, 0, 0, 0),
                        insn(OR, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testLongArithmetic() throws Exception {
        // (col0 + col0) > 100
        assertParity(
                longCol(ROW_COUNT, i -> i * 10L),
                ir(
                        insn(IMM, I8_TYPE, 100, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(ADD, 0, 0, 0),
                        insn(GT, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testAllMatch() throws Exception {
        // col0 >= 0 (everything matches)
        assertParity(
                longCol(ROW_COUNT, i -> (long) i),
                ir(
                        insn(IMM, I8_TYPE, 0, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(GE, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testNoneMatch() throws Exception {
        // col0 < 0 (nothing matches)
        assertParity(
                longCol(ROW_COUNT, i -> (long) i),
                ir(
                        insn(IMM, I8_TYPE, 0, 0),
                        insn(MEM, I8_TYPE, 0, 0),
                        insn(LT, 0, 0, 0),
                        insn(RET, 0, 0, 0)
                )
        );
    }

    @Test
    public void testLongGtNullCheck() throws Exception {
        // col0 > 42 with null checks enabled
        // Include some LONG_NULL values — they should not match
        long[] data = longCol(ROW_COUNT, i -> i == 3 || i == 7 ? io.questdb.std.Numbers.LONG_NULL : i * 10L);
        assertParityWithOptions(data, ir(
                insn(IMM, I8_TYPE, 42, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), LONG_NULL_OPTIONS);
    }

    @Test
    public void testLongLeNullCheck() throws Exception {
        // col0 <= 50 with null checks — NULL <= NULL should be true
        long[] data = longCol(ROW_COUNT, i -> i == 2 || i == 5 ? io.questdb.std.Numbers.LONG_NULL : i * 10L);
        assertParityWithOptions(data, ir(
                insn(IMM, I8_TYPE, 50, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(LE, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), LONG_NULL_OPTIONS);
    }

    @Test
    public void testMixedDoubleFirstThenLongNullOrdered() throws Exception {
        // F8 column first, then I8 ordered null comparison.
        // This exercises the bug where nullVecSlot was DoubleVector
        // but longNullGt expects LongVector.
        double[] dblData = new double[ROW_COUNT];
        long[] longData = longCol(ROW_COUNT, i -> i == 3 ? io.questdb.std.Numbers.LONG_NULL : i * 10L);
        for (int i = 0; i < ROW_COUNT; i++) dblData[i] = i * 0.1;

        int mixedOptions = (3 << 1) | (1 << 4) | (1 << 6); // log2(8), single-size, null checks

        // col0(double) < 0.5 AND col1(long) > 25
        // F8 is loaded FIRST — primaryType would have been F8 before the fix.
        IrDecoder.Instruction[] instructions = ir(
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                insn(MEM, F8_TYPE, 0, 0),
                insn(LT, 0, 0, 0),
                insn(IMM, I8_TYPE, 25, 0),
                insn(MEM, I8_TYPE, 1, 0),
                insn(GT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        );

        int len = ROW_COUNT;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long col0 = Unsafe.malloc((long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long col1 = Unsafe.malloc((long) len * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(col0 + (long) i * Double.BYTES, dblData[i]);
            Unsafe.getUnsafe().putLong(col1 + (long) i * Long.BYTES, longData[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, col0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, col1);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, mixedOptions);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 2, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support mixed F8-first + I8", vector);
            long actual = vector.filterRows(colPtrArray, 2, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(col0, (long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col1, (long) len * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMixedLongAndDouble() throws Exception {
        // col0(long) > 25 AND col1(double) < 0.5
        // Mixed I8+F8 program — both types in one filter
        long[] longData = longCol(ROW_COUNT, i -> i * 10L);
        double[] dblData = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) dblData[i] = i * 0.1;

        // Options: mixed I8+F8 uses single-size (both 8 bytes)
        int mixedOptions = (3 << 1) | (1 << 4) | (1 << 6); // log2(8), single-size, null checks

        IrDecoder.Instruction[] instructions = ir(
                insn(IMM, I8_TYPE, 25, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                insn(MEM, F8_TYPE, 1, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        );

        int len = ROW_COUNT;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long col0 = Unsafe.malloc((long) len * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long col1 = Unsafe.malloc((long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putLong(col0 + (long) i * Long.BYTES, longData[i]);
            Unsafe.getUnsafe().putDouble(col1 + (long) i * Double.BYTES, dblData[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, col0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, col1);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, mixedOptions);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 2, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support mixed I8+F8", vector);
            long actual = vector.filterRows(colPtrArray, 2, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(col0, (long) len * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col1, (long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ========================
    // Arithmetic semantics
    // ========================

    @Test
    public void testLongArithmeticNullAware() throws Exception {
        // (col0 + col0) > 100 with null checks — LONG_NULL should propagate
        long[] data = longCol(ROW_COUNT, i -> i == 4 ? io.questdb.std.Numbers.LONG_NULL : i * 10L);
        assertParityWithOptions(data, ir(
                insn(IMM, I8_TYPE, 100, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(ADD, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), LONG_NULL_OPTIONS);
    }

    @Test
    public void testDoubleArithmeticDivByZero() throws Exception {
        // col0 / col1 > 0.5 — division by zero should produce NaN (not infinity)
        double[] col0Data = new double[ROW_COUNT];
        double[] col1Data = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            col0Data[i] = i * 1.0;
            col1Data[i] = (i % 3 == 0) ? 0.0 : 2.0; // every 3rd row is div-by-zero
        }

        IrDecoder.Instruction[] instructions = ir(
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                insn(MEM, F8_TYPE, 0, 0),
                insn(MEM, F8_TYPE, 1, 0),
                insn(DIV, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        );

        int options = (3 << 1) | (1 << 4); // double, single-size, no null checks
        int len = ROW_COUNT;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long col0 = Unsafe.malloc((long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long col1 = Unsafe.malloc((long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(col0 + (long) i * Double.BYTES, col0Data[i]);
            Unsafe.getUnsafe().putDouble(col1 + (long) i * Double.BYTES, col1Data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, col0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, col1);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 2, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull(vector);
            long actual = vector.filterRows(colPtrArray, 2, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(col0, (long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col1, (long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ========================
    // AUTO backend selection
    // ========================

    @Test
    public void testAutoSelectsVectorBytecodeForEligibleProgram() throws Exception {
        // An I8 straight-line program with single-size hint should be compiled
        // by the vectorized bytecode compiler when using AUTO backend.
        IrDecoder.Instruction[] instructions = ir(
                insn(IMM, I8_TYPE, 42, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        );

        try (MemoryCARW irMem = Vm.getCARWInstance(1024, 1, MemoryTag.NATIVE_JIT)) {
            for (IrDecoder.Instruction insn : instructions) {
                irMem.putInt(insn.opcode());
                irMem.putInt(insn.type());
                irMem.putLong(insn.payloadLo());
                irMem.putLong(insn.payloadHi());
            }

            VectorCompiledFilter filter = new VectorCompiledFilter();
            filter.compile(irMem, LONG_OPTIONS, JitBackend.AUTO);

            Assert.assertTrue("AUTO should select vectorized bytecode for eligible I8 program",
                    filter.usesVectorBytecode());
        }
    }

    @Test
    public void testAutoFallsBackToScalarForControlFlow() throws Exception {
        // A program with short-circuit (IN) should fall back to scalar bytecode.
        // Use BEGIN_SC/AND_SC/END_SC to create control flow.
        IrDecoder.Instruction[] instructions = ir(
                insn(CompiledFilterIRSerializer.BEGIN_SC, 0, 2, 0),
                insn(IMM, I8_TYPE, 1, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(CompiledFilterIRSerializer.OR_SC, 0, 2, 0),
                insn(IMM, I8_TYPE, 2, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(CompiledFilterIRSerializer.END_SC, 0, 2, 0),
                insn(RET, 0, 0, 0)
        );

        try (MemoryCARW irMem = Vm.getCARWInstance(1024, 1, MemoryTag.NATIVE_JIT)) {
            for (IrDecoder.Instruction insn : instructions) {
                irMem.putInt(insn.opcode());
                irMem.putInt(insn.type());
                irMem.putLong(insn.payloadLo());
                irMem.putLong(insn.payloadHi());
            }

            VectorCompiledFilter filter = new VectorCompiledFilter();
            filter.compile(irMem, LONG_OPTIONS, JitBackend.AUTO);

            Assert.assertFalse("Control-flow program should NOT use vectorized bytecode",
                    filter.usesVectorBytecode());
            Assert.assertTrue("Control-flow program should fall back to scalar bytecode",
                    filter.usesBytecode());
        }
    }

    @Test
    public void testEmptyInput() throws Exception {
        long[] data = new long[0];
        assertParity(data, ir(
                insn(IMM, I8_TYPE, 42, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    // ========================
    // Helpers
    // ========================

    @FunctionalInterface
    interface LongGenerator {
        long generate(int index);
    }

    private static long[] longCol(int count, LongGenerator gen) {
        long[] data = new long[count];
        for (int i = 0; i < count; i++) {
            data[i] = gen.generate(i);
        }
        return data;
    }

    private void assertParityWithOptions(long[] data, IrDecoder.Instruction[] instructions, int options) throws Exception {
        int len = data.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc(Math.max(1, (long) len * Long.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putLong(colData + (long) i * Long.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 1, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support this program", vector);
            long actual = vector.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Long.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertParity(long[] data, IrDecoder.Instruction[] instructions) throws Exception {
        int len = data.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc(Math.max(1, (long) len * Long.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        // Output buffer: extra room for compress store (writes full vector past last match)
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putLong(colData + (long) i * Long.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            // Scalar oracle
            LoweredProgram prog = IrLowering.lower(instructions, LONG_OPTIONS);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 1, 0, 0, 0, expectedBuf, len);

            // Vectorized under test
            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support this program", vector);
            long actual = vector.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Long.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertParityTwoCols(long[] data0, long[] data1, IrDecoder.Instruction[] instructions) throws Exception {
        Assert.assertEquals(data0.length, data1.length);
        int len = data0.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long col0 = allocLongColumn(data0);
        long col1 = allocLongColumn(data1);
        long colPtrArray = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(colPtrArray, col0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, col1);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, LONG_OPTIONS);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 2, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull(vector);
            long actual = vector.filterRows(colPtrArray, 2, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(col0, (long) len * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col1, (long) len * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertCountParity(long[] data, IrDecoder.Instruction[] instructions) throws Exception {
        int len = data.length;
        long colData = Unsafe.malloc(Math.max(1, (long) len * Long.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putLong(colData + (long) i * Long.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, LONG_OPTIONS);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.countRows(colPtrArray, 1, 0, 0, 0, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull(vector);
            long actual = vector.countRows(colPtrArray, 1, 0, 0, 0, len);

            Assert.assertEquals("count mismatch", expected, actual);
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Long.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long allocLongColumn(long[] data) {
        long mem = Unsafe.malloc((long) data.length * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putLong(mem + (long) i * Long.BYTES, data[i]);
        }
        return mem;
    }

    private static long allocIntColumn(int[] data) {
        long mem = Unsafe.malloc((long) data.length * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putInt(mem + (long) i * Integer.BYTES, data[i]);
        }
        return mem;
    }

    private static long allocDoubleColumn(double[] data) {
        long mem = Unsafe.malloc((long) data.length * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < data.length; i++) {
            Unsafe.getUnsafe().putDouble(mem + (long) i * Double.BYTES, data[i]);
        }
        return mem;
    }

    private static IrDecoder.Instruction[] ir(IrDecoder.Instruction... instructions) {
        return instructions;
    }

    private static IrDecoder.Instruction insn(int opcode, int type, long lo, long hi) {
        return new IrDecoder.Instruction(opcode, type, lo, hi);
    }

    // ========================
    // Int (I4) type tests
    // ========================

    private static final int INT_OPTIONS = (2 << 1) | (1 << 4); // log2(4), single-size
    private static final int INT_NULL_OPTIONS = INT_OPTIONS | (1 << 6);

    @Test
    public void testIntCompareSupported() throws Exception {
        int[] data = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = i - 5;
        }
        assertParityIntCol(data, ir(
                insn(IMM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(NE, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testIntCompareSupportedWithNullChecks() throws Exception {
        int[] data = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = (i % 11 == 0) ? Numbers.INT_NULL : i - 5;
        }
        assertParityIntCol(data, ir(
                insn(IMM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(NE, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), INT_NULL_OPTIONS);
    }

    @Test
    public void testIntCompareWithIntNullImmediate() throws Exception {
        int[] data = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = (i % 7 == 0) ? Numbers.INT_NULL : i - 5;
        }
        assertParityIntCol(data, ir(
                insn(IMM, I4_TYPE, Numbers.INT_NULL, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(NE, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), INT_NULL_OPTIONS);
    }

    @Test
    public void testIntInWithNullImmediate() throws Exception {
        int[] data = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = switch (i % 6) {
                case 0 -> Numbers.INT_NULL;
                case 1 -> 3;
                default -> i - 5;
            };
        }
        assertParityIntCol(data, ir(
                insn(IMM, I4_TYPE, 3, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(IMM, I4_TYPE, Numbers.INT_NULL, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), INT_NULL_OPTIONS);
    }

    @Test
    public void testIntArithmetic() throws Exception {
        // (col0 + col0) > 10 — basic I4 arithmetic
        int[] data = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = i - 3;
        }
        assertParityIntCol(data, ir(
                insn(IMM, I4_TYPE, 10, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(ADD, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testIntArithmeticNullAware() throws Exception {
        // (col0 + col0) > 10 with INT_NULL — null propagation
        int[] data = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = (i % 7 == 0) ? Numbers.INT_NULL : i - 3;
        }
        assertParityIntCol(data, ir(
                insn(IMM, I4_TYPE, 10, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(ADD, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), INT_NULL_OPTIONS);
    }

    @Test
    public void testIntNegate() throws Exception {
        // -col0 > 5 — I4 negation
        int[] data = new int[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = i - 10;
        }
        assertParityIntCol(data, ir(
                insn(IMM, I4_TYPE, 5, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(NEG, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testMixedLongIntDoubleBenchmarkShape() throws Exception {
        long[] longData = new long[ROW_COUNT];
        int[] intData = new int[ROW_COUNT];
        double[] doubleData = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            longData[i] = i - (ROW_COUNT / 2L);
            intData[i] = (i % 17) - 8;
            doubleData[i] = i / (double) ROW_COUNT;
        }

        int mixedOptions = (3 << 1) | (2 << 4); // log2(8), mixed-size, no null checks
        assertParityLongIntDouble(longData, intData, doubleData, ir(
                insn(IMM, I8_TYPE, 0, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(IMM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 1, 0),
                insn(NE, 0, 0, 0),
                insn(AND, 0, 0, 0),
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                insn(MEM, F8_TYPE, 2, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(IMM, I8_TYPE, 1_000_000, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), mixedOptions);
    }

    @Test
    public void testMixedLongIntDoubleBenchmarkShapeWithNullChecks() throws Exception {
        long[] longData = new long[ROW_COUNT];
        int[] intData = new int[ROW_COUNT];
        double[] doubleData = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            longData[i] = (i % 23 == 0) ? Numbers.LONG_NULL : i - (ROW_COUNT / 2L);
            intData[i] = (i % 17 == 0) ? Numbers.INT_NULL : (i % 17) - 8;
            doubleData[i] = (i % 19 == 0) ? Double.NaN : i / (double) ROW_COUNT;
        }

        int mixedOptions = (3 << 1) | (2 << 4) | (1 << 6); // mixed-size + null checks
        assertParityLongIntDouble(longData, intData, doubleData, ir(
                insn(IMM, I8_TYPE, 0, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(IMM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 1, 0),
                insn(NE, 0, 0, 0),
                insn(AND, 0, 0, 0),
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                insn(MEM, F8_TYPE, 2, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(IMM, I8_TYPE, 1_000_000, 0),
                insn(MEM, I8_TYPE, 0, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), mixedOptions);
    }

    @Test
    public void testIntDoubleCompareAndWithoutLong() throws Exception {
        // col0(I4) > 0 AND col1(F8) < 0.5 — no I8 column present
        int[] intData = new int[ROW_COUNT];
        double[] doubleData = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            intData[i] = (i % 17) - 8;
            doubleData[i] = i / (double) ROW_COUNT;
        }

        // Test both with and without null checks
        int mixedOptions = (3 << 1) | (2 << 4); // log2(8), mixed-size, no null checks
        assertParityIntDouble(intData, doubleData, ir(
                insn(IMM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                insn(MEM, F8_TYPE, 1, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), mixedOptions);

        int mixedNullOptions = (3 << 1) | (2 << 4) | (1 << 6); // + null checks
        assertParityIntDouble(intData, doubleData, ir(
                insn(IMM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                insn(MEM, F8_TYPE, 1, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), mixedNullOptions);
    }

    // ========================
    // Double (F8) type tests
    // ========================

    private static final int DOUBLE_OPTIONS = (3 << 1) | (1 << 4); // log2(8), single-size
    private static final int DOUBLE_NULL_OPTIONS = (3 << 1) | (1 << 4) | (1 << 6); // + null checks

    @Test
    public void testDoubleEqEpsilon() throws Exception {
        // col0 == 1.0 (epsilon comparison — near-equal values should match)
        double[] data = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = switch (i % 4) {
                case 0 -> 1.0;
                case 1 -> 1.0 + 1e-11; // within epsilon → should match
                case 2 -> 1.0 + 1e-9;  // outside epsilon → should NOT match
                default -> 2.0;
            };
        }
        assertParityDoubleCol(data, ir(
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(1.0), 0),
                insn(MEM, F8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testDoubleNaNHandling() throws Exception {
        // col0 > 3.0 with NaN values — NaN should not match
        double[] data = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = (i % 5 == 0) ? Double.NaN : i * 0.5;
        }
        // Use null-check options since NaN is the double null sentinel
        assertParityDoubleColWithOptions(data, ir(
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(3.0), 0),
                insn(MEM, F8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), DOUBLE_NULL_OPTIONS);
    }

    @Test
    public void testDoubleGt() throws Exception {
        // col0 > 3.14
        double[] data = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) data[i] = i * 0.5;
        assertParityDoubleCol(data, ir(
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(3.14), 0),
                insn(MEM, F8_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testDoubleCountOnly() throws Exception {
        // col0 < 2.0
        double[] data = new double[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) data[i] = i * 0.3;
        assertCountParityDoubleCol(data, ir(
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(2.0), 0),
                insn(MEM, F8_TYPE, 0, 0),
                insn(LT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    // ========================
    // Float (F4) type tests
    // ========================

    private static final int FLOAT_OPTIONS = (2 << 1) | (1 << 4); // log2(4), single-size
    private static final int FLOAT_NULL_OPTIONS = FLOAT_OPTIONS | (1 << 6);

    @Test
    public void testFloatGt() throws Exception {
        float[] data = new float[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = i * 1.5f;
        }
        assertParityFloatCol(data, ir(
                new IrDecoder.Instruction(IMM, F4_TYPE, Float.floatToRawIntBits(10.0f), 0),
                insn(MEM, F4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testFloatEqEpsilon() throws Exception {
        float[] data = new float[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = switch (i % 4) {
                case 0 -> 1.0f;
                case 1 -> 1.0f + 1e-11f; // within epsilon
                case 2 -> 1.0f + 1e-9f;  // outside epsilon
                default -> 2.0f;
            };
        }
        assertParityFloatCol(data, ir(
                new IrDecoder.Instruction(IMM, F4_TYPE, Float.floatToRawIntBits(1.0f), 0),
                insn(MEM, F4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    @Test
    public void testFloatNaNHandling() throws Exception {
        float[] data = new float[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = (i % 5 == 0) ? Float.NaN : i * 0.5f;
        }
        assertParityFloatColWithOptions(data, ir(
                new IrDecoder.Instruction(IMM, F4_TYPE, Float.floatToRawIntBits(3.0f), 0),
                insn(MEM, F4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), FLOAT_NULL_OPTIONS);
    }

    @Test
    public void testFloatArithmetic() throws Exception {
        float[] col0 = new float[ROW_COUNT];
        float[] col1 = new float[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            col0[i] = i * 1.0f;
            col1[i] = (i % 3 == 0) ? 0.0f : 2.0f;
        }
        assertParityTwoFloatCols(col0, col1, ir(
                new IrDecoder.Instruction(IMM, F4_TYPE, Float.floatToRawIntBits(0.5f), 0),
                insn(MEM, F4_TYPE, 0, 0),
                insn(MEM, F4_TYPE, 1, 0),
                insn(DIV, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ), FLOAT_OPTIONS);
    }

    @Test
    public void testFloatNegate() throws Exception {
        float[] data = new float[ROW_COUNT];
        for (int i = 0; i < ROW_COUNT; i++) {
            data[i] = i * 1.5f - 10.0f;
        }
        assertParityFloatCol(data, ir(
                new IrDecoder.Instruction(IMM, F4_TYPE, Float.floatToRawIntBits(5.0f), 0),
                insn(MEM, F4_TYPE, 0, 0),
                insn(NEG, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        ));
    }

    // ========================
    // Float helpers
    // ========================

    private void assertParityFloatCol(float[] data, IrDecoder.Instruction[] instructions) throws Exception {
        assertParityFloatColWithOptions(data, instructions, FLOAT_OPTIONS);
    }

    private void assertParityFloatColWithOptions(float[] data, IrDecoder.Instruction[] instructions, int options) throws Exception {
        int len = data.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc(Math.max(1, (long) len * Float.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putFloat(colData + (long) i * Float.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 1, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support F4 program", vector);
            long actual = vector.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Float.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertParityTwoFloatCols(float[] col0, float[] col1,
                                           IrDecoder.Instruction[] instructions, int options) throws Exception {
        Assert.assertEquals(col0.length, col1.length);
        int len = col0.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long c0 = Unsafe.malloc((long) len * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
        long c1 = Unsafe.malloc((long) len * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putFloat(c0 + (long) i * Float.BYTES, col0[i]);
            Unsafe.getUnsafe().putFloat(c1 + (long) i * Float.BYTES, col1[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, c0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, c1);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 2, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support F4 program", vector);
            long actual = vector.filterRows(colPtrArray, 2, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(c0, (long) len * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(c1, (long) len * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ========================
    // Int helpers
    // ========================

    private void assertParityIntCol(int[] data, IrDecoder.Instruction[] instructions) throws Exception {
        assertParityIntCol(data, instructions, INT_OPTIONS);
    }

    private void assertParityIntCol(int[] data, IrDecoder.Instruction[] instructions, int options) throws Exception {
        int len = data.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc(Math.max(1, (long) len * Integer.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putInt(colData + (long) i * Integer.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 1, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support int program", vector);
            long actual = vector.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Integer.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertCountParityIntCol(int[] data, IrDecoder.Instruction[] instructions) throws Exception {
        int len = data.length;
        long colData = Unsafe.malloc(Math.max(1, (long) len * Integer.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putInt(colData + (long) i * Integer.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, INT_OPTIONS);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.countRows(colPtrArray, 1, 0, 0, 0, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull(vector);
            long actual = vector.countRows(colPtrArray, 1, 0, 0, 0, len);

            Assert.assertEquals("count mismatch", expected, actual);
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Integer.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertParityLongIntDouble(
            long[] longData,
            int[] intData,
            double[] doubleData,
            IrDecoder.Instruction[] instructions,
            int options
    ) throws Exception {
        Assert.assertEquals(longData.length, intData.length);
        Assert.assertEquals(longData.length, doubleData.length);
        int len = longData.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long col0 = allocLongColumn(longData);
        long col1 = allocIntColumn(intData);
        long col2 = allocDoubleColumn(doubleData);
        long colPtrArray = Unsafe.malloc(24, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(colPtrArray, col0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, col1);
        Unsafe.getUnsafe().putLong(colPtrArray + 16, col2);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 3, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support mixed I8/I4/F8 compare-only program", vector);
            long actual = vector.filterRows(colPtrArray, 3, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(col0, (long) len * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col1, (long) len * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col2, (long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 24, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertParityIntDouble(
            int[] intData,
            double[] doubleData,
            IrDecoder.Instruction[] instructions,
            int options
    ) throws Exception {
        Assert.assertEquals(intData.length, doubleData.length);
        int len = intData.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long col0 = allocIntColumn(intData);
        long col1 = allocDoubleColumn(doubleData);
        long colPtrArray = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putLong(colPtrArray, col0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, col1);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 2, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support I4+F8 compare-only program", vector);
            long actual = vector.filterRows(colPtrArray, 2, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(col0, (long) len * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col1, (long) len * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ========================
    // Double helpers
    // ========================

    private void assertParityDoubleColWithOptions(double[] data, IrDecoder.Instruction[] instructions, int options) throws Exception {
        int len = data.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc(Math.max(1, (long) len * Double.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(colData + (long) i * Double.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, options);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 1, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support this program", vector);
            long actual = vector.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Double.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertParityDoubleCol(double[] data, IrDecoder.Instruction[] instructions) throws Exception {
        int len = data.length;
        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc(Math.max(1, (long) len * Double.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long expectedBuf = Unsafe.malloc(((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(colData + (long) i * Double.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, DOUBLE_OPTIONS);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.filterRows(colPtrArray, 1, 0, 0, 0, expectedBuf, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull("vector compiler should support double program", vector);
            long actual = vector.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, len);

            Assert.assertEquals("row count mismatch", expected, actual);
            for (long i = 0; i < actual; i++) {
                Assert.assertEquals("row mismatch at index " + i,
                        Unsafe.getUnsafe().getLong(expectedBuf + i * 8),
                        Unsafe.getUnsafe().getLong(outputBuf + i * 8));
            }
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Double.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(expectedBuf, ((long) len + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertCountParityDoubleCol(double[] data, IrDecoder.Instruction[] instructions) throws Exception {
        int len = data.length;
        long colData = Unsafe.malloc(Math.max(1, (long) len * Double.BYTES), MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putDouble(colData + (long) i * Double.BYTES, data[i]);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            LoweredProgram prog = IrLowering.lower(instructions, DOUBLE_OPTIONS);
            ScalarBytecodeFilterCompiler.ScalarFilterBody scalar = ScalarBytecodeFilterCompiler.compile(prog);
            long expected = scalar.countRows(colPtrArray, 1, 0, 0, 0, len);

            VectorFilterBody vector = VectorBytecodeFilterCompiler.compile(prog);
            Assert.assertNotNull(vector);
            long actual = vector.countRows(colPtrArray, 1, 0, 0, 0, len);

            Assert.assertEquals("count mismatch", expected, actual);
        } finally {
            Unsafe.free(colData, Math.max(1, (long) len * Double.BYTES), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
