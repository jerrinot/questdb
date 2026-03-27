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

import io.questdb.jit.*;
import io.questdb.std.MemoryTag;
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

    private static IrDecoder.Instruction[] ir(IrDecoder.Instruction... instructions) {
        return instructions;
    }

    private static IrDecoder.Instruction insn(int opcode, int type, long lo, long hi) {
        return new IrDecoder.Instruction(opcode, type, lo, hi);
    }
}
