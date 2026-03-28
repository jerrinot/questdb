/*******************************************************************************
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

import io.questdb.jit.IrDecoder;
import io.questdb.jit.IrLowering;
import io.questdb.jit.LoweredProgram;
import io.questdb.jit.VectorBytecodeFilterCompiler;
import io.questdb.jit.VectorFilterBody;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import jdk.incubator.vector.LongVector;

import static io.questdb.jit.CompiledFilterIRSerializer.*;

/**
 * Standalone driver for C2 compilation analysis of vector bytecode filters.
 * Run with diagnostic JVM flags to capture inlining and compilation decisions.
 *
 * Usage:
 *   java --add-modules jdk.incubator.vector \
 *        -XX:+UnlockDiagnosticVMOptions \
 *        -XX:+PrintCompilation \
 *        -XX:+PrintInlining \
 *        -cp <classpath> io.questdb.test.jit.VectorBytecodeC2Driver
 */
public class VectorBytecodeC2Driver {

    private static final int ROW_COUNT = 4096;
    private static final int WARMUP_ITERATIONS = 20_000;

    public static void main(String[] args) throws Exception {
        String filter = args.length > 0 ? args[0] : "l_gt_42";

        switch (filter) {
            case "l_gt_42" -> runLongGt42();
            case "in5" -> runLongIn5();
            case "mixed" -> runMixedLongDouble();
            default -> {
                System.err.println("Unknown filter: " + filter);
                System.err.println("Available: l_gt_42, in5, mixed");
                System.exit(1);
            }
        }
    }

    private static void runLongGt42() throws Exception {
        System.err.println("=== Filter: l > 42 (pure I8) ===");

        int options = (3 << 1) | (1 << 4); // log2(8), single-size, no null checks
        IrDecoder.Instruction[] instructions = {
                new IrDecoder.Instruction(IMM, I8_TYPE, 42, 0),
                new IrDecoder.Instruction(MEM, I8_TYPE, 0, 0),
                new IrDecoder.Instruction(GT, 0, 0, 0),
                new IrDecoder.Instruction(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(instructions, options);
        VectorFilterBody body = VectorBytecodeFilterCompiler.compile(prog);
        if (body == null) {
            System.err.println("ERROR: vector compiler rejected program");
            System.exit(1);
        }

        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc((long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) ROW_COUNT + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < ROW_COUNT; i++) {
            Unsafe.getUnsafe().putLong(colData + (long) i * Long.BYTES, i * 10L);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            // Warm up to trigger C2 compilation
            long result = 0;
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                result = body.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, ROW_COUNT);
            }
            System.err.println("filterRows result: " + result + " rows matched");

            // Count-only path
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                result = body.countRows(colPtrArray, 1, 0, 0, 0, ROW_COUNT);
            }
            System.err.println("countRows result: " + result + " rows counted");
        } finally {
            Unsafe.free(colData, (long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) ROW_COUNT + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void runLongIn5() throws Exception {
        System.err.println("=== Filter: l IN (1, 2, 3, 4, 5) (pure I8, straight-line OR) ===");

        int options = (3 << 1) | (1 << 4); // log2(8), single-size, no null checks
        IrDecoder.Instruction[] instructions = {
                new IrDecoder.Instruction(IMM, I8_TYPE, 1, 0),
                new IrDecoder.Instruction(MEM, I8_TYPE, 0, 0),
                new IrDecoder.Instruction(EQ, 0, 0, 0),

                new IrDecoder.Instruction(IMM, I8_TYPE, 2, 0),
                new IrDecoder.Instruction(MEM, I8_TYPE, 0, 0),
                new IrDecoder.Instruction(EQ, 0, 0, 0),
                new IrDecoder.Instruction(OR, 0, 0, 0),

                new IrDecoder.Instruction(IMM, I8_TYPE, 3, 0),
                new IrDecoder.Instruction(MEM, I8_TYPE, 0, 0),
                new IrDecoder.Instruction(EQ, 0, 0, 0),
                new IrDecoder.Instruction(OR, 0, 0, 0),

                new IrDecoder.Instruction(IMM, I8_TYPE, 4, 0),
                new IrDecoder.Instruction(MEM, I8_TYPE, 0, 0),
                new IrDecoder.Instruction(EQ, 0, 0, 0),
                new IrDecoder.Instruction(OR, 0, 0, 0),

                new IrDecoder.Instruction(IMM, I8_TYPE, 5, 0),
                new IrDecoder.Instruction(MEM, I8_TYPE, 0, 0),
                new IrDecoder.Instruction(EQ, 0, 0, 0),
                new IrDecoder.Instruction(OR, 0, 0, 0),

                new IrDecoder.Instruction(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(instructions, options);
        VectorFilterBody body = VectorBytecodeFilterCompiler.compile(prog);
        if (body == null) {
            System.err.println("ERROR: vector compiler rejected program");
            System.exit(1);
        }

        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long colData = Unsafe.malloc((long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) ROW_COUNT + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < ROW_COUNT; i++) {
            // Sparse matches keep the filter realistic and avoid "all lanes match" bias.
            Unsafe.getUnsafe().putLong(colData + (long) i * Long.BYTES, (i % 16) + 1L);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, colData);

        try {
            long result = 0;
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                result = body.filterRows(colPtrArray, 1, 0, 0, 0, outputBuf, ROW_COUNT);
            }
            System.err.println("filterRows result: " + result + " rows matched");

            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                result = body.countRows(colPtrArray, 1, 0, 0, 0, ROW_COUNT);
            }
            System.err.println("countRows result: " + result + " rows counted");
        } finally {
            Unsafe.free(colData, (long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) ROW_COUNT + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void runMixedLongDouble() throws Exception {
        System.err.println("=== Filter: l > 25 AND d < 0.5 (mixed I8+F8, null checks) ===");

        int options = (3 << 1) | (1 << 4) | (1 << 6); // log2(8), single-size, null checks
        IrDecoder.Instruction[] instructions = {
                new IrDecoder.Instruction(IMM, I8_TYPE, 25, 0),
                new IrDecoder.Instruction(MEM, I8_TYPE, 0, 0),
                new IrDecoder.Instruction(GT, 0, 0, 0),
                new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(0.5), 0),
                new IrDecoder.Instruction(MEM, F8_TYPE, 1, 0),
                new IrDecoder.Instruction(LT, 0, 0, 0),
                new IrDecoder.Instruction(AND, 0, 0, 0),
                new IrDecoder.Instruction(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(instructions, options);
        VectorFilterBody body = VectorBytecodeFilterCompiler.compile(prog);
        if (body == null) {
            System.err.println("ERROR: vector compiler rejected program");
            System.exit(1);
        }

        int speciesLen = LongVector.SPECIES_PREFERRED.length();
        long col0 = Unsafe.malloc((long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        long col1 = Unsafe.malloc((long) ROW_COUNT * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(((long) ROW_COUNT + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < ROW_COUNT; i++) {
            Unsafe.getUnsafe().putLong(col0 + (long) i * Long.BYTES, i * 10L);
            Unsafe.getUnsafe().putDouble(col1 + (long) i * Double.BYTES, i * 0.1);
        }
        Unsafe.getUnsafe().putLong(colPtrArray, col0);
        Unsafe.getUnsafe().putLong(colPtrArray + 8, col1);

        try {
            long result = 0;
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                result = body.filterRows(colPtrArray, 2, 0, 0, 0, outputBuf, ROW_COUNT);
            }
            System.err.println("filterRows result: " + result + " rows matched");
        } finally {
            Unsafe.free(col0, (long) ROW_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(col1, (long) ROW_COUNT * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 16, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, ((long) ROW_COUNT + speciesLen) * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
