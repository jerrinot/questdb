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

package io.questdb.cairo;

/**
 * Selects which JIT backend implementation processes filter IR.
 * Orthogonal to {@link SqlJitMode}, which controls scalar vs vectorized
 * IR serialization.
 */
public final class JitBackend {
    /**
     * Default: Java bytecode compiler when available, interpreter as fallback.
     */
    public static final int AUTO = 0;
    /**
     * Native C++ asmjit backend ({@link io.questdb.jit.CompiledFilter}).
     * Scalar vs SIMD is controlled by {@link SqlJitMode}.
     */
    public static final int CPP = 1;
    /**
     * Java interpreter ({@link io.questdb.jit.VectorFilterInterpreter}).
     * Uses Vector API when eligible and {@link SqlJitMode#JIT_MODE_ENABLED}.
     */
    public static final int JAVA_INTERPRETED = 2;
    /**
     * Java scalar bytecode compiler ({@link io.questdb.jit.ScalarBytecodeFilterCompiler}).
     */
    public static final int JAVA_COMPILED = 3;
    /**
     * Java vectorized bytecode compiler ({@link io.questdb.jit.VectorBytecodeFilterCompiler}).
     * Generates bytecode that invokes Vector API for SIMD execution.
     */
    public static final int JAVA_VECTOR_COMPILED = 4;

    private JitBackend() {
    }

    public static String toString(int backend) {
        return switch (backend) {
            case AUTO -> "auto";
            case CPP -> "cpp";
            case JAVA_INTERPRETED -> "java_interpreted";
            case JAVA_COMPILED -> "java_compiled";
            case JAVA_VECTOR_COMPILED -> "java_vector_compiled";
            default -> "unknown";
        };
    }
}
