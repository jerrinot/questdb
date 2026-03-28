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

package io.questdb.jit;

import io.questdb.cairo.JitBackend;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;

public class VectorCompiledFilter implements JitFilter {
    private ScalarBytecodeFilterCompiler.ScalarFilterBody bytecodeFilter;
    private final VectorFilterInterpreter interpreter = new VectorFilterInterpreter();
    private VectorFilterBody vectorBytecodeFilter;

    @Override
    public long call(
            long dataAddress,
            long dataSize,
            long varSizeAuxAddress,
            long varsAddress,
            long varsSize,
            long filteredRowsAddress,
            long rowsCount
    ) {
        if (vectorBytecodeFilter != null) {
            return vectorBytecodeFilter.filterRows(
                    dataAddress,
                    dataSize,
                    varSizeAuxAddress,
                    varsAddress,
                    varsSize,
                    filteredRowsAddress,
                    rowsCount
            );
        }
        if (bytecodeFilter != null) {
            return bytecodeFilter.filterRows(
                    dataAddress,
                    dataSize,
                    varSizeAuxAddress,
                    varsAddress,
                    varsSize,
                    filteredRowsAddress,
                    rowsCount
            );
        }
        return interpreter.filter(
                dataAddress,
                dataSize,
                varSizeAuxAddress,
                varsAddress,
                varsSize,
                filteredRowsAddress,
                rowsCount
        );
    }

    @Override
    public void close() {
    }

    @Override
    public void compile(MemoryCARW filter, int options) throws SqlException {
        compile(filter, options, JitBackend.AUTO);
    }

    public void compile(MemoryCARW filter, int options, int backend) throws SqlException {
        // The interpreter is always compiled as the ultimate fallback,
        // unless a specific compiled backend is forced.
        if (backend != JitBackend.JAVA_COMPILED && backend != JitBackend.JAVA_VECTOR_COMPILED) {
            interpreter.compile(filter, options);
        }
        if (backend != JitBackend.JAVA_INTERPRETED) {
            compileBytecode(filter, options, backend);
        }
    }

    public boolean usesBytecode() {
        return bytecodeFilter != null || vectorBytecodeFilter != null;
    }

    public boolean usesVectorApi() {
        return interpreter.usesVectorApi();
    }

    public boolean usesVectorBytecode() {
        return vectorBytecodeFilter != null;
    }

    private void compileBytecode(MemoryCARW filter, int options, int backend) throws SqlException {
        IrDecoder decoder = new IrDecoder();
        IrDecoder.Instruction[] instructions = decoder.decode(filter);
        LoweredProgram program = IrLowering.lower(instructions, options);

        // Try vectorized bytecode first (unless forced to scalar-only).
        // This generates SIMD code via Vector API — best performance for
        // supported programs (I8/F8, straight-line, no var-size/UUID).
        if (backend != JitBackend.JAVA_COMPILED) {
            vectorBytecodeFilter = VectorBytecodeFilterCompiler.compile(program);
            if (vectorBytecodeFilter != null) {
                return;
            }
        }

        // Fall back to scalar bytecode — handles all programs including
        // control flow, all types, var-size columns, UUID.
        if (backend != JitBackend.JAVA_VECTOR_COMPILED) {
            bytecodeFilter = ScalarBytecodeFilterCompiler.compile(program);
        }
    }
}
