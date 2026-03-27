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

import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;

public class VectorCompiledFilter implements JitFilter {
    private ScalarBytecodeFilterCompiler.ScalarFilterBody bytecodeFilter;
    private final VectorFilterInterpreter interpreter = new VectorFilterInterpreter();

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
        interpreter.compile(filter, options);
        compileBytecode(filter, options);
    }

    public boolean usesBytecode() {
        return bytecodeFilter != null;
    }

    public boolean usesVectorApi() {
        return interpreter.usesVectorApi();
    }

    private void compileBytecode(MemoryCARW filter, int options) throws SqlException {
        if (interpreter.usesVectorApi()) {
            return;
        }
        IrDecoder decoder = new IrDecoder();
        IrDecoder.Instruction[] instructions = decoder.decode(filter);
        LoweredProgram program = IrLowering.lower(instructions, options);
        bytecodeFilter = ScalarBytecodeFilterCompiler.compile(program);
    }
}
