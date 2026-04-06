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

public class VectorCompiledCountOnlyFilter implements JitCountOnlyFilter {
    private VectorFilterBody vectorBytecodeFilter;

    @Override
    public long call(
            long dataAddress,
            long dataSize,
            long varSizeAuxAddress,
            long varsAddress,
            long varsSize,
            long rowsCount
    ) {
        if (vectorBytecodeFilter != null) {
            return vectorBytecodeFilter.countRows(
                    dataAddress,
                    dataSize,
                    varSizeAuxAddress,
                    varsAddress,
                    varsSize,
                    rowsCount
            );
        }
        throw new IllegalStateException("no compiled filter available");
    }

    @Override
    public void close() {
    }

    @Override
    public void compile(MemoryCARW filter, int options) throws SqlException {
        IrDecoder decoder = new IrDecoder();
        IrDecoder.Instruction[] instructions = decoder.decode(filter);
        LoweredProgram program = IrLowering.lower(instructions, options);
        vectorBytecodeFilter = VectorBytecodeFilterCompiler.compile(program);
    }

    public boolean usesVectorBytecode() {
        return vectorBytecodeFilter != null;
    }
}
