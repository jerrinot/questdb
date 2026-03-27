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

package io.questdb.jit;

import io.questdb.std.ObjList;

/**
 * A lowered program: a control-flow graph of {@link LoweredBlock} nodes,
 * structured options, and metadata for execution planning.
 * <p>
 * This is the semantic center of the Java JIT backend. All execution
 * targets (scalar bytecode, vector, etc.) consume this representation.
 */
public final class LoweredProgram {
    private final ObjList<LoweredBlock> blocks;
    private final int entryBlockId;
    private final boolean hasControlFlow;
    private final int nextTempId;
    private final DecodedOptions options;
    private final int[] varOffsets;

    LoweredProgram(
            DecodedOptions options,
            ObjList<LoweredBlock> blocks,
            int entryBlockId,
            int nextTempId,
            int[] varOffsets,
            boolean hasControlFlow
    ) {
        this.options = options;
        this.blocks = blocks;
        this.entryBlockId = entryBlockId;
        this.nextTempId = nextTempId;
        this.varOffsets = varOffsets;
        this.hasControlFlow = hasControlFlow;
    }

    public LoweredBlock getBlock(int index) {
        return blocks.getQuick(index);
    }

    public int getBlockCount() {
        return blocks.size();
    }

    public int getEntryBlockId() {
        return entryBlockId;
    }

    public int getNextTempId() {
        return nextTempId;
    }

    public DecodedOptions getOptions() {
        return options;
    }

    public int[] getVarOffsets() {
        return varOffsets;
    }

    /**
     * Returns true if the program has multiple basic blocks (short-circuit
     * evaluation or other control flow). Programs with control flow are
     * scalar-only in the initial planner.
     */
    public boolean hasControlFlow() {
        return hasControlFlow;
    }
}
