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
import io.questdb.std.Unsafe;

public final class IrDecoder {
    public static final int INSTRUCTION_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;

    public Instruction[] decode(MemoryCARW filter) {
        return decode(filter.getPageAddress(0), filter.getAppendOffset());
    }

    public Instruction[] decode(long filterAddress, long filterSize) {
        if (filterSize % INSTRUCTION_SIZE != 0) {
            throw new IllegalArgumentException("invalid IR size [size=" + filterSize + ", instructionSize=" + INSTRUCTION_SIZE + ']');
        }

        final int instructionCount = Math.toIntExact(filterSize / INSTRUCTION_SIZE);
        final Instruction[] instructions = new Instruction[instructionCount];
        final sun.misc.Unsafe unsafe = Unsafe.getUnsafe();
        for (int i = 0; i < instructionCount; i++) {
            final long offset = filterAddress + (long) i * INSTRUCTION_SIZE;
            instructions[i] = new Instruction(
                    unsafe.getInt(offset),
                    unsafe.getInt(offset + Integer.BYTES),
                    unsafe.getLong(offset + 2L * Integer.BYTES),
                    unsafe.getLong(offset + 2L * Integer.BYTES + Long.BYTES)
            );
        }
        return instructions;
    }

    public record Instruction(int opcode, int type, long payloadLo, long payloadHi) {
        public double doublePayload() {
            return Double.longBitsToDouble(payloadLo);
        }
    }
}
