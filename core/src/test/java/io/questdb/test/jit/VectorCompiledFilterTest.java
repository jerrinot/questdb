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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.jit.VectorCompiledCountOnlyFilter;
import io.questdb.jit.VectorCompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.jit.CompiledFilterIRSerializer.ADD;
import static io.questdb.jit.CompiledFilterIRSerializer.AND;
import static io.questdb.jit.CompiledFilterIRSerializer.AND_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.BEGIN_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.END_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.EQ;
import static io.questdb.jit.CompiledFilterIRSerializer.GT;
import static io.questdb.jit.CompiledFilterIRSerializer.I16_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.I4_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.IMM;
import static io.questdb.jit.CompiledFilterIRSerializer.LT;
import static io.questdb.jit.CompiledFilterIRSerializer.MEM;
import static io.questdb.jit.CompiledFilterIRSerializer.OR;
import static io.questdb.jit.CompiledFilterIRSerializer.OR_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.RET;
import static io.questdb.jit.CompiledFilterIRSerializer.VAR;

public class VectorCompiledFilterTest {
    @Test
    public void testInterpretsArithmeticAndCountOnly() throws Exception {
        try (
                MemoryCARW ir = Vm.getCARWInstance(1024, 1, MemoryTag.NATIVE_JIT);
                MemoryCARW col0 = Vm.getCARWInstance(64, 1, MemoryTag.NATIVE_JIT);
                MemoryCARW col1 = Vm.getCARWInstance(64, 1, MemoryTag.NATIVE_JIT);
                MemoryCARW vars = Vm.getCARWInstance(64, 1, MemoryTag.NATIVE_JIT);
                DirectLongList dataAddresses = new DirectLongList(2, MemoryTag.NATIVE_OFFLOAD);
                DirectLongList filteredRows = new DirectLongList(8, MemoryTag.NATIVE_OFFLOAD);
                VectorCompiledFilter filter = new VectorCompiledFilter();
                VectorCompiledCountOnlyFilter countOnlyFilter = new VectorCompiledCountOnlyFilter()
        ) {
            putInts(col0, 6, 4, 7, 9);
            putInts(col1, 1, 1, 9, 2);
            vars.putLong(3);

            dataAddresses.add(col0.getAddress());
            dataAddresses.add(col1.getAddress());

            putInstruction(ir, IMM, I4_TYPE, 5, 0);
            putInstruction(ir, MEM, I4_TYPE, 0, 0);
            putOperator(ir, GT);
            putInstruction(ir, IMM, I4_TYPE, 10, 0);
            putInstruction(ir, VAR, I4_TYPE, 0, 0);
            putInstruction(ir, MEM, I4_TYPE, 1, 0);
            putOperator(ir, ADD);
            putOperator(ir, LT);
            putOperator(ir, AND);
            putOperator(ir, RET);

            filter.compile(ir, 0);
            countOnlyFilter.compile(ir, 0);

            long count = filter.call(
                    dataAddresses.getAddress(),
                    dataAddresses.size(),
                    0,
                    vars.getAddress(),
                    1,
                    filteredRows.getAddress(),
                    4
            );
            filteredRows.setPos(count);

            Assert.assertEquals(2, count);
            Assert.assertEquals(0, filteredRows.get(0));
            Assert.assertEquals(3, filteredRows.get(1));

            long countOnly = countOnlyFilter.call(
                    dataAddresses.getAddress(),
                    dataAddresses.size(),
                    0,
                    vars.getAddress(),
                    1,
                    4
            );
            Assert.assertEquals(2, countOnly);
        }
    }

    @Test
    public void testInterpretsShortCircuitInProgramsWithEmptyFinalStack() throws Exception {
        try (
                MemoryCARW ir = Vm.getCARWInstance(1024, 1, MemoryTag.NATIVE_JIT);
                MemoryCARW column = Vm.getCARWInstance(64, 1, MemoryTag.NATIVE_JIT);
                DirectLongList dataAddresses = new DirectLongList(1, MemoryTag.NATIVE_OFFLOAD);
                DirectLongList filteredRows = new DirectLongList(8, MemoryTag.NATIVE_OFFLOAD);
                VectorCompiledFilter filter = new VectorCompiledFilter()
        ) {
            putInts(column, 0, 1, 2, 3);
            dataAddresses.add(column.getAddress());

            putLabel(ir, BEGIN_SC, 2);
            putInstruction(ir, IMM, I4_TYPE, 1, 0);
            putInstruction(ir, MEM, I4_TYPE, 0, 0);
            putOperator(ir, EQ);
            putLabel(ir, OR_SC, 2);
            putInstruction(ir, IMM, I4_TYPE, 2, 0);
            putInstruction(ir, MEM, I4_TYPE, 0, 0);
            putOperator(ir, EQ);
            putLabel(ir, AND_SC, 0);
            putLabel(ir, END_SC, 2);
            putOperator(ir, RET);

            filter.compile(ir, 0);

            long count = filter.call(
                    dataAddresses.getAddress(),
                    dataAddresses.size(),
                    0,
                    0,
                    0,
                    filteredRows.getAddress(),
                    4
            );
            filteredRows.setPos(count);

            Assert.assertEquals(2, count);
            Assert.assertEquals(1, filteredRows.get(0));
            Assert.assertEquals(2, filteredRows.get(1));
        }
    }

    @Test
    public void testComputesBindVariableOffsetsForMixedWidths() throws Exception {
        try (
                MemoryCARW ir = Vm.getCARWInstance(1024, 1, MemoryTag.NATIVE_JIT);
                MemoryCARW uuidColumn = Vm.getCARWInstance(128, 1, MemoryTag.NATIVE_JIT);
                MemoryCARW intColumn = Vm.getCARWInstance(64, 1, MemoryTag.NATIVE_JIT);
                MemoryCARW vars = Vm.getCARWInstance(64, 1, MemoryTag.NATIVE_JIT);
                DirectLongList dataAddresses = new DirectLongList(2, MemoryTag.NATIVE_OFFLOAD);
                DirectLongList filteredRows = new DirectLongList(8, MemoryTag.NATIVE_OFFLOAD);
                VectorCompiledFilter filter = new VectorCompiledFilter()
        ) {
            putLong128(uuidColumn, 11, 101);
            putLong128(uuidColumn, 12, 102);
            putLong128(uuidColumn, 13, 103);
            putInts(intColumn, 1, 42, 7);

            vars.putLong(11);
            vars.putLong(101);
            vars.putLong(42);

            dataAddresses.add(uuidColumn.getAddress());
            dataAddresses.add(intColumn.getAddress());

            putInstruction(ir, VAR, I16_TYPE, 0, 0);
            putInstruction(ir, MEM, I16_TYPE, 0, 0);
            putOperator(ir, EQ);
            putInstruction(ir, VAR, I4_TYPE, 1, 0);
            putInstruction(ir, MEM, I4_TYPE, 1, 0);
            putOperator(ir, EQ);
            putOperator(ir, OR);
            putOperator(ir, RET);

            filter.compile(ir, 0);

            long count = filter.call(
                    dataAddresses.getAddress(),
                    dataAddresses.size(),
                    0,
                    vars.getAddress(),
                    2,
                    filteredRows.getAddress(),
                    3
            );
            filteredRows.setPos(count);

            Assert.assertEquals(2, count);
            Assert.assertEquals(0, filteredRows.get(0));
            Assert.assertEquals(1, filteredRows.get(1));
        }
    }

    private static void putInstruction(MemoryCARW memory, int opcode, int type, long payloadLo, long payloadHi) {
        memory.putInt(opcode);
        memory.putInt(type);
        memory.putLong(payloadLo);
        memory.putLong(payloadHi);
    }

    private static void putInts(MemoryCARW memory, int... values) {
        for (int value : values) {
            memory.putInt(value);
        }
    }

    private static void putLabel(MemoryCARW memory, int opcode, int labelIndex) {
        putInstruction(memory, opcode, 0, labelIndex, 0);
    }

    private static void putLong128(MemoryCARW memory, long lo, long hi) {
        memory.putLong(lo);
        memory.putLong(hi);
    }

    private static void putOperator(MemoryCARW memory, int opcode) {
        putInstruction(memory, opcode, 0, 0, 0);
    }
}
