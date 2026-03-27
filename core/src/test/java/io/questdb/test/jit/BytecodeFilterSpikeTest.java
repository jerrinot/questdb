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

import io.questdb.jit.FilterHelpers;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Feasibility spike: proves that BytecodeAssembler can generate a JIT filter
 * class with the full call signature, a row iteration loop, conditional
 * branches, static helper method calls, and correct stack map tables.
 */
public class BytecodeFilterSpikeTest {

    public interface SpikeFilter {
        long call(long dataAddress, long dataSize, long varSizeAuxAddress,
                  long varsAddress, long varsSize, long filteredRowsAddress, long rowsCount);
    }

    @Test
    public void testGeneratedFilterIntGt() throws Exception {
        SpikeFilter filter = generateIntGtFilter(5);

        int rowCount = 6;
        long colData = Unsafe.malloc((long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc((long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);

        try {
            Unsafe.getUnsafe().putInt(colData, 3);
            Unsafe.getUnsafe().putInt(colData + 4, 7);
            Unsafe.getUnsafe().putInt(colData + 8, 5);
            Unsafe.getUnsafe().putInt(colData + 12, 10);
            Unsafe.getUnsafe().putInt(colData + 16, 1);
            Unsafe.getUnsafe().putInt(colData + 20, 6);
            Unsafe.getUnsafe().putLong(colPtrArray, colData);

            long matched = filter.call(colPtrArray, 1, 0, 0, 0, outputBuf, rowCount);

            Assert.assertEquals(3, matched);
            Assert.assertEquals(1, Unsafe.getUnsafe().getLong(outputBuf));
            Assert.assertEquals(3, Unsafe.getUnsafe().getLong(outputBuf + 8));
            Assert.assertEquals(5, Unsafe.getUnsafe().getLong(outputBuf + 16));
        } finally {
            Unsafe.free(colData, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, (long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGeneratedFilterEmptyInput() throws Exception {
        SpikeFilter filter = generateIntGtFilter(5);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            long matched = filter.call(colPtrArray, 1, 0, 0, 0, outputBuf, 0);
            Assert.assertEquals(0, matched);
        } finally {
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGeneratedFilterAllMatch() throws Exception {
        SpikeFilter filter = generateIntGtFilter(0);
        int rowCount = 4;
        long colData = Unsafe.malloc((long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc((long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(colData, 1);
            Unsafe.getUnsafe().putInt(colData + 4, 2);
            Unsafe.getUnsafe().putInt(colData + 8, 3);
            Unsafe.getUnsafe().putInt(colData + 12, 4);
            Unsafe.getUnsafe().putLong(colPtrArray, colData);

            long matched = filter.call(colPtrArray, 1, 0, 0, 0, outputBuf, rowCount);
            Assert.assertEquals(4, matched);
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(i, Unsafe.getUnsafe().getLong(outputBuf + (long) i * 8));
            }
        } finally {
            Unsafe.free(colData, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, (long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testGeneratedFilterNoneMatch() throws Exception {
        SpikeFilter filter = generateIntGtFilter(100);
        int rowCount = 3;
        long colData = Unsafe.malloc((long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        long colPtrArray = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        long outputBuf = Unsafe.malloc((long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(colData, 1);
            Unsafe.getUnsafe().putInt(colData + 4, 50);
            Unsafe.getUnsafe().putInt(colData + 8, 100);
            Unsafe.getUnsafe().putLong(colPtrArray, colData);

            long matched = filter.call(colPtrArray, 1, 0, 0, 0, outputBuf, rowCount);
            Assert.assertEquals(0, matched);
        } finally {
            Unsafe.free(colData, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(colPtrArray, 8, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(outputBuf, (long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Generates a filter: accept row if col0 > threshold.
     * <p>
     * Locals: this=0, dataAddr=1-2, dataSize=3-4, varSizeAux=5-6,
     * varsAddr=7-8, varsSize=9-10, filteredRows=11-12,
     * rowsCount=13-14, outputCount=15-16, row=17-18
     */
    private static SpikeFilter generateIntGtFilter(int threshold) {
        BytecodeAssembler asm = new BytecodeAssembler();
        asm.init(SpikeFilter.class);
        asm.setupPool();

        int thisClass = asm.poolClass(asm.poolUtf8("io/questdb/test/jit/gen"));
        int ifaceClass = asm.poolClass(SpikeFilter.class);
        int callName = asm.poolUtf8("call");
        int callSig = asm.poolUtf8("(JJJJJJJ)J");
        int stackMapAttr = asm.poolUtf8("StackMapTable");
        int readIntMethod = asm.poolMethod(FilterHelpers.class, "readInt", "(JIJ)I");
        int writeRowMethod = asm.poolMethod(FilterHelpers.class, "writeRow", "(JJJ)V");

        asm.finishPool();

        asm.defineClass(thisClass);
        asm.interfaceCount(1);
        asm.putShort(ifaceClass);
        asm.fieldCount(0);
        asm.methodCount(2); // <init> + call
        asm.defineDefaultConstructor();

        // call() — maxStack=6 (writeRow: 3 longs), maxLocals=19
        asm.startMethod(callName, callSig, 6, 19);

        // outputCount = 0
        asm.lconst_0();
        asm.lstore(15);
        // row = 0
        asm.lconst_0();
        asm.lstore(17);

        // LOOP:
        int loopStart = asm.position();
        asm.lload(13);  // rowsCount
        asm.lload(17);  // row
        asm.lcmp();
        int exitBranch = asm.ifle(); // rowsCount <= row -> exit

        // readInt(dataAddress, 0, row) -> int
        asm.lload(1);
        asm.iconst(0);
        asm.lload(17);
        asm.invokeStatic(readIntMethod);

        // if value - threshold <= 0 -> skip (value <= threshold)
        asm.iconst(threshold);
        asm.isub();
        int skipBranch = asm.ifle();

        // writeRow(filteredRowsAddress, outputCount, row)
        asm.lload(11);
        asm.lload(15);
        asm.lload(17);
        asm.invokeStatic(writeRowMethod);

        // outputCount++
        asm.lload(15);
        asm.iconst(1);
        asm.i2l();
        asm.ladd();
        asm.lstore(15);

        // NEXT: row++
        int nextStart = asm.position();
        asm.lload(17);
        asm.iconst(1);
        asm.i2l();
        asm.ladd();
        asm.lstore(17);
        int backJmp = asm.goto_();
        asm.setJmp(backJmp, loopStart);

        // EXIT:
        int exitStart = asm.position();
        asm.lload(15);
        asm.lreturn();

        // Patch forward branches
        asm.setJmp(exitBranch, exitStart);
        asm.setJmp(skipBranch, nextStart);

        asm.endMethodCode();
        asm.putShort(0); // 0 exceptions

        // StackMapTable: 3 frames (LOOP, NEXT, EXIT)
        asm.putShort(1); // 1 attribute
        asm.startStackMapTables(stackMapAttr, 3);

        int loopOffset = loopStart - asm.getCodeStart();
        asm.append_frame(2, loopOffset);
        asm.putITEM_Long();  // outputCount
        asm.putITEM_Long();  // row

        int nextOffset = nextStart - asm.getCodeStart();
        asm.same_frame(nextOffset - loopOffset - 1);

        int exitOffset = exitStart - asm.getCodeStart();
        asm.same_frame(exitOffset - nextOffset - 1);

        asm.endStackMapTables();
        asm.endMethod();
        asm.putShort(0); // 0 class attributes

        return asm.newInstance();
    }
}
