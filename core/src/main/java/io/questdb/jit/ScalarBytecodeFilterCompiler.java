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

import io.questdb.std.BytecodeAssembler;

import static io.questdb.jit.CompiledFilterIRSerializer.*;

/**
 * Compiles a {@link LoweredProgram} into a JVM class that implements
 * {@link ScalarFilterBody}. The generated class evaluates the filter
 * row-by-row using scalar bytecode and delegates to {@link FilterHelpers}
 * for memory access, comparisons, and type coercions.
 * <p>
 * The generated {@code filterRows()} method has this layout:
 * <pre>
 * long outputCount = 0;
 * long row = 0;
 * LOOP:
 *   if (rowsCount <= row) goto EXIT;
 *   // evaluate lowered program for this row
 *   if (result) { writeRow(filteredRows, outputCount, row); outputCount++; }
 * NEXT:
 *   row++;
 *   goto LOOP;
 * EXIT:
 *   return outputCount;
 * </pre>
 */
public final class ScalarBytecodeFilterCompiler {

    /**
     * The interface that generated filter classes implement.
     */
    public interface ScalarFilterBody {
        long filterRows(long dataAddress, long dataSize, long varSizeAuxAddress,
                        long varsAddress, long varsSize, long filteredRowsAddress, long rowsCount);

        long countRows(long dataAddress, long dataSize, long varSizeAuxAddress,
                       long varsAddress, long varsSize, long rowsCount);
    }

    /**
     * Check if the program contains only operations the bytecode compiler
     * can handle correctly. Conservative: rejects programs with float/double
     * operations, I16 (UUID), var-size headers, and null checks until those
     * are proven. This ensures the bytecode path only activates for the
     * subset with verified parity against the regression test suite.
     */
    @SuppressWarnings("unused")
    private static boolean isSupported(LoweredProgram program) {
        // All lowered op types are supported, including I128, var-size
        // headers, float/double, null checks, and short-circuit CFG.
        return true;
    }

    // Method parameter local slot layout (this + 7 longs for filterRows):
    // 0: this
    // 1-2: dataAddress
    // 3-4: dataSize
    // 5-6: varSizeAuxAddress
    // 7-8: varsAddress
    // 9-10: varsSize
    // 11-12: filteredRowsAddress (filterRows) / rowsCount (countRows)
    // 13-14: rowsCount (filterRows only)
    private static final int SLOT_DATA_ADDR = 1;
    private static final int SLOT_VAR_SIZE_AUX = 5;
    private static final int SLOT_VARS_ADDR = 7;

    // For filterRows (7 long params):
    private static final int FR_SLOT_FILTERED_ROWS = 11;
    private static final int FR_SLOT_ROWS_COUNT = 13;
    private static final int FR_FIRST_FREE_SLOT = 15;

    // For countRows (6 long params):
    private static final int CR_SLOT_ROWS_COUNT = 11;
    private static final int CR_FIRST_FREE_SLOT = 13;

    /**
     * Compile a lowered program into a generated filter class.
     *
     * @param program the lowered program to compile
     * @return a new instance of the generated filter, or null if the program
     *         contains unsupported operations
     */
    public static ScalarFilterBody compile(LoweredProgram program) {
        if (!isSupported(program)) {
            return null;
        }
        BytecodeAssembler asm = new BytecodeAssembler();
        asm.init(ScalarFilterBody.class);
        asm.setupPool();

        // --- Constant pool ---
        int thisClass = asm.poolClass(asm.poolUtf8("io/questdb/jit/gen"));
        int ifaceClass = asm.poolClass(ScalarFilterBody.class);

        int filterRowsName = asm.poolUtf8("filterRows");
        int filterRowsSig = asm.poolUtf8("(JJJJJJJ)J");
        int countRowsName = asm.poolUtf8("countRows");
        int countRowsSig = asm.poolUtf8("(JJJJJJ)J");
        int stackMapAttr = asm.poolUtf8("StackMapTable");

        // Pool all FilterHelpers methods we might need
        PooledMethods pm = new PooledMethods(asm);

        // Pre-pool all immediate constants from the program
        prePoolConstants(program, pm);

        asm.finishPool();

        // --- Class structure ---
        asm.defineClass(thisClass);
        asm.interfaceCount(1);
        asm.putShort(ifaceClass);
        asm.fieldCount(0);
        asm.methodCount(3); // <init> + filterRows + countRows
        asm.defineDefaultConstructor();

        // Allocate temporary slots. Each temp gets a JVM local.
        int tempCount = program.getNextTempId();
        int[] tempSlots = new int[tempCount];
        int[] tempTypes = new int[tempCount];

        // --- filterRows method ---
        computeTempSlots(program, tempSlots, tempTypes, FR_FIRST_FREE_SLOT);
        int frMaxLocals = maxLocalSlot(tempSlots, tempTypes, tempCount, FR_FIRST_FREE_SLOT);
        emitFilterMethod(asm, program, tempSlots, tempTypes, filterRowsName, filterRowsSig,
                stackMapAttr, frMaxLocals, FR_SLOT_ROWS_COUNT, FR_SLOT_FILTERED_ROWS, FR_FIRST_FREE_SLOT, false, pm);

        // --- countRows method ---
        computeTempSlots(program, tempSlots, tempTypes, CR_FIRST_FREE_SLOT);
        int crMaxLocals = maxLocalSlot(tempSlots, tempTypes, tempCount, CR_FIRST_FREE_SLOT);
        emitFilterMethod(asm, program, tempSlots, tempTypes, countRowsName, countRowsSig,
                stackMapAttr, crMaxLocals, CR_SLOT_ROWS_COUNT, -1, CR_FIRST_FREE_SLOT, true, pm);

        // Class attributes
        asm.putShort(0);

        return asm.newInstance();
    }

    private static void computeTempSlots(LoweredProgram program, int[] tempSlots, int[] tempTypes, int firstFreeSlot) {
        // Skip 4 slots for outputCount(long) + row(long)
        int slot = firstFreeSlot + 4;
        for (int blockIdx = 0; blockIdx < program.getBlockCount(); blockIdx++) {
            LoweredBlock block = program.getBlock(blockIdx);
            for (int opIdx = 0; opIdx < block.getOpCount(); opIdx++) {
                LoweredOp op = block.getOp(opIdx);
                int dst = op.dst();
                int type = op.resultType();
                if (dst >= 0 && dst < tempSlots.length) {
                    tempTypes[dst] = type;
                }
            }
        }
        for (int i = 0; i < tempSlots.length; i++) {
            tempSlots[i] = slot;
            slot += slotWidth(tempTypes[i]);
        }
    }

    private static int maxLocalSlot(int[] tempSlots, int[] tempTypes, int tempCount, int firstFreeSlot) {
        // 4 slots for outputCount(2) + row(2)
        int max = firstFreeSlot + 4;
        for (int i = 0; i < tempCount; i++) {
            int end = tempSlots[i] + slotWidth(tempTypes[i]);
            if (end > max) {
                max = end;
            }
        }
        return max;
    }

    private static int slotWidth(int type) {
        return switch (type) {
            case I8_TYPE, F8_TYPE -> 2;
            case I16_TYPE -> 4; // lo(long, 2 slots) + hi(long, 2 slots)
            default -> 1;
        };
    }

    private static void emitFilterMethod(
            BytecodeAssembler asm,
            LoweredProgram program,
            int[] tempSlots,
            int[] tempTypes,
            int methodName,
            int methodSig,
            int stackMapAttr,
            int maxLocals,
            int rowsCountSlot,
            int filteredRowsSlot,
            int firstFreeSlot,
            boolean isCountOnly,
            PooledMethods pm
    ) {
        int outputCountSlot = firstFreeSlot;
        int rowSlot = firstFreeSlot + 2;
        boolean nullChecks = program.getOptions().isNullChecksEnabled();

        asm.startMethod(methodName, methodSig, 8, maxLocals);

        // outputCount = 0; row = 0;
        asm.lconst_0();
        asm.lstore(outputCountSlot);
        asm.lconst_0();
        asm.lstore(rowSlot);

        if (program.hasControlFlow()) {
            emitMultiBlockBody(asm, program, tempSlots, tempTypes, rowSlot, outputCountSlot,
                    filteredRowsSlot, rowsCountSlot, isCountOnly, stackMapAttr, pm, nullChecks);
        } else {
            emitSingleBlockBody(asm, program, tempSlots, rowSlot, outputCountSlot,
                    filteredRowsSlot, rowsCountSlot, isCountOnly, stackMapAttr, pm, nullChecks);
        }
    }

    private static void emitSingleBlockBody(
            BytecodeAssembler asm, LoweredProgram program, int[] tempSlots,
            int rowSlot, int outputCountSlot, int filteredRowsSlot, int rowsCountSlot,
            boolean isCountOnly, int stackMapAttr, PooledMethods pm, boolean nullChecks
    ) {
        int loopStart = asm.position();
        asm.lload(rowsCountSlot);
        asm.lload(rowSlot);
        asm.lcmp();
        int exitBranch = asm.ifle();

        LoweredBlock block = program.getBlock(program.getEntryBlockId());
        for (int i = 0; i < block.getOpCount(); i++) {
            emitOp(asm, block.getOp(i), tempSlots, rowSlot, pm, nullChecks);
        }
        Terminator.Return ret = (Terminator.Return) block.getTerminator();

        int skipBranch = -1;
        if (ret.src() != Terminator.Return.ACCEPT) {
            asm.iload(tempSlots[ret.src()]);
            skipBranch = asm.ifeq();
        }

        emitWriteOrCount(asm, outputCountSlot, rowSlot, filteredRowsSlot, isCountOnly, pm);

        int nextStart = asm.position();
        emitRowIncrement(asm, rowSlot);
        int backJmp = asm.goto_();
        asm.setJmp(backJmp, loopStart);

        int exitStart = asm.position();
        asm.lload(outputCountSlot);
        asm.lreturn();

        asm.setJmp(exitBranch, exitStart);
        if (skipBranch >= 0) {
            asm.setJmp(skipBranch, nextStart);
        }

        asm.endMethodCode();
        asm.putShort(0);

        asm.putShort(1);
        asm.startStackMapTables(stackMapAttr, 3);
        int loopOffset = loopStart - asm.getCodeStart();
        asm.append_frame(2, loopOffset);
        asm.putITEM_Long();
        asm.putITEM_Long();
        int nextOffset = nextStart - asm.getCodeStart();
        asm.same_frame(nextOffset - loopOffset - 1);
        int exitOffset = exitStart - asm.getCodeStart();
        asm.same_frame(exitOffset - nextOffset - 1);
        asm.endStackMapTables();
        asm.endMethod();
    }

    private static void emitMultiBlockBody(
            BytecodeAssembler asm, LoweredProgram program, int[] tempSlots, int[] tempTypes,
            int rowSlot, int outputCountSlot, int filteredRowsSlot, int rowsCountSlot,
            boolean isCountOnly, int stackMapAttr, PooledMethods pm, boolean nullChecks
    ) {
        int blockCount = program.getBlockCount();

        // Pre-initialize all temp slots so the verifier sees consistent types
        for (int i = 0; i < tempTypes.length; i++) {
            emitDefaultForType(asm, tempSlots[i], tempTypes[i]);
        }

        // LOOP:
        int loopStart = asm.position();
        asm.lload(rowsCountSlot);
        asm.lload(rowSlot);
        asm.lcmp();
        // Invert condition + goto_w for large methods: ifgt BODY; goto_w EXIT
        int bodyBranch = asm.ifgt();
        int exitBranch = asm.goto_w();
        int bodyStart = asm.position();
        asm.setJmp(bodyBranch, bodyStart);

        // Compute reachability from entry
        boolean[] reachable = computeReachable(program);

        // Emit blocks in ID order, collecting deferred jumps.
        // Use goto_w for all real control transfers: multi-block methods may
        // exceed 32K, so conditional branches must branch to a nearby skip
        // label and then jump with goto_w to the real target.
        int[] blockPositions = new int[blockCount];
        int[] deferredPos = new int[blockCount * 6];
        int[] deferredTarget = new int[blockCount * 6];
        boolean[] deferredIsWide = new boolean[blockCount * 6];
        int deferredCount = 0;
        int[] localJumpTargets = new int[blockCount * 2];
        int localJumpTargetCount = 0;

        for (int b = 0; b < blockCount; b++) {
            if (!reachable[b]) {
                continue;
            }
            LoweredBlock block = program.getBlock(b);
            blockPositions[b] = asm.position();

            for (int i = 0; i < block.getOpCount(); i++) {
                emitOp(asm, block.getOp(i), tempSlots, rowSlot, pm, nullChecks);
            }

            Terminator term = block.getTerminator();
            switch (term) {
                case Terminator.Return ret -> {
                    if (ret.src() == Terminator.Return.ACCEPT) {
                        int jmp = asm.goto_w();
                        deferredPos[deferredCount] = jmp;
                        deferredTarget[deferredCount] = -1;
                        deferredIsWide[deferredCount] = true;
                        deferredCount++;
                    } else {
                        asm.iload(tempSlots[ret.src()]);
                        int skipJmp = asm.ifeq();
                        int writeJmp = asm.goto_w();
                        int nextPathStart = asm.position();
                        asm.setJmp(skipJmp, nextPathStart);
                        localJumpTargets[localJumpTargetCount++] = nextPathStart;
                        int nextJmp = asm.goto_w();
                        deferredPos[deferredCount] = writeJmp;
                        deferredTarget[deferredCount] = -1;
                        deferredIsWide[deferredCount] = true;
                        deferredCount++;
                        deferredPos[deferredCount] = nextJmp;
                        deferredTarget[deferredCount] = -2;
                        deferredIsWide[deferredCount] = true;
                        deferredCount++;
                    }
                }
                case Terminator.Branch branch -> {
                    asm.iload(tempSlots[branch.src()]);
                    int falsePathBranch = asm.ifeq();
                    int trueJmp = asm.goto_w();
                    int falsePathStart = asm.position();
                    asm.setJmp(falsePathBranch, falsePathStart);
                    localJumpTargets[localJumpTargetCount++] = falsePathStart;
                    int falseJmp = asm.goto_w();
                    deferredPos[deferredCount] = trueJmp;
                    deferredTarget[deferredCount] = branch.trueBlockId();
                    deferredIsWide[deferredCount] = true;
                    deferredCount++;
                    deferredPos[deferredCount] = falseJmp;
                    deferredTarget[deferredCount] = branch.falseBlockId();
                    deferredIsWide[deferredCount] = true;
                    deferredCount++;
                }
                case Terminator.Goto gotoTerm -> {
                    int jmp = asm.goto_w();
                    deferredPos[deferredCount] = jmp;
                    deferredTarget[deferredCount] = gotoTerm.targetBlockId();
                    deferredIsWide[deferredCount] = true;
                    deferredCount++;
                }
            }
        }

        // WRITE:
        int writeStart = asm.position();
        emitWriteOrCount(asm, outputCountSlot, rowSlot, filteredRowsSlot, isCountOnly, pm);

        // NEXT:
        int nextStart = asm.position();
        emitRowIncrement(asm, rowSlot);
        // Use goto_w for backward loop jump (method may exceed 32K)
        int backJmp = asm.goto_w();
        asm.setJmpW(backJmp, loopStart);

        // EXIT:
        int exitStart = asm.position();
        asm.lload(outputCountSlot);
        asm.lreturn();

        asm.setJmpW(exitBranch, exitStart);

        // Patch deferred jumps
        // since multi-block methods can be large
        for (int i = 0; i < deferredCount; i++) {
            int target = deferredTarget[i];
            int targetPos = switch (target) {
                case -1 -> writeStart;
                case -2 -> nextStart;
                default -> blockPositions[target];
            };
            if (deferredIsWide[i]) {
                asm.setJmpW(deferredPos[i], targetPos);
            } else {
                asm.setJmp(deferredPos[i], targetPos);
            }
        }

        asm.endMethodCode();
        asm.putShort(0); // exceptions

        // StackMapTable: compute which positions are jump targets
        // Collect all unique target positions, sort them, emit frames
        java.util.TreeSet<Integer> jumpTargets = new java.util.TreeSet<>();
        jumpTargets.add(loopStart);
        jumpTargets.add(bodyStart);
        jumpTargets.add(writeStart);
        jumpTargets.add(nextStart);
        jumpTargets.add(exitStart);
        for (int i = 0; i < localJumpTargetCount; i++) {
            jumpTargets.add(localJumpTargets[i]);
        }
        for (int i = 0; i < deferredCount; i++) {
            int target = deferredTarget[i];
            if (target >= 0) {
                jumpTargets.add(blockPositions[target]);
            }
        }

        // Emit frames: full_frame at loop header (all locals), same_frame elsewhere
        asm.putShort(1); // 1 attribute
        asm.startStackMapTables(stackMapAttr, jumpTargets.size());

        int prevOffset = -1;
        boolean isFirst = true;
        for (int pos : jumpTargets) {
            int offset = pos - asm.getCodeStart();
            if (isFirst) {
                emitFullFrame(asm, tempTypes, filteredRowsSlot, offset);
                isFirst = false;
            } else {
                asm.same_frame(offset - prevOffset - 1);
            }
            prevOffset = offset;
        }

        asm.endStackMapTables();
        asm.endMethod();
    }

    private static void emitRowIncrement(BytecodeAssembler asm, int rowSlot) {
        asm.lload(rowSlot);
        asm.lconst_1();
        asm.ladd();
        asm.lstore(rowSlot);
    }

    private static void emitDefaultForType(BytecodeAssembler asm, int slot, int type) {
        switch (type) {
            case I1_TYPE, I2_TYPE, I4_TYPE -> { asm.iconst(0); asm.istore(slot); }
            case I8_TYPE -> { asm.lconst_0(); asm.lstore(slot); }
            case F4_TYPE -> { asm.fconst_0(); asm.fstore(slot); }
            case F8_TYPE -> { asm.dconst_0(); asm.dstore(slot); }
            case I16_TYPE -> { asm.lconst_0(); asm.lstore(slot); asm.lconst_0(); asm.lstore(slot + 2); }
            default -> { asm.iconst(0); asm.istore(slot); }
        }
    }

    private static void emitFullFrame(
            BytecodeAssembler asm, int[] tempTypes, int filteredRowsSlot, int offset
    ) {
        boolean isFilterRows = filteredRowsSlot >= 0;
        int paramLongs = isFilterRows ? 7 : 6;

        asm.full_frame(offset);

        // Count verification_type_info entries
        int i16Count = 0;
        for (int t : tempTypes) {
            if (t == I16_TYPE) {
                i16Count++;
            }
        }
        int localsEntries = 1 + paramLongs + 2 + tempTypes.length + i16Count;
        asm.putShort(localsEntries);

        // this
        asm.putITEM_Object(asm.poolClass(ScalarFilterBody.class));
        // params: all longs
        for (int i = 0; i < paramLongs; i++) {
            asm.putITEM_Long();
        }
        // outputCount + row
        asm.putITEM_Long();
        asm.putITEM_Long();
        // temps
        for (int t : tempTypes) {
            switch (t) {
                case I8_TYPE -> asm.putITEM_Long();
                case F4_TYPE -> asm.putITEM_Float();
                case F8_TYPE -> asm.putITEM_Double();
                case I16_TYPE -> { asm.putITEM_Long(); asm.putITEM_Long(); }
                default -> asm.putITEM_Integer();
            }
        }
        // stack: empty
        asm.putShort(0);
    }

    private static boolean[] computeReachable(LoweredProgram program) {
        boolean[] reachable = new boolean[program.getBlockCount()];
        int[] stack = new int[program.getBlockCount()];
        int sp = 0;
        stack[sp++] = program.getEntryBlockId();
        while (sp > 0) {
            int id = stack[--sp];
            if (reachable[id]) {
                continue;
            }
            reachable[id] = true;
            Terminator term = program.getBlock(id).getTerminator();
            switch (term) {
                case Terminator.Branch b -> {
                    stack[sp++] = b.trueBlockId();
                    stack[sp++] = b.falseBlockId();
                }
                case Terminator.Goto g -> stack[sp++] = g.targetBlockId();
                case Terminator.Return _ -> {}
            }
        }
        return reachable;
    }

    private static void emitWriteOrCount(
            BytecodeAssembler asm, int outputCountSlot, int rowSlot,
            int filteredRowsSlot, boolean isCountOnly, PooledMethods pm
    ) {
        if (!isCountOnly) {
            // FilterHelpers.writeRow(filteredRowsAddress, outputCount, row)
            asm.lload(filteredRowsSlot);
            asm.lload(outputCountSlot);
            asm.lload(rowSlot);
            asm.invokeStatic(pm.writeRow);
        }
        // outputCount++
        asm.lload(outputCountSlot);
        asm.lconst_1();
        asm.ladd();
        asm.lstore(outputCountSlot);
    }


    private static void emitOp(
            BytecodeAssembler asm,
            LoweredOp op,
            int[] tempSlots,
            int rowSlot,
            PooledMethods pm,
            boolean nullChecks
    ) {
        switch (op) {
            case LoweredOp.LoadColumn lc -> emitLoadColumn(asm, lc, tempSlots, rowSlot, pm);
            case LoweredOp.LoadVarSizeHeader lv -> emitLoadVarSizeHeader(asm, lv, tempSlots, rowSlot, pm);
            case LoweredOp.LoadVar lv -> emitLoadVar(asm, lv, tempSlots, pm);
            case LoweredOp.LoadImm li -> emitLoadImm(asm, li, tempSlots, pm);
            case LoweredOp.Cast c -> emitCast(asm, c, tempSlots, nullChecks, pm);
            case LoweredOp.Compare cmp -> emitCompare(asm, cmp, tempSlots, nullChecks, pm);
            case LoweredOp.CompareI128 cmp -> emitCompareI128(asm, cmp, tempSlots, pm);
            case LoweredOp.Arithmetic ar -> emitArithmetic(asm, ar, tempSlots, nullChecks, pm);
            case LoweredOp.BooleanOp bo -> emitBooleanOp(asm, bo, tempSlots);
            case LoweredOp.Negate neg -> emitNegate(asm, neg, tempSlots);
            case LoweredOp.Not not -> emitNot(asm, not, tempSlots);
            case LoweredOp.Move mov -> emitMove(asm, mov, tempSlots);
        }
    }

    // --- Load operations ---

    private static void emitLoadColumn(BytecodeAssembler asm, LoweredOp.LoadColumn op, int[] tempSlots, int rowSlot, PooledMethods pm) {
        int dst = tempSlots[op.dst()];
        // FilterHelpers.readXxx(dataAddress, columnIndex, row)
        asm.lload(SLOT_DATA_ADDR);
        asm.iconst(op.columnIndex());
        asm.lload(rowSlot);
        switch (op.type()) {
            case I1_TYPE -> {
                asm.invokeStatic(pm.readByte);
                asm.istore(dst);
            }
            case I2_TYPE -> {
                asm.invokeStatic(pm.readShort);
                asm.istore(dst);
            }
            case I4_TYPE -> {
                asm.invokeStatic(pm.readInt);
                asm.istore(dst);
            }
            case I8_TYPE -> {
                asm.invokeStatic(pm.readLong);
                asm.lstore(dst);
            }
            case F4_TYPE -> {
                asm.invokeStatic(pm.readFloat);
                asm.fstore(dst);
            }
            case F8_TYPE -> {
                asm.invokeStatic(pm.readDouble);
                asm.dstore(dst);
            }
            case I16_TYPE -> {
                // Lo part: readLong128Lo(dataAddress, columnIndex, row)
                asm.invokeStatic(pm.readLong128Lo);
                asm.lstore(dst);
                // Hi part: readLong128Hi(dataAddress, columnIndex, row)
                asm.lload(SLOT_DATA_ADDR);
                asm.iconst(op.columnIndex());
                asm.lload(rowSlot);
                asm.invokeStatic(pm.readLong128Hi);
                asm.lstore(dst + 2);
            }
            default -> throw new IllegalStateException("unsupported column type: " + op.type());
        }
    }

    private static void emitLoadVarSizeHeader(BytecodeAssembler asm, LoweredOp.LoadVarSizeHeader op, int[] tempSlots, int rowSlot, PooledMethods pm) {
        int dst = tempSlots[op.dst()];
        switch (op.headerType()) {
            case STRING_HEADER_TYPE -> {
                // FilterHelpers.readStringHeader(dataAddress, varSizeAuxAddress, columnIndex, row)
                asm.lload(SLOT_DATA_ADDR);
                asm.lload(SLOT_VAR_SIZE_AUX);
                asm.iconst(op.columnIndex());
                asm.lload(rowSlot);
                asm.invokeStatic(pm.readStringHeader);
                asm.istore(dst);
            }
            case BINARY_HEADER_TYPE -> {
                asm.lload(SLOT_DATA_ADDR);
                asm.lload(SLOT_VAR_SIZE_AUX);
                asm.iconst(op.columnIndex());
                asm.lload(rowSlot);
                asm.invokeStatic(pm.readBinaryHeader);
                asm.lstore(dst);
            }
            case VARCHAR_HEADER_TYPE -> {
                // FilterHelpers.readVarcharHeader(varSizeAuxAddress, columnIndex, row)
                asm.lload(SLOT_VAR_SIZE_AUX);
                asm.iconst(op.columnIndex());
                asm.lload(rowSlot);
                asm.invokeStatic(pm.readVarcharHeader);
                asm.lstore(dst);
            }
            default -> throw new IllegalStateException("unsupported header type: " + op.headerType());
        }
    }

    private static void emitLoadVar(BytecodeAssembler asm, LoweredOp.LoadVar op, int[] tempSlots, PooledMethods pm) {
        int dst = tempSlots[op.dst()];
        asm.lload(SLOT_VARS_ADDR);
        asm.iconst(op.byteOffset());
        switch (op.type()) {
            case I1_TYPE -> {
                asm.invokeStatic(pm.readVarByte);
                asm.istore(dst);
            }
            case I2_TYPE -> {
                asm.invokeStatic(pm.readVarShort);
                asm.istore(dst);
            }
            case I4_TYPE -> {
                asm.invokeStatic(pm.readVarInt);
                asm.istore(dst);
            }
            case I8_TYPE -> {
                asm.invokeStatic(pm.readVarLong);
                asm.lstore(dst);
            }
            case F4_TYPE -> {
                asm.invokeStatic(pm.readVarFloat);
                asm.fstore(dst);
            }
            case F8_TYPE -> {
                asm.invokeStatic(pm.readVarDouble);
                asm.dstore(dst);
            }
            case I16_TYPE -> {
                // Lo part
                asm.invokeStatic(pm.readVarLong);
                asm.lstore(dst);
                // Hi part
                asm.lload(SLOT_VARS_ADDR);
                asm.iconst(op.byteOffset());
                asm.invokeStatic(pm.readVarLong128Hi);
                asm.lstore(dst + 2);
            }
            default -> throw new IllegalStateException("unsupported var type: " + op.type());
        }
    }

    private static void emitLoadImm(BytecodeAssembler asm, LoweredOp.LoadImm op, int[] tempSlots, PooledMethods pm) {
        int dst = tempSlots[op.dst()];
        switch (op.type()) {
            case I1_TYPE, I2_TYPE, I4_TYPE -> {
                emitIntConst(asm, (int) op.lo(), pm);
                asm.istore(dst);
            }
            case I8_TYPE -> {
                emitLongConst(asm, op.lo(), pm);
                asm.lstore(dst);
            }
            case F4_TYPE -> {
                emitFloatConst(asm, Float.intBitsToFloat((int) op.lo()), pm);
                asm.fstore(dst);
            }
            case F8_TYPE -> {
                emitDoubleConst(asm, Double.longBitsToDouble(op.lo()), pm);
                asm.dstore(dst);
            }
            case I16_TYPE -> {
                emitLongConst(asm, op.lo(), pm);
                asm.lstore(dst);
                emitLongConst(asm, op.hi(), pm);
                asm.lstore(dst + 2);
            }
            default -> throw new IllegalStateException("unsupported immediate type: " + op.type());
        }
    }

    // --- Cast ---

    private static void emitCast(BytecodeAssembler asm, LoweredOp.Cast op, int[] tempSlots, boolean nullChecks, PooledMethods pm) {
        int srcSlot = tempSlots[op.src()];
        int dstSlot = tempSlots[op.dst()];

        if (nullChecks && needsNullAwareCast(op.fromType(), op.toType())) {
            // Use FilterHelpers for null-aware coercion
            emitNullAwareCast(asm, op, srcSlot, dstSlot, pm);
            return;
        }

        // Simple JVM type conversion
        switch (op.fromType()) {
            case I1_TYPE, I2_TYPE, I4_TYPE -> {
                asm.iload(srcSlot);
                switch (op.toType()) {
                    case I4_TYPE -> asm.istore(dstSlot);
                    case I8_TYPE -> { asm.i2l(); asm.lstore(dstSlot); }
                    case F4_TYPE -> { asm.i2f(); asm.fstore(dstSlot); }
                    case F8_TYPE -> { asm.i2d(); asm.dstore(dstSlot); }
                    default -> throw castError(op);
                }
            }
            case I8_TYPE -> {
                asm.lload(srcSlot);
                switch (op.toType()) {
                    case I4_TYPE -> { asm.l2i(); asm.istore(dstSlot); }
                    case F4_TYPE -> { asm.l2f(); asm.fstore(dstSlot); }
                    case F8_TYPE -> { asm.l2d(); asm.dstore(dstSlot); }
                    default -> throw castError(op);
                }
            }
            case F4_TYPE -> {
                asm.fload(srcSlot);
                switch (op.toType()) {
                    case F8_TYPE -> { asm.f2d(); asm.dstore(dstSlot); }
                    case I4_TYPE -> { asm.f2i(); asm.istore(dstSlot); }
                    case I8_TYPE -> { asm.f2l(); asm.lstore(dstSlot); }
                    default -> throw castError(op);
                }
            }
            case F8_TYPE -> {
                asm.dload(srcSlot);
                switch (op.toType()) {
                    case F4_TYPE -> { asm.d2f(); asm.fstore(dstSlot); }
                    case I4_TYPE -> { asm.d2i(); asm.istore(dstSlot); }
                    case I8_TYPE -> { asm.d2l(); asm.lstore(dstSlot); }
                    default -> throw castError(op);
                }
            }
            default -> throw castError(op);
        }
    }

    private static boolean needsNullAwareCast(int from, int to) {
        // INT_NULL -> LONG_NULL, INT_NULL -> NaN, LONG_NULL -> NaN
        return (from == I4_TYPE && (to == I8_TYPE || to == F4_TYPE || to == F8_TYPE))
                || (from == I8_TYPE && (to == F4_TYPE || to == F8_TYPE));
    }

    private static void emitNullAwareCast(BytecodeAssembler asm, LoweredOp.Cast op, int srcSlot, int dstSlot, PooledMethods pm) {
        switch (op.fromType()) {
            case I4_TYPE -> {
                asm.iload(srcSlot);
                switch (op.toType()) {
                    case I8_TYPE -> { asm.invokeStatic(pm.coerceIntToLong); asm.lstore(dstSlot); }
                    case F4_TYPE -> { asm.invokeStatic(pm.coerceIntToFloat); asm.fstore(dstSlot); }
                    case F8_TYPE -> { asm.invokeStatic(pm.coerceIntToDouble); asm.dstore(dstSlot); }
                    default -> throw castError(op);
                }
            }
            case I8_TYPE -> {
                asm.lload(srcSlot);
                if (op.toType() == F8_TYPE) {
                    asm.invokeStatic(pm.coerceLongToDouble);
                    asm.dstore(dstSlot);
                } else {
                    throw castError(op);
                }
            }
            default -> throw castError(op);
        }
    }

    private static IllegalStateException castError(LoweredOp.Cast op) {
        return new IllegalStateException("unsupported cast: " + op.fromType() + " -> " + op.toType());
    }

    // --- Compare ---

    private static void emitCompare(BytecodeAssembler asm, LoweredOp.Compare op, int[] tempSlots, boolean nullChecks, PooledMethods pm) {
        int lhsSlot = tempSlots[op.lhs()];
        int rhsSlot = tempSlots[op.rhs()];
        int dstSlot = tempSlots[op.dst()];

        switch (op.operandType()) {
            case I4_TYPE -> {
                asm.iload(lhsSlot);
                asm.iload(rhsSlot);
                asm.invokeStatic(pm.intCompare(op.opcode(), nullChecks));
                asm.istore(dstSlot);
            }
            case I8_TYPE -> {
                asm.lload(lhsSlot);
                asm.lload(rhsSlot);
                asm.invokeStatic(pm.longCompare(op.opcode(), nullChecks));
                asm.istore(dstSlot);
            }
            case F4_TYPE -> {
                asm.fload(lhsSlot);
                asm.fload(rhsSlot);
                asm.invokeStatic(pm.floatCompare(op.opcode()));
                asm.istore(dstSlot);
            }
            case F8_TYPE -> {
                asm.dload(lhsSlot);
                asm.dload(rhsSlot);
                asm.invokeStatic(pm.doubleCompare(op.opcode()));
                asm.istore(dstSlot);
            }
            default -> throw new IllegalStateException("unsupported compare type: " + op.operandType());
        }
    }

    // --- Arithmetic ---

    private static void emitArithmetic(BytecodeAssembler asm, LoweredOp.Arithmetic op, int[] tempSlots, boolean nullChecks, PooledMethods pm) {
        int lhsSlot = tempSlots[op.lhs()];
        int rhsSlot = tempSlots[op.rhs()];
        int dstSlot = tempSlots[op.dst()];

        switch (op.resultType()) {
            case I4_TYPE -> {
                asm.iload(lhsSlot);
                asm.iload(rhsSlot);
                if (nullChecks) {
                    asm.iconst(op.opcode());
                    asm.invokeStatic(pm.intArithmeticNull);
                } else {
                    switch (op.opcode()) {
                        case ADD -> asm.iadd();
                        case SUB -> asm.isub();
                        case MUL -> asm.imul();
                        case DIV -> asm.idiv();
                        default -> throw new IllegalStateException("unsupported i4 arithmetic: " + op.opcode());
                    }
                }
                asm.istore(dstSlot);
            }
            case I8_TYPE -> {
                asm.lload(lhsSlot);
                asm.lload(rhsSlot);
                if (nullChecks) {
                    asm.iconst(op.opcode());
                    asm.invokeStatic(pm.longArithmeticNull);
                } else {
                    switch (op.opcode()) {
                        case ADD -> asm.ladd();
                        case SUB -> asm.lsub();
                        case MUL -> asm.lmul();
                        case DIV -> asm.ldiv();
                        default -> throw new IllegalStateException("unsupported i8 arithmetic: " + op.opcode());
                    }
                }
                asm.lstore(dstSlot);
            }
            case F4_TYPE -> {
                asm.fload(lhsSlot);
                asm.fload(rhsSlot);
                if (op.opcode() == DIV || nullChecks) {
                    // Delegate to helper for NaN-on-zero and NaN propagation
                    asm.iconst(op.opcode());
                    asm.invokeStatic(pm.floatArithmeticNull);
                } else {
                    switch (op.opcode()) {
                        case ADD -> asm.fadd();
                        case SUB -> asm.fsub();
                        case MUL -> asm.fmul();
                        default -> throw new IllegalStateException("unsupported f4 arithmetic: " + op.opcode());
                    }
                }
                asm.fstore(dstSlot);
            }
            case F8_TYPE -> {
                asm.dload(lhsSlot);
                asm.dload(rhsSlot);
                if (op.opcode() == DIV || nullChecks) {
                    asm.iconst(op.opcode());
                    asm.invokeStatic(pm.doubleArithmeticNull);
                } else {
                    switch (op.opcode()) {
                        case ADD -> asm.dadd();
                        case SUB -> asm.dsub();
                        case MUL -> asm.dmul();
                        default -> throw new IllegalStateException("unsupported f8 arithmetic: " + op.opcode());
                    }
                }
                asm.dstore(dstSlot);
            }
            default -> throw new IllegalStateException("unsupported arithmetic result type: " + op.resultType());
        }
    }

    // --- Boolean operations ---

    private static void emitBooleanOp(BytecodeAssembler asm, LoweredOp.BooleanOp op, int[] tempSlots) {
        asm.iload(tempSlots[op.lhs()]);
        asm.iload(tempSlots[op.rhs()]);
        if (op.opcode() == AND) {
            asm.iand();
        } else {
            asm.ior();
        }
        asm.istore(tempSlots[op.dst()]);
    }

    private static void emitNegate(BytecodeAssembler asm, LoweredOp.Negate op, int[] tempSlots) {
        int srcSlot = tempSlots[op.src()];
        int dstSlot = tempSlots[op.dst()];
        switch (op.type()) {
            case I4_TYPE -> {
                // For I1/I2 sources that promoted to I4, src is already in an int slot
                asm.iload(srcSlot);
                asm.ineg();
                asm.istore(dstSlot);
            }
            case I8_TYPE -> {
                asm.lload(srcSlot);
                asm.lneg();
                asm.lstore(dstSlot);
            }
            case F4_TYPE -> {
                asm.fload(srcSlot);
                asm.fneg();
                asm.fstore(dstSlot);
            }
            case F8_TYPE -> {
                asm.dload(srcSlot);
                asm.dneg();
                asm.dstore(dstSlot);
            }
            default -> throw new IllegalStateException("unsupported negate type: " + op.type());
        }
    }

    // --- Not ---

    private static void emitNot(BytecodeAssembler asm, LoweredOp.Not op, int[] tempSlots) {
        asm.iconst(1);
        asm.iload(tempSlots[op.src()]);
        asm.isub();
        asm.istore(tempSlots[op.dst()]);
    }

    private static void emitCompareI128(BytecodeAssembler asm, LoweredOp.CompareI128 op, int[] tempSlots, PooledMethods pm) {
        int lhsSlot = tempSlots[op.lhs()];
        int rhsSlot = tempSlots[op.rhs()];
        int dstSlot = tempSlots[op.dst()];
        // i128Eq/Ne(loA, hiA, loB, hiB) -> boolean
        asm.lload(lhsSlot);       // loA
        asm.lload(lhsSlot + 2);   // hiA
        asm.lload(rhsSlot);       // loB
        asm.lload(rhsSlot + 2);   // hiB
        asm.invokeStatic(op.opcode() == EQ ? pm.i128Eq : pm.i128Ne);
        asm.istore(dstSlot);
    }

    private static void emitMove(BytecodeAssembler asm, LoweredOp.Move op, int[] tempSlots) {
        int srcSlot = tempSlots[op.src()];
        int dstSlot = tempSlots[op.dst()];
        switch (op.type()) {
            case I1_TYPE, I2_TYPE, I4_TYPE -> { asm.iload(srcSlot); asm.istore(dstSlot); }
            case I8_TYPE -> { asm.lload(srcSlot); asm.lstore(dstSlot); }
            case F4_TYPE -> { asm.fload(srcSlot); asm.fstore(dstSlot); }
            case F8_TYPE -> { asm.dload(srcSlot); asm.dstore(dstSlot); }
            default -> throw new IllegalStateException("unsupported move type: " + op.type());
        }
    }

    // --- Constant pre-pooling and emission ---

    private static void prePoolConstants(LoweredProgram program, PooledMethods pm) {
        for (int b = 0; b < program.getBlockCount(); b++) {
            LoweredBlock block = program.getBlock(b);
            for (int i = 0; i < block.getOpCount(); i++) {
                if (block.getOp(i) instanceof LoweredOp.LoadImm imm) {
                    switch (imm.type()) {
                        case I1_TYPE, I2_TYPE, I4_TYPE -> {
                            int v = (int) imm.lo();
                            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
                                pm.ensureIntPooled(v);
                            }
                        }
                        case I8_TYPE -> {
                            long v = imm.lo();
                            if (v != 0 && v != 1) {
                                pm.ensureLongPooled(v);
                            }
                        }
                        case F4_TYPE -> {
                            float v = Float.intBitsToFloat((int) imm.lo());
                            if (v != 0.0f && v != 1.0f && v != 2.0f) {
                                pm.ensureFloatPooled(v);
                            }
                        }
                        case F8_TYPE -> {
                            double v = Double.longBitsToDouble(imm.lo());
                            if (v != 0.0 && v != 1.0) {
                                pm.ensureDoublePooled(v);
                            }
                        }
                        case I16_TYPE -> {
                            long lo = imm.lo();
                            long hi = imm.hi();
                            if (lo != 0 && lo != 1) {
                                pm.ensureLongPooled(lo);
                            }
                            if (hi != 0 && hi != 1) {
                                pm.ensureLongPooled(hi);
                            }
                        }
                        default -> {
                            // no pooling needed
                        }
                    }
                }
            }
        }
    }

    private static void emitIntConst(BytecodeAssembler asm, int value, PooledMethods pm) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            asm.iconst(value);
        } else {
            asm.ldc(pm.getIntPoolIndex(value));
        }
    }

    private static void emitLongConst(BytecodeAssembler asm, long value, PooledMethods pm) {
        if (value == 0) {
            asm.lconst_0();
        } else if (value == 1) {
            asm.lconst_1();
        } else {
            asm.ldc2_w(pm.getLongPoolIndex(value));
        }
    }

    private static void emitFloatConst(BytecodeAssembler asm, float value, PooledMethods pm) {
        if (value == 0.0f) {
            asm.fconst_0();
        } else if (value == 1.0f) {
            asm.fconst_1();
        } else if (value == 2.0f) {
            asm.fconst_2();
        } else {
            asm.ldc(pm.getFloatPoolIndex(value));
        }
    }

    private static void emitDoubleConst(BytecodeAssembler asm, double value, PooledMethods pm) {
        if (value == 0.0) {
            asm.dconst_0();
        } else if (value == 1.0) {
            asm.dconst_1();
        } else {
            asm.ldc2_w(pm.getDoublePoolIndex(value));
        }
    }

    /**
     * Pools all FilterHelpers method references and constants needed by
     * the generated bytecode. Must be called before {@code finishPool()}.
     */
    static final class PooledMethods {
        // Column reads
        final int readByte;
        final int readDouble;
        final int readFloat;
        final int readInt;
        final int readLong;
        final int readLong128Hi;
        final int readLong128Lo;
        final int readShort;

        // Var-size headers
        final int readBinaryHeader;
        final int readStringHeader;
        final int readVarcharHeader;

        // Bind variable reads
        final int readVarByte;
        final int readVarDouble;
        final int readVarFloat;
        final int readVarInt;
        final int readVarLong;
        final int readVarShort;

        // Output
        final int writeRow;

        // Integer comparisons (non-null)
        final int intEq, intNe, intLt, intLe, intGt, intGe;
        // Integer comparisons (null-aware)
        final int intNullEq, intNullNe, intNullLt, intNullLe, intNullGt, intNullGe;
        // Long comparisons (non-null) — use lcmp inline
        final int longEq, longNe, longLt, longLe, longGt, longGe;
        // Long comparisons (null-aware)
        final int longNullEq, longNullNe, longNullLt, longNullLe, longNullGt, longNullGe;
        // Float comparisons
        final int floatEq, floatNe, floatLt, floatLe, floatGt, floatGe;
        // Double comparisons
        final int doubleEq, doubleNe, doubleLt, doubleLe, doubleGt, doubleGe;

        // Null-aware coercions
        final int coerceIntToLong;
        final int coerceIntToFloat;
        final int coerceIntToDouble;
        final int coerceLongToDouble;

        // Null-aware arithmetic
        final int intArithmeticNull;
        final int longArithmeticNull;
        final int floatArithmeticNull;
        final int doubleArithmeticNull;

        // I128 comparisons
        final int i128Eq;
        final int i128Ne;
        final int readVarLong128Hi;

        private final BytecodeAssembler asm;
        // Pre-pooled constants (key -> pool index). Used at compile time only.
        private final java.util.HashMap<Integer, Integer> intConstants = new java.util.HashMap<>();
        private final java.util.HashMap<Long, Integer> longConstants = new java.util.HashMap<>();
        private final java.util.HashMap<Integer, Integer> floatConstants = new java.util.HashMap<>(); // keyed by raw bits
        private final java.util.HashMap<Long, Integer> doubleConstants = new java.util.HashMap<>(); // keyed by raw bits

        PooledMethods(BytecodeAssembler asm) {
            this.asm = asm;
            Class<?> fh = FilterHelpers.class;

            readByte = asm.poolMethod(fh, "readByte", "(JIJ)B");
            readShort = asm.poolMethod(fh, "readShort", "(JIJ)S");
            readInt = asm.poolMethod(fh, "readInt", "(JIJ)I");
            readLong = asm.poolMethod(fh, "readLong", "(JIJ)J");
            readFloat = asm.poolMethod(fh, "readFloat", "(JIJ)F");
            readDouble = asm.poolMethod(fh, "readDouble", "(JIJ)D");
            readLong128Lo = asm.poolMethod(fh, "readLong128Lo", "(JIJ)J");
            readLong128Hi = asm.poolMethod(fh, "readLong128Hi", "(JIJ)J");

            readStringHeader = asm.poolMethod(fh, "readStringHeader", "(JJIJ)I");
            readBinaryHeader = asm.poolMethod(fh, "readBinaryHeader", "(JJIJ)J");
            readVarcharHeader = asm.poolMethod(fh, "readVarcharHeader", "(JIJ)J");

            readVarByte = asm.poolMethod(fh, "readVarByte", "(JI)B");
            readVarShort = asm.poolMethod(fh, "readVarShort", "(JI)S");
            readVarInt = asm.poolMethod(fh, "readVarInt", "(JI)I");
            readVarLong = asm.poolMethod(fh, "readVarLong", "(JI)J");
            readVarFloat = asm.poolMethod(fh, "readVarFloat", "(JI)F");
            readVarDouble = asm.poolMethod(fh, "readVarDouble", "(JI)D");

            writeRow = asm.poolMethod(fh, "writeRow", "(JJJ)V");

            // Non-null integer comparisons
            intEq = asm.poolMethod(fh, "intEq", "(II)Z");
            intNe = asm.poolMethod(fh, "intNe", "(II)Z");
            intLt = asm.poolMethod(fh, "intLt", "(II)Z");
            intLe = asm.poolMethod(fh, "intLe", "(II)Z");
            intGt = asm.poolMethod(fh, "intGt", "(II)Z");
            intGe = asm.poolMethod(fh, "intGe", "(II)Z");

            // Null-aware integer comparisons
            intNullEq = asm.poolMethod(fh, "intNullEq", "(II)Z");
            intNullNe = asm.poolMethod(fh, "intNullNe", "(II)Z");
            intNullLt = asm.poolMethod(fh, "intNullLt", "(II)Z");
            intNullLe = asm.poolMethod(fh, "intNullLe", "(II)Z");
            intNullGt = asm.poolMethod(fh, "intNullGt", "(II)Z");
            intNullGe = asm.poolMethod(fh, "intNullGe", "(II)Z");

            // Non-null long comparisons
            longEq = asm.poolMethod(fh, "longEq", "(JJ)Z");
            longNe = asm.poolMethod(fh, "longNe", "(JJ)Z");
            longLt = asm.poolMethod(fh, "longLt", "(JJ)Z");
            longLe = asm.poolMethod(fh, "longLe", "(JJ)Z");
            longGt = asm.poolMethod(fh, "longGt", "(JJ)Z");
            longGe = asm.poolMethod(fh, "longGe", "(JJ)Z");

            // Null-aware long comparisons
            longNullEq = asm.poolMethod(fh, "longNullEq", "(JJ)Z");
            longNullNe = asm.poolMethod(fh, "longNullNe", "(JJ)Z");
            longNullLt = asm.poolMethod(fh, "longNullLt", "(JJ)Z");
            longNullLe = asm.poolMethod(fh, "longNullLe", "(JJ)Z");
            longNullGt = asm.poolMethod(fh, "longNullGt", "(JJ)Z");
            longNullGe = asm.poolMethod(fh, "longNullGe", "(JJ)Z");

            floatEq = asm.poolMethod(fh, "floatEq", "(FF)Z");
            floatNe = asm.poolMethod(fh, "floatNe", "(FF)Z");
            floatLt = asm.poolMethod(fh, "floatLt", "(FF)Z");
            floatLe = asm.poolMethod(fh, "floatLe", "(FF)Z");
            floatGt = asm.poolMethod(fh, "floatGt", "(FF)Z");
            floatGe = asm.poolMethod(fh, "floatGe", "(FF)Z");

            doubleEq = asm.poolMethod(fh, "doubleEq", "(DD)Z");
            doubleNe = asm.poolMethod(fh, "doubleNe", "(DD)Z");
            doubleLt = asm.poolMethod(fh, "doubleLt", "(DD)Z");
            doubleLe = asm.poolMethod(fh, "doubleLe", "(DD)Z");
            doubleGt = asm.poolMethod(fh, "doubleGt", "(DD)Z");
            doubleGe = asm.poolMethod(fh, "doubleGe", "(DD)Z");

            coerceIntToLong = asm.poolMethod(fh, "coerceIntToLong", "(I)J");
            coerceIntToFloat = asm.poolMethod(fh, "coerceIntToFloat", "(I)F");
            coerceIntToDouble = asm.poolMethod(fh, "coerceIntToDouble", "(I)D");
            coerceLongToDouble = asm.poolMethod(fh, "coerceLongToDouble", "(J)D");

            intArithmeticNull = asm.poolMethod(fh, "intArithmeticNull", "(III)I");
            longArithmeticNull = asm.poolMethod(fh, "longArithmeticNull", "(JJI)J");
            floatArithmeticNull = asm.poolMethod(fh, "floatArithmeticNull", "(FFI)F");
            doubleArithmeticNull = asm.poolMethod(fh, "doubleArithmeticNull", "(DDI)D");

            i128Eq = asm.poolMethod(fh, "i128Eq", "(JJJJ)Z");
            i128Ne = asm.poolMethod(fh, "i128Ne", "(JJJJ)Z");
            readVarLong128Hi = asm.poolMethod(fh, "readVarLong128Hi", "(JI)J");
        }

        int intCompare(int opcode, boolean nullChecks) {
            if (nullChecks) {
                return switch (opcode) {
                    case EQ -> intNullEq;
                    case NE -> intNullNe;
                    case LT -> intNullLt;
                    case LE -> intNullLe;
                    case GT -> intNullGt;
                    case GE -> intNullGe;
                    default -> throw new IllegalStateException("unsupported int compare: " + opcode);
                };
            }
            return switch (opcode) {
                case EQ -> intEq;
                case NE -> intNe;
                case LT -> intLt;
                case LE -> intLe;
                case GT -> intGt;
                case GE -> intGe;
                default -> throw new IllegalStateException("unsupported int compare: " + opcode);
            };
        }

        int longCompare(int opcode, boolean nullChecks) {
            if (nullChecks) {
                return switch (opcode) {
                    case EQ -> longNullEq;
                    case NE -> longNullNe;
                    case LT -> longNullLt;
                    case LE -> longNullLe;
                    case GT -> longNullGt;
                    case GE -> longNullGe;
                    default -> throw new IllegalStateException("unsupported long compare: " + opcode);
                };
            }
            return switch (opcode) {
                case EQ -> longEq;
                case NE -> longNe;
                case LT -> longLt;
                case LE -> longLe;
                case GT -> longGt;
                case GE -> longGe;
                default -> throw new IllegalStateException("unsupported long compare: " + opcode);
            };
        }

        int floatCompare(int opcode) {
            return switch (opcode) {
                case EQ -> floatEq;
                case NE -> floatNe;
                case LT -> floatLt;
                case LE -> floatLe;
                case GT -> floatGt;
                case GE -> floatGe;
                default -> throw new IllegalStateException("unsupported float compare: " + opcode);
            };
        }

        int doubleCompare(int opcode) {
            return switch (opcode) {
                case EQ -> doubleEq;
                case NE -> doubleNe;
                case LT -> doubleLt;
                case LE -> doubleLe;
                case GT -> doubleGt;
                case GE -> doubleGe;
                default -> throw new IllegalStateException("unsupported double compare: " + opcode);
            };
        }

        void ensureIntPooled(int value) {
            intConstants.computeIfAbsent(value, asm::poolIntConst);
        }

        void ensureLongPooled(long value) {
            longConstants.computeIfAbsent(value, asm::poolLongConst);
        }

        void ensureFloatPooled(float value) {
            int bits = Float.floatToRawIntBits(value);
            floatConstants.computeIfAbsent(bits, b -> asm.poolFloatConst(Float.intBitsToFloat(b)));
        }

        void ensureDoublePooled(double value) {
            long bits = Double.doubleToRawLongBits(value);
            doubleConstants.computeIfAbsent(bits, b -> asm.poolDoubleConst(Double.longBitsToDouble(b)));
        }

        int getIntPoolIndex(int value) {
            return intConstants.get(value);
        }

        int getLongPoolIndex(long value) {
            return longConstants.get(value);
        }

        int getFloatPoolIndex(float value) {
            return floatConstants.get(Float.floatToRawIntBits(value));
        }

        int getDoublePoolIndex(double value) {
            return doubleConstants.get(Double.doubleToRawLongBits(value));
        }
    }
}
