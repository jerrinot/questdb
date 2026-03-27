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

import io.questdb.std.BytecodeAssembler;

import java.util.HashMap;

import static io.questdb.jit.CompiledFilterIRSerializer.*;

/**
 * Compiles a {@link LoweredProgram} into a JVM class that implements
 * {@link VectorFilterBody}. The generated class evaluates the filter
 * in vector-width chunks using the Java Vector API.
 * <p>
 * Phase 1: I8_TYPE (long) only, no null checks, straight-line only.
 * <p>
 * Setup (MemorySegment creation, species, ByteOrder) uses FilterHelpers
 * to keep the constant pool small. The hot loop emits direct Vector API
 * calls (invokestatic/invokevirtual) so C2 can intrinsify them to SIMD.
 */
public final class VectorBytecodeFilterCompiler {

    // --- Method parameter slot layout (same as scalar compiler) ---
    private static final int SLOT_DATA_ADDR = 1;
    private static final int SLOT_VARS_ADDR = 7;
    private static final int FR_SLOT_FILTERED_ROWS = 11;
    private static final int FR_SLOT_ROWS_COUNT = 13;
    private static final int FR_FIRST_FREE = 15;
    private static final int CR_SLOT_ROWS_COUNT = 11;
    private static final int CR_FIRST_FREE = 13;

    public static VectorFilterBody compile(LoweredProgram program) {
        if (!isSupported(program)) {
            return null;
        }

        BytecodeAssembler asm = new BytecodeAssembler();
        asm.init(FilterHelpers.class);
        asm.setupPool();

        int thisClass = asm.poolClass(asm.poolUtf8("io/questdb/jit/vgen"));
        int ifaceClass = asm.poolClass(VectorFilterBody.class);
        int filterRowsName = asm.poolUtf8("filterRows");
        int filterRowsSig = asm.poolUtf8("(JJJJJJJ)J");
        int countRowsName = asm.poolUtf8("countRows");
        int countRowsSig = asm.poolUtf8("(JJJJJJ)J");
        int stackMapAttr = asm.poolUtf8("StackMapTable");

        Pool pool = new Pool(asm, program);

        asm.finishPool();

        // --- Class structure ---
        asm.defineClass(thisClass);
        asm.interfaceCount(1);
        asm.putShort(ifaceClass);
        asm.fieldCount(0);
        asm.methodCount(3);
        asm.defineDefaultConstructor();

        int tempCount = program.getNextTempId();

        // filterRows
        emitMethod(asm, program, pool, tempCount, filterRowsName, filterRowsSig,
                stackMapAttr, FR_SLOT_ROWS_COUNT, FR_SLOT_FILTERED_ROWS, FR_FIRST_FREE, false);

        // countRows
        emitMethod(asm, program, pool, tempCount, countRowsName, countRowsSig,
                stackMapAttr, CR_SLOT_ROWS_COUNT, -1, CR_FIRST_FREE, true);

        asm.putShort(0); // class attributes
        return asm.newInstance();
    }

    private static boolean isSupported(LoweredProgram program) {
        if (program.hasControlFlow()) {
            return false;
        }
        if (program.getOptions().isScalarOnly()) {
            return false;
        }
        LoweredBlock block = program.getBlock(program.getEntryBlockId());
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadVarSizeHeader) return false;
            if (op instanceof LoweredOp.CompareI128) return false;
            if (op instanceof LoweredOp.LoadColumn lc && lc.type() != I8_TYPE) return false;
            if (op instanceof LoweredOp.LoadVar lv && lv.type() != I8_TYPE) return false;
            if (op instanceof LoweredOp.LoadImm li && li.type() != I8_TYPE) return false;
            if (op instanceof LoweredOp.Cast) return false;
            if (op instanceof LoweredOp.Arithmetic a && a.resultType() != I8_TYPE) return false;
            if (op instanceof LoweredOp.Compare c && c.operandType() != I8_TYPE) return false;
            if (op instanceof LoweredOp.Negate n && n.type() != I8_TYPE) return false;
        }
        return true;
    }

    private static void emitMethod(
            BytecodeAssembler asm, LoweredProgram program, Pool pool,
            int tempCount, int methodName, int methodSig, int stackMapAttr,
            int rowsCountSlot, int filteredRowsSlot, int firstFree, boolean isCountOnly
    ) {
        // Work area layout (all Object locals are 1 slot, longs are 2):
        int filteredCountSlot = firstFree;       // long (2 slots)
        int rowSlot = firstFree + 2;             // long (2 slots)
        int speciesSlot = firstFree + 4;         // Object
        int nativeOrderSlot = firstFree + 5;     // Object
        int activeMaskSlot = firstFree + 6;      // Object

        int nextSlot = firstFree + 7;
        int outputSegSlot = -1;
        int iotaSlot = -1;
        if (!isCountOnly) {
            outputSegSlot = nextSlot++;
            iotaSlot = nextSlot++;
        }

        int maxColIndex = findMaxColumnIndex(program);
        int[] colSegSlots = new int[maxColIndex + 1];
        for (int i = 0; i <= maxColIndex; i++) {
            colSegSlots[i] = nextSlot++;
        }
        int varsSegSlot = nextSlot++;

        // All IR temporaries → Object locals (1 slot each)
        int[] tempSlots = new int[tempCount];
        for (int i = 0; i < tempCount; i++) {
            tempSlots[i] = nextSlot++;
        }
        int maxLocals = nextSlot;
        int objectLocalCount = maxLocals - (firstFree + 4); // everything after row

        // maxStack: upper bound for Vector API calls
        asm.startMethod(methodName, methodSig, 12, maxLocals);

        // === SETUP ===

        // filteredCount = 0
        asm.lconst_0();
        asm.lstore(filteredCountSlot);
        // row = 0
        asm.lconst_0();
        asm.lstore(rowSlot);

        // species = FilterHelpers.longSpecies()
        asm.invokeStatic(pool.helpersLongSpecies);
        asm.astore(speciesSlot);

        // nativeOrder = FilterHelpers.nativeByteOrder()
        asm.invokeStatic(pool.helpersNativeByteOrder);
        asm.astore(nativeOrderSlot);

        // Column segments: colSeg[i] = FilterHelpers.columnSegment(dataAddr, i)
        for (int i = 0; i <= maxColIndex; i++) {
            asm.lload(SLOT_DATA_ADDR);
            asm.iconst(i);
            asm.invokeStatic(pool.helpersColumnSegment);
            asm.astore(colSegSlots[i]);
        }

        // varsSeg = FilterHelpers.segment(varsAddress)
        asm.lload(SLOT_VARS_ADDR);
        asm.invokeStatic(pool.helpersSegment);
        asm.astore(varsSegSlot);

        if (!isCountOnly) {
            // outputSeg = FilterHelpers.segment(filteredRowsAddress)
            asm.lload(filteredRowsSlot);
            asm.invokeStatic(pool.helpersSegment);
            asm.astore(outputSegSlot);

            // iota = FilterHelpers.iotaVector(species)
            asm.aload(speciesSlot);
            asm.invokeStatic(pool.helpersIotaVector);
            asm.astore(iotaSlot);
        }

        // Initialize activeMask + all temp slots to null
        asm.aconst_null();
        asm.astore(activeMaskSlot);
        for (int i = 0; i < tempCount; i++) {
            asm.aconst_null();
            asm.astore(tempSlots[i]);
        }

        // === LOOP ===
        int loopStart = asm.position();

        // if (row >= rowsCount) goto EXIT
        asm.lload(rowSlot);
        asm.lload(rowsCountSlot);
        asm.lcmp();
        int exitBranch = asm.ifge();

        // activeMask = species.indexInRange(row, rowsCount)
        asm.aload(speciesSlot);
        asm.checkcast(pool.vecSpeciesClass);
        asm.lload(rowSlot);
        asm.lload(rowsCountSlot);
        asm.invokeInterface(pool.speciesIndexInRange, 4); // 2 longs = 4 slots
        asm.astore(activeMaskSlot);

        // === Emit ops ===
        LoweredBlock block = program.getBlock(program.getEntryBlockId());
        for (int i = 0; i < block.getOpCount(); i++) {
            emitOp(asm, block.getOp(i), tempSlots, rowSlot, activeMaskSlot,
                    speciesSlot, nativeOrderSlot, colSegSlots, varsSegSlot, pool);
        }

        // === Terminator ===
        Terminator.Return ret = (Terminator.Return) block.getTerminator();

        if (ret.src() == Terminator.Return.ACCEPT) {
            asm.aload(activeMaskSlot);
        } else {
            asm.aload(tempSlots[ret.src()]);
            asm.checkcast(pool.vectorMaskClass);
            asm.aload(activeMaskSlot);
            asm.checkcast(pool.vectorMaskClass);
            asm.invokeVirtual(pool.maskAnd);
        }
        // Stack: resultMask (as Object or VectorMask)

        if (isCountOnly) {
            // filteredCount += resultMask.trueCount()
            asm.checkcast(pool.vectorMaskClass);
            asm.invokeVirtual(pool.maskTrueCount);
            asm.i2l();
            asm.lload(filteredCountSlot);
            asm.ladd();
            asm.lstore(filteredCountSlot);
        } else {
            // Save resultMask (reuse activeMaskSlot since we're past the ops)
            asm.checkcast(pool.vectorMaskClass);
            asm.astore(activeMaskSlot);

            // matchCount = resultMask.trueCount()
            asm.aload(activeMaskSlot);
            asm.checkcast(pool.vectorMaskClass);
            asm.invokeVirtual(pool.maskTrueCount);
            // Stack: int matchCount
            int skipBranch = asm.ifeq(); // if 0, skip to NEXT

            // rowIds = iota.add(row)
            asm.aload(iotaSlot);
            asm.checkcast(pool.longVectorClass);
            asm.lload(rowSlot);
            asm.invokeVirtual(pool.longVecAddScalar);

            // compressed = rowIds.compress(resultMask)
            asm.aload(activeMaskSlot);
            asm.checkcast(pool.vectorMaskClass);
            asm.invokeVirtual(pool.longVecCompress);

            // compressed.intoMemorySegment(outputSeg, filteredCount * 8, nativeOrder)
            asm.aload(outputSegSlot);
            asm.checkcast(pool.memSegClass);
            asm.lload(filteredCountSlot);
            asm.ldc2_w(pool.longEight);
            asm.lmul();
            asm.aload(nativeOrderSlot);
            asm.checkcast(pool.byteOrderClass);
            asm.invokeVirtual(pool.longVecIntoMemSeg);

            // filteredCount += resultMask.trueCount()
            asm.aload(activeMaskSlot);
            asm.checkcast(pool.vectorMaskClass);
            asm.invokeVirtual(pool.maskTrueCount);
            asm.i2l();
            asm.lload(filteredCountSlot);
            asm.ladd();
            asm.lstore(filteredCountSlot);

            asm.setJmp(skipBranch, asm.position());
        }

        // === NEXT ===
        int nextStart = asm.position();

        // row += species.length()
        asm.lload(rowSlot);
        asm.aload(speciesSlot);
        asm.checkcast(pool.vecSpeciesClass);
        asm.invokeInterface(pool.speciesLength, 0);
        asm.i2l();
        asm.ladd();
        asm.lstore(rowSlot);

        int backJmp = asm.goto_();
        asm.setJmp(backJmp, loopStart);

        // === EXIT ===
        int exitStart = asm.position();
        asm.setJmp(exitBranch, exitStart);
        asm.lload(filteredCountSlot);
        asm.lreturn();

        // === StackMapTable ===
        asm.endMethodCode();
        asm.putShort(0); // exception table

        // StackMapTable offsets are relative to code start, not buffer start
        int codeStart = asm.getCodeStart();
        int loopBci = loopStart - codeStart;
        int nextBci = nextStart - codeStart;
        int exitBci = exitStart - codeStart;

        // 3 frames: LOOP, NEXT, EXIT
        asm.putShort(1); // 1 attribute: StackMapTable
        asm.startStackMapTables(stackMapAttr, 3);

        // Frame 1: full_frame at LOOP
        asm.putByte(0xff); // full_frame tag
        asm.putShort(loopBci);
        int longParamCount = isCountOnly ? 6 : 7;
        int totalLocals = 1 + longParamCount + 2 + objectLocalCount;
        asm.putShort(totalLocals);
        asm.putITEM_Object(pool.objectClassIndex);
        for (int i = 0; i < longParamCount; i++) {
            asm.putITEM_Long();
        }
        asm.putITEM_Long(); // filteredCount
        asm.putITEM_Long(); // row
        for (int i = 0; i < objectLocalCount; i++) {
            asm.putITEM_Object(pool.objectClassIndex);
        }
        asm.putShort(0); // empty stack

        // Frame 2: same_frame at NEXT (offset relative to previous frame + 1)
        emitSameFrame(asm, nextBci, loopBci);
        // Frame 3: same_frame at EXIT
        emitSameFrame(asm, exitBci, nextBci);

        asm.endStackMapTables();
        asm.endMethod();
    }

    private static void emitSameFrame(BytecodeAssembler asm, int pos, int prevPos) {
        int offset = pos - prevPos - 1;
        if (offset <= 63) {
            asm.putByte(offset);
        } else {
            asm.putByte(251); // same_frame_extended
            asm.putShort(offset);
        }
    }

    // === Op emission ===

    private static void emitOp(
            BytecodeAssembler asm, LoweredOp op, int[] tempSlots,
            int rowSlot, int activeMaskSlot, int speciesSlot, int nativeOrderSlot,
            int[] colSegSlots, int varsSegSlot, Pool pool
    ) {
        switch (op) {
            case LoweredOp.LoadColumn lc -> {
                // LongVector.fromMemorySegment(species, colSeg, row * 8, nativeOrder, activeMask)
                asm.aload(speciesSlot);
                asm.checkcast(pool.vecSpeciesClass);
                asm.aload(colSegSlots[lc.columnIndex()]);
                asm.checkcast(pool.memSegClass);
                asm.lload(rowSlot);
                asm.ldc2_w(pool.longEight);
                asm.lmul();
                asm.aload(nativeOrderSlot);
                asm.checkcast(pool.byteOrderClass);
                asm.aload(activeMaskSlot);
                asm.checkcast(pool.vectorMaskClass);
                asm.invokeStatic(pool.longVecFromMemSegMasked);
                asm.astore(tempSlots[lc.dst()]);
            }
            case LoweredOp.LoadImm li -> {
                // LongVector.broadcast(species, value)
                asm.aload(speciesSlot);
                asm.checkcast(pool.vecSpeciesClass);
                asm.ldc2_w(pool.getLong(li.lo()));
                asm.invokeStatic(pool.longVecBroadcast);
                asm.astore(tempSlots[li.dst()]);
            }
            case LoweredOp.LoadVar lv -> {
                // Load scalar from vars, broadcast
                asm.aload(speciesSlot);
                asm.checkcast(pool.vecSpeciesClass);
                asm.aload(varsSegSlot);
                asm.checkcast(pool.memSegClass);
                asm.getstatic(pool.javaLongUnaligned);
                asm.ldc2_w(pool.getLong(lv.byteOffset()));
                asm.invokeInterface(pool.memSegGetLong, 3); // ValueLayout(1) + long(2) = 3
                asm.invokeStatic(pool.longVecBroadcast);
                asm.astore(tempSlots[lv.dst()]);
            }
            case LoweredOp.Compare c -> {
                // lhs.compare(VectorOperators.XX, rhs)
                asm.aload(tempSlots[c.lhs()]);
                asm.checkcast(pool.longVectorClass);
                asm.getstatic(pool.comparisonOp(c.opcode()));
                asm.aload(tempSlots[c.rhs()]);
                asm.checkcast(pool.longVectorClass);
                asm.invokeVirtual(pool.longVecCompare);
                asm.astore(tempSlots[c.dst()]);
            }
            case LoweredOp.BooleanOp bo -> {
                asm.aload(tempSlots[bo.lhs()]);
                asm.checkcast(pool.vectorMaskClass);
                asm.aload(tempSlots[bo.rhs()]);
                asm.checkcast(pool.vectorMaskClass);
                asm.invokeVirtual(bo.opcode() == AND ? pool.maskAnd : pool.maskOr);
                asm.astore(tempSlots[bo.dst()]);
            }
            case LoweredOp.Not n -> {
                asm.aload(tempSlots[n.src()]);
                asm.checkcast(pool.vectorMaskClass);
                asm.invokeVirtual(pool.maskNot);
                asm.astore(tempSlots[n.dst()]);
            }
            case LoweredOp.Arithmetic a -> {
                asm.aload(tempSlots[a.lhs()]);
                asm.checkcast(pool.longVectorClass);
                asm.aload(tempSlots[a.rhs()]);
                asm.checkcast(pool.longVectorClass);
                int method = switch (a.opcode()) {
                    case ADD -> pool.longVecAdd;
                    case SUB -> pool.longVecSub;
                    case MUL -> pool.longVecMul;
                    case DIV -> pool.longVecDiv;
                    default -> throw new UnsupportedOperationException("arith op: " + a.opcode());
                };
                asm.invokeVirtual(method);
                asm.astore(tempSlots[a.dst()]);
            }
            case LoweredOp.Negate neg -> {
                asm.aload(tempSlots[neg.src()]);
                asm.checkcast(pool.longVectorClass);
                asm.invokeVirtual(pool.longVecNeg);
                asm.astore(tempSlots[neg.dst()]);
            }
            case LoweredOp.Move m -> {
                asm.aload(tempSlots[m.src()]);
                asm.astore(tempSlots[m.dst()]);
            }
            default -> throw new UnsupportedOperationException("Vector op: " + op);
        }
    }

    private static int findMaxColumnIndex(LoweredProgram program) {
        int max = 0;
        for (int b = 0; b < program.getBlockCount(); b++) {
            LoweredBlock block = program.getBlock(b);
            for (int i = 0; i < block.getOpCount(); i++) {
                if (block.getOp(i) instanceof LoweredOp.LoadColumn lc) {
                    max = Math.max(max, lc.columnIndex());
                }
            }
        }
        return max;
    }

    // === Constant pool ===

    static final class Pool {
        final int thisClassIndex;
        final int objectClassIndex;
        final int longVectorClass;
        final int vectorMaskClass;
        final int memSegClass;
        final int byteOrderClass;
        final int vecSpeciesClass;

        // FilterHelpers setup methods
        final int helpersLongSpecies;
        final int helpersNativeByteOrder;
        final int helpersColumnSegment;
        final int helpersSegment;
        final int helpersIotaVector;

        // VectorSpecies interface methods
        final int speciesIndexInRange;
        final int speciesLength;

        // MemorySegment interface methods
        final int memSegGetLong;

        // Static fields
        final int javaLongUnaligned;
        private final int opEQ, opNE, opLT, opLE, opGT, opGE;

        // LongVector methods
        final int longVecFromMemSegMasked;
        final int longVecBroadcast;
        final int longVecCompare;
        final int longVecAdd;
        final int longVecSub;
        final int longVecMul;
        final int longVecDiv;
        final int longVecNeg;
        final int longVecAddScalar;
        final int longVecCompress;
        final int longVecIntoMemSeg;

        // VectorMask methods
        final int maskAnd;
        final int maskOr;
        final int maskNot;
        final int maskTrueCount;

        // Pre-pooled constants
        final int longEight;
        private final HashMap<Long, Integer> pooledLongs = new HashMap<>();

        Pool(BytecodeAssembler asm, LoweredProgram program) {
            // --- Classes ---
            int longVecCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/LongVector"));
            longVectorClass = longVecCls;
            int vecMaskCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/VectorMask"));
            vectorMaskClass = vecMaskCls;
            int vecSpeciesCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/VectorSpecies"));
            int vecOpsCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/VectorOperators"));
            int memSegCls = asm.poolClass(asm.poolUtf8("java/lang/foreign/MemorySegment"));
            memSegClass = memSegCls;
            int byteOrderCls = asm.poolClass(asm.poolUtf8("java/nio/ByteOrder"));
            byteOrderClass = byteOrderCls;
            vecSpeciesClass = vecSpeciesCls;
            int valueLayoutCls = asm.poolClass(asm.poolUtf8("java/lang/foreign/ValueLayout"));
            int helpersCls = asm.poolClass(asm.poolUtf8("io/questdb/jit/FilterHelpers"));
            objectClassIndex = asm.poolClass(asm.poolUtf8("java/lang/Object"));
            thisClassIndex = asm.poolClass(asm.poolUtf8("io/questdb/jit/vgen"));

            // --- Reusable signature fragments ---
            String sMask = "Ljdk/incubator/vector/VectorMask;";
            String sLVec = "Ljdk/incubator/vector/LongVector;";
            String sVec = "Ljdk/incubator/vector/Vector;";
            String sSpec = "Ljdk/incubator/vector/VectorSpecies;";
            String sMSeg = "Ljava/lang/foreign/MemorySegment;";
            String sBO = "Ljava/nio/ByteOrder;";
            String sComp = "Ljdk/incubator/vector/VectorOperators$Comparison;";

            // --- FilterHelpers setup methods (all invokestatic on a class) ---
            helpersLongSpecies = asm.poolMethod(helpersCls, "longSpecies", "()" + sSpec);
            helpersNativeByteOrder = asm.poolMethod(helpersCls, "nativeByteOrder", "()Ljava/nio/ByteOrder;");
            helpersColumnSegment = asm.poolMethod(helpersCls, "columnSegment", "(JI)" + sMSeg);
            helpersSegment = asm.poolMethod(helpersCls, "segment", "(J)" + sMSeg);
            helpersIotaVector = asm.poolMethod(helpersCls, "iotaVector", "(" + sSpec + ")" + sLVec);

            // --- VectorSpecies (interface) methods ---
            speciesIndexInRange = asm.poolInterfaceMethod(vecSpeciesCls, "indexInRange", "(JJ)" + sMask);
            speciesLength = asm.poolInterfaceMethod(vecSpeciesCls, "length", "()I");

            // --- MemorySegment (interface) methods ---
            memSegGetLong = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfLong;J)J");

            // --- ValueLayout static field ---
            javaLongUnaligned = asm.poolField(valueLayoutCls,
                    asm.poolNameAndType(asm.poolUtf8("JAVA_LONG_UNALIGNED"),
                            asm.poolUtf8("Ljava/lang/foreign/ValueLayout$OfLong;")));

            // --- VectorOperators comparison fields ---
            String compType = "Ljdk/incubator/vector/VectorOperators$Comparison;";
            opEQ = poolCompField(asm, vecOpsCls, "EQ", compType);
            opNE = poolCompField(asm, vecOpsCls, "NE", compType);
            opLT = poolCompField(asm, vecOpsCls, "LT", compType);
            opLE = poolCompField(asm, vecOpsCls, "LE", compType);
            opGT = poolCompField(asm, vecOpsCls, "GT", compType);
            opGE = poolCompField(asm, vecOpsCls, "GE", compType);

            // --- LongVector methods (class, so poolMethod + invokevirtual/invokestatic) ---
            // static: fromMemorySegment(species, seg, offset, order, mask) -> LongVector
            longVecFromMemSegMasked = asm.poolMethod(longVecCls, "fromMemorySegment",
                    "(" + sSpec + sMSeg + "J" + sBO + sMask + ")" + sLVec);
            // static: broadcast(species, long) -> LongVector
            longVecBroadcast = asm.poolMethod(longVecCls, "broadcast",
                    "(" + sSpec + "J)" + sLVec);
            // instance: compare(Comparison, Vector) -> VectorMask (generic erased to Vector)
            longVecCompare = asm.poolMethod(longVecCls, "compare",
                    "(" + sComp + sVec + ")" + sMask);
            // instance: add/sub/mul/div(Vector) -> LongVector
            longVecAdd = asm.poolMethod(longVecCls, "add", "(" + sVec + ")" + sLVec);
            longVecSub = asm.poolMethod(longVecCls, "sub", "(" + sVec + ")" + sLVec);
            longVecMul = asm.poolMethod(longVecCls, "mul", "(" + sVec + ")" + sLVec);
            longVecDiv = asm.poolMethod(longVecCls, "div", "(" + sVec + ")" + sLVec);
            // instance: neg() -> LongVector
            longVecNeg = asm.poolMethod(longVecCls, "neg", "()" + sLVec);
            // instance: add(long) -> LongVector
            longVecAddScalar = asm.poolMethod(longVecCls, "add", "(J)" + sLVec);
            // instance: compress(VectorMask) -> LongVector
            longVecCompress = asm.poolMethod(longVecCls, "compress", "(" + sMask + ")" + sLVec);
            // instance: intoMemorySegment(MemorySegment, long, ByteOrder) -> void
            longVecIntoMemSeg = asm.poolMethod(longVecCls, "intoMemorySegment",
                    "(" + sMSeg + "J" + sBO + ")V");

            // --- VectorMask methods (abstract class, so poolMethod) ---
            maskAnd = asm.poolMethod(vecMaskCls, "and", "(" + sMask + ")" + sMask);
            maskOr = asm.poolMethod(vecMaskCls, "or", "(" + sMask + ")" + sMask);
            maskNot = asm.poolMethod(vecMaskCls, "not", "()" + sMask);
            maskTrueCount = asm.poolMethod(vecMaskCls, "trueCount", "()I");

            // --- Pre-pool long constants ---
            longEight = ensureLong(asm, 8L);
            prePoolConstants(asm, program);
        }

        int comparisonOp(int opcode) {
            return switch (opcode) {
                case EQ -> opEQ;
                case NE -> opNE;
                case LT -> opLT;
                case LE -> opLE;
                case GT -> opGT;
                case GE -> opGE;
                default -> throw new UnsupportedOperationException("cmp op: " + opcode);
            };
        }

        int getLong(long value) {
            Integer idx = pooledLongs.get(value);
            if (idx == null) {
                throw new IllegalStateException("Long constant " + value + " was not pre-pooled");
            }
            return idx;
        }

        private int ensureLong(BytecodeAssembler asm, long value) {
            return pooledLongs.computeIfAbsent(value, v -> asm.poolLongConst(v));
        }

        private void prePoolConstants(BytecodeAssembler asm, LoweredProgram program) {
            for (int b = 0; b < program.getBlockCount(); b++) {
                LoweredBlock block = program.getBlock(b);
                for (int i = 0; i < block.getOpCount(); i++) {
                    LoweredOp op = block.getOp(i);
                    if (op instanceof LoweredOp.LoadImm li) {
                        ensureLong(asm, li.lo());
                    }
                    if (op instanceof LoweredOp.LoadVar lv) {
                        ensureLong(asm, (long) lv.byteOffset());
                    }
                }
            }
        }

        private static int poolCompField(BytecodeAssembler asm, int classCp, String name, String type) {
            return asm.poolField(classCp, asm.poolNameAndType(asm.poolUtf8(name), asm.poolUtf8(type)));
        }
    }
}
