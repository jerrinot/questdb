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
 * Supports straight-line programs (no control flow) with 8-byte element
 * types: I8, F8, and mixed I8+F8. 4-byte types (I4, F4) are not yet
 * supported due to lane count mismatch with the LongVector loop stride.
 * Setup delegates to {@link FilterHelpers}; the hot loop emits direct
 * Vector API calls for C2 intrinsification.
 */
public final class VectorBytecodeFilterCompiler {

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

        asm.defineClass(thisClass);
        asm.interfaceCount(1);
        asm.putShort(ifaceClass);
        asm.fieldCount(0);
        asm.methodCount(3);
        asm.defineDefaultConstructor();

        int tempCount = program.getNextTempId();

        int primary = primaryType(program);
        boolean pureF8 = isPureF8(program);
        emitMethod(asm, program, pool, tempCount, filterRowsName, filterRowsSig,
                stackMapAttr, FR_SLOT_ROWS_COUNT, FR_SLOT_FILTERED_ROWS, FR_FIRST_FREE, false, primary, pureF8);
        emitMethod(asm, program, pool, tempCount, countRowsName, countRowsSig,
                stackMapAttr, CR_SLOT_ROWS_COUNT, -1, CR_FIRST_FREE, true, primary, pureF8);

        asm.putShort(0);
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
        // Each vector op emits ~30-40 bytes of bytecode. JVM methods are
        // limited to 65535 bytes. Reject programs that would exceed this.
        if (block.getOpCount() > 1500) {
            return false;
        }
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadVarSizeHeader) return false;
            if (op instanceof LoweredOp.CompareI128) return false;
            // Reject types we don't vectorize yet (I1, I2, I16)
            if (op instanceof LoweredOp.LoadColumn lc && !isSupportedVectorType(lc.type())) return false;
            if (op instanceof LoweredOp.LoadVar lv && !isSupportedVectorType(lv.type())) return false;
            if (op instanceof LoweredOp.LoadImm li && !isSupportedVectorType(li.type())) return false;
            if (op instanceof LoweredOp.Cast c && !isSupportedCast(c)) return false;
            if (op instanceof LoweredOp.Arithmetic a && !isSupportedVectorType(a.resultType())) return false;
            if (op instanceof LoweredOp.Compare c && !isSupportedVectorType(c.operandType())) return false;
            if (op instanceof LoweredOp.Negate n && !isSupportedVectorType(n.type())) return false;
        }
        return true;
    }

    private static boolean isSupportedVectorType(int type) {
        // 8-byte types: same lane count as LongVector, so loop stride works
        return type == I8_TYPE || type == F8_TYPE;
    }

    private static boolean isSupportedCast(LoweredOp.Cast c) {
        // Same-width casts: I8<->F8
        return (c.fromType() == I8_TYPE && c.toType() == F8_TYPE)
                || (c.fromType() == F8_TYPE && c.toType() == I8_TYPE);
    }

    /**
     * Determines the primary vector element type of the program.
     * For single-element-size programs, this is the type of the data columns.
     * Mixed I8+F8 programs use I8 as primary (LongVector species for the loop).
     */
    private static int primaryType(LoweredProgram program) {
        LoweredBlock block = program.getBlock(program.getEntryBlockId());
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadColumn lc) {
                return lc.type();
            }
        }
        return I8_TYPE; // default
    }

    private static boolean isPureF8(LoweredProgram program) {
        LoweredBlock block = program.getBlock(program.getEntryBlockId());
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadColumn lc && lc.type() == I8_TYPE) return false;
            if (op instanceof LoweredOp.LoadVar lv && lv.type() == I8_TYPE) return false;
            if (op instanceof LoweredOp.LoadImm li && li.type() == I8_TYPE) return false;
            if (op instanceof LoweredOp.Arithmetic a && a.resultType() == I8_TYPE) return false;
            if (op instanceof LoweredOp.Cast) return false; // casts involve mixed types
        }
        return primaryType(program) == F8_TYPE;
    }

    private static int elementBytes(int type) {
        return switch (type) {
            case I1_TYPE -> 1;
            case I2_TYPE -> 2;
            case I4_TYPE, F4_TYPE -> 4;
            case I8_TYPE, F8_TYPE -> 8;
            default -> throw new UnsupportedOperationException("element size for type: " + type);
        };
    }

    // === Method emission ===

    private static void emitMethod(
            BytecodeAssembler asm, LoweredProgram program, Pool pool,
            int tempCount, int methodName, int methodSig, int stackMapAttr,
            int rowsCountSlot, int filteredRowsSlot, int firstFree, boolean isCountOnly,
            int primaryType, boolean pureF8
    ) {
        int filteredCountSlot = firstFree;
        int rowSlot = firstFree + 2;
        int speciesSlot = firstFree + 4;
        int nativeOrderSlot = firstFree + 5;
        int activeMaskSlot = firstFree + 6;

        boolean nullChecks = program.getOptions().isNullChecksEnabled();

        int nextSlot = firstFree + 7;
        int nullVecSlot = -1;
        boolean needsNullVec = nullChecks && !pureF8;
        if (needsNullVec) {
            nullVecSlot = nextSlot++;
        }
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

        int[] tempSlots = new int[tempCount];
        for (int i = 0; i < tempCount; i++) {
            tempSlots[i] = nextSlot++;
        }
        int maxLocals = nextSlot;
        int objectLocalCount = maxLocals - (firstFree + 4);

        asm.startMethod(methodName, methodSig, 12, maxLocals);

        // === SETUP ===
        asm.lconst_0();
        asm.lstore(filteredCountSlot);
        asm.lconst_0();
        asm.lstore(rowSlot);

        // Pure F8 programs use DoubleVector.SPECIES_PREFERRED so masks stay
        // VectorMask<Double> throughout the hot path without cast traffic.
        // Mixed I8+F8 programs use LongVector.SPECIES_PREFERRED for uniform
        // mask type across BooleanOp AND/OR; loads and compares cast as needed.
        if (pureF8) {
            asm.invokeStatic(pool.helpersDoubleSpecies);
        } else {
            asm.invokeStatic(pool.helpersLongSpecies);
        }
        asm.astore(speciesSlot);
        asm.invokeStatic(pool.helpersNativeByteOrder);
        asm.astore(nativeOrderSlot);

        for (int i = 0; i <= maxColIndex; i++) {
            asm.lload(SLOT_DATA_ADDR);
            asm.iconst(i);
            asm.invokeStatic(pool.helpersColumnSegment);
            asm.astore(colSegSlots[i]);
        }

        asm.lload(SLOT_VARS_ADDR);
        asm.invokeStatic(pool.helpersSegment);
        asm.astore(varsSegSlot);

        if (!isCountOnly) {
            asm.lload(filteredRowsSlot);
            asm.invokeStatic(pool.helpersSegment);
            asm.astore(outputSegSlot);
            // iota always uses Long species (row IDs are longs)
            asm.invokeStatic(pool.helpersLongSpecies);
            asm.invokeStatic(pool.helpersIotaVector);
            asm.astore(iotaSlot);
        }

        if (nullChecks && !pureF8) {
            // Load LONG_NULL sentinel vector for I8 null-aware comparisons
            // and arithmetic. F8 comparisons detect NaN internally via IS_NAN,
            // so pure F8 programs don't need this.
            asm.invokeStatic(pool.helpersLongSpecies);
            asm.invokeStatic(pool.helpersLongNullVector);
            asm.astore(nullVecSlot);
        }

        asm.aconst_null();
        asm.astore(activeMaskSlot);
        for (int i = 0; i < tempCount; i++) {
            asm.aconst_null();
            asm.astore(tempSlots[i]);
        }

        // Hoist loop-invariant ops (immediates and bind variables) out of the
        // hot loop. These values don't change between iterations.
        LoweredBlock block = program.getBlock(program.getEntryBlockId());
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadImm li) {
                emitLoadImm(asm, li, tempSlots, speciesSlot, pool);
            } else if (op instanceof LoweredOp.LoadVar lv) {
                emitLoadVar(asm, lv, tempSlots, speciesSlot, varsSegSlot, pool);
            }
        }

        // === LOOP ===
        int loopStart = asm.position();
        asm.lload(rowSlot);
        asm.lload(rowsCountSlot);
        asm.lcmp();
        int exitBranch = asm.ifge();

        asm.aload(speciesSlot);
        asm.lload(rowSlot);
        asm.lload(rowsCountSlot);
        asm.invokeInterface(pool.speciesIndexInRange, 4);
        asm.astore(activeMaskSlot);

        // === Emit ops (skip hoisted LoadImm/LoadVar) ===
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadImm || op instanceof LoweredOp.LoadVar) {
                continue;
            }
            emitOp(asm, op, tempSlots, rowSlot, activeMaskSlot,
                    speciesSlot, nativeOrderSlot, colSegSlots, varsSegSlot, pool,
                    nullChecks, nullVecSlot, pureF8);
        }

        // === Terminator ===
        Terminator.Return ret = (Terminator.Return) block.getTerminator();
        if (ret.src() == Terminator.Return.ACCEPT) {
            asm.aload(activeMaskSlot);
        } else {
            asm.aload(tempSlots[ret.src()]);
            asm.checkcast(pool.vectorMaskClass);
            asm.aload(activeMaskSlot);
            asm.invokeVirtual(pool.maskAnd);
        }
        // Stack: resultMask (VectorMask)

        if (isCountOnly) {
            asm.invokeVirtual(pool.maskTrueCount);
            asm.i2l();
            asm.lload(filteredCountSlot);
            asm.ladd();
            asm.lstore(filteredCountSlot);
        } else {
            asm.astore(activeMaskSlot);
            asm.aload(activeMaskSlot);
            asm.invokeVirtual(pool.maskTrueCount);
            int skipBranch = asm.ifeq();

            // Row-ID output always uses LongVector (row IDs are longs).
            asm.aload(iotaSlot);
            asm.lload(rowSlot);
            asm.invokeVirtual(pool.longVecAddScalar);
            asm.aload(activeMaskSlot);
            if (primaryType == F8_TYPE) {
                asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
                asm.invokeVirtual(pool.maskCast);
            }
            asm.invokeVirtual(pool.longVecCompress);
            // Stack: compressed(LongVector)

            // Masked store: writeCompressedRows(compressed, matchCount, output, offset, order)
            asm.aload(activeMaskSlot);
            asm.invokeVirtual(pool.maskTrueCount);
            // Stack: compressed, matchCount(int)
            asm.aload(outputSegSlot);
            asm.lload(filteredCountSlot);
            asm.ldc2_w(pool.longEight);
            asm.lmul();
            asm.aload(nativeOrderSlot);
            asm.invokeStatic(pool.writeCompressedRows);

            // filteredCount += matchCount
            asm.aload(activeMaskSlot);
            asm.invokeVirtual(pool.maskTrueCount);
            asm.i2l();
            asm.lload(filteredCountSlot);
            asm.ladd();
            asm.lstore(filteredCountSlot);

            asm.setJmp(skipBranch, asm.position());
        }

        // === NEXT ===
        int nextStart = asm.position();
        asm.lload(rowSlot);
        asm.aload(speciesSlot);
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
        int codeStart = asm.getCodeStart();
        int loopBci = loopStart - codeStart;
        int nextBci = nextStart - codeStart;
        int exitBci = exitStart - codeStart;

        asm.endMethodCode();
        asm.putShort(0);

        asm.putShort(1);
        asm.startStackMapTables(stackMapAttr, 3);

        // Build precise local type declarations for the full_frame.
        // Typed locals eliminate checkcast overhead in the hot loop.
        int longParamCount = isCountOnly ? 6 : 7;
        int totalLocals = 1 + longParamCount + 2 + objectLocalCount;

        asm.putByte(0xff); // full_frame
        asm.putShort(loopBci);
        asm.putShort(totalLocals);
        // this
        asm.putITEM_Object(pool.objectClassIndex);
        // long params
        for (int i = 0; i < longParamCount; i++) {
            asm.putITEM_Long();
        }
        // filteredCount, row
        asm.putITEM_Long();
        asm.putITEM_Long();
        // Object locals with precise types (order must match slot allocation)
        asm.putITEM_Object(pool.vecSpeciesClass); // speciesSlot
        asm.putITEM_Object(pool.byteOrderClass);  // nativeOrderSlot
        asm.putITEM_Object(pool.vectorMaskClass);  // activeMaskSlot
        if (needsNullVec) {
            asm.putITEM_Object(pool.longVectorClass); // nullVecSlot — always LongVector
        }
        if (!isCountOnly) {
            asm.putITEM_Object(pool.memSegClass);      // outputSegSlot
            asm.putITEM_Object(pool.longVectorClass);  // iotaSlot
        }
        for (int i = 0; i <= maxColIndex; i++) {
            asm.putITEM_Object(pool.memSegClass); // colSegSlots[i]
        }
        asm.putITEM_Object(pool.memSegClass); // varsSegSlot
        for (int i = 0; i < tempCount; i++) {
            asm.putITEM_Object(pool.objectClassIndex); // temp[i] — generic
        }
        // empty stack
        asm.putShort(0);

        emitSameFrame(asm, nextBci, loopBci);
        emitSameFrame(asm, exitBci, nextBci);

        asm.endStackMapTables();
        asm.endMethod();
    }

    private static void emitSameFrame(BytecodeAssembler asm, int pos, int prevPos) {
        int offset = pos - prevPos - 1;
        if (offset <= 63) {
            asm.putByte(offset);
        } else {
            asm.putByte(251);
            asm.putShort(offset);
        }
    }

    // === Op emission ===

    private static void emitOp(
            BytecodeAssembler asm, LoweredOp op, int[] tempSlots,
            int rowSlot, int activeMaskSlot, int speciesSlot, int nativeOrderSlot,
            int[] colSegSlots, int varsSegSlot, Pool pool,
            boolean nullChecks, int nullVecSlot, boolean pureF8
    ) {
        switch (op) {
            case LoweredOp.LoadColumn lc -> emitLoadColumn(asm, lc, tempSlots, rowSlot,
                    activeMaskSlot, speciesSlot, nativeOrderSlot, colSegSlots, pool, pureF8);
            case LoweredOp.LoadImm li -> emitLoadImm(asm, li, tempSlots, speciesSlot, pool);
            case LoweredOp.LoadVar lv -> emitLoadVar(asm, lv, tempSlots, speciesSlot, varsSegSlot, pool);
            case LoweredOp.Compare c -> emitCompare(asm, c, tempSlots, pool, nullChecks, nullVecSlot, pureF8);
            case LoweredOp.BooleanOp bo -> emitBooleanOp(asm, bo, tempSlots, pool);
            case LoweredOp.Not n -> emitNot(asm, n, tempSlots, pool);
            case LoweredOp.Arithmetic a -> emitArithmetic(asm, a, tempSlots, pool, nullChecks, nullVecSlot);
            case LoweredOp.Negate neg -> emitNegate(asm, neg, tempSlots, pool);
            case LoweredOp.Move m -> {
                asm.aload(tempSlots[m.src()]);
                asm.astore(tempSlots[m.dst()]);
            }
            case LoweredOp.Cast c -> emitCast(asm, c, tempSlots, speciesSlot, pool, nullChecks, nullVecSlot);
            default -> throw new UnsupportedOperationException("Vector op: " + op);
        }
    }

    private static void emitLoadColumn(
            BytecodeAssembler asm, LoweredOp.LoadColumn lc, int[] tempSlots,
            int rowSlot, int activeMaskSlot, int speciesSlot, int nativeOrderSlot,
            int[] colSegSlots, Pool pool, boolean pureF8
    ) {
        Pool.VecType vt = pool.vecType(lc.type());
        asm.getstatic(vt.speciesPreferred); // type-correct species
        asm.aload(colSegSlots[lc.columnIndex()]);
        asm.lload(rowSlot);
        asm.ldc2_w(pool.ensureLongPooled(elementBytes(lc.type())));
        asm.lmul();
        asm.aload(nativeOrderSlot);
        asm.aload(activeMaskSlot);
        if (!pureF8) {
            // Cast mask to match column type's species (same lane count, different element type).
            // Pure F8 programs already have VectorMask<Double> from the loop species.
            asm.getstatic(vt.speciesPreferred);
            asm.invokeVirtual(pool.maskCast);
        }
        asm.invokeStatic(vt.fromMemSegMasked);
        asm.astore(tempSlots[lc.dst()]);
    }

    private static void emitLoadImm(BytecodeAssembler asm, LoweredOp.LoadImm li, int[] tempSlots,
                                     int speciesSlot, Pool pool) {
        Pool.VecType vt = pool.vecType(li.type());
        asm.getstatic(vt.speciesPreferred); // type-correct species
        switch (li.type()) {
            case I4_TYPE -> asm.iconst((int) li.lo());
            case I8_TYPE -> asm.ldc2_w(pool.ensureLongPooled(li.lo()));
            case F4_TYPE -> asm.ldc(pool.ensureFloatPooled(Float.intBitsToFloat((int) li.lo())));
            case F8_TYPE -> asm.ldc2_w(pool.ensureDoublePooled(Double.longBitsToDouble(li.lo())));
            default -> throw new UnsupportedOperationException("LoadImm type: " + li.type());
        }
        asm.invokeStatic(vt.broadcast);
        asm.astore(tempSlots[li.dst()]);
    }

    private static void emitLoadVar(BytecodeAssembler asm, LoweredOp.LoadVar lv, int[] tempSlots,
                                     int speciesSlot, int varsSegSlot, Pool pool) {
        Pool.VecType vt = pool.vecType(lv.type());
        asm.getstatic(vt.speciesPreferred); // type-correct species
        // Load scalar from vars segment and broadcast
        asm.aload(varsSegSlot);
        asm.getstatic(pool.varLayout(lv.type()));
        asm.ldc2_w(pool.ensureLongPooled(lv.byteOffset()));
        switch (lv.type()) {
            case I4_TYPE -> {
                asm.invokeInterface(pool.memSegGetInt, 3);
                asm.invokeStatic(vt.broadcast);
            }
            case I8_TYPE -> {
                asm.invokeInterface(pool.memSegGetLong, 3);
                asm.invokeStatic(vt.broadcast);
            }
            case F4_TYPE -> {
                asm.invokeInterface(pool.memSegGetFloat, 3);
                asm.invokeStatic(vt.broadcast);
            }
            case F8_TYPE -> {
                asm.invokeInterface(pool.memSegGetDouble, 3);
                asm.invokeStatic(vt.broadcast);
            }
            default -> throw new UnsupportedOperationException("LoadVar type: " + lv.type());
        }
        asm.astore(tempSlots[lv.dst()]);
    }

    private static void emitCompare(BytecodeAssembler asm, LoweredOp.Compare c, int[] tempSlots,
                                     Pool pool, boolean nullChecks, int nullVecSlot, boolean pureF8) {
        Pool.VecType vt = pool.vecType(c.operandType());

        if (c.operandType() == F8_TYPE) {
            // Double comparisons always use helpers (epsilon + NaN handling).
            int helperMethod = pool.doubleCompareHelper(c.opcode());
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.invokeStatic(helperMethod);
            if (!pureF8) {
                // Mixed I8+F8: cast VectorMask<Double> → VectorMask<Long> for uniform mask type
                asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
                asm.invokeVirtual(pool.maskCast);
            }
            asm.astore(tempSlots[c.dst()]);
        } else if (!nullChecks) {
            // I8 without null checks: direct compare
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.getstatic(pool.comparisonOp(c.opcode()));
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.invokeVirtual(vt.compare);
            asm.astore(tempSlots[c.dst()]);
        } else if (c.opcode() == EQ || c.opcode() == NE) {
            // I8 EQ/NE with null checks: LONG_NULL == LONG_NULL works naturally
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.getstatic(pool.comparisonOp(c.opcode()));
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.invokeVirtual(vt.compare);
            asm.astore(tempSlots[c.dst()]);
        } else {
            // I8 null-aware ordered comparison (LT, LE, GT, GE)
            int helperMethod = pool.nullCompareHelper(c.operandType(), c.opcode());
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(nullVecSlot);
            asm.invokeStatic(helperMethod);
            asm.astore(tempSlots[c.dst()]);
        }
    }

    private static void emitBooleanOp(BytecodeAssembler asm, LoweredOp.BooleanOp bo, int[] tempSlots, Pool pool) {
        asm.aload(tempSlots[bo.lhs()]);
        asm.checkcast(pool.vectorMaskClass);
        asm.aload(tempSlots[bo.rhs()]);
        asm.checkcast(pool.vectorMaskClass);
        asm.invokeVirtual(bo.opcode() == AND ? pool.maskAnd : pool.maskOr);
        asm.astore(tempSlots[bo.dst()]);
    }

    private static void emitNot(BytecodeAssembler asm, LoweredOp.Not n, int[] tempSlots, Pool pool) {
        asm.aload(tempSlots[n.src()]);
        asm.checkcast(pool.vectorMaskClass);
        asm.invokeVirtual(pool.maskNot);
        asm.astore(tempSlots[n.dst()]);
    }

    private static void emitArithmetic(BytecodeAssembler asm, LoweredOp.Arithmetic a, int[] tempSlots,
                                       Pool pool, boolean nullChecks, int nullVecSlot) {
        Pool.VecType vt = pool.vecType(a.resultType());

        if (a.resultType() == F8_TYPE) {
            // F8: always use helper for NaN propagation and div-by-zero → NaN
            asm.aload(tempSlots[a.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[a.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.iconst(a.opcode());
            asm.invokeStatic(pool.doubleVecArithmetic);
            asm.astore(tempSlots[a.dst()]);
        } else if (nullChecks && a.resultType() == I8_TYPE) {
            // I8 with null checks: use helper for LONG_NULL preservation
            asm.aload(tempSlots[a.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[a.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(nullVecSlot);
            asm.iconst(a.opcode());
            asm.invokeStatic(pool.longVecArithmeticNull);
            asm.astore(tempSlots[a.dst()]);
        } else {
            // I8 without null checks: raw vector arithmetic
            asm.aload(tempSlots[a.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[a.rhs()]);
            asm.checkcast(vt.vecClass);
            int method = switch (a.opcode()) {
                case ADD -> vt.add;
                case SUB -> vt.sub;
                case MUL -> vt.mul;
                case DIV -> vt.div;
                default -> throw new UnsupportedOperationException("arith op: " + a.opcode());
            };
            asm.invokeVirtual(method);
            asm.astore(tempSlots[a.dst()]);
        }
    }

    private static void emitNegate(BytecodeAssembler asm, LoweredOp.Negate neg, int[] tempSlots, Pool pool) {
        Pool.VecType vt = pool.vecType(neg.type());
        asm.aload(tempSlots[neg.src()]);
        asm.checkcast(vt.vecClass);
        asm.invokeVirtual(vt.neg);
        asm.astore(tempSlots[neg.dst()]);
    }

    private static void emitCast(BytecodeAssembler asm, LoweredOp.Cast c, int[] tempSlots,
                                  int speciesSlot, Pool pool, boolean nullChecks, int nullVecSlot) {
        // Same-width cast: I8<->F8 via L2D/D2L.
        Pool.VecType srcVt = pool.vecType(c.fromType());
        asm.aload(tempSlots[c.src()]);
        asm.checkcast(srcVt.vecClass);

        if (nullChecks && c.fromType() == I8_TYPE && c.toType() == F8_TYPE) {
            // Null-aware I8→F8: LONG_NULL must become NaN, not -9.22E18.
            asm.aload(nullVecSlot);
            asm.checkcast(pool.vecType(I8_TYPE).vecClass);
            asm.invokeStatic(pool.longToDoubleNullAware);
        } else {
            asm.getstatic(pool.conversionOp(c.fromType(), c.toType()));
            asm.getstatic(pool.vecType(c.toType()).speciesPreferred);
            asm.iconst(0);
            asm.invokeVirtual(srcVt.convertShape);
        }
        asm.checkcast(pool.vecType(c.toType()).vecClass);
        asm.astore(tempSlots[c.dst()]);
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
        final int objectClassIndex;
        final int longVectorClass;
        final int vectorMaskClass;
        final int memSegClass;
        final int byteOrderClass;
        final int vecSpeciesClass;

        // FilterHelpers setup methods
        final int helpersLongSpecies;
        final int helpersDoubleSpecies;
        final int helpersNativeByteOrder;
        final int helpersLongNullVector;
        final int helpersDoubleNanVector;
        final int helpersColumnSegment;
        final int helpersSegment;
        final int helpersIotaVector;

        // VectorSpecies interface methods
        final int speciesIndexInRange;
        final int speciesLength;

        // MemorySegment interface methods
        final int memSegGetLong;
        final int memSegGetInt;
        final int memSegGetFloat;
        final int memSegGetDouble;

        // ValueLayout fields
        final int javaLongUnaligned;
        final int javaIntUnaligned;
        final int javaFloatUnaligned;
        final int javaDoubleUnaligned;

        // VectorOperators comparison fields
        private final int opEQ, opNE, opLT, opLE, opGT, opGE;

        // VectorOperators conversion fields
        private int convI2F, convF2I, convL2D, convD2L;

        // Null-aware comparison helpers (I8)
        private int longNullLt, longNullLe, longNullGt, longNullGe;
        // Double comparison helpers (epsilon + NaN)
        private int doubleVecEq, doubleVecNe, doubleVecLt, doubleVecLe, doubleVecGt, doubleVecGe;
        // Arithmetic helpers
        final int longVecArithmeticNull;
        final int doubleVecArithmetic;
        // Null-aware cast helper
        final int longToDoubleNullAware;

        // LongVector-specific (always needed for row-ID output)
        final int longVecAddScalar;
        final int longVecCompress;
        final int writeCompressedRows;

        // VectorMask methods
        final int maskAnd;
        final int maskOr;
        final int maskNot;
        final int maskTrueCount;
        final int maskCast;

        // Pre-pooled constants
        final int longEight;
        private final HashMap<Long, Integer> pooledLongs = new HashMap<>();
        private final HashMap<Integer, Integer> pooledInts = new HashMap<>();
        private final HashMap<Float, Integer> pooledFloats = new HashMap<>();
        private final HashMap<Double, Integer> pooledDoubles = new HashMap<>();

        // Per-type vector method pools
        private final VecType[] vecTypes = new VecType[7]; // indexed by IR type constant

        Pool(BytecodeAssembler asm, LoweredProgram program) {
            // --- Classes ---
            int longVecCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/LongVector"));
            longVectorClass = longVecCls;
            int intVecCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/IntVector"));
            int floatVecCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/FloatVector"));
            int doubleVecCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/DoubleVector"));
            int vecMaskCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/VectorMask"));
            vectorMaskClass = vecMaskCls;
            int vecSpeciesCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/VectorSpecies"));
            vecSpeciesClass = vecSpeciesCls;
            int vecOpsCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/VectorOperators"));
            int memSegCls = asm.poolClass(asm.poolUtf8("java/lang/foreign/MemorySegment"));
            memSegClass = memSegCls;
            int byteOrderCls = asm.poolClass(asm.poolUtf8("java/nio/ByteOrder"));
            byteOrderClass = byteOrderCls;
            int valueLayoutCls = asm.poolClass(asm.poolUtf8("java/lang/foreign/ValueLayout"));
            int helpersCls = asm.poolClass(asm.poolUtf8("io/questdb/jit/FilterHelpers"));
            objectClassIndex = asm.poolClass(asm.poolUtf8("java/lang/Object"));

            // Signature fragments
            String sMask = "Ljdk/incubator/vector/VectorMask;";
            String sSpec = "Ljdk/incubator/vector/VectorSpecies;";
            String sMSeg = "Ljava/lang/foreign/MemorySegment;";
            String sBO = "Ljava/nio/ByteOrder;";
            String sComp = "Ljdk/incubator/vector/VectorOperators$Comparison;";
            String sVec = "Ljdk/incubator/vector/Vector;";
            String sConv = "Ljdk/incubator/vector/VectorOperators$Conversion;";

            // --- FilterHelpers ---
            helpersLongSpecies = asm.poolMethod(helpersCls, "longSpecies", "()" + sSpec);
            helpersDoubleSpecies = asm.poolMethod(helpersCls, "doubleSpecies", "()" + sSpec);
            helpersNativeByteOrder = asm.poolMethod(helpersCls, "nativeByteOrder", "()Ljava/nio/ByteOrder;");
            helpersColumnSegment = asm.poolMethod(helpersCls, "columnSegment", "(JI)" + sMSeg);
            helpersSegment = asm.poolMethod(helpersCls, "segment", "(J)" + sMSeg);
            helpersIotaVector = asm.poolMethod(helpersCls, "iotaVector",
                    "(" + sSpec + ")Ljdk/incubator/vector/LongVector;");
            helpersLongNullVector = asm.poolMethod(helpersCls, "longNullVector",
                    "(" + sSpec + ")Ljdk/incubator/vector/LongVector;");
            helpersDoubleNanVector = asm.poolMethod(helpersCls, "doubleNanVector",
                    "(" + sSpec + ")Ljdk/incubator/vector/DoubleVector;");

            // Null-aware comparison helpers
            String longNullSig = "(Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;)" + sMask;
            longNullLt = asm.poolMethod(helpersCls, "longNullLt", longNullSig);
            longNullLe = asm.poolMethod(helpersCls, "longNullLe", longNullSig);
            longNullGt = asm.poolMethod(helpersCls, "longNullGt", longNullSig);
            longNullGe = asm.poolMethod(helpersCls, "longNullGe", longNullSig);

            // Double comparison helpers (epsilon + NaN handling, always used for F8)
            String dblCmpSig = "(Ljdk/incubator/vector/DoubleVector;Ljdk/incubator/vector/DoubleVector;)" + sMask;
            doubleVecEq = asm.poolMethod(helpersCls, "doubleVecEq", dblCmpSig);
            doubleVecNe = asm.poolMethod(helpersCls, "doubleVecNe", dblCmpSig);
            doubleVecLt = asm.poolMethod(helpersCls, "doubleVecLt", dblCmpSig);
            doubleVecLe = asm.poolMethod(helpersCls, "doubleVecLe", dblCmpSig);
            doubleVecGt = asm.poolMethod(helpersCls, "doubleVecGt", dblCmpSig);
            doubleVecGe = asm.poolMethod(helpersCls, "doubleVecGe", dblCmpSig);

            // Arithmetic helpers
            longVecArithmeticNull = asm.poolMethod(helpersCls, "longVecArithmeticNull",
                    "(Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;I)Ljdk/incubator/vector/LongVector;");
            doubleVecArithmetic = asm.poolMethod(helpersCls, "doubleVecArithmetic",
                    "(Ljdk/incubator/vector/DoubleVector;Ljdk/incubator/vector/DoubleVector;I)Ljdk/incubator/vector/DoubleVector;");
            longToDoubleNullAware = asm.poolMethod(helpersCls, "longToDoubleNullAware",
                    "(Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;)Ljdk/incubator/vector/DoubleVector;");

            // --- VectorSpecies (interface) ---
            speciesIndexInRange = asm.poolInterfaceMethod(vecSpeciesCls, "indexInRange", "(JJ)" + sMask);
            speciesLength = asm.poolInterfaceMethod(vecSpeciesCls, "length", "()I");

            // --- MemorySegment (interface) ---
            memSegGetLong = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfLong;J)J");
            memSegGetInt = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfInt;J)I");
            memSegGetFloat = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfFloat;J)F");
            memSegGetDouble = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfDouble;J)D");

            // --- ValueLayout fields ---
            javaLongUnaligned = poolStaticField(asm, valueLayoutCls, "JAVA_LONG_UNALIGNED",
                    "Ljava/lang/foreign/ValueLayout$OfLong;");
            javaIntUnaligned = poolStaticField(asm, valueLayoutCls, "JAVA_INT_UNALIGNED",
                    "Ljava/lang/foreign/ValueLayout$OfInt;");
            javaFloatUnaligned = poolStaticField(asm, valueLayoutCls, "JAVA_FLOAT_UNALIGNED",
                    "Ljava/lang/foreign/ValueLayout$OfFloat;");
            javaDoubleUnaligned = poolStaticField(asm, valueLayoutCls, "JAVA_DOUBLE_UNALIGNED",
                    "Ljava/lang/foreign/ValueLayout$OfDouble;");

            // --- Comparison ops ---
            String compType = "Ljdk/incubator/vector/VectorOperators$Comparison;";
            opEQ = poolStaticField(asm, vecOpsCls, "EQ", compType);
            opNE = poolStaticField(asm, vecOpsCls, "NE", compType);
            opLT = poolStaticField(asm, vecOpsCls, "LT", compType);
            opLE = poolStaticField(asm, vecOpsCls, "LE", compType);
            opGT = poolStaticField(asm, vecOpsCls, "GT", compType);
            opGE = poolStaticField(asm, vecOpsCls, "GE", compType);

            // --- Conversion ops (for Cast) ---
            String convType = "Ljdk/incubator/vector/VectorOperators$Conversion;";
            convI2F = poolStaticField(asm, vecOpsCls, "I2F", convType);
            convF2I = poolStaticField(asm, vecOpsCls, "F2I", convType);
            convL2D = poolStaticField(asm, vecOpsCls, "L2D", convType);
            convD2L = poolStaticField(asm, vecOpsCls, "D2L", convType);

            // --- Per-type VecType pools ---
            String sLVec = "Ljdk/incubator/vector/LongVector;";
            String sIVec = "Ljdk/incubator/vector/IntVector;";
            String sFVec = "Ljdk/incubator/vector/FloatVector;";
            String sDVec = "Ljdk/incubator/vector/DoubleVector;";

            vecTypes[I8_TYPE] = poolVecType(asm, longVecCls, sLVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "J");
            vecTypes[I4_TYPE] = poolVecType(asm, intVecCls, sIVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "I");
            vecTypes[F4_TYPE] = poolVecType(asm, floatVecCls, sFVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "F");
            vecTypes[F8_TYPE] = poolVecType(asm, doubleVecCls, sDVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "D");

            // --- LongVector-specific (for row-ID output) ---
            longVecAddScalar = asm.poolMethod(longVecCls, "add", "(J)" + sLVec);
            longVecCompress = asm.poolMethod(longVecCls, "compress", "(" + sMask + ")" + sLVec);
            writeCompressedRows = asm.poolMethod(asm.poolClass(FilterHelpers.class),
                    "writeCompressedRows",
                    "(" + sLVec + "I" + sMSeg + "J" + sBO + ")V");

            // --- VectorMask methods ---
            maskAnd = asm.poolMethod(vecMaskCls, "and", "(" + sMask + ")" + sMask);
            maskOr = asm.poolMethod(vecMaskCls, "or", "(" + sMask + ")" + sMask);
            maskNot = asm.poolMethod(vecMaskCls, "not", "()" + sMask);
            maskTrueCount = asm.poolMethod(vecMaskCls, "trueCount", "()I");
            maskCast = asm.poolMethod(vecMaskCls, "cast", "(" + sSpec + ")" + sMask);

            // --- Constants ---
            doPoolLong(asm, 8L);
            prePoolConstants(asm, program);
            longEight = ensureLongPooled(8L);
        }

        VecType vecType(int type) {
            VecType vt = vecTypes[type];
            if (vt == null) {
                throw new UnsupportedOperationException("No VecType for type: " + type);
            }
            return vt;
        }

        int comparisonOp(int opcode) {
            return switch (opcode) {
                case EQ -> opEQ;
                case NE -> opNE;
                case LT -> opLT;
                case LE -> opLE;
                case GT -> opGT;
                case GE -> opGE;
                default -> throw new UnsupportedOperationException("cmp: " + opcode);
            };
        }

        int doubleCompareHelper(int opcode) {
            return switch (opcode) {
                case EQ -> doubleVecEq;
                case NE -> doubleVecNe;
                case LT -> doubleVecLt;
                case LE -> doubleVecLe;
                case GT -> doubleVecGt;
                case GE -> doubleVecGe;
                default -> throw new UnsupportedOperationException("double cmp: " + opcode);
            };
        }

        int nullCompareHelper(int operandType, int opcode) {
            if (operandType == I8_TYPE) {
                return switch (opcode) {
                    case LT -> longNullLt;
                    case LE -> longNullLe;
                    case GT -> longNullGt;
                    case GE -> longNullGe;
                    default -> throw new UnsupportedOperationException("null cmp: " + opcode);
                };
            }
            throw new UnsupportedOperationException("null compare for type: " + operandType);
        }

        int conversionOp(int fromType, int toType) {
            if (fromType == I4_TYPE && toType == F4_TYPE) return convI2F;
            if (fromType == F4_TYPE && toType == I4_TYPE) return convF2I;
            if (fromType == I8_TYPE && toType == F8_TYPE) return convL2D;
            if (fromType == F8_TYPE && toType == I8_TYPE) return convD2L;
            throw new UnsupportedOperationException("conversion: " + fromType + " -> " + toType);
        }

        int varLayout(int type) {
            return switch (type) {
                case I4_TYPE -> javaIntUnaligned;
                case I8_TYPE -> javaLongUnaligned;
                case F4_TYPE -> javaFloatUnaligned;
                case F8_TYPE -> javaDoubleUnaligned;
                default -> throw new UnsupportedOperationException("var layout: " + type);
            };
        }

        int ensureLongPooled(long value) {
            Integer idx = pooledLongs.get(value);
            if (idx == null) throw new IllegalStateException("Long " + value + " not pre-pooled");
            return idx;
        }

        int ensureFloatPooled(float value) {
            Integer idx = pooledFloats.get(value);
            if (idx == null) throw new IllegalStateException("Float " + value + " not pre-pooled");
            return idx;
        }

        int ensureDoublePooled(double value) {
            Integer idx = pooledDoubles.get(value);
            if (idx == null) throw new IllegalStateException("Double " + value + " not pre-pooled");
            return idx;
        }

        // Pool-time methods (called during constructor, before finishPool)

        private void doPoolLong(BytecodeAssembler asm, long value) {
            pooledLongs.computeIfAbsent(value, v -> asm.poolLongConst(v));
        }

        private void doPoolFloat(BytecodeAssembler asm, float value) {
            pooledFloats.computeIfAbsent(value, v -> asm.poolFloatConst(v));
        }

        private void doPoolDouble(BytecodeAssembler asm, double value) {
            pooledDoubles.computeIfAbsent(value, v -> asm.poolDoubleConst(v));
        }

        private void prePoolConstants(BytecodeAssembler asm, LoweredProgram program) {
            for (int b = 0; b < program.getBlockCount(); b++) {
                LoweredBlock block = program.getBlock(b);
                for (int i = 0; i < block.getOpCount(); i++) {
                    LoweredOp op = block.getOp(i);
                    if (op instanceof LoweredOp.LoadImm li) {
                        switch (li.type()) {
                            case I8_TYPE -> doPoolLong(asm, li.lo());
                            case F8_TYPE -> doPoolDouble(asm, Double.longBitsToDouble(li.lo()));
                            case F4_TYPE -> doPoolFloat(asm, Float.intBitsToFloat((int) li.lo()));
                            case I4_TYPE -> {} // iconst handles small ints inline
                        }
                    }
                    if (op instanceof LoweredOp.LoadVar lv) {
                        doPoolLong(asm, lv.byteOffset());
                    }
                    if (op instanceof LoweredOp.LoadColumn lc) {
                        doPoolLong(asm, elementBytes(lc.type()));
                    }
                }
            }
            // Ensure element-byte constants are pooled for all types used
            doPoolLong(asm, 4L);
            doPoolLong(asm, 8L);
        }

        private static int poolStaticField(BytecodeAssembler asm, int classCp, String name, String type) {
            return asm.poolField(classCp, asm.poolNameAndType(asm.poolUtf8(name), asm.poolUtf8(type)));
        }

        private static VecType poolVecType(BytecodeAssembler asm, int vecCls, String sVecType,
                                            String sSpec, String sMSeg, String sBO, String sMask,
                                            String sVec, String sConv, String scalarDesc) {
            // fromMemorySegment(species, seg, offset, order, mask) -> XxxVector
            int fromMemSegMasked = asm.poolMethod(vecCls, "fromMemorySegment",
                    "(" + sSpec + sMSeg + "J" + sBO + sMask + ")" + sVecType);

            // broadcast(species, scalar) -> XxxVector
            int broadcast = asm.poolMethod(vecCls, "broadcast",
                    "(" + sSpec + scalarDesc + ")" + sVecType);

            // compare(Comparison, Vector) -> VectorMask (generic erased to Vector)
            int compare = asm.poolMethod(vecCls, "compare",
                    "(" + "Ljdk/incubator/vector/VectorOperators$Comparison;" + sVec + ")" + sMask);

            // add/sub/mul/div(Vector) -> XxxVector
            int add = asm.poolMethod(vecCls, "add", "(" + sVec + ")" + sVecType);
            int sub = asm.poolMethod(vecCls, "sub", "(" + sVec + ")" + sVecType);
            int mul = asm.poolMethod(vecCls, "mul", "(" + sVec + ")" + sVecType);
            int div = asm.poolMethod(vecCls, "div", "(" + sVec + ")" + sVecType);

            // neg() -> XxxVector
            int neg = asm.poolMethod(vecCls, "neg", "()" + sVecType);

            // convertShape(Conversion, VectorSpecies, int) -> Vector
            int convertShape = asm.poolMethod(vecCls, "convertShape",
                    "(" + sConv + sSpec + "I)" + sVec);

            // SPECIES_PREFERRED field
            int speciesPreferred = asm.poolField(vecCls,
                    asm.poolNameAndType(asm.poolUtf8("SPECIES_PREFERRED"), asm.poolUtf8(sSpec)));

            return new VecType(vecCls, fromMemSegMasked, broadcast, compare,
                    add, sub, mul, div, neg, convertShape, speciesPreferred);
        }

        record VecType(
                int vecClass,
                int fromMemSegMasked,
                int broadcast,
                int compare,
                int add, int sub, int mul, int div, int neg,
                int convertShape,
                int speciesPreferred
        ) {}
    }
}
