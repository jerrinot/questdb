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
 * Supports straight-line programs (no control flow) with:
 * <ul>
 *   <li>full 8-byte programs: I8, F8, and mixed I8+F8</li>
 *   <li>compare-only I4 predicates, including mixed I8+I4+F8 programs</li>
 * </ul>
 * <p>
 * Mixed-width support keeps the row stride based on LongVector and uses an
 * IntVector species with the same row count per chunk, so masks can still be
 * normalized to a single Long-based row mask for boolean composition and
 * row-ID output.
 * <p>
 * Setup delegates to {@link FilterHelpers}; the hot loop emits direct Vector
 * API calls for C2 intrinsification.
 */
public final class VectorBytecodeFilterCompiler {

    private static final int SLOT_DATA_ADDR = 1;
    private static final int SLOT_VAR_SIZE_AUX = 5;
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

        ProgramShape shape = ProgramShape.analyze(program);
        emitMethod(asm, program, pool, tempCount, filterRowsName, filterRowsSig,
                stackMapAttr, FR_SLOT_ROWS_COUNT, FR_SLOT_FILTERED_ROWS, FR_FIRST_FREE, false, shape);
        emitMethod(asm, program, pool, tempCount, countRowsName, countRowsSig,
                stackMapAttr, CR_SLOT_ROWS_COUNT, -1, CR_FIRST_FREE, true, shape);

        asm.putShort(0);

        String dumpPath = System.getProperty("questdb.jit.vector.dump");
        if (dumpPath != null) {
            asm.dump(dumpPath);
        }
        if (Boolean.getBoolean("questdb.jit.vector.decompile")) {
            decompileToStderr(asm);
        }

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
            // CompareI128 is supported (UUID EQ/NE only)
            if (op instanceof LoweredOp.LoadColumn lc && !isSupportedLoadType(lc.type())) return false;
            if (op instanceof LoweredOp.LoadVar lv && !isSupportedLoadType(lv.type())) return false;
            if (op instanceof LoweredOp.LoadImm li && !isSupportedLoadType(li.type())) return false;
            if (op instanceof LoweredOp.Cast c && !isSupportedCast(c)) return false;
            if (op instanceof LoweredOp.Arithmetic a && !isSupportedArithmeticType(a.resultType())) return false;
            if (op instanceof LoweredOp.Compare c && !isSupportedCompareType(c.operandType())) return false;
            if (op instanceof LoweredOp.Negate n && !isSupportedArithmeticType(n.type())) return false;
        }
        return true;
    }

    private static boolean isSupportedArithmeticType(int type) {
        return type == I8_TYPE || type == F8_TYPE || type == I4_TYPE || type == F4_TYPE;
    }

    private static boolean isSupportedCompareType(int type) {
        return type == I8_TYPE || type == F8_TYPE || type == I4_TYPE || type == F4_TYPE;
    }

    private static boolean isSupportedLoadType(int type) {
        return type == I8_TYPE || type == F8_TYPE || type == I4_TYPE || type == F4_TYPE
                || type == I1_TYPE || type == I2_TYPE || type == I16_TYPE;
    }

    private static boolean isSupportedCast(LoweredOp.Cast c) {
        int from = c.fromType();
        int to = c.toType();
        // Same-width casts: I8<->F8, I4<->F4
        if ((from == I8_TYPE && to == F8_TYPE) || (from == F8_TYPE && to == I8_TYPE)) return true;
        if ((from == I4_TYPE && to == F4_TYPE) || (from == F4_TYPE && to == I4_TYPE)) return true;
        // Cross-width casts: F4<->F8, I4<->I8
        if ((from == F4_TYPE && to == F8_TYPE) || (from == F8_TYPE && to == F4_TYPE)) return true;
        if ((from == I4_TYPE && to == I8_TYPE) || (from == I8_TYPE && to == I4_TYPE)) return true;
        // Cross-width int→float: I4→F8 (for mixed int+double predicates)
        if (from == I4_TYPE && to == F8_TYPE) return true;
        // Widening from narrow types: I1/I2 → I4
        if ((from == I1_TYPE || from == I2_TYPE) && to == I4_TYPE) return true;
        return false;
    }

    /**
     * Precomputed program-shape analysis. A single scan over all ops replaces
     * the former {@code primaryType()}, {@code isPureF8()}, {@code usesType()},
     * and {@code findMaxColumnIndex()} helpers, which each walked the program
     * independently.
     *
     * @param pureF8         true when every op is F8 (no I8, no casts) and the
     *                       first column load is F8 — enables DoubleVector as
     *                       the primary species
     * @param usesI4         true when any op references I4_TYPE — requires an
     *                       IntVector species slot
     * @param maxColumnIndex highest column index seen across all LoadColumn ops
     */
    record ProgramShape(boolean pureF8, boolean usesI4, boolean usesF4,
                        boolean usesI1, boolean usesI2, int maxColumnIndex) {

        static ProgramShape analyze(LoweredProgram program) {
            boolean hasI8 = false;
            boolean hasCast = false;
            boolean usesI4 = false;
            boolean usesF4 = false;
            boolean usesI1 = false;
            boolean usesI2 = false;
            int primaryType = I8_TYPE;
            boolean primarySet = false;
            int maxColIdx = 0;

            for (int b = 0; b < program.getBlockCount(); b++) {
                LoweredBlock block = program.getBlock(b);
                for (int i = 0; i < block.getOpCount(); i++) {
                    LoweredOp op = block.getOp(i);
                    if (op instanceof LoweredOp.LoadColumn lc) {
                        if (!primarySet) {
                            primaryType = lc.type();
                            primarySet = true;
                        }
                        if (lc.type() == I8_TYPE) hasI8 = true;
                        if (lc.type() == I4_TYPE) usesI4 = true;
                        if (lc.type() == F4_TYPE) usesF4 = true;
                        if (lc.type() == I1_TYPE) usesI1 = true;
                        if (lc.type() == I2_TYPE) usesI2 = true;
                        maxColIdx = Math.max(maxColIdx, lc.columnIndex());
                    } else if (op instanceof LoweredOp.LoadVar lv) {
                        if (lv.type() == I8_TYPE) hasI8 = true;
                        if (lv.type() == I4_TYPE) usesI4 = true;
                        if (lv.type() == F4_TYPE) usesF4 = true;
                        if (lv.type() == I1_TYPE) usesI1 = true;
                        if (lv.type() == I2_TYPE) usesI2 = true;
                    } else if (op instanceof LoweredOp.LoadImm li) {
                        if (li.type() == I8_TYPE) hasI8 = true;
                        if (li.type() == I4_TYPE) usesI4 = true;
                        if (li.type() == F4_TYPE) usesF4 = true;
                        if (li.type() == I1_TYPE) usesI1 = true;
                        if (li.type() == I2_TYPE) usesI2 = true;
                    } else if (op instanceof LoweredOp.Compare c) {
                        if (c.operandType() == I4_TYPE) usesI4 = true;
                        if (c.operandType() == F4_TYPE) usesF4 = true;
                    } else if (op instanceof LoweredOp.Arithmetic a) {
                        if (a.resultType() == I8_TYPE) hasI8 = true;
                        if (a.resultType() == I4_TYPE) usesI4 = true;
                        if (a.resultType() == F4_TYPE) usesF4 = true;
                    } else if (op instanceof LoweredOp.Negate n) {
                        if (n.type() == I4_TYPE) usesI4 = true;
                        if (n.type() == F4_TYPE) usesF4 = true;
                    } else if (op instanceof LoweredOp.LoadVarSizeHeader vh) {
                        int normalized = IrLowering.normalizeVarSizeType(vh.headerType());
                        if (normalized == I4_TYPE) usesI4 = true;
                        maxColIdx = Math.max(maxColIdx, vh.columnIndex());
                    } else if (op instanceof LoweredOp.Cast) {
                        hasCast = true;
                    }
                }
            }

            boolean pureF8 = !hasI8 && !hasCast && !usesI4 && !usesF4
                    && !usesI1 && !usesI2 && primaryType == F8_TYPE;
            return new ProgramShape(pureF8, usesI4, usesF4, usesI1, usesI2, maxColIdx);
        }
    }

    private static int elementBytes(int type) {
        return switch (type) {
            case I1_TYPE -> 1;
            case I2_TYPE -> 2;
            case I4_TYPE, F4_TYPE -> 4;
            case I8_TYPE, F8_TYPE -> 8;
            case I16_TYPE -> 16;
            default -> throw new UnsupportedOperationException("element size for type: " + type);
        };
    }

    // === Slot layout ===

    record SlotLayout(
            int filteredCountSlot,
            int rowSlot,
            int strideSlot,
            int speciesSlot,
            int nativeOrderSlot,
            int activeMaskSlot,
            int intSpeciesSlot,
            int floatSpeciesSlot,
            int byteSpeciesSlot,
            int shortSpeciesSlot,
            int nullVecSlot,
            int nullMaskCacheSlot,
            int outputSegSlot,
            int iotaSlot,
            int matchCountSlot,
            int countAccSlot,
            int fullMaskSlot,
            int[] colSegSlots,
            int varsSegSlot,
            int[] tempSlots,
            int maxLocals,
            int objectLocalCount
    ) {

        static SlotLayout allocate(int firstFree, int tempCount, ProgramShape shape,
                                   boolean isCountOnly, boolean nullChecks) {
            boolean pureF8 = shape.pureF8();
            boolean usesI4 = shape.usesI4();
            boolean usesF4 = shape.usesF4();
            int filteredCountSlot = firstFree;
            int rowSlot = firstFree + 2;
            int strideSlot = firstFree + 4;
            int speciesSlot = firstFree + 6;
            int nativeOrderSlot = firstFree + 7;
            int activeMaskSlot = firstFree + 8;

            int nextSlot = firstFree + 9;
            int intSpeciesSlot = -1;
            if (usesI4) {
                intSpeciesSlot = nextSlot++;
            }
            int floatSpeciesSlot = -1;
            if (usesF4) {
                floatSpeciesSlot = nextSlot++;
            }
            int byteSpeciesSlot = -1;
            if (shape.usesI1()) {
                byteSpeciesSlot = nextSlot++;
            }
            int shortSpeciesSlot = -1;
            if (shape.usesI2()) {
                shortSpeciesSlot = nextSlot++;
            }
            int nullVecSlot = -1;
            int nullMaskCacheSlot = -1;
            if (nullChecks && !pureF8) {
                nullVecSlot = nextSlot++;
                nullMaskCacheSlot = nextSlot++;
            }
            int outputSegSlot = -1;
            int iotaSlot = -1;
            int matchCountSlot = -1;
            int countAccSlot = -1;
            int fullMaskSlot = -1;
            if (!isCountOnly) {
                outputSegSlot = nextSlot++;
                iotaSlot = nextSlot++;
                matchCountSlot = nextSlot++;
                fullMaskSlot = nextSlot++;
            }

            int maxColIndex = shape.maxColumnIndex();
            int[] colSegSlots = new int[maxColIndex + 1];
            for (int i = 0; i <= maxColIndex; i++) {
                colSegSlots[i] = nextSlot++;
            }
            int varsSegSlot = nextSlot++;
            if (isCountOnly) {
                countAccSlot = nextSlot++;
                fullMaskSlot = nextSlot++;
            }

            int[] tempSlots = new int[tempCount];
            for (int i = 0; i < tempCount; i++) {
                tempSlots[i] = nextSlot++;
            }
            int maxLocals = nextSlot;
            int objectLocalCount = maxLocals - (firstFree + 6);

            return new SlotLayout(
                    filteredCountSlot, rowSlot, strideSlot,
                    speciesSlot, nativeOrderSlot, activeMaskSlot,
                    intSpeciesSlot, floatSpeciesSlot, byteSpeciesSlot, shortSpeciesSlot,
                    nullVecSlot, nullMaskCacheSlot,
                    outputSegSlot, iotaSlot, matchCountSlot, countAccSlot, fullMaskSlot,
                    colSegSlots, varsSegSlot, tempSlots,
                    maxLocals, objectLocalCount
            );
        }
    }

    /**
     * BCI positions captured during loop emission, needed by stack-map generation.
     */
    record LoopEmission(int loopStart, int nextStart, int tailStart, int exitStart) {}

    /**
     * Immutable context threaded through all emission helpers. Replaces the
     * long parameter lists that previously passed asm, pool, slot layout,
     * and flags individually. Op-specific arguments stay explicit.
     */
    record EmitContext(
            BytecodeAssembler asm,
            Pool pool,
            SlotLayout s,
            boolean nullChecks,
            boolean pureF8,
            LoweredBlock block,
            // Tracks which lhs temp ID has its null mask cached in nullMaskCacheSlot.
            // cachedNullMaskOwner[0] == -1 means no mask is cached.
            int[] cachedNullMaskOwner
    ) {}

    // === Method emission ===

    private static void emitMethod(
            BytecodeAssembler asm, LoweredProgram program, Pool pool,
            int tempCount, int methodName, int methodSig, int stackMapAttr,
            int rowsCountSlot, int filteredRowsSlot, int firstFree, boolean isCountOnly,
            ProgramShape shape
    ) {
        boolean pureF8 = shape.pureF8();
        boolean usesI4 = shape.usesI4();
        boolean nullChecks = program.getOptions().isNullChecksEnabled();
        boolean needsNullVec = nullChecks && !pureF8;

        SlotLayout s = SlotLayout.allocate(firstFree, tempCount, shape, isCountOnly, nullChecks);
        LoweredBlock block = program.getBlock(program.getEntryBlockId());
        EmitContext ctx = new EmitContext(asm, pool, s, nullChecks, pureF8, block, new int[]{-1});
        int[] useCounts = computeUseCounts(block, tempCount);
        Terminator.Return ret = (Terminator.Return) block.getTerminator();

        asm.startMethod(methodName, methodSig, 12, s.maxLocals());

        emitSetup(ctx, block, filteredRowsSlot, isCountOnly, shape, needsNullVec);

        // Count-only and row-ID paths are kept separate because they differ in:
        //   - reduction strategy: count-only accumulates a LongVector via masked
        //     add(1), reduced to scalar at exit; row-ID compresses matching row
        //     indices and writes them out each iteration
        //   - local slots: count-only needs countAccSlot; row-ID needs
        //     outputSegSlot, iotaSlot, matchCountSlot
        //   - stack-map frame count: count-only has 3 frames (loop/tail/exit);
        //     row-ID has 4 (loop/next/tail/exit) because the skip-branch after
        //     trueCount == 0 creates an extra join point
        LoopEmission loop;
        if (isCountOnly) {
            loop = emitCountOnlyLoop(ctx, block, useCounts, ret, rowsCountSlot);
        } else {
            loop = emitFilterRowsLoop(ctx, block, useCounts, ret, rowsCountSlot);
        }
        asm.lload(s.filteredCountSlot());
        asm.lreturn();

        emitStackMaps(ctx, loop, stackMapAttr, isCountOnly,
                usesI4, needsNullVec, tempCount, shape);
        asm.endMethod();
    }

    private static void emitSetup(
            EmitContext ctx, LoweredBlock block,
            int filteredRowsSlot, boolean isCountOnly, ProgramShape shape,
            boolean needsNullVec
    ) {
        boolean usesI4 = shape.usesI4();
        boolean usesF4 = shape.usesF4();
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();

        asm.lconst_0();
        asm.lstore(s.filteredCountSlot());
        asm.lconst_0();
        asm.lstore(s.rowSlot());

        // Pure F8 programs use DoubleVector.SPECIES_PREFERRED so masks stay
        // VectorMask<Double> throughout the hot path without cast traffic.
        // Mixed I8+F8 programs use LongVector.SPECIES_PREFERRED for uniform
        // mask type across BooleanOp AND/OR; loads and compares cast as needed.
        if (ctx.pureF8()) {
            asm.invokeStatic(pool.helpersDoubleSpecies);
        } else {
            asm.invokeStatic(pool.helpersLongSpecies);
        }
        asm.astore(s.speciesSlot());
        if (usesI4) {
            asm.invokeStatic(pool.helpersIntSpeciesForLongRows);
            asm.astore(s.intSpeciesSlot());
        }
        if (usesF4) {
            asm.invokeStatic(pool.helpersFloatSpeciesForLongRows);
            asm.astore(s.floatSpeciesSlot());
        }
        if (shape.usesI1()) {
            asm.invokeStatic(pool.helpersByteSpeciesForLongRows);
            asm.astore(s.byteSpeciesSlot());
        }
        if (shape.usesI2()) {
            asm.invokeStatic(pool.helpersShortSpeciesForLongRows);
            asm.astore(s.shortSpeciesSlot());
        }
        // Hoist species.length() as a long to avoid per-iteration invokeInterface + i2l.
        asm.aload(s.speciesSlot());
        asm.invokeInterface(pool.speciesLength, 0);
        asm.i2l();
        asm.lstore(s.strideSlot());
        asm.invokeStatic(pool.helpersNativeByteOrder);
        asm.astore(s.nativeOrderSlot());

        for (int i = 0; i < s.colSegSlots().length; i++) {
            asm.lload(SLOT_DATA_ADDR);
            asm.iconst(i);
            asm.invokeStatic(pool.helpersColumnSegment);
            asm.astore(s.colSegSlots()[i]);
        }

        asm.lload(SLOT_VARS_ADDR);
        asm.invokeStatic(pool.helpersSegment);
        asm.astore(s.varsSegSlot());

        if (!isCountOnly) {
            asm.lload(filteredRowsSlot);
            asm.invokeStatic(pool.helpersSegment);
            asm.astore(s.outputSegSlot());
            // iota always uses Long species (row IDs are longs)
            asm.invokeStatic(pool.helpersLongSpecies);
            asm.invokeStatic(pool.helpersIotaVector);
            asm.astore(s.iotaSlot());
        }

        if (needsNullVec) {
            // Load LONG_NULL sentinel vector for I8 null-aware comparisons
            // and arithmetic. F8 comparisons detect NaN internally via IS_NAN,
            // so pure F8 programs don't need this.
            asm.invokeStatic(pool.helpersLongSpecies);
            asm.invokeStatic(pool.helpersLongNullVector);
            asm.astore(s.nullVecSlot());
            asm.aconst_null();
            asm.astore(s.nullMaskCacheSlot());
        }
        if (isCountOnly) {
            // countAccSlot is kept allocated for stack-map stability but unused.
            // Count-only reduction accumulates into filteredCountSlot via scalar
            // trueCount() + ladd, which avoids creating a LongVector accumulator
            // that OSR C2 would struggle to scalarize.
            asm.aconst_null();
            asm.astore(s.countAccSlot());
        }
        // fullMaskSlot holds species.indexInRange(0, stride) — a mask with all
        // lanes active. Precomputing it avoids re-deriving the full mask on every
        // full-chunk iteration; the tail loop computes its own partial mask.
        asm.aload(s.speciesSlot());
        asm.lconst_0();
        asm.lload(s.strideSlot());
        asm.invokeInterface(pool.speciesIndexInRange, 4);
        asm.astore(s.fullMaskSlot());

        asm.aconst_null();
        asm.astore(s.activeMaskSlot());
        if (s.matchCountSlot() >= 0) {
            asm.iconst(0);
            asm.istore(s.matchCountSlot());
        }
        for (int i = 0; i < s.tempSlots().length; i++) {
            asm.aconst_null();
            asm.astore(s.tempSlots()[i]);
        }

        // Hoist loop-invariant ops (immediates and bind variables) out of the
        // hot loop. These values don't change between iterations.
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadImm li && li.type() != I16_TYPE) {
                emitLoadImm(ctx, li);
            } else if (op instanceof LoweredOp.LoadVar lv && lv.type() != I16_TYPE) {
                emitLoadVar(ctx, lv);
            }
        }
    }

    private static LoopEmission emitCountOnlyLoop(
            EmitContext ctx, LoweredBlock block, int[] useCounts,
            Terminator.Return ret, int rowsCountSlot
    ) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();

        int loopStart = asm.position();

        asm.lload(s.rowSlot());
        asm.lload(s.strideSlot());
        asm.ladd();
        asm.lload(rowsCountSlot);
        asm.lcmp();
        int tailBranch = asm.ifgt();

        asm.aload(s.fullMaskSlot());
        asm.astore(s.activeMaskSlot());
        emitBlockOps(ctx, block, useCounts, false);
        emitCountOnlyReduction(ctx, ret);

        int nextStart = asm.position();
        asm.lload(s.rowSlot());
        asm.lload(s.strideSlot());
        asm.ladd();
        asm.lstore(s.rowSlot());
        int backJmp = asm.goto_();
        asm.setJmp(backJmp, loopStart);

        int tailStart = asm.position();
        asm.setJmp(tailBranch, tailStart);
        asm.lload(s.rowSlot());
        asm.lload(rowsCountSlot);
        asm.lcmp();
        int exitBranch = asm.ifge();

        asm.aload(s.speciesSlot());
        asm.lload(s.rowSlot());
        asm.lload(rowsCountSlot);
        asm.invokeInterface(pool.speciesIndexInRange, 4);
        asm.astore(s.activeMaskSlot());
        emitBlockOps(ctx, block, useCounts, true);
        emitCountOnlyReduction(ctx, ret);

        int exitStart = asm.position();
        asm.setJmp(exitBranch, exitStart);
        // filteredCountSlot already holds the scalar count — no vector reduction needed.

        return new LoopEmission(loopStart, nextStart, tailStart, exitStart);
    }

    private static LoopEmission emitFilterRowsLoop(
            EmitContext ctx, LoweredBlock block, int[] useCounts,
            Terminator.Return ret, int rowsCountSlot
    ) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();

        int loopStart = asm.position();

        asm.lload(s.rowSlot());
        asm.lload(s.strideSlot());
        asm.ladd();
        asm.lload(rowsCountSlot);
        asm.lcmp();
        int tailBranch = asm.ifgt();

        asm.aload(s.fullMaskSlot());
        asm.astore(s.activeMaskSlot());

        emitBlockOps(ctx, block, useCounts, false);

        int skipBranch = emitRowIdTerminator(ctx, ret);

        int nextStart = asm.position();
        asm.setJmp(skipBranch, nextStart);
        asm.lload(s.rowSlot());
        asm.lload(s.strideSlot());
        asm.ladd();
        asm.lstore(s.rowSlot());

        int backJmp = asm.goto_();
        asm.setJmp(backJmp, loopStart);

        int tailStart = asm.position();
        asm.setJmp(tailBranch, tailStart);
        asm.lload(s.rowSlot());
        asm.lload(rowsCountSlot);
        asm.lcmp();
        int exitBranch = asm.ifge();

        asm.aload(s.speciesSlot());
        asm.lload(s.rowSlot());
        asm.lload(rowsCountSlot);
        asm.invokeInterface(pool.speciesIndexInRange, 4);
        asm.astore(s.activeMaskSlot());

        emitBlockOps(ctx, block, useCounts, true);

        int tailSkipBranch = emitRowIdTerminator(ctx, ret);

        int exitStart = asm.position();
        asm.setJmp(tailSkipBranch, exitStart);
        asm.setJmp(exitBranch, exitStart);

        return new LoopEmission(loopStart, nextStart, tailStart, exitStart);
    }

    /**
     * Emits the row-ID terminator sequence shared by the full-chunk and
     * tail-chunk paths: finalize result mask, compute trueCount, skip if
     * zero, compress matching rows, write output, and increment filteredCount.
     *
     * @return the ifeq branch position; the caller resolves it to the
     *         correct join point (nextStart or exitStart)
     */
    private static int emitRowIdTerminator(EmitContext ctx, Terminator.Return ret) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();

        // Finalize result mask
        if (ret.src() == Terminator.Return.ACCEPT) {
            asm.aload(s.activeMaskSlot());
        } else {
            asm.aload(s.tempSlots()[ret.src()]);
            asm.checkcast(pool.vectorMaskClass);
            asm.aload(s.activeMaskSlot());
            asm.invokeVirtual(pool.maskAnd);
        }
        asm.astore(s.activeMaskSlot());

        // Compute trueCount and skip if zero
        asm.aload(s.activeMaskSlot());
        asm.invokeVirtual(pool.maskTrueCount);
        asm.istore(s.matchCountSlot());
        asm.iload(s.matchCountSlot());
        int skipBranch = asm.ifeq();

        // Add row base to iota vector and compress
        asm.aload(s.iotaSlot());
        asm.lload(s.rowSlot());
        asm.invokeVirtual(pool.longVecAddScalar);
        asm.aload(s.activeMaskSlot());
        if (ctx.pureF8()) {
            // Pure F8: cast VectorMask<Double> → VectorMask<Long> for LongVector.compress
            asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
            asm.invokeVirtual(pool.maskCast);
        }
        asm.invokeVirtual(pool.longVecCompress);

        // Write compressed row IDs to output segment
        asm.iload(s.matchCountSlot());
        asm.aload(s.outputSegSlot());
        asm.lload(s.filteredCountSlot());
        asm.ldc2_w(pool.longEight);
        asm.lmul();
        asm.aload(s.nativeOrderSlot());
        asm.invokeStatic(pool.writeCompressedRows);

        // Increment filtered count
        asm.iload(s.matchCountSlot());
        asm.i2l();
        asm.lload(s.filteredCountSlot());
        asm.ladd();
        asm.lstore(s.filteredCountSlot());

        return skipBranch;
    }

    // The JVM verifier requires a StackMapTable with a frame at every branch
    // target. BCIs are captured at the exact positions of loopStart, nextStart
    // (row-ID only), tailStart, and exitStart because those are the four jump
    // targets. The first frame is a full_frame that declares every local with
    // its precise type; subsequent frames are same_frame deltas. The order
    // must match the slot-allocation order above.
    private static void emitStackMaps(
            EmitContext ctx, LoopEmission loop, int stackMapAttr,
            boolean isCountOnly, boolean usesI4, boolean needsNullVec,
            int tempCount, ProgramShape shape
    ) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();
        int codeStart = asm.getCodeStart();
        int loopBci = loop.loopStart() - codeStart;
        int nextBci = loop.nextStart() - codeStart;
        int exitBci = loop.exitStart() - codeStart;
        int tailBci = loop.tailStart() >= 0 ? loop.tailStart() - codeStart : -1;

        asm.endMethodCode();
        asm.putShort(0);

        asm.putShort(1);
        int stackMapCount = isCountOnly ? 3 : 4;
        asm.startStackMapTables(stackMapAttr, stackMapCount);

        // Build precise local type declarations for the full_frame.
        // Typed locals eliminate checkcast overhead in the hot loop.
        int longParamCount = isCountOnly ? 6 : 7;
        int totalLocals = 1 + longParamCount + 3 + s.objectLocalCount();

        asm.putByte(0xff); // full_frame
        asm.putShort(loopBci);
        asm.putShort(totalLocals);
        // this
        asm.putITEM_Object(pool.objectClassIndex);
        // long params
        for (int i = 0; i < longParamCount; i++) {
            asm.putITEM_Long();
        }
        // filteredCount, row, stride
        asm.putITEM_Long();
        asm.putITEM_Long();
        asm.putITEM_Long();
        // Object locals with precise types (order must match slot allocation)
        asm.putITEM_Object(pool.vecSpeciesClass); // speciesSlot
        asm.putITEM_Object(pool.byteOrderClass);  // nativeOrderSlot
        asm.putITEM_Object(pool.vectorMaskClass);  // activeMaskSlot
        if (usesI4) {
            asm.putITEM_Object(pool.vecSpeciesClass); // intSpeciesSlot
        }
        if (shape.usesF4()) {
            asm.putITEM_Object(pool.vecSpeciesClass); // floatSpeciesSlot
        }
        if (shape.usesI1()) {
            asm.putITEM_Object(pool.vecSpeciesClass); // byteSpeciesSlot
        }
        if (shape.usesI2()) {
            asm.putITEM_Object(pool.vecSpeciesClass); // shortSpeciesSlot
        }
        if (needsNullVec) {
            asm.putITEM_Object(pool.longVectorClass); // nullVecSlot — always LongVector
            asm.putITEM_Object(pool.vectorMaskClass);  // nullMaskCacheSlot
        }
        if (!isCountOnly) {
            asm.putITEM_Object(pool.memSegClass);      // outputSegSlot
            asm.putITEM_Object(pool.longVectorClass);  // iotaSlot
            asm.putITEM_Integer();                      // matchCountSlot
            asm.putITEM_Object(pool.vectorMaskClass);   // fullMaskSlot
        }
        for (int i = 0; i <= shape.maxColumnIndex(); i++) {
            asm.putITEM_Object(pool.memSegClass); // colSegSlots[i]
        }
        asm.putITEM_Object(pool.memSegClass); // varsSegSlot
        if (isCountOnly) {
            asm.putITEM_Object(pool.longVectorClass); // countAccSlot
            asm.putITEM_Object(pool.vectorMaskClass); // fullMaskSlot
        }
        for (int i = 0; i < tempCount; i++) {
            asm.putITEM_Object(pool.objectClassIndex); // temp[i] — generic
        }
        // empty stack
        asm.putShort(0);

        if (isCountOnly) {
            emitSameFrame(asm, tailBci, loopBci);
            emitSameFrame(asm, exitBci, tailBci);
        } else {
            emitSameFrame(asm, nextBci, loopBci);
            emitSameFrame(asm, tailBci, nextBci);
            emitSameFrame(asm, exitBci, tailBci);
        }

        asm.endStackMapTables();
    }

    private static int[] computeUseCounts(LoweredBlock block, int tempCount) {
        int[] counts = new int[tempCount];
        for (int i = 0; i < block.getOpCount(); i++) {
            switch (block.getOp(i)) {
                case LoweredOp.LoadColumn ignored -> {
                }
                case LoweredOp.LoadVarSizeHeader ignored -> {
                }
                case LoweredOp.LoadVar ignored -> {
                }
                case LoweredOp.LoadImm ignored -> {
                }
                case LoweredOp.Cast c -> counts[c.src()]++;
                case LoweredOp.Compare c -> {
                    counts[c.lhs()]++;
                    counts[c.rhs()]++;
                }
                case LoweredOp.CompareI128 c -> {
                    counts[c.lhs()]++;
                    counts[c.rhs()]++;
                }
                case LoweredOp.Arithmetic a -> {
                    counts[a.lhs()]++;
                    counts[a.rhs()]++;
                }
                case LoweredOp.BooleanOp bo -> {
                    counts[bo.lhs()]++;
                    counts[bo.rhs()]++;
                }
                case LoweredOp.Negate neg -> counts[neg.src()]++;
                case LoweredOp.Not n -> counts[n.src()]++;
                case LoweredOp.Move m -> counts[m.src()]++;
            }
        }

        switch (block.getTerminator()) {
            case Terminator.Branch branch -> counts[branch.src()]++;
            case Terminator.Return ret -> {
                if (ret.src() >= 0) {
                    counts[ret.src()]++;
                }
            }
            case Terminator.Goto ignored -> {
            }
        }

        return counts;
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

    private static void emitCountOnlyReduction(EmitContext ctx, Terminator.Return ret) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();

        // Finalize the result mask into activeMaskSlot.
        if (ret.src() == Terminator.Return.ACCEPT) {
            asm.aload(s.activeMaskSlot());
        } else {
            asm.aload(s.tempSlots()[ret.src()]);
            asm.checkcast(pool.vectorMaskClass);
            asm.aload(s.activeMaskSlot());
            asm.invokeVirtual(pool.maskAnd);
        }

        if (ctx.pureF8()) {
            asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
            asm.invokeVirtual(pool.maskCast);
        }

        // Accumulate matching rows via scalar trueCount() + ladd.
        // This avoids creating a LongVector accumulator per iteration,
        // which OSR C2 struggles to scalarize into a ZMM register.
        // The trueCount() intrinsic maps to kmov + popcnt (2 cycles).
        asm.invokeVirtual(pool.maskTrueCount);
        asm.i2l();
        asm.lload(s.filteredCountSlot());
        asm.ladd();
        asm.lstore(s.filteredCountSlot());
    }

    private static void emitBlockOps(
            EmitContext ctx, LoweredBlock block, int[] useCounts, boolean maskedLoads
    ) {
        BytecodeAssembler asm = ctx.asm();
        int[] tempSlots = ctx.s().tempSlots();
        HashMap<Long, Integer> loadCache = new HashMap<>();
        // Invalidate cached null mask — column data changes each iteration.
        ctx.cachedNullMaskOwner()[0] = -1;
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            int consumed = tryEmitLongInEqOrChain(ctx, block, i, useCounts, maskedLoads);
            if (consumed >= 0) {
                i = consumed;
                continue;
            }
            if (op instanceof LoweredOp.LoadImm li && li.type() == I16_TYPE) {
                // I16 immediates are consumed directly by CompareI128
                continue;
            }
            if (op instanceof LoweredOp.LoadImm || op instanceof LoweredOp.LoadVar) {
                continue;
            }
            if (op instanceof LoweredOp.LoadColumn lc && lc.type() == I16_TYPE) {
                // I16 column loads are consumed directly by CompareI128
                continue;
            }
            if (op instanceof LoweredOp.LoadColumn lc) {
                long cacheKey = (((long) lc.type()) << 32) | (lc.columnIndex() & 0xffff_ffffL);
                Integer cachedTemp = loadCache.get(cacheKey);
                if (cachedTemp != null) {
                    if (cachedTemp != lc.dst()) {
                        asm.aload(tempSlots[cachedTemp]);
                        asm.astore(tempSlots[lc.dst()]);
                    }
                } else {
                    emitLoadColumn(ctx, lc, maskedLoads);
                    loadCache.put(cacheKey, lc.dst());
                }
                continue;
            }
            emitOp(ctx, op, maskedLoads);
        }
    }

    private static int tryEmitLongInEqOrChain(
            EmitContext ctx, LoweredBlock block, int startIndex,
            int[] useCounts, boolean maskedLoads
    ) {
        if (!(block.getOp(startIndex) instanceof LoweredOp.LoadColumn firstLoad) || firstLoad.type() != I8_TYPE) {
            return -1;
        }
        if (startIndex + 4 >= block.getOpCount()) {
            return -1;
        }
        if (useCounts[firstLoad.dst()] != 1) {
            return -1;
        }

        if (!(block.getOp(startIndex + 1) instanceof LoweredOp.Compare firstCompare)
                || firstCompare.opcode() != EQ
                || firstCompare.operandType() != I8_TYPE) {
            return -1;
        }
        int firstValueTemp = eqOtherOperand(firstCompare, firstLoad.dst());
        if (firstValueTemp < 0) {
            return -1;
        }

        int accumTemp = firstCompare.dst();
        int scan = startIndex + 2;
        int chainLength = 1;

        while (scan < block.getOpCount()) {
            // Skip LoadImm/LoadVar ops that precede the next chain link.
            int s = scan;
            while (s < block.getOpCount() && (block.getOp(s) instanceof LoweredOp.LoadImm
                    || block.getOp(s) instanceof LoweredOp.LoadVar)) {
                s++;
            }
            if (s + 2 >= block.getOpCount()) {
                break;
            }
            if (!(block.getOp(s) instanceof LoweredOp.LoadColumn nextLoad)
                    || nextLoad.type() != I8_TYPE
                    || nextLoad.columnIndex() != firstLoad.columnIndex()) {
                break;
            }
            if (useCounts[nextLoad.dst()] != 1) {
                break;
            }
            if (!(block.getOp(s + 1) instanceof LoweredOp.Compare nextCompare)
                    || nextCompare.opcode() != EQ
                    || nextCompare.operandType() != I8_TYPE) {
                break;
            }
            if (eqOtherOperand(nextCompare, nextLoad.dst()) < 0) {
                break;
            }
            if (!(block.getOp(s + 2) instanceof LoweredOp.BooleanOp or)
                    || or.opcode() != OR
                    || !matchesBooleanInputs(or, accumTemp, nextCompare.dst())) {
                break;
            }
            if (useCounts[accumTemp] != 1 || useCounts[nextCompare.dst()] != 1) {
                break;
            }
            accumTemp = or.dst();
            scan = s + 3;
            chainLength++;
        }

        if (chainLength < 2) {
            return -1;
        }

        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        int[] tempSlots = ctx.s().tempSlots();

        // Emit the first compare.  Store the running OR accumulator in the
        // slot that the rest of the program expects the final result in
        // (accumTemp — the last OR's dst from the scan phase above).
        int resultSlot = tempSlots[accumTemp];
        emitLoadColumn(ctx, firstLoad, maskedLoads);
        emitLongEqMaskFromLoadedValue(asm, tempSlots[firstLoad.dst()], tempSlots[firstValueTemp], pool);
        asm.astore(resultSlot);

        int pos = startIndex + 2;
        for (int i = 1; i < chainLength; i++) {
            // Skip LoadImm/LoadVar ops preceding the next chain link.
            while (block.getOp(pos) instanceof LoweredOp.LoadImm
                    || block.getOp(pos) instanceof LoweredOp.LoadVar) {
                pos++;
            }
            LoweredOp.LoadColumn load = (LoweredOp.LoadColumn) block.getOp(pos);
            LoweredOp.Compare compare = (LoweredOp.Compare) block.getOp(pos + 1);

            // Load the running accumulator, emit the next compare (result stays
            // on the JVM stack), then OR directly — no intermediate temp store.
            asm.aload(resultSlot);
            asm.checkcast(pool.vectorMaskClass);
            emitLongEqMaskFromLoadedValue(asm, tempSlots[firstLoad.dst()], tempSlots[eqOtherOperand(compare, load.dst())], pool);
            asm.checkcast(pool.vectorMaskClass);
            asm.invokeVirtual(pool.maskOr);
            asm.astore(resultSlot);

            pos += 3;
        }

        return scan - 1;
    }

    private static int eqOtherOperand(LoweredOp.Compare compare, int loadTemp) {
        if (compare.lhs() == loadTemp) {
            return compare.rhs();
        }
        if (compare.rhs() == loadTemp) {
            return compare.lhs();
        }
        return -1;
    }

    private static boolean matchesBooleanInputs(LoweredOp.BooleanOp op, int lhs, int rhs) {
        return (op.lhs() == lhs && op.rhs() == rhs) || (op.lhs() == rhs && op.rhs() == lhs);
    }

    private static void emitLongEqMaskFromLoadedValue(BytecodeAssembler asm, int loadedSlot, int valueSlot, Pool pool) {
        Pool.VecType vt = pool.vecType(I8_TYPE);
        asm.aload(loadedSlot);
        asm.checkcast(vt.vecClass);
        asm.getstatic(pool.comparisonOp(EQ));
        asm.aload(valueSlot);
        asm.checkcast(vt.vecClass);
        asm.invokeVirtual(vt.compare);
    }

    private static void emitTypeSpecies(EmitContext ctx, int type) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();
        switch (type) {
            case I8_TYPE -> asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
            case F8_TYPE -> {
                if (ctx.pureF8()) {
                    asm.aload(s.speciesSlot());
                } else {
                    asm.getstatic(pool.vecType(F8_TYPE).speciesPreferred);
                }
            }
            case I4_TYPE -> asm.aload(s.intSpeciesSlot());
            case F4_TYPE -> asm.aload(s.floatSpeciesSlot());
            case I1_TYPE -> asm.aload(s.byteSpeciesSlot());
            case I2_TYPE -> asm.aload(s.shortSpeciesSlot());
            default -> throw new UnsupportedOperationException("vector species for type: " + type);
        }
    }

    private static boolean needsLoadMaskCast(int type, boolean pureF8) {
        return type == I4_TYPE || type == F4_TYPE || type == I1_TYPE || type == I2_TYPE
                || (!pureF8 && type == F8_TYPE);
    }

    // === Op emission ===

    private static void emitOp(EmitContext ctx, LoweredOp op, boolean maskedLoads) {
        BytecodeAssembler asm = ctx.asm();
        int[] tempSlots = ctx.s().tempSlots();
        switch (op) {
            case LoweredOp.LoadColumn lc -> emitLoadColumn(ctx, lc, maskedLoads);
            case LoweredOp.LoadVarSizeHeader vh -> emitLoadVarSizeHeader(ctx, vh);
            case LoweredOp.LoadImm li -> emitLoadImm(ctx, li);
            case LoweredOp.LoadVar lv -> emitLoadVar(ctx, lv);
            case LoweredOp.Compare c -> emitCompare(ctx, c);
            case LoweredOp.BooleanOp bo -> emitBooleanOp(ctx, bo);
            case LoweredOp.Not n -> emitNot(ctx, n);
            case LoweredOp.Arithmetic a -> emitArithmetic(ctx, a);
            case LoweredOp.Negate neg -> emitNegate(ctx, neg);
            case LoweredOp.Move m -> {
                asm.aload(tempSlots[m.src()]);
                asm.astore(tempSlots[m.dst()]);
            }
            case LoweredOp.Cast c -> emitCast(ctx, c);
            case LoweredOp.CompareI128 c128 -> emitCompareI128(ctx, c128);
            default -> throw new UnsupportedOperationException("Vector op: " + op);
        }
    }

    private static void emitLoadColumn(EmitContext ctx, LoweredOp.LoadColumn lc, boolean maskedLoads) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();
        Pool.VecType vt = pool.vecType(lc.type());
        emitTypeSpecies(ctx, lc.type());
        asm.aload(s.colSegSlots()[lc.columnIndex()]);
        asm.lload(s.rowSlot());
        asm.ldc2_w(pool.ensureLongPooled(elementBytes(lc.type())));
        asm.lmul();
        asm.aload(s.nativeOrderSlot());
        if (maskedLoads) {
            asm.aload(s.activeMaskSlot());
            if (needsLoadMaskCast(lc.type(), ctx.pureF8())) {
                emitTypeSpecies(ctx, lc.type());
                asm.invokeVirtual(pool.maskCast);
            }
            asm.invokeStatic(vt.fromMemSegMasked);
        } else {
            asm.invokeStatic(vt.fromMemSeg);
        }
        asm.astore(s.tempSlots()[lc.dst()]);
    }

    private static void emitLoadVarSizeHeader(EmitContext ctx, LoweredOp.LoadVarSizeHeader vh) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();
        int normalizedType = IrLowering.normalizeVarSizeType(vh.headerType());
        // Gather header values via scalar helper → vector
        // STRING_HEADER → gatherStringHeaders(dataAddr, auxAddr, col, row, intSpecies) → IntVector
        // BINARY_HEADER → gatherBinaryHeaders(dataAddr, auxAddr, col, row, longSpecies) → LongVector
        // VARCHAR_HEADER → gatherVarcharHeaders(auxAddr, col, row, longSpecies) → LongVector
        int gatherMethod = pool.headerGatherMethod(vh.headerType());
        if (vh.headerType() != VARCHAR_HEADER_TYPE) {
            asm.lload(SLOT_DATA_ADDR);
        }
        asm.lload(SLOT_VAR_SIZE_AUX);
        asm.iconst(vh.columnIndex());
        asm.lload(s.rowSlot());
        if (normalizedType == I4_TYPE) {
            asm.aload(s.intSpeciesSlot());
        } else {
            asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
        }
        asm.invokeStatic(gatherMethod);
        asm.astore(s.tempSlots()[vh.dst()]);
    }

    private static void emitLoadImm(EmitContext ctx, LoweredOp.LoadImm li) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        Pool.VecType vt = pool.vecType(li.type());
        emitTypeSpecies(ctx, li.type());
        switch (li.type()) {
            case I1_TYPE -> asm.iconst((int) (byte) li.lo());
            case I2_TYPE -> asm.iconst((int) (short) li.lo());
            case I4_TYPE -> emitIntConst(asm, (int) li.lo(), pool);
            case I8_TYPE -> asm.ldc2_w(pool.ensureLongPooled(li.lo()));
            case F4_TYPE -> asm.ldc(pool.ensureFloatPooled(Float.intBitsToFloat((int) li.lo())));
            case F8_TYPE -> asm.ldc2_w(pool.ensureDoublePooled(Double.longBitsToDouble(li.lo())));
            default -> throw new UnsupportedOperationException("LoadImm type: " + li.type());
        }
        asm.invokeStatic(vt.broadcast);
        asm.astore(ctx.s().tempSlots()[li.dst()]);
    }

    private static void emitIntConst(BytecodeAssembler asm, int value, Pool pool) {
        if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            asm.iconst(value);
        } else {
            asm.ldc(pool.ensureIntPooled(value));
        }
    }

    private static void emitLoadVar(EmitContext ctx, LoweredOp.LoadVar lv) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();
        Pool.VecType vt = pool.vecType(lv.type());
        emitTypeSpecies(ctx, lv.type());
        // Load scalar from vars segment and broadcast
        asm.aload(s.varsSegSlot());
        asm.getstatic(pool.varLayout(lv.type()));
        asm.ldc2_w(pool.ensureLongPooled(lv.byteOffset()));
        int getter = switch (lv.type()) {
            case I1_TYPE -> pool.memSegGetByte;
            case I2_TYPE -> pool.memSegGetShort;
            case I4_TYPE -> pool.memSegGetInt;
            case I8_TYPE -> pool.memSegGetLong;
            case F4_TYPE -> pool.memSegGetFloat;
            case F8_TYPE -> pool.memSegGetDouble;
            default -> throw new UnsupportedOperationException("LoadVar type: " + lv.type());
        };
        asm.invokeInterface(getter, 3);
        asm.invokeStatic(vt.broadcast);
        asm.astore(s.tempSlots()[lv.dst()]);
    }

    private static void emitCompare(EmitContext ctx, LoweredOp.Compare c) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        int[] tempSlots = ctx.s().tempSlots();
        boolean nullChecks = ctx.nullChecks();
        Pool.VecType vt = pool.vecType(c.operandType());

        if (c.operandType() == F8_TYPE) {
            // Double comparisons always use helpers (epsilon + NaN handling).
            int helperMethod = pool.doubleCompareHelper(c.opcode());
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.invokeStatic(helperMethod);
            if (!ctx.pureF8()) {
                // Mixed I8+F8: cast VectorMask<Double> → VectorMask<Long> for uniform mask type
                asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
                asm.invokeVirtual(pool.maskCast);
            }
            asm.astore(tempSlots[c.dst()]);
        } else if (c.operandType() == F4_TYPE) {
            // Float comparisons always use helpers (epsilon + NaN handling).
            int helperMethod = pool.floatCompareHelper(c.opcode());
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.invokeStatic(helperMethod);
            // Cast VectorMask<Float> → VectorMask<Long> for uniform mask type
            asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
            asm.invokeVirtual(pool.maskCast);
            asm.astore(tempSlots[c.dst()]);
        } else if (c.operandType() == I4_TYPE) {
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            if (nullChecks) {
                asm.aload(tempSlots[c.rhs()]);
                asm.checkcast(vt.vecClass);
                asm.invokeStatic(pool.nullCompareHelper(c.operandType(), c.opcode()));
            } else {
                asm.getstatic(pool.comparisonOp(c.opcode()));
                asm.aload(tempSlots[c.rhs()]);
                asm.checkcast(vt.vecClass);
                asm.invokeVirtual(vt.compare);
            }
            asm.getstatic(pool.vecType(I8_TYPE).speciesPreferred);
            asm.invokeVirtual(pool.maskCast);
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
        } else if (ctx.s().nullMaskCacheSlot() >= 0 && isNonNullImmediateTemp(ctx.block(), c.rhs())) {
            // I8 null-aware ordered comparison where rhs is a compile-time
            // constant (never LONG_NULL).  Compute the lhs null mask once,
            // cache it in nullMaskCacheSlot, and reuse for all subsequent
            // comparisons on the same column vector.
            //
            // Emits: lhs.compare(op, rhs).andNot(nullMask)
            //
            // This replaces the longNullGt/Lt/Ge/Le helpers which do
            // 5 mask ops per call (lhs==null, rhs==null, or, compare, and-not).
            // With caching: 1 null compare (first time) + 1 andNot per predicate.
            if (ctx.cachedNullMaskOwner()[0] != c.lhs()) {
                // Compute lhs null mask: lhs.compare(EQ, nullVec)
                asm.aload(tempSlots[c.lhs()]);
                asm.checkcast(vt.vecClass);
                asm.getstatic(pool.comparisonOp(EQ));
                asm.aload(ctx.s().nullVecSlot());
                asm.checkcast(vt.vecClass);
                asm.invokeVirtual(vt.compare);
                asm.astore(ctx.s().nullMaskCacheSlot());
                ctx.cachedNullMaskOwner()[0] = c.lhs();
            }
            // result = lhs.compare(op, rhs).andNot(nullMask)
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.getstatic(pool.comparisonOp(c.opcode()));
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.invokeVirtual(vt.compare);
            asm.aload(ctx.s().nullMaskCacheSlot());
            asm.checkcast(pool.vectorMaskClass);
            asm.invokeVirtual(pool.maskAndNot);
            asm.astore(tempSlots[c.dst()]);
        } else {
            // I8 null-aware ordered comparison — general case (both operands
            // may be null, or rhs is not an immediate).
            int helperMethod = pool.nullCompareHelper(c.operandType(), c.opcode());
            asm.aload(tempSlots[c.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[c.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(ctx.s().nullVecSlot());
            asm.invokeStatic(helperMethod);
            asm.astore(tempSlots[c.dst()]);
        }
    }

    /**
     * Returns true when the given temp was produced by a LoadImm with a value
     * that is NOT the LONG_NULL sentinel — i.e. a non-null compile-time constant.
     */
    private static boolean isNonNullImmediateTemp(LoweredBlock block, int tempId) {
        for (int i = 0; i < block.getOpCount(); i++) {
            if (block.getOp(i) instanceof LoweredOp.LoadImm li && li.dst() == tempId) {
                return li.lo() != io.questdb.std.Numbers.LONG_NULL;
            }
        }
        return false;
    }

    private static void emitBooleanOp(EmitContext ctx, LoweredOp.BooleanOp bo) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        int[] tempSlots = ctx.s().tempSlots();
        asm.aload(tempSlots[bo.lhs()]);
        asm.checkcast(pool.vectorMaskClass);
        asm.aload(tempSlots[bo.rhs()]);
        asm.checkcast(pool.vectorMaskClass);
        asm.invokeVirtual(bo.opcode() == AND ? pool.maskAnd : pool.maskOr);
        asm.astore(tempSlots[bo.dst()]);
    }

    private static void emitNot(EmitContext ctx, LoweredOp.Not n) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        int[] tempSlots = ctx.s().tempSlots();
        asm.aload(tempSlots[n.src()]);
        asm.checkcast(pool.vectorMaskClass);
        asm.invokeVirtual(pool.maskNot);
        asm.astore(tempSlots[n.dst()]);
    }

    private static void emitCompareI128(EmitContext ctx, LoweredOp.CompareI128 c128) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        SlotLayout s = ctx.s();
        int[] tempSlots = s.tempSlots();
        LoweredBlock block = ctx.block();

        // Look up source ops for lhs and rhs by scanning the block
        LoweredOp lhsOp = findOpByDst(block, c128.lhs());
        LoweredOp rhsOp = findOpByDst(block, c128.rhs());

        // Push species FIRST — it stays underneath while the i128 helper runs.
        // After the helper returns long, the stack is [VectorSpecies, long]
        // which matches VectorMask.fromLong(VectorSpecies, long).
        asm.aload(s.speciesSlot());

        if (lhsOp instanceof LoweredOp.LoadColumn lc && rhsOp instanceof LoweredOp.LoadImm li) {
            asm.lload(SLOT_DATA_ADDR);
            asm.iconst(lc.columnIndex());
            asm.lload(s.rowSlot());
            asm.aload(s.speciesSlot());
            asm.invokeInterface(pool.speciesLength, 0);
            asm.ldc2_w(pool.ensureLongPooled(li.lo()));
            asm.ldc2_w(pool.ensureLongPooled(li.hi()));
            asm.iconst(c128.opcode());
            asm.invokeStatic(pool.i128CompareColumnImm);
        } else if (lhsOp instanceof LoweredOp.LoadColumn lc && rhsOp instanceof LoweredOp.LoadVar lv) {
            asm.lload(SLOT_DATA_ADDR);
            asm.iconst(lc.columnIndex());
            asm.lload(s.rowSlot());
            asm.aload(s.speciesSlot());
            asm.invokeInterface(pool.speciesLength, 0);
            asm.lload(SLOT_VARS_ADDR);
            asm.ldc(pool.ensureIntPooled((int) lv.byteOffset()));
            asm.iconst(c128.opcode());
            asm.invokeStatic(pool.i128CompareColumnVar);
        } else if (lhsOp instanceof LoweredOp.LoadColumn lc1 && rhsOp instanceof LoweredOp.LoadColumn lc2) {
            asm.lload(SLOT_DATA_ADDR);
            asm.iconst(lc1.columnIndex());
            asm.iconst(lc2.columnIndex());
            asm.lload(s.rowSlot());
            asm.aload(s.speciesSlot());
            asm.invokeInterface(pool.speciesLength, 0);
            asm.iconst(c128.opcode());
            asm.invokeStatic(pool.i128CompareColumns);
        } else {
            throw new UnsupportedOperationException("I128 compare: unsupported operand combination");
        }
        // Stack: [VectorSpecies, long] → fromLong(VectorSpecies, long) → VectorMask
        asm.invokeStatic(pool.maskFromLong);
        asm.astore(tempSlots[c128.dst()]);
    }

    private static LoweredOp findOpByDst(LoweredBlock block, int tempId) {
        for (int i = 0; i < block.getOpCount(); i++) {
            LoweredOp op = block.getOp(i);
            if (op instanceof LoweredOp.LoadColumn lc && lc.dst() == tempId) return op;
            if (op instanceof LoweredOp.LoadImm li && li.dst() == tempId) return op;
            if (op instanceof LoweredOp.LoadVar lv && lv.dst() == tempId) return op;
        }
        throw new IllegalStateException("I128 source temp " + tempId + " not found");
    }

    private static void emitArithmetic(EmitContext ctx, LoweredOp.Arithmetic a) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        int[] tempSlots = ctx.s().tempSlots();
        Pool.VecType vt = pool.vecType(a.resultType());

        if (a.resultType() == F8_TYPE) {
            // F8: per-op helper for NaN propagation and div-by-zero → NaN
            asm.aload(tempSlots[a.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[a.rhs()]);
            asm.checkcast(vt.vecClass);
            int f8method = switch (a.opcode()) {
                case ADD -> pool.doubleVecAdd;
                case SUB -> pool.doubleVecSub;
                case MUL -> pool.doubleVecMul;
                case DIV -> pool.doubleVecDiv;
                default -> throw new UnsupportedOperationException("F8 arith op: " + a.opcode());
            };
            asm.invokeStatic(f8method);
            asm.astore(tempSlots[a.dst()]);
        } else if (a.resultType() == F4_TYPE) {
            // F4: per-op helper for NaN propagation and div-by-zero → NaN
            asm.aload(tempSlots[a.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[a.rhs()]);
            asm.checkcast(vt.vecClass);
            int f4method = switch (a.opcode()) {
                case ADD -> pool.floatVecAdd;
                case SUB -> pool.floatVecSub;
                case MUL -> pool.floatVecMul;
                case DIV -> pool.floatVecDiv;
                default -> throw new UnsupportedOperationException("F4 arith op: " + a.opcode());
            };
            asm.invokeStatic(f4method);
            asm.astore(tempSlots[a.dst()]);
        } else if (ctx.nullChecks() && a.resultType() == I8_TYPE) {
            // I8 with null checks: per-op helper for LONG_NULL preservation
            asm.aload(tempSlots[a.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[a.rhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(ctx.s().nullVecSlot());
            int i8method = switch (a.opcode()) {
                case ADD -> pool.longVecAddNull;
                case SUB -> pool.longVecSubNull;
                case MUL -> pool.longVecMulNull;
                case DIV -> pool.longVecDivNull;
                default -> throw new UnsupportedOperationException("I8 arith op: " + a.opcode());
            };
            asm.invokeStatic(i8method);
            asm.astore(tempSlots[a.dst()]);
        } else if (ctx.nullChecks() && a.resultType() == I4_TYPE) {
            // I4 with null checks: per-op helper for INT_NULL preservation
            asm.aload(tempSlots[a.lhs()]);
            asm.checkcast(vt.vecClass);
            asm.aload(tempSlots[a.rhs()]);
            asm.checkcast(vt.vecClass);
            int i4method = switch (a.opcode()) {
                case ADD -> pool.intVecAddNull;
                case SUB -> pool.intVecSubNull;
                case MUL -> pool.intVecMulNull;
                case DIV -> pool.intVecDivNull;
                default -> throw new UnsupportedOperationException("I4 arith op: " + a.opcode());
            };
            asm.invokeStatic(i4method);
            asm.astore(tempSlots[a.dst()]);
        } else {
            // I8/I4 without null checks: raw vector arithmetic
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

    private static void emitNegate(EmitContext ctx, LoweredOp.Negate neg) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        int[] tempSlots = ctx.s().tempSlots();
        Pool.VecType vt = pool.vecType(neg.type());
        asm.aload(tempSlots[neg.src()]);
        asm.checkcast(vt.vecClass);
        asm.invokeVirtual(vt.neg);
        asm.astore(tempSlots[neg.dst()]);
    }

    private static void emitCast(EmitContext ctx, LoweredOp.Cast c) {
        BytecodeAssembler asm = ctx.asm();
        Pool pool = ctx.pool();
        int[] tempSlots = ctx.s().tempSlots();
        // Same-width cast: I8<->F8 via L2D/D2L.
        Pool.VecType srcVt = pool.vecType(c.fromType());
        asm.aload(tempSlots[c.src()]);
        asm.checkcast(srcVt.vecClass);

        if (ctx.nullChecks() && c.fromType() == I8_TYPE && c.toType() == F8_TYPE) {
            // Null-aware I8→F8: LONG_NULL must become NaN, not -9.22E18.
            asm.aload(ctx.s().nullVecSlot());
            asm.checkcast(pool.vecType(I8_TYPE).vecClass);
            asm.invokeStatic(pool.longToDoubleNullAware);
        } else if (ctx.nullChecks() && c.fromType() == I4_TYPE && c.toType() == F8_TYPE) {
            // Null-aware I4→F8: INT_NULL must become NaN, not -2.14e9.
            emitTypeSpecies(ctx, F8_TYPE);
            asm.invokeStatic(pool.intToDoubleNullAware);
        } else if (ctx.nullChecks() && c.fromType() == I4_TYPE && c.toType() == F4_TYPE) {
            // Null-aware I4→F4: INT_NULL must become NaN, not -2.14e9.
            emitTypeSpecies(ctx, F4_TYPE);
            asm.invokeStatic(pool.intToFloatNullAware);
        } else if (ctx.nullChecks() && c.fromType() == I4_TYPE && c.toType() == I8_TYPE) {
            // Null-aware I4→I8: INT_NULL sign-extends to 0xFFFFFFFF80000000,
            // not LONG_NULL. Detect and replace with proper LONG_NULL.
            asm.aload(ctx.s().nullVecSlot());
            asm.invokeStatic(pool.intToLongNullAware);
        } else {
            asm.getstatic(pool.conversionOp(c.fromType(), c.toType()));
            // For I4/F4 targets, use the narrowed species (matching Long lane count)
            // instead of SPECIES_PREFERRED which has more lanes on wide hardware.
            emitTypeSpecies(ctx, c.toType());
            asm.iconst(0);
            asm.invokeVirtual(srcVt.convertShape);
        }
        asm.checkcast(pool.vecType(c.toType()).vecClass);
        asm.astore(tempSlots[c.dst()]);
    }

    // === Debug: decompile generated class ===

    /**
     * Decompiles the generated class bytes and prints the result to stderr.
     * Uses Vineflower/Fernflower via reflection so there is no compile-time
     * dependency — the decompiler JAR only needs to be on the classpath at
     * runtime (e.g. test scope).  Falls back to javap if unavailable.
     * <p>
     * Enable with {@code -Dquestdb.jit.vector.decompile=true}.
     */
    private static void decompileToStderr(BytecodeAssembler asm) {
        byte[] classBytes = asm.toByteArray();
        try {
            // Try Vineflower/Fernflower first (produces readable Java source)
            Class.forName("org.jetbrains.java.decompiler.api.Decompiler");
            decompileWithVineflower(classBytes);
        } catch (ClassNotFoundException e) {
            // Vineflower not on classpath — fall back to javap
            decompileWithJavap(classBytes);
        } catch (Exception e) {
            System.err.println("Decompilation failed: " + e.getMessage());
            decompileWithJavap(classBytes);
        }
    }

    private static void decompileWithVineflower(byte[] classBytes) throws Exception {
        // Vineflower 1.11+ uses a builder API:
        //   Decompiler.builder()
        //       .inputs(File...)
        //       .output(IResultSaver)
        //       .build()
        //       .decompile()
        //
        // All calls use reflection to avoid a compile-time dependency.
        var result = new Object() {
            String source;
        };

        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        // IResultSaver proxy — captures the decompiled source text
        Class<?> saverIface = cl.loadClass("org.jetbrains.java.decompiler.main.extern.IResultSaver");
        Object saverProxy = java.lang.reflect.Proxy.newProxyInstance(cl, new Class<?>[]{saverIface},
                (proxy, method, args) -> {
                    if ("saveClassFile".equals(method.getName())) {
                        // args: String path, String qualifiedName, String entryName, String content, int[] mapping
                        result.source = (String) args[3];
                    }
                    return null;
                });

        // Write class bytes to a temp file (Vineflower reads from File)
        java.io.File tmp = java.io.File.createTempFile("vgen", ".class");
        try {
            try (var fos = new java.io.FileOutputStream(tmp)) {
                fos.write(classBytes);
            }

            Class<?> decompilerClass = cl.loadClass("org.jetbrains.java.decompiler.api.Decompiler");
            Class<?> builderClass = cl.loadClass("org.jetbrains.java.decompiler.api.Decompiler$Builder");

            // Decompiler.builder()
            Object builder = decompilerClass.getMethod("builder").invoke(null);

            // .inputs(File...)
            builderClass.getMethod("inputs", java.io.File[].class)
                    .invoke(builder, (Object) new java.io.File[]{tmp});

            // .output(IResultSaver)
            builderClass.getMethod("output", saverIface).invoke(builder, saverProxy);

            // .build()
            Object decompiler = builderClass.getMethod("build").invoke(builder);

            // .decompile()
            decompilerClass.getMethod("decompile").invoke(decompiler);
        } finally {
            tmp.delete();
        }

        if (result.source != null) {
            System.err.println("=== Decompiled vector filter (Vineflower) ===");
            System.err.println(result.source);
        } else {
            System.err.println("Vineflower produced no output, falling back to javap");
            decompileWithJavap(classBytes);
        }
    }

    private static void decompileWithJavap(byte[] classBytes) {
        try {
            java.io.File tmp = java.io.File.createTempFile("vgen", ".class");
            try {
                try (var fos = new java.io.FileOutputStream(tmp)) {
                    fos.write(classBytes);
                }
                ProcessBuilder pb = new ProcessBuilder("javap", "-c", "-p", tmp.getAbsolutePath());
                pb.redirectErrorStream(true);
                Process p = pb.start();
                System.err.println("=== Disassembled vector filter (javap) ===");
                p.getInputStream().transferTo(System.err);
                p.waitFor();
            } finally {
                tmp.delete();
            }
        } catch (Exception e) {
            System.err.println("javap fallback failed: " + e.getMessage());
        }
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
        final int helpersIntSpeciesForLongRows;
        final int helpersFloatSpeciesForLongRows;
        final int helpersByteSpeciesForLongRows;
        final int helpersShortSpeciesForLongRows;
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
        final int memSegGetByte;
        final int memSegGetShort;
        final int memSegGetLong;
        final int memSegGetInt;
        final int memSegGetFloat;
        final int memSegGetDouble;

        // ValueLayout fields
        final int javaByteUnaligned;
        final int javaShortUnaligned;
        final int javaLongUnaligned;
        final int javaIntUnaligned;
        final int javaFloatUnaligned;
        final int javaDoubleUnaligned;

        // VectorOperators comparison fields
        private final int opEQ, opNE, opLT, opLE, opGT, opGE;

        // VectorOperators conversion fields
        private final int convI2F, convF2I, convL2D, convD2L, convF2D, convD2F, convI2L, convL2I,
                convB2I, convS2I, convI2D;

        // Null-aware comparison helpers (I8 and I4)
        private final int longNullLt, longNullLe, longNullGt, longNullGe;
        private final int intNullEq, intNullNe, intNullLt, intNullLe, intNullGt, intNullGe;
        // Double comparison helpers (epsilon + NaN)
        private final int doubleVecEq, doubleVecNe, doubleVecLt, doubleVecLe, doubleVecGt, doubleVecGe;
        // Float comparison helpers (epsilon + NaN)
        private final int floatVecEq, floatVecNe, floatVecLt, floatVecLe, floatVecGt, floatVecGe;
        // Arithmetic helpers
        // Per-operation arithmetic helpers (no runtime opcode switch)
        final int longVecAddNull, longVecSubNull, longVecMulNull, longVecDivNull;
        final int intVecAddNull, intVecSubNull, intVecMulNull, intVecDivNull;
        final int doubleVecAdd, doubleVecSub, doubleVecMul, doubleVecDiv;
        final int floatVecAdd, floatVecSub, floatVecMul, floatVecDiv;
        // Null-aware cast helpers
        final int longToDoubleNullAware;
        final int intToDoubleNullAware;
        final int intToLongNullAware;
        final int intToFloatNullAware;
        // Var-size header gather helpers
        final int gatherStringHeaders;
        final int gatherBinaryHeaders;
        final int gatherVarcharHeaders;
        // I128 (UUID) comparison helpers
        final int i128CompareColumnImm;
        final int i128CompareColumnVar;
        final int i128CompareColumns;
        // VectorMask.fromLong (for I128 bitmask → mask conversion)
        final int maskFromLong;

        // LongVector-specific (always needed for row-ID output)
        final int longVecAddScalar;
        final int longVecAddScalarMasked;
        final int longVecCompress;
        final int longVecReduceLanesToLong;
        final int writeCompressedRows;

        // VectorMask methods
        final int maskAnd;
        final int maskAndNot;
        final int maskOr;
        final int maskNot;
        final int maskTrueCount;
        final int maskCast;
        final int associativeAdd;

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
            int byteVecCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/ByteVector"));
            int shortVecCls = asm.poolClass(asm.poolUtf8("jdk/incubator/vector/ShortVector"));
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
            byteOrderClass = asm.poolClass(asm.poolUtf8("java/nio/ByteOrder"));
            int valueLayoutCls = asm.poolClass(asm.poolUtf8("java/lang/foreign/ValueLayout"));
            int helpersCls = asm.poolClass(asm.poolUtf8("io/questdb/jit/FilterHelpers"));
            objectClassIndex = asm.poolClass(asm.poolUtf8("java/lang/Object"));

            // Signature fragments
            String sMask = "Ljdk/incubator/vector/VectorMask;";
            String sSpec = "Ljdk/incubator/vector/VectorSpecies;";
            String sMSeg = "Ljava/lang/foreign/MemorySegment;";
            String sBO = "Ljava/nio/ByteOrder;";
            String sVec = "Ljdk/incubator/vector/Vector;";
            String sConv = "Ljdk/incubator/vector/VectorOperators$Conversion;";
            String sAssoc = "Ljdk/incubator/vector/VectorOperators$Associative;";

            // --- FilterHelpers: setup ---
            helpersLongSpecies = asm.poolMethod(helpersCls, "longSpecies", "()" + sSpec);
            helpersDoubleSpecies = asm.poolMethod(helpersCls, "doubleSpecies", "()" + sSpec);
            helpersIntSpeciesForLongRows = asm.poolMethod(helpersCls, "intSpeciesForLongRows", "()" + sSpec);
            helpersFloatSpeciesForLongRows = asm.poolMethod(helpersCls, "floatSpeciesForLongRows", "()" + sSpec);
            helpersByteSpeciesForLongRows = asm.poolMethod(helpersCls, "byteSpeciesForLongRows", "()" + sSpec);
            helpersShortSpeciesForLongRows = asm.poolMethod(helpersCls, "shortSpeciesForLongRows", "()" + sSpec);
            helpersNativeByteOrder = asm.poolMethod(helpersCls, "nativeByteOrder", "()Ljava/nio/ByteOrder;");
            helpersColumnSegment = asm.poolMethod(helpersCls, "columnSegment", "(JI)" + sMSeg);
            helpersSegment = asm.poolMethod(helpersCls, "segment", "(J)" + sMSeg);
            helpersIotaVector = asm.poolMethod(helpersCls, "iotaVector",
                    "(" + sSpec + ")Ljdk/incubator/vector/LongVector;");
            helpersLongNullVector = asm.poolMethod(helpersCls, "longNullVector",
                    "(" + sSpec + ")Ljdk/incubator/vector/LongVector;");
            helpersDoubleNanVector = asm.poolMethod(helpersCls, "doubleNanVector",
                    "(" + sSpec + ")Ljdk/incubator/vector/DoubleVector;");

            // --- FilterHelpers: null-aware comparison ---
            String longNullSig = "(Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;)" + sMask;
            longNullLt = asm.poolMethod(helpersCls, "longNullLt", longNullSig);
            longNullLe = asm.poolMethod(helpersCls, "longNullLe", longNullSig);
            longNullGt = asm.poolMethod(helpersCls, "longNullGt", longNullSig);
            longNullGe = asm.poolMethod(helpersCls, "longNullGe", longNullSig);
            String intNullSig = "(Ljdk/incubator/vector/IntVector;Ljdk/incubator/vector/IntVector;)" + sMask;
            intNullEq = asm.poolMethod(helpersCls, "intVecNullEq", intNullSig);
            intNullNe = asm.poolMethod(helpersCls, "intVecNullNe", intNullSig);
            intNullLt = asm.poolMethod(helpersCls, "intVecNullLt", intNullSig);
            intNullLe = asm.poolMethod(helpersCls, "intVecNullLe", intNullSig);
            intNullGt = asm.poolMethod(helpersCls, "intVecNullGt", intNullSig);
            intNullGe = asm.poolMethod(helpersCls, "intVecNullGe", intNullSig);

            // --- FilterHelpers: double comparison (epsilon + NaN) ---
            String dblCmpSig = "(Ljdk/incubator/vector/DoubleVector;Ljdk/incubator/vector/DoubleVector;)" + sMask;
            doubleVecEq = asm.poolMethod(helpersCls, "doubleVecEq", dblCmpSig);
            doubleVecNe = asm.poolMethod(helpersCls, "doubleVecNe", dblCmpSig);
            doubleVecLt = asm.poolMethod(helpersCls, "doubleVecLt", dblCmpSig);
            doubleVecLe = asm.poolMethod(helpersCls, "doubleVecLe", dblCmpSig);
            doubleVecGt = asm.poolMethod(helpersCls, "doubleVecGt", dblCmpSig);
            doubleVecGe = asm.poolMethod(helpersCls, "doubleVecGe", dblCmpSig);

            // --- FilterHelpers: float comparison (epsilon + NaN) ---
            String fltCmpSig = "(Ljdk/incubator/vector/FloatVector;Ljdk/incubator/vector/FloatVector;)" + sMask;
            floatVecEq = asm.poolMethod(helpersCls, "floatVecEq", fltCmpSig);
            floatVecNe = asm.poolMethod(helpersCls, "floatVecNe", fltCmpSig);
            floatVecLt = asm.poolMethod(helpersCls, "floatVecLt", fltCmpSig);
            floatVecLe = asm.poolMethod(helpersCls, "floatVecLe", fltCmpSig);
            floatVecGt = asm.poolMethod(helpersCls, "floatVecGt", fltCmpSig);
            floatVecGe = asm.poolMethod(helpersCls, "floatVecGe", fltCmpSig);

            // --- FilterHelpers: per-operation arithmetic helpers ---
            String sLV = "Ljdk/incubator/vector/LongVector;";
            String sIV = "Ljdk/incubator/vector/IntVector;";
            String sDV = "Ljdk/incubator/vector/DoubleVector;";
            String sFV = "Ljdk/incubator/vector/FloatVector;";
            longVecAddNull = asm.poolMethod(helpersCls, "longVecAddNull", "(" + sLV + sLV + sLV + ")" + sLV);
            longVecSubNull = asm.poolMethod(helpersCls, "longVecSubNull", "(" + sLV + sLV + sLV + ")" + sLV);
            longVecMulNull = asm.poolMethod(helpersCls, "longVecMulNull", "(" + sLV + sLV + sLV + ")" + sLV);
            longVecDivNull = asm.poolMethod(helpersCls, "longVecDivNull", "(" + sLV + sLV + sLV + ")" + sLV);
            intVecAddNull = asm.poolMethod(helpersCls, "intVecAddNull", "(" + sIV + sIV + ")" + sIV);
            intVecSubNull = asm.poolMethod(helpersCls, "intVecSubNull", "(" + sIV + sIV + ")" + sIV);
            intVecMulNull = asm.poolMethod(helpersCls, "intVecMulNull", "(" + sIV + sIV + ")" + sIV);
            intVecDivNull = asm.poolMethod(helpersCls, "intVecDivNull", "(" + sIV + sIV + ")" + sIV);
            doubleVecAdd = asm.poolMethod(helpersCls, "doubleVecAdd", "(" + sDV + sDV + ")" + sDV);
            doubleVecSub = asm.poolMethod(helpersCls, "doubleVecSub", "(" + sDV + sDV + ")" + sDV);
            doubleVecMul = asm.poolMethod(helpersCls, "doubleVecMul", "(" + sDV + sDV + ")" + sDV);
            doubleVecDiv = asm.poolMethod(helpersCls, "doubleVecDiv", "(" + sDV + sDV + ")" + sDV);
            floatVecAdd = asm.poolMethod(helpersCls, "floatVecAdd", "(" + sFV + sFV + ")" + sFV);
            floatVecSub = asm.poolMethod(helpersCls, "floatVecSub", "(" + sFV + sFV + ")" + sFV);
            floatVecMul = asm.poolMethod(helpersCls, "floatVecMul", "(" + sFV + sFV + ")" + sFV);
            floatVecDiv = asm.poolMethod(helpersCls, "floatVecDiv", "(" + sFV + sFV + ")" + sFV);
            longToDoubleNullAware = asm.poolMethod(helpersCls, "longToDoubleNullAware",
                    "(Ljdk/incubator/vector/LongVector;Ljdk/incubator/vector/LongVector;)Ljdk/incubator/vector/DoubleVector;");
            intToDoubleNullAware = asm.poolMethod(helpersCls, "intToDoubleNullAware",
                    "(Ljdk/incubator/vector/IntVector;" + sSpec + ")Ljdk/incubator/vector/DoubleVector;");
            intToLongNullAware = asm.poolMethod(helpersCls, "intToLongNullAware",
                    "(Ljdk/incubator/vector/IntVector;Ljdk/incubator/vector/LongVector;)Ljdk/incubator/vector/LongVector;");
            intToFloatNullAware = asm.poolMethod(helpersCls, "intToFloatNullAware",
                    "(Ljdk/incubator/vector/IntVector;" + sSpec + ")Ljdk/incubator/vector/FloatVector;");

            // --- FilterHelpers: var-size header gathers ---
            gatherStringHeaders = asm.poolMethod(helpersCls, "gatherStringHeaders",
                    "(JJI" + "J" + sSpec + ")Ljdk/incubator/vector/IntVector;");
            gatherBinaryHeaders = asm.poolMethod(helpersCls, "gatherBinaryHeaders",
                    "(JJI" + "J" + sSpec + ")Ljdk/incubator/vector/LongVector;");
            gatherVarcharHeaders = asm.poolMethod(helpersCls, "gatherVarcharHeaders",
                    "(JI" + "J" + sSpec + ")Ljdk/incubator/vector/LongVector;");

            // --- FilterHelpers: I128 (UUID) comparisons ---
            i128CompareColumnImm = asm.poolMethod(helpersCls, "i128CompareColumnImm",
                    "(JIJIJJI)J");
            i128CompareColumnVar = asm.poolMethod(helpersCls, "i128CompareColumnVar",
                    "(JIJIJII)J");
            i128CompareColumns = asm.poolMethod(helpersCls, "i128CompareColumns",
                    "(JIIJII)J");

            // --- VectorMask.fromLong (static method) ---
            maskFromLong = asm.poolMethod(vecMaskCls, "fromLong", "(" + sSpec + "J)" + sMask);

            // --- VectorSpecies (interface) ---
            speciesIndexInRange = asm.poolInterfaceMethod(vecSpeciesCls, "indexInRange", "(JJ)" + sMask);
            speciesLength = asm.poolInterfaceMethod(vecSpeciesCls, "length", "()I");

            // --- MemorySegment (interface) ---
            memSegGetByte = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfByte;J)B");
            memSegGetShort = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfShort;J)S");
            memSegGetLong = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfLong;J)J");
            memSegGetInt = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfInt;J)I");
            memSegGetFloat = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfFloat;J)F");
            memSegGetDouble = asm.poolInterfaceMethod(memSegCls, "get",
                    "(Ljava/lang/foreign/ValueLayout$OfDouble;J)D");

            // --- ValueLayout fields ---
            javaByteUnaligned = poolStaticField(asm, valueLayoutCls, "JAVA_BYTE",
                    "Ljava/lang/foreign/ValueLayout$OfByte;");
            javaShortUnaligned = poolStaticField(asm, valueLayoutCls, "JAVA_SHORT_UNALIGNED",
                    "Ljava/lang/foreign/ValueLayout$OfShort;");
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
            associativeAdd = poolStaticField(asm, vecOpsCls, "ADD", sAssoc);

            // --- Conversion ops (for Cast) ---
            String convType = "Ljdk/incubator/vector/VectorOperators$Conversion;";
            convI2F = poolStaticField(asm, vecOpsCls, "I2F", convType);
            convF2I = poolStaticField(asm, vecOpsCls, "F2I", convType);
            convL2D = poolStaticField(asm, vecOpsCls, "L2D", convType);
            convD2L = poolStaticField(asm, vecOpsCls, "D2L", convType);
            convF2D = poolStaticField(asm, vecOpsCls, "F2D", convType);
            convD2F = poolStaticField(asm, vecOpsCls, "D2F", convType);
            convI2L = poolStaticField(asm, vecOpsCls, "I2L", convType);
            convL2I = poolStaticField(asm, vecOpsCls, "L2I", convType);
            convB2I = poolStaticField(asm, vecOpsCls, "B2I", convType);
            convS2I = poolStaticField(asm, vecOpsCls, "S2I", convType);
            convI2D = poolStaticField(asm, vecOpsCls, "I2D", convType);

            // --- Per-type VecType pools ---
            String sLVec = "Ljdk/incubator/vector/LongVector;";
            String sIVec = "Ljdk/incubator/vector/IntVector;";
            String sFVec = "Ljdk/incubator/vector/FloatVector;";
            String sDVec = "Ljdk/incubator/vector/DoubleVector;";

            String sBVec = "Ljdk/incubator/vector/ByteVector;";
            String sSVec = "Ljdk/incubator/vector/ShortVector;";

            vecTypes[I1_TYPE] = poolVecType(asm, byteVecCls, sBVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "B");
            vecTypes[I2_TYPE] = poolVecType(asm, shortVecCls, sSVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "S");
            vecTypes[I8_TYPE] = poolVecType(asm, longVecCls, sLVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "J");
            vecTypes[I4_TYPE] = poolVecType(asm, intVecCls, sIVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "I");
            vecTypes[F4_TYPE] = poolVecType(asm, floatVecCls, sFVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "F");
            vecTypes[F8_TYPE] = poolVecType(asm, doubleVecCls, sDVec, sSpec, sMSeg, sBO, sMask, sVec, sConv, "D");

            // --- LongVector-specific (for row-ID output) ---
            longVecAddScalar = asm.poolMethod(longVecCls, "add", "(J)" + sLVec);
            longVecAddScalarMasked = asm.poolMethod(longVecCls, "add", "(J" + sMask + ")" + sLVec);
            longVecCompress = asm.poolMethod(longVecCls, "compress", "(" + sMask + ")" + sLVec);
            longVecReduceLanesToLong = asm.poolMethod(longVecCls, "reduceLanesToLong", "(" + sAssoc + ")J");
            writeCompressedRows = asm.poolMethod(asm.poolClass(FilterHelpers.class),
                    "writeCompressedRows",
                    "(" + sLVec + "I" + sMSeg + "J" + sBO + ")V");

            // --- VectorMask methods ---
            maskAnd = asm.poolMethod(vecMaskCls, "and", "(" + sMask + ")" + sMask);
            maskAndNot = asm.poolMethod(vecMaskCls, "andNot", "(" + sMask + ")" + sMask);
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

        int floatCompareHelper(int opcode) {
            return switch (opcode) {
                case EQ -> floatVecEq;
                case NE -> floatVecNe;
                case LT -> floatVecLt;
                case LE -> floatVecLe;
                case GT -> floatVecGt;
                case GE -> floatVecGe;
                default -> throw new UnsupportedOperationException("float cmp: " + opcode);
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
            if (operandType == I4_TYPE) {
                return switch (opcode) {
                    case EQ -> intNullEq;
                    case NE -> intNullNe;
                    case LT -> intNullLt;
                    case LE -> intNullLe;
                    case GT -> intNullGt;
                    case GE -> intNullGe;
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
            if (fromType == F4_TYPE && toType == F8_TYPE) return convF2D;
            if (fromType == F8_TYPE && toType == F4_TYPE) return convD2F;
            if (fromType == I4_TYPE && toType == I8_TYPE) return convI2L;
            if (fromType == I8_TYPE && toType == I4_TYPE) return convL2I;
            if (fromType == I1_TYPE && toType == I4_TYPE) return convB2I;
            if (fromType == I2_TYPE && toType == I4_TYPE) return convS2I;
            if (fromType == I4_TYPE && toType == F8_TYPE) return convI2D;
            throw new UnsupportedOperationException("conversion: " + fromType + " -> " + toType);
        }

        int headerGatherMethod(int headerType) {
            return switch (headerType) {
                case STRING_HEADER_TYPE -> gatherStringHeaders;
                case BINARY_HEADER_TYPE -> gatherBinaryHeaders;
                case VARCHAR_HEADER_TYPE -> gatherVarcharHeaders;
                default -> throw new UnsupportedOperationException("header gather: " + headerType);
            };
        }

        int varLayout(int type) {
            return switch (type) {
                case I1_TYPE -> javaByteUnaligned;
                case I2_TYPE -> javaShortUnaligned;
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

        int ensureIntPooled(int value) {
            Integer idx = pooledInts.get(value);
            if (idx == null) throw new IllegalStateException("Int " + value + " not pre-pooled");
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

        private void doPoolInt(BytecodeAssembler asm, int value) {
            pooledInts.computeIfAbsent(value, v -> asm.poolIntConst(v));
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
                            case I4_TYPE -> doPoolInt(asm, (int) li.lo());
                            case I8_TYPE -> doPoolLong(asm, li.lo());
                            case F8_TYPE -> doPoolDouble(asm, Double.longBitsToDouble(li.lo()));
                            case F4_TYPE -> doPoolFloat(asm, Float.intBitsToFloat((int) li.lo()));
                            case I16_TYPE -> {
                                doPoolLong(asm, li.lo());
                                doPoolLong(asm, li.hi());
                            }
                        }
                    }
                    if (op instanceof LoweredOp.LoadVar lv) {
                        doPoolLong(asm, lv.byteOffset());
                        if (lv.type() == I16_TYPE) {
                            doPoolInt(asm, (int) lv.byteOffset());
                        }
                    }
                    if (op instanceof LoweredOp.LoadColumn lc && lc.type() != I16_TYPE) {
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
            // fromMemorySegment(species, seg, offset, order) -> XxxVector
            int fromMemSeg = asm.poolMethod(vecCls, "fromMemorySegment",
                    "(" + sSpec + sMSeg + "J" + sBO + ")" + sVecType);
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

            return new VecType(vecCls, fromMemSeg, fromMemSegMasked, broadcast, compare,
                    add, sub, mul, div, neg, convertShape, speciesPreferred);
        }

        record VecType(
                int vecClass,
                int fromMemSeg,
                int fromMemSegMasked,
                int broadcast,
                int compare,
                int add, int sub, int mul, int div, int neg,
                int convertShape,
                int speciesPreferred
        ) {}
    }
}
