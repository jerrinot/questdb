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

import io.questdb.jit.DecodedOptions;
import io.questdb.jit.IrDecoder;
import io.questdb.jit.IrLowering;
import io.questdb.jit.LoweredBlock;
import io.questdb.jit.LoweredOp;
import io.questdb.jit.LoweredProgram;
import io.questdb.jit.Terminator;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.jit.CompiledFilterIRSerializer.*;

public class IrLoweringTest {

    // Option constants
    private static final int NULL_CHECKS = 1 << 6;
    private static final int SCALAR_I4 = (2 << 1) | (0 << 4); // log2(4)=2, scalar hint
    private static final int SINGLE_SIZE_I4 = (2 << 1) | (1 << 4); // log2(4)=2, single-size hint
    private static final int SINGLE_SIZE_I8 = (3 << 1) | (1 << 4); // log2(8)=3, single-size hint

    // ========================
    // DecodedOptions tests
    // ========================

    @Test
    public void testDecodedOptionsDebugFlag() {
        DecodedOptions opts = DecodedOptions.decode(1);
        Assert.assertTrue(opts.isDebug());
        Assert.assertFalse(opts.isNullChecksEnabled());
    }

    @Test
    public void testDecodedOptionsMaxColumnTypeSize() {
        // log2(4) = 2, stored in bits 1-3
        DecodedOptions opts = DecodedOptions.decode(2 << 1);
        Assert.assertEquals(2, opts.maxColumnTypeSizeLog2());
        Assert.assertEquals(4, opts.maxColumnTypeSize());
    }

    @Test
    public void testDecodedOptionsNullChecks() {
        DecodedOptions opts = DecodedOptions.decode(NULL_CHECKS);
        Assert.assertTrue(opts.isNullChecksEnabled());
    }

    @Test
    public void testDecodedOptionsSingleSizeHint() {
        DecodedOptions opts = DecodedOptions.decode(1 << 4);
        Assert.assertTrue(opts.isSingleSize());
        Assert.assertFalse(opts.isScalarOnly());
    }

    @Test
    public void testDecodedOptionsScalarHint() {
        DecodedOptions opts = DecodedOptions.decode(0);
        Assert.assertTrue(opts.isScalarOnly());
    }

    @Test
    public void testDecodedOptionsCombined() {
        // debug + log2(8)=3 + single-size + null_checks
        int options = 1 | (3 << 1) | (1 << 4) | (1 << 6);
        DecodedOptions opts = DecodedOptions.decode(options);
        Assert.assertTrue(opts.isDebug());
        Assert.assertEquals(3, opts.maxColumnTypeSizeLog2());
        Assert.assertEquals(8, opts.maxColumnTypeSize());
        Assert.assertTrue(opts.isSingleSize());
        Assert.assertTrue(opts.isNullChecksEnabled());
    }

    // ========================
    // Simple lowering tests
    // ========================

    @Test
    public void testLowerSimpleIntComparison() throws Exception {
        // IR: IMM I4 42, MEM I4 col0, GT, RET
        // Meaning: col0 > 42
        IrDecoder.Instruction[] ir = {
                insn(IMM, I4_TYPE, 42, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);

        Assert.assertEquals(1, prog.getBlockCount());
        Assert.assertFalse(prog.hasControlFlow());

        LoweredBlock block = prog.getBlock(0);
        Assert.assertEquals(3, block.getOpCount()); // LoadImm, LoadColumn, Compare

        assertOp(block, 0, LoweredOp.LoadImm.class);
        assertOp(block, 1, LoweredOp.LoadColumn.class);

        LoweredOp.LoadImm imm = (LoweredOp.LoadImm) block.getOp(0);
        Assert.assertEquals(I4_TYPE, imm.type());
        Assert.assertEquals(42, imm.lo());

        LoweredOp.LoadColumn mem = (LoweredOp.LoadColumn) block.getOp(1);
        Assert.assertEquals(0, mem.columnIndex());
        Assert.assertEquals(I4_TYPE, mem.type());

        LoweredOp.Compare cmp = (LoweredOp.Compare) block.getOp(2);
        Assert.assertEquals(GT, cmp.opcode());
        Assert.assertEquals(I4_TYPE, cmp.operandType());
        // lhs = MEM (temp 1), rhs = IMM (temp 0)
        Assert.assertEquals(1, cmp.lhs());
        Assert.assertEquals(0, cmp.rhs());

        assertReturnTerminator(block);
    }

    @Test
    public void testLowerMixedTypeComparison() throws Exception {
        // IR: IMM I8 100, MEM I4 col0, EQ, RET
        // I4 must be promoted to I8 for comparison
        IrDecoder.Instruction[] ir = {
                insn(IMM, I8_TYPE, 100, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I8);

        LoweredBlock block = prog.getBlock(0);
        // LoadImm(I8), LoadColumn(I4), Cast(I4->I8), Compare(I8)
        Assert.assertEquals(4, block.getOpCount());

        LoweredOp.Cast cast = (LoweredOp.Cast) block.getOp(2);
        Assert.assertEquals(I4_TYPE, cast.fromType());
        Assert.assertEquals(I8_TYPE, cast.toType());

        LoweredOp.Compare cmp = (LoweredOp.Compare) block.getOp(3);
        Assert.assertEquals(I8_TYPE, cmp.operandType());
    }

    @Test
    public void testLowerIntFloatPromotesToDouble() throws Exception {
        // IR: IMM I8 100, MEM F4 col0, LT, RET
        // I8 mixed with F4 -> both promote to F8
        IrDecoder.Instruction[] ir = {
                insn(IMM, I8_TYPE, 100, 0),
                memF4(0),
                insn(LT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);

        LoweredBlock block = prog.getBlock(0);

        // Find the casts
        int castCount = 0;
        for (int i = 0; i < block.getOpCount(); i++) {
            if (block.getOp(i) instanceof LoweredOp.Cast) {
                castCount++;
            }
        }
        Assert.assertEquals(2, castCount);

        LoweredOp lastOp = block.getOp(block.getOpCount() - 1);
        Assert.assertTrue(lastOp instanceof LoweredOp.Compare);
        Assert.assertEquals(F8_TYPE, ((LoweredOp.Compare) lastOp).operandType());
    }

    @Test
    public void testLowerArithmetic() throws Exception {
        // IR: IMM I4 5, MEM I4 col0, MEM I4 col1, ADD, GT, RET
        // Meaning: (col0 + col1) > 5 — but remember RPN order
        // Stack trace: push 5, push col0, push col1
        // ADD: lhs=col1, rhs=col0, result=col0+col1
        // GT: lhs=result, rhs=5
        IrDecoder.Instruction[] ir = {
                insn(IMM, I4_TYPE, 5, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 1, 0),
                insn(ADD, 0, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        // LoadImm, LoadColumn, LoadColumn, Arithmetic, Compare
        Assert.assertEquals(5, block.getOpCount());

        LoweredOp.Arithmetic arith = (LoweredOp.Arithmetic) block.getOp(3);
        Assert.assertEquals(ADD, arith.opcode());
        Assert.assertEquals(I4_TYPE, arith.resultType());

        LoweredOp.Compare cmp = (LoweredOp.Compare) block.getOp(4);
        Assert.assertEquals(GT, cmp.opcode());
    }

    @Test
    public void testLowerMixedTypeArithmetic() throws Exception {
        // IMM F8 1.5, MEM I4 col0, ADD, RET
        // I4 + F8 -> F8 (I4 must be promoted)
        IrDecoder.Instruction[] ir = {
                immF8(1.5),
                insn(MEM, I4_TYPE, 0, 0),
                insn(ADD, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        LoweredBlock block = prog.getBlock(0);

        // LoadImm(F8), LoadColumn(I4), Cast(I4->F8), Arithmetic(F8)
        Assert.assertEquals(4, block.getOpCount());

        assertOp(block, 2, LoweredOp.Cast.class);
        LoweredOp.Cast cast = (LoweredOp.Cast) block.getOp(2);
        Assert.assertEquals(I4_TYPE, cast.fromType());
        Assert.assertEquals(F8_TYPE, cast.toType());

        LoweredOp.Arithmetic arith = (LoweredOp.Arithmetic) block.getOp(3);
        Assert.assertEquals(F8_TYPE, arith.resultType());
    }

    @Test
    public void testLowerNeg() throws Exception {
        // MEM I4 col0, NEG, RET
        IrDecoder.Instruction[] ir = {
                insn(MEM, I4_TYPE, 0, 0),
                insn(NEG, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        Assert.assertEquals(2, block.getOpCount());
        LoweredOp.Negate neg = (LoweredOp.Negate) block.getOp(1);
        Assert.assertEquals(I4_TYPE, neg.type());
    }

    @Test
    public void testLowerNegPromotesSmallTypes() throws Exception {
        // MEM I1 col0, NEG, RET — I1 promotes to I4 on negation
        IrDecoder.Instruction[] ir = {
                insn(MEM, I1_TYPE, 0, 0),
                insn(NEG, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.Negate neg = (LoweredOp.Negate) block.getOp(1);
        Assert.assertEquals(I4_TYPE, neg.type());
    }

    @Test
    public void testLowerNot() throws Exception {
        // MEM I4 col0, IMM I4 0, EQ, NOT, RET
        IrDecoder.Instruction[] ir = {
                insn(MEM, I4_TYPE, 0, 0),
                insn(IMM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(NOT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        // LoadColumn, LoadImm, Compare, Not
        Assert.assertEquals(4, block.getOpCount());
        assertOp(block, 3, LoweredOp.Not.class);
    }

    @Test
    public void testLowerBooleanAndOr() throws Exception {
        // Two comparisons combined with AND:
        // IMM I4 5, MEM I4 col0, GT, IMM I4 10, MEM I4 col0, LT, AND, RET
        IrDecoder.Instruction[] ir = {
                insn(IMM, I4_TYPE, 5, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(IMM, I4_TYPE, 10, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(LT, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        Assert.assertFalse(prog.hasControlFlow());
        LoweredBlock block = prog.getBlock(0);

        // 2x(LoadImm, LoadColumn, Compare) + BooleanOp = 7
        Assert.assertEquals(7, block.getOpCount());
        LoweredOp.BooleanOp boolOp = (LoweredOp.BooleanOp) block.getOp(6);
        Assert.assertEquals(AND, boolOp.opcode());
    }

    // ========================
    // Bind variable tests
    // ========================

    @Test
    public void testLowerBindVariable() throws Exception {
        // VAR I4 index=0, MEM I4 col0, EQ, RET
        IrDecoder.Instruction[] ir = {
                insn(VAR, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.LoadVar var = (LoweredOp.LoadVar) block.getOp(0);
        Assert.assertEquals(0, var.varIndex());
        Assert.assertEquals(0, var.byteOffset());
        Assert.assertEquals(I4_TYPE, var.type());

        Assert.assertArrayEquals(new int[]{0}, prog.getVarOffsets());
    }

    @Test
    public void testLowerMultipleBindVariables() throws Exception {
        // VAR I4 0, MEM I4 col0, EQ, VAR I8 1, MEM I8 col1, EQ, AND, RET
        IrDecoder.Instruction[] ir = {
                insn(VAR, I4_TYPE, 0, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(VAR, I8_TYPE, 1, 0),
                insn(MEM, I8_TYPE, 1, 0),
                insn(EQ, 0, 0, 0),
                insn(AND, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);

        // var[0] at offset 0, width 8; var[1] at offset 8, width 8
        Assert.assertArrayEquals(new int[]{0, 8}, prog.getVarOffsets());
    }

    @Test
    public void testLowerI16BindVariable() throws Exception {
        // VAR I16 0, MEM I16 col0, EQ, RET
        IrDecoder.Instruction[] ir = {
                insn(VAR, I16_TYPE, 0, 0),
                insn(MEM, I16_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);

        // I16 is 16 bytes wide
        Assert.assertArrayEquals(new int[]{0}, prog.getVarOffsets());

        LoweredBlock block = prog.getBlock(0);
        // LoadVar, LoadColumn, CompareI128
        Assert.assertEquals(3, block.getOpCount());
        assertOp(block, 2, LoweredOp.CompareI128.class);
    }

    // ========================
    // Float immediate tests
    // ========================

    @Test
    public void testLowerFloatImmediate() throws Exception {
        // IMM F4 3.14, MEM F4 col0, GT, RET
        IrDecoder.Instruction[] ir = {
                immF4(3.14f),
                insn(MEM, F4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.LoadImm imm = (LoweredOp.LoadImm) block.getOp(0);
        Assert.assertEquals(F4_TYPE, imm.type());
        Assert.assertEquals(Float.floatToRawIntBits(3.14f), imm.lo());
    }

    @Test
    public void testLowerDoubleImmediate() throws Exception {
        // IMM F8 2.718, MEM F8 col0, EQ, RET
        IrDecoder.Instruction[] ir = {
                immF8(2.718),
                insn(MEM, F8_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I8);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.LoadImm imm = (LoweredOp.LoadImm) block.getOp(0);
        Assert.assertEquals(F8_TYPE, imm.type());
        Assert.assertEquals(Double.doubleToRawLongBits(2.718), imm.lo());
    }

    // ========================
    // Var-size header tests
    // ========================

    @Test
    public void testLowerStringHeader() throws Exception {
        // MEM STRING_HEADER col0, IMM I4 -1, EQ, RET
        // Tests: string IS NULL
        IrDecoder.Instruction[] ir = {
                insn(MEM, STRING_HEADER_TYPE, 0, 0),
                insn(IMM, I4_TYPE, -1, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.LoadVarSizeHeader hdr = (LoweredOp.LoadVarSizeHeader) block.getOp(0);
        Assert.assertEquals(STRING_HEADER_TYPE, hdr.headerType());
        Assert.assertEquals(I4_TYPE, hdr.resultType());
    }

    @Test
    public void testLowerVarcharHeader() throws Exception {
        // MEM VARCHAR_HEADER col0, IMM I8 4, EQ, RET
        // Tests: varchar IS NULL (null flag = 4)
        IrDecoder.Instruction[] ir = {
                insn(MEM, VARCHAR_HEADER_TYPE, 0, 0),
                insn(IMM, I8_TYPE, 4, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.LoadVarSizeHeader hdr = (LoweredOp.LoadVarSizeHeader) block.getOp(0);
        Assert.assertEquals(VARCHAR_HEADER_TYPE, hdr.headerType());
        Assert.assertEquals(I8_TYPE, hdr.resultType());

        // No cast needed: varchar header is I8, IMM is I8
        LoweredOp.Compare cmp = (LoweredOp.Compare) block.getOp(2);
        Assert.assertEquals(I8_TYPE, cmp.operandType());
    }

    @Test
    public void testLowerBinaryHeader() throws Exception {
        // MEM BINARY_HEADER col0, IMM I8 -1, NE, RET
        // Tests: binary IS NOT NULL
        IrDecoder.Instruction[] ir = {
                insn(MEM, BINARY_HEADER_TYPE, 0, 0),
                insn(IMM, I8_TYPE, -1, 0),
                insn(NE, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.LoadVarSizeHeader hdr = (LoweredOp.LoadVarSizeHeader) block.getOp(0);
        Assert.assertEquals(BINARY_HEADER_TYPE, hdr.headerType());
        Assert.assertEquals(I8_TYPE, hdr.resultType());
    }

    // ========================
    // I128 (UUID) tests
    // ========================

    @Test
    public void testLowerI128Comparison() throws Exception {
        // IMM I16 lo=123 hi=456, MEM I16 col0, EQ, RET
        IrDecoder.Instruction[] ir = {
                insn(IMM, I16_TYPE, 123, 456),
                insn(MEM, I16_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        LoweredBlock block = prog.getBlock(0);

        Assert.assertEquals(3, block.getOpCount());

        LoweredOp.LoadImm imm = (LoweredOp.LoadImm) block.getOp(0);
        Assert.assertEquals(I16_TYPE, imm.type());
        Assert.assertEquals(123, imm.lo());
        Assert.assertEquals(456, imm.hi());

        LoweredOp.CompareI128 cmp = (LoweredOp.CompareI128) block.getOp(2);
        Assert.assertEquals(EQ, cmp.opcode());
    }

    @Test
    public void testLowerI128NE() throws Exception {
        IrDecoder.Instruction[] ir = {
                insn(VAR, I16_TYPE, 0, 0),
                insn(MEM, I16_TYPE, 0, 0),
                insn(NE, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        LoweredBlock block = prog.getBlock(0);

        LoweredOp.CompareI128 cmp = (LoweredOp.CompareI128) block.getOp(2);
        Assert.assertEquals(NE, cmp.opcode());
    }

    // ========================
    // Short-circuit CFG tests
    // ========================

    @Test
    public void testLowerAndScRejectRow() throws Exception {
        // AND-short-circuit with label 0 (reject row):
        // IMM I4 5, MEM I4 col0, GT, AND_SC(0),
        // IMM I4 10, MEM I4 col0, LT, RET
        IrDecoder.Instruction[] ir = {
                insn(IMM, I4_TYPE, 5, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(AND_SC, 0, 0, 0),  // label 0 = reject row
                insn(IMM, I4_TYPE, 10, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(LT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        Assert.assertTrue(prog.hasControlFlow());
        Assert.assertTrue(prog.getBlockCount() >= 3); // entry, reject, fallthrough

        // Entry block ends with a branch
        LoweredBlock entry = prog.getBlock(prog.getEntryBlockId());
        Assert.assertTrue(entry.getTerminator() instanceof Terminator.Branch);

        Terminator.Branch branch = (Terminator.Branch) entry.getTerminator();
        // False -> reject block (which returns false)
        LoweredBlock rejectBlock = prog.getBlock(branch.falseBlockId());
        Assert.assertTrue(rejectBlock.getTerminator() instanceof Terminator.Return);
    }

    @Test
    public void testLowerOrScAcceptRow() throws Exception {
        // OR-short-circuit with label 1 (accept row):
        // IMM I4 5, MEM I4 col0, EQ, OR_SC(1),
        // IMM I4 10, MEM I4 col0, EQ, RET
        IrDecoder.Instruction[] ir = {
                insn(IMM, I4_TYPE, 5, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR_SC, 0, 1, 0),  // label 1 = accept row
                insn(IMM, I4_TYPE, 10, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        Assert.assertTrue(prog.hasControlFlow());

        LoweredBlock entry = prog.getBlock(prog.getEntryBlockId());
        Terminator.Branch branch = (Terminator.Branch) entry.getTerminator();
        // True -> accept block
        LoweredBlock acceptBlock = prog.getBlock(branch.trueBlockId());
        Assert.assertTrue(acceptBlock.getTerminator() instanceof Terminator.Return);
        Terminator.Return ret = (Terminator.Return) acceptBlock.getTerminator();
        Assert.assertEquals(Terminator.Return.ACCEPT, ret.src());
    }

    @Test
    public void testLowerChainedAndScWithLabels() throws Exception {
        // Pattern from IN() with 3 values:
        // BEGIN_SC(2),
        // IMM I4 1, MEM I4 col0, EQ, OR_SC(2),
        // IMM I4 2, MEM I4 col0, EQ, OR_SC(2),
        // IMM I4 3, MEM I4 col0, EQ,
        // END_SC(2),
        // RET
        IrDecoder.Instruction[] ir = {
                insn(BEGIN_SC, 0, 2, 0),
                insn(IMM, I4_TYPE, 1, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR_SC, 0, 2, 0),
                insn(IMM, I4_TYPE, 2, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR_SC, 0, 2, 0),
                insn(IMM, I4_TYPE, 3, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(END_SC, 0, 2, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        Assert.assertTrue(prog.hasControlFlow());
        // At least: entry block, 2 fallthrough blocks, 1 merge block after END_SC
        Assert.assertTrue(prog.getBlockCount() >= 4);
    }

    @Test
    public void testLowerTwoInExpressionsReusedLabels() throws Exception {
        // Two IN() expressions with the same label index (2), connected by AND_SC.
        // This tests the per-instruction jump target computation.
        //
        // BEGIN_SC(2), IMM 1, MEM col0, EQ, OR_SC(2), IMM 2, MEM col0, EQ, END_SC(2),
        // AND_SC(0),
        // BEGIN_SC(2), IMM 10, MEM col1, EQ, OR_SC(2), IMM 20, MEM col1, EQ, END_SC(2),
        // RET
        IrDecoder.Instruction[] ir = {
                insn(BEGIN_SC, 0, 2, 0),
                insn(IMM, I4_TYPE, 1, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(OR_SC, 0, 2, 0),
                insn(IMM, I4_TYPE, 2, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(END_SC, 0, 2, 0),
                insn(AND_SC, 0, 0, 0),  // label 0 = reject
                insn(BEGIN_SC, 0, 2, 0),
                insn(IMM, I4_TYPE, 10, 0),
                insn(MEM, I4_TYPE, 1, 0),
                insn(EQ, 0, 0, 0),
                insn(OR_SC, 0, 2, 0),
                insn(IMM, I4_TYPE, 20, 0),
                insn(MEM, I4_TYPE, 1, 0),
                insn(EQ, 0, 0, 0),
                insn(END_SC, 0, 2, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SCALAR_I4);
        Assert.assertTrue(prog.hasControlFlow());
        // Must not crash. The two IN()s with reused label 2 must each resolve
        // to their own nearest END_SC.
        Assert.assertTrue(prog.getBlockCount() >= 5);
    }

    // ========================
    // Type promotion utilities
    // ========================

    @Test
    public void testArithmeticResultTypePromotion() {
        Assert.assertEquals(I4_TYPE, IrLowering.arithmeticResultType(I4_TYPE, I4_TYPE));
        Assert.assertEquals(I8_TYPE, IrLowering.arithmeticResultType(I4_TYPE, I8_TYPE));
        Assert.assertEquals(F4_TYPE, IrLowering.arithmeticResultType(I4_TYPE, F4_TYPE));
        Assert.assertEquals(F8_TYPE, IrLowering.arithmeticResultType(I4_TYPE, F8_TYPE));
        Assert.assertEquals(F8_TYPE, IrLowering.arithmeticResultType(I8_TYPE, F4_TYPE));
        Assert.assertEquals(F8_TYPE, IrLowering.arithmeticResultType(F4_TYPE, I8_TYPE));
        Assert.assertEquals(F8_TYPE, IrLowering.arithmeticResultType(F4_TYPE, F8_TYPE));
    }

    @Test
    public void testComparisonTypePromotion() {
        Assert.assertEquals(I4_TYPE, IrLowering.comparisonType(I4_TYPE, I4_TYPE));
        Assert.assertEquals(I8_TYPE, IrLowering.comparisonType(I4_TYPE, I8_TYPE));
        Assert.assertEquals(F4_TYPE, IrLowering.comparisonType(I4_TYPE, F4_TYPE));
        Assert.assertEquals(F8_TYPE, IrLowering.comparisonType(I4_TYPE, F8_TYPE));
        Assert.assertEquals(F8_TYPE, IrLowering.comparisonType(I8_TYPE, F4_TYPE));
        Assert.assertEquals(I16_TYPE, IrLowering.comparisonType(I16_TYPE, I16_TYPE));
        // Var-size headers normalize
        Assert.assertEquals(I4_TYPE, IrLowering.comparisonType(STRING_HEADER_TYPE, I4_TYPE));
        Assert.assertEquals(I8_TYPE, IrLowering.comparisonType(VARCHAR_HEADER_TYPE, I8_TYPE));
        Assert.assertEquals(I8_TYPE, IrLowering.comparisonType(BINARY_HEADER_TYPE, I8_TYPE));
    }

    @Test
    public void testNormalizeVarSizeType() {
        Assert.assertEquals(I4_TYPE, IrLowering.normalizeVarSizeType(STRING_HEADER_TYPE));
        Assert.assertEquals(I8_TYPE, IrLowering.normalizeVarSizeType(BINARY_HEADER_TYPE));
        Assert.assertEquals(I8_TYPE, IrLowering.normalizeVarSizeType(VARCHAR_HEADER_TYPE));
        Assert.assertEquals(I4_TYPE, IrLowering.normalizeVarSizeType(I4_TYPE));
        Assert.assertEquals(I16_TYPE, IrLowering.normalizeVarSizeType(I16_TYPE));
    }

    // ========================
    // Edge cases
    // ========================

    @Test
    public void testLowerEmptyProgram() throws Exception {
        // Just RET with nothing on stack -> accept
        IrDecoder.Instruction[] ir = {
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);
        Assert.assertEquals(0, block.getOpCount());
        Terminator.Return ret = (Terminator.Return) block.getTerminator();
        Assert.assertEquals(Terminator.Return.ACCEPT, ret.src());
    }

    @Test
    public void testLowerNoCastWhenTypesMatch() throws Exception {
        // IMM I4 42, MEM I4 col0, EQ, RET — no cast needed
        IrDecoder.Instruction[] ir = {
                insn(IMM, I4_TYPE, 42, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        // No cast ops should exist
        for (int i = 0; i < block.getOpCount(); i++) {
            Assert.assertFalse(
                    "unexpected Cast op at index " + i,
                    block.getOp(i) instanceof LoweredOp.Cast
            );
        }
    }

    @Test
    public void testLowerOptionsPreserved() throws Exception {
        int options = 1 | (3 << 1) | (1 << 4) | (1 << 6); // debug + log2(8) + single-size + null-checks
        IrDecoder.Instruction[] ir = {
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, options);
        Assert.assertTrue(prog.getOptions().isDebug());
        Assert.assertTrue(prog.getOptions().isSingleSize());
        Assert.assertTrue(prog.getOptions().isNullChecksEnabled());
        Assert.assertEquals(8, prog.getOptions().maxColumnTypeSize());
    }

    @Test
    public void testLowerTempIdsAreSequential() throws Exception {
        // IMM I4 5, MEM I4 col0, GT, RET
        IrDecoder.Instruction[] ir = {
                insn(IMM, I4_TYPE, 5, 0),
                insn(MEM, I4_TYPE, 0, 0),
                insn(GT, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        // Temps: 0 (IMM), 1 (MEM), 2 (Compare result)
        Assert.assertEquals(0, block.getOp(0).dst());
        Assert.assertEquals(1, block.getOp(1).dst());
        Assert.assertEquals(2, block.getOp(2).dst());
        Assert.assertEquals(3, prog.getNextTempId());
    }

    @Test
    public void testLowerI2TypeComparison() throws Exception {
        // I2 type should work in comparisons. I2 vs I2 -> I4 comparison type
        IrDecoder.Instruction[] ir = {
                insn(IMM, I2_TYPE, 100, 0),
                insn(MEM, I2_TYPE, 0, 0),
                insn(EQ, 0, 0, 0),
                insn(RET, 0, 0, 0)
        };

        LoweredProgram prog = IrLowering.lower(ir, SINGLE_SIZE_I4);
        LoweredBlock block = prog.getBlock(0);

        // comparisonType(I2, I2) = I4, so we expect 2 casts + 1 compare
        // LoadImm(I2), LoadColumn(I2), Cast(I2->I4), Cast(I2->I4), Compare(I4)
        Assert.assertEquals(5, block.getOpCount());
        LoweredOp.Compare cmp = (LoweredOp.Compare) block.getOp(4);
        Assert.assertEquals(I4_TYPE, cmp.operandType());
    }

    // ========================
    // Helpers
    // ========================

    private static IrDecoder.Instruction insn(int opcode, int type, long payloadLo, long payloadHi) {
        return new IrDecoder.Instruction(opcode, type, payloadLo, payloadHi);
    }

    private static IrDecoder.Instruction immF4(float value) {
        return new IrDecoder.Instruction(IMM, F4_TYPE, Double.doubleToRawLongBits(value), 0);
    }

    private static IrDecoder.Instruction immF8(double value) {
        return new IrDecoder.Instruction(IMM, F8_TYPE, Double.doubleToRawLongBits(value), 0);
    }

    private static IrDecoder.Instruction memF4(int columnIndex) {
        return new IrDecoder.Instruction(MEM, F4_TYPE, columnIndex, 0);
    }

    private static void assertOp(LoweredBlock block, int index, Class<? extends LoweredOp> expectedType) {
        Assert.assertTrue(
                "expected " + expectedType.getSimpleName() + " at index " + index + " but got " + block.getOp(index).getClass().getSimpleName(),
                expectedType.isInstance(block.getOp(index))
        );
    }

    private static void assertReturnTerminator(LoweredBlock block) {
        Assert.assertTrue(block.getTerminator() instanceof Terminator.Return);
    }
}
