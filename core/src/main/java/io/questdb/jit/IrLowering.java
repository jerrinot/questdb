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

import io.questdb.griffin.SqlException;
import io.questdb.std.IntStack;
import io.questdb.std.ObjList;

import java.util.Arrays;

import static io.questdb.jit.CompiledFilterIRSerializer.*;

/**
 * Lowers decoded stack-based JIT IR into a typed CFG ({@link LoweredProgram}).
 * <p>
 * This pass:
 * <ul>
 *   <li>Converts implicit stack slots into explicit temporaries</li>
 *   <li>Resolves type promotions and inserts explicit {@link LoweredOp.Cast} ops</li>
 *   <li>Converts short-circuit opcodes into explicit CFG edges</li>
 *   <li>Computes bind-variable byte offsets</li>
 * </ul>
 */
public final class IrLowering {

    /**
     * Lower decoded IR instructions into a {@link LoweredProgram}.
     *
     * @param instructions decoded IR from {@link IrDecoder}
     * @param options      packed options word from the serializer
     * @return the lowered program
     */
    public static LoweredProgram lower(IrDecoder.Instruction[] instructions, int options) throws SqlException {
        DecodedOptions decoded = DecodedOptions.decode(options);
        int[] varOffsets = computeVarOffsets(instructions);
        return new LoweringPass(instructions, decoded, varOffsets).run();
    }

    // --- Bind-variable offset computation ---

    static int[] computeVarOffsets(IrDecoder.Instruction[] decoded) throws SqlException {
        int maxVarIndex = -1;
        for (IrDecoder.Instruction instruction : decoded) {
            if (instruction.opcode() == VAR) {
                maxVarIndex = Math.max(maxVarIndex, Math.toIntExact(instruction.payloadLo()));
            }
        }
        if (maxVarIndex < 0) {
            return new int[0];
        }

        int[] widths = new int[maxVarIndex + 1];
        Arrays.fill(widths, -1);
        for (IrDecoder.Instruction instruction : decoded) {
            if (instruction.opcode() == VAR) {
                int index = Math.toIntExact(instruction.payloadLo());
                int width = typeWidth(instruction.type());
                if (width < 0) {
                    throw SqlException.position(0).put("unsupported bind variable type [type=").put(instruction.type()).put(']');
                }
                if (widths[index] != -1 && widths[index] != width) {
                    throw SqlException.position(0).put("bind variable uses conflicting widths [index=").put(index).put(']');
                }
                widths[index] = width;
            }
        }

        int[] offsets = new int[maxVarIndex + 1];
        int offset = 0;
        for (int i = 0; i < widths.length; i++) {
            if (widths[i] < 0) {
                throw SqlException.position(0).put("bind variable index is missing [index=").put(i).put(']');
            }
            offsets[i] = offset;
            offset += widths[i];
        }
        return offsets;
    }

    // --- Type utilities ---

    public static int arithmeticResultType(int leftType, int rightType) {
        if (leftType == I16_TYPE || rightType == I16_TYPE) {
            throw new IllegalArgumentException("unsupported i128 arithmetic");
        }
        if (leftType == F8_TYPE || rightType == F8_TYPE
                || ((leftType == I8_TYPE || rightType == I8_TYPE) && (leftType == F4_TYPE || rightType == F4_TYPE))) {
            return F8_TYPE;
        }
        if (leftType == F4_TYPE || rightType == F4_TYPE) {
            return F4_TYPE;
        }
        if (leftType == I8_TYPE || rightType == I8_TYPE) {
            return I8_TYPE;
        }
        return I4_TYPE;
    }

    public static int comparisonType(int leftType, int rightType) {
        int left = normalizeVarSizeType(leftType);
        int right = normalizeVarSizeType(rightType);
        if (left == I16_TYPE || right == I16_TYPE) {
            if (left != I16_TYPE || right != I16_TYPE) {
                throw new IllegalArgumentException("unsupported mixed i128 comparison");
            }
            return I16_TYPE;
        }
        if (left == F8_TYPE || right == F8_TYPE
                || ((left == I8_TYPE || right == I8_TYPE) && (left == F4_TYPE || right == F4_TYPE))) {
            return F8_TYPE;
        }
        if (left == F4_TYPE || right == F4_TYPE) {
            return F4_TYPE;
        }
        if (left == I8_TYPE || right == I8_TYPE) {
            return I8_TYPE;
        }
        return I4_TYPE;
    }

    public static int normalizeVarSizeType(int type) {
        return switch (type) {
            case STRING_HEADER_TYPE -> I4_TYPE;
            case BINARY_HEADER_TYPE, VARCHAR_HEADER_TYPE -> I8_TYPE;
            default -> type;
        };
    }

    private static int typeWidth(int type) {
        return switch (type) {
            case I1_TYPE, I2_TYPE, I4_TYPE, F4_TYPE, I8_TYPE, F8_TYPE,
                 STRING_HEADER_TYPE, BINARY_HEADER_TYPE, VARCHAR_HEADER_TYPE -> 8;
            case I16_TYPE -> 16;
            default -> -1;
        };
    }

    private static boolean isVarSizeHeaderType(int type) {
        return type == STRING_HEADER_TYPE || type == BINARY_HEADER_TYPE || type == VARCHAR_HEADER_TYPE;
    }

    // --- Inner lowering pass ---

    private static final class LoweringPass {
        private final ObjList<LoweredBlock> blocks = new ObjList<>();
        private LoweredBlock currentBlock;
        private boolean hasControlFlow;
        private final IrDecoder.Instruction[] instructions;
        // Merge temps: END_SC pc -> temp ID for the short-circuit result
        private final int[] mergeTempMap;
        private int nextBlockId;
        private int nextTempId;
        private final DecodedOptions options;
        private final IntStack tempIdStack = new IntStack();
        private final IntStack typeStack = new IntStack();
        private final int[] varOffsets;

        LoweringPass(IrDecoder.Instruction[] instructions, DecodedOptions options, int[] varOffsets) {
            this.instructions = instructions;
            this.options = options;
            this.varOffsets = varOffsets;
            this.mergeTempMap = new int[instructions.length];
            Arrays.fill(this.mergeTempMap, -1);
        }

        LoweredProgram run() throws SqlException {
            currentBlock = newBlock();
            int entryBlockId = currentBlock.getId();

            // Pre-compute per-instruction jump targets for short-circuit
            int[] jumpTargets = computeJumpTargets();
            // Map from END_SC pc -> block that follows it
            int[] endScBlockMap = new int[instructions.length];
            Arrays.fill(endScBlockMap, -1);

            int pc = 0;
            while (pc < instructions.length) {
                IrDecoder.Instruction insn = instructions[pc];
                switch (insn.opcode()) {
                    case IMM -> lowerImm(insn);
                    case MEM -> lowerMem(insn);
                    case VAR -> lowerVar(insn);
                    case NEG -> lowerNeg();
                    case NOT -> lowerNot();
                    case AND -> lowerBooleanBinary(AND);
                    case OR -> lowerBooleanBinary(OR);
                    case EQ, NE, LT, LE, GT, GE -> lowerComparison(insn.opcode());
                    case ADD, SUB, MUL, DIV -> lowerArithmetic(insn.opcode());
                    case AND_SC -> {
                        pc = lowerAndSc(insn, pc, jumpTargets, endScBlockMap);
                        continue; // pc already advanced
                    }
                    case OR_SC -> {
                        pc = lowerOrSc(insn, pc, jumpTargets, endScBlockMap);
                        continue;
                    }
                    case BEGIN_SC -> {
                        // no-op in the lowered form; CFG edges handle control flow
                        pc++;
                        continue;
                    }
                    case END_SC -> {
                        pc = lowerEndSc(pc, endScBlockMap);
                        continue;
                    }
                    case RET -> lowerRet();
                    default -> throw SqlException.position(0)
                            .put("unsupported opcode during lowering [opcode=").put(insn.opcode()).put(']');
                }
                pc++;
            }

            // If the last block has no terminator, add a return based on stack top
            if (currentBlock.getTerminator() == null) {
                if (tempIdStack.size() > 0) {
                    currentBlock.setTerminator(new Terminator.Return(tempIdStack.peek()));
                } else {
                    currentBlock.setTerminator(new Terminator.Return(Terminator.Return.ACCEPT));
                }
            }

            return new LoweredProgram(options, blocks, entryBlockId, nextTempId, varOffsets, hasControlFlow);
        }

        private int allocTemp() {
            return nextTempId++;
        }

        private int[] computeJumpTargets() throws SqlException {
            int[] targets = new int[instructions.length];
            Arrays.fill(targets, -1);
            for (int pc = 0; pc < instructions.length; pc++) {
                IrDecoder.Instruction insn = instructions[pc];
                if (insn.opcode() == AND_SC || insn.opcode() == OR_SC) {
                    int labelIndex = Math.toIntExact(insn.payloadLo());
                    if (labelIndex >= 2) {
                        int target = findNextEndSc(pc, labelIndex);
                        if (target < 0) {
                            throw SqlException.position(0)
                                    .put("short-circuit label not bound [index=").put(labelIndex).put(']');
                        }
                        targets[pc] = target;
                    }
                }
            }
            return targets;
        }

        private int findNextEndSc(int fromPc, int labelIndex) {
            for (int j = fromPc + 1; j < instructions.length; j++) {
                if (instructions[j].opcode() == END_SC
                        && Math.toIntExact(instructions[j].payloadLo()) == labelIndex) {
                    return j;
                }
            }
            return -1;
        }

        /**
         * Coerce a temporary to the target type by inserting a Cast if needed.
         * Returns the (possibly new) temporary ID holding the value in the target type.
         */
        private int coerceIfNeeded(int tempId, int fromType, int toType) {
            if (fromType == toType) {
                return tempId;
            }
            int castDst = allocTemp();
            currentBlock.addOp(new LoweredOp.Cast(castDst, tempId, fromType, toType));
            return castDst;
        }

        private void lowerArithmetic(int opcode) {
            int lhsType = typeStack.pop();
            int lhsTmp = tempIdStack.pop();
            int rhsType = typeStack.pop();
            int rhsTmp = tempIdStack.pop();

            int resultType = arithmeticResultType(lhsType, rhsType);
            int coercedLhs = coerceIfNeeded(lhsTmp, lhsType, resultType);
            int coercedRhs = coerceIfNeeded(rhsTmp, rhsType, resultType);

            int dst = allocTemp();
            currentBlock.addOp(new LoweredOp.Arithmetic(dst, coercedLhs, coercedRhs, opcode, resultType));
            tempIdStack.push(dst);
            typeStack.push(resultType);
        }

        private void lowerBooleanBinary(int opcode) {
            // lhs is on top (popped first)
            typeStack.pop();
            int lhsTmp = tempIdStack.pop();
            typeStack.pop();
            int rhsTmp = tempIdStack.pop();

            int dst = allocTemp();
            currentBlock.addOp(new LoweredOp.BooleanOp(dst, lhsTmp, rhsTmp, opcode));
            tempIdStack.push(dst);
            typeStack.push(I1_TYPE);
        }

        private void lowerComparison(int opcode) {
            int lhsType = typeStack.pop();
            int lhsTmp = tempIdStack.pop();
            int rhsType = typeStack.pop();
            int rhsTmp = tempIdStack.pop();

            int targetType = comparisonType(lhsType, rhsType);
            int dst = allocTemp();

            if (targetType == I16_TYPE) {
                // I128 comparison — no coercion needed, both must be I16 already
                currentBlock.addOp(new LoweredOp.CompareI128(dst, lhsTmp, rhsTmp, opcode));
            } else {
                int coercedLhs = coerceIfNeeded(lhsTmp, normalizeVarSizeType(lhsType), targetType);
                int coercedRhs = coerceIfNeeded(rhsTmp, normalizeVarSizeType(rhsType), targetType);
                currentBlock.addOp(new LoweredOp.Compare(dst, coercedLhs, coercedRhs, opcode, targetType));
            }
            tempIdStack.push(dst);
            typeStack.push(I1_TYPE);
        }

        private void lowerImm(IrDecoder.Instruction insn) {
            int dst = allocTemp();
            int type = insn.type();
            // For float immediates, the IR stores the double-encoded value in payloadLo.
            // Convert to raw bits at lowering time to match runtime representation.
            long lo = insn.payloadLo();
            long hi = insn.payloadHi();
            if (type == F4_TYPE) {
                lo = Float.floatToRawIntBits((float) insn.doublePayload());
                hi = 0;
            } else if (type == F8_TYPE) {
                lo = Double.doubleToRawLongBits(insn.doublePayload());
                hi = 0;
            }
            currentBlock.addOp(new LoweredOp.LoadImm(dst, type, lo, hi));
            tempIdStack.push(dst);
            typeStack.push(type);
        }

        private void lowerMem(IrDecoder.Instruction insn) {
            int dst = allocTemp();
            int columnIndex = Math.toIntExact(insn.payloadLo());
            int type = insn.type();

            if (isVarSizeHeaderType(type)) {
                currentBlock.addOp(new LoweredOp.LoadVarSizeHeader(dst, columnIndex, type));
                typeStack.push(normalizeVarSizeType(type));
            } else {
                currentBlock.addOp(new LoweredOp.LoadColumn(dst, columnIndex, type));
                typeStack.push(type);
            }
            tempIdStack.push(dst);
        }

        private void lowerNeg() {
            int srcType = typeStack.pop();
            int srcTmp = tempIdStack.pop();

            // NEG promotes I1/I2 -> I4: insert a widening cast first
            int resultType = switch (srcType) {
                case I1_TYPE, I2_TYPE -> I4_TYPE;
                default -> srcType;
            };
            srcTmp = coerceIfNeeded(srcTmp, srcType, resultType);

            int dst = allocTemp();
            currentBlock.addOp(new LoweredOp.Negate(dst, srcTmp, resultType));
            tempIdStack.push(dst);
            typeStack.push(resultType);
        }

        private void lowerNot() {
            int srcType = typeStack.pop();
            int srcTmp = tempIdStack.pop();

            int dst = allocTemp();
            currentBlock.addOp(new LoweredOp.Not(dst, srcTmp));
            tempIdStack.push(dst);
            typeStack.push(I1_TYPE);
        }

        private void lowerRet() {
            if (tempIdStack.size() > 0) {
                int src = tempIdStack.pop();
                typeStack.pop();
                currentBlock.setTerminator(new Terminator.Return(src));
            } else {
                currentBlock.setTerminator(new Terminator.Return(Terminator.Return.ACCEPT));
            }
        }

        private void lowerVar(IrDecoder.Instruction insn) {
            int dst = allocTemp();
            int varIndex = Math.toIntExact(insn.payloadLo());
            int type = insn.type();
            int byteOffset = varOffsets[varIndex];
            currentBlock.addOp(new LoweredOp.LoadVar(dst, varIndex, byteOffset, type));
            tempIdStack.push(dst);
            typeStack.push(type);
        }

        // --- Short-circuit lowering ---

        /**
         * AND_SC(label): pop value; if false, jump to label target.
         * Label 0 = reject row (return false).
         * Label 1 = accept row (return true).
         * Label 2+ = jump to the END_SC with matching label index.
         */
        private int lowerAndSc(IrDecoder.Instruction insn, int pc, int[] jumpTargets, int[] endScBlockMap) {
            hasControlFlow = true;
            typeStack.pop();
            int condTmp = tempIdStack.pop();
            int labelIndex = Math.toIntExact(insn.payloadLo());

            // "true" continues to the next instruction in the fall-through block
            LoweredBlock fallthroughBlock = newBlock();
            int fallthroughId = fallthroughBlock.getId();

            if (labelIndex == 0) {
                // false -> reject row
                LoweredBlock rejectBlock = newBlock();
                rejectBlock.setTerminator(new Terminator.Return(Terminator.Return.ACCEPT + 1)); // -1 means accept; we need a "reject" return
                // Actually, Return(src) where src=-1 means accept. For reject, we need a constant false.
                // Let's use a dedicated approach: emit a return with a false constant.
                int falseTmp = allocTemp();
                rejectBlock.addOp(new LoweredOp.LoadImm(falseTmp, I1_TYPE, 0, 0));
                rejectBlock.setTerminator(new Terminator.Return(falseTmp));

                currentBlock.setTerminator(new Terminator.Branch(condTmp, fallthroughId, rejectBlock.getId()));
            } else if (labelIndex == 1) {
                // false -> accept row
                LoweredBlock acceptBlock = newBlock();
                acceptBlock.setTerminator(new Terminator.Return(Terminator.Return.ACCEPT));

                currentBlock.setTerminator(new Terminator.Branch(condTmp, fallthroughId, acceptBlock.getId()));
            } else {
                // false -> jump to END_SC merge block via a phi-store block
                // that sets the merge temp to 0 (false = no match in AND chain)
                int targetPc = jumpTargets[pc];
                int targetBlockId = getOrCreateEndScBlock(targetPc, endScBlockMap);
                int mergeTmp = getOrAllocMergeTemp(targetPc);

                LoweredBlock phiBlock = newBlock();
                phiBlock.addOp(new LoweredOp.LoadImm(mergeTmp, I1_TYPE, 0, 0));
                phiBlock.setTerminator(new Terminator.Goto(targetBlockId));

                currentBlock.setTerminator(new Terminator.Branch(condTmp, fallthroughId, phiBlock.getId()));
            }

            currentBlock = fallthroughBlock;
            return pc + 1;
        }

        /**
         * OR_SC(label): pop value; if true, jump to label target.
         */
        private int lowerOrSc(IrDecoder.Instruction insn, int pc, int[] jumpTargets, int[] endScBlockMap) {
            hasControlFlow = true;
            typeStack.pop();
            int condTmp = tempIdStack.pop();
            int labelIndex = Math.toIntExact(insn.payloadLo());

            LoweredBlock fallthroughBlock = newBlock();
            int fallthroughId = fallthroughBlock.getId();

            if (labelIndex == 0) {
                // true -> reject row
                LoweredBlock rejectBlock = newBlock();
                int falseTmp = allocTemp();
                rejectBlock.addOp(new LoweredOp.LoadImm(falseTmp, I1_TYPE, 0, 0));
                rejectBlock.setTerminator(new Terminator.Return(falseTmp));

                currentBlock.setTerminator(new Terminator.Branch(condTmp, rejectBlock.getId(), fallthroughId));
            } else if (labelIndex == 1) {
                // true -> accept row
                LoweredBlock acceptBlock = newBlock();
                acceptBlock.setTerminator(new Terminator.Return(Terminator.Return.ACCEPT));

                currentBlock.setTerminator(new Terminator.Branch(condTmp, acceptBlock.getId(), fallthroughId));
            } else {
                // true -> jump to END_SC merge block via a phi-store block
                // that sets the merge temp to 1 (true = match found in OR chain)
                int targetPc = jumpTargets[pc];
                int targetBlockId = getOrCreateEndScBlock(targetPc, endScBlockMap);
                int mergeTmp = getOrAllocMergeTemp(targetPc);

                LoweredBlock phiBlock = newBlock();
                phiBlock.addOp(new LoweredOp.LoadImm(mergeTmp, I1_TYPE, 1, 0));
                phiBlock.setTerminator(new Terminator.Goto(targetBlockId));

                currentBlock.setTerminator(new Terminator.Branch(condTmp, phiBlock.getId(), fallthroughId));
            }

            currentBlock = fallthroughBlock;
            return pc + 1;
        }

        /**
         * END_SC marks a label binding point. The current block's last result
         * is moved into the merge temp, then control transfers to the merge block.
         */
        private int lowerEndSc(int pc, int[] endScBlockMap) {
            int preCreatedBlockId = endScBlockMap[pc];
            if (preCreatedBlockId >= 0) {
                int mergeTmp = mergeTempMap[pc];
                // Move the current stack top into the merge temp
                if (mergeTmp >= 0 && tempIdStack.size() > 0) {
                    int srcTmp = tempIdStack.peek();
                    int srcType = typeStack.peek();
                    currentBlock.addOp(new LoweredOp.Move(mergeTmp, srcTmp, srcType));
                    // Replace stack top with the merge temp
                    tempIdStack.pop();
                    typeStack.pop();
                    tempIdStack.push(mergeTmp);
                    typeStack.push(I1_TYPE);
                }
                if (currentBlock.getTerminator() == null) {
                    currentBlock.setTerminator(new Terminator.Goto(preCreatedBlockId));
                }
                currentBlock = blocks.getQuick(preCreatedBlockId);
            }
            return pc + 1;
        }

        /**
         * Get or create the block that will follow an END_SC instruction.
         * Multiple AND_SC/OR_SC instructions may target the same END_SC.
         */
        private int getOrCreateEndScBlock(int endScPc, int[] endScBlockMap) {
            if (endScBlockMap[endScPc] >= 0) {
                return endScBlockMap[endScPc];
            }
            LoweredBlock block = newBlock();
            endScBlockMap[endScPc] = block.getId();
            return block.getId();
        }

        private int getOrAllocMergeTemp(int endScPc) {
            if (mergeTempMap[endScPc] < 0) {
                mergeTempMap[endScPc] = allocTemp();
            }
            return mergeTempMap[endScPc];
        }

        private LoweredBlock newBlock() {
            LoweredBlock block = new LoweredBlock(nextBlockId++);
            blocks.add(block);
            return block;
        }
    }
}
