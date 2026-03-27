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
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.jit.CompiledFilterIRSerializer.ADD;
import static io.questdb.jit.CompiledFilterIRSerializer.AND;
import static io.questdb.jit.CompiledFilterIRSerializer.AND_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.BEGIN_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.BINARY_HEADER_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.DIV;
import static io.questdb.jit.CompiledFilterIRSerializer.END_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.EQ;
import static io.questdb.jit.CompiledFilterIRSerializer.F4_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.F8_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.GE;
import static io.questdb.jit.CompiledFilterIRSerializer.GT;
import static io.questdb.jit.CompiledFilterIRSerializer.I16_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.I1_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.I2_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.I4_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.I8_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.IMM;
import static io.questdb.jit.CompiledFilterIRSerializer.LE;
import static io.questdb.jit.CompiledFilterIRSerializer.LT;
import static io.questdb.jit.CompiledFilterIRSerializer.MEM;
import static io.questdb.jit.CompiledFilterIRSerializer.MUL;
import static io.questdb.jit.CompiledFilterIRSerializer.NE;
import static io.questdb.jit.CompiledFilterIRSerializer.NEG;
import static io.questdb.jit.CompiledFilterIRSerializer.NOT;
import static io.questdb.jit.CompiledFilterIRSerializer.OR;
import static io.questdb.jit.CompiledFilterIRSerializer.OR_SC;
import static io.questdb.jit.CompiledFilterIRSerializer.RET;
import static io.questdb.jit.CompiledFilterIRSerializer.STRING_HEADER_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.SUB;
import static io.questdb.jit.CompiledFilterIRSerializer.VAR;
import static io.questdb.jit.CompiledFilterIRSerializer.VARCHAR_HEADER_TYPE;

public class VectorFilterInterpreter {
    private static final double DOUBLE_EPSILON = 1e-10;
    private static final float FLOAT_EPSILON = 1e-10f;
    private static final int NULL_CHECKS_FLAG = 1 << 6;
    private static final sun.misc.Unsafe UNSAFE = Unsafe.getUnsafe();
    private static final AtomicLong VECTOR_API_EXECUTION_COUNT = new AtomicLong();

    private final IrDecoder decoder = new IrDecoder();
    private final java.lang.ThreadLocal<ExecutionState> tlState = new java.lang.ThreadLocal<>();
    private IrDecoder.Instruction[] instructions = new IrDecoder.Instruction[0];
    private int[] jumpTargets = new int[0];
    private boolean nullChecks;
    private int[] varOffsets = new int[0];
    private VectorApiFilterExecutor vectorExecutor;

    public void compile(MemoryCARW filter, int options) throws SqlException {
        final IrDecoder.Instruction[] decoded = decoder.decode(filter);
        final int[] offsets = computeVarOffsetsAndValidate(decoded);
        final int[] targets = computeJumpTargets(decoded);

        instructions = decoded;
        varOffsets = offsets;
        jumpTargets = targets;
        nullChecks = (options & NULL_CHECKS_FLAG) != 0;
        vectorExecutor = VectorApiFilterExecutor.tryCreate(decoded, offsets, options, nullChecks);
        tlState.remove();
    }

    public long filter(
            long dataAddress,
            long dataSize,
            long varSizeAuxAddress,
            long varsAddress,
            long varsSize,
            long filteredRowsAddress,
            long rowsCount
    ) {
        if (vectorExecutor != null) {
            VECTOR_API_EXECUTION_COUNT.incrementAndGet();
            return vectorExecutor.filter(
                    dataAddress,
                    dataSize,
                    varsAddress,
                    filteredRowsAddress,
                    rowsCount
            );
        }
        long filteredCount = 0;
        for (long row = 0; row < rowsCount; row++) {
            if (evaluateRow(dataAddress, dataSize, varSizeAuxAddress, varsAddress, varsSize, row)) {
                UNSAFE.putLong(filteredRowsAddress + (filteredCount << 3), row);
                filteredCount++;
            }
        }
        return filteredCount;
    }

    public long filterCount(
            long dataAddress,
            long dataSize,
            long varSizeAuxAddress,
            long varsAddress,
            long varsSize,
            long rowsCount
    ) {
        if (vectorExecutor != null) {
            VECTOR_API_EXECUTION_COUNT.incrementAndGet();
            return vectorExecutor.filterCount(
                    dataAddress,
                    dataSize,
                    varsAddress,
                    rowsCount
            );
        }
        long filteredCount = 0;
        for (long row = 0; row < rowsCount; row++) {
            if (evaluateRow(dataAddress, dataSize, varSizeAuxAddress, varsAddress, varsSize, row)) {
                filteredCount++;
            }
        }
        return filteredCount;
    }

    public static long getVectorApiExecutionCount() {
        return VECTOR_API_EXECUTION_COUNT.get();
    }

    public static void resetVectorApiExecutionCount() {
        VECTOR_API_EXECUTION_COUNT.set(0);
    }

    public boolean usesVectorApi() {
        return vectorExecutor != null;
    }

    private static boolean asBoolean(ScalarValue value) {
        return value.lo != 0;
    }

    private static void binaryArithmetic(
            ScalarValue lhs,
            ScalarValue rhs,
            ScalarValue out,
            ScalarValue left,
            ScalarValue right,
            int opcode,
            boolean nullChecks
    ) {
        final int targetType = arithmeticResultType(lhs.type, rhs.type);
        coerce(lhs, targetType, left);
        coerce(rhs, targetType, right);

        switch (targetType) {
            case I4_TYPE: {
                final int leftValue = (int) left.lo;
                final int rightValue = (int) right.lo;
                if (nullChecks && (leftValue == Numbers.INT_NULL || rightValue == Numbers.INT_NULL || (opcode == DIV && rightValue == 0))) {
                    out.set(I4_TYPE, Numbers.INT_NULL, 0);
                    return;
                }
                final int result;
                switch (opcode) {
                    case ADD:
                        result = leftValue + rightValue;
                        break;
                    case SUB:
                        result = leftValue - rightValue;
                        break;
                    case MUL:
                        result = leftValue * rightValue;
                        break;
                    case DIV:
                        result = leftValue / rightValue;
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                }
                out.set(I4_TYPE, result, 0);
                return;
            }
            case I8_TYPE: {
                final long leftValue = left.lo;
                final long rightValue = right.lo;
                if (nullChecks && (leftValue == Numbers.LONG_NULL || rightValue == Numbers.LONG_NULL || (opcode == DIV && rightValue == 0))) {
                    out.set(I8_TYPE, Numbers.LONG_NULL, 0);
                    return;
                }
                final long result;
                switch (opcode) {
                    case ADD:
                        result = leftValue + rightValue;
                        break;
                    case SUB:
                        result = leftValue - rightValue;
                        break;
                    case MUL:
                        result = leftValue * rightValue;
                        break;
                    case DIV:
                        result = leftValue / rightValue;
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                }
                out.set(I8_TYPE, result, 0);
                return;
            }
            case F4_TYPE: {
                final float leftValue = Float.intBitsToFloat((int) left.lo);
                final float rightValue = Float.intBitsToFloat((int) right.lo);
                if (Float.isNaN(leftValue) || Float.isNaN(rightValue) || (opcode == DIV && rightValue == 0)) {
                    out.set(F4_TYPE, Float.floatToRawIntBits(Float.NaN), 0);
                    return;
                }
                final float result;
                switch (opcode) {
                    case ADD:
                        result = leftValue + rightValue;
                        break;
                    case SUB:
                        result = leftValue - rightValue;
                        break;
                    case MUL:
                        result = leftValue * rightValue;
                        break;
                    case DIV:
                        result = leftValue / rightValue;
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                }
                out.set(F4_TYPE, Float.floatToRawIntBits(result), 0);
                return;
            }
            case F8_TYPE: {
                final double leftValue = Double.longBitsToDouble(left.lo);
                final double rightValue = Double.longBitsToDouble(right.lo);
                if (Double.isNaN(leftValue) || Double.isNaN(rightValue) || (opcode == DIV && rightValue == 0)) {
                    out.set(F8_TYPE, Double.doubleToRawLongBits(Double.NaN), 0);
                    return;
                }
                final double result;
                switch (opcode) {
                    case ADD:
                        result = leftValue + rightValue;
                        break;
                    case SUB:
                        result = leftValue - rightValue;
                        break;
                    case MUL:
                        result = leftValue * rightValue;
                        break;
                    case DIV:
                        result = leftValue / rightValue;
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                }
                out.set(F8_TYPE, Double.doubleToRawLongBits(result), 0);
                return;
            }
            default:
                throw new IllegalArgumentException("unsupported arithmetic type: " + targetType);
        }
    }

    private static void coerce(ScalarValue source, int targetType, ScalarValue out) {
        if (source.type == targetType) {
            out.set(source.type, source.lo, source.hi);
            return;
        }

        switch (targetType) {
            case I4_TYPE:
                out.set(I4_TYPE, coerceToInt(source), 0);
                return;
            case I8_TYPE:
                out.set(I8_TYPE, coerceToLong(source), 0);
                return;
            case F4_TYPE:
                out.set(F4_TYPE, Float.floatToRawIntBits(coerceToFloat(source)), 0);
                return;
            case F8_TYPE:
                out.set(F8_TYPE, Double.doubleToRawLongBits(coerceToDouble(source)), 0);
                return;
            default:
                throw new IllegalArgumentException("unsupported coercion [sourceType=" + source.type + ", targetType=" + targetType + ']');
        }
    }

    private static int coerceToInt(ScalarValue source) {
        switch (source.type) {
            case I1_TYPE:
            case I2_TYPE:
            case I4_TYPE:
                return (int) source.lo;
            default:
                throw new IllegalArgumentException("cannot coerce to int: " + source.type);
        }
    }

    private static double coerceToDouble(ScalarValue source) {
        switch (source.type) {
            case I1_TYPE:
            case I2_TYPE:
                return (int) source.lo;
            case I4_TYPE:
                if ((int) source.lo == Numbers.INT_NULL) {
                    return Double.NaN;
                }
                return (int) source.lo;
            case I8_TYPE:
                if (source.lo == Numbers.LONG_NULL) {
                    return Double.NaN;
                }
                return source.lo;
            case F4_TYPE:
                return Float.intBitsToFloat((int) source.lo);
            case F8_TYPE:
                return Double.longBitsToDouble(source.lo);
            default:
                throw new IllegalArgumentException("cannot coerce to double: " + source.type);
        }
    }

    private static float coerceToFloat(ScalarValue source) {
        switch (source.type) {
            case I1_TYPE:
            case I2_TYPE:
                return (int) source.lo;
            case I4_TYPE:
                if ((int) source.lo == Numbers.INT_NULL) {
                    return Float.NaN;
                }
                return (int) source.lo;
            case F4_TYPE:
                return Float.intBitsToFloat((int) source.lo);
            default:
                throw new IllegalArgumentException("cannot coerce to float: " + source.type);
        }
    }

    private static long coerceToLong(ScalarValue source) {
        switch (source.type) {
            case I1_TYPE:
            case I2_TYPE:
                return source.lo;
            case I4_TYPE:
                if ((int) source.lo == Numbers.INT_NULL) {
                    return Numbers.LONG_NULL;
                }
                return (int) source.lo;
            case I8_TYPE:
                return source.lo;
            default:
                throw new IllegalArgumentException("cannot coerce to long: " + source.type);
        }
    }

    private static void compare(
            ScalarValue lhs,
            ScalarValue rhs,
            ScalarValue out,
            ScalarValue left,
            ScalarValue right,
            int opcode,
            boolean nullChecks
    ) {
        final int targetType = comparisonType(lhs.type, rhs.type);
        coerce(lhs, targetType, left);
        coerce(rhs, targetType, right);

        final boolean result;
        switch (targetType) {
            case I4_TYPE: {
                final int leftValue = (int) left.lo;
                final int rightValue = (int) right.lo;
                if (nullChecks && (leftValue == Numbers.INT_NULL || rightValue == Numbers.INT_NULL)) {
                    result = intNullCompare(leftValue, rightValue, opcode);
                } else {
                    result = compareLongs(leftValue, rightValue, opcode);
                }
                break;
            }
            case I8_TYPE: {
                final long leftValue = left.lo;
                final long rightValue = right.lo;
                if (nullChecks && (leftValue == Numbers.LONG_NULL || rightValue == Numbers.LONG_NULL)) {
                    result = longNullCompare(leftValue, rightValue, opcode);
                } else {
                    result = compareLongs(leftValue, rightValue, opcode);
                }
                break;
            }
            case F4_TYPE: {
                final float leftValue = Float.intBitsToFloat((int) left.lo);
                final float rightValue = Float.intBitsToFloat((int) right.lo);
                result = compareFloating(leftValue, rightValue, FLOAT_EPSILON, opcode);
                break;
            }
            case F8_TYPE: {
                final double leftValue = Double.longBitsToDouble(left.lo);
                final double rightValue = Double.longBitsToDouble(right.lo);
                result = compareFloating(leftValue, rightValue, DOUBLE_EPSILON, opcode);
                break;
            }
            case I16_TYPE: {
                final boolean eq = lhs.lo == rhs.lo && lhs.hi == rhs.hi;
                switch (opcode) {
                    case EQ:
                        result = eq;
                        break;
                    case NE:
                        result = !eq;
                        break;
                    default:
                        throw new IllegalArgumentException("unsupported i128 comparison opcode: " + opcode);
                }
                break;
            }
            default:
                throw new IllegalArgumentException("unsupported comparison type: " + targetType);
        }
        out.set(I1_TYPE, result ? 1 : 0, 0);
    }

    private static boolean intNullCompare(int leftValue, int rightValue, int opcode) {
        switch (opcode) {
            case EQ:
                return leftValue == rightValue;
            case NE:
                return leftValue != rightValue;
            case LE:
            case GE:
                return leftValue == rightValue;
            default: // LT, GT
                return false;
        }
    }

    private static boolean longNullCompare(long leftValue, long rightValue, int opcode) {
        switch (opcode) {
            case EQ:
                return leftValue == rightValue;
            case NE:
                return leftValue != rightValue;
            case LE:
            case GE:
                return leftValue == rightValue;
            default: // LT, GT
                return false;
        }
    }

    private static boolean compareFloating(double leftValue, double rightValue, double epsilon, int opcode) {
        final boolean leftNaN = Double.isNaN(leftValue);
        final boolean rightNaN = Double.isNaN(rightValue);
        if (opcode == EQ || opcode == NE) {
            final boolean eq = leftNaN || rightNaN
                    ? leftNaN && rightNaN
                    : Math.abs(leftValue - rightValue) <= epsilon;
            return opcode == EQ ? eq : !eq;
        }

        if (leftNaN || rightNaN) {
            if (!(leftNaN && rightNaN)) {
                return false;
            }
            return opcode == LE || opcode == GE;
        }

        final boolean eq = Math.abs(leftValue - rightValue) <= epsilon;
        switch (opcode) {
            case GT:
                return !eq && leftValue > rightValue;
            case GE:
                return eq || leftValue >= rightValue;
            case LT:
                return !eq && leftValue < rightValue;
            case LE:
                return eq || leftValue <= rightValue;
            default:
                throw new IllegalArgumentException("unsupported floating comparison opcode: " + opcode);
        }
    }

    private static boolean compareLongs(long leftValue, long rightValue, int opcode) {
        switch (opcode) {
            case EQ:
                return leftValue == rightValue;
            case NE:
                return leftValue != rightValue;
            case LT:
                return leftValue < rightValue;
            case LE:
                return leftValue <= rightValue;
            case GT:
                return leftValue > rightValue;
            case GE:
                return leftValue >= rightValue;
            default:
                throw new IllegalArgumentException("unsupported integer comparison opcode: " + opcode);
        }
    }

    private static int comparisonType(int leftType, int rightType) {
        final int left = normalizeVarSizeType(leftType);
        final int right = normalizeVarSizeType(rightType);
        if (left == I16_TYPE || right == I16_TYPE) {
            if (left != I16_TYPE || right != I16_TYPE) {
                throw new IllegalArgumentException("unsupported mixed i128 comparison");
            }
            return I16_TYPE;
        }
        if (left == F8_TYPE || right == F8_TYPE || ((left == I8_TYPE || right == I8_TYPE) && (left == F4_TYPE || right == F4_TYPE))) {
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

    private static int normalizeVarSizeType(int type) {
        switch (type) {
            case STRING_HEADER_TYPE:
                return I4_TYPE;
            case BINARY_HEADER_TYPE:
            case VARCHAR_HEADER_TYPE:
                return I8_TYPE;
            default:
                return type;
        }
    }

    private int[] computeJumpTargets(IrDecoder.Instruction[] decoded) throws SqlException {
        final int[] targets = new int[decoded.length];
        Arrays.fill(targets, -1);
        for (int pc = 0, n = decoded.length; pc < n; pc++) {
            final IrDecoder.Instruction instruction = decoded[pc];
            final int opcode = instruction.opcode();
            if (opcode == AND_SC || opcode == OR_SC) {
                final int labelIndex = Math.toIntExact(instruction.payloadLo());
                if (labelIndex < 0) {
                    throw SqlException.position(0).put("vector backend label index out of range [index=").put(labelIndex).put(']');
                }
                if (labelIndex >= 2) {
                    int target = findNextEndSc(decoded, pc, labelIndex);
                    if (target < 0) {
                        throw SqlException.position(0).put("vector backend short-circuit label is not bound [index=").put(labelIndex).put(']');
                    }
                    targets[pc] = target;
                }
            }
        }
        return targets;
    }

    private static int findNextEndSc(IrDecoder.Instruction[] decoded, int fromPc, int labelIndex) {
        for (int j = fromPc + 1; j < decoded.length; j++) {
            if (decoded[j].opcode() == END_SC && Math.toIntExact(decoded[j].payloadLo()) == labelIndex) {
                return j + 1;
            }
        }
        return -1;
    }

    private int[] computeVarOffsets(IrDecoder.Instruction[] decoded) throws SqlException {
        int maxVarIndex = -1;
        for (IrDecoder.Instruction instruction : decoded) {
            if (instruction.opcode() == VAR) {
                maxVarIndex = Math.max(maxVarIndex, Math.toIntExact(instruction.payloadLo()));
            }
        }
        if (maxVarIndex < 0) {
            return new int[0];
        }

        final int[] widths = new int[maxVarIndex + 1];
        Arrays.fill(widths, -1);
        for (IrDecoder.Instruction instruction : decoded) {
            if (instruction.opcode() == VAR) {
                final int index = Math.toIntExact(instruction.payloadLo());
                validateType(instruction.type());
                final int width = typeWidth(instruction.type());
                if (width < 0) {
                    throw unsupportedTypeException(instruction.type());
                }
                if (widths[index] != -1 && widths[index] != width) {
                    throw SqlException.position(0).put("vector backend bind variable uses conflicting widths [index=").put(index).put(']');
                }
                widths[index] = width;
            }
        }

        final int[] offsets = new int[maxVarIndex + 1];
        int offset = 0;
        for (int i = 0; i < widths.length; i++) {
            if (widths[i] < 0) {
                throw SqlException.position(0).put("vector backend bind variable index is missing [index=").put(i).put(']');
            }
            offsets[i] = offset;
            offset += widths[i];
        }
        return offsets;
    }

    private static int arithmeticResultType(int leftType, int rightType) {
        if (leftType == I16_TYPE || rightType == I16_TYPE) {
            throw new IllegalArgumentException("unsupported i128 arithmetic");
        }
        if (leftType == F8_TYPE || rightType == F8_TYPE || ((leftType == I8_TYPE || rightType == I8_TYPE) && (leftType == F4_TYPE || rightType == F4_TYPE))) {
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

    private ExecutionState executionState() {
        ExecutionState state = tlState.get();
        if (state == null || state.stack.length < instructions.length + 1) {
            state = new ExecutionState(instructions.length + 1);
            tlState.set(state);
        }
        return state;
    }

    private boolean evaluateRow(
            long dataAddress,
            long dataSize,
            long varSizeAuxAddress,
            long varsAddress,
            long varsSize,
            long row
    ) {
        final ExecutionState state = executionState();
        final ScalarValue[] stack = state.stack;
        final ScalarValue tempLeft = state.tempLeft;
        final ScalarValue tempRight = state.tempRight;
        int sp = 0;
        int pc = 0;

        while (pc < instructions.length) {
            final IrDecoder.Instruction instruction = instructions[pc];
            switch (instruction.opcode()) {
                case IMM:
                    loadImmediate(stack[sp++], instruction);
                    break;
                case MEM:
                    loadMemoryValue(stack[sp++], instruction, dataAddress, varSizeAuxAddress, dataSize, row);
                    break;
                case VAR:
                    loadBindVariable(stack[sp++], instruction, varsAddress, varsSize);
                    break;
                case NEG:
                    unaryNeg(stack[sp - 1], nullChecks);
                    break;
                case NOT:
                    stack[sp - 1].set(I1_TYPE, asBoolean(stack[sp - 1]) ? 0 : 1, 0);
                    break;
                case AND: {
                    final ScalarValue lhs = stack[sp - 1];
                    final ScalarValue rhs = stack[sp - 2];
                    rhs.set(I1_TYPE, asBoolean(lhs) && asBoolean(rhs) ? 1 : 0, 0);
                    sp--;
                    break;
                }
                case OR: {
                    final ScalarValue lhs = stack[sp - 1];
                    final ScalarValue rhs = stack[sp - 2];
                    rhs.set(I1_TYPE, asBoolean(lhs) || asBoolean(rhs) ? 1 : 0, 0);
                    sp--;
                    break;
                }
                case EQ:
                case NE:
                case LT:
                case LE:
                case GT:
                case GE: {
                    final ScalarValue lhs = stack[sp - 1];
                    final ScalarValue rhs = stack[sp - 2];
                    compare(lhs, rhs, rhs, tempLeft, tempRight, instruction.opcode(), nullChecks);
                    sp--;
                    break;
                }
                case ADD:
                case SUB:
                case MUL:
                case DIV: {
                    final ScalarValue lhs = stack[sp - 1];
                    final ScalarValue rhs = stack[sp - 2];
                    binaryArithmetic(lhs, rhs, rhs, tempLeft, tempRight, instruction.opcode(), nullChecks);
                    sp--;
                    break;
                }
                case AND_SC: {
                    final boolean value = asBoolean(stack[--sp]);
                    if (!value) {
                        final int labelIndex = Math.toIntExact(instruction.payloadLo());
                        if (labelIndex == 0) {
                            return false;
                        }
                        if (labelIndex == 1) {
                            return true;
                        }
                        pc = jumpTargets[pc];
                        continue;
                    }
                    break;
                }
                case OR_SC: {
                    final boolean value = asBoolean(stack[--sp]);
                    if (value) {
                        final int labelIndex = Math.toIntExact(instruction.payloadLo());
                        if (labelIndex == 0) {
                            return false;
                        }
                        if (labelIndex == 1) {
                            return true;
                        }
                        pc = jumpTargets[pc];
                        continue;
                    }
                    break;
                }
                case BEGIN_SC:
                case END_SC:
                    break;
                case RET:
                    return sp == 0 || asBoolean(stack[sp - 1]);
                default:
                    throw new IllegalArgumentException("unsupported vector backend opcode: " + instruction.opcode());
            }
            pc++;
        }
        return sp == 0 || asBoolean(stack[sp - 1]);
    }

    private void loadBindVariable(ScalarValue out, IrDecoder.Instruction instruction, long varsAddress, long varsSize) {
        final int index = Math.toIntExact(instruction.payloadLo());
        if (index < 0 || index >= varsSize) {
            throw new IllegalArgumentException("bind variable index out of bounds [index=" + index + ", count=" + varsSize + ']');
        }
        final long address = varsAddress + varOffsets[index];
        switch (instruction.type()) {
            case I1_TYPE:
            case I2_TYPE:
            case I4_TYPE:
            case I8_TYPE:
                out.set(instruction.type(), UNSAFE.getLong(address), 0);
                return;
            case F4_TYPE:
                out.set(F4_TYPE, Float.floatToRawIntBits(UNSAFE.getFloat(address)), 0);
                return;
            case F8_TYPE:
                out.set(F8_TYPE, Double.doubleToRawLongBits(UNSAFE.getDouble(address)), 0);
                return;
            case I16_TYPE:
                out.set(I16_TYPE, UNSAFE.getLong(address), UNSAFE.getLong(address + Long.BYTES));
                return;
            default:
                throw new IllegalArgumentException("unsupported bind variable type: " + instruction.type());
        }
    }

    private static void loadImmediate(ScalarValue out, IrDecoder.Instruction instruction) {
        switch (instruction.type()) {
            case I1_TYPE:
            case I2_TYPE:
            case I4_TYPE:
            case I8_TYPE:
            case I16_TYPE:
                out.set(instruction.type(), instruction.payloadLo(), instruction.payloadHi());
                return;
            case F4_TYPE:
                out.set(F4_TYPE, Float.floatToRawIntBits((float) instruction.doublePayload()), 0);
                return;
            case F8_TYPE:
                out.set(F8_TYPE, Double.doubleToRawLongBits(instruction.doublePayload()), 0);
                return;
            default:
                throw new IllegalArgumentException("unsupported immediate type: " + instruction.type());
        }
    }

    private void loadMemoryValue(ScalarValue out, IrDecoder.Instruction instruction, long dataAddress, long varSizeAuxAddress, long dataSize, long row) {
        final int columnIndex = Math.toIntExact(instruction.payloadLo());
        if (columnIndex < 0 || columnIndex >= dataSize) {
            throw new IllegalArgumentException("column index out of bounds [index=" + columnIndex + ", count=" + dataSize + ']');
        }

        final long columnAddress = UNSAFE.getLong(dataAddress + ((long) columnIndex << 3));
        switch (instruction.type()) {
            case I1_TYPE:
                out.set(I1_TYPE, columnAddress != 0 ? UNSAFE.getByte(columnAddress + row) : 0, 0);
                return;
            case I2_TYPE:
                out.set(I2_TYPE, columnAddress != 0 ? UNSAFE.getShort(columnAddress + (row << 1)) : 0, 0);
                return;
            case I4_TYPE:
                out.set(I4_TYPE, columnAddress != 0 ? UNSAFE.getInt(columnAddress + (row << 2)) : Numbers.INT_NULL, 0);
                return;
            case I8_TYPE:
                out.set(I8_TYPE, columnAddress != 0 ? UNSAFE.getLong(columnAddress + (row << 3)) : Numbers.LONG_NULL, 0);
                return;
            case F4_TYPE:
                out.set(F4_TYPE, Float.floatToRawIntBits(columnAddress != 0 ? UNSAFE.getFloat(columnAddress + (row << 2)) : Float.NaN), 0);
                return;
            case F8_TYPE:
                out.set(F8_TYPE, Double.doubleToRawLongBits(columnAddress != 0 ? UNSAFE.getDouble(columnAddress + (row << 3)) : Double.NaN), 0);
                return;
            case I16_TYPE: {
                final long offset = row << 4;
                out.set(I16_TYPE,
                        columnAddress != 0 ? UNSAFE.getLong(columnAddress + offset) : Numbers.LONG_NULL,
                        columnAddress != 0 ? UNSAFE.getLong(columnAddress + offset + Long.BYTES) : Numbers.LONG_NULL
                );
                return;
            }
            case STRING_HEADER_TYPE: {
                if (columnAddress == 0) {
                    out.set(I4_TYPE, -1, 0);
                    return;
                }
                final long auxAddress = UNSAFE.getLong(varSizeAuxAddress + ((long) columnIndex << 3));
                final long dataOffset = UNSAFE.getLong(auxAddress + (row << 3));
                out.set(I4_TYPE, UNSAFE.getInt(columnAddress + dataOffset), 0);
                return;
            }
            case BINARY_HEADER_TYPE: {
                if (columnAddress == 0) {
                    out.set(I8_TYPE, -1L, 0);
                    return;
                }
                final long auxAddress = UNSAFE.getLong(varSizeAuxAddress + ((long) columnIndex << 3));
                final long dataOffset = UNSAFE.getLong(auxAddress + (row << 3));
                out.set(I8_TYPE, UNSAFE.getLong(columnAddress + dataOffset), 0);
                return;
            }
            case VARCHAR_HEADER_TYPE: {
                if (columnAddress == 0) {
                    out.set(I8_TYPE, 0, 0);
                    return;
                }
                final long auxAddress = UNSAFE.getLong(varSizeAuxAddress + ((long) columnIndex << 3));
                out.set(I8_TYPE, UNSAFE.getLong(auxAddress + (row << 4)), 0);
                return;
            }
            default:
                throw new IllegalArgumentException("unsupported memory type: " + instruction.type());
        }
    }

    private static void unaryNeg(ScalarValue value, boolean nullChecks) {
        switch (value.type) {
            case I1_TYPE:
            case I2_TYPE:
            case I4_TYPE: {
                final int intValue = (int) value.lo;
                value.set(I4_TYPE, nullChecks && intValue == Numbers.INT_NULL ? Numbers.INT_NULL : -intValue, 0);
                return;
            }
            case I8_TYPE:
                value.set(I8_TYPE, nullChecks && value.lo == Numbers.LONG_NULL ? Numbers.LONG_NULL : -value.lo, 0);
                return;
            case F4_TYPE:
                value.set(F4_TYPE, Float.floatToRawIntBits(-Float.intBitsToFloat((int) value.lo)), 0);
                return;
            case F8_TYPE:
                value.set(F8_TYPE, Double.doubleToRawLongBits(-Double.longBitsToDouble(value.lo)), 0);
                return;
            default:
                throw new IllegalArgumentException("unsupported negation type: " + value.type);
        }
    }

    private static int typeWidth(int type) {
        switch (type) {
            case I1_TYPE:
            case I2_TYPE:
            case I4_TYPE:
            case F4_TYPE:
            case I8_TYPE:
            case F8_TYPE:
            case STRING_HEADER_TYPE:
            case BINARY_HEADER_TYPE:
            case VARCHAR_HEADER_TYPE:
                return 8;
            case I16_TYPE:
                return 16;
            default:
                return -1;
        }
    }

    private static SqlException unsupportedOpcodeException(int opcode) {
        return SqlException.position(0).put("unsupported vector backend opcode [opcode=").put(opcode).put(']');
    }

    private static SqlException unsupportedTypeException(int type) {
        return SqlException.position(0).put("unsupported vector backend type [type=").put(type).put(']');
    }

    private static void validateType(int type) throws SqlException {
        switch (type) {
            case I1_TYPE:
            case I2_TYPE:
            case I4_TYPE:
            case I8_TYPE:
            case F4_TYPE:
            case F8_TYPE:
            case I16_TYPE:
            case STRING_HEADER_TYPE:
            case BINARY_HEADER_TYPE:
            case VARCHAR_HEADER_TYPE:
                return;
            default:
                throw unsupportedTypeException(type);
        }
    }

    private void validateInstructions(IrDecoder.Instruction[] decoded) throws SqlException {
        final int[] stack = new int[decoded.length + 1];
        int sp = 0;
        for (IrDecoder.Instruction instruction : decoded) {
            switch (instruction.opcode()) {
                case IMM:
                case MEM:
                case VAR:
                    validateType(instruction.type());
                    stack[sp++] = instruction.type();
                    break;
                case RET:
                    break;
                case NEG:
                    if (sp < 1) {
                        throw SqlException.position(0).put("vector backend IR stack underflow");
                    }
                    stack[sp - 1] = arithmeticResultType(stack[sp - 1], stack[sp - 1]);
                    break;
                case NOT:
                    if (sp < 1) {
                        throw SqlException.position(0).put("vector backend IR stack underflow");
                    }
                    stack[sp - 1] = I1_TYPE;
                    break;
                case AND:
                case OR:
                    if (sp < 2) {
                        throw SqlException.position(0).put("vector backend IR stack underflow");
                    }
                    sp--;
                    stack[sp - 1] = I1_TYPE;
                    break;
                case EQ:
                case NE:
                case LT:
                case LE:
                case GT:
                case GE:
                    if (sp < 2) {
                        throw SqlException.position(0).put("vector backend IR stack underflow");
                    }
                    comparisonType(stack[sp - 2], stack[sp - 1]);
                    sp--;
                    stack[sp - 1] = I1_TYPE;
                    break;
                case ADD:
                case SUB:
                case MUL:
                case DIV:
                    if (sp < 2) {
                        throw SqlException.position(0).put("vector backend IR stack underflow");
                    }
                    stack[sp - 2] = arithmeticResultType(stack[sp - 2], stack[sp - 1]);
                    sp--;
                    break;
                case AND_SC:
                case OR_SC:
                    if (sp < 1) {
                        throw SqlException.position(0).put("vector backend IR stack underflow");
                    }
                    sp--;
                    break;
                case BEGIN_SC:
                case END_SC:
                    break;
                default:
                    throw unsupportedOpcodeException(instruction.opcode());
            }
        }
    }

    private int[] computeVarOffsetsAndValidate(IrDecoder.Instruction[] decoded) throws SqlException {
        validateInstructions(decoded);
        return computeVarOffsets(decoded);
    }

    private static final class ExecutionState {
        private final ScalarValue[] stack;
        private final ScalarValue tempLeft = new ScalarValue();
        private final ScalarValue tempRight = new ScalarValue();

        private ExecutionState(int stackSize) {
            stack = new ScalarValue[stackSize];
            for (int i = 0; i < stackSize; i++) {
                stack[i] = new ScalarValue();
            }
        }
    }

    private static final class ScalarValue {
        private long hi;
        private long lo;
        private int type;

        private void set(int type, long lo, long hi) {
            this.type = type;
            this.lo = lo;
            this.hi = hi;
        }
    }
}
