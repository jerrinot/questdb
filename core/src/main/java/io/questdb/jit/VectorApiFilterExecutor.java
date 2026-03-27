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

import io.questdb.std.Numbers;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static io.questdb.jit.CompiledFilterIRSerializer.ADD;
import static io.questdb.jit.CompiledFilterIRSerializer.AND;
import static io.questdb.jit.CompiledFilterIRSerializer.DIV;
import static io.questdb.jit.CompiledFilterIRSerializer.EQ;
import static io.questdb.jit.CompiledFilterIRSerializer.F4_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.F8_TYPE;
import static io.questdb.jit.CompiledFilterIRSerializer.GE;
import static io.questdb.jit.CompiledFilterIRSerializer.GT;
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
import static io.questdb.jit.CompiledFilterIRSerializer.RET;
import static io.questdb.jit.CompiledFilterIRSerializer.SUB;
import static io.questdb.jit.CompiledFilterIRSerializer.VAR;

abstract class VectorApiFilterExecutor {
    private static final int EXEC_HINT_SHIFT = 4;
    private static final int EXEC_HINT_SINGLE_SIZE = 1;
    private static final int KIND_MASK = 1;
    private static final int KIND_VECTOR = 0;
    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();
    private static final double DOUBLE_EPSILON = 1e-10;
    private static final float FLOAT_EPSILON = 1e-10f;
    private static final ValueLayout.OfByte NATIVE_BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfDouble NATIVE_DOUBLE = ValueLayout.JAVA_DOUBLE.withOrder(NATIVE_ORDER);
    private static final ValueLayout.OfFloat NATIVE_FLOAT = ValueLayout.JAVA_FLOAT.withOrder(NATIVE_ORDER);
    private static final ValueLayout.OfInt NATIVE_INT = ValueLayout.JAVA_INT.withOrder(NATIVE_ORDER);
    private static final ValueLayout.OfLong NATIVE_LONG = ValueLayout.JAVA_LONG.withOrder(NATIVE_ORDER);
    private static final ValueLayout.OfShort NATIVE_SHORT = ValueLayout.JAVA_SHORT.withOrder(NATIVE_ORDER);

    protected final IrDecoder.Instruction[] instructions;
    protected final boolean nullChecks;
    protected final int[] varOffsets;
    protected final int varsSizeBytes;

    protected VectorApiFilterExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
        this.instructions = instructions;
        this.nullChecks = nullChecks;
        this.varOffsets = varOffsets;
        this.varsSizeBytes = varOffsets.length == 0 ? 0 : varOffsets[varOffsets.length - 1] + Long.BYTES;
    }

    public static VectorApiFilterExecutor tryCreate(
            IrDecoder.Instruction[] instructions,
            int[] varOffsets,
            int options,
            boolean nullChecks
    ) {
        if (((options >> EXEC_HINT_SHIFT) & 0x3) != EXEC_HINT_SINGLE_SIZE) {
            return null;
        }

        final ProgramSpec spec = analyze(instructions);
        if (spec == null) {
            return null;
        }

        return switch (spec.programType) {
            case I1_TYPE -> new ByteVectorExecutor(instructions, nullChecks, varOffsets);
            case I2_TYPE -> new ShortVectorExecutor(instructions, nullChecks, varOffsets);
            case I4_TYPE -> new IntVectorExecutor(instructions, nullChecks, varOffsets);
            case I8_TYPE -> new LongVectorExecutor(instructions, nullChecks, varOffsets);
            case F4_TYPE -> new FloatVectorExecutor(instructions, nullChecks, varOffsets);
            case F8_TYPE -> new DoubleVectorExecutor(instructions, nullChecks, varOffsets);
            case MIXED_I4_F4_TYPE -> new IntFloatVectorExecutor(instructions, nullChecks, varOffsets);
            case MIXED_I8_F8_TYPE -> new LongDoubleVectorExecutor(instructions, nullChecks, varOffsets);
            default -> null;
        };
    }

    public abstract long filter(
            long dataAddress,
            long dataSize,
            long varsAddress,
            long filteredRowsAddress,
            long rowsCount
    );

    public abstract long filterCount(
            long dataAddress,
            long dataSize,
            long varsAddress,
            long rowsCount
    );

    private static ProgramSpec analyze(IrDecoder.Instruction[] instructions) {
        int programType = -1;
        final int[] stack = new int[instructions.length + 1];
        int sp = 0;

        for (IrDecoder.Instruction instruction : instructions) {
            switch (instruction.opcode()) {
                case IMM:
                case MEM:
                case VAR: {
                    final int type = instruction.type();
                    if (type != I1_TYPE && type != I2_TYPE && type != I4_TYPE && type != I8_TYPE && type != F4_TYPE && type != F8_TYPE) {
                        return null;
                    }
                    if (programType == -1) {
                        programType = type;
                    } else if (programType != type) {
                        if ((programType == I4_TYPE && type == F4_TYPE) || (programType == F4_TYPE && type == I4_TYPE)) {
                            programType = MIXED_I4_F4_TYPE;
                        } else if ((programType == I8_TYPE && type == F8_TYPE) || (programType == F8_TYPE && type == I8_TYPE)) {
                            programType = MIXED_I8_F8_TYPE;
                        } else if (!isCompatibleProgramType(programType, type)) {
                            return null;
                        }
                    }
                    if (!isCompatibleProgramType(programType, type)) {
                        return null;
                    }
                    stack[sp++] = KIND_VECTOR;
                    break;
                }
                case NEG:
                    if (sp < 1 || stack[sp - 1] != KIND_VECTOR) {
                        return null;
                    }
                    if (programType == I1_TYPE || programType == I2_TYPE) {
                        return null;
                    }
                    break;
                case NOT:
                    if (sp < 1 || stack[sp - 1] != KIND_MASK) {
                        return null;
                    }
                    break;
                case AND:
                case OR:
                    if (sp < 2 || stack[sp - 1] != KIND_MASK || stack[sp - 2] != KIND_MASK) {
                        return null;
                    }
                    sp--;
                    stack[sp - 1] = KIND_MASK;
                    break;
                case EQ:
                case NE:
                case LT:
                case LE:
                case GT:
                case GE:
                    if (sp < 2 || stack[sp - 1] != KIND_VECTOR || stack[sp - 2] != KIND_VECTOR) {
                        return null;
                    }
                    sp--;
                    stack[sp - 1] = KIND_MASK;
                    break;
                case ADD:
                case SUB:
                case MUL:
                case DIV:
                    if (sp < 2 || stack[sp - 1] != KIND_VECTOR || stack[sp - 2] != KIND_VECTOR) {
                        return null;
                    }
                    if (programType == I1_TYPE || programType == I2_TYPE) {
                        return null;
                    }
                    sp--;
                    stack[sp - 1] = KIND_VECTOR;
                    break;
                case RET:
                    if (sp != 1 || stack[0] != KIND_MASK) {
                        return null;
                    }
                    break;
                default:
                    return null;
            }
        }

        return programType == -1 ? null : new ProgramSpec(programType);
    }

    private static boolean isCompatibleProgramType(int programType, int type) {
        return switch (programType) {
            case I1_TYPE -> type == I1_TYPE;
            case I2_TYPE -> type == I2_TYPE;
            case I4_TYPE -> type == I4_TYPE;
            case I8_TYPE -> type == I8_TYPE;
            case F4_TYPE -> type == F4_TYPE;
            case F8_TYPE -> type == F8_TYPE;
            case MIXED_I4_F4_TYPE -> type == I4_TYPE || type == F4_TYPE;
            case MIXED_I8_F8_TYPE -> type == I8_TYPE || type == F8_TYPE;
            default -> false;
        };
    }

    protected static MemorySegment[] prepareColumnSegments(long dataAddress, long dataSize, long rowsCount, long elementBytes) {
        final int columnCount = Math.toIntExact(dataSize);
        final MemorySegment addressSegment = MemorySegment.ofAddress(dataAddress).reinterpret((long) columnCount * Long.BYTES);
        final MemorySegment[] segments = new MemorySegment[columnCount];
        final long byteSize = rowsCount * elementBytes;
        for (int i = 0; i < columnCount; i++) {
            final long columnAddress = addressSegment.getAtIndex(NATIVE_LONG, i);
            segments[i] = columnAddress == 0 ? null : MemorySegment.ofAddress(columnAddress).reinterpret(byteSize);
        }
        return segments;
    }

    protected static MemorySegment prepareOutputSegment(long filteredRowsAddress, long rowsCount) {
        return MemorySegment.ofAddress(filteredRowsAddress).reinterpret(rowsCount * Long.BYTES);
    }

    protected MemorySegment prepareVarsSegment(long varsAddress) {
        if (varsSizeBytes == 0 || varsAddress == 0) {
            return null;
        }
        return MemorySegment.ofAddress(varsAddress).reinterpret(varsSizeBytes);
    }

    private static final class ProgramSpec {
        private final int programType;

        private ProgramSpec(int programType) {
            this.programType = programType;
        }
    }

    private static final int MIXED_I4_F4_TYPE = 1001;
    private static final int MIXED_I8_F8_TYPE = 1002;

    private static final class ByteVectorExecutor extends VectorApiFilterExecutor {
        private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

        private final ThreadLocal<ByteExecutionState> tlState = new ThreadLocal<>();

        private ByteVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Byte.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final ByteExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Byte> activeMask = SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Byte> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Byte.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final ByteExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Byte> activeMask = SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private ByteExecutionState executionState() {
            ByteExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new ByteExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Byte> evaluate(
                ByteExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Byte> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        state.stack[sp++].setVector(ByteVector.broadcast(SPECIES, (byte) instruction.payloadLo()));
                        break;
                    case MEM:
                        state.stack[sp++].setVector(loadMemory(columnSegments, instruction, row, activeMask));
                        break;
                    case VAR:
                        state.stack[sp++].setVector(loadVar(varsSegment, instruction));
                        break;
                    case NEG:
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV:
                        throw new IllegalArgumentException("unsupported byte vector opcode: " + instruction.opcode());
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final ByteSlot lhs = state.stack[sp - 1];
                        final ByteSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final ByteSlot lhs = state.stack[sp - 1];
                        final ByteSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final ByteSlot lhs = state.stack[sp - 1];
                        final ByteSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private ByteVector loadMemory(
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Byte> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            if (segment == null) {
                return ByteVector.zero(SPECIES);
            }
            return ByteVector.fromMemorySegment(SPECIES, segment, row, NATIVE_ORDER, activeMask);
        }

        private ByteVector loadVar(MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final byte value = varsSegment.get(NATIVE_BYTE, varOffsets[Math.toIntExact(instruction.payloadLo())]);
            return ByteVector.broadcast(SPECIES, value);
        }

        private VectorMask<Byte> compare(ByteVector lhs, ByteVector rhs, int opcode) {
            return switch (opcode) {
                case EQ -> lhs.compare(VectorOperators.EQ, rhs);
                case NE -> lhs.compare(VectorOperators.NE, rhs);
                case LT -> lhs.compare(VectorOperators.LT, rhs);
                case LE -> lhs.compare(VectorOperators.LE, rhs);
                case GT -> lhs.compare(VectorOperators.GT, rhs);
                case GE -> lhs.compare(VectorOperators.GE, rhs);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private static final class ByteExecutionState {
            private final ByteSlot[] stack;

            private ByteExecutionState(int stackSize) {
                this.stack = new ByteSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new ByteSlot();
                }
            }
        }

        private static final class ByteSlot {
            private VectorMask<Byte> mask;
            private ByteVector vector;

            private void setMask(VectorMask<Byte> mask) {
                this.mask = mask;
                this.vector = null;
            }

            private void setVector(ByteVector vector) {
                this.vector = vector;
                this.mask = null;
            }
        }
    }

    private static final class ShortVectorExecutor extends VectorApiFilterExecutor {
        private static final VectorSpecies<Short> SPECIES = ShortVector.SPECIES_PREFERRED;

        private final ThreadLocal<ShortExecutionState> tlState = new ThreadLocal<>();

        private ShortVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Short.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final ShortExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Short> activeMask = SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Short> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Short.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final ShortExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Short> activeMask = SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private ShortExecutionState executionState() {
            ShortExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new ShortExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Short> evaluate(
                ShortExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Short> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        state.stack[sp++].setVector(ShortVector.broadcast(SPECIES, (short) instruction.payloadLo()));
                        break;
                    case MEM:
                        state.stack[sp++].setVector(loadMemory(columnSegments, instruction, row, activeMask));
                        break;
                    case VAR:
                        state.stack[sp++].setVector(loadVar(varsSegment, instruction));
                        break;
                    case NEG:
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV:
                        throw new IllegalArgumentException("unsupported short vector opcode: " + instruction.opcode());
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final ShortSlot lhs = state.stack[sp - 1];
                        final ShortSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final ShortSlot lhs = state.stack[sp - 1];
                        final ShortSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final ShortSlot lhs = state.stack[sp - 1];
                        final ShortSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private ShortVector loadMemory(
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Short> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            if (segment == null) {
                return ShortVector.zero(SPECIES);
            }
            return ShortVector.fromMemorySegment(SPECIES, segment, row * Short.BYTES, NATIVE_ORDER, activeMask);
        }

        private ShortVector loadVar(MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final short value = varsSegment.get(NATIVE_SHORT, varOffsets[Math.toIntExact(instruction.payloadLo())]);
            return ShortVector.broadcast(SPECIES, value);
        }

        private VectorMask<Short> compare(ShortVector lhs, ShortVector rhs, int opcode) {
            return switch (opcode) {
                case EQ -> lhs.compare(VectorOperators.EQ, rhs);
                case NE -> lhs.compare(VectorOperators.NE, rhs);
                case LT -> lhs.compare(VectorOperators.LT, rhs);
                case LE -> lhs.compare(VectorOperators.LE, rhs);
                case GT -> lhs.compare(VectorOperators.GT, rhs);
                case GE -> lhs.compare(VectorOperators.GE, rhs);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private static final class ShortExecutionState {
            private final ShortSlot[] stack;

            private ShortExecutionState(int stackSize) {
                this.stack = new ShortSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new ShortSlot();
                }
            }
        }

        private static final class ShortSlot {
            private VectorMask<Short> mask;
            private ShortVector vector;

            private void setMask(VectorMask<Short> mask) {
                this.mask = mask;
                this.vector = null;
            }

            private void setVector(ShortVector vector) {
                this.vector = vector;
                this.mask = null;
            }
        }
    }

    private static final class IntVectorExecutor extends VectorApiFilterExecutor {
        private static final IntVector INT_NULL_VECTOR = IntVector.broadcast(IntVector.SPECIES_PREFERRED, Numbers.INT_NULL);
        private static final IntVector ZERO_VECTOR = IntVector.zero(IntVector.SPECIES_PREFERRED);
        private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

        private final ThreadLocal<IntExecutionState> tlState = new ThreadLocal<>();

        private IntVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Integer.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final IntExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Integer> activeMask = SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Integer> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Integer.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final IntExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Integer> activeMask = SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private IntExecutionState executionState() {
            IntExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new IntExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Integer> evaluate(
                IntExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Integer> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        state.stack[sp++].setVector(IntVector.broadcast(SPECIES, (int) instruction.payloadLo()));
                        break;
                    case MEM:
                        state.stack[sp++].setVector(loadMemory(columnSegments, instruction, row, activeMask));
                        break;
                    case VAR:
                        state.stack[sp++].setVector(loadVar(varsSegment, instruction));
                        break;
                    case NEG: {
                        final IntSlot slot = state.stack[sp - 1];
                        IntVector negated = slot.vector.neg();
                        if (nullChecks) {
                            final VectorMask<Integer> nullMask = slot.vector.compare(VectorOperators.EQ, INT_NULL_VECTOR);
                            negated = negated.blend(INT_NULL_VECTOR, nullMask);
                        }
                        slot.setVector(negated);
                        break;
                    }
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final IntSlot lhs = state.stack[sp - 1];
                        final IntSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final IntSlot lhs = state.stack[sp - 1];
                        final IntSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final IntSlot lhs = state.stack[sp - 1];
                        final IntSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV: {
                        final IntSlot lhs = state.stack[sp - 1];
                        final IntSlot rhs = state.stack[sp - 2];
                        rhs.setVector(arithmetic(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private IntVector loadMemory(
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Integer> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            if (segment == null) {
                return INT_NULL_VECTOR;
            }
            return IntVector.fromMemorySegment(SPECIES, segment, row * Integer.BYTES, NATIVE_ORDER, activeMask);
        }

        private IntVector loadVar(MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final int value = varsSegment.get(NATIVE_INT, varOffsets[Math.toIntExact(instruction.payloadLo())]);
            return IntVector.broadcast(SPECIES, value);
        }

        private IntVector arithmetic(IntVector lhs, IntVector rhs, int opcode) {
            if (!nullChecks) {
                return switch (opcode) {
                    case ADD -> lhs.add(rhs);
                    case SUB -> lhs.sub(rhs);
                    case MUL -> lhs.mul(rhs);
                    case DIV -> lhs.div(rhs);
                    default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                };
            }

            final VectorMask<Integer> invalidMask = lhs.compare(VectorOperators.EQ, INT_NULL_VECTOR)
                    .or(rhs.compare(VectorOperators.EQ, INT_NULL_VECTOR))
                    .or(opcode == DIV ? rhs.compare(VectorOperators.EQ, ZERO_VECTOR) : SPECIES.maskAll(false));
            IntVector result = switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs, invalidMask.not());
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
            return result.blend(INT_NULL_VECTOR, invalidMask);
        }

        private VectorMask<Integer> compare(IntVector lhs, IntVector rhs, int opcode) {
            if (!nullChecks || opcode == EQ || opcode == NE) {
                return switch (opcode) {
                    case EQ -> lhs.compare(VectorOperators.EQ, rhs);
                    case NE -> lhs.compare(VectorOperators.NE, rhs);
                    case LT -> lhs.compare(VectorOperators.LT, rhs);
                    case LE -> lhs.compare(VectorOperators.LE, rhs);
                    case GT -> lhs.compare(VectorOperators.GT, rhs);
                    case GE -> lhs.compare(VectorOperators.GE, rhs);
                    default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
                };
            }

            final VectorMask<Integer> leftNull = lhs.compare(VectorOperators.EQ, INT_NULL_VECTOR);
            final VectorMask<Integer> rightNull = rhs.compare(VectorOperators.EQ, INT_NULL_VECTOR);
            final VectorMask<Integer> anyNull = leftNull.or(rightNull);
            final VectorMask<Integer> bothNull = leftNull.and(rightNull);
            return switch (opcode) {
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNull.not());
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNull.not());
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNull.not()).or(bothNull);
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNull.not()).or(bothNull);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private static final class IntExecutionState {
            private final IntSlot[] stack;

            private IntExecutionState(int stackSize) {
                this.stack = new IntSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new IntSlot();
                }
            }
        }

        private static final class IntSlot {
            private VectorMask<Integer> mask;
            private IntVector vector;

            private void setMask(VectorMask<Integer> mask) {
                this.mask = mask;
                this.vector = null;
            }

            private void setVector(IntVector vector) {
                this.vector = vector;
                this.mask = null;
            }
        }
    }

    private static final class LongVectorExecutor extends VectorApiFilterExecutor {
        private static final LongVector LONG_NULL_VECTOR = LongVector.broadcast(LongVector.SPECIES_PREFERRED, Numbers.LONG_NULL);
        private static final LongVector ZERO_VECTOR = LongVector.zero(LongVector.SPECIES_PREFERRED);
        private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

        private final ThreadLocal<LongExecutionState> tlState = new ThreadLocal<>();

        private LongVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Long.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final LongExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Long> activeMask = SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Long> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Long.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final LongExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Long> activeMask = SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private LongExecutionState executionState() {
            LongExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new LongExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Long> evaluate(
                LongExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Long> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        state.stack[sp++].setVector(LongVector.broadcast(SPECIES, instruction.payloadLo()));
                        break;
                    case MEM:
                        state.stack[sp++].setVector(loadMemory(columnSegments, instruction, row, activeMask));
                        break;
                    case VAR:
                        state.stack[sp++].setVector(loadVar(varsSegment, instruction));
                        break;
                    case NEG: {
                        final LongSlot slot = state.stack[sp - 1];
                        LongVector negated = slot.vector.neg();
                        if (nullChecks) {
                            final VectorMask<Long> nullMask = slot.vector.compare(VectorOperators.EQ, LONG_NULL_VECTOR);
                            negated = negated.blend(LONG_NULL_VECTOR, nullMask);
                        }
                        slot.setVector(negated);
                        break;
                    }
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final LongSlot lhs = state.stack[sp - 1];
                        final LongSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final LongSlot lhs = state.stack[sp - 1];
                        final LongSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final LongSlot lhs = state.stack[sp - 1];
                        final LongSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV: {
                        final LongSlot lhs = state.stack[sp - 1];
                        final LongSlot rhs = state.stack[sp - 2];
                        rhs.setVector(arithmetic(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private LongVector loadMemory(
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Long> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            if (segment == null) {
                return LONG_NULL_VECTOR;
            }
            return LongVector.fromMemorySegment(SPECIES, segment, row * Long.BYTES, NATIVE_ORDER, activeMask);
        }

        private LongVector loadVar(MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final long value = varsSegment.get(NATIVE_LONG, varOffsets[Math.toIntExact(instruction.payloadLo())]);
            return LongVector.broadcast(SPECIES, value);
        }

        private LongVector arithmetic(LongVector lhs, LongVector rhs, int opcode) {
            if (!nullChecks) {
                return switch (opcode) {
                    case ADD -> lhs.add(rhs);
                    case SUB -> lhs.sub(rhs);
                    case MUL -> lhs.mul(rhs);
                    case DIV -> lhs.div(rhs);
                    default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                };
            }

            final VectorMask<Long> invalidMask = lhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR)
                    .or(rhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR))
                    .or(opcode == DIV ? rhs.compare(VectorOperators.EQ, ZERO_VECTOR) : SPECIES.maskAll(false));
            LongVector result = switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs, invalidMask.not());
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
            return result.blend(LONG_NULL_VECTOR, invalidMask);
        }

        private VectorMask<Long> compare(LongVector lhs, LongVector rhs, int opcode) {
            if (!nullChecks || opcode == EQ || opcode == NE) {
                return switch (opcode) {
                    case EQ -> lhs.compare(VectorOperators.EQ, rhs);
                    case NE -> lhs.compare(VectorOperators.NE, rhs);
                    case LT -> lhs.compare(VectorOperators.LT, rhs);
                    case LE -> lhs.compare(VectorOperators.LE, rhs);
                    case GT -> lhs.compare(VectorOperators.GT, rhs);
                    case GE -> lhs.compare(VectorOperators.GE, rhs);
                    default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
                };
            }

            final VectorMask<Long> leftNull = lhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR);
            final VectorMask<Long> rightNull = rhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR);
            final VectorMask<Long> anyNull = leftNull.or(rightNull);
            final VectorMask<Long> bothNull = leftNull.and(rightNull);
            return switch (opcode) {
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNull.not());
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNull.not());
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNull.not()).or(bothNull);
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNull.not()).or(bothNull);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private static final class LongExecutionState {
            private final LongSlot[] stack;

            private LongExecutionState(int stackSize) {
                this.stack = new LongSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new LongSlot();
                }
            }
        }

        private static final class LongSlot {
            private VectorMask<Long> mask;
            private LongVector vector;

            private void setMask(VectorMask<Long> mask) {
                this.mask = mask;
                this.vector = null;
            }

            private void setVector(LongVector vector) {
                this.vector = vector;
                this.mask = null;
            }
        }
    }

    private static final class FloatVectorExecutor extends VectorApiFilterExecutor {
        private static final FloatVector NAN_VECTOR = FloatVector.broadcast(FloatVector.SPECIES_PREFERRED, Float.NaN);
        private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

        private final ThreadLocal<FloatExecutionState> tlState = new ThreadLocal<>();

        private FloatVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Float.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final FloatExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Float> activeMask = SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Float> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Float.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final FloatExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Float> activeMask = SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private FloatExecutionState executionState() {
            FloatExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new FloatExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Float> evaluate(
                FloatExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Float> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        state.stack[sp++].setVector(FloatVector.broadcast(SPECIES, (float) instruction.doublePayload()));
                        break;
                    case MEM:
                        state.stack[sp++].setVector(loadMemory(columnSegments, instruction, row, activeMask));
                        break;
                    case VAR:
                        state.stack[sp++].setVector(loadVar(varsSegment, instruction));
                        break;
                    case NEG:
                        state.stack[sp - 1].setVector(state.stack[sp - 1].vector.neg());
                        break;
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final FloatSlot lhs = state.stack[sp - 1];
                        final FloatSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final FloatSlot lhs = state.stack[sp - 1];
                        final FloatSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final FloatSlot lhs = state.stack[sp - 1];
                        final FloatSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV: {
                        final FloatSlot lhs = state.stack[sp - 1];
                        final FloatSlot rhs = state.stack[sp - 2];
                        rhs.setVector(arithmetic(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private FloatVector loadMemory(
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Float> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            if (segment == null) {
                return NAN_VECTOR;
            }
            return FloatVector.fromMemorySegment(SPECIES, segment, row * Float.BYTES, NATIVE_ORDER, activeMask);
        }

        private FloatVector loadVar(MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final float value = varsSegment.get(NATIVE_FLOAT, varOffsets[Math.toIntExact(instruction.payloadLo())]);
            return FloatVector.broadcast(SPECIES, value);
        }

        private FloatVector arithmetic(FloatVector lhs, FloatVector rhs, int opcode) {
            return switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs).blend(NAN_VECTOR, nanOrZeroMask(lhs, rhs));
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
        }

        private VectorMask<Float> compare(FloatVector lhs, FloatVector rhs, int opcode) {
            final VectorMask<Float> leftNaN = lhs.test(VectorOperators.IS_NAN);
            final VectorMask<Float> rightNaN = rhs.test(VectorOperators.IS_NAN);
            final VectorMask<Float> anyNaN = leftNaN.or(rightNaN);
            final VectorMask<Float> bothNaN = leftNaN.and(rightNaN);
            final VectorMask<Float> eq = lhs.sub(rhs).abs().compare(VectorOperators.LE, FLOAT_EPSILON, anyNaN.not());
            return switch (opcode) {
                case EQ -> eq.or(bothNaN);
                case NE -> eq.or(bothNaN).not();
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNaN.not().and(eq.not()));
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNaN.not().and(eq.not()));
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private VectorMask<Float> nanOrZeroMask(FloatVector lhs, FloatVector rhs) {
            return lhs.test(VectorOperators.IS_NAN)
                    .or(rhs.test(VectorOperators.IS_NAN))
                    .or(rhs.compare(VectorOperators.EQ, 0.0f));
        }

        private static final class FloatExecutionState {
            private final FloatSlot[] stack;

            private FloatExecutionState(int stackSize) {
                this.stack = new FloatSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new FloatSlot();
                }
            }
        }

        private static final class FloatSlot {
            private VectorMask<Float> mask;
            private FloatVector vector;

            private void setMask(VectorMask<Float> mask) {
                this.mask = mask;
                this.vector = null;
            }

            private void setVector(FloatVector vector) {
                this.vector = vector;
                this.mask = null;
            }
        }
    }

    private static final class IntFloatVectorExecutor extends VectorApiFilterExecutor {
        private static final IntVector INT_NULL_VECTOR = IntVector.broadcast(IntVector.SPECIES_PREFERRED, Numbers.INT_NULL);
        private static final IntVector INT_ZERO_VECTOR = IntVector.zero(IntVector.SPECIES_PREFERRED);
        private static final FloatVector FLOAT_NAN_VECTOR = FloatVector.broadcast(FloatVector.SPECIES_PREFERRED, Float.NaN);
        private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
        private static final VectorSpecies<Float> FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;

        private final ThreadLocal<IntFloatExecutionState> tlState = new ThreadLocal<>();

        private IntFloatVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Integer.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final IntFloatExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += INT_SPECIES.length()) {
                final VectorMask<Integer> activeMask = INT_SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Integer> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = INT_SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Integer.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final IntFloatExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += INT_SPECIES.length()) {
                final VectorMask<Integer> activeMask = INT_SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private IntFloatExecutionState executionState() {
            IntFloatExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new IntFloatExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Integer> evaluate(
                IntFloatExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Integer> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        loadImmediate(state.stack[sp++], instruction);
                        break;
                    case MEM:
                        loadMemory(state.stack[sp++], columnSegments, instruction, row, activeMask);
                        break;
                    case VAR:
                        loadVar(state.stack[sp++], varsSegment, instruction);
                        break;
                    case NEG:
                        negate(state.stack[sp - 1]);
                        break;
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final IntFloatSlot lhs = state.stack[sp - 1];
                        final IntFloatSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final IntFloatSlot lhs = state.stack[sp - 1];
                        final IntFloatSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final IntFloatSlot lhs = state.stack[sp - 1];
                        final IntFloatSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs, rhs, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV: {
                        final IntFloatSlot lhs = state.stack[sp - 1];
                        final IntFloatSlot rhs = state.stack[sp - 2];
                        arithmetic(lhs, rhs, instruction.opcode());
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private void loadImmediate(IntFloatSlot slot, IrDecoder.Instruction instruction) {
            switch (instruction.type()) {
                case I4_TYPE:
                    slot.setIntVector(IntVector.broadcast(INT_SPECIES, (int) instruction.payloadLo()));
                    return;
                case F4_TYPE:
                    slot.setFloatVector(FloatVector.broadcast(FLOAT_SPECIES, (float) instruction.doublePayload()));
                    return;
                default:
                    throw new IllegalArgumentException("unsupported mixed immediate type: " + instruction.type());
            }
        }

        private void loadMemory(
                IntFloatSlot slot,
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Integer> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            switch (instruction.type()) {
                case I4_TYPE:
                    slot.setIntVector(segment == null
                            ? INT_NULL_VECTOR
                            : IntVector.fromMemorySegment(INT_SPECIES, segment, row * Integer.BYTES, NATIVE_ORDER, activeMask));
                    return;
                case F4_TYPE:
                    slot.setFloatVector(segment == null
                            ? FLOAT_NAN_VECTOR
                            : FloatVector.fromMemorySegment(FLOAT_SPECIES, segment, row * Float.BYTES, NATIVE_ORDER, activeMask.cast(FLOAT_SPECIES)));
                    return;
                default:
                    throw new IllegalArgumentException("unsupported mixed memory type: " + instruction.type());
            }
        }

        private void loadVar(IntFloatSlot slot, MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final int offset = varOffsets[Math.toIntExact(instruction.payloadLo())];
            switch (instruction.type()) {
                case I4_TYPE:
                    slot.setIntVector(IntVector.broadcast(INT_SPECIES, varsSegment.get(NATIVE_INT, offset)));
                    return;
                case F4_TYPE:
                    slot.setFloatVector(FloatVector.broadcast(FLOAT_SPECIES, varsSegment.get(NATIVE_FLOAT, offset)));
                    return;
                default:
                    throw new IllegalArgumentException("unsupported mixed bind variable type: " + instruction.type());
            }
        }

        private void negate(IntFloatSlot slot) {
            if (slot.type == I4_TYPE) {
                IntVector negated = slot.intVector.neg();
                if (nullChecks) {
                    negated = negated.blend(INT_NULL_VECTOR, slot.intVector.compare(VectorOperators.EQ, INT_NULL_VECTOR));
                }
                slot.setIntVector(negated);
                return;
            }
            slot.setFloatVector(slot.floatVector.neg());
        }

        private void arithmetic(IntFloatSlot lhs, IntFloatSlot rhs, int opcode) {
            if (lhs.type == I4_TYPE && rhs.type == I4_TYPE) {
                rhs.setIntVector(intArithmetic(lhs.intVector, rhs.intVector, opcode));
            } else {
                rhs.setFloatVector(floatArithmetic(toFloat(lhs), toFloat(rhs), opcode));
            }
        }

        private VectorMask<Integer> compare(IntFloatSlot lhs, IntFloatSlot rhs, int opcode) {
            if (lhs.type == I4_TYPE && rhs.type == I4_TYPE) {
                return compareInts(lhs.intVector, rhs.intVector, opcode);
            }
            return compareFloats(toFloat(lhs), toFloat(rhs), opcode).cast(INT_SPECIES);
        }

        private FloatVector toFloat(IntFloatSlot slot) {
            if (slot.type == F4_TYPE) {
                return slot.floatVector;
            }

            FloatVector converted = (FloatVector) slot.intVector.convertShape(VectorOperators.I2F, FLOAT_SPECIES, 0);
            if (nullChecks) {
                converted = converted.blend(
                        FLOAT_NAN_VECTOR,
                        slot.intVector.compare(VectorOperators.EQ, INT_NULL_VECTOR).cast(FLOAT_SPECIES)
                );
            }
            return converted;
        }

        private IntVector intArithmetic(IntVector lhs, IntVector rhs, int opcode) {
            if (!nullChecks) {
                return switch (opcode) {
                    case ADD -> lhs.add(rhs);
                    case SUB -> lhs.sub(rhs);
                    case MUL -> lhs.mul(rhs);
                    case DIV -> lhs.div(rhs);
                    default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                };
            }

            final VectorMask<Integer> invalidMask = lhs.compare(VectorOperators.EQ, INT_NULL_VECTOR)
                    .or(rhs.compare(VectorOperators.EQ, INT_NULL_VECTOR))
                    .or(opcode == DIV ? rhs.compare(VectorOperators.EQ, INT_ZERO_VECTOR) : INT_SPECIES.maskAll(false));
            IntVector result = switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs, invalidMask.not());
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
            return result.blend(INT_NULL_VECTOR, invalidMask);
        }

        private FloatVector floatArithmetic(FloatVector lhs, FloatVector rhs, int opcode) {
            return switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs).blend(FLOAT_NAN_VECTOR, nanOrZeroMask(lhs, rhs));
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
        }

        private VectorMask<Integer> compareInts(IntVector lhs, IntVector rhs, int opcode) {
            if (!nullChecks || opcode == EQ || opcode == NE) {
                return switch (opcode) {
                    case EQ -> lhs.compare(VectorOperators.EQ, rhs);
                    case NE -> lhs.compare(VectorOperators.NE, rhs);
                    case LT -> lhs.compare(VectorOperators.LT, rhs);
                    case LE -> lhs.compare(VectorOperators.LE, rhs);
                    case GT -> lhs.compare(VectorOperators.GT, rhs);
                    case GE -> lhs.compare(VectorOperators.GE, rhs);
                    default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
                };
            }

            final VectorMask<Integer> leftNull = lhs.compare(VectorOperators.EQ, INT_NULL_VECTOR);
            final VectorMask<Integer> rightNull = rhs.compare(VectorOperators.EQ, INT_NULL_VECTOR);
            final VectorMask<Integer> anyNull = leftNull.or(rightNull);
            final VectorMask<Integer> bothNull = leftNull.and(rightNull);
            return switch (opcode) {
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNull.not());
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNull.not());
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNull.not()).or(bothNull);
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNull.not()).or(bothNull);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private VectorMask<Float> compareFloats(FloatVector lhs, FloatVector rhs, int opcode) {
            final VectorMask<Float> leftNaN = lhs.test(VectorOperators.IS_NAN);
            final VectorMask<Float> rightNaN = rhs.test(VectorOperators.IS_NAN);
            final VectorMask<Float> anyNaN = leftNaN.or(rightNaN);
            final VectorMask<Float> bothNaN = leftNaN.and(rightNaN);
            final VectorMask<Float> eq = lhs.sub(rhs).abs().compare(VectorOperators.LE, FLOAT_EPSILON, anyNaN.not());
            return switch (opcode) {
                case EQ -> eq.or(bothNaN);
                case NE -> eq.or(bothNaN).not();
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNaN.not().and(eq.not()));
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNaN.not().and(eq.not()));
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private VectorMask<Float> nanOrZeroMask(FloatVector lhs, FloatVector rhs) {
            return lhs.test(VectorOperators.IS_NAN)
                    .or(rhs.test(VectorOperators.IS_NAN))
                    .or(rhs.compare(VectorOperators.EQ, 0.0f));
        }

        private static final class IntFloatExecutionState {
            private final IntFloatSlot[] stack;

            private IntFloatExecutionState(int stackSize) {
                this.stack = new IntFloatSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new IntFloatSlot();
                }
            }
        }

        private static final class IntFloatSlot {
            private FloatVector floatVector;
            private IntVector intVector;
            private VectorMask<Integer> mask;
            private int type;

            private void setMask(VectorMask<Integer> mask) {
                this.mask = mask;
                this.intVector = null;
                this.floatVector = null;
                this.type = 0;
            }

            private void setIntVector(IntVector intVector) {
                this.intVector = intVector;
                this.floatVector = null;
                this.mask = null;
                this.type = I4_TYPE;
            }

            private void setFloatVector(FloatVector floatVector) {
                this.floatVector = floatVector;
                this.intVector = null;
                this.mask = null;
                this.type = F4_TYPE;
            }
        }
    }

    private static final class LongDoubleVectorExecutor extends VectorApiFilterExecutor {
        private static final LongVector LONG_NULL_VECTOR = LongVector.broadcast(LongVector.SPECIES_PREFERRED, Numbers.LONG_NULL);
        private static final LongVector LONG_ZERO_VECTOR = LongVector.zero(LongVector.SPECIES_PREFERRED);
        private static final DoubleVector DOUBLE_NAN_VECTOR = DoubleVector.broadcast(DoubleVector.SPECIES_PREFERRED, Double.NaN);
        private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
        private static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

        private final ThreadLocal<LongDoubleExecutionState> tlState = new ThreadLocal<>();

        private LongDoubleVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Long.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final LongDoubleExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += LONG_SPECIES.length()) {
                final VectorMask<Long> activeMask = LONG_SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Long> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = LONG_SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Long.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final LongDoubleExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += LONG_SPECIES.length()) {
                final VectorMask<Long> activeMask = LONG_SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private LongDoubleExecutionState executionState() {
            LongDoubleExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new LongDoubleExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Long> evaluate(
                LongDoubleExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Long> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        loadImmediate(state.stack[sp++], instruction);
                        break;
                    case MEM:
                        loadMemory(state.stack[sp++], columnSegments, instruction, row, activeMask);
                        break;
                    case VAR:
                        loadVar(state.stack[sp++], varsSegment, instruction);
                        break;
                    case NEG:
                        negate(state.stack[sp - 1]);
                        break;
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final LongDoubleSlot lhs = state.stack[sp - 1];
                        final LongDoubleSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final LongDoubleSlot lhs = state.stack[sp - 1];
                        final LongDoubleSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final LongDoubleSlot lhs = state.stack[sp - 1];
                        final LongDoubleSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs, rhs, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV: {
                        final LongDoubleSlot lhs = state.stack[sp - 1];
                        final LongDoubleSlot rhs = state.stack[sp - 2];
                        arithmetic(lhs, rhs, instruction.opcode());
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private void loadImmediate(LongDoubleSlot slot, IrDecoder.Instruction instruction) {
            switch (instruction.type()) {
                case I8_TYPE:
                    slot.setLongVector(LongVector.broadcast(LONG_SPECIES, instruction.payloadLo()));
                    return;
                case F8_TYPE:
                    slot.setDoubleVector(DoubleVector.broadcast(DOUBLE_SPECIES, instruction.doublePayload()));
                    return;
                default:
                    throw new IllegalArgumentException("unsupported mixed immediate type: " + instruction.type());
            }
        }

        private void loadMemory(
                LongDoubleSlot slot,
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Long> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            switch (instruction.type()) {
                case I8_TYPE:
                    slot.setLongVector(segment == null
                            ? LONG_NULL_VECTOR
                            : LongVector.fromMemorySegment(LONG_SPECIES, segment, row * Long.BYTES, NATIVE_ORDER, activeMask));
                    return;
                case F8_TYPE:
                    slot.setDoubleVector(segment == null
                            ? DOUBLE_NAN_VECTOR
                            : DoubleVector.fromMemorySegment(DOUBLE_SPECIES, segment, row * Double.BYTES, NATIVE_ORDER, activeMask.cast(DOUBLE_SPECIES)));
                    return;
                default:
                    throw new IllegalArgumentException("unsupported mixed memory type: " + instruction.type());
            }
        }

        private void loadVar(LongDoubleSlot slot, MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final int offset = varOffsets[Math.toIntExact(instruction.payloadLo())];
            switch (instruction.type()) {
                case I8_TYPE:
                    slot.setLongVector(LongVector.broadcast(LONG_SPECIES, varsSegment.get(NATIVE_LONG, offset)));
                    return;
                case F8_TYPE:
                    slot.setDoubleVector(DoubleVector.broadcast(DOUBLE_SPECIES, varsSegment.get(NATIVE_DOUBLE, offset)));
                    return;
                default:
                    throw new IllegalArgumentException("unsupported mixed bind variable type: " + instruction.type());
            }
        }

        private void negate(LongDoubleSlot slot) {
            if (slot.type == I8_TYPE) {
                LongVector negated = slot.longVector.neg();
                if (nullChecks) {
                    negated = negated.blend(LONG_NULL_VECTOR, slot.longVector.compare(VectorOperators.EQ, LONG_NULL_VECTOR));
                }
                slot.setLongVector(negated);
                return;
            }
            slot.setDoubleVector(slot.doubleVector.neg());
        }

        private void arithmetic(LongDoubleSlot lhs, LongDoubleSlot rhs, int opcode) {
            if (lhs.type == I8_TYPE && rhs.type == I8_TYPE) {
                rhs.setLongVector(longArithmetic(lhs.longVector, rhs.longVector, opcode));
            } else {
                rhs.setDoubleVector(doubleArithmetic(toDouble(lhs), toDouble(rhs), opcode));
            }
        }

        private VectorMask<Long> compare(LongDoubleSlot lhs, LongDoubleSlot rhs, int opcode) {
            if (lhs.type == I8_TYPE && rhs.type == I8_TYPE) {
                return compareLongs(lhs.longVector, rhs.longVector, opcode);
            }
            return compareDoubles(toDouble(lhs), toDouble(rhs), opcode).cast(LONG_SPECIES);
        }

        private DoubleVector toDouble(LongDoubleSlot slot) {
            if (slot.type == F8_TYPE) {
                return slot.doubleVector;
            }

            DoubleVector converted = (DoubleVector) slot.longVector.convertShape(VectorOperators.L2D, DOUBLE_SPECIES, 0);
            if (nullChecks) {
                converted = converted.blend(
                        DOUBLE_NAN_VECTOR,
                        slot.longVector.compare(VectorOperators.EQ, LONG_NULL_VECTOR).cast(DOUBLE_SPECIES)
                );
            }
            return converted;
        }

        private LongVector longArithmetic(LongVector lhs, LongVector rhs, int opcode) {
            if (!nullChecks) {
                return switch (opcode) {
                    case ADD -> lhs.add(rhs);
                    case SUB -> lhs.sub(rhs);
                    case MUL -> lhs.mul(rhs);
                    case DIV -> lhs.div(rhs);
                    default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
                };
            }

            final VectorMask<Long> invalidMask = lhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR)
                    .or(rhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR))
                    .or(opcode == DIV ? rhs.compare(VectorOperators.EQ, LONG_ZERO_VECTOR) : LONG_SPECIES.maskAll(false));
            LongVector result = switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs, invalidMask.not());
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
            return result.blend(LONG_NULL_VECTOR, invalidMask);
        }

        private DoubleVector doubleArithmetic(DoubleVector lhs, DoubleVector rhs, int opcode) {
            return switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs).blend(DOUBLE_NAN_VECTOR, nanOrZeroMask(lhs, rhs));
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
        }

        private VectorMask<Long> compareLongs(LongVector lhs, LongVector rhs, int opcode) {
            if (!nullChecks || opcode == EQ || opcode == NE) {
                return switch (opcode) {
                    case EQ -> lhs.compare(VectorOperators.EQ, rhs);
                    case NE -> lhs.compare(VectorOperators.NE, rhs);
                    case LT -> lhs.compare(VectorOperators.LT, rhs);
                    case LE -> lhs.compare(VectorOperators.LE, rhs);
                    case GT -> lhs.compare(VectorOperators.GT, rhs);
                    case GE -> lhs.compare(VectorOperators.GE, rhs);
                    default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
                };
            }

            final VectorMask<Long> leftNull = lhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR);
            final VectorMask<Long> rightNull = rhs.compare(VectorOperators.EQ, LONG_NULL_VECTOR);
            final VectorMask<Long> anyNull = leftNull.or(rightNull);
            final VectorMask<Long> bothNull = leftNull.and(rightNull);
            return switch (opcode) {
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNull.not());
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNull.not());
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNull.not()).or(bothNull);
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNull.not()).or(bothNull);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private VectorMask<Double> compareDoubles(DoubleVector lhs, DoubleVector rhs, int opcode) {
            final VectorMask<Double> leftNaN = lhs.test(VectorOperators.IS_NAN);
            final VectorMask<Double> rightNaN = rhs.test(VectorOperators.IS_NAN);
            final VectorMask<Double> anyNaN = leftNaN.or(rightNaN);
            final VectorMask<Double> bothNaN = leftNaN.and(rightNaN);
            final VectorMask<Double> eq = lhs.sub(rhs).abs().compare(VectorOperators.LE, DOUBLE_EPSILON, anyNaN.not());
            return switch (opcode) {
                case EQ -> eq.or(bothNaN);
                case NE -> eq.or(bothNaN).not();
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNaN.not().and(eq.not()));
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNaN.not().and(eq.not()));
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private VectorMask<Double> nanOrZeroMask(DoubleVector lhs, DoubleVector rhs) {
            return lhs.test(VectorOperators.IS_NAN)
                    .or(rhs.test(VectorOperators.IS_NAN))
                    .or(rhs.compare(VectorOperators.EQ, 0.0d));
        }

        private static final class LongDoubleExecutionState {
            private final LongDoubleSlot[] stack;

            private LongDoubleExecutionState(int stackSize) {
                this.stack = new LongDoubleSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new LongDoubleSlot();
                }
            }
        }

        private static final class LongDoubleSlot {
            private DoubleVector doubleVector;
            private LongVector longVector;
            private VectorMask<Long> mask;
            private int type;

            private void setMask(VectorMask<Long> mask) {
                this.mask = mask;
                this.longVector = null;
                this.doubleVector = null;
                this.type = 0;
            }

            private void setLongVector(LongVector longVector) {
                this.longVector = longVector;
                this.doubleVector = null;
                this.mask = null;
                this.type = I8_TYPE;
            }

            private void setDoubleVector(DoubleVector doubleVector) {
                this.doubleVector = doubleVector;
                this.longVector = null;
                this.mask = null;
                this.type = F8_TYPE;
            }
        }
    }

    private static final class DoubleVectorExecutor extends VectorApiFilterExecutor {
        private static final DoubleVector NAN_VECTOR = DoubleVector.broadcast(DoubleVector.SPECIES_PREFERRED, Double.NaN);
        private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

        private final ThreadLocal<DoubleExecutionState> tlState = new ThreadLocal<>();

        private DoubleVectorExecutor(IrDecoder.Instruction[] instructions, boolean nullChecks, int[] varOffsets) {
            super(instructions, nullChecks, varOffsets);
        }

        @Override
        public long filter(long dataAddress, long dataSize, long varsAddress, long filteredRowsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Double.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final MemorySegment output = prepareOutputSegment(filteredRowsAddress, rowsCount);
            final DoubleExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Double> activeMask = SPECIES.indexInRange(row, rowsCount);
                final VectorMask<Double> result = evaluate(state, columnSegments, varsSegment, row, activeMask);
                for (int lane = 0, laneCount = SPECIES.length(); lane < laneCount; lane++) {
                    if (activeMask.laneIsSet(lane) && result.laneIsSet(lane)) {
                        output.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                    }
                }
            }
            return filteredCount;
        }

        @Override
        public long filterCount(long dataAddress, long dataSize, long varsAddress, long rowsCount) {
            final MemorySegment[] columnSegments = prepareColumnSegments(dataAddress, dataSize, rowsCount, Double.BYTES);
            final MemorySegment varsSegment = prepareVarsSegment(varsAddress);
            final DoubleExecutionState state = executionState();
            long filteredCount = 0;

            for (long row = 0; row < rowsCount; row += SPECIES.length()) {
                final VectorMask<Double> activeMask = SPECIES.indexInRange(row, rowsCount);
                filteredCount += evaluate(state, columnSegments, varsSegment, row, activeMask).and(activeMask).trueCount();
            }
            return filteredCount;
        }

        private DoubleExecutionState executionState() {
            DoubleExecutionState state = tlState.get();
            if (state == null || state.stack.length < instructions.length + 1) {
                state = new DoubleExecutionState(instructions.length + 1);
                tlState.set(state);
            }
            return state;
        }

        private VectorMask<Double> evaluate(
                DoubleExecutionState state,
                MemorySegment[] columnSegments,
                MemorySegment varsSegment,
                long row,
                VectorMask<Double> activeMask
        ) {
            int sp = 0;
            for (IrDecoder.Instruction instruction : instructions) {
                switch (instruction.opcode()) {
                    case IMM:
                        state.stack[sp++].setVector(DoubleVector.broadcast(SPECIES, instruction.doublePayload()));
                        break;
                    case MEM:
                        state.stack[sp++].setVector(loadMemory(columnSegments, instruction, row, activeMask));
                        break;
                    case VAR:
                        state.stack[sp++].setVector(loadVar(varsSegment, instruction));
                        break;
                    case NEG:
                        state.stack[sp - 1].setVector(state.stack[sp - 1].vector.neg());
                        break;
                    case NOT:
                        state.stack[sp - 1].setMask(state.stack[sp - 1].mask.not());
                        break;
                    case AND: {
                        final DoubleSlot lhs = state.stack[sp - 1];
                        final DoubleSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.and(lhs.mask));
                        sp--;
                        break;
                    }
                    case OR: {
                        final DoubleSlot lhs = state.stack[sp - 1];
                        final DoubleSlot rhs = state.stack[sp - 2];
                        rhs.setMask(rhs.mask.or(lhs.mask));
                        sp--;
                        break;
                    }
                    case EQ:
                    case NE:
                    case LT:
                    case LE:
                    case GT:
                    case GE: {
                        final DoubleSlot lhs = state.stack[sp - 1];
                        final DoubleSlot rhs = state.stack[sp - 2];
                        rhs.setMask(compare(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case ADD:
                    case SUB:
                    case MUL:
                    case DIV: {
                        final DoubleSlot lhs = state.stack[sp - 1];
                        final DoubleSlot rhs = state.stack[sp - 2];
                        rhs.setVector(arithmetic(lhs.vector, rhs.vector, instruction.opcode()));
                        sp--;
                        break;
                    }
                    case RET:
                        return state.stack[sp - 1].mask.and(activeMask);
                    default:
                        throw new IllegalArgumentException("unsupported vector opcode: " + instruction.opcode());
                }
            }
            return state.stack[sp - 1].mask.and(activeMask);
        }

        private DoubleVector loadMemory(
                MemorySegment[] columnSegments,
                IrDecoder.Instruction instruction,
                long row,
                VectorMask<Double> activeMask
        ) {
            final int index = Math.toIntExact(instruction.payloadLo());
            final MemorySegment segment = columnSegments[index];
            if (segment == null) {
                return NAN_VECTOR;
            }
            return DoubleVector.fromMemorySegment(SPECIES, segment, row * Double.BYTES, NATIVE_ORDER, activeMask);
        }

        private DoubleVector loadVar(MemorySegment varsSegment, IrDecoder.Instruction instruction) {
            final double value = varsSegment.get(NATIVE_DOUBLE, varOffsets[Math.toIntExact(instruction.payloadLo())]);
            return DoubleVector.broadcast(SPECIES, value);
        }

        private DoubleVector arithmetic(DoubleVector lhs, DoubleVector rhs, int opcode) {
            return switch (opcode) {
                case ADD -> lhs.add(rhs);
                case SUB -> lhs.sub(rhs);
                case MUL -> lhs.mul(rhs);
                case DIV -> lhs.div(rhs).blend(NAN_VECTOR, nanOrZeroMask(lhs, rhs));
                default -> throw new IllegalArgumentException("unsupported arithmetic opcode: " + opcode);
            };
        }

        private VectorMask<Double> compare(DoubleVector lhs, DoubleVector rhs, int opcode) {
            final VectorMask<Double> leftNaN = lhs.test(VectorOperators.IS_NAN);
            final VectorMask<Double> rightNaN = rhs.test(VectorOperators.IS_NAN);
            final VectorMask<Double> anyNaN = leftNaN.or(rightNaN);
            final VectorMask<Double> bothNaN = leftNaN.and(rightNaN);
            final VectorMask<Double> eq = lhs.sub(rhs).abs().compare(VectorOperators.LE, DOUBLE_EPSILON, anyNaN.not());
            return switch (opcode) {
                case EQ -> eq.or(bothNaN);
                case NE -> eq.or(bothNaN).not();
                case LT -> lhs.compare(VectorOperators.LT, rhs, anyNaN.not().and(eq.not()));
                case LE -> lhs.compare(VectorOperators.LE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                case GT -> lhs.compare(VectorOperators.GT, rhs, anyNaN.not().and(eq.not()));
                case GE -> lhs.compare(VectorOperators.GE, rhs, anyNaN.not()).or(eq).or(bothNaN);
                default -> throw new IllegalArgumentException("unsupported comparison opcode: " + opcode);
            };
        }

        private VectorMask<Double> nanOrZeroMask(DoubleVector lhs, DoubleVector rhs) {
            return lhs.test(VectorOperators.IS_NAN)
                    .or(rhs.test(VectorOperators.IS_NAN))
                    .or(rhs.compare(VectorOperators.EQ, 0.0d));
        }

        private static final class DoubleExecutionState {
            private final DoubleSlot[] stack;

            private DoubleExecutionState(int stackSize) {
                this.stack = new DoubleSlot[stackSize];
                for (int i = 0; i < stackSize; i++) {
                    stack[i] = new DoubleSlot();
                }
            }
        }

        private static final class DoubleSlot {
            private VectorMask<Double> mask;
            private DoubleVector vector;

            private void setMask(VectorMask<Double> mask) {
                this.mask = mask;
                this.vector = null;
            }

            private void setVector(DoubleVector vector) {
                this.vector = vector;
                this.mask = null;
            }
        }
    }
}
