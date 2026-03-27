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

import static io.questdb.jit.CompiledFilterIRSerializer.*;

/**
 * Lowered operations for the typed internal representation.
 * Each operation writes its result to a destination temporary identified by
 * an integer ID. Source operands are also temporary IDs.
 * <p>
 * Types use the same constants as {@link CompiledFilterIRSerializer}:
 * I1_TYPE, I2_TYPE, I4_TYPE, F4_TYPE, I8_TYPE, F8_TYPE, I16_TYPE.
 */
public sealed interface LoweredOp {

    int dst();

    int resultType();

    // --- Data loads ---

    /**
     * Load a fixed-width column value for the current row.
     *
     * @param dst         destination temporary
     * @param columnIndex column index in the column-address array
     * @param type        IR type (I1, I2, I4, F4, I8, F8, I16)
     */
    record LoadColumn(int dst, int columnIndex, int type) implements LoweredOp {
        @Override
        public int resultType() {
            return type;
        }
    }

    /**
     * Load a variable-size column header for NULL detection.
     * The result type is the normalized type:
     * STRING_HEADER -> I4, BINARY_HEADER/VARCHAR_HEADER -> I8.
     *
     * @param dst         destination temporary
     * @param columnIndex column index
     * @param headerType  original header type (STRING_HEADER_TYPE, BINARY_HEADER_TYPE, VARCHAR_HEADER_TYPE)
     */
    record LoadVarSizeHeader(int dst, int columnIndex, int headerType) implements LoweredOp {
        @Override
        public int resultType() {
            return switch (headerType) {
                case STRING_HEADER_TYPE -> I4_TYPE;
                case BINARY_HEADER_TYPE, VARCHAR_HEADER_TYPE -> I8_TYPE;
                default -> throw new IllegalArgumentException("not a var-size header type: " + headerType);
            };
        }
    }

    /**
     * Load a bind variable.
     *
     * @param dst       destination temporary
     * @param varIndex  bind variable index in the IR
     * @param byteOffset computed byte offset in the bind-variable memory
     * @param type      IR type of the variable
     */
    record LoadVar(int dst, int varIndex, int byteOffset, int type) implements LoweredOp {
        @Override
        public int resultType() {
            return type;
        }
    }

    /**
     * Load an immediate constant.
     *
     * @param dst  destination temporary
     * @param type IR type
     * @param lo   low 64 bits of the value
     * @param hi   high 64 bits (used only for I16)
     */
    record LoadImm(int dst, int type, long lo, long hi) implements LoweredOp {
        @Override
        public int resultType() {
            return type;
        }
    }

    // --- Type conversion ---

    /**
     * Explicit type cast from one IR type to another. Null-aware conversions
     * (INT_NULL -> LONG_NULL, INT_NULL -> NaN, etc.) are handled by the
     * backend according to the program's null-check flag.
     *
     * @param dst      destination temporary
     * @param src      source temporary
     * @param fromType source IR type
     * @param toType   target IR type
     */
    record Cast(int dst, int src, int fromType, int toType) implements LoweredOp {
        @Override
        public int resultType() {
            return toType;
        }
    }

    // --- Comparisons ---

    /**
     * Comparison of two values after coercion to a common type.
     * The result is always I1 (boolean).
     * Float comparisons use epsilon semantics. Null handling follows
     * the strict/non-strict rules based on the comparison opcode.
     *
     * @param dst         destination temporary (boolean result)
     * @param lhs         left operand temporary (already coerced)
     * @param rhs         right operand temporary (already coerced)
     * @param opcode      comparison opcode (EQ, NE, LT, LE, GT, GE)
     * @param operandType the common type both operands have been coerced to
     */
    record Compare(int dst, int lhs, int rhs, int opcode, int operandType) implements LoweredOp {
        @Override
        public int resultType() {
            return I1_TYPE;
        }
    }

    /**
     * I128 (UUID) comparison. Only EQ and NE are supported.
     * Both operands must already be I16_TYPE.
     *
     * @param dst    destination temporary (boolean result)
     * @param lhs    left operand temporary
     * @param rhs    right operand temporary
     * @param opcode EQ or NE
     */
    record CompareI128(int dst, int lhs, int rhs, int opcode) implements LoweredOp {
        @Override
        public int resultType() {
            return I1_TYPE;
        }
    }

    // --- Arithmetic ---

    /**
     * Binary arithmetic operation after coercion to a common type.
     *
     * @param dst        destination temporary
     * @param lhs        left operand temporary (already coerced)
     * @param rhs        right operand temporary (already coerced)
     * @param opcode     arithmetic opcode (ADD, SUB, MUL, DIV)
     * @param resultType the promoted result type
     */
    record Arithmetic(int dst, int lhs, int rhs, int opcode, int resultType) implements LoweredOp {
        @Override
        public int resultType() {
            return resultType;
        }
    }

    // --- Logical ---

    /**
     * Boolean AND/OR of two boolean values.
     *
     * @param dst    destination temporary (boolean result)
     * @param lhs    left operand temporary
     * @param rhs    right operand temporary
     * @param opcode AND or OR
     */
    record BooleanOp(int dst, int lhs, int rhs, int opcode) implements LoweredOp {
        @Override
        public int resultType() {
            return I1_TYPE;
        }
    }

    /**
     * Unary negation. The result type is promoted (I1/I2 -> I4).
     *
     * @param dst  destination temporary
     * @param src  source temporary
     * @param type result type after promotion
     */
    record Negate(int dst, int src, int type) implements LoweredOp {
        @Override
        public int resultType() {
            return type;
        }
    }

    /**
     * Logical NOT. Result is boolean.
     *
     * @param dst destination temporary
     * @param src source temporary
     */
    record Not(int dst, int src) implements LoweredOp {
        @Override
        public int resultType() {
            return I1_TYPE;
        }
    }

    // --- Data movement ---

    /**
     * Copy a value from one temporary to another. Used by the lowering
     * pass to implement phi-store blocks for short-circuit merge points.
     *
     * @param dst  destination temporary
     * @param src  source temporary
     * @param type the IR type of the value
     */
    record Move(int dst, int src, int type) implements LoweredOp {
        @Override
        public int resultType() {
            return type;
        }
    }
}
