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

package org.questdb.experimental.codegen;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.conditional.CaseFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.VarcharCaseFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.Unsafe;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Resolved UTF-8 values borrow a frame or the caller's literal descriptor; no row allocations. */
final class ResolvedVarchar {
    private final ColumnTypes types;
    private final ResolvedInputs numeric;
    private final ArrayList<Integer> columns = new ArrayList<>();
    private final ByteArrayOutputStream literals = new ByteArrayOutputStream();

    ResolvedVarchar(ColumnTypes types, ResolvedInputs numeric) {
        this.types = types;
        this.numeric = numeric;
    }

    Expression column(int index) {
        if (index < 0 || index >= types.getColumnCount() || types.getColumnType(index) != ColumnType.VARCHAR) return null;
        int compact = columns.indexOf(index);
        if (compact < 0) { compact = columns.size(); columns.add(index); }
        return new Column(compact);
    }

    Expression bind(Function function) { return bind(function, 0); }
    Binding binding() {
        return new Binding(numeric.indexes(), numeric.columns(), columns.stream().mapToInt(Integer::intValue).toArray(), literals.toByteArray());
    }

    /** Numeric page pointers, then pinned UTF-8 views, then slot-owned literal bytes. */
    record Binding(int[] numericColumns, List<Pipeline.Column> numericInputs, int[] varcharColumns, byte[] literals) {
        long bytes() { return (numericColumns.length * 8L + varcharColumns.length * 48L + literals.length + 7) & -8L; }

        boolean prepare(PageFrameMemoryRecord record, long descriptor) {
            for (int i = 0; i < numericColumns.length; i++) {
                long page = record.getPageAddress(numericColumns[i]);
                // Zero also denotes lazy type conversion: fall back before mutating aggregate state.
                if (page == 0) return false;
                Unsafe.putLong(descriptor + i * 8L, page);
            }
            long strings = descriptor + numericColumns.length * 8L;
            for (int i = 0; i < varcharColumns.length; i++) {
                if (!record.writeNativeVarcharView(varcharColumns[i], strings + i * 48L)) return false;
            }
            long constants = strings + varcharColumns.length * 48L;
            for (int i = 0; i < literals.length; i++) Unsafe.getUnsafe().putByte(constants + i, literals[i]);
            return true;
        }

        Frame frame(Pipeline.Row numeric, Expr row, Expr descriptor) {
            Expr views = at(descriptor, numericColumns.length * 8L);
            return new Frame(numeric, row, views, at(views, varcharColumns.length * 48L));
        }
    }

    private Expression bind(Function function, int depth) {
        if (depth > 64 || (function.getType() != ColumnType.VARCHAR && function.getClass() != StrConstant.class)) return null;
        if (function.getClass() == VarcharColumn.class) return column(((VarcharColumn) function).getColumnIndex());
        if (function.isConstant()) {
            var value = function.getVarcharA(null);
            int offset = literals.size();
            if (value == null) return new Literal(offset, 0, true, true);
            for (int i = 0; i < value.size(); i++) literals.write(value.byteAt(i));
            return new Literal(offset, value.size(), false, value.isAscii());
        }
        if (function instanceof VarcharCaseFunction conditional
                && conditional.getPicker() instanceof CaseFunctionFactory.SearchedCasePicker picker) {
            var branches = new ArrayList<Branch>();
            for (int i = 0; i < picker.getBranchCount(); i++) {
                var condition = numeric.predicate(picker.getCondition(i));
                var value = bind(picker.getValue(i), depth + 1);
                if (condition == null || value == null) return null;
                branches.add(new Branch(condition, value));
            }
            var otherwise = bind(picker.getElseBranch(), depth + 1);
            return otherwise == null ? null : new Conditional(List.copyOf(branches), otherwise);
        }
        return null;
    }

    record Frame(Pipeline.Row numeric, Expr row, Expr columns, Expr literals,
                 java.util.Map<Expression, Pipeline.Utf8Value> materialized) {
        Frame(Pipeline.Row numeric, Expr row, Expr columns, Expr literals) {
            this(numeric, row, columns, literals, null);
        }
        Pipeline.Utf8Value value(Builder b, Expression argument) {
            if (materialized == null) return argument.emit(b, this);
            Pipeline.Utf8Value value = materialized.get(argument);
            if (value == null) throw new IllegalArgumentException("Unmaterialized string argument");
            return value;
        }
    }

    sealed interface Expression permits Column, Literal, Conditional {
        Pipeline.Utf8Value emit(Builder builder, Frame frame);
    }

    private record Column(int index) implements Expression {
        @Override
        public Pipeline.Utf8Value emit(Builder b, Frame frame) {
            return QuestVarchar.decode(b, at(frame.columns, index * 48L), frame.row);
        }
    }

    private record Literal(int offset, int size, boolean isNull, boolean isAscii) implements Expression {
        @Override
        public Pipeline.Utf8Value emit(Builder b, Frame frame) {
            // A literal descriptor is slot-owned; it must never escape as a stable source pointer.
            return new Pipeline.Utf8Value(at(frame.literals, offset), i64(size), bool(isNull), bool(isAscii), bool(false));
        }
    }

    private record Branch(Pipeline.Predicate condition, Expression value) { }

    private record Conditional(List<Branch> branches, Expression otherwise) implements Expression {
        @Override
        public Pipeline.Utf8Value emit(Builder b, Frame frame) {
            Var address = b.let(frame.literals), size = b.let(i64(0)), isNull = b.let(bool(true));
            Var isAscii = b.let(bool(true)), stable = b.let(bool(false)), matched = b.let(bool(false));
            for (Branch branch : branches) {
                b.when(eq(matched, bool(false)), next -> next.when(branch.condition.emit(frame.numeric), selected -> {
                    assign(selected, branch.value.emit(selected, frame), address, size, isNull, isAscii, stable);
                    selected.assign(matched, bool(true));
                }));
            }
            b.when(eq(matched, bool(false)), selected -> assign(selected, otherwise.emit(selected, frame), address, size, isNull, isAscii, stable));
            return new Pipeline.Utf8Value(address, size, isNull, isAscii, stable);
        }

        private static void assign(Builder b, Pipeline.Utf8Value value, Var address, Var size, Var isNull, Var isAscii, Var stable) {
            b.assign(address, value.address());
            b.assign(size, value.size());
            b.assign(isNull, value.isNull());
            b.assign(isAscii, value.isAscii());
            b.assign(stable, value.stable());
        }
    }
}
