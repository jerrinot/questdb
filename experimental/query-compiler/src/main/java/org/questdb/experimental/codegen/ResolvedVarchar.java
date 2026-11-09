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
import io.questdb.griffin.engine.functions.conditional.CaseFunctionPicker;
import io.questdb.griffin.engine.functions.conditional.VarcharCaseFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.regex.RegexpReplaceVarcharFunctionFactory;
import io.questdb.std.Unsafe;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

import static org.questdb.experimental.codegen.Ir.*;

/** Resolved UTF-8 values borrow a frame or the caller's literal descriptor; no row allocations. */
final class ResolvedVarchar {
    private final ColumnTypes types;
    private final ResolvedInputs numeric;
    private final ArrayList<Integer> columns;
    private final ByteArrayOutputStream literals;
    private final ArrayList<NativeCapture.Plan> captures;
    private final ArrayList<Pipeline.FrameScalar> expressions = new ArrayList<>();

    ResolvedVarchar(ColumnTypes types, ResolvedInputs numeric) {
        this(types, numeric, new ArrayList<>(), new ByteArrayOutputStream(), new ArrayList<>());
    }

    ResolvedVarchar(ColumnTypes types, ResolvedInputs numeric, ArrayList<Integer> columns, ByteArrayOutputStream literals) {
        this(types, numeric, columns, literals, null); // Filter descriptors have no helper owner yet.
    }

    private ResolvedVarchar(ColumnTypes types, ResolvedInputs numeric, ArrayList<Integer> columns,
                            ByteArrayOutputStream literals, ArrayList<NativeCapture.Plan> captures) {
        this.captures = captures;
        this.types = types;
        this.numeric = numeric;
        this.columns = columns;
        this.literals = literals;
        numeric.bindStrings(this);
    }

    Expression column(int index) {
        if (index < 0 || index >= types.getColumnCount() || types.getColumnType(index) != ColumnType.VARCHAR) return null;
        int compact = columns.indexOf(index);
        if (compact < 0) { compact = columns.size(); columns.add(index); }
        return new Column(compact);
    }

    Expression bind(Function function) { return bind(function, 0); }
    Pipeline.FrameScalar length(Function function, int depth) {
        Expression argument = bind(function, depth);
        if (argument == null) return null;
        var length = new Length(argument);
        if (!expressions.contains(length)) expressions.add(length);
        return length;
    }

    Binding binding() {
        return new Binding(numeric.indexes(), numeric.columns(), columns.stream().mapToInt(Integer::intValue).toArray(), literals.toByteArray(), List.copyOf(expressions), captures == null ? List.of() : List.copyOf(captures), numeric.parameters());
    }

    /** Numeric page pointers, UTF-8 views, literals, helper descriptors and cursor constants. */
    record Binding(int[] numericColumns, List<Pipeline.Column> numericInputs, int[] varcharColumns, byte[] literals,
                   List<Pipeline.FrameScalar> expressions, List<NativeCapture.Plan> captures, List<IntSupplier> parameters) {
        Binding(int[] numericColumns, List<Pipeline.Column> numericInputs, int[] varcharColumns, byte[] literals) {
            this(numericColumns, numericInputs, varcharColumns, literals, List.of(), List.of(), List.of());
        }
        long helperOffset() { return (numericColumns.length * 8L + varcharColumns.length * 48L + literals.length + 7) & -8L; }
        long parameterOffset() { return helperOffset() + (long) captures.size() * NativeCapture.DESCRIPTOR_BYTES; }
        long bytes() { return parameterOffset() + parameters.size() * 8L; }

        // Ordinary functions own initialization and dictionary lifetime. Snapshot their resolved
        // IDs after init(); reopening the cursor refreshes these values.
        void prepareParameters(long address) {
            for (int i = 0; i < parameters.size(); i++)
                Unsafe.putLong(address + i * 8L, parameters.get(i).getAsInt());
        }

        List<Expr> loadParameters(Builder b, Expr address) {
            var values = new ArrayList<Expr>(parameters.size());
            for (int i = 0; i < parameters.size(); i++)
                values.add(b.let(new Load(MemoryType.I32, at(address, i * 8L))));
            return values;
        }

        Slot newSlot(long descriptor) { return captures.isEmpty() ? null : new Slot(captures, descriptor + helperOffset()); }

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
            prepareParameters(descriptor + parameterOffset());
            return true;
        }

        Frame frame(Pipeline.Row numeric, Expr row, Expr descriptor) {
            Expr views = at(descriptor, numericColumns.length * 8L);
            return new Frame(numeric, row, views, at(views, varcharColumns.length * 48L), at(descriptor, helperOffset()), null);
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
        if (captures != null && function instanceof RegexpReplaceVarcharFunctionFactory.SingleGroupAsciiFunc capture) {
            var plan = NativeCapture.Plan.bind(capture.getPattern(), capture.getGroup());
            if (plan == null) return null;
            var argument = bind(capture.getArg(), depth + 1);
            if (argument == null) return null;
            int index = captures.size();
            captures.add(plan);
            return new NativeCapture.Expression(argument, plan, index);
        }
        if (function instanceof VarcharCaseFunction conditional
                && conditional.getPicker() instanceof CaseFunctionPicker.Integral picker) {
            int count = picker.getBranchCount();
            if (count > 16 || count > 64 - depth) return null;
            var branches = new ArrayList<Branch>();
            for (int i = 0; i < count; i++) {
                var condition = numeric.switchCondition(picker, i, depth + 1);
                var value = bind(picker.getValue(i), depth + i + 1);
                if (condition == null || value == null) return null;
                branches.add(new Branch(condition, value));
            }
            var otherwise = bind(picker.getElseBranch(), depth + count);
            return otherwise == null ? null : new Conditional(List.copyOf(branches), otherwise);
        }
        if (function instanceof VarcharCaseFunction conditional
                && conditional.getPicker() instanceof CaseFunctionFactory.SearchedCasePicker picker) {
            var branches = new ArrayList<Branch>();
            for (int i = 0; i < picker.getBranchCount(); i++) {
                var condition = numeric.predicate(picker.getCondition(i), depth + 1);
                var value = bind(picker.getValue(i), depth + 1);
                if (condition == null || value == null) return null;
                branches.add(new Branch(condition, value));
            }
            var otherwise = bind(picker.getElseBranch(), depth + 1);
            return otherwise == null ? null : new Conditional(List.copyOf(branches), otherwise);
        }
        return null;
    }

    static final class Slot implements AutoCloseable {
        private final NativeCapture.Slot[] captures;
        Slot(List<NativeCapture.Plan> plans, long descriptor) {
            captures = new NativeCapture.Slot[plans.size()];
            try {
                for (int i = 0; i < captures.length; i++)
                    captures[i] = new NativeCapture.Slot(plans.get(i), descriptor + (long) i * NativeCapture.DESCRIPTOR_BYTES);
            } catch (Throwable failure) {
                try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        void bind(long descriptor) {
            for (int i = 0; i < captures.length; i++)
                captures[i].bind(descriptor + (long) i * NativeCapture.DESCRIPTOR_BYTES);
        }
        @Override public void close() {
            Throwable failure = null;
            for (int i = captures.length - 1; i >= 0; i--) if (captures[i] != null) {
                try { captures[i].close(); }
                catch (Throwable cleanup) { if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup); }
            }
            if (failure instanceof Error error) throw error;
            if (failure instanceof RuntimeException error) throw error;
        }
    }

    record Frame(Pipeline.Row numeric, Expr row, Expr columns, Expr literals, Expr helpers,
                 java.util.Map<Expression, Pipeline.Utf8Value> materialized, List<Pipeline.Utf8Value> joinedValues) {
        Frame(Pipeline.Row numeric, Expr row, Expr columns, Expr literals, Expr helpers,
              java.util.Map<Expression, Pipeline.Utf8Value> materialized) {
            this(numeric, row, columns, literals, helpers, materialized, null);
        }
        Frame(Pipeline.Row numeric, Expr row, Expr columns, Expr literals) {
            this(numeric, row, columns, literals, nil(), null);
        }
        Frame(Pipeline.Row numeric, Expr row, Expr columns, Expr literals,
              java.util.Map<Expression, Pipeline.Utf8Value> materialized) {
            this(numeric, row, columns, literals, nil(), materialized);
        }
        Pipeline.Row numeric(Builder builder) {
            if (numeric.computed() != null) return numeric;
            var values = new java.util.HashMap<Pipeline.FrameScalar, Pipeline.Value>();
            return new Pipeline.Row(numeric.columns(), numeric.values(), numeric.parameters(),
                    expression -> values.computeIfAbsent(expression, key -> key.emit(builder, this)));
        }
        Pipeline.Utf8Value value(Builder b, Expression argument) {
            if (materialized == null) return argument.emit(b, this);
            Pipeline.Utf8Value value = materialized.get(argument);
            if (value == null) throw new IllegalArgumentException("Unmaterialized string argument");
            return value;
        }
        Expr isNull(Builder b, Expression argument) {
            return materialized == null ? argument.isNull(b, this) : value(b, argument).isNull();
        }
    }

    sealed interface Expression permits Column, Literal, Conditional, NativeCapture.Expression {
        Pipeline.Utf8Value emit(Builder builder, Frame frame);
        Expr isNull(Builder builder, Frame frame);
    }

    record Length(Expression argument) implements Pipeline.FrameScalar {
        @Override public int columnType() { return ColumnType.INT; }
        @Override public Pipeline.Value emit(Builder builder, Frame frame) {
            // Utf8s.length(NULL) is -1, an ordinary INT value rather than INT NULL.
            return new Pipeline.Value(ByteSequences.length(builder, frame.value(builder, argument)), bool(false));
        }
        @Override public void collectColumns(java.util.BitSet columns) { collect(argument, columns); }
    }

    private static void collect(Expression expression, java.util.BitSet columns) {
        if (expression instanceof NativeCapture.Expression capture) collect(capture.argument(), columns);
        else if (expression instanceof Conditional conditional) {
            for (Branch branch : conditional.branches) {
                ScanPlan.collect(branch.condition, columns);
                collect(branch.value, columns);
            }
            collect(conditional.otherwise, columns);
        }
    }

    private record Column(int index) implements Expression {
        @Override
        public Pipeline.Utf8Value emit(Builder b, Frame frame) {
            return frame.joinedValues != null ? frame.joinedValues.get(index)
                    : QuestVarchar.decode(b, at(frame.columns, index * 48L), frame.row);
        }
        @Override public Expr isNull(Builder b, Frame frame) {
            return frame.joinedValues != null ? frame.joinedValues.get(index).isNull()
                    : ne(binary(Op.AND, QuestVarchar.header(b, at(frame.columns, index * 48L), frame.row), i64(4)), i64(0));
        }
    }

    private record Literal(int offset, int size, boolean isNull, boolean isAscii) implements Expression {
        @Override
        public Pipeline.Utf8Value emit(Builder b, Frame frame) {
            // A literal descriptor is slot-owned; it must never escape as a stable source pointer.
            return new Pipeline.Utf8Value(at(frame.literals, offset), i64(size), bool(isNull), bool(isAscii), bool(false));
        }
        @Override public Expr isNull(Builder b, Frame frame) { return bool(isNull); }
    }

    private record Branch(Pipeline.Predicate condition, Expression value) { }

    private record Conditional(List<Branch> branches, Expression otherwise) implements Expression {
        @Override public Expr isNull(Builder b, Frame frame) {
            Var isNull = b.let(bool(true)), matched = b.let(bool(false));
            for (Branch branch : branches) {
                b.when(eq(matched, bool(false)), next -> next.when(branch.condition.emit(frame.numeric(next)), selected -> {
                    selected.assign(isNull, branch.value.isNull(selected, frame));
                    selected.assign(matched, bool(true));
                }));
            }
            b.when(eq(matched, bool(false)), selected -> selected.assign(isNull, otherwise.isNull(selected, frame)));
            return isNull;
        }

        @Override
        public Pipeline.Utf8Value emit(Builder b, Frame frame) {
            Var address = b.let(frame.literals), size = b.let(i64(0)), isNull = b.let(bool(true));
            Var isAscii = b.let(bool(true)), stable = b.let(bool(false)), matched = b.let(bool(false));
            for (Branch branch : branches) {
                b.when(eq(matched, bool(false)), next -> next.when(branch.condition.emit(frame.numeric(next)), selected -> {
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
