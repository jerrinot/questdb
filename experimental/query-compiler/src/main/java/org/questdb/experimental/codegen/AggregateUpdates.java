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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.AbstractCountDistinctIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.AvgDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.AvgIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.AvgLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.AvgShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctSymbolGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountSymbolGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountVarcharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxVarcharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinVarcharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumShortGroupByFunction;
import io.questdb.std.ObjList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.IntToLongFunction;

import static org.questdb.experimental.codegen.Ir.*;

/** Bound aggregate lifecycles. All dispatch here emits IR during compilation. */
record AggregateUpdates(ResolvedVarchar.Binding inputs, List<AggregateUpdates.Update> updates) {
    List<Pipeline.Column> columns() { return inputs.numericInputs(); }
    int[] sourceIndexes() { return inputs.numericColumns(); }
    long cardinalityOffset() { return 8 + inputs.bytes(); }

    /** Shared resolved-function binding; state offsets come from the owning engine value layout. */
    static AggregateUpdates bind(ColumnTypes inputTypes, ColumnTypes valueTypes, ObjList<GroupByFunction> functions,
                           IntToLongFunction offsets, boolean allowDistinct) {
        return bind(valueTypes, functions, offsets, allowDistinct, new ResolvedInputs(inputTypes));
    }

    static AggregateUpdates bind(ColumnTypes valueTypes, ObjList<GroupByFunction> functions,
                           IntToLongFunction offsets, boolean allowDistinct, ResolvedInputs inputs) {
        return bind(valueTypes, functions, offsets, allowDistinct, inputs, false);
    }

    static AggregateUpdates bind(ColumnTypes valueTypes, ObjList<GroupByFunction> functions,
                                   IntToLongFunction offsets, boolean allowDistinct, ResolvedInputs inputs,
                                   boolean mutableMap) {
        if (functions.size() == 0) {
            return null;
        }
        var updates = new ArrayList<Update>();
        var strings = new ResolvedVarchar(inputs.types(), inputs);
        var assignedSlots = new HashSet<Integer>();
        for (int i = 0; i < functions.size(); i++) {
            GroupByFunction function = functions.getQuick(i);
            Class<?> implementation = function.getClass();
            int argumentType;
            boolean average = isAverage(implementation);
            boolean floating = implementation == SumDoubleGroupByFunction.class || implementation == MinDoubleGroupByFunction.class
                    || implementation == MaxDoubleGroupByFunction.class || implementation == AvgDoubleGroupByFunction.class;
            int stateType = floating || average && implementation != AvgShortGroupByFunction.class ? ColumnType.DOUBLE : ColumnType.LONG;
            boolean sum;
            boolean minimum = implementation == MinLongGroupByFunction.class || implementation == MinIntGroupByFunction.class
                    || implementation == MinShortGroupByFunction.class;
            boolean extremum = minimum || implementation == MaxLongGroupByFunction.class || implementation == MaxIntGroupByFunction.class
                    || implementation == MaxShortGroupByFunction.class;
            if (extremum) {
                argumentType = implementation == MinLongGroupByFunction.class || implementation == MaxLongGroupByFunction.class
                        ? ColumnType.LONG : implementation == MinIntGroupByFunction.class || implementation == MaxIntGroupByFunction.class
                        ? ColumnType.INT : ColumnType.SHORT;
                stateType = argumentType == ColumnType.LONG ? ColumnType.LONG : ColumnType.INT;
                sum = false;
            } else if (implementation == CountLongConstGroupByFunction.class) {
                argumentType = ColumnType.UNDEFINED;
                sum = false;
            } else if (floating || implementation == CountDoubleGroupByFunction.class) {
                argumentType = ColumnType.DOUBLE;
                sum = false;
            } else if (implementation == CountLongGroupByFunction.class || implementation == SumLongGroupByFunction.class
                    || implementation == CountDistinctLongGroupByFunction.class || implementation == AvgLongGroupByFunction.class) {
                argumentType = ColumnType.LONG;
                sum = implementation == SumLongGroupByFunction.class;
            } else if (implementation == CountIntGroupByFunction.class || implementation == SumIntGroupByFunction.class
                    || implementation == CountDistinctIntGroupByFunction.class || implementation == AvgIntGroupByFunction.class) {
                argumentType = ColumnType.INT;
                sum = implementation == SumIntGroupByFunction.class;
            } else if (implementation == CountSymbolGroupByFunction.class || implementation == CountDistinctSymbolGroupByFunction.class) {
                argumentType = ColumnType.SYMBOL;
                sum = false;
            } else if (implementation == SumShortGroupByFunction.class || implementation == AvgShortGroupByFunction.class) {
                argumentType = ColumnType.SHORT;
                sum = true;
            } else if (implementation == CountVarcharGroupByFunction.class
                    || mutableMap && (implementation == MinVarcharGroupByFunction.class || implementation == MaxVarcharGroupByFunction.class)) {
                argumentType = ColumnType.VARCHAR;
                sum = false;
            } else {
                return null;
            }
            int valueIndex = function.getValueIndex();
            if (valueIndex < 0 || valueIndex >= valueTypes.getColumnCount()
                    || valueTypes.getColumnType(valueIndex) != stateType || !assignedSlots.add(valueIndex)) {
                throw new IllegalArgumentException("Resolved aggregate state does not match its assigned slot");
            }
            boolean distinct = implementation == CountDistinctLongGroupByFunction.class
                    || implementation == CountDistinctIntGroupByFunction.class || implementation == CountDistinctSymbolGroupByFunction.class;
            if (distinct && !allowDistinct) return null;
            if (argumentType == ColumnType.VARCHAR) {
                var expression = strings.bind(((UnaryFunction) function).getArg());
                if (expression == null) return null;
                updates.add(implementation == CountVarcharGroupByFunction.class
                        ? new VarcharCount(expression, offsets.applyAsLong(valueIndex))
                        : new VarcharExtremum(expression, offsets.applyAsLong(valueIndex), implementation == MinVarcharGroupByFunction.class));
                continue;
            }
            if ((distinct || average) && (valueIndex + 1 >= valueTypes.getColumnCount()
                    || valueTypes.getColumnType(valueIndex + 1) != ColumnType.LONG || !assignedSlots.add(valueIndex + 1)
                    || (distinct && offsets.applyAsLong(valueIndex + 1) != offsets.applyAsLong(valueIndex) + 8))) {
                throw new IllegalArgumentException("Resolved aggregate count or payload state does not match its assigned slots");
            }
            Pipeline.RowAggregate operation;
            if (argumentType == ColumnType.UNDEFINED) {
                operation = new Pipeline.Count();
            } else {
                Function argument = ((UnaryFunction) function).getArg();
                Pipeline.Scalar expression = inputs.scalar(argument, argumentType);
                if (expression == null) return null;
                if (extremum) {
                    updates.add(new IntegerExtremum(expression, offsets.applyAsLong(valueIndex),
                            stateType == ColumnType.LONG ? MemoryType.I64 : MemoryType.I32, minimum));
                    continue;
                }
                if (average) {
                    updates.add(new AverageUpdate(expression, offsets.applyAsLong(valueIndex), offsets.applyAsLong(valueIndex + 1),
                            stateType == ColumnType.DOUBLE ? MemoryType.F64 : MemoryType.I64));
                    continue;
                }
                if (floating) {
                    long offset = offsets.applyAsLong(valueIndex);
                    updates.add(implementation == SumDoubleGroupByFunction.class ? new DoubleSum(expression, offset)
                            : new DoubleExtremum(expression, offset, implementation == MinDoubleGroupByFunction.class));
                    continue;
                }
                if (distinct) {
                    if (function instanceof CountDistinctLongGroupByFunction count) {
                        updates.add(new DistinctUpdate(expression, offsets.applyAsLong(valueIndex), NestedDistinct.LONG,
                                count.getSetInitialCapacity(), count.getSetLoadFactor()));
                    } else {
                        var count = (AbstractCountDistinctIntGroupByFunction) function;
                        updates.add(new DistinctUpdate(expression, offsets.applyAsLong(valueIndex), NestedDistinct.INT,
                                count.getSetInitialCapacity(), count.getSetLoadFactor()));
                    }
                    continue;
                }
                operation = sum ? new Pipeline.Sum(expression) : new Pipeline.CountValue(expression);
            }
            updates.add(new ScalarUpdate(operation, offsets.applyAsLong(valueIndex)));
        }
        return new AggregateUpdates(strings.binding(), List.copyOf(updates));
    }

    /** Frame loading is one producer; joined or computed values use emit directly. */
    Pipeline.Row loadFrameRow(Builder b, Var rowIndex, List<Var> addresses) {
        return loadFrameRow(b, rowIndex, addresses, null);
    }

    private Pipeline.Row loadFrameRow(Builder b, Var rowIndex, List<Var> addresses, java.util.BitSet required) {
        var columns = columns();
        var values = new ArrayList<Pipeline.Value>();
        for (int i = 0; i < columns.size(); i++) {
            if (required != null && !required.get(i)) { values.add(null); continue; }
            var column = columns.get(i);
            Var value = b.let(new Load(column.width(), new Offset(addresses.get(i),
                    binary(Op.MUL, rowIndex, i64(column.width().bytes)))));
            values.add(column.value(value));
        }
        return new Pipeline.Row(columns, values);
    }

    void initialize(Builder b, Expr destination) {
        for (var update : updates) update.initialize(b, destination);
    }

    void merge(Builder b, Expr source, Expr destination) {
        for (var update : updates) update.merge(b, source, destination);
    }

    void emit(Builder b, Pipeline.Row row, Expr destination, Expr cardinalityAddress) {
        var frame = new ResolvedVarchar.Frame(row, i64(0), nil(), nil());
        emit(b, frame, destination, cardinalityAddress);
    }

    void emit(Builder b, ResolvedVarchar.Frame source, Expr destination, Expr cardinalityAddress) {
        var frame = new ResolvedVarchar.Frame(source.numeric(b), source.row(), source.columns(), source.literals(), source.helpers(), source.materialized());
        for (var update : updates) update.emit(b, frame, destination, cardinalityAddress);
    }

    void emitFrame(Builder b, Var row, List<Var> addresses, Expr destination, Expr cardinalityAddress) {
        var frame = inputs.frame(loadFrameRow(b, row, addresses), row, at(COLUMNS, 8));
        emit(b, frame, destination, cardinalityAddress);
    }

    void finish(Builder b, Expr destination, boolean deferred) {
        for (var update : updates) update.finish(b, destination, deferred);
    }

    void prefetch(Builder b, Var row, List<Var> addresses, Expr destination) {
        for (var update : updates) update.prefetch(b, argument -> {
            var required = ScanPlan.of(columns(), new Pipeline.IsNotNull(argument)).predicateColumns();
            return argument.emit(inputs.frame(loadFrameRow(b, row, addresses, required), row, at(COLUMNS, 8)).numeric(b));
        }, destination);
    }

    void prefetchHeaders(Builder b, Expr destination) {
        for (var update : updates) if (update instanceof DistinctUpdate distinct) {
            Expr countAddress = at(destination, distinct.offset());
            b.when(binary(Op.GE, load(countAddress), i64(2)), existing ->
                    existing.prefetch(new Load(MemoryType.ADDRESS, at(countAddress, 8)), false));
        }
    }

    private static boolean isAverage(Class<?> implementation) {
        return implementation == AvgShortGroupByFunction.class || implementation == AvgIntGroupByFunction.class
                || implementation == AvgLongGroupByFunction.class || implementation == AvgDoubleGroupByFunction.class;
    }

    static Function outputFunction(Function function) {
        return isAverage(function.getClass())
                ? DoubleColumn.newInstance(((GroupByFunction) function).getValueIndex()) : function;
    }
    sealed interface Update permits ScalarUpdate, IntegerExtremum, DoubleSum, DoubleExtremum, DistinctUpdate, AverageUpdate, VarcharCount, VarcharExtremum, VectorSumUpdate, VectorLongAverageUpdate, VectorDoubleCount, VectorDoubleExtremum {
        long offset();
        void initialize(Builder b, Expr destination);
        void merge(Builder b, Expr source, Expr destination);
        void emit(Builder builder, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress);
        default boolean allocates() { return false; }
        default boolean finalizesResult() { return false; }
        default boolean deferredMerge() { return false; }
        default void finish(Builder b, Expr destination, boolean deferred) { }
        default void prefetch(Builder b, java.util.function.Function<Pipeline.Scalar, Pipeline.Value> input, Expr destination) { }
    }

    record VarcharCount(ResolvedVarchar.Expression argument, long offset) implements Update {
        @Override public void initialize(Builder b, Expr destination) { b.store(at(destination, offset), i64(0)); }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            b.store(at(destination, offset), add(load(at(destination, offset)), load(at(source, offset))));
        }
        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            Expr present = new Select(frame.isNull(b, argument), i64(0), i64(1));
            b.store(at(destination, offset), add(load(at(destination, offset)), present));
        }
    }

    /** SUM(DOUBLE) ignores NaN inputs and replaces a NaN accumulator on the next value. */
    record DoubleSum(Pipeline.Scalar argument, long offset) implements Update {
        @Override public void initialize(Builder b, Expr destination) { b.store(MemoryType.F64, at(destination, offset), f64(Double.NaN)); }
        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            combine(b, destination, argument.emit(frame.numeric(b)).data());
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            combine(b, destination, new Load(MemoryType.F64, at(source, offset)));
        }
        private void combine(Builder b, Expr destination, Expr value) {
            Var incoming = b.let(value);
            b.when(binary(Op.FEQ, incoming, incoming), present -> {
                Expr address = at(destination, offset);
                Var current = present.let(new Load(MemoryType.F64, address));
                present.store(MemoryType.F64, address, new Select(binary(Op.FEQ, current, current),
                        binary(Op.FADD, current, incoming), incoming));
            });
        }
    }

    /** Preserve the ordinary extrema rule: a nonfinite current value is replaceable. */
    record DoubleExtremum(Pipeline.Scalar argument, long offset, boolean minimum) implements Update {
        @Override public void initialize(Builder b, Expr destination) { b.store(MemoryType.F64, at(destination, offset), f64(Double.NaN)); }
        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            combine(b, destination, argument.emit(frame.numeric(b)).data());
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            combine(b, destination, new Load(MemoryType.F64, at(source, offset)));
        }
        private void combine(Builder b, Expr destination, Expr value) {
            Var incoming = b.let(value), current = b.let(new Load(MemoryType.F64, at(destination, offset)));
            Expr better = minimum ? binary(Op.FLT, incoming, current) : binary(Op.FLT, current, incoming);
            b.when(binary(Op.EITHER, better, Pipeline.doubleNull(current)), replace ->
                    replace.store(MemoryType.F64, at(destination, offset), incoming));
        }
    }

    /** Rosti SUM/AVG with a single-word sum and non-NaN/non-null count. */
    record VectorSumUpdate(Pipeline.Scalar argument, long offset, MemoryType sumType) implements Update {
        VectorSumUpdate(Pipeline.Scalar argument, long offset) { this(argument, offset, MemoryType.I64); }
        @Override public void initialize(Builder b, Expr destination) {
            b.store(sumType, at(destination, offset), sumType == MemoryType.F64 ? f64(0) : i64(0));
            b.store(at(destination, offset + 8), i64(0));
        }
        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            var value = argument.emit(frame.numeric(b));
            if (sumType == MemoryType.F64) {
                Expr present = binary(Op.FEQ, value.data(), value.data());
                b.store(MemoryType.F64, at(destination, offset), binary(Op.FADD,
                        new Load(MemoryType.F64, at(destination, offset)), new Select(present, value.data(), f64(0))));
                b.store(at(destination, offset + 8), add(load(at(destination, offset + 8)), new Select(present, i64(1), i64(0))));
                return;
            }
            b.when(eq(value.isNull(), bool(false)), valid -> {
                valid.store(at(destination, offset), add(load(at(destination, offset)), value.data()));
                valid.store(at(destination, offset + 8), add(load(at(destination, offset + 8)), i64(1)));
            });
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            b.store(sumType, at(destination, offset), binary(sumType == MemoryType.F64 ? Op.FADD : Op.ADD,
                    new Load(sumType, at(destination, offset)), new Load(sumType, at(source, offset))));
            b.store(at(destination, offset + 8), add(load(at(destination, offset + 8)), load(at(source, offset + 8))));
        }
    }

    /** The vector COUNT(DOUBLE) implementation counts infinities and excludes NaNs. */
    record VectorDoubleCount(Pipeline.Scalar argument, long offset) implements Update {
        @Override public void initialize(Builder b, Expr destination) { b.store(at(destination, offset), i64(0)); }
        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            Expr value = argument.emit(frame.numeric(b)).data();
            b.store(at(destination, offset), add(load(at(destination, offset)), new Select(binary(Op.FEQ, value, value), i64(1), i64(0))));
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            b.store(at(destination, offset), add(load(at(destination, offset)), load(at(source, offset))));
        }
    }

    /** Rosti normalizes NaNs to an infinite sentinel and takes the incoming value on ties. */
    record VectorDoubleExtremum(Pipeline.Scalar argument, long offset, boolean minimum) implements Update {
        private Expr empty() { return f64(minimum ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY); }
        @Override public void initialize(Builder b, Expr destination) { b.store(MemoryType.F64, at(destination, offset), empty()); }
        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            combine(b, destination, argument.emit(frame.numeric(b)).data());
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            combine(b, destination, new Load(MemoryType.F64, at(source, offset)));
        }
        private void combine(Builder b, Expr destination, Expr value) {
            Var raw = b.let(value);
            Var incoming = b.let(new Select(binary(Op.FEQ, raw, raw), raw, empty()));
            Var current = b.let(new Load(MemoryType.F64, at(destination, offset)));
            Expr replace = minimum ? binary(Op.FGE, current, incoming) : binary(Op.FGE, incoming, current);
            b.store(MemoryType.F64, at(destination, offset), new Select(replace, incoming, current));
        }
    }

    /** Rosti AVG(LONG): signed 128-bit sum (low, high), then non-null count. */
    record VectorLongAverageUpdate(Pipeline.Scalar argument, long offset) implements Update {
        @Override public void initialize(Builder b, Expr destination) {
            for (int i = 0; i < 3; i++) b.store(at(destination, offset + i * 8), i64(0));
        }
        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            var value = argument.emit(frame.numeric(b));
            b.when(eq(value.isNull(), bool(false)), valid -> {
                addSum(valid, destination, value.data(), new Select(binary(Op.LT, value.data(), i64(0)), i64(-1), i64(0)));
                valid.store(at(destination, offset + 16), add(load(at(destination, offset + 16)), i64(1)));
            });
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            addSum(b, destination, load(at(source, offset)), load(at(source, offset + 8)));
            b.store(at(destination, offset + 16), add(load(at(destination, offset + 16)), load(at(source, offset + 16))));
        }
        private void addSum(Builder b, Expr destination, Expr low, Expr high) {
            Var previous = b.let(load(at(destination, offset)));
            Var sum = b.let(add(previous, low));
            // Bias both operands for an unsigned carry comparison using the signed IR.
            Expr carry = binary(Op.LT, binary(Op.XOR, sum, i64(Long.MIN_VALUE)), binary(Op.XOR, previous, i64(Long.MIN_VALUE)));
            b.store(at(destination, offset + 8), add(add(load(at(destination, offset + 8)), high), new Select(carry, i64(1), i64(0))));
            b.store(at(destination, offset), sum);
        }
    }

    record ScalarUpdate(Pipeline.RowAggregate operation, long offset) implements Update {
        @Override public void initialize(Builder b, Expr destination) {
            b.store(at(destination, offset), i64(operation.initialValue()));
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            operation.merge(b, at(source, offset), at(destination, offset));
        }
        @Override
        public void emit(Builder builder, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            operation.emit(builder, frame.numeric(builder), at(destination, offset));
        }
    }

    /** Existing MIN/MAX state: LONG for LONG input, INT for INT/SHORT input. */
    record IntegerExtremum(Pipeline.Scalar argument, long offset, MemoryType width, boolean minimum) implements Update {
        private long nullValue() { return width == MemoryType.I64 ? Long.MIN_VALUE : Integer.MIN_VALUE; }

        @Override public void initialize(Builder b, Expr destination) {
            b.store(width, at(destination, offset), i64(nullValue()));
        }

        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            var value = argument.emit(frame.numeric(b));
            combine(b, destination, new Select(value.isNull(), i64(nullValue()), value.data()));
        }

        @Override public void merge(Builder b, Expr source, Expr destination) {
            combine(b, destination, new Load(width, at(source, offset)));
        }

        private void combine(Builder b, Expr destination, Expr value) {
            Var incoming = b.let(value);
            Expr address = at(destination, offset);
            Var current = b.let(new Load(width, address));
            Expr replace = minimum
                    ? binary(Op.BOTH, ne(incoming, i64(nullValue())), binary(Op.EITHER,
                            eq(current, i64(nullValue())), binary(Op.LT, incoming, current)))
                    : binary(Op.LT, current, incoming);
            b.store(width, address, new Select(replace, incoming, current));
        }
    }

    /** Ordinary averages: SHORT has a LONG sum; INT/LONG/DOUBLE accumulate in DOUBLE. */
    record AverageUpdate(Pipeline.Scalar argument, long offset, long countOffset, MemoryType sumType) implements Update {
        @Override public boolean finalizesResult() { return true; }
        @Override public void initialize(Builder b, Expr destination) {
            b.store(sumType, at(destination, offset), sumType == MemoryType.F64 ? f64(0) : i64(Long.MIN_VALUE));
            b.store(at(destination, countOffset), i64(0));
        }
        @Override
        public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            Expr sumAddress = at(destination, offset), countAddress = at(destination, countOffset);
            Var count = b.let(load(countAddress));
            var input = argument.emit(frame.numeric(b));
            Expr missing = input.data().type() == Type.F64 ? binary(Op.FNE, input.data(), input.data()) : input.isNull();
            if (sumType == MemoryType.F64) {
                // Ordinary computeFirst(NULL) stores (0,0). A later Java computeNext
                // adds directly, so leaving the empty etalon's NaN here is unsafe.
                b.when(binary(Op.BOTH, missing, eq(count, i64(0))), absent ->
                        absent.store(MemoryType.F64, sumAddress, f64(0)));
            }
            b.when(eq(missing, bool(false)), present -> {
                Expr value = sumType == MemoryType.F64 && input.data().type() == Type.I64 ? new ToDouble(input.data()) : input.data();
                Expr sum = binary(sumType == MemoryType.F64 ? Op.FADD : Op.ADD, new Load(sumType, sumAddress), value);
                present.store(sumType, sumAddress, new Select(eq(count, i64(0)), value, sum));
                present.store(countAddress, add(count, i64(1)));
            });
        }

        @Override public void merge(Builder b, Expr source, Expr destination) {
            Var count = b.let(load(at(source, countOffset)));
            b.when(binary(Op.LT, i64(0), count), nonempty -> {
                Var destinationCount = nonempty.let(load(at(destination, countOffset)));
                Var sum = nonempty.let(new Load(sumType, at(source, offset)));
                Var populated = nonempty.let(binary(Op.LT, i64(0), destinationCount));
                Expr combined = binary(sumType == MemoryType.F64 ? Op.FADD : Op.ADD, new Load(sumType, at(destination, offset)), sum);
                nonempty.store(sumType, at(destination, offset), new Select(populated, combined, sum));
                nonempty.store(at(destination, countOffset), new Select(populated, add(destinationCount, count), count));
            });
        }

        @Override public void finish(Builder b, Expr destination, boolean deferred) {
            Var sum = b.let(new Load(sumType, at(destination, offset)));
            Var count = b.let(load(at(destination, countOffset)));
            Var value = b.let(f64(Double.NaN));
            Expr valid = binary(Op.LT, i64(0), count);
            if (sumType == MemoryType.I64) valid = binary(Op.BOTH, ne(sum, i64(Long.MIN_VALUE)), valid);
            b.when(valid, nonempty -> nonempty.assign(value, binary(Op.FDIV,
                    sumType == MemoryType.F64 ? sum : new ToDouble(sum), new ToDouble(count))));
            b.store(MemoryType.F64, at(destination, offset), value);
        }
    }

    record DistinctUpdate(Pipeline.Scalar argument, long offset, NestedDistinct layout, int initialCapacity, double loadFactor) implements Update {
        @Override public boolean allocates() { return true; }
        @Override public boolean deferredMerge() { return layout == NestedDistinct.LONG; }
        @Override public void initialize(Builder b, Expr destination) {
            b.store(at(destination, offset), i64(0));
            b.store(at(destination, offset + 8), i64(0));
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            if (deferredMerge()) {
                layout.mergeWithCapacity(b, at(source, offset), at(destination, offset),
                        initialCapacity, loadFactor, at(STATE, DeferredDistinct.CONTROL));
            } else {
                layout.mergeWithCapacity(b, at(source, offset), at(destination, offset), initialCapacity, loadFactor);
            }
        }
        @Override public void finish(Builder b, Expr destination, boolean deferred) {
            if (deferred && deferredMerge()) DeferredDistinct.finish(b, at(destination, offset));
        }
        @Override public void prefetch(Builder b, java.util.function.Function<Pipeline.Scalar, Pipeline.Value> input, Expr destination) {
            layout.prefetch(b, input.apply(argument), at(destination, offset));
        }
        @Override
        public void emit(Builder builder, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            Expr countAddress = at(destination, offset);
            Var before = builder.let(load(countAddress));
            layout.emitWithCapacity(builder, argument.emit(frame.numeric(builder)), countAddress, initialCapacity, loadFactor);
            builder.when(ne(before, load(countAddress)), changed ->
                    changed.store(cardinalityAddress, add(load(cardinalityAddress), i64(1))));
        }
    }
}
