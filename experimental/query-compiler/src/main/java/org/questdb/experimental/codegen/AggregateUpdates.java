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
import io.questdb.griffin.engine.functions.groupby.AvgShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongGroupByFunction;
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
record AggregateUpdates(List<Pipeline.Column> columns, int[] sourceIndexes, List<AggregateUpdates.Update> updates) {
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
                                   boolean allowFinalization) {
        if (functions.size() == 0) {
            return null;
        }
        var updates = new ArrayList<Update>();
        var assignedSlots = new HashSet<Integer>();
        for (int i = 0; i < functions.size(); i++) {
            GroupByFunction function = functions.getQuick(i);
            Class<?> implementation = function.getClass();
            int argumentType;
            boolean sum;
            if (implementation == CountLongConstGroupByFunction.class) {
                argumentType = ColumnType.UNDEFINED;
                sum = false;
            } else if (implementation == CountLongGroupByFunction.class || implementation == SumLongGroupByFunction.class
                    || implementation == CountDistinctLongGroupByFunction.class) {
                argumentType = ColumnType.LONG;
                sum = implementation == SumLongGroupByFunction.class;
            } else if (implementation == CountIntGroupByFunction.class || implementation == SumIntGroupByFunction.class) {
                argumentType = ColumnType.INT;
                sum = implementation == SumIntGroupByFunction.class;
            } else if (implementation == SumShortGroupByFunction.class || (allowFinalization && implementation == AvgShortGroupByFunction.class)) {
                argumentType = ColumnType.SHORT;
                sum = true;
            } else {
                return null;
            }
            int valueIndex = function.getValueIndex();
            if (valueIndex < 0 || valueIndex >= valueTypes.getColumnCount()
                    || valueTypes.getColumnType(valueIndex) != ColumnType.LONG || !assignedSlots.add(valueIndex)) {
                throw new IllegalArgumentException("Resolved aggregate state does not match its assigned LONG slot");
            }
            CountDistinctLongGroupByFunction distinct = implementation == CountDistinctLongGroupByFunction.class
                    ? (CountDistinctLongGroupByFunction) function : null;
            if (distinct != null && !allowDistinct) return null;
            boolean average = implementation == AvgShortGroupByFunction.class;
            if ((distinct != null || average) && (valueIndex + 1 >= valueTypes.getColumnCount()
                    || valueTypes.getColumnType(valueIndex + 1) != ColumnType.LONG || !assignedSlots.add(valueIndex + 1)
                    || offsets.applyAsLong(valueIndex + 1) != offsets.applyAsLong(valueIndex) + 8)) {
                throw new IllegalArgumentException("Resolved aggregate state requires contiguous LONG slots");
            }
            Pipeline.RowAggregate operation;
            if (argumentType == ColumnType.UNDEFINED) {
                operation = new Pipeline.Count();
            } else {
                Function argument = ((UnaryFunction) function).getArg();
                Pipeline.Scalar expression = inputs.scalar(argument, argumentType);
                if (expression == null) return null;
                if (average) {
                    updates.add(new AverageUpdate(expression, offsets.applyAsLong(valueIndex)));
                    continue;
                }
                if (distinct != null) {
                    updates.add(new DistinctUpdate(expression, offsets.applyAsLong(valueIndex), distinct.getSetInitialCapacity(), distinct.getSetLoadFactor()));
                    continue;
                }
                operation = sum ? new Pipeline.Sum(expression) : new Pipeline.CountValue(expression);
            }
            updates.add(new ScalarUpdate(operation, offsets.applyAsLong(valueIndex)));
        }
        return new AggregateUpdates(inputs.columns(), inputs.indexes(), List.copyOf(updates));
    }

    /** Frame loading is one producer; joined or computed values use emit directly. */
    Pipeline.Row loadFrameRow(Builder b, Var rowIndex, List<Var> addresses) {
        return loadFrameRow(b, rowIndex, addresses, null);
    }

    private Pipeline.Row loadFrameRow(Builder b, Var rowIndex, List<Var> addresses, java.util.BitSet required) {
        var values = new ArrayList<Pipeline.Value>();
        for (int i = 0; i < columns.size(); i++) {
            if (required != null && !required.get(i)) { values.add(null); continue; }
            var column = columns.get(i);
            Var value = b.let(new Load(column.width(), new Offset(addresses.get(i),
                    binary(Op.MUL, rowIndex, i64(column.width().bytes)))));
            values.add(new Pipeline.Value(value, column.nullable() ? eq(value, i64(column.nullValue())) : bool(false)));
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
        for (var update : updates) update.emit(b, row, destination, cardinalityAddress);
    }

    void finish(Builder b, Expr destination, boolean deferred) {
        for (var update : updates) update.finish(b, destination, deferred);
    }

    void prefetch(Builder b, Var row, List<Var> addresses, Expr destination) {
        for (var update : updates) update.prefetch(b, argument -> {
            var required = ScanPlan.of(columns, new Pipeline.IsNotNull(argument)).predicateColumns();
            return argument.emit(loadFrameRow(b, row, addresses, required));
        }, destination);
    }

    static Function outputFunction(Function function) {
        return function.getClass() == AvgShortGroupByFunction.class
                ? DoubleColumn.newInstance(((GroupByFunction) function).getValueIndex()) : function;
    }
    sealed interface Update permits ScalarUpdate, DistinctUpdate, AverageUpdate {
        void initialize(Builder b, Expr destination);
        void merge(Builder b, Expr source, Expr destination);
        void emit(Builder builder, Pipeline.Row row, Expr destination, Expr cardinalityAddress);
        default boolean allocates() { return false; }
        default boolean finalizesResult() { return false; }
        default boolean deferredMerge() { return false; }
        default void finish(Builder b, Expr destination, boolean deferred) { }
        default void prefetch(Builder b, java.util.function.Function<Pipeline.Scalar, Pipeline.Value> input, Expr destination) { }
    }

    record ScalarUpdate(Pipeline.RowAggregate operation, long offset) implements Update {
        @Override public void initialize(Builder b, Expr destination) {
            b.store(at(destination, offset), i64(operation.initialValue()));
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            operation.merge(b, at(source, offset), at(destination, offset));
        }
        @Override
        public void emit(Builder builder, Pipeline.Row row, Expr destination, Expr cardinalityAddress) {
            operation.emit(builder, row, at(destination, offset));
        }
    }

    /** AvgShortGroupByFunction's LONG sum/count state, including its sentinel and merge rules. */
    record AverageUpdate(Pipeline.Scalar argument, long offset) implements Update {
        @Override public boolean finalizesResult() { return true; }
        @Override public void initialize(Builder b, Expr destination) {
            b.store(at(destination, offset), i64(Long.MIN_VALUE));
            b.store(at(destination, offset + 8), i64(0));
        }
        @Override
        public void emit(Builder b, Pipeline.Row row, Expr destination, Expr cardinalityAddress) {
            Expr sumAddress = at(destination, offset), countAddress = at(destination, offset + 8);
            Var count = b.let(load(countAddress));
            Expr value = argument.emit(row).data();
            b.store(sumAddress, new Select(eq(count, i64(0)), value, add(load(sumAddress), value)));
            b.store(countAddress, add(count, i64(1)));
        }

        @Override public void merge(Builder b, Expr source, Expr destination) {
            Var count = b.let(load(at(source, offset + 8)));
            b.when(binary(Op.LT, i64(0), count), nonempty -> {
                Var destinationCount = nonempty.let(load(at(destination, offset + 8)));
                Var sum = nonempty.let(load(at(source, offset)));
                Var populated = nonempty.let(binary(Op.LT, i64(0), destinationCount));
                nonempty.store(at(destination, offset), new Select(populated, add(load(at(destination, offset)), sum), sum));
                nonempty.store(at(destination, offset + 8), new Select(populated, add(destinationCount, count), count));
            });
        }

        @Override public void finish(Builder b, Expr destination, boolean deferred) {
            Var sum = b.let(load(at(destination, offset)));
            Var count = b.let(load(at(destination, offset + 8)));
            Var value = b.let(f64(Double.NaN));
            b.when(binary(Op.BOTH, ne(sum, i64(Long.MIN_VALUE)), binary(Op.LT, i64(0), count)), valid ->
                    valid.assign(value, binary(Op.FDIV, new ToDouble(sum), new ToDouble(count))));
            b.store(MemoryType.F64, at(destination, offset), value);
        }
    }

    record DistinctUpdate(Pipeline.Scalar argument, long offset, int initialCapacity, double loadFactor) implements Update {
        @Override public boolean allocates() { return true; }
        @Override public boolean deferredMerge() { return true; }
        @Override public void initialize(Builder b, Expr destination) {
            b.store(at(destination, offset), i64(0));
            b.store(at(destination, offset + 8), i64(0));
        }
        @Override public void merge(Builder b, Expr source, Expr destination) {
            NestedDistinct.merge(b, at(source, offset), at(destination, offset),
                    initialCapacity, loadFactor, at(STATE, DeferredDistinct.CONTROL));
        }
        @Override public void finish(Builder b, Expr destination, boolean deferred) {
            if (deferred) DeferredDistinct.finish(b, at(destination, offset));
        }
        @Override public void prefetch(Builder b, java.util.function.Function<Pipeline.Scalar, Pipeline.Value> input, Expr destination) {
            NestedDistinct.prefetch(b, input.apply(argument), at(destination, offset));
        }
        @Override
        public void emit(Builder builder, Pipeline.Row row, Expr destination, Expr cardinalityAddress) {
            Expr countAddress = at(destination, offset);
            Var before = builder.let(load(countAddress));
            NestedDistinct.emitWithCapacity(builder, argument.emit(row), countAddress, initialCapacity, loadFactor);
            builder.when(ne(before, load(countAddress)), changed ->
                    changed.store(cardinalityAddress, add(load(cardinalityAddress), i64(1))));
        }
    }
}
