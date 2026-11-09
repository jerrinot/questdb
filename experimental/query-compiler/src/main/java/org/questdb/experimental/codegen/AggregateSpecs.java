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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntToLongFunction;

import static io.questdb.cairo.ColumnType.*;
import static org.questdb.experimental.codegen.AggregateUpdates.*;
import static org.questdb.experimental.codegen.Ir.*;

/** Exact ordinary implementations and their generated state/result contracts. Binding only, never row dispatch. */
final class AggregateSpecs {
    private static final Map<Class<?>, Spec> SPECS = specifications();

    private AggregateSpecs() { }

    static Update bind(GroupByFunction function, ColumnTypes values, IntToLongFunction offsets,
                       Set<Integer> assigned, ResolvedInputs inputs, boolean allowDistinct, boolean mutableMap) {
        Spec spec = SPECS.get(function.getClass());
        if (spec == null || spec.requirement == Requirement.DISTINCT && !allowDistinct
                || spec.requirement == Requirement.MUTABLE_MAP && !mutableMap) return null;
        int[] types = spec.stateTypes.apply(function);
        int start = function.getValueIndex();
        if (start < 0 || start > values.getColumnCount() - types.length)
            throw new IllegalArgumentException("Aggregate state exceeds assigned slots: " + function.getClass().getSimpleName());
        long[] state = new long[types.length];
        for (int i = 0; i < types.length; i++) {
            if (values.getColumnType(start + i) != types[i] || !assigned.add(start + i))
                throw new IllegalArgumentException("Aggregate state type or slot overlap: " + function.getClass().getSimpleName());
            state[i] = offsets.applyAsLong(start + i);
            if (spec.contiguous && state[i] != state[0] + i * 8L)
                throw new IllegalArgumentException("Top-K requires contiguous LONG state slots");
        }
        Update update = spec.factory.bind(function, inputs, state);
        if (update != null && spec.reader == Reader.FINALIZED_DOUBLE
                && (!update.finalizesResult() || update.resultAccess() != ResultAccess.FINALIZED))
            throw new IllegalArgumentException("Finalized reader requires finalized aggregate state");
        return update;
    }

    static Function outputFunction(Function function) {
        Spec spec = SPECS.get(function.getClass());
        return spec != null && spec.reader == Reader.FINALIZED_DOUBLE
                ? DoubleColumn.newInstance(((GroupByFunction) function).getValueIndex()) : function;
    }

    private static Map<Class<?>, Spec> specifications() {
        var specs = new HashMap<Class<?>, Spec>();
        add(specs, CountLongConstGroupByFunction.class, new int[]{LONG},
                (f, in, s) -> new ScalarUpdate(new Pipeline.Count(), s[0]));
        count(specs, CountLongGroupByFunction.class, LONG);
        count(specs, CountIntGroupByFunction.class, INT);
        count(specs, CountDoubleGroupByFunction.class, DOUBLE);
        count(specs, CountFloatGroupByFunction.class, FLOAT);
        count(specs, CountIPv4GroupByFunction.class, IPv4);
        count(specs, CountSymbolGroupByFunction.class, SYMBOL);
        add(specs, CountStrGroupByFunction.class, new int[]{LONG}, (f, in, s) -> {
            Function arg = ((UnaryFunction) f).getArg();
            if (arg.getType() != CHAR) return null;
            var value = in.scalar(arg, CHAR);
            return value == null ? null : new ScalarUpdate(new Pipeline.CountValue(value), s[0]);
        });
        varchar(specs, CountVarcharGroupByFunction.class, false, false);
        varchar(specs, MinVarcharGroupByFunction.class, true, true);
        varchar(specs, MaxVarcharGroupByFunction.class, true, false);

        sum(specs, SumShortGroupByFunction.class, SHORT);
        sum(specs, SumIntGroupByFunction.class, INT);
        sum(specs, SumLongGroupByFunction.class, LONG);
        unary(specs, SumFloatGroupByFunction.class, FLOAT, new int[]{FLOAT}, Reader.ORDINARY,
                (v, s) -> new FloatingSum(v, s[0], MemoryType.F32));
        unary(specs, SumDoubleGroupByFunction.class, DOUBLE, new int[]{DOUBLE}, Reader.ORDINARY,
                (v, s) -> new FloatingSum(v, s[0], MemoryType.F64));
        unary(specs, KSumDoubleGroupByFunction.class, DOUBLE, new int[]{DOUBLE, DOUBLE, LONG}, Reader.ORDINARY,
                (v, s) -> new KahanSum(v, s[0], s[1], s[2]));
        unary(specs, NSumDoubleGroupByFunction.class, DOUBLE, new int[]{DOUBLE, DOUBLE, LONG}, Reader.FINALIZED_DOUBLE,
                (v, s) -> new NeumaierAggregate(v, s[0], s[1], s[2]));
        average(specs, AvgShortGroupByFunction.class, SHORT, LONG);
        average(specs, AvgIntGroupByFunction.class, INT, DOUBLE);
        average(specs, AvgLongGroupByFunction.class, LONG, DOUBLE);
        average(specs, AvgDoubleGroupByFunction.class, DOUBLE, DOUBLE);

        extrema(specs, MinShortGroupByFunction.class, MaxShortGroupByFunction.class, SHORT, INT);
        extrema(specs, MinIntGroupByFunction.class, MaxIntGroupByFunction.class, INT, INT);
        extrema(specs, MinLongGroupByFunction.class, MaxLongGroupByFunction.class, LONG, LONG);
        extrema(specs, MinDateGroupByFunction.class, MaxDateGroupByFunction.class, DATE, DATE);
        extrema(specs, MinCharGroupByFunction.class, MaxCharGroupByFunction.class, CHAR, CHAR);
        extrema(specs, MinIPv4GroupByFunction.class, MaxIPv4GroupByFunction.class, IPv4, IPv4);
        extrema(specs, MinFloatGroupByFunction.class, MaxFloatGroupByFunction.class, FLOAT, FLOAT);
        extrema(specs, MinDoubleGroupByFunction.class, MaxDoubleGroupByFunction.class, DOUBLE, DOUBLE);
        timestampExtremum(specs, MinTimestampGroupByFunction.class, true);
        timestampExtremum(specs, MaxTimestampGroupByFunction.class, false);

        distinct(specs, CountDistinctLongGroupByFunction.class, LONG, NestedDistinct.LONG);
        distinct(specs, CountDistinctIntGroupByFunction.class, INT, NestedDistinct.INT);
        distinct(specs, CountDistinctSymbolGroupByFunction.class, SYMBOL, NestedDistinct.INT);
        distinct(specs, CountDistinctIPv4GroupByFunction.class, IPv4, NestedDistinct.IPV4);

        bitwise(specs, BitAndByteGroupByFunction.class, BYTE, Op.AND);
        bitwise(specs, BitAndShortGroupByFunction.class, SHORT, Op.AND);
        bitwise(specs, BitAndIntGroupByFunction.class, INT, Op.AND);
        bitwise(specs, BitAndLongGroupByFunction.class, LONG, Op.AND);
        bitwise(specs, BitOrByteGroupByFunction.class, BYTE, Op.OR);
        bitwise(specs, BitOrShortGroupByFunction.class, SHORT, Op.OR);
        bitwise(specs, BitOrIntGroupByFunction.class, INT, Op.OR);
        bitwise(specs, BitOrLongGroupByFunction.class, LONG, Op.OR);
        bitwise(specs, BitXorByteGroupByFunction.class, BYTE, Op.XOR);
        bitwise(specs, BitXorShortGroupByFunction.class, SHORT, Op.XOR);
        bitwise(specs, BitXorIntGroupByFunction.class, INT, Op.XOR);
        bitwise(specs, BitXorLongGroupByFunction.class, LONG, Op.XOR);
        // Finalization changes BOOLEAN state, but its ordinary reader remains valid.
        unary(specs, BoolAndGroupByFunction.class, BOOLEAN, new int[]{BOOLEAN, LONG}, Reader.ORDINARY,
                (v, s) -> new BooleanAggregate(v, s[0], s[1], true));
        unary(specs, BoolOrGroupByFunction.class, BOOLEAN, new int[]{BOOLEAN, LONG}, Reader.ORDINARY,
                (v, s) -> new BooleanAggregate(v, s[0], s[1], false));

        moment(specs, VarSampleGroupByFunctionFactory.VarSampleGroupByFunction.class, true, false);
        moment(specs, VarPopGroupByFunctionFactory.VarPopGroupByFunction.class, false, false);
        moment(specs, StdDevSampleGroupByFunctionFactory.StdDevSampleGroupByFunction.class, true, true);
        moment(specs, StdDevPopGroupByFunctionFactory.StdDevPopGroupByFunction.class, false, true);
        pair(specs, CovarPopGroupByFunction.class, new int[]{DOUBLE, DOUBLE, DOUBLE, LONG},
                (y, x, s) -> new CovarianceAggregate(y, x, s[0], s[1], s[2], s[3], false));
        pair(specs, CovarSampleGroupByFunctionFactory.CovarSampleGroupByFunction.class, new int[]{DOUBLE, DOUBLE, DOUBLE, LONG},
                (y, x, s) -> new CovarianceAggregate(y, x, s[0], s[1], s[2], s[3], true));
        regression(specs, CorrGroupByFunctionFactory.CorrGroupByFunction.class, RegressionAggregate.Kind.CORRELATION);
        regression(specs, RegressionSlopeFunctionFactory.RegressionSlopeFunction.class, RegressionAggregate.Kind.SLOPE);
        regression(specs, RegressionInterceptFunctionFactory.RegressionInterceptFunction.class, RegressionAggregate.Kind.INTERCEPT);
        regression(specs, RegressionR2FunctionFactory.RegressionR2Function.class, RegressionAggregate.Kind.R2);
        pair(specs, WeightedAvgDoubleGroupByFunction.class, new int[]{DOUBLE, DOUBLE},
                (sample, weight, s) -> new WeightedAverageAggregate(sample, weight, s[0], s[1]));

        add(specs, GroupedTopKAggregate.class, new Spec(f -> {
            int[] types = new int[((GroupedTopKAggregate) f).limit + 1];
            Arrays.fill(types, LONG);
            return types;
        }, (f, in, s) -> {
            var topK = (GroupedTopKAggregate) f;
            int type = topK.getArg().getType();
            var value = in.scalar(topK.getArg(), type == FLOAT ? DOUBLE : type);
            return value == null ? null : new GroupedTopKAggregate.Update(value, s[0], topK.limit, topK.ascending);
        }, Reader.ORDINARY, Requirement.NONE, true));
        return Map.copyOf(specs);
    }

    private static void add(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, Spec spec) {
        if (specs.put(type, spec) != null) throw new IllegalArgumentException("Duplicate aggregate specification: " + type);
    }

    private static void add(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, int[] state, Factory factory) {
        add(specs, type, new Spec(f -> state, factory, Reader.ORDINARY, Requirement.NONE, false));
    }

    private static void unary(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, int argumentType,
                              int[] state, Reader reader, UnaryFactory factory) {
        add(specs, type, new Spec(f -> state, (f, in, s) -> {
            var value = in.scalar(((UnaryFunction) f).getArg(), argumentType);
            return value == null ? null : factory.bind(value, s);
        }, reader, Requirement.NONE, false));
    }

    private static void pair(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type,
                             int[] state, PairFactory factory) {
        add(specs, type, new Spec(f -> state, (f, in, s) -> {
            var pair = (BinaryFunction) f;
            var left = in.scalar(pair.getLeft(), DOUBLE);
            var right = in.scalar(pair.getRight(), DOUBLE);
            return left == null || right == null ? null : factory.bind(left, right, s);
        }, Reader.FINALIZED_DOUBLE, Requirement.NONE, false));
    }

    private static void count(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, int argumentType) {
        unary(specs, type, argumentType, new int[]{LONG}, Reader.ORDINARY,
                (v, s) -> new ScalarUpdate(new Pipeline.CountValue(v), s[0]));
    }

    private static void sum(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, int argumentType) {
        unary(specs, type, argumentType, new int[]{LONG}, Reader.ORDINARY,
                (v, s) -> new ScalarUpdate(new Pipeline.Sum(v), s[0]));
    }

    private static void average(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, int argumentType, int stateType) {
        unary(specs, type, argumentType, new int[]{stateType, LONG}, Reader.FINALIZED_DOUBLE,
                (v, s) -> new AverageUpdate(v, s[0], s[1], stateType == DOUBLE ? MemoryType.F64 : MemoryType.I64));
    }

    private static void extrema(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> min,
                                Class<? extends GroupByFunction> max, int argumentType, int stateType) {
        unary(specs, min, argumentType, new int[]{stateType}, Reader.ORDINARY,
                (v, s) -> extremum(v, s[0], stateType, true));
        unary(specs, max, argumentType, new int[]{stateType}, Reader.ORDINARY,
                (v, s) -> extremum(v, s[0], stateType, false));
    }

    private static Update extremum(Pipeline.Scalar value, long offset, int type, boolean minimum) {
        return type == FLOAT || type == DOUBLE ? new FloatingExtremum(value, offset, Pipeline.widthOf(type), minimum)
                : new IntegerExtremum(value, offset, type, minimum);
    }

    private static void timestampExtremum(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, boolean minimum) {
        add(specs, type, new Spec(f -> new int[]{f.getType()}, (f, in, s) -> {
            var value = in.scalar(((UnaryFunction) f).getArg(), f.getType());
            return value == null ? null : new IntegerExtremum(value, s[0], f.getType(), minimum);
        }, Reader.ORDINARY, Requirement.NONE, false));
    }

    private static void bitwise(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, int width, Op operation) {
        unary(specs, type, width, new int[]{width}, Reader.ORDINARY,
                (v, s) -> new BitwiseAggregate(v, s[0], width, operation));
    }

    private static void moment(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, boolean sample, boolean sqrt) {
        unary(specs, type, DOUBLE, new int[]{DOUBLE, DOUBLE, LONG}, Reader.FINALIZED_DOUBLE,
                (v, s) -> new MomentAggregate(v, s[0], s[1], s[2], sample, sqrt));
    }

    private static void regression(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, RegressionAggregate.Kind kind) {
        pair(specs, type, new int[]{DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG},
                (y, x, s) -> new RegressionAggregate(y, x, s[0], s[1], s[2], s[3], s[4], s[5], kind));
    }

    private static void varchar(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, boolean extremum, boolean minimum) {
        add(specs, type, new Spec(f -> new int[]{LONG}, (f, in, s) -> {
            var value = in.strings().bind(((UnaryFunction) f).getArg());
            return value == null ? null : extremum ? new VarcharExtremum(value, s[0], minimum) : new VarcharCount(value, s[0]);
        }, Reader.ORDINARY, extremum ? Requirement.MUTABLE_MAP : Requirement.NONE, false));
    }

    private static void distinct(Map<Class<?>, Spec> specs, Class<? extends GroupByFunction> type, int argumentType, NestedDistinct layout) {
        add(specs, type, new Spec(f -> new int[]{LONG, LONG}, (f, in, s) -> {
            var value = in.scalar(((UnaryFunction) f).getArg(), argumentType);
            if (value == null) return null;
            int capacity;
            double loadFactor;
            if (f instanceof CountDistinctLongGroupByFunction count) {
                capacity = count.getSetInitialCapacity(); loadFactor = count.getSetLoadFactor();
            } else {
                var count = (AbstractCountDistinctIntGroupByFunction) f;
                capacity = count.getSetInitialCapacity(); loadFactor = count.getSetLoadFactor();
            }
            return new DistinctUpdate(value, s[0], layout.withPayloadOffset(s[1] - s[0]), capacity, loadFactor);
        }, Reader.ORDINARY, Requirement.DISTINCT, false));
    }

    private enum Reader { ORDINARY, FINALIZED_DOUBLE }
    private enum Requirement { NONE, DISTINCT, MUTABLE_MAP }
    private record Spec(java.util.function.Function<GroupByFunction, int[]> stateTypes, Factory factory,
                        Reader reader, Requirement requirement, boolean contiguous) { }
    @FunctionalInterface private interface Factory {
        Update bind(GroupByFunction function, ResolvedInputs inputs, long[] state);
    }
    @FunctionalInterface private interface UnaryFactory {
        Update bind(Pipeline.Scalar value, long[] state);
    }
    @FunctionalInterface private interface PairFactory {
        Update bind(Pipeline.Scalar left, Pipeline.Scalar right, long[] state);
    }
}
