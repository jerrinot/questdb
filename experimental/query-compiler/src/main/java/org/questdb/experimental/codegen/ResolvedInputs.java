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
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.bool.AndFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InLongFunctionFactory;
import io.questdb.griffin.engine.functions.bool.OrFunctionFactory;
import io.questdb.griffin.engine.functions.bool.NotFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqSymLongFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqIntStrCFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqLongFunctionFactory;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.lt.LtIntFunctionFactory;
import io.questdb.griffin.engine.functions.lt.LtLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.AddLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.MulIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.MulLongFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubIntFunctionFactory;
import io.questdb.griffin.engine.functions.math.SubLongFunctionFactory;
import io.questdb.griffin.engine.functions.cast.*;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.date.HourOfDayFunctionFactory;
import io.questdb.griffin.engine.functions.date.MinuteOfHourFunctionFactory;
import io.questdb.griffin.engine.functions.date.SecondOfMinuteFunctionFactory;

import java.util.ArrayList;
import java.util.List;

/** Compiler-time bindings shared by resolved predicates and aggregate arguments. */
final class ResolvedInputs {
    private final ColumnTypes types;
    private final int columnOffset;
    private final ArrayList<Pipeline.Column> columns = new ArrayList<>();
    private final ArrayList<Integer> indexes = new ArrayList<>();

    ResolvedInputs(ColumnTypes types) {
        this(types, 0);
    }

    ResolvedInputs(ColumnTypes types, int columnOffset) {
        this.types = types;
        this.columnOffset = columnOffset;
    }

    ColumnTypes types() { return types; }

    static Pipeline.Row loadFrameRow(Ir.Builder b, List<Pipeline.Column> columns, Ir.Expr pointers, Ir.Expr row) {
        var values = new ArrayList<Pipeline.Value>();
        for (int i = 0; i < columns.size(); i++) {
            var column = columns.get(i);
            Ir.Expr base = new Ir.Load(Ir.MemoryType.ADDRESS, Ir.at(pointers, i * 8L));
            var value = b.let(new Ir.Load(column.width(), new Ir.Offset(base, Ir.binary(Ir.Op.MUL, row, Ir.i64(column.width().bytes)))));
            values.add(new Pipeline.Value(value, column.nullable() ? Ir.eq(value, Ir.i64(column.nullValue())) : Ir.bool(false)));
        }
        return new Pipeline.Row(columns, values);
    }

    Pipeline.Column column(Function function, int readType) {
        int index;
        int type;
        if (function.getClass() == LongColumn.class) {
            index = ((LongColumn) function).getColumnIndex();
            type = ColumnType.LONG;
        } else if (function.getClass() == IntColumn.class) {
            index = ((IntColumn) function).getColumnIndex();
            type = ColumnType.INT;
        } else if (function.getClass() == ShortColumn.class) {
            index = ((ShortColumn) function).getColumnIndex();
            type = ColumnType.SHORT;
        } else if (function.getClass() == ByteColumn.class) {
            index = ((ByteColumn) function).getColumnIndex();
            type = ColumnType.BYTE;
        } else if (function.getClass() == io.questdb.griffin.engine.functions.columns.SymbolColumn.class
                && ((io.questdb.griffin.engine.functions.columns.SymbolColumn) function).isSymbolTableStatic()) {
            index = ((io.questdb.griffin.engine.functions.columns.SymbolColumn) function).getColumnIndex();
            type = ColumnType.SYMBOL;
        } else {
            return null;
        }
        // These exact functions implement widening getters, including INT null conversion.
        if (ColumnType.sizeOf(type) > ColumnType.sizeOf(readType)) return null;
        return column(index, type);
    }

    Pipeline.Column column(int index, int type) {
        if (index < columnOffset) return null; // A reference to an earlier virtual expression.
        if (index - columnOffset >= types.getColumnCount() || types.getColumnType(index - columnOffset) != type) {
            throw new IllegalArgumentException("Resolved argument does not match input metadata");
        }
        int compact = indexes.indexOf(index);
        if (compact < 0) {
            compact = columns.size();
            indexes.add(index);
            columns.add(new Pipeline.Column(compact, "arg" + compact, type));
        }
        return columns.get(compact);
    }

    List<Pipeline.Column> columns() { return List.copyOf(columns); }
    int[] indexes() { return indexes.stream().mapToInt(Integer::intValue).toArray(); }

    static Function[] andArguments(Function function) {
        if (function == null) return null;
        Class<?> implementation = function.getClass();
        if (implementation == AndFunctionFactory.AndBooleanFunction.class) {
            var and = (BinaryFunction) function;
            return new Function[]{and.getLeft(), and.getRight()};
        }
        if (implementation == AndFunctionFactory.TernaryAndBooleanFunction.class) {
            var and = (AndFunctionFactory.TernaryAndBooleanFunction) function;
            return new Function[]{and.getLeft(), and.getCenter(), and.getRight()};
        }
        if (implementation == AndFunctionFactory.QuaternaryAndBooleanFunction.class) {
            var and = (AndFunctionFactory.QuaternaryAndBooleanFunction) function;
            return new Function[]{and.getFunc0(), and.getFunc1(), and.getFunc2(), and.getFunc3()};
        }
        return null;
    }

    Pipeline.Predicate predicate(Function function) {
        return predicate(function, 0);
    }

    // Only total, side-effect-free leaves are admitted, so SIMD evaluation of both
    // Boolean operands preserves the resolved functions' short-circuit semantics.
    private Pipeline.Predicate predicate(Function function, int depth) {
        if (depth > 64) return null;
        if (function == null) return new Pipeline.All();
        Class<?> implementation = function.getClass();
        if (implementation == BooleanConstant.class) {
            return function.getBool(null) ? new Pipeline.All() : new Pipeline.Not(new Pipeline.All());
        }
        var conjunction = andArguments(function);
        if (conjunction != null) return conjunction(depth, conjunction);
        if (implementation == OrFunctionFactory.MyBooleanFunction.class) {
            var binary = (BinaryFunction) function;
            var left = predicate(binary.getLeft(), depth + 1);
            var right = predicate(binary.getRight(), depth + 1);
            if (left == null || right == null) return null;
            return new Pipeline.Or(left, right);
        }
        if (implementation == NotFunctionFactory.Func.class) {
            var value = predicate(((NotFunctionFactory.Func) function).getArg(), depth + 1);
            return value == null ? null : new Pipeline.Not(value);
        }
        int type;
        boolean equality;
        if (implementation == io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory.NullCheckFunc.class) {
            var check = (io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory.NullCheckFunc) function;
            var value = column(check.getArg(), ColumnType.SYMBOL);
            if (value == null) return null;
            var present = new Pipeline.IsNotNull(value);
            return check.isNegated() ? present : new Pipeline.Not(present);
        }
        if (implementation == InLongFunctionFactory.InLongSingleConstFunction.class
                || implementation == InLongFunctionFactory.InLongTwoConstFunction.class
                || implementation == InLongFunctionFactory.InLongConstFunction.class) {
            var value = scalar(((UnaryFunction) function).getArg(), ColumnType.LONG);
            if (value == null) return null;
            var constants = new io.questdb.std.LongList();
            if (function instanceof InLongFunctionFactory.InLongSingleConstFunction single) constants.add(single.getValue());
            else if (function instanceof InLongFunctionFactory.InLongTwoConstFunction pair) {
                constants.add(pair.getValue0()); constants.add(pair.getValue1());
            } else {
                var set = (InLongFunctionFactory.InLongConstFunction) function;
                // Small constant sets lower to SIMD equality masks. Larger sets keep the
                // existing hash lookup until a read-only constant-table IR is available.
                if (set.getValueCount() > 16) return null;
                set.copyValues(constants);
            }
            Pipeline.Predicate member = null;
            for (int i = 0; i < constants.size(); i++) {
                var equal = new Pipeline.SqlCompare(Ir.Op.EQ, value, new Pipeline.Constant(constants.getQuick(i)));
                member = member == null ? equal : new Pipeline.Or(member, equal);
            }
            if (member == null) member = new Pipeline.Not(new Pipeline.All());
            return ((NegatableBooleanFunction) function).isNegated() ? new Pipeline.Not(member) : member;
        }
        if (implementation == EqIntStrCFunctionFactory.Func.class) {
            var comparison = (EqIntStrCFunctionFactory.Func) function;
            var value = scalar(comparison.getArg(), ColumnType.INT);
            if (value == null) return null;
            long constant = comparison.getConstant();
            if (constant == Integer.MIN_VALUE) constant = Long.MIN_VALUE;
            return new Pipeline.SqlCompare(comparison.isNegated() ? Ir.Op.NE : Ir.Op.EQ, value, new Pipeline.Constant(constant));
        }
        if (implementation == EqDoubleFunctionFactory.FuncLongIsNaN.class
                || implementation == EqDoubleFunctionFactory.FuncIntIsNaN.class
                || implementation == EqSymLongFunctionFactory.EqLongNullFunction.class) {
            type = implementation == EqDoubleFunctionFactory.FuncIntIsNaN.class ? ColumnType.INT : ColumnType.LONG;
            var value = scalar(((UnaryFunction) function).getArg(), type);
            if (value == null) return null;
            return new Pipeline.SqlCompare(((NegatableBooleanFunction) function).isNegated() ? Ir.Op.NE : Ir.Op.EQ,
                    value, new Pipeline.Constant(Long.MIN_VALUE));
        }
        if (implementation == LtLongFunctionFactory.LtLongFunction.class) { type = ColumnType.LONG; equality = false; }
        else if (implementation == LtIntFunctionFactory.LtIntFunction.class) { type = ColumnType.INT; equality = false; }
        else if (implementation == EqLongFunctionFactory.Func.class) { type = ColumnType.LONG; equality = true; }
        else if (implementation == EqIntFunctionFactory.Func.class) { type = ColumnType.INT; equality = true; }
        else return null;
        var binary = (BinaryFunction) function;
        var left = scalar(binary.getLeft(), type);
        var right = scalar(binary.getRight(), type);
        if (left == null || right == null) return null;
        boolean negated = ((NegatableBooleanFunction) function).isNegated();
        // Numbers.lessThan treats two nulls as equal for the negated (>=) form.
        return new Pipeline.SqlCompare(equality ? (negated ? Ir.Op.NE : Ir.Op.EQ) : (negated ? Ir.Op.GE : Ir.Op.LT), left, right);
    }

    private Pipeline.Predicate conjunction(int depth, Function... functions) {
        Pipeline.Predicate result = null;
        for (Function function : functions) {
            var next = predicate(function, depth + 1);
            if (next == null) return null;
            result = result == null ? next : new Pipeline.And(result, next);
        }
        return result;
    }

    Pipeline.Scalar scalar(Function function, int readType) {
        return scalar(function, readType, 0);
    }

    private Pipeline.Scalar scalar(Function function, int readType, int depth) {
        if (depth > 64) return null;
        Class<?> implementation = function.getClass();
        if (implementation == NullConstant.class) {
            // BYTE/SHORT have no NULL sentinel; NullConstant's narrow getters return zero.
            return new Pipeline.Constant(readType == ColumnType.BYTE || readType == ColumnType.SHORT ? 0 : Long.MIN_VALUE);
        }
        int type = function.getType();
        if (type == ColumnType.IPv4 && readType == ColumnType.IPv4) {
            if (implementation == io.questdb.griffin.engine.functions.columns.IPv4Column.class)
                return column(((io.questdb.griffin.engine.functions.columns.IPv4Column) function).getColumnIndex(), type);
            if (implementation == IPv4Constant.class) {
                int value = function.getIPv4(null);
                return new Pipeline.Constant(value == 0 ? Long.MIN_VALUE : value);
            }
            if (implementation == io.questdb.griffin.engine.functions.math.IPv4MinusIntFunctionFactory.Func.class) {
                var binary = (BinaryFunction) function;
                var left = scalar(binary.getLeft(), ColumnType.IPv4, depth + 1);
                var right = scalar(binary.getRight(), ColumnType.INT, depth + 1);
                return left == null || right == null ? null : new Pipeline.IPv4Subtract(left, right);
            }
            return null;
        }
        if ((type != ColumnType.BYTE && type != ColumnType.SHORT && type != ColumnType.INT && type != ColumnType.LONG)
                || ColumnType.sizeOf(type) > ColumnType.sizeOf(readType)) return null;
        if (implementation == IntConstant.class || implementation == LongConstant.class
                || implementation == ShortConstant.class || implementation == ByteConstant.class) {
            long value = readType == ColumnType.LONG ? function.getLong(null) : function.getInt(null);
            if (readType == ColumnType.INT && value == Integer.MIN_VALUE) value = Long.MIN_VALUE;
            return new Pipeline.Constant(value);
        }
        if (Boolean.parseBoolean(System.getProperty("query.codegen.expressions", "true"))) {
            if (implementation == HourOfDayFunctionFactory.HourOfDayFunction.class
                    || implementation == MinuteOfHourFunctionFactory.MinuteFunction.class
                    || implementation == SecondOfMinuteFunctionFactory.SecondOfMinuteFunc.class) {
                var argument = ((UnaryFunction) function).getArg();
                if (argument.getClass() != TimestampColumn.class || !ColumnType.isTimestamp(argument.getType())) return null;
                long second = argument.getType() == ColumnType.TIMESTAMP_NANO ? 1_000_000_000L : 1_000_000L;
                long unit = implementation == HourOfDayFunctionFactory.HourOfDayFunction.class ? second * 3600
                        : implementation == MinuteOfHourFunctionFactory.MinuteFunction.class ? second * 60 : second;
                int period = implementation == HourOfDayFunctionFactory.HourOfDayFunction.class ? 24 : 60;
                var input = column(((TimestampColumn) argument).getColumnIndex(), argument.getType());
                return input == null ? null : new Pipeline.TimestampPart(input, unit, period);
            }
            if (implementation == AddIntFunctionFactory.AddIntFunc.class || implementation == AddLongFunctionFactory.AddLongFunc.class
                    || implementation == SubIntFunctionFactory.SubIntFunc.class || implementation == SubLongFunctionFactory.SubLongFunc.class
                    || implementation == MulIntFunctionFactory.Func.class || implementation == MulLongFunctionFactory.Func.class) {
                var binary = (BinaryFunction) function;
                var left = scalar(binary.getLeft(), type, depth + 1);
                var right = scalar(binary.getRight(), type, depth + 1);
                if (left == null || right == null) return null;
                if (implementation == MulIntFunctionFactory.Func.class || implementation == MulLongFunctionFactory.Func.class) {
                    return new Pipeline.Multiply(left, right, type);
                }
                return implementation == AddIntFunctionFactory.AddIntFunc.class || implementation == AddLongFunctionFactory.AddLongFunc.class
                        ? new Pipeline.Add(left, right, type) : new Pipeline.Subtract(left, right, type);
            }
            if (implementation == CastByteToShortFunctionFactory.Func.class || implementation == CastByteToIntFunctionFactory.Func.class
                    || implementation == CastByteToLongFunctionFactory.Func.class || implementation == CastShortToIntFunctionFactory.Func.class
                    || implementation == CastShortToLongFunctionFactory.Func.class || implementation == CastIntToLongFunctionFactory.Func.class) {
                // All admitted casts widen; values already carry the source's signed width and NULL flag.
                var argument = ((UnaryFunction) function).getArg();
                int sourceType = implementation == CastByteToShortFunctionFactory.Func.class || implementation == CastByteToIntFunctionFactory.Func.class
                        || implementation == CastByteToLongFunctionFactory.Func.class ? ColumnType.BYTE
                        : implementation == CastShortToIntFunctionFactory.Func.class || implementation == CastShortToLongFunctionFactory.Func.class
                        ? ColumnType.SHORT : ColumnType.INT;
                return scalar(argument, sourceType, depth + 1);
            }
        }
        return column(function, readType);
    }
}
