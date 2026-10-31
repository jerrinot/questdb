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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;

import static org.questdb.experimental.codegen.Ir.*;

/** Small ordered prefixes in ordinary group-by state: count followed by signed sort keys. */
final class GroupedTopKAggregate extends LongFunction implements GroupByFunction, UnaryFunction {
    private final Function argument;
    final int limit;
    final boolean ascending;
    private int valueIndex;

    GroupedTopKAggregate(Function argument, int limit, boolean ascending) {
        this.argument = argument;
        this.limit = limit;
        this.ascending = ascending;
    }

    @Override public Function getArg() { return argument; }
    @Override public long getLong(Record record) { return record.getLong(valueIndex); }
    @Override public String getName() { return "top_k_values"; }
    @Override public int getValueIndex() { return valueIndex; }
    @Override public void initValueIndex(int index) { valueIndex = index; }
    @Override public boolean isConstant() { return false; }
    @Override public boolean isThreadSafe() { return argument.isThreadSafe(); }
    @Override public boolean supportsParallelism() { return argument.supportsParallelism(); }
    @Override public void setNull(MapValue value) { value.putLong(valueIndex, 0); }

    @Override public void initValueTypes(ArrayColumnTypes types) {
        valueIndex = types.getColumnCount();
        for (int i = 0; i <= limit; i++) types.add(ColumnType.LONG);
    }

    @Override public void computeFirst(MapValue value, Record record, long rowId) {
        value.putLong(valueIndex, 0);
        computeNext(value, record, rowId);
    }

    @Override public void computeNext(MapValue value, Record record, long rowId) {
        long key = switch (argument.getType()) {
            case ColumnType.INT -> {
                int number = argument.getInt(record);
                yield number == Numbers.INT_NULL ? Numbers.LONG_NULL : number;
            }
            case ColumnType.LONG -> argument.getLong(record);
            case ColumnType.FLOAT -> encode(argument.getFloat(record));
            case ColumnType.DOUBLE -> encode(argument.getDouble(record));
            default -> throw new IllegalStateException("Unsupported Top-K value");
        };
        insert(value, key);
    }

    @Override public void merge(MapValue destination, MapValue source) {
        int count = (int) source.getLong(valueIndex);
        for (int i = 0; i < count; i++) insert(destination, source.getLong(valueIndex + 1 + i));
    }

    @Override public void toPlan(PlanSink sink) {
        sink.val("top_k_values(").val(argument).val(',').val(limit).val(ascending ? ",asc)" : ",desc)");
    }

    private void insert(MapValue value, long key) {
        int count = (int) value.getLong(valueIndex);
        if (count == limit && !before(key, value.getLong(valueIndex + count))) return;
        int position = Math.min(count, limit - 1);
        while (position > 0) {
            long previous = value.getLong(valueIndex + position);
            if (!before(key, previous)) break;
            value.putLong(valueIndex + position + 1, previous);
            position--;
        }
        value.putLong(valueIndex + position + 1, key);
        value.putLong(valueIndex, Math.min(count + 1, limit));
    }

    private boolean before(long left, long right) { return ascending ? left < right : right < left; }

    static long encode(double value) {
        long bits = Double.doubleToLongBits(value);
        return bits < 0 ? bits ^ Long.MAX_VALUE : bits;
    }

    static double decode(long key) { return Double.longBitsToDouble(key < 0 ? key ^ Long.MAX_VALUE : key); }

    record Update(Pipeline.Scalar argument, long offset, int limit, boolean ascending) implements AggregateUpdates.Update {
        @Override public void initialize(Builder b, Expr destination) { b.store(at(destination, offset), i64(0)); }

        @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
            var value = argument.emit(frame.numeric(b));
            Expr key = value.data().type() == Type.F64 ? QuestSortValue.encodeDouble(b, value.data())
                    : new Select(value.isNull(), i64(Long.MIN_VALUE), value.data());
            insert(b, destination, b.let(key));
        }

        @Override public void merge(Builder b, Expr source, Expr destination) {
            Var count = b.let(load(at(source, offset))), position = b.let(i64(0));
            b.loop(binary(Op.LT, position, count), loop -> {
                insert(loop, destination, loop.let(load(slot(source, position))));
                loop.assign(position, add(position, i64(1)));
            });
        }

        private Expr slot(Expr state, Expr index) { return new Offset(at(state, offset + 8), binary(Op.MUL, index, i64(8))); }
        private Expr before(Expr left, Expr right) { return ascending ? binary(Op.LT, left, right) : binary(Op.LT, right, left); }

        private void insert(Builder b, Expr destination, Var key) {
            Var count = b.let(load(at(destination, offset))), accept = b.let(bool(true));
            b.when(eq(count, i64(limit)), full -> full.assign(accept, before(key, load(slot(destination, i64(limit - 1))))));
            b.when(accept, insert -> {
                Var position = insert.let(new Select(binary(Op.LT, count, i64(limit)), count, i64(limit - 1)));
                Var moving = insert.let(bool(true));
                insert.loop(binary(Op.BOTH, binary(Op.LT, i64(0), position), moving), shift -> {
                    Var previous = shift.let(load(slot(destination, binary(Op.SUB, position, i64(1)))));
                    shift.assign(moving, before(key, previous));
                    shift.when(moving, move -> {
                        move.store(slot(destination, position), previous);
                        move.assign(position, binary(Op.SUB, position, i64(1)));
                    });
                });
                insert.store(slot(destination, position), key);
                insert.store(at(destination, offset), new Select(binary(Op.LT, count, i64(limit)), add(count, i64(1)), count));
            });
        }
    }
}
