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
import io.questdb.cairo.sql.RecordMetadata;

import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Physical scan -> filter -> hash aggregation operators, independent of the backend. */
public record Pipeline(List<Column> columns, Predicate filter, List<Scalar> keys, List<Aggregate> aggregates) {

    public Pipeline {
        columns = List.copyOf(columns);
        keys = List.copyOf(keys);
        aggregates = List.copyOf(aggregates);
        if (columns.isEmpty() || aggregates.isEmpty()) {
            throw new IllegalArgumentException("Expected columns and at least one aggregate");
        }
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).index() != i) {
                throw new IllegalArgumentException("Columns must use consecutive projection indexes");
            }
        }
        if (filter == null) {
            throw new IllegalArgumentException("Filter is required (use All for no filter)");
        }
        if (aggregates.stream().anyMatch(a -> a instanceof CountDistinct) && aggregates.size() != 1) {
            throw new IllegalArgumentException("Flat COUNT DISTINCT cannot be mixed with other aggregates");
        }
        if (aggregates.stream().filter(a -> a instanceof NestedCountDistinct).count() > 1) {
            throw new IllegalArgumentException("Prototype supports one nested COUNT DISTINCT per pipeline");
        }
    }

    public static List<Column> schema(RecordMetadata metadata) {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            columns.add(new Column(i, metadata.getColumnName(i), metadata.getColumnType(i)));
        }
        return List.copyOf(columns);
    }

    public record Value(Expr data, Expr isNull) {
        public Value {
            if (data.type() != Type.I64 || isNull.type() != Type.BOOL) {
                throw new IllegalArgumentException("Expected an integer value and null flag");
            }
        }
    }

    /** A borrowed contiguous UTF-8 view; stability is a lifetime contract, not an encoding. */
    public record Utf8Value(Expr address, Expr size, Expr isNull, Expr isAscii, Expr stable) {
        public Utf8Value {
            if (address.type() != Type.ADDRESS || size.type() != Type.I64
                    || isNull.type() != Type.BOOL || isAscii.type() != Type.BOOL || stable.type() != Type.BOOL) {
                throw new IllegalArgumentException("Invalid UTF-8 view");
            }
        }
    }

    public sealed interface Scalar permits Column, Constant, Add, Subtract, TimestampPart, IPv4Subtract {
        Value emit(Row row);
    }

    public record Column(int index, String name, int columnType) implements Scalar {
        public Column {
            if (index < 0 || name == null) {
                throw new IllegalArgumentException("Invalid column");
            }
            // Fail at plan construction, before reading any native memory.
            widthOf(columnType);
        }

        @Override
        public Value emit(Row row) {
            if (index >= row.columns().size() || !equals(row.columns().get(index))) {
                throw new IllegalArgumentException("Column does not belong to this scan: " + name);
            }
            return row.values().get(index);
        }

        public long nullValue() {
            return switch (columnType) {
                case ColumnType.INT -> Integer.MIN_VALUE;
                case ColumnType.SYMBOL -> io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL;
                case ColumnType.IPv4 -> io.questdb.std.Numbers.IPv4_NULL;
                case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> Long.MIN_VALUE;
                default -> 0; // BYTE and SHORT have no null sentinel in QuestDB.
            };
        }

        public boolean nullable() { return columnType == ColumnType.INT || columnType == ColumnType.LONG || columnType == ColumnType.SYMBOL || columnType == ColumnType.IPv4
                || columnType == ColumnType.DATE || ColumnType.isTimestamp(columnType); }
        public MemoryType width() { return widthOf(columnType); }
    }

    public record Constant(long value) implements Scalar {
        @Override
        public Value emit(Row row) { return new Value(i64(value), bool(value == Long.MIN_VALUE)); }
    }

    /** Example of adding a SQL operation once, without modifying either backend. */
    public record Add(Scalar left, Scalar right, int columnType) implements Scalar {
        public Add(Scalar left, Scalar right) { this(left, right, ColumnType.LONG); }
        public Add { checkArithmeticType(columnType); }
        @Override
        public Value emit(Row row) {
            return integerArithmetic(Op.ADD, left.emit(row), right.emit(row), columnType);
        }
    }

    /** UTC cyclic timestamp fields, preserving the timestamp driver's pre-epoch rules. */
    public record TimestampPart(Scalar argument, long unitTicks, int period) implements Scalar {
        public TimestampPart {
            if (unitTicks <= 0 || period <= 0) throw new IllegalArgumentException("Positive time unit required");
        }
        @Override
        public Value emit(Row row) {
            Value value = argument.emit(row);
            Expr positive = binary(Op.GE, value.data(), i64(0));
            Expr adjusted = new Select(positive, value.data(), add(value.data(), i64(1)));
            Expr units = binary(Op.SDIV, adjusted, i64(unitTicks));
            Expr remainder = binary(Op.SUB, units, binary(Op.MUL, binary(Op.SDIV, units, i64(period)), i64(period)));
            return new Value(new Select(positive, remainder, add(i64(period - 1), remainder)), value.isNull());
        }
    }

    public record Subtract(Scalar left, Scalar right, int columnType) implements Scalar {
        public Subtract { checkArithmeticType(columnType); }
        @Override
        public Value emit(Row row) {
            return integerArithmetic(Op.SUB, left.emit(row), right.emit(row), columnType);
        }
    }

    /** QuestDB IPv4 subtraction treats the integer operand as unsigned and zero as NULL. */
    public record IPv4Subtract(Scalar left, Scalar right) implements Scalar {
        @Override public Value emit(Row row) {
            Value l = left.emit(row), r = right.emit(row);
            Expr lhs = binary(Op.AND, l.data(), i64(0xffff_ffffL));
            Expr rhs = binary(Op.AND, r.data(), i64(0xffff_ffffL));
            Expr absent = binary(Op.EITHER, binary(Op.EITHER, l.isNull(), r.isNull()), binary(Op.GE, rhs, lhs));
            Expr value = binary(Op.SUB, lhs, rhs);
            value = binary(Op.SUB, binary(Op.XOR, binary(Op.AND, value, i64(0xffff_ffffL)), i64(0x8000_0000L)), i64(0x8000_0000L));
            return new Value(value, absent);
        }
    }

    private static void checkArithmeticType(int type) {
        if (type != ColumnType.INT && type != ColumnType.LONG) throw new IllegalArgumentException("Integer arithmetic type required");
    }

    private static Value integerArithmetic(Op op, Value left, Value right, int type) {
        Expr result = binary(op, left.data(), right.data());
        // IntFunction.getLong sign-extends the already wrapped INT result. Preserve that
        // width at every expression node, including when its parent reads LONG.
        if (type == ColumnType.INT) {
            result = binary(Op.SUB, binary(Op.XOR, binary(Op.AND, result, i64(0xffff_ffffL)), i64(0x8000_0000L)), i64(0x8000_0000L));
        }
        return new Value(result, binary(Op.EITHER, binary(Op.EITHER, left.isNull(), right.isNull()),
                eq(result, i64(type == ColumnType.INT ? Integer.MIN_VALUE : Long.MIN_VALUE))));
    }

    public sealed interface Predicate permits All, Compare, SqlCompare, IsNotNull, And, Or, Not {
        Expr emit(Row row);
    }

    public record All() implements Predicate {
        @Override
        public Expr emit(Row row) { return bool(true); }
    }

    /** Explicit null-rejecting comparison; null tests are separate operators. */
    public record Compare(Op op, Scalar left, Scalar right) implements Predicate {
        public Compare {
            if (op != Op.EQ && op != Op.NE && op != Op.LT && op != Op.GE) {
                throw new IllegalArgumentException("Not a comparison: " + op);
            }
        }

        @Override
        public Expr emit(Row row) {
            Value a = left.emit(row);
            Value b = right.emit(row);
            return binary(Op.BOTH,
                    eq(binary(Op.EITHER, a.isNull(), b.isNull()), bool(false)),
                    binary(op, a.data(), b.data()));
        }
    }

    public record IsNotNull(Scalar value) implements Predicate {
        @Override
        public Expr emit(Row row) { return eq(value.emit(row).isNull(), bool(false)); }
    }

    /** QuestDB integer comparisons: null equals null; ordered comparisons reject unequal nulls. */
    public record SqlCompare(Op op, Scalar left, Scalar right) implements Predicate {
        public SqlCompare { new Compare(op, left, right); }
        @Override
        public Expr emit(Row row) {
            Value a = left.emit(row);
            Value b = right.emit(row);
            Expr x = new Select(a.isNull(), i64(Long.MIN_VALUE), a.data());
            Expr y = new Select(b.isNull(), i64(Long.MIN_VALUE), b.data());
            if (op == Op.EQ || op == Op.NE) { return binary(op, x, y); }
            Expr ordered = binary(Op.BOTH, eq(binary(Op.EITHER, a.isNull(), b.isNull()), bool(false)), binary(op, x, y));
            return op == Op.GE ? binary(Op.EITHER, eq(x, y), ordered) : ordered;
        }
    }

    public record And(Predicate left, Predicate right) implements Predicate {
        @Override
        public Expr emit(Row row) { return binary(Op.BOTH, left.emit(row), right.emit(row)); }
    }

    public record Or(Predicate left, Predicate right) implements Predicate {
        @Override
        public Expr emit(Row row) { return binary(Op.EITHER, left.emit(row), right.emit(row)); }
    }

    public record Not(Predicate value) implements Predicate {
        @Override
        public Expr emit(Row row) { return eq(value.emit(row), bool(false)); }
    }

    public sealed interface Aggregate permits RowAggregate, CountDistinct {
        long initialValue();
        default int stateLongs() { return 1; }
    }

    public sealed interface RowAggregate extends Aggregate permits Count, CountValue, Sum, NestedCountDistinct {
        void emit(Builder builder, Row row, Expr address);
        void merge(Builder builder, Expr source, Expr destination);
    }

    public record CountDistinct(Scalar argument) implements Aggregate {
        @Override
        public long initialValue() { return 0; }
    }

    /** QuestDB's singleton-inline, then growable per-group long-set algorithm. */
    public record NestedCountDistinct(Scalar argument, int initialCapacity, double loadFactor) implements RowAggregate {
        public NestedCountDistinct {
            NestedDistinct.initialCapacity(initialCapacity, loadFactor);
        }
        public NestedCountDistinct(Scalar argument) { this(argument, 3, 0.7); }
        @Override
        public int stateLongs() { return 2; }
        @Override
        public long initialValue() { return 0; }
        @Override
        public void emit(Builder builder, Row row, Expr address) {
            NestedDistinct.emit(builder, argument.emit(row), address, initialCapacity, loadFactor);
        }
        @Override
        public void merge(Builder builder, Expr source, Expr destination) {
            NestedDistinct.merge(builder, source, destination, initialCapacity, loadFactor);
        }
    }

    public record Count() implements RowAggregate {
        @Override
        public long initialValue() { return 0; }
        @Override
        public void emit(Builder builder, Row row, Expr address) {
            builder.store(address, add(load(address), i64(1)));
        }
        @Override
        public void merge(Builder builder, Expr source, Expr destination) {
            mergeCount(builder, source, destination);
        }
    }

    public record CountValue(Scalar argument) implements RowAggregate {
        @Override
        public long initialValue() { return 0; }
        @Override
        public void emit(Builder builder, Row row, Expr address) {
            builder.when(eq(argument.emit(row).isNull(), bool(false)),
                    b -> b.store(address, add(load(address), i64(1))));
        }
        @Override
        public void merge(Builder builder, Expr source, Expr destination) {
            mergeCount(builder, source, destination);
        }
    }

    /** Matches QuestDB's scalar long SUM, including its null sentinel. */
    public record Sum(Scalar argument) implements RowAggregate {
        @Override
        public long initialValue() { return Long.MIN_VALUE; }
        @Override
        public void emit(Builder builder, Row row, Expr address) {
            Value value = argument.emit(row);
            builder.when(eq(value.isNull(), bool(false)), b -> {
                Var old = b.let(load(address));
                b.store(address, new Select(eq(old, i64(Long.MIN_VALUE)), value.data(), add(old, value.data())));
            });
        }
        @Override
        public void merge(Builder builder, Expr source, Expr destination) {
            Var value = builder.let(load(source));
            builder.when(ne(value, i64(Long.MIN_VALUE)), present -> {
                Var old = present.let(load(destination));
                present.store(destination, new Select(eq(old, i64(Long.MIN_VALUE)), value, add(old, value)));
            });
        }
    }

    private static void mergeCount(Builder builder, Expr source, Expr destination) {
        Var value = builder.let(load(source));
        builder.when(binary(Op.LT, i64(0), value), present -> {
            Var old = present.let(load(destination));
            present.store(destination, new Select(binary(Op.LT, i64(0), old), add(old, value), value));
        });
    }

    public record Row(List<Column> columns, List<Value> values) { }

    public long entryBytes() { return aggregateOffset(aggregates.size()); }
    public long aggregateOffset(int index) {
        long longs = 1L + keys.size();
        for (int i = 0; i < index; i++) { longs += aggregates.get(i).stateLongs(); }
        return Math.multiplyExact(8L, longs);
    }
    public boolean distinct() { return flatDistinct() || nestedDistinct(); }
    public boolean flatDistinct() { return aggregates.getFirst() instanceof CountDistinct; }
    public boolean nestedDistinct() { return aggregates.stream().anyMatch(a -> a instanceof NestedCountDistinct); }
    public long inputEntryBytes() { return flatDistinct() ? 8L * (2 + keys.size()) : entryBytes(); }

    CompiledKernel compileScan(Backend backend, java.nio.file.Path artifacts, boolean sharded, boolean statistics)
            throws java.io.IOException {
        if (filter instanceof All) return backend.compile(lower(sharded, statistics, ScanLowering.Mode.FUSED), artifacts);
        return backend.compileAlternatives(java.util.Arrays.stream(ScanLowering.Mode.values())
                .map(mode -> lower(sharded, statistics, mode)).toList(), artifacts);
    }

    public Program lower() {
        return lower(false, false);
    }

    Program lowerParallel(boolean sharded) {
        if (flatDistinct()) { throw new UnsupportedPlan("Parallel mode follows Java's nested DISTINCT algorithm"); }
        return lower(sharded, true);
    }

    private Program lower(boolean sharded, boolean statistics) {
        return lower(sharded, statistics, ScanLowering.Mode.MASKED);
    }

    Program lower(boolean sharded, boolean statistics, ScanLowering.Mode mode) {
        Builder builder = new Builder();
        ScanLowering.emit(ScanPlan.of(columns, filter), builder,
                (consumer, row) -> emitGroupBy(consumer, row, STATE, sharded, statistics), mode);
        builder.exit(0);
        return new Program(builder.block());
    }

    void emitGroupBy(Builder builder, Row row, Expr state) {
        emitGroupBy(builder, row, state, false, false);
    }

    void emitGroupBy(Builder builder, Row row, Expr state, boolean sharded, boolean statistics) {
        List<Var> groupKeys = new ArrayList<>();
        for (Scalar key : keys) {
            Value value = key.emit(row);
            Var normalized = builder.let(new Select(value.isNull(), i64(Long.MIN_VALUE), value.data()));
            groupKeys.add(normalized);
        }
        if (flatDistinct()) {
            Value argument = ((CountDistinct) aggregates.getFirst()).argument().emit(row);
            // Retain one null pair per group so groups with no non-null arguments
            // survive the collapse with a zero count.
            groupKeys.add(builder.let(new Select(argument.isNull(), i64(Long.MIN_VALUE), argument.data())));
            GroupTable.findOrInsert(builder, state, groupKeys, List.of());
        } else {
            List<Expr> initial = new ArrayList<>();
            for (Aggregate aggregate : aggregates) {
                initial.add(i64(aggregate.initialValue()));
                for (int i = 1; i < aggregate.stateLongs(); i++) { initial.add(i64(0)); }
            }
            Expr target = sharded ? shardState(builder, state, groupKeys) : state;
            Var entry = GroupTable.findOrInsert(builder, target, groupKeys, initial);
            for (int i = 0; i < aggregates.size(); i++) {
                Expr address = at(entry, aggregateOffset(i));
                boolean distinctStat = statistics && aggregates.get(i) instanceof NestedCountDistinct;
                Var before = distinctStat ? builder.let(load(address)) : null;
                ((RowAggregate) aggregates.get(i)).emit(builder, row, address);
                if (distinctStat) {
                    builder.when(ne(before, load(address)), changed -> {
                        Expr cardinality = at(state, TableState.FUNCTION_CARDINALITY);
                        changed.store(cardinality, add(load(cardinality), i64(1)));
                    });
                }
            }
        }
    }

    /** Map.merge: copy a new group's complete state; invoke aggregate merge for existing keys. */
    public Program lowerMerge() {
        return lowerEntries(false);
    }

    /** GroupByMapFragment.shard: route by the top hash bits and copy complete states. */
    Program lowerShard() {
        return lowerEntries(true);
    }

    private Program lowerEntries(boolean sharding) {
        if (flatDistinct()) { throw new UnsupportedPlan("Parallel mode follows Java's nested DISTINCT algorithm"); }
        Builder builder = new Builder();
        Var index = builder.let(i64(0));
        builder.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var source = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(inputEntryBytes()))));
            loop.when(ne(load(source), i64(0)), occupied -> {
                List<Var> groupKeys = new ArrayList<>();
                for (int k = 0; k < keys.size(); k++) {
                    groupKeys.add(occupied.let(load(at(source, 8L * (k + 1)))));
                }
                List<Expr> initial = new ArrayList<>();
                for (long offset = 8L * (1 + keys.size()); offset < entryBytes(); offset += 8) {
                    initial.add(load(at(source, offset)));
                }
                Expr target = sharding ? shardState(occupied, STATE, groupKeys) : STATE;
                Var inserted = occupied.let(bool(false));
                Var destination = GroupTable.findOrInsert(occupied, target, groupKeys, initial, inserted);
                if (!sharding) {
                    occupied.when(eq(inserted, bool(false)), existing -> {
                        for (int a = 0; a < aggregates.size(); a++) {
                            long offset = aggregateOffset(a);
                            ((RowAggregate) aggregates.get(a)).merge(existing, at(source, offset), at(destination, offset));
                        }
                    });
                }
            });
            loop.assign(index, add(index, i64(1)));
        });
        builder.exit(0);
        return new Program(builder.block());
    }

    private static Var shardState(Builder builder, Expr state, List<Var> keys) {
        Expr index = binary(Op.USHR, GroupTable.hash(builder, keys),
                i64(io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS_SHR));
        Expr shards = new Load(MemoryType.ADDRESS, at(state, TableState.SHARDS));
        return builder.let(new Load(MemoryType.ADDRESS, new Offset(shards, binary(Op.MUL, index, i64(8)))));
    }

    /** Second physical phase: unique pairs -> group counts, also emitted as IR. */
    public Program lowerFinish() {
        if (!flatDistinct()) { throw new IllegalStateException("Only flat DISTINCT needs a collapse phase"); }
        Builder builder = new Builder();
        Var index = builder.let(i64(0));
        builder.loop(binary(Op.LT, index, ROWS), loop -> {
            Var pair = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(inputEntryBytes()))));
            loop.when(ne(load(pair), i64(0)), occupied -> {
                List<Var> groupKeys = new ArrayList<>();
                for (int i = 0; i < keys.size(); i++) {
                    groupKeys.add(occupied.let(load(at(pair, 8L * (i + 1)))));
                }
                Var entry = GroupTable.findOrInsert(occupied, STATE, groupKeys, List.of(i64(0)));
                Expr argument = load(at(pair, 8L * (keys.size() + 1)));
                occupied.when(ne(argument, i64(Long.MIN_VALUE)), counted -> {
                    Expr count = at(entry, aggregateOffset(0));
                    counted.store(count, add(load(count), i64(1)));
                });
            });
            loop.assign(index, add(index, i64(1)));
        });
        builder.exit(0);
        return new Program(builder.block());
    }

    private static MemoryType widthOf(int columnType) {
        return switch (columnType) {
            case ColumnType.BYTE -> MemoryType.I8;
            case ColumnType.SHORT -> MemoryType.I16;
            case ColumnType.INT, ColumnType.SYMBOL, ColumnType.IPv4 -> MemoryType.I32;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> MemoryType.I64;
            default -> throw new IllegalArgumentException("Prototype does not support " + ColumnType.nameOf(columnType));
        };
    }
}
