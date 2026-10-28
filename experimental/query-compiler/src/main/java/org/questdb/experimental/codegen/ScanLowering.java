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

import java.util.ArrayList;
import java.util.AbstractList;
import java.util.function.IntFunction;
import java.util.List;
import java.util.function.BiConsumer;
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Shared physical scan lowering. Storage lifetime and stateful consumers stay outside it. */
final class ScanLowering {
    enum Mode { FUSED, EAGER, MASKED }
    static final int LANES = 64;
    private ScanLowering() { }

    @FunctionalInterface
    private interface RowConsumer {
        void accept(Builder builder, Row row, Expr rowIndex);
    }

    @FunctionalInterface
    private interface MaskConsumer {
        void accept(Builder builder, Expr rowIndex, Expr mask);
    }

    @FunctionalInterface
    interface RowPredicate {
        Expr emit(Builder builder, Expr rowIndex);
    }

    /** Existing ordered selection-vector contract, with bounded vector reads and a scalar tail. */
    static Program selection(ScanPlan plan) {
        return selection(plan, null);
    }

    /** A residual consumes only rows admitted by the numeric predicate, in source order. */
    static Program selection(ScanPlan plan, RowPredicate residual) {
        return selection(plan, residual, true, false, null);
    }

    /** Existing filter ABI: borrowed full column-pointer array and frame-level cancellation. */
    static Program selection(ScanPlan plan, int[] sourceIndexes, boolean countOnly) {
        return selection(plan, null, false, countOnly, sourceIndexes);
    }

    /** Frame descriptors with the same residual semantics for selection and count-only consumers. */
    static Program selection(ScanPlan plan, RowPredicate residual, boolean countOnly) {
        return selection(plan, residual, false, countOnly, null);
    }

    /** ROWS is the exclusive frame-row end; output IDs retain the frame's coordinates. */
    static Program selectionRange(ScanPlan plan, RowPredicate residual, long beginOffset) {
        return selection(plan, residual, true, false, null, new Load(MemoryType.I64, at(COLUMNS, beginOffset)));
    }

    private static Program selection(ScanPlan plan, RowPredicate residual, boolean cancellable,
                                     boolean countOnly, int[] sourceIndexes) {
        return selection(plan, residual, cancellable, countOnly, sourceIndexes, i64(0));
    }

    private static Program selection(ScanPlan plan, RowPredicate residual, boolean cancellable,
                                     boolean countOnly, int[] sourceIndexes, Expr begin) {
        // Row IDs are 64-bit: keep each predicate mask at the output-vector width.
        final int selectionLanes = countOnly ? 64 : 8;
        Builder b = new Builder();
        Var count = b.let(i64(0));
        if (cancellable) b.poll();
        emit(plan, b, (selected, row, index) -> {
            java.util.function.Consumer<Builder> accept = accepted -> {
                if (countOnly) accepted.assign(count, add(count, i64(1)));
                else selectRow(accepted, count, index);
            };
            if (residual == null) accept.accept(selected);
            else selected.when(residual.emit(selected, index), accept);
        }, Mode.MASKED, cancellable, selectionLanes, residual != null ? null : (selected, index, mask) -> selected.assign(count, add(count,
                countOnly ? new PopulationCount(mask)
                        : new SelectRows(selectionLanes, mask, index, new Offset(STATE, binary(Op.MUL, count, i64(8)))))), sourceIndexes, begin);
        b.exit(count);
        return new Program(b.block());
    }

    private static void selectRow(Builder b, Var count, Expr index) {
        b.store(new Offset(STATE, binary(Op.MUL, count, i64(8))), index);
        b.assign(count, add(count, i64(1)));
    }

    static void emit(ScanPlan plan, Builder b, BiConsumer<Builder, Row> consume) {
        emit(plan, b, consume, Mode.MASKED);
    }
    static void emit(ScanPlan plan, Builder b, BiConsumer<Builder, Row> consume, Mode mode) {
        emit(plan, b, (builder, row, index) -> consume.accept(builder, row), mode, false, LANES, null);
    }
    private static void emit(ScanPlan plan, Builder b, RowConsumer consume, Mode mode, boolean cancellable, int lanes, MaskConsumer consumeMask) {
        emit(plan, b, consume, mode, cancellable, lanes, consumeMask, null);
    }

    private static void emit(ScanPlan plan, Builder b, RowConsumer consume, Mode mode, boolean cancellable,
                             int lanes, MaskConsumer consumeMask, int[] sourceIndexes) {
        emit(plan, b, consume, mode, cancellable, lanes, consumeMask, sourceIndexes, i64(0));
    }

    private static void emit(ScanPlan plan, Builder b, RowConsumer consume, Mode mode, boolean cancellable,
                             int lanes, MaskConsumer consumeMask, int[] sourceIndexes, Expr begin) {
        List<Var> addresses = new ArrayList<>();
        for (Column column : plan.input()) {
            int source = sourceIndexes == null ? column.index() : sourceIndexes[column.index()];
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, source * 8L))));
        }
        Var index = b.let(begin);
        if (!(plan.predicate() instanceof All) && mode == Mode.MASKED) {
            Expr present = bool(true);
            var columns = plan.predicateColumns();
            for (int i = columns.nextSetBit(0); i >= 0; i = columns.nextSetBit(i + 1))
                present = binary(Op.BOTH, present, ne(addresses.get(i), nil()));
            b.when(present, fast -> fast.loop(binary(Op.GE, binary(Op.SUB, ROWS, index), i64(lanes)), batch -> {
                if (cancellable) batch.when(eq(binary(Op.AND, index, i64(4095)), i64(0)), Builder::poll);
                Var mask = batch.let(i64(lanes == 64 ? -1L : (1L << lanes) - 1));
                predicateMask(batch, mask, plan.predicate(), row(plan.input(), addresses, index, false), lanes);
                if (consumeMask != null) {
                    batch.when(ne(mask, i64(0)), selected -> consumeMask.accept(selected, index, mask));
                } else {
                    batch.loop(ne(mask, i64(0)), selected -> {
                        Var rowIndex = selected.let(add(index, new TrailingZeros(mask)));
                        consume(plan, selected, row(plan.input(), addresses, rowIndex, true), rowIndex, consume);
                        selected.assign(mask, binary(Op.AND, mask, binary(Op.SUB, mask, i64(1))));
                    });
                }
                batch.assign(index, add(index, i64(lanes)));
            }));
        }
        // Also handles column tops and zero rows. Never speculatively read a tail.
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            if (cancellable) loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Row raw = row(plan.input(), addresses, index, true);
            if (mode == Mode.EAGER && !(plan.predicate() instanceof All)) {
                // Discover demands while generating the body, then bind inputs in
                // source-column order before the predicate, as the original scan did.
                Builder[] loads = new Builder[plan.input().size()];
                Row source = lazy(plan.input(), i -> {
                    loads[i] = loop.fork();
                    return materializeColumn(loads[i], plan.input().get(i), raw.values().get(i));
                });
                Builder body = loop.fork();
                if (plan.predicate() instanceof All) consume(plan, body, source, index, consume);
                else body.when(plan.predicate().emit(source), accepted -> consume(plan, accepted, source, index, consume));
                for (Builder load : loads) if (load != null) loop.append(load.block());
                loop.append(body.block());
                loop.assign(index, add(index, i64(1)));
                return;
            }
            List<Value> available = new ArrayList<>(raw.values());
            var predicateColumns = plan.predicateColumns();
            for (int i = predicateColumns.nextSetBit(0); i >= 0; i = predicateColumns.nextSetBit(i + 1))
                available.set(i, materializeColumn(loop, plan.input().get(i), available.get(i)));
            Row source = new Row(plan.input(), available);
            if (plan.predicate() instanceof All) consume(plan, loop, source, index, consume);
            else loop.when(plan.predicate().emit(source), accepted -> consume(plan, accepted, source, index, consume));
            loop.assign(index, add(index, i64(1)));
        });
    }
    /** A bounded input batch; scalar tails never speculate beyond the pinned frame. */
    static Var mask(ScanPlan plan, Builder b, List<Var> addresses, Expr start, Expr end, int lanes) {
        Var mask = b.let(i64(0));
        Expr full = eq(binary(Op.SUB, end, start), i64(lanes));
        b.when(full, vector -> {
            vector.assign(mask, i64(lanes == 64 ? -1L : (1L << lanes) - 1));
            predicateMask(vector, mask, plan.predicate(), row(plan.input(), addresses, start, false), lanes);
        });
        b.when(eq(full, bool(false)), tail -> {
            Var index = tail.let(start);
            tail.loop(binary(Op.LT, index, end), scalar -> {
                scalar.when(plan.predicate().emit(row(plan.input(), addresses, index, false)), selected ->
                        selected.assign(mask, add(mask, binary(Op.USHR, i64(Long.MIN_VALUE), binary(Op.SUB, i64(63), binary(Op.SUB, index, start))))));
                scalar.assign(index, add(index, i64(1)));
            });
        });
        return mask;
    }

    private static void predicateMask(Builder b, Var mask, Predicate predicate, Row row, int lanes) {
        if (predicate instanceof And and) {
            predicateMask(b, mask, and.left(), row, lanes);
            // An empty batch needs neither the remaining predicate columns nor row IDs.
            // Separate lane regions also avoid a chain of dependent masked memory loads.
            b.when(ne(mask, i64(0)), right -> predicateMask(right, mask, and.right(), row, lanes));
        } else {
            b.assign(mask, binary(Op.AND, mask, new VectorMask(lanes, predicate.emit(row))));
        }
    }

    /**
     * Bind each requested input once at the stage entrance, after eligibility is
     * established and before stateful work. A compiler-time demand pass builds
     * the prologue while emitting the consumer into a separate region. This
     * avoids both eager rejected-row reads and reloading inputs across state writes.
     */
    private static void consume(ScanPlan plan, Builder parent, Row source, Expr index, RowConsumer consumer) {
        Builder prologue = parent.fork(), body = parent.fork();
        Row inputs = lazy(plan.input(), i -> materializeColumn(prologue, plan.input().get(i), source.values().get(i)));
        Row projected = lazy(plan.output(), i -> {
            Value value = plan.bindings().get(i).emit(inputs);
            if (value.data() instanceof Var) return value;
            return new Value(prologue.let(value.data()), prologue.let(value.isNull()));
        });
        consumer.accept(body, projected, index);
        parent.append(prologue.block()); parent.append(body.block());
    }
    private static Value materializeColumn(Builder b, Column column, Value value) {
        if (value.data() instanceof Var) return value;
        Var data = b.let(value.data());
        return new Value(data, column.nullable() ? eq(data, i64(column.nullValue())) : bool(false));
    }
    private static Row lazy(List<Column> columns, IntFunction<Value> producer) {
        Value[] cache = new Value[columns.size()];
        return new Row(columns, new AbstractList<>() {
            @Override public int size() { return cache.length; }
            @Override public Value get(int index) {
                if (cache[index] == null) cache[index] = producer.apply(index);
                return cache[index];
            }
        });
    }

    private static Row row(List<Column> columns, List<Var> addresses, Expr index, boolean guardTops) {
        List<Value> values = new ArrayList<>();
        for (Column column : columns) {
            Var base = addresses.get(column.index());
            Expr address = new Offset(base, binary(Op.MUL, index, i64(column.width().bytes)));
            Expr data = new Load(column.width(), address);
            if (guardTops) data = new Select(eq(base, nil()), i64(column.nullValue()), data);
            // Keep the read as an expression. Unused columns generate no loads,
            // and consumer columns appear only inside the selected-row region.
            values.add(new Value(data, column.nullable() ? eq(data, i64(column.nullValue())) : bool(false)));
        }
        return new Row(columns, values);
    }
}
