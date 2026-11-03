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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryCodeGenerator.IntJoinFilterSource;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

import static org.questdb.experimental.codegen.Ir.*;

/** Build-derived redundant selection. Ordinary joins retain payload lookup and multiplicity. */
final class ResolvedJoinFilter {
    private static final int DESCRIPTOR_BYTES = 8 + IntegralMembership.BYTES;

    private ResolvedJoinFilter() { }

    static QueryCodeGenerator.JoinFilter compile(ColumnTypes inputs, ObjList<IntJoinFilterSource> sources,
                                                 Backend backend, Path artifacts) throws IOException {
        for (int i = 0; i < sources.size(); i++) {
            if (ColumnType.tagOf(inputs.getColumnType(sources.getQuick(i).probeColumn())) != ColumnType.INT) return null;
        }
        State state = new State(sources);
        CompiledKernel rows = backend.compile(program(sources.size(), false), artifacts);
        CompiledKernel count = null;
        try {
            count = backend.compile(program(sources.size(), true), artifacts);
            return new QueryCodeGenerator.JoinFilter(new Predicate(state), new QueryCodeGenerator.CompiledFilters(
                    new Rows(rows, state, backend.name()), new Count(count, state)));
        } catch (Throwable failure) {
            try { rows.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            if (count != null) try { count.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static Program program(int filters, boolean countOnly) {
        Builder b = new Builder();
        Expr[] columns = new Expr[filters];
        IntegralMembership.Descriptor[] sets = new IntegralMembership.Descriptor[filters];
        for (int i = 0; i < filters; i++) {
            columns[i] = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, i * DESCRIPTOR_BYTES)));
            sets[i] = IntegralMembership.bind(b, at(COLUMNS, i * DESCRIPTOR_BYTES + 8));
        }
        Var count = b.let(i64(0));
        Var row = b.let(i64(0));
        b.loop(binary(Op.LT, row, ROWS), next -> {
            fused(next, sets, columns, row, count, 0, countOnly);
            next.assign(row, binary(Op.ADD, row, i64(1)));
        });
        b.exit(count);
        return new Program(b.block());
    }

    private static Expr contains(Builder b, IntegralMembership.Descriptor set, Expr column, Expr row) {
        return IntegralMembership.containsJoin(b, set, b.let(new Load(MemoryType.I32, new Offset(column, binary(Op.MUL, row, i64(4))))));
    }

    private static void append(Builder b, Var count, Expr row) {
        b.store(new Offset(STATE, binary(Op.MUL, count, i64(8))), row);
        b.assign(count, binary(Op.ADD, count, i64(1)));
    }

    private static void fused(Builder b, IntegralMembership.Descriptor[] sets, Expr[] columns, Expr row, Var count,
                              int filter, boolean countOnly) {
        if (filter == sets.length) {
            if (countOnly) b.assign(count, binary(Op.ADD, count, i64(1)));
            else append(b, count, row);
        } else {
            b.when(contains(b, sets[filter], columns[filter], row), next -> fused(next, sets, columns, row, count, filter + 1, countOnly));
        }
    }

    private static final class State implements AutoCloseable {
        private final IntJoinFilterSource[] sources;
        private final IntegralMembership[] sets;
        private MemoryTracker tracker;
        private SqlExecutionCircuitBreaker breaker;

        State(ObjList<IntJoinFilterSource> sources) {
            this.sources = new IntJoinFilterSource[sources.size()];
            sets = new IntegralMembership[sources.size()];
            for (int i = 0; i < sets.length; i++) {
                this.sources[i] = sources.getQuick(i);
                sets[i] = new IntegralMembership();
            }
        }

        void prepare() {
            try {
                for (int i = 0; i < sets.length; i++) {
                    sets[i].prepareJoin(sources[i].completedBuild().get(), sources[i].mapKeyColumn(), tracker, breaker);
                }
            } catch (Throwable failure) {
                // A paused query may retry publication without reinitializing the atom.
                for (IntegralMembership set : sets) set.close();
                throw failure;
            }
        }

        long run(CompiledKernel kernel, long data, long rows, long output) {
            long bytes = (long) sets.length * DESCRIPTOR_BYTES;
            long descriptors = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT, tracker);
            try {
                for (int i = 0; i < sets.length; i++) {
                    long descriptor = descriptors + (long) i * DESCRIPTOR_BYTES;
                    Unsafe.putLong(descriptor, Unsafe.getLong(data + (long) sources[i].probeColumn() * 8));
                    sets[i].write(descriptor + 8);
                }
                long count = kernel.run(descriptors, rows, output);
                if (count < 0 || count > rows) throw new IllegalStateException("Invalid join filter count: " + count);
                return count;
            } finally {
                Unsafe.free(descriptors, bytes, MemoryTag.NATIVE_JIT, tracker);
            }
        }

        @Override public void close() {
            for (IntegralMembership set : sets) set.close();
            tracker = null;
            breaker = null;
        }
    }

    private static final class Predicate extends BooleanFunction {
        private final State state;
        Predicate(State state) { this.state = state; }
        @Override public boolean getBool(Record record) {
            for (int i = 0; i < state.sets.length; i++) {
                if (!state.sets[i].contains(record.getInt(state.sources[i].probeColumn()))) return false;
            }
            return true;
        }
        @Override public boolean isThreadSafe() { return true; }
        @Override public void init(SymbolTableSource symbols, SqlExecutionContext context) {
            state.close();
            state.tracker = context.getMemoryTracker();
            state.breaker = context.getCircuitBreaker();
        }
        @Override public void toPlan(PlanSink sink) { sink.val("build membership"); }
    }

    private static final class Rows extends CompiledFilter {
        private final CompiledKernel kernel;
        private final State state;
        private final String backend;
        private final LongAdder inputRows = new LongAdder();
        Rows(CompiledKernel kernel, State state, String backend) { this.kernel = kernel; this.state = state; this.backend = backend; }
        @Override public void prepareForDispatch() { state.prepare(); }
        @Override public void clearExecution() { state.close(); }
        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long output, long rows) {
            long count = state.run(kernel, data, rows, output);
            inputRows.add(rows);
            return count;
        }
        @Override public CharSequence getBackendName() { return backend; }
        @Override public long getNativeInputRows() { return inputRows.sum(); }
        @Override public void close() { try (state; kernel) { } }
    }

    private static final class Count extends CompiledCountOnlyFilter {
        private final CompiledKernel kernel;
        private final State state;
        Count(CompiledKernel kernel, State state) { this.kernel = kernel; this.state = state; }
        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long rows) {
            return state.run(kernel, data, rows, 0);
        }
        @Override public void close() { kernel.close(); }
    }
}
