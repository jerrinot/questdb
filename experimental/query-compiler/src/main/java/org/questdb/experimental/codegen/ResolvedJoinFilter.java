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
import io.questdb.griffin.engine.QueryCodeGenerator.JoinFilterSource;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

import static org.questdb.experimental.codegen.Ir.*;

/** Build-derived redundant selection. Ordinary joins retain payload lookup and multiplicity. */
final class ResolvedJoinFilter {

    private ResolvedJoinFilter() { }

    static QueryCodeGenerator.JoinFilter compile(ColumnTypes inputs, ObjList<JoinFilterSource> sources,
                                                 Backend backend, Path artifacts) throws IOException {
        for (int i = 0; i < sources.size(); i++) {
            var source = sources.getQuick(i);
            if (source.probeColumns().length == 0 || source.probeColumns().length != source.keyTypes().length) return null;
            for (int k = 0; k < source.keyTypes().length; k++) {
                int type = source.keyTypes()[k];
                int inputType = inputs.getColumnType(source.probeColumns()[k]);
                if (type != ColumnType.INT && type != ColumnType.LONG
                        || inputType != type && !(type == ColumnType.INT && inputType == ColumnType.SYMBOL)) return null;
            }
        }
        State state = new State(sources, backend, artifacts);
        CompiledKernel rows = backend.compile(program(sources, false), artifacts);
        CompiledKernel count = null;
        try {
            count = backend.compile(program(sources, true), artifacts);
            return new QueryCodeGenerator.JoinFilter(new Predicate(state), new QueryCodeGenerator.CompiledFilters(
                    new Rows(rows, state, backend.name()), new Count(count, state)));
        } catch (Throwable failure) {
            try { rows.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            if (count != null) try { count.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static Program program(ObjList<JoinFilterSource> sources, boolean countOnly) {
        Builder b = new Builder();
        Expr[][] columns = new Expr[sources.size()][];
        IntegralMembership.Descriptor[] sets = new IntegralMembership.Descriptor[sources.size()];
        long offset = 0;
        for (int i = 0; i < sources.size(); i++) {
            columns[i] = new Expr[sources.getQuick(i).probeColumns().length];
            sets[i] = IntegralMembership.bind(b, at(COLUMNS, offset));
            offset += IntegralMembership.BYTES;
            for (int k = 0; k < columns[i].length; k++, offset += 8) {
                columns[i][k] = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, offset)));
            }
        }
        Var count = b.let(i64(0));
        Var row = b.let(i64(0));
        // Tuple hashing supplies enough arithmetic to amortize indexed SIMD reads.
        // Cheap single-key bitmap probes regress with AVX2 gathers; retain their scalar loop.
        boolean tuples = true;
        for (Expr[] column : columns) tuples &= column.length > 1;
        if (tuples) {
            int lanes = 8; // Same selection width as ScanLowering; LLVM legalizes it for the target.
            b.loop(binary(Op.GE, binary(Op.SUB, ROWS, row), i64(lanes)), batch -> {
                Var mask = batch.let(i64((1L << lanes) - 1));
                for (int i = 0; i < sets.length; i++) {
                    final int filter = i;
                    batch.when(ne(mask, i64(0)), selected -> {
                        Expr key = key(columns[filter], sources.getQuick(filter).keyTypes(), row);
                        Var found = IntegralMembership.maskJoin(selected, sets[filter], key, lanes);
                        selected.assign(mask, binary(Op.AND, mask, found));
                    });
                }
                batch.when(ne(mask, i64(0)), selected -> selected.assign(count, add(count, countOnly
                        ? new PopulationCount(mask)
                        : new SelectRows(lanes, mask, row, new Offset(STATE, binary(Op.MUL, count, i64(8)))))));
                batch.assign(row, add(row, i64(lanes)));
            });
        }
        b.loop(binary(Op.LT, row, ROWS), next -> {
            fused(next, sources, sets, columns, row, count, 0, countOnly);
            next.assign(row, binary(Op.ADD, row, i64(1)));
        });
        b.exit(count);
        return new Program(b.block());
    }

    private static Expr contains(Builder b, IntegralMembership.Descriptor set, Expr[] columns, int[] types, Expr row) {
        return IntegralMembership.containsJoin(b, set, b.let(key(columns, types, row)), true);
    }

    private static Expr key(Expr[] columns, int[] types, Expr row) {
        Expr key = i64(0);
        for (int k = 0; k < columns.length; k++) {
            MemoryType width = types[k] == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
            Expr value = new Load(width, new Offset(columns[k], binary(Op.MUL, row, i64(width.bytes))));
            key = columns.length == 1 ? value : QuestMapProbe.hashLong128(key, value);
        }
        return key;
    }

    // Fixed tuple order and signed widening match the generated loads, including NULL sentinels.
    private static long key(Record record, JoinFilterSource source, boolean mapRecord) {
        long hash = 0;
        for (int k = 0; k < source.keyTypes().length; k++) {
            int column = mapRecord ? source.mapKeyColumn() + k : source.probeColumns()[k];
            long value = source.keyTypes()[k] == ColumnType.INT ? record.getInt(column) : record.getLong(column);
            hash = source.keyTypes().length == 1 ? value : Hash.hashLong128_64(hash, value);
        }
        return hash;
    }

    private static void append(Builder b, Var count, Expr row) {
        b.store(new Offset(STATE, binary(Op.MUL, count, i64(8))), row);
        b.assign(count, binary(Op.ADD, count, i64(1)));
    }

    private static void fused(Builder b, ObjList<JoinFilterSource> sources, IntegralMembership.Descriptor[] sets, Expr[][] columns, Expr row, Var count,
                              int filter, boolean countOnly) {
        if (filter == sets.length) {
            if (countOnly) b.assign(count, binary(Op.ADD, count, i64(1)));
            else append(b, count, row);
        } else {
            b.when(contains(b, sets[filter], columns[filter], sources.getQuick(filter).keyTypes(), row), next -> fused(next, sources, sets, columns, row, count, filter + 1, countOnly));
        }
    }

    private static final class State implements AutoCloseable {
        private final JoinFilterSource[] sources;
        private final IntegralMembership[] sets;
        private final java.util.List<JoinMembershipPreparation> preparations = new java.util.ArrayList<>();
        private final Backend backend;
        private final Path artifacts;
        private MemoryTracker tracker;
        private SqlExecutionCircuitBreaker breaker;

        State(ObjList<JoinFilterSource> sources, Backend backend, Path artifacts) {
            this.backend = backend;
            this.artifacts = artifacts;
            this.sources = new JoinFilterSource[sources.size()];
            sets = new IntegralMembership[sources.size()];
            for (int i = 0; i < sets.length; i++) {
                this.sources[i] = sources.getQuick(i);
                sets[i] = new IntegralMembership();
            }
        }

        void prepare() {
            try {
                for (int i = 0; i < sets.length; i++) {
                    var source = sources[i];
                    int keyBytes = source.keyTypes().length == 1 && source.keyTypes()[0] == ColumnType.INT
                            ? Integer.BYTES : Long.BYTES;
                    var map = source.completedBuild().get();
                    JoinMembershipPreparation preparation = null;
                    if (map != null && map.size() > 0
                            && IntegralMembership.canPrepareJoin(map.size(), tracker, JoinMembershipPreparation.SCRATCH_BYTES)) {
                        for (var existing : preparations) {
                            if (existing.matches(source.keyTypes(), map)) { preparation = existing; break; }
                        }
                        if (preparation == null) {
                            preparation = JoinMembershipPreparation.compile(source.keyTypes(), map, backend, artifacts);
                            if (preparation != null) preparations.add(preparation);
                        }
                    }
                    if (preparation != null) preparation.prepare(sets[i], map, keyBytes, tracker, breaker);
                    else sets[i].prepareJoin(map, record -> key(record, source, true), keyBytes, tracker, breaker);
                }
            } catch (Throwable failure) {
                // A paused query may retry publication without reinitializing the atom.
                for (IntegralMembership set : sets) set.close();
                throw failure;
            }
        }

        long run(CompiledKernel kernel, long data, long rows, long output) {
            long bytes = 0;
            for (var source : sources) bytes += IntegralMembership.BYTES + (long) source.probeColumns().length * 8;
            long descriptors = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT, tracker);
            try {
                long descriptor = descriptors;
                for (int i = 0; i < sets.length; i++) {
                    sets[i].write(descriptor);
                    descriptor += IntegralMembership.BYTES;
                    for (int column : sources[i].probeColumns()) {
                        Unsafe.putLong(descriptor, Unsafe.getLong(data + (long) column * 8));
                        descriptor += 8;
                    }
                }
                long count = kernel.run(descriptors, rows, output);
                if (count < 0 || count > rows) throw new IllegalStateException("Invalid join filter count: " + count);
                return count;
            } finally {
                Unsafe.free(descriptors, bytes, MemoryTag.NATIVE_JIT, tracker);
            }
        }

        void clear() {
            for (IntegralMembership set : sets) set.close();
            tracker = null;
            breaker = null;
        }

        @Override public void close() {
            Throwable failure = null;
            try { clear(); } catch (Throwable cleanup) { failure = cleanup; }
            for (var preparation : preparations) failure = io.questdb.std.Misc.freeBestEffort(failure, preparation);
            preparations.clear();
            io.questdb.cairo.CairoException.rethrowCleanupFailure(failure);
        }
    }

    private static final class Predicate extends BooleanFunction {
        private final State state;
        Predicate(State state) { this.state = state; }
        @Override public boolean getBool(Record record) {
            for (int i = 0; i < state.sets.length; i++) {
                if (!state.sets[i].contains(key(record, state.sources[i], false))) return false;
            }
            return true;
        }
        @Override public boolean isThreadSafe() { return true; }
        @Override public void init(SymbolTableSource symbols, SqlExecutionContext context) {
            state.clear();
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
        private final LongAdder outputRows = new LongAdder();
        Rows(CompiledKernel kernel, State state, String backend) { this.kernel = kernel; this.state = state; this.backend = backend; }
        @Override public void prepareForDispatch() { state.prepare(); }
        @Override public void clearExecution() { state.clear(); }
        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long output, long rows) {
            long count = state.run(kernel, data, rows, output);
            inputRows.add(rows);
            outputRows.add(count);
            return count;
        }
        @Override public CharSequence getBackendName() { return backend; }
        @Override public long getNativeInputRows() { return inputRows.sum(); }
        @Override public long getNativeOutputRows() { return outputRows.sum(); }
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
