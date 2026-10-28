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
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.bool.NotFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.regex.AbstractLikeVarcharFunctionFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;

import static org.questdb.experimental.codegen.Ir.*;

/** Resolved predicates shared by standalone filtering and aggregate selection. */
final class ResolvedFilter implements AutoCloseable {
    private final CompiledKernel code;
    private final ScanPlan plan;
    private final int[] sourceIndexes;
    private final int[] varcharColumns;
    private boolean closed;

    private ResolvedFilter(CompiledKernel code, int[] sourceIndexes, int[] varcharColumns, ScanPlan plan) {
        this.plan = plan;
        this.varcharColumns = varcharColumns;
        this.code = code;
        this.sourceIndexes = sourceIndexes;
    }

    static ResolvedFilter compile(ColumnTypes types, Function function, Backend backend, Path artifacts) throws IOException {
        Binding binding = bind(types, function);
        if (binding == null) return null;
        var program = ScanLowering.selectionRange(binding.plan(), binding.residual(), binding.bytes());
        var code = backend.compile(program, artifacts);
        try { return new ResolvedFilter(code, binding.sourceIndexes(), binding.varcharColumns(), binding.plan()); }
        catch (Throwable failure) { code.close(); throw failure; }
    }

    static Binding bind(ColumnTypes types, Function function) {
        var inputs = new ResolvedInputs(types);
        var leaves = new java.util.ArrayList<Function>();
        if (!conjuncts(function, leaves, 0)) return null;
        var strings = new java.util.ArrayList<Integer>();
        var tests = new java.util.ArrayList<StringPredicate>();
        Pipeline.Predicate numeric = new Pipeline.All();
        for (Function leaf : leaves) {
            Function original = leaf;
            int negations = 0;
            while (leaf != null && leaf.getClass() == NotFunctionFactory.Func.class) {
                if (++negations > 64) return null;
                leaf = ((NotFunctionFactory.Func) leaf).getArg();
            }
            final boolean negate = (negations & 1) != 0;
            if (leaf != null && leaf.getClass() == EqVarcharFunctionFactory.NullCheckFunc.class) {
                var check = (EqVarcharFunctionFactory.NullCheckFunc) leaf;
                int column = varcharColumn(types, check.getArg());
                if (column < 0) return null;
                strings.add(column);
                boolean wantNull = !check.isNegated() ^ negate;
                tests.add((builder, descriptor, row) -> eq(ne(binary(Op.AND,
                        QuestVarchar.header(builder, descriptor, row), i64(4)), i64(0)), bool(wantNull)));
            } else if (leaf != null && leaf.getClass() == AbstractLikeVarcharFunctionFactory.ConstContainsSwarVarcharFunction.class) {
                var contains = (AbstractLikeVarcharFunctionFactory.ConstContainsSwarVarcharFunction) leaf;
                int column = varcharColumn(types, contains.getArg());
                if (column < 0) return null;
                strings.add(column);
                var literal = contains.getPattern();
                byte[] pattern = new byte[literal.size()];
                for (int i = 0; i < pattern.length; i++) pattern[i] = literal.byteAt(i);
                tests.add((builder, descriptor, row) -> {
                    var value = QuestVarchar.decode(builder, descriptor, row);
                    Var found = builder.let(bool(false));
                    builder.when(eq(value.isNull(), bool(false)), present ->
                            present.assign(found, ByteSequences.contains(present, value.address(), value.size(), pattern)));
                    // NOT negates the resolved Boolean, including false for a NULL input.
                    return negate ? eq(found, bool(false)) : found;
                });
            } else {
                var predicate = inputs.predicate(original);
                if (predicate == null) return null;
                numeric = numeric instanceof Pipeline.All ? predicate : new Pipeline.And(numeric, predicate);
            }
        }
        // Admitted leaves are total and side-effect free. Reject numeric rows before decoding strings.
        long stringOffset = inputs.columns().size() * 8L;
        ScanLowering.RowPredicate residual = strings.isEmpty() ? null : (builder, row) -> {
            Var accepted = builder.let(bool(true));
            for (int i = 0; i < strings.size(); i++) {
                long offset = stringOffset + i * 48L;
                StringPredicate test = tests.get(i);
                builder.when(accepted, check -> check.assign(accepted, test.emit(check, at(COLUMNS, offset), row)));
            }
            return accepted;
        };
        return new Binding(ScanPlan.of(inputs.columns(), numeric), inputs.indexes(),
                strings.stream().mapToInt(Integer::intValue).toArray(), residual);
    }

    private static int varcharColumn(ColumnTypes types, Function function) {
        if (function.getClass() != VarcharColumn.class) return -1;
        int column = ((VarcharColumn) function).getColumnIndex();
        return column >= 0 && column < types.getColumnCount() && types.getColumnType(column) == ColumnType.VARCHAR ? column : -1;
    }

    @FunctionalInterface
    private interface StringPredicate {
        Expr emit(Builder builder, Expr descriptor, Expr row);
    }

    record Binding(ScanPlan plan, int[] sourceIndexes, int[] varcharColumns, ScanLowering.RowPredicate residual) {
        long bytes() { return sourceIndexes.length * 8L + varcharColumns.length * 48L; }

        void prepare(PageFrameMemory frame, long data, long aux, long rows, long addresses) {
            for (int i = 0; i < sourceIndexes.length; i++)
                Unsafe.putLong(addresses + i * 8L, Unsafe.getLong(data + sourceIndexes[i] * 8L));
            for (int i = 0; i < varcharColumns.length; i++) {
                int column = varcharColumns[i];
                long descriptor = addresses + sourceIndexes.length * 8L + i * 48L;
                // Latest-by can supply a shifted aux vector for a bounded row slice.
                Unsafe.putLong(descriptor, Unsafe.getLong(aux + column * 8L));
                Unsafe.putLong(descriptor + 8, Unsafe.getLong(data + column * 8L));
                Unsafe.putLong(descriptor + 16, rows * 16L);
                Unsafe.putLong(descriptor + 24, frame.getPageSize(column));
                Unsafe.putLong(descriptor + 32, frame.getFrameFormat() == PartitionFormat.PARQUET ? 1 : 0);
                Unsafe.putLong(descriptor + 40, 0); // predicates never retain input pointers
            }
        }
    }

    private static boolean conjuncts(Function function, java.util.List<Function> leaves, int depth) {
        if (depth > 64) return false;
        var arguments = ResolvedInputs.andArguments(function);
        if (arguments == null) leaves.add(function);
        else for (Function argument : arguments) if (!conjuncts(argument, leaves, depth + 1)) return false;
        return true;
    }

    ScanPlan fusedPlan() { return varcharColumns.length == 0 ? plan : null; }

    long compilationNanos() { return code.compilationNanos(); }

    Slot newSlot() {
        if (closed) throw new IllegalStateException("Filter code is closed");
        return new Slot();
    }

    @Override
    public void close() {
        if (!closed) { closed = true; code.close(); }
    }

    final class Slot implements AutoCloseable {
        private final KernelMemory memory = KernelMemory.reusable((Runnable) null);
        private final long bytes = sourceIndexes.length * 8L + varcharColumns.length * 48L + 8;
        private long addresses = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT);
        private long inputRows;
        private long selectedRows;

        /** Borrowed pointer array; zero declines before the caller mutates aggregate state. */
        long prepare(PageFrameMemoryRecord record) {
            if (closed || addresses == 0) throw new IllegalStateException("Filter slot is closed");
            if (record instanceof PageFrameFilteredMemoryRecord) return 0;
            for (int i = 0; i < sourceIndexes.length; i++) {
                long address = record.getPageAddress(sourceIndexes[i]);
                // Zero may denote a lazy cast or a column top: ordinary getters retain that contract.
                if (address == 0) return 0;
                Unsafe.putLong(addresses + 8L * i, address);
            }
            for (int i = 0; i < varcharColumns.length; i++) {
                if (!record.writeNativeVarcharView(varcharColumns[i], addresses + sourceIndexes.length * 8L + i * 48L)) return 0;
            }
            Unsafe.putLong(addresses + bytes - 8, 0);
            return addresses;
        }

        boolean filter(PageFrameMemoryRecord record, long rowCount, DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
            if (prepare(record) == 0) return false;
            filterRange(0, rowCount, rows, breaker);
            return true;
        }

        /** Reuse prepared frame pointers and the caller's bounded selection scratch. */
        long filterRange(long begin, long end, DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
            rows.clearAndEnsureCapacity(end - begin);
            Unsafe.putLong(addresses + bytes - 8, begin);
            memory.enter(breaker);
            try {
                long count = code.run(addresses, end, rows.getAddress());
                memory.throwIfFailed();
                if (count < 0 || count > end - begin) throw new IllegalStateException("Invalid native filter count: " + count);
                rows.setPos(count);
                inputRows += end - begin;
                selectedRows += count;
                return count;
            } finally { memory.close(); }
        }

        long[] counters() { return new long[]{inputRows, selectedRows}; }

        @Override
        public void close() {
            if (addresses != 0) addresses = Unsafe.free(addresses, bytes, MemoryTag.NATIVE_JIT);
        }
    }
}
