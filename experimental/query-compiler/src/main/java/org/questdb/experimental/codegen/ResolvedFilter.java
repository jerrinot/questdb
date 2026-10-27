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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.eq.EqVarcharFunctionFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;

import static org.questdb.experimental.codegen.Ir.*;

/** Resolved integer predicates and direct VARCHAR NULL checks, using the engine's borrowed selection-vector storage. */
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
        var inputs = new ResolvedInputs(types);
        var leaves = new java.util.ArrayList<Function>();
        if (!conjuncts(function, leaves, 0)) return null;
        var strings = new java.util.ArrayList<Integer>();
        var nullChecks = new java.util.ArrayList<Boolean>();
        Pipeline.Predicate numeric = new Pipeline.All();
        for (Function leaf : leaves) {
            if (leaf != null && leaf.getClass() == EqVarcharFunctionFactory.NullCheckFunc.class) {
                var check = (EqVarcharFunctionFactory.NullCheckFunc) leaf;
                if (check.getArg().getClass() != VarcharColumn.class) return null;
                int column = ((VarcharColumn) check.getArg()).getColumnIndex();
                if (column < 0 || column >= types.getColumnCount() || types.getColumnType(column) != ColumnType.VARCHAR) return null;
                strings.add(column);
                nullChecks.add(!check.isNegated());
            } else {
                var predicate = inputs.predicate(leaf);
                if (predicate == null) return null;
                numeric = numeric instanceof Pipeline.All ? predicate : new Pipeline.And(numeric, predicate);
            }
        }
        // All admitted leaves are total and side-effect free. Numeric rejection may precede
        // string checks regardless of their SQL spelling; descriptors remain frame-owned.
        long stringOffset = inputs.columns().size() * 8L;
        ScanLowering.RowPredicate residual = strings.isEmpty() ? null : (builder, row) -> {
            Var accepted = builder.let(bool(true));
            for (int i = 0; i < strings.size(); i++) {
                long offset = stringOffset + i * 48L;
                boolean wantNull = nullChecks.get(i);
                builder.when(accepted, check -> {
                    Expr isNull = ne(binary(Op.AND, QuestVarchar.header(check, at(COLUMNS, offset), row), i64(4)), i64(0));
                    check.assign(accepted, eq(isNull, bool(wantNull)));
                });
            }
            return accepted;
        };
        var plan = ScanPlan.of(inputs.columns(), numeric);
        var program = ScanLowering.selection(plan, residual);
        var code = backend.compile(program, artifacts);
        try { return new ResolvedFilter(code, inputs.indexes(), strings.stream().mapToInt(Integer::intValue).toArray(), plan); }
        catch (Throwable failure) { code.close(); throw failure; }
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
        private final long bytes = Math.max(8, sourceIndexes.length * 8L + varcharColumns.length * 48L);
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
            return addresses;
        }

        boolean filter(PageFrameMemoryRecord record, long rowCount, DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
            if (prepare(record) == 0) return false;
            rows.clearAndEnsureCapacity(rowCount);
            memory.enter(breaker);
            try {
                long count = code.run(addresses, rowCount, rows.getAddress());
                memory.throwIfFailed();
                if (count < 0 || count > rowCount) throw new IllegalStateException("Invalid native filter count: " + count);
                rows.setPos(count);
                inputRows += rowCount;
                selectedRows += count;
                return true;
            } finally { memory.close(); }
        }

        long[] counters() { return new long[]{inputRows, selectedRows}; }

        @Override
        public void close() {
            if (addresses != 0) addresses = Unsafe.free(addresses, bytes, MemoryTag.NATIVE_JIT);
        }
    }
}
