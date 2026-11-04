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
import io.questdb.griffin.engine.join.CompiledAsOfLookup;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;

import java.nio.file.Path;
import java.util.ArrayList;
import static org.questdb.experimental.codegen.Ir.*;

/** Same descending scalar search, within one borrowed frame. */
final class ResolvedAsOfLookup implements CompiledAsOfLookup {
    private static final int KEY_DATA = 0, TS_DATA = 8, KEY = 16, MIN = 24, LO = 32, FILTER = 40;
    private final ResolvedFilter.Binding filter;
    private final CompiledKernel code;
    private final int keyColumn, timestampColumn;
    private final boolean finiteTolerance;
    private final DirectLongList buffer = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
    private final KernelMemory memory = KernelMemory.reusable((Runnable) null);
    private final Result result = new Result();
    private final String name;
    private long nativeRows;

    static CompiledAsOfLookup compile(ColumnTypes inputs, Function function, int key, int timestamp,
                                        long scale, boolean finite, Backend backend, Path artifacts) {
        if (key < 0 || key >= inputs.getColumnCount() || timestamp < 0 || timestamp >= inputs.getColumnCount()
                || !ColumnType.isTimestamp(inputs.getColumnType(timestamp)) || scale < 1) return null;
        int type = inputs.getColumnType(key);
        if (!function.isStableWithinExecution()
                || type != ColumnType.INT && type != ColumnType.LONG && type != ColumnType.SYMBOL) return null;
        var filter = ResolvedFilter.bind(inputs, function);
        if (filter == null) return null;
        try {
            return new ResolvedAsOfLookup(filter, key, timestamp, type, scale, finite, backend, artifacts);
        } catch (Throwable failure) {
            try { filter.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private ResolvedAsOfLookup(ResolvedFilter.Binding filter, int key, int timestamp, int type,
                                         long scale, boolean finite, Backend backend, Path artifacts) {
        this.filter = filter;
        keyColumn = key;
        timestampColumn = timestamp;
        finiteTolerance = finite;
        name = backend.name();
        code = CompiledKernel.defer(backend, () -> emit(type, scale), artifacts);
    }

    private Program emit(int type, long scale) {
        Builder b = new Builder();
        Var row = b.let(binary(Op.SUB, ROWS, i64(1)));
        Var lo = b.let(load(at(COLUMNS, LO))), key = b.let(load(at(COLUMNS, KEY)));
        Var keyData = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, KEY_DATA)));
        Var tsData = finiteTolerance ? b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, TS_DATA))) : null;
        Expr descriptors = at(COLUMNS, FILTER);
        var addresses = new ArrayList<Expr>();
        for (int i = 0; i < filter.plan().input().size(); i++)
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(descriptors, i * 8L))));
        var parameters = new ArrayList<Expr>();
        for (int i = 0; i < filter.parameters().size(); i++)
            parameters.add(b.let(new Load(MemoryType.I32, at(descriptors, (filter.sourceIndexes().length + i) * 8L))));
        b.loop(binary(Op.GE, row, lo), loop -> {
            if (finiteTolerance) {
                Expr ts = loop.let(load(new Offset(tsData, binary(Op.MUL, row, i64(8)))));
                if (scale != 1) {
                    // Same saturation as AbstractAsOfJoinFastRecordCursor.scaleTimestamp
                    // and the existing ResolvedAsOfJoin lowering, including LONG_NULL.
                    ts = new Select(binary(Op.LT, i64(Long.MAX_VALUE / scale), ts), i64(Long.MAX_VALUE),
                            new Select(binary(Op.LT, ts, i64(Long.MIN_VALUE / scale)), i64(Long.MIN_VALUE),
                                    binary(Op.MUL, ts, i64(scale))));
                }
                loop.when(binary(Op.LT, ts, load(at(COLUMNS, MIN))), expired -> {
                    expired.store(STATE, row);
                    expired.exit(2);
                });
            }
            var input = ScanLowering.row(filter.plan().input(), addresses, row, false, parameters);
            loop.when(filter.plan().predicate().emit(input), selected -> {
                java.util.function.Consumer<Builder> compare = test -> {
                    MemoryType width = type == ColumnType.LONG ? MemoryType.I64 : MemoryType.I32;
                    Expr actual = new Load(width, new Offset(keyData, binary(Op.MUL, row, i64(width.bytes))));
                    test.when(eq(actual, key), found -> { found.store(STATE, row); found.exit(1); });
                };
                if (filter.residual() == null) compare.accept(selected);
                else selected.when(filter.residual().emit(selected, descriptors, row), compare);
            });
            loop.assign(row, binary(Op.SUB, row, i64(1)));
            loop.when(eq(binary(Op.AND, row, i64(1023)), i64(0)), Builder::poll);
        });
        b.store(STATE, row);
        b.exit(0);
        return new Program(b.block());
    }

    @Override public Result scan(PageFrameMemoryRecord record, long lo, long hi, long minTimestamp, long key,
                              SqlExecutionCircuitBreaker breaker) {
        if (record instanceof PageFrameFilteredMemoryRecord) return null;
        long keys = record.getPageAddress(keyColumn);
        long timestamps = finiteTolerance ? record.getPageAddress(timestampColumn) : 0;
        if (keys == 0 || finiteTolerance && timestamps == 0) return null;
        buffer.clearAndEnsureCapacity((8 + FILTER + filter.bytes() + 7) / 8);
        long state = buffer.getAddress(), args = state + 8;
        if (!filter.prepare(record, args + FILTER)) return null;
        Unsafe.putLong(args + KEY_DATA, keys);
        Unsafe.putLong(args + TS_DATA, timestamps);
        Unsafe.putLong(args + KEY, key);
        Unsafe.putLong(args + MIN, minTimestamp);
        Unsafe.putLong(args + LO, lo);
        memory.enter(breaker);
        try {
            long status = code.run(args, hi, state);
            memory.throwIfFailed();
            if (status < 0 || status > 2) throw new IllegalStateException("Invalid ASOF lookup status: " + status);
            long row = Unsafe.getLong(state);
            long examined = status == 1 ? hi - row : hi - row - 1;
            if (row < lo - 1 || row >= hi || examined < 0 || examined > hi - lo)
                throw new IllegalStateException("Invalid ASOF lookup progress");
            result.row = row;
            result.examined = examined;
            result.matched = status == 1;
            nativeRows += examined;
            return result;
        } finally { memory.close(); }
    }
    @Override public CharSequence getName() { return name; }
    @Override public long getNativeRows() { return nativeRows; }
    @Override public void prepareExecution(MemoryTracker tracker) {
        clearExecution();
        buffer.setMemoryTracker(tracker);
        filter.prepareExecution(tracker);
        nativeRows = 0;
    }
    @Override public void clearExecution() {
        try { filter.close(); } finally { buffer.close(); buffer.setMemoryTracker(null); }
    }
    @Override public void close() { try { clearExecution(); } finally { code.close(); } }
}
