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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.join.CompiledAsOfScan;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.MapKernelState.*;

/** The existing forward ASOF operation over a borrowed frame and its ordinary latest-row map. */
final class ResolvedAsOfScan implements CompiledAsOfScan {
    private static final int TIMESTAMPS = 0;
    private static final int MIN_TIMESTAMP = 8;
    private static final int MAX_TIMESTAMP = 16;
    private static final int ROW_ID_BASE = 24;
    private static final int MISSING_KEY = 32;
    private static final int FILTER = 40;
    private final CompiledKernel code;
    private final ResolvedFilter.Binding filter;
    private final QuestMapProbe probe;
    private final int timestampColumn;
    private final String name;
    private final long stateBytes;
    private final long bytes;
    private final DirectLongList buffer = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
    private final KernelMemory memory = KernelMemory.reusable((Runnable) null);
    private long nativeRows;

    private ResolvedAsOfScan(CompiledKernel code, ResolvedFilter.Binding filter, QuestMapProbe probe,
                            int timestampColumn, String name) {
        this.code = code;
        this.filter = filter;
        this.probe = probe;
        this.timestampColumn = timestampColumn;
        this.name = name;
        stateBytes = CONTROL_BYTES + QuestMapProbe.VIEW_BYTES + ((probe.keyBytes() + 7L) & ~7L);
        bytes = stateBytes + FILTER + filter.bytes() + probe.fixedKey().descriptorBytes();
    }

    static CompiledAsOfScan compile(ColumnTypes inputs, ColumnTypes keyTypes, ListColumnFilter columns,
                                    Map layout, Function function, int timestampColumn, long timestampScale,
                                    Backend backend, Path artifacts) throws IOException {
        if (!function.isStableWithinExecution() || timestampColumn < 0
                || timestampColumn >= inputs.getColumnCount() || !ColumnType.isTimestamp(inputs.getColumnType(timestampColumn))
                || timestampScale < 1 || keyTypes.getColumnCount() != columns.size()) return null;
        // RecordSink encoding must agree with the raw columns, including translated SYMBOL IDs.
        // In particular a timestamp key scaled to nanos is not its raw microsecond representation.
        var rawTypes = new ArrayColumnTypes();
        for (int i = 0; i < columns.size(); i++) {
            int column = columns.getColumnIndexFactored(i);
            if (column < 0 || column >= inputs.getColumnCount()) return null;
            int raw = inputs.getColumnType(column), encoded = keyTypes.getColumnType(i);
            if (raw != encoded && !(raw == ColumnType.SYMBOL && encoded == ColumnType.INT)) return null;
            rawTypes.add(raw);
        }
        var key = QuestFixedKey.bind(new ResolvedInputs(inputs, backend.options()), rawTypes, columns, null, layout);
        if (key == null) return null;
        var probe = QuestMapProbe.fixed(key, rawTypes.getColumnCount() == 1 ? rawTypes.getColumnType(0) : -1,
                layout instanceof OrderedMap);
        var filter = ResolvedFilter.bind(inputs, function, backend.options());
        if (filter == null) return null;
        CompiledKernel code = null;
        try {
            code = backend.compile(emit(filter, probe, timestampScale), artifacts);
            return new ResolvedAsOfScan(code, filter, probe, timestampColumn, backend.name());
        } catch (Throwable failure) {
            if (code != null) try { code.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            try { filter.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static Program emit(ResolvedFilter.Binding filter, QuestMapProbe probe, long scale) {
        Builder b = new Builder();
        Var row = b.let(load(at(STATE, START_ROW)));
        Var timestamps = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, TIMESTAMPS)));
        Var min = b.let(load(at(COLUMNS, MIN_TIMESTAMP))), max = b.let(load(at(COLUMNS, MAX_TIMESTAMP)));
        Var prefix = b.let(load(at(COLUMNS, ROW_ID_BASE)));
        Expr descriptors = at(COLUMNS, FILTER);
        var addresses = new ArrayList<Expr>();
        for (int i = 0; i < filter.plan().input().size(); i++)
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(descriptors, i * 8L))));
        var parameters = new ArrayList<Expr>();
        for (int i = 0; i < filter.parameters().size(); i++)
            parameters.add(b.let(new Load(MemoryType.I32, at(descriptors, (filter.sourceIndexes().length + i) * 8L))));
        b.loop(binary(Op.LT, row, ROWS), loop -> {
            Var timestamp = loop.let(load(new Offset(timestamps, binary(Op.MUL, row, i64(8)))));
            if (scale != 1) {
                loop.assign(timestamp, new Select(binary(Op.LT, i64(Long.MAX_VALUE / scale), timestamp), i64(Long.MAX_VALUE),
                        new Select(binary(Op.LT, timestamp, i64(Long.MIN_VALUE / scale)), i64(Long.MIN_VALUE),
                                binary(Op.MUL, timestamp, i64(scale)))));
            }
            loop.when(binary(Op.LT, max, timestamp), stop -> {
                stop.store(at(STATE, START_ROW), row);
                stop.exit(0);
            });
            loop.when(binary(Op.GE, timestamp, min), eligible -> {
                var source = ScanLowering.row(filter.plan().input(), addresses, row, false, parameters);
                eligible.when(filter.plan().predicate().emit(source), selected -> {
                    java.util.function.Consumer<Builder> update = accepted -> {
                        Var key = probe.loadFrameKey(accepted, row, at(STATE, CONTROL_BYTES + QuestMapProbe.VIEW_BYTES));
                        Var destination = probe.find(accepted, key, row, i64(0), missing ->
                                missing.store(probe.stagedKeyType(), at(COLUMNS, MISSING_KEY), key));
                        accepted.store(destination, add(prefix, row));
                    };
                    if (filter.residual() == null) update.accept(selected);
                    else selected.when(filter.residual().emit(selected, descriptors, row), update);
                });
            });
            loop.assign(row, add(row, i64(1)));
            loop.when(eq(binary(Op.AND, row, i64(1023)), i64(0)), Builder::poll);
        });
        b.store(at(STATE, START_ROW), row);
        b.exit(0);
        return new Program(b.block());
    }

    @Override public void prepareExecution(MemoryTracker tracker) {
        clearExecution();
        buffer.setMemoryTracker(tracker);
        // Allocate the small descriptor block only if a forward region is actually admitted.
        filter.prepareExecution(tracker);
        nativeRows = 0;
    }

    @Override public void clearExecution() {
        try { filter.close(); }
        finally { buffer.close(); buffer.setMemoryTracker(null); }
    }

    @Override public CharSequence getName() { return name; }
    @Override public long getNativeRows() { return nativeRows; }

    @Override public long scan(PageFrameMemoryRecord record, Map map, long lo, long hi, long minTimestamp,
                               long maxTimestamp, SqlExecutionCircuitBreaker breaker) {
        if (record instanceof PageFrameFilteredMemoryRecord) return -1;
        long timestamps = record.getPageAddress(timestampColumn);
        if (timestamps == 0) return -1;
        buffer.clearAndEnsureCapacity((bytes + 7) / 8);
        long state = buffer.getAddress(), arguments = state + stateBytes;
        long keyDescriptors = arguments + FILTER + filter.bytes();
        if (!filter.prepare(record, arguments + FILTER) || !probe.fixedKey().prepare(record, keyDescriptors)) return -1;
        Unsafe.putLong(state + START_ROW, lo);
        Unsafe.putLong(state + KEY_COLUMNS, keyDescriptors);
        Unsafe.putLong(arguments + TIMESTAMPS, timestamps);
        Unsafe.putLong(arguments + MIN_TIMESTAMP, minTimestamp);
        Unsafe.putLong(arguments + MAX_TIMESTAMP, maxTimestamp);
        long prefix = Rows.toRowID(record.getFrameIndex(), 0);
        Unsafe.putLong(arguments + ROW_ID_BASE, prefix);
        map.writeNativeProbeView(state + CONTROL_BYTES);
        memory.enter(breaker);
        try {
            while (true) {
                long status = code.run(arguments, hi, state);
                memory.throwIfFailed();
                if (status == 0) {
                    long next = Unsafe.getLong(state + START_ROW);
                    if (next < lo || next > hi) throw new IllegalStateException("Invalid ASOF scan position");
                    nativeRows += next - lo;
                    return next;
                }
                if (status != NEED_KEY) throw new IllegalStateException("ASOF scan failed: " + status);
                long row = Unsafe.getLong(state + RESUME_ROW);
                long rawKey = Unsafe.getLong(arguments + MISSING_KEY);
                MapKey key;
                if (probe.orderedNative()) key = ((OrderedMap) map).withRawKey(rawKey, probe.keyBytes());
                else {
                    key = map.withKey();
                    if (probe.keyType() == ColumnType.INT) key.putInt((int) rawKey);
                    else key.putLong(rawKey);
                }
                // Ordinary insertion owns growth, accounting and ASOF's optional-state limit.
                key.createValue().putLong(0, prefix + row);
                map.writeNativeProbeView(state + CONTROL_BYTES);
                Unsafe.putLong(state + START_ROW, row + 1);
            }
        } finally { memory.close(); }
    }

    @Override public void close() {
        try { clearExecution(); }
        finally { code.close(); }
    }
}
