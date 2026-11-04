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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.join.CompiledAsOfJoin;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.MapKernelState.*;

/** Fuses a bounded, already-selected forward strategy with ordinary aggregate updates. */
final class ResolvedAsOfJoin implements CompiledAsOfJoin {
    private static final int LEFT = 0, RIGHT = 8, RIGHT_HI = 16, PROBE_ROWS = 24, LIMIT = 32,
            LATEST = 40, AGG = 48, PREFIX = 56, LEFT_LO = 64, MISSING = 72,
            HISTORY_PREFIX = 80, HISTORY_ROW = 88, MASTER_TS = 96, SLAVE_TS = 104, HISTORY_TS = 112,
            CACHE_LO = 120, CACHE_HI = 128, CACHE_POS = 136, CACHE_COUNT = 144;
    private static final int FILTER_BATCH_ROWS = 32_768;
    private static final int STOP = 1, HISTORY = 7;
    private static final long STATE_BYTES = CONTROL_BYTES + QuestMapProbe.VIEW_BYTES;
    private final Input input;
    private final AggregateUpdates updates;
    private final ResolvedFilter.Binding filter;
    private final QuestMapProbe probe;
    private final CompiledKernel code;
    private final String name;
    private final int masterData, slaveData, historyData, filterData, symbolData, backwardWork, forwardWork, vectorData, selectedRows;
    private final long bytes;
    private final int keyType;
    private final boolean vectorFilter;
    private final DirectLongList buffer = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
    private final KernelMemory memory = KernelMemory.reusable((Runnable) null);
    private long nativeRows;
    private int filterFrame = -1;
    private long filterConsumedTo;

    private ResolvedAsOfJoin(Input input, AggregateUpdates updates, ResolvedFilter.Binding filter,
                             QuestMapProbe probe, Backend backend, Path artifacts) throws IOException {
        this.input = input;
        this.updates = updates;
        this.filter = filter;
        this.probe = probe;
        this.keyType = input.master().getColumnType(input.masterKey());
        vectorFilter = filter.residual() == null && filter.parameters().isEmpty();
        masterData = 160;
        slaveData = masterData + input.master().getColumnCount() * 8;
        historyData = slaveData + input.slave().getColumnCount() * 8;
        filterData = historyData + input.slave().getColumnCount() * 8;
        symbolData = (int) (filterData + filter.bytes() + 7) & ~7;
        backwardWork = symbolData + State.CAPACITY * 8;
        forwardWork = backwardWork + State.CAPACITY * 8;
        vectorData = forwardWork + State.CAPACITY * 8;
        selectedRows = vectorData + input.slave().getColumnCount() * 8 + 8;
        bytes = STATE_BYTES + (vectorFilter ? selectedRows + FILTER_BATCH_ROWS * 8 : vectorData);
        name = backend.name();
        // The adaptive owner may stay in lookup mode for the entire execution.
        code = CompiledKernel.defer(backend, this::emit, artifacts);
    }

    static CompiledAsOfJoin compile(Input input, Map map, Function predicate, ColumnTypes values,
                                    ObjList<GroupByFunction> functions, MapValue layout,
                                    Backend backend, Path artifacts) throws IOException {
        if (input.masterKey() < 0 || input.slaveKey() < 0 || !predicate.isStableWithinExecution()) return null;
        int keyType = input.master().getColumnType(input.masterKey());
        if (keyType != input.slave().getColumnType(input.slaveKey())
                || keyType != ColumnType.INT && keyType != ColumnType.LONG && keyType != ColumnType.SYMBOL
                || keyType == ColumnType.LONG && !(map instanceof Unordered8Map)
                || keyType != ColumnType.LONG && !(map instanceof Unordered4Map)) return null;
        if (input.master().getTimestampIndex() < 0 || input.slave().getTimestampIndex() < 0) return null;
        long base = layout.getAddress(0);
        var updates = AggregateUpdates.bind(input.aggregateInputs(), values, functions, i -> layout.getAddress(i) - base, false);
        if (updates == null || updates.inputs().varcharColumns().length != 0
                || updates.inputs().literals().length != 0 || !updates.inputs().captures().isEmpty()
                || !updates.inputs().expressions().isEmpty()) return null;
        for (var update : updates.updates()) if (update.allocates() || update.scalarState().isEmpty()) return null;
        for (var column : updates.columns()) {
            if (column.width() != MemoryType.I32 && column.width() != MemoryType.I64 && column.width() != MemoryType.F64)
                return null;
        }
        var filter = ResolvedFilter.bind(input.slave(), predicate);
        if (filter == null) return null;
        var probe = QuestMapProbe.direct(-1, keyType == ColumnType.SYMBOL ? ColumnType.INT : keyType, false);
        try {
            return new ResolvedAsOfJoin(input, updates, filter, probe, backend, artifacts);
        } catch (Throwable failure) {
            try { filter.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private Program emit() {
        Helper selection = selectionHelper();
        Builder b = new Builder();
        Var left = b.let(load(at(COLUMNS, LEFT))), right = b.let(load(at(COLUMNS, RIGHT)));
        Var work = b.let(load(at(COLUMNS, PROBE_ROWS)));
        Var latest = b.let(load(at(COLUMNS, LATEST)));
        Expr descriptors = at(COLUMNS, filterData);
        var addresses = new ArrayList<Expr>();
        for (int i = 0; i < filter.plan().input().size(); i++)
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(descriptors, i * 8L))));
        var parameters = new ArrayList<Expr>();
        for (int i = 0; i < filter.parameters().size(); i++)
            parameters.add(b.let(new Load(MemoryType.I32, at(descriptors, (filter.sourceIndexes().length + i) * 8L))));
        boolean scale = input.master().getTimestampType() != input.slave().getTimestampType();
        long masterScale = scale ? ColumnType.getTimestampDriver(input.master().getTimestampType()).toNanosScale() : 1;
        long slaveScale = scale ? ColumnType.getTimestampDriver(input.slave().getTimestampType()).toNanosScale() : 1;
        MemoryType keyWidth = keyType == ColumnType.LONG ? MemoryType.I64 : MemoryType.I32;
        b.loop(binary(Op.LT, left, ROWS), loop -> {
            Expr offset = binary(Op.MUL, binary(Op.SUB, left, load(at(COLUMNS, LEFT_LO))), i64(8));
            Var key = loop.let(keyType == ColumnType.SYMBOL
                    ? load(new Offset(at(COLUMNS, symbolData), offset))
                    : column(keyWidth, at(COLUMNS, masterData), input.masterKey(), left));
            Var known = loop.let(keyType == ColumnType.SYMBOL ? ne(key, i64(SymbolTable.VALUE_NOT_FOUND)) : bool(true));
            Var ts = loop.let(scaled(timestamp(MASTER_TS, left), masterScale));
            Var min = loop.let(input.tolerance() == Numbers.LONG_NULL ? i64(Long.MIN_VALUE) : binary(Op.SUB, ts, i64(input.tolerance())));
            Var match = loop.let(bool(false));
            Var matchedRow = loop.let(i64(0)), payload = loop.let(at(COLUMNS, slaveData));
            Var backward = loop.let(i64(-1));
            loop.when(known, resolve -> {
                if (selection == null) {
                    Var scanning = resolve.let(bool(true));
                    resolve.loop(scanning, scan -> {
                        scan.when(eq(right, load(at(COLUMNS, RIGHT_HI))), stop -> {
                            save(stop, left, right, work, latest); stop.exit(STOP);
                        });
                        Var rts = scan.let(scaled(timestamp(SLAVE_TS, right), slaveScale));
                        scan.assign(scanning, binary(Op.GE, ts, rts));
                        scan.when(scanning, advance -> {
                            advance.when(binary(Op.GE, work, load(at(COLUMNS, LIMIT))), stop -> {
                                save(stop, left, right, work, latest); stop.exit(STOP);
                            });
                            advance.when(binary(Op.GE, rts, min), eligible -> {
                                var row = ScanLowering.row(filter.plan().input(), addresses, right, false, parameters);
                                eligible.when(filter.plan().predicate().emit(row), selected -> {
                                    java.util.function.Consumer<Builder> update = accepted -> {
                                        Var rhsKey = accepted.let(column(keyWidth, at(COLUMNS, slaveData), input.slaveKey(), right));
                                        Var destination = probe.find(accepted, rhsKey, right, i64(0), missing -> {
                                            save(missing, left, right, work, latest);
                                            missing.store(at(COLUMNS, MISSING), rhsKey);
                                        });
                                        accepted.store(destination, add(load(at(COLUMNS, PREFIX)), right));
                                    };
                                    if (filter.residual() == null) update.accept(selected);
                                    else selected.when(filter.residual().emit(selected, descriptors, right), update);
                                });
                            });
                            advance.assign(latest, add(load(at(COLUMNS, PREFIX)), right));
                            advance.assign(right, add(right, i64(1)));
                            advance.assign(work, add(work, i64(1)));
                            advance.when(eq(binary(Op.AND, right, i64(1023)), i64(0)), Builder::poll);
                        });
                    });
                } else {
                    emitVectorScan(resolve, left, right, work, latest, ts, min, slaveScale, selection);
                }
                Var destination = probe.findExisting(resolve, key);
                resolve.when(eq(destination, nil()), missing -> {
                    save(missing, left, right, work, latest); missing.exit(STOP);
                });
                Var rowId = resolve.let(load(destination));
                resolve.when(eq(rowId, i64(Numbers.LONG_NULL)), absent -> absent.assign(backward, i64(0)));
                resolve.when(ne(rowId, i64(Numbers.LONG_NULL)), found -> {
                    found.assign(matchedRow, binary(Op.AND, rowId, i64(Rows.toLocalRowID(-1))));
                    Var prefix = found.let(binary(Op.SUB, rowId, matchedRow));
                    Var timestampAddress = found.let(new Load(MemoryType.ADDRESS, at(COLUMNS, SLAVE_TS)));
                    found.when(ne(prefix, load(at(COLUMNS, PREFIX))), history -> {
                        history.when(ne(prefix, load(at(COLUMNS, HISTORY_PREFIX))), borrow -> {
                            save(borrow, left, right, work, latest);
                            borrow.store(at(COLUMNS, HISTORY_ROW), rowId); borrow.exit(HISTORY);
                        });
                        history.assign(payload, at(COLUMNS, historyData));
                        history.assign(timestampAddress, new Load(MemoryType.ADDRESS, at(COLUMNS, HISTORY_TS)));
                    });
                    found.assign(match, binary(Op.GE, scaled(load(new Offset(timestampAddress, binary(Op.MUL, matchedRow, i64(8)))), slaveScale), min));
                    found.when(eq(binary(Op.AND, latest, i64(~Rows.toLocalRowID(-1))), prefix), same ->
                            same.assign(backward, add(binary(Op.SUB, latest, rowId), i64(1))));
                });
            });
            var inputs = new ArrayList<Pipeline.Value>();
            int split = input.master().getColumnCount();
            for (int i = 0; i < updates.sourceIndexes().length; i++) {
                var column = updates.columns().get(i);
                int source = joinedColumn(updates.sourceIndexes()[i]);
                Var value;
                if (source < split) value = loop.let(column(column.width(), at(COLUMNS, masterData), source, left));
                else {
                    value = loop.let(column.width() == MemoryType.F64 ? f64(Double.NaN) : i64(column.nullValue()));
                    int rhs = slaveColumn(source - split);
                    loop.when(match, present -> present.assign(value, column(column.width(), payload, rhs, matchedRow)));
                }
                inputs.add(column.value(value));
            }
            updates.emit(loop, new Pipeline.Row(updates.columns(), inputs), new Load(MemoryType.ADDRESS, at(COLUMNS, AGG)), nil());
            loop.store(new Offset(at(COLUMNS, backwardWork), offset), backward);
            loop.store(new Offset(at(COLUMNS, forwardWork), offset), work);
            loop.assign(left, add(left, i64(1)));
            loop.assign(work, i64(0));
        });
        save(b, left, right, work, latest); b.exit(0);
        return new Program(b.block());
    }

    private Helper selectionHelper() {
        if (!vectorFilter) return null;
        var statements = new ArrayList<>(ScanLowering.selection(filter.plan(), filter.sourceIndexes(), false).body().statements());
        Return result = (Return) statements.removeLast();
        statements.add(new Store(at(STATE, -8), result.value()));
        statements.add(new Return(i64(0)));
        return new Helper("asof_select", java.util.List.of(COLUMNS, ROWS, STATE), new Block(statements));
    }

    private void emitVectorScan(Builder b, Var left, Var right, Var work, Var latest, Expr timestamp,
                                Expr minTimestamp, long scale, Helper selection) {
        b.when(eq(right, load(at(COLUMNS, RIGHT_HI))), stop -> {
            save(stop, left, right, work, latest); stop.exit(STOP);
        });
        Expr remaining = binary(Op.SUB, load(at(COLUMNS, LIMIT)), work);
        Var budgetEnd = b.let(add(right, new Select(binary(Op.LT, remaining, binary(Op.SUB, load(at(COLUMNS, RIGHT_HI)), right)),
                remaining, binary(Op.SUB, load(at(COLUMNS, RIGHT_HI)), right))));
        Var scanning = b.let(bool(true));
        b.loop(binary(Op.BOTH, scanning, binary(Op.LT, right, budgetEnd)), scan -> {
            scan.when(binary(Op.EITHER, binary(Op.LT, right, load(at(COLUMNS, CACHE_LO))),
                    binary(Op.GE, right, load(at(COLUMNS, CACHE_HI)))), refill -> {
                Expr remainingRows = binary(Op.SUB, budgetEnd, right);
                Var count = refill.let(new Select(binary(Op.LT, remainingRows, i64(FILTER_BATCH_ROWS)), remainingRows, i64(FILTER_BATCH_ROWS)));
                for (int i = 0; i < filter.sourceIndexes().length; i++) {
                    int column = filter.sourceIndexes()[i];
                    refill.store(MemoryType.ADDRESS, at(COLUMNS, vectorData + column * 8L), new Offset(
                            new Load(MemoryType.ADDRESS, at(COLUMNS, slaveData + column * 8L)),
                            binary(Op.MUL, right, i64(filter.plan().input().get(i).width().bytes))));
                }
                refill.poll();
                refill.call(selection, java.util.List.of(at(COLUMNS, vectorData), count, at(COLUMNS, selectedRows)));
                refill.store(at(COLUMNS, CACHE_COUNT), load(at(COLUMNS, selectedRows - 8)));
                refill.store(at(COLUMNS, CACHE_POS), i64(0));
                refill.store(at(COLUMNS, CACHE_LO), right);
                refill.store(at(COLUMNS, CACHE_HI), add(right, count));
                // A full selection is a contiguous stream; its hardware-prefetched loads
                // do not benefit from another pass issuing one hint per selected row.
                refill.when(binary(Op.LT, load(at(COLUMNS, CACHE_COUNT)), count), sparse -> {
                    Var ahead = sparse.let(i64(0));
                    sparse.loop(binary(Op.LT, ahead, load(at(COLUMNS, CACHE_COUNT))), prefetch -> {
                        Var row = prefetch.let(add(right, load(new Offset(at(COLUMNS, selectedRows), binary(Op.MUL, ahead, i64(8))))));
                        prefetch.prefetch(new Offset(new Load(MemoryType.ADDRESS, at(COLUMNS, SLAVE_TS)), binary(Op.MUL, row, i64(8))), false);
                        prefetch.prefetch(new Offset(new Load(MemoryType.ADDRESS, at(COLUMNS, slaveData + input.slaveKey() * 8L)),
                                binary(Op.MUL, row, i64(keyType == ColumnType.LONG ? 8 : 4))), false);
                        prefetch.assign(ahead, add(ahead, i64(1)));
                    });
                });
            });
            Var selected = scan.let(load(at(COLUMNS, CACHE_POS)));
            Var chunkEnd = scan.let(new Select(binary(Op.LT, budgetEnd, load(at(COLUMNS, CACHE_HI))), budgetEnd, load(at(COLUMNS, CACHE_HI))));
            Var boundary = scan.let(chunkEnd);
            Var more = scan.let(bool(true));
            java.util.function.BiConsumer<Builder, Expr> consume = (matches, offset) -> {
                Var row = matches.let(add(load(at(COLUMNS, CACHE_LO)), offset));
                matches.assign(more, binary(Op.LT, row, chunkEnd));
                matches.when(more, inRange -> {
                    inRange.when(binary(Op.GE, row, right), update -> {
                        Var selectedTimestamp = update.let(scaled(timestamp(SLAVE_TS, row), scale));
                        update.when(binary(Op.LT, timestamp, selectedTimestamp), future -> {
                            future.assign(boundary, row);
                            future.assign(more, bool(false));
                            future.assign(scanning, bool(false));
                        });
                        update.when(more, passed -> {
                            passed.assign(work, add(work, binary(Op.SUB, row, right)));
                            passed.assign(right, row);
                            passed.when(binary(Op.GE, selectedTimestamp, minTimestamp), eligible -> {
                                MemoryType width = keyType == ColumnType.LONG ? MemoryType.I64 : MemoryType.I32;
                                Var key = eligible.let(column(width, at(COLUMNS, slaveData), input.slaveKey(), row));
                                Var destination = probe.find(eligible, key, row, i64(0), missing -> {
                                    save(missing, left, right, work, latest);
                                    missing.store(at(COLUMNS, MISSING), key);
                                    missing.store(at(COLUMNS, CACHE_POS), selected);
                                });
                                eligible.store(destination, add(load(at(COLUMNS, PREFIX)), row));
                            });
                            passed.assign(right, add(right, i64(1)));
                            passed.assign(work, add(work, i64(1)));
                            passed.assign(latest, add(load(at(COLUMNS, PREFIX)), row));
                        });
                    });
                    inRange.when(more, next -> next.assign(selected, add(selected, i64(1))));
                });
            };
            // A full selection is the identity row sequence. Keep this choice outside
            // the row loop; both versions share the same update emitter and semantics.
            Var full = scan.let(eq(load(at(COLUMNS, CACHE_COUNT)),
                    binary(Op.SUB, load(at(COLUMNS, CACHE_HI)), load(at(COLUMNS, CACHE_LO)))));
            Expr hasMore = binary(Op.BOTH, more, binary(Op.LT, selected, load(at(COLUMNS, CACHE_COUNT))));
            scan.when(full, dense -> dense.loop(hasMore, matches -> consume.accept(matches, selected)));
            scan.when(eq(full, bool(false)), sparse -> sparse.loop(hasMore, matches -> consume.accept(matches,
                    load(new Offset(at(COLUMNS, selectedRows), binary(Op.MUL, selected, i64(8)))))));
            scan.store(at(COLUMNS, CACHE_POS), selected);
            // Match scanFilteredBatch(): predicate-rejected rows are permanently irrelevant,
            // including future timestamps. Stop before the first qualifying future row.
            scan.assign(work, add(work, binary(Op.SUB, boundary, right)));
            scan.when(binary(Op.LT, right, boundary), consumed -> consumed.assign(latest,
                    binary(Op.SUB, add(load(at(COLUMNS, PREFIX)), boundary), i64(1))));
            scan.assign(right, boundary);
        });
        b.when(eq(right, load(at(COLUMNS, RIGHT_HI))), stop -> {
            save(stop, left, right, work, latest); stop.exit(STOP);
        });
        b.when(binary(Op.BOTH, eq(right, budgetEnd), binary(Op.GE, timestamp, scaled(timestamp(SLAVE_TS, right), scale))), stop -> {
            save(stop, left, right, work, latest); stop.exit(STOP);
        });
    }

    private static Expr column(MemoryType width, Expr descriptors, int column, Expr row) {
        return new Load(width, new Offset(new Load(MemoryType.ADDRESS, at(descriptors, column * 8L)), binary(Op.MUL, row, i64(width.bytes))));
    }

    private static Expr timestamp(int descriptor, Expr row) {
        return load(new Offset(new Load(MemoryType.ADDRESS, at(COLUMNS, descriptor)), binary(Op.MUL, row, i64(8))));
    }

    private static Expr scaled(Expr timestamp, long scale) {
        if (scale == 1) return timestamp;
        return new Select(binary(Op.LT, i64(Long.MAX_VALUE / scale), timestamp), i64(Long.MAX_VALUE),
                new Select(binary(Op.LT, timestamp, i64(Long.MIN_VALUE / scale)), i64(Long.MIN_VALUE), binary(Op.MUL, timestamp, i64(scale))));
    }

    private static void save(Builder b, Expr left, Expr right, Expr work, Expr latest) {
        b.store(at(COLUMNS, LEFT), left); b.store(at(COLUMNS, RIGHT), right);
        b.store(at(COLUMNS, PROBE_ROWS), work); b.store(at(COLUMNS, LATEST), latest);
    }

    private int joinedColumn(int column) {
        return input.aggregateColumns() == null ? column : input.aggregateColumns().getQuick(column);
    }

    private int slaveColumn(int column) {
        return input.slaveProjection() == null ? column : input.slaveProjection().getQuick(column);
    }

    @Override
    public void aggregate(PageFrameMemoryRecord master, TimeFrameCursor slave, Map map, MapValue value,
                          State progress, SqlExecutionCircuitBreaker breaker) {
        if (master instanceof PageFrameFilteredMemoryRecord || !(slave.getRecord() instanceof PageFrameMemoryRecord rhs)
                || rhs instanceof PageFrameFilteredMemoryRecord) return;
        buffer.clearAndEnsureCapacity((bytes + 7) / 8);
        long state = buffer.getAddress(), args = state + STATE_BYTES;
        long prefix = Rows.toRowID(Rows.toPartitionIndex(progress.forwardRowId), 0);
        long right = Rows.toLocalRowID(progress.forwardRowId);
        if (progress.forwardRowId == progress.slaveHi) return;
        slave.recordAt(rhs, progress.forwardRowId);
        if (!prepare(master, rhs, args) || !filter.prepare(rhs, args + filterData)) return;
        if (filterFrame != rhs.getFrameIndex() || right < filterConsumedTo) {
            Unsafe.putLong(args + CACHE_LO, right);
            Unsafe.putLong(args + CACHE_HI, right);
            Unsafe.putLong(args + CACHE_POS, 0);
            Unsafe.putLong(args + CACHE_COUNT, 0);
        }
        Unsafe.putLong(args + LEFT, progress.masterLo);
        Unsafe.putLong(args + LEFT_LO, progress.masterLo);
        Unsafe.putLong(args + RIGHT, right);
        Unsafe.putLong(args + RIGHT_HI, progress.slaveHi - prefix);
        Unsafe.putLong(args + PREFIX, prefix);
        Unsafe.putLong(args + PROBE_ROWS, 0);
        Unsafe.putLong(args + LIMIT, progress.forwardLimit);
        Unsafe.putLong(args + LATEST, progress.latestRowId);
        Unsafe.putLong(args + AGG, value.getAddress(0));
        Unsafe.putLong(args + HISTORY_PREFIX, -1);
        if (keyType == ColumnType.SYMBOL) {
            for (int i = 0; i < progress.masterHi - progress.masterLo; i++)
                Unsafe.putLong(args + symbolData + i * 8L, progress.symbolKeys[i]);
        }
        map.writeNativeProbeView(state + CONTROL_BYTES);
        memory.enter(breaker);
        try {
            while (true) {
                long status = code.run(args, progress.masterHi, state);
                memory.throwIfFailed();
                if (status == 0 || status == STOP) return;
                if (status == HISTORY) {
                    long rowId = Unsafe.getLong(args + HISTORY_ROW);
                    PageFrameMemoryRecord historical = (PageFrameMemoryRecord) slave.getRecordB();
                    slave.recordAt(historical, rowId);
                    if (!prepareSlave(historical, args + historyData, args + HISTORY_TS)) return;
                    Unsafe.putLong(args + HISTORY_PREFIX, Rows.toRowID(Rows.toPartitionIndex(rowId), 0));
                } else if (status == NEED_KEY) {
                    long keyValue = Unsafe.getLong(args + MISSING);
                    var key = map.withKey();
                    if (keyType == ColumnType.LONG) key.putLong(keyValue); else key.putInt((int) keyValue);
                    long row = Unsafe.getLong(args + RIGHT);
                    key.createValue().putLong(0, prefix + row);
                    Unsafe.putLong(args + RIGHT, row + 1);
                    Unsafe.putLong(args + PROBE_ROWS, Unsafe.getLong(args + PROBE_ROWS) + 1);
                    Unsafe.putLong(args + LATEST, prefix + row);
                    map.writeNativeProbeView(state + CONTROL_BYTES);
                } else throw new IllegalStateException("Invalid ASOF aggregate status: " + status);
            }
        } finally {
            filterFrame = rhs.getFrameIndex();
            filterConsumedTo = Unsafe.getLong(args + RIGHT);
            progress.nextMaster = Unsafe.getLong(args + LEFT);
            progress.forwardRowId = prefix + Unsafe.getLong(args + RIGHT);
            progress.forwardRows = Unsafe.getLong(args + PROBE_ROWS);
            progress.latestRowId = Unsafe.getLong(args + LATEST);
            for (int i = 0; i < progress.nextMaster - progress.masterLo; i++) {
                progress.backwardWork[i] = Unsafe.getLong(args + backwardWork + i * 8L);
                progress.forwardWork[i] = Unsafe.getLong(args + forwardWork + i * 8L);
            }
            nativeRows += progress.nextMaster - progress.masterLo;
            memory.close();
        }
    }

    private boolean prepare(PageFrameMemoryRecord master, PageFrameMemoryRecord rhs, long args) {
        long timestamp = master.getPageAddress(input.master().getTimestampIndex());
        if (timestamp == 0 || master.getPageAddress(input.masterKey()) == 0) return false;
        Unsafe.putLong(args + MASTER_TS, timestamp);
        for (int i = 0; i < input.master().getColumnCount(); i++) Unsafe.putLong(args + masterData + i * 8L, master.getPageAddress(i));
        for (int aggregateColumn : updates.sourceIndexes()) {
            int source = joinedColumn(aggregateColumn);
            if (source < input.master().getColumnCount() && master.getPageAddress(source) == 0) return false;
        }
        return prepareSlave(rhs, args + slaveData, args + SLAVE_TS) && rhs.getPageAddress(input.slaveKey()) != 0;
    }

    private boolean prepareSlave(PageFrameMemoryRecord record, long descriptors, long timestampDescriptor) {
        long timestamp = record.getPageAddress(input.slave().getTimestampIndex());
        if (timestamp == 0) return false;
        Unsafe.putLong(timestampDescriptor, timestamp);
        for (int i = 0; i < input.slave().getColumnCount(); i++) Unsafe.putLong(descriptors + i * 8L, record.getPageAddress(i));
        int split = input.master().getColumnCount();
        for (int aggregateColumn : updates.sourceIndexes()) {
            int source = joinedColumn(aggregateColumn);
            if (source >= split && record.getPageAddress(slaveColumn(source - split)) == 0) return false;
        }
        return true;
    }

    @Override public void prepareExecution(MemoryTracker tracker) {
        clearExecution(); buffer.setMemoryTracker(tracker); filter.prepareExecution(tracker); nativeRows = 0;
    }
    @Override public void clearExecution() {
        try { filter.close(); } finally { buffer.close(); buffer.setMemoryTracker(null); filterFrame = -1; }
    }
    @Override public CharSequence getName() { return name; }
    @Override public long getNativeRows() { return nativeRows; }
    @Override public void close() {
        try { clearExecution(); } finally { code.close(); }
    }
}
