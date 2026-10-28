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
import io.questdb.cairo.map.NativeMapView;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.ObjList;
import org.questdb.experimental.codegen.AggregateUpdates.ScalarUpdate;
import org.questdb.experimental.codegen.AggregateUpdates.Update;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Compiles resolved updates and fused probes over existing QuestDB map state. */
public final class ResolvedAggregateKernel {
    private ResolvedAggregateKernel() {
    }

    /** Borrows functions only during binding. No SQL spelling or mutable function is retained. */
    public static Binding bind(ColumnTypes inputTypes, ColumnTypes valueTypes, ObjList<GroupByFunction> functions) {
        return bind(inputTypes, valueTypes, functions, null);
    }

    static Binding bind(ColumnTypes inputTypes, ColumnTypes valueTypes, ObjList<GroupByFunction> functions, QuestMapProbe probe) {
        var layout = new FlyweightPackedMapValue(valueTypes);
        // The ordinary planner represents scalar DISTINCT as key-only grouping plus
        // cardinality metadata. Its empty state needs probing/merge but no update slots.
        AggregateUpdates resolved = functions.size() == 0 && valueTypes.getColumnCount() == 0 && probe != null
                ? new AggregateUpdates(new ResolvedVarchar.Binding(new int[0], List.of(), new int[0], new byte[0]), List.of())
                : AggregateUpdates.bind(valueTypes, functions, layout::getOffset, true, new ResolvedInputs(inputTypes), probe != null && (!probe.ordered() || probe.orderedNative()));
        if (resolved == null) return null;
        var columns = resolved.columns();
        var updates = resolved.updates();
        boolean allocates = updates.stream().anyMatch(Update::allocates);
        Builder b = new Builder();
        Var batches = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < columns.size(); i++) {
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, (i + 1L) * 8))));
        }
        Expr cardinalityAddress = at(COLUMNS, resolved.cardinalityOffset());
        emitUpdates(b, batches, addresses, resolved, cardinalityAddress, allocates);
        b.exit(0);
        int prefetchRows = Integer.getInteger("query.codegen.prefetchRows", 32);
        if (prefetchRows < 0 || prefetchRows > 1024 || (prefetchRows & (prefetchRows - 1)) != 0) {
            throw new IllegalArgumentException("Prefetch rows must be zero or a power of two up to 1024");
        }
        boolean countSumReference = columns.size() == 3 && layout.getSizeInBytes() == 40
                && columns.get(0).columnType() == ColumnType.LONG
                && columns.get(1).columnType() == ColumnType.INT
                && columns.get(2).columnType() == ColumnType.SHORT
                && updates.equals(List.of(new ScalarUpdate(new Pipeline.Count(), 0),
                new ScalarUpdate(new Pipeline.CountValue(columns.get(0)), 8),
                new ScalarUpdate(new Pipeline.Sum(columns.get(0)), 16),
                new ScalarUpdate(new Pipeline.Sum(columns.get(1)), 24),
                new ScalarUpdate(new Pipeline.Sum(columns.get(2)), 32)));
        var readers = new ArrayList<ResultRead>();
        for (int i = 0; i < functions.size(); i++) {
            var function = functions.getQuick(i);
            // Capture the exact output slot and type while resolved functions are borrowed.
            readers.add(new ResultRead(function.getClass(), function.getValueIndex(), layout.getOffset(function.getValueIndex()), function.getType()));
        }
        return new Binding(new Program(b.block()), allocates, probe, prefetchRows, countSumReference, resolved, layout.getSizeInBytes(), List.copyOf(readers));
    }

    static final int NEED_CAPACITY = 5;

    private static Program mapProgram(AggregateUpdates updates, QuestMapProbe probe, long valueBytes, int prefetchRows, int localCapacity) {
        return mapProgram(updates, probe, valueBytes, prefetchRows, localCapacity, null);
    }

    private static Program mapProgram(AggregateUpdates updates, QuestMapProbe probe, long valueBytes,
                                      int prefetchRows, int localCapacity, ScanPlan filter) {
        var columns = updates.columns();
        Builder b = new Builder();
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < columns.size(); i++) {
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, (i + 1L) * 8))));
        }
        Expr cardinalityAddress = at(COLUMNS, updates.cardinalityOffset());
        Var rowIds = filter == null ? b.let(new Load(MemoryType.ADDRESS, at(STATE, 8))) : null;
        var filterAddresses = new ArrayList<Var>();
        Var accepted = filter != null ? b.let(i64(0)) : null;
        if (filter != null) {
            Var pointers = b.let(new Load(MemoryType.ADDRESS, at(STATE, 8)));
            for (int i = 0; i < filter.input().size(); i++) {
                filterAddresses.add(b.let(new Load(MemoryType.ADDRESS, at(pointers, 8L * i))));
            }
        }
        Var index = b.let(load(STATE));
        Var batchEnd = b.let(probe.mutable() && !probe.variable() ? new Select(eq(load(at(STATE, 56)), i64(0)), index, ROWS) : ROWS);
        if (prefetchRows > 0) {
            emitPrefetched(b, index, batchEnd, rowIds, addresses, updates, cardinalityAddress, probe, valueBytes, prefetchRows, localCapacity, filter, filterAddresses, accepted);
        } else b.loop(binary(Op.LT, index, ROWS), loop -> {
            checkLocalBound(loop, probe, index, localCapacity);
            reserveBatch(loop, probe, index, batchEnd);
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var rowIndex = loop.let(index);
            loop.when(ne(rowIds, nil()), filtered -> filtered.assign(rowIndex,
                    load(new Offset(rowIds, binary(Op.MUL, index, i64(8))))));
            if (probe.variable()) { loop.store(at(STATE, 80), i64(0)); loop.store(at(STATE, 32), index); }
            Var destination = probe.find(loop, probe.loadFrameKey(loop, rowIndex, at(STATE, prefetchScratch(probe) + 24)), index,
                    probe.mutable() ? load(at(STATE, 56)) : null, null, updates::initialize);
            updates.emitFrame(loop, rowIndex, addresses, destination, cardinalityAddress);
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(filter == null ? b.block() : saveAcceptedRows(b.block(), accepted));
    }

    // This word exists only in the fused slot, between cardinality and deferred-merge state.
    static final int FILTERED_ROWS = -24;

    /** Save progress at resumable exits; keep the counter in SSA during normal row updates. */
    private static Block saveAcceptedRows(Block block, Var accepted) {
        var statements = new ArrayList<Statement>();
        for (Statement statement : block.statements()) {
            if (statement instanceof Return) statements.add(new Store(at(STATE, FILTERED_ROWS), accepted));
            statements.add(statement instanceof If branch ? new If(branch.condition(), saveAcceptedRows(branch.body(), accepted))
                    : statement instanceof While loop ? new While(loop.condition(), saveAcceptedRows(loop.body(), accepted)) : statement);
        }
        return new Block(statements);
    }

    // Scratch rows hold source row, key and candidate address. OrderedMap also retains
    // the hash and candidate slot before any packed key bytes.
    // Each native invocation rebuilds scratch before consumption, including after heap growth.
    static long prefetchScratch(QuestMapProbe probe) {
        return probe.controlBytes() + (long) GroupByMapFragment.NUM_SHARDS * probe.viewBytes();
    }

    private static void emitPrefetched(Builder b, Var index, Var batchEnd, Var rowIds,
                                      List<Var> addresses, AggregateUpdates updates, Expr cardinalityAddress,
                                      QuestMapProbe probe, long valueBytes, int batchRows, int localCapacity,
                                      ScanPlan filter, List<Var> filterAddresses, Var accepted) {
        boolean stagedState = probe.varchar() && updates.updates().stream().anyMatch(Update::allocates);
        Var batches = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), batch -> {
            checkLocalBound(batch, probe, index, localCapacity);
            reserveBatch(batch, probe, index, batchEnd);
            batch.when(eq(binary(Op.AND, batches, i64(1024 / batchRows - 1)), i64(0)), Builder::poll);
            batch.assign(batches, add(batches, i64(1)));
            Var limit = batch.let(new Select(binary(Op.LT, add(index, i64(batchRows)), batchEnd),
                    add(index, i64(batchRows)), batchEnd));
            if (probe.variable()) { batch.store(at(STATE, 80), i64(0)); batch.store(at(STATE, 32), index); }
            Var batchStart = filter != null ? batch.let(index) : null;
            Var mask = filter != null ? ScanLowering.mask(filter, batch, filterAddresses, batchStart, limit, batchRows) : null;
            Var remaining = filter != null ? batch.let(mask) : null;
            Var position = batch.let(index);
            Var scratch = batch.let(at(STATE, prefetchScratch(probe)));
            boolean staged = probe.orderedNative();
            int keyOffset = probe.orderedNative() ? 40 : 24;
            if (staged) {
                batch.loop(filter == null ? binary(Op.LT, position, limit) : ne(remaining, i64(0)), stage -> {
                    if (filter != null) stage.assign(position, add(batchStart, new TrailingZeros(remaining)));
                    Var row = stage.let(position);
                    if (filter == null) stage.when(ne(rowIds, nil()), filtered -> filtered.assign(row,
                            load(new Offset(rowIds, binary(Op.MUL, position, i64(8))))));
                    Var key = probe.loadFrameKey(stage, row, at(scratch, keyOffset));
                    Var hash = probe.prefetchOffset(stage, key);
                    stage.store(scratch, row);
                    stage.store(probe.stagedKeyType(), at(scratch, 8), key);
                    stage.store(at(scratch, 24), hash);
                    stage.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                    stage.assign(position, add(position, i64(1)));
                    if (filter != null) stage.assign(remaining, binary(Op.AND, remaining, binary(Op.SUB, remaining, i64(1))));
                });
                batch.assign(position, index);
                if (filter != null) batch.assign(remaining, mask);
                batch.assign(scratch, at(STATE, prefetchScratch(probe)));
            }
            batch.loop(filter == null ? binary(Op.LT, position, limit) : ne(remaining, i64(0)), prepare -> {
                if (filter != null) prepare.assign(position, add(batchStart, new TrailingZeros(remaining)));
                Var row = prepare.let(staged ? load(scratch) : position);
                if (!staged && filter == null) prepare.when(ne(rowIds, nil()), filtered -> filtered.assign(row,
                        load(new Offset(rowIds, binary(Op.MUL, position, i64(8))))));
                Var key = staged ? prepare.let(new Load(probe.stagedKeyType(), at(scratch, 8)))
                        : probe.loadFrameKey(prepare, row, at(scratch, keyOffset));
                Var hash = staged ? prepare.let(load(at(scratch, 24))) : null;
                var candidate = probe.prepare(prepare, key, valueBytes, (hint, destination) -> {
                    if (!stagedState) updates.prefetch(hint, row, addresses, destination);
                }, hash);
                prepare.store(scratch, row);
                prepare.store(probe.stagedKeyType(), at(scratch, 8), candidate.key());
                prepare.store(MemoryType.ADDRESS, at(scratch, 16), candidate.entry());
                if (staged) prepare.store(at(scratch, 32), candidate.slot());
                prepare.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                prepare.assign(position, add(position, i64(1)));
                if (filter != null) prepare.assign(remaining, binary(Op.AND, remaining, binary(Op.SUB, remaining, i64(1))));
            });
            if (stagedState) {
                // Issue independent map prefetches before reading their allocated state pointers.
                // No mutation occurs until consumption; a growth exit rebuilds the remaining batch.
                batch.assign(position, index);
                if (filter != null) batch.assign(remaining, mask);
                batch.assign(scratch, at(STATE, prefetchScratch(probe)));
                batch.loop(filter == null ? binary(Op.LT, position, limit) : ne(remaining, i64(0)), hints -> {
                    if (filter != null) hints.assign(position, add(batchStart, new TrailingZeros(remaining)));
                    Var row = hints.let(load(scratch));
                    Var entry = hints.let(new Load(MemoryType.ADDRESS, at(scratch, 16)));
                    hints.when(ne(load(entry), i64(0)), occupied ->
                            updates.prefetch(occupied, row, addresses, at(entry, probe.keyBytes())));
                    hints.assign(position, add(position, i64(1)));
                    if (filter != null) hints.assign(remaining, binary(Op.AND, remaining, binary(Op.SUB, remaining, i64(1))));
                    hints.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                });
            }
            batch.assign(scratch, at(STATE, prefetchScratch(probe)));
            if (filter != null) batch.assign(remaining, mask);
            batch.loop(filter == null ? binary(Op.LT, index, limit) : ne(remaining, i64(0)), consume -> {
                if (filter != null) consume.assign(index, add(batchStart, new TrailingZeros(remaining)));
                Var row = consume.let(load(scratch));
                Var key = consume.let(new Load(probe.stagedKeyType(), at(scratch, 8)));
                Var entry = consume.let(new Load(MemoryType.ADDRESS, at(scratch, 16)));
                Var destination = staged
                        ? probe.finishOrdered(consume, key, entry, consume.let(load(at(scratch, 24))),
                                consume.let(load(at(scratch, 32))), index, updates::initialize)
                        : probe.finish(consume, key, entry, index, updates::initialize);
                updates.emitFrame(consume, row, addresses, destination, cardinalityAddress);
                consume.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                consume.assign(index, add(index, i64(1)));
                if (filter != null) {
                    consume.assign(accepted, add(accepted, i64(1)));
                    consume.assign(remaining, binary(Op.AND, remaining, binary(Op.SUB, remaining, i64(1))));
                }
            });
            if (filter != null) batch.assign(index, limit);
        });
    }

    private static void checkLocalBound(Builder b, QuestMapProbe probe, Var index, int capacity) {
        if (capacity == 0) return;
        b.when(binary(Op.BOTH, eq(load(at(STATE, 56)), i64(0)),
                binary(Op.GE, load(at(STATE, probe.controlBytes() + 56)), i64(capacity))), full -> {
            full.store(at(STATE, 32), index);
            full.exit(QuestPartitions.NEED_FLUSH);
        });
    }

    private static void reserveBatch(Builder b, QuestMapProbe probe, Var index, Var batchEnd) {
        if (!probe.mutable() || probe.variable()) return;
        b.when(eq(index, batchEnd), boundary -> {
            Var remaining = boundary.let(binary(Op.SUB, ROWS, index));
            Var requested = boundary.let(load(COLUMNS));
            Var batchRows = boundary.let(new Select(binary(Op.LT, remaining, requested), remaining, requested));
            Expr view = at(STATE, probe.controlBytes());
            Expr needs = probe.orderedNative()
                    ? binary(Op.LT, load(at(view, NativeMapView.Ordered.LIMIT)), add(load(at(view, NativeMapView.Ordered.APPEND)), binary(Op.MUL, batchRows, load(at(view, NativeMapView.STRIDE)))))
                    : binary(Op.GE, batchRows, load(at(view, NativeMapView.FREE)));
            boundary.when(needs, grow -> {
                grow.store(at(STATE, 24), i64(0));
                grow.store(at(STATE, 32), index);
                grow.exit(NEED_CAPACITY);
            });
            boundary.assign(batchEnd, add(index, batchRows));
        });
    }

    private static void emitUpdates(Builder b, Var batches, List<Var> addresses,
                                    AggregateUpdates updates, Expr cardinalityAddress, boolean allocates) {
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            if (allocates) loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var entry = loop.let(new Offset(batches, binary(Op.MUL, index, i64(8))));
            Var encoded = loop.let(load(entry));
            Var rowIndex = loop.let(binary(Op.AND, binary(Op.USHR, encoded, i64(Map.BATCH_ROW_INDEX_SHIFT)), i64(Map.BATCH_ROW_INDEX_MASK)));
            Var destination = loop.let(new Offset(STATE, binary(Op.AND, encoded, i64(Map.BATCH_OFFSET_MASK))));
            updates.emitFrame(loop, rowIndex, addresses, destination, cardinalityAddress);
            loop.assign(index, add(index, i64(1)));
        });
    }

    private record ResultRead(Class<?> implementation, int index, long offset, int type) { }

    public static final class Binding {
        private final Program program;
        final ResolvedVarchar.Binding inputs;
        final boolean allocates;
        final QuestMapProbe probe;
        final int prefetchRows;
        private final boolean countSumReference;
        private final AggregateUpdates updates;
        private final long valueBytes;
        private final List<ResultRead> readers;

        private Binding(Program program, boolean allocates, QuestMapProbe probe, int prefetchRows, boolean countSumReference, AggregateUpdates updates, long valueBytes, List<ResultRead> readers) {
            this.readers = readers;
            this.program = program;
            this.inputs = updates.inputs();
            this.allocates = allocates;
            this.probe = probe;
            this.prefetchRows = prefetchRows;
            this.countSumReference = countSumReference;
            this.updates = updates;
            this.valueBytes = valueBytes;
        }

        Prepared compileHandwritten(Path artifacts) throws IOException {
            if (!countSumReference || probe == null || prefetchRows != 32) {
                throw new IllegalArgumentException("Handwritten reference requires a fixed-width key, 32-row batches and "
                        + "COUNT(), COUNT(long), SUM(same long), SUM(int), SUM(short) in that order");
            }
            Prepared prepared = new Prepared(this, probe, null, artifacts, 0);
            try {
                prepared.code = prepared.own(HandwrittenCountSum.compile(probe, false, artifacts));
                prepared.mapCode = prepared.own(HandwrittenCountSum.compile(probe, true, artifacts));
                return prepared;
            } catch (Throwable failure) {
                prepared.closeAfterFailure(failure);
                throw failure;
            }
        }

        public Prepared compile(Backend backend, Path artifacts) throws IOException {
            return compile(backend, artifacts, false);
        }

        Prepared compile(Backend backend, Path artifacts, boolean partitionOwner) throws IOException {
            QuestMapProbe activeProbe = probe != null && (!probe.ordered() || probe.orderedNative()) ? probe.mutating() : probe;
            int localCapacity = partitionOwner ? localCapacity(activeProbe) : 0;
            Prepared prepared = new Prepared(this, activeProbe, backend, artifacts, localCapacity);
            try {
                prepared.code = prepared.compile(program);
                if (activeProbe != null) {
                    prepared.mapCode = prepared.compile(mapProgram(updates, activeProbe, valueBytes, prefetchRows, localCapacity));
                    if (activeProbe.mutable()) {
                        prepared.rehashCode = prepared.compile(activeProbe.varchar() ? QuestVarchar.rehash(valueBytes) : activeProbe.orderedNative() ? QuestOrderedMap.rehash() : QuestMapMutation.rehash(activeProbe, valueBytes));
                        prepared.mergeCode = prepared.compile(activeProbe.varchar() ? QuestVarchar.entries(activeProbe, updates, valueBytes) : activeProbe.orderedNative() ? QuestOrderedMap.entries(activeProbe, updates, valueBytes) : QuestMapMutation.entries(activeProbe, updates, valueBytes));
                    }
                }
                if (partitionOwner && localCapacity == 0 && activeProbe != null && activeProbe.varchar()
                        && valueBytes == 8 && updates.updates().equals(List.of(new ScalarUpdate(new Pipeline.Count(), 0)))
                        && Boolean.getBoolean("query.codegen.sharedVarchar")) {
                    prepared.sharedCode = prepared.compile(QuestSharedVarchar.program());
                    prepared.sharedMaps = prepared.own(new SharedVarcharMaps(), 0);
                }
                boolean deferred = activeProbe != null && activeProbe.mutable()
                        && updates.updates().stream().anyMatch(Update::deferredMerge);
                if (deferred) {
                    prepared.unionPrepareCode = prepared.compile(DeferredDistinct.prepare());
                    prepared.unionZeroCode = prepared.compile(DeferredDistinct.zero());
                    prepared.unionCode = prepared.compile(DeferredDistinct.union());
                }
                if (deferred || updates.updates().stream().anyMatch(Update::finalizesResult)) {
                    Builder finish = new Builder();
                    QuestMapScan.emit(finish, activeProbe, COLUMNS, (entryBuilder, entry) -> {
                        updates.finish(entryBuilder, activeProbe.valueAddress(entry), deferred);
                    });
                    finish.exit(0);
                    prepared.finalizeCode = prepared.compile(new Program(finish.block()));
                }
                if (prepared.localCapacity > 0) {
                    prepared.flushCode = prepared.defer(() -> QuestPartitions.flush(activeProbe, valueBytes));
                    prepared.partitionMergeCode = prepared.defer(() -> QuestPartitions.merge(activeProbe, updates, valueBytes));
                    if (prepared.inputPartitionRowBytes > 0 && prepared.inputPartitionRowBytes < prepared.partitionRowBytes) {
                        prepared.inputPartitionCode = prepared.defer(() -> QuestPartitionInputs.rows(activeProbe, updates));
                        prepared.inputPartitionMergeCode = prepared.defer(() -> QuestPartitionInputs.merge(activeProbe, updates, valueBytes, prefetchRows));
                    }
                }
                return prepared;
            } catch (Throwable failure) {
                prepared.closeAfterFailure(failure);
                throw failure;
            }
        }

        private int localCapacity(QuestMapProbe activeProbe) {
            if (activeProbe == null || !activeProbe.mutable() || activeProbe.variable()) return 0;
            if (allocates && (activeProbe.varchar()
                    || updates.updates().stream().anyMatch(update -> update.allocates() && !(update instanceof VarcharExtremum))
                    || QuestPartitionInputs.rowBytes(activeProbe, updates) >= activeProbe.keyBytes() + valueBytes)) return 0;
            String enabled = System.getProperty("query.codegen.bounded");
            // Fixed-width states benefit from bounded working sets. VARCHAR's extra partial
            // groups can cost more than they save; keep it an explicit experiment for now.
            if (enabled == null ? activeProbe.varchar() : !Boolean.parseBoolean(enabled)) return 0;
            int capacity = Integer.getInteger("query.codegen.localCapacity",
                    Integer.highestOneBit((int) (256 * 1024 / (2 * (activeProbe.keyBytes() + valueBytes)))));
            if (capacity < 64 || capacity > 65536 || (capacity & (capacity - 1)) != 0)
                throw new IllegalArgumentException("Local aggregation capacity must be a power of two in [64,65536]");
            return capacity;
        }

        Program program() {
            return program;
        }
    }

    /** Factory code owner. Optional kernels compile on first use; all slots must join before close. */
    public static final class Prepared implements AutoCloseable {
        final Binding binding;
        final boolean finalizeResults;
        CompiledKernel code;
        CompiledKernel mapCode;
        CompiledKernel fusedFilterCode;
        final QuestMapProbe probe;
        CompiledKernel rehashCode;
        CompiledKernel mergeCode;
        private final Backend backend;
        private final Path artifacts;
        CompiledKernel topKCode;
        CompiledKernel partitionSelectCode;
        CompiledKernel rankStateCode, rankInputCode, lateStateCode, lateInputCode;
        CompiledKernel splitStateCode, splitInputCode;
        int partitionTopKLimit;
        long partitionTopKOffset;
        int topKOrder = -1;
        int topKCapacityClass = -1;
        CompiledKernel finalizeCode;
        CompiledKernel longSortCode;
        CompiledKernel rowLimitCode;
        CompiledKernel flushCode;
        CompiledKernel partitionMergeCode;
        CompiledKernel inputPartitionCode;
        CompiledKernel inputPartitionMergeCode;
        CompiledKernel unionPrepareCode;
        CompiledKernel unionCode;
        CompiledKernel unionZeroCode;
        CompiledKernel sharedCode;
        SharedVarcharMaps sharedMaps;
        final int localCapacity;
        final long partitionRowBytes;
        final long inputPartitionRowBytes;
        ResolvedFilter filter;
        boolean closed;

        private final ArrayList<AutoCloseable> resources = new ArrayList<>();
        private long compilationNanos;

        private Prepared(Binding binding, QuestMapProbe probe, Backend backend, Path artifacts, int localCapacity) {
            this.localCapacity = localCapacity;
            this.partitionRowBytes = probe != null ? probe.keyBytes() + binding.valueBytes : 0;
            this.inputPartitionRowBytes = localCapacity > 0 && !probe.varchar()
                    ? QuestPartitionInputs.rowBytes(probe, binding.updates) : 0;
            this.backend = backend;
            this.artifacts = artifacts;
            this.binding = binding;
            this.finalizeResults = binding.updates.updates().stream().anyMatch(Update::finalizesResult);
            this.probe = probe;
        }

        private CompiledKernel compile(Program program) throws IOException {
            return own(backend.compile(program, artifacts));
        }

        private CompiledKernel defer(java.util.function.Supplier<Program> program) {
            return own(CompiledKernel.defer(backend, program, artifacts));
        }

        private CompiledKernel own(CompiledKernel kernel) {
            return own(kernel, 0);
        }

        private <T extends AutoCloseable> T own(T resource, long nanos) {
            try {
                resources.add(resource);
            } catch (Throwable failure) {
                try { resource.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            compilationNanos += nanos;
            return resource;
        }

        private void closeAfterFailure(Throwable failure) {
            try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            long totalCompilationNanos = compilationNanos();
            Throwable failure = null;
            for (int i = resources.size() - 1; i >= 0; i--) {
                try { resources.get(i).close(); }
                catch (Throwable cleanup) { failure = io.questdb.std.Misc.foldCleanupFailure(failure, cleanup); }
            }
            resources.clear();
            compilationNanos = totalCompilationNanos;
            io.questdb.cairo.CairoException.rethrowCleanupFailure(failure);
        }

        ObjList<Function> outputFunctions(ObjList<Function> functions) {
            if (finalizeCode == null) return functions;
            var result = new ObjList<Function>(functions.size());
            for (int i = 0; i < functions.size(); i++) {
                Function function = functions.getQuick(i);
                function = AggregateUpdates.outputFunction(function);
                result.add(function);
            }
            return result;
        }

        void prepareFilter(ColumnTypes inputs, Function function) throws IOException {
            prepareFilter(inputs, function, false);
        }

        void prepareFilter(ColumnTypes inputs, Function function, boolean fuse) throws IOException {
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            if (backend != null && filter == null) {
                var compiled = ResolvedFilter.compile(inputs, function, backend, artifacts);
                if (compiled != null) filter = own(compiled, compiled.compilationNanos());
                if (fuse && filter != null && filter.fusedPlan() != null && probe != null && probe.mutable()
                        && sharedMaps == null && binding.prefetchRows > 0 && binding.prefetchRows <= 64) {
                    fusedFilterCode = compile(mapProgram(binding.updates, probe, binding.valueBytes,
                            binding.prefetchRows, localCapacity, filter.fusedPlan()));
                }
            }
        }

        boolean hasCompiledFilter() { return filter != null; }
        boolean needsWorkerAllocators() { return probe != null || localCapacity > 0; }

        boolean prepareRowLimit() throws IOException {
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            if (backend == null || probe == null || !probe.mutable()) return false;
            if (rowLimitCode == null) rowLimitCode = compile(QuestRowLimit.program(probe));
            return true;
        }

        boolean prepareLongSort(Function function) throws IOException {
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            if (backend == null || probe == null || !probe.mutable() || resultOffset(function) < 0) return false;
            if (longSortCode == null) longSortCode = compile(QuestLongSort.program(probe));
            return true;
        }

        boolean supportsLongTopK(Function function) {
            return backend != null && probe != null && probe.mutable() && resultOffset(function) >= 0;
        }

        void prepareLongTopK(Function function) throws IOException {
            prepareLongTopK(function, -1);
        }

        void prepareLongTopK(Function function, int order) throws IOException {
            prepareLongTopK(function, order, 0);
        }

        void prepareLongTopK(Function function, int order, int limit) throws IOException {
            if (limit < 0) throw new IllegalArgumentException("Invalid Top-K limit");
            if (order < -1 || order > 1) throw new IllegalArgumentException("Invalid Top-K order");
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            long offset = resultOffset(function);
            if (topKCode == null && backend != null && probe != null && probe.mutable() && offset >= 0) {
                int capacityClass = QuestLongTopK.capacityClass(limit);
                topKCode = compile(QuestLongTopK.program(probe, order, capacityClass));
                topKOrder = order;
                topKCapacityClass = capacityClass;
                if (localCapacity > 0 && !probe.varchar() && !probe.variable()
                        && limit > 0 && capacityClass == 0 && order >= 0
                        && binding.updates.updates().stream().noneMatch(update -> update.offset() == offset
                        && (update.finalizesResult() || update.deferredMerge()))) {
                    // This direct Top-K consumer ranks exact LONG state. Independent string
                    // extrema can be filled from retained partition records after selection.
                    var delayed = binding.updates.updates().stream().filter(update -> update instanceof VarcharExtremum).toList();
                    if (!delayed.isEmpty()) {
                        var early = new AggregateUpdates(binding.updates.inputs(), binding.updates.updates().stream()
                                .filter(update -> !(update instanceof VarcharExtremum)).toList());
                        var late = new AggregateUpdates(binding.updates.inputs(), delayed);
                        rankStateCode = defer(() -> QuestPartitions.merge(probe, early, binding.valueBytes, binding.updates));
                        lateStateCode = defer(() -> QuestPartitions.replay(probe, late, binding.valueBytes));
                        if (inputPartitionCode != null) {
                            rankInputCode = defer(() -> QuestPartitionInputs.merge(probe, binding.updates, early, binding.valueBytes, binding.prefetchRows));
                            lateInputCode = defer(() -> QuestPartitionInputs.replay(probe, binding.updates, late));
                        }
                    }
                    partitionSelectCode = defer(() -> QuestPartitions.selected(probe, binding.valueBytes));
                    splitStateCode = defer(() -> QuestRepartition.rows(probe, partitionRowBytes));
                    if (inputPartitionCode != null) splitInputCode = defer(() -> QuestRepartition.rows(probe, inputPartitionRowBytes));
                    partitionTopKLimit = limit;
                    partitionTopKOffset = offset;
                }
            }
        }

        long resultOffset(Function function) {
            if (function instanceof GroupByFunction aggregate) {
                for (var reader : binding.readers) {
                    if (reader.type() == ColumnType.LONG && reader.implementation() == function.getClass() && reader.index() == aggregate.getValueIndex()) return reader.offset();
                }
            }
            return -1;
        }

        public long compilationNanos() {
            long total = compilationNanos;
            for (AutoCloseable resource : resources) {
                if (resource instanceof CompiledKernel kernel) total += kernel.compilationNanos();
            }
            return total;
        }

        public AggregateSlot newSlot() {
            return newSlot(null);
        }

        public AggregateSlot newSlot(GroupByAllocator allocator) {
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            return new AggregateSlot(this, allocator);
        }
    }

}
