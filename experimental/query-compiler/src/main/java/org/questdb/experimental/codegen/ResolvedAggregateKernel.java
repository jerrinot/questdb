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
import static org.questdb.experimental.codegen.MapKernelState.*;

/** Compiles resolved updates and fused probes over existing QuestDB map state. */
public final class ResolvedAggregateKernel {
    private ResolvedAggregateKernel() {
    }

    /** Binds resolved functions; cursor constants borrow their ordinary initialization lifecycle. */
    public static Binding bind(ColumnTypes inputTypes, ColumnTypes valueTypes, ObjList<GroupByFunction> functions) {
        return bind(inputTypes, valueTypes, functions, null);
    }

    static Binding bind(ColumnTypes inputTypes, ColumnTypes valueTypes, ObjList<GroupByFunction> functions, QuestMapProbe probe) {
        return bind(inputTypes, valueTypes, functions, probe, CompilerOptions.DEFAULT);
    }

    static Binding bind(ColumnTypes inputTypes, ColumnTypes valueTypes, ObjList<GroupByFunction> functions, QuestMapProbe probe, CompilerOptions options) {
        var layout = new FlyweightPackedMapValue(valueTypes);
        // The ordinary planner represents scalar DISTINCT as key-only grouping plus
        // cardinality metadata. Its empty state needs probing/merge but no update slots.
        AggregateUpdates resolved = functions.size() == 0 && valueTypes.getColumnCount() == 0 && probe != null
                ? new AggregateUpdates(new ResolvedVarchar.Binding(new int[0], List.of(), new int[0], new byte[0]), List.of())
                : AggregateUpdates.bind(valueTypes, functions, layout::getOffset, true, new ResolvedInputs(inputTypes, new ArrayList<>(), options), probe != null && (!probe.ordered() || probe.orderedNative()));
        if (resolved == null) return null;
        var columns = resolved.columns();
        var updates = resolved.updates();
        boolean allocates = updates.stream().anyMatch(Update::allocates);
        Builder b = new Builder();
        Var batches = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        var addresses = resolved.loadFrameInputs(b);
        Expr cardinalityAddress = at(COLUMNS, resolved.cardinalityOffset());
        emitUpdates(b, batches, addresses, resolved, cardinalityAddress, allocates);
        b.exit(0);
        int prefetchRows = options.prefetchRows();
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
            readers.add(new ResultRead(function.getClass(), AggregateSpecs.outputFunction(function).getClass(),
                    function.getValueIndex(), layout.getOffset(function.getValueIndex()), function.getType(), updates.get(i).resultAccess()));
        }
        return new Binding(new Program(b.block()), allocates, probe, prefetchRows, countSumReference, resolved, layout.getSizeInBytes(), valueTypes.getColumnCount(), List.copyOf(readers), options);
    }

    private static Program mapProgram(AggregateUpdates updates, QuestMapProbe probe, long valueBytes, int prefetchRows, int localCapacity) {
        return mapProgram(updates, probe, valueBytes, prefetchRows, localCapacity, null);
    }

    private static Program mapProgram(AggregateUpdates updates, QuestMapProbe probe, long valueBytes,
                                      int prefetchRows, int localCapacity, ScanPlan filter) {
        return mapProgram(updates, probe, valueBytes, prefetchRows, localCapacity, filter, false);
    }

    private static Program mapProgram(AggregateUpdates updates, QuestMapProbe probe, long valueBytes,
                                      int prefetchRows, int localCapacity, ScanPlan filter, boolean stagedDistinct) {
        Builder b = new Builder();
        var addresses = updates.loadFrameInputs(b);
        Expr cardinalityAddress = at(COLUMNS, updates.cardinalityOffset());
        Var rowIds = filter == null ? b.let(new Load(MemoryType.ADDRESS, at(STATE, SELECTION))) : null;
        var filterAddresses = new ArrayList<Var>();
        Var accepted = filter != null ? b.let(i64(0)) : null;
        if (filter != null) {
            Var pointers = b.let(new Load(MemoryType.ADDRESS, at(STATE, SELECTION)));
            for (int i = 0; i < filter.input().size(); i++) {
                filterAddresses.add(b.let(new Load(MemoryType.ADDRESS, at(pointers, 8L * i))));
            }
        }
        Var index = b.let(load(STATE));
        Var batchEnd = b.let(probe.mutable() && !probe.variable() ? new Select(eq(load(at(STATE, SHARD_MASK)), i64(0)), index, ROWS) : ROWS);
        if (prefetchRows > 0) {
            emitPrefetched(b, index, batchEnd, rowIds, addresses, updates, cardinalityAddress, probe, valueBytes, prefetchRows, localCapacity, filter, filterAddresses, accepted, stagedDistinct);
        } else b.loop(binary(Op.LT, index, ROWS), loop -> {
            checkLocalBound(loop, probe, index, localCapacity);
            reserveBatch(loop, probe, index, batchEnd);
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var rowIndex = loop.let(index);
            loop.when(ne(rowIds, nil()), filtered -> filtered.assign(rowIndex,
                    load(new Offset(rowIds, binary(Op.MUL, index, i64(8))))));
            if (probe.variable()) { loop.store(at(STATE, VariableKey.STAGING_USED), i64(0)); loop.store(at(STATE, RESUME_ROW), index); }
            Var first = updates.firstRowFlag(loop);
            Var destination = probe.find(loop, probe.loadFrameKey(loop, rowIndex, at(STATE, prefetchScratch(probe) + 24)), index,
                    probe.mutable() ? load(at(STATE, SHARD_MASK)) : null, null, updates.initializer(first));
            updates.emitFrame(loop, rowIndex, addresses, destination, cardinalityAddress, first);
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(filter == null ? b.block() : saveAcceptedRows(b.block(), accepted));
    }

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
    // Copy-only VARCHAR retries retain scratch; growth retries rebuild all candidates.
    static long prefetchScratch(QuestMapProbe probe) {
        return probe.controlBytes() + (long) GroupByMapFragment.NUM_SHARDS * probe.viewBytes();
    }

    private static void emitPrefetched(Builder b, Var index, Var batchEnd, Var rowIds,
                                      AggregateUpdates.FrameInputs addresses, AggregateUpdates updates, Expr cardinalityAddress,
                                      QuestMapProbe probe, long valueBytes, int batchRows, int localCapacity,
                                      ScanPlan filter, List<Var> filterAddresses, Var accepted, boolean stagedDistinct) {
        boolean stagedState = stagedDistinct || (probe.varchar() && updates.updates().stream().anyMatch(Update::allocates));
        boolean staged = probe.orderedNative();
        Var resumeEnd = probe.varchar() && probe.mutable() ? b.let(load(at(STATE, VarcharKey.BATCH_END))) : null;
        // Only NEED_COPY below republishes a continuation. Growth, errors and completion
        // leave it invalid, so no candidate address can survive a map relocation.
        if (resumeEnd != null) b.store(at(STATE, VarcharKey.BATCH_END), i64(0));
        Var batches = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), batch -> {
            batch.when(eq(binary(Op.AND, batches, i64(1024 / batchRows - 1)), i64(0)), Builder::poll);
            batch.assign(batches, add(batches, i64(1)));
            Var limit = batch.let(i64(0));
            Var batchStart = filter != null ? batch.let(index) : null;
            Var remaining = filter != null ? batch.let(i64(0)) : null;
            Var scratch = batch.let(at(STATE, prefetchScratch(probe)));
            Builder prepareBatch = batch.fork();
            checkLocalBound(prepareBatch, probe, index, localCapacity);
            reserveBatch(prepareBatch, probe, index, batchEnd);
            prepareBatch.assign(limit, new Select(binary(Op.LT, add(index, i64(batchRows)), batchEnd),
                    add(index, i64(batchRows)), batchEnd));
            if (probe.variable()) { prepareBatch.store(at(STATE, VariableKey.STAGING_USED), i64(0)); prepareBatch.store(at(STATE, RESUME_ROW), index); }
            Var mask = filter != null ? ScanLowering.mask(filter, prepareBatch, filterAddresses, batchStart, limit, batchRows) : null;
            if (filter != null) prepareBatch.assign(remaining, mask);
            Var position = prepareBatch.let(index);
            int keyOffset = probe.orderedNative() ? 40 : 24;
            if (staged) {
                prepareBatch.loop(filter == null ? binary(Op.LT, position, limit) : ne(remaining, i64(0)), stage -> {
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
                prepareBatch.assign(position, index);
                if (filter != null) prepareBatch.assign(remaining, mask);
                prepareBatch.assign(scratch, at(STATE, prefetchScratch(probe)));
            }
            prepareBatch.loop(filter == null ? binary(Op.LT, position, limit) : ne(remaining, i64(0)), prepare -> {
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
                // Prefetch outer entries before dereferencing their allocated state. For large
                // numeric maps, prefetch set headers in their own pass before reading capacities.
                // Only hints cross stages: consumption reloads all mutable pointers after growth.
                for (int phase = 0, phases = stagedDistinct ? 2 : 1; phase < phases; phase++) {
                    boolean headers = stagedDistinct && phase == 0;
                    prepareBatch.assign(position, index);
                    if (filter != null) prepareBatch.assign(remaining, mask);
                    prepareBatch.assign(scratch, at(STATE, prefetchScratch(probe)));
                    prepareBatch.loop(filter == null ? binary(Op.LT, position, limit) : ne(remaining, i64(0)), hints -> {
                        if (filter != null) hints.assign(position, add(batchStart, new TrailingZeros(remaining)));
                        Var row = hints.let(load(scratch));
                        Var entry = hints.let(new Load(MemoryType.ADDRESS, at(scratch, 16)));
                        if (probe.varchar()) {
                            hints.when(ne(load(entry), i64(0)), occupied ->
                                    updates.prefetch(occupied, row, addresses, at(entry, probe.keyBytes())));
                        } else {
                            hints.when(ne(entry, nil()), candidate -> {
                                Expr occupied = binary(Op.EITHER, eq(load(at(scratch, 8)), i64(0)),
                                        ne(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, entry), i64(0)));
                                candidate.when(occupied, present -> {
                                    if (headers) updates.prefetchHeaders(present, at(entry, probe.keyBytes()));
                                    else updates.prefetch(present, row, addresses, at(entry, probe.keyBytes()));
                                });
                            });
                        }
                        hints.assign(position, add(position, i64(1)));
                        if (filter != null) hints.assign(remaining, binary(Op.AND, remaining, binary(Op.SUB, remaining, i64(1))));
                        hints.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                    });
                }
            }
            prepareBatch.assign(scratch, at(STATE, prefetchScratch(probe)));
            if (filter != null) prepareBatch.assign(remaining, mask);
            if (resumeEnd == null) {
                batch.append(prepareBatch.block());
            } else {
                batch.when(eq(resumeEnd, i64(0)), fresh -> fresh.append(prepareBatch.block()));
                batch.when(ne(resumeEnd, i64(0)), resume -> {
                    resume.assign(limit, resumeEnd);
                    resume.assign(batchEnd, load(at(STATE, VarcharKey.BATCH_CAPACITY_END)));
                    resume.assign(scratch, new Load(MemoryType.ADDRESS, at(STATE, VarcharKey.BATCH_SCRATCH)));
                    if (filter != null) {
                        resume.assign(batchStart, load(at(STATE, VarcharKey.BATCH_START)));
                        resume.assign(remaining, load(at(STATE, VarcharKey.BATCH_MASK)));
                    }
                    resume.assign(resumeEnd, i64(0));
                });
            }
            batch.loop(filter == null ? binary(Op.LT, index, limit) : ne(remaining, i64(0)), consume -> {
                if (filter != null) consume.assign(index, add(batchStart, new TrailingZeros(remaining)));
                Var row = consume.let(load(scratch));
                Var key = consume.let(new Load(probe.stagedKeyType(), at(scratch, 8)));
                Var entry = consume.let(new Load(MemoryType.ADDRESS, at(scratch, 16)));
                Var first = updates.firstRowFlag(consume);
                Builder lookup = consume.fork();
                Var destination = staged
                        ? probe.finishOrdered(lookup, key, entry, lookup.let(load(at(scratch, 24))),
                                lookup.let(load(at(scratch, 32))), index, updates.initializer(first))
                        : probe.finish(lookup, key, entry, index, updates.initializer(first));
                consume.append(resumeEnd == null ? lookup.block()
                        : saveCopyBatch(lookup.block(), limit, batchEnd, scratch, batchStart, remaining));
                updates.emitFrame(consume, row, addresses, destination, cardinalityAddress, first);
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

    /** Save only before the uncommitted row's copy request, never on the hot hit path. */
    private static Block saveCopyBatch(Block block, Var limit, Var capacityEnd, Var scratch, Var start, Var remaining) {
        var statements = new ArrayList<Statement>();
        for (Statement statement : block.statements()) {
            if (statement instanceof Return exit && exit.value().equals(i64(NEED_COPY))) {
                statements.add(new Store(at(STATE, VarcharKey.BATCH_END), limit));
                statements.add(new Store(at(STATE, VarcharKey.BATCH_CAPACITY_END), capacityEnd));
                statements.add(new Store(MemoryType.ADDRESS, at(STATE, VarcharKey.BATCH_SCRATCH), scratch));
                if (start != null) {
                    statements.add(new Store(at(STATE, VarcharKey.BATCH_START), start));
                    statements.add(new Store(at(STATE, VarcharKey.BATCH_MASK), remaining));
                }
            }
            statements.add(statement instanceof If branch
                    ? new If(branch.condition(), saveCopyBatch(branch.body(), limit, capacityEnd, scratch, start, remaining))
                    : statement instanceof While loop
                    ? new While(loop.condition(), saveCopyBatch(loop.body(), limit, capacityEnd, scratch, start, remaining)) : statement);
        }
        return new Block(statements);
    }

    private static void checkLocalBound(Builder b, QuestMapProbe probe, Var index, int capacity) {
        if (capacity == 0) return;
        Expr size = load(at(STATE, probe.controlBytes() + NativeMapView.SIZE));
        // Reaching the limit does not imply another group will arrive. A map at
        // exactly capacity can keep combining indefinitely without allocating.
        Expr fullMap = binary(Op.LT, i64(capacity), size);
        if (probe.orderedNative()) {
            Expr used = binary(Op.SUB, load(at(STATE, probe.controlBytes() + NativeMapView.Ordered.APPEND)),
                    load(at(STATE, probe.controlBytes())));
            Expr slots = add(load(at(STATE, probe.controlBytes() + NativeMapView.MASK)), i64(1));
            Expr footprint = add(used, binary(Op.MUL, slots, i64(Long.BYTES)));
            // A few oversized, repeated keys must keep combining instead of flushing every batch.
            fullMap = binary(Op.EITHER, fullMap, binary(Op.BOTH, binary(Op.GE, size, i64(64)),
                    binary(Op.LT, i64(CacheSizing.LOCAL_AGGREGATE_BYTES), footprint)));
        }
        b.when(binary(Op.BOTH, eq(load(at(STATE, SHARD_MASK)), i64(0)), fullMap), full -> {
            full.store(at(STATE, RESUME_ROW), index);
            full.exit(NEED_FLUSH);
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
                grow.store(at(STATE, SHARD), i64(0));
                grow.store(at(STATE, RESUME_ROW), index);
                grow.exit(NEED_CAPACITY);
            });
            boundary.assign(batchEnd, add(index, batchRows));
        });
    }

    private static void emitUpdates(Builder b, Var batches, AggregateUpdates.FrameInputs addresses,
                                    AggregateUpdates updates, Expr cardinalityAddress, boolean allocates) {
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            if (allocates) loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var entry = loop.let(new Offset(batches, binary(Op.MUL, index, i64(8))));
            Var encoded = loop.let(load(entry));
            Var rowIndex = loop.let(binary(Op.AND, binary(Op.USHR, encoded, i64(Map.BATCH_ROW_INDEX_SHIFT)), i64(Map.BATCH_ROW_INDEX_MASK)));
            Var destination = loop.let(new Offset(STATE, binary(Op.AND, encoded, i64(Map.BATCH_OFFSET_MASK))));
            updates.emitEmptyFrame(loop, rowIndex, addresses, destination, cardinalityAddress,
                    binary(Op.LT, encoded, i64(0)));
            loop.assign(index, add(index, i64(1)));
        });
    }

    private record ResultRead(Class<?> implementation, Class<?> outputImplementation, int index, long offset, int type,
                              AggregateUpdates.ResultAccess access) { }

    public static final class Binding {
        private final Program program;
        final ResolvedVarchar.Binding inputs;
        final boolean allocates;
        final QuestMapProbe probe;
        final int prefetchRows;
        private final boolean countSumReference;
        final AggregateUpdates updates;
        final int valueColumnCount;
        final long valueBytes;
        private final List<ResultRead> readers;
        final CompilerOptions options;

        private Binding(Program program, boolean allocates, QuestMapProbe probe, int prefetchRows, boolean countSumReference, AggregateUpdates updates, long valueBytes, int valueColumnCount, List<ResultRead> readers, CompilerOptions options) {
            this.options = options;
            this.readers = readers;
            this.valueColumnCount = valueColumnCount;
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
            Prepared prepared = new Prepared(this, activeProbe, backend.withOptions(options), artifacts, localCapacity);
            try {
                prepared.dictionaryAggregate = DictionaryAggregate.bind(prepared);
                prepared.code = prepared.compileFallback(() -> program);
                if (activeProbe != null) {
                    prepared.mapCode = prepared.compileFallback(() -> mapProgram(updates, activeProbe, valueBytes, prefetchRows, localCapacity));
                    if (activeProbe.mutable() && !activeProbe.ordered() && !activeProbe.varchar() && prefetchRows > 0 && localCapacity == 0
                            && updates.updates().stream().anyMatch(u -> u instanceof AggregateUpdates.DistinctUpdate)
                            && updates.updates().stream().allMatch(u -> !u.allocates() || u instanceof AggregateUpdates.DistinctUpdate)) {
                        prepared.stagedMapCode = prepared.defer(() -> mapProgram(updates, activeProbe, valueBytes,
                                prefetchRows, localCapacity, null, true));
                        prepared.mapChoice = new AdaptiveKernel(prepared.mapCode, prepared.stagedMapCode, System::nanoTime);
                    }
                    if (activeProbe.mutable()) {
                        prepared.rehashCode = prepared.compileFallback(() -> activeProbe.varchar() ? QuestVarchar.rehash(valueBytes) : activeProbe.orderedNative() ? QuestOrderedMap.rehash() : QuestMapMutation.rehash(activeProbe, valueBytes));
                        prepared.mergeCode = prepared.compileFallback(() -> activeProbe.varchar() ? QuestVarchar.entries(activeProbe, updates, valueBytes) : activeProbe.orderedNative() ? QuestOrderedMap.entries(activeProbe, updates, valueBytes, options) : QuestMapMutation.entries(activeProbe, updates, valueBytes));
                    }
                }
                if (partitionOwner && localCapacity == 0 && activeProbe != null && activeProbe.varchar() && activeProbe.varcharKey() == null
                        && valueBytes == 8 && updates.updates().equals(List.of(new ScalarUpdate(new Pipeline.Count(), 0)))
                        && options.sharedVarchar()) {
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
                if (activeProbe != null && activeProbe.mutable()
                        && (deferred || updates.updates().stream().anyMatch(Update::finalizesResult))) {
                    Builder finish = new Builder();
                    QuestMapScan.emit(finish, activeProbe, COLUMNS, (entryBuilder, entry) -> {
                        updates.finish(entryBuilder, activeProbe.valueAddress(entry), deferred);
                    });
                    finish.exit(0);
                    prepared.finalizeCode = prepared.compile(new Program(finish.block()));
                }
                if (prepared.localCapacity > 0) {
                    if (valueBytes == 0 && updates.updates().isEmpty() && !activeProbe.variable() && !activeProbe.varchar()) {
                        prepared.keyFilterCode = prepared.defer(() -> QuestKeyFilter.program(activeProbe, localCapacity, null));
                    }
                    prepared.flushCode = prepared.defer(() -> QuestPartitions.flush(activeProbe, valueBytes));
                    prepared.partitionMergeCode = prepared.defer(() -> QuestPartitions.merge(activeProbe, updates, valueBytes, prefetchRows));
                    if (activeProbe.orderedNative() && !activeProbe.variable() && prefetchRows > 0) {
                        prepared.stagedPartitionMergeCode = prepared.defer(() -> QuestPartitions.mergeStaged(
                                activeProbe, updates, valueBytes, prefetchRows, null, false));
                    }
                    if (!updates.updates().isEmpty() && prepared.inputPartitionRowBytes > 0 && (activeProbe.variable()
                            ? QuestPartitionInputs.valueBytes(updates) < valueBytes
                            : prepared.inputPartitionRowBytes <= prepared.partitionRowBytes)) {
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
            if (activeProbe == null || !activeProbe.mutable() || (activeProbe.variable() && allocates)) return 0;
            if (allocates && (activeProbe.varchar()
                    || updates.updates().stream().anyMatch(update -> update.allocates() && !(update instanceof VarcharExtremum))
                    || QuestPartitionInputs.rowBytes(activeProbe, updates) >= activeProbe.keyBytes() + valueBytes)) return 0;
            Boolean enabled = options.bounded();
            // The direct VARCHAR map still uses its separate pointer-based partition layout.
            // OrderedMap variable keys use packed records and share the default bounded path.
            if (enabled == null ? activeProbe.varchar() : !enabled) return 0;
            int keyBytes = activeProbe.variable() ? activeProbe.variableKey().minimumBytes() : activeProbe.keyBytes();
            long entryBytes = (keyBytes + valueBytes + 7) & ~7L;
            // OrderedMap keeps entries densely packed and has a separate offset
            // table. This count is an upper bound; the generated byte check uses
            // its actual table capacity and variable entry sizes. Keep the
            // existing inline-map limit: enlarging its sparse slot array can
            // displace useful cache data even when most rows hit one hot key.
            long budget = activeProbe.orderedNative() ? CacheSizing.LOCAL_AGGREGATE_BYTES : 256 * 1024;
            long groupBytes = activeProbe.orderedNative() ? entryBytes + Long.BYTES : 2 * (keyBytes + valueBytes);
            int capacity = options.localCapacity() != null ? options.localCapacity()
                    : Math.max(64, Integer.highestOneBit((int) Math.min(65536, budget / groupBytes)));
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
        DictionaryAggregate dictionaryAggregate;
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
        int topKType = ColumnType.UNDEFINED;
        int longSortType = ColumnType.UNDEFINED;
        int topKOrder = -1;
        int topKCapacityClass = -1;
        CompiledKernel finalizeCode;
        CompiledKernel longSortCode;
        CompiledKernel rowLimitCode;
        CompiledKernel flushCode;
        CompiledKernel partitionMergeCode;
        CompiledKernel stagedPartitionMergeCode;
        CompiledKernel inputPartitionCode;
        CompiledKernel inputPartitionMergeCode;
        AdaptiveKernel mapChoice;
        AdaptiveKernel filterChoice;
        CompiledKernel stagedMapCode;
        CompiledKernel stagedFilterCode;
        CompiledKernel keyFilterCode;
        CompiledKernel filteredKeyFilterCode;
        CompiledKernel unionPrepareCode;
        CompiledKernel unionCode;
        CompiledKernel unionZeroCode;
        CompiledKernel sharedCode;
        SharedVarcharMaps sharedMaps;
        final int localCapacity;
        final long partitionRowBytes;
        final long inputPartitionRowBytes;
        ResolvedFilter filter;
        GroupAdmission groupAdmission;
        boolean closed;

        private final ArrayList<AutoCloseable> resources = new ArrayList<>();
        private long compilationNanos;

        private Prepared(Binding binding, QuestMapProbe probe, Backend backend, Path artifacts, int localCapacity) {
            this.localCapacity = localCapacity;
            this.partitionRowBytes = probe != null && !probe.variable() ? probe.keyBytes() + binding.valueBytes : 0;
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

        private CompiledKernel compileFallback(java.util.function.Supplier<Program> program) throws IOException {
            // Array admission needs the cursor snapshot. Keep its alternative paths lazy.
            return dictionaryAggregate != null ? defer(program) : compile(program.get());
        }

        CompiledKernel defer(java.util.function.Supplier<Program> program) {
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
            if (probe != null && probe.fixedKey() != null) probe.fixedKey().clearExecution();
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
                function = AggregateSpecs.outputFunction(function);
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
                    if (stagedMapCode != null) {
                        stagedFilterCode = defer(() -> mapProgram(binding.updates, probe, binding.valueBytes,
                                binding.prefetchRows, localCapacity, filter.fusedPlan(), true));
                        filterChoice = new AdaptiveKernel(fusedFilterCode, stagedFilterCode, System::nanoTime);
                    }
                    if (keyFilterCode != null) {
                        filteredKeyFilterCode = defer(() -> QuestKeyFilter.program(probe, localCapacity, filter.fusedPlan()));
                    }
                }
            }
        }

        boolean hasCompiledFilter() { return filter != null; }
        boolean needsWorkerAllocators() { return probe != null || localCapacity > 0; }

        boolean prepareGroupLimit(long requiredGroups) throws IOException {
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            if (backend == null) return false;
            if (groupAdmission == null) {
                var admission = GroupAdmission.compile(probe, requiredGroups, backend, artifacts);
                if (admission != null) groupAdmission = own(admission, admission.compilationNanos());
            }
            return groupAdmission != null && groupAdmission.supports(requiredGroups);
        }

        boolean prepareRowLimit() throws IOException {
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            if (backend == null || probe == null || !probe.mutable()) return false;
            if (rowLimitCode == null) rowLimitCode = compile(QuestRowLimit.program(probe));
            return true;
        }

        boolean prepareLongSort(Function function) throws IOException {
            if (closed) throw new IllegalStateException("Aggregate code is closed");
            if (backend == null || probe == null || !probe.mutable() || resultOffset(function) < 0) return false;
            if (longSortCode == null) {
                longSortCode = compile(QuestLongSort.program(probe, function.getType()));
                longSortType = function.getType();
            }
            return longSortType == function.getType();
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
                topKCode = compile(QuestLongTopK.program(probe, order, capacityClass, function.getType(), binding.options));
                topKType = function.getType();
                topKOrder = order;
                topKCapacityClass = capacityClass;
                if (localCapacity > 0 && !probe.varchar()
                        && limit > 0 && capacityClass == 0 && order >= 0
                        && binding.updates.updates().stream().noneMatch(update -> update.offset() == offset
                        && (update.finalizesResult() || update.deferredMerge()))) {
                    // This direct Top-K consumer ranks completed numeric state. Independent string
                    // extrema can be filled from retained partition records after selection.
                    var delayed = binding.updates.updates().stream().filter(update -> update instanceof VarcharExtremum).toList();
                    if (!delayed.isEmpty()) {
                        var early = new AggregateUpdates(binding.updates.inputs(), binding.updates.updates().stream()
                                .filter(update -> !(update instanceof VarcharExtremum)).toList());
                        var late = new AggregateUpdates(binding.updates.inputs(), delayed);
                        rankStateCode = defer(() -> QuestPartitions.merge(probe, early, binding.valueBytes, binding.prefetchRows, binding.updates));
                        lateStateCode = defer(() -> QuestPartitions.replay(probe, late, binding.valueBytes));
                        if (inputPartitionCode != null) {
                            rankInputCode = defer(() -> QuestPartitionInputs.merge(probe, binding.updates, early, binding.valueBytes, binding.prefetchRows));
                            lateInputCode = defer(() -> QuestPartitionInputs.replay(probe, binding.updates, late));
                        }
                    }
                    partitionSelectCode = defer(() -> QuestPartitions.selected(probe, binding.valueBytes));
                    if (probe.variable()) {
                        splitStateCode = defer(() -> QuestRepartition.variableRows(binding.valueBytes, false));
                        if (inputPartitionCode != null) splitInputCode = defer(() -> QuestRepartition.variableRows(
                                QuestPartitionInputs.valueBytes(binding.updates), true));
                    } else {
                        splitStateCode = defer(() -> QuestRepartition.rows(probe, partitionRowBytes));
                        if (inputPartitionCode != null) splitInputCode = defer(() -> QuestRepartition.rows(probe, inputPartitionRowBytes));
                    }
                    partitionTopKLimit = limit;
                    partitionTopKOffset = offset;
                }
            }
        }

        Prepared compileConsumer(Binding consumer) throws IOException { return consumer.compile(backend, artifacts); }
        CompiledKernel compileConsumer(Program program) throws IOException { return backend.compile(program, artifacts); }

        long numericResultOffset(Function function) {
            int index = function instanceof GroupByFunction aggregate ? aggregate.getValueIndex()
                    : function instanceof io.questdb.griffin.engine.functions.columns.ColumnFunction column ? column.getColumnIndex() : -1;
            for (var reader : binding.readers) {
                if (reader.access() == AggregateUpdates.ResultAccess.NONE
                        || reader.access() == AggregateUpdates.ResultAccess.FINALIZED && finalizeCode == null) continue;
                if (reader.type() == function.getType() && reader.index() == index
                        && (reader.implementation() == function.getClass()
                        || finalizeCode != null && reader.outputImplementation() == function.getClass())) return reader.offset();
            }
            return -1;
        }

        long resultOffset(Function function) {
            return switch (function.getType()) {
                case ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.DOUBLE -> numericResultOffset(function);
                default -> -1;
            };
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
