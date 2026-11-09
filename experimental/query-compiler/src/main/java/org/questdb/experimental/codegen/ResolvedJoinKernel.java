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
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.NativeMapView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

import static org.questdb.experimental.codegen.Ir.*;

/** Generated typed joins, predicates and updates over engine-owned scalar or keyed state. */
final class ResolvedJoinKernel {
    private static final int LOOKAHEAD_ROWS = 16;
    private static final int LOOKAHEAD_SLOTS = 2 * LOOKAHEAD_ROWS;
    private static final int MATCHED = 8;
    private static final int POSITION = 16;
    private static final int MISSING_KEY = 24;
    private static final int SHARD_MASK = 32;
    private static final int MODE = 40;
    private static final int SYMBOL_BUILD = 48, SYMBOL_KEY = 56;
    private static final long NEED_SYMBOL = 2;
    private static final int BUILDS = 64;
    private static final int LOOKUP = 0;
    private static final int RESUME_LINK = LOOKUP + FrozenHashJoinBuild.LOOKUP_BYTES;
    private static final int MEMBERSHIP = RESUME_LINK + 8;
    private static final int DIRECT = MEMBERSHIP + IntegralMembership.BYTES;
    private static final int HEAP_STRIDE = DIRECT + IntegralJoinIndex.BYTES;
    private static final int KEYS = HEAP_STRIDE + 8;
    private static final int DEPENDENCY_LINKS = KEYS;
    private static final int KEY_BYTES = 56;
    private static final int SYMBOL_CACHE = 24, SYMBOL_COUNT = 32, SYMBOL_INPUT = 40, SYMBOL_RESULT = 48;
    private static final int ARGUMENT_BYTES = HashJoinPayloadSource.NATIVE_COLUMN_BYTES;
    private static final int FACT_BYTES = FrozenHashJoinBuild.INTEGRAL_LOOKUP_BYTES;
    private static int groupKeyOffset(QuestMapProbe probe) {
        return probe.controlBytes() + GroupByMapFragment.NUM_SHARDS * QuestMapProbe.VIEW_BYTES;
    }

    private record Source(int build, int column) {
        static Source bind(int column, int probeColumns, CompiledJoin.Build[] builds, int available) {
            if (column < 0) return null;
            if (column < probeColumns) return new Source(-1, column);
            column -= probeColumns;
            for (int i = 0; i < available; i++) {
                if (column < builds[i].payloadColumnCount()) return new Source(i, column);
                column -= builds[i].payloadColumnCount();
            }
            return null;
        }
    }

    private record Key(MemoryType[] widths, int bytes, QuestMapProbe map, Source[] sources) {
        int producer() {
            int producer = sources[0].build;
            for (Source source : sources) if (source.build != producer) return -1;
            return producer;
        }

        boolean dependent() {
            for (Source source : sources) if (source.build >= 0) return true;
            return false;
        }

        Expr membershipKey(Builder b, Expr key) {
            if (widths.length == 1) return map != null && map.orderedNative() ? new Load(widths[0], key) : key;
            Expr hash = i64(0);
            int offset = 0;
            for (MemoryType width : widths) {
                hash = QuestMapProbe.hashLong128(b, hash, new Load(width, at(key, offset)));
                offset += width.bytes;
            }
            return hash;
        }

        static Key bind(CompiledJoin.Build build, int probeColumns, CompiledJoin.Build[] builds, int available) {
            MemoryType[] widths = new MemoryType[build.keyTypes().length];
            int bytes = 0;
            Source[] sources = new Source[widths.length];
            for (int i = 0; i < widths.length; i++) {
                sources[i] = Source.bind(build.keyColumns()[i], probeColumns, builds, available);
                if (sources[i] == null) return null;
                widths[i] = QuestFixedKey.width(build.keyTypes()[i]);
                if (widths[i] == null) return null;
                bytes += widths[i].bytes;
            }
            long[] nullValues = java.util.Arrays.stream(build.keyTypes()).mapToLong(QuestFixedKey::nullValue).toArray();
            QuestMapProbe map = null;
            int type = build.keyTypes()[0];
            if (widths.length == 1 && io.questdb.cairo.map.Unordered8Map.isSupportedKeyType(type)) {
                map = QuestMapProbe.direct(0, ColumnType.LONG, false);
            } else if (widths.length > 1 || type != ColumnType.INT && type != ColumnType.SYMBOL) {
                var fixed = new QuestFixedKey(null, java.util.List.of(), widths, nullValues, bytes, java.util.List.of());
                map = QuestMapProbe.fixed(fixed, -1, true);
            }
            return new Key(widths, bytes, map, sources);
        }
    }

    private record Layout(CompiledJoin.Build[] builds, Key[] keys, int[] sourceBuilds, int[] sourceColumns,
                          boolean[] counts, boolean subtrees, boolean rowCountsOnly, List<IntSupplier> parameters, Source[] strings, byte[] literals, Facts facts) {
        Layout specialize(Facts facts) { return new Layout(builds, keys, sourceBuilds, sourceColumns, counts, subtrees, rowCountsOnly, parameters, strings, literals, facts); }
        Layout withoutDirectCaches() {
            if (facts == null) return this;
            var shapes = new ArrayList<BuildShape>(facts.builds);
            boolean changed = false;
            for (int i = 0; i < shapes.size(); i++) {
                BuildShape shape = shapes.get(i);
                if (keys[i].producer() >= 0 && shape.direct && shape.cached) {
                    shapes.set(i, new BuildShape(shape.unique, shape.direct, false, shape.stride, shape.columns));
                    changed = true;
                }
            }
            return changed ? specialize(new Facts(facts.constants, java.util.List.copyOf(shapes), facts.groupCache)) : this;
        }
        Expr meta(Expr address) {
            Expr base = address;
            long offset = 0;
            while (base instanceof Offset o && o.bytes() instanceof Literal l) {
                offset += l.value(); base = o.address();
            }
            if (base.equals(COLUMNS)) {
                if (facts != null) {
                    Long value = facts.constants.get(offset);
                    if (value != null) return i64(value);
                }
                // Cached dependency heads keep their dense I32 layout.
                for (int i = 0; i < builds.length; i++) {
                    // Physical stride is bound at execution time until a layout is specialized.
                    if (keys[i].producer() >= 0 && offset == build(i) + DEPENDENCY_LINKS + 8) return i64(4);
                }
            }
            return load(address);
        }
        int build(int index) {
            int offset = BUILDS;
            for (int i = 0; i < index; i++) offset += (keyStart(i) + keys[i].widths.length * KEY_BYTES + keys[i].bytes + 7) & ~7;
            return offset;
        }
        boolean hasCounts(int build) { return counts[build] || subtrees && !keys[build].dependent(); }
        int keyStart(int build) { return KEYS + (keys[build].producer() >= 0 ? 16 : 0) + (hasCounts(build) ? 8 : 0); }
        int counts(int build) { return build(build) + KEYS + (keys[build].producer() >= 0 ? 16 : 0); }
        int key(int build, int key) { return build(build) + keyStart(build) + key * KEY_BYTES; }
        int scratch(int build) { return key(build, keys[build].widths.length); }
        int arguments() { return build(builds.length); }
        long stringOffset() { return arguments() + (long) ARGUMENT_BYTES * sourceBuilds.length; }
        long literalOffset() { return stringOffset() + strings.length * 48L; }
        long parameterOffset() { return (literalOffset() + literals.length + 7) & -8L; }
        // One validity word followed by root heads. Twice the staging distance
        // prevents a future write from overwriting the current row's slot.
        long ringOffset() { return parameterOffset() + parameters.size() * 8L; }
        long groupCacheOffset() { return ringOffset() + ringBytes(); }
        long functionCardinalityOffset() { return groupCacheOffset() + 8; }
        long ringBytes() { return (long) LOOKAHEAD_SLOTS * (builds.length + 1) * 8; }
        Expr ring(Expr position) { return new Offset(at(COLUMNS, ringOffset()),
                binary(Op.MUL, binary(Op.AND, position, i64(LOOKAHEAD_SLOTS - 1)), i64((builds.length + 1) * 8L))); }
        boolean dependent() {
            for (Key key : keys) if (key.dependent()) return true;
            return false;
        }
        boolean canFilter(int index) { return keys[index].widths.length == 1 || canReduce(index); }
        boolean canReduce(int producer) {
            for (Key key : keys) if (key.producer() == producer) return true;
            return false;
        }
    }

    private static int rowStride(FrozenHashJoinBuild build, CompiledJoin.Build shape) {
        int stride = build.getRowStride();
        return stride > 0 ? stride : shape.payloadColumnCount() > 0 ? 16 : 8;
    }

    private record BuildShape(boolean unique, boolean direct, boolean cached, long stride,
                              java.util.List<Integer> columns) { }
    private record Facts(java.util.Map<Long, Long> constants, java.util.List<BuildShape> builds, boolean groupCache) { }

    private ResolvedJoinKernel() {
    }

    static CompiledJoin compile(ColumnTypes joinedTypes, int probeColumnCount,
            ColumnTypes valueTypes, ObjList<GroupByFunction> functions, int keyColumn, MapValue layout,
            Function postJoinFilter, int groupKeyColumn, Map groupLayout, Backend backend, Path artifacts) throws IOException {
        var builds = new ObjList<CompiledJoin.Build>();
        builds.add(new CompiledJoin.Build(keyColumn, joinedTypes.getColumnCount() - probeColumnCount));
        return compile(joinedTypes, probeColumnCount, valueTypes, functions, builds, layout,
                postJoinFilter, groupKeyColumn, groupLayout, backend, artifacts);
    }

    static CompiledJoin compile(ColumnTypes joinedTypes, int probeColumnCount,
            ColumnTypes valueTypes, ObjList<GroupByFunction> functions, ObjList<CompiledJoin.Build> sources, MapValue layout,
            Function postJoinFilter, int groupKeyColumn, Map groupLayout, Backend backend, Path artifacts) throws IOException {
        var keyTypes = new io.questdb.cairo.ArrayColumnTypes();
        var keyColumns = new io.questdb.cairo.ListColumnFilter();
        if (groupLayout != null) {
            if (groupKeyColumn < 0 || groupKeyColumn >= joinedTypes.getColumnCount()) return null;
            keyTypes.add(joinedTypes.getColumnType(groupKeyColumn));
            keyColumns.add(groupKeyColumn + 1);
        }
        return compile(joinedTypes, probeColumnCount, valueTypes, functions, sources, layout, postJoinFilter,
                keyTypes, keyColumns, null, groupLayout, backend, artifacts);
    }

    static CompiledJoin compile(ColumnTypes joinedTypes, int probeColumnCount,
            ColumnTypes valueTypes, ObjList<GroupByFunction> functions, ObjList<CompiledJoin.Build> sources, MapValue layout,
            Function postJoinFilter, ColumnTypes keyTypes, io.questdb.cairo.ListColumnFilter keyColumns,
            ObjList<Function> keyFunctions, Map groupLayout, Backend backend, Path artifacts) throws IOException {
        if (probeColumnCount < 0 || probeColumnCount > joinedTypes.getColumnCount() || sources.size() == 0) return null;
        var builds = new CompiledJoin.Build[sources.size()];
        Key[] keys = new Key[builds.length];
        long columns = probeColumnCount;
        for (int i = 0; i < builds.length; i++) {
            var build = sources.getQuick(i);
            if (build.keyColumns().length == 0 || build.keyTypes().length != build.keyColumns().length
                    || build.payloadColumnCount() < 0 || (keys[i] = Key.bind(build, probeColumnCount, builds, i)) == null) return null;
            for (int k = 0; k < build.keyColumns().length; k++) {
                int column = build.keyColumns()[k];
                if (column < 0 || column >= columns || joinedTypes.getColumnType(column) != build.keyTypes()[k]) return null;
            }
            builds[i] = build;
            columns += build.payloadColumnCount();
        }
        if (columns != joinedTypes.getColumnCount()) return null;
        var parameters = new ArrayList<IntSupplier>();
        var inputs = new ResolvedInputs(joinedTypes, parameters, backend.options());
        QuestMapProbe mapProbe = null;
        Pipeline.Column groupKey = null;
        ResolvedVarchar.Expression groupString = null;
        if (groupLayout != null) {
            if (keyTypes.getColumnCount() == 1 && keyColumns.size() == 1 && keyColumns.getQuick(0) > 0
                    && (keyFunctions == null || keyFunctions.size() == 0)) {
                int index = keyColumns.getQuick(0) - 1;
                if (index >= joinedTypes.getColumnCount() || joinedTypes.getColumnType(index) != keyTypes.getColumnType(0)) return null;
                mapProbe = QuestMapProbe.bind(index, keyTypes.getColumnType(0), groupLayout);
                if (mapProbe != null && mapProbe.varchar()) groupString = inputs.strings().column(index);
                else if (mapProbe != null) groupKey = inputs.column(index, keyTypes.getColumnType(0));
            }
            if (mapProbe == null && groupLayout instanceof io.questdb.cairo.map.UnorderedVarcharMap
                    && keyTypes.getColumnCount() == 1 && keyTypes.getColumnType(0) == ColumnType.VARCHAR
                    && keyColumns.size() == 0 && keyFunctions != null && keyFunctions.size() == 1) {
                groupString = inputs.strings().bind(keyFunctions.getQuick(0));
                if (groupString == null) return null;
                mapProbe = QuestMapProbe.varchar(new QuestVarchar.Key(inputs.strings().binding(), groupString));
            }
            if (mapProbe == null) {
                var variable = QuestVariableKey.bind(inputs, joinedTypes, keyTypes, keyColumns, keyFunctions, groupLayout);
                if (variable != null) mapProbe = QuestMapProbe.variable(variable);
            }
            if (mapProbe == null) {
                var key = QuestFixedKey.bind(inputs, keyTypes, keyColumns, keyFunctions, groupLayout);
                // Joined rows have numeric cells; dictionary-derived keys need execution preparation.
                if (key == null || !key.dictionaries().isEmpty()) return null;
                mapProbe = QuestMapProbe.fixed(key, keyTypes.getColumnCount() == 1 ? keyTypes.getColumnType(0) : -1,
                        groupLayout instanceof io.questdb.cairo.map.OrderedMap);
            }
        }
        // Before predicate and aggregate binding, these are precisely the grouping inputs.
        int groupBuild = mapProbe != null && (mapProbe.variable() || mapProbe.varchar())
                ? groupBuild(inputs.strings().binding(), probeColumnCount, builds) : -1;
        var predicate = inputs.predicate(postJoinFilter);
        if (predicate == null) return null;
        var packed = new FlyweightPackedMapValue(valueTypes);
        long base = layout != null ? layout.getAddress(0) : 0;
        var updates = AggregateUpdates.bind(valueTypes, functions,
                index -> groupLayout != null ? packed.getOffset(index) : layout.getAddress(index) - base, true, inputs);
        if (updates == null) return null;
        // Capture helpers own separate per-slot state; leave those on the ordinary path.
        if (!updates.inputs().captures().isEmpty()) return null;
        long valueBytes = 0;
        for (int i = 0; i < valueTypes.getColumnCount(); i++) valueBytes += ColumnType.sizeOf(valueTypes.getColumnType(i));
        var target = new Target(mapProbe, groupKey, groupString, null, groupBuild, valueBytes);
        int[] sourceBuilds = new int[updates.sourceIndexes().length];
        int[] sourceColumns = new int[sourceBuilds.length];
        for (int i = 0; i < sourceBuilds.length; i++) {
            int index = updates.sourceIndexes()[i];
            int owner = -1;
            if (index >= probeColumnCount) {
                index -= probeColumnCount;
                owner = 0;
                while (index >= builds[owner].payloadColumnCount()) index -= builds[owner++].payloadColumnCount();
            }
            sourceBuilds[i] = owner;
            sourceColumns[i] = index;
        }
        Source[] strings = new Source[updates.inputs().varcharColumns().length];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = Source.bind(updates.inputs().varcharColumns()[i], probeColumnCount, builds, builds.length);
            if (strings[i] == null) return null;
        }
        boolean[] counts = new boolean[builds.length];
        boolean subtrees = true;
        boolean dependent = false;
        int lastObserved = -1;
        java.util.Arrays.fill(counts, true);
        // Binding has collected every aggregate, predicate and grouping input.
        for (int owner : sourceBuilds) if (owner >= 0) {
            counts[owner] = false;
            subtrees = false;
            lastObserved = Math.max(lastObserved, owner);
        }
        for (Source source : strings) if (source.build >= 0) {
            counts[source.build] = false;
            subtrees = false;
            lastObserved = Math.max(lastObserved, source.build);
        }
        // Folding an earlier unobserved join would turn a,b,a,b into a,a,b,b.
        // SUM sentinel overflow and floating updates require consecutive identical inputs.
        if (!updates.countsRowsOnly()) java.util.Arrays.fill(counts, 0, lastObserved + 1, false);
        for (Key key : keys) {
            for (Source source : key.sources) if (source.build >= 0) counts[source.build] = false;
            if (key.dependent()) {
                dependent = true;
                subtrees &= key.producer() >= 0;
            }
        }
        var descriptors = new Layout(builds, keys, sourceBuilds, sourceColumns, counts, subtrees && dependent,
                updates.countsRowsOnly(), List.copyOf(parameters), strings, updates.inputs().literals(), null);
        var programs = new Programs(updates, predicate, target, descriptors, backend, artifacts, false, 1);
        try {
            return new Prepared(programs, descriptors, mapProbe, backend.name(), updates, predicate, target, backend, artifacts);
        } catch (Throwable failure) {
            try { programs.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static Program program(AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                                   Layout layout, boolean membership, boolean promoteState, Lookahead lookahead) {
        Builder b = new Builder();
        var state = promoteState ? ScalarAggregateState.bind(b, updates) : null;
        var destinationTarget = new Target(target.probe, target.key, target.varchar, state, target.groupBuild, target.valueBytes);
        if (layout.subtrees) b.when(eq(layout.meta(at(COLUMNS, MODE)), i64(2)), folded -> {
            emitScan(folded, updates, predicate, destinationTarget, layout, Iteration.SUBTREES, membership, lookahead);
            folded.exit(0);
        });
        b.when(ne(layout.meta(at(COLUMNS, MODE)), i64(0)), unique -> emitScan(unique, updates, predicate, destinationTarget, layout, Iteration.UNIQUE, membership, lookahead));
        b.when(eq(layout.meta(at(COLUMNS, MODE)), i64(0)), multiple -> {
            Expr counted = bool(false);
            for (int i = 0; i < layout.counts.length; i++) if (layout.counts[i]) {
                counted = binary(Op.EITHER, counted, ne(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.counts(i))), nil()));
            }
            // Unique leaves and declined preparation retain the ordinary loop. Choose once
            // per frame instead of paying unused multiplicity checks for every joined row.
            Var hasCounts = multiple.let(counted);
            multiple.when(eq(hasCounts, bool(false)), ordinary -> emitScan(ordinary, updates, predicate, destinationTarget, layout, Iteration.EXPANDED, membership, lookahead));
            multiple.when(hasCounts, folded -> emitScan(folded, updates, predicate, destinationTarget, layout, Iteration.COUNTED, membership, lookahead));
        });
        b.exit(0);
        return new Program(state == null ? b.block() : state.publishExits(b.block()));
    }

    private record Target(QuestMapProbe probe, Pipeline.Column key, ResolvedVarchar.Expression varchar,
                          ScalarAggregateState state, int groupBuild, long valueBytes) { }

    private static int groupBuild(ResolvedVarchar.Binding inputs, int probeColumns, CompiledJoin.Build[] builds) {
        int owner = -1;
        for (int[] columns : new int[][]{inputs.numericColumns(), inputs.varcharColumns()}) {
            for (int column : columns) {
                Source source = Source.bind(column, probeColumns, builds, builds.length);
                if (source == null || source.build < 0 || owner >= 0 && source.build != owner) return -1;
                owner = source.build;
            }
        }
        return owner;
    }
    private record Lookup(Var table, Var mask, Var directory, Var shift, Var heap, Var counts, IntegralJoinIndex.Descriptor direct, Var stride) { }
    private record RowInputs(List<Var> addresses, List<Expr> parameters) { }
    private enum Iteration { UNIQUE, EXPANDED, COUNTED, SUBTREES }
    private enum Lookahead { NONE, JOINS, JOINS_AND_GROUPS }

    private static void emitScan(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate,
                                 Target target, Layout layout, Iteration iteration, boolean membership, Lookahead lookahead) {
        boolean prefetch = lookahead != Lookahead.NONE;
        Var rowIds = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Lookup[] lookups = new Lookup[layout.builds.length];
        IntegralMembership.Descriptor[] sets = membership ? new IntegralMembership.Descriptor[lookups.length] : null;
        Var[] links = new Var[lookups.length];
        Var[] resumes = new Var[lookups.length];
        for (int i = 0; i < lookups.length; i++) {
            if (iteration == Iteration.SUBTREES && layout.keys[i].dependent()) {
                links[i] = b.let(i64(0));
                continue;
            }
            int offset = layout.build(i);
            lookups[i] = new Lookup(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, offset + LOOKUP))),
                    b.let(load(at(COLUMNS, offset + LOOKUP + 8))),
                    b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, offset + LOOKUP + 16))),
                    b.let(load(at(COLUMNS, offset + LOOKUP + 24))),
                    b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, offset + LOOKUP + 32))),
                    iteration == Iteration.SUBTREES || iteration == Iteration.COUNTED && layout.counts[i]
                            ? b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.counts(i)))) : null,
                    IntegralJoinIndex.bind(b, at(COLUMNS, offset + DIRECT)), b.let(layout.meta(at(COLUMNS, offset + HEAP_STRIDE))));
            if (membership) sets[i] = IntegralMembership.bind(b, at(COLUMNS, offset + MEMBERSHIP));
            links[i] = b.let(i64(0));
            if ((target.probe != null || layout.dependent()) && iteration != Iteration.UNIQUE && iteration != Iteration.SUBTREES) {
                resumes[i] = b.let(load(at(COLUMNS, offset + RESUME_LINK)));
            }
        }
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < updates.columns().size(); i++)
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.arguments() + (long) ARGUMENT_BYTES * i))));
        var parameters = new ArrayList<Expr>();
        for (int i = 0; i < layout.parameters.size(); i++)
            parameters.add(b.let(new Load(MemoryType.I32, at(COLUMNS, layout.parameterOffset() + i * 8L))));
        var inputs = new RowInputs(addresses, parameters);
        Var destination = target.state != null ? STATE : b.let(STATE);
        Var position = b.let(load(at(COLUMNS, POSITION)));
        // Every native re-entry warms the ring again; no stored head survives by assumption.
        Var ringBegin = prefetch ? b.let(position) : null;
        Var matched = b.let(load(at(COLUMNS, MATCHED)));
        Var pairs = b.let(i64(0));
        b.loop(binary(Op.LT, position, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, position, i64(1023)), i64(0)), Builder::poll);
            // Future keys use the scratch before the current row stages its keys.
            if (prefetch) loop.when(binary(Op.LT, add(position, i64(LOOKAHEAD_SLOTS)), ROWS), future -> {
                Var futurePosition = future.let(add(position, i64(LOOKAHEAD_SLOTS)));
                Var futureRow = rowAt(future, rowIds, futurePosition);
                Var[] futureKeys = new Var[lookups.length];
                loadKeys(future, layout, futureKeys, 0, futureRow, position, matched, lookups, links, true, loaded ->
                        filterAll(loaded, futureKeys, sets, layout, 0, eligible -> {
                            for (int i = 0; i < lookups.length; i++) {
                                if (!layout.keys[i].dependent()) prefetchLookup(eligible, lookups[i], futureKeys[i], layout, i);
                            }
                        }));
            });
            Var row = rowAt(loop, rowIds, position);
            Var[] heads = new Var[lookups.length];
            java.util.function.Consumer<Builder> consume = found -> {
                if (iteration == Iteration.SUBTREES) {
                    consumeSubtrees(found, updates, predicate, target, layout, lookups, heads, links, inputs,
                            row, destination, matched, position, i64(1), 0);
                } else expand(found, updates, predicate, target, layout, lookups, heads, links, resumes, sets, inputs,
                        row, destination, matched, position, pairs, i64(1), 0, iteration == Iteration.UNIQUE);
            };
            if (prefetch) lookupAhead(loop, layout, lookahead == Lookahead.JOINS_AND_GROUPS ? target : null, iteration, lookups, sets, links, rowIds,
                    position, ringBegin, matched, row, heads, consume);
            else {
                Var[] keys = new Var[lookups.length];
                loadKeys(loop, layout, keys, 0, row, position, matched, lookups, links, false, loaded ->
                        filterAll(loaded, keys, sets, layout, 0, eligible ->
                                lookupAll(eligible, lookups, heads, keys, layout, 0, consume)));
            }
            loop.assign(position, add(position, i64(1)));
        });
        b.store(at(COLUMNS, MATCHED), matched);
    }

    /** Stage roots once, retaining their exact heads until consumption in probe order. */
    private static void lookupAhead(Builder loop, Layout layout, Target target, Iteration iteration,
                                    Lookup[] lookups, IntegralMembership.Descriptor[] sets, Var[] links,
                                    Var rowIds, Var position, Var ringBegin, Var matched, Var row,
                                    Var[] heads, java.util.function.Consumer<Builder> consume) {
        loop.when(binary(Op.LT, add(position, i64(LOOKAHEAD_ROWS)), ROWS), future -> {
            Var futurePosition = future.let(add(position, i64(LOOKAHEAD_ROWS)));
            Var futureRow = rowAt(future, rowIds, futurePosition);
            Expr entry = layout.ring(futurePosition);
            future.store(entry, i64(0));
            for (int i = 0; i < lookups.length; i++) if (!layout.keys[i].dependent())
                future.store(at(entry, (i + 1) * 8L), i64(0));
            Var[] futureKeys = new Var[lookups.length];
            Var[] futureHeads = new Var[lookups.length];
            loadKeys(future, layout, futureKeys, 0, futureRow, position, matched, lookups, links, true, loaded -> {
                // An unresolved future SYMBOL leaves this entry invalid; current-row
                // execution retains its ordinary translation and resume protocol.
                loaded.store(entry, i64(1));
                filterAll(loaded, futureKeys, sets, layout, 0, eligible ->
                        lookupAll(eligible, lookups, futureHeads, futureKeys, layout, 0, found -> {
                            prefetchChildren(found, layout, iteration, lookups, sets, futureHeads, futureRow, position, matched);
                            if (target != null) prefetchGroup(found, target, layout, futureHeads);
                            for (int i = 0; i < lookups.length; i++) if (!layout.keys[i].dependent()) {
                                found.store(at(entry, (i + 1) * 8L), futureHeads[i]);
                                if (layout.builds[i].payloadColumnCount() > 0) {
                                    Expr ordinal = binary(Op.USHR, binary(Op.SUB, futureHeads[i], i64(8)), i64(4));
                                    found.prefetch(new Offset(lookups[i].heap, binary(Op.MUL, ordinal, lookups[i].stride)), false);
                                }
                            }
                        }));
            });
        });
        Var ready = loop.let(bool(false));
        for (int i = 0; i < lookups.length; i++) if (!layout.keys[i].dependent()) heads[i] = loop.let(i64(0));
        Var cached = loop.let(bool(false));
        loop.when(binary(Op.GE, position, add(ringBegin, i64(LOOKAHEAD_ROWS))), warmed ->
                warmed.assign(cached, ne(load(layout.ring(position)), i64(0))));
        loop.when(cached, hit -> {
            Expr all = bool(true);
            for (int i = 0; i < lookups.length; i++) if (!layout.keys[i].dependent()) {
                hit.assign(heads[i], load(at(layout.ring(position), (i + 1) * 8L)));
                all = binary(Op.BOTH, all, ne(heads[i], i64(0)));
            }
            hit.assign(ready, all);
        });
        loop.when(eq(cached, bool(false)), ordinary -> {
            Var[] keys = new Var[lookups.length];
            Var[] ordinaryHeads = new Var[lookups.length];
            loadKeys(ordinary, layout, keys, 0, row, position, matched, lookups, links, false, loaded ->
                    filterAll(loaded, keys, sets, layout, 0, eligible ->
                            lookupAll(eligible, lookups, ordinaryHeads, keys, layout, 0, found -> {
                                for (int i = 0; i < lookups.length; i++) if (!layout.keys[i].dependent())
                                    found.assign(heads[i], ordinaryHeads[i]);
                                found.assign(ready, bool(true));
                            })));
        });
        loop.when(ready, consume);
    }

    private static void prefetchGroup(Builder b, Target target, Layout layout, Var[] roots) {
        if (layout.facts == null || !layout.facts.groupCache || target.valueBytes == 0) return;
        Expr head = futureHead(b, layout, roots, target.groupBuild);
        if (head == null) return;
        b.when(ne(head, i64(0)), present -> {
            Expr ordinal = binary(Op.USHR, binary(Op.SUB, head, i64(8)), i64(4));
            Expr cache = new Load(MemoryType.ADDRESS, at(COLUMNS, layout.groupCacheOffset()));
            Var value = present.let(new Load(MemoryType.ADDRESS, new Offset(cache, binary(Op.MUL, ordinal, i64(8)))));
            present.when(ne(value, nil()), cached -> {
                // The state may be unaligned after a variable key. Prefetch its last byte too;
                // all addresses remain within the value, independent of cache-line alignment.
                for (long offset = 0; offset < target.valueBytes; offset += io.questdb.std.Misc.CACHE_LINE_SIZE)
                    cached.prefetch(at(value, offset), true);
                if (target.valueBytes > 1) cached.prefetch(at(value, target.valueBytes - 1), true);
            });
        });
    }

    private static boolean canPrefetchGroup(Target target, Layout layout) {
        if (layout.facts == null || !layout.facts.groupCache || target.valueBytes == 0) return false;
        for (int owner = target.groupBuild; layout.keys[owner].dependent(); owner = layout.keys[owner].producer()) {
            if (layout.keys[owner].producer() < 0 || !layout.facts.builds.get(owner).cached) return false;
        }
        return true;
    }

    /** A hint for the first duplicate only; consumption still expands every exact match. */
    private static Expr futureHead(Builder b, Layout layout, Var[] roots, int build) {
        if (!layout.keys[build].dependent()) return roots[build];
        int producer = layout.keys[build].producer();
        if (producer < 0 || !layout.facts.builds.get(build).cached) return null;
        Expr parent = futureHead(b, layout, roots, producer);
        if (parent == null) return null;
        Var head = b.let(i64(0));
        b.when(ne(parent, i64(0)), present -> {
            Expr ordinal = binary(Op.USHR, binary(Op.SUB, parent, i64(8)), i64(4));
            Expr links = new Load(MemoryType.ADDRESS, at(COLUMNS, layout.build(build) + DEPENDENCY_LINKS));
            Expr packed = new Load(MemoryType.I32, new Offset(links, binary(Op.MUL, ordinal, i64(4))));
            present.assign(head, binary(Op.MUL, binary(Op.AND, packed, i64(0xffffffffL)), i64(8)));
        });
        return head;
    }

    /** Only a unique root has one payload to inspect without expanding duplicates. */
    private static void prefetchChildren(Builder found, Layout layout, Iteration iteration,
                                         Lookup[] lookups, IntegralMembership.Descriptor[] sets,
                                         Var[] futureHeads, Var futureRow, Var position, Var matched) {
        if (layout.facts != null && iteration != Iteration.SUBTREES) {
            Var[] futureLinks = new Var[lookups.length];
            for (int i = 0; i < lookups.length; i++) futureLinks[i] =
                    found.let(layout.keys[i].dependent() ? i64(0) : futureHeads[i]);
            for (int child = 0; child < lookups.length; child++) {
                int parent = layout.keys[child].producer();
                if (parent < 0 || layout.keys[parent].dependent()
                        || !layout.facts.builds.get(parent).unique
                        || layout.facts.builds.get(child).cached) continue;
                int index = child;
                loadKey(found, layout, child, futureRow, position, matched, lookups, futureLinks, true,
                        (loadedChild, key) -> {
                            if (sets != null && layout.canFilter(index)) {
                                Expr value = layout.keys[index].membershipKey(loadedChild, key);
                                loadedChild.when(IntegralMembership.containsJoin(loadedChild, sets[index], value, layout.canReduce(index)),
                                        hit -> prefetchLookup(hit, lookups[index], key, layout, index));
                            } else prefetchLookup(loadedChild, lookups[index], key, layout, index);
                        });
            }
        }
    }

    private static Var rowAt(Builder b, Var rowIds, Expr position) {
        Var row = b.let(position);
        b.when(ne(rowIds, nil()), selected -> selected.assign(row,
                load(new Offset(rowIds, binary(Op.MUL, position, i64(8))))));
        return row;
    }

    private static void consumeSubtrees(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                                      Layout layout, Lookup[] lookups, Var[] heads, Var[] links, RowInputs inputs,
                                      Var row, Var destination, Var matched, Var position, Expr repetitions, int index) {
        if (index == links.length) {
            consume(b, updates, predicate, target, loadRow(b, updates, layout, inputs, row, lookups, links),
                    destination, matched, position, layout, links, repetitions);
            return;
        }
        if (layout.keys[index].dependent()) {
            consumeSubtrees(b, updates, predicate, target, layout, lookups, heads, links, inputs,
                    row, destination, matched, position, repetitions, index + 1);
            return;
        }
        b.assign(links[index], heads[index]);
        Var count = b.let(i64(1)); // An independent, unique leaf needs no count array.
        Var counts = lookups[index].counts;
        b.when(ne(counts, nil()), weighted -> {
            Expr offset = binary(Op.SUB, heads[index], i64(8));
            if (layout.builds[index].payloadColumnCount() > 0) offset = binary(Op.USHR, offset, i64(1));
            weighted.assign(count, load(new Offset(counts, offset)));
        });
        // Preparation rejects subtree overflow, so zero here really means no joined row.
        b.when(ne(count, i64(0)), present -> consumeSubtrees(present, updates, predicate, target, layout,
                lookups, heads, links, inputs, row, destination, matched, position,
                present.let(binary(Op.MUL, repetitions, count)), index + 1));
    }

    private static void loadKeys(Builder b, Layout layout, Var[] keys, int index, Var row,
                                 Var position, Var matched, Lookup[] lookups, Var[] links, boolean speculative,
                                 java.util.function.Consumer<Builder> loaded) {
        if (index == keys.length) { loaded.accept(b); return; }
        if (layout.keys[index].dependent()) {
            loadKeys(b, layout, keys, index + 1, row, position, matched, lookups, links, speculative, loaded);
        } else {
            loadKey(b, layout, index, row, position, matched, lookups, links, speculative, (hit, key) -> {
                keys[index] = key;
                loadKeys(hit, layout, keys, index + 1, row, position, matched, lookups, links, speculative, loaded);
            });
        }
    }

    private static void loadKey(Builder b, Layout layout, int index, Var row, Var position, Var matched,
                                Lookup[] lookups, Var[] links, boolean speculative, java.util.function.BiConsumer<Builder, Var> loaded) {
        Key shape = layout.keys[index];
        Var accepted = b.let(bool(true));
        Var result = null;
        int offset = 0;
        for (int k = 0; k < shape.widths.length; k++) {
            int descriptor = layout.key(index, k);
            Var key = loadValue(b, layout, shape.widths[k], i64(QuestFixedKey.nullValue(layout.builds[index].keyTypes()[k])),
                    shape.sources[k], at(COLUMNS, descriptor), new Load(MemoryType.ADDRESS, at(COLUMNS, descriptor)),
                    row, lookups, links);
            if (layout.builds[index].keyTypes()[k] == ColumnType.SYMBOL) {
                Var translated = b.let(i64(SymbolTable.VALUE_IS_NULL));
                int buildIndex = index, keyIndex = k;
                b.when(ne(key, i64(SymbolTable.VALUE_IS_NULL)), symbol -> {
                    symbol.assign(translated, i64(SymbolKeyTranslator.UNRESOLVED));
                    symbol.when(binary(Op.BOTH, binary(Op.GE, key, i64(0)),
                            binary(Op.LT, key, load(at(COLUMNS, descriptor + SYMBOL_COUNT)))), cached ->
                            cached.assign(translated, new Load(MemoryType.I32, new Offset(
                                    new Load(MemoryType.ADDRESS, at(COLUMNS, descriptor + SYMBOL_CACHE)), binary(Op.MUL, key, i64(4))))));
                    symbol.when(eq(translated, i64(SymbolKeyTranslator.UNRESOLVED)), unresolved -> {
                        unresolved.when(eq(key, load(at(COLUMNS, descriptor + SYMBOL_INPUT))), supplied ->
                                supplied.assign(translated, load(at(COLUMNS, descriptor + SYMBOL_RESULT))));
                        unresolved.when(eq(translated, i64(SymbolKeyTranslator.UNRESOLVED)), missing -> {
                            // A future dictionary miss must not publish a later row's resume state.
                            if (speculative) {
                                missing.assign(accepted, bool(false));
                            } else {
                                missing.store(at(COLUMNS, POSITION), position);
                                missing.store(at(COLUMNS, MATCHED), matched);
                                // A dependent dictionary miss occurs inside duplicate expansion. Resume
                                // its selected prefix, without replaying pairs already consumed.
                                for (int i = 0; i < links.length; i++) {
                                    // Independent keys are read before consuming this row. Older map
                                    // misses must not supply duplicate positions for a new row.
                                    missing.store(at(COLUMNS, layout.build(i) + RESUME_LINK),
                                            shape.dependent() && i < buildIndex ? links[i] : i64(0));
                                }
                                missing.store(at(COLUMNS, SYMBOL_BUILD), i64(buildIndex));
                                missing.store(at(COLUMNS, SYMBOL_KEY), i64(keyIndex));
                                missing.store(at(COLUMNS, descriptor + SYMBOL_INPUT), key);
                                missing.exit(NEED_SYMBOL);
                            }
                        });
                    });
                });
                b.assign(key, translated);
                b.assign(accepted, binary(Op.BOTH, accepted, ne(key, i64(SymbolTable.VALUE_NOT_FOUND))));
            }
            if (shape.map != null && shape.map.orderedNative()) b.store(shape.widths[k], at(COLUMNS, layout.scratch(index) + offset), key);
            else result = key;
            offset += shape.widths[k].bytes;
        }
        Var value = shape.map != null && shape.map.orderedNative() ? b.let(at(COLUMNS, layout.scratch(index))) : result;
        b.when(accepted, hit -> loaded.accept(hit, value));
    }

    private static void filterAll(Builder b, Var[] keys, IntegralMembership.Descriptor[] sets, Layout layout, int index,
                                  java.util.function.Consumer<Builder> eligible) {
        if (sets == null || index == sets.length) { eligible.accept(b); return; }
        if (!layout.keys[index].dependent() && layout.canFilter(index)) {
            Expr key = layout.keys[index].membershipKey(b, keys[index]);
            b.when(IntegralMembership.containsJoin(b, sets[index], key, layout.canReduce(index)), hit -> filterAll(hit, keys, sets, layout, index + 1, eligible));
        } else filterAll(b, keys, sets, layout, index + 1, eligible);
    }

    /** Reject misses before loading payload or expanding any duplicate cross-product. */
    private static void lookupAll(Builder b, Lookup[] lookups, Var[] heads, Var[] keys, Layout layout, int index,
                                  java.util.function.Consumer<Builder> found) {
        if (index == lookups.length) { found.accept(b); return; }
        if (layout.keys[index].dependent()) {
            lookupAll(b, lookups, heads, keys, layout, index + 1, found);
        } else {
            lookupOne(b, lookups, heads, keys[index], layout, index,
                    hit -> lookupAll(hit, lookups, heads, keys, layout, index + 1, found));
        }
    }

    private static void lookupOne(Builder b, Lookup[] lookups, Var[] heads, Var key, Layout layout, int index,
                                  java.util.function.Consumer<Builder> found) {
        if (layout.keys[index].widths.length == 1) {
            var direct = lookups[index].direct;
            Var head = b.let(i64(0));
            Expr useDirect = layout.facts == null ? ne(direct.address(), nil()) : bool(layout.facts.builds.get(index).direct);
            b.when(useDirect, dense -> {
                Expr value = layout.keys[index].map != null && layout.keys[index].map.orderedNative()
                        ? new Load(layout.keys[index].widths[0], key) : key;
                dense.assign(head, IntegralJoinIndex.find(dense, direct, value, layout.builds[index].keyTypes()[0]));
            });
            b.when(eq(useDirect, bool(false)), hash -> lookupHash(hash, lookups, heads, key, layout, index,
                    hit -> hit.assign(head, heads[index])));
            heads[index] = head;
            b.when(ne(head, i64(0)), found);
        } else lookupHash(b, lookups, heads, key, layout, index, found);
    }

    private static void prefetchLookup(Builder b, Lookup lookup, Var key, Layout layout, int index) {
        Key shape = layout.keys[index];
        Expr direct = shape.widths.length == 1
                ? layout.facts == null ? ne(lookup.direct.address(), nil()) : bool(layout.facts.builds.get(index).direct)
                : bool(false);
        if (shape.widths.length == 1) {
            Expr value = shape.map != null && shape.map.orderedNative() ? new Load(shape.widths[0], key) : key;
            b.when(direct, dense -> dense.when(binary(Op.BOTH, binary(Op.GE, value, lookup.direct.first()),
                    binary(Op.GE, lookup.direct.last(), value)), inside -> inside.prefetch(new Offset(lookup.direct.address(),
                    binary(Op.MUL, binary(Op.SUB, value, lookup.direct.first()), i64(Integer.BYTES))), false)));
        }
        b.when(eq(direct, bool(false)), hashLookup -> {
            if (shape.map != null) {
                Var hash = shape.map.hash(hashLookup, key);
                Var partition = hashLookup.let(binary(Op.AND, binary(Op.USHR, binary(Op.USHR, hash, i64(32)), lookup.shift), lookup.mask));
                Var view = hashLookup.let(new Offset(lookup.table, binary(Op.MUL, partition, i64(NativeMapView.PROBE_BYTES))));
                shape.map.prefetchExisting(hashLookup, view, hash);
            } else {
                IntSlot slot = intSlot(hashLookup, lookup, key);
                hashLookup.prefetch(slot.entry, false);
            }
        });
    }

    private record IntSlot(Var base, Var mask, Var slot, Var entry) { }

    private static IntSlot intSlot(Builder b, Lookup lookup, Var key) {
        Var hash = b.let(binary(Op.AND, key, i64(0xffff_ffffL)));
        b.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(37))));
        b.assign(hash, binary(Op.MUL, hash, i64(0x165667919E3779F9L)));
        b.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(32))));
        Var base = b.let(lookup.table);
        Var mask = b.let(lookup.mask);
        b.when(ne(lookup.directory, nil()), partitioned -> {
            Var partition = partitioned.let(binary(Op.USHR, binary(Op.USHR, hash, i64(32)), lookup.shift));
            Var entry = partitioned.let(new Offset(lookup.directory, binary(Op.MUL, partition, i64(16))));
            partitioned.assign(base, new Load(MemoryType.ADDRESS, entry));
            partitioned.assign(mask, load(at(entry, 8)));
        });
        Var slot = b.let(binary(Op.AND, hash, mask));
        Var entry = b.let(new Offset(base, binary(Op.MUL, slot, i64(8))));
        return new IntSlot(base, mask, slot, entry);
    }

    private static void lookupHash(Builder b, Lookup[] lookups, Var[] heads, Var key, Layout layout, int index,
                                   java.util.function.Consumer<Builder> found) {
        Lookup lookup = lookups[index];
        QuestMapProbe map = layout.keys[index].map;
        if (map != null) {
            Var hash = map.hash(b, key);
            Var partition = b.let(binary(Op.AND, binary(Op.USHR, binary(Op.USHR, hash, i64(32)), lookup.shift), lookup.mask));
            Var view = b.let(new Offset(lookup.table, binary(Op.MUL, partition, i64(NativeMapView.PROBE_BYTES))));
            Var value = map.findExisting(b, key, view, hash);
            heads[index] = b.let(i64(0));
            b.when(ne(value, nil()), hit -> {
                hit.assign(heads[index], binary(Op.MUL, binary(Op.AND, new Load(MemoryType.I32, value), i64(0xffff_ffffL)), i64(8)));
                found.accept(hit);
            });
            return;
        }
        IntSlot first = intSlot(b, lookup, key);
        Var base = first.base, mask = first.mask, slot = first.slot, entry = first.entry;
        Var head = b.let(new Load(MemoryType.I32, at(entry, 4)));
        b.loop(binary(Op.BOTH, ne(head, i64(0)), ne(new Load(MemoryType.I32, entry), key)), collision -> {
            collision.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
            collision.assign(entry, new Offset(base, binary(Op.MUL, slot, i64(8))));
            collision.assign(head, new Load(MemoryType.I32, at(entry, 4)));
            collision.when(eq(binary(Op.AND, slot, i64(1023)), i64(0)), Builder::poll);
        });
        heads[index] = b.let(binary(Op.MUL, binary(Op.AND, head, i64(0xffff_ffffL)), i64(8)));
        b.when(ne(head, i64(0)), found);
    }

    private static void expand(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                               Layout layout, Lookup[] lookups, Var[] heads, Var[] links, Var[] resumes,
                               IntegralMembership.Descriptor[] sets, RowInputs inputs,
                               Var row, Var destination, Var matched, Var position, Var pairs, Expr repetitions, int index, boolean unique) {
        if (index == links.length) {
            consume(b, updates, predicate, target, loadRow(b, updates, layout, inputs, row, lookups, links),
                    destination, matched, position, layout, links, repetitions);
            return;
        }
        if (!layout.keys[index].dependent()) {
            expandMatches(b, updates, predicate, target, layout, lookups, heads, links, resumes, sets, inputs,
                    row, destination, matched, position, pairs, repetitions, index, unique);
            return;
        }
        int producer = layout.keys[index].producer();
        Var head = b.let(i64(0));
        Expr cacheAddress = producer < 0 ? null : new Load(MemoryType.ADDRESS, at(COLUMNS, layout.build(index) + DEPENDENCY_LINKS));
        Var cached = cacheAddress == null ? null : b.let(cacheAddress);
        Expr cacheReady = cached == null ? bool(false) : layout.facts == null ? ne(cached, nil()) : bool(layout.facts.builds.get(index).cached);
        if (cached != null) b.when(cacheReady, ready -> {
            // A payload-bearing producer heap has sixteen-byte rows, biased by eight.
            Expr ordinal = binary(Op.USHR, binary(Op.SUB, links[producer], i64(8)), i64(4));
            Expr offset = binary(Op.MUL, ordinal, layout.meta(at(COLUMNS, layout.build(index) + DEPENDENCY_LINKS + 8)));
            Var compressed = ready.let(new Load(MemoryType.I32, new Offset(cached, offset)));
            ready.assign(head, binary(Op.MUL, binary(Op.AND, compressed, i64(0xffff_ffffL)), i64(8)));
        });
        java.util.function.Consumer<Builder> ordinary = fallback ->
                loadKey(fallback, layout, index, row, position, matched, lookups, links, false, (loaded, key) -> {
                    java.util.function.Consumer<Builder> probe = eligible ->
                            lookupOne(eligible, lookups, heads, key, layout, index, hit -> hit.assign(head, heads[index]));
                    if (sets != null && layout.canFilter(index)) {
                        Expr value = layout.keys[index].membershipKey(loaded, key);
                        loaded.when(IntegralMembership.containsJoin(loaded, sets[index], value, layout.canReduce(index)), probe);
                    } else probe.accept(loaded);
                });
        if (cached == null) ordinary.accept(b);
        else b.when(eq(cacheReady, bool(false)), ordinary);
        // Join both lookup paths before emitting the consumer, so code size does not
        // double at every dependency in a longer chain.
        heads[index] = head;
        b.when(ne(head, i64(0)), hit -> expandMatches(hit, updates, predicate, target, layout, lookups, heads,
                links, resumes, sets, inputs, row, destination, matched, position, pairs, repetitions, index, unique));
    }

    private static void expandMatches(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                               Layout layout, Lookup[] lookups, Var[] heads, Var[] links, Var[] resumes, IntegralMembership.Descriptor[] sets, RowInputs inputs,
                               Var row, Var destination, Var matched, Var position, Var pairs, Expr repetitions, int index, boolean unique) {
        b.assign(links[index], heads[index]);
        if (unique || layout.facts != null && layout.facts.builds.get(index).unique) {
            expand(b, updates, predicate, target, layout, lookups, heads, links, resumes, sets, inputs,
                    row, destination, matched, position, pairs, repetitions, index + 1, unique);
        } else {
            if (resumes[index] != null) b.when(ne(resumes[index], i64(0)), resumed -> {
                resumed.assign(links[index], resumes[index]);
                resumed.assign(resumes[index], i64(0));
                resumed.store(at(COLUMNS, layout.build(index) + RESUME_LINK), i64(0));
            });
            Var counts = lookups[index].counts;
            Var count = b.let(i64(1));
            if (counts != null) b.when(ne(counts, nil()), counted -> {
                Expr offset = binary(Op.SUB, heads[index], i64(8));
                // Counts are dense LONGs; a payload heap has sixteen-byte rows, otherwise eight.
                if (layout.builds[index].payloadColumnCount() > 0) offset = binary(Op.USHR, offset, i64(1));
                counted.assign(count, load(new Offset(counts, offset)));
            });
            Expr repeated = counts == null ? repetitions : b.let(binary(Op.MUL, repetitions, count));
            b.loop(ne(links[index], i64(0)), duplicate -> {
                duplicate.when(eq(binary(Op.AND, pairs, i64(1023)), i64(0)), Builder::poll);
                duplicate.assign(pairs, add(pairs, i64(1)));
                expand(duplicate, updates, predicate, target, layout, lookups, heads, links, resumes, sets, inputs,
                        row, destination, matched, position, pairs, repeated, index + 1, false);
                Expr ordinal = binary(Op.USHR, binary(Op.SUB, links[index], i64(8)), i64(layout.builds[index].payloadColumnCount() > 0 ? 4 : 3));
                Expr next = load(new Offset(lookups[index].heap, binary(Op.MUL, ordinal, lookups[index].stride)));
                duplicate.assign(links[index], counts == null ? next : new Select(eq(counts, nil()), next, i64(0)));
            });
        }
    }

    private static void consume(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                                ResolvedVarchar.Frame frame, Var destination, Var matched, Var position, Layout layout, Var[] links, Expr repetitions) {
        if (predicate instanceof Pipeline.All) {
            update(b, updates, target, frame, destination, matched, position, layout, links, repetitions);
        } else {
            b.when(predicate.emit(frame.numeric(b)), accepted -> update(accepted, updates, target, frame, destination, matched, position, layout, links, repetitions));
        }
    }

    private static void update(Builder b, AggregateUpdates updates, Target target, ResolvedVarchar.Frame frame,
                               Var scalar, Var matched, Var position, Layout layout, Var[] links, Expr repetitions) {
        Var destination = scalar;
        if (target.probe != null) {
            if (layout.facts != null && layout.facts.groupCache) {
                Expr ordinal = binary(Op.USHR, binary(Op.SUB, links[target.groupBuild], i64(8)), i64(4));
                Var entry = b.let(new Offset(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.groupCacheOffset())),
                        binary(Op.MUL, ordinal, i64(8))));
                Var cached = b.let(new Load(MemoryType.ADDRESS, entry));
                b.when(eq(cached, nil()), miss -> {
                    miss.assign(cached, findGroup(miss, target, frame, position, layout, links));
                    miss.store(MemoryType.ADDRESS, entry, cached);
                });
                destination = cached;
            } else destination = findGroup(b, target, frame, position, layout, links);
        }
        // MATCHED distinguishes a fresh group from existing state, including NULL-only state.
        Var firstUpdate = updates.firstUpdateFlag(b, binary(Op.LT, matched, i64(0)));
        updates.initializeEmptyState(b, target.state == null ? destination : STATE, binary(Op.LT, matched, i64(0)),
                (first, seeds) -> {
                    first.append(target.state == null ? seeds : target.state.promote(seeds));
                    // Staging and symbol-resolution exits may resume this kernel.
                    first.store(at(COLUMNS, MATCHED), i64(1));
                });
        b.assign(matched, i64(1));
        if (target.state == null) updates.emitRepeated(b, frame, destination, repetitions, at(COLUMNS, layout.functionCardinalityOffset()), firstUpdate);
        else {
            Builder emitted = b.fork();
            updates.emitRepeated(emitted, frame, STATE, repetitions, nil(), firstUpdate);
            b.append(target.state.promote(emitted.block()));
        }
    }

    private static Var findGroup(Builder b, Target target, ResolvedVarchar.Frame frame,
                                 Var position, Layout layout, Var[] links) {
        Pipeline.Row row = frame.numeric(b);
        if (target.probe.variable()) {
            // Staging can grow before lookup. Preserve duplicate-chain coordinates too.
            b.store(at(STATE, MapKernelState.RESUME_ROW), position);
            for (int i = 0; i < links.length; i++) b.store(at(COLUMNS, layout.build(i) + RESUME_LINK), links[i]);
            b.store(at(STATE, MapKernelState.VariableKey.STAGING_USED), i64(0));
        }
        Var key = target.key != null ? b.let(target.key.emit(row).data())
                : target.probe.varchar() ? QuestVarchar.stage(b, target.varchar.emit(b, frame), at(STATE, groupKeyOffset(target.probe)))
                : target.probe.variable() ? target.probe.variableKey().stage(b, frame)
                : target.probe.orderedNative() ? target.probe.fixedKey().stage(b, row, at(STATE, groupKeyOffset(target.probe)))
                : target.probe.fixedKey().scalar(b, row);
        return target.probe.find(b, key, position, load(at(COLUMNS, SHARD_MASK)), missing -> {
            for (int i = 0; i < links.length; i++) missing.store(at(COLUMNS, layout.build(i) + RESUME_LINK), links[i]);
            missing.store(target.probe.stagedKeyType(), at(COLUMNS, MISSING_KEY), key);
        });
    }

    private static ResolvedVarchar.Frame loadRow(Builder b, AggregateUpdates updates, Layout layout,
                                       RowInputs inputs, Var row, Lookup[] lookups, Var[] links) {
        var values = new ArrayList<Pipeline.Value>();
        for (int i = 0; i < updates.columns().size(); i++) {
            var column = updates.columns().get(i);
            Var base = inputs.addresses.get(i);
            Var value = loadValue(b, layout, column.width(), column.nullLiteral(),
                    new Source(layout.sourceBuilds[i], layout.sourceColumns[i]),
                    at(COLUMNS, layout.arguments() + (long) ARGUMENT_BYTES * i), base, row, lookups, links);
            values.add(column.value(value));
        }
        var strings = new ArrayList<Pipeline.Utf8Value>();
        for (int i = 0; i < layout.strings.length; i++) {
            Source source = layout.strings[i];
            Expr descriptor = at(COLUMNS, layout.stringOffset() + i * 48L);
            if (source.build < 0) strings.add(QuestVarchar.decode(b, descriptor, row));
            else {
                Var id = payloadRowId(b, source.build, lookups, links);
                Expr offset = binary(Op.MUL, binary(Op.USHR, id, i64(44)), load(at(descriptor, 32)));
                Expr[] view = new Expr[4];
                for (int word = 0; word < view.length; word++) {
                    Expr entry = new Offset(new Load(MemoryType.ADDRESS, at(descriptor, word * 8L)), offset);
                    view[word] = b.let(new Load(word < 2 ? MemoryType.ADDRESS : MemoryType.I64, entry));
                }
                strings.add(QuestVarchar.decode(b, view[0], view[1], view[2], view[3], i64(0), i64(0),
                        binary(Op.AND, id, i64(0xfffffffffffL))));
            }
        }
        var frame = new ResolvedVarchar.Frame(new Pipeline.Row(updates.columns(), values, inputs.parameters),
                row, nil(), at(COLUMNS, layout.literalOffset()), nil(), null, strings);
        var numeric = frame.numeric(b);
        // Pure frame expressions are row inputs, not aggregate state operations. Materialize
        // before register promotion so their byte reads stay outside the update block.
        for (var expression : updates.inputs().expressions()) numeric.computed().apply(expression);
        return new ResolvedVarchar.Frame(numeric, row, nil(), frame.literals(), nil(), null, strings);
    }

    private static Var payloadRowId(Builder b, int build, Lookup[] lookups, Var[] links) {
        Expr ordinal = binary(Op.USHR, binary(Op.SUB, links[build], i64(8)), i64(4));
        return b.let(load(at(new Offset(lookups[build].heap, binary(Op.MUL, ordinal, lookups[build].stride)), 8)));
    }

    private static Var loadValue(Builder b, Layout layout, MemoryType width, Expr nullLiteral, Source source,
                                 Expr descriptor, Expr base, Var row, Lookup[] lookups, Var[] links) {
        if (source.build < 0) return b.let(new Load(width, new Offset(base, binary(Op.MUL, row, i64(width.bytes)))));
        Var heap = lookups[source.build].heap;
        Var link = links[source.build];
        if (layout.facts != null) {
            BuildShape shape = layout.facts.builds.get(source.build);
            if (shape.stride > 0) {
                Expr ordinal = binary(Op.USHR, binary(Op.SUB, link, i64(8)), i64(4));
                Expr offset = add(binary(Op.MUL, ordinal, i64(shape.stride)), i64(shape.columns.get(source.column)));
                return b.let(new Load(width, new Offset(heap, offset)));
            }
        }
        Var stride = b.let(layout.meta(at(descriptor, 8)));
        Var mode = b.let(layout.meta(at(descriptor, 16)));
        Var address = b.let(nil());
        b.when(ne(mode, i64(0)), copied -> {
            Expr ordinal = binary(Op.USHR, binary(Op.SUB, link, i64(8)), i64(4));
            copied.assign(address, new Offset(base, binary(Op.MUL, ordinal, stride)));
        });
        b.when(eq(mode, i64(0)), direct -> {
            // A payload-bearing heap row is [next link, row id]; link is biased by eight.
            Var id = payloadRowId(direct, source.build, lookups, links);
            Expr frame = binary(Op.USHR, id, i64(44));
            Var page = direct.let(new Load(MemoryType.ADDRESS, new Offset(base, binary(Op.MUL, frame, stride))));
            direct.when(ne(page, nil()), present -> present.assign(address,
                    new Offset(page, binary(Op.MUL, binary(Op.AND, id, i64(0xfffffffffffL)), i64(width.bytes)))));
        });
        Var value = b.let(nullLiteral);
        b.when(ne(address, nil()), present -> present.assign(value, new Load(width, address)));
        return value;
    }

    /** One owner for exact/filter code and the existing measured execution choices. */
    private static final class Programs implements AutoCloseable {
        private enum Membership { NONE, ORIGINAL, EXPANDED }

        private record Variant(CompiledKernel code, boolean originalMembership, Lookahead lookahead) implements Kernel {
            @Override public long run(long columns, long rows, long state) { return code.run(columns, rows, state); }
        }
        private final java.util.List<Variant> owned = new ArrayList<>();
        private final Variant first;
        // Rows admit group prefetch; columns select the available membership sets.
        private final ParallelAdaptiveKernel[][] choices;
        private final boolean membership;

        Programs(AggregateUpdates updates, Pipeline.Predicate predicate, Target target, Layout layout,
                 Backend backend, Path artifacts, boolean specialized, int parallelism) {
            boolean filter = false;
            for (int i = 0; i < layout.keys.length; i++) filter |= layout.canFilter(i);
            membership = filter;
            boolean promote = specialized && target.probe == null;
            for (var update : updates.updates()) promote &= !update.allocates() && !update.scalarState().isEmpty();
            var exact = new ArrayList<Variant>();
            var normal = new ArrayList<Variant>();
            var expanded = new ArrayList<Variant>();
            Layout uncached = layout.withoutDirectCaches();
            Layout[] lookups = uncached == layout ? new Layout[]{layout} : new Layout[]{layout, uncached};
            try {
                for (Layout selected : lookups) {
                    for (int state = 0; state < (promote ? 2 : 1); state++) {
                        boolean registers = state != 0;
                        Lookahead[] aheads = !specialized ? new Lookahead[]{Lookahead.NONE}
                                : canPrefetchGroup(target, selected) ? Lookahead.values() : new Lookahead[]{Lookahead.NONE, Lookahead.JOINS};
                        for (Lookahead ahead : aheads) {
                            CompiledKernel code = CompiledKernel.defer(backend,
                                    () -> program(updates, predicate, target, selected, false, registers, ahead), artifacts);
                            Variant plain = new Variant(code, false, ahead);
                            owned.add(plain);
                            exact.add(plain); normal.add(plain); expanded.add(plain);
                            if (filter) {
                                CompiledKernel filtered = CompiledKernel.defer(backend,
                                        () -> program(updates, predicate, target, selected, true, registers, ahead), artifacts);
                                Variant complete = new Variant(filtered, false, ahead);
                                owned.add(complete);
                                normal.add(complete);
                                expanded.add(new Variant(filtered, true, ahead));
                                expanded.add(complete);
                            }
                        }
                    }
                }
            } catch (Throwable failure) {
                try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            first = exact.getFirst();
            var alternatives = membership ? List.of(exact, normal, expanded) : List.of(exact);
            choices = new ParallelAdaptiveKernel[2][alternatives.size()];
            boolean groupPrefetch = exact.stream().anyMatch(variant -> variant.lookahead == Lookahead.JOINS_AND_GROUPS);
            for (int prefetch = 0; prefetch < choices.length; prefetch++) {
                if (prefetch == 1 && !groupPrefetch) {
                    choices[prefetch] = choices[0];
                    continue;
                }
                for (int mode = 0; mode < alternatives.size(); mode++) {
                    var variants = alternatives.get(mode);
                    choices[prefetch][mode] = choices(prefetch == 0 ? withoutGroupPrefetch(variants) : variants, parallelism);
                }
            }
        }

        private static java.util.List<Variant> withoutGroupPrefetch(java.util.List<Variant> variants) {
            return variants.stream().filter(variant -> variant.lookahead != Lookahead.JOINS_AND_GROUPS).toList();
        }

        private static ParallelAdaptiveKernel choices(java.util.List<Variant> variants, int parallelism) {
            return variants.size() > 1 ? new ParallelAdaptiveKernel(variants.toArray(Kernel[]::new), parallelism, System::nanoTime) : null;
        }

        void prepare() {
            // Optional lookahead compiles only when sampled. The existing settling
            // period excludes that first call from the policy's measured costs.
            for (var variant : owned) if (variant.lookahead == Lookahead.NONE) variant.code.prepare();
        }

        void runBatch(long rows, boolean filter, boolean expanded, boolean prefetchGroups, java.util.function.Consumer<Variant> work) {
            Membership mode = !filter || !membership ? Membership.NONE : expanded ? Membership.EXPANDED : Membership.ORIGINAL;
            ParallelAdaptiveKernel policy = choices[prefetchGroups ? 1 : 0][mode.ordinal()];
            if (policy == null) work.accept(first);
            else policy.runBatch(rows, variant -> work.accept((Variant) variant));
        }

        @Override public void close() {
            Throwable failure = null;
            for (var variant : owned) try { variant.code.close(); }
            catch (Throwable cleanup) { if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup); }
            owned.clear();
            io.questdb.cairo.CairoException.rethrowCleanupFailure(failure);
        }
    }

    private static final class Prepared implements CompiledJoin {
        private final Programs programs;
        private final boolean[] expanded;
        private boolean hasExpanded;
        private int updaterCount;
        private final Layout layout;
        private final QuestMapProbe probe;
        private final String name;
        private final IntegralMembership[] sets;
        private final DirectLongList[] mapViews;
        private final DirectLongList[] dependencyLinks;
        private final DirectLongList[] chainCounts;
        private final AggregateUpdates updates;
        private final Pipeline.Predicate predicate;
        private final Target target;
        private final long groupPrefetchThreshold;
        private final Backend backend;
        private final Path artifacts;
        private Facts cachedFacts;
        private boolean specializationReady;
        private Programs specializedPrograms;
        private boolean membershipReady;
        private boolean membershipCanReject;
        private boolean subtreesReady;
        private final JoinGroupCache groupCache = new JoinGroupCache();

        Prepared(Programs programs, Layout layout, QuestMapProbe probe, String name,
                 AggregateUpdates updates, Pipeline.Predicate predicate, Target target, Backend backend, Path artifacts) {
            this.updates = updates; this.predicate = predicate; this.target = target; this.backend = backend; this.artifacts = artifacts;
            // A packed value may begin anywhere within a cache line. Bound the lines it
            // touches, excluding cold key bytes, using the existing per-worker cache budget.
            long line = io.questdb.std.Misc.CACHE_LINE_SIZE;
            long footprint = (target.valueBytes + 2 * line - 2) / line * line;
            groupPrefetchThreshold = CacheSizing.LOCAL_AGGREGATE_BYTES / Math.max(1, footprint);
            this.programs = programs;
            expanded = new boolean[layout.builds.length];
            this.layout = layout;
            this.probe = probe;
            this.name = name;
            sets = new IntegralMembership[layout.builds.length];
            mapViews = new DirectLongList[sets.length];
            dependencyLinks = new DirectLongList[sets.length];
            chainCounts = new DirectLongList[sets.length];
            for (int i = 0; i < sets.length; i++) {
                sets[i] = new IntegralMembership();
            }
        }

        @Override
        public void clearExecution() {
            specializationReady = false;
            subtreesReady = false;
            membershipReady = false;
            membershipCanReject = false;
            hasExpanded = false;
            java.util.Arrays.fill(expanded, false);
            groupCache.close();
            for (var set : sets) set.close();
            for (int i = 0; i < mapViews.length; i++) {
                if (chainCounts[i] != null) { chainCounts[i].close(); chainCounts[i] = null; }
                if (dependencyLinks[i] != null) { dependencyLinks[i].close(); dependencyLinks[i] = null; }
                if (mapViews[i] != null) { mapViews[i].close(); mapViews[i] = null; }
            }
        }

        @Override
        public void close() {
            try { clearExecution(); }
            finally {
                try { programs.close(); }
                finally { closeSpecialization(); }
            }
        }

        @Override
        public void preparePayloads(FrozenHashJoinBuild[] builds, long probeRows, MemoryTracker tracker,
                                    SqlExecutionCircuitBreaker breaker) {
            groupCache.prepare(builds, target.groupBuild, probeRows, updaterCount, FACT_BYTES, tracker);
            if (tracker == null || tracker.getLimit() <= 0 || tracker.getLimit() - tracker.getUsed() >= FACT_BYTES) specialize(builds, tracker);
            else {
                programs.prepare();
            }
        }

        @Override
        public CharSequence getName() { return name; }

        private void closeSpecialization() {
            Programs old = specializedPrograms;
            specializedPrograms = null;
            cachedFacts = null;
            if (old != null) old.close();
        }

        private void specialize(FrozenHashJoinBuild[] builds, MemoryTracker tracker) {
            Facts facts = facts(builds, tracker);
            if (!facts.equals(cachedFacts)) {
                Programs next = new Programs(updates, predicate, target, layout.specialize(facts), backend, artifacts, true, Math.max(1, updaterCount));
                try {
                    next.prepare();
                    closeSpecialization();
                } catch (Throwable failure) {
                    try { next.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                    throw failure;
                }
                specializedPrograms = next;
                cachedFacts = facts;
            }
            specializationReady = true;
        }

        private Facts facts(FrozenHashJoinBuild[] builds, MemoryTracker tracker) {
            var constants = new java.util.HashMap<Long, Long>();
            var shapes = new java.util.ArrayList<BuildShape>();
            long descriptor = Unsafe.malloc(FACT_BYTES, MemoryTag.NATIVE_JIT, tracker);
            try {
                boolean unique = true;
                for (int i = 0; i < builds.length; i++) {
                    boolean one = builds[i].getRowCount() == builds[i].getKeyCount();
                    unique &= one;
                    long stride = rowStride(builds[i], layout.builds[i]);
                    var columns = inlineColumns(builds[i], layout.builds[i].payloadColumnCount(), descriptor);
                    shapes.add(new BuildShape(one, builds[i].writeIntegralLookup(descriptor), dependencyLinks[i] != null,
                            columns.isEmpty() ? 0 : stride, columns));
                    constants.put((long) layout.build(i) + HEAP_STRIDE, stride);
                    if (layout.keys[i].producer() >= 0) {
                        constants.put((long) layout.build(i) + DEPENDENCY_LINKS + 8, 4L);
                    }
                    for (int k = 0; k < layout.keys[i].widths.length; k++) {
                        Source source = layout.keys[i].sources[k];
                        if (source.build >= 0) payloadFacts(builds, source.build, source.column, layout.key(i, k), descriptor, constants);
                    }
                }
                constants.put((long) MODE, subtreesReady ? 2L : unique ? 1L : 0L);
                for (int i = 0; i < layout.sourceBuilds.length; i++) {
                    if (layout.sourceBuilds[i] >= 0) payloadFacts(builds, layout.sourceBuilds[i], layout.sourceColumns[i],
                            layout.arguments() + (long) ARGUMENT_BYTES * i, descriptor, constants);
                }
                return new Facts(java.util.Map.copyOf(constants), java.util.List.copyOf(shapes), groupCache.isAllocated());
            } finally { Unsafe.free(descriptor, FACT_BYTES, MemoryTag.NATIVE_JIT, tracker); }
        }

        /** Structural offsets only: addresses stay in the execution descriptors. */
        private static java.util.List<Integer> inlineColumns(FrozenHashJoinBuild build, int count, long descriptor) {
            int stride = build.getRowStride();
            if (stride <= 16 || count == 0 || build.getRowCount() == 0) return java.util.List.of();
            long base = build.getRowAddress(0);
            var columns = new ArrayList<Integer>(count);
            for (int column = 0; column < count; column++) {
                if (!build.writeNativePayload(column, descriptor) || Unsafe.getLong(descriptor + 16) != 1
                        || Unsafe.getLong(descriptor + 8) != stride) return java.util.List.of();
                long offset = Unsafe.getLong(descriptor) - base;
                if (offset < 16 || offset >= stride) return java.util.List.of();
                columns.add((int) offset);
            }
            return java.util.List.copyOf(columns);
        }

        private void payloadFacts(FrozenHashJoinBuild[] builds, int build, int column, long offset,
                                  long descriptor, java.util.Map<Long, Long> constants) {
            if (!builds[build].writeNativePayload(column, descriptor)) return;
            constants.put(offset + 8, Unsafe.getLong(descriptor + 8));
            constants.put(offset + 16, Unsafe.getLong(descriptor + 16));
        }

        private void checkRepetitions(FrozenHashJoinBuild[] builds, SqlExecutionCircuitBreaker breaker) {
            if (!layout.rowCountsOnly && !JoinDependencyFilter.repetitionsFit(builds, chainCounts, breaker)) {
                subtreesReady = false;
                for (int i = 0; i < chainCounts.length; i++) {
                    if (chainCounts[i] != null) { chainCounts[i].close(); chainCounts[i] = null; }
                }
            }
        }

        @Override
        public CompiledJoin.Updater newUpdater(io.questdb.griffin.engine.groupby.GroupByAllocator allocator, Runnable checkpoint) {
            return new Slot(this, allocator, checkpoint, updaterCount++);
        }

        private void runBatch(long rows, long arguments, boolean prefetchGroups, java.util.function.Consumer<Kernel> work) {
            Programs active = specializationReady ? specializedPrograms : programs;
            active.runBatch(rows, membershipCanReject, hasExpanded, prefetchGroups, variant -> {
                boolean original = variant.originalMembership;
                if (original) {
                    for (int i = 0; i < sets.length; i++) if (expanded[i]) {
                        Unsafe.putLong(arguments + layout.build(i) + MEMBERSHIP + 16, 0);
                    }
                }
                try { work.accept(variant); }
                finally {
                    if (original) for (int i = 0; i < sets.length; i++) if (expanded[i]) {
                        sets[i].write(arguments + layout.build(i) + MEMBERSHIP);
                    }
                }
            });
        }

        @Override
        public void prepareBuilds(FrozenHashJoinBuild[] builds, long probeRows, MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
            clearExecution();
            if (builds.length != sets.length) throw new IllegalArgumentException("Join build count changed");
            try {
                long keyBytes = 0, reserved = 0;
                for (int i = 0; i < builds.length; i++) {
                    var build = builds[i];
                    if (layout.keys[i].widths.length != 1) continue;
                    long localBudget = Math.max(0, build.getKeyCount()) * layout.keys[i].widths[0].bytes;
                    long bytes = IntegralMembership.joinBitmapBytes(build);
                    keyBytes += localBudget * 2;
                    // Unknown bounds may still produce an old-policy bitmap through iteration.
                    reserved += bytes < 0 ? localBudget : bytes <= localBudget ? bytes : 0;
                }
                // Allow one native key/head slot per distinct key across builds; reserve old filters.
                // One bit per known probe row also bounds added construction: expanded filters
                // cost >4 bytes/key, so at most one extra build key is visited per 32 probes.
                long extra = Math.max(0, Math.min(keyBytes, Math.max(0, probeRows) / 8) - reserved);
                if (tracker != null && tracker.getLimit() > 0) {
                    extra = Math.min(extra, Math.max(0, tracker.getLimit() - tracker.getUsed() - reserved));
                }
                for (int i = 0; i < sets.length; i++) {
                    int maps = builds[i].getNativeMapCount();
                    if (maps > 0) {
                        mapViews[i] = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
                        mapViews[i].setMemoryTracker(tracker);
                        mapViews[i].ensureCapacity((long) maps * NativeMapView.PROBE_BYTES / 8);
                        builds[i].writeNativeMapViews(mapViews[i].getAddress());
                    }
                    var build = builds[i];
                    // Bound the extra key scan when maps do not publish integral bounds.
                    if (!programs.membership || layout.keys[i].widths.length != 1
                            || build.getMinIntegralKey() > build.getMaxIntegralKey()
                            && (!(build instanceof FrozenHashJoinBuild.IntKeyed)
                                && (probeRows <= 0 || build.getKeyCount() > probeRows / 32))) {
                        sets[i].universal();
                        continue;
                    }
                    long budget = Math.max(0, build.getKeyCount()) * layout.keys[i].widths[0].bytes;
                    long bytes = IntegralMembership.joinBitmapBytes(build);
                    if (bytes > budget && bytes <= extra) {
                        budget = bytes;
                        extra -= bytes;
                    }
                    sets[i].prepareJoin(build, tracker, breaker, budget);
                    expanded[i] = sets[i].hasBitmap() && bytes > Math.max(0, builds[i].getKeyCount()) * layout.keys[i].widths[0].bytes;
                    hasExpanded |= expanded[i];
                    membershipCanReject |= !sets[i].isUniversal();
                }
                for (int i = 0; i < builds.length; i++) {
                    long rows = builds[i].getRowCount();
                    if (!layout.counts[i] || rows <= builds[i].getKeyCount() || probeRows <= 0 || rows > probeRows / 32
                            || tracker != null && tracker.getLimit() > 0
                                && rows * Long.BYTES > tracker.getLimit() - tracker.getUsed()) continue;
                    chainCounts[i] = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
                    chainCounts[i].setMemoryTracker(tracker);
                    chainCounts[i].ensureCapacity(rows);
                    if (!builds[i].copyKeyRowCounts(chainCounts[i].getAddress())) {
                        chainCounts[i].close();
                        chainCounts[i] = null;
                    }
                }
                checkRepetitions(builds, breaker);
                membershipReady = true;
            } catch (Throwable failure) {
                clearExecution();
                throw failure;
            }
        }
    }

    private static final class Slot implements CompiledJoin.Updater {
        private final Prepared prepared;
        private final KernelMemory memory;
        private final long bytes;
        private final long argumentBytes;
        private final JoinGroupCache.Slot groupCache;
        private final FrozenHashJoinBuild[] single = new FrozenHashJoinBuild[1];
        private long arguments;
        private long nativeRows;
        private long functionCardinality;
        private long mapMisses;
        private final io.questdb.std.str.DirectUtf8String keyString = new io.questdb.std.str.DirectUtf8String();
        private final SymbolKeyTranslator.View[][] translators;

        Slot(Prepared prepared, io.questdb.griffin.engine.groupby.GroupByAllocator allocator, Runnable checkpoint, int slotIndex) {
            this.prepared = prepared;
            groupCache = prepared.target.groupBuild >= 0 ? prepared.groupCache.new Slot(slotIndex) : null;
            translators = new SymbolKeyTranslator.View[prepared.layout.builds.length][];
            for (int i = 0; i < translators.length; i++) translators[i] = new SymbolKeyTranslator.View[prepared.layout.keys[i].widths.length];
            memory = KernelMemory.reusable(allocator, checkpoint);
            argumentBytes = prepared.layout.functionCardinalityOffset() + 8;
            bytes = argumentBytes + (prepared.probe == null ? 0
                    : groupKeyOffset(prepared.probe) + (prepared.probe.varchar() ? 24 : prepared.probe.orderedNative() ? prepared.probe.keyBytes() : 0));
        }

        @Override
        public void clearExecution() {
            arguments = Unsafe.free(arguments, bytes, MemoryTag.NATIVE_JIT);
            if (groupCache != null) groupCache.clear();
        }

        @Override
        public void close() { clearExecution(); }

        private void reopen() {
            if (arguments == 0) arguments = Unsafe.calloc(bytes, MemoryTag.NATIVE_JIT);
        }

        @Override
        public void bindSymbol(int build, int key, SymbolKeyTranslator.View translator) {
            translators[build][key] = translator;
        }

        @Override
        public void prepareDependencies(FrozenHashJoinBuild[] builds, long probeRows,
                                        MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
            Layout layout = prepared.layout;
            if (builds.length != layout.builds.length) return;
            for (var build : builds) if (build.getRowCount() == 0) return;
            for (int producer = builds.length - 2; producer >= 0; producer--) {
                if (!prepared.programs.membership || !layout.canReduce(producer)
                        || !JoinDependencyFilter.affordable(builds[producer], probeRows, tracker)) continue;
                var children = new ObjList<JoinDependencyFilter.Lookup>();
                try {
                    for (int child = producer + 1; child < builds.length; child++) {
                        if (layout.keys[child].producer() != producer) continue;
                        var lookup = dependencyLookup(builds[child], child);
                        if (lookup != null) children.add(lookup);
                    }
                    if (children.size() > 0 && JoinDependencyFilter.prepare(builds[producer],
                            layout.builds[producer].keyTypes(), children, prepared.sets[producer], tracker, breaker)) {
                        // Retain the exact alternative. Reduced membership owns its own representation;
                        // disabling an expanded bitmap must not reinterpret its Bloom storage.
                        prepared.expanded[producer] = false;
                        prepared.membershipCanReject = true;
                    }
                } finally {
                    for (int i = 0; i < children.size(); i++) children.getQuick(i).close();
                }
            }
            prepared.hasExpanded = false;
            for (boolean expanded : prepared.expanded) prepared.hasExpanded |= expanded;
            for (int child = 1; child < builds.length; child++) {
                int producer = layout.keys[child].producer();
                if (producer < 0 || !JoinDependencyFilter.affordableLinks(builds[producer], probeRows, tracker)) continue;
                try (var lookup = dependencyLookup(builds[child], child)) {
                    if (lookup != null) prepared.dependencyLinks[child] = JoinDependencyFilter.prepareLinks(
                            builds[producer], layout.builds[producer].keyTypes(), lookup, probeRows, tracker, breaker);
                }
            }
            if (layout.subtrees) {
                int[] parents = new int[builds.length];
                for (int i = 0; i < parents.length; i++) parents[i] = layout.keys[i].producer();
                prepared.subtreesReady = JoinDependencyFilter.prepareSubtrees(builds, layout.builds, parents,
                        prepared.chainCounts, prepared.dependencyLinks, tracker, breaker);
            }
            prepared.checkRepetitions(builds, breaker);

        }

        private JoinDependencyFilter.Lookup dependencyLookup(FrozenHashJoinBuild build, int child) {
            Layout layout = prepared.layout;
            int[] types = layout.builds[child].keyTypes();
            int[] columns = new int[types.length];
            for (int k = 0; k < columns.length; k++) {
                columns[k] = layout.keys[child].sources[k].column;
                if (types[k] == ColumnType.SYMBOL && translators[child][k] == null) return null;
            }
            return new JoinDependencyFilter.Lookup(build, columns, types, translators[child], prepared.sets[child]);
        }

        private static long resumePosition(long address, long rows) {
            long position = Unsafe.getLong(address);
            if (position < 0 || position >= rows) throw new IllegalStateException("Invalid join resume position: " + position);
            return position;
        }

        private void resolveSymbol() {
            int build = (int) Unsafe.getLong(arguments + SYMBOL_BUILD);
            int key = (int) Unsafe.getLong(arguments + SYMBOL_KEY);
            if (build < 0 || build >= translators.length || key < 0 || key >= translators[build].length
                    || translators[build][key] == null) throw new IllegalStateException("Invalid join symbol request");
            long descriptor = arguments + prepared.layout.key(build, key);
            Unsafe.putLong(descriptor + SYMBOL_RESULT, translators[build][key].translate((int) Unsafe.getLong(descriptor + SYMBOL_INPUT)));
        }

        @Override
        public long getNativeRows() { return nativeRows; }

        @Override
        public long getFunctionCardinality() { return functionCardinality; }

        @Override
        public long getNativeMapMisses() { return mapMisses; }

        private boolean bind(PageFrameMemoryRecord record, long rowIdsAddress, FrozenHashJoinBuild[] builds) {
            Layout layout = prepared.layout;
            if (builds.length != layout.builds.length) return false;
            Unsafe.putLong(arguments, rowIdsAddress);
            Unsafe.putLong(arguments + POSITION, 0);
            Unsafe.putLong(arguments + layout.functionCardinalityOffset(), 0);
            boolean unique = true;
            for (int i = 0; i < builds.length; i++) {
                if (prepared.subtreesReady && layout.keys[i].dependent()) continue;
                long views = prepared.mapViews[i] == null ? 0 : prepared.mapViews[i].getAddress();
                long lookup = arguments + layout.build(i) + LOOKUP;
                long direct = arguments + layout.build(i) + DIRECT;
                if (builds[i].writeIntegralLookup(direct)) {
                    for (int word = 0; word < 4; word++) Unsafe.putLong(lookup + word * 8L, 0);
                    Unsafe.putLong(lookup + 32, Unsafe.getLong(direct + 32));
                } else {
                    Unsafe.putLong(direct, 0);
                    if (!builds[i].writeLookup(lookup, views)) return false;
                }
                Unsafe.putLong(arguments + layout.build(i) + HEAP_STRIDE,
                        rowStride(builds[i], layout.builds[i]));
                for (int k = 0; k < layout.keys[i].widths.length; k++) {
                    long descriptor = arguments + layout.key(i, k);
                    Source source = layout.keys[i].sources[k];
                    if (source.build < 0) {
                        long key = record.getPageAddress(source.column);
                        if (key == 0) return false;
                        Unsafe.putLong(descriptor, key);
                    } else if (!writePayload(builds, source.build, source.column, descriptor)) return false;
                    if (layout.builds[i].keyTypes()[k] == ColumnType.SYMBOL) {
                        var translator = translators[i][k];
                        if (translator == null) return false;
                        Unsafe.putLong(descriptor + SYMBOL_CACHE, translator.getCacheAddress());
                        Unsafe.putLong(descriptor + SYMBOL_COUNT, translator.getCacheKeyCount());
                        Unsafe.putLong(descriptor + SYMBOL_INPUT, SymbolKeyTranslator.UNRESOLVED);
                        Unsafe.putLong(descriptor + SYMBOL_RESULT, SymbolKeyTranslator.UNRESOLVED);
                    }
                }
                Unsafe.putLong(arguments + layout.build(i) + RESUME_LINK, 0);
                int producer = layout.keys[i].producer();
                if (producer >= 0) {
                    Unsafe.putLong(arguments + layout.build(i) + DEPENDENCY_LINKS,
                            prepared.dependencyLinks[i] == null ? 0 : prepared.dependencyLinks[i].getAddress());
                    Unsafe.putLong(arguments + layout.build(i) + DEPENDENCY_LINKS + 8, 4);
                }
                if (layout.hasCounts(i)) Unsafe.putLong(arguments + layout.counts(i),
                        prepared.chainCounts[i] == null ? 0 : prepared.chainCounts[i].getAddress());
                unique &= builds[i].getKeyCount() == builds[i].getRowCount();
                long membership = arguments + layout.build(i) + MEMBERSHIP;
                if (prepared.membershipReady) prepared.sets[i].write(membership);
                else {
                    // Direct callers may omit preparation; exact joins still decide membership.
                    Unsafe.putLong(membership, Integer.MIN_VALUE);
                    Unsafe.putLong(membership + 8, Integer.MAX_VALUE);
                    Unsafe.putLong(membership + 16, 0);
                    Unsafe.putLong(membership + 24, 0);
                }
            }
            for (int i = 0; i < layout.sourceBuilds.length; i++) {
                long descriptor = arguments + layout.arguments() + (long) ARGUMENT_BYTES * i;
                int build = layout.sourceBuilds[i];
                int column = layout.sourceColumns[i];
                if (build < 0) {
                    long address = record.getPageAddress(column);
                    if (address == 0) return false;
                    Unsafe.putLong(descriptor, address);
                } else if (!writePayload(builds, build, column, descriptor)) {
                    return false;
                }
            }
            for (int i = 0; i < layout.strings.length; i++) {
                Source source = layout.strings[i];
                long descriptor = arguments + layout.stringOffset() + i * 48L;
                if (source.build < 0) {
                    if (!record.writeNativeVarcharView(source.column, descriptor)) return false;
                } else if (!builds[source.build].writeNativeVarcharPayload(source.column, descriptor)) return false;
            }
            for (int i = 0; i < layout.literals.length; i++)
                Unsafe.getUnsafe().putByte(arguments + layout.literalOffset() + i, layout.literals[i]);
            // Function.init resolves these IDs against this cursor's symbol sources before
            // worker publication. Rebind on every frame; cached code must not retain old IDs.
            for (int i = 0; i < layout.parameters.size(); i++)
                Unsafe.putInt(arguments + layout.parameterOffset() + i * 8L, layout.parameters.get(i).getAsInt());
            Unsafe.putLong(arguments + MODE, prepared.subtreesReady ? 2 : unique ? 1 : 0);
            Unsafe.putLong(arguments + MATCHED, 0);
            return true;
        }

        private boolean writePayload(FrozenHashJoinBuild[] builds, int build, int column, long descriptor) {
            return builds[build].writeNativePayload(column, descriptor);
        }

        private boolean empty(FrozenHashJoinBuild[] builds) {
            for (var build : builds) if (build.getRowCount() == 0) return true;
            return false;
        }

        @Override
        public boolean update(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                              FrozenHashJoinBuild build, MapValue value) {
            single[0] = build;
            try { return updateMulti(record, rowCount, rowIdsAddress, single, value); }
            finally { single[0] = null; }
        }

        @Override
        public boolean updateKeyed(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                   FrozenHashJoinBuild build, Map map, ObjList<Map> shards,
                                   GroupByFunctionsUpdater functions) {
            single[0] = build;
            try { return updateKeyedMulti(record, rowCount, rowIdsAddress, single, map, shards, functions); }
            finally { single[0] = null; }
        }

        @Override
        public boolean updateMulti(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                              FrozenHashJoinBuild[] builds, MapValue value) {
            if (prepared.probe != null) return false;
            if (builds.length != prepared.layout.builds.length) return false;
            if (rowCount == 0 || empty(builds)) return true;
            reopen();
            try {
                if (!bind(record, rowIdsAddress, builds)) return false;
                Unsafe.putLong(arguments + MATCHED, value.isNew() ? -1 : 0);
                memory.enter(null);
                try {
                    prepared.runBatch(rowCount, arguments, false, kernel -> {
                        while (true) {
                            long status = kernel.run(arguments, rowCount, value.getAddress(0));
                            memory.throwIfFailed();
                            if (status == 0) break;
                            if (status != NEED_SYMBOL) throw new IllegalStateException("Join kernel failed: " + status);
                            resumePosition(arguments + POSITION, rowCount);
                            resolveSymbol();
                        }
                    });
                } finally {
                    memory.close();
                }
                if (Unsafe.getLong(arguments + MATCHED) > 0) value.setNew(false);
                nativeRows += rowCount;
                return true;
            } finally {
                Unsafe.setMemory(arguments, bytes, (byte) 0);
            }
        }

        @Override
        public boolean updateKeyedMulti(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                   FrozenHashJoinBuild[] builds, Map map, ObjList<Map> shards,
                                   GroupByFunctionsUpdater functions) {
            functionCardinality = 0;
            if (prepared.probe == null) return false;
            if (builds.length != prepared.layout.builds.length) return false;
            if (rowCount == 0 || empty(builds)) return true;
            reopen();
            final long state = arguments + argumentBytes;
            try {
                if (!bind(record, rowIdsAddress, builds)) return false;
                int count = shards == null ? 1 : shards.size();
                if (shards != null && count != GroupByMapFragment.NUM_SHARDS) return false;
                long groups = 0;
                for (int i = 0; i < count; i++) {
                    Map current = shards == null ? map : shards.getQuick(i);
                    current.writeNativeProbeView(state + prepared.probe.controlBytes() + (long) i * QuestMapProbe.VIEW_BYTES);
                    if (prepared.groupCache.isAllocated()) groups += current.size();
                }
                boolean prefetchGroups = groups > prepared.groupPrefetchThreshold;
                if (groupCache != null) Unsafe.putLong(arguments + prepared.layout.groupCacheOffset(),
                        groupCache.bind(state + prepared.probe.controlBytes(), count));
                if (prepared.probe.variable()) {
                    ((io.questdb.cairo.map.OrderedMap) map).writeNativeKeyStagingView(
                            state + MapKernelState.VariableKey.STAGING_ADDRESS, prepared.probe.variableKey().minimumBytes());
                }
                Unsafe.putLong(arguments + SHARD_MASK, count - 1);
                Unsafe.putLong(arguments + POSITION, 0);
                for (int i = 0; i < builds.length; i++) Unsafe.putLong(arguments + prepared.layout.build(i) + RESUME_LINK, 0);
                memory.enter(null);
                try {
                    prepared.runBatch(rowCount, arguments, prefetchGroups, kernel -> {
                        while (true) {
                            long status = kernel.run(arguments, rowCount, state);
                            memory.throwIfFailed();
                            if (status == 0) break;
                            if (status == NEED_SYMBOL) { resumePosition(arguments + POSITION, rowCount); resolveSymbol(); continue; }
                            if (status == MapKernelState.NEED_STAGING && prepared.probe.variable()) {
                                long resume = resumePosition(state + MapKernelState.RESUME_ROW, rowCount);
                                ((io.questdb.cairo.map.OrderedMap) map).writeNativeKeyStagingView(
                                        state + MapKernelState.VariableKey.STAGING_ADDRESS,
                                        Unsafe.getLong(state + MapKernelState.VariableKey.REQUIRED_BYTES));
                                Unsafe.putLong(arguments + POSITION, resume);
                                continue;
                            }
                            if (status != MapKernelState.NEED_KEY) throw new IllegalStateException("Join map kernel failed: " + status);
                            // A miss has not updated its match. Keep its probe and duplicate positions;
                            // the ordinary map remains the only owner of insertion and growth.
                            Unsafe.putLong(arguments + POSITION, resumePosition(state + MapKernelState.RESUME_ROW, rowCount));
                            long rawKey = Unsafe.getLong(arguments + MISSING_KEY);
                            MapKey key;
                            if (prepared.probe.orderedNative()) {
                                key = ((io.questdb.cairo.map.OrderedMap) map).withRawKey(
                                        rawKey + (prepared.probe.variable() ? 4 : 0),
                                        prepared.probe.variable() ? Unsafe.getInt(rawKey) : prepared.probe.keyBytes());
                            } else if (prepared.probe.varchar()) {
                                long header = Unsafe.getLong(rawKey);
                                int size = (int) (header >>> 32) & 0x3fffffff;
                                long pointer = Unsafe.getLong(rawKey + 8) & Long.MAX_VALUE;
                                key = map.withKey();
                                key.putVarchar((header & 0x4000000000000000L) != 0 ? null
                                        : keyString.of(pointer, pointer + size, (header & Long.MIN_VALUE) != 0));
                            } else {
                                key = map.withKey();
                                if (prepared.probe.keyType() == ColumnType.INT) key.putInt((int) rawKey);
                                else key.putLong(rawKey);
                            }
                            key.commit();
                            long hash = key.hash();
                            int shardIndex = shards == null ? 0 : (int) (hash >>> GroupByMapFragment.NUM_SHARDS_SHR);
                            if (shardIndex != Unsafe.getLong(state + MapKernelState.SHARD)) throw new IllegalStateException("Join map hash ABI mismatch");
                            Map target = shards == null ? map : shards.getQuick(shardIndex);
                            if (target != map) {
                                MapKey targetKey = target.withKey();
                                targetKey.copyFrom(key);
                                key = targetKey;
                            }
                            MapValue value = key.createValue(hash);
                            if (!value.isNew()) throw new IllegalStateException("Join probe missed an existing group");
                            functions.updateEmpty(value);
                            // Freshness belongs to the inserted group, independently of its aggregate family.
                            Unsafe.putLong(arguments + MATCHED, -1);
                            mapMisses++;
                            // OrderedMap scratch-key writes may also move shard zero's heap.
                            map.writeNativeProbeView(state + prepared.probe.controlBytes());
                            target.writeNativeProbeView(state + prepared.probe.controlBytes() + (long) shardIndex * QuestMapProbe.VIEW_BYTES);
                            if (groupCache != null) groupCache.inserted(state + prepared.probe.controlBytes(), shardIndex);
                        }
                    });
                } finally {
                    memory.close();
                }
                functionCardinality = Unsafe.getLong(arguments + prepared.layout.functionCardinalityOffset());
                nativeRows += rowCount;
                return true;
            } finally {
                Unsafe.setMemory(arguments, bytes, (byte) 0);
            }
        }
    }
}
