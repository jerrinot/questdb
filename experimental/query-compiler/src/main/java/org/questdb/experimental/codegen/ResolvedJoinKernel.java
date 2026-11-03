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

import static org.questdb.experimental.codegen.Ir.*;

/** Generated typed joins, predicates and updates over engine-owned scalar or keyed state. */
final class ResolvedJoinKernel {
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
    private static final int KEYS = MEMBERSHIP + IntegralMembership.BYTES;
    private static final int DEPENDENCY_LINKS = KEYS;
    private static final int KEY_BYTES = 56;
    private static final int SYMBOL_CACHE = 24, SYMBOL_COUNT = 32, SYMBOL_INPUT = 40, SYMBOL_RESULT = 48;
    private static final int ARGUMENT_BYTES = HashJoinPayloadSource.NATIVE_COLUMN_BYTES;
    private static final int GROUP_KEY = MapKernelState.CONTROL_BYTES + GroupByMapFragment.NUM_SHARDS * QuestMapProbe.VIEW_BYTES;

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

        static Key bind(CompiledJoin.Build build, int probeColumns, CompiledJoin.Build[] builds, int available) {
            MemoryType[] widths = new MemoryType[build.keyTypes().length];
            int bytes = 0;
            Source[] sources = new Source[widths.length];
            for (int i = 0; i < widths.length; i++) {
                sources[i] = Source.bind(build.keyColumns()[i], probeColumns, builds, available);
                if (sources[i] == null) return null;
                widths[i] = switch (build.keyTypes()[i]) {
                    case ColumnType.INT, ColumnType.SYMBOL -> MemoryType.I32;
                    case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> MemoryType.I64;
                    default -> null;
                };
                if (widths[i] == null) return null;
                bytes += widths[i].bytes;
            }
            QuestMapProbe map = null;
            if (widths.length > 1) {
                var fixed = new QuestFixedKey(null, java.util.List.of(), widths, bytes, java.util.List.of());
                map = QuestMapProbe.fixed(fixed, -1, true);
            } else if (widths[0] == MemoryType.I64) {
                // TIMESTAMP_NS uses OrderedMap in the ordinary builder.
                if (build.keyTypes()[0] == ColumnType.TIMESTAMP_NANO) {
                    var fixed = new QuestFixedKey(null, java.util.List.of(), widths, bytes, java.util.List.of());
                    map = QuestMapProbe.fixed(fixed, -1, true);
                } else map = QuestMapProbe.direct(0, ColumnType.LONG, false);
            }
            return new Key(widths, bytes, map, sources);
        }
    }

    private record Layout(CompiledJoin.Build[] builds, Key[] keys, int[] sourceBuilds, int[] sourceColumns,
                          boolean[] counts, boolean subtrees) {
        int build(int index) {
            int offset = BUILDS;
            for (int i = 0; i < index; i++) offset += (keyStart(i) + keys[i].widths.length * KEY_BYTES + keys[i].bytes + 7) & ~7;
            return offset;
        }
        boolean hasCounts(int build) { return counts[build] || subtrees && !keys[build].dependent(); }
        int keyStart(int build) { return KEYS + (keys[build].producer() >= 0 ? 8 : 0) + (hasCounts(build) ? 8 : 0); }
        int counts(int build) { return build(build) + KEYS + (keys[build].producer() >= 0 ? 8 : 0); }
        int key(int build, int key) { return build(build) + keyStart(build) + key * KEY_BYTES; }
        int scratch(int build) { return key(build, keys[build].widths.length); }
        int arguments() { return build(builds.length); }
        boolean dependent() {
            for (Key key : keys) if (key.dependent()) return true;
            return false;
        }
        boolean canReduce(int producer) {
            if (keys[producer].widths.length != 1) return false;
            for (Key key : keys) if (key.producer() == producer) return true;
            return false;
        }
    }

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
        var inputs = new ResolvedInputs(joinedTypes);
        var predicate = inputs.predicate(postJoinFilter);
        if (predicate == null) return null;
        QuestMapProbe mapProbe = null;
        Pipeline.Column groupKey = null;
        if (groupLayout != null) {
            if (keyTypes.getColumnCount() == 1 && keyColumns.size() == 1 && keyColumns.getQuick(0) > 0
                    && (keyFunctions == null || keyFunctions.size() == 0)) {
                int index = keyColumns.getQuick(0) - 1;
                if (index >= joinedTypes.getColumnCount() || joinedTypes.getColumnType(index) != keyTypes.getColumnType(0)) return null;
                mapProbe = QuestMapProbe.bind(index, keyTypes.getColumnType(0), groupLayout);
                if (mapProbe != null) groupKey = inputs.column(index, keyTypes.getColumnType(0));
            }
            if (mapProbe == null) {
                var key = QuestFixedKey.bind(inputs, keyTypes, keyColumns, keyFunctions, groupLayout);
                // Joined rows have numeric cells; dictionary-derived keys need execution preparation.
                if (key == null || !key.dictionaries().isEmpty() || !key.frameInputs().expressions().isEmpty()) return null;
                mapProbe = QuestMapProbe.fixed(key, keyTypes.getColumnCount() == 1 ? keyTypes.getColumnType(0) : -1,
                        groupLayout instanceof io.questdb.cairo.map.OrderedMap);
            }
        }
        var packed = new FlyweightPackedMapValue(valueTypes);
        long base = layout != null ? layout.getAddress(0) : 0;
        var updates = AggregateUpdates.bind(valueTypes, functions,
                index -> groupLayout != null ? packed.getOffset(index) : layout.getAddress(index) - base, false, inputs);
        if (updates == null) return null;
        if (!updates.inputs().expressions().isEmpty()) return null;
        // Joined rows currently carry numeric cells, not VARCHAR frame descriptors.
        for (var update : updates.updates()) if (update instanceof AggregateUpdates.VarcharCount) return null;
        var target = new Target(mapProbe, groupKey);
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
        boolean[] counts = new boolean[builds.length];
        boolean subtrees = updates.countsRowsOnly();
        boolean dependent = false;
        if (updates.countsRowsOnly()) {
            java.util.Arrays.fill(counts, true);
            // Binding has collected every aggregate, predicate and grouping input.
            // A later join key also observes its producer, even when COUNT itself does not.
            for (int owner : sourceBuilds) if (owner >= 0) { counts[owner] = false; subtrees = false; }
            for (Key key : keys) {
                for (Source source : key.sources) if (source.build >= 0) counts[source.build] = false;
                if (key.dependent()) {
                    dependent = true;
                    subtrees &= key.producer() >= 0;
                }
            }
        }
        var descriptors = new Layout(builds, keys, sourceBuilds, sourceColumns, counts, subtrees && dependent);
        var code = backend.compile(program(updates, predicate, target, descriptors, false), artifacts);
        CompiledKernel membership = null;
        try {
            for (Key key : keys) {
                if (key.widths.length == 1) {
                    membership = backend.compile(program(updates, predicate, target, descriptors, true), artifacts);
                    break;
                }
            }
            return new Prepared(code, membership, descriptors, mapProbe, backend.name());
        } catch (IOException | RuntimeException | Error failure) {
            try { code.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            if (membership != null) try { membership.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static Program program(AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                                   Layout layout, boolean membership) {
        Builder b = new Builder();
        if (layout.subtrees) b.when(eq(load(at(COLUMNS, MODE)), i64(2)), folded -> {
            emitScan(folded, updates, predicate, target, layout, Iteration.SUBTREES, membership);
            folded.exit(0);
        });
        b.when(ne(load(at(COLUMNS, MODE)), i64(0)), unique -> emitScan(unique, updates, predicate, target, layout, Iteration.UNIQUE, membership));
        b.when(eq(load(at(COLUMNS, MODE)), i64(0)), multiple -> {
            Expr counted = bool(false);
            for (int i = 0; i < layout.counts.length; i++) if (layout.counts[i]) {
                counted = binary(Op.EITHER, counted, ne(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.counts(i))), nil()));
            }
            // Unique leaves and declined preparation retain the ordinary loop. Choose once
            // per frame instead of paying unused multiplicity checks for every joined row.
            Var hasCounts = multiple.let(counted);
            multiple.when(eq(hasCounts, bool(false)), ordinary -> emitScan(ordinary, updates, predicate, target, layout, Iteration.EXPANDED, membership));
            multiple.when(hasCounts, folded -> emitScan(folded, updates, predicate, target, layout, Iteration.COUNTED, membership));
        });
        b.exit(0);
        return new Program(b.block());
    }

    private record Target(QuestMapProbe probe, Pipeline.Column key) { }
    private record Lookup(Var table, Var mask, Var directory, Var shift, Var heap, Var counts) { }
    private enum Iteration { UNIQUE, EXPANDED, COUNTED, SUBTREES }

    private static void emitScan(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate,
                                 Target target, Layout layout, Iteration iteration, boolean membership) {
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
                            ? b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.counts(i)))) : null);
            if (membership) sets[i] = IntegralMembership.bind(b, at(COLUMNS, offset + MEMBERSHIP));
            links[i] = b.let(i64(0));
            if ((target.probe != null || layout.dependent()) && iteration != Iteration.UNIQUE && iteration != Iteration.SUBTREES) {
                resumes[i] = b.let(load(at(COLUMNS, offset + RESUME_LINK)));
            }
        }
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < updates.columns().size(); i++)
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.arguments() + (long) ARGUMENT_BYTES * i))));
        Var destination = b.let(STATE);
        Var position = b.let(load(at(COLUMNS, POSITION)));
        Var matched = b.let(load(at(COLUMNS, MATCHED)));
        Var pairs = b.let(i64(0));
        b.loop(binary(Op.LT, position, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, position, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(position);
            loop.when(ne(rowIds, nil()), filtered -> filtered.assign(row,
                    load(new Offset(rowIds, binary(Op.MUL, position, i64(8))))));
            Var[] heads = new Var[lookups.length];
            Var[] keys = new Var[lookups.length];
            loadKeys(loop, layout, keys, 0, row, position, matched, lookups, links, loaded ->
                    filterAll(loaded, keys, sets, layout, 0, eligible ->
                            lookupAll(eligible, lookups, heads, keys, layout, 0, found -> {
                                if (iteration == Iteration.SUBTREES) {
                                    countSubtrees(found, updates, predicate, target, layout, lookups, heads, links, addresses,
                                            row, destination, matched, position, i64(1), 0);
                                } else expand(found, updates, predicate, target, layout, lookups, heads, links, resumes, sets, addresses,
                                        row, destination, matched, position, pairs, i64(1), 0, iteration == Iteration.UNIQUE);
                            })));
            loop.assign(position, add(position, i64(1)));
        });
        b.store(at(COLUMNS, MATCHED), matched);
    }

    private static void countSubtrees(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                                      Layout layout, Lookup[] lookups, Var[] heads, Var[] links, ArrayList<Var> addresses,
                                      Var row, Var destination, Var matched, Var position, Expr repetitions, int index) {
        if (index == links.length) {
            consume(b, updates, predicate, target, loadRow(b, updates, layout, addresses, row, lookups, links),
                    destination, matched, position, layout, links, repetitions);
            return;
        }
        if (layout.keys[index].dependent()) {
            countSubtrees(b, updates, predicate, target, layout, lookups, heads, links, addresses,
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
        b.when(ne(count, i64(0)), present -> countSubtrees(present, updates, predicate, target, layout,
                lookups, heads, links, addresses, row, destination, matched, position,
                present.let(binary(Op.MUL, repetitions, count)), index + 1));
    }

    private static void loadKeys(Builder b, Layout layout, Var[] keys, int index, Var row,
                                 Var position, Var matched, Lookup[] lookups, Var[] links,
                                 java.util.function.Consumer<Builder> loaded) {
        if (index == keys.length) { loaded.accept(b); return; }
        if (layout.keys[index].dependent()) {
            loadKeys(b, layout, keys, index + 1, row, position, matched, lookups, links, loaded);
        } else {
            loadKey(b, layout, index, row, position, matched, lookups, links, (hit, key) -> {
                keys[index] = key;
                loadKeys(hit, layout, keys, index + 1, row, position, matched, lookups, links, loaded);
            });
        }
    }

    private static void loadKey(Builder b, Layout layout, int index, Var row, Var position, Var matched,
                                Lookup[] lookups, Var[] links, java.util.function.BiConsumer<Builder, Var> loaded) {
        Key shape = layout.keys[index];
        Var accepted = b.let(bool(true));
        Var result = null;
        int offset = 0;
        for (int k = 0; k < shape.widths.length; k++) {
            int descriptor = layout.key(index, k);
            Var key = loadValue(b, shape.widths[k], i64(shape.widths[k] == MemoryType.I32 ? Integer.MIN_VALUE : Long.MIN_VALUE),
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
                            missing.store(at(COLUMNS, POSITION), position);
                            missing.store(at(COLUMNS, MATCHED), matched);
                            // A dependent dictionary miss occurs inside duplicate expansion. Resume
                            // its selected prefix, without replaying pairs already consumed.
                            if (shape.dependent()) for (int i = 0; i < links.length; i++) {
                                missing.store(at(COLUMNS, layout.build(i) + RESUME_LINK), i < buildIndex ? links[i] : i64(0));
                            }
                            missing.store(at(COLUMNS, SYMBOL_BUILD), i64(buildIndex));
                            missing.store(at(COLUMNS, SYMBOL_KEY), i64(keyIndex));
                            missing.store(at(COLUMNS, descriptor + SYMBOL_INPUT), key);
                            missing.exit(NEED_SYMBOL);
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
        if (!layout.keys[index].dependent() && layout.keys[index].widths.length == 1) {
            Expr key = layout.keys[index].map != null && layout.keys[index].map.orderedNative()
                    ? new Load(layout.keys[index].widths[0], keys[index]) : keys[index];
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
                               IntegralMembership.Descriptor[] sets, ArrayList<Var> addresses,
                               Var row, Var destination, Var matched, Var position, Var pairs, Expr repetitions, int index, boolean unique) {
        if (index == links.length) {
            consume(b, updates, predicate, target, loadRow(b, updates, layout, addresses, row, lookups, links),
                    destination, matched, position, layout, links, repetitions);
            return;
        }
        if (!layout.keys[index].dependent()) {
            expandMatches(b, updates, predicate, target, layout, lookups, heads, links, resumes, sets, addresses,
                    row, destination, matched, position, pairs, repetitions, index, unique);
            return;
        }
        int producer = layout.keys[index].producer();
        Var head = b.let(i64(0));
        Var cached = producer < 0 ? null
                : b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, layout.build(index) + DEPENDENCY_LINKS)));
        if (cached != null) b.when(ne(cached, nil()), ready -> {
            // A payload-bearing producer heap has sixteen-byte rows, biased by eight.
            Expr offset = binary(Op.USHR, binary(Op.SUB, links[producer], i64(8)), i64(2));
            Var compressed = ready.let(new Load(MemoryType.I32, new Offset(cached, offset)));
            ready.assign(head, binary(Op.MUL, binary(Op.AND, compressed, i64(0xffff_ffffL)), i64(8)));
        });
        java.util.function.Consumer<Builder> ordinary = fallback ->
                loadKey(fallback, layout, index, row, position, matched, lookups, links, (loaded, key) -> {
                    java.util.function.Consumer<Builder> probe = eligible ->
                            lookupOne(eligible, lookups, heads, key, layout, index, hit -> hit.assign(head, heads[index]));
                    if (sets != null && layout.keys[index].widths.length == 1) {
                        Expr value = layout.keys[index].map != null && layout.keys[index].map.orderedNative()
                                ? new Load(layout.keys[index].widths[0], key) : key;
                        loaded.when(IntegralMembership.containsJoin(loaded, sets[index], value, layout.canReduce(index)), probe);
                    } else probe.accept(loaded);
                });
        if (cached == null) ordinary.accept(b);
        else b.when(eq(cached, nil()), ordinary);
        // Join both lookup paths before emitting the consumer, so code size does not
        // double at every dependency in a longer chain.
        heads[index] = head;
        b.when(ne(head, i64(0)), hit -> expandMatches(hit, updates, predicate, target, layout, lookups, heads,
                links, resumes, sets, addresses, row, destination, matched, position, pairs, repetitions, index, unique));
    }

    private static void expandMatches(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                               Layout layout, Lookup[] lookups, Var[] heads, Var[] links, Var[] resumes, IntegralMembership.Descriptor[] sets, ArrayList<Var> addresses,
                               Var row, Var destination, Var matched, Var position, Var pairs, Expr repetitions, int index, boolean unique) {
        b.assign(links[index], heads[index]);
        if (unique) {
            expand(b, updates, predicate, target, layout, lookups, heads, links, resumes, sets, addresses,
                    row, destination, matched, position, pairs, repetitions, index + 1, true);
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
                expand(duplicate, updates, predicate, target, layout, lookups, heads, links, resumes, sets, addresses,
                        row, destination, matched, position, pairs, repeated, index + 1, false);
                Expr next = load(new Offset(lookups[index].heap, binary(Op.SUB, links[index], i64(8))));
                duplicate.assign(links[index], counts == null ? next : new Select(eq(counts, nil()), next, i64(0)));
            });
        }
    }

    private static void consume(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                                Pipeline.Row row, Var destination, Var matched, Var position, Layout layout, Var[] links, Expr repetitions) {
        if (predicate instanceof Pipeline.All) {
            update(b, updates, target, row, destination, matched, position, layout, links, repetitions);
        } else {
            b.when(predicate.emit(row), accepted -> update(accepted, updates, target, row, destination, matched, position, layout, links, repetitions));
        }
    }

    private static void update(Builder b, AggregateUpdates updates, Target target, Pipeline.Row row,
                               Var scalar, Var matched, Var position, Layout layout, Var[] links, Expr repetitions) {
        Var destination = scalar;
        if (target.probe != null) {
            Var key = target.key != null ? b.let(target.key.emit(row).data())
                    : target.probe.orderedNative() ? target.probe.fixedKey().stage(b, row, at(STATE, GROUP_KEY))
                    : target.probe.fixedKey().scalar(b, row);
            destination = target.probe.find(b, key, position, load(at(COLUMNS, SHARD_MASK)), missing -> {
                for (int i = 0; i < links.length; i++) missing.store(at(COLUMNS, layout.build(i) + RESUME_LINK), links[i]);
                missing.store(target.probe.orderedNative() ? MemoryType.ADDRESS : MemoryType.I64, at(COLUMNS, MISSING_KEY), key);
            });
        }
        b.assign(matched, i64(1));
        if (updates.countsRowsOnly()) updates.addCountedRows(b, destination, repetitions);
        else updates.emit(b, row, destination, nil());
    }

    private static Pipeline.Row loadRow(Builder b, AggregateUpdates updates, Layout layout,
                                       ArrayList<Var> addresses, Var row, Lookup[] lookups, Var[] links) {
        var values = new ArrayList<Pipeline.Value>();
        for (int i = 0; i < updates.columns().size(); i++) {
            var column = updates.columns().get(i);
            Var base = addresses.get(i);
            Var value = loadValue(b, column.width(), column.nullLiteral(),
                    new Source(layout.sourceBuilds[i], layout.sourceColumns[i]),
                    at(COLUMNS, layout.arguments() + (long) ARGUMENT_BYTES * i), base, row, lookups, links);
            values.add(column.value(value));
        }
        return new Pipeline.Row(updates.columns(), values);
    }

    private static Var loadValue(Builder b, MemoryType width, Expr nullLiteral, Source source,
                                 Expr descriptor, Expr base, Var row, Lookup[] lookups, Var[] links) {
        if (source.build < 0) return b.let(new Load(width, new Offset(base, binary(Op.MUL, row, i64(width.bytes)))));
        Var heap = lookups[source.build].heap;
        Var link = links[source.build];
        Var stride = b.let(load(at(descriptor, 8)));
        Var mode = b.let(load(at(descriptor, 16)));
        Var address = b.let(nil());
        b.when(ne(mode, i64(0)), copied -> {
            Expr ordinal = binary(Op.USHR, binary(Op.SUB, link, i64(8)), i64(4));
            copied.assign(address, new Offset(base, binary(Op.MUL, ordinal, stride)));
        });
        b.when(eq(mode, i64(0)), direct -> {
            // A payload-bearing heap row is [next link, row id]; link is biased by eight.
            Var id = direct.let(load(new Offset(heap, link)));
            Expr frame = binary(Op.USHR, id, i64(44));
            Var page = direct.let(new Load(MemoryType.ADDRESS, new Offset(base, binary(Op.MUL, frame, stride))));
            direct.when(ne(page, nil()), present -> present.assign(address,
                    new Offset(page, binary(Op.MUL, binary(Op.AND, id, i64(0xfffffffffffL)), i64(width.bytes)))));
        });
        Var value = b.let(nullLiteral);
        b.when(ne(address, nil()), present -> present.assign(value, new Load(width, address)));
        return value;
    }

    private static final class Prepared implements CompiledJoin {
        private final CompiledKernel code;
        private final CompiledKernel membership;
        private final Kernel originalMembership;
        private final AdaptiveKernel choices;
        private final AdaptiveKernel expandedChoices;
        private final boolean[] expanded;
        private boolean hasExpanded;
        private final Layout layout;
        private final QuestMapProbe probe;
        private final String name;
        private final IntegralMembership[] sets;
        private final DirectLongList[] mapViews;
        private final DirectLongList[] dependencyLinks;
        private final DirectLongList[] chainCounts;
        private boolean membershipReady;
        private boolean membershipCanReject;
        private boolean subtreesReady;

        Prepared(CompiledKernel code, CompiledKernel membership, Layout layout, QuestMapProbe probe, String name) {
            this.code = code;
            this.membership = membership;
            // Distinct execution choices share the membership machine code. The descriptor
            // change surrounds a whole frame, including every map-growth retry.
            originalMembership = membership == null ? null : (columns, rows, state) -> membership.run(columns, rows, state);
            choices = membership == null ? null : new AdaptiveKernel(code, membership, System::nanoTime);
            expandedChoices = membership == null ? null : new AdaptiveKernel(
                    new Kernel[]{code, originalMembership, membership}, System::nanoTime);
            expanded = new boolean[layout.builds.length];
            this.layout = layout;
            this.probe = probe;
            this.name = name;
            sets = new IntegralMembership[layout.builds.length];
            mapViews = new DirectLongList[sets.length];
            dependencyLinks = new DirectLongList[sets.length];
            chainCounts = new DirectLongList[sets.length];
            for (int i = 0; i < sets.length; i++) sets[i] = new IntegralMembership();
        }

        @Override
        public void clearExecution() {
            subtreesReady = false;
            membershipReady = false;
            membershipCanReject = false;
            hasExpanded = false;
            java.util.Arrays.fill(expanded, false);
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
                try { code.close(); }
                finally { if (membership != null) membership.close(); }
            }
        }

        @Override
        public CharSequence getName() { return name; }

        @Override
        public CompiledJoin.Updater newUpdater(Runnable checkpoint) { return new Slot(this, checkpoint); }

        private void runBatch(long rows, long arguments, java.util.function.Consumer<Kernel> work) {
            if (membership == null || !membershipCanReject) {
                work.accept(code);
                return;
            }
            (hasExpanded ? expandedChoices : choices).runBatch(rows, kernel -> {
                boolean original = kernel == originalMembership;
                if (original) {
                    for (int i = 0; i < sets.length; i++) if (expanded[i]) {
                        Unsafe.putLong(arguments + layout.build(i) + MEMBERSHIP + 16, 0);
                    }
                }
                try { work.accept(kernel); }
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
                    if (membership == null || layout.keys[i].widths.length != 1
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
        private final FrozenHashJoinBuild[] single = new FrozenHashJoinBuild[1];
        private long arguments;
        private long nativeRows;
        private long mapMisses;
        private final SymbolKeyTranslator.View[][] translators;

        Slot(Prepared prepared, Runnable checkpoint) {
            this.prepared = prepared;
            translators = new SymbolKeyTranslator.View[prepared.layout.builds.length][];
            for (int i = 0; i < translators.length; i++) translators[i] = new SymbolKeyTranslator.View[prepared.layout.keys[i].widths.length];
            memory = KernelMemory.reusable(checkpoint);
            argumentBytes = prepared.layout.arguments() + (long) ARGUMENT_BYTES * prepared.layout.sourceBuilds.length;
            bytes = argumentBytes + (prepared.probe == null ? 0
                    : GROUP_KEY + (prepared.probe.orderedNative() ? prepared.probe.keyBytes() : 0));
            arguments = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT);
        }

        @Override
        public void close() { arguments = Unsafe.free(arguments, bytes, MemoryTag.NATIVE_JIT); }

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
                if (prepared.membership == null || !layout.canReduce(producer)
                        || !JoinDependencyFilter.affordable(builds[producer], probeRows, tracker)) continue;
                var children = new ObjList<JoinDependencyFilter.Lookup>();
                try {
                    for (int child = producer + 1; child < builds.length; child++) {
                        if (layout.keys[child].producer() != producer) continue;
                        var lookup = dependencyLookup(builds[child], child);
                        if (lookup != null) children.add(lookup);
                    }
                    if (children.size() > 0 && JoinDependencyFilter.prepare(builds[producer],
                            layout.builds[producer].keyTypes()[0], children, prepared.sets[producer], tracker, breaker)) {
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

        private void resolveSymbol() {
            int build = (int) Unsafe.getLong(arguments + SYMBOL_BUILD);
            int key = (int) Unsafe.getLong(arguments + SYMBOL_KEY);
            long descriptor = arguments + prepared.layout.key(build, key);
            Unsafe.putLong(descriptor + SYMBOL_RESULT, translators[build][key].translate((int) Unsafe.getLong(descriptor + SYMBOL_INPUT)));
        }

        @Override
        public long getNativeRows() { return nativeRows; }

        @Override
        public long getNativeMapMisses() { return mapMisses; }

        private boolean bind(PageFrameMemoryRecord record, long rowIdsAddress, FrozenHashJoinBuild[] builds) {
            Layout layout = prepared.layout;
            if (builds.length != layout.builds.length) return false;
            Unsafe.putLong(arguments, rowIdsAddress);
            Unsafe.putLong(arguments + POSITION, 0);
            boolean unique = true;
            for (int i = 0; i < builds.length; i++) {
                if (prepared.subtreesReady && layout.keys[i].dependent()) continue;
                long views = prepared.mapViews[i] == null ? 0 : prepared.mapViews[i].getAddress();
                if (!builds[i].writeLookup(arguments + layout.build(i) + LOOKUP, views)) return false;
                for (int k = 0; k < layout.keys[i].widths.length; k++) {
                    long descriptor = arguments + layout.key(i, k);
                    Source source = layout.keys[i].sources[k];
                    if (source.build < 0) {
                        long key = record.getPageAddress(source.column);
                        if (key == 0) return false;
                        Unsafe.putLong(descriptor, key);
                    } else if (!builds[source.build].writeNativePayload(source.column, descriptor)) return false;
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
                if (layout.keys[i].producer() >= 0) Unsafe.putLong(arguments + layout.build(i) + DEPENDENCY_LINKS,
                        prepared.dependencyLinks[i] == null ? 0 : prepared.dependencyLinks[i].getAddress());
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
                } else if (!builds[build].writeNativePayload(column, descriptor)) {
                    return false;
                }
            }
            Unsafe.putLong(arguments + MODE, prepared.subtreesReady ? 2 : unique ? 1 : 0);
            Unsafe.putLong(arguments + MATCHED, 0);
            return true;
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
            try {
                if (!bind(record, rowIdsAddress, builds)) return false;
                memory.enter(null);
                try {
                    prepared.runBatch(rowCount, arguments, kernel -> {
                        while (true) {
                            long status = kernel.run(arguments, rowCount, value.getAddress(0));
                            memory.throwIfFailed();
                            if (status == 0) break;
                            if (status != NEED_SYMBOL) throw new IllegalStateException("Join kernel failed: " + status);
                            resolveSymbol();
                        }
                    });
                } finally {
                    memory.close();
                }
                if (Unsafe.getLong(arguments + MATCHED) != 0) value.setNew(false);
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
            if (prepared.probe == null) return false;
            if (builds.length != prepared.layout.builds.length) return false;
            if (rowCount == 0 || empty(builds)) return true;
            final long state = arguments + argumentBytes;
            try {
                if (!bind(record, rowIdsAddress, builds)) return false;
                int count = shards == null ? 1 : shards.size();
                if (shards != null && count != GroupByMapFragment.NUM_SHARDS) return false;
                for (int i = 0; i < count; i++) {
                    Map current = shards == null ? map : shards.getQuick(i);
                    current.writeNativeProbeView(state + MapKernelState.CONTROL_BYTES + (long) i * QuestMapProbe.VIEW_BYTES);
                }
                Unsafe.putLong(arguments + SHARD_MASK, count - 1);
                Unsafe.putLong(arguments + POSITION, 0);
                for (int i = 0; i < builds.length; i++) Unsafe.putLong(arguments + prepared.layout.build(i) + RESUME_LINK, 0);
                memory.enter(null);
                try {
                    prepared.runBatch(rowCount, arguments, kernel -> {
                        while (true) {
                            long status = kernel.run(arguments, rowCount, state);
                            memory.throwIfFailed();
                            if (status == 0) break;
                            if (status == NEED_SYMBOL) { resolveSymbol(); continue; }
                            if (status != MapKernelState.NEED_KEY) throw new IllegalStateException("Join map kernel failed: " + status);
                            // A miss has not updated its match. Keep its probe and duplicate positions;
                            // the ordinary map remains the only owner of insertion and growth.
                            Unsafe.putLong(arguments + POSITION, Unsafe.getLong(state + MapKernelState.RESUME_ROW));
                            long rawKey = Unsafe.getLong(arguments + MISSING_KEY);
                            MapKey key;
                            if (prepared.probe.orderedNative()) {
                                key = ((io.questdb.cairo.map.OrderedMap) map).withRawKey(rawKey, prepared.probe.keyBytes());
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
                            mapMisses++;
                            // OrderedMap scratch-key writes may also move shard zero's heap.
                            map.writeNativeProbeView(state + MapKernelState.CONTROL_BYTES);
                            target.writeNativeProbeView(state + MapKernelState.CONTROL_BYTES + (long) shardIndex * QuestMapProbe.VIEW_BYTES);
                        }
                    });
                } finally {
                    memory.close();
                }
                nativeRows += rowCount;
                return true;
            } finally {
                Unsafe.setMemory(arguments, bytes, (byte) 0);
            }
        }
    }
}
