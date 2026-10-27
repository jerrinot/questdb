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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.NativeMapView;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.griffin.engine.table.GroupByMapFragment;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.questdb.experimental.codegen.Ir.*;

/** Emits the existing QuestDB map's hash and probe algorithm over a borrowed snapshot. */
final class QuestMapProbe {
    private enum Layout { FIXED, VARCHAR, ORDERED_LOOKUP, ORDERED_FIXED, ORDERED_VARIABLE }

    private final int column;
    private final int keyType;
    private final Layout layout;
    private final boolean mutable;
    private final QuestFixedKey fixedKey;
    private final QuestVariableKey variableKey;

    private QuestMapProbe(int column, int keyType, Layout layout, boolean mutable,
                          QuestFixedKey fixedKey, QuestVariableKey variableKey) {
        this.column = column;
        this.keyType = keyType;
        this.layout = layout;
        this.mutable = mutable;
        this.fixedKey = fixedKey;
        this.variableKey = variableKey;
    }

    static QuestMapProbe direct(int column, int type, boolean ordered) {
        if (type != ColumnType.INT && type != ColumnType.LONG && (ordered || type != ColumnType.VARCHAR)) {
            throw new IllegalArgumentException("Unsupported direct map key type: " + type);
        }
        return new QuestMapProbe(column, type, ordered ? Layout.ORDERED_LOOKUP
                : type == ColumnType.VARCHAR ? Layout.VARCHAR : Layout.FIXED, false, null, null);
    }

    static QuestMapProbe fixed(QuestFixedKey key, int type, boolean ordered) {
        java.util.Objects.requireNonNull(key);
        if (type == ColumnType.SYMBOL) type = ColumnType.INT;
        if (!ordered && type != ColumnType.INT && type != ColumnType.LONG) {
            throw new IllegalArgumentException("Unsupported fixed map key type: " + type);
        }
        return new QuestMapProbe(-1, type, ordered ? Layout.ORDERED_FIXED : Layout.FIXED, false, key, null);
    }

    static QuestMapProbe variable(QuestVariableKey key) {
        return new QuestMapProbe(-1, -1, Layout.ORDERED_VARIABLE, false, null, java.util.Objects.requireNonNull(key));
    }

    int column() { return column; }
    int keyType() { return keyType; }
    boolean mutable() { return mutable; }
    QuestFixedKey fixedKey() { return fixedKey; }
    QuestVariableKey variableKey() { return variableKey; }
    boolean variable() { return layout == Layout.ORDERED_VARIABLE; }
    boolean varchar() { return layout == Layout.VARCHAR; }
    boolean ordered() { return layout == Layout.ORDERED_LOOKUP || orderedNative(); }
    boolean orderedNative() { return layout == Layout.ORDERED_FIXED || variable(); }
    Expr valueAddress(Expr entry) { return variable() ? QuestVariableKey.value(entry) : at(entry, keyBytes()); }
    Expr keysEqual(Builder b, Expr entry, Expr key) {
        return variable() ? QuestVariableKey.equal(b, entry, key) : fixedKey.equal(entry, key);
    }
    int keyBytes() { return variable() ? 0 : varchar() ? 16 : fixedKey != null ? fixedKey.bytes() : keyType == ColumnType.INT ? 4 : 8; }
    int prefetchRowBytes() { return (orderedNative() ? 40 : 24) + (varchar() ? 24 : variable() ? 0 : orderedNative() ? (fixedKey.bytes() + 7) & ~7 : 0); }
    MemoryType stagedKeyType() { return orderedNative() || varchar() ? MemoryType.ADDRESS : MemoryType.I64; }
    int controlBytes() { return variable() ? 104 : varchar() ? 88 : CONTROL_BYTES; }
    int viewBytes() { return mutable ? NativeMapView.MUTATION_BYTES : NativeMapView.PROBE_BYTES; }

    QuestMapProbe mutating() {
        if (layout == Layout.ORDERED_LOOKUP) throw new IllegalStateException("Ordered map mutation is not bound");
        return new QuestMapProbe(column, keyType, layout, true, fixedKey, variableKey);
    }

    static final int CONTROL_BYTES = 64;
    static final int VIEW_BYTES = NativeMapView.PROBE_BYTES;
    static final int NEED_KEY = 4;
    private static final boolean PROFILE_PROBES = Boolean.getBoolean("query.codegen.probeCounters");

    static QuestMapProbe bind(int column, int type, Map map) {
        // A direct SYMBOL key is the table's stable 32-bit dictionary ID. The map
        // and result cursor retain the existing symbol table and NULL (-1) contract.
        if (type == ColumnType.SYMBOL || type == ColumnType.IPv4) type = ColumnType.INT;
        else if (type == ColumnType.DATE || ColumnType.isTimestamp(type)) type = ColumnType.LONG;
        if (column >= 0 && type == ColumnType.VARCHAR && map instanceof io.questdb.cairo.map.UnorderedVarcharMap) {
            return direct(column, type, false);
        }
        if (column < 0 || (type != ColumnType.INT && type != ColumnType.LONG)) return null;
        if (map instanceof OrderedMap || type == ColumnType.INT && map instanceof Unordered4Map
                || type == ColumnType.LONG && map instanceof Unordered8Map) {
            return direct(column, type, map instanceof OrderedMap);
        }
        return null;
    }

    Var loadFrameKey(Builder b, Var row) { return loadFrameKey(b, row, null); }

    Var loadFrameKey(Builder b, Var row, Expr scratch) {
        if (variable()) return variableKey.stage(b, row);
        if (varchar()) return QuestVarchar.stage(b, row, scratch);
        if (fixedKey != null) return orderedNative() ? fixedKey.stage(b, row, scratch) : fixedKey.scalar(b, row);
        MemoryType width = keyType == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
        return b.let(new Load(width, new Offset(new Load(MemoryType.ADDRESS, at(STATE, 16)),
                binary(Op.MUL, row, i64(width.bytes)))));
    }

    /** The producer supplies a key in its SQL storage representation, including its null sentinel. */
    Var find(Builder b, Var inputKey, Var position) {
        return find(b, inputKey, position, null, null);
    }

    Var find(Builder b, Var inputKey, Var position, Expr shardMask, Consumer<Builder> beforeMiss) {
        return find(b, inputKey, position, shardMask, beforeMiss, null);
    }

    Var find(Builder b, Var inputKey, Var position, Expr shardMask, Consumer<Builder> beforeMiss,
             BiConsumer<Builder, Expr> initialize) {
        if (varchar()) return QuestVarchar.find(this, b, inputKey, position, shardMask, beforeMiss, initialize);
        if (orderedNative()) return QuestOrderedMap.find(this, b, inputKey, position, shardMask, beforeMiss, initialize);
        MemoryType width = keyType == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
        Lookup lookup = lookup(b, inputKey, shardMask);
        Var key = lookup.key(), hash = lookup.hash(), shard = lookup.shard(), view = lookup.view();
        Var base = lookup.base(), mask = lookup.mask(), slot = lookup.slot();
        Var destination = b.let(nil());
        if (ordered()) {
            Var offsets = b.let(new Load(MemoryType.ADDRESS, at(view, NativeMapView.OFFSETS)));
            b.loop(eq(destination, nil()), probe -> {
                count(probe, 40);
                Var packed = probe.let(load(new Offset(offsets, binary(Op.MUL, slot, i64(8)))));
                Var rawOffset = probe.let(binary(Op.AND, packed, i64(0xffff_ffffL)));
                probe.when(eq(rawOffset, i64(0)), missing -> miss(missing, position, shard, beforeMiss));
                probe.when(eq(binary(Op.USHR, packed, i64(32)), binary(Op.AND, hash, i64(0xffff_ffffL))), sameHash -> {
                    count(sameHash, 48);
                    Var entry = sameHash.let(new Offset(base, binary(Op.MUL, binary(Op.SUB, rawOffset, i64(1)), i64(8))));
                    sameHash.when(eq(new Load(width, entry), key), found -> found.assign(destination, at(entry, width.bytes)));
                });
                probe.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
            });
        } else {
            b.when(eq(key, i64(0)), zero -> {
                zero.when(eq(load(at(view, NativeMapView.HAS_ZERO)), i64(0)), missing -> {
                    if (mutable) {
                        initialize.accept(missing, at(new Load(MemoryType.ADDRESS, at(view, NativeMapView.ZERO_ENTRY)), width.bytes));
                        missing.store(at(view, NativeMapView.HAS_ZERO), i64(1));
                    } else miss(missing, position, shard, beforeMiss);
                });
                zero.assign(destination, at(new Load(MemoryType.ADDRESS, at(view, NativeMapView.ZERO_ENTRY)), width.bytes));
            });
            b.when(ne(key, i64(0)), nonzero -> {
                Var stride = nonzero.let(load(at(view, NativeMapView.STRIDE)));
                nonzero.loop(eq(destination, nil()), probe -> {
                    count(probe, 40);
                    Var entry = probe.let(new Offset(base, binary(Op.MUL, slot, stride)));
                    Var stored = probe.let(new Load(width, entry));
                    probe.when(eq(stored, i64(0)), missing -> {
                        if (mutable) {
                            // Grow before committing this row: a return never replays an update.
                            missing.when(binary(Op.LT, load(at(view, NativeMapView.FREE)), i64(2)), full -> miss(full, position, shard, beforeMiss));
                            initialize.accept(missing, at(entry, width.bytes));
                            missing.store(width, entry, key);
                            missing.store(at(view, NativeMapView.FREE), binary(Op.SUB, load(at(view, NativeMapView.FREE)), i64(1)));
                            missing.store(at(view, NativeMapView.SIZE), add(load(at(view, NativeMapView.SIZE)), i64(1)));
                            missing.assign(stored, key);
                        } else miss(missing, position, shard, beforeMiss);
                    });
                    count(probe, 48);
                    probe.when(eq(stored, key), found -> found.assign(destination, at(entry, width.bytes)));
                    probe.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
                });
            });
        }
        return destination;
    }

    record Candidate(Var key, Var entry, Var hash, Var slot) {
        Candidate(Var key, Var entry) { this(key, entry, null, null); }
    }

    /** Resolve/prefetch a candidate once; full key validation is deferred to consumption. */
    Candidate prepare(Builder b, Var key, long valueBytes, BiConsumer<Builder, Var> stateHint) {
        return prepare(b, key, valueBytes, stateHint, null);
    }

    Candidate prepare(Builder b, Var key, long valueBytes, BiConsumer<Builder, Var> stateHint, Var hash) {
        if (varchar()) return QuestVarchar.prepare(this, b, key, valueBytes, stateHint);
        Lookup lookup = hash == null ? lookup(b, key) : lookupHashed(b, key, load(at(STATE, 56)), hash);
        int width = keyBytes();
        Var entry = b.let(nil());
        if (ordered()) {
            Var offsets = b.let(new Load(MemoryType.ADDRESS, at(lookup.view(), NativeMapView.OFFSETS)));
            Var done = b.let(bool(false));
            b.loop(eq(done, bool(false)), probe -> {
                count(probe, 40);
                Var packed = probe.let(load(new Offset(offsets, binary(Op.MUL, lookup.slot(), i64(8)))));
                Var raw = probe.let(binary(Op.AND, packed, i64(0xffff_ffffL)));
                probe.assign(done, eq(raw, i64(0)));
                probe.when(binary(Op.BOTH, ne(raw, i64(0)),
                        eq(binary(Op.USHR, packed, i64(32)), binary(Op.AND, lookup.hash(), i64(0xffff_ffffL)))), occupied -> {
                    occupied.assign(entry, new Offset(lookup.base(), binary(Op.MUL, binary(Op.SUB, raw, i64(1)), i64(8))));
                    occupied.assign(done, bool(true));
                });
                probe.when(eq(done, bool(false)), advance ->
                        advance.assign(lookup.slot(), binary(Op.AND, add(lookup.slot(), i64(1)), lookup.mask())));
            });
        } else {
            b.when(eq(lookup.key(), i64(0)), zero -> {
                zero.when(ne(load(at(lookup.view(), NativeMapView.HAS_ZERO)), i64(0)), present ->
                        present.assign(entry, new Load(MemoryType.ADDRESS, at(lookup.view(), NativeMapView.ZERO_ENTRY))));
            });
            b.when(ne(lookup.key(), i64(0)), nonzero -> {
                Var stride = nonzero.let(load(at(lookup.view(), NativeMapView.STRIDE)));
                nonzero.assign(entry, new Offset(lookup.base(), binary(Op.MUL, lookup.slot(), stride)));
            });
        }
        b.when(ne(entry, nil()), candidate -> {
            prefetchEntry(candidate, entry, valueBytes);
            if (ordered()) {
                // A hash collision still refers to a live entry with the same aggregate layout.
                stateHint.accept(candidate, candidate.let(valueAddress(entry)));
            } else {
                candidate.when(binary(Op.EITHER, eq(lookup.key(), i64(0)),
                        ne(new Load(keyType == ColumnType.INT ? MemoryType.I32 : MemoryType.I64, entry), i64(0))), occupied ->
                        stateHint.accept(occupied, occupied.let(at(entry, width))));
            }
        });
        return new Candidate(lookup.key(), entry, lookup.hash(), lookup.slot());
    }

    /** Metadata slots are prefetched before the dependent entry lookup stage. */
    Var prefetchOffset(Builder b, Var key) {
        Var hash = hash(b, key);
        prefetchOffset(b, key, hash);
        return hash;
    }

    void prefetchOffset(Builder b, Var key, Var hash) {
        Lookup lookup = lookupHashed(b, key, load(at(STATE, 56)), hash);
        Expr offsets = new Load(MemoryType.ADDRESS, at(lookup.view(), NativeMapView.OFFSETS));
        b.prefetch(new Offset(offsets, binary(Op.MUL, lookup.slot(), i64(8))), true);
    }

    Var finishOrdered(Builder b, Var key, Var entry, Var hash, Var slot, Var position,
                      BiConsumer<Builder, Expr> initialize) {
        Var destination = b.let(nil());
        b.when(ne(entry, nil()), candidate -> {
            count(candidate, 48);
            candidate.when(keysEqual(candidate, entry, key), hit -> hit.assign(destination, valueAddress(entry)));
        });
        b.when(eq(destination, nil()), missing -> missing.assign(destination,
                QuestOrderedMap.findPrepared(this, missing, key, hash, slot, position, initialize)));
        return destination;
    }

    Var finish(Builder b, Var key, Var entry, Var position) {
        return finish(b, key, entry, position, null);
    }

    Var finish(Builder b, Var key, Var entry, Var position, BiConsumer<Builder, Expr> initialize) {
        MemoryType width = keyType == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
        Var destination = b.let(nil());
        b.when(ne(entry, nil()), candidate -> {
            if (!ordered()) count(candidate, 40);
            count(candidate, 48);
            candidate.when(varchar() ? QuestVarchar.equal(candidate, entry, key) : orderedNative() ? keysEqual(candidate, entry, key) : eq(new Load(width, entry), key), hit -> hit.assign(destination, valueAddress(entry)));
        });
        // Empty entries and hash/key collisions use the exact existing lookup/miss path.
        b.when(eq(destination, nil()), fallback -> fallback.assign(destination, find(fallback, key, position, mutable ? load(at(STATE, 56)) : null, null, initialize)));
        return destination;
    }

    private void prefetchEntry(Builder b, Var entry, long valueBytes) {
        if (variable()) {
            b.prefetch(entry, true);
            b.prefetch(valueAddress(entry), true);
            if (valueBytes > 1) b.prefetch(at(valueAddress(entry), valueBytes - 1), true);
            return;
        }
        long bytes = keyBytes() + valueBytes;
        for (long offset = 0; offset < bytes; offset += 64) b.prefetch(at(entry, offset), true);
        // Entries need not start on a cache line; cover the final line as well.
        if (bytes > 1) b.prefetch(at(entry, bytes - 1), true);
    }

    record Lookup(Var key, Var hash, Var shard, Var view, Var base, Var mask, Var slot) { }

    private Lookup lookup(Builder b, Var key) {
        return lookup(b, key, mutable ? load(at(STATE, 56)) : null);
    }

    Lookup lookup(Builder b, Var key, Expr shardMask) {
        return lookupHashed(b, key, shardMask, hash(b, key));
    }

    Lookup lookupHashed(Builder b, Var key, Expr shardMask, Var hash) {
        Var shard = b.let(binary(Op.USHR, hash, i64(GroupByMapFragment.NUM_SHARDS_SHR)));
        if (shardMask != null) b.assign(shard, binary(Op.AND, shard, shardMask));
        Var view = b.let(new Offset(at(STATE, controlBytes()), binary(Op.MUL, shard, i64(viewBytes()))));
        Var base = b.let(new Load(MemoryType.ADDRESS, view));
        Var mask = b.let(load(at(view, NativeMapView.MASK)));
        Var slot = b.let(binary(Op.AND, hash, mask));
        return new Lookup(key, hash, shard, view, base, mask, slot);
    }

    Var hash(Builder b, Expr key) {
        if (variable()) return QuestVariableKey.hash(b, key);
        if (varchar()) return b.let(load(at(key, 16)));
        if (orderedNative()) return fixedKey.hash(b, key);
        // Hash.hashInt64 widens unsigned; OrderedMap's four-byte hashMem64 tail is signed.
        Var hash = b.let(keyType == ColumnType.INT && !ordered() ? binary(Op.AND, key, i64(0xffff_ffffL)) : key);
        b.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(37))));
        b.assign(hash, binary(Op.MUL, hash, i64(0x165667919E3779F9L)));
        b.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(32))));
        return hash;
    }

    static void count(Builder b, long offset) {
        if (PROFILE_PROBES) b.store(at(STATE, offset), add(load(at(STATE, offset)), i64(1)));
    }

    static void miss(Builder b, Var position, Var shard, Consumer<Builder> beforeMiss) {
        if (beforeMiss != null) beforeMiss.accept(b);
        b.store(at(STATE, 24), shard);
        b.store(at(STATE, 32), position);
        b.exit(NEED_KEY);
    }
}
