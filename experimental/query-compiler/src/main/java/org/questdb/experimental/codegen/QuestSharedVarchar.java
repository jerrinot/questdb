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

import io.questdb.cairo.map.NativeMapView;

import static org.questdb.experimental.codegen.Ir.*;

/** Shared final VARCHAR maps: exact existing keys, publication, probing and COUNT updates. */
final class QuestSharedVarchar {
    static final int NEED_MAP = 12;
    static final int COUNTS = 64;
    static final int KEYS = COUNTS + 256 * 8;
    static final int WINDOW = 32;
    // Private cache entry: packed key header, stable key pointer, destination entry,
    // pending count. Copy the key rather than reading the concurrently updated line.
    static final int CACHE = KEYS + WINDOW * 24;
    static final int CACHE_ENTRIES = 4096;
    static final int SCRATCH_BYTES = CACHE + CACHE_ENTRIES * 32;

    private QuestSharedVarchar() { }

    static Program program() {
        Builder b = new Builder();
        Var views = b.let(new Load(MemoryType.ADDRESS, at(STATE, 24)));
        Var counts = b.let(at(STATE, COUNTS));
        Var clear = b.let(i64(0));
        b.loop(binary(Op.LT, clear, i64(256)), zero -> {
            zero.store(new Offset(counts, binary(Op.MUL, clear, i64(8))), i64(0));
            zero.assign(clear, add(clear, i64(1)));
        });
        Var cacheClear = b.let(i64(0));
        b.loop(binary(Op.LT, cacheClear, i64(CACHE_ENTRIES)), zero -> {
            zero.store(new Offset(at(STATE, CACHE), binary(Op.MUL, cacheClear, i64(32))), i64(0));
            zero.assign(cacheClear, add(cacheClear, i64(1)));
        });
        Var index = b.let(load(STATE));
        Var rowIds = b.let(new Load(MemoryType.ADDRESS, at(STATE, 8)));
        b.loop(binary(Op.LT, index, ROWS), batch -> {
            batch.poll();
            Var end = batch.let(new Select(binary(Op.LT, ROWS, add(index, i64(WINDOW))), ROWS, add(index, i64(WINDOW))));
            Var prepare = batch.let(index);
            batch.loop(binary(Op.LT, prepare, end), stage -> {
                Var row = stage.let(prepare);
                stage.when(ne(rowIds, nil()), selected -> selected.assign(row,
                        load(new Offset(rowIds, binary(Op.MUL, prepare, i64(8))))));
                Var key = QuestVarchar.stage(stage, row, new Offset(at(STATE, KEYS),
                        binary(Op.MUL, binary(Op.SUB, prepare, index), i64(24))));
                Var hash = stage.let(load(at(key, 16)));
                Var view = stage.let(new Offset(views, binary(Op.MUL, binary(Op.USHR, hash, i64(56)), i64(64))));
                Var base = stage.let(new Offset(nil(), new AtomicLoad(view, true)));
                stage.when(binary(Op.BOTH, ne(base, nil()), ne(load(cache(hash)), load(key))), present -> present.prefetch(new Offset(base,
                        binary(Op.MUL, binary(Op.AND, hash, load(at(view, NativeMapView.MASK))), i64(24))), true));
                stage.assign(prepare, add(prepare, i64(1)));
            });
            Var window = batch.let(index);
            batch.loop(binary(Op.LT, index, end), outer -> {
                Var key = outer.let(new Offset(at(STATE, KEYS), binary(Op.MUL, binary(Op.SUB, index, window), i64(24))));
                Var hash = outer.let(load(at(key, 16)));
                Var cached = outer.let(cache(hash));
                Var cachedHeader = outer.let(load(cached));
                Var hit = QuestVarchar.equal(outer, cached, key, cachedHeader);
                outer.when(hit, found -> found.store(at(cached, 24), add(load(at(cached, 24)), i64(1))));
                outer.when(eq(hit, bool(false)), missingCache -> {
                    flushCached(missingCache, cached);
                    // A map-growth return below must not flush this evicted count twice.
                    missingCache.store(cached, i64(0));
                    fillCache(missingCache, cached, key, hash, views, counts, index);
                });
                outer.assign(index, add(index, i64(1)));
            });
        });
        b.store(STATE, index);
        flushCounts(b, views, counts);
        b.exit(0);
        return new Program(b.block());
    }

    private static void fillCache(Builder row, Expr cached, Expr key, Expr hash,
                                  Expr views, Expr counts, Expr index) {
        Var shard = row.let(binary(Op.USHR, hash, i64(56)));
        Var view = row.let(new Offset(views, binary(Op.MUL, shard, i64(64))));
        Var base = row.let(new Offset(nil(), new AtomicLoad(view, true)));
        row.when(eq(base, nil()), missing -> needMap(missing, views, counts, index, shard, base, i64(0)));
        Var mask = row.let(load(at(view, NativeMapView.MASK)));
        Var slot = row.let(binary(Op.AND, hash, mask));
        Var count = row.let(new Offset(counts, binary(Op.MUL, shard, i64(8))));
        Var probes = row.let(i64(0)), spins = row.let(i64(0));
        Var done = row.let(bool(false));
        row.loop(eq(done, bool(false)), probe -> {
            Var entry = probe.let(new Offset(base, binary(Op.MUL, slot, i64(24))));
            Var header = probe.let(new AtomicLoad(entry, true));
            probe.when(eq(header, i64(0)), absent -> {
                Var previous = absent.let(new CompareExchange(entry, i64(0), i64(1)));
                absent.when(eq(previous, i64(0)), inserted -> {
                    // Only stable frame pointers enter this path. The owner pins them
                    // until all final maps and output cursors have finished.
                    inserted.store(at(entry, 8), load(at(key, 8)));
                    inserted.store(at(entry, 16), i64(0));
                    inserted.store(MemoryType.ADDRESS, at(cached, 16), entry);
                    inserted.atomicStore(entry, load(key));
                    inserted.store(count, add(load(count), i64(1)));
                    inserted.assign(done, bool(true));
                });
            });
            probe.when(binary(Op.BOTH, eq(done, bool(false)), ne(header, i64(0))), occupied -> {
                occupied.when(eq(header, i64(1)), busy -> {
                    busy.assign(spins, add(spins, i64(1)));
                    busy.when(eq(binary(Op.AND, spins, i64(1023)), i64(0)), Builder::poll);
                });
                occupied.when(ne(header, i64(1)), published -> {
                    Var same = QuestVarchar.equal(published, entry, key, header);
                    published.when(same, match -> {
                        match.store(MemoryType.ADDRESS, at(cached, 16), entry);
                        match.assign(done, bool(true));
                    });
                    published.when(eq(same, bool(false)), collision -> {
                        collision.assign(probes, add(probes, i64(1)));
                        Expr capacity = add(mask, i64(1));
                        Expr crowded = binary(Op.BOTH, binary(Op.GE, probes, i64(64)),
                                binary(Op.GE, add(load(count), new AtomicLoad(at(view, NativeMapView.SIZE))),
                                        binary(Op.SDIV, binary(Op.MUL, capacity, i64(7)), i64(10))));
                        collision.when(binary(Op.EITHER, eq(probes, capacity), crowded), full ->
                                needMap(full, views, counts, index, shard, base, mask));
                        collision.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
                    });
                });
            });
        });
        row.store(at(cached, 8), load(at(key, 8)));
        row.store(at(cached, 24), i64(1));
        row.store(cached, load(key));
    }

    private static void flushCached(Builder b, Expr slot) {
        b.when(ne(load(slot), i64(0)), dirty -> {
            Var entry = dirty.let(new Load(MemoryType.ADDRESS, at(slot, 16)));
            dirty.let(new AtomicAdd(at(entry, 16), load(at(slot, 24))));
        });
    }

    private static void needMap(Builder b, Expr views, Expr counts, Expr index, Expr shard, Expr base, Expr mask) {
        b.store(STATE, index);
        b.store(at(STATE, 32), shard);
        b.store(MemoryType.ADDRESS, at(STATE, 40), base);
        b.store(at(STATE, 48), mask);
        flushCounts(b, views, counts);
        b.exit(NEED_MAP);
    }

    private static Expr cache(Expr hash) {
        return new Offset(at(STATE, CACHE), binary(Op.MUL, binary(Op.AND, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(32))), i64(CACHE_ENTRIES - 1)), i64(32)));
    }

    // Flush on every return, while the caller still holds the resize read lock.
    // Cached destination pointers cannot survive its release.
    private static void flushCounts(Builder b, Expr views, Expr counts) {
        Var c = b.let(i64(0));
        b.loop(binary(Op.LT, c, i64(CACHE_ENTRIES)), loop -> {
            Var slot = loop.let(new Offset(at(STATE, CACHE), binary(Op.MUL, c, i64(32))));
            flushCached(loop, slot);
            loop.assign(c, add(c, i64(1)));
        });
        Var shard = b.let(i64(0));
        b.loop(binary(Op.LT, shard, i64(256)), loop -> {
            Var count = loop.let(load(new Offset(counts, binary(Op.MUL, shard, i64(8)))));
            loop.when(ne(count, i64(0)), present -> present.let(new AtomicAdd(
                    new Offset(at(views, 56), binary(Op.MUL, shard, i64(64))), count)));
            loop.assign(shard, add(shard, i64(1)));
        });
    }
}
