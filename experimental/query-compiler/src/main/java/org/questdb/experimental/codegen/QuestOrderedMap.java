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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.questdb.experimental.codegen.Ir.*;

/** OrderedMap's offset table and heap algorithms. Allocation stays with the map. */
final class QuestOrderedMap {
    private QuestOrderedMap() { }

    static Var find(QuestMapProbe probe, Builder b, Var key, Var position, Expr shardMask,
                    Consumer<Builder> beforeMiss, BiConsumer<Builder, Expr> initialize) {
        return find(probe, b, key, position, shardMask, beforeMiss, initialize, null);
    }

    private static Var find(QuestMapProbe probe, Builder b, Var key, Var position, Expr shardMask,
                            Consumer<Builder> beforeMiss, BiConsumer<Builder, Expr> initialize, Var hash) {
        return findFrom(probe, b, key, position, shardMask, beforeMiss, initialize, hash, null);
    }

    static Var findPrepared(QuestMapProbe probe, Builder b, Var key, Var hash, Var slot, Var position,
                            BiConsumer<Builder, Expr> initialize) {
        return findFrom(probe, b, key, position, load(at(STATE, 56)), null, initialize, hash, slot);
    }

    private static Var findFrom(QuestMapProbe probe, Builder b, Var key, Var position, Expr shardMask,
                               Consumer<Builder> beforeMiss, BiConsumer<Builder, Expr> initialize, Var hash, Var startSlot) {
        var lookup = hash == null ? probe.lookup(b, key, shardMask) : probe.lookupHashed(b, key, shardMask, hash);
        Var view = lookup.view(), slot = lookup.slot();
        if (startSlot != null) b.assign(slot, startSlot);
        Var offsets = b.let(new Load(MemoryType.ADDRESS, at(view, NativeMapView.OFFSETS)));
        Var destination = b.let(nil());
        b.loop(eq(destination, nil()), loop -> {
            QuestMapProbe.count(loop, 40);
            Var slotAddress = loop.let(new Offset(offsets, binary(Op.MUL, slot, i64(8))));
            Var packed = loop.let(load(slotAddress));
            Var raw = loop.let(binary(Op.AND, packed, i64(0xffff_ffffL)));
            loop.when(eq(raw, i64(0)), missing -> {
                Var entry = missing.let(new Load(MemoryType.ADDRESS, at(view, NativeMapView.Ordered.APPEND)));
                Var bytes = missing.let(probe.variable() ? QuestVariableKey.stride(key, load(at(view, NativeMapView.STRIDE))) : load(at(view, NativeMapView.STRIDE)));
                missing.when(binary(Op.EITHER, binary(Op.LT, load(at(view, NativeMapView.FREE)), i64(2)),
                        binary(Op.LT, load(at(view, NativeMapView.Ordered.LIMIT)), add(load(at(view, NativeMapView.Ordered.APPEND)), bytes))), full -> {
                    if (probe.variable()) full.store(at(STATE, 88), bytes);
                    QuestMapProbe.miss(full, position, lookup.shard(), beforeMiss);
                });
                if (probe.variable()) QuestMapMutation.copy(missing, key, entry, QuestVariableKey.bytes(key));
                else QuestMapMutation.copy(missing, key, entry, probe.keyBytes());
                initialize.accept(missing, probe.valueAddress(entry));
                Expr offset = binary(Op.SUB, load(at(view, NativeMapView.Ordered.APPEND)), load(view));
                Expr compressed = add(binary(Op.USHR, offset, i64(3)), i64(1));
                missing.store(slotAddress, add(compressed, binary(Op.MUL, binary(Op.AND, lookup.hash(), i64(0xffff_ffffL)), i64(0x100000000L))));
                missing.store(MemoryType.ADDRESS, at(view, NativeMapView.Ordered.APPEND), new Offset(entry, bytes));
                missing.store(at(view, NativeMapView.FREE), binary(Op.SUB, load(at(view, NativeMapView.FREE)), i64(1)));
                missing.store(at(view, NativeMapView.SIZE), add(load(at(view, NativeMapView.SIZE)), i64(1)));
                missing.assign(destination, probe.valueAddress(entry));
            });
            loop.when(binary(Op.BOTH, ne(raw, i64(0)), eq(binary(Op.USHR, packed, i64(32)), binary(Op.AND, lookup.hash(), i64(0xffff_ffffL)))), occupied -> {
                QuestMapProbe.count(occupied, 48);
                Var entry = occupied.let(new Offset(lookup.base(), binary(Op.MUL, binary(Op.SUB, raw, i64(1)), i64(8))));
                occupied.when(probe.keysEqual(occupied, entry, key), hit -> hit.assign(destination, probe.valueAddress(entry)));
            });
            loop.assign(slot, binary(Op.AND, add(slot, i64(1)), lookup.mask()));
        });
        return destination;
    }

    static Program rehash() {
        Builder b = new Builder();
        Var source = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var target = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var mask = b.let(load(at(COLUMNS, 16))), index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var packed = loop.let(load(new Offset(source, binary(Op.MUL, index, i64(8)))));
            loop.when(ne(binary(Op.AND, packed, i64(0xffff_ffffL)), i64(0)), occupied -> {
                Var slot = occupied.let(binary(Op.AND, binary(Op.USHR, packed, i64(32)), mask));
                Var destination = occupied.let(new Offset(target, binary(Op.MUL, slot, i64(8))));
                occupied.loop(ne(new Load(MemoryType.I32, destination), i64(0)), collision -> {
                    collision.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
                    collision.assign(destination, new Offset(target, binary(Op.MUL, slot, i64(8))));
                });
                occupied.store(destination, packed);
            });
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0); return new Program(b.block());
    }

    // Source: heap, offsets, capacity, stride, size, merge flag. Merge follows hash-slot
    // order; redistribution follows the ordinary cursor's insertion order.
    static Program entries(QuestMapProbe probe, AggregateUpdates updates, long valueBytes) {
        int batchRows = Integer.getInteger("query.codegen.prefetchRows", 32);
        if (Boolean.parseBoolean(System.getProperty("query.codegen.orderedPipeline", "true")) && batchRows > 0) {
            return stagedEntries(probe, updates, valueBytes, batchRows);
        }
        Builder b = new Builder();
        Var base = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var offsets = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var stride = b.let(load(at(COLUMNS, 24)));
        Var merge = b.let(ne(load(at(COLUMNS, 40)), i64(0)));
        Var index = b.let(load(STATE));
        Var sourceOffset = probe.variable() ? b.let(load(at(STATE, 96))) : null;
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var source = loop.let(new Offset(base, probe.variable() ? sourceOffset : binary(Op.MUL, index, stride)));
            Var occupied = loop.let(bool(true));
            Var hash = loop.let(i64(0));
            loop.when(merge, hashed -> {
                Var packed = hashed.let(load(new Offset(offsets, binary(Op.MUL, index, i64(8)))));
                Var raw = hashed.let(binary(Op.AND, packed, i64(0xffff_ffffL)));
                hashed.assign(hash, binary(Op.USHR, packed, i64(32)));
                hashed.assign(occupied, ne(raw, i64(0)));
                hashed.assign(source, new Offset(base, binary(Op.MUL, binary(Op.SUB, raw, i64(1)), i64(8))));
            });
            loop.when(occupied, row -> {
                row.when(eq(merge, bool(false)), shard -> shard.assign(hash, probe.hash(shard, source)));
                Var inserted = row.let(bool(false));
                Var destination = find(probe, row, source, index, load(at(STATE, 56)),
                        probe.variable() ? miss -> miss.store(at(STATE, 96), sourceOffset) : null, (init, value) -> {
                    QuestMapMutation.copy(init, probe.valueAddress(source), value, valueBytes);
                    init.assign(inserted, bool(true));
                }, hash);
                row.when(binary(Op.BOTH, merge, eq(inserted, bool(false))), existing ->
                        updates.merge(existing, probe.valueAddress(source), destination));
            });
            if (probe.variable()) loop.when(eq(merge, bool(false)), advance ->
                    advance.assign(sourceOffset, add(sourceOffset, QuestVariableKey.stride(source, stride))));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0); return new Program(b.block());
    }
    /** Same merge/redistribution order, with independent lookups staged in the slot scratch. */
    private static Program stagedEntries(QuestMapProbe probe, AggregateUpdates updates,
                                         long valueBytes, int batchRows) {
        Builder b = new Builder();
        Var base = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var offsets = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var stride = b.let(load(at(COLUMNS, 24)));
        Var merge = b.let(ne(load(at(COLUMNS, 40)), i64(0)));
        Var index = b.let(load(STATE));
        Var sourceOffset = probe.variable() ? b.let(load(at(STATE, 96))) : null;
        b.loop(binary(Op.LT, index, ROWS), batch -> {
            Var count = batch.let(i64(0)), position = batch.let(index);
            Var scratch = batch.let(at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            batch.loop(binary(Op.BOTH, binary(Op.LT, position, ROWS), binary(Op.LT, count, i64(batchRows))), stage -> {
                stage.when(eq(binary(Op.AND, position, i64(1023)), i64(0)), Builder::poll);
                Var source = stage.let(new Offset(base, probe.variable() ? sourceOffset : binary(Op.MUL, position, stride)));
                Var occupied = stage.let(bool(true)), hash = stage.let(i64(0));
                stage.when(merge, hashed -> {
                    Var packed = hashed.let(load(new Offset(offsets, binary(Op.MUL, position, i64(8)))));
                    Var raw = hashed.let(binary(Op.AND, packed, i64(0xffff_ffffL)));
                    hashed.assign(hash, binary(Op.USHR, packed, i64(32)));
                    hashed.assign(occupied, ne(raw, i64(0)));
                    hashed.assign(source, new Offset(base, binary(Op.MUL, binary(Op.SUB, raw, i64(1)), i64(8))));
                });
                stage.when(occupied, row -> {
                    row.when(eq(merge, bool(false)), shard -> shard.assign(hash, probe.hash(shard, source)));
                    probe.prefetchOffset(row, source, hash);
                    row.store(scratch, position);
                    row.store(MemoryType.ADDRESS, at(scratch, 8), source);
                    row.store(at(scratch, 24), hash);
                    row.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                    row.assign(count, add(count, i64(1)));
                });
                if (probe.variable()) stage.when(eq(merge, bool(false)), advance ->
                        advance.assign(sourceOffset, add(sourceOffset, QuestVariableKey.stride(source, stride))));
                stage.assign(position, add(position, i64(1)));
            });
            batch.assign(scratch, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            Var prepared = batch.let(i64(0));
            batch.loop(binary(Op.LT, prepared, count), prepare -> {
                Var key = prepare.let(new Load(MemoryType.ADDRESS, at(scratch, 8)));
                var candidate = probe.prepare(prepare, key, valueBytes, (hint, destination) -> {}, prepare.let(load(at(scratch, 24))));
                prepare.store(MemoryType.ADDRESS, at(scratch, 16), candidate.entry());
                prepare.store(at(scratch, 32), candidate.slot());
                prepare.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                prepare.assign(prepared, add(prepared, i64(1)));
            });
            batch.assign(scratch, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            Var consumed = batch.let(i64(0));
            batch.loop(binary(Op.LT, consumed, count), consume -> {
                Var source = consume.let(new Load(MemoryType.ADDRESS, at(scratch, 8)));
                Var inserted = consume.let(bool(false));
                if (probe.variable()) consume.when(eq(merge, bool(false)), save ->
                        save.store(at(STATE, 96), binary(Op.SUB, load(at(scratch, 8)), load(COLUMNS))));
                Var destination = probe.finishOrdered(consume, source,
                        consume.let(new Load(MemoryType.ADDRESS, at(scratch, 16))), consume.let(load(at(scratch, 24))),
                        consume.let(load(at(scratch, 32))), consume.let(load(scratch)), (init, value) -> {
                            QuestMapMutation.copy(init, probe.valueAddress(source), value, valueBytes);
                            init.assign(inserted, bool(true));
                        });
                consume.when(binary(Op.BOTH, merge, eq(inserted, bool(false))), existing ->
                        updates.merge(existing, probe.valueAddress(source), destination));
                consume.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                consume.assign(consumed, add(consumed, i64(1)));
            });
            batch.assign(index, position);
        });
        b.exit(0); return new Program(b.block());
    }

}
