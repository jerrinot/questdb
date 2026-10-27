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
import io.questdb.griffin.engine.table.GroupByMapFragment;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.questdb.experimental.codegen.Ir.*;

/** VARCHAR frame decoding and UnorderedVarcharMap's existing byte-key algorithms. */
final class QuestVarchar {
    static final int NEED_COPY = 6;
    static final int INVALID_FRAME = -7;
    private static final long PTR_MASK = Long.MAX_VALUE;
    private static final long M2 = 0x517cc1b727220a95L;

    private QuestVarchar() { }

    // Native split and Parquet slice headers both carry NULL at bit 2.
    static Var header(Builder b, Expr descriptor, Expr row) {
        Var aux = b.let(new Load(MemoryType.ADDRESS, descriptor));
        Var raw = b.let(i64(4)); // column top
        b.when(ne(aux, nil()), present -> {
            Var offset = present.let(binary(Op.MUL, row, i64(16)));
            present.when(binary(Op.LT, load(at(descriptor, 16)), add(offset, i64(16))), bad -> bad.exit(INVALID_FRAME));
            present.assign(raw, new Load(MemoryType.I32, new Offset(aux, offset)));
        });
        return raw;
    }

    static Pipeline.Utf8Value decode(Builder b, Expr descriptor, Expr row) {
        Var raw = header(b, descriptor, row);
        Var isNull = b.let(ne(binary(Op.AND, raw, i64(4)), i64(0)));
        Var ptr = b.let(nil()), size = b.let(i64(0));
        b.when(eq(isNull, bool(false)), value -> {
            Var aux = value.let(new Offset(new Load(MemoryType.ADDRESS, descriptor), binary(Op.MUL, row, i64(16))));
            Var slice = value.let(ne(load(at(descriptor, 32)), i64(0)));
            value.when(slice, parquet -> {
                parquet.assign(size, binary(Op.USHR, binary(Op.AND, raw, i64(0xffff_ffffL)), i64(4)));
                parquet.assign(ptr, new Load(MemoryType.ADDRESS, at(aux, 8)));
            });
            value.when(eq(slice, bool(false)), nativeFrame -> {
                Var inline = nativeFrame.let(ne(binary(Op.AND, raw, i64(1)), i64(0)));
                nativeFrame.when(inline, small -> {
                    small.assign(size, binary(Op.AND, binary(Op.USHR, raw, i64(4)), i64(15)));
                    small.when(binary(Op.LT, i64(9), size), bad -> bad.exit(INVALID_FRAME));
                    small.assign(ptr, at(aux, 1));
                });
                nativeFrame.when(eq(inline, bool(false)), split -> {
                    split.assign(size, binary(Op.AND, binary(Op.USHR, raw, i64(4)), i64(0x0fff_ffffL)));
                    Var offset = split.let(binary(Op.USHR, load(at(aux, 8)), i64(16)));
                    split.when(binary(Op.LT, load(at(descriptor, 24)), add(offset, size)), bad -> bad.exit(INVALID_FRAME));
                    split.assign(ptr, new Offset(new Load(MemoryType.ADDRESS, at(descriptor, 8)), offset));
                });
            });
        });
        return new Pipeline.Utf8Value(ptr, size, isNull, ne(binary(Op.AND, raw, i64(2)), i64(0)),
                ne(load(at(descriptor, 40)), i64(0)));
    }

    // Staged key: packed hash/size/flags, pointer with lifetime bit, full hash for sharding.
    static Var stage(Builder b, Expr row, Expr scratch) {
        var value = decode(b, new Load(MemoryType.ADDRESS, at(STATE, 16)), row);
        Var hash = hash(b, value.address(), value.size());
        Expr flags = new Select(value.isNull(), i64(0xc000_0000L),
                new Select(binary(Op.EITHER, value.isAscii(), eq(value.size(), i64(0))), i64(0x8000_0000L), i64(0)));
        b.store(scratch, binary(Op.XOR, binary(Op.MUL, binary(Op.XOR, flags, value.size()), i64(1L << 32)),
                binary(Op.AND, hash, i64(0xffff_ffffL))));
        b.store(MemoryType.ADDRESS, at(scratch, 8), value.address());
        b.when(binary(Op.BOTH, eq(value.stable(), bool(false)), ne(value.size(), i64(0))), unstable ->
                unstable.store(at(scratch, 8), binary(Op.XOR, load(at(scratch, 8)), i64(Long.MIN_VALUE))));
        b.store(at(scratch, 16), hash);
        return b.let(scratch);
    }

    /** Hash.hashMem64: signed 8/4/1-byte pieces followed by xxh3Avalanche64. No tail overread. */
    static Var hash(Builder b, Expr pointer, Expr size) {
        Var hash = b.let(i64(0)), offset = b.let(i64(0));
        b.loop(binary(Op.GE, binary(Op.SUB, size, offset), i64(8)), word -> {
            word.assign(hash, add(binary(Op.MUL, hash, i64(M2)), load(new Offset(pointer, offset))));
            word.assign(offset, add(offset, i64(8)));
        });
        b.when(binary(Op.GE, binary(Op.SUB, size, offset), i64(4)), tail -> {
            tail.assign(hash, add(binary(Op.MUL, hash, i64(M2)), new Load(MemoryType.I32, new Offset(pointer, offset))));
            tail.assign(offset, add(offset, i64(4)));
        });
        b.loop(binary(Op.LT, offset, size), tail -> {
            tail.assign(hash, add(binary(Op.MUL, hash, i64(M2)), new Load(MemoryType.I8, new Offset(pointer, offset))));
            tail.assign(offset, add(offset, i64(1)));
        });
        b.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(37))));
        b.assign(hash, binary(Op.MUL, hash, i64(0x165667919E3779F9L)));
        b.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(32))));
        return hash;
    }

    static Expr size(Expr key) { return binary(Op.AND, binary(Op.USHR, load(key), i64(32)), i64(0x3fff_ffffL)); }
    static Expr pointer(Expr key) { return new Offset(nil(), binary(Op.AND, load(at(key, 8)), i64(PTR_MASK))); }

    static Var equal(Builder b, Expr entry, Expr key) {
        return equal(b, entry, key, load(entry));
    }

    static Var equal(Builder b, Expr entry, Expr key, Expr header) {
        Var same = b.let(binary(Op.BOTH, ne(header, i64(0)),
                eq(binary(Op.AND, header, i64(PTR_MASK)), binary(Op.AND, load(key), i64(PTR_MASK)))));
        b.when(same, compare -> {
            Var left = compare.let(pointer(entry)), right = compare.let(pointer(key)), length = compare.let(size(key));
            compare.assign(same, ByteSequences.equal(compare, left, right, length));
        });
        return same;
    }

    static QuestMapProbe.Candidate prepare(QuestMapProbe probe, Builder b, Var key, long valueBytes,
                                          BiConsumer<Builder, Var> hint) {
        var lookup = probe.lookup(b, key, load(at(STATE, 56)));
        Var entry = b.let(new Offset(lookup.base(), binary(Op.MUL, lookup.slot(), load(at(lookup.view(), NativeMapView.STRIDE)))));
        b.prefetch(entry, true);
        if (16 + valueBytes > 1) b.prefetch(at(entry, 16 + valueBytes - 1), true);
        b.when(ne(load(entry), i64(0)), occupied -> hint.accept(occupied, occupied.let(at(entry, 16))));
        return new QuestMapProbe.Candidate(key, entry);
    }

    static Var find(QuestMapProbe probe, Builder b, Var key, Var position, Expr shardMask,
                    Consumer<Builder> beforeMiss, BiConsumer<Builder, Expr> initialize) {
        var lookup = probe.lookup(b, key, shardMask);
        Var destination = b.let(nil()), stride = b.let(load(at(lookup.view(), NativeMapView.STRIDE)));
        b.loop(eq(destination, nil()), step -> {
            QuestMapProbe.count(step, 40);
            Var entry = step.let(new Offset(lookup.base(), binary(Op.MUL, lookup.slot(), stride)));
            step.when(eq(load(entry), i64(0)), missing -> {
                missing.when(binary(Op.LT, load(at(lookup.view(), NativeMapView.FREE)), i64(2)), full ->
                        QuestMapProbe.miss(full, position, lookup.shard(), beforeMiss));
                Var ptr = missing.let(load(at(key, 8)));
                missing.when(binary(Op.BOTH, binary(Op.LT, ptr, i64(0)), ne(size(key), i64(0))), unstable -> {
                    unstable.when(ne(load(at(STATE, 80)), position), copy -> {
                        copy.store(at(STATE, 64), binary(Op.AND, ptr, i64(PTR_MASK)));
                        copy.store(at(STATE, 72), size(key));
                        copy.store(at(STATE, 24), lookup.shard());
                        copy.store(at(STATE, 32), position);
                        copy.exit(NEED_COPY);
                    });
                    unstable.assign(ptr, load(at(STATE, 64)));
                });
                initialize.accept(missing, at(entry, 16));
                missing.store(entry, load(key));
                missing.store(at(entry, 8), ptr);
                missing.store(at(lookup.view(), NativeMapView.FREE), binary(Op.SUB, load(at(lookup.view(), NativeMapView.FREE)), i64(1)));
                missing.store(at(lookup.view(), NativeMapView.SIZE), add(load(at(lookup.view(), NativeMapView.SIZE)), i64(1)));
                missing.store(at(STATE, 80), i64(-1));
                missing.assign(destination, at(entry, 16));
            });
            step.when(eq(destination, nil()), occupied -> {
                QuestMapProbe.count(occupied, 48);
                occupied.when(equal(occupied, entry, key), match -> match.assign(destination, at(entry, 16)));
            });
            step.assign(lookup.slot(), binary(Op.AND, add(lookup.slot(), i64(1)), lookup.mask()));
        });
        return destination;
    }

    static Program rehash(long valueBytes) {
        Builder b = new Builder();
        Var stride = b.let(load(at(COLUMNS, 24))), mask = b.let(load(at(COLUMNS, 16)));
        Var sourceBase = b.let(new Load(MemoryType.ADDRESS, COLUMNS)), targetBase = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var source = loop.let(new Offset(sourceBase, binary(Op.MUL, index, stride)));
            loop.when(ne(load(source), i64(0)), occupied -> {
                Var slot = occupied.let(binary(Op.AND, load(source), mask));
                Var dest = occupied.let(new Offset(targetBase, binary(Op.MUL, slot, stride)));
                occupied.loop(ne(load(dest), i64(0)), collision -> {
                    collision.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
                    collision.assign(dest, new Offset(targetBase, binary(Op.MUL, slot, stride)));
                });
                QuestMapMutation.copy(occupied, source, dest, 16 + valueBytes);
            });
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0); return new Program(b.block());
    }

    static Program entries(QuestMapProbe probe, AggregateUpdates updates, long valueBytes) {
        Builder b = new Builder();
        Var base = b.let(new Load(MemoryType.ADDRESS, COLUMNS)), stride = b.let(load(at(COLUMNS, 24)));
        Var merge = b.let(eq(load(at(STATE, 56)), i64(0))), index = b.let(load(STATE));
        Expr scratch = at(STATE, probe.controlBytes() + (long) GroupByMapFragment.NUM_SHARDS * probe.viewBytes());
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var source = loop.let(new Offset(base, binary(Op.MUL, index, stride)));
            loop.when(ne(load(source), i64(0)), occupied -> {
                QuestMapMutation.copy(occupied, source, scratch, 16);
                // Merge only uses low hash bits. Redistribution needs the original full hash.
                Var hash = occupied.let(binary(Op.AND, load(source), i64(0xffff_ffffL)));
                occupied.when(eq(merge, bool(false)), shard -> shard.assign(hash, hash(shard, pointer(source), size(source))));
                occupied.store(at(scratch, 16), hash);
                Var inserted = occupied.let(bool(false));
                Var destination = probe.find(occupied, occupied.let(scratch), index, load(at(STATE, 56)), null, (init, value) -> {
                    QuestMapMutation.copy(init, at(source, 16), value, valueBytes);
                    init.assign(inserted, bool(true));
                });
                occupied.when(binary(Op.BOTH, merge, eq(inserted, bool(false))), existing -> updates.merge(existing, at(source, 16), destination));
            });
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0); return new Program(b.block());
    }
}
