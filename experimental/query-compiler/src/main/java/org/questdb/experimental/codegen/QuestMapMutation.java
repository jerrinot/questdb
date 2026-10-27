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

import static org.questdb.experimental.codegen.Ir.*;

/** Existing unordered-map relocation and merge algorithms, specialized for the bound layout. */
final class QuestMapMutation {
    private QuestMapMutation() {
    }

    static Program rehash(QuestMapProbe probe, long valueBytes) {
        Builder b = new Builder();
        MemoryType width = probe.keyType() == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
        Var stride = b.let(load(at(COLUMNS, 24)));
        Var sourceBase = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var targetBase = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var mask = b.let(load(at(COLUMNS, 16)));
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var source = loop.let(new Offset(sourceBase, binary(Op.MUL, index, stride)));
            Var key = loop.let(new Load(width, source));
            loop.when(ne(key, i64(0)), occupied -> {
                Var slot = occupied.let(binary(Op.AND, probe.hash(occupied, key), mask));
                Var destination = occupied.let(new Offset(targetBase, binary(Op.MUL, slot, stride)));
                occupied.loop(ne(new Load(width, destination), i64(0)), collision -> {
                    collision.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
                    collision.assign(destination, new Offset(targetBase, binary(Op.MUL, slot, stride)));
                });
                copy(occupied, source, destination, width.bytes + valueBytes);
            });
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    // Source descriptor: base, capacity, stride, zero entry, hasZero, merge (vs shard).
    // Visit zero first, then nonzero entries in physical slot order, as Map.merge does.
    static Program entries(QuestMapProbe probe, AggregateUpdates updates, long valueBytes) {
        Builder b = new Builder();
        MemoryType width = probe.keyType() == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
        Var base = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var stride = b.let(load(at(COLUMNS, 16)));
        Var zero = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 24)));
        Var hasZero = b.let(ne(load(at(COLUMNS, 32)), i64(0)));
        Var merge = b.let(ne(load(at(COLUMNS, 40)), i64(0)));
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var source = loop.let(zero);
            loop.when(ne(index, i64(0)), nonzero -> nonzero.assign(source,
                    new Offset(base, binary(Op.MUL, binary(Op.SUB, index, i64(1)), stride))));
            Var key = loop.let(new Load(width, source));
            loop.when(binary(Op.EITHER, binary(Op.BOTH, eq(index, i64(0)), hasZero),
                    binary(Op.BOTH, ne(index, i64(0)), ne(key, i64(0)))), occupied -> {
                Var inserted = occupied.let(bool(false));
                Var destination = probe.find(occupied, key, index, load(at(STATE, 56)), null, (init, value) -> {
                    copy(init, at(source, width.bytes), value, valueBytes);
                    init.assign(inserted, bool(true));
                });
                occupied.when(binary(Op.BOTH, merge, eq(inserted, bool(false))), existing ->
                        updates.merge(existing, at(source, width.bytes), destination));
            });
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    static void copy(Builder b, Expr source, Expr destination, Expr bytes) {
        Var offset = b.let(i64(0));
        b.loop(binary(Op.GE, binary(Op.SUB, bytes, offset), i64(8)), word -> {
            word.store(new Offset(destination, offset), load(new Offset(source, offset)));
            word.assign(offset, add(offset, i64(8)));
        });
        b.loop(binary(Op.LT, offset, bytes), tail -> {
            tail.store(MemoryType.I8, new Offset(destination, offset), new Load(MemoryType.I8, new Offset(source, offset)));
            tail.assign(offset, add(offset, i64(1)));
        });
    }

    static void copy(Builder b, Expr source, Expr destination, long bytes) {
        long offset = 0;
        for (; offset + 8 <= bytes; offset += 8) b.store(at(destination, offset), load(at(source, offset)));
        for (; offset < bytes; offset++) b.store(MemoryType.I8, at(destination, offset), new Load(MemoryType.I8, at(source, offset)));
    }
}
