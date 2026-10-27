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

import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Generated open-addressed table operations; state is a stable TableState descriptor. */
final class GroupTable {
    private GroupTable() { }

    static Var findOrInsert(Builder b, Expr state, List<Var> keys, List<Expr> initialValues) {
        return findOrInsert(b, state, keys, initialValues, null);
    }

    static Var findOrInsert(Builder b, Expr state, List<Var> keys, List<Expr> initialValues, Var inserted) {
        long stride = 8L * (1 + keys.size() + initialValues.size());
        Var capacity = b.let(load(state));
        Var entries = b.let(new Load(MemoryType.ADDRESS, at(state, TableState.ENTRIES)));
        Var entry = find(b, entries, capacity, keys, stride);
        b.when(binary(Op.EITHER, eq(entry, nil()), eq(load(entry), i64(0))), missing -> {
            Expr threshold = binary(Op.SUB, capacity, binary(Op.USHR, capacity, i64(2)));
            missing.when(binary(Op.BOTH, binary(Op.GE, load(at(state, 8)), threshold),
                    binary(Op.LT, capacity, load(at(state, TableState.MAX_CAPACITY)))), grow -> {
                rehash(grow, state, entries, capacity, keys.size(), stride);
                grow.assign(entry, find(grow, entries, capacity, keys, stride));
            });
            missing.when(eq(entry, nil()), full -> full.exit(1));
            missing.store(entry, i64(1));
            for (int i = 0; i < keys.size(); i++) { missing.store(at(entry, 8L * (i + 1)), keys.get(i)); }
            for (int i = 0; i < initialValues.size(); i++) {
                missing.store(at(entry, 8L * (1 + keys.size() + i)), initialValues.get(i));
            }
            missing.store(at(state, 8), add(load(at(state, 8)), i64(1)));
            if (inserted != null) { missing.assign(inserted, bool(true)); }
        });
        return entry;
    }

    static Var findExisting(Builder b, Expr state, List<Var> keys, long stride) {
        Var entry = find(b, new Load(MemoryType.ADDRESS, at(state, TableState.ENTRIES)), load(state), keys, stride);
        b.when(binary(Op.BOTH, ne(entry, nil()), eq(load(entry), i64(0))), missing -> missing.assign(entry, nil()));
        return entry;
    }

    private static Var find(Builder b, Expr entries, Expr capacity, List<Var> keys, long stride) {
        Var mixed = hash(b, keys);
        return findHashed(b, entries, capacity, keys, stride, mixed);
    }

    static Var hash(Builder b, List<Var> keys) {
        Expr hash = i64(0);
        for (Var key : keys) { hash = binary(Op.XOR, binary(Op.MUL, hash, i64(31)), key); }
        Var mixed = b.let(hash);
        b.assign(mixed, binary(Op.XOR, mixed, binary(Op.USHR, mixed, i64(33))));
        b.assign(mixed, binary(Op.MUL, mixed, i64(0xff51afd7ed558ccdL)));
        b.assign(mixed, binary(Op.XOR, mixed, binary(Op.USHR, mixed, i64(33))));
        return mixed;
    }

    private static Var findHashed(Builder b, Expr entries, Expr capacity, List<Var> keys, long stride, Var mixed) {
        Var mask = b.let(binary(Op.SUB, capacity, i64(1)));
        Var slot = b.let(binary(Op.AND, mixed, mask));
        Var entry = b.let(address(entries, slot, stride));
        Var probes = b.let(i64(0));
        Expr matches = bool(true);
        for (int i = 0; i < keys.size(); i++) {
            matches = binary(Op.BOTH, matches, eq(load(at(entry, 8L * (i + 1))), keys.get(i)));
        }
        b.loop(binary(Op.BOTH, binary(Op.LT, probes, capacity),
                binary(Op.BOTH, ne(load(entry), i64(0)), eq(matches, bool(false)))), probe -> {
            probe.assign(probes, add(probes, i64(1)));
            probe.when(eq(binary(Op.AND, probes, i64(1023)), i64(0)), Builder::poll);
            probe.assign(slot, binary(Op.AND, add(slot, i64(1)), mask));
            probe.assign(entry, address(entries, slot, stride));
        });
        b.when(binary(Op.GE, probes, capacity), full -> full.assign(entry, nil()));
        return entry;
    }

    private static void rehash(Builder b, Expr state, Var entries, Var capacity, int keyCount, long stride) {
        Var newCapacity = b.let(binary(Op.MUL, capacity, i64(2)));
        Var grown = b.let(new Allocate(binary(Op.MUL, newCapacity, i64(stride))));
        b.when(eq(grown, nil()), failed -> failed.exit(2));
        Var offset = b.let(i64(0));
        b.loop(binary(Op.LT, offset, binary(Op.MUL, newCapacity, i64(stride))), zero -> {
            zero.when(eq(binary(Op.AND, offset, i64(8191)), i64(0)), Builder::poll);
            zero.store(new Offset(grown, offset), i64(0));
            zero.assign(offset, add(offset, i64(8)));
        });
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, capacity), copy -> {
            copy.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var old = copy.let(address(entries, index, stride));
            copy.when(ne(load(old), i64(0)), occupied -> {
                List<Var> keys = new ArrayList<>();
                for (int i = 0; i < keyCount; i++) { keys.add(occupied.let(load(at(old, 8L * (i + 1))))); }
                Var target = find(occupied, grown, newCapacity, keys, stride);
                occupied.when(eq(target, nil()), corrupt -> corrupt.exit(3));
                for (long i = 0; i < stride; i += 8) { occupied.store(at(target, i), load(at(old, i))); }
            });
            copy.assign(index, add(index, i64(1)));
        });
        b.free(entries, binary(Op.MUL, capacity, i64(stride)));
        b.assign(entries, grown);
        b.assign(capacity, newCapacity);
        b.store(state, capacity);
        b.store(MemoryType.ADDRESS, at(state, TableState.ENTRIES), entries);
        b.store(at(state, TableState.GROWS), add(load(at(state, TableState.GROWS)), i64(1)));
    }

    private static Expr address(Expr entries, Expr slot, long stride) {
        return new Offset(entries, binary(Op.MUL, slot, i64(stride)));
    }
}
