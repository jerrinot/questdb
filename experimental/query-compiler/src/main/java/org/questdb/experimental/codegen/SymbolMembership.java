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

import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;

import static org.questdb.experimental.codegen.Ir.*;

/** Exact membership over the existing resolved dictionary match set. Shared until workers drain. */
final class SymbolMembership implements AutoCloseable {
    static final int BYTES = 32;
    private final SymbolKeySetProvider source;
    private final DirectLongList memory = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
    private long first, last = -1, count;

    SymbolMembership(SymbolKeySetProvider source) { this.source = source; }

    void prepare(MemoryTracker tracker) {
        close();
        memory.setMemoryTracker(tracker);
        var keys = source.getMatchedSymbolKeys();
        int size = keys.size();
        if (size == 0) return;
        first = keys.getQuick(0);
        last = keys.getQuick(size - 1);
        long span = last - first + 1;
        if (span == size) return; // A contiguous range needs no allocation or lookup.
        long words = (span + 63) >>> 6;
        if (words * 8 <= size * 4L) {
            memory.ensureCapacity(words);
            memory.fill(0);
            for (int i = 0; i < size; i++) {
                long bit = keys.getQuick(i) - first;
                long address = memory.getAddress() + (bit >>> 6) * 8;
                Unsafe.putLong(address, Unsafe.getLong(address) | (1L << bit));
            }
            count = -1; // Bitmap storage never exceeds the sorted-key representation.
        } else {
            memory.ensureCapacity((size + 1L) / 2);
            for (int i = 0; i < size; i++) Unsafe.putInt(memory.getAddress() + i * 4L, keys.getQuick(i));
            count = size;
        }
    }

    void write(long descriptor) {
        Unsafe.putLong(descriptor, first);
        Unsafe.putLong(descriptor + 8, last);
        Unsafe.putLong(descriptor + 16, memory.getAddress());
        Unsafe.putLong(descriptor + 24, count);
    }

    static Expr contains(Builder b, Expr descriptor, Expr key) {
        Var found = b.let(bool(false));
        Var first = b.let(load(descriptor));
        b.when(binary(Op.BOTH, binary(Op.GE, key, first), binary(Op.GE, load(at(descriptor, 8)), key)), present -> {
            Var count = present.let(load(at(descriptor, 24)));
            present.when(eq(count, i64(0)), range -> range.assign(found, bool(true)));
            present.when(ne(count, i64(0)), lookup -> {
                Var base = lookup.let(new Load(MemoryType.ADDRESS, at(descriptor, 16)));
                lookup.when(eq(count, i64(-1)), bitmap -> {
                    Var bit = bitmap.let(binary(Op.SUB, key, first));
                    Var word = bitmap.let(load(new Offset(base, binary(Op.MUL, binary(Op.USHR, bit, i64(6)), i64(8)))));
                    bitmap.assign(found, ne(binary(Op.AND, binary(Op.USHR, word, binary(Op.AND, bit, i64(63))), i64(1)), i64(0)));
                });
                lookup.when(ne(count, i64(-1)), sorted -> {
                    Var lo = sorted.let(i64(0)), hi = sorted.let(count);
                    sorted.loop(binary(Op.LT, lo, hi), search -> {
                        Var mid = search.let(binary(Op.USHR, binary(Op.ADD, lo, hi), i64(1)));
                        Var value = search.let(new Load(MemoryType.I32, new Offset(base, binary(Op.MUL, mid, i64(4)))));
                        search.when(binary(Op.LT, value, key), lower -> lower.assign(lo, binary(Op.ADD, mid, i64(1))));
                        search.when(binary(Op.GE, value, key), upper -> upper.assign(hi, mid));
                    });
                    sorted.when(binary(Op.LT, lo, count), valid -> valid.assign(found,
                            eq(new Load(MemoryType.I32, new Offset(base, binary(Op.MUL, lo, i64(4)))), key)));
                });
            });
        });
        return found;
    }

    @Override public void close() {
        memory.close();
        memory.setMemoryTracker(null);
        first = 0;
        last = -1;
        count = 0;
    }
}
