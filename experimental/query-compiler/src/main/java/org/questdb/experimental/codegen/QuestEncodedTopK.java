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

import static io.questdb.griffin.engine.orderby.CompiledTopK.*;
import static org.questdb.experimental.codegen.Ir.*;

/** Operations on QuestDB's normalized 40-byte variable-key entries, independent of source types. */
final class QuestEncodedTopK {
    static final int SOURCE = 0, SOURCE_HEAP = 8, NEXT = 64, NEED = 72, INPUT_BYTES = 80;
    static final int DONE = 0, ENTRY_SPACE = 1, HEAP_SPACE = 2, BOUNDARY = 3;

    private QuestEncodedTopK() { }

    static Program merge() {
        Builder b = new Builder();
        Var index = b.let(load(at(COLUMNS, NEXT))), count = b.let(load(at(STATE, COUNT)));
        Var heapPos = b.let(load(at(STATE, HEAP_POS)));
        Var source = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, SOURCE)));
        Var sourceHeap = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, SOURCE_HEAP)));
        Var entries = b.let(new Load(MemoryType.ADDRESS, at(STATE, ENTRIES)));
        Var heap = b.let(new Load(MemoryType.ADDRESS, at(STATE, HEAP)));
        b.loop(binary(Op.LT, index, ROWS), row -> {
            row.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var entry = row.let(new Offset(source, binary(Op.MUL, index, i64(40))));
            // Match fastRejectsKey: FIRST N only, and equal prefixes still reach the full comparison.
            Var reject = row.let(bool(false));
            row.when(binary(Op.BOTH, ne(load(at(STATE, HAS_THRESHOLD)), i64(0)), ne(load(at(STATE, FIRST)), i64(0))), threshold ->
                    threshold.assign(reject, binary(Op.LT, i64(0), compareUnsigned(threshold, load(entry), load(at(STATE, K1))))));
            row.when(eq(reject, bool(false)), candidate -> {
                candidate.when(binary(Op.GE, count, load(at(STATE, STOP))), full -> finish(full, ENTRY_SPACE, index, count, heapPos));
                Var length = candidate.let(load(at(entry, 16)));
                Var required = candidate.let(add(heapPos, new Select(binary(Op.LT, i64(16), length), length, i64(0))));
                candidate.when(binary(Op.LT, load(at(STATE, HEAP_SIZE)), required), full -> {
                    full.store(at(COLUMNS, NEED), required);
                    finish(full, HEAP_SPACE, index, count, heapPos);
                });
                Var dest = candidate.let(new Offset(entries, binary(Op.MUL, count, i64(40))));
                Var key = candidate.let(new Offset(heap, heapPos));
                QuestMapMutation.copy(candidate, entry, dest, 40);
                candidate.when(binary(Op.LT, i64(16), length), longKey ->
                        QuestMapMutation.copy(longKey, new Offset(sourceHeap, load(at(entry, 24))), key, length));
                candidate.store(at(dest, 24), heapPos);
                candidate.when(accepts(candidate, dest, key), keep -> {
                    keep.assign(count, add(count, i64(1)));
                    keep.assign(heapPos, required);
                    keep.when(eq(count, load(at(STATE, STOP))), full -> finish(full, BOUNDARY, add(index, i64(1)), count, heapPos));
                });
            });
            row.assign(index, add(index, i64(1)));
        });
        finish(b, DONE, index, count, heapPos);
        return new Program(b.block());
    }

    static Program compact() {
        Builder b = new Builder();
        Var index = b.let(load(at(COLUMNS, NEXT))), packed = b.let(load(at(STATE, HEAP_POS)));
        Var source = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, SOURCE)));
        Var entries = b.let(new Load(MemoryType.ADDRESS, at(STATE, ENTRIES)));
        Var scratch = b.let(new Load(MemoryType.ADDRESS, at(STATE, HEAP)));
        b.loop(binary(Op.LT, index, ROWS), row -> {
            row.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var src = row.let(new Offset(source, binary(Op.MUL, index, i64(40))));
            Var dst = row.let(new Offset(entries, binary(Op.MUL, index, i64(40))));
            Var length = row.let(load(at(src, 16)));
            Var required = row.let(add(packed, new Select(binary(Op.LT, i64(16), length), length, i64(0))));
            row.when(binary(Op.LT, load(at(STATE, HEAP_SIZE)), required), full -> {
                full.store(at(COLUMNS, NEED), required);
                finish(full, HEAP_SPACE, index, ROWS, packed);
            });
            row.when(binary(Op.LT, i64(16), length), longKey ->
                    QuestMapMutation.copy(longKey, new Load(MemoryType.ADDRESS, at(src, 24)), new Offset(scratch, packed), length));
            // FIRST N already occupies these slots. LAST N moves forward, safely even with overlap.
            row.when(ne(source, entries), move -> QuestMapMutation.copy(move, src, dst, 40));
            row.store(at(dst, 24), new Select(binary(Op.LT, i64(16), length), packed, i64(0)));
            row.assign(packed, required);
            row.assign(index, add(index, i64(1)));
        });
        // Surviving keys may come from anywhere in the old heap: pack first, then copy back.
        QuestMapMutation.copy(b, scratch, new Load(MemoryType.ADDRESS, at(COLUMNS, SOURCE_HEAP)), packed);
        finish(b, DONE, index, ROWS, packed);
        return new Program(b.block());
    }

    static Var accepts(Builder b, Expr entry, Expr key) {
        Var keep = b.let(bool(true));
        b.when(ne(load(at(STATE, HAS_THRESHOLD)), i64(0)), threshold -> {
            Var length = threshold.let(load(at(entry, 16)));
            Var cmp = compareUnsigned(threshold, load(entry), load(at(STATE, K1)));
            threshold.when(eq(cmp, i64(0)), second -> second.assign(cmp, compareUnsigned(second, load(at(entry, 8)), load(at(STATE, K2)))));
            threshold.when(eq(cmp, i64(0)), tail -> {
                Var min = tail.let(new Select(binary(Op.LT, length, load(at(STATE, LENGTH))), length, load(at(STATE, LENGTH))));
                compareTail(tail, cmp, at(key, 16), new Load(MemoryType.ADDRESS, at(STATE, TAIL)), binary(Op.SUB, min, i64(16)));
                tail.when(eq(cmp, i64(0)), sizes -> sizes.assign(cmp, compareUnsigned(sizes, length, load(at(STATE, LENGTH)))));
                tail.when(eq(cmp, i64(0)), same -> same.assign(cmp, compareUnsigned(same, load(at(entry, 32)), load(at(STATE, ROW_ID)))));
            });
            threshold.assign(keep, eq(beyond(cmp, ne(load(at(STATE, FIRST)), i64(0))), bool(false)));
        });
        return keep;
    }

    static void finish(Builder b, int status, Expr index, Expr count, Expr heapPos) {
        b.store(at(COLUMNS, NEXT), index);
        b.store(at(STATE, COUNT), count);
        b.store(at(STATE, HEAP_POS), heapPos);
        b.exit(status);
    }

    static Expr beyond(Expr cmp, Expr first) {
        return binary(Op.EITHER, eq(cmp, i64(0)), new Select(first, binary(Op.LT, i64(0), cmp), binary(Op.LT, cmp, i64(0))));
    }

    static Var compareUnsigned(Builder b, Expr a, Expr c) {
        Var x = b.let(a), y = b.let(c);
        return b.let(new Select(eq(x, y), i64(0), new Select(binary(Op.LT,
                binary(Op.XOR, x, i64(Long.MIN_VALUE)), binary(Op.XOR, y, i64(Long.MIN_VALUE))), i64(-1), i64(1))));
    }

    static Var reverseBytes(Builder b, Expr value) {
        Var word = b.let(value);
        Expr result = i64(0);
        for (int i = 0; i < 8; i++) {
            Expr part = binary(Op.AND, binary(Op.USHR, word, i64(i * 8)), i64(255));
            result = binary(Op.XOR, result, binary(Op.MUL, part, i64(1L << ((7 - i) * 8))));
        }
        return b.let(result);
    }

    static void compareTail(Builder b, Var cmp, Expr left, Expr right, Expr length) {
        Var index = b.let(i64(0));
        b.loop(binary(Op.BOTH, eq(cmp, i64(0)), binary(Op.GE, binary(Op.SUB, length, index), i64(8))), word -> {
            word.assign(cmp, compareUnsigned(word, reverseBytes(word, load(new Offset(left, index))), reverseBytes(word, load(new Offset(right, index)))));
            word.assign(index, add(index, i64(8)));
        });
        b.loop(binary(Op.BOTH, eq(cmp, i64(0)), binary(Op.LT, index, length)), tail -> {
            tail.assign(cmp, binary(Op.SUB, binary(Op.AND, new Load(MemoryType.I8, new Offset(left, index)), i64(255)),
                    binary(Op.AND, new Load(MemoryType.I8, new Offset(right, index)), i64(255))));
            tail.assign(index, add(index, i64(1)));
        });
    }
}
