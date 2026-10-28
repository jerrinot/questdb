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
import static org.questdb.experimental.codegen.QuestEncodedTopK.*;

/** SortKeyEncoder's single-VARCHAR encoding and EncodedTopKBuffer's admission, in batches. */
final class QuestVarcharTopK {
    static final int SELECTION = 48, ROW_BASE = 56;
    static final int INVALID_UTF8 = -8;
    private static final long ONES = 0x0101010101010101L;

    private QuestVarcharTopK() { }

    static Program program(boolean descending) {
        Builder b = new Builder();
        Var index = b.let(load(at(COLUMNS, NEXT))), count = b.let(load(at(STATE, COUNT)));
        Var heapPos = b.let(load(at(STATE, HEAP_POS)));
        Var entries = b.let(new Load(MemoryType.ADDRESS, at(STATE, ENTRIES)));
        Var heap = b.let(new Load(MemoryType.ADDRESS, at(STATE, HEAP)));
        Var selection = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, SELECTION)));
        Var first = b.let(ne(load(at(STATE, FIRST)), i64(0)));
        b.loop(binary(Op.LT, index, ROWS), row -> {
            row.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var r = row.let(new Select(eq(selection, nil()), index,
                    load(new Offset(selection, binary(Op.MUL, index, i64(8))))));
            Var rowId = row.let(add(load(at(COLUMNS, ROW_BASE)), r));
            var value = QuestVarchar.decode(row, COLUMNS, r);
            Var length = row.let(new Select(value.isNull(), i64(1), add(value.size(), i64(2))));
            Var prefix = row.let(i64(descending ? 0xff00000000000000L : 0));
            Var prefix6 = row.let(bool(false));
            row.when(eq(value.isNull(), bool(false)), present -> {
                Var aux = present.let(new Offset(new Load(MemoryType.ADDRESS, COLUMNS), binary(Op.MUL, r, i64(16))));
                present.assign(prefix6, binary(Op.BOTH, eq(load(at(COLUMNS, 32)), i64(0)),
                        eq(binary(Op.AND, new Load(MemoryType.I32, aux), i64(1)), i64(0))));
                present.when(prefix6, split -> {
                    Var word = split.let(binary(Op.AND, load(at(aux, 4)), i64(0x0000ffffffffffffL)));
                    validateWord(split, word);
                    Expr normalized = binary(Op.XOR, add(word, i64(0x0000010101010101L)),
                            i64(descending ? 0x0000ffffffffffffL : 0));
                    split.assign(prefix, binary(Op.XOR, i64(descending ? 0xfe00000000000000L : 0x0100000000000000L),
                            binary(Op.USHR, reverseBytes(split, normalized), i64(8))));
                });
                present.when(eq(prefix6, bool(false)), inline -> inline.assign(prefix,
                        leadingWord(inline, value.address(), value.size(), descending)));
            });
            Var rejected = row.let(bool(false));
            row.when(ne(load(at(STATE, HAS_THRESHOLD)), i64(0)), threshold -> {
                Var mask = threshold.let(new Select(prefix6, i64(-256), i64(-1)));
                Var cmp = compareUnsigned(threshold, binary(Op.AND, prefix, mask), binary(Op.AND, load(at(STATE, K1)), mask));
                Var settled = threshold.let(ne(cmp, i64(0)));
                threshold.when(eq(cmp, i64(0)), tie -> {
                    Expr prefixBytes = new Select(prefix6, i64(7), i64(8));
                    tie.when(binary(Op.EITHER, binary(Op.GE, prefixBytes, length), binary(Op.GE, prefixBytes, load(at(STATE, LENGTH)))), shortKey -> {
                        shortKey.assign(cmp, compareUnsigned(shortKey, length, load(at(STATE, LENGTH))));
                        shortKey.when(eq(cmp, i64(0)), same -> same.assign(cmp, compareUnsigned(same, rowId, load(at(STATE, ROW_ID)))));
                        shortKey.assign(settled, bool(true));
                    });
                });
                threshold.assign(rejected, binary(Op.BOTH, settled, beyond(cmp, first)));
            });
            row.when(eq(rejected, bool(false)), accepted -> {
                accepted.when(binary(Op.GE, count, load(at(STATE, STOP))), full -> finish(full, ENTRY_SPACE, index, count, heapPos));
                // Match encodeVariable's padding reservation, including short-key scratch bytes.
                Var required = accepted.let(add(heapPos, add(length, new Select(binary(Op.LT, length, i64(16)), i64(16), i64(0)))));
                accepted.when(binary(Op.LT, load(at(STATE, HEAP_SIZE)), required), full -> {
                    full.store(at(COLUMNS, NEED), required);
                    finish(full, HEAP_SPACE, index, count, heapPos);
                });
                Var key = accepted.let(new Offset(heap, heapPos));
                accepted.store(MemoryType.I8, key, new Select(value.isNull(), i64(descending ? 255 : 0), i64(descending ? 254 : 1)));
                accepted.when(eq(value.isNull(), bool(false)), nonNull -> encodeBytes(nonNull, value.address(), value.size(), at(key, 1), descending));
                accepted.when(binary(Op.LT, length, i64(16)), pad -> {
                    pad.store(new Offset(key, length), i64(0));
                    pad.store(new Offset(key, add(length, i64(8))), i64(0));
                });
                Var entry = accepted.let(new Offset(entries, binary(Op.MUL, count, i64(40))));
                accepted.store(entry, reverseBytes(accepted, load(key)));
                accepted.store(at(entry, 8), reverseBytes(accepted, load(at(key, 8))));
                accepted.store(at(entry, 16), length);
                accepted.store(at(entry, 24), heapPos);
                accepted.store(at(entry, 32), rowId);
                Var keep = accepts(accepted, entry, key);
                accepted.when(keep, commit -> {
                    commit.assign(count, add(count, i64(1)));
                    commit.when(binary(Op.LT, i64(16), length), longKey -> longKey.assign(heapPos, add(heapPos, length)));
                    commit.when(eq(count, load(at(STATE, STOP))), full -> finish(full, BOUNDARY, add(index, i64(1)), count, heapPos));
                });
            });
            row.assign(index, add(index, i64(1)));
        });
        finish(b, DONE, index, count, heapPos);
        return new Program(b.block());
    }

    private static void validateWord(Builder b, Expr word) {
        Expr inv = binary(Op.XOR, word, i64(-1));
        Expr ff = binary(Op.AND, binary(Op.AND, binary(Op.SUB, inv, i64(ONES)), word), i64(0x8080808080808080L));
        b.when(ne(ff, i64(0)), invalid -> invalid.exit(INVALID_UTF8));
    }

    private static Var leadingWord(Builder b, Expr pointer, Expr size, boolean descending) {
        Var result = b.let(i64(descending ? 0xfe00000000000000L : 0x0100000000000000L));
        b.when(binary(Op.GE, size, i64(8)), full -> {
            Var word = full.let(load(pointer));
            validateWord(full, word);
            full.assign(result, binary(Op.XOR, result, binary(Op.USHR,
                    reverseBytes(full, binary(Op.XOR, add(word, i64(ONES)), i64(descending ? -1 : 0))), i64(8))));
        });
        b.when(binary(Op.LT, size, i64(8)), shortKey -> {
            Var index = shortKey.let(i64(0));
            shortKey.loop(binary(Op.LT, index, size), part -> {
                Var value = part.let(binary(Op.AND, new Load(MemoryType.I8, new Offset(pointer, index)), i64(255)));
                part.when(eq(value, i64(255)), invalid -> invalid.exit(INVALID_UTF8));
                Expr normalized = binary(Op.XOR, add(value, i64(1)), i64(descending ? 255 : 0));
                // Shift left via multiplication, with a byte-aligned shift in [0,48].
                Expr weight = binary(Op.USHR, i64(1L << 48), binary(Op.MUL, index, i64(8)));
                part.assign(result, binary(Op.XOR, result, binary(Op.MUL, normalized, weight)));
                part.assign(index, add(index, i64(1)));
            });
            if (descending) shortKey.when(binary(Op.LT, size, i64(7)), terminator -> terminator.assign(result,
                    binary(Op.XOR, result, binary(Op.MUL, i64(255), binary(Op.USHR, i64(1L << 48), binary(Op.MUL, size, i64(8)))))));
        });
        return result;
    }

    private static void encodeBytes(Builder b, Expr source, Expr size, Expr target, boolean descending) {
        Var index = b.let(i64(0));
        b.loop(binary(Op.GE, binary(Op.SUB, size, index), i64(8)), word -> {
            Var value = word.let(load(new Offset(source, index)));
            validateWord(word, value);
            word.store(new Offset(target, index), binary(Op.XOR, add(value, i64(ONES)), i64(descending ? -1 : 0)));
            word.assign(index, add(index, i64(8)));
        });
        b.loop(binary(Op.LT, index, size), tail -> {
            Var value = tail.let(binary(Op.AND, new Load(MemoryType.I8, new Offset(source, index)), i64(255)));
            tail.when(eq(value, i64(255)), invalid -> invalid.exit(INVALID_UTF8));
            tail.store(MemoryType.I8, new Offset(target, index), binary(Op.XOR, add(value, i64(1)), i64(descending ? 255 : 0)));
            tail.assign(index, add(index, i64(1)));
        });
        b.store(MemoryType.I8, new Offset(target, size), i64(descending ? 255 : 0));
    }

}
