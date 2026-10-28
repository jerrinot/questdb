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

import static org.questdb.experimental.codegen.Ir.*;

/** Exact byte predicates. Every vector and tail load stays inside the input slices. */
final class ByteSequences {
    private ByteSequences() { }

    /** Lazy, zero-padded prefix with no reads beyond the borrowed slice. */
    static Expr sixPrefix(Expr address, Expr size) {
        Expr result = i64(0);
        for (int i = 0; i < 6; i++) {
            Expr value = binary(Op.AND, new Load(MemoryType.I8, at(address, i)), i64(255));
            result = binary(Op.XOR, result, new Select(binary(Op.LT, i64(i), size),
                    binary(Op.MUL, value, i64(1L << (i * 8))), i64(0)));
        }
        return result;
    }

    /** Utf8s.compare: prefix, full words from byte six, bounded tail, then length. */
    static Var compare(Builder b, Expr left, Expr leftSize, Expr leftPrefix,
                       Expr right, Expr rightSize, Expr rightPrefix) {
        Var a = b.let(leftPrefix), c = b.let(rightPrefix), order = b.let(i64(0));
        b.when(ne(a, c), prefix -> prefix.assign(order, compareWord(a, c)));
        b.when(eq(order, i64(0)), rest -> {
            Var length = rest.let(new Select(binary(Op.LT, leftSize, rightSize), leftSize, rightSize));
            Var index = rest.let(i64(6));
            rest.loop(binary(Op.BOTH, eq(order, i64(0)), binary(Op.GE, binary(Op.SUB, length, index), i64(8))), word -> {
                Var x = word.let(load(new Offset(left, index))), y = word.let(load(new Offset(right, index)));
                word.when(ne(x, y), different -> different.assign(order, compareWord(x, y)));
                word.assign(index, add(index, i64(8)));
            });
            rest.loop(binary(Op.BOTH, eq(order, i64(0)), binary(Op.LT, index, length)), tail -> {
                Expr x = binary(Op.AND, new Load(MemoryType.I8, new Offset(left, index)), i64(255));
                Expr y = binary(Op.AND, new Load(MemoryType.I8, new Offset(right, index)), i64(255));
                tail.assign(order, binary(Op.SUB, x, y));
                tail.assign(index, add(index, i64(1)));
            });
            rest.when(eq(order, i64(0)), equal -> equal.assign(order, binary(Op.SUB, leftSize, rightSize)));
        });
        return order;
    }

    private static Expr compareWord(Expr left, Expr right) {
        Expr shift = binary(Op.AND, new TrailingZeros(binary(Op.XOR, left, right)), i64(-8));
        return binary(Op.SUB, binary(Op.AND, binary(Op.USHR, left, shift), i64(255)),
                binary(Op.AND, binary(Op.USHR, right, shift), i64(255)));
    }

    /** Literal bytes are compiler constants; NULL handling belongs to the resolved predicate. */
    static Var contains(Builder b, Expr pointer, Expr length, byte[] pattern) {
        if (pattern.length < 1 || pattern.length > 8) throw new IllegalArgumentException("Expected 1..8 literal bytes");
        int probe = pattern.length - 1;
        while (probe > 0 && pattern[probe] == pattern[0]) probe--;
        final int second = probe, lanes = 32;
        // Repeated-byte literals use exact contiguous runs in a single byte-match mask.
        int readable = lanes + (second == 0 ? 0 : pattern.length - 1);
        int step = second == 0 ? lanes - pattern.length + 1 : lanes;
        Var found = b.let(bool(false));
        Expr sufficient = binary(Op.GE, length, i64(readable));
        b.when(sufficient, vector -> {
            Var index = vector.let(i64(0));
            Expr last = binary(Op.SUB, length, i64(readable));
            vector.loop(binary(Op.BOTH, eq(found, bool(false)), binary(Op.LT, index, last)), block -> {
                containsCandidates(block, pointer, index, pattern, second, lanes, found);
                block.assign(index, add(index, i64(step)));
            });
            // An overlapping final block avoids a bytewise remainder without reading past the slice.
            vector.when(eq(found, bool(false)), tail -> containsCandidates(tail, pointer, last, pattern, second, lanes, found));
        });
        b.when(eq(sufficient, bool(false)), small -> small.assign(found, containsSwar(small, pointer, length, pattern)));
        return found;
    }

    private static void containsCandidates(Builder b, Expr pointer, Expr index, byte[] pattern,
                                           int second, int lanes, Var found) {
        Expr first = eq(new Load(MemoryType.I8, new Offset(pointer, index)), i64(pattern[0]));
        Expr pair = second == 0 ? first : binary(Op.BOTH, first,
                eq(new Load(MemoryType.I8, new Offset(pointer, add(index, i64(second)))), i64(pattern[second])));
        Var matches = b.let(new VectorMask(lanes, pair));
        if (second == 0) {
            // Each intersection extends a proven run; overlap between blocks preserves runs
            // straddling their boundary. No scalar candidate verification is needed.
            for (int covered = 1; covered < pattern.length;) {
                int shift = Math.min(covered, pattern.length - covered);
                b.assign(matches, binary(Op.AND, matches, binary(Op.USHR, matches, i64(shift))));
                covered += shift;
            }
            b.assign(found, ne(matches, i64(0)));
            return;
        }
        long expected = literalWord(pattern);
        b.loop(binary(Op.BOTH, eq(found, bool(false)), ne(matches, i64(0))), candidate -> {
            Expr offset = add(index, new TrailingZeros(matches));
            candidate.assign(found, eq(loadLiteral(new Offset(pointer, offset), pattern.length), i64(expected)));
            candidate.assign(matches, binary(Op.AND, matches, binary(Op.SUB, matches, i64(1))));
        });
    }

    /** QuestDB's first-byte SWAR search, with constant-sized exact verification and bounded tails. */
    private static Var containsSwar(Builder b, Expr pointer, Expr length, byte[] pattern) {
        long expected = literalWord(pattern);
        long mask = pattern.length == 8 ? -1L : (1L << (pattern.length * 8)) - 1;
        Var found = b.let(bool(false)), index = b.let(i64(0));
        Expr end = binary(Op.SUB, length, i64(7 + pattern.length - 1));
        b.loop(binary(Op.BOTH, eq(found, bool(false)), binary(Op.LT, index, end)), block -> {
            Var x = block.let(binary(Op.XOR, load(new Offset(pointer, index)), i64((pattern[0] & 255L) * 0x0101010101010101L)));
            Var marked = block.let(binary(Op.AND, binary(Op.AND, binary(Op.SUB, x, i64(0x0101010101010101L)),
                    binary(Op.XOR, x, i64(-1))), i64(0x8080808080808080L)));
            block.loop(binary(Op.BOTH, eq(found, bool(false)), ne(marked, i64(0))), candidate -> {
                Var offset = candidate.let(add(index, binary(Op.USHR, new TrailingZeros(marked), i64(3))));
                Var value = candidate.let(i64(0));
                Expr fullWord = binary(Op.GE, binary(Op.SUB, length, offset), i64(8));
                candidate.when(fullWord, wide -> wide.assign(value, binary(Op.AND, load(new Offset(pointer, offset)), i64(mask))));
                candidate.when(eq(fullWord, bool(false)), tail -> tail.assign(value, loadLiteral(new Offset(pointer, offset), pattern.length)));
                candidate.assign(found, eq(value, i64(expected)));
                candidate.assign(marked, binary(Op.AND, marked, binary(Op.SUB, marked, i64(1))));
            });
            block.assign(index, add(index, i64(8)));
        });
        Expr last = binary(Op.SUB, length, i64(pattern.length));
        b.loop(binary(Op.BOTH, eq(found, bool(false)), binary(Op.GE, last, index)), tail -> {
            Expr address = new Offset(pointer, index);
            tail.when(eq(new Load(MemoryType.I8, address), i64(pattern[0])), match ->
                    match.assign(found, eq(loadLiteral(address, pattern.length), i64(expected))));
            tail.assign(index, add(index, i64(1)));
        });
        return found;
    }

    private static long literalWord(byte[] pattern) {
        long word = 0;
        for (int i = 0; i < pattern.length; i++) word |= (pattern[i] & 255L) << (i * 8);
        return word;
    }

    private static Expr loadLiteral(Expr address, int bytes) {
        Expr result = i64(0);
        int offset = 0;
        for (int width : new int[]{8, 4, 2, 1}) {
            if ((bytes & width) == 0) continue;
            MemoryType type = switch (width) {
                case 8 -> MemoryType.I64;
                case 4 -> MemoryType.I32;
                case 2 -> MemoryType.I16;
                default -> MemoryType.I8;
            };
            Expr value = new Load(type, at(address, offset));
            if (width < 8) value = binary(Op.AND, value, i64((1L << (width * 8)) - 1));
            if (offset != 0) value = binary(Op.MUL, value, i64(1L << (offset * 8)));
            result = binary(Op.XOR, result, value);
            offset += width;
        }
        return result;
    }

    static Var equal(Builder b, Expr left, Expr right, Expr length) {
        Var same = b.let(bool(true));
        b.when(binary(Op.GE, length, i64(64)), large -> {
            Var offset = large.let(i64(0));
            Expr last = binary(Op.SUB, length, i64(64));
            large.loop(binary(Op.BOTH, same, binary(Op.LT, offset, last)), block -> {
                block.assign(same, equalBlock(left, right, offset, 64));
                block.assign(offset, add(offset, i64(64)));
            });
            // The final block may overlap the previous block, but cannot cross
            // either slice's end. Avoid decoding the remainder's individual bits.
            large.when(same, tail -> tail.assign(same, equalBlock(left, right, last, 64)));
        });
        b.when(binary(Op.LT, length, i64(64)), small -> equalSmall(small, left, right, length, same, 32));
        return same;
    }

    private static void equalSmall(Builder b, Expr left, Expr right, Expr length, Var same, int bytes) {
        if (bytes == 1) {
            b.when(ne(length, i64(0)), one -> one.assign(same, equalBlock(left, right, i64(0), 1)));
            return;
        }
        // For bytes <= length < 2*bytes, first and last blocks cover the whole
        // slice. Both reads are bounded, even when a slice touches a guard page.
        b.when(binary(Op.GE, length, i64(bytes)), pair -> pair.assign(same,
                binary(Op.BOTH, equalBlock(left, right, i64(0), bytes),
                        equalBlock(left, right, binary(Op.SUB, length, i64(bytes)), bytes))));
        b.when(binary(Op.LT, length, i64(bytes)), smaller -> equalSmall(smaller, left, right, length, same, bytes / 2));
    }

    private static Expr equalBlock(Expr left, Expr right, Expr offset, int bytes) {
        MemoryType width = switch (bytes) {
            case 1 -> MemoryType.I8;
            case 2 -> MemoryType.I16;
            case 4 -> MemoryType.I32;
            default -> MemoryType.I64;
        };
        Expr equal = eq(new Load(width, new Offset(left, offset)), new Load(width, new Offset(right, offset)));
        int lanes = bytes / width.bytes;
        return lanes == 1 ? equal : eq(new VectorMask(lanes, equal), i64((1L << lanes) - 1));
    }
}
