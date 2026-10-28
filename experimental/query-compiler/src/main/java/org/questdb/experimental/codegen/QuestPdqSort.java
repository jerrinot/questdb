/*
    pdqsort.h - Pattern-defeating quicksort.

    Copyright (c) 2021 Orson Peters

    This software is provided 'as-is', without any express or implied warranty. In no event will the
    authors be held liable for any damages arising from the use of this software.

    Permission is granted to anyone to use this software for any purpose, including commercial
    applications, and to alter it and redistribute it freely, subject to the following restrictions:

    1. The origin of this software must not be misrepresented; you must not claim that you wrote the
       original software. If you use this software in a product, an acknowledgment in the product
       documentation would be appreciated but is not required.

    2. Altered source versions must be plainly marked as such, and must not be misrepresented as
       being the original software.

    3. This notice may not be removed or altered from any source distribution.
*/

package org.questdb.experimental.codegen;

import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.QuestEncodedSort.*;

/** Altered port of pdqsort.h's non-branchless path to typed IR, with a bounded explicit work stack. */
final class QuestPdqSort {
    private QuestPdqSort() { }

    static void emit(Builder b, Expr entries, Expr count, Expr stack, Expr tailOnly) {
        b.when(tailOnly, tail -> tail.call(Helpers.TAIL, List.of(entries, count, stack)));
        b.when(eq(tailOnly, bool(false)), full -> full.call(Helpers.FULL, List.of(entries, count, stack)));
    }

    private static final class Helpers {
        static final Helper FULL = create(false), TAIL = create(true);
        private static Helper create(boolean tailOnly) {
            var args = List.of(new Var(0, Type.ADDRESS), new Var(1, Type.I64), new Var(2, Type.ADDRESS));
            Builder b = new Builder(args);
            Expr entries = args.get(0), count = args.get(1), stack = args.get(2);
            Var bad = b.let(i64(0)), n = b.let(count);
            b.loop(binary(Op.LT, i64(1), n), log -> { log.assign(n, binary(Op.USHR, n, i64(1))); log.assign(bad, add(bad, i64(1))); });
            Var sp = b.let(i64(0));
            b.when(binary(Op.LT, i64(1), count), init -> push(init, stack, sp, i64(0), count, bad, bool(true)));
            b.loop(binary(Op.LT, i64(0), sp), range -> {
                range.assign(sp, binary(Op.SUB, sp, i64(1)));
                Expr frame = new Offset(stack, binary(Op.MUL, sp, i64(32)));
                Var begin = range.let(load(frame)), end = range.let(load(at(frame, 8)));
                range.assign(bad, load(at(frame, 16)));
                Var leftmost = range.let(ne(load(at(frame, 24)), i64(0)));
                range.loop(binary(Op.LT, add(begin, i64(1)), end), part -> {
                    part.poll();
                    Var size = part.let(binary(Op.SUB, end, begin)), done = part.let(bool(false));
                    part.when(binary(Op.LT, size, i64(24)), small -> {
                        insertion(small, entries, begin, end, false, leftmost, tailOnly);
                        small.assign(done, bool(true));
                    });
                    part.when(eq(done, bool(false)), quick -> {
                        Var middle = quick.let(add(begin, binary(Op.USHR, size, i64(1))));
                        quick.when(binary(Op.LT, i64(128), size), ninther -> {
                            sort3(ninther, entries, begin, middle, binary(Op.SUB, end, i64(1)), tailOnly);
                            sort3(ninther, entries, add(begin, i64(1)), binary(Op.SUB, middle, i64(1)), binary(Op.SUB, end, i64(2)), tailOnly);
                            sort3(ninther, entries, add(begin, i64(2)), add(middle, i64(1)), binary(Op.SUB, end, i64(3)), tailOnly);
                            sort3(ninther, entries, binary(Op.SUB, middle, i64(1)), middle, add(middle, i64(1)), tailOnly);
                            swap(ninther, entry(entries, begin), entry(entries, middle));
                        });
                        quick.when(binary(Op.GE, i64(128), size), median -> sort3(median, entries, middle, begin, binary(Op.SUB, end, i64(1)), tailOnly));
                        Var equal = quick.let(bool(false));
                        quick.when(eq(leftmost, bool(false)), previous -> previous.assign(equal,
                                eq(less(previous, words(entry(entries, binary(Op.SUB, begin, i64(1)))), words(entry(entries, begin)), tailOnly), bool(false))));
                        quick.when(equal, duplicates -> {
                            Var pivot = partitionLeft(duplicates, entries, begin, end, tailOnly);
                            duplicates.assign(begin, add(pivot, i64(1)));
                        });
                        quick.when(eq(equal, bool(false)), unique -> {
                            Var[] result = partitionRight(unique, entries, begin, end, tailOnly);
                            Var pivot = result[0], already = result[1];
                            Var left = unique.let(binary(Op.SUB, pivot, begin));
                            Var right = unique.let(binary(Op.SUB, end, add(pivot, i64(1))));
                            Var unbalanced = unique.let(binary(Op.EITHER, binary(Op.LT, left, binary(Op.USHR, size, i64(3))), binary(Op.LT, right, binary(Op.USHR, size, i64(3)))));
                            unique.when(unbalanced, badPart -> {
                                badPart.assign(bad, binary(Op.SUB, bad, i64(1)));
                                badPart.when(eq(bad, i64(0)), fallback -> {
                                    heapSort(fallback, entries, begin, end, tailOnly);
                                    fallback.assign(done, bool(true));
                                });
                                badPart.when(eq(done, bool(false)), shuffle -> {
                                    breakPattern(shuffle, entries, begin, pivot, left);
                                    breakPattern(shuffle, entries, add(pivot, i64(1)), end, right);
                                });
                            });
                            unique.when(binary(Op.BOTH, eq(unbalanced, bool(false)), already), ordered -> {
                                Var leftSorted = insertion(ordered, entries, begin, pivot, true, bool(true), tailOnly);
                                ordered.when(leftSorted, remaining -> remaining.assign(done,
                                        insertion(remaining, entries, add(pivot, i64(1)), end, true, bool(true), tailOnly)));
                            });
                            unique.when(eq(done, bool(false)), recurse -> {
                                // Same disjoint partitions and sentinel contracts; smaller first bounds stack to log2(n).
                                Var leftFirst = recurse.let(binary(Op.LT, left, right));
                                recurse.when(leftFirst, smaller -> {
                                    smaller.when(binary(Op.LT, i64(1), right), save -> push(save, stack, sp, add(pivot, i64(1)), end, bad, bool(false)));
                                    smaller.assign(end, pivot);
                                });
                                recurse.when(eq(leftFirst, bool(false)), smaller -> {
                                    smaller.when(binary(Op.LT, i64(1), left), save -> push(save, stack, sp, begin, pivot, bad, leftmost));
                                    smaller.assign(begin, add(pivot, i64(1)));
                                    smaller.assign(leftmost, bool(false));
                                });
                            });
                        });
                    });
                    part.when(done, finish -> finish.assign(begin, end));
                });
            });
            b.exit(0);
            return new Helper(tailOnly ? "encoded_pdq_tail" : "encoded_pdq_full", args, b.block());
        }
    }

    private static void push(Builder b, Expr stack, Var sp, Expr begin, Expr end, Expr bad, Expr leftmost) {
        b.when(binary(Op.GE, sp, i64(64)), overflow -> overflow.exit(INVALID_STACK));
        Expr frame = new Offset(stack, binary(Op.MUL, sp, i64(32)));
        b.store(frame, begin); b.store(at(frame, 8), end); b.store(at(frame, 16), bad);
        b.store(at(frame, 24), new Select(leftmost, i64(1), i64(0)));
        b.assign(sp, add(sp, i64(1)));
    }

    private static Var insertion(Builder b, Expr entries, Expr begin, Expr end, boolean partial, Expr leftmost, boolean tailOnly) {
        Var current = b.let(add(begin, i64(1))), moves = b.let(i64(0));
        b.loop(binary(Op.BOTH, binary(Op.LT, current, end), partial ? binary(Op.GE, i64(8), moves) : bool(true)), next -> {
            poll(next, current);
            Var sift = next.let(current);
            Expr[] value = words(entry(entries, current));
            next.when(less(next, value, words(entry(entries, binary(Op.SUB, current, i64(1)))), tailOnly), misplaced -> {
                Expr[] saved = save(misplaced, value);
                Var moving = misplaced.let(bool(true));
                misplaced.loop(moving, shift -> {
                    store(shift, entry(entries, sift), words(entry(entries, binary(Op.SUB, sift, i64(1)))));
                    shift.assign(sift, binary(Op.SUB, sift, i64(1)));
                    shift.assign(moving, bool(false));
                    // The non-leftmost partition has a smaller/equal sentinel immediately before begin.
                    shift.when(binary(Op.EITHER, eq(leftmost, bool(false)), ne(sift, begin)), compare -> compare.assign(moving,
                            less(compare, saved, words(entry(entries, binary(Op.SUB, sift, i64(1)))), tailOnly)));
                });
                store(misplaced, entry(entries, sift), saved);
                misplaced.assign(moves, add(moves, binary(Op.SUB, current, sift)));
            });
            next.assign(current, add(current, i64(1)));
        });
        return b.let(binary(Op.BOTH, binary(Op.GE, current, end), partial ? binary(Op.GE, i64(8), moves) : bool(true)));
    }

    private static void sort3(Builder b, Expr entries, Expr a, Expr c, Expr d, boolean tailOnly) {
        sort2(b, entry(entries, a), entry(entries, c), tailOnly);
        sort2(b, entry(entries, c), entry(entries, d), tailOnly);
        sort2(b, entry(entries, a), entry(entries, c), tailOnly);
    }
    private static void sort2(Builder b, Expr a, Expr c, boolean tailOnly) { b.when(less(b, words(c), words(a), tailOnly), swap -> swap(swap, a, c)); }

    private static void scan(Builder b, Expr entries, Var pos, int step, Expr[] pivot, boolean pivotLeft, boolean whileLess, boolean tailOnly) {
        b.assign(pos, add(pos, i64(step)));
        Var keep = less(b, pivotLeft ? pivot : words(entry(entries, pos)), pivotLeft ? words(entry(entries, pos)) : pivot, tailOnly);
        b.loop(eq(keep, bool(whileLess)), next -> {
            poll(next, pos);
            next.assign(pos, add(pos, i64(step)));
            next.assign(keep, less(next, pivotLeft ? pivot : words(entry(entries, pos)), pivotLeft ? words(entry(entries, pos)) : pivot, tailOnly));
        });
    }

    private static Var[] partitionRight(Builder b, Expr entries, Expr begin, Expr end, boolean tailOnly) {
        Expr[] pivot = save(b, words(entry(entries, begin)));
        Var first = b.let(begin), last = b.let(end);
        scan(b, entries, first, 1, pivot, false, true, tailOnly);
        Var guarded = b.let(eq(first, add(begin, i64(1))));
        b.when(guarded, guard -> {
            Var keep = guard.let(bool(true));
            guard.loop(binary(Op.BOTH, binary(Op.LT, first, last), keep), next -> {
                poll(next, last);
                next.assign(last, binary(Op.SUB, last, i64(1)));
                next.assign(keep, eq(less(next, words(entry(entries, last)), pivot, tailOnly), bool(false)));
            });
        });
        b.when(eq(guarded, bool(false)), unguarded -> scan(unguarded, entries, last, -1, pivot, false, false, tailOnly));
        Var already = b.let(binary(Op.GE, first, last));
        b.loop(binary(Op.LT, first, last), exchange -> {
            swap(exchange, entry(entries, first), entry(entries, last));
            scan(exchange, entries, first, 1, pivot, false, true, tailOnly);
            scan(exchange, entries, last, -1, pivot, false, false, tailOnly);
        });
        Var position = b.let(binary(Op.SUB, first, i64(1)));
        store(b, entry(entries, begin), words(entry(entries, position)));
        store(b, entry(entries, position), pivot);
        return new Var[]{position, already};
    }

    private static Var partitionLeft(Builder b, Expr entries, Expr begin, Expr end, boolean tailOnly) {
        Expr[] pivot = save(b, words(entry(entries, begin)));
        Var first = b.let(begin), last = b.let(end);
        scan(b, entries, last, -1, pivot, true, true, tailOnly);
        Var guarded = b.let(eq(add(last, i64(1)), end));
        b.when(guarded, guard -> {
            Var keep = guard.let(bool(true));
            guard.loop(binary(Op.BOTH, binary(Op.LT, first, last), keep), next -> {
                poll(next, first);
                next.assign(first, add(first, i64(1)));
                next.assign(keep, eq(less(next, pivot, words(entry(entries, first)), tailOnly), bool(false)));
            });
        });
        b.when(eq(guarded, bool(false)), unguarded -> scan(unguarded, entries, first, 1, pivot, true, false, tailOnly));
        b.loop(binary(Op.LT, first, last), exchange -> {
            swap(exchange, entry(entries, first), entry(entries, last));
            scan(exchange, entries, last, -1, pivot, true, true, tailOnly);
            scan(exchange, entries, first, 1, pivot, true, false, tailOnly);
        });
        store(b, entry(entries, begin), words(entry(entries, last)));
        store(b, entry(entries, last), pivot);
        return last;
    }

    private static void breakPattern(Builder b, Expr entries, Expr begin, Expr end, Expr size) {
        b.when(binary(Op.GE, size, i64(24)), shuffle -> {
            Expr quarter = binary(Op.USHR, size, i64(2));
            swap(shuffle, entry(entries, begin), entry(entries, add(begin, quarter)));
            swap(shuffle, entry(entries, binary(Op.SUB, end, i64(1))), entry(entries, binary(Op.SUB, end, quarter)));
            shuffle.when(binary(Op.LT, i64(128), size), large -> {
                for (int i = 1; i <= 2; i++) {
                    swap(large, entry(entries, add(begin, i64(i))), entry(entries, add(begin, add(quarter, i64(i)))));
                    swap(large, entry(entries, binary(Op.SUB, end, i64(i + 1))), entry(entries, binary(Op.SUB, end, add(quarter, i64(i)))));
                }
            });
        });
    }

    static void heapSort(Builder b, Expr entries, Expr begin, Expr end, boolean tailOnly) {
        Var count = b.let(binary(Op.SUB, end, begin));
        Var root = b.let(binary(Op.USHR, count, i64(1)));
        b.loop(binary(Op.LT, i64(0), root), build -> {
            build.assign(root, binary(Op.SUB, root, i64(1)));
            siftDown(build, entry(entries, begin), root, count, tailOnly);
        });
        b.loop(binary(Op.LT, i64(1), count), pop -> {
            poll(pop, count);
            pop.assign(count, binary(Op.SUB, count, i64(1)));
            swap(pop, entry(entries, begin), entry(entries, add(begin, count)));
            siftDown(pop, entry(entries, begin), i64(0), count, tailOnly);
        });
    }

    private static void siftDown(Builder b, Expr entries, Expr root, Expr count, boolean tailOnly) {
        Var hole = b.let(root), active = b.let(bool(true));
        Expr[] value = save(b, words(entry(entries, hole)));
        b.loop(binary(Op.BOTH, active, binary(Op.LT, add(binary(Op.MUL, hole, i64(2)), i64(1)), count)), sift -> {
            Var child = sift.let(add(binary(Op.MUL, hole, i64(2)), i64(1)));
            sift.when(binary(Op.LT, add(child, i64(1)), count), right -> right.when(
                    less(right, words(entry(entries, child)), words(entry(entries, add(child, i64(1)))), tailOnly), larger -> larger.assign(child, add(child, i64(1)))));
            sift.assign(active, less(sift, value, words(entry(entries, child)), tailOnly));
            sift.when(active, move -> { store(move, entry(entries, hole), words(entry(entries, child))); move.assign(hole, child); });
        });
        store(b, entry(entries, hole), value);
    }
}
