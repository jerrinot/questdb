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

import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.QuestEncodedTopK.*;

/** ooo.cpp's serial variable-entry verge/MSD sort. Keys are already normalized, including direction. */
final class QuestEncodedSort {
    static final int HEAP_BASE = 0, UNSTABLE_LIMIT = 8, STACK_CAPACITY = 16, WORKSPACE = 24;
    static final int LIMIT = 32, SELECTED = 40, INPUT_BYTES = 48;
    static final int HISTOGRAM_BYTES = 3 * 256 * 8;
    static final int PDQ_STACK_BYTES = 64 * 32;
    static final int RADIX_STACK_OFFSET = HISTOGRAM_BYTES + PDQ_STACK_BYTES;
    static final int MAX_RADIX_PENDING = 16 * 255 + 1;
    static final int INVALID_STACK = -9;

    private QuestEncodedSort() { }

    static Program program() {
        Builder b = new Builder();
        b.when(binary(Op.LT, i64(1), ROWS), sort -> {
            Var heap = sort.let(load(at(STATE, HEAP_BASE)));
            sort.when(ne(heap, i64(0)), rebase -> {
                Var index = rebase.let(i64(0));
                rebase.loop(binary(Op.LT, index, ROWS), row -> {
                    poll(row, index);
                    Expr key = at(entry(COLUMNS, index), 24);
                    row.store(key, add(load(key), heap));
                    row.assign(index, add(index, i64(1)));
                });
            });
            verge(sort, COLUMNS, ROWS, load(at(STATE, UNSTABLE_LIMIT)),
                    new Load(MemoryType.ADDRESS, at(STATE, WORKSPACE)), load(at(STATE, STACK_CAPACITY)));
        });
        b.store(at(STATE, SELECTED), QuestRowLimit.prefix(ROWS, load(at(STATE, LIMIT))));
        b.exit(0);
        return new Program(b.block());
    }

    private static void verge(Builder b, Expr entries, Expr count, Expr unstableLimit, Expr work, Expr capacity) {
        b.when(binary(Op.LT, count, i64(128)), small -> QuestPdqSort.emit(small, entries, count, work, bool(false)));
        b.when(binary(Op.GE, count, i64(128)), runs -> {
            Var pos = runs.let(i64(0)), unstable = runs.let(i64(-1));
            // Stock uses only the number and final boundary; no boundary array is needed here.
            Var bounds = runs.let(i64(0)), lastBound = runs.let(i64(-1));
            runs.loop(binary(Op.LT, pos, count), run -> {
                run.poll();
                Var begin = run.let(pos);
                Var tail = run.let(binary(Op.GE, unstableLimit, binary(Op.SUB, count, pos)));
                run.when(tail, end -> {
                    end.when(binary(Op.LT, unstable, i64(0)), mark -> mark.assign(unstable, begin));
                    end.assign(pos, count);
                });
                run.when(eq(tail, bool(false)), probe -> {
                    Var hi = probe.let(add(pos, unstableLimit)), lo = probe.let(binary(Op.SUB, hi, i64(1)));
                    Var descending = less(probe, words(entry(entries, hi)), words(entry(entries, lo)), false);
                    extendRun(probe, entries, lo, begin, -1, descending);
                    extendRun(probe, entries, hi, binary(Op.SUB, count, i64(1)), 1, descending);
                    probe.when(binary(Op.GE, add(binary(Op.SUB, hi, lo), i64(1)), unstableLimit), stable -> {
                        stable.when(descending, reverse -> {
                            Var left = reverse.let(lo), right = reverse.let(hi);
                            reverse.loop(binary(Op.LT, left, right), swap -> {
                                poll(swap, left);
                                swap(swap, entry(entries, left), entry(entries, right));
                                swap.assign(left, add(left, i64(1))); swap.assign(right, binary(Op.SUB, right, i64(1)));
                            });
                        });
                        stable.when(binary(Op.BOTH, binary(Op.LT, begin, lo), binary(Op.LT, unstable, i64(0))), mark -> mark.assign(unstable, begin));
                        stable.when(binary(Op.GE, unstable, i64(0)), prefix -> {
                            radix(prefix, entry(entries, unstable), binary(Op.SUB, lo, unstable), work, capacity);
                            boundary(prefix, bounds, lastBound, unstable);
                            prefix.assign(unstable, i64(-1));
                        });
                        boundary(stable, bounds, lastBound, lo);
                    });
                    probe.when(binary(Op.LT, add(binary(Op.SUB, hi, lo), i64(1)), unstableLimit), shortRun ->
                            shortRun.when(binary(Op.LT, unstable, i64(0)), mark -> mark.assign(unstable, begin)));
                    probe.assign(pos, add(hi, i64(1)));
                });
            });
            runs.when(binary(Op.BOTH, binary(Op.GE, unstable, i64(0)), binary(Op.LT, unstable, count)), tail -> {
                radix(tail, entry(entries, unstable), binary(Op.SUB, count, unstable), work, capacity);
                tail.when(binary(Op.EITHER, eq(bounds, i64(0)), ne(lastBound, unstable)), mark -> mark.assign(bounds, add(bounds, i64(1))));
            });
            // Adding the final `count` boundary leaves <=2 for one complete run.
            runs.when(binary(Op.LT, i64(1), bounds), multiple -> radix(multiple, entries, count, work, capacity));
        });
    }

    private static void boundary(Builder b, Var count, Var last, Expr value) {
        b.when(binary(Op.LT, count, i64(254)), add -> { add.assign(count, add(count, i64(1))); add.assign(last, value); });
    }

    private static void extendRun(Builder b, Expr entries, Var position, Expr bound, int step, Expr descending) {
        Var active = b.let(bool(true));
        b.loop(binary(Op.BOTH, active, step < 0 ? binary(Op.LT, bound, position) : binary(Op.LT, position, bound)), scan -> {
            poll(scan, position);
            Expr left = step < 0 ? position : add(position, i64(1));
            Expr right = step < 0 ? binary(Op.SUB, position, i64(1)) : position;
            Var less = less(scan, words(entry(entries, left)), words(entry(entries, right)), false);
            scan.assign(active, eq(less, descending));
            scan.when(active, next -> next.assign(position, add(position, i64(step))));
        });
    }

    private static void radix(Builder b, Expr entries, Expr count, Expr work, Expr capacity) {
        b.call(Radix.HELPER, List.of(entries, count, work, capacity));
    }

    private static final class Radix {
        static final Helper HELPER = create();
        private static Helper create() {
            var args = List.of(new Var(0, Type.ADDRESS), new Var(1, Type.I64), new Var(2, Type.ADDRESS), new Var(3, Type.I64));
            Builder b = new Builder(args);
            Expr entries = args.get(0), total = args.get(1), work = args.get(2), capacity = args.get(3);
            Expr offsets = at(work, 2048), ends = at(work, 4096), pdq = at(work, HISTOGRAM_BYTES), stack = at(work, RADIX_STACK_OFFSET);
            Var sp = b.let(i64(0));
            b.when(binary(Op.LT, i64(1), total), init -> push(init, stack, sp, capacity, i64(0), total, i64(0)));
            b.loop(binary(Op.LT, i64(0), sp), range -> {
                range.poll();
                range.assign(sp, binary(Op.SUB, sp, i64(1)));
                Var frame = range.let(new Offset(stack, binary(Op.MUL, sp, i64(24))));
                Var start = range.let(load(frame)), count = range.let(load(at(frame, 8))), digit = range.let(load(at(frame, 16)));
                Var base = range.let(entry(entries, start));
                Var split = range.let(bool(false));
                range.loop(binary(Op.BOTH, binary(Op.BOTH, binary(Op.GE, count, i64(128)), binary(Op.LT, digit, i64(16))), eq(split, bool(false))), histogram -> {
                    Var v = histogram.let(i64(0));
                    histogram.loop(binary(Op.LT, v, i64(256)), clear -> { clear.store(bucket(work, v), i64(0)); clear.assign(v, add(v, i64(1))); });
                    Var i = histogram.let(i64(0));
                    histogram.loop(binary(Op.LT, i, count), row -> {
                        poll(row, i);
                        Var address = row.let(bucket(work, digit(words(entry(base, i)), digit)));
                        row.store(address, add(load(address), i64(1)));
                        row.assign(i, add(i, i64(1)));
                    });
                    Var nonempty = histogram.let(i64(0));
                    histogram.assign(v, i64(0));
                    histogram.loop(binary(Op.LT, v, i64(256)), check -> {
                        check.when(ne(load(bucket(work, v)), i64(0)), found -> found.assign(nonempty, add(nonempty, i64(1))));
                        check.assign(v, add(v, i64(1)));
                    });
                    histogram.assign(split, binary(Op.LT, i64(1), nonempty));
                    histogram.when(eq(split, bool(false)), uniform -> uniform.assign(digit, add(digit, i64(1))));
                });
                range.when(eq(split, bool(false)), compare -> QuestPdqSort.emit(compare, base, count, pdq, eq(digit, i64(16))));
                range.when(split, partition -> {
                    Var sum = partition.let(i64(0)), v = partition.let(i64(0));
                    partition.loop(binary(Op.LT, v, i64(256)), prefix -> {
                        prefix.store(bucket(offsets, v), sum);
                        prefix.assign(sum, add(sum, load(bucket(work, v))));
                        prefix.store(bucket(ends, v), sum);
                        prefix.assign(v, add(v, i64(1)));
                    });
                    Var steps = partition.let(i64(0));
                    partition.assign(v, i64(0));
                    partition.loop(binary(Op.LT, v, i64(256)), bin -> {
                        bin.poll();
                        bin.loop(binary(Op.LT, load(bucket(offsets, v)), load(bucket(ends, v))), cycle -> {
                            poll(cycle, steps);
                            cycle.assign(steps, add(steps, i64(1)));
                            Var at = cycle.let(entry(base, load(bucket(offsets, v))));
                            Var target = cycle.let(digit(words(at), digit));
                            cycle.when(ne(target, v), displaced -> {
                                Expr[] value = save(displaced, words(at));
                                displaced.loop(ne(target, v), swap -> {
                                    poll(swap, steps);
                                    Var dest = swap.let(entry(base, load(bucket(offsets, target))));
                                    Expr[] previous = save(swap, words(dest));
                                    store(swap, dest, value);
                                    for (int w = 0; w < 5; w++) swap.assign((Var) value[w], previous[w]);
                                    swap.store(bucket(offsets, target), add(load(bucket(offsets, target)), i64(1)));
                                    swap.assign(target, digit(value, digit));
                                    swap.assign(steps, add(steps, i64(1)));
                                });
                                store(displaced, at, value);
                            });
                            cycle.store(bucket(offsets, v), add(load(bucket(offsets, v)), i64(1)));
                        });
                        bin.assign(v, add(v, i64(1)));
                    });
                    // Reverse push gives the same low-to-high bucket traversal as stock recursion.
                    partition.assign(v, i64(256));
                    partition.loop(binary(Op.LT, i64(0), v), child -> {
                        child.assign(v, binary(Op.SUB, v, i64(1)));
                        Var n = child.let(load(bucket(work, v)));
                        child.when(binary(Op.LT, i64(1), n), nontrivial -> push(nontrivial, stack, sp, capacity,
                                add(start, binary(Op.SUB, load(bucket(ends, v)), n)), n, add(digit, i64(1))));
                    });
                });
            });
            b.exit(0);
            return new Helper("encoded_msd_sort", args, b.block());
        }
    }

    private static void push(Builder b, Expr stack, Var sp, Expr capacity, Expr start, Expr count, Expr digit) {
        b.when(binary(Op.GE, sp, capacity), overflow -> overflow.exit(INVALID_STACK));
        Expr frame = new Offset(stack, binary(Op.MUL, sp, i64(24)));
        b.store(frame, start); b.store(at(frame, 8), count); b.store(at(frame, 16), digit);
        b.assign(sp, add(sp, i64(1)));
    }

    private static Expr digit(Expr[] value, Expr digit) {
        Expr word = new Select(binary(Op.LT, digit, i64(8)), value[0], value[1]);
        Expr shift = binary(Op.SUB, i64(56), binary(Op.MUL, binary(Op.AND, digit, i64(7)), i64(8)));
        return binary(Op.AND, binary(Op.USHR, word, shift), i64(255));
    }

    static Expr entry(Expr base, Expr index) { return new Offset(base, binary(Op.MUL, index, i64(40))); }
    static Expr bucket(Expr base, Expr index) { return new Offset(base, binary(Op.MUL, index, i64(8))); }
    static Expr[] words(Expr entry) { return new Expr[]{load(entry), load(at(entry, 8)), load(at(entry, 16)), load(at(entry, 24)), load(at(entry, 32))}; }
    static Expr[] save(Builder b, Expr[] value) {
        Expr[] result = new Expr[5];
        for (int i = 0; i < 5; i++) result[i] = b.let(value[i]);
        return result;
    }
    static void store(Builder b, Expr dest, Expr[] value) { for (int i = 0; i < 5; i++) b.store(at(dest, i * 8L), value[i]); }
    static void swap(Builder b, Expr left, Expr right) { Expr[] value = save(b, words(left)); store(b, left, words(right)); store(b, right, value); }
    static void poll(Builder b, Expr index) { b.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll); }

    static Var less(Builder b, Expr[] left, Expr[] right, boolean tailOnly) {
        Var cmp = b.let(i64(0));
        if (!tailOnly) {
            b.assign(cmp, compareUnsigned(b, left[0], right[0]));
            b.when(eq(cmp, i64(0)), second -> second.assign(cmp, compareUnsigned(second, left[1], right[1])));
        }
        b.when(eq(cmp, i64(0)), tail -> {
            Var len = tail.let(new Select(binary(Op.LT, left[2], right[2]), left[2], right[2]));
            // Load the key field as ADDRESS without introducing an integer-to-pointer IR operation.
            // Values saved in registers are I64; use an Offset from nil for these trusted native pointers.
            compareTail(tail, cmp, new Offset(nil(), add(left[3], i64(16))), new Offset(nil(), add(right[3], i64(16))), binary(Op.SUB, len, i64(16)));
            tail.when(eq(cmp, i64(0)), size -> size.assign(cmp, compareUnsigned(size, left[2], right[2])));
            tail.when(eq(cmp, i64(0)), row -> row.assign(cmp, compareUnsigned(row, left[4], right[4])));
        });
        return b.let(binary(Op.LT, cmp, i64(0)));
    }
}
