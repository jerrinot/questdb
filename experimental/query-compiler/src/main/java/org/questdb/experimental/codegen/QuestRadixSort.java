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

/** ooo.cpp radix_sort_long_index_asc_in_place: eight histograms, prefix offsets and stable scatters. */
final class QuestRadixSort {
    static final long HISTOGRAM_BYTES = 8 * 256 * 8;
    private QuestRadixSort() { }

    static void emit(Builder b, Expr entries, Expr scratch, Expr histograms, Expr count, boolean ascending) {
        b.call(ascending ? Helpers.ASC : Helpers.DESC, java.util.List.of(entries, scratch, histograms, count));
    }

    private static final class Helpers {
        static final Helper ASC = create(true), DESC = create(false);

        private static Helper create(boolean ascending) {
            var parameters = java.util.List.of(new Var(0, Type.ADDRESS), new Var(1, Type.ADDRESS),
                    new Var(2, Type.ADDRESS), new Var(3, Type.I64));
            Builder b = new Builder(parameters);
            emitBody(b, parameters.get(0), parameters.get(1), parameters.get(2), parameters.get(3), ascending);
            b.exit(0);
            return new Helper(ascending ? "radix_asc" : "radix_desc", parameters, b.block());
        }
    }

    private static void emitBody(Builder b, Expr entries, Expr scratch, Expr histograms, Expr count, boolean ascending) {
        Var zero = b.let(i64(0));
        b.loop(binary(Op.LT, zero, i64(HISTOGRAM_BYTES)), clear -> {
            clear.store(new Offset(histograms, zero), i64(0));
            clear.assign(zero, add(zero, i64(8)));
        });
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, count), histogram -> {
            poll(histogram, index);
            Var value = histogram.let(binary(Op.XOR, load(pair(entries, index)), i64(ascending ? Long.MIN_VALUE : Long.MAX_VALUE)));
            for (int pass = 0; pass < 8; pass++) {
                Expr bucket = bucket(histograms, value, pass);
                histogram.store(bucket, add(load(bucket), i64(1)));
            }
            histogram.assign(index, add(index, i64(1)));
        });
        for (int pass = 0; pass < 8; pass++) {
            Var offset = b.let(i64(0));
            Var digit = b.let(i64(0));
            Expr counts = at(histograms, pass * 2048L);
            b.loop(binary(Op.LT, digit, i64(256)), prefix -> {
                Expr address = new Offset(counts, binary(Op.MUL, digit, i64(8)));
                Var end = prefix.let(add(offset, load(address)));
                prefix.store(address, offset);
                prefix.assign(offset, end);
                prefix.assign(digit, add(digit, i64(1)));
            });
        }
        for (int pass = 0; pass < 8; pass++) {
            int shift = pass;
            Expr from = pass % 2 == 0 ? entries : scratch, to = pass % 2 == 0 ? scratch : entries;
            b.assign(index, i64(0));
            b.loop(binary(Op.LT, index, count), scatter -> {
                poll(scatter, index);
                Var source = scatter.let(pair(from, index));
                Var value = scatter.let(load(source));
                Expr transformed = binary(Op.XOR, value, i64(ascending ? Long.MIN_VALUE : Long.MAX_VALUE));
                Var bucket = scatter.let(bucket(histograms, transformed, shift));
                Var position = scatter.let(load(bucket));
                Var destination = scatter.let(pair(to, position));
                scatter.store(destination, value);
                scatter.store(at(destination, 8), load(at(source, 8)));
                scatter.store(bucket, add(position, i64(1)));
                scatter.assign(index, add(index, i64(1)));
            });
        }
    }

    static Expr pair(Expr base, Expr index) { return new Offset(base, binary(Op.MUL, index, i64(16))); }
    private static Expr bucket(Expr histograms, Expr value, int pass) {
        Expr digit = binary(Op.AND, binary(Op.USHR, value, i64(pass * 8L)), i64(255));
        return new Offset(at(histograms, pass * 2048L), binary(Op.MUL, digit, i64(8)));
    }
    private static void poll(Builder b, Expr index) {
        b.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
    }
}
