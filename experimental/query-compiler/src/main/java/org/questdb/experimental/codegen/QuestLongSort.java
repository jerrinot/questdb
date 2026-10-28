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

/** Full LONG sort: direct map collection plus the same stable radix primitive as bounded Top-K. */
final class QuestLongSort {
    static final int COUNT_OFFSET = 8;
    static final int MODE_OFFSET = 16;
    static final int ORDER_OFFSET = 24;
    static final int VALUE_OFFSET = 32;
    static final int SCRATCH_OFFSET = 40;
    static final int HISTOGRAMS_OFFSET = 48;
    static final int COLLECT = 0;
    static final int SORT = 1;

    private QuestLongSort() { }
    // STATE starts with the pair buffer address, followed by the fields above.
    static Program program(QuestMapProbe probe) {
        Builder b = new Builder();
        Var pairs = b.let(new Load(MemoryType.ADDRESS, STATE));
        Var count = b.let(load(at(STATE, COUNT_OFFSET)));
        b.when(eq(load(at(STATE, MODE_OFFSET)), i64(COLLECT)), collect -> {
            Var offset = collect.let(load(at(STATE, VALUE_OFFSET)));
            QuestMapScan.emit(collect, probe, COLUMNS, (entryBuilder, entry) -> {
                Expr value = probe.variable() ? new Offset(QuestVariableKey.value(entry), offset) : new Offset(entry, offset);
                Var pair = entryBuilder.let(QuestRadixSort.pair(pairs, count));
                entryBuilder.store(pair, load(value));
                entryBuilder.store(MemoryType.ADDRESS, at(pair, 8), entry);
                entryBuilder.assign(count, add(count, i64(1)));
            });
            collect.store(at(STATE, COUNT_OFFSET), count);
            collect.exit(0);
        });
        b.when(binary(Op.LT, i64(1), count), sort -> {
            Var scratch = sort.let(new Load(MemoryType.ADDRESS, at(STATE, SCRATCH_OFFSET)));
            Var histograms = sort.let(new Load(MemoryType.ADDRESS, at(STATE, HISTOGRAMS_OFFSET)));
            for (int order = 0; order < 2; order++) {
                boolean ascending = order == 0;
                sort.when(eq(load(at(STATE, ORDER_OFFSET)), i64(order)), ordered ->
                        QuestRadixSort.emit(ordered, pairs, scratch, histograms, count, ascending));
            }
        });
        b.exit(0);
        return new Program(b.block());
    }
}
