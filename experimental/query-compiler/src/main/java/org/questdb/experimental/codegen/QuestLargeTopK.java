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

/** EncodedTopKBuffer's bounded sort/truncate strategy over the existing LONG pair list. */
final class QuestLargeTopK {
    private QuestLargeTopK() { }

    static void emit(Builder b, QuestMapProbe probe, Var destination, Var size, Var capacity, int sortOrder) {
        // STATE+48/+56 persist slot-owned query allocations between maps, and clear with the allocator.
        Var bufferCapacity = b.let(new Select(binary(Op.LT, capacity, i64(2048)), i64(4096), binary(Op.MUL, capacity, i64(2))));
        Var memory = b.let(new Load(MemoryType.ADDRESS, at(STATE, 48)));
        b.when(binary(Op.LT, load(at(STATE, 56)), bufferCapacity), grow -> {
            grow.assign(memory, new Allocate(add(binary(Op.MUL, bufferCapacity, i64(32)), i64(QuestRadixSort.HISTOGRAM_BYTES))));
            grow.when(eq(memory, nil()), failed -> failed.exit(2));
            grow.store(MemoryType.ADDRESS, at(STATE, 48), memory);
            grow.store(at(STATE, 56), bufferCapacity);
        });
        Var scratch = b.let(new Offset(memory, binary(Op.MUL, bufferCapacity, i64(16))));
        Var histogram = b.let(new Offset(scratch, binary(Op.MUL, bufferCapacity, i64(16))));
        QuestMapMutation.copy(b, destination, memory, binary(Op.MUL, size, i64(16)));
        for (int order = 0; order < 2; order++) {
            if (sortOrder != -1 && sortOrder != order) continue;
            boolean ascending = order == 0;
            b.when(sortOrder == -1 ? eq(load(at(STATE, 32)), i64(order)) : bool(true), sorted -> {
                Var count = sorted.let(size);
                Var hasThreshold = sorted.let(eq(size, capacity));
                Var threshold = sorted.let(i64(0));
                sorted.when(hasThreshold, full -> full.assign(threshold, load(QuestRadixSort.pair(memory, binary(Op.SUB, capacity, i64(1))))));
                java.util.function.BiConsumer<Builder, Expr[]> append = (next, pair) -> {
                    Expr rowId = pair[0], value = pair[1];
                    Var reject = next.let(bool(false));
                    next.when(hasThreshold, bounded -> bounded.assign(reject,
                            ascending ? binary(Op.GE, value, threshold) : binary(Op.GE, threshold, value)));
                    next.when(eq(reject, bool(false)), accepted -> {
                        Var target = accepted.let(QuestRadixSort.pair(memory, count));
                        accepted.store(target, value);
                        accepted.store(MemoryType.ADDRESS, at(target, 8), rowId);
                        accepted.assign(count, add(count, i64(1)));
                        accepted.when(eq(count, bufferCapacity), compact -> {
                            QuestRadixSort.emit(compact, memory, scratch, histogram, count, ascending);
                            compact.assign(count, capacity);
                            compact.assign(threshold, load(QuestRadixSort.pair(memory, binary(Op.SUB, capacity, i64(1)))));
                            compact.assign(hasThreshold, bool(true));
                        });
                    });
                };
                sorted.when(eq(load(at(STATE, 24)), i64(2)), maps -> {
                    Var mapIndex = maps.let(i64(0));
                    Var valueOffset = maps.let(load(at(STATE, 40)));
                    maps.loop(binary(Op.LT, mapIndex, ROWS), map -> {
                        Expr view = new Offset(COLUMNS, binary(Op.MUL, mapIndex, i64(64)));
                        QuestMapScan.emit(map, probe, view, (entryBuilder, entry) -> {
                            Expr value = probe.variable() ? new Offset(QuestVariableKey.value(entry), valueOffset) : new Offset(entry, valueOffset);
                            append.accept(entryBuilder, new Expr[]{entry, entryBuilder.let(load(value))});
                        });
                        map.assign(mapIndex, add(mapIndex, i64(1)));
                    });
                });
                sorted.when(eq(load(at(STATE, 24)), i64(0)), map -> {
                    Var valueOffset = map.let(load(at(STATE, 40)));
                    QuestMapScan.emit(map, probe, COLUMNS, (entryBuilder, entry) -> {
                        Expr value = probe.variable() ? new Offset(QuestVariableKey.value(entry), valueOffset) : new Offset(entry, valueOffset);
                        append.accept(entryBuilder, new Expr[]{entry, entryBuilder.let(load(value))});
                    });
                });
                sorted.when(eq(load(at(STATE, 24)), i64(1)), list -> {
                    Var index = list.let(i64(0));
                    list.loop(binary(Op.LT, index, ROWS), loop -> {
                        loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                        Var pair = loop.let(QuestRadixSort.pair(COLUMNS, index));
                        append.accept(loop, new Expr[]{new Load(MemoryType.ADDRESS, at(pair, 8)), loop.let(load(pair))});
                        loop.assign(index, add(index, i64(1)));
                    });
                });
                sorted.when(binary(Op.LT, i64(1), count), finish -> QuestRadixSort.emit(finish, memory, scratch, histogram, count, ascending));
                sorted.assign(size, new Select(binary(Op.LT, count, capacity), count, capacity));
                QuestMapMutation.copy(sorted, memory, destination, binary(Op.MUL, size, i64(16)));
                sorted.store(at(STATE, 8), size);
                sorted.exit(0);
            });
        }
        b.exit(1);
    }
}
