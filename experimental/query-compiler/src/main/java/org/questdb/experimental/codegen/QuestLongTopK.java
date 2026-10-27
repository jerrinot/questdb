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

/** DirectLongLongAsc/DescList's bounded insertion algorithm over borrowed engine storage. */
final class QuestLongTopK {
    private QuestLongTopK() {
    }

    // STATE: destination address, size, capacity, source kind (map=0/list=1), order, value offset.
    // COLUMNS: the existing 48-byte map probe view, or source pairs. ROWS: source list size.
    static Program program(int keyType) { return program(QuestMapProbe.direct(-1, keyType, false)); }

    static Program program(QuestMapProbe probe) { return program(probe, -1); }

    // Zero is unknown during preparation; positive limits select the existing algorithm boundary.
    static int capacityClass(int limit) { return limit == 0 ? -1 : limit <= 64 ? 0 : 1; }

    static Program program(QuestMapProbe probe, int sortOrder) { return program(probe, sortOrder, -1); }

    static Program program(QuestMapProbe probe, int sortOrder, int capacityClass) {
        if (capacityClass < -1 || capacityClass > 1) throw new IllegalArgumentException("Invalid Top-K capacity class");
        if (sortOrder < -1 || sortOrder > 1) throw new IllegalArgumentException("Invalid Top-K order");
        Builder b = new Builder();
        Var destination = b.let(new Load(MemoryType.ADDRESS, STATE));
        Var size = b.let(load(at(STATE, 8)));
        Var capacity = b.let(load(at(STATE, 16)));
        if (capacityClass != -1) {
            // Retain the range information supplied by generic algorithm dispatch.
            b.when(eq(binary(Op.LT, i64(64), capacity), bool(capacityClass == 0)), invalid -> invalid.exit(1));
        }
        if (capacityClass != 0 && Boolean.parseBoolean(System.getProperty("query.codegen.largeTopK", "true"))) {
            if (capacityClass == 1) {
                QuestLargeTopK.emit(b, probe, destination, size, capacity, sortOrder);
                return new Program(b.block());
            }
            b.when(binary(Op.LT, i64(64), capacity), large -> QuestLargeTopK.emit(large, probe, destination, size, capacity, sortOrder));
        }
        for (int order = 0; order < 2; order++) {
            if (sortOrder != -1 && sortOrder != order) continue;
            boolean ascending = order == 0;
            b.when(sortOrder == -1 ? eq(load(at(STATE, 32)), i64(order)) : bool(true), ordered -> {
                ordered.when(eq(load(at(STATE, 24)), i64(0)), map -> {
                    Var valueOffset = map.let(load(at(STATE, 40)));
                    QuestMapScan.emit(map, probe, COLUMNS, (entryBuilder, entry) -> {
                        Expr value = probe.variable() ? new Offset(QuestVariableKey.value(entry), valueOffset)
                                : new Offset(entry, valueOffset);
                        insert(entryBuilder, destination, size, capacity, entry, entryBuilder.let(load(value)), ascending);
                    });
                    map.exit(0);
                });
                Var index = ordered.let(i64(0));
                ordered.loop(binary(Op.LT, index, ROWS), loop -> {
                    loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                    Var pair = loop.let(pair(COLUMNS, index));
                    insert(loop, destination, size, capacity,
                            new Load(MemoryType.ADDRESS, at(pair, 8)), loop.let(load(pair)), ascending);
                    loop.assign(index, add(index, i64(1)));
                });
                ordered.exit(0);
            });
        }
        b.exit(1);
        return new Program(b.block());
    }

    private static Expr pair(Expr base, Expr index) {
        return new Offset(base, binary(Op.MUL, index, i64(16)));
    }

    private static Expr before(Expr a, Expr b, boolean ascending) {
        return ascending ? binary(Op.LT, a, b) : binary(Op.LT, b, a);
    }

    private static void insert(Builder b, Expr base, Var size, Expr capacity, Expr rowId, Expr value, boolean ascending) {
        Var accept = b.let(bool(true));
        b.when(eq(size, capacity), full -> full.assign(accept,
                before(value, load(pair(base, binary(Op.SUB, size, i64(1)))), ascending)));
        b.when(accept, add -> {
            Var low = add.let(i64(0));
            Var high = add.let(size);
            Var position = add.let(size);
            Var found = add.let(bool(false));
            add.loop(binary(Op.BOTH, binary(Op.LT, i64(65), binary(Op.SUB, high, low)), eq(found, bool(false))), search -> {
                Var mid = search.let(binary(Op.USHR, binary(Op.SUB, Ir.add(low, high), i64(1)), i64(1)));
                Var midValue = search.let(load(pair(base, mid)));
                search.when(before(midValue, value, ascending), lower -> lower.assign(low, Ir.add(mid, i64(1))));
                search.when(before(value, midValue, ascending), upper -> upper.assign(high, mid));
                search.when(eq(midValue, value), equal -> {
                    equal.assign(mid, Ir.add(mid, i64(1)));
                    Var ended = equal.let(bool(false));
                    equal.loop(binary(Op.BOTH, binary(Op.LT, mid, high), eq(ended, bool(false))), ties -> {
                        ties.assign(ended, ne(load(pair(base, mid)), value));
                        ties.when(eq(ended, bool(false)), next -> next.assign(mid, Ir.add(mid, i64(1))));
                    });
                    equal.assign(position, mid);
                    equal.assign(found, bool(true));
                });
            });
            add.when(eq(found, bool(false)), linear -> {
                linear.assign(position, low);
                linear.loop(binary(Op.BOTH, binary(Op.LT, position, size), eq(found, bool(false))), scan -> {
                    scan.assign(found, before(value, load(pair(base, position)), ascending));
                    scan.when(eq(found, bool(false)), next -> next.assign(position, Ir.add(position, i64(1))));
                });
            });
            add.when(binary(Op.LT, position, capacity), insert -> {
                // Backward pair copy is the existing overlapping memmove, including its capacity bound.
                Var tail = insert.let(binary(Op.SUB, capacity, i64(1)));
                insert.loop(binary(Op.LT, position, tail), move -> {
                    Var from = move.let(pair(base, binary(Op.SUB, tail, i64(1))));
                    Var to = move.let(pair(base, tail));
                    move.store(to, load(from));
                    move.store(at(to, 8), load(at(from, 8)));
                    move.assign(tail, binary(Op.SUB, tail, i64(1)));
                });
                Var target = insert.let(pair(base, position));
                insert.store(target, value);
                insert.store(MemoryType.ADDRESS, at(target, 8), rowId);
                insert.when(binary(Op.LT, size, capacity), grow -> grow.assign(size, Ir.add(size, i64(1))));
                insert.store(at(STATE, 8), size);
            });
        });
    }
}
