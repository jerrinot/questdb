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

import io.questdb.griffin.engine.table.GroupByMapFragment;

import static org.questdb.experimental.codegen.Ir.*;

/** Dense partial-state pages. Allocation and final maps retain their existing query owners. */
final class QuestPartitions {
    static final int PAGE_BYTES = 64 * 1024;
    static final int STATE_BYTES = GroupByMapFragment.NUM_SHARDS * 16 + 16;
    static final int NEED_FLUSH = 10;

    private QuestPartitions() { }

    static Program flush(QuestMapProbe probe, long valueBytes) {
        Builder b = new Builder();
        int keyBytes = probe.varchar() ? 24 : probe.keyBytes();
        long rowBytes = keyBytes + valueBytes;
        QuestMapScan.emit(b, probe, COLUMNS, (entry, source) -> {
                Var hash = probe.varchar() ? QuestVarchar.hash(entry, QuestVarchar.pointer(source), QuestVarchar.size(source))
                        : probe.orderedNative() ? probe.hash(entry, source)
                        : entry.let(probe.hash(entry, new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, source)));
                Var bucket = entry.let(new Offset(STATE, binary(Op.MUL,
                        binary(Op.USHR, hash, i64(GroupByMapFragment.NUM_SHARDS_SHR)), i64(16))));
                Var tail = entry.let(new Load(MemoryType.ADDRESS, at(bucket, 8)));
                Var count = entry.let(i64(0));
                entry.when(ne(tail, nil()), existing -> existing.assign(count, load(at(tail, 8))));
                entry.when(binary(Op.EITHER, eq(tail, nil()), binary(Op.GE, count, i64((PAGE_BYTES - 16) / rowBytes))), full -> {
                    Var page = full.let(new Allocate(i64(PAGE_BYTES)));
                    full.when(eq(page, nil()), failed -> failed.exit(2));
                    full.store(MemoryType.ADDRESS, page, nil());
                    full.store(at(page, 8), i64(0));
                    full.when(ne(tail, nil()), link -> link.store(MemoryType.ADDRESS, tail, page));
                    full.when(eq(tail, nil()), first -> first.store(MemoryType.ADDRESS, bucket, page));
                    full.store(MemoryType.ADDRESS, at(bucket, 8), page);
                    full.assign(tail, page);
                    full.assign(count, i64(0));
                });
                Var row = entry.let(new Offset(at(tail, 16), binary(Op.MUL, count, i64(rowBytes))));
                QuestMapMutation.copy(entry, source, row, probe.keyBytes());
                if (probe.varchar()) {
                    entry.store(at(row, 16), hash);
                    // Map-owned arenas die at clear(). Copy only those keys into the slot's
                    // existing allocator; native table pointers remain borrowed and pinned.
                    entry.when(binary(Op.BOTH, binary(Op.LT, load(at(source, 8)), i64(0)), ne(QuestVarchar.size(source), i64(0))), unstable -> {
                        Expr blob = at(STATE, GroupByMapFragment.NUM_SHARDS * 16L);
                        Var next = unstable.let(new Load(MemoryType.ADDRESS, blob));
                        Var available = unstable.let(load(at(blob, 8)));
                        Var length = unstable.let(QuestVarchar.size(source));
                        unstable.when(binary(Op.LT, available, length), full -> {
                            Var capacity = full.let(new Select(binary(Op.LT, length, i64(PAGE_BYTES)), i64(PAGE_BYTES), length));
                            full.assign(next, new Allocate(capacity));
                            full.when(eq(next, nil()), failed -> failed.exit(2));
                            full.assign(available, capacity);
                        });
                        QuestMapMutation.copy(unstable, QuestVarchar.pointer(source), next, length);
                        unstable.store(MemoryType.ADDRESS, at(row, 8), next);
                        unstable.store(MemoryType.ADDRESS, blob, new Offset(next, length));
                        unstable.store(at(blob, 8), binary(Op.SUB, available, length));
                    });
                }
                QuestMapMutation.copy(entry, at(source, probe.keyBytes()), at(row, keyBytes), valueBytes);
                entry.store(at(tail, 8), add(count, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    static Program merge(QuestMapProbe probe, AggregateUpdates updates, long valueBytes) {
        Builder b = new Builder();
        int keyBytes = probe.varchar() ? 24 : probe.keyBytes();
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(keyBytes + valueBytes))));
            Var key = probe.varchar() || probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            Var inserted = loop.let(bool(false));
            Var destination = probe.find(loop, key, index, i64(0), null, (init, value) -> {
                QuestMapMutation.copy(init, at(row, keyBytes), value, valueBytes);
                init.assign(inserted, bool(true));
            });
            loop.when(eq(inserted, bool(false)), existing -> updates.merge(existing, at(row, keyBytes), destination));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }
}
