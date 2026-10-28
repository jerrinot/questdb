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
    static final int BLOB_OFFSET = GroupByMapFragment.NUM_SHARDS * 16 + 16;
    static final int STATE_BYTES = BLOB_OFFSET + 16;
    static final int NEED_FLUSH = 10;

    private QuestPartitions() { }

    /** Reserve one row without publishing its count until the payload is complete. */
    static Append append(Builder b, Expr partitions, Expr hash, long rowBytes) {
        Var bucket = b.let(new Offset(partitions, binary(Op.MUL,
                binary(Op.USHR, hash, i64(GroupByMapFragment.NUM_SHARDS_SHR)), i64(16))));
        return reserve(b, bucket, rowBytes);
    }

    /** A reset tail reuses the head chain; consumers stop at the current tail, not a stale successor. */
    static Append reserve(Builder b, Expr bucket, long rowBytes) {
        Var tail = b.let(new Load(MemoryType.ADDRESS, at(bucket, 8)));
        Var count = b.let(i64(0));
        b.when(ne(tail, nil()), existing -> existing.assign(count, load(at(tail, 8))));
        b.when(binary(Op.EITHER, eq(tail, nil()), binary(Op.GE, count, i64((PAGE_BYTES - 16) / rowBytes))), full -> {
            Var page = full.let(nil());
            full.when(eq(tail, nil()), first -> first.assign(page, new Load(MemoryType.ADDRESS, bucket)));
            full.when(ne(tail, nil()), following -> following.assign(page, new Load(MemoryType.ADDRESS, tail)));
            full.when(eq(page, nil()), allocate -> {
                allocate.assign(page, new Allocate(i64(PAGE_BYTES)));
                allocate.when(eq(page, nil()), failed -> failed.exit(2));
                allocate.store(MemoryType.ADDRESS, page, nil());
                allocate.when(ne(tail, nil()), link -> link.store(MemoryType.ADDRESS, tail, page));
                allocate.when(eq(tail, nil()), first -> first.store(MemoryType.ADDRESS, bucket, page));
            });
            full.store(at(page, 8), i64(0));
            full.store(MemoryType.ADDRESS, at(bucket, 8), page);
            full.assign(tail, page);
            full.assign(count, i64(0));
        });
        Var row = b.let(new Offset(at(tail, 16), binary(Op.MUL, count, i64(rowBytes))));
        return new Append(row, tail, count);
    }

    record Append(Var row, Var tail, Var count) {
        void commit(Builder b) { b.store(at(tail, 8), add(count, i64(1))); }
    }

    /** Retain only transient bytes; stable payloads already belong to the pinned query snapshot. */
    static Var retain(Builder b, Expr partitions, Pipeline.Utf8Value value) {
        Var pointer = b.let(value.address());
        b.when(binary(Op.BOTH, eq(value.stable(), bool(false)), ne(value.size(), i64(0))), unstable -> {
            Expr blob = at(partitions, BLOB_OFFSET);
            Var next = unstable.let(new Load(MemoryType.ADDRESS, blob));
            Var available = unstable.let(load(at(blob, 8)));
            Var length = unstable.let(value.size());
            unstable.when(binary(Op.LT, available, length), full -> {
                Var capacity = full.let(new Select(binary(Op.LT, length, i64(PAGE_BYTES)), i64(PAGE_BYTES), length));
                full.assign(next, new Allocate(capacity));
                full.when(eq(next, nil()), failed -> failed.exit(2));
                full.assign(available, capacity);
            });
            QuestMapMutation.copy(unstable, pointer, next, length);
            unstable.assign(pointer, next);
            unstable.store(MemoryType.ADDRESS, blob, new Offset(next, length));
            unstable.store(at(blob, 8), binary(Op.SUB, available, length));
        });
        return pointer;
    }

    static Program flush(QuestMapProbe probe, long valueBytes) {
        Builder b = new Builder();
        int keyBytes = probe.varchar() ? 24 : probe.keyBytes();
        long rowBytes = keyBytes + valueBytes;
        Var sketch = probe.varchar() ? null : b.let(new Load(MemoryType.ADDRESS, at(STATE, QuestPartitionSketch.OFFSET)));
        QuestMapScan.emit(b, probe, COLUMNS, (entry, source) -> {
                Var hash = probe.varchar() ? QuestVarchar.hash(entry, QuestVarchar.pointer(source), QuestVarchar.size(source))
                        : probe.orderedNative() ? probe.hash(entry, source)
                        : entry.let(probe.hash(entry, new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, source)));
                if (sketch != null) QuestPartitionSketch.add(entry, sketch, hash);
                Append append = append(entry, STATE, hash, rowBytes);
                Var row = append.row();
                QuestMapMutation.copy(entry, source, row, probe.keyBytes());
                if (probe.varchar()) {
                    entry.store(at(row, 16), hash);
                    // Map-owned arenas die at clear(); table payloads remain pinned by the query.
                    Var pointer = retain(entry, STATE, new Pipeline.Utf8Value(QuestVarchar.pointer(source), QuestVarchar.size(source),
                            bool(false), bool(false), binary(Op.GE, load(at(source, 8)), i64(0))));
                    entry.store(MemoryType.ADDRESS, at(row, 8), pointer);
                }
                QuestMapMutation.copy(entry, at(source, probe.keyBytes()), at(row, keyBytes), valueBytes);
                append.commit(entry);
        });
        b.exit(0);
        return new Program(b.block());
    }

    /** Copy selected complete states before the merge workspace is cleared. */
    static Program selected(QuestMapProbe probe, long valueBytes) {
        Builder b = new Builder();
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Load(MemoryType.ADDRESS, new Offset(COLUMNS, add(binary(Op.MUL, index, i64(16)), i64(8)))));
            Var key = probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            probe.find(loop, key, index, i64(0), null, (init, value) ->
                    QuestMapMutation.copy(init, at(row, probe.keyBytes()), value, valueBytes));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    static Program replay(QuestMapProbe probe, AggregateUpdates updates, long valueBytes) {
        Builder b = new Builder();
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(probe.keyBytes() + valueBytes))));
            Var key = probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            Var destination = probe.findExisting(loop, key);
            loop.when(ne(destination, nil()), hit -> updates.merge(hit, at(row, probe.keyBytes()), destination));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    static Program merge(QuestMapProbe probe, AggregateUpdates updates, long valueBytes) {
        return merge(probe, updates, valueBytes, null);
    }

    static Program merge(QuestMapProbe probe, AggregateUpdates updates, long valueBytes, AggregateUpdates initialization) {
        Builder b = new Builder();
        int keyBytes = probe.varchar() ? 24 : probe.keyBytes();
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(keyBytes + valueBytes))));
            Var key = probe.varchar() || probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            Var inserted = loop.let(bool(false));
            Var destination = probe.find(loop, key, index, i64(0), null, (init, value) -> {
                if (initialization == null) QuestMapMutation.copy(init, at(row, keyBytes), value, valueBytes);
                else initialization.initialize(init, value);
                init.assign(inserted, bool(true));
            });
            if (initialization != null) updates.merge(loop, at(row, keyBytes), destination);
            else loop.when(eq(inserted, bool(false)), existing -> updates.merge(existing, at(row, keyBytes), destination));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }
}
