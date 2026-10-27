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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinPayloadSource;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.questdb.experimental.codegen.Ir.*;

/** Generated INT join probing, resolved predicates and updates over engine-owned scalar or keyed state. */
final class ResolvedJoinKernel {
    private static final int KEY = 8;
    private static final int LOOKUP = 16;
    private static final int UNIQUE = LOOKUP + FrozenHashJoinBuild.IntKeyed.LOOKUP_BYTES;
    private static final int MATCHED = UNIQUE + 8;
    private static final int POSITION = MATCHED + 8;
    private static final int RESUME_LINK = POSITION + 8;
    private static final int MISSING_KEY = RESUME_LINK + 8;
    private static final int SHARD_MASK = MISSING_KEY + 8;
    private static final int ARGUMENTS = SHARD_MASK + 8;
    private static final int ARGUMENT_BYTES = HashJoinPayloadSource.NATIVE_COLUMN_BYTES;

    private ResolvedJoinKernel() {
    }

    static CompiledJoin compile(ColumnTypes joinedTypes, int probeColumnCount,
            ColumnTypes valueTypes, ObjList<GroupByFunction> functions, int keyColumn, MapValue layout,
            Function postJoinFilter, int groupKeyColumn, Map groupLayout, Backend backend, Path artifacts) throws IOException {
        if (keyColumn < 0 || keyColumn >= probeColumnCount || joinedTypes.getColumnType(keyColumn) != ColumnType.INT) return null;
        var inputs = new ResolvedInputs(joinedTypes);
        var predicate = inputs.predicate(postJoinFilter);
        if (predicate == null) return null;
        QuestMapProbe mapProbe = null;
        Pipeline.Column groupKey = null;
        if (groupLayout != null) {
            if (groupKeyColumn < 0 || groupKeyColumn >= joinedTypes.getColumnCount()) return null;
            int type = joinedTypes.getColumnType(groupKeyColumn);
            mapProbe = QuestMapProbe.bind(groupKeyColumn, type, groupLayout);
            if (mapProbe == null) return null;
            groupKey = inputs.column(groupKeyColumn, type);
        }
        var packed = new FlyweightPackedMapValue(valueTypes);
        long base = layout != null ? layout.getAddress(0) : 0;
        var updates = AggregateUpdates.bind(valueTypes, functions,
                index -> groupLayout != null ? packed.getOffset(index) : layout.getAddress(index) - base, false, inputs);
        if (updates == null) return null;
        var target = new Target(mapProbe, groupKey);
        Builder b = new Builder();
        b.when(ne(load(at(COLUMNS, UNIQUE)), i64(0)), unique -> emitScan(unique, updates, predicate, target, probeColumnCount, true));
        b.when(eq(load(at(COLUMNS, UNIQUE)), i64(0)), multiple -> emitScan(multiple, updates, predicate, target, probeColumnCount, false));
        b.exit(0);
        return new Prepared(backend.compile(new Program(b.block()), artifacts), updates.sourceIndexes(), probeColumnCount, keyColumn, mapProbe, backend.name());
    }

    private record Target(QuestMapProbe probe, Pipeline.Column key) { }

    private static void emitScan(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate,
                                 Target target, int probeColumns, boolean unique) {
        Var rowIds = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var keyColumn = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, KEY)));
        Var table = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, LOOKUP)));
        Var mask = b.let(load(at(COLUMNS, LOOKUP + 8)));
        Var directory = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, LOOKUP + 16)));
        Var shift = b.let(load(at(COLUMNS, LOOKUP + 24)));
        Var heap = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, LOOKUP + 32)));
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < updates.columns().size(); i++)
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, ARGUMENTS + (long) ARGUMENT_BYTES * i))));
        boolean hasPayload = java.util.Arrays.stream(updates.sourceIndexes()).anyMatch(index -> index >= probeColumns);
        Var destination = b.let(STATE);
        Var position = b.let(target.probe != null ? load(at(COLUMNS, POSITION)) : i64(0));
        Var resumeLink = target.probe != null ? b.let(load(at(COLUMNS, RESUME_LINK))) : null;
        Var matched = b.let(i64(0));
        Var pairs = b.let(i64(0));
        b.loop(binary(Op.LT, position, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, position, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(position);
            loop.when(ne(rowIds, nil()), filtered -> filtered.assign(row,
                    load(new Offset(rowIds, binary(Op.MUL, position, i64(8))))));
            Var key = loop.let(new Load(MemoryType.I32, new Offset(keyColumn, binary(Op.MUL, row, i64(4)))));
            // Hash.hashInt64: unsigned INT widening followed by xxh3Avalanche64.
            Var hash = loop.let(binary(Op.AND, key, i64(0xffff_ffffL)));
            loop.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(37))));
            loop.assign(hash, binary(Op.MUL, hash, i64(0x165667919E3779F9L)));
            loop.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(32))));
            Var base = loop.let(table);
            Var slotMask = loop.let(mask);
            loop.when(ne(directory, nil()), partitioned -> {
                Var partition = partitioned.let(binary(Op.USHR, binary(Op.USHR, hash, i64(32)), shift));
                Var entry = partitioned.let(new Offset(directory, binary(Op.MUL, partition, i64(16))));
                partitioned.assign(base, new Load(MemoryType.ADDRESS, entry));
                partitioned.assign(slotMask, load(at(entry, 8)));
            });
            Var slot = loop.let(binary(Op.AND, hash, slotMask));
            Var entry = loop.let(new Offset(base, binary(Op.MUL, slot, i64(8))));
            Var head = loop.let(new Load(MemoryType.I32, at(entry, 4)));
            loop.loop(binary(Op.BOTH, ne(head, i64(0)), ne(new Load(MemoryType.I32, entry), key)), collision -> {
                collision.assign(slot, binary(Op.AND, add(slot, i64(1)), slotMask));
                collision.assign(entry, new Offset(base, binary(Op.MUL, slot, i64(8))));
                collision.assign(head, new Load(MemoryType.I32, at(entry, 4)));
                // An adversarial collision run must remain cancellable too.
                collision.when(eq(binary(Op.AND, slot, i64(1023)), i64(0)), Builder::poll);
            });
            loop.when(ne(head, i64(0)), hit -> {
                if (unique) {
                    Var link = hasPayload || target.probe != null ? hit.let(binary(Op.MUL, binary(Op.AND, head, i64(0xffff_ffffL)), i64(8))) : null;
                    consume(hit, updates, predicate, target, loadRow(hit, updates, probeColumns, addresses, row, heap, link), destination, matched, position, link);
                } else {
                    Var link = hit.let(binary(Op.MUL, binary(Op.AND, head, i64(0xffff_ffffL)), i64(8)));
                    if (resumeLink != null) hit.when(ne(resumeLink, i64(0)), resumed -> {
                        resumed.assign(link, resumeLink);
                        resumed.assign(resumeLink, i64(0));
                    });
                    hit.loop(ne(link, i64(0)), duplicate -> {
                        duplicate.when(eq(binary(Op.AND, pairs, i64(1023)), i64(0)), Builder::poll);
                        duplicate.assign(pairs, add(pairs, i64(1)));
                        consume(duplicate, updates, predicate, target, loadRow(duplicate, updates, probeColumns, addresses, row, heap, link), destination, matched, position, link);
                        duplicate.assign(link, load(new Offset(heap, binary(Op.SUB, link, i64(8)))));
                    });
                }
            });
            loop.assign(position, add(position, i64(1)));
        });
        b.store(at(COLUMNS, MATCHED), matched);
    }

    private static void consume(Builder b, AggregateUpdates updates, Pipeline.Predicate predicate, Target target,
                                Pipeline.Row row, Var destination, Var matched, Var position, Var link) {
        if (predicate instanceof Pipeline.All) {
            update(b, updates, target, row, destination, matched, position, link);
        } else {
            b.when(predicate.emit(row), accepted -> update(accepted, updates, target, row, destination, matched, position, link));
        }
    }

    private static void update(Builder b, AggregateUpdates updates, Target target, Pipeline.Row row,
                               Var scalar, Var matched, Var position, Var link) {
        Var destination = scalar;
        if (target.probe != null) {
            Var key = b.let(target.key.emit(row).data());
            destination = target.probe.find(b, key, position, load(at(COLUMNS, SHARD_MASK)), missing -> {
                missing.store(at(COLUMNS, RESUME_LINK), link);
                missing.store(at(COLUMNS, MISSING_KEY), key);
            });
        }
        b.assign(matched, i64(1));
        updates.emit(b, row, destination, nil());
    }

    private static Pipeline.Row loadRow(Builder b, AggregateUpdates updates, int probeColumns,
                                       ArrayList<Var> addresses, Var row, Var heap, Var link) {
        var values = new ArrayList<Pipeline.Value>();
        for (int i = 0; i < updates.columns().size(); i++) {
            var column = updates.columns().get(i);
            Var base = addresses.get(i);
            Var value;
            if (updates.sourceIndexes()[i] < probeColumns) {
                value = b.let(new Load(column.width(), new Offset(base, binary(Op.MUL, row, i64(column.width().bytes)))));
            } else {
                Expr descriptor = at(COLUMNS, ARGUMENTS + (long) ARGUMENT_BYTES * i);
                Var stride = b.let(load(at(descriptor, 8)));
                Var mode = b.let(load(at(descriptor, 16)));
                Var address = b.let(nil());
                b.when(ne(mode, i64(0)), copied -> {
                    Expr ordinal = binary(Op.USHR, binary(Op.SUB, link, i64(8)), i64(4));
                    copied.assign(address, new Offset(base, binary(Op.MUL, ordinal, stride)));
                });
                b.when(eq(mode, i64(0)), direct -> {
                    // A payload-bearing heap row is [next link, row id]; link is biased by eight.
                    Var id = direct.let(load(new Offset(heap, link)));
                    Expr frame = binary(Op.USHR, id, i64(44));
                    Var page = direct.let(new Load(MemoryType.ADDRESS, new Offset(base, binary(Op.MUL, frame, stride))));
                    direct.when(ne(page, nil()), present -> present.assign(address,
                            new Offset(page, binary(Op.MUL, binary(Op.AND, id, i64(0xfffffffffffL)), i64(column.width().bytes)))));
                });
                value = b.let(i64(column.nullable() ? column.nullValue() : 0));
                b.when(ne(address, nil()), present -> present.assign(value, new Load(column.width(), address)));
            }
            values.add(new Pipeline.Value(value, column.nullable() ? eq(value, i64(column.nullValue())) : bool(false)));
        }
        return new Pipeline.Row(updates.columns(), values);
    }

    private record Prepared(CompiledKernel code, int[] indexes, int probeColumns, int keyColumn, QuestMapProbe probe, String name)
            implements CompiledJoin {
        @Override
        public void close() { code.close(); }

        @Override
        public CharSequence getName() { return name; }

        @Override
        public CompiledJoin.Updater newUpdater(Runnable checkpoint) { return new Slot(this, checkpoint); }
    }

    private static final class Slot implements CompiledJoin.Updater {
        private final Prepared prepared;
        private final KernelMemory memory;
        private final long bytes;
        private final long argumentBytes;
        private long arguments;
        private long nativeRows;
        private long mapMisses;

        Slot(Prepared prepared, Runnable checkpoint) {
            this.prepared = prepared;
            memory = KernelMemory.reusable(checkpoint);
            argumentBytes = ARGUMENTS + (long) ARGUMENT_BYTES * prepared.indexes.length;
            bytes = argumentBytes + (prepared.probe == null ? 0
                    : QuestMapProbe.CONTROL_BYTES + (long) GroupByMapFragment.NUM_SHARDS * QuestMapProbe.VIEW_BYTES);
            arguments = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT);
        }

        @Override
        public void close() { arguments = Unsafe.free(arguments, bytes, MemoryTag.NATIVE_JIT); }

        @Override
        public long getNativeRows() { return nativeRows; }

        @Override
        public long getNativeMapMisses() { return mapMisses; }

        private boolean bind(PageFrameMemoryRecord record, long rowIdsAddress, FrozenHashJoinBuild.IntKeyed build) {
            long key = record.getPageAddress(prepared.keyColumn);
            if (key == 0) return false;
            Unsafe.putLong(arguments, rowIdsAddress);
            Unsafe.putLong(arguments + KEY, key);
            for (int i = 0; i < prepared.indexes.length; i++) {
                long descriptor = arguments + ARGUMENTS + (long) ARGUMENT_BYTES * i;
                int index = prepared.indexes[i];
                if (index < prepared.probeColumns) {
                    long address = record.getPageAddress(index);
                    if (address == 0) return false;
                    Unsafe.putLong(descriptor, address);
                } else if (!build.writeNativePayload(index - prepared.probeColumns, descriptor)) {
                    return false;
                }
            }
            if (!build.writeLookup(arguments + LOOKUP)) return false;
            Unsafe.putLong(arguments + UNIQUE, build.getKeyCount() == build.getRowCount() ? 1 : 0);
            Unsafe.putLong(arguments + MATCHED, 0);
            return true;
        }

        @Override
        public boolean update(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                              FrozenHashJoinBuild.IntKeyed build, MapValue value) {
            if (prepared.probe != null) return false;
            if (rowCount == 0 || build.getRowCount() == 0) return true;
            try {
                if (!bind(record, rowIdsAddress, build)) return false;
                memory.enter(null);
                try {
                    long status = prepared.code.run(arguments, rowCount, value.getAddress(0));
                    memory.throwIfFailed();
                    if (status != 0) throw new IllegalStateException("Join kernel failed: " + status);
                } finally {
                    memory.close();
                }
                if (Unsafe.getLong(arguments + MATCHED) != 0) value.setNew(false);
                nativeRows += rowCount;
                return true;
            } finally {
                Unsafe.setMemory(arguments, bytes, (byte) 0);
            }
        }

        @Override
        public boolean updateKeyed(PageFrameMemoryRecord record, long rowCount, long rowIdsAddress,
                                   FrozenHashJoinBuild.IntKeyed build, Map map, ObjList<Map> shards,
                                   GroupByFunctionsUpdater functions) {
            if (prepared.probe == null) return false;
            if (rowCount == 0 || build.getRowCount() == 0) return true;
            final long state = arguments + argumentBytes;
            try {
                if (!bind(record, rowIdsAddress, build)) return false;
                int count = shards == null ? 1 : shards.size();
                if (shards != null && count != GroupByMapFragment.NUM_SHARDS) return false;
                for (int i = 0; i < count; i++) {
                    Map current = shards == null ? map : shards.getQuick(i);
                    current.writeNativeProbeView(state + QuestMapProbe.CONTROL_BYTES + (long) i * QuestMapProbe.VIEW_BYTES);
                }
                Unsafe.putLong(arguments + SHARD_MASK, count - 1);
                Unsafe.putLong(arguments + POSITION, 0);
                Unsafe.putLong(arguments + RESUME_LINK, 0);
                memory.enter(null);
                try {
                    while (true) {
                        long status = prepared.code.run(arguments, rowCount, state);
                        memory.throwIfFailed();
                        if (status == 0) break;
                        if (status != QuestMapProbe.NEED_KEY) throw new IllegalStateException("Join map kernel failed: " + status);
                        // A miss has not updated its match. Keep its probe and duplicate positions;
                        // the ordinary map remains the only owner of insertion and growth.
                        Unsafe.putLong(arguments + POSITION, Unsafe.getLong(state + 32));
                        long rawKey = Unsafe.getLong(arguments + MISSING_KEY);
                        MapKey key = map.withKey();
                        if (prepared.probe.keyType() == ColumnType.INT) key.putInt((int) rawKey);
                        else key.putLong(rawKey);
                        key.commit();
                        long hash = key.hash();
                        int shardIndex = shards == null ? 0 : (int) (hash >>> GroupByMapFragment.NUM_SHARDS_SHR);
                        if (shardIndex != Unsafe.getLong(state + 24)) throw new IllegalStateException("Join map hash ABI mismatch");
                        Map target = shards == null ? map : shards.getQuick(shardIndex);
                        if (target != map) {
                            MapKey targetKey = target.withKey();
                            targetKey.copyFrom(key);
                            key = targetKey;
                        }
                        MapValue value = key.createValue(hash);
                        if (!value.isNew()) throw new IllegalStateException("Join probe missed an existing group");
                        functions.updateEmpty(value);
                        mapMisses++;
                        // OrderedMap scratch-key writes may also move shard zero's heap.
                        map.writeNativeProbeView(state + QuestMapProbe.CONTROL_BYTES);
                        target.writeNativeProbeView(state + QuestMapProbe.CONTROL_BYTES + (long) shardIndex * QuestMapProbe.VIEW_BYTES);
                    }
                } finally {
                    memory.close();
                }
                nativeRows += rowCount;
                return true;
            } finally {
                Unsafe.setMemory(arguments, bytes, (byte) 0);
            }
        }
    }
}
