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

import io.questdb.cairo.map.NativeMapView;
import io.questdb.griffin.engine.table.GroupByMapFragment;

import java.util.function.BiConsumer;

import static org.questdb.experimental.codegen.Ir.*;

/** Dense partial-state pages. Allocation and final maps retain their existing query owners. */
final class QuestPartitions {
    // Each bucket starts with its head pointer; a zero tail marks an empty live chain.
    static final int BUCKET_TAIL_OFFSET = Long.BYTES;
    static final int BUCKET_BYTES = 2 * Long.BYTES;
    // Each page starts with its successor pointer, followed by the published row count.
    static final int PAGE_COUNT_OFFSET = Long.BYTES;
    static final int PAGE_HEADER_BYTES = 2 * Long.BYTES;
    static final int PAGE_BYTES = 64 * 1024;
    // Variable pages add used/capacity bytes; each record is hash + aligned OrderedMap key/state.
    static final int VARIABLE_HEADER_BYTES = 4 * Long.BYTES;
    private static final int PAGE_USED_OFFSET = 2 * Long.BYTES;
    private static final int PAGE_CAPACITY_OFFSET = 3 * Long.BYTES;
    static final int SKETCH_OFFSET = GroupByMapFragment.NUM_SHARDS * BUCKET_BYTES;
    // A reserved word follows the sketch pointer.
    private static final int BLOB_OFFSET = SKETCH_OFFSET + 2 * Long.BYTES;
    // The blob descriptor starts with the next writable byte pointer.
    private static final int BLOB_AVAILABLE_OFFSET = Long.BYTES;
    static final int STATE_BYTES = BLOB_OFFSET + 2 * Long.BYTES;
    static final int NEED_FLUSH = 10;
    static final int EXPORTED_ROWS_OFFSET = NativeMapView.MUTATION_BYTES;
    private static final int EXPORT_KEY_OFFSET = EXPORTED_ROWS_OFFSET + 16;

    private QuestPartitions() { }

    static int distinctExportScratchBytes(QuestMapProbe pairs) { return EXPORT_KEY_OFFSET + pairs.keyBytes(); }

    /** Export an ordinary DISTINCT state into the existing key-only pair partitions.
     * COLUMNS is the borrowed source view followed by count/key scratch; STATE is the partition descriptor.
     * Group keys are the pair-key prefix. Source storage remains immutable; allocation uses the owning slot.
     * Failure aborts the query after possible partial append, never requests ordinary-frame replay.
     */
    static Program distinctValues(QuestMapProbe source, long countOffset, QuestMapProbe pairs) {
        if (source.variable() || source.varchar() || (source.ordered() && !source.orderedNative())
                || pairs.variable() || !pairs.orderedNative() || pairs.keyBytes() != source.keyBytes() + 8
                || countOffset < 0) {
            throw new IllegalArgumentException("Fixed group-key prefix and one LONG DISTINCT argument required");
        }
        Builder b = new Builder();
        Var emitted = b.let(i64(0));
        QuestMapScan.emit(b, source, COLUMNS, (group, entry) -> {
            Expr state = at(source.valueAddress(entry), countOffset);
            BiConsumer<Builder, Expr> append = (next, value) -> {
                Var key = next.let(at(COLUMNS, EXPORT_KEY_OFFSET));
                QuestMapMutation.copy(next, entry, key, source.keyBytes());
                next.store(at(key, source.keyBytes()), value);
                Var hash = pairs.hash(next, key);
                Append output = append(next, STATE, hash, pairs.keyBytes());
                QuestMapMutation.copy(next, key, output.row(), pairs.keyBytes());
                output.commit(next);
                next.assign(emitted, add(emitted, i64(1)));
            };
            // COUNT(argument) must still produce a zero-count group for entirely NULL input.
            group.when(binary(Op.EITHER, eq(load(state), i64(0)), eq(load(state), i64(Long.MIN_VALUE))),
                    empty -> append.accept(empty, i64(Long.MIN_VALUE)));
            NestedDistinct.LONG.forEach(group, state, append);
        });
        b.store(at(COLUMNS, EXPORTED_ROWS_OFFSET), emitted);
        b.exit(0);
        return new Program(b.block());
    }

    /** Reserve one row without publishing its count until the payload is complete. */
    static Append append(Builder b, Expr partitions, Expr hash, long rowBytes) {
        Var bucket = b.let(new Offset(partitions, binary(Op.MUL,
                binary(Op.USHR, hash, i64(GroupByMapFragment.NUM_SHARDS_SHR)), i64(BUCKET_BYTES))));
        return reserve(b, bucket, rowBytes);
    }

    static Append appendVariable(Builder b, Expr partitions, Expr hash, Var rowBytes) {
        Var bucket = b.let(new Offset(partitions, binary(Op.MUL,
                binary(Op.USHR, hash, i64(GroupByMapFragment.NUM_SHARDS_SHR)), i64(BUCKET_BYTES))));
        return reserveVariable(b, bucket, rowBytes);
    }

    /** A reset tail reuses the head chain; consumers stop at the current tail, not a stale successor. */
    static Append reserve(Builder b, Expr bucket, long rowBytes) {
        Var tail = b.let(new Load(MemoryType.ADDRESS, at(bucket, BUCKET_TAIL_OFFSET)));
        Var count = b.let(i64(0));
        b.when(ne(tail, nil()), existing -> existing.assign(count, load(at(tail, PAGE_COUNT_OFFSET))));
        b.when(binary(Op.EITHER, eq(tail, nil()), binary(Op.GE, count, i64((PAGE_BYTES - PAGE_HEADER_BYTES) / rowBytes))), full -> {
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
            full.store(at(page, PAGE_COUNT_OFFSET), i64(0));
            full.store(MemoryType.ADDRESS, at(bucket, BUCKET_TAIL_OFFSET), page);
            full.assign(tail, page);
            full.assign(count, i64(0));
        });
        Var row = b.let(new Offset(at(tail, PAGE_HEADER_BYTES), binary(Op.MUL, count, i64(rowBytes))));
        return new Append(row, tail, count);
    }

    record Append(Var row, Var tail, Var count) {
        void commit(Builder b) { b.store(at(tail, PAGE_COUNT_OFFSET), add(count, i64(1))); }
    }

    /** Retain only transient bytes; stable payloads already belong to the pinned query snapshot. */
    static Var retain(Builder b, Expr partitions, Pipeline.Utf8Value value) {
        Var pointer = b.let(value.address());
        b.when(binary(Op.BOTH, eq(value.stable(), bool(false)), ne(value.size(), i64(0))), unstable -> {
            Expr blob = at(partitions, BLOB_OFFSET);
            Var next = unstable.let(new Load(MemoryType.ADDRESS, blob));
            Var available = unstable.let(load(at(blob, BLOB_AVAILABLE_OFFSET)));
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
            unstable.store(at(blob, BLOB_AVAILABLE_OFFSET), binary(Op.SUB, available, length));
        });
        return pointer;
    }

    static Program flush(QuestMapProbe probe, long valueBytes) {
        if (probe.variable()) return flushVariable(probe, valueBytes);
        Builder b = new Builder();
        int keyBytes = probe.varchar() ? 24 : probe.keyBytes();
        long rowBytes = keyBytes + valueBytes;
        Var sketch = probe.varchar() ? null : b.let(new Load(MemoryType.ADDRESS, at(STATE, SKETCH_OFFSET)));
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
                    QuestMapMutation.copy(init, probe.valueAddress(row), value, valueBytes));
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

    static Program merge(QuestMapProbe probe, AggregateUpdates updates, long valueBytes, int batchRows) {
        return merge(probe, updates, valueBytes, batchRows, null);
    }

    static Program merge(QuestMapProbe probe, AggregateUpdates updates, long valueBytes, int batchRows, AggregateUpdates initialization) {
        if (probe.variable()) return mergeStaged(probe, updates, valueBytes, batchRows, initialization, false);
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

    static Program mergeInputs(QuestMapProbe probe, AggregateUpdates updates, long valueBytes, int batchRows) {
        return mergeStaged(probe, updates, valueBytes, batchRows, null, true);
    }

    static Append reserveVariable(Builder b, Expr bucket, Var rowBytes) {
        Var tail = b.let(new Load(MemoryType.ADDRESS, at(bucket, BUCKET_TAIL_OFFSET)));
        Var used = b.let(i64(0)), count = b.let(i64(0)), available = b.let(i64(0));
        b.when(ne(tail, nil()), existing -> {
            existing.assign(count, load(at(tail, PAGE_COUNT_OFFSET)));
            existing.assign(used, load(at(tail, PAGE_USED_OFFSET)));
            existing.assign(available, load(at(tail, PAGE_CAPACITY_OFFSET)));
        });
        b.when(binary(Op.EITHER, eq(tail, nil()), binary(Op.LT, available, add(used, rowBytes))), full -> {
            Var page = full.let(nil());
            Var fits = full.let(bool(false));
            full.when(eq(tail, nil()), first -> first.assign(page, new Load(MemoryType.ADDRESS, bucket)));
            full.when(ne(tail, nil()), following -> following.assign(page, new Load(MemoryType.ADDRESS, tail)));
            full.when(ne(page, nil()), existing -> existing.assign(fits,
                    binary(Op.GE, load(at(page, PAGE_CAPACITY_OFFSET)), rowBytes)));
            full.when(eq(fits, bool(false)), allocate -> {
                Var required = allocate.let(add(rowBytes, i64(VARIABLE_HEADER_BYTES)));
                Var capacity = allocate.let(new Select(binary(Op.LT, required, i64(PAGE_BYTES)), i64(PAGE_BYTES), required));
                Var next = allocate.let(new Allocate(capacity));
                allocate.when(eq(next, nil()), failed -> failed.exit(2));
                // Keep the unused suffix reachable when an oversized record needs a larger page.
                allocate.store(MemoryType.ADDRESS, next, page);
                allocate.store(at(next, PAGE_CAPACITY_OFFSET), binary(Op.SUB, capacity, i64(VARIABLE_HEADER_BYTES)));
                allocate.when(eq(tail, nil()), first -> first.store(MemoryType.ADDRESS, bucket, next));
                allocate.when(ne(tail, nil()), following -> following.store(MemoryType.ADDRESS, tail, next));
                allocate.assign(page, next);
            });
            full.store(at(page, PAGE_COUNT_OFFSET), i64(0));
            full.store(at(page, PAGE_USED_OFFSET), i64(0));
            full.store(MemoryType.ADDRESS, at(bucket, BUCKET_TAIL_OFFSET), page);
            full.assign(tail, page);
            full.assign(used, i64(0));
            full.assign(count, i64(0));
        });
        Var row = b.let(new Offset(at(tail, VARIABLE_HEADER_BYTES), used));
        b.store(at(tail, PAGE_USED_OFFSET), add(used, rowBytes));
        return new Append(row, tail, count);
    }

    private static Program flushVariable(QuestMapProbe probe, long valueBytes) {
        Builder b = new Builder();
        Var sketch = b.let(new Load(MemoryType.ADDRESS, at(STATE, SKETCH_OFFSET)));
        QuestMapScan.emit(b, probe, COLUMNS, (entry, source) -> {
            Var hash = probe.hash(entry, source);
            QuestPartitionSketch.add(entry, sketch, hash, QuestVariableKey.bytes(source));
            Var bytes = entry.let(QuestVariableKey.stride(source, i64(valueBytes)));
            Var rowBytes = entry.let(add(bytes, i64(8)));
            Append append = appendVariable(entry, STATE, hash, rowBytes);
            entry.store(append.row(), hash);
            QuestMapMutation.copy(entry, source, at(append.row(), 8), bytes);
            append.commit(entry);
        });
        b.exit(0);
        return new Program(b.block());
    }

    /** Stage source keys and hashes before dependent destination probes. */
    static Program mergeStaged(QuestMapProbe probe, AggregateUpdates updates, long valueBytes, int batchRows,
                               AggregateUpdates initialization, boolean inputs) {
        Builder b = new Builder();
        int window = Math.max(1, batchRows);
        Var index = b.let(load(STATE));
        Var batches = b.let(i64(0));
        Var sourceOffset = probe.variable() ? b.let(load(at(STATE, 96))) : null;
        b.loop(binary(Op.LT, index, ROWS), batch -> {
            batch.when(eq(binary(Op.AND, batches, i64(Math.max(1, 1024 / window) - 1)), i64(0)), Builder::poll);
            batch.assign(batches, add(batches, i64(1)));
            Var end = batch.let(new Select(binary(Op.LT, add(index, i64(window)), ROWS), add(index, i64(window)), ROWS));
            Var position = batch.let(index);
            Var scratch = batch.let(at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            batch.loop(binary(Op.LT, position, end), stage -> {
                Var row = stage.let(new Offset(COLUMNS, probe.variable() ? sourceOffset
                        : binary(Op.MUL, position, i64(probe.keyBytes() + valueBytes))));
                Var key = probe.variable() ? stage.let(at(row, 8)) : row;
                Var hash = probe.variable() ? stage.let(load(row)) : probe.hash(stage, key);
                stage.prefetch(key, false);
                probe.prefetchOffset(stage, key, hash);
                if (probe.variable()) stage.store(scratch, sourceOffset);
                stage.store(MemoryType.ADDRESS, at(scratch, 8), key);
                stage.store(at(scratch, 24), hash);
                stage.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                if (probe.variable()) {
                    Expr payloadBytes = inputs ? add(QuestVariableKey.bytes(key), i64(QuestPartitionInputs.valueBytes(updates)))
                            : QuestVariableKey.stride(key, i64(valueBytes));
                    stage.assign(sourceOffset, add(sourceOffset, add(i64(8), payloadBytes)));
                }
                stage.assign(position, add(position, i64(1)));
            });
            batch.assign(position, index);
            batch.assign(scratch, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            batch.loop(binary(Op.LT, position, end), prepare -> {
                Var key = prepare.let(new Load(MemoryType.ADDRESS, at(scratch, 8)));
                var candidate = probe.prepare(prepare, key, valueBytes, (hint, destination) -> {}, prepare.let(load(at(scratch, 24))));
                prepare.store(MemoryType.ADDRESS, at(scratch, 16), candidate.entry());
                prepare.store(at(scratch, 32), candidate.slot());
                prepare.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                prepare.assign(position, add(position, i64(1)));
            });
            batch.assign(scratch, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            batch.loop(binary(Op.LT, index, end), consume -> {
                Var key = consume.let(new Load(MemoryType.ADDRESS, at(scratch, 8)));
                Var source = consume.let(probe.valueAddress(key));
                // Variable records resume by byte offset; fixed records resume by index.
                if (probe.variable()) consume.store(at(STATE, 96), load(scratch));
                Var inserted = consume.let(bool(false));
                Var destination = probe.finishOrdered(consume, key, consume.let(new Load(MemoryType.ADDRESS, at(scratch, 16))),
                        consume.let(load(at(scratch, 24))), consume.let(load(at(scratch, 32))), index, (init, value) -> {
                            if (inputs) updates.initialize(init, value);
                            else if (initialization == null) QuestMapMutation.copy(init, source, value, valueBytes);
                            else initialization.initialize(init, value);
                            init.assign(inserted, bool(true));
                        });
                if (inputs) updates.emit(consume, QuestPartitionInputs.loadFrame(consume, updates, source,
                        QuestPartitionInputs.numericBytes(updates)), destination, nil());
                else if (initialization != null) updates.merge(consume, source, destination);
                else consume.when(eq(inserted, bool(false)), existing -> updates.merge(existing, source, destination));
                consume.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                consume.assign(index, add(index, i64(1)));
            });
        });
        b.exit(0);
        return new Program(b.block());
    }
}
