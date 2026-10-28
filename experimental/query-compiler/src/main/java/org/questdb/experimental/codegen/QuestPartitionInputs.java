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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Compact input records with query-safe string payloads when local aggregation does not reduce work. */
final class QuestPartitionInputs {
    private QuestPartitionInputs() { }

    private static List<ResolvedVarchar.Expression> stringArguments(AggregateUpdates updates) {
        var result = new ArrayList<ResolvedVarchar.Expression>();
        for (var update : updates.updates()) if (update instanceof VarcharExtremum extrema
                && !result.contains(extrema.argument())) result.add(extrema.argument());
        return result;
    }

    static long rowBytes(QuestMapProbe probe, AggregateUpdates updates) {
        long bytes = probe.keyBytes();
        for (var column : updates.columns()) bytes += column.width().bytes;
        return ((bytes + 7) & ~7L) + stringArguments(updates).size() * 16L;
    }

    static Program rows(QuestMapProbe probe, AggregateUpdates updates) {
        Builder b = new Builder();
        Var partitions = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var sketch = b.let(new Load(MemoryType.ADDRESS, at(partitions, QuestPartitionSketch.OFFSET)));
        Var rowIds = b.let(new Load(MemoryType.ADDRESS, at(STATE, 8)));
        var addresses = new ArrayList<Var>();
        for (int i = 0; i < updates.columns().size(); i++) {
            addresses.add(b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, (i + 1L) * 8))));
        }
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), entry -> {
            entry.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var inputRow = entry.let(index);
            entry.when(ne(rowIds, nil()), filtered -> filtered.assign(inputRow,
                    load(new Offset(rowIds, binary(Op.MUL, index, i64(8))))));
            Var key = probe.loadFrameKey(entry, inputRow, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe) + 24));
            Var hash = probe.hash(entry, key);
            QuestPartitionSketch.add(entry, sketch, hash);
            var append = QuestPartitions.append(entry, partitions, hash, rowBytes(probe, updates));
            if (probe.orderedNative()) QuestMapMutation.copy(entry, key, append.row(), probe.keyBytes());
            else entry.store(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, append.row(), key);
            var values = updates.loadFrameRow(entry, inputRow, addresses);
            long offset = probe.keyBytes();
            for (int i = 0; i < updates.columns().size(); i++) {
                var width = updates.columns().get(i).width();
                entry.store(width, at(append.row(), offset), values.values().get(i).data());
                offset += width.bytes;
            }
            offset = (offset + 7) & ~7L;
            var frame = updates.inputs().frame(values, inputRow, at(COLUMNS, 8));
            for (var argument : stringArguments(updates)) {
                var value = argument.emit(entry, frame);
                Var pointer = QuestPartitions.retain(entry, partitions, value);
                entry.store(at(append.row(), offset), binary(Op.XOR, value.size(),
                        binary(Op.XOR, new Select(value.isNull(), i64(1L << 32), i64(0)),
                                new Select(value.isAscii(), i64(1L << 33), i64(0)))));
                entry.store(MemoryType.ADDRESS, at(append.row(), offset + 8), pointer);
                offset += 16;
            }
            append.commit(entry);
            entry.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    /** Reuse the scan probe stages and slot scratch; a growth exit rebuilds all candidates. */
    static Program merge(QuestMapProbe probe, AggregateUpdates updates, long valueBytes, int batchRows) {
        return merge(probe, updates, updates, valueBytes, batchRows);
    }

    static Program replay(QuestMapProbe probe, AggregateUpdates layout, AggregateUpdates updates) {
        Builder b = new Builder();
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(rowBytes(probe, layout)))));
            Var key = probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            Var destination = probe.findExisting(loop, key);
            loop.when(ne(destination, nil()), hit -> emitValues(hit, layout, updates, probe, row, destination));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    static Program merge(QuestMapProbe probe, AggregateUpdates layout, AggregateUpdates updates, long valueBytes, int batchRows) {
        if (batchRows == 0) return mergeScalar(probe, layout, updates);
        Builder b = new Builder();
        Var index = b.let(load(STATE));
        Var batches = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), batch -> {
            batch.when(eq(binary(Op.AND, batches, i64(Math.max(1, 1024 / batchRows) - 1)), i64(0)), Builder::poll);
            batch.assign(batches, add(batches, i64(1)));
            Var limit = batch.let(new Select(binary(Op.LT, add(index, i64(batchRows)), ROWS),
                    add(index, i64(batchRows)), ROWS));
            Var position = batch.let(index);
            Var scratch = batch.let(at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            batch.loop(binary(Op.LT, position, limit), stage -> {
                Var row = stage.let(new Offset(COLUMNS, binary(Op.MUL, position, i64(rowBytes(probe, layout)))));
                var strings = stringArguments(layout);
                var activeStrings = stringArguments(updates);
                long stringOffset = rowBytes(probe, layout) - strings.size() * 16L;
                for (int i = 0; i < strings.size(); i++) {
                    if (!activeStrings.contains(strings.get(i))) continue;
                    Expr view = at(row, stringOffset + i * 16L);
                    stage.when(ne(binary(Op.AND, load(view), i64(0xffff_ffffL)), i64(0)), present ->
                            present.prefetch(new Load(MemoryType.ADDRESS, at(view, 8)), false));
                }
                Var key = probe.orderedNative() ? row : stage.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
                stage.store(probe.stagedKeyType(), at(scratch, 8), key);
                if (probe.orderedNative()) stage.store(at(scratch, 24), probe.prefetchOffset(stage, key));
                else {
                    var candidate = probe.prepare(stage, key, valueBytes, (hint, destination) -> {});
                    stage.store(MemoryType.ADDRESS, at(scratch, 16), candidate.entry());
                }
                stage.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                stage.assign(position, add(position, i64(1)));
            });
            if (probe.orderedNative()) {
                batch.assign(position, index);
                batch.assign(scratch, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
                batch.loop(binary(Op.LT, position, limit), prepare -> {
                    Var key = prepare.let(new Load(probe.stagedKeyType(), at(scratch, 8)));
                    var candidate = probe.prepare(prepare, key, valueBytes, (hint, destination) -> {}, prepare.let(load(at(scratch, 24))));
                    prepare.store(MemoryType.ADDRESS, at(scratch, 16), candidate.entry());
                    prepare.store(at(scratch, 32), candidate.slot());
                    prepare.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                    prepare.assign(position, add(position, i64(1)));
                });
            }
            batch.assign(scratch, at(STATE, ResolvedAggregateKernel.prefetchScratch(probe)));
            batch.loop(binary(Op.LT, index, limit), consume -> {
                Var key = consume.let(new Load(probe.stagedKeyType(), at(scratch, 8)));
                Var entry = consume.let(new Load(MemoryType.ADDRESS, at(scratch, 16)));
                Var destination = probe.orderedNative()
                        ? probe.finishOrdered(consume, key, entry, consume.let(load(at(scratch, 24))),
                                consume.let(load(at(scratch, 32))), index, layout::initialize)
                        : probe.finish(consume, key, entry, index, layout::initialize);
                Var row = consume.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(rowBytes(probe, layout)))));
                emitValues(consume, layout, updates, probe, row, destination);
                consume.assign(scratch, at(scratch, probe.prefetchRowBytes()));
                consume.assign(index, add(index, i64(1)));
            });
        });
        b.exit(0);
        return new Program(b.block());
    }

    private static void emitValues(Builder b, AggregateUpdates layout, AggregateUpdates updates, QuestMapProbe probe, Var row, Var destination) {
        var values = new ArrayList<Pipeline.Value>();
        long offset = probe.keyBytes();
        for (var column : layout.columns()) {
            Var value = b.let(new Load(column.width(), at(row, offset)));
            values.add(new Pipeline.Value(value, column.nullable() ? eq(value, i64(column.nullValue())) : bool(false)));
            offset += column.width().bytes;
        }
        offset = (offset + 7) & ~7L;
        var strings = new LinkedHashMap<ResolvedVarchar.Expression, Pipeline.Utf8Value>();
        for (var argument : stringArguments(layout)) {
            Var header = b.let(load(at(row, offset)));
            strings.put(argument, new Pipeline.Utf8Value(new Load(MemoryType.ADDRESS, at(row, offset + 8)),
                    binary(Op.AND, header, i64(0xffff_ffffL)),
                    ne(binary(Op.AND, header, i64(1L << 32)), i64(0)),
                    ne(binary(Op.AND, header, i64(1L << 33)), i64(0)), bool(true)));
            offset += 16;
        }
        var frame = new ResolvedVarchar.Frame(new Pipeline.Row(layout.columns(), values), i64(0), nil(), nil(), strings);
        updates.emit(b, frame, destination, nil());
    }

    private static Program mergeScalar(QuestMapProbe probe, AggregateUpdates layout, AggregateUpdates updates) {
        Builder b = new Builder();
        Var index = b.let(load(STATE));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(rowBytes(probe, layout)))));
            Var key = probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            Var destination = probe.find(loop, key, index, i64(0), null, layout::initialize);
            emitValues(loop, layout, updates, probe, row, destination);
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }
}
