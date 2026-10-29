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

import java.util.function.BiConsumer;

import static org.questdb.experimental.codegen.Ir.*;

/**
 * Exact duplicate filter for fixed-width key-only aggregation. Each remembered key already
 * has a partition record. Collisions only forget keys: final deduplication remains exact.
 */
final class QuestKeyFilter {
    private QuestKeyFilter() { }

    static long stride(QuestMapProbe probe) { return (8 + probe.keyBytes() + 7) & ~7L; }

    static Program program(QuestMapProbe probe, int capacity, ScanPlan filter) {
        Builder b = new Builder();
        long scratch = ResolvedAggregateKernel.prefetchScratch(probe);
        Var cache = b.let(new Load(MemoryType.ADDRESS, at(STATE, scratch)));
        Var partitions = b.let(new Load(MemoryType.ADDRESS, at(STATE, scratch + 8)));
        Var emitted = b.let(i64(0)), accepted = b.let(i64(0));
        BiConsumer<Builder, Expr> consume = (hit, row) -> {
            hit.assign(accepted, add(accepted, i64(1)));
            Var key = probe.loadFrameKey(hit, hit.let(row), at(STATE, scratch + 24));
            Var hash = probe.hash(hit, key);
            Var tag = hit.let(add(binary(Op.AND, hash, i64(-2)), i64(1)));
            Var entry = hit.let(new Offset(cache, binary(Op.MUL, binary(Op.AND, hash, i64(capacity - 1)), i64(stride(probe)))));
            Var same = hit.let(bool(false));
            hit.when(eq(load(entry), tag), candidate -> candidate.assign(same, probe.orderedNative()
                    ? probe.keysEqual(candidate, at(entry, 8), key)
                    : eq(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, at(entry, 8)), key)));
            hit.when(eq(same, bool(false)), missing -> {
                Var sketch = missing.let(new Load(MemoryType.ADDRESS, at(partitions, QuestPartitions.SKETCH_OFFSET)));
                missing.when(ne(sketch, nil()), present -> QuestPartitionSketch.add(present, sketch, hash));
                var append = QuestPartitions.append(missing, partitions, hash, probe.keyBytes());
                if (probe.orderedNative()) {
                    QuestMapMutation.copy(missing, key, append.row(), probe.keyBytes());
                    QuestMapMutation.copy(missing, key, at(entry, 8), probe.keyBytes());
                } else {
                    MemoryType width = probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64;
                    missing.store(width, append.row(), key);
                    missing.store(width, at(entry, 8), key);
                }
                append.commit(missing);
                // Publish after appending. A collision can forget an old key, never invent one.
                missing.store(entry, tag);
                missing.assign(emitted, add(emitted, i64(1)));
            });
        };
        Var index = b.let(load(STATE));
        Var selection = b.let(new Load(MemoryType.ADDRESS, at(STATE, 8)));
        if (filter != null) {
            ScanLowering.filteredRange(filter, null, b, selection, index, consume);
        } else {
            b.loop(binary(Op.LT, index, ROWS), loop -> {
                loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                Var row = loop.let(index);
                loop.when(ne(selection, nil()), selected -> selected.assign(row,
                        load(new Offset(selection, binary(Op.MUL, index, i64(8))))));
                consume.accept(loop, row);
                loop.assign(index, add(index, i64(1)));
            });
        }
        b.store(at(STATE, scratch + 16), emitted);
        if (filter != null) b.store(at(STATE, ResolvedAggregateKernel.FILTERED_ROWS), accepted);
        b.exit(0);
        return new Program(b.block());
    }
}
