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
import io.questdb.std.Unsafe;
import java.lang.foreign.Arena;

/** GroupByMapFragment's state transition over the prototype's existing table ABI.
 * The allocator is bound by the caller. All shards have disjoint grouping keys.
 */
final class ParallelFragment {
    final long root;
    final long columns;
    long[] shards;
    private final Arena arena;
    private final Pipeline pipeline;
    private final CompiledGroupBy.Options options;

    ParallelFragment(Pipeline pipeline, Arena arena, CompiledGroupBy.Options options) {
        this.arena = arena;
        this.pipeline = pipeline;
        this.options = options;
        root = TableState.create(arena, options.initialCapacity(), options.maxCapacity(), pipeline.entryBytes());
        columns = arena.allocate(pipeline.columns().size() * 8L, 8).address();
    }

    void openShards() {
        if (shards != null) { return; }
        shards = new long[GroupByMapFragment.NUM_SHARDS];
        long addresses = arena.allocate(shards.length * 8L, 8).address();
        Unsafe.getUnsafe().putLong(root + TableState.SHARDS, addresses);
        for (int i = 0; i < shards.length; i++) {
            shards[i] = TableState.create(arena, options.initialCapacity(), options.maxCapacity(), pipeline.entryBytes());
            Unsafe.getUnsafe().putLong(addresses + i * 8L, shards[i]);
        }
    }

    long groups() { return sum(8); }
    long grows() { return get(root + TableState.GROWS) + (shards == null ? 0 : sum(TableState.GROWS)); }
    long cardinality() { return get(root + TableState.FUNCTION_CARDINALITY); }

    private long sum(long offset) {
        if (shards == null) { return get(root + offset); }
        long total = 0;
        for (long shard : shards) { total += get(shard + offset); }
        return total;
    }
    private static long get(long address) { return Unsafe.getUnsafe().getLong(address); }
}
