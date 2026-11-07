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
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;

/** Per-worker group destinations indexed by frozen build row; released after workers drain. */
final class JoinGroupCache implements AutoCloseable {
    private final DirectLongList memory = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
    private long rows;

    void prepare(FrozenHashJoinBuild[] builds, int build, long probeRows, int slots,
                 long reservedBytes, MemoryTracker tracker) {
        if (build < 0 || slots == 0 || probeRows <= 0) return;
        for (var input : builds) if (input.getRowCount() == 0) return;
        long count = builds[build].getRowCount();
        // At least 32 probes per entry across all slots; total storage <= probeRows/4.
        if (count > probeRows / 32 / slots || builds[build].getRowHandle(0) < 0) return;
        long entries = count * slots;
        if (tracker != null && tracker.getLimit() > 0
                && entries * 8 > tracker.getLimit() - tracker.getUsed() - reservedBytes) return;
        memory.setMemoryTracker(tracker);
        memory.ensureCapacity(entries);
        rows = count;
    }

    boolean isAllocated() { return memory.getAddress() != 0; }

    @Override
    public void close() {
        memory.close();
        memory.setMemoryTracker(null);
        rows = 0;
    }

    final class Slot {
        private final int index;
        private final long[] mapBases = new long[GroupByMapFragment.NUM_SHARDS];
        private int mapCount;

        Slot(int index) { this.index = index; }

        void clear() { mapCount = 0; }

        long bind(long views, int count) {
            if (!isAllocated()) return 0;
            boolean moved = mapCount != count;
            for (int i = 0; i < count; i++) moved |= moved(views, i);
            mapCount = count;
            // First acquisition clears this worker's slice on its own CPU.
            invalidate(moved);
            return address();
        }

        void inserted(long views, int shard) {
            if (!isAllocated()) return;
            // OrderedMap scratch-key writes can move shard zero as well as the target.
            boolean moved = moved(views, 0);
            if (shard != 0) moved |= moved(views, shard);
            invalidate(moved);
        }

        private long address() { return memory.getAddress() + index * rows * 8; }

        private void invalidate(boolean moved) {
            if (moved) Unsafe.setMemory(address(), rows * 8, (byte) 0);
        }

        private boolean moved(long views, int shard) {
            long base = Unsafe.getLong(views + (long) shard * QuestMapProbe.VIEW_BYTES + NativeMapView.ENTRIES);
            boolean moved = mapBases[shard] != base;
            mapBases[shard] = base;
            return moved;
        }
    }
}
