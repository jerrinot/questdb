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

import io.questdb.std.Unsafe;
import io.questdb.griffin.engine.groupby.GroupByLongHashSet;

/** Handwritten controls for the demo plans, not an alternative general backend. */
final class HandwrittenKernels {
    private HandwrittenKernels() {
    }

    /** Same outer map as generated code; the inner set is QuestDB's actual implementation. */
    static Kernel nestedDistinct(int initialCapacity, double loadFactor) {
        GroupByLongHashSet set = new GroupByLongHashSet(initialCapacity, loadFactor, Long.MIN_VALUE);
        return (columns, rows, state) -> {
            set.setAllocator(KernelMemory.allocator());
            long keys = get(columns);
            long values = get(columns + 8);
            for (long row = 0; row < rows; row++) {
                long key = keys == 0 ? Integer.MIN_VALUE : Unsafe.getUnsafe().getInt(keys + row * 4);
                long value = values == 0 ? Long.MIN_VALUE : get(values + row * 8);
                if (key == Integer.MIN_VALUE) { key = Long.MIN_VALUE; }
                long entry = find(state, key, 0, false, 32);
                if (entry == 0) { return 1; }
                if (get(entry) == 0) {
                    put(entry, 1);
                    put(entry + 8, key);
                    put(entry + 16, 0);
                    put(entry + 24, 0);
                    put(state + 8, get(state + 8) + 1);
                }
                updateDistinct(set, entry + 16, value);
            }
            return 0;
        };
    }

    // RegionID INT, UserID LONG, AdvEngineID SHORT; SUM, COUNT, nested DISTINCT.
    static Kernel nestedMixed(int initialCapacity, double loadFactor) {
        GroupByLongHashSet set = new GroupByLongHashSet(initialCapacity, loadFactor, Long.MIN_VALUE);
        return (columns, rows, state) -> {
            set.setAllocator(KernelMemory.allocator());
            long keys = get(columns);
            long users = get(columns + 8);
            long engines = get(columns + 16);
            for (long row = 0; row < rows; row++) {
                long key = keys == 0 ? Integer.MIN_VALUE : Unsafe.getUnsafe().getInt(keys + row * 4);
                long user = users == 0 ? Long.MIN_VALUE : get(users + row * 8);
                long engine = engines == 0 ? 0 : Unsafe.getUnsafe().getShort(engines + row * 2);
                if (key == Integer.MIN_VALUE) { key = Long.MIN_VALUE; }
                long entry = find(state, key, 0, false, 48);
                if (entry == 0) { return 1; }
                if (get(entry) == 0) {
                    put(entry, 1);
                    put(entry + 8, key);
                    put(entry + 16, Long.MIN_VALUE);
                    put(entry + 24, 0);
                    put(entry + 32, 0);
                    put(entry + 40, 0);
                    put(state + 8, get(state + 8) + 1);
                }
                long sum = get(entry + 16);
                put(entry + 16, sum == Long.MIN_VALUE ? engine : sum + engine);
                put(entry + 24, get(entry + 24) + 1);
                updateDistinct(set, entry + 32, user);
            }
            return 0;
        };
    }

    private static void updateDistinct(GroupByLongHashSet set, long countAddress, long value) {
        if (value != Long.MIN_VALUE) {
            long count = get(countAddress);
            if (count == 0) {
                put(countAddress, 1);
                put(countAddress + 8, value);
            } else if (count == 1) {
                long old = get(countAddress + 8);
                if (old != value) {
                    set.of(0).add(value);
                    set.add(old);
                    put(countAddress, 2);
                    put(countAddress + 8, set.ptr());
                }
            } else {
                long index = set.of(get(countAddress + 8)).keyIndex(value);
                if (index >= 0) {
                    set.addAt(index, value);
                    put(countAddress, count + 1);
                    put(countAddress + 8, set.ptr());
                }
            }
        }
    }

    // k INT, v LONG, f BYTE; WHERE f != 0; GROUP BY k; COUNT, COUNT(v), SUM(v+3).
    static long mixed(long columns, long rows, long state) {
        long keys = get(columns);
        long values = get(columns + 8);
        long flags = get(columns + 16);
        for (long row = 0; row < rows; row++) {
            long key = keys == 0 ? Integer.MIN_VALUE : Unsafe.getUnsafe().getInt(keys + row * 4);
            long value = values == 0 ? Long.MIN_VALUE : get(values + row * 8);
            long flag = flags == 0 ? 0 : Unsafe.getUnsafe().getByte(flags + row);
            if (flag != 0) {
                if (key == Integer.MIN_VALUE) { key = Long.MIN_VALUE; }
                long entry = find(state, key, 0, false, 40);
                if (entry == 0) { return 1; }
                if (get(entry) == 0) {
                    put(entry, 1);
                    put(entry + 8, key);
                    put(entry + 16, 0);
                    put(entry + 24, 0);
                    put(entry + 32, Long.MIN_VALUE);
                    put(state + 8, get(state + 8) + 1);
                }
                put(entry + 16, get(entry + 16) + 1);
                if (value != Long.MIN_VALUE) { put(entry + 24, get(entry + 24) + 1); }
                long term = value + 3;
                if (value != Long.MIN_VALUE && term != Long.MIN_VALUE) {
                    long sum = get(entry + 32);
                    put(entry + 32, sum == Long.MIN_VALUE ? term : sum + term);
                }
            }
        }
        return 0;
    }

    // RegionID INT, UserID LONG; retain unique pairs, including one null marker per group.
    static long distinct(long columns, long rows, long state) {
        long keys = get(columns);
        long values = get(columns + 8);
        for (long row = 0; row < rows; row++) {
            long key = keys == 0 ? Integer.MIN_VALUE : Unsafe.getUnsafe().getInt(keys + row * 4);
            long value = values == 0 ? Long.MIN_VALUE : get(values + row * 8);
            if (key == Integer.MIN_VALUE) { key = Long.MIN_VALUE; }
            long entry = find(state, key, value, true, 24);
            if (entry == 0) { return 1; }
            if (get(entry) == 0) {
                put(entry, 1);
                put(entry + 8, key);
                put(entry + 16, value);
                put(state + 8, get(state + 8) + 1);
            }
        }
        return 0;
    }

    static long collapse(long pairs, long slots, long state) {
        for (long i = 0; i < slots; i++) {
            long pair = pairs + i * 24;
            if (get(pair) != 0) {
                long key = get(pair + 8);
                long entry = find(state, key, 0, false, 24);
                if (entry == 0) { return 1; }
                if (get(entry) == 0) {
                    put(entry, 1);
                    put(entry + 8, key);
                    put(entry + 16, 0);
                    put(state + 8, get(state + 8) + 1);
                }
                if (get(pair + 16) != Long.MIN_VALUE) { put(entry + 16, get(entry + 16) + 1); }
            }
        }
        return 0;
    }

    private static long find(long state, long key, long second, boolean pair, long stride) {
        long hash = pair ? (key * 31) ^ second : key;
        hash ^= hash >>> 33;
        hash *= 0xff51afd7ed558ccdL;
        hash ^= hash >>> 33;
        while (true) {
            long capacity = get(state);
            long mask = capacity - 1;
            long entries = TableState.entries(state);
            long slot = hash & mask;
            long entry = entries + slot * stride;
            long probes = 0;
            while (get(entry) != 0 && (get(entry + 8) != key || (pair && get(entry + 16) != second))) {
                if ((probes & 1023) == 1023) { KernelMemory.poll(); KernelMemory.checkFailure(); }
                if (++probes >= capacity) { entry = 0; break; }
                slot = (slot + 1) & mask;
                entry = entries + slot * stride;
            }
            if ((entry == 0 || get(entry) == 0) && get(state + 8) >= capacity - (capacity >>> 2)
                    && TableState.grow(state, pair ? 2 : 1, stride)) { continue; }
            return entry;
        }
    }

    private static long get(long address) { return Unsafe.getUnsafe().getLong(address); }
    private static void put(long address, long value) { Unsafe.getUnsafe().putLong(address, value); }
}
