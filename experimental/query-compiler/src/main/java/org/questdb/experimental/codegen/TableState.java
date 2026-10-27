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
import java.lang.foreign.Arena;

/** Stable descriptor, shared by generated kernels and handwritten controls. */
final class TableState {
    static final long ENTRIES = 16;
    static final long MAX_CAPACITY = 24;
    static final long GROWS = 32;
    static final long FUNCTION_CARDINALITY = 40;
    static final long SHARDS = 48;
    static final long BYTES = 56;

    private TableState() { }

    static long create(Arena arena, int capacity, int maxCapacity, long stride) {
        return create(arena, capacity, maxCapacity, stride, BYTES);
    }

    static long create(Arena arena, int capacity, int maxCapacity, long stride, long descriptorBytes) {
        checkCapacity(capacity);
        checkCapacity(maxCapacity);
        if (capacity > maxCapacity) { throw new IllegalArgumentException("Initial capacity exceeds maximum"); }
        long bytes = Math.multiplyExact(capacity, stride);
        long entries = KernelMemory.malloc(bytes);
        KernelMemory.checkFailure();
        if (entries == 0) { throw new IllegalStateException("No query allocator is bound"); }
        zero(entries,bytes);
        long state = arena.allocate(descriptorBytes, 8).address();
        put(state, capacity);
        put(state + ENTRIES, entries);
        put(state + MAX_CAPACITY, maxCapacity);
        return state;
    }

    static long entries(long state) { return get(state + ENTRIES); }
    static int capacity(long state) { return Math.toIntExact(get(state)); }

    /** Cold control path: move whole states, including nested-set pointers. */
    static boolean grow(long state, int keyCount, long stride) {
        long oldCapacity = get(state);
        if (oldCapacity >= get(state + MAX_CAPACITY)) { return false; }
        long capacity = oldCapacity * 2;
        long old = entries(state);
        long target = KernelMemory.malloc(capacity * stride);
        KernelMemory.checkFailure();
        if (target == 0) { throw new IllegalStateException("Table allocation failed"); }
        zero(target,capacity*stride);
        for (long i = 0; i < oldCapacity; i++) {
            if ((i & 1023) == 0) { KernelMemory.poll(); KernelMemory.checkFailure(); }
            long entry = old + i * stride;
            if (get(entry) == 0) { continue; }
            long hash = 0;
            for (int k = 0; k < keyCount; k++) { hash = (hash * 31) ^ get(entry + 8L * (k + 1)); }
            hash ^= hash >>> 33;
            hash *= 0xff51afd7ed558ccdL;
            hash ^= hash >>> 33;
            long slot = hash & (capacity - 1);
            while (get(target + slot * stride) != 0) { slot = (slot + 1) & (capacity - 1); }
            for (long offset = 0; offset < stride; offset += 8) { put(target + slot * stride + offset, get(entry + offset)); }
        }
        KernelMemory.free(old, oldCapacity * stride);
        KernelMemory.checkFailure();
        put(state, capacity);
        put(state + ENTRIES, target);
        put(state + GROWS, get(state + GROWS) + 1);
        return true;
    }

    private static void zero(long address,long bytes) {
        for(long offset=0;offset<bytes;offset+=65536) {
            KernelMemory.poll();KernelMemory.checkFailure();
            Unsafe.getUnsafe().setMemory(address+offset,Math.min(65536,bytes-offset),(byte)0);
        }
    }

    static void checkCapacity(int capacity) {
        if (capacity < 1 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Table capacity must be a positive power of two");
        }
    }

    private static long get(long address) { return Unsafe.getUnsafe().getLong(address); }
    private static void put(long address, long value) { Unsafe.getUnsafe().putLong(address, value); }
}
