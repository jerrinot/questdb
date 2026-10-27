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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/** Fixed-capacity raw-kernel fixtures; arena owns entries so these must never grow. */
final class TestTables {
    static MemorySegment create(Arena arena, int capacity, long stride) {
        var descriptor = arena.allocate(TableState.BYTES, 8);
        long entries = arena.allocate(capacity * stride, 8).address();
        Unsafe.getUnsafe().putLong(descriptor.address(), capacity);
        Unsafe.getUnsafe().putLong(descriptor.address() + TableState.ENTRIES, entries);
        Unsafe.getUnsafe().putLong(descriptor.address() + TableState.MAX_CAPACITY, capacity);
        return descriptor;
    }
    static byte[] bytes(MemorySegment state, long stride) {
        return data(state, stride).toArray(ValueLayout.JAVA_BYTE);
    }
    static void clear(MemorySegment state, long stride) {
        data(state, stride).fill((byte) 0);
        Unsafe.getUnsafe().putLong(state.address() + 8, 0);
    }
    private static MemorySegment data(MemorySegment state, long stride) {
        return MemorySegment.ofAddress(TableState.entries(state.address())).reinterpret(TableState.capacity(state.address()) * stride);
    }
}
