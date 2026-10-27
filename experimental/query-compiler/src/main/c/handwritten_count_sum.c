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

// Independent reference for COUNT(), COUNT(long), SUM(long), SUM(int), SUM(short).
// Same QuestDB layouts, hashes, row order and Java miss/restart protocol as the generated loop.
// Plan-shape validation and compile-time constants come from HandwrittenCountSum.java.
#include <stdint.h>
#include <stddef.h>
#include <string.h>

static inline uint64_t u64(uint64_t address) {
    uint64_t value;
    memcpy(&value, (const void *)(uintptr_t)address, sizeof(value));
    return value;
}

static inline int64_t i64(uint64_t address) { return (int64_t)u64(address); }

static inline int32_t i32(uint64_t address) {
    int32_t value;
    memcpy(&value, (const void *)(uintptr_t)address, sizeof(value));
    return value;
}

static inline int16_t i16(uint64_t address) {
    int16_t value;
    memcpy(&value, (const void *)(uintptr_t)address, sizeof(value));
    return value;
}

static inline void put(uint64_t address, uint64_t value) {
    memcpy((void *)(uintptr_t)address, &value, sizeof(value));
}

static inline int64_t key_at(uint64_t address) {
#if KEY_BYTES == 4
    return i32(address);
#else
    return i64(address);
#endif
}

static inline uint64_t hash_key(int64_t key) {
#if KEY_BYTES == 4 && !ORDERED
    uint64_t hash = (uint32_t)key;
#else
    uint64_t hash = (uint64_t)key;
#endif
    hash ^= hash >> 37;
    hash *= UINT64_C(0x165667919e3779f9);
    return hash ^ (hash >> 32);
}

static inline void sum(uint64_t address, int64_t value) {
    int64_t old = i64(address);
    put(address, old == INT64_MIN ? (uint64_t)value : (uint64_t)old + (uint64_t)value);
}

static inline void update(uint64_t destination, uint64_t row, uint64_t longs, uint64_t ints, uint64_t shorts) {
    int64_t v = i64(longs + row * 8);
    int32_t w = i32(ints + row * 4);
    int16_t s = i16(shorts + row * 2);
    put(destination, u64(destination) + 1);
    if (v != INT64_MIN) {
        put(destination + 8, u64(destination + 8) + 1);
        sum(destination + 16, v);
    }
    if (w != INT32_MIN) sum(destination + 24, w);
    sum(destination + 32, s);
}

// Full scalar probe is used only if the prepared candidate does not match.
// Returning zero leaves the missing row untouched; Java inserts and refreshes views.
static inline uint64_t find(uint64_t state, int64_t key, uint64_t position) {
    uint64_t hash = hash_key(key), shard = hash >> 56;
    uint64_t view = state + 64 + shard * 48;
    uint64_t base = u64(view), mask = u64(view + 16), slot = hash & mask;
#if ORDERED
    uint64_t offsets = u64(view + 8);
    for (;;) {
        uint64_t packed = u64(offsets + slot * 8);
        uint32_t offset = (uint32_t)packed;
        if (!offset) break;
        if ((packed >> 32) == (uint32_t)hash) {
            uint64_t entry = base + ((uint64_t)offset - 1) * 8;
            if (key_at(entry) == key) return entry + KEY_BYTES;
        }
        slot = (slot + 1) & mask;
    }
#else
    if (key == 0) {
        if (u64(view + 40)) return u64(view + 32) + KEY_BYTES;
    } else {
        uint64_t stride = u64(view + 24);
        for (;;) {
            uint64_t entry = base + slot * stride;
            int64_t stored = key_at(entry);
            if (stored == 0) break;
            if (stored == key) return entry + KEY_BYTES;
            slot = (slot + 1) & mask;
        }
    }
#endif
    put(state + 24, shard);
    put(state + 32, position);
    return 0;
}

static inline uint64_t prepare(uint64_t state, int64_t key) {
    uint64_t hash = hash_key(key), view = state + 64 + (hash >> 56) * 48;
    uint64_t base = u64(view), mask = u64(view + 16), slot = hash & mask, entry = 0;
#if ORDERED
    uint64_t offsets = u64(view + 8);
    for (;;) {
        uint64_t packed = u64(offsets + slot * 8);
        uint32_t offset = (uint32_t)packed;
        if (!offset) break;
        if ((packed >> 32) == (uint32_t)hash) {
            entry = base + ((uint64_t)offset - 1) * 8;
            break;
        }
        slot = (slot + 1) & mask;
    }
#else
    if (key == 0) {
        if (u64(view + 40)) entry = u64(view + 32);
    } else {
        entry = base + slot * u64(view + 24);
    }
#endif
#if HINTS
    if (entry) {
        __builtin_prefetch((const void *)(uintptr_t)entry, 1, 3);
        __builtin_prefetch((const void *)(uintptr_t)(entry + KEY_BYTES + 40 - 1), 1, 3);
    }
#endif
    return entry;
}

int64_t query(uint64_t columns, int64_t rows, uint64_t state,
              uint64_t allocate_fn, uint64_t free_fn, uint64_t poll_fn) {
    (void)allocate_fn;
    (void)free_fn;
    uint64_t longs = u64(columns + 8), ints = u64(columns + 16), shorts = u64(columns + 24);
#if SHARDED
    uint64_t row_ids = u64(state + 8), keys = u64(state + 16);
    uint64_t scratch = state + 64 + 256 * 48;
    int64_t batches = 0;
    for (int64_t index = i64(state); index < rows;) {
        if ((batches++ & 31) == 0 && ((int64_t (*)(void))(uintptr_t)poll_fn)()) return 4;
        int64_t limit = rows - index < 32 ? rows : index + 32;
        uint64_t cursor = scratch;
        for (int64_t position = index; position < limit; position++, cursor += 24) {
            uint64_t row = row_ids ? u64(row_ids + (uint64_t)position * 8) : (uint64_t)position;
            int64_t key = key_at(keys + row * KEY_BYTES);
            uint64_t entry = prepare(state, key);
            put(cursor, row);
            put(cursor + 8, (uint64_t)key);
            put(cursor + 16, entry);
        }
        cursor = scratch;
        for (; index < limit; index++, cursor += 24) {
            uint64_t row = u64(cursor), entry = u64(cursor + 16);
            int64_t key = i64(cursor + 8);
            uint64_t destination = entry && key_at(entry) == key ? entry + KEY_BYTES : find(state, key, (uint64_t)index);
            if (!destination) return 4;
            update(destination, row, longs, ints, shorts);
        }
    }
#else
    (void)poll_fn;
    uint64_t batches = u64(columns);
    for (int64_t index = 0; index < rows; index++) {
        uint64_t encoded = u64(batches + (uint64_t)index * 8);
        update(state + (encoded & OFFSET_MASK), (encoded >> ROW_SHIFT) & ROW_MASK, longs, ints, shorts);
    }
#endif
    return 0;
}
