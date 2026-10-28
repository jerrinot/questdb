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

// Isolated execution-headroom experiment, not an engine operator.
// Hash probing, control-byte SIMD, insertion and growth come from QuestDB's Rosti.
#include <cstdlib>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <cstdio>
#include "rosti.h"

void *rosti_malloc(size_t bytes) { return std::malloc(bytes); }

namespace {
using Clock = std::chrono::steady_clock;
uint64_t elapsed(Clock::time_point begin) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin).count();
}
struct Frame { const int64_t *keys, *longs; const int32_t *ints; const int16_t *shorts; uint64_t rows, begin; };
struct Agg { int64_t count, count_value, sum_long, sum_int, sum_short; };
struct Result { int64_t key; Agg value; };
static_assert(sizeof(Frame) == 48 && sizeof(Agg) == 40 && sizeof(Result) == 48);
const Agg empty = {0, 0, INT64_MIN, INT64_MIN, INT64_MIN};

inline void sum(int64_t &target, int64_t value) {
    target = target == INT64_MIN ? value : (int64_t)((uint64_t)target + (uint64_t)value);
}
inline void update(Agg &a, int64_t v, int32_t w, int16_t s) {
    a.count++;
    if (v != INT64_MIN) { a.count_value++; sum(a.sum_long, v); }
    if (w != INT32_MIN) sum(a.sum_int, w);
    sum(a.sum_short, s);
}
inline void merge(Agg &a, const Agg &b) {
    a.count += b.count;
    a.count_value += b.count_value;
    if (b.sum_long != INT64_MIN) sum(a.sum_long, b.sum_long);
    if (b.sum_int != INT64_MIN) sum(a.sum_int, b.sum_int);
    if (b.sum_short != INT64_MIN) sum(a.sum_short, b.sum_short);
}
inline uint64_t hash_long(int64_t key) {
    uint64_t h = (uint64_t)key;
    h ^= h >> 37; h *= UINT64_C(0x165667919e3779f9); return h ^ (h >> 32);
}
inline bool eq_long(void *p, int64_t key) { return ((Result *)p)->key == key; }
inline uint64_t hash_slot(void *p) { return hash_long(((Result *)p)->key); }

struct MapDelete {
    void operator()(rosti_t *p) const { if (p) { std::free(p->slot_initial_values_); delete p; } }
};
using Map = std::unique_ptr<rosti_t, MapDelete>;
Map make_map() {
    Map map(new rosti_t());
    map->slot_size_ = 64; map->slot_size_shift_ = 6; map->capacity_ = 63;
    rosti_t *p = map.get();
    if (!initialize_slots(&p)) throw std::bad_alloc();
    std::memset(map->slot_initial_values_, 0, 64);
    ((Result *)map->slot_initial_values_)->value = empty;
    return map;
}
inline Agg &find_value(rosti_t *map, int64_t key) {
    auto found = find_or_prepare_insert<int64_t>(map, key, hash_long, eq_long, hash_slot, cpySlot);
    if (found.first == UL_MAX) throw std::bad_alloc();
    auto *entry = (Result *)(map->slots_ + found.first);
    if (found.second) entry->key = key;
    return entry->value;
}

template<class F> void parallel(int workers, F fn) {
    std::mutex mutex;
    std::exception_ptr failure;
    auto work = [&](int worker) {
        try { fn(worker); }
        catch (...) { std::lock_guard<std::mutex> lock(mutex); if (!failure) failure = std::current_exception(); }
    };
    std::vector<std::jthread> threads;
    for (int w = 1; w < workers; ++w) threads.emplace_back(work, w);
    work(0);
    threads.clear(); // joins even when a worker failed
    if (failure) std::rethrow_exception(failure);
}

template<class F> void slices(const Frame *frames, int64_t frame_count, uint64_t total, int worker, int workers, F fn) {
    uint64_t lo = total / workers * worker + std::min<uint64_t>(total % workers, worker);
    uint64_t hi = total / workers * (worker + 1) + std::min<uint64_t>(total % workers, worker + 1);
    for (int64_t f = 0; f < frame_count; ++f) {
        const auto &frame = frames[f];
        if (frame.begin >= hi || frame.begin + frame.rows <= lo) continue;
        fn(frame, lo > frame.begin ? lo - frame.begin : 0, std::min(hi - frame.begin, frame.rows));
    }
}

struct Free { void operator()(Agg *p) const { std::free(p); } };
using Dense = std::unique_ptr<Agg, Free>;
}

extern "C" int64_t query(uint64_t input, int64_t frame_count, uint64_t state_address,
                          uint64_t allocate_fn, uint64_t free_fn, uint64_t poll_fn, uint64_t realloc_fn) {
    (void)allocate_fn; (void)free_fn; (void)poll_fn; (void)realloc_fn;
    auto *state = (uint64_t *)state_address;
    if ((int64_t)state[0] == -1) { std::free((void *)state[2]); state[2] = state[3] = 0; return 0; }
    try {
        const auto *frames = (const Frame *)input;
        const int workers = (int)state[1];
        if (workers < 1 || workers > 64 || frame_count < 0) return 2;
        const uint64_t total = frame_count ? frames[frame_count - 1].begin + frames[frame_count - 1].rows : 0;
        state[11] = total;
        uint64_t minimum = UINT64_MAX, maximum = 0, accepted = 0;
        bool dense = false;
        state[4] = state[5] = state[6] = state[7] = state[8] = state[9] = state[10] = 0;
        if (state[0] == 1) {
            auto begin = Clock::now();
            struct Range { uint64_t lo = UINT64_MAX, hi = 0, count = 0; };
            std::vector<Range> ranges(workers);
            parallel(workers, [&](int worker) {
                // Keep the reduction private while scanning; adjacent worker
                // summaries must not bounce cache lines on every input row.
                Range r;
                slices(frames, frame_count, total, worker, workers, [&](const Frame &f, uint64_t lo, uint64_t hi) {
                    for (uint64_t i = lo; i < hi; ++i) {
                        int64_t k = f.keys[i];
                        if (k >= 0) { r.lo = std::min(r.lo, (uint64_t)k); r.hi = std::max(r.hi, (uint64_t)k); ++r.count; }
                    }
                });
                ranges[worker] = r;
            });
            for (const auto &r : ranges) { minimum = std::min(minimum, r.lo); maximum = std::max(maximum, r.hi); accepted += r.count; }
            const uint64_t max_range = (512ULL << 20) / sizeof(Agg) / workers;
            dense = accepted && maximum - minimum < max_range;
            state[4] = elapsed(begin);
            state[9] = accepted ? maximum - minimum + 1 : 0;
        }
        state[7] = dense;
        std::vector<std::vector<Result>> results(workers);
        auto build_begin = Clock::now();
        if (dense) {
            const uint64_t range = maximum - minimum + 1;
            std::vector<Dense> states(workers);
            parallel(workers, [&](int worker) {
                states[worker].reset((Agg *)std::calloc(range, sizeof(Agg)));
                if (!states[worker]) throw std::bad_alloc();
                Agg *__restrict a = states[worker].get();
                slices(frames, frame_count, total, worker, workers, [&](const Frame &f, uint64_t lo, uint64_t hi) {
                    for (uint64_t i = lo; i < hi; ++i) {
                        int64_t key = f.keys[i];
                        if (key >= 0) {
                            auto &value = a[(uint64_t)key - minimum];
                            if (value.count == 0) value = empty;
                            update(value, f.longs[i], f.ints[i], f.shorts[i]);
                        }
                    }
                });
            });
            state[5] = elapsed(build_begin);
            state[10] = range * sizeof(Agg) * workers;
            auto merge_begin = Clock::now();
            parallel(workers, [&](int worker) {
                auto &output = results[worker];
                uint64_t lo = range * worker / workers, hi = range * (worker + 1) / workers;
                output.reserve(hi - lo);
                for (uint64_t i = lo; i < hi; ++i) {
                    Agg value = empty;
                    for (int w = 0; w < workers; ++w) if (states[w].get()[i].count) merge(value, states[w].get()[i]);
                    if (value.count) output.push_back({(int64_t)(minimum + i), value});
                }
            });
            state[6] = elapsed(merge_begin);
        } else {
            std::vector<Map> maps(workers);
            parallel(workers, [&](int worker) {
                maps[worker] = make_map();
                auto *map = maps[worker].get();
                slices(frames, frame_count, total, worker, workers, [&](const Frame &f, uint64_t lo, uint64_t hi) {
                    for (uint64_t i = lo; i < hi; ++i) {
                        int64_t key = f.keys[i];
                        if (key >= 0) update(find_value(map, key), f.longs[i], f.ints[i], f.shorts[i]);
                    }
                });
            });
            state[5] = elapsed(build_begin);
            for (const auto &m : maps) state[10] += memorySize(m.get());
            auto merge_begin = Clock::now();
            parallel(workers, [&](int worker) {
                auto combined = make_map();
                for (const auto &source : maps) {
                    for (uint64_t i = 0; i < source->capacity_; ++i) {
                        if (!IsFull(source->ctrl_[i])) continue;
                        const auto &entry = *(const Result *)(source->slots_ + (i << source->slot_size_shift_));
                        if ((hash_long(entry.key) >> 56) % workers == (uint64_t)worker)
                            merge(find_value(combined.get(), entry.key), entry.value);
                    }
                }
                auto &output = results[worker];
                output.reserve(combined->size_);
                for (uint64_t i = 0; i < combined->capacity_; ++i)
                    if (IsFull(combined->ctrl_[i])) output.push_back(*(const Result *)(combined->slots_ + (i << combined->slot_size_shift_)));
            });
            state[6] = elapsed(merge_begin);
        }
        uint64_t count = 0;
        for (const auto &r : results) count += r.size();
        auto *output = (Result *)std::malloc(std::max<uint64_t>(1, count) * sizeof(Result));
        if (!output) throw std::bad_alloc();
        uint64_t offset = 0;
        for (const auto &r : results) {
            if (!r.empty()) std::memcpy(output + offset, r.data(), r.size() * sizeof(Result));
            for (const auto &entry : r) state[8] += entry.value.count;
            offset += r.size();
        }
        state[2] = (uint64_t)output; state[3] = count;
        return 0;
    } catch (const std::exception &e) {
        std::fprintf(stderr, "COUNT/SUM reference: %s\n", e.what());
        return 1;
    }
}
