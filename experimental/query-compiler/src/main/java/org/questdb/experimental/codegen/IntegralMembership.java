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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.SplitBlockBloomFilter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.std.DirectLongList;
import io.questdb.std.Hash;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;

import java.util.function.LongConsumer;
import java.util.function.Predicate;

import static org.questdb.experimental.codegen.Ir.*;

/** Exact INT sets and conservative integral join membership, owned until frame workers drain. */
final class IntegralMembership implements AutoCloseable {
    static final int BYTES = 32;
    private final DirectLongList memory = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
    private long first, last = -1, count;

    /** Exact membership; keys must be sorted and distinct. */
    void prepare(IntList keys, MemoryTracker tracker) {
        close();
        memory.setMemoryTracker(tracker);
        int size = keys.size();
        if (size == 0) return;
        first = keys.getQuick(0);
        last = keys.getQuick(size - 1);
        long span = last - first + 1;
        if (span == size) return; // A contiguous range needs no allocation or lookup.
        long words = (span + 63) >>> 6;
        if (words * 8 <= size * 4L) {
            memory.ensureCapacity(words);
            memory.fill(0);
            for (int i = 0; i < size; i++) {
                long bit = keys.getQuick(i) - first;
                long address = memory.getAddress() + (bit >>> 6) * 8;
                Unsafe.putLong(address, Unsafe.getLong(address) | (1L << bit));
            }
            count = -1; // Bitmap storage never exceeds the sorted-key representation.
        } else {
            memory.ensureCapacity((size + 1L) / 2);
            for (int i = 0; i < size; i++) Unsafe.putInt(memory.getAddress() + i * 4L, keys.getQuick(i));
            count = size;
        }
    }

    /** Completed ordinary keys; sparse domains use the same bounded Bloom layout as reduced builds. */
    void prepareJoin(Map map, java.util.function.ToLongFunction<io.questdb.cairo.sql.Record> key,
                     int keyBytes, MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        long size = map == null ? -1 : map.size();
        if (size < 0 || !canPrepareJoin(size, tracker, 0)) {
            universal();
            return;
        }
        close();
        memory.setMemoryTracker(tracker);
        if (size == 0) return;
        first = Long.MAX_VALUE;
        last = Long.MIN_VALUE;
        var cursor = map.getCursor();
        var record = cursor.getRecord();
        while (cursor.hasNext()) {
            breaker.statefulThrowExceptionIfTrippedOrYield();
            long value = key.applyAsLong(record);
            first = Math.min(first, value);
            last = Math.max(last, value);
        }
        allocateJoin(size, size * keyBytes);
        if (count == 0) return;
        cursor.toTop();
        while (cursor.hasNext()) {
            breaker.statefulThrowExceptionIfTrippedOrYield();
            insertJoin(key.applyAsLong(record));
        }
    }

    static boolean canPrepareJoin(long size, MemoryTracker tracker, long scratchBytes) {
        return size <= (Integer.MAX_VALUE - 31L) / Long.BYTES
                && (size <= 0 || tracker == null || tracker.getLimit() <= 0
                    || size * Long.BYTES + 32 + scratchBytes <= tracker.getLimit() - tracker.getUsed());
    }

    /** Same representation policy after a generated scan supplies the completed map's bounds. */
    void prepareJoin(long size, long first, long last, int keyBytes, MemoryTracker tracker) {
        close();
        memory.setMemoryTracker(tracker);
        this.first = first;
        this.last = last;
        allocateJoin(size, size * keyBytes);
    }

    // Completed maps and reduced keys have repeatable iteration. Optional frozen-build
    // iteration below retains its bounds-only fallback and lazy bitmap allocation.
    private void allocateJoin(long size, long bitmapBudget) {
        long span = last - first + 1;
        if (span > 0 && span == size) return;
        long bitmapBytes = bitmapBytes(span);
        boolean bitmap = bitmapBytes <= bitmapBudget;
        long bytes = bitmap ? bitmapBytes : Math.max(SplitBlockBloomFilter.BLOCK_SIZE,
                (size * Long.BYTES + SplitBlockBloomFilter.BLOCK_SIZE - 1) & -SplitBlockBloomFilter.BLOCK_SIZE);
        memory.ensureCapacity(bytes / Long.BYTES);
        memory.fill(0);
        count = bitmap ? -1 : -bytes;
    }

    /** Exact bitmap footprint, or -1 when the producer does not provide bounds. */
    static long joinBitmapBytes(FrozenHashJoinBuild build) {
        long size = build.getKeyCount();
        if (size == 0) return 0;
        long first = build.getMinIntegralKey(), last = build.getMaxIntegralKey();
        if (size < 0 || first > last) return -1;
        long span = last - first + 1;
        return span == size && span > 0 ? 0 : bitmapBytes(span);
    }

    void prepareJoin(FrozenHashJoinBuild build, MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        prepareJoin(build, tracker, breaker, Math.max(0, build.getKeyCount()) * Integer.BYTES);
    }

    void prepareJoin(FrozenHashJoinBuild build, MemoryTracker tracker, SqlExecutionCircuitBreaker breaker, long bitmapBudget) {
        close();
        memory.setMemoryTracker(tracker);
        long size = build.getKeyCount();
        if (size == 0) return;
        first = build.getMinIntegralKey();
        last = build.getMaxIntegralKey();
        if (first > last) {
            prepareJoin(size, visitor -> build.forEachIntegralKey(visitor, breaker), tracker, bitmapBudget);
        } else {
            prepareJoinBitmap(size, visitor -> build.forEachIntegralKey(visitor, breaker), bitmapBudget);
        }
    }

    private void prepareJoin(long size, Predicate<LongConsumer> visit, MemoryTracker tracker) {
        prepareJoin(size, visit, tracker, Math.max(0, size) * Integer.BYTES);
    }

    private void prepareJoin(long size, Predicate<LongConsumer> visit, MemoryTracker tracker, long bitmapBudget) {
        close();
        memory.setMemoryTracker(tracker);
        if (size == 0) return;
        first = Long.MAX_VALUE;
        last = Long.MIN_VALUE;
        if (size < 0 || !visit.test(key -> {
            first = Math.min(first, key);
            last = Math.max(last, key);
        })) {
            first = Long.MIN_VALUE;
            last = Long.MAX_VALUE;
            return; // An unavailable producer must not discard any probe key.
        }
        prepareJoinBitmap(size, visit, bitmapBudget);
    }

    private static long bitmapBytes(long span) {
        // Subtraction and rounding must never wrap a full-width LONG range into a small bitmap.
        return span <= 0 || span > Long.MAX_VALUE - 63 ? Long.MAX_VALUE : ((span + 63) >>> 6) * Long.BYTES;
    }

    void universal() {
        close();
        first = Long.MIN_VALUE;
        last = Long.MAX_VALUE;
    }

    private void prepareJoinBitmap(long size, Predicate<LongConsumer> visit, long bitmapBudget) {
        if (size < 0) return;
        long span = last - first + 1;
        if (span == size && span > 0) return;
        long bytes = bitmapBytes(span);
        long words = bytes / Long.BYTES;
        if (bytes != Long.MAX_VALUE && bytes <= bitmapBudget) {
            if (!visit.test(key -> {
                // Iteration is optional even when bounds exist; allocate only once it yields a key.
                if (memory.getAddress() == 0) {
                    memory.ensureCapacity(words);
                    memory.fill(0);
                }
                long bit = key - first;
                long address = memory.getAddress() + (bit >>> 6) * 8;
                Unsafe.putLong(address, Unsafe.getLong(address) | (1L << bit));
            })) {
                // Bounds can be available without iteration; retain them without an empty bitmap.
                memory.close();
                return;
            }
            count = memory.getAddress() == 0 ? 0 : -1;
        }
        // Sparse domains retain conservative bounds; exact probing still decides membership.
    }

    boolean contains(long key) {
        if (key < first || key > last) return false;
        if (count == 0) return true;
        long base = memory.getAddress();
        if (count < -1) return SplitBlockBloomFilter.mightContain(base, (int) -count, Hash.hashLong64(key));
        if (count == -1) {
            long bit = key - first;
            return ((Unsafe.getLong(base + (bit >>> 6) * 8) >>> (bit & 63)) & 1) != 0;
        }
        long lo = 0, hi = count;
        while (lo < hi) {
            long mid = (lo + hi) >>> 1;
            if (Unsafe.getInt(base + mid * 4) < key) lo = mid + 1;
            else hi = mid;
        }
        return lo < count && Unsafe.getInt(base + lo * 4) == key;
    }

    boolean hasBitmap() { return count == -1; }

    /** Reduced distinct keys in the producer's domain; sparse keys use the existing split-block layout. */
    void prepareReducedKeys(DirectLongList keys, MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        close();
        memory.setMemoryTracker(tracker);
        long size = keys.size();
        if (size == 0) return;
        first = Long.MAX_VALUE;
        last = Long.MIN_VALUE;
        for (long i = 0; i < size; i++) {
            breaker.statefulThrowExceptionIfTrippedTimeThrottled();
            long key = keys.get(i);
            first = Math.min(first, key);
            last = Math.max(last, key);
        }
        allocateJoin(size, size * Long.BYTES);
        if (count == 0) return;
        for (long i = 0; i < size; i++) {
            breaker.statefulThrowExceptionIfTrippedTimeThrottled();
            insertJoin(keys.get(i));
        }
    }

    private void insertJoin(long key) {
        if (count == -1) {
            long bit = key - first;
            long address = memory.getAddress() + (bit >>> 6) * Long.BYTES;
            Unsafe.putLong(address, Unsafe.getLong(address) | (1L << bit));
        } else SplitBlockBloomFilter.insert(memory.getAddress(), (int) -count, Hash.hashLong64(key));
    }

    boolean isUniversal() {
        return count == 0 && (first == Long.MIN_VALUE && last == Long.MAX_VALUE
                || first == Integer.MIN_VALUE && last == Integer.MAX_VALUE);
    }

    void write(long descriptor) {
        Unsafe.putLong(descriptor, first);
        Unsafe.putLong(descriptor + 8, last);
        Unsafe.putLong(descriptor + 16, memory.getAddress());
        Unsafe.putLong(descriptor + 24, count);
    }

    record Descriptor(Expr first, Expr last, Expr base, Expr count) { }

    static Descriptor bind(Builder b, Expr descriptor) {
        return new Descriptor(b.let(load(descriptor)), b.let(load(at(descriptor, 8))),
                b.let(new Load(MemoryType.ADDRESS, at(descriptor, 16))), b.let(load(at(descriptor, 24))));
    }

    static Expr contains(Builder b, Expr descriptor, Expr key) {
        return contains(b, bind(b, descriptor), key);
    }

    /** Ordinary join preparation publishes ranges or bitmaps; reduced dependencies may also use Bloom storage. */
    static Expr containsJoin(Builder b, Descriptor descriptor, Expr key) {
        return containsJoin(b, descriptor, key, false);
    }

    static Expr containsJoin(Builder b, Descriptor descriptor, Expr key, boolean allowBloom) {
        Var found = b.let(bool(false));
        b.when(binary(Op.BOTH, binary(Op.GE, key, descriptor.first()), binary(Op.GE, descriptor.last(), key)), present -> {
            present.assign(found, bool(true));
            // Range-only sets have no allocation; the bitmap pointer is also the representation tag.
            present.when(ne(descriptor.base(), nil()), bits -> {
                if (allowBloom) {
                    bits.when(binary(Op.LT, descriptor.count(), i64(-1)), bloom ->
                            bloom.assign(found, bloomContains(bloom, descriptor, key)));
                    bits.when(eq(descriptor.count(), i64(-1)), bitmap ->
                            bitmap.assign(found, bitmapContains(bitmap, descriptor, key)));
                } else bits.assign(found, bitmapContains(bits, descriptor, key));
            });
        });
        return found;
    }

    private static Expr bloomContains(Builder b, Descriptor descriptor, Expr key) {
        Var found = b.let(bool(true));
        bloomMasks(b, descriptor, key, (address, bits) ->
                b.assign(found, binary(Op.BOTH, found, eq(binary(Op.AND, load(address), bits), bits))));
        return found;
    }

    /** Key loads denote consecutive lanes; membership loads use explicit indexes. */
    static Var maskJoin(Builder b, Descriptor descriptor, Expr key, int lanes) {
        Expr inRange = binary(Op.BOTH, binary(Op.GE, key, descriptor.first()), binary(Op.GE, descriptor.last(), key));
        Var mask = b.let(new VectorMask(lanes, inRange));
        b.when(ne(mask, i64(0)), present -> present.when(ne(descriptor.base(), nil()), stored -> {
            stored.when(binary(Op.LT, descriptor.count(), i64(-1)), bloom -> {
                Expr hash = QuestMapProbe.hashLong(key);
                Expr block = bloomBlockBytes(descriptor, hash);
                Expr found = bool(true);
                for (int lane = 0; lane < 8; lane += 2) {
                    Expr bits = bloomMask(hash, lane);
                    Expr word = new Gather(MemoryType.I64, descriptor.base(), add(block, i64(lane * 4L)));
                    found = binary(Op.BOTH, found, eq(binary(Op.AND, word, bits), bits));
                }
                bloom.assign(mask, binary(Op.AND, mask, new VectorMask(lanes, found)));
            });
            stored.when(eq(descriptor.count(), i64(-1)), bitmap -> {
                // Vector predicates are eager: excluded keys must still read an allocated word.
                Expr bit = new Select(inRange, binary(Op.SUB, key, descriptor.first()), i64(0));
                Expr word = new Gather(MemoryType.I64, descriptor.base(), binary(Op.MUL, binary(Op.USHR, bit, i64(6)), i64(8)));
                Expr found = ne(binary(Op.AND, binary(Op.USHR, word, binary(Op.AND, bit, i64(63))), i64(1)), i64(0));
                bitmap.assign(mask, binary(Op.AND, mask, new VectorMask(lanes, found)));
            });
        }));
        return mask;
    }

    /** Owner-only insertion into an allocated bitmap or split-block Bloom filter. */
    static void insertJoin(Builder b, Descriptor descriptor, Expr key) {
        b.when(eq(descriptor.count(), i64(-1)), bitmap -> {
            Var bit = bitmap.let(binary(Op.SUB, key, descriptor.first()));
            Expr address = new Offset(descriptor.base(), binary(Op.MUL, binary(Op.USHR, bit, i64(6)), i64(8)));
            putBits(bitmap, address, binary(Op.USHR, i64(Long.MIN_VALUE), binary(Op.SUB, i64(63), binary(Op.AND, bit, i64(63)))));
        });
        b.when(binary(Op.LT, descriptor.count(), i64(-1)), bloom ->
                bloomMasks(bloom, descriptor, key, (address, bits) -> putBits(bloom, address, bits)));
    }

    private static void putBits(Builder b, Expr address, Expr bits) {
        Var old = b.let(load(address));
        // a | b = (a ^ b) ^ (a & b); LLVM folds the existing IR operations to OR.
        b.store(address, binary(Op.XOR, binary(Op.XOR, old, bits), binary(Op.AND, old, bits)));
    }

    private static void bloomMasks(Builder b, Descriptor descriptor, Expr key,
                                   java.util.function.BiConsumer<Expr, Expr> consumer) {
        Var hash = QuestMapProbe.direct(0, ColumnType.LONG, false).hash(b, key);
        Var block = b.let(new Offset(descriptor.base(), bloomBlockBytes(descriptor, hash)));
        for (int lane = 0; lane < 8; lane += 2) {
            Var bits = b.let(bloomMask(hash, lane));
            consumer.accept(at(block, lane * 4L), bits);
        }
    }

    private static Expr bloomBlockBytes(Descriptor descriptor, Expr hash) {
        Expr blocks = binary(Op.USHR, binary(Op.SUB, i64(0), descriptor.count()), i64(SplitBlockBloomFilter.BLOCK_SIZE_SHIFT));
        Expr index = binary(Op.USHR, binary(Op.MUL, binary(Op.USHR, hash, i64(32)), blocks), i64(32));
        return binary(Op.MUL, index, i64(SplitBlockBloomFilter.BLOCK_SIZE));
    }

    private static Expr bloomMask(Expr hash, int lane) {
        Expr mask = i64(0);
        for (int half = 0; half < 2; half++) {
            Expr product = binary(Op.AND, binary(Op.MUL, hash, i64(SplitBlockBloomFilter.getSalt(lane + half))), i64(0xffff_ffffL));
            Expr bit = add(binary(Op.USHR, product, i64(27)), i64(half * 32L));
            // The two lanes occupy disjoint 32-bit halves; the IR already has logical right shift.
            mask = binary(Op.XOR, mask, binary(Op.USHR, i64(Long.MIN_VALUE), binary(Op.SUB, i64(63), bit)));
        }
        return mask;
    }

    private static Expr bitmapContains(Builder b, Descriptor descriptor, Expr key) {
        Var bit = b.let(binary(Op.SUB, key, descriptor.first()));
        Var word = b.let(load(new Offset(descriptor.base(), binary(Op.MUL, binary(Op.USHR, bit, i64(6)), i64(8)))));
        return ne(binary(Op.AND, binary(Op.USHR, word, binary(Op.AND, bit, i64(63))), i64(1)), i64(0));
    }

    static Expr contains(Builder b, Descriptor descriptor, Expr key) {
        Var found = b.let(bool(false));
        Expr first = descriptor.first(), count = descriptor.count(), base = descriptor.base();
        b.when(binary(Op.BOTH, binary(Op.GE, key, first), binary(Op.GE, descriptor.last(), key)), present -> {
            present.when(eq(count, i64(0)), range -> range.assign(found, bool(true)));
            present.when(ne(count, i64(0)), lookup -> {
                lookup.when(eq(count, i64(-1)), bits -> bits.assign(found, bitmapContains(bits, descriptor, key)));
                lookup.when(ne(count, i64(-1)), sorted -> {
                    Var lo = sorted.let(i64(0)), hi = sorted.let(count);
                    sorted.loop(binary(Op.LT, lo, hi), search -> {
                        Var mid = search.let(binary(Op.USHR, binary(Op.ADD, lo, hi), i64(1)));
                        Var value = search.let(new Load(MemoryType.I32, new Offset(base, binary(Op.MUL, mid, i64(4)))));
                        search.when(binary(Op.LT, value, key), lower -> lower.assign(lo, binary(Op.ADD, mid, i64(1))));
                        search.when(binary(Op.GE, value, key), upper -> upper.assign(hi, mid));
                    });
                    sorted.when(binary(Op.LT, lo, count), valid -> valid.assign(found,
                            eq(new Load(MemoryType.I32, new Offset(base, binary(Op.MUL, lo, i64(4)))), key)));
                });
            });
        });
        return found;
    }

    @Override public void close() {
        memory.close();
        memory.setMemoryTracker(null);
        first = 0;
        last = -1;
        count = 0;
    }
}
