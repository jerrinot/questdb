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

import io.questdb.std.Numbers;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.questdb.experimental.codegen.Ir.*;

/** The ordinary INT/LONG DISTINCT set layouts, including inline values and pointer adoption. */
final class NestedDistinct {
    static final NestedDistinct LONG = new NestedDistinct(MemoryType.I64, Long.MIN_VALUE);
    static final NestedDistinct INT = new NestedDistinct(MemoryType.I32, Integer.MIN_VALUE);
    static final NestedDistinct IPV4 = new NestedDistinct(MemoryType.I32, Numbers.IPv4_NULL);
    private final MemoryType width;
    private final long noKeyValue;
    final long payloadOffset;

    private NestedDistinct(MemoryType width, long noKeyValue) {
        this(width, noKeyValue, 8);
    }

    private NestedDistinct(MemoryType width, long noKeyValue, long payloadOffset) {
        this.width = width;
        this.noKeyValue = noKeyValue;
        this.payloadOffset = payloadOffset;
    }

    NestedDistinct withPayloadOffset(long offset) {
        if (offset < 8) throw new IllegalArgumentException("Overlapping DISTINCT state cells");
        return offset == payloadOffset ? this : new NestedDistinct(width, noKeyValue, offset);
    }

    static int initialCapacity(int requested, double loadFactor) {
        if (requested < 1 || !Double.isFinite(loadFactor) || loadFactor <= 0 || loadFactor >= 1
                || Math.max(requested, 2) / loadFactor > (1L << 30)) {
            throw new IllegalArgumentException("Invalid distinct set sizing");
        }
        return Numbers.ceilPow2((int) (Math.max(requested, 2) / loadFactor));
    }

    void emit(Builder builder, Pipeline.Value value, Expr countAddress, int requested, double loadFactor) {
        emitWithCapacity(builder, value, countAddress, initialCapacity(requested, loadFactor), loadFactor);
    }

    // The resolved function already owns the adjusted capacity; do not round it a second time.
    void emitWithCapacity(Builder builder, Pipeline.Value value, Expr countAddress, int capacity, double loadFactor) {
        if (capacity < 2 || capacity > (1 << 30) || (capacity & (capacity - 1)) != 0
                || !Double.isFinite(loadFactor) || loadFactor <= 0 || loadFactor >= 1) {
            throw new IllegalArgumentException("Invalid resolved distinct set sizing");
        }
        Expr payload = at(countAddress, payloadOffset);
        builder.when(eq(value.isNull(), bool(false)), present -> {
            Var count = present.let(load(countAddress));
            Var key = present.let(value.data());
            present.when(eq(count, i64(0)), first -> {
                first.store(countAddress, i64(1));
                first.store(payload, key);
            });
            present.when(eq(count, i64(1)), second -> {
                Var previous = second.let(load(payload));
                second.when(ne(key, previous), different -> {
                    Var set = allocate(different, i64(capacity), i64((int) (capacity * loadFactor)));
                    // Match QuestDB's insertion order, including growth on the second add.
                    addKey(different, set, key);
                    addKey(different, set, previous);
                    different.store(countAddress, i64(2));
                    different.store(MemoryType.ADDRESS, payload, set);
                });
            });
            present.when(binary(Op.GE, count, i64(2)), existing -> {
                Var set = existing.let(new Load(MemoryType.ADDRESS, payload));
                addKey(existing, set, key, inserted -> {
                    inserted.store(countAddress, add(count, i64(1)));
                    inserted.store(MemoryType.ADDRESS, payload, set);
                });
            });
        });
    }

    /** Hint the first set slot only; the real update always reloads state after any intervening growth. */
    void prefetch(Builder b, Pipeline.Value value, Expr countAddress) {
        b.when(binary(Op.BOTH, eq(value.isNull(), bool(false)), binary(Op.GE, load(countAddress), i64(2))), existing -> {
            Var set = existing.let(new Load(MemoryType.ADDRESS, at(countAddress, payloadOffset)));
            Var capacity = existing.let(intAt(set, 0));
            Var hash = hash(existing, value.data());
            Var index = existing.let(binary(Op.AND, hash, binary(Op.SUB, capacity, i64(1))));
            existing.prefetch(slot(set, index), false);
        });
    }

    /** Visit the non-NULL values of an ordinary state without changing it.
     * Source state and its allocator must remain alive and immutable during the visit.
     */
    void forEach(Builder b, Expr countAddress, BiConsumer<Builder, Expr> consumer) {
        Var count = b.let(load(countAddress));
        b.when(binary(Op.BOTH, binary(Op.LT, count, i64(0)), ne(count, i64(Long.MIN_VALUE))),
                unfinished -> unfinished.exit(3));
        b.when(eq(count, i64(1)), singleton -> consumer.accept(singleton, load(at(countAddress, payloadOffset))));
        b.when(binary(Op.GE, count, i64(2)), multiple -> {
            Var set = multiple.let(new Load(MemoryType.ADDRESS, at(countAddress, payloadOffset)));
            forEachSet(multiple, set, consumer);
        });
    }

    private Var allocate(Builder builder, Expr capacity, Expr limit) {
        Var set = builder.let(new Allocate(bytes(capacity)));
        builder.when(eq(set, nil()), failed -> failed.exit(2));
        builder.store(MemoryType.I32, set, capacity);
        builder.store(MemoryType.I32, at(set, 4), i64(0));
        builder.store(MemoryType.I32, at(set, 8), limit);
        Var index = builder.let(i64(0));
        builder.loop(binary(Op.LT, index, capacity), zero -> {
            zero.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            zero.store(width, slot(set, index), i64(noKeyValue));
            zero.assign(index, add(index, i64(1)));
        });
        return set;
    }

    /** Ordinary DISTINCT merge, including pointer adoption and size-directed merge.
     * All source allocators must outlive the destination, exactly as in AsyncGroupByAtom.
     */
    void merge(Builder builder, Expr source, Expr destination, int requested, double loadFactor) {
        mergeWithCapacity(builder, source, destination, initialCapacity(requested, loadFactor), loadFactor);
    }

    void mergeWithCapacity(Builder builder, Expr source, Expr destination, int capacity, double loadFactor) {
        Var srcCount = builder.let(load(source));
        builder.when(binary(Op.BOTH, ne(srcCount, i64(0)), ne(srcCount, i64(Long.MIN_VALUE))), valid -> {
            Var dstCount = valid.let(load(destination));
            Var empty = valid.let(binary(Op.EITHER, eq(dstCount, i64(0)), eq(dstCount, i64(Long.MIN_VALUE))));
            valid.when(empty, adopt -> {
                adopt.store(destination, srcCount);
                adopt.store(at(destination, payloadOffset), load(at(source, payloadOffset)));
            });
            valid.when(eq(empty, bool(false)), occupied -> {
                occupied.when(eq(srcCount, i64(1)), singleton ->
                        emitWithCapacity(singleton, new Pipeline.Value(load(at(source, payloadOffset)), bool(false)), destination, capacity, loadFactor));
                occupied.when(ne(srcCount, i64(1)), multiple -> {
                    Var srcSet = multiple.let(new Load(MemoryType.ADDRESS, at(source, payloadOffset)));
                    multiple.when(eq(dstCount, i64(1)), singleton -> {
                        addKey(singleton, srcSet, load(at(destination, payloadOffset)));
                        writeSet(singleton, destination, srcSet);
                    });
                    multiple.when(ne(dstCount, i64(1)), sets -> {
                        Var dstSet = sets.let(new Load(MemoryType.ADDRESS, at(destination, payloadOffset)));
                        Var intoDestination = sets.let(binary(Op.LT, binary(Op.USHR, intAt(srcSet, 4), i64(1)), intAt(dstSet, 4)));
                        // Select ownership before lowering the union, so its probe/grow loops
                        // occur once in the generated program. Growth updates the chosen target.
                        Var target = sets.let(new Select(intoDestination, dstSet, srcSet));
                        Var input = sets.let(new Select(intoDestination, srcSet, dstSet));
                        mergeSets(sets, target, input);
                        writeSet(sets, destination, target);
                    });
                });
            });
        });
    }

    void mergeWithCapacity(Builder b, Expr source, Expr destination, int capacity, double loadFactor, Expr control) {
        if (this != LONG) throw new IllegalArgumentException("Deferred union requires LONG sets");
        Var srcCount = b.let(load(source)), dstCount = b.let(load(destination));
        Var valid = b.let(binary(Op.BOTH, ne(srcCount, i64(0)), ne(srcCount, i64(Long.MIN_VALUE))));
        Var large = b.let(binary(Op.BOTH, binary(Op.GE, srcCount, i64(32768)), binary(Op.GE, dstCount, i64(131072))));
        Var defer = b.let(binary(Op.BOTH, ne(load(control), i64(0)), binary(Op.BOTH, valid,
                binary(Op.EITHER, eq(dstCount, i64(DeferredDistinct.PENDING)), large))));
        b.when(defer, pending -> DeferredDistinct.append(pending, source, destination, control));
        b.when(eq(defer, bool(false)), ordinary -> mergeWithCapacity(ordinary, source, destination, capacity, loadFactor));
    }

    private void writeSet(Builder b, Expr destination, Var set) {
        b.store(destination, intAt(set, 4));
        b.store(MemoryType.ADDRESS, at(destination, payloadOffset), set);
    }

    /** Slot order and addAt growth rules are preserved. */
    private void mergeSets(Builder b, Var destination, Expr source) {
        forEachSet(b, source, (present, value) -> addKey(present, destination, value));
    }

    private void forEachSet(Builder b, Expr source, BiConsumer<Builder, Expr> consumer) {
        Var capacity = b.let(intAt(source, 0));
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, capacity), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var value = loop.let(new Load(width, slot(source, index)));
            loop.when(ne(value, i64(noKeyValue)), present -> consumer.accept(present, value));
            loop.assign(index, add(index, i64(1)));
        });
    }

    private void addKey(Builder builder, Var set, Expr key) {
        addKey(builder, set, key, inserted -> { });
    }

    private void addKey(Builder builder, Var set, Expr key, Consumer<Builder> onInsert) {
        Var address = find(builder, set, key);
        builder.when(eq(new Load(width, address), i64(noKeyValue)), insert -> {
            insert.store(width, address, key);
            Var size = insert.let(add(intAt(set, 4), i64(1)));
            insert.store(MemoryType.I32, at(set, 4), size);
            insert.when(binary(Op.GE, size, intAt(set, 8)), grow -> rehash(grow, set));
            onInsert.accept(insert);
        });
    }

    private Var find(Builder builder, Expr set, Expr key) {
        Var capacity = builder.let(intAt(set, 0));
        Var mask = builder.let(binary(Op.SUB, capacity, i64(1)));
        Var hash = hash(builder, key);
        Var index = builder.let(binary(Op.AND, hash, mask));
        Var address = builder.let(slot(set, index));
        Var probes = builder.let(i64(0));
        builder.loop(binary(Op.BOTH, ne(new Load(width, address), i64(noKeyValue)), ne(new Load(width, address), key)), probe -> {
            probe.assign(probes, add(probes, i64(1)));
            probe.when(eq(binary(Op.AND, probes, i64(1023)), i64(0)), Builder::poll);
            probe.when(binary(Op.GE, probes, capacity), corrupt -> corrupt.exit(3));
            probe.assign(index, binary(Op.AND, add(index, i64(1)), mask));
            probe.assign(address, slot(set, index));
        });
        return address;
    }

    private Var hash(Builder builder, Expr key) {
        // Hash.hashInt64 zero-extends before the same avalanche used by hashLong64.
        if (width == MemoryType.I32) key = binary(Op.AND, key, i64(0xffffffffL));
        Var hash = builder.let(binary(Op.XOR, key, binary(Op.USHR, key, i64(37))));
        builder.assign(hash, binary(Op.MUL, hash, i64(0x165667919E3779F9L)));
        builder.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(32))));
        return hash;
    }

    private void rehash(Builder builder, Var set) {
        Var oldCapacity = builder.let(intAt(set, 0));
        builder.when(binary(Op.GE, oldCapacity, i64(1L << 30)), overflow -> overflow.exit(3));
        Var capacity = builder.let(binary(Op.MUL, oldCapacity, i64(2)));
        Var limit = builder.let(binary(Op.MUL, intAt(set, 8), i64(2)));
        Var grown = allocate(builder, capacity, limit);
        builder.store(MemoryType.I32, at(grown, 4), intAt(set, 4));
        Var index = builder.let(i64(0));
        builder.loop(binary(Op.LT, index, oldCapacity), copy -> {
            copy.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var key = copy.let(new Load(width, slot(set, index)));
            copy.when(ne(key, i64(noKeyValue)), occupied -> {
                Var address = find(occupied, grown, key);
                occupied.store(width, address, key);
            });
            copy.assign(index, add(index, i64(1)));
        });
        builder.free(set, bytes(oldCapacity));
        builder.assign(set, grown);
    }

    private Expr bytes(Expr capacity) { return add(i64(width == MemoryType.I32 ? 12 : 16), binary(Op.MUL, capacity, i64(width.bytes))); }
    private static Expr intAt(Expr address, long offset) { return new Load(MemoryType.I32, at(address, offset)); }
    private Expr slot(Expr set, Expr index) { return new Offset(set, bytes(index)); }
}
