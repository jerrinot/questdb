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

import static org.questdb.experimental.codegen.Ir.*;

/** Large inner sets finish after outer-map merging, using the existing query workers.
 * Only control descriptors are linked during merge; source allocators outlive this phase.
 * count=-1 is transient and must never reach output or an ordinary aggregate updater.
 */
final class DeferredDistinct {
    static final int CONTROL = -16;
    static final int PARTS = 8;
    static final int CONTEXT_BYTES = 112;
    static final long PENDING = -1;
    // Context: next, inputs, count upper bound, largest set, largest count, unused,
    // target, initial target count, padded task counts, partition shift, serial count/payload, serial flag.
    private DeferredDistinct() { }

    static void append(Builder b, Expr source, Expr destination, Expr control) {
        Var context = b.let(nil());
        b.when(eq(load(destination), i64(PENDING)), pending ->
                pending.assign(context, pointer(at(destination, 8))));
        b.when(eq(context, nil()), first -> {
            first.assign(context, allocation(first, i64(CONTEXT_BYTES)));
            for (int i = 0; i < CONTEXT_BYTES; i += 8) first.store(at(context, i), i64(0));
            first.store(MemoryType.ADDRESS, context, pointer(at(control, 8)));
            first.store(MemoryType.ADDRESS, at(control, 8), context);
            appendInput(first, context, load(destination), load(at(destination, 8)));
            first.store(destination, i64(PENDING));
            first.store(MemoryType.ADDRESS, at(destination, 8), context);
        });
        appendInput(b, context, load(source), load(at(source, 8)));
    }

    private static void appendInput(Builder b, Expr context, Expr count, Expr payload) {
        Var node = allocation(b, i64(24));
        b.store(MemoryType.ADDRESS, node, pointer(at(context, 8)));
        b.store(at(node, 8), count);
        b.store(at(node, 16), payload);
        b.store(MemoryType.ADDRESS, at(context, 8), node);
        b.store(at(context, 16), add(load(at(context, 16)), count));
        b.when(binary(Op.LT, load(at(context, 32)), count), largest -> {
            largest.store(at(context, 24), payload);
            largest.store(at(context, 32), count);
        });
    }

    /** One worker allocates each target. A separate drained phase initializes its ranges. */
    static Program prepare() {
        Builder b = new Builder();
        Var total = b.let(load(at(COLUMNS, 16)));
        Var largest = b.let(pointer(at(COLUMNS, 24)));
        Var capacity = b.let(new Load(MemoryType.I32, largest));
        Var limit = b.let(new Load(MemoryType.I32, at(largest, 8)));
        Var oldCapacity = b.let(capacity);
        Var serial = b.let(eq(ROWS, i64(1)));
        b.store(at(COLUMNS, 72), new TrailingZeros(ROWS));
        b.loop(binary(Op.BOTH, eq(serial, bool(false)), binary(Op.GE, total, limit)), grow -> {
            grow.when(binary(Op.GE, capacity, i64(1L << 25)), overflow -> overflow.assign(serial, bool(true)));
            grow.when(eq(serial, bool(false)), valid -> {
                valid.assign(capacity, binary(Op.MUL, capacity, i64(2)));
                valid.assign(limit, binary(Op.MUL, limit, i64(2)));
            });
        });
        b.store(at(COLUMNS, 96), new Select(serial, i64(1), i64(0)));
        b.when(serial, fallback -> fallback.exit(0));
        Var target = b.let(largest);
        b.store(at(COLUMNS, 56), load(at(COLUMNS, 32)));
        b.when(ne(capacity, oldCapacity), fresh -> {
            fresh.assign(target, allocation(fresh, add(i64(16), binary(Op.MUL, capacity, i64(8)))));
            fresh.store(MemoryType.I32, target, capacity);
            fresh.store(MemoryType.I32, at(target, 4), i64(0));
            fresh.store(MemoryType.I32, at(target, 8), limit);
            fresh.store(at(COLUMNS, 56), i64(0));
        });
        b.store(MemoryType.ADDRESS, at(COLUMNS, 48), target);
        Var counts = allocation(b, i64(PARTS * 64));
        for (int i = 0; i < PARTS; i++) b.store(at(counts, i * 64L), i64(0));
        b.store(MemoryType.ADDRESS, at(COLUMNS, 64), counts);
        b.exit(0);
        return new Program(b.block());
    }

    static Program zero() {
        Builder b = new Builder();
        Var target = b.let(pointer(at(COLUMNS, 48)));
        Var capacity = b.let(new Load(MemoryType.I32, target));
        Var shift = b.let(load(at(COLUMNS, 72)));
        Var index = b.let(binary(Op.USHR, binary(Op.MUL, capacity, ROWS), shift));
        Var end = b.let(binary(Op.USHR, binary(Op.MUL, capacity, add(ROWS, i64(1))), shift));
        b.loop(binary(Op.LT, index, end), chunk -> {
            chunk.poll();
            Var limit = chunk.let(new Select(binary(Op.LT, add(index, i64(1024)), end), add(index, i64(1024)), end));
            chunk.loop(binary(Op.LT, index, limit), fill -> {
                fill.store(slot(target, index), i64(Long.MIN_VALUE));
                fill.assign(index, add(index, i64(1)));
            });
        });
        b.exit(0);
        return new Program(b.block());
    }

    /** One independently scheduled range. Relaxed atomics suffice: task publication/join orders phases. */
    static Program union() {
        Builder b = new Builder();
        b.when(ne(load(at(COLUMNS, 96)), i64(0)), serial -> {
            Var node = serial.let(pointer(at(COLUMNS, 8)));
            serial.loop(ne(node, nil()), next -> {
                next.poll();
                // Every deferred input is an ordinary count/payload pair, never another context.
                NestedDistinct.merge(next, at(node, 8), at(COLUMNS, 80), 16, .5);
                next.assign(node, pointer(node));
            });
            serial.exit(0);
        });
        Var target = b.let(pointer(at(COLUMNS, 48)));
        Var capacity = b.let(new Load(MemoryType.I32, target));
        Var mask = b.let(binary(Op.SUB, capacity, i64(1)));
        Var added = b.let(i64(0));
        Var node = b.let(pointer(at(COLUMNS, 8)));
        b.loop(ne(node, nil()), input -> {
            input.poll();
            Var count = input.let(load(at(node, 8)));
            input.when(binary(Op.BOTH, eq(count, i64(1)), eq(ROWS, i64(0))), one ->
                    insert(one, target, mask, capacity, load(at(node, 16)), added));
            input.when(binary(Op.GE, count, i64(2)), set -> {
                Var source = set.let(pointer(at(node, 16)));
                // A reused largest input already occupies the destination; other inputs are immutable.
                set.when(ne(source, target), distinct -> {
                    Var sourceCapacity = distinct.let(new Load(MemoryType.I32, source));
                    Var index = distinct.let(binary(Op.USHR, binary(Op.MUL, sourceCapacity, ROWS), load(at(COLUMNS, 72))));
                    Var end = distinct.let(binary(Op.USHR, binary(Op.MUL, sourceCapacity, add(ROWS, i64(1))), load(at(COLUMNS, 72))));
                    distinct.loop(binary(Op.LT, index, end), scan -> {
                        scan.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                        Var key = scan.let(load(slot(source, index)));
                        scan.when(ne(key, i64(Long.MIN_VALUE)), present -> insert(present, target, mask, capacity, key, added));
                        scan.assign(index, add(index, i64(1)));
                    });
                });
            });
            input.assign(node, pointer(node));
        });
        b.store(new Offset(pointer(at(COLUMNS, 64)), binary(Op.MUL, ROWS, i64(64))), added);
        b.exit(0);
        return new Program(b.block());
    }

    private static void insert(Builder b, Expr target, Expr mask, Expr capacity, Expr key, Var added) {
        Var hash = b.let(binary(Op.XOR, key, binary(Op.USHR, key, i64(37))));
        b.assign(hash, binary(Op.MUL, hash, i64(0x165667919E3779F9L)));
        b.assign(hash, binary(Op.XOR, hash, binary(Op.USHR, hash, i64(32))));
        Var index = b.let(binary(Op.AND, hash, mask));
        Var done = b.let(bool(false));
        Var probes = b.let(i64(0));
        b.loop(eq(done, bool(false)), probe -> {
            Var address = probe.let(slot(target, index));
            Var value = probe.let(new AtomicLoad(address));
            probe.when(eq(value, i64(Long.MIN_VALUE)), empty -> {
                empty.assign(value, new CompareExchange(address, i64(Long.MIN_VALUE), key));
                empty.when(eq(value, i64(Long.MIN_VALUE)), inserted -> {
                    inserted.assign(added, add(added, i64(1)));
                    inserted.assign(done, bool(true));
                });
            });
            probe.when(eq(value, key), duplicate -> duplicate.assign(done, bool(true)));
            probe.when(eq(done, bool(false)), collision -> {
                collision.assign(probes, add(probes, i64(1)));
                collision.when(eq(binary(Op.AND, probes, i64(1023)), i64(0)), Builder::poll);
                collision.when(binary(Op.GE, probes, capacity), corrupt -> corrupt.exit(3));
                collision.assign(index, binary(Op.AND, add(index, i64(1)), mask));
            });
        });
    }

    static void finish(Builder b, Expr destination) {
        b.when(eq(load(destination), i64(PENDING)), pending -> {
            Var context = pending.let(pointer(at(destination, 8)));
            pending.when(ne(load(at(context, 96)), i64(0)), serial -> {
                serial.store(destination, load(at(context, 80)));
                serial.store(at(destination, 8), load(at(context, 88)));
            });
            pending.when(eq(load(at(context, 96)), i64(0)), parallel -> {
                Var count = parallel.let(load(at(context, 56)));
                Var counts = parallel.let(pointer(at(context, 64)));
                for (int i = 0; i < PARTS; i++) parallel.assign(count, add(count, load(at(counts, i * 64L))));
                Var target = parallel.let(pointer(at(context, 48)));
                parallel.store(MemoryType.I32, at(target, 4), count);
                parallel.store(destination, count);
                parallel.store(MemoryType.ADDRESS, at(destination, 8), target);
            });
        });
    }

    private static Var allocation(Builder b, Expr bytes) {
        Var address = b.let(new Allocate(bytes));
        b.when(eq(address, nil()), failed -> failed.exit(2));
        return address;
    }
    private static Expr pointer(Expr address) { return new Load(MemoryType.ADDRESS, address); }
    private static Expr slot(Expr set, Expr index) { return new Offset(at(set, 16), binary(Op.MUL, index, i64(8))); }
}
