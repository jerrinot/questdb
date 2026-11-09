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

import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.util.ArrayList;
import java.util.function.Supplier;

import static org.questdb.experimental.codegen.Ir.*;

/** Small proven INT result domains use the ordinary packed aggregate state in private arrays. */
final class DictionaryAggregate {
    private static final int HEADER_BYTES = 32;
    private final QuestFixedKey key;
    private final SymbolExpression dictionary;
    private final AggregateUpdates updates;
    private final long valueBytes;
    private final long stride;
    private final ArrayList<Slot> slots = new ArrayList<>();
    private final CompiledKernel scan;
    private final CompiledKernel merge;
    private final CompiledKernel export;
    private long entryCount;

    static DictionaryAggregate bind(ResolvedAggregateKernel.Prepared prepared) {
        var binding = prepared.binding;
        var probe = prepared.probe;
        if (prepared.localCapacity == 0 || binding.allocates || binding.valueBytes == 0
                || binding.updates.inputs().varcharColumns().length != 0 || probe == null
                || probe.fixedKey() == null || probe.fixedKey().expressions().size() != 1
                || probe.fixedKey().dictionaries().size() != 1) return null;
        return new DictionaryAggregate(prepared);
    }

    private DictionaryAggregate(ResolvedAggregateKernel.Prepared prepared) {
        key = prepared.probe.fixedKey();
        this.dictionary = key.dictionaries().getFirst();
        updates = prepared.binding.updates;
        valueBytes = prepared.binding.valueBytes;
        stride = (valueBytes + 7) & ~7L;
        scan = prepared.defer(this::scanProgram);
        merge = prepared.defer(this::mergeProgram);
        export = prepared.defer(this::exportProgram);
    }

    void prepare(long evaluationBudget) {
        entryCount = 0;
        if (dictionary.address() == 0) return;
        long entries = dictionary.range() + 1; // dedicated NULL slot, even for an all-NULL domain
        // Serial merge work must also be smaller than one scan slot's input work.
        if (entries >= evaluationBudget || entries > (CacheSizing.LOCAL_AGGREGATE_BYTES - HEADER_BYTES) / (stride + 1)) return;
        entryCount = entries;
        scan.prepare();
    }

    long entries() { return dictionary.address() == 0 ? 0 : entryCount; }

    Slot newSlot(KernelMemory memory, long scratchAddress) {
        Slot slot = new Slot(memory, scratchAddress);
        slots.add(slot);
        return slot;
    }

    void clear() {
        entryCount = 0;
        for (Slot slot : slots) slot.state = 0;
    }

    void finishScan(Supplier<Map> destination, Runnable checkpoint) {
        if (slots.isEmpty() || entries() == 0) return;
        Slot owner = slots.getFirst();
        long combined = 0;
        owner.memory.enterCheckpoint(checkpoint);
        try {
            for (Slot slot : slots) {
                if (slot.state == 0) continue;
                checkpoint.run();
                if (combined == 0) combined = slot.state;
                else merge(combined, slot.state, owner.scratchAddress);
            }
            if (combined != 0) export(combined, destination.get(), checkpoint);
        } finally { owner.memory.close(); }
    }

    private long allocate() {
        long entries = entries();
        if (entries == 0) throw new IllegalStateException("Dictionary array was not admitted");
        long state = KernelMemory.allocator().malloc(HEADER_BYTES + entries * (stride + 1));
        long seen = state + HEADER_BYTES + entries * stride;
        Unsafe.putLong(state, dictionary.minimum());
        Unsafe.putLong(state + 8, entries - 1);
        Unsafe.putLong(state + 16, seen);
        Vect.memset(seen, entries, 0);
        return state;
    }

    private void merge(long destination, long source, long args) {
        Unsafe.putLong(args, source + HEADER_BYTES);
        Unsafe.putLong(args + 8, Unsafe.getLong(source + 16));
        Unsafe.putLong(args + 16, Unsafe.getLong(destination + 16));
        if (merge.run(args, entries(), destination + HEADER_BYTES) != 0) {
            KernelMemory.checkFailure();
            throw new IllegalStateException("Dictionary array merge failed");
        }
    }

    private void export(long state, Map destination, Runnable checkpoint) {
        long seen = Unsafe.getLong(state + 16), entries = entries();
        for (long id = 0; id < entries; id++) {
            if ((id & 1023) == 0) checkpoint.run();
            if (Unsafe.getByte(seen + id) == 0) continue;
            var mapKey = destination.withKey();
            mapKey.putInt(id == entries - 1 ? Integer.MIN_VALUE : (int) (dictionary.minimum() + id));
            var value = mapKey.createValue();
            long source = state + HEADER_BYTES + id * stride;
            if (value.isNew()) Vect.memcpy(value.getAddress(0), source, valueBytes);
            else if (export.run(source, 1, value.getAddress(0)) != 0)
                throw new IllegalStateException("Dictionary array export failed");
        }
    }

    private Program scanProgram() {
        Builder b = new Builder();
        b.when(eq(ROWS, i64(0)), empty -> empty.exit(0));
        Var state = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var minimum = b.let(load(state));
        Var nullIndex = b.let(load(at(state, 8)));
        Var seen = b.let(new Load(MemoryType.ADDRESS, at(state, 16)));
        var addresses = updates.loadFrameInputs(b);
        Var row = b.let(i64(0));
        b.loop(binary(Op.LT, row, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, row, i64(65535)), i64(0)), Builder::poll);
            Var value = key.scalar(loop, row);
            Var id = loop.let(new Select(eq(value, i64(Integer.MIN_VALUE)), nullIndex,
                    binary(Op.SUB, value, minimum)));
            Var first = updates.firstRowFlag(loop);
            Var destination = DenseAggregate.entry(loop, updates, at(state, HEADER_BYTES), seen, id, stride, first);
            updates.emitFrame(loop, row, addresses, destination, nil(), first);
            loop.assign(row, add(row, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    private Program mergeProgram() { return DenseAggregate.merge(updates, stride); }

    private Program exportProgram() {
        Builder b = new Builder();
        updates.merge(b, COLUMNS, STATE);
        b.exit(0);
        return new Program(b.block());
    }

    /** Borrows the acquired slot's allocator bridge and phase scratch until that slot closes. */
    final class Slot {
        private final KernelMemory memory;
        private long scratchAddress;
        private long state;
        private long rows;

        private Slot(KernelMemory memory, long scratchAddress) {
            this.memory = memory;
            this.scratchAddress = scratchAddress;
        }

        void bindScratch(long address) { scratchAddress = address; }

        boolean update(long columns, long rowCount, long keyState, SqlExecutionCircuitBreaker breaker) {
            if (entries() == 0) return false;
            memory.enter(breaker);
            try {
                if (state == 0) state = allocate();
                Unsafe.putLong(columns, state);
                long status = scan.run(columns, rowCount, keyState);
                memory.throwIfFailed();
                if (status != 0) throw new IllegalStateException("Dictionary array scan failed: " + status);
                rows += rowCount;
                return true;
            } finally { memory.close(); }
        }

        long rows() { return rows; }

        void close() {
            state = 0;
            slots.remove(this);
        }
    }
}
