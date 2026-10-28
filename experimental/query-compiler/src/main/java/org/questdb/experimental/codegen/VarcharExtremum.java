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

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_INLINED_PREFIX_MASK;
import static io.questdb.griffin.engine.groupby.GroupByUtf8Sink.*;
import static org.questdb.experimental.codegen.Ir.*;

/** MIN/MAX over GroupByUtf8Sink's allocator-owned layout and byte ordering. */
record VarcharExtremum(ResolvedVarchar.Expression argument, long offset, boolean minimum) implements AggregateUpdates.Update {
    @Override public boolean allocates() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        b.store(at(destination, offset), i64(0));
    }

    @Override public void prefetch(Builder b, java.util.function.Function<Pipeline.Scalar, Pipeline.Value> input, Expr destination) {
        Var pointer = b.let(new Load(MemoryType.ADDRESS, at(destination, offset)));
        b.when(ne(pointer, nil()), present -> present.prefetch(pointer, false));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        var value = frame.value(b, argument);
        b.when(eq(value.isNull(), bool(false)), present -> {
            Var pointer = present.let(new Load(MemoryType.ADDRESS, at(destination, offset)));
            Var replace = present.let(eq(pointer, nil()));
            present.when(ne(pointer, nil()), existing -> existing.assign(replace,
                    wins(ByteSequences.compare(existing, at(pointer, HEADER_SIZE), new Load(MemoryType.I32, at(pointer, SIZE_OFFSET)), prefix(pointer),
                            value.address(), value.size(), ByteSequences.sixPrefix(value.address(), value.size())))));
            present.when(replace, winner -> copy(winner, pointer, value, at(destination, offset)));
        });
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        Var from = b.let(new Load(MemoryType.ADDRESS, at(source, offset)));
        b.when(ne(from, nil()), present -> {
            Var to = present.let(new Load(MemoryType.ADDRESS, at(destination, offset)));
            Var replace = present.let(eq(to, nil()));
            present.when(ne(to, nil()), existing -> existing.assign(replace,
                    wins(ByteSequences.compare(existing, at(to, HEADER_SIZE), new Load(MemoryType.I32, at(to, SIZE_OFFSET)), prefix(to),
                            at(from, HEADER_SIZE), new Load(MemoryType.I32, at(from, SIZE_OFFSET)), prefix(from)))));
            // The factory retains source allocators through result consumption, as Java merge requires.
            present.when(replace, winner -> winner.store(MemoryType.ADDRESS, at(destination, offset), from));
        });
    }

    private Expr wins(Expr order) {
        return minimum ? binary(Op.LT, i64(0), order) : binary(Op.LT, order, i64(0));
    }

    private static Expr prefix(Expr pointer) {
        // Every sink has at least eight payload bytes, padded on creation and replacement.
        return binary(Op.AND, load(at(pointer, HEADER_SIZE)), i64(VARCHAR_INLINED_PREFIX_MASK));
    }

    private static void copy(Builder b, Var pointer, Pipeline.Utf8Value value, Expr destination) {
        Var capacity = b.let(i64(0));
        b.when(ne(pointer, nil()), allocated -> allocated.assign(capacity, new Load(MemoryType.I32, pointer)));
        b.when(binary(Op.EITHER, eq(pointer, nil()), binary(Op.LT, capacity, value.size())), grow -> {
            Var next = grow.let(new Select(binary(Op.LT, capacity, i64(MIN_CAPACITY)), i64(MIN_CAPACITY), capacity));
            grow.loop(binary(Op.LT, next, value.size()), doubling -> doubling.assign(next, binary(Op.MUL, next, i64(2))));
            Var replacement = grow.let(new Select(eq(pointer, nil()), new Allocate(add(next, i64(HEADER_SIZE))),
                    new Reallocate(pointer, add(capacity, i64(HEADER_SIZE)), add(next, i64(HEADER_SIZE)))));
            grow.when(eq(replacement, nil()), failed -> failed.exit(2));
            grow.assign(pointer, replacement);
            grow.store(MemoryType.I32, pointer, next);
        });
        b.store(at(pointer, HEADER_SIZE), i64(0));
        QuestMapMutation.copy(b, value.address(), at(pointer, HEADER_SIZE), value.size());
        b.store(MemoryType.I32, at(pointer, SIZE_OFFSET), value.size());
        b.store(MemoryType.I8, at(pointer, IS_ASCII_OFFSET), new Select(value.isAscii(), i64(1), i64(0)));
        b.store(MemoryType.ADDRESS, destination, pointer);
    }
}
