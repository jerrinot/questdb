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

/** Shared array-state initialization and merge; each caller retains its aggregate ABI. */
final class DenseAggregate {
    static Var entry(Builder b, AggregateUpdates updates, Expr state, Expr seen, Expr id, long stride) {
        Var destination = b.let(new Offset(state, binary(Op.MUL, id, i64(stride))));
        Expr visited = new Offset(seen, id);
        b.when(eq(new Load(MemoryType.I8, visited), i64(0)), first -> {
            updates.initialize(first, destination);
            first.store(MemoryType.I8, visited, i64(1));
        });
        return destination;
    }

    static Program merge(AggregateUpdates updates, long stride) {
        Builder b = new Builder();
        b.when(eq(ROWS, i64(0)), empty -> empty.exit(0));
        Var source = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var sourceSeen = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var destinationSeen = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 16)));
        Var id = b.let(i64(0));
        b.loop(binary(Op.LT, id, ROWS), loop -> {
            loop.when(ne(new Load(MemoryType.I8, new Offset(sourceSeen, id)), i64(0)), present -> {
                Var destination = entry(present, updates, STATE, destinationSeen, id, stride);
                updates.merge(present, new Offset(source, binary(Op.MUL, id, i64(stride))), destination);
            });
            loop.assign(id, add(id, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }
}
