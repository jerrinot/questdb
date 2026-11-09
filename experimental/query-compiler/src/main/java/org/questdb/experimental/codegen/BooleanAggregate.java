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

import java.util.List;

import static org.questdb.experimental.codegen.AggregateUpdates.*;
import static org.questdb.experimental.codegen.Ir.*;

/** Ordinary BOOLEAN result and LONG count, including false for an empty AND. */
record BooleanAggregate(Pipeline.Scalar argument, long offset, long countOffset, boolean and) implements Update {
    @Override public List<StateCell> scalarState() {
        return List.of(new StateCell(MemoryType.I8, offset), new StateCell(MemoryType.I64, countOffset));
    }
    @Override public ResultAccess resultAccess() { return ResultAccess.FINALIZED; }
    @Override public boolean finalizesResult() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        b.store(MemoryType.I8, at(destination, offset), i64(and ? 1 : 0));
        b.store(at(destination, countOffset), i64(0));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        Expr result = at(destination, offset), count = at(destination, countOffset);
        // Preserve ordinary short-circuiting; count still advances for every row.
        b.when(eq(new Load(MemoryType.I8, result), i64(and ? 1 : 0)), update ->
                update.store(MemoryType.I8, result, argument.emit(frame.numeric(update)).data()));
        b.store(count, add(load(count), i64(1)));
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        Var sourceCount = b.let(load(at(source, countOffset)));
        b.when(ne(sourceCount, i64(0)), nonempty -> {
            Var destinationCount = nonempty.let(load(at(destination, countOffset)));
            Var sourceValue = nonempty.let(new Load(MemoryType.I8, at(source, offset)));
            nonempty.when(eq(destinationCount, i64(0)), copy -> {
                copy.store(MemoryType.I8, at(destination, offset), sourceValue);
                copy.store(at(destination, countOffset), sourceCount);
            });
            nonempty.when(ne(destinationCount, i64(0)), combine -> {
                Expr current = new Load(MemoryType.I8, at(destination, offset));
                Expr value = new Select(binary(and ? Op.BOTH : Op.EITHER, ne(current, i64(0)), ne(sourceValue, i64(0))), i64(1), i64(0));
                combine.store(MemoryType.I8, at(destination, offset), value);
                combine.store(at(destination, countOffset), add(destinationCount, sourceCount));
            });
        });
    }

    @Override public void finish(Builder b, Expr destination, boolean deferred) {
        b.when(eq(load(at(destination, countOffset)), i64(0)), empty ->
                empty.store(MemoryType.I8, at(destination, offset), i64(0)));
    }
}
