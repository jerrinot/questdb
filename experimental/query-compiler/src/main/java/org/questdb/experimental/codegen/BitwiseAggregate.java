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

import java.util.List;

import static org.questdb.experimental.codegen.AggregateUpdates.*;
import static org.questdb.experimental.codegen.Ir.*;

/** The ordinary one-cell bitwise state, including nullable INT/LONG results. */
record BitwiseAggregate(Pipeline.Scalar argument, long offset, int columnType, Op operation) implements Update {
    private MemoryType width() { return Pipeline.widthOf(columnType); }
    private boolean nullable() { return columnType == ColumnType.INT || columnType == ColumnType.LONG; }
    @Override public List<StateCell> scalarState() { return List.of(new StateCell(width(), offset)); }
    @Override public boolean requiresFirstRowInitialization() { return !nullable() && operation == Op.AND; }
    @Override public ResultAccess resultAccess() { return ResultAccess.DIRECT; }

    @Override public void initialize(Builder b, Expr destination) {
        // A new group has at least one input. Narrow AND's empty SQL result (0)
        // is not its reduction identity; scalar callers publish only nonempty states.
        long seed = nullable() ? Pipeline.nullValueOf(columnType) : operation == Op.AND ? -1 : 0;
        b.store(width(), at(destination, offset), i64(seed));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        var value = argument.emit(frame.numeric(b));
        if (nullable()) b.when(eq(value.isNull(), bool(false)), present -> combine(present, destination, value.data()));
        else combine(b, destination, value.data());
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        combine(b, destination, new Load(width(), at(source, offset)));
    }

    private void combine(Builder b, Expr destination, Expr value) {
        Var incoming = b.let(value);
        if (nullable()) {
            Expr nil = i64(Pipeline.nullValueOf(columnType));
            b.when(ne(incoming, nil), present -> {
                Var current = present.let(new Load(width(), at(destination, offset)));
                // A bitwise result can itself equal the null sentinel. The next
                // ordinary update replaces it, just as it replaces an empty state.
                present.store(width(), at(destination, offset), new Select(eq(current, nil), incoming,
                        binary(operation, current, incoming)));
            });
        } else {
            b.store(width(), at(destination, offset), binary(operation, new Load(width(), at(destination, offset)), incoming));
        }
    }
}
