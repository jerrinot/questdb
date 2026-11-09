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

/** Ordinary weighted average over (sum of products, sum of weights). */
record WeightedAverageAggregate(Pipeline.Scalar sample, Pipeline.Scalar weight, long offset, long weightOffset) implements Update {
    @Override public List<StateCell> scalarState() {
        return List.of(new StateCell(MemoryType.F64, offset), new StateCell(MemoryType.F64, weightOffset));
    }
    @Override public boolean requiresFirstRowInitialization() { return true; }
    @Override public boolean requiresFirstUpdate() { return true; }
    @Override public ResultAccess resultAccess() { return ResultAccess.FINALIZED; }
    @Override public boolean finalizesResult() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        b.store(MemoryType.F64, at(destination, offset), f64(Double.NaN));
        b.store(MemoryType.F64, at(destination, weightOffset), f64(0));
    }

    @Override public void emitFirst(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        emit(b, frame, destination, true);
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        emit(b, frame, destination, false);
    }

    private void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, boolean first) {
        var row = frame.numeric(b);
        Var value = b.let(sample.emit(row).data()), w = b.let(weight.emit(row).data());
        Expr finite = eq(binary(Op.EITHER, Pipeline.nonFinite(value), Pipeline.nonFinite(w)), bool(false));
        Expr valid = binary(Op.BOTH, finite, binary(Op.FNE, w, f64(0)));
        Expr product = binary(Op.FMUL, value, w);
        if (first) {
            b.store(MemoryType.F64, at(destination, offset), new Select(valid, product, f64(0)));
            b.store(MemoryType.F64, at(destination, weightOffset), new Select(valid, w, f64(0)));
        } else b.when(valid, next -> {
            next.store(MemoryType.F64, at(destination, offset), binary(Op.FADD, new Load(MemoryType.F64, at(destination, offset)), product));
            next.store(MemoryType.F64, at(destination, weightOffset), binary(Op.FADD, new Load(MemoryType.F64, at(destination, weightOffset)), w));
        });
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        for (long cell : new long[]{offset, weightOffset})
            b.store(MemoryType.F64, at(destination, cell), binary(Op.FADD,
                    new Load(MemoryType.F64, at(destination, cell)), new Load(MemoryType.F64, at(source, cell))));
    }

    @Override public void finish(Builder b, Expr destination, boolean deferred) {
        b.store(MemoryType.F64, at(destination, offset), binary(Op.FDIV,
                new Load(MemoryType.F64, at(destination, offset)), new Load(MemoryType.F64, at(destination, weightOffset))));
    }
}
