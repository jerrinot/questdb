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

/** Rosti KSUM/NSUM state; NaNs contribute zero, infinities retain vector semantics. */
record VectorCompensatedSum(Pipeline.Scalar argument, long offset, boolean neumaier) implements Update {
    @Override public List<StateCell> scalarState() {
        return List.of(new StateCell(MemoryType.F64, offset), new StateCell(MemoryType.F64, offset + 8),
                new StateCell(MemoryType.I64, offset + 16));
    }
    @Override public boolean requiresFirstUpdate() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        b.store(MemoryType.F64, at(destination, offset), f64(0));
        b.store(MemoryType.F64, at(destination, offset + 8), f64(0));
        b.store(at(destination, offset + 16), i64(0));
    }

    @Override public void emitFirst(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        Var value = b.let(argument.emit(frame.numeric(b)).data());
        Expr present = binary(Op.FEQ, value, value);
        b.store(MemoryType.F64, at(destination, offset), new Select(present, value, f64(0)));
        b.store(MemoryType.F64, at(destination, offset + 8), f64(0));
        b.store(at(destination, offset + 16), new Select(present, i64(1), i64(0)));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        Var value = b.let(argument.emit(frame.numeric(b)).data());
        Expr present = binary(Op.FEQ, value, value);
        Expr incoming = neumaier ? value : binary(Op.FSUB, value, new Load(MemoryType.F64, at(destination, offset + 8)));
        add(b, destination, new Select(present, incoming, f64(0)));
        b.store(at(destination, offset + 16), Ir.add(load(at(destination, offset + 16)), new Select(present, i64(1), i64(0))));
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        Expr incoming = new Load(MemoryType.F64, at(source, offset));
        if (!neumaier) incoming = binary(Op.FSUB, incoming, new Load(MemoryType.F64, at(source, offset + 8)));
        add(b, destination, incoming);
        b.store(at(destination, offset + 16), Ir.add(load(at(destination, offset + 16)), load(at(source, offset + 16))));
    }

    private void add(Builder b, Expr destination, Expr value) {
        Var incoming = b.let(value), sum = b.let(new Load(MemoryType.F64, at(destination, offset)));
        Var total = b.let(binary(Op.FADD, sum, incoming));
        Expr correction;
        if (neumaier) {
            Expr absoluteSum = new BitCast(binary(Op.AND, new BitCast(sum), i64(Long.MAX_VALUE)));
            // Rosti compares abs(sum) with the signed incoming value, unlike row NSUM.
            Expr delta = new Select(binary(Op.FGE, absoluteSum, incoming),
                    binary(Op.FADD, binary(Op.FSUB, sum, total), incoming),
                    binary(Op.FADD, binary(Op.FSUB, incoming, total), sum));
            correction = binary(Op.FADD, new Load(MemoryType.F64, at(destination, offset + 8)), delta);
        } else correction = binary(Op.FSUB, binary(Op.FSUB, total, sum), incoming);
        b.store(MemoryType.F64, at(destination, offset + 8), correction);
        b.store(MemoryType.F64, at(destination, offset), total);
    }
}
