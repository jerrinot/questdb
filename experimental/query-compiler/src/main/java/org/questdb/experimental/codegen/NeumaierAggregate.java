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

/** Ordinary NSUM over (DOUBLE sum, DOUBLE correction, LONG finite count). */
record NeumaierAggregate(Pipeline.Scalar argument, long offset, long compensationOffset, long countOffset) implements Update {
    @Override public List<StateCell> scalarState() {
        return List.of(new StateCell(MemoryType.F64, offset), new StateCell(MemoryType.F64, compensationOffset),
                new StateCell(MemoryType.I64, countOffset));
    }
    @Override public boolean requiresFirstRowInitialization() { return true; }
    @Override public ResultAccess resultAccess() { return ResultAccess.FINALIZED; }
    @Override public boolean finalizesResult() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        b.store(MemoryType.F64, at(destination, offset), f64(0));
        b.store(MemoryType.F64, at(destination, compensationOffset), f64(0));
        b.store(at(destination, countOffset), i64(0));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        Var value = b.let(argument.emit(frame.numeric(b)).data());
        b.when(eq(Pipeline.nonFinite(value), bool(false)), finite -> {
            add(finite, destination, value);
            finite.store(at(destination, countOffset), Ir.add(load(at(destination, countOffset)), i64(1)));
        });
    }

    private void add(Builder b, Expr destination, Expr value) {
        Var sum = b.let(new Load(MemoryType.F64, at(destination, offset)));
        Var total = b.let(binary(Op.FADD, sum, value));
        Expr absSum = new BitCast(binary(Op.AND, new BitCast(sum), i64(Long.MAX_VALUE)));
        Expr absValue = new BitCast(binary(Op.AND, new BitCast(value), i64(Long.MAX_VALUE)));
        Expr correction = new Select(binary(Op.FGE, absSum, absValue),
                binary(Op.FADD, binary(Op.FSUB, sum, total), value),
                binary(Op.FADD, binary(Op.FSUB, value, total), sum));
        b.store(MemoryType.F64, at(destination, compensationOffset),
                binary(Op.FADD, new Load(MemoryType.F64, at(destination, compensationOffset)), correction));
        b.store(MemoryType.F64, at(destination, offset), total);
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        Var count = b.let(load(at(source, countOffset)));
        b.when(binary(Op.LT, i64(0), count), nonempty -> {
            Var destinationCount = nonempty.let(load(at(destination, countOffset)));
            Var populated = nonempty.let(binary(Op.LT, i64(0), destinationCount));
            Var sum = nonempty.let(new Load(MemoryType.F64, at(source, offset)));
            nonempty.when(populated, existing -> {
                // Ordinary NSUM merges the source sum only; its correction is copied
                // only when adopting a source into an empty destination.
                add(existing, destination, sum);
                existing.store(at(destination, countOffset), Ir.add(destinationCount, count));
            });
            nonempty.when(eq(populated, bool(false)), empty -> {
                empty.store(MemoryType.F64, at(destination, offset), sum);
                empty.store(MemoryType.F64, at(destination, compensationOffset),
                        new Load(MemoryType.F64, at(source, compensationOffset)));
                empty.store(at(destination, countOffset), count);
            });
        });
    }

    @Override public void finish(Builder b, Expr destination, boolean deferred) {
        Expr result = binary(Op.FADD, new Load(MemoryType.F64, at(destination, offset)),
                new Load(MemoryType.F64, at(destination, compensationOffset)));
        b.store(MemoryType.F64, at(destination, offset), new Select(
                binary(Op.LT, i64(0), load(at(destination, countOffset))), result, f64(Double.NaN)));
    }
}
