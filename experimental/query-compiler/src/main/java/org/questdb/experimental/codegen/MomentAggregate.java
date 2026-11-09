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

/** Ordinary Welford updates and Chan merges over (DOUBLE mean, DOUBLE M2, LONG count). */
record MomentAggregate(Pipeline.Scalar argument, long offset, long m2Offset, long countOffset,
                       boolean sample, boolean standardDeviation) implements Update {
    @Override public List<StateCell> scalarState() {
        return List.of(new StateCell(MemoryType.F64, offset), new StateCell(MemoryType.F64, m2Offset),
                new StateCell(MemoryType.I64, countOffset));
    }
    @Override public boolean requiresFirstRowInitialization() { return true; }
    @Override public ResultAccess resultAccess() { return ResultAccess.FINALIZED; }
    @Override public boolean finalizesResult() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        b.store(MemoryType.F64, at(destination, offset), f64(0));
        b.store(MemoryType.F64, at(destination, m2Offset), f64(0));
        b.store(at(destination, countOffset), i64(0));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        Expr meanAddress = at(destination, offset), m2Address = at(destination, m2Offset);
        Expr countAddress = at(destination, countOffset);
        Var count = b.let(load(countAddress));
        Var value = b.let(argument.emit(frame.numeric(b)).data());
        b.when(eq(Pipeline.nonFinite(value), bool(false)), finite -> {
            Var mean = finite.let(new Load(MemoryType.F64, meanAddress));
            Var sum = finite.let(new Load(MemoryType.F64, m2Address));
            Var nextCount = finite.let(add(count, i64(1)));
            Var delta = finite.let(binary(Op.FSUB, value, mean));
            Var nextMean = finite.let(binary(Op.FADD, mean, binary(Op.FDIV, delta, new ToDouble(nextCount))));
            finite.store(MemoryType.F64, meanAddress, nextMean);
            finite.store(MemoryType.F64, m2Address, binary(Op.FADD, sum,
                    binary(Op.FMUL, binary(Op.FSUB, value, nextMean), delta)));
            finite.store(countAddress, nextCount);
        });
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        Var sourceCount = b.let(load(at(source, countOffset)));
        b.when(ne(sourceCount, i64(0)), nonempty -> {
            Var destinationCount = nonempty.let(load(at(destination, countOffset)));
            Var sourceMean = nonempty.let(new Load(MemoryType.F64, at(source, offset)));
            Var sourceM2 = nonempty.let(new Load(MemoryType.F64, at(source, m2Offset)));
            nonempty.when(eq(destinationCount, i64(0)), empty -> {
                empty.store(MemoryType.F64, at(destination, offset), sourceMean);
                empty.store(MemoryType.F64, at(destination, m2Offset), sourceM2);
                empty.store(at(destination, countOffset), sourceCount);
            });
            nonempty.when(ne(destinationCount, i64(0)), combine -> {
                Var mean = combine.let(new Load(MemoryType.F64, at(destination, offset)));
                Var sum = combine.let(new Load(MemoryType.F64, at(destination, m2Offset)));
                Var count = combine.let(add(sourceCount, destinationCount));
                Var delta = combine.let(binary(Op.FSUB, mean, sourceMean));
                Expr mergedMean = binary(Op.FDIV, binary(Op.FADD,
                        binary(Op.FMUL, new ToDouble(sourceCount), sourceMean),
                        binary(Op.FMUL, new ToDouble(destinationCount), mean)), new ToDouble(count));
                // Preserve the ordinary implementation's LONG product before conversion.
                Expr weight = binary(Op.FDIV, new ToDouble(binary(Op.MUL, sourceCount, destinationCount)), new ToDouble(count));
                Expr mergedM2 = binary(Op.FADD, binary(Op.FADD, sourceM2, sum),
                        binary(Op.FMUL, binary(Op.FMUL, delta, delta), weight));
                combine.store(MemoryType.F64, at(destination, offset), mergedMean);
                combine.store(MemoryType.F64, at(destination, m2Offset), mergedM2);
                combine.store(at(destination, countOffset), count);
            });
        });
    }

    @Override public void finish(Builder b, Expr destination, boolean deferred) {
        Var count = b.let(load(at(destination, countOffset)));
        Expr denominator = sample ? binary(Op.SUB, count, i64(1)) : count;
        Var result = b.let(f64(Double.NaN));
        b.when(binary(Op.LT, i64(0), denominator), present -> {
            Expr variance = binary(Op.FDIV, new Load(MemoryType.F64, at(destination, m2Offset)), new ToDouble(denominator));
            present.assign(result, standardDeviation ? new Sqrt(variance) : variance);
        });
        b.store(MemoryType.F64, at(destination, offset), result);
    }
}
