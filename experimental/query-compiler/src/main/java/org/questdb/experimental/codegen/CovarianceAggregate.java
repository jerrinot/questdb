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

/** Ordinary pairwise Welford updates over (meanY, meanX, sumXY, count). */
record CovarianceAggregate(Pipeline.Scalar y, Pipeline.Scalar x, long offset, long meanXOffset,
                           long sumOffset, long countOffset, boolean sample) implements Update {
    @Override public List<StateCell> scalarState() {
        return List.of(new StateCell(MemoryType.F64, offset), new StateCell(MemoryType.F64, meanXOffset),
                new StateCell(MemoryType.F64, sumOffset), new StateCell(MemoryType.I64, countOffset));
    }
    @Override public boolean requiresFirstRowInitialization() { return true; }
    @Override public ResultAccess resultAccess() { return ResultAccess.FINALIZED; }
    @Override public boolean finalizesResult() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        b.store(MemoryType.F64, at(destination, offset), f64(0));
        b.store(MemoryType.F64, at(destination, meanXOffset), f64(0));
        b.store(MemoryType.F64, at(destination, sumOffset), f64(0));
        b.store(at(destination, countOffset), i64(0));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        Var count = b.let(load(at(destination, countOffset)));
        var row = frame.numeric(b);
        Var valueY = b.let(y.emit(row).data()), valueX = b.let(x.emit(row).data());
        Expr finite = eq(binary(Op.EITHER, Pipeline.nonFinite(valueY), Pipeline.nonFinite(valueX)), bool(false));
        b.when(finite, present -> {
            Var meanY = present.let(new Load(MemoryType.F64, at(destination, offset)));
            Var meanX = present.let(new Load(MemoryType.F64, at(destination, meanXOffset)));
            Var sum = present.let(new Load(MemoryType.F64, at(destination, sumOffset)));
            Var nextCount = present.let(add(count, i64(1)));
            Var deltaY = present.let(binary(Op.FSUB, valueY, meanY));
            Var nextMeanY = present.let(binary(Op.FADD, meanY, binary(Op.FDIV, deltaY, new ToDouble(nextCount))));
            Var nextMeanX = present.let(binary(Op.FADD, meanX,
                    binary(Op.FDIV, binary(Op.FSUB, valueX, meanX), new ToDouble(nextCount))));
            present.store(MemoryType.F64, at(destination, offset), nextMeanY);
            present.store(MemoryType.F64, at(destination, meanXOffset), nextMeanX);
            present.store(MemoryType.F64, at(destination, sumOffset), binary(Op.FADD, sum,
                    binary(Op.FMUL, deltaY, binary(Op.FSUB, valueX, nextMeanX))));
            present.store(at(destination, countOffset), nextCount);
        });
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        Var sourceCount = b.let(load(at(source, countOffset)));
        b.when(ne(sourceCount, i64(0)), nonempty -> {
            Var destinationCount = nonempty.let(load(at(destination, countOffset)));
            Var sourceY = nonempty.let(new Load(MemoryType.F64, at(source, offset)));
            Var sourceX = nonempty.let(new Load(MemoryType.F64, at(source, meanXOffset)));
            Var sourceSum = nonempty.let(new Load(MemoryType.F64, at(source, sumOffset)));
            nonempty.when(eq(destinationCount, i64(0)), empty -> {
                empty.store(MemoryType.F64, at(destination, offset), sourceY);
                empty.store(MemoryType.F64, at(destination, meanXOffset), sourceX);
                empty.store(MemoryType.F64, at(destination, sumOffset), sourceSum);
                empty.store(at(destination, countOffset), sourceCount);
            });
            nonempty.when(ne(destinationCount, i64(0)), combine -> {
                Var meanY = combine.let(new Load(MemoryType.F64, at(destination, offset)));
                Var meanX = combine.let(new Load(MemoryType.F64, at(destination, meanXOffset)));
                Var sum = combine.let(new Load(MemoryType.F64, at(destination, sumOffset)));
                Var count = combine.let(add(sourceCount, destinationCount));
                Expr deltaY = binary(Op.FSUB, meanY, sourceY), deltaX = binary(Op.FSUB, meanX, sourceX);
                // Unlike variance's merge, covariance converts before multiplying the counts.
                Expr weight = binary(Op.FDIV, binary(Op.FMUL, new ToDouble(sourceCount), new ToDouble(destinationCount)), new ToDouble(count));
                Expr mergedY = binary(Op.FDIV, binary(Op.FADD,
                        binary(Op.FMUL, new ToDouble(sourceCount), sourceY),
                        binary(Op.FMUL, new ToDouble(destinationCount), meanY)), new ToDouble(count));
                Expr mergedX = binary(Op.FDIV, binary(Op.FADD,
                        binary(Op.FMUL, new ToDouble(sourceCount), sourceX),
                        binary(Op.FMUL, new ToDouble(destinationCount), meanX)), new ToDouble(count));
                Expr mergedSum = binary(Op.FADD, binary(Op.FADD, sourceSum, sum),
                        binary(Op.FMUL, binary(Op.FMUL, deltaY, deltaX), weight));
                combine.store(MemoryType.F64, at(destination, offset), mergedY);
                combine.store(MemoryType.F64, at(destination, meanXOffset), mergedX);
                combine.store(MemoryType.F64, at(destination, sumOffset), mergedSum);
                combine.store(at(destination, countOffset), count);
            });
        });
    }

    @Override public void finish(Builder b, Expr destination, boolean deferred) {
        Var count = b.let(load(at(destination, countOffset)));
        Expr denominator = sample ? binary(Op.SUB, count, i64(1)) : count;
        Var result = b.let(f64(Double.NaN));
        b.when(binary(Op.LT, i64(0), denominator), present -> present.assign(result,
                binary(Op.FDIV, new Load(MemoryType.F64, at(destination, sumOffset)), new ToDouble(denominator))));
        b.store(MemoryType.F64, at(destination, offset), result);
    }
}
