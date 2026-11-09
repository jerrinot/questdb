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

/** Ordinary bivariate Welford state shared by correlation and linear regression. */
record RegressionAggregate(Pipeline.Scalar y, Pipeline.Scalar x, long offset, long sumYOffset,
                           long meanXOffset, long sumXOffset, long sumXYOffset, long countOffset,
                           Kind kind) implements Update {
    enum Kind { CORRELATION, SLOPE, INTERCEPT, R2 }

    @Override public List<StateCell> scalarState() {
        return List.of(new StateCell(MemoryType.F64, offset), new StateCell(MemoryType.F64, sumYOffset),
                new StateCell(MemoryType.F64, meanXOffset), new StateCell(MemoryType.F64, sumXOffset),
                new StateCell(MemoryType.F64, sumXYOffset), new StateCell(MemoryType.I64, countOffset));
    }
    @Override public boolean requiresFirstRowInitialization() { return true; }
    @Override public ResultAccess resultAccess() { return ResultAccess.FINALIZED; }
    @Override public boolean finalizesResult() { return true; }

    @Override public void initialize(Builder b, Expr destination) {
        for (var cell : scalarState()) b.store(cell.width(), at(destination, cell.offset()),
                cell.width() == MemoryType.F64 ? f64(0) : i64(0));
    }

    @Override public void emit(Builder b, ResolvedVarchar.Frame frame, Expr destination, Expr cardinalityAddress) {
        var row = frame.numeric(b);
        Var valueY = b.let(y.emit(row).data()), valueX = b.let(x.emit(row).data());
        Expr finite = eq(binary(Op.EITHER, Pipeline.nonFinite(valueY), Pipeline.nonFinite(valueX)), bool(false));
        b.when(finite, present -> {
            Var meanY = present.let(new Load(MemoryType.F64, at(destination, offset)));
            Var meanX = present.let(new Load(MemoryType.F64, at(destination, meanXOffset)));
            Var sumY = present.let(new Load(MemoryType.F64, at(destination, sumYOffset)));
            Var sumX = present.let(new Load(MemoryType.F64, at(destination, sumXOffset)));
            Var sumXY = present.let(new Load(MemoryType.F64, at(destination, sumXYOffset)));
            Var count = present.let(add(load(at(destination, countOffset)), i64(1)));
            Var deltaY = present.let(binary(Op.FSUB, valueY, meanY));
            Var deltaX = present.let(binary(Op.FSUB, valueX, meanX));
            Var nextY = present.let(binary(Op.FADD, meanY, binary(Op.FDIV, deltaY, new ToDouble(count))));
            Var nextX = present.let(binary(Op.FADD, meanX, binary(Op.FDIV, deltaX, new ToDouble(count))));
            present.store(MemoryType.F64, at(destination, offset), nextY);
            present.store(MemoryType.F64, at(destination, meanXOffset), nextX);
            present.store(MemoryType.F64, at(destination, sumYOffset), binary(Op.FADD, sumY,
                    binary(Op.FMUL, binary(Op.FSUB, valueY, nextY), deltaY)));
            present.store(MemoryType.F64, at(destination, sumXOffset), binary(Op.FADD, sumX,
                    binary(Op.FMUL, binary(Op.FSUB, valueX, nextX), deltaX)));
            present.store(MemoryType.F64, at(destination, sumXYOffset), binary(Op.FADD, sumXY,
                    binary(Op.FMUL, deltaY, binary(Op.FSUB, valueX, nextX))));
            present.store(at(destination, countOffset), count);
        });
    }

    @Override public void merge(Builder b, Expr source, Expr destination) {
        Var sourceCount = b.let(load(at(source, countOffset)));
        b.when(ne(sourceCount, i64(0)), nonempty -> {
            Var destinationCount = nonempty.let(load(at(destination, countOffset)));
            nonempty.when(eq(destinationCount, i64(0)), empty -> {
                for (var cell : scalarState()) empty.store(cell.width(), at(destination, cell.offset()),
                        new Load(cell.width(), at(source, cell.offset())));
            });
            nonempty.when(ne(destinationCount, i64(0)), combine -> {
                Var sourceY = combine.let(new Load(MemoryType.F64, at(source, offset)));
                Var sourceX = combine.let(new Load(MemoryType.F64, at(source, meanXOffset)));
                Var meanY = combine.let(new Load(MemoryType.F64, at(destination, offset)));
                Var meanX = combine.let(new Load(MemoryType.F64, at(destination, meanXOffset)));
                Var count = combine.let(add(sourceCount, destinationCount));
                Var deltaY = combine.let(binary(Op.FSUB, meanY, sourceY));
                Var deltaX = combine.let(binary(Op.FSUB, meanX, sourceX));
                // CORR converts the wrapped LONG product; regression converts each count first.
                Expr product = kind == Kind.CORRELATION ? new ToDouble(binary(Op.MUL, sourceCount, destinationCount))
                        : binary(Op.FMUL, new ToDouble(sourceCount), new ToDouble(destinationCount));
                Var weight = combine.let(binary(Op.FDIV, product, new ToDouble(count)));
                combine.store(MemoryType.F64, at(destination, offset), binary(Op.FDIV, binary(Op.FADD,
                        binary(Op.FMUL, new ToDouble(sourceCount), sourceY),
                        binary(Op.FMUL, new ToDouble(destinationCount), meanY)), new ToDouble(count)));
                combine.store(MemoryType.F64, at(destination, meanXOffset), binary(Op.FDIV, binary(Op.FADD,
                        binary(Op.FMUL, new ToDouble(sourceCount), sourceX),
                        binary(Op.FMUL, new ToDouble(destinationCount), meanX)), new ToDouble(count)));
                mergeMoment(combine, source, destination, sumYOffset, deltaY, deltaY, weight);
                mergeMoment(combine, source, destination, sumXOffset, deltaX, deltaX, weight);
                mergeMoment(combine, source, destination, sumXYOffset, deltaX, deltaY, weight);
                combine.store(at(destination, countOffset), count);
            });
        });
    }

    private static void mergeMoment(Builder b, Expr source, Expr destination, long offset, Expr left, Expr right, Expr weight) {
        b.store(MemoryType.F64, at(destination, offset), binary(Op.FADD,
                binary(Op.FADD, new Load(MemoryType.F64, at(source, offset)), new Load(MemoryType.F64, at(destination, offset))),
                binary(Op.FMUL, binary(Op.FMUL, left, right), weight)));
    }

    @Override public void finish(Builder b, Expr destination, boolean deferred) {
        Var count = b.let(load(at(destination, countOffset)));
        Var result = b.let(f64(Double.NaN));
        b.when(binary(Op.LT, i64(kind == Kind.CORRELATION ? 1 : 0), count), present -> {
            Var sumY = present.let(new Load(MemoryType.F64, at(destination, sumYOffset)));
            Var sumX = present.let(new Load(MemoryType.F64, at(destination, sumXOffset)));
            Var sumXY = present.let(new Load(MemoryType.F64, at(destination, sumXYOffset)));
            if (kind == Kind.CORRELATION) {
                // Numbers.corrFromSums: split sqrt only for unusable products, then clamp.
                Var product = present.let(binary(Op.FMUL, sumY, sumX));
                Expr split = binary(Op.EITHER, Pipeline.nonFinite(product), binary(Op.BOTH,
                        binary(Op.FEQ, product, f64(0)), binary(Op.BOTH,
                                binary(Op.FNE, sumY, f64(0)), binary(Op.FNE, sumX, f64(0)))));
                Var denominator = present.let(new Select(split, binary(Op.FMUL, new Sqrt(sumY), new Sqrt(sumX)), new Sqrt(product)));
                present.when(binary(Op.FNE, denominator, f64(0)), defined -> {
                    Var ratio = defined.let(binary(Op.FDIV, sumXY, denominator));
                    defined.assign(result, new Select(binary(Op.FLT, f64(1), ratio), f64(1),
                            new Select(binary(Op.FLT, ratio, f64(-1)), f64(-1), ratio)));
                });
            } else present.when(binary(Op.FNE, sumX, f64(0)), defined -> {
                Expr value = switch (kind) {
                    case SLOPE -> binary(Op.FDIV, sumXY, sumX);
                    case INTERCEPT -> binary(Op.FSUB, new Load(MemoryType.F64, at(destination, offset)),
                            binary(Op.FMUL, new Load(MemoryType.F64, at(destination, meanXOffset)), binary(Op.FDIV, sumXY, sumX)));
                    case R2 -> new Select(binary(Op.FEQ, sumY, f64(0)), f64(1),
                            binary(Op.FDIV, binary(Op.FMUL, sumXY, sumXY), binary(Op.FMUL, sumX, sumY)));
                    case CORRELATION -> throw new AssertionError(kind);
                };
                defined.assign(result, value);
            });
        });
        b.store(MemoryType.F64, at(destination, offset), result);
    }
}
