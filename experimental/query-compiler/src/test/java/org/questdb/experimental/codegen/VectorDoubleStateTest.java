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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.groupby.vect.*;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.lang.foreign.Arena;
import java.util.List;

import static io.questdb.griffin.SqlCodeGenerator.GKK_VANILLA_INT;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class VectorDoubleStateTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend = new LlvmBackend();

    @Test public void generatedAndRostiUpdatesShareStateAcrossExceptionalValuesAndMerges() throws Exception {
        double[] values = {-0.0, Double.NaN, 0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 17,
                Double.longBitsToDouble(0x7ff8000000001234L), -17, Double.MIN_VALUE, -Double.MIN_VALUE,
                Double.MAX_VALUE, Double.MAX_VALUE};
        for (VectorAggregateFunction function : List.of(new CountDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new SumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new AvgDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new KSumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new NSumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new MinDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new MaxDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1))) {
            try (function; var arena = Arena.ofConfined()) {
                var types = new ArrayColumnTypes(); types.add(ColumnType.INT); function.pushValueTypes(types);
                long expected = Rosti.alloc(types, 64), actual = Rosti.alloc(types, 64), source = Rosti.alloc(types, 64);
                try {
                    long key = arena.allocate(4, 4).address(), input = arena.allocate(8, 8).address();
                    Unsafe.putInt(key, 1);
                    for (long map : new long[]{expected, actual, source}) {
                        function.initRosti(map);
                        // Pre-create the group: the ordinary initial state is shared by all aggregate tasks.
                        assertTrue(Rosti.keyedIntDistinct(map, key, 1));
                    }
                    long expectedState = entry(expected), actualState = entry(actual), sourceState = entry(source);
                    long offset = Unsafe.getInt(Rosti.getValueOffsets(actual) + function.getValueOffset() * 4L);
                    var column = new Pipeline.Column(0, "d", ColumnType.DOUBLE);
                    var update = update(function, column, offset);
                    var row = new Pipeline.Row(List.of(column), List.of(column.value(new Load(MemoryType.F64, COLUMNS))));
                    var b = new Builder(); update.emit(b, new ResolvedVarchar.Frame(row, i64(0), nil(), nil()), STATE, nil()); b.exit(0);
                    var merge = new Builder(); update.merge(merge, COLUMNS, STATE); merge.exit(0);
                    try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
                         var merger = backend.compile(new Program(merge.block()), temporary.newFolder().toPath())) {
                        for (int i = 0; i < values.length; i++) {
                            Unsafe.putDouble(input, values[i]);
                            assertTrue(function.aggregate(expected, key, input, 1));
                            if (i % 3 == 1) assertTrue(function.aggregate(actual, key, input, 1));
                            else assertEquals(0, code.run(input, 1, actualState));
                            assertState(types, expected, expectedState, actualState);
                        }
                        for (double value : values) {
                            Unsafe.putDouble(input, value);
                            assertTrue(function.aggregate(source, key, input, 1));
                            assertTrue(function.merge(expected, source));
                            assertEquals(0, merger.run(sourceState, 0, actualState));
                            assertState(types, expected, expectedState, actualState);
                        }
                    }
                } finally { Rosti.free(source); Rosti.free(actual); Rosti.free(expected); }
            }
        }
    }

    @Test public void firstRowsAndFirstPartialCopiesMatchRosti() throws Exception {
        for (VectorAggregateFunction function : List.of(new SumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new KSumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1),
                new NSumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 1))) {
            try (function; var arena = Arena.ofConfined()) {
                var types = new ArrayColumnTypes(); types.add(ColumnType.INT); function.pushValueTypes(types);
                long expected = Rosti.alloc(types, 64), actual = Rosti.alloc(types, 64);
                try {
                    long key = arena.allocate(4, 4).address(), input = arena.allocate(8, 8).address();
                    Unsafe.putInt(key, 1);
                    long offset = Unsafe.getInt(Rosti.getValueOffsets(actual) + function.getValueOffset() * 4L);
                    var column = new Pipeline.Column(0, "d", ColumnType.DOUBLE);
                    var update = update(function, column, offset);
                    var row = new Pipeline.Row(List.of(column), List.of(column.value(new Load(MemoryType.F64, COLUMNS))));
                    var frame = new ResolvedVarchar.Frame(row, i64(0), nil(), nil());
                    var first = new Builder(); update.emitFirst(first, frame, STATE, nil()); first.exit(0);
                    var next = new Builder(); update.emit(next, frame, STATE, nil()); next.exit(0);
                    var updates = new AggregateUpdates(new ResolvedVarchar.Binding(new int[]{0}, List.of(column), new int[0], new byte[0]), List.of(update));
                    long stride = Rosti.getSlotSize(actual);
                    try (var firstCode = backend.compile(new Program(first.block()), temporary.newFolder().toPath());
                         var nextCode = backend.compile(new Program(next.block()), temporary.newFolder().toPath());
                         var dense = backend.compile(DenseAggregate.merge(updates, stride), temporary.newFolder().toPath())) {
                        long copy = arena.allocate(stride, 8).address(), args = arena.allocate(24, 8).address(), seen = arena.allocate(2).address();
                        for (double initial : new double[]{-0.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 1e16}) {
                            Rosti.clear(expected); Rosti.clear(actual); function.initRosti(expected); function.initRosti(actual);
                            assertTrue(Rosti.keyedIntDistinct(actual, key, 1));
                            Unsafe.putDouble(input, initial);
                            assertTrue(function.aggregate(expected, key, input, 1));
                            long expectedState = entry(expected), actualState = entry(actual);
                            assertEquals(0, firstCode.run(input, 1, actualState));
                            assertState(types, expected, expectedState, actualState);
                            Unsafe.putDouble(input, 1);
                            assertTrue(function.aggregate(expected, key, input, 1));
                            assertEquals(0, nextCode.run(input, 1, actualState));
                            assertState(types, expected, expectedState, actualState);
                            Unsafe.putLong(args, actualState); Unsafe.putLong(args+8, seen); Unsafe.putLong(args+16, seen+1);
                            Unsafe.putByte(seen, (byte) 1); Unsafe.putByte(seen+1, (byte) 0);
                            assertEquals(0, dense.run(args, 1, copy));
                            assertState(types, expected, expectedState, copy);
                            assertTrue(function.merge(expected, actual));
                            assertEquals(0, dense.run(args, 1, copy));
                            assertState(types, expected, expectedState, copy);
                        }
                    }
                } finally { Rosti.free(actual); Rosti.free(expected); }
            }
        }
    }

    private static AggregateUpdates.Update update(VectorAggregateFunction function, Pipeline.Column column, long offset) {
        if (function instanceof CountDoubleVectorAggregateFunction) return new AggregateUpdates.VectorDoubleCount(column, offset);
        if (function instanceof MinDoubleVectorAggregateFunction || function instanceof MaxDoubleVectorAggregateFunction)
            return new AggregateUpdates.VectorDoubleExtremum(column, offset, function instanceof MinDoubleVectorAggregateFunction);
        if (function instanceof KSumDoubleVectorAggregateFunction || function instanceof NSumDoubleVectorAggregateFunction)
            return new VectorCompensatedSum(column, offset, function instanceof NSumDoubleVectorAggregateFunction);
        return new AggregateUpdates.VectorSumUpdate(column, offset, MemoryType.F64);
    }

    private static long entry(long map) {
        for (long i = 0; i < Rosti.getCapacity(map); i++) {
            if (Unsafe.getUnsafe().getByte(Rosti.getCtrl(map) + i) >= 0)
                return Rosti.getSlots(map) + i * Rosti.getSlotSize(map);
        }
        throw new AssertionError("missing Rosti group");
    }

    private static void assertState(ArrayColumnTypes types, long map, long expected, long actual) {
        for (int i = 1; i < types.getColumnCount(); i++) {
            long offset = Unsafe.getInt(Rosti.getValueOffsets(map) + i * 4L);
            if (types.getColumnType(i) == ColumnType.DOUBLE)
                assertEquals(Double.doubleToLongBits(Unsafe.getDouble(expected + offset)),
                        Double.doubleToLongBits(Unsafe.getDouble(actual + offset)));
            else assertEquals(Unsafe.getLong(expected + offset), Unsafe.getLong(actual + offset));
        }
    }
}
