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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.*;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class RepeatedAggregateTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void repeatedInputsPreserveOrdinaryStatesAndCancellation() throws Exception {
        repeatedInputs(false);
    }

    @Test public void promotedInputsPreserveOrdinaryStatesAndCancellation() throws Exception {
        repeatedInputs(true);
    }

    private void repeatedInputs(boolean promote) throws Exception {
        long memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction());
        functions.add(new CountLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new AvgLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new MinLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new MaxLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new SumDoubleGroupByFunction(DoubleColumn.newInstance(1)));
        functions.add(new AvgDoubleGroupByFunction(DoubleColumn.newInstance(1)));
        functions.add(new KSumDoubleGroupByFunction(DoubleColumn.newInstance(1)));
        functions.add(new MinDoubleGroupByFunction(DoubleColumn.newInstance(1)));
        functions.add(new MaxDoubleGroupByFunction(DoubleColumn.newInstance(1)));
        functions.add(new MinIntGroupByFunction(IntColumn.newInstance(2)));
        functions.add(new MaxIntGroupByFunction(IntColumn.newInstance(2)));
        var inputs = new ArrayColumnTypes(); inputs.add(ColumnType.LONG); inputs.add(ColumnType.DOUBLE); inputs.add(ColumnType.INT);
        var types = new ArrayColumnTypes();
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
        try (var actual = new SimpleMapValue(types.getColumnCount())) {
            var updates = AggregateUpdates.bind(inputs, types, functions, index -> actual.getAddress(index) - actual.getAddress(0), false);
            assertNotNull(updates);
            Builder b = new Builder();
            var registers = promote ? ScalarAggregateState.bind(b, updates) : null;
            if (promote) assertNotNull(registers);
            var values = new ArrayList<Pipeline.Value>();
            for (int i = 0; i < updates.columns().size(); i++) {
                var column = updates.columns().get(i);
                values.add(column.value(b.let(new Load(column.width(), at(COLUMNS, updates.sourceIndexes()[i] * 8L)))));
            }
            if (promote) {
                Builder emitted = b.fork();
                updates.emitRepeated(emitted, new Pipeline.Row(updates.columns(), values), STATE, ROWS);
                b.append(registers.promote(emitted.block()));
            } else updates.emitRepeated(b, new Pipeline.Row(updates.columns(), values), STATE, ROWS);
            b.exit(0);
            var program = new Program(registers == null ? b.block() : registers.publishExits(b.block()));
            try (var code = new LlvmBackend().compile(program, temporary.newFolder().toPath());
                 var expected = new SimpleMapValue(types.getColumnCount());
                 var input = new DirectLongList(3, MemoryTag.NATIVE_JIT)) {
                // setEmpty need not touch unobservable state, e.g. KSUM's compensation.
                actual.clear();
                expected.clear();
                Sample sample = new Sample();
                long[] integers = {Long.MIN_VALUE, -(1L << 62), Long.MAX_VALUE, Long.MIN_VALUE + 1,
                        Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 9007199254740993L};
                double[] doubles = {Double.NaN, 0.1, -0.0, 0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                        Double.MAX_VALUE, Double.MIN_VALUE, 1e16, 1.0, -1e16};
                try (var scope = KernelMemory.bind(null, () -> { })) {
                    for (double prefix : doubles) for (long integer : integers) for (double floating : doubles) for (int repetitions : new int[]{0, 1, 3, 10, 1025}) {
                        for (int i = 0; i < functions.size(); i++) {
                            functions.getQuick(i).setEmpty(actual);
                            functions.getQuick(i).setEmpty(expected);
                        }
                        boolean first = true;
                        // Cross previous/current floating values: infinities are replaceable extrema
                        // states, and signed-zero ties must retain the ordinary operator's choice.
                        // A NULL prefix also distinguishes an empty AVG from an existing zero sum.
                        for (int stage = 0; stage < 2; stage++) {
                            sample.integer = stage == 0 && Double.isNaN(prefix) ? Long.MIN_VALUE : integer;
                            sample.narrow = sample.integer == Long.MIN_VALUE ? Integer.MIN_VALUE : (int) sample.integer;
                            sample.floating = stage == 0 ? prefix : floating;
                            Unsafe.putLong(input.getAddress(), sample.integer);
                            Unsafe.putDouble(input.getAddress() + 8, sample.floating);
                            Unsafe.putInt(input.getAddress() + 16, sample.narrow);
                            assertEquals(0, code.run(input.getAddress(), repetitions, actual.getAddress(0)));
                            scope.throwIfFailed();
                            for (int row = 0; row < repetitions; row++) {
                                for (int i = 0; i < functions.size(); i++) {
                                    if (first) functions.getQuick(i).computeFirst(expected, sample, row);
                                    else functions.getQuick(i).computeNext(expected, sample, row);
                                }
                                first = false;
                            }
                            for (int i = 0; i < types.getColumnCount(); i++) {
                                String message = "slot=" + i + " value=" + integer + "/" + floating + " prefix=" + prefix + " repeats=" + repetitions + " stage=" + stage;
                                if (types.getColumnType(i) == ColumnType.DOUBLE) {
                                    assertEquals(message, Double.doubleToLongBits(expected.getDouble(i)), Double.doubleToLongBits(actual.getDouble(i)));
                                } else if (types.getColumnType(i) == ColumnType.INT) {
                                    assertEquals(message, expected.getInt(i), actual.getInt(i));
                                } else assertEquals(message, expected.getLong(i), actual.getLong(i));
                            }
                        }
                    }
                }
                Unsafe.putLong(input.getAddress(), 1);
                Unsafe.putDouble(input.getAddress() + 8, 0.1);
                try (var cancelled = KernelMemory.bind(null, () -> { throw new IllegalStateException("cancel repeated inputs"); })) {
                    assertNotEquals(0, code.run(input.getAddress(), Long.MAX_VALUE, actual.getAddress(0)));
                    assertEquals("cancel repeated inputs", assertThrows(IllegalStateException.class, cancelled::throwIfFailed).getMessage());
                }
            }
        }
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void compensatedMergePreservesSourceCorrectionAndCopiesEveryScalarField() throws Exception {
        long memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var function = new KSumDoubleGroupByFunction(DoubleColumn.newInstance(0));
        var functions = new ObjList<GroupByFunction>(); functions.add(function);
        var inputs = new ArrayColumnTypes(); inputs.add(ColumnType.DOUBLE);
        var types = new ArrayColumnTypes(); function.initValueTypes(types);
        double[][] sequences = {{}, {Double.NaN}, {-0.0}, {0.0}, {1e16, 1}, {1e16, 1, 1},
                {1e16, 1, -1e16}, {0.1, 0.1, 0.1}, {Double.MAX_VALUE, Double.MAX_VALUE},
                {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}, {Double.MIN_VALUE, -Double.MIN_VALUE}};
        try (var actual = new SimpleMapValue(3); var expected = new SimpleMapValue(3); var source = new SimpleMapValue(3);
             var copied = new SimpleMapValue(3);
             var code = ResolvedScalarAggregate.compile(inputs, types, functions, actual, null,
                     new LlvmBackend(), temporary.newFolder().toPath())) {
            assertNotNull(code);
            Sample sample = new Sample();
            for (double[] prefix : sequences) {
                actual.putDouble(1, 17.25); // setEmpty intentionally leaves stale compensation.
                function.setEmpty(actual);
                for (int i = 0; i < prefix.length; i++) {
                    sample.floating = prefix[i];
                    if (i == 0) function.computeFirst(actual, sample, i);
                    else function.computeNext(actual, sample, i);
                }
                expected.copy(actual);
                actual.setNew(false);
                for (double[] incoming : sequences) {
                    source.putDouble(1, -23.75);
                    function.setEmpty(source);
                    for (int i = 0; i < incoming.length; i++) {
                        sample.floating = incoming[i];
                        if (i == 0) function.computeFirst(source, sample, i);
                        else function.computeNext(source, sample, i);
                    }
                    function.merge(expected, source);
                    code.merge(source, actual);
                    assertEquals(Double.doubleToLongBits(expected.getDouble(0)), Double.doubleToLongBits(actual.getDouble(0)));
                    assertEquals(Double.doubleToLongBits(expected.getDouble(1)), Double.doubleToLongBits(actual.getDouble(1)));
                    assertEquals(expected.getLong(2), actual.getLong(2));
                    copied.putDouble(1, 99.0);
                    copied.setNew(true);
                    code.merge(source, copied);
                    for (int i = 0; i < 3; i++) assertEquals(source.getLong(i), copied.getLong(i));
                }
            }
        }
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    private static final class Sample implements Record {
        long integer;
        int narrow;
        double floating;
        @Override public int getInt(int col) { return narrow; }
        @Override public long getLong(int col) { return integer; }
        @Override public double getDouble(int col) { return floating; }
    }
}
