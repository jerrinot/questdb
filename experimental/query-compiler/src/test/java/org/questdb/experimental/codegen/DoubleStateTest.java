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
import io.questdb.griffin.engine.functions.groupby.*;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

@RunWith(Parameterized.class)
public class DoubleStateTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    public DoubleStateTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()},
                new Object[]{"c", new NativeBackend()});
    }

    @Test public void mixedUpdatesAndMergesPreserveEachOrdinaryNonfiniteRule() throws Exception {
        double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0, -0.0,
                Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, 17.25,
                Double.longBitsToDouble(0x7ff8000000001234L), -17.25};
        for (GroupByFunction function : List.of(new CountDoubleGroupByFunction(DoubleColumn.newInstance(0)),
                new SumDoubleGroupByFunction(DoubleColumn.newInstance(0)), new AvgDoubleGroupByFunction(DoubleColumn.newInstance(0)),
                new MinDoubleGroupByFunction(DoubleColumn.newInstance(0)), new MaxDoubleGroupByFunction(DoubleColumn.newInstance(0)))) {
            try (function; var arena = Arena.ofConfined()) {
                var types = new ArrayColumnTypes(); function.initValueTypes(types);
                var inputTypes = new ArrayColumnTypes(); inputTypes.add(ColumnType.DOUBLE);
                var functions = new ObjList<GroupByFunction>(); functions.add(function);
                var layout = new FlyweightPackedMapValue(types);
                var updates = AggregateUpdates.bind(inputTypes, types, functions, layout::getOffset, false);
                assertNotNull(updates);
                var b = new Builder();
                var column = updates.columns().getFirst();
                var row = new Pipeline.Row(updates.columns(), List.of(column.value(new Load(MemoryType.F64, COLUMNS))));
                updates.emit(b, row, STATE, nil()); b.exit(0);
                var merge = new Builder(); updates.merge(merge, COLUMNS, STATE); merge.exit(0);
                try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
                     var merger = backend.compile(new Program(merge.block()), temporary.newFolder().toPath())) {
                    long actualAddress = arena.allocate(16, 8).address();
                    var actual = state(types, actualAddress);
                    var expected = state(types, arena.allocate(16, 8).address());
                    long sourceAddress = arena.allocate(16, 8).address();
                    var source = state(types, sourceAddress);
                    long input = arena.allocate(8, 8).address();
                    function.setEmpty(expected); function.setEmpty(actual);
                    for (int i = 0; i < values.length; i++) {
                        Record record = record(values[i]);
                        if (i == 0) function.computeFirst(expected, record, i); else function.computeNext(expected, record, i);
                        if (i % 3 == 1) function.computeNext(actual, record, i);
                        else { Unsafe.putDouble(input, values[i]); assertEquals(0, code.run(input, 1, actualAddress)); }
                        assertState(function.getName(), types, expected, actual);
                    }
                    for (int left : new int[]{0, 1, 2, 3, 9, 12}) for (int right : new int[]{0, 1, 2, 3, 9, 12}) {
                        populate(function, expected, values, left); populate(function, actual, values, left);
                        populate(function, source, values, right);
                        function.merge(expected, source);
                        assertEquals(0, merger.run(sourceAddress, 0, actualAddress));
                        assertState(function.getName(), types, expected, actual);
                    }
                }
            }
        }
    }

    private static Record record(double value) {
        return new Record() { @Override public double getDouble(int ignored) { return value; } };
    }
    private static void populate(GroupByFunction function, FlyweightPackedMapValue state, double[] values, int length) {
        function.setEmpty(state);
        for (int i = 0; i < length; i++) {
            if (i == 0) function.computeFirst(state, record(values[i]), i);
            else function.computeNext(state, record(values[i]), i);
        }
    }
    private static FlyweightPackedMapValue state(ArrayColumnTypes types, long address) {
        var value = new FlyweightPackedMapValue(types); value.of(address); return value;
    }
    private static void assertState(String name, ArrayColumnTypes types, FlyweightPackedMapValue expected, FlyweightPackedMapValue actual) {
        for (int i = 0; i < types.getColumnCount(); i++) {
            if (types.getColumnType(i) == ColumnType.DOUBLE)
                assertEquals(name, Double.doubleToLongBits(expected.getDouble(i)), Double.doubleToLongBits(actual.getDouble(i)));
            else assertEquals(name, expected.getLong(i), actual.getLong(i));
        }
    }
}
