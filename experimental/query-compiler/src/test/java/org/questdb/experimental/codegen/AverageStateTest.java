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
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.AvgIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.AvgLongGroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

@RunWith(Parameterized.class)
public class AverageStateTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    public AverageStateTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"c", new NativeBackend()},
                new Object[]{"asmjit", new AsmJitBackend()});
    }

    @Test public void mixedUpdatesAndMergesMatchOrdinaryFloatingState() throws Exception {
        for (int type : new int[]{ColumnType.INT, ColumnType.LONG}) {
            var column = new Pipeline.Column(0, "value", type);
            var update = new AggregateUpdates.AverageUpdate(column, 0, 8, MemoryType.F64);
            long nullValue = type == ColumnType.INT ? Integer.MIN_VALUE : Long.MIN_VALUE;
            var b = new Builder();
            var row = new Pipeline.Row(List.of(column), List.of(new Pipeline.Value(load(COLUMNS), eq(load(COLUMNS), i64(nullValue)))));
            update.emit(b, new ResolvedVarchar.Frame(row, i64(0), nil(), nil()), STATE, nil());
            b.exit(0);
            var merge = new Builder(); update.merge(merge, COLUMNS, STATE); merge.exit(0);
            try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
                 var merger = backend.compile(new Program(merge.block()), temporary.newFolder().toPath());
                 var arena = Arena.ofConfined();
                 GroupByFunction function = type == ColumnType.INT ? new AvgIntGroupByFunction(IntColumn.newInstance(0))
                         : new AvgLongGroupByFunction(LongColumn.newInstance(0))) {
                var types = new ArrayColumnTypes(); function.initValueTypes(types);
                long expectedAddress = arena.allocate(16, 8).address(), actualAddress = arena.allocate(16, 8).address();
                var expected = state(types, expectedAddress); var actual = state(types, actualAddress);
                function.setEmpty(expected); function.setEmpty(actual);
                long input = arena.allocate(8, 8).address();
                long max = type == ColumnType.INT ? Integer.MAX_VALUE : Long.MAX_VALUE;
                long[] sequence = {nullValue, nullValue, 0, -1, max, nullValue + 1, 17, nullValue, max, max, -9};
                for (int i = 0; i < sequence.length; i++) {
                    long value = sequence[i];
                    Record record = new Record() {
                        @Override public int getInt(int ignored) { return (int) value; }
                        @Override public long getLong(int ignored) { return value; }
                    };
                    if (i == 0) function.computeFirst(expected, record, i); else function.computeNext(expected, record, i);
                    if (i % 3 == 1) function.computeNext(actual, record, i);
                    else {
                        Unsafe.getUnsafe().putLong(input, value);
                        assertEquals(0, code.run(input, 1, actualAddress));
                    }
                    assertState(expected, actual);
                }
                long sourceAddress = arena.allocate(16, 8).address();
                var source = state(types, sourceAddress);
                for (int left : new int[]{0, 1, 13}) {
                    for (int right : new int[]{0, 1, 17}) {
                        expected.putDouble(0, left == 0 ? Double.NaN : max * (double) left);
                        expected.putLong(1, left);
                        actual.putDouble(0, expected.getDouble(0)); actual.putLong(1, left);
                        source.putDouble(0, right == 0 ? Double.NaN : -max * (double) right); source.putLong(1, right);
                        function.merge(expected, source);
                        assertEquals(0, merger.run(sourceAddress, 0, actualAddress));
                        assertState(expected, actual);
                    }
                }
            }
        }
    }

    @Test public void vectorWideSumCarriesAndMergesBothSignedWords() throws Exception {
        var column = new Pipeline.Column(0, "value", ColumnType.LONG);
        var update = new AggregateUpdates.VectorLongAverageUpdate(column, 0);
        var b = new Builder();
        var row = new Pipeline.Row(List.of(column), List.of(new Pipeline.Value(load(COLUMNS), eq(load(COLUMNS), i64(Long.MIN_VALUE)))));
        update.emit(b, new ResolvedVarchar.Frame(row, i64(0), nil(), nil()), STATE, nil()); b.exit(0);
        var merge = new Builder(); update.merge(merge, COLUMNS, STATE); merge.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var merger = backend.compile(new Program(merge.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long state = arena.allocate(24, 8).address(), source = arena.allocate(24, 8).address();
            long input = arena.allocate(8, 8).address();
            long[] values = {Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 1, 1, Long.MIN_VALUE + 1,
                    Long.MIN_VALUE + 1, -1, -1, Long.MIN_VALUE + 1, 17, Long.MAX_VALUE};
            BigInteger sum = BigInteger.ZERO;
            long count = 0;
            for (long value : values) {
                Unsafe.putLong(input, value);
                assertEquals(0, code.run(input, 1, state));
                if (value != Long.MIN_VALUE) { sum = sum.add(BigInteger.valueOf(value)); count++; }
                assertWideState(sum, count, state);
            }
            for (BigInteger addend : List.of(BigInteger.ONE.shiftLeft(90), BigInteger.ONE.shiftLeft(91).negate(),
                    BigInteger.valueOf(-1), BigInteger.ZERO)) {
                Unsafe.putLong(source, addend.longValue());
                Unsafe.putLong(source + 8, addend.shiftRight(64).longValue());
                Unsafe.putLong(source + 16, 23);
                assertEquals(0, merger.run(source, 0, state));
                sum = sum.add(addend); count += 23;
                assertWideState(sum, count, state);
            }
        }
    }

    private static void assertWideState(BigInteger sum, long count, long address) {
        assertEquals(sum.longValue(), Unsafe.getLong(address));
        assertEquals(sum.shiftRight(64).longValue(), Unsafe.getLong(address + 8));
        assertEquals(count, Unsafe.getLong(address + 16));
    }

    private static FlyweightPackedMapValue state(ArrayColumnTypes types, long address) {
        var value = new FlyweightPackedMapValue(types); value.of(address); return value;
    }

    private static void assertState(FlyweightPackedMapValue expected, FlyweightPackedMapValue actual) {
        assertEquals(Double.doubleToLongBits(expected.getDouble(0)), Double.doubleToLongBits(actual.getDouble(0)));
        assertEquals(expected.getLong(1), actual.getLong(1));
    }
}
