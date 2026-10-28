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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.constants.LongConstant;
import io.questdb.griffin.engine.functions.groupby.*;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByLongHashSet;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

@RunWith(Parameterized.class)
public class ResolvedAggregateKernelTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;

    public ResolvedAggregateKernelTest(String name, Backend backend) {
        this.backend = backend;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()},
                new Object[]{"c", new NativeBackend()});
    }

    @Test
    public void scalarReductionUsesActualSlotsSelectionsMergeAndCancellation() throws Exception {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction());
        functions.add(new MinIntGroupByFunction(IntColumn.newInstance(1)));
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new MaxShortGroupByFunction(ShortColumn.newInstance(2)));
        functions.add(new CountLongGroupByFunction(LongColumn.newInstance(0)));
        var types = new ArrayColumnTypes();
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
        var inputs = new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.INT).add(ColumnType.SHORT);
        try (var engine = new CairoEngine(new DefaultCairoConfiguration(temporary.newFolder().toString()));
             var nativeValue = new SimpleMapValue(types.getColumnCount());
             var expected = new SimpleMapValue(types.getColumnCount());
             var merged = new SimpleMapValue(types.getColumnCount());
             var code = ResolvedScalarAggregate.compile(inputs, types, functions, nativeValue, backend, temporary.newFolder().toPath());
             var slot = code.newUpdater(); var arena = Arena.ofConfined()) {
            var noop = io.questdb.cairo.sql.SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            for (int i = 0; i < functions.size(); i++) {
                functions.getQuick(i).setEmpty(nativeValue);
                functions.getQuick(i).setEmpty(expected);
                functions.getQuick(i).setEmpty(merged);
            }
            nativeValue.setNew(true);
            merged.setNew(true);
            int rows = 3001;
            long longs = arena.allocate(rows * 8L, 8).address();
            long ints = arena.allocate(rows * 4L, 4).address();
            long shorts = arena.allocate(rows * 2L, 2).address();
            long selection = arena.allocate(rows * 8L, 8).address();
            for (int i = 0; i < rows; i++) {
                Unsafe.putLong(longs + i * 8L, i % 7 == 0 ? Long.MIN_VALUE : i - 1500);
                Unsafe.putInt(ints + i * 4L, i % 11 == 0 ? Integer.MIN_VALUE : i - 1000);
                Unsafe.putShort(shorts + i * 2L, (short) (i * 37));
                Unsafe.putLong(selection + i * 8L, rows - 1L - i / 2); // reverse and repeat
            }
            var record = new TestRecord(longs, ints, shorts);
            assertTrue(slot.update(record, 0, 0, nativeValue, noop));
            assertTrue(nativeValue.isNew());
            assertFalse(slot.update(new PageFrameFilteredMemoryRecord(), rows, selection, nativeValue, noop));
            assertFalse(slot.update(new TestRecord(0, ints, shorts), rows, selection, nativeValue, noop));
            assertTrue(nativeValue.isNew());
            for (long selected : new long[]{0, selection}) {
                assertTrue(slot.update(record, rows, selected, nativeValue, noop));
                assertFalse(nativeValue.isNew());
                for (int r = 0; r < rows; r++) {
                    record.setRowIndex(selected == 0 ? r : Unsafe.getLong(selection + r * 8L));
                    for (int f = 0; f < functions.size(); f++) functions.getQuick(f).computeNext(expected, record, r);
                }
                assertScalarValues(types, expected, nativeValue);
            }
            code.merge(nativeValue, merged);
            assertScalarValues(types, expected, merged);
            merged.setNew(false);
            code.merge(nativeValue, merged);
            for (int f = 0; f < functions.size(); f++) functions.getQuick(f).merge(expected, nativeValue);
            assertScalarValues(types, expected, merged);
            var breaker = new AtomicBooleanCircuitBreaker(engine) {
                int polls;
                @Override public void statefulThrowExceptionIfTrippedNoThrottle() {
                    if (++polls == 3) cancel();
                    super.statefulThrowExceptionIfTrippedNoThrottle();
                }
            };
            assertThrows(CairoException.class, () -> slot.update(record, rows, 0, nativeValue, breaker));
            assertEquals(2L * rows, slot.getNativeRows());
            assertTrue(slot.update(record, rows, 0, nativeValue, noop));
            assertEquals(3L * rows, slot.getNativeRows());
        }
    }

    private static void assertScalarValues(ArrayColumnTypes types, SimpleMapValue expected, SimpleMapValue actual) {
        for (int i = 0; i < types.getColumnCount(); i++) {
            if (types.getColumnType(i) == ColumnType.INT) assertEquals(expected.getInt(i), actual.getInt(i));
            else assertEquals(expected.getLong(i), actual.getLong(i));
        }
    }

    @Test
    public void producedValuesNeedNoFrameAndPreserveExplicitNulls() throws Exception {
        var inputs = new ArrayColumnTypes().add(ColumnType.LONG);
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction());
        functions.add(new CountLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new MinLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new MaxLongGroupByFunction(LongColumn.newInstance(0)));
        var types = new ArrayColumnTypes();
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
        var layout = new FlyweightPackedMapValue(types);
        var updates = AggregateUpdates.bind(inputs, types, functions, layout::getOffset, false);
        assertNotNull(updates);
        var b = new Builder();
        var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            // An upstream operator supplies an expression and null flag, not a column address.
            var value = new Pipeline.Value(add(index, i64(7)), eq(binary(Op.AND, index, i64(1)), i64(0)));
            updates.emit(loop, new Pipeline.Row(updates.columns(), List.of(value)), STATE, nil());
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long state = arena.allocate(layout.getSizeInBytes(), 8).address();
            initialize(functions, layout, state);
            assertEquals(0, code.run(0, 100, state));
            assertEquals(100, layout.getLong(0));
            assertEquals(50, layout.getLong(1));
            assertEquals(2850, layout.getLong(2));
            assertEquals(8, layout.getLong(3));
            assertEquals(106, layout.getLong(4));
        }
    }

    @Test
    public void packedOffsetsRepeatedRowsNullsOverflowAndJavaMerge() throws Exception {
        ArrayColumnTypes inputs = new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.INT).add(ColumnType.SHORT);
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction());
        functions.add(new CountLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new CountIntGroupByFunction(IntColumn.newInstance(1)));
        functions.add(new SumIntGroupByFunction(IntColumn.newInstance(1)));
        functions.add(new SumShortGroupByFunction(ShortColumn.newInstance(2)));
        functions.add(new MinIntGroupByFunction(IntColumn.newInstance(1)));
        functions.add(new MinLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new MaxShortGroupByFunction(ShortColumn.newInstance(2)));
        functions.add(new MaxLongGroupByFunction(LongColumn.newInstance(0)));
        functions.add(new MinShortGroupByFunction(ShortColumn.newInstance(2)));
        functions.add(new MaxIntGroupByFunction(IntColumn.newInstance(1)));
        // A preceding INT makes the LONG value slots unaligned: offsets must come from core.
        var types = new ArrayColumnTypes().add(ColumnType.INT);
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
        var layout = new FlyweightPackedMapValue(types);
        var binding = ResolvedAggregateKernel.bind(inputs, types, functions);
        assertNotNull(binding);
        try (var code = binding.compile(backend, temporary.newFolder().toPath()); var slot = code.newSlot(); var arena = Arena.ofConfined()) {
            int rows = 1031, groups = 17;
            long stride = layout.getSizeInBytes();
            long nativeValues = arena.allocate(groups * stride, 8).address();
            long javaValues = arena.allocate(groups * stride, 8).address();
            long batch = arena.allocate(rows * 8L, 8).address();
            long longs = arena.allocate(rows * 8L, 8).address();
            long ints = arena.allocate(rows * 4L, 4).address();
            long shorts = arena.allocate(rows * 2L, 2).address();
            var record = new TestRecord(longs, ints, shorts);
            for (int i = 0; i < rows; i++) {
                Unsafe.putLong(longs + i * 8L, i % 7 == 0 ? Long.MIN_VALUE : i % 11 == 0 ? Long.MAX_VALUE : i - 51);
                Unsafe.putInt(ints + i * 4L, i % 13 == 0 ? Integer.MIN_VALUE : i * 357);
                Unsafe.putShort(shorts + i * 2L, (short) (i * 777));
                // Reverse/scatter input rows, repeat groups, exercise the sign bit for isNew.
                Unsafe.putLong(batch + i * 8L, Map.encodeBatchEntry(rows - 1L - i, (i % groups) * stride, i < groups));
            }
            for (int g = 0; g < groups; g++) {
                initialize(functions, layout, javaValues + g * stride);
                initialize(functions, layout, nativeValues + g * stride);
            }
            // Overflow/nonpositive COUNT states must remain usable by the actual Java merge.
            for (int g = 0; g < 3; g++) {
                long count = g == 0 ? -10_000 : g == 1 ? Long.MAX_VALUE - 1 : Long.MIN_VALUE;
                long offset = g * stride + layout.getOffset(functions.getQuick(0).getValueIndex());
                Unsafe.putLong(javaValues + offset, count);
                Unsafe.putLong(nativeValues + offset, count);
            }
            // Begin both sides with Java-produced state, then continue one in native code.
            for (int i = 0; i < functions.size(); i++) {
                functions.getQuick(i).computeKeyedBatch(record, layout, javaValues, batch, rows, 0);
                functions.getQuick(i).computeKeyedBatch(record, layout, nativeValues, batch, rows, 0);
            }
            for (int repeat = 0; repeat < 3; repeat++) {
                assertTrue(slot.update(record, nativeValues, batch, rows));
                for (int i = 0; i < functions.size(); i++) {
                    functions.getQuick(i).computeKeyedBatch(record, layout, javaValues, batch, rows, 0);
                }
                equalValues(javaValues, nativeValues, groups * stride);
            }
            // Feed native state to the actual Java merge implementations, including self-key overlap.
            var source = new FlyweightPackedMapValue(types);
            var expected = new FlyweightPackedMapValue(types);
            for (int g = 1; g < groups; g++) {
                layout.of(nativeValues);
                source.of(nativeValues + g * stride);
                expected.of(javaValues);
                var javaSource = new FlyweightPackedMapValue(types);
                javaSource.of(javaValues + g * stride);
                for (int i = 0; i < functions.size(); i++) {
                    functions.getQuick(i).merge(layout, source);
                    functions.getQuick(i).merge(expected, javaSource);
                }
            }
            equalValues(javaValues, nativeValues, stride);
            assertTrue(slot.update(record, nativeValues, batch, 0));
            equalValues(javaValues, nativeValues, stride);
            slot.close();
            assertThrows(IllegalStateException.class, () -> slot.update(record, nativeValues, batch, 1));
        }
    }

    @Test
    public void zeroAddressDeclinesBeforeAnyMutation() throws Exception {
        var types = new ArrayColumnTypes();
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction());
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
        var binding = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions);
        try (var code = binding.compile(backend, temporary.newFolder().toPath()); var slot = code.newSlot(); var arena = Arena.ofConfined()) {
            long value = arena.allocate(16, 8).address();
            long batch = arena.allocate(8, 8).address();
            Unsafe.putLong(value, 71);
            Unsafe.putLong(value + 8, 97);
            // Zero can mean either a column top OR a converted column with non-null values.
            assertFalse(slot.update(new TestRecord(0, 0, 0), value, batch, 1));
            assertFalse(slot.update(new PageFrameFilteredMemoryRecord(), value, batch, 1));
            assertEquals(71, Unsafe.getLong(value));
            assertEquals(97, Unsafe.getLong(value + 8));
        }
    }

    @Test
    public void resolvedNullConstantsRespectGetterWidth() throws Exception {
        var nullValue = io.questdb.griffin.engine.functions.constants.NullConstant.NULL;
        io.questdb.cairo.sql.Function[] functions = {nullValue, nullValue, nullValue, nullValue,
                new io.questdb.griffin.engine.functions.cast.CastByteToIntFunctionFactory.Func(nullValue),
                new io.questdb.griffin.engine.functions.cast.CastShortToLongFunctionFactory.Func(nullValue),
                new io.questdb.griffin.engine.functions.cast.CastIntToLongFunctionFactory.Func(nullValue)};
        int[] reads = {ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG,
                ColumnType.LONG, ColumnType.LONG, ColumnType.LONG};
        var b = new Builder();
        for (int i = 0; i < functions.length; i++) {
            var expression = new ResolvedInputs(new ArrayColumnTypes()).scalar(functions[i], reads[i]);
            assertNotNull(expression);
            var value = expression.emit(new Pipeline.Row(List.of(), List.of()));
            b.store(at(STATE, 8L * i), new Select(value.isNull(), i64(Long.MIN_VALUE), value.data()));
        }
        b.exit(0);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath()); var arena = Arena.ofConfined()) {
            long state = arena.allocate(8L * functions.length, 8).address();
            assertEquals(0, code.run(0, 0, state));
            for (int i = 0; i < functions.length; i++) {
                long expected = switch (reads[i]) {
                    case ColumnType.BYTE -> functions[i].getByte(null);
                    case ColumnType.SHORT -> functions[i].getShort(null);
                    case ColumnType.INT -> io.questdb.std.Numbers.intToLong(functions[i].getInt(null));
                    default -> functions[i].getLong(null);
                };
                assertEquals("getter " + i, expected, Unsafe.getLong(state + 8L * i));
            }
        }
    }

    @Test
    public void rejectsUnresolvedLayoutsOverloadsAndSubclasses() {
        var inputs = new ArrayColumnTypes().add(ColumnType.LONG);
        var values = new ArrayColumnTypes().add(ColumnType.LONG);
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction() { });
        assertNull(ResolvedAggregateKernel.bind(inputs, values, functions));
        functions.clear();
        functions.add(new SumLongGroupByFunction(new LongConstant(10)));
        assertNotNull(ResolvedAggregateKernel.bind(inputs, values, functions));
        functions.clear();
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        assertThrows(IllegalArgumentException.class, () -> ResolvedAggregateKernel.bind(inputs,
                new ArrayColumnTypes().add(ColumnType.INT), functions));
        functions.clear();
        functions.add(new MinIntGroupByFunction(IntColumn.newInstance(0)));
        assertThrows(IllegalArgumentException.class, () -> ResolvedAggregateKernel.bind(
                new ArrayColumnTypes().add(ColumnType.INT), values, functions));
    }

    @Test
    public void bindsOperatorsSelectedByNormalSqlResolution() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString());
        try (var engine = new CairoEngine(config); var context = new SqlExecutionContextImpl(engine, 1).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null)) {
            engine.execute("create table t as (select cast(x%7 as int) k,x v from long_sequence(101))", context);
            try (var factory = Demo.compileSql(engine, context, "select k,count(),count(v),sum(v) from t where v!=42 group by k", true)) {
                RecordCursorFactory current = factory;
                while (!(current instanceof AsyncGroupByRecordCursorFactory)) {
                    current = current.getBaseFactory();
                    assertNotNull("Expected ordinary async group-by", current);
                }
                var async = (AsyncGroupByRecordCursorFactory) current;
                var functions = async.getAtom().getOwnerGroupByFunctions();
                var values = new ArrayColumnTypes();
                for (int i = 0; i < functions.size(); i++) values.add(ColumnType.LONG);
                var binding = ResolvedAggregateKernel.bind(async.getBaseFactory().getMetadata(), values, functions);
                assertNotNull(binding);
                try (var code = binding.compile(backend, temporary.newFolder().toPath())) {
                    assertTrue(code.compilationNanos() > 0);
                }
            }
        }
    }

    @Test
    public void distinctUsesResolvedSizingJavaStateMergeAndCardinality() throws Exception {
        var inputs = new ArrayColumnTypes().add(ColumnType.LONG);
        var functions = new ObjList<GroupByFunction>();
        var distinct = new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 7, 0.6);
        functions.add(distinct);
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        var types = new ArrayColumnTypes();
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
        var layout = new FlyweightPackedMapValue(types);
        try (var allocator = new FastGroupByAllocator(64, 1 << 20);
             var code = ResolvedAggregateKernel.bind(inputs, types, functions).compile(backend, temporary.newFolder().toPath());
             var slot = code.newSlot(allocator); var arena = Arena.ofConfined()) {
            distinct.setAllocator(allocator);
            long nativeValues = arena.allocate(48, 8).address();
            long javaValues = arena.allocate(48, 8).address();
            long input = arena.allocate(8, 8).address();
            long batch = arena.allocate(8, 8).address();
            var record = new TestRecord(input, 0, 0);
            for (int g = 0; g < 2; g++) {
                initialize(functions, layout, nativeValues + g * 24L);
                initialize(functions, layout, javaValues + g * 24L);
            }
            var view = new GroupByLongHashSet(7, 0.6, Long.MIN_VALUE);
            for (int row = 0; row < 500; row++) {
                int group = row % 2;
                long value = row % 13 == 0 ? Long.MIN_VALUE : row / 2 - 30;
                Unsafe.putLong(input, value);
                // Values are already empty-initialized. No first-row bit on repeated updates.
                Unsafe.putLong(batch, Map.encodeBatchEntry(0, group * 24L, false));
                slot.resetStats();
                distinct.resetStats();
                for (int i = 0; i < functions.size(); i++) functions.getQuick(i).computeKeyedBatch(record, layout, javaValues, batch, 1, 0);
                long expectedNew = distinct.getCardinalityStat();
                if (row < 4) {
                    // Continue Java-produced empty/singleton state in native code.
                    for (int i = 0; i < functions.size(); i++) functions.getQuick(i).computeKeyedBatch(record, layout, nativeValues, batch, 1, 0);
                } else {
                    assertTrue(slot.update(record, nativeValues, batch, 1));
                    assertEquals(expectedNew, slot.getCardinalityStat());
                }
                long actual = nativeValues + group * 24L, expected = javaValues + group * 24L;
                assertEquals(Unsafe.getLong(expected), Unsafe.getLong(actual));
                assertEquals(Unsafe.getLong(expected + 16), Unsafe.getLong(actual + 16));
                if (Unsafe.getLong(actual) > 1) {
                    long a = Unsafe.getLong(actual + 8), e = Unsafe.getLong(expected + 8);
                    int capacity = view.of(a).capacity();
                    assertEquals(view.of(e).capacity(), capacity);
                    // Exact layout/hash/probe/growth, not only cardinality equality.
                    equalValues(e, a, 12);
                    equalValues(e + 16, a + 16, capacity * 8L);
                }
            }
            var source = new FlyweightPackedMapValue(types);
            for (long base : new long[]{javaValues, nativeValues}) {
                layout.of(base);
                source.of(base + 24);
                for (int i = 0; i < functions.size(); i++) functions.getQuick(i).merge(layout, source);
            }
            assertEquals(Unsafe.getLong(javaValues), Unsafe.getLong(nativeValues));
            assertEquals(Unsafe.getLong(javaValues + 16), Unsafe.getLong(nativeValues + 16));
            long a = Unsafe.getLong(nativeValues + 8), e = Unsafe.getLong(javaValues + 8);
            equalValues(e, a, 12);
            equalValues(e + 16, a + 16, view.of(a).capacity() * 8L);
        }
        assertThrows(IllegalStateException.class, KernelMemory::allocator);
    }

    @Test
    public void distinctAllocationFailureUnbindsAndDoesNotCloseBorrowedAllocator() throws Exception {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 7, 0.6));
        var types = new ArrayColumnTypes();
        functions.getQuick(0).initValueTypes(types);
        try (var allocator = new FastGroupByAllocator(64, 64);
             var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions)
                     .compile(backend, temporary.newFolder().toPath());
             var slot = code.newSlot(allocator); var arena = Arena.ofConfined()) {
            long values = arena.allocate(16, 8).address();
            long batch = arena.allocate(16, 8).address();
            long input = arena.allocate(16, 8).address();
            Unsafe.putLong(input, 1); Unsafe.putLong(input + 8, 2);
            Unsafe.putLong(batch, Map.encodeBatchEntry(0, 0, true));
            Unsafe.putLong(batch + 8, Map.encodeBatchEntry(1, 0, false));
            assertThrows(CairoException.class, () -> slot.update(new TestRecord(input, 0, 0), values, batch, 2));
            assertThrows(IllegalStateException.class, KernelMemory::allocator);
            assertTrue(allocator.malloc(8) != 0);
        }
    }

    @Test
    public void nativeDistinctCancellationReturnsThroughCallbackAndReleasesBinding() throws Exception {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 3, 0.7));
        var types = new ArrayColumnTypes();
        functions.getQuick(0).initValueTypes(types);
        try (var engine = new CairoEngine(new DefaultCairoConfiguration(temporary.newFolder().toString()));
             var allocator = new FastGroupByAllocator(64, 1 << 20);
             var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions)
                     .compile(backend, temporary.newFolder().toPath());
             var slot = code.newSlot(allocator); var arena = Arena.ofConfined()) {
            var breaker = new AtomicBooleanCircuitBreaker(engine) {
                private int polls;
                @Override public void statefulThrowExceptionIfTrippedNoThrottle() {
                    // The slot entry check and first native poll succeed. Cancel in
                    // the allocation callback after the singleton was written.
                    if (++polls == 3) cancel();
                    super.statefulThrowExceptionIfTrippedNoThrottle();
                }
            };
            long values = arena.allocate(16, 8).address();
            long batch = arena.allocate(16, 8).address();
            long input = arena.allocate(16, 8).address();
            Unsafe.putLong(input, 1); Unsafe.putLong(input + 8, 2);
            Unsafe.putLong(batch, Map.encodeBatchEntry(0, 0, true));
            Unsafe.putLong(batch + 8, Map.encodeBatchEntry(1, 0, false));
            var record = new TestRecord(input, 0, 0);
            var failure = assertThrows(CairoException.class, () -> slot.update(record, values, batch, 2, breaker));
            assertTrue(failure.isInterruption());
            assertEquals("Partial state is retained, never replayed", 1, Unsafe.getLong(values));
            assertThrows(IllegalStateException.class, KernelMemory::allocator);
            breaker.reset();
            Unsafe.putLong(values, 0); Unsafe.putLong(values + 8, 0);
            assertTrue(slot.update(record, values, batch, 2, breaker));
            assertEquals(2, Unsafe.getLong(values));
        }
    }

    @Test
    public void invalidResolvedDistinctSizingFailsBeforeCodeGeneration() {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 3, Double.NaN));
        var types = new ArrayColumnTypes();
        functions.getQuick(0).initValueTypes(types);
        assertThrows(IllegalArgumentException.class,
                () -> ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions));
    }

    @Test
    public void reusedDistinctBatchBridgeDoesNotAllocateJavaObjects() throws Exception {
        var bean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.Assume.assumeTrue(bean.isThreadAllocatedMemorySupported());
        boolean wasEnabled = bean.isThreadAllocatedMemoryEnabled();
        bean.setThreadAllocatedMemoryEnabled(true);
        try {
            var functions = new ObjList<GroupByFunction>();
            functions.add(new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 3, 0.7));
            var types = new ArrayColumnTypes();
            functions.getQuick(0).initValueTypes(types);
            try (var allocator = new FastGroupByAllocator(64, 1 << 20);
                 var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions)
                         .compile(backend, temporary.newFolder().toPath());
                 var slot = code.newSlot(allocator); var arena = Arena.ofConfined()) {
                long values = arena.allocate(16, 8).address();
                long batch = arena.allocate(16, 8).address();
                long input = arena.allocate(16, 8).address();
                Unsafe.putLong(input, 1); Unsafe.putLong(input + 8, 2);
                Unsafe.putLong(batch, Map.encodeBatchEntry(0, 0, false));
                Unsafe.putLong(batch + 8, Map.encodeBatchEntry(1, 0, false));
                var record = new TestRecord(input, 0, 0);
                for (int i = 0; i < 10_000; i++) slot.update(record, values, batch, 2);
                long thread = Thread.currentThread().threadId(), min = Long.MAX_VALUE;
                // Match core's allocation tests: HotSpot may resolve constants in one window,
                // but an allocation in each batch recurs in every window.
                for (int attempt = 0; attempt < 5 && min > 0; attempt++) {
                    long before = bean.getThreadAllocatedBytes(thread);
                    for (int i = 0; i < 10_000; i++) slot.update(record, values, batch, 2);
                    min = Math.min(min, bean.getThreadAllocatedBytes(thread) - before);
                }
                assertEquals("No per-batch ThreadLocal entry, callback or scope allocation", 0, min);
            }
        } finally {
            bean.setThreadAllocatedMemoryEnabled(wasEnabled);
        }
    }

    @Test
    public void generatedUpdatesGrowWithinPreparedBatchesWithoutReplayingRows() throws Exception {
        for (int keyType : new int[]{ColumnType.INT, ColumnType.LONG}) {
            var functions = new ObjList<GroupByFunction>();
            functions.add(new CountLongConstGroupByFunction());
            functions.add(new SumLongGroupByFunction(LongColumn.newInstance(1)));
            var types = new ArrayColumnTypes();
            for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
            try (Map map = unordered(keyType, types); Map ordinary = unordered(keyType, types);
                 var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(keyType).add(ColumnType.LONG), types,
                         functions, QuestMapProbe.bind(0, keyType, map)).compile(backend, temporary.newFolder().toPath());
                 var slot = code.newSlot(); var arena = Arena.ofConfined()) {
                int rows = 5003, width = keyType == ColumnType.INT ? 4 : 8;
                long keys = arena.allocate((long) rows * width, 8).address();
                long values = arena.allocate((long) rows * 8, 8).address();
                var expected = new java.util.HashMap<Long, long[]>();
                for (int i = 0; i < rows; i++) {
                    long k = i % 31 == 0 ? (keyType == ColumnType.INT ? Integer.MIN_VALUE : Long.MIN_VALUE) : i % 2049 - 1024;
                    if (i % 37 == 0) ordinary.reserveCapacity(Math.min(rows - i, 37));
                    key(ordinary, keyType, k).createValue();
                    if (width == 4) Unsafe.putInt(keys + (long) i * width, (int) k); else Unsafe.putLong(keys + (long) i * width, k);
                    long v = i % 13 == 0 ? Long.MIN_VALUE : i;
                    Unsafe.putLong(values + (long) i * 8, v);
                    long[] state = expected.computeIfAbsent(k, ignored -> new long[]{0, Long.MIN_VALUE});
                    state[0]++;
                    if (v != Long.MIN_VALUE) state[1] = state[1] == Long.MIN_VALUE ? v : state[1] + v;
                }
                assertTrue(slot.updateMap(new TestRecord(keys, values, 0), rows, 0, map, 37,
                        io.questdb.cairo.sql.SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                assertEquals(rows, slot.getNativeMapRows());
                assertEquals(expected.size(), map.size());
                assertEquals("Keep the ordinary batch reservation policy", ordinary.getKeyCapacity(), map.getKeyCapacity());
                for (var entry : expected.entrySet()) {
                    var value = key(map, keyType, entry.getKey()).findValue();
                    assertNotNull(value);
                    assertEquals(entry.getValue()[0], value.getLong(0));
                    assertEquals(entry.getValue()[1], value.getLong(1));
                }
            }
        }
    }

    @Test
    public void generatedMergeGrowsAndPublishesMapStateForJavaReaders() throws Exception {
        for (int keyType : new int[]{ColumnType.INT, ColumnType.LONG}) {
            var functions = new ObjList<GroupByFunction>();
            functions.add(new CountLongConstGroupByFunction());
            functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
            var types = new ArrayColumnTypes();
            for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
            try (Map source = unordered(keyType, types); Map destination = unordered(keyType, types);
                 var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions,
                         QuestMapProbe.bind(0, keyType, destination)).compile(backend, temporary.newFolder().toPath());
                 var slot = code.newSlot()) {
                for (int round = 0; round < 2; round++) {
                    source.clear(); destination.clear();
                    for (int k = -2048; k <= 2048; k++) {
                        var value = key(source, keyType, k).createValue();
                        value.putLong(0, 1);
                        value.putLong(1, k % 7 == 0 ? Long.MIN_VALUE : k);
                    }
                    assertTrue(slot.merge(destination, source, null, () -> { }));
                    assertTrue(slot.merge(destination, source, null, () -> { }));
                    assertEquals(4097, destination.size());
                    long visited = 0;
                    try (var cursor = destination.getCursor()) { while (cursor.hasNext()) visited++; }
                    assertEquals(destination.size(), visited);
                    for (int k = -2048; k <= 2048; k++) {
                        var value = key(destination, keyType, k).findValue();
                        assertNotNull(value);
                        assertEquals(2, value.getLong(0));
                        assertEquals(k % 7 == 0 ? Long.MIN_VALUE : 2L * k, value.getLong(1));
                    }
                    assertTrue(key(destination, keyType, 5000).createValue().isNew());
                    assertEquals(4098, destination.size());
                }
            }
        }
    }

    @Test
    public void failedGeneratedRehashRetainsOldBackingAndBalancesAllocation() throws Exception {
        var types = new ArrayColumnTypes().add(ColumnType.LONG);
        try (Map map = unordered(ColumnType.LONG, types)) {
            for (int k = 0; k < 8; k++) key(map, ColumnType.LONG, k).createValue().putLong(0, k + 10);
            long bytes = Unsafe.getMemUsed();
            assertThrows(IllegalStateException.class, () -> map.growNative((oldBase, oldCapacity, newBase, newCapacity, stride) -> {
                Unsafe.putLong(newBase, 1234); // A partially relocated table must never become visible.
                throw new IllegalStateException("injected rehash failure");
            }));
            assertEquals(bytes, Unsafe.getMemUsed());
            assertEquals(8, map.size());
            for (int k = 0; k < 8; k++) assertEquals(k + 10, key(map, ColumnType.LONG, k).findValue().getLong(0));
        }
    }

    @Test
    public void cancelledNativeMergePublishesPartialStateAndSlotCanBeReused() throws Exception {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction());
        var types = new ArrayColumnTypes();
        functions.getQuick(0).initValueTypes(types);
        for (boolean ordered : new boolean[]{false, true}) {
        try (Map source = ordered ? new io.questdb.cairo.map.OrderedMap(128, new ArrayColumnTypes().add(ColumnType.LONG), types, 16, 0.6, 30) : unordered(ColumnType.LONG, types);
             Map destination = ordered ? new io.questdb.cairo.map.OrderedMap(128, new ArrayColumnTypes().add(ColumnType.LONG), types, 16, 0.6, 30) : unordered(ColumnType.LONG, types);
             var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions,
                     (ordered ? QuestMapProbe.fixed(fixedKey(ColumnType.LONG), ColumnType.LONG, true)
                             : QuestMapProbe.bind(0, ColumnType.LONG, destination))).compile(backend, temporary.newFolder().toPath());
             var slot = code.newSlot()) {
            for (int k = 0; k < 4097; k++) key(source, ColumnType.LONG, k).createValue().putLong(0, 1);
            int[] polls = {0};
            assertThrows(CairoException.class, () -> slot.merge(destination, source, null, () -> {
                if (++polls[0] == 5) throw CairoException.queryCancelled();
            }));
            assertTrue(destination.size() > 0 && destination.size() < source.size());
            long visited = 0;
            try (var cursor = destination.getCursor()) {
                while (cursor.hasNext()) { visited++; assertEquals(1, cursor.getRecord().getLong(0)); }
            }
            assertEquals(destination.size(), visited);
            assertThrows(IllegalStateException.class, KernelMemory::allocator);
            destination.clear();
            assertTrue(slot.merge(destination, source, null, () -> { }));
            assertEquals(source.size(), destination.size());
        }
        }
    }

    @Test
    public void topKPreservesEngineOrderingTiesMergeAndCancellation() throws Exception {
        for (int keyCode : new int[]{ColumnType.INT, ColumnType.LONG, -ColumnType.INT, -ColumnType.LONG}) {
            int keyType = Math.abs(keyCode);
            var functions = new ObjList<GroupByFunction>();
            functions.add(new CountLongConstGroupByFunction());
            functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
            var types = new ArrayColumnTypes();
            for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
            try (var allocator = new FastGroupByAllocator(64, 1 << 20);
                 Map map = keyCode > 0 ? unordered(keyType, types) : new io.questdb.cairo.map.OrderedMap(128, new ArrayColumnTypes().add(keyType), types, 16, 0.6, 30);
                 var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions,
                         (keyCode > 0 ? QuestMapProbe.bind(0, keyType, map) : QuestMapProbe.fixed(fixedKey(keyType), keyType, true))).compile(backend, temporary.newFolder().toPath());
                 var slot = code.newSlot(allocator)) {
                code.prepareLongTopK(functions.getQuick(0));
                for (int k = -2048; k <= 2048; k++) {
                    var value = key(map, keyType, k).createValue();
                    value.putLong(0, k % 97); // ties, negative values, zero key
                    value.putLong(1, k % 11 == 0 ? Long.MIN_VALUE : k % 13 == 0 ? Long.MAX_VALUE : k * 12345L);
                }
                var nullKey = key(map, keyType, keyType == ColumnType.INT ? Integer.MIN_VALUE : Long.MIN_VALUE).createValue();
                nullKey.putLong(0, 42); nullKey.putLong(1, Long.MIN_VALUE);
                for (int order = 0; order < 2; order++) {
                    for (int limit : new int[]{1, 10, 65, 66, 128, 5000}) {
                        try (var expected = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT);
                             var actual = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT);
                             var merged = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT)) {
                            for (int f = 0; f < functions.size(); f++) {
                                expected.clear(); actual.clear(); merged.clear();
                                map.getCursor().longTopK(expected, functions.getQuick(f));
                                assertTrue(slot.longTopK(map, actual, functions.getQuick(f), () -> { }));
                                assertTopKEquals(expected, actual);
                                assertTrue(slot.mergeLongTopK(merged, actual, () -> { }));
                                assertTopKEquals(expected, merged);
                                // Merging repeatedly must match ordinary add, including cutoff ties.
                                var cursor = actual.getCursor(); cursor.toTop();
                                while (cursor.hasNext()) expected.add(cursor.index(), cursor.value());
                                assertTrue(slot.mergeLongTopK(merged, actual, () -> { }));
                                assertTopKEquals(expected, merged);
                            }
                            actual.clear();
                            int[] polls = {0};
                            assertThrows(CairoException.class, () -> slot.longTopK(map, actual, functions.getQuick(0), () -> {
                                if (++polls[0] == 2) throw CairoException.queryCancelled();
                            }));
                            if (limit <= 64) assertTrue(actual.size() > 0);
                            assertThrows(IllegalStateException.class, KernelMemory::allocator);
                            actual.close(); actual.reopen(); expected.clear();
                            map.getCursor().longTopK(expected, functions.getQuick(0));
                            assertTrue(slot.longTopK(map, actual, functions.getQuick(0), () -> { }));
                            assertTopKEquals(expected, actual);
                        }
                    }
                }
                map.clear();
                try (var empty = DirectLongLongSortedList.getInstance(0, 10, MemoryTag.NATIVE_DEFAULT)) {
                    assertTrue(slot.longTopK(map, empty, functions.getQuick(0), () -> { }));
                    assertEquals(0, empty.size());
                }
            }
        }
    }

    @Test
    public void specializedTopKPreservesTiesAndDeclinesOtherDirectionBeforeMutation() throws Exception {
        assertSpecializedTopK(0);
    }

    @Test
    public void capacitySpecializedTopKPreservesBothAlgorithmsAndDeclinesOtherClass() throws Exception {
        assertSpecializedTopK(64);
        assertSpecializedTopK(65);
    }

    private void assertSpecializedTopK(int preparedLimit) throws Exception {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(0)));
        var types = new ArrayColumnTypes();
        functions.getQuick(0).initValueTypes(types);
        for (int order = 0; order < 2; order++) {
            try (var allocator = new FastGroupByAllocator(64, 1 << 20);
                 Map map = unordered(ColumnType.INT, types);
                 var code = ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), types, functions,
                         QuestMapProbe.bind(0, ColumnType.INT, map)).compile(backend, temporary.newFolder().toPath());
                 var slot = code.newSlot(allocator)) {
                code.prepareLongTopK(functions.getQuick(0), order, preparedLimit);
                for (int k = -4096; k <= 4096; k++) {
                    var value = key(map, ColumnType.INT, k).createValue();
                    value.putLong(0, k % 11 == 0 ? Long.MIN_VALUE : k % 13 == 0 ? Long.MAX_VALUE : k % 97);
                }
                for (int limit : new int[]{1, 64, 65, 1024, 5000}) {
                    try (var expected = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT);
                         var actual = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT);
                         var merged = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT);
                         var wrong = DirectLongLongSortedList.getInstance(1 - order, limit, MemoryTag.NATIVE_DEFAULT)) {
                        if (preparedLimit != 0 && (preparedLimit <= 64) != (limit <= 64)) {
                            actual.add(1234, 5678);
                            assertFalse(slot.longTopK(map, actual, functions.getQuick(0), () -> { }));
                            assertFalse(slot.mergeLongTopK(actual, merged, () -> { }));
                            assertEquals(1, actual.size());
                            var row = actual.getCursor(); row.toTop(); assertTrue(row.hasNext());
                            assertEquals(1234, row.index()); assertEquals(5678, row.value());
                            continue;
                        }
                        map.getCursor().longTopK(expected, functions.getQuick(0));
                        assertTrue(slot.longTopK(map, actual, functions.getQuick(0), () -> { }));
                        assertTopKEquals(expected, actual);
                        assertTrue(slot.mergeLongTopK(merged, actual, () -> { }));
                        assertTopKEquals(expected, merged);
                        wrong.add(1234, 5678);
                        assertFalse(slot.longTopK(map, wrong, functions.getQuick(0), () -> { }));
                        assertFalse(slot.mergeLongTopK(wrong, actual, () -> { }));
                        assertEquals(1, wrong.size());
                        var row = wrong.getCursor(); row.toTop(); assertTrue(row.hasNext());
                        assertEquals(1234, row.index()); assertEquals(5678, row.value());
                        actual.clear();
                        int[] polls = {0};
                        assertThrows(CairoException.class, () -> slot.longTopK(map, actual, functions.getQuick(0), () -> {
                            if (++polls[0] == 2) throw CairoException.queryCancelled();
                        }));
                        actual.close(); actual.reopen();
                        assertTrue(slot.longTopK(map, actual, functions.getQuick(0), () -> { }));
                        assertTopKEquals(expected, actual);
                    }
                }
            }
        }
    }

    private static void assertTopKEquals(DirectLongLongSortedList expected, DirectLongLongSortedList actual) {
        assertEquals(expected.size(), actual.size());
        var a = expected.getCursor(); var b = actual.getCursor(); a.toTop(); b.toTop();
        while (a.hasNext()) {
            assertTrue(b.hasNext());
            assertEquals(a.value(), b.value());
            assertEquals(a.index(), b.index());
        }
        assertFalse(b.hasNext());
    }

    @Test
    public void resolvedFilterMatchesJavaForNullsWideningBooleanTreesAndTails() throws Exception {
        var types = new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.INT).add(ColumnType.SHORT);
        var left = LongColumn.newInstance(0);
        var right = IntColumn.newInstance(1);
        var lt = new io.questdb.griffin.engine.functions.lt.LtLongFunctionFactory.LtLongFunction(left, right);
        var ge = new io.questdb.griffin.engine.functions.lt.LtLongFunctionFactory.LtLongFunction(left, right); ge.setNegated();
        var eq = new io.questdb.griffin.engine.functions.eq.EqLongFunctionFactory.Func(left, right);
        var ne = new io.questdb.griffin.engine.functions.eq.EqLongFunctionFactory.Func(left, right); ne.setNegated();
        var nullEq = new io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory.Func(right, io.questdb.griffin.engine.functions.constants.IntConstant.NULL);
        var shortEq = new io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory.Func(ShortColumn.newInstance(2), new io.questdb.griffin.engine.functions.constants.ShortConstant((short) 0));
        var predicates = List.of(lt, ge, eq, ne, nullEq, shortEq,
                new io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory.FuncLongIsNaN(left),
                new io.questdb.griffin.engine.functions.eq.EqDoubleFunctionFactory.FuncIntIsNaN(right),
                new io.questdb.griffin.engine.functions.bool.AndFunctionFactory.AndBooleanFunction(ge, ne),
                new io.questdb.griffin.engine.functions.bool.OrFunctionFactory.MyBooleanFunction(lt, nullEq),
                new io.questdb.griffin.engine.functions.bool.NotFunctionFactory.Func(lt),
                io.questdb.griffin.engine.functions.constants.BooleanConstant.FALSE,
                io.questdb.griffin.engine.functions.constants.BooleanConstant.TRUE);
        try (var arena = Arena.ofConfined(); var rows = new io.questdb.std.DirectLongList(1, MemoryTag.NATIVE_DEFAULT)) {
            int count = 1031;
            long longs = arena.allocate(8L * count, 8).address();
            long ints = arena.allocate(4L * count, 4).address();
            long shorts = arena.allocate(2L * count, 2).address();
            long[] values = {Long.MIN_VALUE, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE, Long.MAX_VALUE};
            for (int i = 0; i < count; i++) {
                Unsafe.putLong(longs + 8L * i, values[i % values.length]);
                Unsafe.putInt(ints + 4L * i, (int) values[(i / values.length) % values.length]);
                Unsafe.putShort(shorts + 2L * i, (short) (i % 3 - 1));
            }
            var record = new TestRecord(longs, ints, shorts);
            for (var predicate : predicates) {
                try (var code = ResolvedFilter.compile(types, predicate, backend, temporary.newFolder().toPath()); var slot = code.newSlot()) {
                    for (int n : new int[]{0, 1, 63, 64, 65, count}) {
                        rows.clear();
                        assertTrue(slot.filter(record, n, rows, io.questdb.cairo.sql.SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                        int selected = 0;
                        for (int i = 0; i < n; i++) {
                            record.setRowIndex(i);
                            if (predicate.getBool(record)) assertEquals(i, rows.get(selected++));
                        }
                        assertEquals(selected, rows.size());
                    }
                }
            }
            try (var engine = new CairoEngine(new DefaultCairoConfiguration(temporary.newFolder().toString()));
                 var code = ResolvedFilter.compile(types, lt, backend, temporary.newFolder().toPath()); var slot = code.newSlot()) {
                rows.clear(); rows.add(77);
                assertFalse(slot.filter(new TestRecord(0, ints, shorts), count, rows, io.questdb.cairo.sql.SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
                assertEquals(1, rows.size()); assertEquals(77, rows.get(0));
                var breaker = new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine); breaker.cancel();
                assertThrows(CairoException.class, () -> slot.filter(record, count, rows, breaker));
                assertThrows(IllegalStateException.class, KernelMemory::allocator);
                rows.clear();
                assertTrue(slot.filter(record, count, rows, io.questdb.cairo.sql.SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            }
        }
    }

    @Test
    public void orderedOffsetsRemainUnsignedThroughNativeProbeAndRehash() throws Exception {
        var key = fixedKey(ColumnType.LONG);
        var probe = QuestMapProbe.fixed(key, -1, true).mutating();
        var b = new Ir.Builder();
        var found = probe.find(b, b.let(Ir.COLUMNS), b.let(Ir.i64(0)), Ir.i64(0), null, (init, value) -> { });
        b.exit(Ir.load(found));
        try (var lookup = backend.compile(new Ir.Program(b.block()), temporary.newFolder().toPath());
             var rehash = backend.compile(QuestOrderedMap.rehash(), temporary.newFolder().toPath());
             var arena = Arena.ofConfined()) {
            long input = arena.allocate(8, 8).address(), entry = arena.allocate(16, 8).address();
            long state = arena.allocate(128, 8).address(), offsets = arena.allocate(64, 8).address();
            long target = arena.allocate(128, 8).address(), args = arena.allocate(32, 8).address();
            Unsafe.putLong(input, -987654321); Unsafe.putLong(entry, -987654321); Unsafe.putLong(entry + 8, 123);
            long hash = io.questdb.std.Hash.hashMem64(input, 8);
            for (long raw : new long[]{1, 0x80000000L, 0xffffffffL}) {
                for (int i = 0; i < 8; i++) Unsafe.putLong(offsets + 8L * i, 0);
                for (int i = 0; i < 16; i++) Unsafe.putLong(target + 8L * i, 0);
                long packed = raw | (hash << 32);
                Unsafe.putLong(offsets + (hash & 7) * 8, packed);
                Unsafe.putLong(state + 64, entry - (raw - 1) * 8);
                Unsafe.putLong(state + 72, offsets); Unsafe.putLong(state + 80, 7);
                Unsafe.putLong(state + 112, 1); // A mistaken miss returns before mutation.
                assertEquals(123, lookup.run(input, 0, state));
                Unsafe.putLong(args, offsets); Unsafe.putLong(args + 8, target); Unsafe.putLong(args + 16, 15);
                try (var memory = KernelMemory.bind(null, () -> { })) {
                    assertEquals(0, rehash.run(args, 8, 0)); memory.throwIfFailed();
                }
                assertEquals(packed, Unsafe.getLong(target + (hash & 15) * 8));
            }
        }
    }

    @Test
    public void orderedRehashFailureKeepsOwnershipAfterHeapGrowth() {
        var keys = new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.INT);
        var values = new ArrayColumnTypes().add(ColumnType.LONG);
        try (var arena = Arena.ofConfined();
             var map = new io.questdb.cairo.map.OrderedMap(432, keys, values, 16, 0.6, 30, MemoryTag.NATIVE_DEFAULT)) {
            long view = arena.allocate(64, 8).address();
            int count = 0;
            map.writeNativeMutationView(view);
            while (Unsafe.getLong(view + 48) > 1) {
                var key = map.withKey(); key.putLong(count); key.putInt(-count);
                key.createValue().putLong(0, count * 7L); count++;
                map.writeNativeMutationView(view);
            }
            long heapBefore = Unsafe.getLong(view + 40) - Unsafe.getLong(view);
            long memoryBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            int expectedCount = count;
            assertThrows(IllegalStateException.class, () -> map.growNative((old, oldCapacity, next, newCapacity, stride) -> {
                assertEquals(8, stride);
                throw new IllegalStateException("injected relocation failure");
            }));
            map.writeNativeMutationView(view);
            long heapAfter = Unsafe.getLong(view + 40) - Unsafe.getLong(view);
            assertEquals(heapAfter - heapBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - memoryBefore);
            assertEquals(expectedCount, map.size());
            for (int i = 0; i < count; i++) {
                var key = map.withKey(); key.putLong(i); key.putInt(-i);
                assertEquals(i * 7L, key.findValue().getLong(0));
            }
        }
    }

    private static Map unordered(int type, ArrayColumnTypes values) {
        return type == ColumnType.INT ? new io.questdb.cairo.map.Unordered4Map(type, values, 16, 0.6, 30)
                : new io.questdb.cairo.map.Unordered8Map(type, values, 16, 0.6, 30);
    }

    private static io.questdb.cairo.map.MapKey key(Map map, int type, long value) {
        var key = map.withKey();
        if (type == ColumnType.INT) key.putInt((int) value); else key.putLong(value);
        return key;
    }

    private static void initialize(ObjList<GroupByFunction> functions, FlyweightPackedMapValue value, long address) {
        value.of(address);
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).setEmpty(value);
    }

    private static void equalValues(long expected, long actual, long bytes) {
        for (long offset = 0; offset < bytes; offset++) assertEquals("byte " + offset,
                Unsafe.getUnsafe().getByte(expected + offset), Unsafe.getUnsafe().getByte(actual + offset));
    }

    static final class TestRecord extends PageFrameMemoryRecord {
        private final long[] addresses;

        TestRecord(long longs, long ints, long shorts) {
            addresses = new long[]{longs, ints, shorts};
        }

        @Override public long getPageAddress(int column) { return addresses[column]; }
        @Override public long getLong(int column) { return addresses[column] == 0 ? Long.MIN_VALUE : Unsafe.getLong(addresses[column] + rowIndex * 8); }
        @Override public int getInt(int column) { return addresses[column] == 0 ? Integer.MIN_VALUE : Unsafe.getInt(addresses[column] + rowIndex * 4); }
        @Override public short getShort(int column) { return addresses[column] == 0 ? 0 : Unsafe.getShort(addresses[column] + rowIndex * 2); }
    }
    private static QuestFixedKey fixedKey(int type) {
        var column = new Pipeline.Column(0, "key", type);
        return new QuestFixedKey(new int[]{0}, List.of(column), List.of(column),
                new Ir.MemoryType[]{column.width()}, ColumnType.sizeOf(type));
    }

}
