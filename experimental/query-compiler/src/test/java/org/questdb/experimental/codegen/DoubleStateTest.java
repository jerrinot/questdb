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
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
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

    @Test public void booleanStatesPreserveEmptyMergeAndCountWrap() throws Exception {
        var argument = io.questdb.griffin.engine.functions.columns.BooleanColumn.newInstance(0);
        for (GroupByFunction function : List.of(new BoolAndGroupByFunction(argument),new BoolOrGroupByFunction(argument))) {
            try (function; var arena = Arena.ofConfined()) {
                var types = new ArrayColumnTypes(); function.initValueTypes(types);
                var inputTypes = new ArrayColumnTypes(); inputTypes.add(ColumnType.BOOLEAN);
                var functions = new ObjList<GroupByFunction>(); functions.add(function);
                var layout = new FlyweightPackedMapValue(types);
                var updates = AggregateUpdates.bind(inputTypes,types,functions,layout::getOffset,false);
                assertNotNull(updates);
                var b = new Builder();
                var row = new Pipeline.Row(updates.columns(),List.of(updates.columns().getFirst().value(new Load(MemoryType.I8,COLUMNS))));
                updates.emitInitialized(b,row,STATE,nil()); b.exit(0);
                var merge = new Builder(); updates.merge(merge,COLUMNS,STATE); merge.exit(0);
                var finish = new Builder(); updates.finish(finish,STATE,false); finish.exit(0);
                try (var code = backend.compile(new Program(b.block()),temporary.newFolder().toPath());
                     var merger = backend.compile(new Program(merge.block()),temporary.newFolder().toPath());
                     var finalizer = backend.compile(new Program(finish.block()),temporary.newFolder().toPath())) {
                    long actualAddress=arena.allocate(16,8).address(),sourceAddress=arena.allocate(16,8).address();
                    var actual=state(types,actualAddress); var expected=state(types,arena.allocate(16,8).address());
                    var source=state(types,sourceAddress); long input=arena.allocate(8,8).address();
                    function.setEmpty(actual); function.setEmpty(expected);
                    for (int n=0;n<12;n++) {
                        boolean value=n%3==0;
                        Record record=new Record() { @Override public boolean getBool(int ignored) { return value; } };
                        if (n==0) function.computeFirst(expected,record,n); else function.computeNext(expected,record,n);
                        if (n%3==1) function.computeNext(actual,record,n);
                        else { Unsafe.putByte(input,(byte)(value?1:0)); assertEquals(0,code.run(input,1,actualAddress)); }
                        assertState(function.getName(),types,expected,actual);
                    }
                    for (long left : new long[]{0,1,Long.MAX_VALUE,-1}) for (long right : new long[]{0,1,Long.MIN_VALUE,-1}) {
                        for (int bits=0;bits<4;bits++) {
                            actual.putBool(0,(bits&1)!=0); expected.putBool(0,(bits&1)!=0);
                            actual.putLong(1,left); expected.putLong(1,left);
                            source.putBool(0,(bits&2)!=0); source.putLong(1,right);
                            function.merge(expected,source); assertEquals(0,merger.run(sourceAddress,0,actualAddress));
                            assertState(function.getName(),types,expected,actual);
                            boolean result=function.getBool(new Record() {
                                @Override public boolean getBool(int index) { return expected.getBool(index); }
                                @Override public long getLong(int index) { return expected.getLong(index); }
                            });
                            assertEquals(0,finalizer.run(0,0,actualAddress)); assertEquals(result,actual.getBool(0));
                        }
                    }
                    for (long count : new long[]{Long.MAX_VALUE,-1}) {
                        function.setEmpty(actual); function.setEmpty(expected); actual.putLong(1,count); expected.putLong(1,count);
                        Record record=new Record() { @Override public boolean getBool(int ignored) { return true; } };
                        function.computeNext(expected,record,0); Unsafe.putByte(input,(byte)1);
                        assertEquals(0,code.run(input,1,actualAddress)); assertState(function.getName(),types,expected,actual);
                    }
                }
            }
        }
    }

    @Test public void mixedUpdatesAndMergesPreserveEachOrdinaryNonfiniteRule() throws Exception {
        double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                0x1p40, 0x1p40+0.25, 0x1p40-0.25, 0x1p40+0.5, Double.NaN, 0x1p40-0.5,
                0.0, -0.0, Double.MIN_VALUE, -Double.MIN_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, 17.25,
                Double.longBitsToDouble(0x7ff8000000001234L), -17.25,
                Float.MIN_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE, 0x1p24, 1.0, 1.0, -0x1p24};
        for (GroupByFunction function : List.of(new CountDoubleGroupByFunction(DoubleColumn.newInstance(0)),
                new SumDoubleGroupByFunction(DoubleColumn.newInstance(0)), new NSumDoubleGroupByFunction(DoubleColumn.newInstance(0)), new AvgDoubleGroupByFunction(DoubleColumn.newInstance(0)),
                new MinDoubleGroupByFunction(DoubleColumn.newInstance(0)), new MaxDoubleGroupByFunction(DoubleColumn.newInstance(0)),
                new CountFloatGroupByFunction(FloatColumn.newInstance(0)), new SumFloatGroupByFunction(FloatColumn.newInstance(0)),
                new MinFloatGroupByFunction(FloatColumn.newInstance(0)), new MaxFloatGroupByFunction(FloatColumn.newInstance(0)),
                new AvgDoubleGroupByFunction(FloatColumn.newInstance(0)),
                statistic(new VarSampleGroupByFunctionFactory()), statistic(new VarPopGroupByFunctionFactory()),
                statistic(new StdDevSampleGroupByFunctionFactory()), statistic(new StdDevPopGroupByFunctionFactory()))) {
            try (function; var arena = Arena.ofConfined()) {
                var types = new ArrayColumnTypes(); function.initValueTypes(types);
                var inputTypes = new ArrayColumnTypes(); inputTypes.add(((UnaryFunction) function).getArg().getType());
                var functions = new ObjList<GroupByFunction>(); functions.add(function);
                var layout = new FlyweightPackedMapValue(types);
                var updates = AggregateUpdates.bind(inputTypes, types, functions, layout::getOffset, false);
                assertNotNull(updates);
                var b = new Builder();
                var column = updates.columns().getFirst();
                var row = new Pipeline.Row(updates.columns(), List.of(column.value(new Load(column.width(), COLUMNS))));
                updates.emitInitialized(b, row, STATE, nil()); b.exit(0);
                var merge = new Builder(); updates.merge(merge, COLUMNS, STATE); merge.exit(0);
                var finish = new Builder(); updates.finish(finish, STATE, false); finish.exit(0);
                var fresh = new Builder(); updates.initializeEmptyState(fresh, STATE, bool(true)); fresh.exit(0);
                try (var initializer = backend.compile(new Program(fresh.block()), temporary.newFolder().toPath());
                     var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
                     var merger = backend.compile(new Program(merge.block()), temporary.newFolder().toPath());
                     var finalizer = backend.compile(new Program(finish.block()), temporary.newFolder().toPath())) {
                    long actualAddress = arena.allocate(32, 8).address();
                    var actual = state(types, actualAddress);
                    var expected = state(types, arena.allocate(32, 8).address());
                    long sourceAddress = arena.allocate(32, 8).address();
                    var source = state(types, sourceAddress);
                    long input = arena.allocate(8, 8).address();
                    function.setEmpty(expected); function.setEmpty(actual);
                    assertEquals(0, initializer.run(0, 0, actualAddress));
                    for (int i = 0; i < values.length; i++) {
                        Record record = record(values[i]);
                        if (i == 0) function.computeFirst(expected, record, i); else function.computeNext(expected, record, i);
                        if (i % 3 == 1) function.computeNext(actual, record, i);
                        else {
                            if (column.width() == MemoryType.F32) Unsafe.putFloat(input, (float) values[i]);
                            else Unsafe.putDouble(input, values[i]);
                            assertEquals(0, code.run(input, 1, actualAddress));
                        }
                        assertState(function.getName(), types, expected, actual);
                    }
                    for (int left : new int[]{0, 1, 2, 3, 9, values.length}) for (int right : new int[]{0, 1, 2, 3, 9, values.length}) {
                        populate(function, expected, values, left); populate(function, actual, values, left);
                        populate(function, source, values, right);
                        function.merge(expected, source);
                        assertEquals(0, merger.run(sourceAddress, 0, actualAddress));
                        assertState(function.getName(), types, expected, actual);
                        if (updates.updates().stream().anyMatch(AggregateUpdates.Update::finalizesResult)) {
                            double result = function.getDouble(new Record() {
                                @Override public double getDouble(int index) { return expected.getDouble(index); }
                                @Override public long getLong(int index) { return expected.getLong(index); }
                            });
                            assertEquals(0, finalizer.run(0, 0, actualAddress));
                            assertEquals(function.getName(), Double.doubleToLongBits(result), Double.doubleToLongBits(actual.getDouble(0)));
                        }
                    }
                }
            }
        }
    }

    @Test public void regressionStatesPreserveOrdinaryUpdatesMergesAndFinalization() throws Exception {
        double[][] pairs={{Double.NaN,1},{1,Double.POSITIVE_INFINITY},{3,Double.NEGATIVE_INFINITY},
                {-0.0,0.0},{0.0,-0.0},{0x1p40,-0x1p40},{0x1p40+0.25,-0x1p40+0.5},
                {0x1p40-0.25,-0x1p40-0.5},{Double.MIN_VALUE,-Double.MIN_VALUE},
                {Double.MAX_VALUE,Double.MAX_VALUE},{-Double.MAX_VALUE,Double.MAX_VALUE},{3.25,-7.5}};
        for(var factory:List.of(new CorrGroupByFunctionFactory(),new RegressionSlopeFunctionFactory(),
                new RegressionInterceptFunctionFactory(),new RegressionR2FunctionFactory())) {
            var arguments=new ObjList<io.questdb.cairo.sql.Function>();arguments.add(DoubleColumn.newInstance(0));arguments.add(DoubleColumn.newInstance(1));
            try(var function=(GroupByFunction)factory.newInstance(0,arguments,null,null,null);var arena=Arena.ofConfined()) {
                var types=new ArrayColumnTypes();function.initValueTypes(types);
                var inputTypes=new ArrayColumnTypes();inputTypes.add(ColumnType.DOUBLE);inputTypes.add(ColumnType.DOUBLE);
                var functions=new ObjList<GroupByFunction>();functions.add(function);
                var layout=new FlyweightPackedMapValue(types);
                var updates=AggregateUpdates.bind(inputTypes,types,functions,layout::getOffset,false);assertNotNull(updates);
                var values=new java.util.ArrayList<Pipeline.Value>();
                for(var column:updates.columns())values.add(column.value(new Load(column.width(),at(COLUMNS,column.index()*8L))));
                var row=new Pipeline.Row(updates.columns(),values);
                var first=new Builder();updates.initialize(first,STATE);updates.emitInitialized(first,row,STATE,nil());first.exit(0);
                var next=new Builder();updates.emitInitialized(next,row,STATE,nil());next.exit(0);
                var merge=new Builder();updates.merge(merge,COLUMNS,STATE);merge.exit(0);
                var finish=new Builder();updates.finish(finish,STATE,false);finish.exit(0);
                try(var firstCode=backend.compile(new Program(first.block()),temporary.newFolder().toPath());
                    var nextCode=backend.compile(new Program(next.block()),temporary.newFolder().toPath());
                    var merger=backend.compile(new Program(merge.block()),temporary.newFolder().toPath());
                    var finalizer=backend.compile(new Program(finish.block()),temporary.newFolder().toPath())) {
                    long address=arena.allocate(48,8).address(),sourceAddress=arena.allocate(48,8).address(),input=arena.allocate(16,8).address();
                    var actual=state(types,address);var expected=state(types,arena.allocate(48,8).address());var source=state(types,sourceAddress);
                    for(int start=0;start<pairs.length;start++) {
                        function.setEmpty(actual);function.setEmpty(expected);
                        for(int i=start;i<pairs.length;i++) {
                            if(i==start)function.computeFirst(expected,pair(pairs[i]),i);else function.computeNext(expected,pair(pairs[i]),i);
                            if(i>start&&i%3==1)function.computeNext(actual,pair(pairs[i]),i);
                            else {
                                Unsafe.putDouble(input,pairs[i][0]);Unsafe.putDouble(input+8,pairs[i][1]);
                                assertEquals(0,(i==start?firstCode:nextCode).run(input,1,address));
                            }
                            assertState(function.getName(),types,expected,actual);
                        }
                    }
                    for(int left:new int[]{0,1,3,5,8,pairs.length})for(int right:new int[]{0,1,3,5,8,pairs.length}) {
                        populatePairs(function,actual,pairs,left);populatePairs(function,expected,pairs,left);populatePairs(function,source,pairs,right);
                        function.merge(expected,source);assertEquals(0,merger.run(sourceAddress,0,address));assertState(function.getName(),types,expected,actual);
                        double result=function.getDouble(new Record(){
                            @Override public double getDouble(int index){return expected.getDouble(index);}
                            @Override public long getLong(int index){return expected.getLong(index);}
                        });
                        assertEquals(0,finalizer.run(0,0,address));assertEquals(Double.doubleToLongBits(result),Double.doubleToLongBits(actual.getDouble(0)));
                    }
                    for(long count:new long[]{1L<<32,(1L<<53)+1,Long.MAX_VALUE-3,Long.MIN_VALUE}) {
                        populatePairs(function,actual,pairs,8);populatePairs(function,expected,pairs,8);populatePairs(function,source,pairs,7);
                        actual.putLong(5,count);expected.putLong(5,count);source.putLong(5,count);
                        function.merge(expected,source);assertEquals(0,merger.run(sourceAddress,0,address));assertState(function.getName(),types,expected,actual);
                        function.computeNext(expected,pair(new double[]{9.5,2.75}),1);Unsafe.putDouble(input,9.5);Unsafe.putDouble(input+8,2.75);
                        assertEquals(0,nextCode.run(input,1,address));assertState(function.getName(),types,expected,actual);
                    }
                    // Result projection: overflow/underflow products, zero variance, clamp and singleton rules.
                    for(double[] state:new double[][]{{2,1e308,3,1e308,1e308},{2,1e-300,3,1e-300,1e-300},
                            {2,0,3,1,0},{2,1,3,0,0},{2,1,3,1,Math.nextUp(1.0)},
                            {2,1,3,1,-Math.nextUp(1.0)},{2,-1,3,1,1},{2,Double.NaN,3,1,1}}) {
                        for(long count:new long[]{0,1,2,Long.MAX_VALUE}) {
                            for(int i=0;i<5;i++){actual.putDouble(i,state[i]);expected.putDouble(i,state[i]);}
                            actual.putLong(5,count);expected.putLong(5,count);
                            double result=function.getDouble(new Record(){
                                @Override public double getDouble(int index){return expected.getDouble(index);}
                                @Override public long getLong(int index){return expected.getLong(index);}
                            });
                            assertEquals(0,finalizer.run(0,0,address));assertEquals(function.getName(),Double.doubleToLongBits(result),Double.doubleToLongBits(actual.getDouble(0)));
                        }
                    }
                }
            }
        }
    }

    @Test public void zeroCountAfterMergeIsNotANewStatisticalState() throws Exception {
        for(var factory:List.of(new VarPopGroupByFunctionFactory(),new VarSampleGroupByFunctionFactory(),
                new StdDevPopGroupByFunctionFactory(),new StdDevSampleGroupByFunctionFactory(),
                new CovarPopGroupByFunctionFactory(),new CovarSampleGroupByFunctionFactory())) {
            boolean covariance=factory instanceof CovarPopGroupByFunctionFactory || factory instanceof CovarSampleGroupByFunctionFactory;
            var arguments=new ObjList<io.questdb.cairo.sql.Function>();arguments.add(DoubleColumn.newInstance(0));
            if(covariance)arguments.add(DoubleColumn.newInstance(1));
            try(var function=(GroupByFunction)factory.newInstance(0,arguments,null,null,null);var arena=Arena.ofConfined()) {
                var types=new ArrayColumnTypes();function.initValueTypes(types);
                var inputs=new ArrayColumnTypes();inputs.add(ColumnType.DOUBLE);if(covariance)inputs.add(ColumnType.DOUBLE);
                var functions=new ObjList<GroupByFunction>();functions.add(function);
                var layout=new FlyweightPackedMapValue(types);
                var updates=AggregateUpdates.bind(inputs,types,functions,layout::getOffset,false);assertNotNull(updates);
                var b=new Builder();var values=new java.util.ArrayList<Pipeline.Value>();
                for(var column:updates.columns())values.add(column.value(new Load(column.width(),at(COLUMNS,column.index()*8L))));
                updates.emitInitialized(b,new Pipeline.Row(updates.columns(),values),STATE,nil());b.exit(0);
                try(var code=backend.compile(new Program(b.block()),temporary.newFolder().toPath())) {
                    long address=arena.allocate(32,8).address(),input=arena.allocate(16,8).address();
                    var actual=state(types,address);var expected=state(types,arena.allocate(32,8).address());
                    var source=state(types,arena.allocate(32,8).address());
                    Record first=pair(new double[]{3.25,-7.5});
                    function.computeFirst(actual,first,0);function.computeFirst(expected,first,0);function.computeFirst(source,first,0);
                    int countIndex=types.getColumnCount()-1;
                    actual.putLong(countIndex,Long.MIN_VALUE);expected.putLong(countIndex,Long.MIN_VALUE);source.putLong(countIndex,Long.MIN_VALUE);
                    function.merge(actual,source);function.merge(expected,source);
                    assertEquals(0,actual.getLong(countIndex));
                    Record next=pair(new double[]{9.5,2.75});function.computeNext(expected,next,1);
                    Unsafe.putDouble(input,9.5);Unsafe.putDouble(input+8,2.75);assertEquals(0,code.run(input,1,address));
                    assertState(function.getName(),types,expected,actual);
                }
            }
        }
    }

    @Test public void covariancePreservesPairwiseNullsMergesAndCountConversion() throws Exception {
        double[][] pairs = {{Double.NaN, 1}, {1, Double.NaN}, {Double.POSITIVE_INFINITY, 2},
                {3, Double.NEGATIVE_INFINITY}, {0x1p40, -0x1p40}, {0x1p40+0.25, -0x1p40+0.5},
                {0x1p40-0.25, -0x1p40-0.5}, {Double.NaN, 7}, {-0.0, 0.0}, {0.0, -0.0},
                {Double.MIN_VALUE, -Double.MIN_VALUE}, {Double.MAX_VALUE, Double.MAX_VALUE},
                {-Double.MAX_VALUE, Double.MAX_VALUE}, {3.25, -7.5}};
        for (var factory : List.of(new CovarPopGroupByFunctionFactory(), new CovarSampleGroupByFunctionFactory())) {
            var arguments = new ObjList<io.questdb.cairo.sql.Function>();
            arguments.add(DoubleColumn.newInstance(0)); arguments.add(DoubleColumn.newInstance(1));
            try (var function = (GroupByFunction) factory.newInstance(0, arguments, null, null, null);
                 var arena = Arena.ofConfined()) {
                var types = new ArrayColumnTypes(); function.initValueTypes(types);
                var inputTypes = new ArrayColumnTypes(); inputTypes.add(ColumnType.DOUBLE); inputTypes.add(ColumnType.DOUBLE);
                var functions = new ObjList<GroupByFunction>(); functions.add(function);
                var layout = new FlyweightPackedMapValue(types);
                var updates = AggregateUpdates.bind(inputTypes, types, functions, layout::getOffset, false);
                assertNotNull(updates);
                var b = new Builder();
                var values = new java.util.ArrayList<Pipeline.Value>();
                for (var column : updates.columns())
                    values.add(column.value(new Load(column.width(), at(COLUMNS, column.index()*8L))));
                updates.emitInitialized(b, new Pipeline.Row(updates.columns(), values), STATE, nil()); b.exit(0);
                var merge = new Builder(); updates.merge(merge, COLUMNS, STATE); merge.exit(0);
                var finish = new Builder(); updates.finish(finish, STATE, false); finish.exit(0);
                var fresh = new Builder(); updates.initialize(fresh, STATE); fresh.exit(0);
                try (var initializer = backend.compile(new Program(fresh.block()), temporary.newFolder().toPath());
                     var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
                     var merger = backend.compile(new Program(merge.block()), temporary.newFolder().toPath());
                     var finalizer = backend.compile(new Program(finish.block()), temporary.newFolder().toPath())) {
                    long actualAddress = arena.allocate(32, 8).address(), sourceAddress = arena.allocate(32, 8).address();
                    var actual = state(types, actualAddress);
                    var expected = state(types, arena.allocate(32, 8).address());
                    var source = state(types, sourceAddress);
                    long input = arena.allocate(16, 8).address();
                    function.setEmpty(actual); function.setEmpty(expected);
                    assertEquals(0, initializer.run(0, 0, actualAddress));
                    for (int i = 0; i < pairs.length; i++) {
                        Record record = pair(pairs[i]);
                        if (i == 0) function.computeFirst(expected, record, i); else function.computeNext(expected, record, i);
                        if (i%3 == 1) function.computeNext(actual, record, i);
                        else {
                            Unsafe.putDouble(input, pairs[i][0]); Unsafe.putDouble(input+8, pairs[i][1]);
                            assertEquals(0, code.run(input, 1, actualAddress));
                        }
                        assertState(function.getName(), types, expected, actual);
                    }
                    for (int left : new int[]{0, 1, 2, 5, 7, pairs.length}) for (int right : new int[]{0, 1, 2, 5, 7, pairs.length}) {
                        populatePairs(function, expected, pairs, left); populatePairs(function, actual, pairs, left);
                        populatePairs(function, source, pairs, right);
                        function.merge(expected, source); assertEquals(0, merger.run(sourceAddress, 0, actualAddress));
                        assertState(function.getName(), types, expected, actual);
                        double result = function.getDouble(new Record() {
                            @Override public double getDouble(int index) { return expected.getDouble(index); }
                            @Override public long getLong(int index) { return expected.getLong(index); }
                        });
                        assertEquals(0, finalizer.run(0, 0, actualAddress));
                        assertEquals(Double.doubleToLongBits(result), Double.doubleToLongBits(actual.getDouble(0)));
                    }
                    // Covariance casts before count multiplication; variance casts the LONG product.
                    for (long count : new long[]{1L<<32, (1L<<53)+1, Long.MAX_VALUE-3}) {
                        populatePairs(function, expected, pairs, 7); populatePairs(function, actual, pairs, 7);
                        populatePairs(function, source, pairs, 5);
                        expected.putLong(3, count); actual.putLong(3, count); source.putLong(3, count);
                        function.merge(expected, source); assertEquals(0, merger.run(sourceAddress, 0, actualAddress));
                        assertState(function.getName(), types, expected, actual);
                    }
                }
            }
        }
    }

    @Test public void weightedAverageFirstUpdateAndPartialAdoptionPreserveOrdinaryBits() throws Exception {
        double[][] pairs = {{-0.0, 1}, {Double.NaN, 1}, {-0.0, 1}, {0.0, -1},
                {2, 1}, {2, -1}, {-0.0, 1}, {4, 0}, {Double.POSITIVE_INFINITY, 2},
                {3, Double.NEGATIVE_INFINITY}, {Double.MIN_VALUE, -1},
                {Double.MAX_VALUE, 2}, {-Double.MAX_VALUE, 2}, {7, 1}};
        try (var function = new WeightedAvgDoubleGroupByFunction(DoubleColumn.newInstance(0), DoubleColumn.newInstance(1));
             var arena = Arena.ofConfined()) {
            var types = new ArrayColumnTypes(); function.initValueTypes(types);
            var inputs = new ArrayColumnTypes(); inputs.add(ColumnType.DOUBLE); inputs.add(ColumnType.DOUBLE);
            var functions = new ObjList<GroupByFunction>(); functions.add(function);
            var layout = new FlyweightPackedMapValue(types);
            var updates = AggregateUpdates.bind(inputs, types, functions, layout::getOffset, false);
            assertNotNull(updates);
            var values = new java.util.ArrayList<Pipeline.Value>();
            for (var column : updates.columns()) values.add(column.value(new Load(column.width(), at(COLUMNS, column.index()*8L))));
            var row = new Pipeline.Row(updates.columns(), values);
            var first = new Builder(); updates.emitInitialized(first, row, STATE, nil(), bool(true)); first.exit(0);
            var next = new Builder(); updates.emitInitialized(next, row, STATE, nil()); next.exit(0);
            var merge = new Builder(); updates.merge(merge, COLUMNS, STATE); merge.exit(0);
            var finish = new Builder(); updates.finish(finish, STATE, false); finish.exit(0);
            try (var firstCode = backend.compile(new Program(first.block()), temporary.newFolder().toPath());
                 var nextCode = backend.compile(new Program(next.block()), temporary.newFolder().toPath());
                 var merger = backend.compile(new Program(merge.block()), temporary.newFolder().toPath());
                 var finalizer = backend.compile(new Program(finish.block()), temporary.newFolder().toPath());
                 var dense = backend.compile(DenseAggregate.merge(updates, 16), temporary.newFolder().toPath())) {
                long address = arena.allocate(16, 8).address(), sourceAddress = arena.allocate(16, 8).address();
                var actual = state(types, address); var expected = state(types, arena.allocate(16, 8).address());
                var source = state(types, sourceAddress); long input = arena.allocate(16, 8).address();
                for (int start = 0; start < pairs.length; start++) {
                    function.setEmpty(actual); function.setEmpty(expected);
                    for (int i = start; i < pairs.length; i++) {
                        if (i == start) function.computeFirst(expected, pair(pairs[i]), i);
                        else function.computeNext(expected, pair(pairs[i]), i);
                        if (i > start && i%3 == 1) function.computeNext(actual, pair(pairs[i]), i);
                        else {
                            Unsafe.putDouble(input, pairs[i][0]); Unsafe.putDouble(input+8, pairs[i][1]);
                            assertEquals(0, (i == start ? firstCode : nextCode).run(input, 1, address));
                        }
                        assertState("weighted_avg", types, expected, actual);
                    }
                }
                for (int left : new int[]{0,1,2,6,pairs.length}) for (int right : new int[]{0,1,2,6,pairs.length}) {
                    populatePairs(function, expected, pairs, left); populatePairs(function, actual, pairs, left);
                    populatePairs(function, source, pairs, right);
                    function.merge(expected, source); assertEquals(0, merger.run(sourceAddress, 0, address));
                    assertState("weighted_avg", types, expected, actual);
                    double result = expected.getDouble(0)/expected.getDouble(1);
                    assertEquals(0, finalizer.run(0, 0, address));
                    assertEquals(Double.doubleToLongBits(result), Double.doubleToLongBits(actual.getDouble(0)));
                }
                long args = arena.allocate(24, 8).address(), seen = arena.allocate(2).address();
                Unsafe.putByte(seen, (byte) 1); Unsafe.putByte(seen+1, (byte) 0);
                Unsafe.putLong(args, sourceAddress); Unsafe.putLong(args+8, seen); Unsafe.putLong(args+16, seen+1);
                populatePairs(function, source, pairs, 1); function.setEmpty(actual);
                assertEquals(0, dense.run(args, 1, address));
                assertState("first partial", types, source, actual);
                populatePairs(function, expected, pairs, 1);
                function.merge(expected, source); assertEquals(0, dense.run(args, 1, address));
                assertState("existing partial", types, expected, actual);
            }
        }
    }

    private static Record pair(double[] values) {
        return new Record() { @Override public double getDouble(int index) { return values[index]; } };
    }

    private static void populatePairs(GroupByFunction function, FlyweightPackedMapValue state, double[][] values, int length) {
        function.setEmpty(state);
        for (int i = 0; i < length; i++) {
            if (i == 0) function.computeFirst(state, pair(values[i]), i);
            else function.computeNext(state, pair(values[i]), i);
        }
    }

    private static GroupByFunction statistic(io.questdb.griffin.FunctionFactory factory) throws Exception {
        var arguments = new ObjList<io.questdb.cairo.sql.Function>(); arguments.add(DoubleColumn.newInstance(0));
        return (GroupByFunction) factory.newInstance(0, arguments, null, null, null);
    }

    private static Record record(double value) {
        return new Record() {
            @Override public double getDouble(int ignored) { return value; }
            @Override public float getFloat(int ignored) { return (float) value; }
        };
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
            if (types.getColumnType(i) == ColumnType.BOOLEAN)
                assertEquals(name, expected.getBool(i), actual.getBool(i));
            else if (types.getColumnType(i) == ColumnType.DOUBLE)
                assertEquals(name, Double.doubleToLongBits(expected.getDouble(i)), Double.doubleToLongBits(actual.getDouble(i)));
            else if (types.getColumnType(i) == ColumnType.FLOAT)
                assertEquals(name, Float.floatToIntBits(expected.getFloat(i)), Float.floatToIntBits(actual.getFloat(i)));
            else assertEquals(name, expected.getLong(i), actual.getLong(i));
        }
    }
}
