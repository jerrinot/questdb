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
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.groupby.*;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.lang.foreign.Arena;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class BitwiseStateTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend = new LlvmBackend();

    @Test public void mixedStatesRetainSentinelCollisionsAndNarrowFirstRows() throws Exception {
        for (GroupByFunction function : List.of(
                new BitAndByteGroupByFunction(ByteColumn.newInstance(0)),new BitOrByteGroupByFunction(ByteColumn.newInstance(0)),new BitXorByteGroupByFunction(ByteColumn.newInstance(0)),
                new BitAndShortGroupByFunction(ShortColumn.newInstance(0)),new BitOrShortGroupByFunction(ShortColumn.newInstance(0)),new BitXorShortGroupByFunction(ShortColumn.newInstance(0)),
                new BitAndIntGroupByFunction(IntColumn.newInstance(0)),new BitOrIntGroupByFunction(IntColumn.newInstance(0)),new BitXorIntGroupByFunction(IntColumn.newInstance(0)),
                new BitAndLongGroupByFunction(LongColumn.newInstance(0)),new BitOrLongGroupByFunction(LongColumn.newInstance(0)),new BitXorLongGroupByFunction(LongColumn.newInstance(0)))) {
            try (function; var arena=Arena.ofConfined()) {
                int type=((UnaryFunction)function).getArg().getType();
                var types=new ArrayColumnTypes(); function.initValueTypes(types);
                var inputTypes=new ArrayColumnTypes(); inputTypes.add(type);
                var functions=new ObjList<GroupByFunction>(); functions.add(function);
                var layout=new FlyweightPackedMapValue(types);
                var updates=AggregateUpdates.bind(inputTypes,types,functions,layout::getOffset,false);
                assertNotNull(updates);
                var b=new Builder();
                var row=new Pipeline.Row(updates.columns(),List.of(updates.columns().getFirst().value(new Load(Pipeline.widthOf(type),COLUMNS))));
                updates.emitInitialized(b,row,STATE,nil()); b.exit(0);
                var init=new Builder(); updates.initialize(init,STATE); init.exit(0);
                var merge=new Builder(); updates.merge(merge,COLUMNS,STATE); merge.exit(0);
                try (var code=backend.compile(new Program(b.block()),temporary.newFolder().toPath());
                     var initializer=backend.compile(new Program(init.block()),temporary.newFolder().toPath());
                     var merger=backend.compile(new Program(merge.block()),temporary.newFolder().toPath())) {
                    long actualAddress=arena.allocate(8,8).address(), sourceAddress=arena.allocate(8,8).address();
                    var actual=state(types,actualAddress); var expected=state(types,arena.allocate(8,8).address());
                    var source=state(types,sourceAddress); long input=arena.allocate(8,8).address();
                    long sentinel=type==ColumnType.LONG?Long.MIN_VALUE:Integer.MIN_VALUE;
                    long[] values={sentinel,sentinel+1,sentinel+2,1,sentinel+1,1,7,-1,0,Long.MAX_VALUE,Integer.MAX_VALUE,255,32768};
                    for (boolean javaFirst : new boolean[]{false,true}) {
                        function.setEmpty(actual); function.setEmpty(expected);
                        for (int n=0;n<values.length;n++) {
                            long value=values[n];
                            Record record=record(value);
                            if (n==0) function.computeFirst(expected,record,n); else function.computeNext(expected,record,n);
                            if (n==0 && javaFirst) function.computeFirst(actual,record,n);
                            else if (n%3==1) function.computeNext(actual,record,n);
                            else {
                                if (n==0) assertEquals(0,initializer.run(0,0,actualAddress));
                                Unsafe.putLong(input,value); assertEquals(0,code.run(input,1,actualAddress));
                            }
                            assertEquals(function.getName(),read(type,expected),read(type,actual));
                        }
                    }
                    for (long left:values) for (long right:values) {
                        function.computeFirst(actual,record(left),0); function.computeFirst(expected,record(left),0);
                        function.computeFirst(source,record(right),0); function.merge(expected,source);
                        assertEquals(0,merger.run(sourceAddress,0,actualAddress));
                        assertEquals(function.getName(),read(type,expected),read(type,actual));
                    }
                }
            }
        }
    }

    private static Record record(long value) {
        return new Record() {
            @Override public byte getByte(int ignored) { return (byte)value; }
            @Override public short getShort(int ignored) { return (short)value; }
            @Override public int getInt(int ignored) { return (int)value; }
            @Override public long getLong(int ignored) { return value; }
        };
    }
    private static long read(int type,FlyweightPackedMapValue value) {
        return switch (type) {
            case ColumnType.BYTE -> value.getByte(0);
            case ColumnType.SHORT -> value.getShort(0);
            case ColumnType.INT -> value.getInt(0);
            default -> value.getLong(0);
        };
    }
    private static FlyweightPackedMapValue state(ArrayColumnTypes types,long address) {
        var value=new FlyweightPackedMapValue(types); value.of(address); return value;
    }
}
