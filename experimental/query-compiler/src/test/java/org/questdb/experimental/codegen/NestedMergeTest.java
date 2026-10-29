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
import io.questdb.griffin.engine.functions.groupby.CountDistinctIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctSymbolGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDistinctLongGroupByFunction;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.foreign.Arena;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

/** Oracle is the real Java function, including its set slots, growth and pointer adoption. */
@RunWith(Parameterized.class)
public class NestedMergeTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private final NestedDistinct layout;
    private final int type;
    public NestedMergeTest(String name, Backend backend, int type) {
        this.backend = backend;
        this.type = type;
        this.layout = type == ColumnType.LONG ? NestedDistinct.LONG : NestedDistinct.INT;
    }
    @Parameterized.Parameters(name="{0}") public static List<Object[]> parameters() {
        var cases = new java.util.ArrayList<Object[]>();
        for (var backend : List.of(new LlvmBackend(), new NativeBackend(), new AsmJitBackend())) {
            cases.add(new Object[]{backend.getClass().getSimpleName() + "-long", backend, ColumnType.LONG});
            cases.add(new Object[]{backend.getClass().getSimpleName() + "-int", backend, ColumnType.INT});
            cases.add(new Object[]{backend.getClass().getSimpleName() + "-symbol", backend, ColumnType.SYMBOL});
        }
        return cases;
    }

    @Test public void generatedMergeMatchesJavaBranchesLayoutGrowthAndAllocatorOwnership() throws Exception {
        Builder b=new Builder();layout.mergeWithCapacity(b,COLUMNS,STATE,NestedDistinct.initialCapacity(3,0.7),0.7);b.exit(0);
        try(var code=backend.compile(new Program(b.block()),temporary.getRoot().toPath())) {
            for(int dstSize:new int[]{-1,0,1,2,3,4,17,127}) {
                for(int srcSize:new int[]{-1,0,1,2,3,4,17,127}) {
                    compare(code,dstSize,srcSize);
                }
            }
        }
    }

    @Test public void generatedAndOrdinaryUpdatesShareEverySlotIncludingSignedKeys() throws Exception {
        long nullValue = type == ColumnType.LONG ? Long.MIN_VALUE : type == ColumnType.INT ? Integer.MIN_VALUE : io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL;
        var b = new Builder();
        layout.emitWithCapacity(b, new Pipeline.Value(load(COLUMNS), eq(load(COLUMNS), i64(nullValue))), STATE,
                NestedDistinct.initialCapacity(3, .7), .7);
        b.exit(0);
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
        try (var code = backend.compile(new Program(b.block()), temporary.newFolder().toPath());
             var arena = Arena.ofConfined(); var ordinary = allocator(); var mixed = allocator()) {
            var function = function();
            var types = new ArrayColumnTypes(); function.initValueTypes(types);
            var expected = state(function, types, arena, ordinary, 0, 0, 1);
            var actual = state(function, types, arena, mixed, 0, 0, 1);
            long input = arena.allocate(8, 8).address();
            try (var memory = KernelMemory.bind(mixed, () -> { })) {
                for (int i = 0; i < 600; i++) {
                    long key = i % 13 == 0 ? nullValue : input(i % 257);
                    var record = record(key);
                    function.setAllocator(ordinary);
                    function.computeNext(expected.value, record, i);
                    if (i % 3 == 0) {
                        function.setAllocator(mixed);
                        function.computeNext(actual.value, record, i);
                    } else {
                        Unsafe.getUnsafe().putLong(input, key);
                        assertEquals(0, code.run(input, 1, actual.address));
                        memory.throwIfFailed();
                    }
                    assertState(expected.value, actual.value);
                }
            }
        }
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
    }

    private GroupByFunction function() {
        if (layout == NestedDistinct.LONG) return new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 3, .7);
        if (type == ColumnType.INT) return new CountDistinctIntGroupByFunction(IntColumn.newInstance(0), 3, .7);
        return new CountDistinctSymbolGroupByFunction(IntColumn.newInstance(0), 3, .7);
    }

    private long input(int value) {
        if (type == ColumnType.SYMBOL || value == 0) return value;
        if (layout == NestedDistinct.INT) return value % 2 == 0 ? Integer.MIN_VALUE + value : Integer.MAX_VALUE - value;
        return value % 2 == 0 ? Long.MIN_VALUE + value : Long.MAX_VALUE - value;
    }

    private static Record record(long value) {
        return new Record() {
            @Override public long getLong(int ignored) { return value; }
            @Override public int getInt(int ignored) { return (int) value; }
        };
    }

    private void compare(CompiledKernel code,int dstSize,int srcSize) {
        long before=Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
        try(var arena=Arena.ofConfined();var javaDst=allocator();var javaSrc=allocator();var javaMerge=allocator();
            var nativeDst=allocator();var nativeSrc=allocator();var nativeMerge=allocator()) {
            var function=function();
            var types=new ArrayColumnTypes();function.initValueTypes(types);
            var dst=state(function,types,arena,javaDst,dstSize,1,1);
            var src=state(function,types,arena,javaSrc,srcSize,0,2);
            var actualDst=state(function,types,arena,nativeDst,dstSize,1,1);
            var actualSrc=state(function,types,arena,nativeSrc,srcSize,0,2);
            long dstOld=dst.value.getLong(1),srcOld=src.value.getLong(1);
            long actualDstOld=actualDst.value.getLong(1),actualSrcOld=actualSrc.value.getLong(1);
            function.setAllocator(javaMerge);
            function.merge(dst.value,src.value);
            try(var binding=KernelMemory.bind(nativeMerge,()->{})) {
                assertEquals(0,code.run(actualSrc.address,0,actualDst.address));binding.throwIfFailed();
            }
            assertState(dst.value,actualDst.value);
            if(dst.value.getLong(0)>1) {
                assertEquals("destination pointer retention",dst.value.getLong(1)==dstOld,actualDst.value.getLong(1)==actualDstOld);
                assertEquals("source pointer adoption",dst.value.getLong(1)==srcOld,actualDst.value.getLong(1)==actualSrcOld);
            }
            assertEquals("merge allocation",javaMerge.allocated(),nativeMerge.allocated());
            assertEquals("source owner retained",javaSrc.allocated(),nativeSrc.allocated());
            assertEquals("destination owner retained",javaDst.allocated(),nativeDst.allocated());
        }
        assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
    }

    private record State(long address,FlyweightPackedMapValue value) { }
    private State state(GroupByFunction function,ArrayColumnTypes types,Arena arena,
                               FastGroupByAllocator allocator,int size,int start,int stride) {
        long address=arena.allocate(16,8).address();
        var value=new FlyweightPackedMapValue(types);value.of(address);function.setAllocator(allocator);
        if(size<0)function.setNull(value);
        else if(size==0)function.setEmpty(value);
        else for(int i=0;i<size;i++) {
            long input=input(start+i*stride);
            Record record=record(input);
            if(i==0)function.computeFirst(value,record,i);else function.computeNext(value,record,i);
        }
        return new State(address,value);
    }
    private static FastGroupByAllocator allocator(){return new FastGroupByAllocator(64,1L<<30);}
    private void assertState(FlyweightPackedMapValue expected,FlyweightPackedMapValue actual) {
        assertEquals(expected.getLong(0),actual.getLong(0));
        long expectedPtr=expected.getLong(1),actualPtr=actual.getLong(1);
        if(expected.getLong(0)<=1){assertEquals(expectedPtr,actualPtr);return;}
        for(int offset=0;offset<12;offset+=4) {
            assertEquals(Unsafe.getUnsafe().getInt(expectedPtr+offset),Unsafe.getUnsafe().getInt(actualPtr+offset));
        }
        int capacity=Unsafe.getUnsafe().getInt(expectedPtr);
        for(int slot=0;slot<capacity;slot++) {
            if (layout == NestedDistinct.LONG) {
                assertEquals("set slot " + slot, Unsafe.getUnsafe().getLong(expectedPtr + 16 + slot * 8L),
                        Unsafe.getUnsafe().getLong(actualPtr + 16 + slot * 8L));
            } else {
                assertEquals("set slot " + slot, Unsafe.getUnsafe().getInt(expectedPtr + 12 + slot * 4L),
                        Unsafe.getUnsafe().getInt(actualPtr + 12 + slot * 4L));
            }
        }
    }
}
