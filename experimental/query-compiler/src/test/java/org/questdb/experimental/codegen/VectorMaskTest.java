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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
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
public class VectorMaskTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    public VectorMaskTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name="{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm0",new LlvmBackend(0)},new Object[]{"llvm1",new LlvmBackend(1)},
                new Object[]{"llvm2",new LlvmBackend(2)},new Object[]{"llvm3",new LlvmBackend(3)},
                new Object[]{"c",new NativeBackend()},new Object[]{"asmjit",new AsmJitBackend()});
    }
    @Test public void widthsLaneOrderAndMaskTails() throws Exception {
        for (MemoryType width : new MemoryType[]{MemoryType.I8,MemoryType.I16,MemoryType.I32,MemoryType.I64}) {
            Builder b = new Builder();
            int[] sizes = {1,7,16,32,63,64};
            for (int i=0;i<sizes.length;i++) b.store(at(STATE,i*8L), new VectorMask(sizes[i],binary(Op.LT,new Load(width,COLUMNS),i64(0))));
            b.exit(0);
            try (var kernel=backend.compile(new Program(b.block()),temporary.getRoot().toPath()); Arena arena=Arena.ofConfined()) {
                long data=arena.allocate(64L*width.bytes,8).address(),output=arena.allocate(48,8).address();
                for (long pattern : new long[]{0,-1,Long.MIN_VALUE,1,0x5555555555555555L,0x8123456789abcdefL}) {
                    for (int lane=0;lane<64;lane++) write(width,data+(long)lane*width.bytes,(pattern>>>lane&1)!=0?-1:1);
                    assertEquals(0,kernel.run(data,64,output));
                    for (int i=0;i<sizes.length;i++) assertEquals(pattern & (-1L >>> (64-sizes[i])),Unsafe.getUnsafe().getLong(output+i*8L));
                }
            }
        }
    }
    @Test public void wrappingArithmeticAndEagerSafeSelectsMatchScalarLanes() throws Exception {
        Builder b=new Builder();Expr value=new Load(MemoryType.I64,COLUMNS),sum=add(value,i64(Long.MAX_VALUE));
        Expr predicate=binary(Op.BOTH,ne(value,i64(Long.MIN_VALUE)),binary(Op.GE,new Select(binary(Op.LT,sum,i64(0)),sum,value),i64(-9)));
        b.store(STATE,new VectorMask(64,predicate));b.exit(0);
        try(var kernel=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined()) {
            long data=arena.allocate(512,8).address(),output=arena.allocate(8,8).address(),expected=0;
            long[] cases={Long.MIN_VALUE,Long.MAX_VALUE,-1,0,1,-9,-10,19};
            for(int i=0;i<64;i++){long v=cases[i%cases.length],s=v+Long.MAX_VALUE;Unsafe.getUnsafe().putLong(data+i*8L,v);if(v!=Long.MIN_VALUE&&(s<0?s:v)>=-9)expected|=1L<<i;}
            assertEquals(0,kernel.run(data,64,output));assertEquals(expected,Unsafe.getUnsafe().getLong(output));
        }
    }
    @Test public void maskReadsCannotCrossStores() throws Exception {
        Builder b=new Builder();Expr read=new VectorMask(64,ne(new Load(MemoryType.I8,COLUMNS),i64(0)));
        Var before=b.let(read);b.store(MemoryType.I8,at(COLUMNS,63),i64(1));Var after=b.let(read);
        b.store(STATE,before);b.store(at(STATE,8),after);b.exit(0);
        var graph=SsaProgram.lower(new Program(b.block()));SsaOptimizer.optimize(graph);SsaOptimizer.verify(graph);
        assertTrue(graph.blocks.stream().flatMap(x->x.code.stream()).anyMatch(s->SsaOptimizer.effect(s)==SsaOptimizer.Effect.READ));
        try(var kernel=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined()) {
            long data=arena.allocate(64,8).address(),out=arena.allocate(16,8).address();
            assertEquals(0,kernel.run(data,64,out));assertEquals(0,Unsafe.getUnsafe().getLong(out));assertEquals(Long.MIN_VALUE,Unsafe.getUnsafe().getLong(out+8));
        }
    }
    @Test public void bitScanIsDefinedAtZeroAndHighBit() throws Exception {
        Builder b=new Builder();b.store(STATE,new TrailingZeros(load(COLUMNS)));b.exit(0);
        try(var kernel=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined()) {
            long data=arena.allocate(8,8).address(),out=arena.allocate(8,8).address();
            for(long value:new long[]{0,1,2,Long.MIN_VALUE,-1,0x100000000L}){
                Unsafe.getUnsafe().putLong(data,value);assertEquals(0,kernel.run(data,0,out));assertEquals(Long.numberOfTrailingZeros(value),Unsafe.getUnsafe().getLong(out));
            }
        }
    }

    @Test public void populationCountCoversZeroDenseAndHighMasks() throws Exception {
        Builder b = new Builder(); b.exit(new PopulationCount(load(COLUMNS)));
        try (var kernel = backend.compile(new Program(b.block()), temporary.getRoot().toPath());
             Arena arena = Arena.ofConfined()) {
            long address = arena.allocate(8, 8).address();
            for (long mask : new long[]{0, 1, -1, Long.MIN_VALUE, 0x5555555555555555L, 0x8123456789abcdefL}) {
                Unsafe.putLong(address, mask);
                assertEquals(Long.bitCount(mask), kernel.run(address, 0, 0));
            }
        }
    }
    @Test public void selectionStoresPreserveOrderBoundsAndUnusedWriteEffects() throws Exception {
        Builder b = new Builder();
        int[] widths = {1, 7, 8, 9, 63, 64};
        for (int i = 0; i < widths.length; i++) {
            // Deliberately unused result: neither optimizer may discard the stores.
            b.let(new SelectRows(widths[i], load(COLUMNS), i64(Long.MAX_VALUE - 31), at(STATE, i * 65L * 8)));
        }
        b.exit(0);
        var program = new Program(b.block());
        var graph = SsaProgram.lower(program); SsaOptimizer.optimize(graph); SsaOptimizer.verify(graph);
        assertEquals(widths.length, graph.blocks.stream().flatMap(block -> block.code.stream())
                .filter(s -> SsaOptimizer.effect(s) == SsaOptimizer.Effect.WRITE).count());
        try (var kernel = backend.compile(program, temporary.getRoot().toPath()); var arena = Arena.ofConfined()) {
            long input = arena.allocate(8, 8).address(), output = arena.allocate(widths.length * 65L * 8, 8).address();
            for (long mask : new long[]{0, -1, Long.MIN_VALUE, 1, 0x5555555555555555L, 0x8123456789abcdefL}) {
                Unsafe.putLong(input, mask);
                for (int i = 0; i < widths.length * 65; i++) Unsafe.putLong(output + 8L * i, 77);
                assertEquals(0, kernel.run(input, 0, output));
                for (int i = 0; i < widths.length; i++) {
                    long start = output + i * 65L * 8;
                    int selected = 0;
                    for (int lane = 0; lane < widths[i]; lane++) if ((mask & (1L << lane)) != 0)
                        assertEquals(Long.MAX_VALUE - 31 + lane, Unsafe.getLong(start + 8L * selected++));
                    for (int tail = selected; tail < 65; tail++) assertEquals(77, Unsafe.getLong(start + 8L * tail));
                }
            }
        }
    }
    @Test public void portableExpansionPreservesLazyBranchesAndRepeatedConditions() throws Exception {
        Builder b=new Builder();
        Expr mask=new VectorMask(1,ne(new Load(MemoryType.I8,COLUMNS),i64(0)));
        b.store(STATE,new Select(eq(ROWS,i64(0)),i64(77),mask));
        b.loop(binary(Op.BOTH,ne(ROWS,i64(0)),ne(mask,i64(0))),loop->loop.store(MemoryType.I8,COLUMNS,i64(0)));
        b.exit(0);
        try(var kernel=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined()) {
            long out=arena.allocate(8,8).address(),data=arena.allocate(1).address();
            assertEquals(0,kernel.run(0,0,out));assertEquals(77,Unsafe.getUnsafe().getLong(out));
            Unsafe.getUnsafe().putByte(data,(byte)1);assertEquals(0,kernel.run(data,1,out));assertEquals(1,Unsafe.getUnsafe().getLong(out));assertEquals(0,Unsafe.getUnsafe().getByte(data));
        }
    }
    @Test public void portableExpansionPreservesAddressBeforeValueEffects() throws Exception {
        Builder b=new Builder();
        b.store(new Allocate(i64(8)),new VectorMask(1,ne(new Load(MemoryType.I8,COLUMNS),i64(0))));b.exit(0);
        try(var kernel=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined()){
            long input=arena.allocate(1).address();long[] allocated={0};
            try(var memory=KernelMemory.open(new FastGroupByAllocator(1024,1048576){
                @Override public long malloc(long bytes){Unsafe.getUnsafe().putByte(input,(byte)1);return allocated[0]=super.malloc(bytes);}
            })){
                assertEquals(0,kernel.run(input,1,0));memory.throwIfFailed();
                assertEquals(1,Unsafe.getUnsafe().getLong(allocated[0]));
            }
        }
    }
    @Test public void rejectsUnsafeLaneExpressions() {
        assertThrows(IllegalArgumentException.class,()->new VectorMask(0,bool(true)));
        assertThrows(IllegalArgumentException.class,()->new VectorMask(65,bool(true)));
        assertThrows(IllegalArgumentException.class,()->new VectorMask(64,i64(0)));
        assertThrows(IllegalArgumentException.class,()->new VectorMask(64,eq(new Allocate(i64(8)),nil())));
        assertThrows(IllegalArgumentException.class,()->new VectorMask(64,eq(new Load(MemoryType.ADDRESS,COLUMNS),nil())));
        assertThrows(IllegalArgumentException.class,()->new VectorMask(64,eq(load(new Load(MemoryType.ADDRESS,COLUMNS)),i64(0))));
        assertThrows(IllegalArgumentException.class,()->new VectorMask(64,eq(new VectorMask(1,bool(true)),i64(1))));
    }
    private static void write(MemoryType type,long address,long value) {
        var u=Unsafe.getUnsafe();switch(type){case I8->u.putByte(address,(byte)value);case I16->u.putShort(address,(short)value);case I32->u.putInt(address,(int)value);case I64->u.putLong(address,value);default->throw new AssertionError();}
    }
}
