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

import io.questdb.cairo.*;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.util.List;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.Pipeline.*;

@RunWith(Parameterized.class)
public class ScanStageTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private final Backend backend;
    public ScanStageTest(String name,Backend backend){this.backend=backend;}
    @Parameterized.Parameters(name="{0}") public static List<Object[]> parameters(){return List.of(
            new Object[]{"llvm",new LlvmBackend(2)},new Object[]{"c",new NativeBackend()},new Object[]{"asmjit",new AsmJitBackend()});}

    @Test public void literalSearchNeverReadsOutsideTheSlice() throws Throwable {
        var random = new java.util.Random(72571);
        for (int size = 1; size <= 8; size++) for (boolean repeated : new boolean[]{false, true}) {
            byte[] pattern = new byte[size];
            for (int i = 0; i < size; i++) pattern[i] = (byte) (repeated ? 'a' : size % 2 == 0 ? 'a' + i % 2 : 0x80 + i % 3);
            Builder b = new Builder();
            Var found = ByteSequences.contains(b, COLUMNS, ROWS, pattern);
            b.exit(new Select(found, i64(1), i64(0)));
            try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath())) {
                for (int length = 0; length <= 140; length++) {
                    for (boolean atEnd : new boolean[]{true, false}) try (var page = new GuardedPage(length, atEnd)) {
                        for (int trial = 0; trial < 5; trial++) {
                            byte[] bytes = new byte[length];
                            for (int i = 0; i < length; i++) bytes[i] = trial == 0 ? pattern[i % size] : repeated && trial == 4 ? (byte) ('a' + i % 2) : (byte) random.nextInt(256);
                            if (length >= size && trial > 0 && trial < 4) {
                                int start = trial == 1 ? 0 : trial == 2 ? length - size : random.nextInt(length - size + 1);
                                System.arraycopy(pattern, 0, bytes, start, size);
                            }
                            boolean expected = false;
                            for (int i = 0; i + size <= length; i++) {
                                boolean same = true;
                                for (int j = 0; j < size; j++) same &= bytes[i + j] == pattern[j];
                                expected |= same;
                            }
                            for (int i = 0; i < length; i++) Unsafe.getUnsafe().putByte(page.address + i, bytes[i]);
                            assertEquals("size=" + size + " length=" + length + " trial=" + trial,
                                    expected ? 1 : 0, code.run(page.address, length, 0));
                        }
                    }
                }
            }
        }
    }

    @Test public void byteMasksNeverReadOutsideTheSlice() throws Throwable {
        for (int lanes : new int[]{8, 16, 32, 64}) {
            Builder b = new Builder();
            b.exit(new VectorMask(lanes, eq(new Load(MemoryType.I8, COLUMNS), i64(-1))));
            try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath())) {
                for (boolean atEnd : new boolean[]{true, false}) try (var page = new GuardedPage(lanes, atEnd)) {
                    for (int i = 0; i < lanes; i++) Unsafe.getUnsafe().putByte(page.address + i, (byte) -1);
                    assertEquals(-1L >>> (64 - lanes), code.run(page.address, lanes, 0));
                }
            }
        }
    }

    @Test public void byteOrderingPreservesUnsignedOrderAndSliceBounds() throws Throwable {
        Builder b = new Builder();
        Var leftSize = b.let(load(STATE)), rightSize = b.let(load(at(STATE, 8)));
        Var right = b.let(new Load(MemoryType.ADDRESS, at(STATE, 16)));
        b.exit(ByteSequences.compare(b, COLUMNS, leftSize, ByteSequences.sixPrefix(COLUMNS, leftSize),
                right, rightSize, ByteSequences.sixPrefix(right, rightSize)));
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath()); Arena arena = Arena.ofConfined()) {
            long state = arena.allocate(24, 8).address();
            for (int length = 0; length <= 80; length++) for (int delta : new int[]{-1, 0, 1}) {
                int rightLength = Math.max(0, length + delta);
                for (boolean end : new boolean[]{true, false}) try (var left = new GuardedPage(length, end);
                                                                  var other = new GuardedPage(rightLength, !end)) {
                    byte[] a = new byte[length], c = new byte[rightLength];
                    for (int i = 0; i < a.length; i++) a[i] = (byte) (i * 37);
                    for (int i = 0; i < c.length; i++) c[i] = (byte) (i * 37);
                    for (int i = 0; i < a.length; i++) Unsafe.getUnsafe().putByte(left.address + i, a[i]);
                    for (int i = 0; i < c.length; i++) Unsafe.getUnsafe().putByte(other.address + i, c[i]);
                    Unsafe.putLong(state, length); Unsafe.putLong(state + 8, rightLength); Unsafe.putLong(state + 16, other.address);
                    assertEquals(Integer.signum(java.util.Arrays.compareUnsigned(a, c)), Long.signum(code.run(left.address, 0, state)));
                    for (int i = 0; i < Math.min(length, rightLength); i++) {
                        byte saved = c[i]; c[i] ^= (byte) 0x80;
                        Unsafe.getUnsafe().putByte(other.address + i, c[i]);
                        assertEquals("length=" + length + " delta=" + delta + " difference=" + i,
                                Integer.signum(java.util.Arrays.compareUnsigned(a, c)), Long.signum(code.run(left.address, 0, state)));
                        c[i] = saved; Unsafe.getUnsafe().putByte(other.address + i, saved);
                    }
                }
            }
        }
    }

    @Test public void byteEqualityNeverReadsBeyondEitherSlice() throws Throwable {
        Builder b = new Builder();
        Var same = ByteSequences.equal(b, COLUMNS, STATE, ROWS);
        b.exit(new Select(same, i64(1), i64(0)));
        try (var code = backend.compile(new Program(b.block()), temporary.getRoot().toPath())) {
            for (int length = 0; length <= 258; length++) {
                for (int placement = 0; placement < 3; placement++) {
                    try (var left = new GuardedPage(length, placement != 1);
                         var right = new GuardedPage(length, placement != 2)) {
                        for (int i=0; i<length; i++) {
                            Unsafe.getUnsafe().putByte(left.address+i, (byte)(i*37));
                            Unsafe.getUnsafe().putByte(right.address+i, (byte)(i*37));
                        }
                        assertEquals(1, code.run(left.address, length, right.address));
                        for (int i=0; i<length; i++) {
                            Unsafe.getUnsafe().putByte(right.address+i, (byte)(i*37+1));
                            assertEquals("length="+length+" mismatch="+i, 0, code.run(left.address, length, right.address));
                            Unsafe.getUnsafe().putByte(right.address+i, (byte)(i*37));
                        }
                    }
                }
            }
        }
    }

    @Test public void tailsDoNotCrossProtectedPagesAndSelectedRowsKeepOrder() throws Throwable {
        for(int type:new int[]{ColumnType.BYTE,ColumnType.SHORT,ColumnType.INT,ColumnType.LONG}){
            var c=new Column(0,"value",type);var plan=ScanPlan.of(List.of(c),new Compare(Op.NE,c,new Constant(0)));
            Builder b=new Builder();ScanLowering.emit(plan,b,(consumer,row)->{
                consumer.store(STATE,add(load(STATE),i64(1)));
                consumer.store(at(STATE,8),add(binary(Op.MUL,load(at(STATE,8)),i64(31)),c.emit(row).data()));
            });b.exit(0);
            try(var code=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined()){
                long columns=arena.allocate(8,8).address(),out=arena.allocate(16,8).address();
                for(int pattern=0;pattern<3;pattern++)
                for(int rows:new int[]{0,1,7,63,64,65,127,128,129})try(var page=new GuardedPage(rows*c.width().bytes)){
                    long hash=0,count=0;
                    for(int i=0;i<rows;i++){
                        long value=i%3==0?0:(i%91)-45;
                        if(pattern==1&&value==0)value=1;
                        if(pattern==2)value=i/64%2==0?i%91+1:0;
                        write(c.width(),page.address+(long)i*c.width().bytes,value);
                        if(value!=0){count++;hash=hash*31+value;}
                    }
                    Unsafe.getUnsafe().putLong(columns,page.address);Unsafe.getUnsafe().putLong(out,0);Unsafe.getUnsafe().putLong(out+8,0);
                    assertEquals(0,code.run(columns,rows,out));assertEquals(count,Unsafe.getUnsafe().getLong(out));assertEquals(hash,Unsafe.getUnsafe().getLong(out+8));
                }
            }
        }
    }
    @Test public void rejectedRowsNeverReadConsumerOnlyColumns() throws Throwable {
        var k=new Column(0,"inaccessible",ColumnType.LONG);var f=new Column(1,"flag",ColumnType.BYTE);
        Builder b=new Builder();ScanLowering.emit(ScanPlan.of(List.of(k,f),new Compare(Op.NE,f,new Constant(0))),b,
                (consumer,row)->consumer.store(STATE,k.emit(row).data()));b.exit(0);
        try(var code=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined();var inaccessible=new GuardedPage(0)){
            long columns=arena.allocate(16,8).address(),flags=arena.allocate(129).address(),out=arena.allocate(8,8).address();
            Unsafe.getUnsafe().putLong(columns,inaccessible.address);Unsafe.getUnsafe().putLong(columns+8,flags);
            Unsafe.getUnsafe().putLong(out,123);assertEquals(0,code.run(columns,129,out));assertEquals(123,Unsafe.getUnsafe().getLong(out));
        }
    }
    @Test public void acceptedRowsDoNotLoadUnrequestedColumns() throws Throwable {
        var k=new Column(0,"unused",ColumnType.LONG);var f=new Column(1,"flag",ColumnType.BYTE);
        for(var mode:ScanLowering.Mode.values()){
        Builder b=new Builder();ScanLowering.emit(ScanPlan.of(List.of(k,f),new Compare(Op.NE,f,new Constant(0))),b,
                (consumer,row)->consumer.store(STATE,add(load(STATE),f.emit(row).data())),mode);b.exit(0);
        try(var code=backend.compile(new Program(b.block()),temporary.getRoot().toPath());Arena arena=Arena.ofConfined();var inaccessible=new GuardedPage(0)){
            long columns=arena.allocate(16,8).address(),flags=arena.allocate(129).fill((byte)1).address(),out=arena.allocate(8,8).address();
            Unsafe.getUnsafe().putLong(columns,inaccessible.address);Unsafe.getUnsafe().putLong(columns+8,flags);
            assertEquals(0,code.run(columns,129,out));assertEquals(129,Unsafe.getUnsafe().getLong(out));
        }
        }
    }
    @Test public void sqlSemanticsAndProjectionBindingsAcrossVectorAndTopFrames() throws Exception {
        var cfg=new DefaultCairoConfiguration(temporary.newFolder("db").toString()){
            @Override public int getSqlPageFrameMaxRows(){return 129;}
            @Override public int getSqlPageFrameMinRows(){return 1;}
        };
        try(var engine=new CairoEngine(cfg);var context=new SqlExecutionContextImpl(engine,1).with(cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),null,null,-1,null)){
            context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            engine.execute("create table t as (select cast(case when x%17=0 then null else x%23 end as int) k,case when x%11=0 then null else x%137 end v from long_sequence(5003))",context);
            var k=new Column(0,"k",ColumnType.INT);var v=new Column(1,"v",ColumnType.LONG);
            var predicates=List.<Predicate>of(new SqlCompare(Op.EQ,v,new Constant(Long.MIN_VALUE)),new SqlCompare(Op.NE,v,new Constant(3)),
                    new And(new IsNotNull(k),new SqlCompare(Op.GE,new Add(v,new Constant(7)),new Constant(80))));
            String[] where={"v=null","v<>3","k is not null and v+7>=80"};
            for(var mode:ScanLowering.Mode.values())for(int i=0;i<predicates.size();i++){
                var p=new Pipeline(List.of(k,v),predicates.get(i),List.of(k),List.of(new Count(),new CountValue(v),new Sum(v),new NestedCountDistinct(v)));
                try(var sql=Demo.compileSql(engine,context,"select k,count(*),count(v),sum(v),count_distinct(v) from t where "+where[i]+" group by k",false);
                    var query=CompiledGroupBy.compile(engine,context,"t",p,force(mode),temporary.getRoot().toPath())){
                    assertEquals(Demo.readSql(sql,context,1),query.execute(context));
                }
            }
            var scan=new PhysicalPlan.Scan("t",List.of(k,v));
            var project=new PhysicalPlan.Project(scan,List.of(new Add(v,new Constant(7)),k,v),List.of("derived","key","unused"));
            var filter=new PhysicalPlan.Filter(project,new SqlCompare(Op.GE,project.columns().get(0),new Constant(80)));
            var group=new PhysicalPlan.Aggregate(filter,List.of(project.columns().get(1)),List.of(new Count(),new Sum(project.columns().get(0))));
            var planned=ScanPlan.from(filter);assertEquals(1,planned.predicateColumns().cardinality());assertTrue(planned.predicateColumns().get(1));
            for(var mode:ScanLowering.Mode.values())try(var query=CompiledPlan.compile(engine,context,group,force(mode),temporary.getRoot().toPath());
                var sql=Demo.compileSql(engine,context,"select k,count(*),sum(v+7) from t where v+7>=80 group by k",false)){
                var actual=query.execute(context).stream().map(row -> new CompiledGroupBy.ResultRow(row.subList(0,1),row.subList(1,row.size()))).sorted().toList();
                assertEquals(Demo.readSql(sql,context,1),actual);
            }
            PhysicalPlan repeated=new PhysicalPlan.Scan("t",List.of(k,v));
            for(int i=0;i<20;i++){
                var argument=repeated.columns().get(0);
                repeated=new PhysicalPlan.Project(repeated,List.of(new Add(argument,argument)),List.of("doubled"));
            }
            assertNull(ScanPlan.from(repeated)); // Materialize before substitution can grow exponentially.
            var reduced=new PhysicalPlan.Aggregate(repeated,List.of(),List.of(new Sum(repeated.columns().get(0))));
            try(var query=CompiledPlan.compile(engine,context,reduced,backend,temporary.getRoot().toPath());
                var sql=Demo.compileSql(engine,context,"select sum(cast(k as long)*1048576) from t",false)){
                assertEquals(Demo.readSql(sql,context,0).getFirst().values(),query.execute(context).getFirst());
            }
            engine.execute("alter table t add column top long",context);
            var top=new Column(2,"top",ColumnType.LONG);
            var p=new Pipeline(List.of(k,v,top),new SqlCompare(Op.EQ,top,new Constant(Long.MIN_VALUE)),List.of(k),List.of(new Sum(v),new Count()));
            for(var mode:ScanLowering.Mode.values())try(var query=CompiledGroupBy.compile(engine,context,"t",p,force(mode),temporary.getRoot().toPath());
                var sql=Demo.compileSql(engine,context,"select k,sum(v),count(*) from t where top=null group by k",false)){
                assertEquals(Demo.readSql(sql,context,1),query.execute(context));
            }
        }
    }
    private Backend force(ScanLowering.Mode mode){return new Backend(){
        public String name(){return backend.name();}
        public CompiledKernel compile(Program program,java.nio.file.Path path)throws java.io.IOException{return backend.compile(program,path);}
        public CompiledKernel compileAlternatives(List<Program> programs,java.nio.file.Path path)throws java.io.IOException{
            return backend.compile(programs.get(mode.ordinal()),path);
        }
    };}
    @Test public void foreignColumnsFailBeforeNativeExecution() {
        var k=new Column(0,"k",ColumnType.INT);var foreign=new Column(0,"other",ColumnType.INT);
        assertThrows(IllegalArgumentException.class,()->ScanPlan.of(List.of(k),new IsNotNull(foreign)));
        var scan=new PhysicalPlan.Scan("t",List.of(k));var project=new PhysicalPlan.Project(scan,List.of(foreign),List.of("bad"));
        assertThrows(IllegalArgumentException.class,()->ScanPlan.from(project));
    }
    private static void write(MemoryType type,long p,long v){var u=Unsafe.getUnsafe();switch(type){case I8->u.putByte(p,(byte)v);case I16->u.putShort(p,(short)v);case I32->u.putInt(p,(int)v);case I64->u.putLong(p,v);default->throw new AssertionError();}}

    /** Linux guard page detects an actual over-read, including SIMD tails. */
    private static final class GuardedPage implements AutoCloseable {
        static final Linker LINKER=Linker.nativeLinker();
        static MethodHandle function(String name,FunctionDescriptor fd){return LINKER.downcallHandle(LINKER.defaultLookup().find(name).orElseThrow(),fd);}
        static final MethodHandle MMAP=function("mmap",FunctionDescriptor.of(ValueLayout.ADDRESS,ValueLayout.ADDRESS,ValueLayout.JAVA_LONG,ValueLayout.JAVA_INT,ValueLayout.JAVA_INT,ValueLayout.JAVA_INT,ValueLayout.JAVA_LONG));
        static final MethodHandle PROTECT=function("mprotect",FunctionDescriptor.of(ValueLayout.JAVA_INT,ValueLayout.ADDRESS,ValueLayout.JAVA_LONG,ValueLayout.JAVA_INT));
        static final MethodHandle UNMAP=function("munmap",FunctionDescriptor.of(ValueLayout.JAVA_INT,ValueLayout.ADDRESS,ValueLayout.JAVA_LONG));
        final long page=Unsafe.getUnsafe().pageSize(),address;final MemorySegment mapping;
        GuardedPage(int bytes)throws Throwable{
            this(bytes, true);
        }
        GuardedPage(int bytes, boolean atEnd)throws Throwable{
            if(bytes>page)throw new IllegalArgumentException();
            mapping=(MemorySegment)MMAP.invokeExact(MemorySegment.NULL,page*3,3,0x22,-1,0L);
            if(mapping.address()==-1)throw new IllegalStateException("mmap failed");
            address=mapping.address()+page+(atEnd?page-bytes:0);
            if((int)PROTECT.invokeExact(mapping,page,0)!=0 || (int)PROTECT.invokeExact(MemorySegment.ofAddress(mapping.address()+page*2),page,0)!=0){close();throw new IllegalStateException("mprotect failed");}
        }
        @Override public void close(){try{if((int)UNMAP.invokeExact(mapping,page*3)!=0)throw new IllegalStateException("munmap failed");}catch(Throwable e){throw new RuntimeException(e);}}
    }
}
