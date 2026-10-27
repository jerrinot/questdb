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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Pipeline.*;

@RunWith(Parameterized.class)
public class ParallelGroupByTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private final Column k = new Column(0, "k", ColumnType.INT);
    private final Column v = new Column(1, "v", ColumnType.LONG);

    public ParallelGroupByTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"c", new NativeBackend()},
                new Object[]{"asmjit", new AsmJitBackend()});
    }
    @Before public void setUp() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder("db").toString());
        engine = new CairoEngine(config);
        context = new SqlExecutionContextImpl(engine, 1).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
    }
    @After public void tearDown() { if (context != null) context.close(); if (engine != null) engine.close(); }

    @Test public void mixedStatesGrowMergeAndReuseAcrossWorkerCounts() throws Exception {
        engine.execute("create table t as (select cast(case when x%17=0 then null else x%257 end as int) k,"
                + "case when x%13=0 then null else x%769 end v from long_sequence(150003))", context);
        var p = new Pipeline(List.of(k,v), new SqlCompare(Ir.Op.NE,v,new Constant(2)),
                List.of(k,new Add(k,new Constant(7))), List.of(new Count(),new CountValue(v),new Sum(v),new NestedCountDistinct(v)));
        var expected = sql("select k,cast(k as long)+7,count(*),count(v),sum(v),count_distinct(v) from t where v<>2 group by k,cast(k as long)+7",2);
        for (int workers : new int[]{1,2,4}) {
            try (var q = compile(p,workers)) {
                for (int repeat=0;repeat<2;repeat++) {
                    var t = new ExecutionTimings();
                    assertEquals(expected,q.execute(context,new CompiledGroupBy.Options(2,1<<20,128L<<20),t));
                    assertEquals(150003,Arrays.stream(t.workerRows).sum());
                    assertEquals(workers,Arrays.stream(t.workerRows).filter(n->n>0).count());
                    assertTrue(t.grows>workers);
                    assertTrue(t.merge>0);
                    assertEquals(256,t.shards);
                }
            }
        }
    }

    @Test public void parallelModeRejectsTheSeparateFlatDistinctExperiment() throws Exception {
        var p = new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new CountDistinct(v)));
        assertThrows(UnsupportedPlan.class,()->compile(p,4));
    }

    @Test public void globalEmptyFilteredAndFewerRowsThanWorkers() throws Exception {
        engine.execute("create table t (k int,v long)",context);
        var p=new Pipeline(List.of(k,v),new SqlCompare(Ir.Op.GE,v,new Constant(0)),List.of(),
                List.of(new Count(),new CountValue(v),new Sum(v),new NestedCountDistinct(v)));
        try(var q=compile(p,4)) {
            String query="select count(*),count(v),sum(v),count_distinct(v) from t where v>=0";
            assertEquals(sql(query,0),q.execute(context));
            engine.execute("insert into t values (1,null),(2,-3)",context);
            assertEquals(sql(query,0),q.execute(context));
            engine.execute("insert into t values (1,7),(2,7),(3,9)",context);
            assertEquals(sql(query,0),q.execute(context));
        }
    }

    @Test public void partitionFramesAndColumnTopsRemainReadableUntilWorkersExit() throws Exception {
        engine.execute("create table t as (select cast(x%17 as int) k,timestamp_sequence(0,1000000000) ts "
                + "from long_sequence(2000)) timestamp(ts) partition by day",context);
        engine.execute("alter table t add column v long",context);
        engine.execute("insert into t select cast(x%17 as int),timestamp_sequence(2000000000000,1000000000),x%97 from long_sequence(2000)",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new Count(),new CountValue(v),new Sum(v),new NestedCountDistinct(v)));
        try(var q=compile(p,4)) {
            assertEquals(sql("select k,count(*),count(v),sum(v),count_distinct(v) from t group by k",1),q.execute(context));
        }
    }

    @Test public void shardedGlobalDistinctProducesOneRowEvenWithEmptyShards() throws Exception {
        engine.execute("create table t as (select cast(1 as int) k,x%19001 v from long_sequence(50001))",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(),List.of(new Count(),new Sum(v),new NestedCountDistinct(v)));
        var expected=sql("select count(*),sum(v),count_distinct(v) from t",0);
        try(var q=compile(p,4)) {
            for(int repeat=0;repeat<2;repeat++) {
                var t=new ExecutionTimings();
                assertEquals(expected,q.execute(context,CompiledGroupBy.Options.DEFAULT,t));
                assertEquals(256,t.shards);
            }
        }
    }

    @Test public void failuresReleaseAllWorkerMemoryAndAllowReuse() throws Exception {
        engine.execute("create table t as (select cast(x as int) k,x v from long_sequence(40000))",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new Count(),new NestedCountDistinct(v)));
        try(var q=compile(p,4)) {
            long before=Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            assertThrows(IllegalStateException.class,()->q.execute(context,new CompiledGroupBy.Options(2,16,16L<<20)));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            assertThrows(CairoException.class,()->q.execute(context,new CompiledGroupBy.Options(2,65536,100000)));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            Thread coordinator=Thread.currentThread();
            var breaker=new AtomicBooleanCircuitBreaker(engine) {
                int polls;
                @Override public void statefulThrowExceptionIfTrippedNoThrottle() {
                    assertSame(coordinator,Thread.currentThread());
                    if(++polls==12) cancel();
                    super.statefulThrowExceptionIfTrippedNoThrottle();
                }
            };
            context.with(breaker);
            assertThrows(CairoException.class,()->q.execute(context));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            context.with(new AtomicBooleanCircuitBreaker(engine));
            assertEquals(sql("select k,count(*),count_distinct(v) from t group by k",1),q.execute(context));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
        }
    }

    @Test public void sharedBudgetIncludesEveryWorkerAndMergeAndPreservesCallerTracker() throws Exception {
        engine.execute("create table t as (select cast(x%1024 as int) k,x%4096 v from long_sequence(100003))",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new NestedCountDistinct(v)));
        try(var q=compile(p,4);var tracker=new io.questdb.std.MemoryTracker() {
            @Override public long getQueryId(){return 17;}
            @Override public io.questdb.std.MemoryTrackerWorkload getWorkload(){return io.questdb.std.MemoryTrackerWorkload.QUERY;}
            @Override public void close(){destroyNativeBlock();}
        }) {
            context.setMemoryTracker(tracker);
            try {
                Unsafe.getUnsafe().putLong(tracker.nativeAddress()+Unsafe.MEMORY_TRACKER_LIMIT_OFFSET,100000);
                assertThrows(CairoException.class,()->q.execute(context));
                assertEquals(0,tracker.getUsed());
                Unsafe.getUnsafe().putLong(tracker.nativeAddress()+Unsafe.MEMORY_TRACKER_LIMIT_OFFSET,64L<<20);
                var t=new ExecutionTimings();
                assertEquals(sql("select k,count_distinct(v) from t group by k",1),q.execute(context,CompiledGroupBy.Options.DEFAULT,t));
                assertEquals(0,tracker.getUsed());
                assertTrue(t.peakBytes>100000);
                assertThrows(CairoException.class,()->q.execute(context,new CompiledGroupBy.Options(64,1<<20,100000)));
                assertEquals(0,tracker.getUsed());
                assertEquals(64L<<20,tracker.getLimit());
            } finally {context.setMemoryTracker(null);}
        }
    }

    @Test public void mergeCapacityFailureCleansUpCompletedWorkerStates() throws Exception {
        engine.execute("create table t as (select cast(x as int) k,x v from long_sequence(200))",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new Count()));
        try(var q=compile(p,4)) {
            long before=Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            var t=new ExecutionTimings();
            assertThrows(IllegalStateException.class,()->q.execute(context,new CompiledGroupBy.Options(2,128,16L<<20),t));
            assertEquals(200,Arrays.stream(t.workerRows).sum());
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            assertEquals(200,q.execute(context).size());
        }
    }

    @Test public void compilationFailureClosesAlreadyCompiledCode() throws Exception {
        engine.execute("create table t (k int,v long)",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new Count()));
        var closed=new AtomicInteger();
        Backend injected=new Backend() {
            int calls;
            @Override public String name(){return "compile-failure";}
            @Override public CompiledKernel compile(Ir.Program program,Path path)throws IOException {
                if(calls++!=0)throw new IOException("merge compilation failed");
                var code=backend.compile(program,path);
                return new CompiledKernel(code,()->{code.close();closed.incrementAndGet();},path,code.compilationNanos());
            }
        };
        assertThrows(IOException.class,()->ParallelGroupBy.compile(engine,context,"t",p,injected,artifacts(),2));
        assertEquals(1,closed.get());
    }

    @Test public void generatedCountAndSumMergeMatchCoreIncludingNullAndOverflowStates() throws Exception {
        var p=new Pipeline(List.of(k,v),new All(),List.of(),List.of(new Count(),new Sum(v)));
        try(var code=backend.compile(p.lowerMerge(),artifacts());var arena=Arena.ofConfined();
            var allocator=new io.questdb.griffin.engine.groupby.FastGroupByAllocator(4096,1L<<30);
            var binding=KernelMemory.bind(allocator,()->{})) {
            var count=new CountLongConstGroupByFunction();
            var sum=new SumLongGroupByFunction(LongColumn.newInstance(0));
            var types=new ArrayColumnTypes(); count.initValueTypes(types);sum.initValueTypes(types);
            var dst=new FlyweightPackedMapValue(types);var src=new FlyweightPackedMapValue(types);
            dst.of(arena.allocate(16,8).address());src.of(arena.allocate(16,8).address());
            long source=arena.allocate(p.entryBytes(),8).address();Unsafe.getUnsafe().putLong(source,1);
            for(long a:new long[]{0,1,Long.MIN_VALUE,Long.MAX_VALUE,-7}) {
                for(long b:new long[]{0,2,Long.MIN_VALUE,Long.MAX_VALUE,-3}) {
                    long state=TestTables.create(arena,8,p.entryBytes()).address();
                    dst.putLong(0,a);dst.putLong(1,a); // Map.merge copies new keys verbatim.
                    src.putLong(0,b);src.putLong(1,b);
                    count.merge(dst,src);sum.merge(dst,src);
                    for(long value:new long[]{a,b}) {
                        Unsafe.getUnsafe().putLong(source+8,value);Unsafe.getUnsafe().putLong(source+16,value);
                        assertEquals(0,code.run(source,1,state));binding.throwIfFailed();
                    }
                    long entry=TableState.entries(state); // Empty key hashes to slot 0.
                    assertEquals(dst.getLong(0),Unsafe.getUnsafe().getLong(entry+8));
                    assertEquals(dst.getLong(1),Unsafe.getUnsafe().getLong(entry+16));
                }
            }
        }
    }

    @Test public void workerFailureWaitsForOtherNativeInvocationBeforeFreeingMemory() throws Exception {
        failureWaitsForOtherInvocation(0);
    }

    @Test public void shardMergeFailureWaitsForOtherNativeInvocationBeforeFreeingMemory() throws Exception {
        failureWaitsForOtherInvocation(1);
    }

    private void failureWaitsForOtherInvocation(int phase) throws Exception {
        engine.execute("create table t as (select cast(x as int) k,x v from long_sequence(10000))",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new Count()));
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var failed=new CountDownLatch(1);
        var invocations=new AtomicInteger();
        Backend injected=new Backend() {
            int compiled;
            @Override public String name(){return "failure-injection";}
            @Override public CompiledKernel compile(Ir.Program program,Path path)throws IOException {
                var code=backend.compile(program,path);
                if(compiled++!=phase)return code;
                return new CompiledKernel((columns,rows,state)->{
                    int invocation=invocations.getAndIncrement();
                    if(invocation==0) {
                        entered.countDown();awaitLatch(release);
                        // The allocator and raw table must survive the other task's failure.
                        assertTrue(KernelMemory.allocator().allocated()>0);
                        return code.run(columns,rows,state);
                    }
                    if(invocation==1) {
                        awaitLatch(entered);failed.countDown();throw new IllegalStateException("injected worker failure");
                    }
                    return code.run(columns,rows,state);
                },code,path,code.compilationNanos());
            }
        };
        try(var q=ParallelGroupBy.compile(engine,context,"t",p,injected,artifacts(),2);
            var caller=Executors.newSingleThreadExecutor()) {
            long before=Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            var result=caller.submit(()->q.execute(context));
            try {
                assertTrue(failed.await(10,TimeUnit.SECONDS));
                assertThrows(TimeoutException.class,()->result.get(100,TimeUnit.MILLISECONDS));
            } finally {release.countDown();}
            var error=assertThrows(ExecutionException.class,()->result.get(10,TimeUnit.SECONDS));
            assertEquals("injected worker failure",error.getCause().getMessage());
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            assertEquals(10000,q.execute(context).size());
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
        } finally {release.countDown();}
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {if(!latch.await(10,TimeUnit.SECONDS))throw new AssertionError("Latch timed out");}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw new AssertionError(e);}
    }
    private Path artifacts(){return temporary.getRoot().toPath().resolve("code");}
    private ParallelGroupBy compile(Pipeline p,int workers)throws Exception {
        return ParallelGroupBy.compile(engine,context,"t",p,backend,artifacts(),workers);
    }
    private List<CompiledGroupBy.ResultRow> sql(String query,int keys)throws Exception {
        try(var factory=Demo.compileSql(engine,context,query,false)){return Demo.readSql(factory,context,keys);}
    }
}
