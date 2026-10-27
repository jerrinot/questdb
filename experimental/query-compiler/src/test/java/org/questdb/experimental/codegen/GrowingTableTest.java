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
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.List;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Pipeline.*;

@RunWith(Parameterized.class)
public class GrowingTableTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private final Column k = new Column(0, "k", ColumnType.INT);
    private final Column v = new Column(1, "v", ColumnType.LONG);
    public GrowingTableTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"native", new NativeBackend()}, new Object[]{"asmjit", new AsmJitBackend()});
    }
    @Before public void setUp() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder("db").toString());
        engine = new CairoEngine(config);
        context = new SqlExecutionContextImpl(engine, 1).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
    }
    @After public void tearDown() { if (context != null) context.close(); if (engine != null) engine.close(); }

    @Test public void growthPreservesNestedAndMixedStatesAcrossExecutions() throws Exception {
        engine.execute("create table t as (select cast((x-1)/5 as int) k, x%5 v from long_sequence(20000))", context);
        var p = new Pipeline(List.of(k,v), new All(), List.of(k, new Add(k,new Constant(7))),
                List.of(new Sum(v),new NestedCountDistinct(v),new Count(),new CountValue(v)));
        try (var q = compile(p)) {
            for (int i=0;i<2;i++) {
                var t = new ExecutionTimings();
                var actual = q.execute(context,new CompiledGroupBy.Options(2,16384,16L<<20),t);
                assertTrue(t.grows >= 11);
                assertTrue(t.peakBytes < 16L<<20);
                assertEquals(sql("select k,cast(k as long)+7,sum(v),count_distinct(v),count(*),count(v) from t group by k,cast(k as long)+7",2),actual);
                engine.execute("insert into t values (0,99),(null,null),(9999,42)",context);
            }
        }
    }
    @Test public void flatPairAndCollapseTablesBothGrow() throws Exception {
        engine.execute("create table t as (select cast(x%2000 as int) k,x%7000 v from long_sequence(25000))",context);
        var p = new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new CountDistinct(v)));
        try (var q=compile(p)) {
            var t=new ExecutionTimings();
            assertEquals(sql("select k,count_distinct(v) from t group by k",1),
                    q.execute(context,new CompiledGroupBy.Options(2,65536,16L<<20),t));
            assertTrue(t.grows>20);
        }
    }
    @Test public void boundedFailureAndCancellationReleaseMemoryAndAllowReuse() throws Exception {
        engine.execute("create table t as (select cast(x as int) k,x v from long_sequence(40000))",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new NestedCountDistinct(v),new Sum(v)));
        try(var q=compile(p)) {
            long before=Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            assertThrows(IllegalStateException.class,()->q.execute(context,new CompiledGroupBy.Options(2,16,16L<<20)));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            assertThrows(CairoException.class,()->q.execute(context,new CompiledGroupBy.Options(2,65536,32768)));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            var breaker=new AtomicBooleanCircuitBreaker(engine) {
                int polls;
                @Override public void statefulThrowExceptionIfTrippedNoThrottle() {
                    if (++polls==20) cancel();
                    super.statefulThrowExceptionIfTrippedNoThrottle();
                }
            };
            context.with(breaker);
            assertThrows(CairoException.class,()->q.execute(context));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            context.with(new AtomicBooleanCircuitBreaker(engine));
            assertEquals(sql("select k,count_distinct(v),sum(v) from t group by k",1),q.execute(context));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
        }
    }
    @Test public void honorsCallerMemoryTrackerWithoutOwningIt() throws Exception {
        engine.execute("create table t as (select cast(x as int) k,x v from long_sequence(10000))",context);
        var p=new Pipeline(List.of(k,v),new All(),List.of(k),List.of(new Count()));
        try(var q=compile(p);var tracker=new io.questdb.std.MemoryTracker() {
            @Override public long getQueryId(){return 17;}
            @Override public io.questdb.std.MemoryTrackerWorkload getWorkload(){return io.questdb.std.MemoryTrackerWorkload.QUERY;}
            @Override public void close(){destroyNativeBlock();}
        }) {
            Unsafe.getUnsafe().putLong(tracker.nativeAddress()+Unsafe.MEMORY_TRACKER_LIMIT_OFFSET,32768);
            context.setMemoryTracker(tracker);
            try {
                assertThrows(CairoException.class,()->q.execute(context));
                assertEquals(0,tracker.getUsed());
                Unsafe.getUnsafe().putLong(tracker.nativeAddress()+Unsafe.MEMORY_TRACKER_LIMIT_OFFSET,16L<<20);
                assertEquals(10000,q.execute(context).size());
                assertEquals(0,tracker.getUsed());
                assertThrows(CairoException.class,()->q.execute(context,new CompiledGroupBy.Options(2,65536,32768)));
                assertEquals(0,tracker.getUsed());
            } finally {context.setMemoryTracker(null);}
        }
    }
    @Test public void globalAggregationEmptyFilteredAndPopulated() throws Exception {
        engine.execute("create table t (k int,v long)",context);
        var p=new Pipeline(List.of(k,v),new SqlCompare(Ir.Op.GE,v,new Constant(0)),List.of(),
                List.of(new Count(),new Sum(v),new NestedCountDistinct(v)));
        try(var q=compile(p)) {
            String sql="select count(*),sum(v),count_distinct(v) from t where v>=0";
            assertEquals(sql(sql,0),q.execute(context));
            engine.execute("insert into t values (1,null),(2,-3)",context);
            assertEquals(sql(sql,0),q.execute(context));
            engine.execute("insert into t values (1,7),(2,7),(3,9)",context);
            assertEquals(sql(sql,0),q.execute(context));
        }
    }
    @Test public void sqlComparisonsRespectNullsAndWidening() throws Exception {
        engine.execute("create table t (k int,v long)",context);
        engine.execute("insert into t values (null,null),(null,1),(1,null),(1,1),(2,1),(-1,0)",context);
        for(var op:List.of(Ir.Op.EQ,Ir.Op.NE,Ir.Op.LT,Ir.Op.GE)) {
            String token=switch(op){case EQ->"=";case NE->"!=";case LT->"<";default->">=";};
            for(boolean literal:List.of(false,true)) {
                var p=new Pipeline(List.of(k,v),new SqlCompare(op,k,literal?new Constant(Long.MIN_VALUE):v),
                        List.of(k),List.of(new Count()));
                try(var q=compile(p)) {
                    assertEquals(sql("select k,count(*) from t where k"+token+(literal?"null":"v")+" group by k",1),q.execute(context));
                }
            }
        }
    }
    private CompiledGroupBy compile(Pipeline p)throws Exception {
        return CompiledGroupBy.compile(engine,context,"t",p,backend,temporary.getRoot().toPath().resolve("code"));
    }
    private List<CompiledGroupBy.ResultRow> sql(String sql,int keys)throws Exception {
        try(var f=Demo.compileSql(engine,context,sql,false)){return Demo.readSql(f,context,keys);}
    }
}
