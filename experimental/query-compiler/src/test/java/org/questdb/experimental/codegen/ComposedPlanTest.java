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
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Pipeline.*;

@RunWith(Parameterized.class)
public class ComposedPlanTest {
    @Rule public TemporaryFolder temporary=new TemporaryFolder();
    private final Backend backend;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private final Column key=new Column(0,"k",ColumnType.INT),value=new Column(1,"v",ColumnType.LONG);
    public ComposedPlanTest(String name,Backend backend){this.backend=backend;}
    @Parameterized.Parameters(name="{0}") public static List<Object[]> parameters(){return List.of(
            new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"native",new NativeBackend()},new Object[]{"asmjit",new AsmJitBackend(true)});}
    @Before public void setUp()throws Exception {
        var config=new DefaultCairoConfiguration(temporary.newFolder("db").toString());
        engine=new CairoEngine(config);context=new SqlExecutionContextImpl(engine,1).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(),null,null,-1,null);
        engine.execute("create table l(k int,v long)",context);engine.execute("create table r(k int,v long)",context);
        engine.execute("insert into l values (1,10),(1,20),(2,null),(null,7),(4,-9)",context);
        engine.execute("insert into r values (1,3),(1,5),(2,4),(null,11),(3,99),(4,-1)",context);
    }
    @After public void tearDown(){if(context!=null)context.close();if(engine!=null)engine.close();}
    private PhysicalPlan.Scan scan(String table){return new PhysicalPlan.Scan(table,List.of(key,value));}

    @Test public void standaloneScanFilterProjectionAndEmptyResult()throws Exception {
        var filter=new PhysicalPlan.Filter(scan("l"),new SqlCompare(Ir.Op.GE,value,new Constant(0)));
        var project=new PhysicalPlan.Project(filter,List.of(new Add(value,new Constant(5)),key),List.of("amount","key"));
        assertMatches(project,"select v+5,k from l where v>=0",1);
        assertMatches(new PhysicalPlan.Filter(project,new SqlCompare(Ir.Op.LT,project.columns().getFirst(),new Constant(0))),
                "select v+5,k from l where v>=0 and v+5<0",1);
    }
    @Test public void duplicateAndNullJoinKeysFilterProjectionAggregate()throws Exception {
        var join=new PhysicalPlan.HashJoin(scan("l"),scan("r"),List.of(key),List.of(key));
        var c=join.columns();
        var filter=new PhysicalPlan.Filter(join,new SqlCompare(Ir.Op.GE,c.get(3),new Constant(0)));
        var project=new PhysicalPlan.Project(filter,List.of(c.get(0),new Add(c.get(1),c.get(3)),c.get(3)),List.of("k","term","rv"));
        var p=project.columns();
        var aggregate=new PhysicalPlan.Aggregate(project,List.of(p.get(0)),List.of(new Sum(p.get(1)),new Count(),new NestedCountDistinct(p.get(2))));
        assertMatches(aggregate,"select l.k,sum(l.v+r.v),count(*),count_distinct(r.v) from l join r on l.k=r.k where r.v>=0 group by l.k",3);
    }
    @Test public void aggregateJoinAggregateCreatesDependentPipelines()throws Exception {
        var left=new PhysicalPlan.Aggregate(scan("l"),List.of(key),List.of(new Sum(value),new Count()));
        var right=new PhysicalPlan.Aggregate(scan("r"),List.of(key),List.of(new Count()));
        var join=new PhysicalPlan.HashJoin(left,right,List.of(left.columns().get(0)),List.of(right.columns().get(0)));
        var c=join.columns();
        var aggregate=new PhysicalPlan.Aggregate(join,List.of(),List.of(new Sum(c.get(1)),new Sum(new Add(c.get(2),c.get(4))),new Count()));
        assertMatches(aggregate,"select sum(a.s),sum(a.n+b.n),count(*) from (select k,sum(v) s,count(*) n from l group by k) a join (select k,count(*) n from r group by k) b on a.k=b.k",5);
        engine.execute("truncate table l",context);
        assertMatches(aggregate,"select sum(a.s),sum(a.n+b.n),count(*) from (select k,sum(v) s,count(*) n from l group by k) a join (select k,count(*) n from r group by k) b on a.k=b.k",5);
    }
    @Test public void compositeJoinKeysAndSharedBuildSubplan()throws Exception {
        var left=scan("l");var right=scan("r");
        engine.execute("insert into r values (1,10),(1,10),(null,7)",context);
        var join=new PhysicalPlan.HashJoin(left,right,List.of(key,value),List.of(key,value));
        var output=new PhysicalPlan.Project(join,List.of(join.columns().get(0),join.columns().get(1)),List.of("k","v"));
        assertMatches(output,"select l.k,l.v from l join r on l.k=r.k and l.v=r.v",2);
        var self=new PhysicalPlan.HashJoin(left,left,List.of(key),List.of(key));
        var aggregate=new PhysicalPlan.Aggregate(self,List.of(),List.of(new Count()));
        assertMatches(aggregate,"select count(*) from l a join l b on a.k=b.k",3);
        var shared=new PhysicalPlan.Aggregate(left,List.of(key),List.of(new Count()));
        var groupedSelf=new PhysicalPlan.HashJoin(shared,shared,List.of(shared.columns().getFirst()),List.of(shared.columns().getFirst()));
        assertMatches(new PhysicalPlan.Aggregate(groupedSelf,List.of(),List.of(new Count())),
                "select count(*) from (select k,count(*) n from l group by k) a join (select k,count(*) n from l group by k) b on a.k=b.k",4);
        var first=new PhysicalPlan.HashJoin(left,right,List.of(key),List.of(key));
        var threeWay=new PhysicalPlan.HashJoin(first,left,List.of(first.columns().getFirst()),List.of(key));
        assertMatches(new PhysicalPlan.Aggregate(threeWay,List.of(),List.of(new Count(),new Sum(threeWay.columns().get(3)),new Sum(threeWay.columns().get(5)))),
                "select count(*),sum(b.v),sum(c.v) from l a join r b on a.k=b.k join l c on b.k=c.k",4);
    }
    @Test public void growthMemoryFailureCancellationAndRepeatedExecution()throws Exception {
        engine.execute("truncate table l",context);engine.execute("truncate table r",context);
        engine.execute("insert into l select cast(x%3000 as int),x from long_sequence(30000)",context);
        engine.execute("insert into r select cast(x%5000 as int),x from long_sequence(20000)",context);
        var join=new PhysicalPlan.HashJoin(scan("l"),scan("r"),List.of(key),List.of(key));
        var aggregate=new PhysicalPlan.Aggregate(join,List.of(),List.of(new Count(),new Sum(join.columns().get(1))));
        try(var query=compile(aggregate)) {
            long before=Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            assertThrows(IllegalStateException.class,()->query.execute(context,new CompiledGroupBy.Options(2,64,16L<<20)));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            assertThrows(CairoException.class,()->query.execute(context,new CompiledGroupBy.Options(2,65536,32768)));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            var expected=sql("select count(*),sum(l.v) from l join r on l.k=r.k");
            for(int i=0;i<2;i++){assertEquals(expected,canonical(query.execute(context)));assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));}
            var breaker=new AtomicBooleanCircuitBreaker(engine) {
                int polls;
                @Override public void statefulThrowExceptionIfTrippedNoThrottle(){if(++polls==50)cancel();super.statefulThrowExceptionIfTrippedNoThrottle();}
            };
            context.with(breaker);assertThrows(CairoException.class,()->query.execute(context));
            assertEquals(before,Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION));
            context.with(new AtomicBooleanCircuitBreaker(engine));assertEquals(expected,canonical(query.execute(context)));
        }
    }
    @Test public void globalAggregateOverEmptyInputHasOneRow()throws Exception {
        engine.execute("truncate table l",context);
        var a=new PhysicalPlan.Aggregate(scan("l"),List.of(),List.of(new Count(),new Sum(value),new NestedCountDistinct(value)));
        assertMatches(a,"select count(*),sum(v),count_distinct(v) from l",2);
    }
    @Test public void compilationFailureReleasesEarlierPipelinesAndScans()throws Exception {
        int[] compiled={0},closed={0};
        var failing=new Backend() {
            @Override public String name(){return backend.name();}
            @Override public CompiledKernel compile(Ir.Program program,Path artifacts)throws IOException {
                if(++compiled[0]==2)throw new IOException("injected second-pipeline compilation failure");
                var actual=backend.compile(program,artifacts);
                return new CompiledKernel(actual,()->{actual.close();closed[0]++;},actual.artifacts(),actual.compilationNanos());
            }
        };
        var aggregate=new PhysicalPlan.Aggregate(scan("l"),List.of(key),List.of(new Count()));
        int readers=engine.getBusyReaderCount();
        var failure=assertThrows(IOException.class,()->CompiledPlan.compile(engine,context,aggregate,failing,temporary.getRoot().toPath()));
        assertEquals("injected second-pipeline compilation failure",failure.getMessage());
        assertEquals(2,compiled[0]);assertEquals(1,closed[0]);assertEquals(readers,engine.getBusyReaderCount());
        assertMatches(aggregate,"select k,count(*) from l group by k",2);
    }
    private void assertMatches(PhysicalPlan p,String sql,int pipelines)throws Exception {
        try(var query=compile(p)){assertEquals(pipelines,query.pipelineCount());assertEquals(sql(sql),canonical(query.execute(context)));}
    }
    private CompiledPlan compile(PhysicalPlan p)throws Exception{return CompiledPlan.compile(engine,context,p,backend,temporary.getRoot().toPath().resolve("code"));}
    private List<CompiledGroupBy.ResultRow> sql(String sql)throws Exception {
        try(var factory=Demo.compileSql(engine,context,sql,false)){return Demo.readSql(factory,context,factory.getMetadata().getColumnCount());}
    }
    private static List<CompiledGroupBy.ResultRow> canonical(List<List<Long>> rows) {
        var result=new ArrayList<CompiledGroupBy.ResultRow>();for(var row:rows)result.add(new CompiledGroupBy.ResultRow(row,List.of()));
        Collections.sort(result);return result;
    }
}
