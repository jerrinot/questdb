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
import io.questdb.log.LogFactory;
import java.nio.file.*;
import java.util.*;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Fresh code for each measured execution. Includes phase preparation and first execution, without a query-code cache. */
public final class CompilerDemo {
    private record Case(String name,PhysicalPlan plan,String sql) { }
    private static final Path ARTIFACTS=Path.of("experimental/query-compiler/target/compiler-code");
    public static void main(String[] args)throws Exception {
        int rows=args.length>0?Integer.parseInt(args[0]):100000;
        int repetitions=args.length>1?Integer.parseInt(args[1]):9;
        if(rows<0||repetitions<3)throw new IllegalArgumentException("Non-negative rows and at least three repetitions required");
        var directory=Files.createTempDirectory("questdb-compiler-");
        try {
            var config=new DefaultCairoConfiguration(directory.toString());
            try(var engine=new CairoEngine(config);var context=new SqlExecutionContextImpl(engine,1).with(
                    config.getFactoryProvider().getSecurityContextFactory().getRootContext(),null,null,-1,null)) {
                engine.execute("create table facts as (select cast(x%1021 as int) k,case when x%19=0 then null else x%1009 end amount,x%1777 customer from long_sequence("+rows+"))",context);
                engine.execute("create table dim as (select cast(x%257 as int) k,x%11 category from long_sequence(700))",context);
                System.out.println("One thread; every measured query compiles fresh code and executes it once; no query-code cache.");
                var backends=List.<Backend>of(new AsmJitBackend(),new NativeBackend(),new LlvmBackend(1),new LlvmBackend(2));
                for(var scenario:cases())measure(engine,context,scenario,backends,repetitions,rows);
            }
        }finally{LogFactory.haltInstance();WorkloadMatrix.clearDirectory(directory);}
    }
    private static void measure(CairoEngine engine,SqlExecutionContextImpl context,Case scenario,List<Backend> backends,int repetitions,int rows)throws Exception {
        List<CompiledGroupBy.ResultRow> expected;
        try(var factory=Demo.compileSql(engine,context,scenario.sql(),false)){expected=Demo.readSql(factory,context,factory.getMetadata().getColumnCount());}
        int count=backends.size()+1;
        double[][] preparation=new double[count][repetitions],execution=new double[count][repetitions],total=new double[count][repetitions];
        double[][] code=new double[count][repetitions];
        // Three unmeasured passes initialize the JVM/compiler infrastructure. They never cache query code.
        for(int round=-3;round<repetitions;round++) {
            for(int offset=0;offset<count;offset++) {
                int which=Math.floorMod(round+offset,count);
                double compileMs,executeMs,codeMs=0;
                if(which==0) {
                    long start=System.nanoTime();
                    try(var factory=Demo.compileSql(engine,context,scenario.sql(),true)) {
                        compileMs=(System.nanoTime()-start)/1e6;
                        var timing=new ExecutionTimings();start=System.nanoTime();
                        var result=Demo.readSql(factory,context,factory.getMetadata().getColumnCount(),timing);
                        executeMs=(System.nanoTime()-start-timing.sort)/1e6;
                        if(!expected.equals(result))throw new AssertionError("Normal SQL mismatch");
                    }
                } else {
                    WorkloadMatrix.clearDirectory(ARTIFACTS);
                    long start=System.nanoTime();
                    try(var query=CompiledPlan.compile(engine,context,scenario.plan(),backends.get(which-1),ARTIFACTS)) {
                        compileMs=(System.nanoTime()-start)/1e6;codeMs=query.compilationNanos()/1e6;
                        start=System.nanoTime();var result=query.execute(context);executeMs=(System.nanoTime()-start)/1e6;
                        if(!expected.equals(canonical(result)))throw new AssertionError(scenario.name()+" / "+backends.get(which-1).name()+" mismatch");
                        if(round==-3)System.out.printf(Locale.ROOT,"%s %s first preparation %.3f ms (backend %.3f), execute %.3f ms, pipelines %d%n",
                                scenario.name(),backends.get(which-1).name(),compileMs,codeMs,executeMs,query.pipelineCount());
                    }
                }
                if(round>=0) {
                    preparation[which][round]=compileMs;execution[which][round]=executeMs;
                    code[which][round]=codeMs;total[which][round]=compileMs+executeMs;
                }
            }
        }
        System.out.printf("%n%s: %d fact rows, %d output rows; %d fresh compiles per backend%n",scenario.name(),rows,expected.size(),repetitions);
        for(int i=0;i<count;i++) {
            Arrays.sort(preparation[i]);Arrays.sort(execution[i]);Arrays.sort(total[i]);Arrays.sort(code[i]);
            System.out.printf(Locale.ROOT,"  %-7s prepare %.3f ms (backend %.3f), first execute %.3f ms, total median %.3f ms [%.3f, %.3f]%n",
                    i==0?"normal":backends.get(i-1).name(),median(preparation[i]),median(code[i]),median(execution[i]),median(total[i]),total[i][0],total[i][repetitions-1]);
        }
    }
    private static List<Case> cases() {
        Column fk=new Column(0,"k",ColumnType.INT),amount=new Column(1,"amount",ColumnType.LONG),customer=new Column(2,"customer",ColumnType.LONG);
        Column dk=new Column(0,"k",ColumnType.INT),category=new Column(1,"category",ColumnType.LONG);
        var facts=new PhysicalPlan.Scan("facts",List.of(fk,amount,customer));
        var dim=new PhysicalPlan.Scan("dim",List.of(dk,category));
        var filter=new PhysicalPlan.Filter(facts,new SqlCompare(Ir.Op.GE,amount,new Constant(0)));
        var aggregate=new PhysicalPlan.Aggregate(filter,List.of(fk),List.of(new Sum(amount),new Count(),new NestedCountDistinct(customer)));
        var join=new PhysicalPlan.HashJoin(dim,filter,List.of(dk),List.of(fk));
        var joined=join.columns();
        var joinAggregate=new PhysicalPlan.Aggregate(join,List.of(joined.get(1)),
                List.of(new Sum(joined.get(3)),new Count(),new NestedCountDistinct(joined.get(4))));
        var dimGroups=new PhysicalPlan.Aggregate(dim,List.of(dk),List.of(new Sum(category)));
        var factGroups=new PhysicalPlan.Aggregate(filter,List.of(fk),List.of(new Sum(amount)));
        var groupedJoin=new PhysicalPlan.HashJoin(dimGroups,factGroups,List.of(dimGroups.columns().get(0)),List.of(factGroups.columns().get(0)));
        var nested=new PhysicalPlan.Aggregate(groupedJoin,List.of(),List.of(new Sum(groupedJoin.columns().get(1)),new Sum(groupedJoin.columns().get(3)),new Count()));
        return List.of(new Case("filter-group",aggregate,"select k,sum(amount),count(*),count_distinct(customer) from facts where amount>=0 group by k"),
                new Case("join-group",joinAggregate,"select d.category,sum(f.amount),count(*),count_distinct(f.customer) from dim d join facts f on d.k=f.k where f.amount>=0 group by d.category"),
                new Case("aggregate-join-aggregate",nested,"select sum(d.s),sum(f.s),count(*) from (select k,sum(category) s from dim group by k) d join (select k,sum(amount) s from facts where amount>=0 group by k) f on d.k=f.k"));
    }
    private static List<CompiledGroupBy.ResultRow> canonical(List<List<Long>> rows) {
        var result=new ArrayList<CompiledGroupBy.ResultRow>();for(var row:rows)result.add(new CompiledGroupBy.ResultRow(row,List.of()));
        Collections.sort(result);return result;
    }
    private static double median(double[] v){int m=v.length/2;return v.length%2==0?(v[m-1]+v[m])/2:v[m];}
}
