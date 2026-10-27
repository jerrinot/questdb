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

/** Console-only exploratory matrix. No timings are persisted; each result is checked against SQL. */
public final class WorkloadMatrix {
    private static final Column K = new Column(0,"k",ColumnType.INT);
    private static final Column V = new Column(1,"v",ColumnType.LONG);
    private static final Column F = new Column(2,"f",ColumnType.BYTE);
    private static final Column B = new Column(2,"b",ColumnType.SHORT);
    private static final Path ARTIFACTS = Path.of("experimental/query-compiler/target/matrix-code");
    record Spec(String name, String ddl, String sql, Pipeline pipeline, Kernel control, Kernel finish) { }
    @FunctionalInterface interface Execution { List<CompiledGroupBy.ResultRow> run(ExecutionTimings t) throws Exception; }
    record Run(String name, Execution execute) { }

    public static void main(String[] args) throws Exception {
        int repetitions=args.length>0?Integer.parseInt(args[0]):9;
        String filter=args.length>1?args[1]:"";
        if(repetitions<3)throw new IllegalArgumentException("At least three repetitions required");
        var database=Files.createTempDirectory("questdb-codegen-matrix-");
        try {
            var config=new DefaultCairoConfiguration(database.toString());
            try(var engine=new CairoEngine(config);var context=new SqlExecutionContextImpl(engine,1).with(
                    config.getFactoryProvider().getSecurityContextFactory().getRootContext(),null,null,-1,null)) {
                System.out.println("One thread; 20 warmups; rotated execution order. All maps start at 64 slots.");
                System.out.println("Times exclude ONLY the per-sample canonical comparison sort; allocation/copy/cleanup remain included.");
                for(var spec:specs()) {
                    if(!spec.name().contains(filter))continue;
                    clearDirectory(ARTIFACTS);
                    engine.execute(spec.ddl(),context);
                    try { measure(engine,context,spec,repetitions); }
                    finally {engine.execute("drop table input",context);}
                }
            }
        } finally {LogFactory.haltInstance();clearDirectory(database);}
    }

    static void measure(CairoEngine engine,SqlExecutionContextImpl context,Spec s,int repetitions)throws Exception {
        measure(engine,context,s,repetitions,20);
    }
    static void measure(CairoEngine engine,SqlExecutionContextImpl context,Spec s,int repetitions,int warmups)throws Exception {
        var p=s.pipeline();
        long inputRows;
        try(var reader=engine.getReader("input")){inputRows=reader.size();}
        try(var scalar=Demo.compileSql(engine,context,s.sql(),false);
            var normal=Demo.compileSql(engine,context,s.sql(),true);
            var control=s.control()==null?null:CompiledGroupBy.withKernels(engine,context,"input",p,s.control(),s.finish());
            var nativeQuery=CompiledGroupBy.compile(engine,context,"input",p,new NativeBackend(),ARTIFACTS);
            var asmjit=CompiledGroupBy.compile(engine,context,"input",p,new AsmJitBackend(),ARTIFACTS);
            var llvm=CompiledGroupBy.compile(engine,context,"input",p,new LlvmBackend(),ARTIFACTS)) {
            var expected=Demo.readSql(scalar,context,p.keys().size());
            var runs=new ArrayList<>(List.of(
                    new Run("scalar",t->Demo.readSql(scalar,context,p.keys().size(),t)),
                    new Run("normal",t->Demo.readSql(normal,context,p.keys().size(),t)),
                    new Run("native",t->nativeQuery.execute(context,CompiledGroupBy.Options.DEFAULT,t)),
                    new Run("asmjit",t->asmjit.execute(context,CompiledGroupBy.Options.DEFAULT,t)),
                    new Run("llvm-o2",t->llvm.execute(context,CompiledGroupBy.Options.DEFAULT,t))));
            if(control!=null)runs.add(new Run("control",t->control.execute(context,CompiledGroupBy.Options.DEFAULT,t)));
            double[][] elapsed=new double[runs.size()][repetitions];
            double[] first=new double[runs.size()];
            long[] peaks=new long[runs.size()],grows=new long[runs.size()];
            for(int round=-warmups;round<repetitions;round++) {
                for(int offset=0;offset<runs.size();offset++) {
                    int i=Math.floorMod(round+offset,runs.size());
                    var t=new ExecutionTimings();
                    long start=System.nanoTime();
                    var actual=runs.get(i).execute().run(t);
                    double millis=(System.nanoTime()-start-t.sort)/1e6;
                    if(!expected.equals(actual))throw new AssertionError(s.name()+" / "+runs.get(i).name()+" mismatched SQL");
                    if(round==-warmups)first[i]=millis;
                    if(round>=0)elapsed[i][round]=millis;
                    peaks[i]=Math.max(peaks[i],t.peakBytes);grows[i]=Math.max(grows[i],t.grows);
                }
            }
            long pairs=-1;
            for(int a=0;a<p.aggregates().size();a++) {
                if(p.aggregates().get(a) instanceof NestedCountDistinct||p.aggregates().get(a) instanceof CountDistinct) {
                    pairs=0;for(var row:expected)pairs+=row.values().get(a);break;
                }
            }
            System.out.println(String.format(Locale.ROOT,"%n%s: input rows=%d; output groups=%d; non-null distinct pairs=%s; warmups=%d; compile native=%.1f ms AsmJit=%.3f ms LLVM-O2=%.3f ms",
                    s.name(),inputRows,expected.size(),pairs<0?"n/a":Long.toString(pairs),warmups,nativeQuery.compilationNanos()/1e6,asmjit.compilationNanos()/1e6,llvm.compilationNanos()/1e6));
            for(int i=0;i<runs.size();i++) {
                Arrays.sort(elapsed[i]);
                System.out.printf(Locale.ROOT,"  %-7s median %8.3f ms [%.3f, %.3f]; first %.3f; peak %s; grows %d%n",
                        runs.get(i).name(),median(elapsed[i]),elapsed[i][0],elapsed[i][repetitions-1],first[i],i<2?"n/a":String.format(Locale.ROOT,"%.2f MiB",peaks[i]/1048576.0),grows[i]);
            }
        }
    }
    static List<Spec> specs() {
        var specs=new ArrayList<Spec>();
        specs.add(mixed("tiny",128,17,7));
        specs.add(mixed("mixed",1000000,1024,7));
        specs.add(mixed("selective",1000000,8191,100));
        specs.add(distinct("nested-repeated",1000000,"(x-1)%1024","((x-1)/1024)%128",false));
        specs.add(distinct("nested-unique",1000000,"(x-1)%1024","x",false));
        specs.add(distinct("nested-singleton",100000,"x","x",false));
        specs.add(distinct("nested-skew",500000,"case when x%10<9 then 0 else x%997 end","x%8191",false));
        specs.add(distinct("flat-repeated",500000,"(x-1)%1024","((x-1)/1024)%32",true));
        specs.add(distinct("flat-unique",200000,"(x-1)%1024","x",true));
        var two=new Pipeline(List.of(K,V,B),new SqlCompare(Ir.Op.GE,V,new Constant(0)),List.of(K,B),
                List.of(new CountValue(V),new Sum(new Add(V,new Constant(13))),new NestedCountDistinct(V)));
        specs.add(new Spec("multiple-keys-null-skew",ddl(300000,"cast(case when x%4=0 then null else x%509 end as int) k,"
                +"case when x%3=0 then null else x%997 end v,cast(x%13-6 as short) b"),
                "select k,b,count(v),sum(v+13),count_distinct(v) from input where v>=0 group by k,b",two,null,null));
        var shortKey=new Column(0,"k",ColumnType.SHORT);
        specs.add(new Spec("short-key-count",ddl(1000000,"cast(x%31-15 as short) k"),
                "select k,count(*) from input where k<>0 group by k",
                new Pipeline(List.of(shortKey),new SqlCompare(Ir.Op.NE,shortKey,new Constant(0)),List.of(shortKey),List.of(new Count())),null,null));
        var byteKey=new Column(0,"k",ColumnType.BYTE);
        specs.add(new Spec("byte-global",ddl(500000,"cast(x%121 as byte) k"),
                "select count(*),sum(k) from input where k>=60",
                new Pipeline(List.of(byteKey),new SqlCompare(Ir.Op.GE,byteKey,new Constant(60)),List.of(),List.of(new Count(),new Sum(byteKey))),null,null));
        // These distributions are validation inputs, not code-generation strategy selectors.
        specs.add(distinct("heldout-correlated",370019,"case when x%17=0 then null else (x*97)%7919-4000 end",
                "case when x%5=0 then null else (x*37)%10007 end",false));
        specs.add(distinct("heldout-all-null",180031,"(x*101)%1237","cast(null as long)",false));
        var longKey=new Column(0,"k",ColumnType.LONG);
        specs.add(new Spec("heldout-wide-key",ddl(170011,"x%997*4294967291-9000000000 k,case when x%11=0 then null else x%7 end v"),
                "select k,count(*),sum(v),count_distinct(v) from input where v<>2 group by k",
                new Pipeline(List.of(longKey,V),new SqlCompare(Ir.Op.NE,V,new Constant(2)),List.of(longKey),
                        List.of(new Count(),new Sum(V),new NestedCountDistinct(V))),null,null));
        return specs;
    }
    private static Spec mixed(String name,int rows,int groups,int selectivity) {
        var p=new Pipeline(List.of(K,V,F),new SqlCompare(Ir.Op.NE,F,new Constant(0)),List.of(K),
                List.of(new Count(),new CountValue(V),new Sum(new Add(V,new Constant(3)))));
        String flag=selectivity==100?"case when x%100=0 then 1 else 0 end":"x%7";
        return new Spec(name,ddl(rows,"cast(x%"+groups+" as int) k,case when x%19=0 then null else x*3 end v,cast("+flag+" as byte) f"),
                "select k,count(*),count(v),sum(v+3) from input where f<>0 group by k",p,HandwrittenKernels::mixed,null);
    }
    private static Spec distinct(String name,int rows,String key,String value,boolean flat) {
        var p=new Pipeline(List.of(K,V),new All(),List.of(K),List.of(flat?new CountDistinct(V):new NestedCountDistinct(V)));
        return new Spec(name,ddl(rows,"cast("+key+" as int) k,case when x%97=0 then null else "+value+" end v"),
                "select k,count_distinct(v) from input group by k",p,
                flat?HandwrittenKernels::distinct:HandwrittenKernels.nestedDistinct(3,0.7),flat?HandwrittenKernels::collapse:null);
    }
    private static String ddl(int rows,String columns){return "create table input as (select "+columns+" from long_sequence("+rows+"))";}
    static void clearDirectory(Path path)throws Exception {
        if(Files.exists(path))try(var files=Files.walk(path)){for(var file:files.sorted(Comparator.reverseOrder()).toList())Files.delete(file);}
    }
    private static double median(double[] a){int m=a.length/2;return a.length%2==0?(a[m-1]+a[m])/2:a[m];}
}
