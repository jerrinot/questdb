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
import io.questdb.griffin.TextPlanSink;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Fixed-work controls for scan lowering; prints results, retains no results archive. */
public final class ScanMatrix {
    private record Run(String name,WorkloadMatrix.Execution execute) { }
    enum Strategy { EAGER_CONTROL, EAGER, FUSED, MASKED, ADAPTIVE }
    static final class ControlBackend implements Backend {
        final Pipeline pipeline;final Strategy strategy;final Backend backend;int phase;
        ControlBackend(Pipeline pipeline,Strategy strategy,Backend backend){this.pipeline=pipeline;this.strategy=strategy;this.backend=backend;}
        @Override public String name(){return strategy.name();}
        @Override public CompiledKernel compile(Program program,Path artifacts)throws IOException {
            // Unfiltered stages deliberately bypass alternative generation.
            int current=phase++;
            if (pipeline.filter() instanceof All && (current==0||current==3) && strategy==Strategy.EAGER_CONTROL)
                program=eager(pipeline,current==3);
            return backend.compile(program,artifacts);
        }
        @Override public CompiledKernel compileAlternatives(List<Program> alternatives,Path artifacts)throws IOException {
            boolean sharded=phase++==3;
            return switch(strategy) {
                case EAGER_CONTROL->backend.compile(eager(pipeline,sharded),artifacts);
                case EAGER->backend.compile(alternatives.get(ScanLowering.Mode.EAGER.ordinal()),artifacts);
                case FUSED->backend.compile(alternatives.get(ScanLowering.Mode.FUSED.ordinal()),artifacts);
                case MASKED->backend.compile(alternatives.get(ScanLowering.Mode.MASKED.ordinal()),artifacts);
                case ADAPTIVE->backend.compileAlternatives(alternatives,artifacts);
            };
        }
    }
    /** The pre-change scan, retained only as an explicit measurement control. */
    static Program eager(Pipeline p,boolean sharded) {
        Builder b=new Builder();List<Var> addresses=new ArrayList<>();
        for(Column c:p.columns())addresses.add(b.let(new Load(MemoryType.ADDRESS,at(COLUMNS,c.index()*8L))));
        Var index=b.let(i64(0));
        b.loop(binary(Op.LT,index,ROWS),loop->{
            List<Value> values=new ArrayList<>();
            for(Column c:p.columns()){
                Var base=addresses.get(c.index());Expr address=new Offset(base,binary(Op.MUL,index,i64(c.width().bytes)));
                Var data=loop.let(new Select(eq(base,nil()),i64(c.nullValue()),new Load(c.width(),address)));
                values.add(new Value(data,c.nullable()?eq(data,i64(c.nullValue())):bool(false)));
            }
            Row row=new Row(p.columns(),values);
            loop.when(p.filter().emit(row),accepted->p.emitGroupBy(accepted,row,STATE,sharded,true));
            loop.assign(index,add(index,i64(1)));
        });b.exit(0);return new Program(b.block());
    }
    public static void main(String[] args)throws Exception {
        if (args.length > 0 && args[0].equals("--resolved-join")) {
            ResolvedJoinMatrix.run(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && args[0].equals("--resolved-three-way")) {
            ResolvedBatchMatrix.runThreeWay(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 0 && args[0].equals("--resolved-sql")) {
            ResolvedBatchMatrix.run(Arrays.copyOfRange(args, 1, args.length), true);
            return;
        }
        if (args.length > 0 && args[0].equals("--resolved-batch")) {
            ResolvedBatchMatrix.run(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        int repetitions=args.length>0?Integer.parseInt(args[0]):9;
        String filter=args.length>1?args[1]:"";
        long rows=args.length>2?Long.parseLong(args[2]):500_000_000L;
        int workers=args.length>3?Integer.parseInt(args[3]):8,warmups=args.length>4?Integer.parseInt(args[4]):5;
        if(rows<500_000_000L||repetitions<3||warmups<1||workers<2||workers>64)throw new IllegalArgumentException("At least 500M rows, 3 samples, 1 warmup, and 2..64 workers required");
        Path db=Files.createTempDirectory("query-scan-matrix-");Path artifacts=Files.createTempDirectory("query-scan-code-");
        try {
            var cfg=new DefaultCairoConfiguration(db.toString());
            try(var engine=new CairoEngine(cfg);var pool=new WorkerPool(new WorkerPoolConfiguration(){public int getWorkerCount(){return workers-1;}public boolean haltOnError(){return true;}})){
                WorkerPoolUtils.setupQueryJobs(pool,engine);pool.start();
                try(var context=new SqlExecutionContextImpl(engine,workers-1).with(cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(),null,null,-1,null)){
                    System.out.printf("Scan A/B: %,d rows/case; %d prototype workers; SQL %d+caller; %d warmups; %d samples; rotated order.%n",rows,workers,workers-1,warmups,repetitions);
                    System.out.println("Allocation/scan/merge/copy/cleanup timed; compilation and comparison sort separate. LLVM O2; no per-thread affinity set.");
                    for(String line:Files.readAllLines(Path.of("/proc/self/status")))if(line.startsWith("Cpus_allowed_list:"))System.out.println(line);
                    for(var spec:specs()){
                        if(!filter.isEmpty()&&Arrays.stream(filter.split(",")).noneMatch(spec.name()::contains))continue;
                        String ddl=Pattern.compile("long_sequence\\((\\d+)\\)").matcher(spec.ddl()).replaceAll(m->"long_sequence("+rows+")");
                        System.out.println("Creating "+spec.name());long start=System.nanoTime();engine.execute(ddl,context);
                        System.out.printf(Locale.ROOT,"Created in %.3f s%n",(System.nanoTime()-start)/1e9);
                        try{measure(engine,context,spec,rows,workers,warmups,repetitions,artifacts);}finally{engine.execute("drop table input",context);}
                    }
                }
            }
        }finally{LogFactory.haltInstance();WorkloadMatrix.clearDirectory(db);WorkloadMatrix.clearDirectory(artifacts);}
    }
    private static void measure(CairoEngine engine,SqlExecutionContextImpl context,WorkloadMatrix.Spec spec,long rows,int workers,int warmups,int reps,Path artifacts)throws Exception {
        try(var reader=engine.getReader("input")){if(reader.size()!=rows)throw new AssertionError("Row count");}
        var queries=new ArrayList<ParallelGroupBy>();var p=spec.pipeline();
        try(var oracle=Demo.compileSql(engine,context,spec.sql(),false);var sql=Demo.compileSql(engine,context,spec.sql(),true)){
            var expected=Demo.readSql(oracle,context,p.keys().size());
            System.out.printf("CASE %s rows=%d groups=%d hash=%d%n",spec.name(),rows,expected.size(),expected.hashCode());
            var sqlPlan=new TextPlanSink();sqlPlan.of(sql,context);
            for(int i=1;i<=sqlPlan.getLineCount();i++)System.out.println("SQL plan: "+sqlPlan.getLine(i));
            var runs=new ArrayList<Run>();
            for(var strategy:Strategy.values()){
                long start=System.nanoTime();var query=ParallelGroupBy.compile(engine,context,"input",p,new ControlBackend(p,strategy,new LlvmBackend()),artifacts,workers);
                queries.add(query);System.out.printf(Locale.ROOT,"COMPILE %s total=%.3f kernels=%.3f ms%n",strategy,(System.nanoTime()-start)/1e6,query.compilationNanos()/1e6);
                runs.add(new Run(strategy.name(),t->query.execute(context,CompiledGroupBy.Options.DEFAULT,t)));
            }
            runs.add(new Run("QUESTDB",t->Demo.readSql(sql,context,p.keys().size(),t)));
            double[][] times=new double[runs.size()][reps],scan=new double[runs.size()][reps],merge=new double[runs.size()][reps];
            long[] faults=new long[runs.size()],reads=new long[runs.size()],peaks=new long[runs.size()];
            for(int round=-warmups;round<reps;round++){
                var line=new StringBuilder(round<0?"warm "+(round+warmups):"sample "+round);
                for(int offset=0;offset<runs.size();offset++){
                    int i=Math.floorMod(round+offset,runs.size());var run=runs.get(i);var t=new ExecutionTimings();var before=Resources.read();long start=System.nanoTime();
                    var actual=run.execute.run(t);double ms=(System.nanoTime()-start-t.sort)/1e6;var after=Resources.read();
                    if(!actual.equals(expected))throw new AssertionError(spec.name()+"/"+run.name+" results");
                    if(t.workerRows!=null&&Arrays.stream(t.workerRows).sum()!=rows)throw new AssertionError("Worker rows");
                    if(round>=0){times[i][round]=ms;scan[i][round]=t.scan/1e6;merge[i][round]=t.merge/1e6;faults[i]+=after.faults-before.faults;reads[i]+=after.bytes-before.bytes;peaks[i]=Math.max(peaks[i],t.peakBytes);}
                    line.append(String.format(Locale.ROOT," %s=%.3f",run.name,ms));
                }System.out.println(line);
            }
            for(int i=0;i<runs.size();i++){
                Arrays.sort(times[i]);Arrays.sort(scan[i]);Arrays.sort(merge[i]);
                System.out.printf(Locale.ROOT,"RESULT %s %s median=%.3f range=[%.3f,%.3f] scan=%.3f merge=%.3f peak-MiB=%.2f major-faults=%d read-bytes=%d%n",spec.name(),runs.get(i).name,median(times[i]),times[i][0],times[i][reps-1],median(scan[i]),median(merge[i]),peaks[i]/1048576.0,faults[i],reads[i]);
            }
        }finally{for(var q:queries)q.close();}
    }
    static List<WorkloadMatrix.Spec> specs(){
        var base=WorkloadMatrix.specs();var result=new ArrayList<WorkloadMatrix.Spec>();
        for(String name:List.of("selective","mixed","nested-repeated","heldout-wide-key"))result.add(base.stream().filter(s->s.name().equals(name)).findFirst().orElseThrow());
        var original=result.getFirst();
        String originalFlag="cast(case when x%100=0 then 1 else 0 end as byte) f";
        String[] names={"random-1pct","random-50pct","none","all","changing"};
        String[] flags={"cast(case when rnd_int(0,9999,0)<100 then 1 else 0 end as byte) f","cast(case when rnd_int(0,9999,0)<5000 then 1 else 0 end as byte) f","cast(0 as byte) f","cast(1 as byte) f",
                "cast(case when x<=250000000 then 0 else 1 end as byte) f"};
        for(int i=0;i<names.length;i++)result.add(new WorkloadMatrix.Spec(names[i],original.ddl().replace(originalFlag,flags[i]),original.sql(),original.pipeline(),null,null));
        var k=original.pipeline().columns().get(0);var v=original.pipeline().columns().get(1);
        for(int type:List.of(ColumnType.SHORT,ColumnType.LONG)){
            var f=new Column(2,"f",type);Scalar term=type==ColumnType.LONG?new Add(f,new Constant(7)):f;
            String field=type==ColumnType.LONG?"case when x%11=0 then null else x%201-100 end f":"cast(x%201-100 as short) f";
            String sql="select k,count(*),count(v),sum(v+3) from input where "+(type==ColumnType.LONG?"f+7":"f")+">=0 group by k";
            result.add(new WorkloadMatrix.Spec(type==ColumnType.LONG?"long-expression":"short-range",original.ddl().replace(originalFlag,field),sql,
                    new Pipeline(List.of(k,v,f),new SqlCompare(Op.GE,term,new Constant(0)),List.of(k),original.pipeline().aggregates()),null,null));
        }
        return result;
    }
    private static double median(double[] x){return x.length%2==0?(x[x.length/2-1]+x[x.length/2])/2:x[x.length/2];}
    record Resources(long faults,long bytes){static Resources read()throws IOException{
        String stat=Files.readString(Path.of("/proc/self/stat"));String[] fields=stat.substring(stat.lastIndexOf(')')+2).trim().split("\\s+");long bytes=0;
        for(String line:Files.readAllLines(Path.of("/proc/self/io")))if(line.startsWith("read_bytes:"))bytes=Long.parseLong(line.substring(11).trim());
        return new Resources(Long.parseLong(fields[9]),bytes);
    }}
}
