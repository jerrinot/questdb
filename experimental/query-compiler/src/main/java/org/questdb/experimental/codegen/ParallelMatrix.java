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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

/** Console-only scaling experiment. Keeps the serial executor and a real SQL worker pool as controls. */
public final class ParallelMatrix {
    private record Run(String name, WorkloadMatrix.Execution execute) { }

    public static void main(String[] args) throws Exception {
        int repetitions=args.length>0?Integer.parseInt(args[0]):9;
        String filter=args.length>1?args[1]:"mixed,selective,nested-repeated";
        long inputRows=args.length>2?Long.parseLong(args[2]):500_000_000L;
        int maxWorkers=args.length>3?Integer.parseInt(args[3]):8;
        int warmups=args.length>4?Integer.parseInt(args[4]):5;
        if(repetitions<3||inputRows<500_000_000L||warmups<1||maxWorkers<2||maxWorkers>32)
            throw new IllegalArgumentException("repetitions>=3, rows>=500000000, warmups>=1, workers in [2,32]");
        var database=Files.createTempDirectory("questdb-codegen-parallel-");
        var artifacts=Path.of("experimental/query-compiler/target/parallel-code");
        try {
            WorkloadMatrix.clearDirectory(artifacts);
            var config=new DefaultCairoConfiguration(database.toString());
            try(var engine=new CairoEngine(config);
                var pool=new WorkerPool(new WorkerPoolConfiguration() {
                    @Override public int getWorkerCount(){return maxWorkers-1;}
                    @Override public boolean haltOnError(){return true;}
                })) {
                WorkerPoolUtils.setupQueryJobs(pool,engine);
                pool.start();
                try(var context=new SqlExecutionContextImpl(engine,maxWorkers-1).with(
                        config.getFactoryProvider().getSecurityContextFactory().getRootContext(),null,null,-1,null)) {
                    System.out.printf("LLVM O2; %d input rows per case; %d warmups; %d measured rounds; rotated order; SQL %d background workers + caller; prototype N workers + coordinator.%n",inputRows,warmups,repetitions,maxWorkers-1);
                    System.out.println("Compilation and comparison sort excluded; allocation, merge, result copy and cleanup included. No CPU pinning.");
                    for(var spec:WorkloadMatrix.specs()) {
                        if(Arrays.stream(filter.split(",",-1)).noneMatch(spec.name()::contains))continue;
                        if(spec.pipeline().flatDistinct()) {
                            System.out.println(spec.name()+": serial-only experimental algorithm; parallel mode follows Java nested DISTINCT");
                            continue;
                        }
                        String ddl=Pattern.compile("long_sequence\\((\\d+)\\)").matcher(spec.ddl())
                                .replaceAll(m->"long_sequence("+inputRows+")");
                        System.out.printf("%nCreating %s: %,d rows%n",spec.name(),inputRows);
                        long creationStarted=System.nanoTime();
                        engine.execute(ddl,context);
                        System.out.printf(Locale.ROOT,"Dataset creation: %.3f s%n",(System.nanoTime()-creationStarted)/1e9);
                        try {measure(engine,context,spec,repetitions,maxWorkers,warmups,inputRows,artifacts);}
                        finally {engine.execute("drop table input",context);}
                    }
                }
            }
        } finally {LogFactory.haltInstance();WorkloadMatrix.clearDirectory(database);}
    }

    private static void measure(CairoEngine engine,SqlExecutionContextImpl context,WorkloadMatrix.Spec spec,
                                int repetitions,int maxWorkers,int warmups,long expectedRows,Path artifacts)throws Exception {
        var p=spec.pipeline();
        var queries=new ArrayList<ParallelGroupBy>();
        long rows;
        try(var reader=engine.getReader("input")){rows=reader.size();}
        if(rows!=expectedRows)throw new AssertionError("Expected "+expectedRows+" input rows, found "+rows);
        try(var scalar=Demo.compileSql(engine,context,spec.sql(),false);
            var normal=Demo.compileSql(engine,context,spec.sql(),true);
            var serial=CompiledGroupBy.compile(engine,context,"input",p,new LlvmBackend(),artifacts)) {
            var expected=Demo.readSql(scalar,context,p.keys().size());
            long pairs=-1;
            for(int i=0;i<p.aggregates().size();i++) {
                if(p.aggregates().get(i) instanceof Pipeline.NestedCountDistinct) {
                    final int column=i;
                    pairs=expected.stream().mapToLong(row->row.values().get(column)).sum();
                }
            }
            System.out.printf("%n%s: rows=%d, groups=%d, distinct pairs=%s, result hash=%d%n",spec.name(),rows,expected.size(),pairs<0?"n/a":Long.toString(pairs),expected.hashCode());
            var plan=new TextPlanSink();plan.of(normal,context);
            for(int i=1;i<=plan.getLineCount();i++)System.out.println("  SQL plan: "+plan.getLine(i));
            var runs=new ArrayList<Run>();
            runs.add(new Run("serial",t->serial.execute(context,CompiledGroupBy.Options.DEFAULT,t)));
            for(int workers=1;workers<=maxWorkers;workers*=2) {
                var query=ParallelGroupBy.compile(engine,context,"input",p,new LlvmBackend(),artifacts,workers);
                queries.add(query);
                runs.add(new Run("parallel-"+workers,t->query.execute(context,CompiledGroupBy.Options.DEFAULT,t)));
                System.out.printf(Locale.ROOT,"  compile parallel-%d: %.3f ms%n",workers,query.compilationNanos()/1e6);
            }
            runs.add(new Run("questdb-"+maxWorkers,t->Demo.readSql(normal,context,p.keys().size(),t)));
            double[][] elapsed=new double[runs.size()][repetitions],scan=new double[runs.size()][repetitions],merge=new double[runs.size()][repetitions];
            long[] peaks=new long[runs.size()];
            long[] majorFaults=new long[runs.size()],readBytes=new long[runs.size()];
            for(int round=-warmups;round<repetitions;round++) {
                var progress=new StringBuilder(round<0?"  warmup "+(round+warmups+1):"  sample "+(round+1));
                for(int offset=0;offset<runs.size();offset++) {
                    int i=Math.floorMod(round+offset,runs.size());
                    var t=new ExecutionTimings();
                    var resourcesBefore=Resources.read();
                    long start=System.nanoTime();
                    var actual=runs.get(i).execute.run(t);
                    double millis=(System.nanoTime()-start-t.sort)/1e6;
                    var resourcesAfter=Resources.read();
                    if(!expected.equals(actual))throw new AssertionError(spec.name()+" / "+runs.get(i).name+" mismatched SQL");
                    if(t.workerRows!=null&&Arrays.stream(t.workerRows).sum()!=rows)throw new AssertionError("Worker row accounting");
                    if(round>=0) {
                        elapsed[i][round]=millis;scan[i][round]=t.scan/1e6;merge[i][round]=t.merge/1e6;
                        majorFaults[i]+=resourcesAfter.majorFaults-resourcesBefore.majorFaults;
                        readBytes[i]+=resourcesAfter.readBytes-resourcesBefore.readBytes;
                    }
                    peaks[i]=Math.max(peaks[i],t.peakBytes);
                    progress.append(String.format(Locale.ROOT,"; %s=%.1f ms",runs.get(i).name,millis));
                }
                System.out.println(progress);
            }
            for(int i=0;i<runs.size();i++) {
                Arrays.sort(elapsed[i]);Arrays.sort(scan[i]);Arrays.sort(merge[i]);
                System.out.printf(Locale.ROOT,"  %-12s median %8.3f ms [%.3f, %.3f]; scan %.3f; merge %.3f; retained peak %s; measured major faults %d; storage reads %.2f MiB%n",
                        runs.get(i).name,median(elapsed[i]),elapsed[i][0],elapsed[i][repetitions-1],median(scan[i]),median(merge[i]),
                        peaks[i]==0?"n/a":String.format(Locale.ROOT,"%.2f MiB",peaks[i]/1048576.0),majorFaults[i],readBytes[i]/1048576.0);
            }
        } finally {for(var query:queries)query.close();}
    }

    private static double median(double[] sorted) {
        int n=sorted.length;return n%2==0?(sorted[n/2-1]+sorted[n/2])/2:sorted[n/2];
    }

    // Linux process counters, sampled outside the query timer. These distinguish
    // warm-cache query execution from the host's unrelated storage pressure.
    private record Resources(long majorFaults,long readBytes) {
        static Resources read()throws Exception {
            String stat=Files.readString(Path.of("/proc/self/stat"));
            String[] fields=stat.substring(stat.lastIndexOf(')')+2).trim().split("\\s+");
            long bytes=0;
            for(String line:Files.readAllLines(Path.of("/proc/self/io"))) {
                if(line.startsWith("read_bytes:"))bytes=Long.parseLong(line.substring("read_bytes:".length()).trim());
            }
            return new Resources(Long.parseLong(fields[9]),bytes);
        }
    }
}
