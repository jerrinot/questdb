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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Unsafe;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.*;

/** Executes a dependency-ordered set of compiled pipelines with one query-scoped state lifetime. */
public final class CompiledPlan implements AutoCloseable {
    private static final int CHUNK_ROWS=65536;
    private final PlanCompiler.Plan plan;
    private final List<CompiledKernel> kernels;
    private final IdentityHashMap<PhysicalPlan.Scan,RecordCursorFactory> scans;
    private final long loweringNanos;
    private boolean closed;
    private CompiledPlan(PlanCompiler.Plan plan,List<CompiledKernel> kernels,
                         IdentityHashMap<PhysicalPlan.Scan,RecordCursorFactory> scans,long loweringNanos) {
        this.plan=plan;this.kernels=kernels;this.scans=scans;this.loweringNanos=loweringNanos;
    }
    public static CompiledPlan compile(CairoEngine engine,SqlExecutionContext context,PhysicalPlan root,Backend backend,Path artifacts)
            throws IOException,SqlException {
        long start=System.nanoTime();
        var alternatives=Arrays.stream(ScanLowering.Mode.values()).map(mode->PlanCompiler.lower(root,mode)).toList();
        var plan=alternatives.getFirst();long lowering=System.nanoTime()-start;
        for (var alternative : alternatives)
            if (!plan.states().equals(alternative.states()) || plan.phases().size() != alternative.phases().size())
                throw new IllegalStateException("Scan alternatives changed the state plan");
        var kernels=new ArrayList<CompiledKernel>();var scans=new IdentityHashMap<PhysicalPlan.Scan,RecordCursorFactory>();
        try {
            for(int i=0;i<plan.phases().size();i++) {
                var phase=plan.phases().get(i);
                if(phase.source() instanceof PhysicalPlan.Scan scan&&!scans.containsKey(scan))
                    scans.put(scan,CompiledGroupBy.openScan(engine,context,scan.table(),scan.columns()));
                int phaseIndex=i;
                kernels.add(backend.compileAlternatives(alternatives.stream()
                        .map(alternative->alternative.phases().get(phaseIndex).program()).toList(),artifacts));
            }
            return new CompiledPlan(plan,kernels,scans,lowering);
        }catch(IOException|SqlException|RuntimeException|Error e) {
            for(var kernel:kernels)try{kernel.close();}catch(Throwable suppressed){e.addSuppressed(suppressed);}
            for(var scan:scans.values())try{scan.close();}catch(Throwable suppressed){e.addSuppressed(suppressed);}
            throw e;
        }
    }
    public int pipelineCount(){return kernels.size();}
    public long loweringNanos(){return loweringNanos;}
    public long compilationNanos(){return kernels.stream().mapToLong(CompiledKernel::compilationNanos).sum();}
    public List<Pipeline.Column> columns(){return plan.output();}
    public List<List<Long>> execute(SqlExecutionContext context)throws SqlException{return execute(context,CompiledGroupBy.Options.DEFAULT);}
    public List<List<Long>> execute(SqlExecutionContext context,CompiledGroupBy.Options options)throws SqlException {
        if(closed)throw new IllegalStateException("Compiled plan is closed");
        context.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
        try(var arena=Arena.ofConfined();var memory=KernelMemory.open(context,options.memoryLimit())) {
            long environment=arena.allocate(plan.states().size()*8L,8).address();
            long[] state=new long[plan.states().size()];
            for(int i=0;i<state.length;i++) {
                var definition=plan.states().get(i);
                state[i]=switch(definition.kind()) {
                    case OUTPUT->arena.allocate(RowStore.BYTES,8).address();
                    case GROUP,JOIN->TableState.create(arena,options.initialCapacity(),options.maxCapacity(),definition.entryBytes(),
                            TableState.BYTES+(definition.kind()==PlanCompiler.StateKind.JOIN?RowStore.BYTES:0));
                };
                Unsafe.getUnsafe().putLong(environment+i*8L,state[i]);
                if(definition.global()) {
                    long entry=TableState.entries(state[i]);
                    Unsafe.getUnsafe().putLong(entry,1);Unsafe.getUnsafe().putLong(state[i]+8,1);
                    for(int j=0;j<definition.initial().size();j++)Unsafe.getUnsafe().putLong(entry+(j+1)*8L,definition.initial().get(j));
                }
            }
            for(int phaseIndex=0;phaseIndex<plan.phases().size();phaseIndex++) {
                var phase=plan.phases().get(phaseIndex);var kernel=kernels.get(phaseIndex);
                if(phase.source() instanceof PhysicalPlan.Scan scan)runScan(context,arena,memory,environment,scan,kernel);
                else {
                    var aggregate=(PhysicalPlan.Aggregate)phase.source();
                    long descriptor=state[phase.inputState()];
                    int capacity=TableState.capacity(descriptor);
                    for(long offset=0;offset<capacity;offset+=CHUNK_ROWS) {
                        context.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                        run(kernel,TableState.entries(descriptor)+offset*aggregate.pipeline().entryBytes(),Math.min(CHUNK_ROWS,capacity-offset),environment,memory);
                    }
                }
            }
            long record=Unsafe.getUnsafe().getLong(state[state.length-1]+RowStore.HEAD);
            List<List<Long>> rows=new ArrayList<>();
            while(record!=0) {
                if((rows.size()&1023)==0)context.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                var values=new ArrayList<Long>();
                for(int i=0;i<plan.output().size();i++) {
                    long value=Unsafe.getUnsafe().getLong(record+(i+1)*8L);
                    values.add(value==Long.MIN_VALUE?null:value);
                }
                rows.add(Collections.unmodifiableList(values));record=Unsafe.getUnsafe().getLong(record);
            }
            return List.copyOf(rows);
        }
    }
    private void runScan(SqlExecutionContext context,Arena arena,KernelMemory memory,long environment,
                         PhysicalPlan.Scan scan,CompiledKernel kernel)throws SqlException {
        long columns=arena.allocate(scan.columns().size()*8L,8).address();
        try(var frames=scans.get(scan).getPageFrameCursor(context,PartitionFrameCursorFactory.ORDER_ASC)) {
            PageFrame frame;
            while((frame=frames.next())!=null) {
                if(frame.getFormat()!=PartitionFormat.NATIVE)throw new UnsupportedPlan("Compiled pipelines require native page frames");
                long rows=frame.getPartitionHi()-frame.getPartitionLo();
                if(rows<0)throw new IllegalStateException("Negative frame size");
                for(var column:scan.columns()) {
                    if(frame.getColumnSource(column.index())!=DataSource.DIRECT)throw new UnsupportedPlan("Covered column");
                    if(frame.getPageAddress(column.index())!=0&&frame.getPageSize(column.index())<Math.multiplyExact(rows,column.width().bytes))
                        throw new IllegalStateException("Column page shorter than frame");
                }
                for(long offset=0;offset<rows;offset+=CHUNK_ROWS) {
                    context.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    for(var column:scan.columns()) {
                        long base=frame.getPageAddress(column.index());
                        Unsafe.getUnsafe().putLong(columns+column.index()*8L,base==0?0:base+offset*column.width().bytes);
                    }
                    run(kernel,columns,Math.min(CHUNK_ROWS,rows-offset),environment,memory);
                }
            }
        }
    }
    private static void run(CompiledKernel kernel,long input,long rows,long environment,KernelMemory memory) {
        long status=kernel.run(input,rows,environment);memory.throwIfFailed();
        if(status!=0)throw new IllegalStateException("Compiled pipeline failed, status="+status);
    }
    @Override public void close() {
        if(!closed) {
            closed=true;RuntimeException failure=null;
            for(var k:kernels)try{k.close();}catch(RuntimeException e){if(failure==null)failure=e;else failure.addSuppressed(e);}
            for(var s:scans.values())try{s.close();}catch(RuntimeException e){if(failure==null)failure=e;else failure.addSuppressed(e);}
            if(failure!=null)throw failure;
        }
    }
}
