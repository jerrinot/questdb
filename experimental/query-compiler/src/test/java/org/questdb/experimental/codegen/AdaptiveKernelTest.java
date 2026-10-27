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

import org.junit.Test;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class AdaptiveKernelTest {
    @Test public void learnsBothWinnersAndRespondsToChangedCostsWithoutReplayingRows() {
        long[] time={0},work={0},cost={9,1};int[] calls={0,0};
        Kernel[] kernels=new Kernel[2];
        for(int i=0;i<2;i++){int choice=i;kernels[i]=(c,r,s)->{calls[choice]++;time[0]+=cost[choice]*r;work[0]+=r;return 0;};}
        var adaptive=new AdaptiveKernel(kernels[0],kernels[1],()->time[0]);
        long rows=65536;
        for(int i=0;i<14;i++)assertEquals(0,adaptive.run(0,rows,0));
        int before=calls[1];for(int i=0;i<128;i++)adaptive.run(0,rows,0);
        assertEquals(before+128,calls[1]);
        cost[0]=1;cost[1]=9;
        for(int i=0;i<10;i++)adaptive.run(0,rows,0);
        before=calls[0];for(int i=0;i<128;i++)adaptive.run(0,rows,0);
        assertEquals(before+128,calls[0]);
        assertEquals((14+128+10+128)*rows,work[0]);
    }

    @Test public void shortCallsDoNotTrainAndFailureIsNeverRetried() {
        int[] calls={0,0};boolean[] fail={false};
        var adaptive=new AdaptiveKernel((c,r,s)->{calls[0]++;return fail[0]?3:0;},(c,r,s)->{calls[1]++;return 0;},()->0);
        for(int i=0;i<100;i++)adaptive.run(0,1,0);
        assertEquals(100,calls[0]);assertEquals(0,calls[1]);
        fail[0]=true;assertEquals(3,adaptive.run(0,65536,0));
        assertEquals(101,calls[0]);assertEquals(0,calls[1]);
    }

    @Test public void nearTiesKeepTheCurrentWinner() {
        long[] time={0};int[] calls={0,0};
        var adaptive=new AdaptiveKernel((c,r,s)->{time[0]+=100*r;calls[0]++;return 0;},
                (c,r,s)->{time[0]+=95*r;calls[1]++;return 0;},()->time[0]);
        for(int i=0;i<14;i++)adaptive.run(0,65536,0);
        int before=calls[0];for(int i=0;i<128;i++)adaptive.run(0,65536,0);
        assertEquals(before+128,calls[0]);
    }

    @Test public void workersLearnIndependently() throws Exception {
        ThreadLocal<long[]> state=ThreadLocal.withInitial(()->new long[5]);
        Kernel a=(c,r,s)->{var x=state.get();x[0]+=(x[1]==0?1:9)*r;x[2]++;return 0;};
        Kernel b=(c,r,s)->{var x=state.get();x[0]+=(x[1]==1?1:9)*r;x[3]++;return 0;};
        var adaptive=new AdaptiveKernel(a,b,()->state.get()[0]);
        var ready=new CountDownLatch(2);
        try(var executor=Executors.newFixedThreadPool(2)){
            Future<?>[] futures=new Future<?>[2];
            for(int i=0;i<2;i++){int winner=i;futures[i]=executor.submit(()->{
                var x=state.get();x[1]=winner;ready.countDown();
                try{assertTrue(ready.await(10,TimeUnit.SECONDS));}catch(InterruptedException e){throw new AssertionError(e);}
                for(int n=0;n<14;n++)adaptive.run(0,65536,0);
                long before=x[winner+2];for(int n=0;n<128;n++)adaptive.run(0,65536,0);
                assertEquals(before+128,x[winner+2]);
            });}
            for(var future:futures)future.get(10,TimeUnit.SECONDS);
        }
    }

    @Test public void aParallelBatchSharesOneChoiceAndLearnsJoinedWork() {
        long[] time={0};int[] calls={0,0};
        Kernel fused=(c,r,s)->{time[0]+=9*r;calls[0]++;return 0;};
        Kernel masked=(c,r,s)->{time[0]+=r;calls[1]++;return 0;};
        var adaptive=new AdaptiveKernel(fused,masked,()->time[0]);
        for(int i=0;i<14;i++)adaptive.runBatch(8*65536,selected->{
            int a=calls[0],b=calls[1];
            for(int worker=0;worker<8;worker++)assertEquals(0,selected.run(0,65536,0));
            assertTrue((calls[0]-a==8&&calls[1]==b)||(calls[1]-b==8&&calls[0]==a));
        });
        int before=calls[1];
        for(int i=0;i<128;i++)adaptive.runBatch(8*65536,selected->{
            assertSame(masked,selected);
            for(int worker=0;worker<8;worker++)selected.run(0,65536,0);
        });
        assertEquals(before+8*128,calls[1]);
    }

    @Test public void failedParallelBatchDoesNotAdvanceFeedbackOrReplay() {
        int[] calls={0};
        Kernel first=(c,r,s)->{calls[0]++;return 3;};
        var adaptive=new AdaptiveKernel(first,(c,r,s)->{throw new AssertionError("second alternative");},()->0);
        for(int i=0;i<2;i++)assertThrows(IllegalStateException.class,()->adaptive.runBatch(65536,selected->{
            assertSame(first,selected);
            if(selected.run(0,65536,0)!=0)throw new IllegalStateException("failed chunk");
        }));
        assertEquals(2,calls[0]);
    }

    @Test public void identicalProgramsCompileOnceAndLeasesOutliveTheCache() throws Exception {
        AtomicInteger opened=new AtomicInteger(),closed=new AtomicInteger();
        Backend backend=counting(opened,closed,false);
        Program p=program(0),q=program(1);
        try(var kernel=backend.compileAlternatives(p,p,Path.of("."))){assertEquals(1,opened.get());}
        assertEquals(1,closed.get());
        var cache=new KernelCache(backend,1);
        var kernel=cache.compileAlternatives(p,q,Path.of("."));
        assertEquals(3,opened.get());assertEquals(1,closed.get());
        cache.close();assertEquals(1,closed.get());
        assertEquals(0,kernel.run(0,1,0));
        kernel.close();kernel.close();assertEquals(3,closed.get());
        assertThrows(IllegalStateException.class,()->kernel.run(0,1,0));
    }

    @Test public void failedSecondCompilationReleasesFirst() {
        AtomicInteger opened=new AtomicInteger(),closed=new AtomicInteger();
        Backend backend=counting(opened,closed,true);
        assertThrows(IOException.class,()->backend.compileAlternatives(program(0),program(1),Path.of(".")));
        assertEquals(1,closed.get());
    }

    @Test public void threeAlternativesRetainAndRevisitEveryCandidate() {
        long[] time={0},cost={9,5,1};int[] calls=new int[3];Kernel[] kernels=new Kernel[3];
        for(int i=0;i<3;i++){int choice=i;kernels[i]=(c,r,s)->{time[0]+=cost[choice]*r;calls[choice]++;return 0;};}
        var adaptive=new AdaptiveKernel(kernels,()->time[0]);
        for(int i=0;i<21;i++)adaptive.run(0,65536,0);
        int before=calls[2];for(int i=0;i<128;i++)adaptive.run(0,65536,0);
        assertEquals(before+128,calls[2]);
        cost[1]=1;cost[2]=9;
        // Round-robin challenges reach the newly faster middle alternative.
        for(int i=0;i<10+128+10;i++)adaptive.run(0,65536,0);
        before=calls[1];for(int i=0;i<128;i++)adaptive.run(0,65536,0);
        assertEquals(before+128,calls[1]);
        assertTrue(calls[0]>7&&calls[1]>7&&calls[2]>7);
    }

    private static Program program(long result){Builder b=new Builder();b.exit(result);return new Program(b.block());}
    private static Backend counting(AtomicInteger opened,AtomicInteger closed,boolean failSecond){return new Backend(){
        public String name(){return "test";}
        public CompiledKernel compile(Program p,Path path)throws IOException{
            if(opened.incrementAndGet()==2&&failSecond)throw new IOException("injected");
            return new CompiledKernel((c,r,s)->0,closed::incrementAndGet,path,1);
        }
    };}
}
