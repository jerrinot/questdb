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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ParallelAdaptiveKernelTest {
    @Test
    public void learnsAndRespondsToChangedCosts() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger calls = new AtomicInteger();
        AtomicBoolean changed = new AtomicBoolean();
        long[] initial = {1000, 300, 600};
        long[] later = {100, 1000, 700};
        Kernel[] code = new Kernel[3];
        for (int i = 0; i < code.length; i++) {
            int id = i;
            code[i] = (a, b, c) -> {
                clock.addAndGet(changed.get() ? later[id] : initial[id]);
                calls.incrementAndGet();
                return id;
            };
        }
        var choice = new ParallelAdaptiveKernel(code, 4, clock::get);
        int[] selected = new int[3];
        for (int i = 0; i < 25000; i++) choice.runBatch(4096, k -> selected[(int) k.run(0, 4096, 0)]++);
        Assert.assertEquals(25000, calls.get());
        Assert.assertTrue(selected[1] > 24000);
        changed.set(true);
        Arrays.fill(selected, 0);
        for (int i = 0; i < 25000; i++) choice.runBatch(4096, k -> selected[(int) k.run(0, 4096, 0)]++);
        Assert.assertEquals(50000, calls.get());
        Assert.assertTrue(selected[0] > 17000);
    }

    @Test
    public void failuresDoNotReplayOrAdvance() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger calls = new AtomicInteger();
        Kernel a = (x, y, z) -> { clock.addAndGet(1000); return 0; };
        Kernel b = (x, y, z) -> { clock.addAndGet(1); return 1; };
        var choice = new ParallelAdaptiveKernel(new Kernel[]{a, b}, 2, clock::get);
        for (int i = 0; i < 100; i++) {
            Assert.assertThrows(IllegalStateException.class, () -> choice.runBatch(4096, k -> {
                Assert.assertSame(a, k);
                calls.incrementAndGet();
                throw new IllegalStateException();
            }));
        }
        Assert.assertEquals(100, calls.get());
        choice.runBatch(4096, k -> Assert.assertSame(a, k));
    }

    @Test
    public void slowOldCallDoesNotBlockTransition() throws Exception {
        AtomicLong clock = new AtomicLong();
        Kernel a = (x, y, z) -> { clock.addAndGet(1000); return 0; };
        Kernel b = (x, y, z) -> { clock.addAndGet(1); return 1; };
        var choice = new ParallelAdaptiveKernel(new Kernel[]{a, b}, 2, clock::get);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread old = Thread.ofPlatform().start(() -> {
            try {
                choice.runBatch(4096, k -> {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("release timed out");
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    k.run(0, 4096, 0);
                });
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        try {
            Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
            for (int i = 0; i < 1000; i++) choice.runBatch(4096, k -> k.run(0, 4096, 0));
            choice.runBatch(4096, k -> Assert.assertSame(b, k));
        } finally {
            release.countDown();
            old.join(10000);
        }
        Assert.assertFalse(old.isAlive());
        Assert.assertNull(failure.get());
        choice.runBatch(1, k -> Assert.assertSame(b, k));
    }

    @Test
    public void concurrentBatchesExecuteOnce() throws Exception {
        AtomicLong rows = new AtomicLong();
        Kernel kernel = (a, b, c) -> { rows.addAndGet(b); return 0; };
        var choice = new ParallelAdaptiveKernel(new Kernel[]{kernel, kernel}, 8, System::nanoTime);
        try (var pool = Executors.newFixedThreadPool(8)) {
            var futures = new ArrayList<Future<?>>();
            for (int t = 0; t < 8; t++) {
                futures.add(pool.submit(() -> {
                    for (int i = 0; i < 1000; i++) choice.runBatch(4096, code -> code.run(0, 4096, 0));
                }));
            }
            for (var future : futures) future.get(10, TimeUnit.SECONDS);
        }
        Assert.assertEquals(8000L * 4096, rows.get());
    }
}
