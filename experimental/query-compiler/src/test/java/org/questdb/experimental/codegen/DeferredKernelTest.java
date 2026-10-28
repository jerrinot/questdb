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

import io.questdb.cairo.CairoException;
import org.junit.Test;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

public class DeferredKernelTest {
    @Test public void firstUsePublishesOnceAndCloseReleasesOnce() throws Exception {
        var compiled = new AtomicInteger(); var closed = new AtomicInteger();
        var start = new CountDownLatch(1);
        Backend backend = new Backend() {
            public String name() { return "test"; }
            public CompiledKernel compile(Ir.Program program, Path path) {
                compiled.incrementAndGet();
                return new CompiledKernel((a,b,c) -> a + b + c, closed::incrementAndGet, null, 71);
            }
        };
        var code = CompiledKernel.defer(backend, () -> {var b = new Ir.Builder();b.exit(0);return new Ir.Program(b.block());}, null);
        assertEquals(0, compiled.get());
        assertEquals(0, code.compilationNanos());
        try (var workers = Executors.newFixedThreadPool(4)) {
            var futures = new java.util.ArrayList<Future<Long>>();
            for (int i = 0; i < 4; i++) futures.add(workers.submit(() -> {start.await();return code.run(1, 2, 3);}));
            start.countDown();
            for (var future : futures) assertEquals(Long.valueOf(6), future.get(10, TimeUnit.SECONDS));
        } finally { code.close(); }
        assertEquals(1, compiled.get());
        assertEquals(71, code.compilationNanos());
        code.close();
        assertEquals(1, closed.get());
        assertThrows(IllegalStateException.class, () -> code.run(1, 2, 3));
    }

    @Test public void unusedCodeDoesNotGenerateAProgram() {
        var code = CompiledKernel.defer(null, () -> {throw new AssertionError("unused program generated");}, null);
        code.close();
        code.close();
        assertEquals(0, code.compilationNanos());
    }

    @Test public void compilationFailureCanRetryWithoutReplayingExecution() {
        var attempts = new AtomicInteger(); var calls = new AtomicInteger(); var closes = new AtomicInteger();
        Backend backend = new Backend() {
            public String name() {return "test";}
            public CompiledKernel compile(Ir.Program program, Path path) throws IOException {
                if (attempts.incrementAndGet() == 1) throw new IOException("injected compilation failure");
                return new CompiledKernel((a,b,c) -> calls.incrementAndGet(), closes::incrementAndGet, null, 19);
            }
        };
        try (var code = CompiledKernel.defer(backend, () -> {var b=new Ir.Builder();b.exit(0);return new Ir.Program(b.block());}, null)) {
            assertThrows(CairoException.class, () -> code.run(0,0,0));
            assertEquals(0, calls.get());
            assertEquals(0, code.compilationNanos());
            assertEquals(1, code.run(0,0,0));
            assertEquals(2, code.run(0,0,0));
            assertEquals(2, attempts.get());
            assertEquals(19, code.compilationNanos());
        }
        assertEquals(1, closes.get());
    }
}
