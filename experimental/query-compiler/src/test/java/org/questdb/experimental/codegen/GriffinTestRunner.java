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

import io.questdb.test.QueryCodegenTestSupport;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/** Runs existing core tests with stock execution or the optional LLVM compiler. */
public final class GriffinTestRunner {
    public static void main(String[] args) throws Exception {
        if (args.length < 2 || !(args[0].equals("llvm") || args[0].equals("stock"))) {
            throw new IllegalArgumentException("Usage: GriffinTestRunner stock|llvm class[#method] ...");
        }
        boolean llvm = args[0].equals("llvm");
        var backend = new CountingBackend();
        Path artifacts = Files.createTempDirectory("griffin-codegen-");
        long liveBefore = llvm ? LlvmBackend.liveFunctions() : 0;
        int failures = 0, tests = 0, ignored = 0;
        try {
            QueryCodegenTestSupport.install(llvm ? new ResolvedQueryCompiler(backend, artifacts) : null, backend.calls::get);
            var junit = new JUnitCore();
            junit.addListener(new RunListener() {
                private long calls, compilations;
                @Override public void testStarted(Description test) {
                    calls = backend.calls.get();
                    compilations = backend.compilations.get();
                    System.out.println("GRIFFIN start " + test);
                }
                @Override public void testAssumptionFailure(Failure failure) {
                    System.out.println("GRIFFIN skipped " + failure);
                }
                @Override public void testFinished(Description test) {
                    System.out.println("GRIFFIN finish " + test + " compiled=" + (backend.compilations.get() - compilations)
                            + " nativeCalls=" + (backend.calls.get() - calls));
                }
            });
            for (int i = 1; i < args.length; i++) {
                String[] target = args[i].split("#", 2);
                Class<?> test = Class.forName(target[0]);
                var result = junit.run(target.length == 1 ? Request.aClass(test) : Request.method(test, target[1]));
                tests += result.getRunCount();
                failures += result.getFailureCount();
                ignored += result.getIgnoreCount() + result.getAssumptionFailureCount();
                for (var failure : result.getFailures()) System.err.println(failure.getTrace());
            }
            System.out.println("GRIFFIN result mode=" + args[0] + " tests=" + tests + " failures=" + failures
                    + " skipped=" + ignored + " compiled=" + backend.compilations.get() + " nativeCalls=" + backend.calls.get());
            if (failures != 0) throw new AssertionError("Griffin failures: " + failures);
            if (llvm && backend.calls.get() == 0) throw new AssertionError("No experimental native kernel executed");
            if (llvm && LlvmBackend.liveFunctions() != liveBefore) throw new AssertionError("LLVM code handles leaked");
        } finally {
            QueryCodegenTestSupport.install(null, () -> 0);
            try (var paths = Files.walk(artifacts)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
    }

    private static final class CountingBackend implements Backend {
        private final AtomicLong compilations = new AtomicLong();
        private final AtomicLong calls = new AtomicLong();
        private final Backend delegate = new LlvmBackend();

        @Override public CompiledKernel compile(Ir.Program program, Path artifacts) throws IOException {
            var code = delegate.compile(program, artifacts);
            compilations.incrementAndGet();
            return new CompiledKernel((columns, rows, state) -> {
                calls.incrementAndGet();
                return code.run(columns, rows, state);
            }, code, code.artifacts(), code.compilationNanos());
        }

        @Override public String name() { return delegate.name(); }
    }
}
