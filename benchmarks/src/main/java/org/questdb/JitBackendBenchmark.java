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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.JitBackend;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Compares all JIT backend implementations at SQL level:
 * <ul>
 *   <li>DISABLED — no JIT, standard node evaluation</li>
 *   <li>NATIVE_SIMD — C++ asmjit with SIMD (AVX2/NEON)</li>
 *   <li>NATIVE_SCALAR — C++ asmjit, scalar only</li>
 *   <li>JAVA_VECTOR_API — Java Vector API interpreter</li>
 *   <li>JAVA_INTERPRETER — Java scalar interpreter</li>
 *   <li>JAVA_BYTECODE — Java bytecode compiler</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class JitBackendBenchmark {
    private static final int NUM_ROWS = 128 * 1024 * 1024;
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));

    @Param({"DISABLED", "NATIVE_SIMD", "NATIVE_SCALAR", "JAVA_VECTOR_API", "JAVA_INTERPRETER", "JAVA_BYTECODE"})
    public Backend backend;

    @Param({"l > 42", "l > 42 AND d < 100.0", "l IN (1, 2, 3, 4, 5)", "l > 0 AND i != 0 AND d < 0.5 AND l < 1000000"})
    public String filter;

    private SqlCompilerImpl compiler;
    private RecordCursorFactory countFactory;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private boolean isSkipped;

    public static void main(String[] args) throws RunnerException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try {
                engine.execute(
                        "CREATE TABLE IF NOT EXISTS jit_bench AS (SELECT" +
                                " rnd_long() l," +
                                " rnd_double(0) d," +
                                " rnd_int() i," +
                                " timestamp_sequence(400_000_000_000, 500_000_000) ts" +
                                " FROM long_sequence(" + NUM_ROWS + ")) TIMESTAMP(ts)",
                        sqlExecutionContext
                );
            } catch (SqlException e) {
                e.printStackTrace(System.out);
            }
        }

        Options opt = new OptionsBuilder()
                .include(JitBackendBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(5)
                .forks(1)
                .build();
        new Runner(opt).run();

        LogFactory.haltInstance();
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = new SqlCompilerImpl(engine);

        ctx.setJitMode(backend.jitMode);
        ctx.setJitBackend(backend.jitBackend);

        isSkipped = false;

        final String query = "jit_bench WHERE " + filter;
        try {
            factory = compiler.compile(query, ctx).getRecordCursorFactory();
        } catch (Exception e) {
            // Some backend/filter combinations may not be supported
            // (e.g., Vector API with IN() short-circuit)
            isSkipped = true;
            return;
        }

        if (backend != Backend.DISABLED && !factory.usesCompiledFilter()) {
            // JIT was expected but not applied — skip
            factory.close();
            isSkipped = true;
            return;
        }

        final String countQuery = "SELECT count(*) FROM " + query;
        try {
            countFactory = compiler.compile(countQuery, ctx).getRecordCursorFactory();
        } catch (Exception e) {
            factory.close();
            isSkipped = true;
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        if (compiler != null) {
            compiler.close();
        }
        if (factory != null) {
            factory.close();
        }
        if (countFactory != null) {
            countFactory.close();
        }
        if (engine != null) {
            engine.close();
        }
    }

    @Benchmark
    public long testCountOnlyFilter() throws SqlException {
        if (isSkipped) {
            return -1;
        }
        long count = 0;
        try (RecordCursor cursor = countFactory.getCursor(ctx)) {
            if (cursor.hasNext()) {
                count = cursor.getRecord().getLong(0);
            }
        }
        return count;
    }

    @Benchmark
    public long testFilter() throws SqlException {
        if (isSkipped) {
            return -1;
        }
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record ignored = cursor.getRecord();
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    public enum Backend {
        DISABLED(SqlJitMode.JIT_MODE_DISABLED, JitBackend.AUTO),
        NATIVE_SIMD(SqlJitMode.JIT_MODE_ENABLED, JitBackend.CPP),
        NATIVE_SCALAR(SqlJitMode.JIT_MODE_FORCE_SCALAR, JitBackend.CPP),
        JAVA_VECTOR_API(SqlJitMode.JIT_MODE_ENABLED, JitBackend.JAVA_INTERPRETED),
        JAVA_INTERPRETER(SqlJitMode.JIT_MODE_FORCE_SCALAR, JitBackend.JAVA_INTERPRETED),
        JAVA_BYTECODE(SqlJitMode.JIT_MODE_FORCE_SCALAR, JitBackend.JAVA_COMPILED);

        final int jitBackend;
        final int jitMode;

        Backend(int jitMode, int jitBackend) {
            this.jitMode = jitMode;
            this.jitBackend = jitBackend;
        }
    }
}
