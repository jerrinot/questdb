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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FilterFusionTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private long jitMemory;

    @Before
    public void setUp() throws Exception {
        jitMemory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var cfg = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public int getGroupByShardingThreshold() { return 2; }
            @Override public int getGroupByMapCapacity() { return 16; }
        };
        engine = new CairoEngine(cfg);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        engine.execute("create table t as (select cast(x%1027 as int) k,"
                + "cast(case when x%11=0 then null else x%100 end as int) p,"
                + "case when x%13=0 then null else x%8191 end v,cast(x%31 as short) a,"
                + "cast(case when x%17=0 then null else 'long-ž-key-'||cast(x%509 as string) end as varchar) s "
                + "from long_sequence(10003))", context);
    }

    @After
    public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(jitMemory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test
    public void masksTailsGrowthAndFlushPreserveResultsOnReopen() throws Exception {
        String old = System.getProperty("query.codegen.localCapacity");
        System.setProperty("query.codegen.localCapacity", "64");
        try {
            for (String predicate : new String[]{"p<0", "p<1", "p<50", "p<100"}) {
                assertMatches("select k,count(),sum(v) from t where " + predicate + " group by k", true);
            }
            assertMatches("select k+1,count(),count(v),sum(v),avg(a),count_distinct(v) "
                    + "from t where p<50 or p=null group by k+1", true);
            assertMatches("select s,k,count(),sum(v) from t where p<50 group by s,k", true);
            assertMatches("select s,count(),sum(v) from t where p<50 group by s", true);
        } finally {
            if (old == null) System.clearProperty("query.codegen.localCapacity");
            else System.setProperty("query.codegen.localCapacity", old);
        }
    }

    @Test
    public void unsupportedPredicatesAndColumnTopsUseFallback() throws Exception {
        assertMatches("select k,count(),sum(v) from t where s!=null and p<50 group by k", false);
        engine.execute("alter table t add column top int", context);
        assertMatches("select k,count(),sum(v) from t where top=null group by k", false);
    }

    @Test
    public void failureAfterNativeProgressPropagatesAndCursorReopens() throws Exception {
        String sql = "select k,count(),sum(v) from t where p<50 group by k";
        var calls = new AtomicInteger();
        Backend failing = new Backend() {
            final Backend llvm = new LlvmBackend();
            @Override public String name() { return llvm.name(); }
            @Override public CompiledKernel compile(Ir.Program program, Path artifacts) throws IOException {
                var code = llvm.compile(program, artifacts);
                return new CompiledKernel((columns, rows, state) -> {
                    long status = code.run(columns, rows, state);
                    if (calls.decrementAndGet() == 0) throw CairoException.nonCritical().put("injected native progress failure");
                    return status;
                }, code, code.artifacts(), code.compilationNanos());
            }
        };
        try (var baseline = Demo.compileSql(engine, context, sql, true)) {
            var expected = Demo.readSql(baseline, context, 1);
            context.setQueryCodeGenerator(ResolvedQueryCompiler.filterFusionExperiment(failing, temporary.newFolder().toPath()));
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                calls.set(5);
                var failure = assertThrows(CairoException.class, () -> Demo.readSql(candidate, context, 1));
                assertTrue(failure.getFlyweightMessage().toString().contains("injected native progress failure"));
                calls.set(0);
                long before = fusedRows(candidate);
                assertEquals(expected, Demo.readSql(candidate, context, 1));
                assertEquals(10003, fusedRows(candidate)-before);
            }
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    private void assertMatches(String sql, boolean fused) throws Exception {
        context.setQueryCodeGenerator(null);
        try (var baseline = Demo.compileSql(engine, context, sql, true)) {
            int columns = baseline.getMetadata().getColumnCount();
            var expected = Demo.readSql(baseline, context, columns);
            context.setQueryCodeGenerator(ResolvedQueryCompiler.filterFusionExperiment(
                    new LlvmBackend(), temporary.newFolder().toPath()));
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                long before = fusedRows(candidate);
                for (int i = 0; i < 2; i++) assertEquals(sql, expected, Demo.readSql(candidate, context, columns));
                assertEquals(sql, fused ? 20006 : 0, fusedRows(candidate)-before);
            }
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    private long fusedRows(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsyncGroupByRecordCursorFactory)) factory = factory.getBaseFactory();
        assertNotNull("Expected asynchronous aggregation", factory);
        var atom = ((AsyncGroupByRecordCursorFactory) factory).getAtom();
        long rows = 0;
        for (int i=-1; i<2; i++) {
            if (atom.getCompiledUpdater(i) instanceof AggregateSlot slot) rows += slot.fusedInputRows();
        }
        return rows;
    }
}
