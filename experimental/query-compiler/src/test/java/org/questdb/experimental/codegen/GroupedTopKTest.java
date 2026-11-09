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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class GroupedTopKTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private final WorkerPoolMode mode;
    private CompilerOptions options = CompilerOptions.DEFAULT.withLocalCapacity(64);
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long queryMemoryLimit;

    public GroupedTopKTest(String name, Backend backend, WorkerPoolMode mode) { this.backend = backend; this.mode = mode; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm-legacy", new LlvmBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"llvm-fiber", new LlvmBackend(), WorkerPoolMode.FIBER_HOST},
                new Object[]{"asmjit", new AsmJitBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"c", new NativeBackend(), WorkerPoolMode.LEGACY});
    }

    @Before public void setUp() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public long getQueryMemoryLimitBytes() { return queryMemoryLimit; }
            @Override public int getSqlPageFrameMaxRows() { return 2048; }
            @Override public int getSqlPageFrameMinRows() { return 64; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
            @Override public WorkerPoolMode getWorkerPoolMode() { return mode; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath(), options);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
    }

    @Test public void numericOrdersUseGeneratedAggregationAndMerge() throws Exception {
        createTable();
        for (String value : List.of("i", "v", "f", "d")) {
            for (String order : List.of("asc", "desc")) {
                String keys = value.equals("d") ? "k,s" : value.equals("i") ? "s" : "k";
                int limit = value.equals("f") ? 2 : value.equals("v") ? 1 : 7;
                assertMatches("select " + keys + "," + value + ",rn from (select " + keys + "," + value
                        + ",row_number() over(partition by " + keys + " order by " + value + " " + order
                        + ") rn from t) where rn<=" + limit, true);
            }
        }
    }

    @Test public void filterColumnTopsParquetAndReopen() throws Exception {
        createTable();
        engine.execute("alter table t add column late float", context);
        engine.execute("insert into t select cast(x%257 as int),cast(x%11 as symbol),cast(x as int),x,"
                + "cast(x*0.25 as float),x*0.25,timestamp_sequence('2024-01-02',1000000L),cast(x*0.5 as float) "
                + "from long_sequence(1003)", context);
        String sql = "select k,late,rn from (select k,late,row_number() over(partition by k order by late desc) rn "
                + "from t where late is not null) where 3>rn";
        assertMatches(sql, true);
        engine.execute("alter table t convert partition to parquet list '2024-01-01','2024-01-02'", context);
        assertMatches(sql, true);
        context.setQueryCodeGenerator(compiler);
        try (var factory = engine.select(sql, context)) {
            var expected = Demo.readSql(factory, context, 3);
            try (var cursor = factory.getCursor(context)) {
                assertTrue(cursor.hasNext());
                cursor.toTop();
                long rows = 0;
                while (cursor.hasNext()) rows++;
                assertEquals(expected.size(), rows);
            }
            try (var cursor = factory.getCursor(context)) { assertTrue(cursor.hasNext()); }
            assertEquals(expected, Demo.readSql(factory, context, 3));
        }
        assertMatches("select k,f,rn from (select k,f,row_number() over(partition by k order by f) rn from t where k<0) where rn<=2", true);
    }

    @Test public void nonfiniteValuesSignedZeroAndTiesKeepWindowOrdering() throws Exception {
        engine.execute("create table edges(k int,f float,d double)", context);
        try (var writer = engine.getWriter(engine.verifyTableName("edges"), "test")) {
            for (int repeat = 0; repeat < 3; repeat++) {
                for (double number : new double[]{Double.NaN,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,
                        -0.0,0.0,Double.MIN_VALUE,-Double.MAX_VALUE,Double.MAX_VALUE,17,17}) {
                    var row = writer.newRow();
                    row.putInt(0, repeat == 0 ? Integer.MIN_VALUE : repeat);
                    row.putFloat(1, (float) number);
                    row.putDouble(2, number);
                    row.append();
                }
            }
            writer.commit();
        }
        for (String value : List.of("f", "d")) {
            for (String order : List.of("asc", "desc")) {
                assertMatches("select k," + value + ",rn from (select k," + value
                        + ",row_number() over(partition by k order by " + value + " " + order
                        + ") rn from edges) where rn<=64", true);
            }
        }
    }

    @Test public void payloadOtherWindowsAndUnsupportedBoundsRetainTheWindow() throws Exception {
        createTable();
        for (String sql : List.of(
                "select k,v,i,rn from (select k,v,i,row_number() over(partition by k order by v) rn from t) where rn<=2",
                "select k,v,rn from (select k,v,row_number() over(partition by k order by v,i) rn from t) where rn<=2",
                "select k,v,rn from (select k,v,row_number() over(partition by k%3 order by v) rn from t) where rn<=2",
                "select k,v,rn from (select k,v,row_number() over(partition by k order by v) rn from t) where rn<=65",
                "select k,v,rn,n from (select k,v,row_number() over(partition by k order by v) rn,count() over(partition by k) n from t) where rn<=2")) {
            assertMatches(sql, false);
        }
    }

    @Test public void variableKeysAndMemoryBreachReuseExistingOwners() throws Exception {
        engine.execute("create table keyed as (select cast(case when x%19=0 then null else "
                + "'key-'||x%257 end as varchar) k,cast(x%11 as double) d,x v from long_sequence(10003))", context);
        for (String key : List.of("k", "d", "k,d")) {
            assertMatches("select " + key + ",v,rn from (select " + key + ",v,row_number() over(partition by "
                    + key + " order by v desc) rn from keyed) where rn<=3", true);
        }
        long nativeMemory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        String sql = "select k,v,rn from (select k,v,row_number() over(partition by k order by v) rn from keyed) where rn<=64";
        try (var factory = engine.select(sql, context)) {
            var expected = Demo.readSql(factory, context, 3);
            queryMemoryLimit = 16384;
            var failure = assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 3));
            assertTrue(failure.isOutOfMemory());
            queryMemoryLimit = 0;
            assertEquals(expected, Demo.readSql(factory, context, 3));
        } finally {
            queryMemoryLimit = 0;
        }
        assertEquals(0, engine.getBusyReaderCount());
        assertEquals(nativeMemory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    private void createTable() throws Exception {
        engine.execute("create table t(k int,s symbol capacity 8192,i int,v long,f float,d double,ts timestamp) "
                + "timestamp(ts) partition by day bypass wal", context);
        engine.execute("insert into t select cast(case when x%19=0 then null else x%257 end as int),"
                + "cast(case when x%23=0 then null else cast(x%11 as string) end as symbol),"
                + "cast(case when x%17=0 then null else x%97-48 end as int),"
                + "case when x%17=0 then null else 9007199254740993L+x%101 end,"
                + "cast(case when x%17=0 then null else (x%97-48)*0.25 end as float),"
                + "case when x%17=0 then null else (x%97-48)*0.25 end,"
                + "timestamp_sequence('2024-01-01',1000000L) from long_sequence(10003)", context);
    }

    private void assertMatches(String sql, boolean generated) throws Exception {
        context.setQueryCodeGenerator(null);
        List<CompiledGroupBy.ResultRow> expected;
        try (var ordinary = engine.select(sql, context)) {
            expected = Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount());
        }
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            var sink = new TextPlanSink();
            sink.of(candidate, context);
            var plan = new StringBuilder();
            for (int i = 1; i <= sink.getLineCount(); i++) plan.append(sink.getLine(i)).append('\n');
            assertEquals(plan.toString(), generated, plan.indexOf("Grouped Top K") >= 0);
            assertEquals(sql, expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
            if (generated && !expected.isEmpty()) {
                RecordCursorFactory physical = candidate;
                while (!(physical instanceof AsyncGroupByRecordCursorFactory)) {
                    physical = physical.getBaseFactory();
                    assertNotNull(physical);
                }
                var atom = ((AsyncGroupByRecordCursorFactory) physical).getAtom();
                long rows = 0;
                for (int slot = -1; slot < 2; slot++) {
                    assertNotNull(atom.getCompiledUpdater(slot));
                    rows += atom.getCompiledUpdater(slot).getNativeMapRows();
                }
                assertTrue("Generated aggregation must execute", rows > 0);
            }
        }
    }
}
