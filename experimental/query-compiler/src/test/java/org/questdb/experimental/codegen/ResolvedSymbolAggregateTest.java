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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
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
public class ResolvedSymbolAggregateTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private final WorkerPoolMode mode;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memoryLimit;

    public ResolvedSymbolAggregateTest(String name, Backend backend, WorkerPoolMode mode) {
        this.backend = backend;
        this.mode = mode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm-fiber", new LlvmBackend(), WorkerPoolMode.FIBER_HOST},
                new Object[]{"llvm-legacy", new LlvmBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"asmjit", new AsmJitBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"c", new NativeBackend(), WorkerPoolMode.LEGACY});
    }

    @Before public void setUp() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public long getQueryMemoryLimitBytes() { return memoryLimit; }
            @Override public int getSqlPageFrameMaxRows() { return 512; }
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
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
    }

    @Test public void fusedUpdatesPreserveNullsMultipleColumnsAndReuse() throws Exception {
        engine.execute("create table t as (select cast(case when x%19=0 then null else cast(x%31 as string) end as symbol) k,"
                + "cast(x%131-65 as short) s,cast(case when x%31=0 then null else x%1001 end as int) v,"
                + "case when x%31=0 then null else x*17 end w,"
                + "case when x%31=0 then null else (x%97-48)*0.25 end d from long_sequence(30001))", context);
        String sql = "select k,min(s),avg(s),max(s),sum(s),sum(v),sum(w),avg(v),avg(w),count(v),count(w),min(v),max(v),min(w),max(w),"
                + "count(d),sum(d),avg(d),min(d),max(d),count() from t group by k";
        long memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            var kernel = vector(candidate).getCompiledAggregate();
            assertNotNull(kernel);
            for (int i = 0; i < 3; i++) {
                assertMatches(sql, candidate);
                assertEquals((i + 1) * 30001L, kernel.getNativeRows());
            }
        }
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void vectorDoublesKeepNonfiniteSignedZeroAndMissingKeyContracts() throws Exception {
        engine.execute("create table doubles (d double,ts timestamp) timestamp(ts) partition by day bypass wal", context);
        try (var writer = engine.getWriter(engine.verifyTableName("doubles"), "test")) {
            for (double value : new double[]{Double.POSITIVE_INFINITY, 17, Double.NaN}) {
                var row = writer.newRow(0); row.putDouble(0, value); row.append();
            }
            writer.commit();
        }
        engine.execute("alter table doubles add column k symbol", context);
        String sql = "select k,count(),count(d),sum(d),avg(d),min(d),max(d) from doubles group by k";
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            assertMatchesVector(sql, candidate);
            assertEquals(0, vector(candidate).getCompiledAggregate().getNativeRows());
            String[] keys = {"zero","zero","null","null","positive","negative","cancel","cancel","cancel","finite",null};
            double[] values = {-0.0,-0.0,Double.NaN,Double.longBitsToDouble(0x7ff8000000001234L),
                    Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,17,
                    Double.MIN_VALUE,42};
            try (var writer = engine.getWriter(engine.verifyTableName("doubles"), "test")) {
                for (int i = 0; i < values.length; i++) {
                    var row = writer.newRow(86400000000L); row.putDouble(0, values[i]); row.putSym(2, keys[i]); row.append();
                }
                writer.commit();
            }
            assertMatchesVector(sql, candidate);
            assertEquals(values.length, vector(candidate).getCompiledAggregate().getNativeRows());
        }
        engine.execute("alter table doubles convert partition to parquet list '1970-01-01'", context);
        try (var candidate = engine.select(sql, context)) { assertMatchesVector(sql, candidate); }
        for (String aggregate : List.of("sum(d)", "avg(d)")) {
            String single = "select k," + aggregate + " from doubles group by k";
            try (var candidate = engine.select(single, context)) {
                assertMatchesVector(single, candidate);
                assertTrue(vector(candidate).getCompiledAggregate().getNativeRows() > 0);
            }
        }
    }

    private void assertMatchesVector(String sql, RecordCursorFactory candidate) throws Exception {
        context.setQueryCodeGenerator(null);
        try (var ordinary = engine.select(sql, context)) {
            var expected = Demo.readSql(ordinary, context, 1);
            var actual = Demo.readSql(candidate, context, 1);
            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i).keys(), actual.get(i).keys());
                for (int j = 0; j < expected.get(i).values().size(); j++) {
                    Long left = expected.get(i).values().get(j), right = actual.get(i).values().get(j);
                    if (ordinary.getMetadata().getColumnType(j + 1) == ColumnType.DOUBLE) {
                        // Rosti's first group creator and worker merge order can change zero's sign.
                        assertEquals(Double.longBitsToDouble(left), Double.longBitsToDouble(right), 0.0);
                    } else assertEquals(left, right);
                }
            }
        } finally { context.setQueryCodeGenerator(compiler); }
    }

    @Test public void wideIntegerStatesPreserveRostiOverflowAndColumnTops() throws Exception {
        engine.execute("create table t (k symbol capacity 8192, v int, w long)", context);
        engine.execute("insert into t select cast(case when x%17=0 then null else cast(x%5 as string) end as symbol),"
                + "cast(case when x%5=0 then null when x%7=0 then -2147483647 else 2147483647 end as int),"
                + "case when x%5=0 then null when x%7=0 then -9223372036854775807 else 9223372036854775807 end "
                + "from long_sequence(10001)", context);
        engine.execute("alter table t add column late long", context);
        String sql = "select k,sum(w),avg(w),avg(late) from t group by k";
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            var kernel = vector(candidate).getCompiledAggregate();
            assertNotNull(kernel);
            for (int pass = 0; pass < 2; pass++) {
                context.setQueryCodeGenerator(null);
                // Keep the vector oracle: its 128-bit sum intentionally differs
                // from ordinary AVG(LONG)'s DOUBLE accumulation.
                try (var stock = engine.select(sql, context)) {
                    assertEquals(Demo.readSql(stock, context, 1), Demo.readSql(candidate, context, 1));
                } finally { context.setQueryCodeGenerator(compiler); }
                assertEquals((pass + 1) * 10001L, kernel.getNativeRows());
            }
        }
        sql = "select k,avg(v),sum(v) from t group by k";
        try (var candidate = engine.select(sql, context)) {
            assertNotNull(vector(candidate).getCompiledAggregate());
            assertMatches(sql, candidate);
            assertEquals(10001, vector(candidate).getCompiledAggregate().getNativeRows());
            context.setQueryCodeGenerator(null);
            try (var ordinary = engine.select(sql, context)) {
                assertEquals(Demo.readSql(ordinary, context, 1), Demo.readSql(candidate, context, 1));
            } finally { context.setQueryCodeGenerator(compiler); }
        }
    }

    @Test public void mixedFloatingAndIntegerAveragesMergeMissingSymbolPages() throws Exception {
        engine.execute("create table mixed as (select cast(2147483647 as int) i,x*11 l,x*0.25 d,"
                + "timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(103)) timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table mixed add column k symbol", context);
        engine.execute("insert into mixed select cast(x as int),x*11,x*0.25,timestamp_sequence('2024-01-02',1000000L),"
                + "cast(case when x%7=0 then null else cast(x%17 as string) end as symbol) from long_sequence(1003)", context);
        String sql = "select k,avg(i),avg(l),sum(d),count() from mixed group by k";
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            assertMatches(sql, candidate);
            assertEquals(1003, vector(candidate).getCompiledAggregate().getNativeRows());
            assertMatches(sql, candidate);
        }
        engine.execute("alter table mixed convert partition to parquet list '2024-01-01'", context);
        try (var candidate = engine.select(sql, context)) { assertMatches(sql, candidate); }
    }

    @Test public void snapshotGrowthDeclinesLargeDictionaryAndReopens() throws Exception {
        engine.execute("create table t (k symbol capacity 8192, s short)", context);
        engine.execute("insert into t select cast(x%7 as symbol),cast(x as short) from long_sequence(1000)", context);
        String sql = "select k,min(s),avg(s),max(s) from t group by k";
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            var kernel = vector(candidate).getCompiledAggregate();
            assertNotNull(kernel);
            assertMatches(sql, candidate);
            assertEquals(1000, kernel.getNativeRows());
            engine.execute("insert into t select cast(x+100 as symbol),cast(x as short) from long_sequence(3000)", context);
            assertMatches(sql, candidate);
            assertEquals(1000, kernel.getNativeRows()); // Rosti fallback, no truncated domain.
            engine.execute("truncate table t", context);
            assertMatches(sql, candidate);
            engine.execute("insert into t select cast(x%3 as symbol),cast(-x as short) from long_sequence(1000)", context);
            assertMatches(sql, candidate);
            assertEquals(2000, kernel.getNativeRows());
        }
    }

    @Test public void columnTopsNullKeysAndEmptyInput() throws Exception {
        engine.execute("create table t as (select x v from long_sequence(1000))", context);
        engine.execute("alter table t add column k symbol", context);
        engine.execute("alter table t add column s short", context);
        engine.execute("insert into t select x,cast(x%5 as symbol),cast(x as short) from long_sequence(1000)", context);
        String sql = "select k,min(s),avg(s),max(s),sum(s),sum(v),avg(v),min(v),max(v),count() from t group by k";
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            assertNotNull(vector(candidate).getCompiledAggregate());
            assertMatches(sql, candidate);
            assertEquals(2000, vector(candidate).getCompiledAggregate().getNativeRows());
        }
        engine.execute("truncate table t", context);
        try (var candidate = engine.select(sql, context)) {
            assertMatches(sql, candidate);
        }
    }

    @Test public void allocationFailureReleasesPartialStateAndAllowsReuse() throws Exception {
        engine.execute("create table t as (select cast(x%31 as symbol) k,cast(x as short) s from long_sequence(10000))", context);
        String sql = "select k,min(s),avg(s),max(s) from t group by k";
        context.setQueryCodeGenerator(compiler);
        try (var candidate = engine.select(sql, context)) {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
            memoryLimit = 3000; // one slot fits, the next exceeds the shared query budget.
            assertThrows(io.questdb.cairo.CairoException.class, () -> {
                try (var cursor = candidate.getCursor(context)) { while (cursor.hasNext()) { } }
            });
            assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
            memoryLimit = 0;
            assertMatches(sql, candidate);
        }
    }

    private void assertMatches(String sql, RecordCursorFactory candidate) throws Exception {
        context.setQueryCodeGenerator(null);
        // Scalar SQL is an independent oracle: vector MAX(SHORT) mishandles an
        // existing group whose initial LONG_NULL narrows to SHORT zero.
        try (var stock = Demo.compileSql(engine, context, sql, false)) {
            assertEquals(Demo.readSql(stock, context, 1), Demo.readSql(candidate, context, 1));
        } finally { context.setQueryCodeGenerator(compiler); }
    }

    private static GroupByRecordCursorFactory vector(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof GroupByRecordCursorFactory)) factory = factory.getBaseFactory();
        assertNotNull("Expected existing vector factory", factory);
        return (GroupByRecordCursorFactory) factory;
    }
}
