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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.CompiledProjection;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByKernel.MergeTask;
import io.questdb.griffin.engine.groupby.GroupByKernel.Updater;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ResolvedSqlAggregateTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private final WorkerPoolMode workerPoolMode;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private TrackingCompiler compiler;
    private int shardingThreshold = 1000;
    private long queryMemoryLimit;
    private int mapEntrySize = 16;
    private int topKThreshold = Integer.MAX_VALUE;
    private String lastPlan;

    public ResolvedSqlAggregateTest(String name, Backend backend, WorkerPoolMode workerPoolMode) {
        this.backend = backend;
        this.workerPoolMode = workerPoolMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(
                new Object[]{"llvm-legacy", new LlvmBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"asmjit-legacy", new AsmJitBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"c-legacy", new NativeBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"llvm-fiber", new LlvmBackend(), WorkerPoolMode.FIBER_HOST},
                new Object[]{"asmjit-fiber", new AsmJitBackend(), WorkerPoolMode.FIBER_HOST},
                new Object[]{"c-fiber", new NativeBackend(), WorkerPoolMode.FIBER_HOST}
        );
    }

    @Before
    public void setUp() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 128; }
            @Override public int getSqlPageFrameMinRows() { return 64; }
            @Override public long getQueryMemoryLimitBytes() { return queryMemoryLimit; }
            @Override public int getGroupByShardingThreshold() { return shardingThreshold; }
            @Override public int getSqlUnorderedMapMaxEntrySize() { return mapEntrySize; }
            @Override public long getGroupByParallelTopKThreshold() { return topKThreshold; }
            @Override public int getGroupByBatchSize() { return 32; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
            @Override public WorkerPoolMode getWorkerPoolMode() { return workerPoolMode; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = new TrackingCompiler(new ResolvedQueryCompiler(backend, temporary.newFolder().toPath()));
        engine.execute("create table t as (select cast(x%17 as int) k,case when x%5=0 then null else x*3 end v,"
                + "cast(case when x%7=0 then null else x end as int) w,cast(x%31 as short) s from long_sequence(10000))", context);
    }

    @After
    public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
    }

    @Test
    public void parallelDistinctUnionPreservesMultipleAggregatesAndReopens() throws Exception {
        shardingThreshold = 2;
        engine.execute("create table unions as (select cast(x%2 as int) k,"
                + "case when x%97=0 then null else x end v,"
                + "case when x%101=0 then null else x%400003 end d,cast(x%311 as short) s "
                + "from long_sequence(2000000))", context);
        assertMatches("select k,count_distinct(v),count_distinct(d),sum(v),avg(s) from unions where k>=0 group by k", true);
        assertMatches("select k,count_distinct(v),sum(v) from unions where k=0 group by k", true);
        assertTrue("Parallel union was not exercised", compiler.parallelUnionTasks.get() > 0);
        assertTrue("Parallel initialization was not exercised", compiler.parallelZeroTasks.get() > 0);
        context.setQueryCodeGenerator(compiler);
        String sql = "select k,count_distinct(v),count_distinct(d),sum(v),avg(s) from unions where k>=0 group by k";
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            compiler.failUnion = true;
            assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 6));
            compiler.failUnion = false;
            var actual = Demo.readSql(factory, context, 6);
            context.setQueryCodeGenerator(null);
            try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(baseline, context, 6), actual);
            }
        }
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void generatedLimitPreservesMapCursorOrderAndReopens() throws Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN);
        engine.execute("create table limits as (select cast(x%1027 as int) k,cast(x%13 as short) s,"
                + "cast(case when x%17=0 then null else 'ž-'||cast(x%509 as string) end as varchar) v "
                + "from long_sequence(10003))", context);
        for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
            shardingThreshold = threshold;
            for (String keys : List.of("k", "v", "k,s")) {
                String query = "select " + keys + ",count() n from limits where k>=0 group by " + keys;
                context.setQueryCodeGenerator(compiler);
                try (var factory = Demo.compileSql(engine, context, query, true)) {
                    var physical = factory.getBaseFactory(); // QueryProgress is the execution owner, outside the plan.
                    assertTrue(plan(factory), physical.prepareRowLimit());
                    try (var cursor = physical.getCursor(context);
                         var rows = new io.questdb.std.DirectLongList(0, MemoryTag.NATIVE_RECORD_CHAIN)) {
                        var expected = new java.util.ArrayList<Long>();
                        while (cursor.hasNext()) expected.add(cursor.getRecord().getRowId());
                        for (long[] bound : new long[][]{{0,10},{7,12},{0,expected.size()},{expected.size()+1,10}}) {
                            cursor.toTop();
                            assertTrue(cursor.selectRows(rows, bound[0], bound[1]));
                            long count = Math.min(bound[1], Math.max(0, expected.size()-bound[0]));
                            assertEquals(count, rows.size());
                            for (int i=0;i<count;i++) assertEquals(expected.get((int)bound[0]+i).longValue(), rows.get(i));
                        }
                    }
                }
                context.setQueryCodeGenerator(null);
                java.util.List<CompiledGroupBy.ResultRow> expected;
                try (var baseline = Demo.compileSql(engine, context, query, true)) {
                    expected = Demo.readSql(baseline, context, baseline.getMetadata().getColumnCount());
                }
                context.setQueryCodeGenerator(compiler);
                try (var factory = Demo.compileSql(engine, context, query+" limit 7,19", true)) {
                    assertTrue(plan(factory), plan(factory).contains("compiledSelection"));
                    for (int run=0;run<2;run++) {
                        var actual = Demo.readSql(factory, context, factory.getMetadata().getColumnCount());
                        assertEquals(12, actual.size());
                        assertTrue(expected.containsAll(actual));
                    }
                    try (var cursor = factory.getCursor(context)) {
                        assertTrue(cursor.hasNext());
                        long first = cursor.getRecord().getRowId();
                        cursor.toTop();
                        assertTrue(cursor.hasNext());
                        assertEquals(first, cursor.getRecord().getRowId());
                        var skip = new io.questdb.cairo.sql.RecordCursor.Counter();
                        skip.set(3); cursor.skipRows(skip, Long.MAX_VALUE); assertEquals(0,skip.get());
                        var count = new io.questdb.cairo.sql.RecordCursor.Counter();
                        cursor.calculateSize(context.getCircuitBreaker(),count); assertEquals(8,count.get());
                        assertFalse(cursor.hasNext());
                    }
                }
            }
        }
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_RECORD_CHAIN));
    }

    @Test
    public void generatedProjectionBatchesPreserveRewindRandomAccessAndNulls() throws Exception {
        long memoryBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        engine.execute("create table projections as (select cast(case when x%13=0 then null else x-600 end as int) k,"
                + "case when x%11=0 then null else x*17 end v,"
                + "cast(cast(case when x%7=0 then null else x*65537 end as int) as ipv4) ip,"
                + "cast('ž-'||cast(x as string) as varchar) label from long_sequence(1027))", context);
        for (String sql : List.of(
                "select k+2147483647 a,v-3 b,ip-1 c,ip-cast(k as int) d,label from projections",
                "select k,k+1 a,v-3 b,1 c,label from projections order by k limit 10,300",
                "select k,k-1 a,k-2 b,count() c from projections where k>=0 group by k order by c desc,k limit 10",
                "select ip,ip-1 a,ip-2 b,ip-3 c,count() n from projections where k>=0 group by ip order by n desc,ip limit 10")) {
            context.setQueryCodeGenerator(null);
            try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                var expected = Demo.readSql(baseline, context, baseline.getMetadata().getColumnCount());
                context.setQueryCodeGenerator(compiler);
                try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                    assertTrue(plan(candidate), plan(candidate).contains("compiledProjection"));
                    assertEquals(sql, expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
                    assertEquals(sql, expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
                }
            }
        }
        String sql = "select k+7 a,v-3 b,label from projections";
        try (var factory = Demo.compileSql(engine, context, sql, true); var cursor = factory.getCursor(context)) {
            assertTrue(cursor.hasNext());
            var a = cursor.getRecord();
            long first = a.getRowId(), firstValue = a.getLong(1);
            assertTrue(cursor.hasNext());
            long secondValue = a.getLong(1);
            var b = cursor.getRecordB();
            cursor.recordAt(b, first);
            assertEquals(firstValue, b.getLong(1));
            assertEquals(secondValue, a.getLong(1));
            cursor.toTop();
            assertTrue(cursor.hasNext());
            assertEquals(firstValue, a.getLong(1));
            var skip = new io.questdb.cairo.sql.RecordCursor.Counter();
            skip.set(131);
            cursor.skipRows(skip, Long.MAX_VALUE);
            assertEquals(0, skip.get());
            assertTrue(cursor.hasNext());
            assertEquals(133*17-3, a.getLong(1));
            var remaining = new io.questdb.cairo.sql.RecordCursor.Counter();
            cursor.calculateSize(context.getCircuitBreaker(), remaining);
            assertEquals(1027-133, remaining.get());
            assertFalse(cursor.hasNext());
        }
        assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test
    public void searchedCaseVarcharKeysUseGeneratedMaps() throws Exception {
        engine.execute("create table choices as (select cast(x%7 as int) mode,"
                + "cast(case when x%11=0 then null when x%13=0 then '' else 'ž-key-'||cast(x%29 as string) end as varchar) a,"
                + "cast('other-long-prefix-'||cast(x%17 as string) as varchar) b,x v from long_sequence(4001))", context);
        for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
            shardingThreshold = threshold;
            for (String expression : List.of(
                    "case when mode<2 then a else '' end",
                    "case when mode<2 then a when mode<5 then b else '🦊-constant' end",
                    "case when mode<2 then a when mode=3 then null end",
                    "case when mode<2 then case when mode<1 then a else b end else 'nested' end",
                    "case when mode<2 then cast('left' as varchar) else 'right' end")) {
                long before = compiler.mapRows.get();
                assertMatches("select mode," + expression + " k,count(),sum(v) from choices group by mode,k", true);
                assertEquals(lastPlan, 8002, compiler.mapRows.get() - before);
            }
        }
        assertTrue(compiler.merges.get() > 0);
        assertTrue(compiler.redistributions.get() > 0);
        // The same result-function class also hosts SWITCH; its picker is a different contract.
        long before = compiler.mapRows.get();
        assertMatches("select case mode when 0 then a else b end k,count(),sum(v) from choices group by k", true);
        assertEquals(before, compiler.mapRows.get());
        assertMatches("select case when mode<2 then a else cast(v as varchar) end k,count() from choices group by k", true);
        assertEquals(before, compiler.mapRows.get());
    }

    @Test
    public void searchedCaseKeysPreserveStorageAndLiteralLifetimes() throws Exception {
        engine.execute("create table choices as (select cast(case when x%7=0 then null else x%5 end as int) mode,"
                + "cast(case when x%11=0 then null when x%13=0 then '' else 'ž-'||cast(x%17 as string) end as varchar) a,"
                + "x v,timestamp_sequence('2024-01-01',3600000000L) ts from long_sequence(48)) "
                + "timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table choices add column b varchar", context);
        engine.execute("insert into choices select cast(x%5 as int),cast(x%17 as varchar),x,"
                + "timestamp_sequence('2024-01-03',3600000000L),cast(x%3 as varchar) from long_sequence(24)", context);
        String literal = "🦊-long-literal-".repeat(300);
        String expression = "case when mode<2 then a when mode>=4 then b else '" + literal + "' end";
        String sql = "select " + expression + " k,count(),sum(v) total from choices group by k";
        long memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table choices convert partition to parquet list '2024-01-01'", context);
            long before = compiler.mapRows.get();
            assertMatches(sql, true);
            assertEquals(lastPlan, 144, compiler.mapRows.get() - before);
            long ranked = compiler.topKRows.get();
            // Retain every group: order ties cannot change the selected set.
            assertMatches(sql + " order by total desc limit 100", true);
            assertTrue(lastPlan, compiler.topKRows.get() > ranked);
        }
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test
    public void variableOrderedKeysShareEncodingMergeAndTopK() throws Exception {
        engine.execute("create table variable as (select cast(case when x%29=0 then null when x%31=0 then '' "
                + "when x%3=0 then cast(x%509 as string) else 'žluťoučký-shared-prefix-'||cast(x%509 as string) end as varchar) a,"
                + "cast(case when x%37=0 then null when x%41=0 then '' else cast(x%13 as string) end as varchar) b,"
                + "cast(x%7-3 as short) s,cast(x%11-5 as int) i,x v,"
                + "timestamp_sequence('2024-01-01',3600000L) ts from long_sequence(48000)) timestamp(ts) partition by day bypass wal", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table variable convert partition to parquet list '2024-01-01'", context);
            for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
                shardingThreshold = threshold;
                topKThreshold = 0;
                for (String keys : List.of("a", "i,a", "a,i", "a,b", "s,b,a,i+1")) {
                    long before = compiler.mapRows.get(), ranked = compiler.topKRows.get();
                    String sql = "select " + keys + ",count(),sum(v) total,count_distinct(v) from variable group by " + keys;
                    assertMatches(sql, true);
                    assertEquals(lastPlan, 96000, compiler.mapRows.get() - before);
                    assertMatches(sql + " order by total desc limit 10", true);
                    assertTrue(lastPlan, compiler.topKRows.get() > ranked);
                }
            }
        }
        assertTrue(compiler.merges.get() > 0);
        assertTrue(compiler.redistributions.get() > 0);
    }

    @Test
    public void variableOrderedLongKeysGrowStagingAndRetainNullBoundaries() throws Exception {
        engine.execute("create table variable as (select cast(case when x%5=0 then null when x%5=1 then '' "
                + "else '" + "shared-prefix-".repeat(300) + "'||cast(x%43 as string) end as varchar) a,"
                + "cast(case when x%7=0 then null else cast(x%13 as string) end as varchar) b,x v from long_sequence(2000))", context);
        for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
            shardingThreshold = threshold;
            long before = compiler.mapRows.get();
            assertMatches("select a,b,count(),sum(v) from variable group by a,b", true);
            assertEquals(lastPlan, 4000, compiler.mapRows.get() - before);
            assertMatches("select a,b,count(),sum(v) from variable where a is not null group by a,b order by sum(v) desc limit 10", true);
        }
        context.setQueryCodeGenerator(compiler);
        String sql = "select a,b,count(),sum(v) from variable group by a,b";
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            queryMemoryLimit = 16384;
            CairoException failure = assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 4));
            assertTrue(failure.isOutOfMemory());
            queryMemoryLimit = 0;
            var actual = Demo.readSql(factory, context, 4);
            context.setQueryCodeGenerator(null);
            try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(baseline, context, 4), actual);
            }
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void sharedVarcharMapsPreserveGrowthFallbackAndFailureReopen() throws Exception {
        String previous = System.getProperty("query.codegen.sharedVarchar");
        System.setProperty("query.codegen.sharedVarchar", "true");
        long memoryBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        try {
            mapEntrySize = 32;
            engine.execute("create table shared_strings as (select cast(case when x%29=0 then null when x%31=0 then '' "
                    + "when x%3=0 then cast(x%30011 as string) else 'žluťoučký-prefix-'||cast(x%30011 as string) end as varchar) k,"
                    + "timestamp_sequence('2024-01-01',3600000L) ts from long_sequence(96000)) timestamp(ts) partition by day bypass wal", context);
            String sql = "select k,count() from shared_strings group by k";
            assertMatches(sql, true);
            assertTrue("Shared maps were not finalized", compiler.sharedFinishes.get() > 0);
            assertTrue("Shared update kernel was not exercised", compiler.delegates.values().stream()
                    .mapToLong(ResolvedQueryCompiler::sharedRows).sum() > 0);
            engine.execute("alter table shared_strings convert partition to parquet list '2024-01-01'", context);
            assertMatches(sql, true);
            assertMatches("select k,count() from shared_strings where k is not null group by k", true);
            engine.execute("create table shared_small as (select cast(x%17 as varchar) k from long_sequence(160000))", context);
            long sharedBefore = compiler.delegates.values().stream().mapToLong(ResolvedQueryCompiler::sharedRows).sum();
            long nativeBefore = compiler.mapRows.get();
            assertMatches("select k,count() from shared_small group by k", true);
            long sharedDelta = compiler.delegates.values().stream().mapToLong(ResolvedQueryCompiler::sharedRows).sum() - sharedBefore;
            assertTrue("Small groups must exercise both shared and private generated updates", sharedDelta > 0 && sharedDelta < 320000);
            assertEquals(320000, compiler.mapRows.get() - nativeBefore);
            // Observing a small prefix must not freeze the choice for later input.
            engine.execute("create table shared_phases as (select cast(case when x<=131072 then x%17 else x%30011 end as varchar) k "
                    + "from long_sequence(196608))", context);
            assertMatches("select k,count() from shared_phases group by k", true);
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql, true)) {
                compiler.failUpdate = true;
                assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 2));
                compiler.failUpdate = false;
                queryMemoryLimit = 65536;
                CairoException breach = assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 2));
                assertTrue(breach.isOutOfMemory());
                queryMemoryLimit = 0;
                var actual = Demo.readSql(factory, context, 2);
                context.setQueryCodeGenerator(null);
                try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                    assertEquals(Demo.readSql(baseline, context, 2), actual);
                }
            }
            assertEquals(0, engine.getBusyReaderCount());
            assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        } finally {
            compiler.failUpdate = false;
            queryMemoryLimit = 0;
            if (previous == null) System.clearProperty("query.codegen.sharedVarchar");
            else System.setProperty("query.codegen.sharedVarchar", previous);
        }
    }

    @Test
    public void varcharFramesUseNativeMapsMergeAndTopK() throws Exception {
        mapEntrySize = 64;
        engine.execute("create table strings as (select cast(case when x%29=0 then null when x%31=0 then '' "
                + "when x%3=0 then cast(x%509 as string) when x%3=1 then 'shared-prefix-abcdefghijkl-'||cast(x%509 as string) "
                + "else 'žluťoučký-'||cast(x%509 as string) end as varchar) k,case when x%13=0 then null else x end v,"
                + "case when x%7=0 then null else x%53 end w,"
                + "timestamp_sequence('2024-01-01',3600000L) ts from long_sequence(48000)) timestamp(ts) partition by day bypass wal", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table strings convert partition to parquet list '2024-01-01'", context);
            for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
                shardingThreshold = threshold;
                long before = compiler.mapRows.get();
                assertMatches("select k,count(),sum(v) from strings group by k", true);
                assertEquals(lastPlan, 96000, compiler.mapRows.get() - before);
                before = compiler.mapRows.get();
                assertMatches("select k,count_distinct(v) n from strings group by k order by n desc limit 2000", true);
                assertEquals(lastPlan, 96000, compiler.mapRows.get() - before);
                before = compiler.mapRows.get();
                assertMatches("select k,count(),count_distinct(w),sum(v),count_distinct(v) from strings group by k", true);
                assertEquals(lastPlan, 96000, compiler.mapRows.get() - before);
                before = compiler.mapRows.get();
                assertMatches("select k,count(),sum(v) from strings where k is not null group by k", true);
                // Filtered Parquet may compact non-filter columns; retain the existing safe fallback.
                long generatedRows = parquet ? 24000 - (48000 / 29 - 24000 / 29) : 48000 - 48000 / 29;
                assertEquals(lastPlan, 2L * generatedRows, compiler.mapRows.get() - before);
                assertMatches("select k,count() from strings where k is null group by k", true);
                topKThreshold = 0;
                long ranked = compiler.topKRows.get();
                assertMatches("select k,sum(v) total,count() from strings where k is not null group by k order by total desc limit 10", true);
                assertTrue(lastPlan, compiler.topKRows.get() > ranked);
            }
        }
        assertTrue(compiler.merges.get() > 0);
        assertTrue(compiler.redistributions.get() > 0);
    }

    @Test
    public void varcharColumnTopsAndLazyConversionsAreSafe() throws Exception {
        mapEntrySize = 32;
        engine.execute("create table strings as (select x v,timestamp_sequence('2024-01-01',3600000000L) ts "
                + "from long_sequence(48)) timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table strings add column k varchar", context);
        engine.execute("insert into strings select x,timestamp_sequence('2024-01-03',3600000000L),cast(x%7 as varchar) from long_sequence(48)", context);
        assertMatches("select k,count() from strings group by k", true);
        assertMatches("select k,count() from strings where k is not null group by k", true);
        engine.execute("create table converted as (select cast(x%17 as string) k,x v,timestamp_sequence('2024-01-01',3600000000L) ts "
                + "from long_sequence(48)) timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table converted convert partition to parquet list '2024-01-01'", context);
        engine.execute("alter table converted alter column k type varchar", context);
        assertMatches("select k,count() from converted where k is not null group by k", true);
    }

    @Test
    public void normalSqlUsesResolvedCodeAndOrdinaryResultCursor() throws Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        for (String sql : List.of(
                "select k,count() from t where v!=42 group by k",
                "select k,count(),count(v),sum(v),count(w),sum(w),sum(s) from t where k>=0 group by k",
                "select sum(v) total,k,sum(v) again,count(v) n from t where v>100 group by k",
                "select k,s,count(),sum(v) from t where w<1000 group by k,s",
                "select k,sum(v+1) from t where k>=0 group by k",
                "select k,sum(v),count_distinct(v),count() from t where k>=0 group by k")) {
            assertMatches(sql, true);
        }
        engine.execute("create table bytes as (select cast(x%11-5 as byte) b,cast(x%3 as int) k from long_sequence(1000))", context);
        assertMatches("select k,count(b),sum(b) from bytes where k>=0 group by k", true);
        assertTrue(compiler.rows.get() > 0);
        assertTrue(compiler.calls.get() < compiler.rows.get());
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void finalizedDistinctTopKUsesWorkersAndPreservesFallbackAndReopen() throws Exception {
        shardingThreshold = 2;
        topKThreshold = 0;
        engine.execute("create table distinct_ranked as (select cast(x%257 as int) k,"
                + "case when x%257=0 then null else cast(x/257 as long)%(x%257) end v "
                + "from long_sequence(154200))", context);
        for (String direction : List.of("asc", "desc")) {
            for (int limit : new int[]{10, 73}) {
                String sql = "select k,count_distinct(v) s,count() n from distinct_ranked group by k order by s "
                        + direction + " limit " + limit;
                context.setQueryCodeGenerator(null);
                java.util.List<Long> expected;
                try (var factory = Demo.compileSql(engine, context, sql, true)) { expected = readOrdered(factory); }
                context.setQueryCodeGenerator(compiler);
                try (var factory = Demo.compileSql(engine, context, sql, true)) {
                    long merges = compiler.topKMerges.get();
                    assertEquals(expected, readOrdered(factory));
                    assertTrue("Finalized DISTINCT must use worker-list merge", compiler.topKMerges.get() > merges);
                    compiler.declineTopK = true;
                    long fallback = compiler.topKFallbacks.get();
                    try { assertEquals(expected, readOrdered(factory)); }
                    finally { compiler.declineTopK = false; }
                    assertTrue("Exercise ordinary map-reader fallback", compiler.topKFallbacks.get() > fallback);
                    compiler.failTopK = true;
                    try { assertThrows(CairoException.class, () -> readOrdered(factory)); }
                    finally { compiler.failTopK = false; }
                    assertEquals(expected, readOrdered(factory));
                }
            }
        }
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void numericTopKUsesNativeCollectionAndWorkerMerge() throws Exception {
        // This test forces ordinary shard collection and worker-list merge. Bounded
        // aggregation deliberately bypasses the sharding threshold for small maps.
        String previous = System.getProperty("query.codegen.bounded");
        System.setProperty("query.codegen.bounded", "false");
        try { assertNumericTopKUsesNativeCollectionAndWorkerMerge(); }
        finally {
            if (previous == null) System.clearProperty("query.codegen.bounded");
            else System.setProperty("query.codegen.bounded", previous);
        }
    }

    private void assertNumericTopKUsesNativeCollectionAndWorkerMerge() throws Exception {
        mapEntrySize = 32;
        engine.execute("create table ranked as (select cast(x%997 as int) k,x v from long_sequence(20000))", context);
        for (int shardAt : new int[]{Integer.MAX_VALUE, 2}) {
            shardingThreshold = shardAt;
            for (int parallelAt : new int[]{Integer.MAX_VALUE, 0}) {
                topKThreshold = parallelAt;
                for (String direction : List.of("asc", "desc")) {
                    String sql = "select k,sum(v) s,count() n from ranked where k>=0 group by k order by s " + direction + " limit 73";
                    context.setQueryCodeGenerator(null);
                    java.util.List<Long> expected;
                    try (var factory = Demo.compileSql(engine, context, sql, true)) { expected = readOrdered(factory); }
                    context.setQueryCodeGenerator(compiler);
                    long before = compiler.topKRows.get();
                    long merges = compiler.topKMerges.get();
                    try (var factory = Demo.compileSql(engine, context, sql, true)) {
                        assertTrue(plan(factory), plan(factory).contains("Long Top K"));
                        assertEquals(expected, readOrdered(factory));
                        assertEquals(expected, readOrdered(factory)); // close/reopen
                    }
                    assertEquals(1994, compiler.topKRows.get() - before);
                    if (shardAt == 2 && parallelAt == 0) assertTrue(compiler.topKMerges.get() > merges);
                }
            }
        }
        long collected = compiler.topKRows.get();
        mapEntrySize = 16; // COUNT+SUM now exercises generated OrderedMap collection.
        assertMatches("select k,sum(v),count() from ranked where k>=0 group by k order by sum(v) desc limit 10", true);
        assertEquals(collected + 1994, compiler.topKRows.get());
        collected = compiler.topKRows.get();
        assertMatches("select sum(v),count() from ranked where k>=0 group by cast(k as varchar) order by sum(v) desc limit 10", true);
        assertEquals(collected, compiler.topKRows.get());
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
    }

    private java.util.List<Long> readOrdered(RecordCursorFactory factory) throws Exception {
        var rows = new java.util.ArrayList<Long>();
        try (var cursor = factory.getCursor(context)) {
            var record = cursor.getRecord();
            while (cursor.hasNext()) {
                rows.add((long) record.getInt(0)); rows.add(record.getLong(1)); rows.add(record.getLong(2));
            }
            cursor.toTop();
            int offset = 0;
            while (cursor.hasNext()) {
                assertEquals((long) rows.get(offset++), record.getInt(0));
                assertEquals((long) rows.get(offset++), record.getLong(1));
                assertEquals((long) rows.get(offset++), record.getLong(2));
            }
            assertEquals(rows.size(), offset);
        }
        return rows;
    }

    @Test
    public void cyclicTimestampPartsMatchDriversBeforeAndAfterEpoch() throws Exception {
        shardingThreshold = 2;
        engine.execute("create table time_parts as (select "
                + "cast(case when x%19=0 then null when x%19=1 then 9223372036854775807L "
                + "when x%19=2 then -9223372036854775807L else (x-5000)*1000001L end as timestamp) tm,"
                + "cast(case when x%19=0 then null when x%19=1 then 9223372036854775807L "
                + "when x%19=2 then -9223372036854775807L else (x-5000)*1000000001L end as timestamp_ns) tn,"
                + "cast(x%31 as short) v,timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(10003)) "
                + "timestamp(ts) partition by day bypass wal", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table time_parts convert partition to parquet list '2024-01-01'", context);
            for (String key : List.of("hour(tm)", "minute(tm)", "second(tm)", "hour(tn)", "minute(tn)", "second(tn)",
                    "extract(minute from tm)", "minute(tm)+1,second(tn)")) {
                long before = compiler.mapRows.get();
                assertMatches("select " + key + ",count(),sum(v),avg(v) from time_parts where v>=0 group by " + key, true);
                assertTrue(lastPlan, compiler.mapRows.get() > before);
            }
        }
    }

    @Test
    public void constantMembershipUsesResolvedValuesAndNullSemantics() throws Exception {
        for (String predicate : List.of("k in (1)", "k not in (1,6)", "v in (null,9,12,9,0)",
                "v not in (null,9,12,0)", "w in (null,2147483647+1,6,7)", "s in (-1,6)",
                "k in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)")) {
            long before = compiler.filterRows.get();
            assertMatches("select k,count() from t where " + predicate + " group by k", true);
            assertTrue(lastPlan, compiler.filterRows.get() > before);
        }
    }

    @Test
    public void fullLongSortUsesGeneratedCollectionAndRadix() throws Exception {
        shardingThreshold = 2;
        engine.execute("create table full_ranks as (select x%10007 k,cast('key-'||cast(x%10007 as string) as varchar) s,"
                + "case when x%10007%11=0 then null else x%10007-5003 end v from long_sequence(40028))", context);
        for (String key : List.of("k", "s")) {
            for (boolean ascending : new boolean[]{true, false}) {
                String sql = "select " + key + ",sum(v) total from full_ranks where k>=0 group by " + key
                        + " order by total " + (ascending ? "asc" : "desc");
                long before = compiler.sortRows.get();
                assertMatches(sql, true);
                assertTrue(lastPlan, lastPlan.contains("compiledSort"));
                assertEquals(20014, compiler.sortRows.get() - before);
                context.setQueryCodeGenerator(compiler);
                try (var factory = Demo.compileSql(engine, context, sql, true); var cursor = factory.getCursor(context)) {
                    for (int repeat = 0; repeat < 2; repeat++) {
                        long previous = ascending ? Long.MIN_VALUE : Long.MAX_VALUE, count = 0;
                        while (cursor.hasNext()) {
                            long value = cursor.getRecord().getLong(1);
                            assertTrue(ascending ? previous <= value : previous >= value);
                            previous = value; count++;
                        }
                        assertEquals(10007, count);
                        cursor.toTop();
                    }
                }
            }
        }
        assertMatches("select k,sum(v) total from full_ranks where k<0 group by k order by total", true);
    }

    @Test
    public void offsetTopKUsesGeneratedBoundedSorting() throws Exception {
        shardingThreshold = 2;
        topKThreshold = 0;
        engine.execute("create table ranks as (select x%12007 k,x%12007 v from long_sequence(48028))", context);
        for (String order : List.of("asc", "desc")) {
            topKThreshold = order.equals("asc") ? 0 : Integer.MAX_VALUE;
            for (String window : List.of("0,10", "63,64", "64,65", "65,80", "10000,10010", "13000,13010")) {
                long before = compiler.topKRows.get(), selectedBefore = compiler.sortedSelections.get();
                assertMatches("select k,sum(v) total from ranks where k>=0 group by k order by total " + order + " limit " + window, true);
                assertTrue(lastPlan, lastPlan.contains("Long Top K"));
                assertTrue(lastPlan, compiler.topKRows.get() > before);
                if (!window.startsWith("0,")) {
                    assertTrue(lastPlan, lastPlan.contains("compiledSelection: true"));
                    long selected = window.equals("63,64") || window.equals("64,65") ? 1 : window.startsWith("65,") ? 15 : window.startsWith("10000,") ? 10 : 0;
                    assertEquals(2 * selected, compiler.sortedSelections.get() - selectedBefore);
                }
            }
        }
    }

    @Test
    public void physicalKeysPreserveIpAndTemporalRepresentations() throws Exception {
        mapEntrySize = 64;
        shardingThreshold = 2;
        engine.execute("create table physical_keys as (select "
                + "cast(case when x%7=0 then null else cast(x%509-2147483648L as int) end as ipv4) ip,"
                + "cast(case when x%11=0 then null else x%509-200 end as date) d,"
                + "cast(case when x%13=0 then null else x%509-200 end as timestamp) tm,"
                + "cast(case when x%17=0 then null else x%509-200 end as timestamp_ns) tn,"
                + "cast(x%3-1 as short) k,cast('ž-'||cast(x%7 as string) as varchar) s,cast(x%31-15 as short) v,"
                + "timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(12003)) "
                + "timestamp(ts) partition by day bypass wal", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table physical_keys convert partition to parquet list '2024-01-01'", context);
            for (String keys : List.of("ip", "d", "tm", "tn", "k,ip", "d,tm,tn,ip", "ip,s,d")) {
                long before = compiler.mapRows.get();
                assertMatches("select " + keys + ",count(),sum(v),avg(v) from physical_keys where k>=-1 group by " + keys, true);
                assertTrue(lastPlan, compiler.mapRows.get() > before);
            }
        }
    }

    @Test
    public void averageFinalizesAfterEveryMergeAndReopens() throws Exception {
        mapEntrySize = 64;
        engine.execute("create table averages as (select cast(x%3001 as int) k,"
                + "cast(case when x%11=0 then null when x%13=0 then '' else 'ž-'||cast(x%3001 as string) end as varchar) s,"
                + "cast(x%65536-32768 as short) v,x%509 d,"
                + "timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(60003)) "
                + "timestamp(ts) partition by day bypass wal", context);
        System.setProperty("query.codegen.localCapacity", "64");
        try {
            for (boolean parquet : new boolean[]{false, true}) {
                if (parquet) engine.execute("alter table averages convert partition to parquet list '2024-01-01'", context);
                for (boolean bounded : new boolean[]{false, true}) {
                    System.setProperty("query.codegen.bounded", Boolean.toString(bounded));
                    for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
                        shardingThreshold = threshold;
                        for (String keys : List.of("k", "s", "k,s")) {
                            long before = compiler.mapRows.get();
                            assertMatches("select " + keys + ",avg(v),sum(v),count() from averages where k>=0 group by " + keys, true);
                            assertTrue(lastPlan, compiler.mapRows.get() > before);
                        }
                    }
                }
            }
            assertMatches("select k,avg(v),count_distinct(d) from averages where k>=0 group by k", true);
            assertMatches("select k,avg(v)+1 value from averages where k>=0 group by k order by k", true);
            assertMatches("select k,avg(v) from averages where k<0 group by k", true);
        } finally {
            System.clearProperty("query.codegen.bounded");
            System.clearProperty("query.codegen.localCapacity");
        }
    }

    @Test
    public void keyOnlyMapsUseTheExistingDistinctRewrite() throws Exception {
        engine.execute("create table distinct_keys as (select "
                + "case when x%11=0 then null else x%10007 end k,"
                + "cast(case when x%7=0 then null when x%13=0 then '' else 'ž-key-'||cast(x%3001 as string) end as varchar) s,"
                + "timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(50003)) "
                + "timestamp(ts) partition by day bypass wal", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table distinct_keys convert partition to parquet list '2024-01-01'", context);
            for (String key : List.of("k", "s")) {
                long before = compiler.mapRows.get();
                assertMatches("select count_distinct(" + key + ") from distinct_keys", true);
                assertTrue("Generated key-only probes: " + key, compiler.mapRows.get() > before);
                assertMatches("select count_distinct(" + key + ") from distinct_keys where k<0", true);
            }
        }
    }

    @Test
    public void boundedPartialStatesPreserveKeysAndReopen() throws Exception {
        System.setProperty("query.codegen.bounded", "true");
        System.setProperty("query.codegen.localCapacity", "64");
        try {
            mapEntrySize = 64;
            engine.execute("create table bounded as (select cast(x%2003 as int) i,x%3001 k,"
                    + "cast(case when x%19=0 then null when x%23=0 then '' else 'ž-prefix-'||cast(x%2003 as string) end as varchar) s,"
                    + "case when x%11=0 then null else x end v,"
                    + "timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(10003)) "
                    + "timestamp(ts) partition by day bypass wal", context);
            for (boolean parquet : new boolean[]{false, true}) {
                if (parquet) engine.execute("alter table bounded convert partition to parquet list '2024-01-01'", context);
                for (String key : List.of("i", "k", "s", "i,k")) {
                    long before = compiler.redistributions.get();
                    assertMatches("select " + key + ",count(),count(v),sum(v) from bounded where i>=0 group by " + key, true);
                    assertTrue("Bounded " + key, compiler.redistributions.get() > before);
                }
                assertMatches("select i%3 k,count(),count(v),sum(v) from bounded where i>=0 group by k", true);
            }
            context.setQueryCodeGenerator(compiler);
            String sql = "select s,count(),sum(v) from bounded where i>=0 group by s";
            try (var factory = Demo.compileSql(engine, context, sql, true)) {
                long failuresBefore = compiler.nativeMemoryFailures.get();
                for (long limit = 4096; limit <= (1 << 20) && compiler.nativeMemoryFailures.get() == failuresBefore; limit *= 2) {
                    queryMemoryLimit = limit;
                    try { Demo.readSql(factory, context, 3); }
                    catch (CairoException failure) { assertTrue(failure.isOutOfMemory()); }
                }
                assertTrue("Exercise tracked partition allocation failure", compiler.nativeMemoryFailures.get() > failuresBefore);
                queryMemoryLimit = 0;
                compiler.failUpdate = true;
                try { assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 3)); }
                finally { compiler.failUpdate = false; }
                var actual = Demo.readSql(factory, context, 3);
                context.setQueryCodeGenerator(null);
                try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                    assertEquals(Demo.readSql(baseline, context, 3), actual);
                }
            }
            assertTrue("Exercise partition publication", compiler.redistributions.get() > 0);
            assertTrue("Exercise generated partial-state merge", compiler.merges.get() > 0);
        } finally {
            System.clearProperty("query.codegen.bounded");
            System.clearProperty("query.codegen.localCapacity");
        }
    }

    @Test
    public void symbolIdsKeepDictionaryAndNullSemanticsAcrossMapsAndMerge() throws Exception {
        engine.execute("create table symbols as (select "
                + "cast(case when x%7=0 then null when x%5=0 then '' else 's'||cast(x%37 as string) end as symbol) a,"
                + "cast(case when x%11=0 then null else 't'||cast(x%13 as string) end as symbol) b,"
                + "cast(x%3 as short) k,cast(case when x%17=0 then null else 'ž-'||cast(x%19 as string) end as varchar) s,"
                + "x%101 v,timestamp_sequence('2024-01-01',3600000L) ts from long_sequence(48000)) "
                + "timestamp(ts) partition by day bypass wal", context);
        mapEntrySize = 32;
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table symbols convert partition to parquet list '2024-01-01'", context);
            for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
                shardingThreshold = threshold;
                for (String keys : List.of("a", "k,a", "a,b", "a,s")) {
                    long before = compiler.mapRows.get();
                    assertMatches("select " + keys + ",count(),count_distinct(v) from symbols group by " + keys, true);
                    assertEquals(lastPlan, 96000, compiler.mapRows.get() - before);
                }
                long before = compiler.filterRows.get();
                assertMatches("select a,count(),count_distinct(v) from symbols where a is not null group by a order by count_distinct(v) desc limit 100", true);
                assertTrue(lastPlan, compiler.filterRows.get() > before);
                assertMatches("select a,count() from symbols where a is null and k>=0 group by a", true);
            }
        }
        assertTrue(compiler.merges.get() > 0);
        assertTrue(compiler.redistributions.get() > 0);
    }

    @Test
    public void standaloneSelectionAndCountShareNativePredicatesAndReopen() throws Exception {
        context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
        var nativeCompiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
        for (String predicate : List.of("v!=null", "w<117", "s>=30 and v!=null", "k=2 or v=null", "k<0")) {
            for (String output : List.of("count()", "v,w")) {
                String sql = "select " + output + " from t where " + predicate;
                context.setQueryCodeGenerator(null);
                try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                    var expected = Demo.readSql(baseline, context, baseline.getMetadata().getColumnCount());
                    context.setQueryCodeGenerator(nativeCompiler);
                    try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                        assertTrue(plan(candidate), plan(candidate).contains("compiledFilter: " + backend.name()));
                        RecordCursorFactory current = candidate;
                        while (!(current instanceof io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory)) {
                            current = current.getBaseFactory();
                            assertNotNull(current);
                        }
                        var filtered = (io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory) current;
                        assertEquals(expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
                        assertEquals(expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
                        assertEquals(20_000, output.equals("count()")
                                ? filtered.getCompiledCountOnlyFilter().getNativeInputRows()
                                : filtered.getCompiledFilter().getNativeInputRows());
                    }
                }
            }
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void standaloneFilterPreservesColumnTopsAndParquetAndClosesPartialCompilation() throws Exception {
        context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
        var nativeCompiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
        engine.execute("create table standalone as (select x v,cast(x%17 as int) k,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(193)) timestamp(ts) partition by day", context);
        engine.execute("alter table standalone add column top long", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table standalone convert partition to parquet list '2024-01-01'", context);
            for (String sql : List.of("select count() from standalone where top!=null",
                    "select count() from standalone where k<4", "select v from standalone where k<4 limit 7")) {
                context.setQueryCodeGenerator(null);
                try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                    var expected = Demo.readSql(baseline, context, 1);
                    context.setQueryCodeGenerator(nativeCompiler);
                    try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                        assertEquals(expected, Demo.readSql(candidate, context, 1));
                        assertEquals(expected, Demo.readSql(candidate, context, 1));
                    }
                }
            }
        }
        var closed = new java.util.concurrent.atomic.AtomicInteger();
        Backend failsSecond = new Backend() {
            int calls;
            @Override public String name() { return backend.name(); }
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path path) throws java.io.IOException {
                if (++calls == 2) throw new java.io.IOException("count compilation failed");
                var code = backend.compile(program, path);
                return new CompiledKernel(code, () -> { code.close(); closed.incrementAndGet(); }, code.artifacts(), code.compilationNanos());
            }
        };
        context.setQueryCodeGenerator(new ResolvedQueryCompiler(failsSecond, temporary.newFolder().toPath()));
        assertThrows(CairoException.class, () -> Demo.compileSql(engine, context, "select v from t where w<100", true));
        assertEquals(1, closed.get());
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void numericAndVarcharConjunctionsUseBorrowedFrameDescriptors() throws Exception {
        engine.execute("create table predicates as (select x%31 k,x v,"
                + "cast(case when x%7=0 then null when x%3=0 then '' else 'žluťoučký-long-value' end as varchar) a,"
                + "cast(case when x%11=0 then null else cast(x%5 as string) end as varchar) b "
                + "from long_sequence(20003))", context);
        for (String predicate : List.of(
                "k<10 and a is not null", "a is null and k<10", "k<0 and a is null",
                "a is not null and k>=0 and b is null and v!=null",
                "(k=1 or k=2) and a is not null and b is not null")) {
            long before = compiler.filterRows.get();
            assertMatches("select k,count(),sum(v) from predicates where " + predicate + " group by k", true);
            assertEquals(predicate, 40006, compiler.filterRows.get() - before);
        }
        long before = compiler.filterRows.get();
        assertMatches("select k,count() from predicates where a is null or k<10 group by k", true);
        assertEquals("Unsupported mixed OR retains the ordinary filter", before, compiler.filterRows.get());
        engine.execute("alter table predicates add column top varchar", context);
        assertMatches("select k,count() from predicates where k<10 and top is null group by k", true);
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
    }

    @Test
    public void resolvedFiltersUseNativeSelectionAndKeepUnsupportedPredicatesOrdinary() throws Exception {
        mapEntrySize = 32;
        for (String predicate : List.of(
                "k>=0", "v<1000", "v>=w", "v=w", "v!=null", "v=null", "v=NaN", "w=NaN",
                "k>=0 and v<1000 and w!=null", "k>=0 and v<1000 and w!=null and s<12",
                "k=2 or (v<1000 and w!=null)", "not (v<w)", "s<=2", "v>9223372036854775806")) {
            long before = compiler.filterRows.get();
            assertMatches("select k,count(),sum(v) from t where " + predicate + " group by k", true);
            assertEquals(predicate, 20000, compiler.filterRows.get() - before);
        }
        long before = compiler.filterRows.get();
        assertMatches("select k,count(),sum(v) from t where v%7=0 group by k", true);
        assertEquals("Unsupported arithmetic must retain ordinary filtering", before, compiler.filterRows.get());
        context.with(new io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl(engine.getConfiguration()));
        context.getBindVariableService().setLong(0, 1000);
        assertMatches("select k,count(),sum(v) from t where v<$1 group by k", true);
        assertEquals("Runtime binds must not become compile-time constants", before, compiler.filterRows.get());
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
    }

    @Test
    public void computedKeysArgumentsAndFiltersShareIntegerSemantics() throws Exception {
        mapEntrySize = 64; // Exercise Unordered4/8 for single computed keys, OrderedMap for composites.
        for (int shardAt : new int[]{Integer.MAX_VALUE, 2}) {
            shardingThreshold = shardAt;
            for (String keys : List.of("w+1", "v-1", "w+1,k")) {
                long before = compiler.mapRows.get();
                assertMatches("select " + keys + ",count(),sum(v+w),count(w-1),count_distinct(v-1) "
                        + "from t where k+1>=0 group by " + keys, true);
                assertEquals(keys, 20000, compiler.mapRows.get() - before);
            }
        }
        mapEntrySize = 16; // Same computed scalar key with the ordinary wide-state OrderedMap selection.
        long before = compiler.mapRows.get(), topKBefore = compiler.topKRows.get();
        assertMatches("select v-1 g,sum(v+w) total,count(w-1) from t where k+1>=0 group by g order by total desc limit 10", true);
        assertEquals(20000, compiler.mapRows.get() - before);
        assertTrue(lastPlan, compiler.topKRows.get() > topKBefore);
        assertTrue(compiler.filterRows.get() > 0);
        assertTrue(compiler.merges.get() > 0);
        assertTrue(compiler.redistributions.get() > 0);
    }

    @Test
    public void integerExpressionOverflowNullsAndWideningMatchResolvedFunctions() throws Exception {
        engine.execute("create table arithmetic as (select cast(x as int) k,"
                + "cast(case x%5 when 0 then null when 1 then 2147483647 when 2 then -2147483647 when 3 then 2147483646 else 0 end as int) i,"
                + "case x%5 when 0 then null when 1 then 9223372036854775807L when 2 then -9223372036854775807L when 3 then 9223372036854775806L else 0L end l,"
                + "cast(x%255-128 as byte) b,cast(x%65535-32768 as short) s from long_sequence(257))", context);
        assertMatches("select k,sum(i+b),sum(i-b),sum((i+b)-b),sum(cast(i+b as long)+b),"
                + "sum(l+b),sum(l-b),count(l+1),count(i+1),sum(cast(b as short)),"
                + "sum(cast(b as int)),sum(cast(b as long)),sum(cast(s as int)),sum(cast(s as long)) "
                + "from arithmetic where i+1>=0 or l-1=null group by k", true);
        assertMatches("select k,count(i+1),sum(cast(i+1 as long)+cast(b as long)) "
                + "from arithmetic where k>=0 group by k", true);
        long before = compiler.mapRows.get();
        assertMatches("select i+1,l-1,count() from arithmetic where k>=0 group by i+1,l-1", true);
        assertEquals(514, compiler.mapRows.get() - before);
        assertTrue(compiler.filterRows.get() > 0);
        // Unsupported children decline the complete binding, before any state mutation.
        assertMatches("select k,sum(i*2+1) from arithmetic where k>=0 group by k", false);
    }

    @Test
    public void fixedCompositeKeysUseGeneratedMutationMergeAndTopK() throws Exception {
        engine.execute("create table composite as (select cast(x%11-5 as byte) b,cast(x%23-11 as short) s,"
                + "cast(case when x%7=0 then null else x%71 end as int) k,"
                + "case when x%13=0 then null else x%97 end v,x n from long_sequence(10000))", context);
        long memoryBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        for (int shardAt : new int[]{Integer.MAX_VALUE, 2}) {
            shardingThreshold = shardAt;
            for (String keys : List.of("b,s", "b,k", "s,k,b", "k,v", "v,b,s,k")) {
                long before = compiler.mapRows.get();
                assertMatches("select " + keys + ",count(),sum(n),count_distinct(v) from composite where n>=0 group by " + keys, true);
                assertEquals(keys, 20000, compiler.mapRows.get() - before);
            }
            for (int parallelAt : new int[]{Integer.MAX_VALUE, 0}) {
                topKThreshold = parallelAt;
                for (String direction : List.of("asc", "desc")) {
                    long before = compiler.topKRows.get();
                    assertMatches("select b,s,sum(n) total,count() from composite where n>=0 group by b,s order by total " + direction + " limit 73", true);
                    assertTrue(compiler.topKRows.get() > before);
                }
            }
        }
        assertTrue(compiler.merges.get() > 0);
        assertTrue(compiler.redistributions.get() > 0);
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
        assertEquals(memoryBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test
    public void compositeKeysUseOrdinaryOrderedMapGrowthAndOffsets() throws Exception {
        shardingThreshold = Integer.MAX_VALUE;
        String sql = "select k,v,count(),sum(w),count_distinct(v) from t where k>=0 group by k,v";
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            RecordCursorFactory base = factory;
            while (!(base instanceof io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory)) base = base.getBaseFactory();
            var atom = ((io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory) base).getAtom();
            assertTrue(atom.getFragment(-1).getMap() instanceof io.questdb.cairo.map.OrderedMap);
            var actual = Demo.readSql(factory, context, 5);
            assertEquals(10_000, compiler.rows.get());
            context.setQueryCodeGenerator(null);
            try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(baseline, context, 5), actual);
            }
            assertEquals(actual, Demo.readSql(factory, context, 5));
            assertEquals(20_000, compiler.rows.get());
        }
    }

    @Test
    public void columnTopsEmptyAndAllNullGroupsUseCompatibleJavaState() throws Exception {
        engine.execute("alter table t add column extra long", context);
        engine.execute("insert into t values (1,3,4,5,7),(null,null,null,0,null)", context);
        assertMatches("select k,count(),sum(extra),count(extra) from t where k!=100 group by k", true);
        assertMatches("select k+extra,count(),sum(v+extra) from t where k>=0 group by k+extra", true);
        assertTrue("Column tops must decline before changing COUNT", compiler.declines.get() > 0);
        assertMatches("select k,count(),sum(v) from t where v<0 group by k", true);
        assertMatches("select k,count(v),sum(v) from t where v=null group by k", true);
        engine.execute("alter table t alter column w type long", context);
        assertMatches("select k,count(),sum(w),count(w) from t where k>=0 group by k", true);

    }

    @Test
    public void unsupportedOverloadsAndPlannerSpecializationsStayOrdinary() throws Exception {
        assertMatches("select k,sum(v*w) from t where k>=0 group by k", false);
        assertMatches("select count() from t", false);
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, "select k,avg(v) from t where k>=0 group by k", true);
             var cursor = factory.getCursor(context)) {
            assertFalse(plan(factory).contains("compiledAggregateBatches"));
            int n = 0;
            while (cursor.hasNext()) n++;
            assertEquals(17, n);
        }
        assertEquals(0, compiler.created.get());
    }

    @Test
    public void cursorRemainsLazyReusableAndSchemaChecked() throws Exception {
        context.setQueryCodeGenerator(compiler);
        String sql = "select k,sum(v),count() from t where k>=0 group by k";
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            try (var ignored = factory.getCursor(context)) { assertEquals(0, compiler.calls.get()); }
            var first = Demo.readSql(factory, context, 3);
            assertEquals(first, Demo.readSql(factory, context, 3));
            engine.execute("alter table t add column newer long", context);
            assertThrows(TableReferenceOutOfDateException.class, () -> factory.getCursor(context));
        }
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
    }

    @Test
    public void preparationFailureClosesAdoptedSlotsAndCodeEvenWhenCloseFails() throws Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        compiler.failSlot = 3;
        compiler.failClose = true;
        context.setQueryCodeGenerator(compiler);
        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> Demo.compileSql(engine, context, "select k,sum(v) from t where k>=0 group by k", true));
        assertEquals("slot preparation", failure.getMessage());
        assertTrue(failure.getSuppressed().length > 0);
        assertEquals(1, compiler.closed.get());
        assertEquals(2, compiler.closedSlots.get());
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void executionFailureIsDrainedAndNotReplayed() throws Exception {
        context.setQueryCodeGenerator(compiler);
        compiler.failUpdate = true;
        try (var factory = Demo.compileSql(engine, context, "select k,sum(v) from t where k>=0 group by k", true)) {
            assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 2));
            compiler.failUpdate = false;
            var actual = Demo.readSql(factory, context, 2);
            context.setQueryCodeGenerator(null);
            try (var baseline = Demo.compileSql(engine, context, "select k,sum(v) from t where k>=0 group by k", true)) {
                assertEquals(Demo.readSql(baseline, context, 2), actual);
            }
        }
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void distinctCardinalityStillTriggersExistingShardingAndJavaMerge() throws Exception {
        shardingThreshold = 20; // 17 groups: only function cardinality can trigger sharding.
        String sql = "select k,count_distinct(v),sum(v) from t where k>=0 group by k";
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            RecordCursorFactory base = factory;
            while (!(base instanceof io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory)) base = base.getBaseFactory();
            var atom = ((io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory) base).getAtom();
            try (var cursor = factory.getCursor(context)) {
                assertTrue(cursor.hasNext());
                assertTrue("Native DISTINCT cardinality must reach the existing sharding decision", atom.isSharded());
            }
            assertTrue(compiler.rows.get() > 0);
            context.setQueryCodeGenerator(null);
            try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                long nativeRowsBefore = compiler.rows.get();
                assertEquals(Demo.readSql(baseline, context, 3), Demo.readSql(factory, context, 3));
                assertEquals("Reopened sharded execution must continue using native updates", 10_000,
                        compiler.rows.get() - nativeRowsBefore);
                compiler.failUpdate = true;
                assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 3));
                compiler.failUpdate = false;
                assertEquals(Demo.readSql(baseline, context, 3), Demo.readSql(factory, context, 3));
            }
        }
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
    }

    @Test
    public void handwrittenCountSumMatchesSqlThroughExistingLifecycle() throws Exception {
        Assume.assumeTrue(backend instanceof LlvmBackend);
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        compiler = new TrackingCompiler(new ResolvedQueryCompiler(backend, temporary.newFolder().toPath(), true));
        engine.execute("create table ref as (select cast(case when x%19=0 then null else x%101-50 end as int) i,"
                + "case when x%23=0 then null else x%103-51 end k,"
                + "case when x%13=0 then null when x%29=0 then 9223372036854775806 else x*3 end v,"
                + "cast(case when x%17=0 then null else x end as int) w,cast(x%31-15 as short) s "
                + "from long_sequence(10001))", context);
        engine.execute("insert into ref values (123456,123456,null,null,1)", context);
        for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
            shardingThreshold = threshold;
            for (String key : List.of("i", "k")) {
                long rowsBefore = compiler.rows.get(), mapsBefore = compiler.mapRows.get();
                assertMatches("select " + key + ",count(),count(v),sum(v),sum(w),sum(s) from ref where s%3=1 group by " + key, true);
                assertTrue(compiler.rows.get() > rowsBefore);
                if (threshold == 2) assertTrue(compiler.mapRows.get() > mapsBefore);
            }
        }
        context.setQueryCodeGenerator(compiler);
        assertThrows(IllegalArgumentException.class,
                () -> Demo.compileSql(engine, context, "select k,count() from ref where s>0 group by k", true));
        assertEquals(compiler.created.get(), compiler.closed.get());
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test
    public void generatedMapProbesUseExistingLayoutsAndHashSemantics() throws Exception {
        // Exercise every supported aggregate layout on both unordered maps. Ordinary
        // planner thresholds are covered by the other SQL tests and unchanged benchmarks.
        mapEntrySize = 64;

        engine.execute("create table keys as (select cast(case when x%19=0 then null else x%101-50 end as int) i,"
                + "case when x%23=0 then null else x%103-51 end k,x v,cast(x as int) w,"
                + "cast(x as short) s from long_sequence(10000))", context);
        for (int threshold : new int[]{2, Integer.MAX_VALUE}) {
            shardingThreshold = threshold;
            for (String sql : List.of(
                    "select i,count(),sum(v) from keys where v>0 group by i",
                    "select k,count(),sum(v) from keys where v>0 group by k",
                    "select k,count(),count(v),sum(v),sum(w),sum(s) from keys where v>0 group by k")) {
                long before = compiler.mapRows.get();
                assertMatches(sql, true);
                assertTrue("Native map coverage [threshold=" + threshold + ",sql=" + sql + ",rows=" + (compiler.mapRows.get() - before) + "]", compiler.mapRows.get() - before >= 10_000);
            }
            // Scattered row ids and short batch tails; DISTINCT can grow between prepared rows.
            long before = compiler.mapRows.get();
            assertMatches("select k,count(),sum(v),count_distinct(v) from keys where v%3=1 group by k", true);
            assertTrue("Filtered rows must use the prepared candidate path", compiler.mapRows.get() - before >= 3334);
        }
        assertTrue("Native merge must be adopted by the existing executor", compiler.merges.get() > 0);
        assertTrue("Native redistribution must be adopted", compiler.redistributions.get() > 0);
    }

    @Test
    public void lazyParquetConversionAndLateMaterializationKeepTypedJavaAccess() throws Exception {
        engine.execute("create table p as (select cast(x%17 as int) k,cast(x as string) v,"
                + "timestamp_sequence('2024-01-01',3600000000L) ts from long_sequence(48)) timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table p convert partition to parquet list '2024-01-01'", context);
        engine.execute("alter table p alter column v type long", context);
        long rowsBefore = compiler.rows.get();
        assertMatches("select k,count(),sum(v),count(v) from p where k>=0 group by k", true);
        // assertMatches executes twice: the native partition contributes 48 rows total;
        // the lazy Parquet partition must use ordinary converting accessors.
        assertTrue(compiler.rows.get() - rowsBefore < 96);
        assertMatches("select k,count(),sum(v) from p where k<3 group by k", true);
    }

    @Test
    public void nativeDistinctUsesExistingQueryMemoryLimitAndReopensAfterBreach() throws Exception {
        String sql = "select k,count_distinct(v) from t where k>=0 group by k";
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            for (long limit = 4096; limit <= (1 << 20) && compiler.nativeMemoryFailures.get() == 0; limit *= 2) {
                queryMemoryLimit = limit;
                try {
                    Demo.readSql(factory, context, 2);
                } catch (CairoException failure) {
                    assertTrue("Memory breach must retain its error category", failure.isOutOfMemory());
                }
            }
            assertTrue("Exercise a breach from the native allocator callback", compiler.nativeMemoryFailures.get() > 0);
            queryMemoryLimit = 0;
            var actual = Demo.readSql(factory, context, 2);
            context.setQueryCodeGenerator(null);
            try (var baseline = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(baseline, context, 2), actual);
            }
        }
        assertEquals(compiler.slots.get(), compiler.closedSlots.get());
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void compilerFailurePropagatesBeforeReadingRows() throws Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        Backend unavailable = new Backend() {
            @Override public String name() { return "unavailable"; }
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path path) throws java.io.IOException {
                throw new java.io.IOException("compiler unavailable");
            }
        };
        context.setQueryCodeGenerator(new ResolvedQueryCompiler(unavailable, temporary.newFolder().toPath()));
        CairoException failure = assertThrows(CairoException.class,
                () -> Demo.compileSql(engine, context, "select k,sum(v) from t where k>=0 group by k", true));
        assertTrue(failure.getFlyweightMessage().toString().contains("compiler unavailable"));
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(0, engine.getBusyReaderCount());
    }

    private void assertMatches(String sql, boolean compiled) throws Exception {
        context.setQueryCodeGenerator(null);
        try (var baseline = Demo.compileSql(engine, context, sql, true)) {
            var expected = Demo.readSql(baseline, context, baseline.getMetadata().getColumnCount());
            context.setQueryCodeGenerator(compiler);
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                lastPlan = plan(candidate);
                assertEquals(sql + " / " + plan(candidate), compiled, plan(candidate).contains("compiledAggregateBatches"));
                assertEquals(sql, expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
                assertEquals(sql, expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
            }
        }
    }

    private String plan(RecordCursorFactory factory) {
        var sink = new TextPlanSink();
        sink.of(factory, context);
        var result = new StringBuilder();
        for (int i = 1; i <= sink.getLineCount(); i++) result.append(sink.getLine(i)).append('\n');
        return result.toString();
    }

    private static final class TrackingCompiler implements QueryCodeGenerator {
        final AtomicLong parallelUnionTasks = new AtomicLong();
        final AtomicLong sharedFinishes = new AtomicLong();
        final AtomicLong parallelZeroTasks = new AtomicLong();
        volatile boolean failUnion;
        private final QueryCodeGenerator delegate;
        private final AtomicLong created = new AtomicLong(), closed = new AtomicLong();
        private final AtomicLong slots = new AtomicLong(), closedSlots = new AtomicLong();
        private final AtomicLong merges = new AtomicLong(), redistributions = new AtomicLong();
        private final AtomicLong filterRows = new AtomicLong(), filterSelected = new AtomicLong();
        private final AtomicLong sortRows = new AtomicLong(), sortedSelections = new AtomicLong();
        private final AtomicLong topKRows = new AtomicLong(), topKMerges = new AtomicLong(), topKFallbacks = new AtomicLong();
        private volatile boolean declineTopK;
        private volatile boolean failTopK;
        private final AtomicLong mapRows = new AtomicLong();
        private final AtomicLong rows = new AtomicLong(), calls = new AtomicLong(), declines = new AtomicLong(), nativeMemoryFailures = new AtomicLong();
        private int failSlot;
        private boolean failClose;
        private final java.util.Map<Updater, Updater> delegates = new java.util.IdentityHashMap<>();
        private volatile boolean failUpdate;

        private TrackingCompiler(QueryCodeGenerator delegate) { this.delegate = delegate; }

        @Override
        public CompiledProjection compileProjection(ColumnTypes inputs, int offset, ObjList<Function> functions) {
            return delegate.compileProjection(inputs, offset, functions);
        }

        @Override
        public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions) {
            return track(delegate.compile(inputs, values, functions));
        }

        @Override
        public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                                int keyColumn, int keyType, io.questdb.cairo.map.Map layout) {
            return track(delegate.compile(inputs, values, functions, keyColumn, keyType, layout));
        }

        @Override
        public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                                ColumnTypes keys, io.questdb.cairo.ListColumnFilter keyColumns, io.questdb.cairo.map.Map layout) {
            return track(delegate.compile(inputs, values, functions, keys, keyColumns, layout));
        }

        @Override
        public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions,
                                ColumnTypes keys, io.questdb.cairo.ListColumnFilter keyColumns,
                                ObjList<Function> keyFunctions, io.questdb.cairo.map.Map layout) {
            return track(delegate.compile(inputs, values, functions, keys, keyColumns, keyFunctions, layout));
        }

        private GroupByKernel track(GroupByKernel code) {
            if (code == null) return null;
            created.incrementAndGet();
            return new GroupByKernel() {
                @Override public CharSequence getName() { return code.getName(); }
                @Override public void prepareLongTopK(Function function) { code.prepareLongTopK(function); }
                @Override public void prepareLongTopK(Function function, boolean ascending, int limit) { code.prepareLongTopK(function, ascending, limit); }
                @Override public void prepareFilter(ColumnTypes inputs, Function function) { code.prepareFilter(inputs, function); }
                @Override public boolean hasCompiledFilter() { return code.hasCompiledFilter(); }
                @Override public ObjList<Function> outputFunctions(ObjList<Function> functions) { return code.outputFunctions(functions); }
                @Override public boolean prepareRowLimit() { return code.prepareRowLimit(); }
                @Override public boolean prepareSortedRowLimit() { return code.prepareSortedRowLimit(); }
                @Override public boolean prepareLongSort(Function function) { return code.prepareLongSort(function); }
                @Override public boolean supportsLongTopK(Function function) { return code.supportsLongTopK(function); }
                @Override public boolean needsWorkerAllocators() { return code.needsWorkerAllocators(); }
                @Override public void close() {
                    closed.incrementAndGet();
                    code.close();
                    if (failClose) throw new IllegalStateException("code close");
                }
                @Override public Updater newUpdater(GroupByAllocator allocator) {
                    if (failSlot > 0 && slots.get() + 1 == failSlot) throw new IllegalStateException("slot preparation");
                    var update = code.newUpdater(allocator);
                    slots.incrementAndGet();
                    Updater wrapped = new Updater() {
                        @Override public boolean initSharedMaps(java.util.function.IntFunction<io.questdb.cairo.map.Map> maps) { return update.initSharedMaps(maps); }
                        @Override public void finishSharedMap(int shard, Runnable checkpoint) {
                            update.finishSharedMap(shard, checkpoint);
                            sharedFinishes.incrementAndGet();
                        }
                        @Override public int localMapCapacity() { return update.localMapCapacity(); }
                        @Override public boolean hasPartitions() { return update.hasPartitions(); }
                        @Override public void clear() { update.clear(); }
                        @Override public void partition(io.questdb.cairo.map.Map source, Runnable checkpoint) {
                            update.partition(source, checkpoint); redistributions.incrementAndGet();
                        }
                        @Override public void finalizeMap(io.questdb.cairo.map.Map map, Runnable checkpoint) { update.finalizeMap(map, checkpoint); }
                        @Override public boolean beginParallelMerge() { return update.beginParallelMerge(); }
                        @Override public void endParallelMerge(boolean hasDeferredStates) { update.endParallelMerge(hasDeferredStates); }
                        @Override public int getPendingMergeCount() { return update.getPendingMergeCount(); }
                        @Override public boolean prepareMergeTasks(ObjList<MergeTask> tasks, int parallelism, Runnable checkpoint) { return update.prepareMergeTasks(tasks, parallelism, checkpoint); }
                        @Override public void runMergeTask(MergeTask task, Runnable checkpoint) {
                            if (failUnion && task.phase() == 2) throw new IllegalStateException("injected union failure");
                            update.runMergeTask(task, checkpoint);
                            if (task.phase() == 2) parallelUnionTasks.incrementAndGet();
                            if (task.phase() == 1) parallelZeroTasks.incrementAndGet();
                        }
                        @Override public void mergePartition(io.questdb.cairo.map.Map destination, Updater source, int partition, Runnable checkpoint) {
                            update.mergePartition(destination, delegates.get(source), partition, checkpoint); merges.incrementAndGet();
                        }
                        @Override public boolean selectRows(io.questdb.cairo.map.Map owner, ObjList<io.questdb.cairo.map.Map> shards,
                                                            io.questdb.std.DirectLongList rows, long skip, long take, Runnable checkpoint) {
                            return update.selectRows(owner, shards, rows, skip, take, checkpoint);
                        }
                        @Override public boolean filter(PageFrameMemoryRecord record, long count, io.questdb.std.DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
                            boolean accepted = update.filter(record, count, rows, breaker);
                            if (accepted) { filterRows.addAndGet(count); filterSelected.addAndGet(rows.size()); }
                            return accepted;
                        }
                        @Override public boolean selectSortedRows(io.questdb.std.DirectLongLongSortedList sorted,
                                                                   io.questdb.std.DirectLongList rows, long skip, long take, Runnable checkpoint) {
                            boolean selected = update.selectSortedRows(sorted, rows, skip, take, checkpoint);
                            if (selected) sortedSelections.addAndGet(rows.size());
                            return selected;
                        }
                        @Override public boolean sortLong(io.questdb.cairo.map.Map owner, ObjList<io.questdb.cairo.map.Map> shards,
                                                          io.questdb.std.DirectLongList entries, io.questdb.std.DirectLongList scratch,
                                                          Function function, boolean ascending, long maxEntries, Runnable checkpoint) {
                            boolean accepted = update.sortLong(owner, shards, entries, scratch, function, ascending, maxEntries, checkpoint);
                            if (accepted) sortRows.addAndGet(entries.size() / 2);
                            return accepted;
                        }
                        @Override public boolean longTopK(io.questdb.cairo.map.Map map, io.questdb.std.DirectLongLongSortedList list, Function function, Runnable checkpoint) {
                            if (failTopK) throw CairoException.queryCancelled();
                            if (declineTopK) { topKFallbacks.incrementAndGet(); return false; }
                            boolean accepted = update.longTopK(map, list, function, checkpoint);
                            if (accepted) topKRows.addAndGet(map.size());
                            return accepted;
                        }
                        @Override public boolean longTopK(ObjList<io.questdb.cairo.map.Map> maps, io.questdb.std.DirectLongLongSortedList list, Function function, Runnable checkpoint) {
                            boolean accepted = update.longTopK(maps, list, function, checkpoint);
                            if (accepted) for (int i = 0; i < maps.size(); i++) topKRows.addAndGet(maps.getQuick(i).size());
                            return accepted;
                        }
                        @Override public boolean mergeLongTopK(io.questdb.std.DirectLongLongSortedList destination, io.questdb.std.DirectLongLongSortedList source, Runnable checkpoint) {
                            boolean accepted = update.mergeLongTopK(destination, source, checkpoint);
                            if (accepted) topKMerges.incrementAndGet();
                            return accepted;
                        }
                        @Override public void close() { closedSlots.incrementAndGet(); update.close(); }
                        @Override public long getCardinalityStat() { return update.getCardinalityStat(); }
                        @Override public void resetStats() { update.resetStats(); }
                        @Override public long getNativeMapRows() { return update.getNativeMapRows(); }
                        @Override public boolean merge(io.questdb.cairo.map.Map destination, io.questdb.cairo.map.Map source, Runnable checkpoint) {
                            boolean accepted = update.merge(destination, source, checkpoint);
                            if (accepted && source.size() > 0) merges.incrementAndGet();
                            return accepted;
                        }
                        @Override public boolean shard(io.questdb.cairo.map.Map source, ObjList<io.questdb.cairo.map.Map> shards, Runnable checkpoint) {
                            boolean accepted = update.shard(source, shards, checkpoint);
                            if (accepted) redistributions.incrementAndGet();
                            return accepted;
                        }
                        @Override public boolean updateMap(PageFrameMemoryRecord record, long count, long rowIds,
                                io.questdb.cairo.map.Map map, int batchSize, SqlExecutionCircuitBreaker breaker) {
                            boolean accepted;
                            try {
                                accepted = update.updateMap(record, count, rowIds, map, batchSize, breaker);
                            } catch (CairoException failure) {
                                if (failure.isOutOfMemory()) nativeMemoryFailures.incrementAndGet();
                                throw failure;
                            }
                            if (accepted) {
                                calls.incrementAndGet();
                                rows.addAndGet(count);
                                mapRows.addAndGet(count);
                                if (failUpdate) throw CairoException.nonCritical().put("injected after update");
                            }
                            return accepted;
                        }
                        @Override public boolean updateSharded(PageFrameMemoryRecord record, long count, long rowIds,
                                ObjList<io.questdb.cairo.map.Map> shards, io.questdb.cairo.RecordSink sink,
                                io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater functions, SqlExecutionCircuitBreaker breaker) {
                            boolean accepted = update.updateSharded(record, count, rowIds, shards, sink, functions, breaker);
                            if (accepted) {
                                calls.incrementAndGet();
                                rows.addAndGet(count);
                                mapRows.addAndGet(count);
                                if (failUpdate) throw CairoException.nonCritical().put("injected after update");
                            }
                            return accepted;
                        }
                        @Override public boolean update(PageFrameMemoryRecord record, long base, long batch, long count,
                                                        SqlExecutionCircuitBreaker breaker) {
                            calls.incrementAndGet();
                            boolean accepted;
                            try {
                                accepted = update.update(record, base, batch, count, breaker);
                            } catch (CairoException failure) {
                                if (failure.isOutOfMemory()) nativeMemoryFailures.incrementAndGet();
                                throw failure;
                            }
                            if (accepted) rows.addAndGet(count); else declines.incrementAndGet();
                            if (failUpdate) throw CairoException.nonCritical().put("injected after update");
                            return accepted;
                        }
                    };
                    delegates.put(wrapped, update);
                    return wrapped;
                }
            };
        }
    }
}
