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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.mp.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ResolvedSqlJoinTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private final WorkerPoolMode mode;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long buildThreshold = Long.MAX_VALUE;
    private long payloadCopyLimit = 128L << 20;
    private int shardThreshold = Integer.MAX_VALUE;

    public ResolvedSqlJoinTest(String name, Backend backend, WorkerPoolMode mode) {
        this.backend = backend;
        this.mode = mode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm-legacy", new LlvmBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"llvm-fiber", new LlvmBackend(), WorkerPoolMode.FIBER_HOST},
                new Object[]{"asmjit", new AsmJitBackend(), WorkerPoolMode.LEGACY},
                new Object[]{"c", new NativeBackend(), WorkerPoolMode.LEGACY});
    }

    @Before
    public void setUp() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 256; }
            @Override public int getSqlPageFrameMinRows() { return 64; }
            @Override public long getSqlParallelHashJoinGroupByBuildParallelMinRows() { return buildThreshold; }
            @Override public long getSqlParallelHashJoinGroupByBuildRowsPerPartition() { return 64; }
            @Override public long getSqlParallelHashJoinGroupByPayloadCopyMaxSize() { return payloadCopyLimit; }
            @Override public int getGroupByShardingThreshold() { return shardThreshold; }
            @Override public int getSqlSmallMapKeyCapacity() { return 4; }
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
        engine.execute("create table p as (select cast(case when x%31=0 then null else x%131-65 end as int) k,"
                + "case when x%5=0 then null else x*3 end v,cast(case when x%7=0 then null else x end as int) w,"
                + "cast(x%17 as short) s from long_sequence(10000))", context);
        engine.execute("create table d as (select cast(case when x%31=0 then null else x%131-65 end as int) k,"
                + "x v from long_sequence(1000))", context);
    }

    @After
    public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
    }

    @Test
    public void serialAndPartitionedBuildsUseNativeUpdatesAndOrdinaryCursor() throws Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        String aggregates = "select count(),count(p.v),sum(p.v),sum(p.w),sum(p.s) from p join d on p.k=d.k";
        for (long threshold : new long[]{Long.MAX_VALUE, 1}) {
            buildThreshold = threshold;
            matches(aggregates, true);
            matches(aggregates + " where p.w>42 and d.v%3!=0", true);
        }
        engine.execute("truncate table d", context);
        engine.execute("insert into d select cast(x-65 as int),x from long_sequence(131)", context);
        buildThreshold = Long.MAX_VALUE;
        matches(aggregates, true);
        buildThreshold = 1;
        // Keep the existing physical plan, but let the owner drain every round and probe task.
        pool.halt();
        matches(aggregates, true);
        matches(aggregates + " where p.w<0", true);
        engine.execute("truncate table d", context);
        engine.execute("insert into d select cast(x+10000 as int),x from long_sequence(30)", context);
        matches(aggregates, true);
        engine.execute("truncate table d", context);
        matches(aggregates, true);
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void buildPayloadsUseExistingDirectAndCopiedStorage() throws Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        for (long limit : new long[]{0, 128L << 20}) {
            payloadCopyLimit = limit;
            for (long threshold : new long[]{Long.MAX_VALUE, 1}) {
                buildThreshold = threshold;
                matches("select count(),count(d.v),sum(d.v),sum(p.w) from p join d on p.k=d.k", true);
            }
        }
        engine.execute("alter table d add column late long", context);
        engine.execute("insert into d select cast(x%131-65 as int),x,x*7 from long_sequence(50)", context);
        for (long limit : new long[]{0, 128L << 20}) {
            payloadCopyLimit = limit;
            matches("select count(),count(d.late),sum(d.late),sum(p.v) from p join d on p.k=d.k", true);
        }
        engine.execute("truncate table d", context);
        engine.execute("insert into d select cast(x-65 as int),x,x*7 from long_sequence(131)", context);
        matches("select count(),sum(d.v),sum(p.v) from p join d on p.k=d.k", true);
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void resolvedPostJoinPredicatesUseJoinedValuesAndNullSemantics() throws Exception {
        engine.execute("update d set v=null where k%5=0", context);
        for (long limit : new long[]{0, 128L << 20}) {
            payloadCopyLimit = limit;
            buildThreshold = limit == 0 ? Long.MAX_VALUE : 1;
            for (String comparison : new String[]{"<", ">=", ">", "<="}) {
                matches("select count(),sum(d.v),sum(p.v) from p join d on p.k=d.k where p.v" + comparison + "d.v", true);
            }
            matches("select count(),sum(d.v) from p join d on p.k=d.k where p.w>d.v", true);
            matches("select count(),sum(d.v) from p join d on p.k=d.k where p.w>=d.k", true);
        }
        engine.execute("update d set v=-1000000", context);
        matches("select count(),sum(p.v) from p join d on p.k=d.k where p.v<d.v", true);
    }

    @Test
    public void joinedValuesFeedGrowingAndShardedMapsWithoutReplayingMatches() throws Exception {
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        for (int threshold : new int[]{Integer.MAX_VALUE, 4}) {
            shardThreshold = threshold;
            buildThreshold = threshold == 4 ? 1 : Long.MAX_VALUE;
            matches("select p.k,count(),sum(d.v),sum(p.v) from p join d on p.k=d.k group by p.k", true, 1);
            matches("select d.v,count(),sum(p.v) from p join d on p.k=d.k group by d.v", true, 1);
            matches("select p.v,count(),sum(d.v) from p join d on p.k=d.k group by p.v", true, 1);
            matches("select d.v,count(),sum(p.w) from p join d on p.k=d.k where p.v>d.v group by d.v", true, 1);
        }
        matches("select p.k,count_distinct(d.v) from p join d on p.k=d.k group by p.k", false, 1);
        pool.halt();
        matches("select d.v,count(),sum(p.v) from p join d on p.k=d.k group by d.v", true, 1);
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void unsupportedShapesAndColumnTopsKeepJavaSemantics() throws Exception {
        matches("select count() from p join d on p.k=d.k", false);
        matches("select sum(p.v),count() from p join d on p.k=d.k where p.v*d.v>0", false);
        matches("select sum(p.v+d.v),count() from p join d on p.k=d.k where p.v>d.v", true);
        matches("select sum(p.v*d.v),count() from p join d on p.k=d.k where p.v>d.v", false);
        matches("select p.k,d.v,sum(p.v) from p join d on p.k=d.k group by p.k,d.v", false, 2);
        matches("select sum(p.v),count() from p left join d on p.k=d.k", false);
        engine.execute("alter table p add column late long", context);
        engine.execute("insert into p select cast(x%131-65 as int),x,cast(x as int),cast(x as short),x*7 from long_sequence(500)", context);
        matches("select count(),sum(p.late),sum(p.v) from p join d on p.k=d.k", true);
    }

    @Test
    public void nativeFailureDrainsAndFactoryCanReopen() throws Exception {
        context.setUseSimpleCircuitBreaker(true);
        AtomicBoolean fail = new AtomicBoolean(true);
        AtomicBoolean cancel = new AtomicBoolean(false);
        AtomicLong polls = new AtomicLong();
        QueryCodeGenerator interrupted = new QueryCodeGenerator() {
            @Override public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions) { return null; }
            @Override public CompiledJoin compileJoin(ColumnTypes inputs, int probeColumns, ColumnTypes values,
                    ObjList<GroupByFunction> functions, int keyColumn, MapValue value, Function predicate,
                    int groupKeyColumn, io.questdb.cairo.map.Map layout) {
                CompiledJoin code = compiler.compileJoin(inputs, probeColumns, values, functions, keyColumn, value, predicate, groupKeyColumn, layout);
                if (code == null) return null;
                return new CompiledJoin() {
                    @Override public CharSequence getName() { return code.getName(); }
                    @Override public void close() { code.close(); }
                    @Override public CompiledJoin.Updater newUpdater(Runnable checkpoint) {
                        return code.newUpdater(() -> {
                            checkpoint.run();
                            if (fail.get() && polls.incrementAndGet() >= 3)
                                throw CairoException.nonCritical().put("native join checkpoint failure");
                            if (cancel.get() && polls.incrementAndGet() >= 3 && cancel.compareAndSet(true, false)) {
                                engine.getQueryRegistry().cancel(context.getMemoryTracker().getQueryId(), context);
                                checkpoint.run();
                            }
                        });
                    }
                };
            }
        };
        long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        for (int keyCount = 0; keyCount <= 1; keyCount++) {
            final int keys = keyCount;
            shardThreshold = keyCount == 0 ? Integer.MAX_VALUE : 4;
            buildThreshold = keyCount == 0 ? Long.MAX_VALUE : 1;
            String sql = keyCount == 0 ? "select count(),sum(p.v) from p join d on p.k=d.k"
                    : "select d.v,count(),sum(p.v) from p join d on p.k=d.k group by d.v";
            fail.set(true);
            polls.set(0);
            context.setQueryCodeGenerator(null);
            List<CompiledGroupBy.ResultRow> expected;
            try (var baseline = Demo.compileSql(engine, context, sql, true)) { expected = Demo.readSql(baseline, context, keys); }
            context.setQueryCodeGenerator(interrupted);
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                assertTrue(plan(candidate).contains("compiledJoinPipeline"));
                CairoException failure = assertThrows(CairoException.class, () -> Demo.readSql(candidate, context, keys));
                assertTrue(failure.getFlyweightMessage().toString(), failure.getFlyweightMessage().toString().contains("native join checkpoint failure"));
                fail.set(false);
                assertEquals(expected, Demo.readSql(candidate, context, keys));
                polls.set(0);
                cancel.set(true);
                CairoException cancelled = assertThrows(CairoException.class, () -> Demo.readSql(candidate, context, keys));
                assertTrue(cancelled.isCancellation());
                assertEquals(expected, Demo.readSql(candidate, context, keys));
            }
        }
        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(0, engine.getBusyReaderCount());
    }

    private void matches(String sql, boolean compiled) throws Exception {
        matches(sql, compiled, 0);
    }

    private void matches(String sql, boolean compiled, int keyCount) throws Exception {
        context.setQueryCodeGenerator(null);
        try (var baseline = Demo.compileSql(engine, context, sql, true)) {
            var expected = Demo.readSql(baseline, context, keyCount);
            context.setQueryCodeGenerator(compiler);
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(sql + " / " + plan(candidate), compiled, plan(candidate).contains("compiledJoinPipeline"));
                // Opening and closing reads no rows and must not run the join build.
                try (var ignored = candidate.getCursor(context)) { }
                assertEquals(sql, expected, Demo.readSql(candidate, context, keyCount));
                assertEquals(sql, expected, Demo.readSql(candidate, context, keyCount));
                if (compiled && !expected.isEmpty() && expected.getFirst().values().getFirst() > 0) {
                    RecordCursorFactory base = candidate;
                    while (!(base instanceof io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory)) base = base.getBaseFactory();
                    var atom = ((io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory) base).getAtom();
                    assertTrue("Native join coverage", atom.getCompiledJoinNativeRows() > 0);
                }
                if (keyCount == 0) try (var cursor = candidate.getCursor(context)) {
                    assertTrue(cursor.hasNext());
                    long first = cursor.getRecord().getLong(0);
                    cursor.toTop();
                    assertTrue(cursor.hasNext());
                    assertEquals(first, cursor.getRecord().getLong(0));
                }
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
}
