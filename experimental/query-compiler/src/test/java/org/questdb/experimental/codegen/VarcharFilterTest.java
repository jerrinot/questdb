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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class VarcharFilterTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

    public VarcharFilterTest(String name, Backend backend) { this.backend = backend; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()}, new Object[]{"c", new NativeBackend()});
    }

    @Before
    public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public int getGroupByShardingThreshold() { return 2; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        context.with(new BindVariableServiceImpl(config));
        context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
        engine.execute("create table strings as (select x id,cast(x%7 as int) k,"
                + "cast(case when x%11=0 then null when x%13=0 then '' when x%17=0 then 'abcdef' when x%19=0 then 'abcdefg' when x%3=0 then 'é' "
                + "when x%5=0 then 'prefix-abcdef-suffix' else 'prefix-longer-shared-0123456789-☃-'||x%31 end as varchar) v,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(1003)) timestamp(ts) partition by day bypass wal", context);
    }

    @After
    public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test
    public void horizonProjectionPreparesFilterLiteralsOnEveryExecution() throws Exception {
        String sql = "select h.offset,t.ts,p.id from strings t horizon join strings p list (0s,2s,4s) as h "
                + "where t.v!='prefix-abcdef-suffix' order by h.offset,t.ts";
        try (var factory = candidate(sql)) {
            var physical = find(factory, io.questdb.griffin.engine.table.AsyncHorizonJoinProjectionRecordCursorFactory.class);
            var filter = ((io.questdb.griffin.engine.table.AsyncHorizonJoinProjectionAtom) physical.getAtom()).getCompiledFilter();
            assertNotNull(filter);
            assertEquals(backend.name(), filter.getBackendName());
            for (int repeat = 0; repeat < 2; repeat++) {
                try (var cursor = physical.getCursor(context)) {
                    assertTrue(cursor.hasNext());
                    cursor.toTop();
                    assertTrue(cursor.hasNext());
                }
                check(factory, sql);
            }
            assertTrue(filter.getNativeInputRows() > 0);
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void windowJoinPreparesLiteralAndSymbolPredicatesOnEveryExecution() throws Exception {
        engine.execute("create table window_strings as (select id,v,cast(v as symbol) s,ts from strings) timestamp(ts)", context);
        for (String predicate : List.of("t.v!='prefix-abcdef-suffix'", "t.s like 'prefix-%'")) {
            String sql = "select t.ts,count(p.id),min(p.id),max(p.id) from window_strings t window join strings p "
                    + "range between 2 second preceding and 2 second following where " + predicate + " order by t.ts";
            try (var factory = candidate(sql)) {
                var physical = find(factory, io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory.class);
                var filter = physical.getAtom().getCompiledMasterFilter();
                assertNotNull(filter);
                assertEquals(backend.name(), filter.getBackendName());
                for (int repeat = 0; repeat < 2; repeat++) {
                    try (var cursor = physical.getCursor(context)) {
                        assertTrue(cursor.hasNext());
                        cursor.toTop();
                        assertTrue(cursor.hasNext());
                    }
                    check(factory, sql);
                }
                assertTrue(filter.getNativeInputRows() > 0);
            }
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test
    public void literalPredicatesKeepResolvedSemanticsAndReachConsumers() throws Exception {
        for (String predicate : List.of("v like 'prefix-%'", "v not like 'prefix-%'", "v like '%suffix'",
                "v not like '%suffix'", "v='é'", "v!='é'", "v='prefix-abcdef-suffix'", "v=''", "v!=''",
                "starts_with(v,''::varchar)", "not starts_with(v,''::varchar)", "v like 'abcdef%'", "v='abcdefg'", "starts_with(v,'é'::varchar)", "v like 'missing-long-prefix%'")) {
            String sql = "select count() from strings where " + predicate;
            try (var factory = candidate(sql)) {
                check(factory, sql); check(factory, sql);
                var sink = new io.questdb.griffin.TextPlanSink(); sink.of(factory, context);
                StringBuilder plan = new StringBuilder(sql);
                for (int i = 1; i <= sink.getLineCount(); i++) plan.append('\n').append(sink.getLine(i));
                assertTrue(plan.toString(), plan.toString().contains("compiledFilter"));
                assertTrue(find(factory, AsyncJitFilteredRecordCursorFactory.class).getCompiledCountOnlyFilter().getNativeInputRows() > 0);
            }
        }
        for (String sql : List.of(
                "select sum(id),count() from strings where v like 'prefix-%' and id>17",
                "select k,count(),sum(id) from strings where v like '%suffix' group by k",
                "select id,v from strings where v!='é' order by v limit 7",
                "select id from strings where v like 'prefix-%' order by ts limit 7",
                "select count() from strings where starts_with(v,null)",
                "select count() from strings where not starts_with(v,null)",
                "select count() from strings where starts_with(v,null::varchar)",
                "select count() from strings where not starts_with(v,null::varchar)",
                "select count() from strings where v ilike 'PREFIX-%'",
                "select count() from strings where starts_with(v,'')")) {
            try (var factory = candidate(sql)) { check(factory, sql); check(factory, sql); }
        }
        context.getBindVariableService().setStr(0, "prefix-%");
        String sql = "select count() from strings where v like $1";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            context.getBindVariableService().setStr(0, "%suffix");
            check(factory, sql);
        }
    }

    @Test
    public void columnTopsParquetAndLongConstantsKeepBoundedComparisons() throws Exception {
        engine.execute("alter table strings add column late varchar", context);
        engine.execute("insert into strings values (1004,1,'é','2024-01-02','prefix-tail'),"
                + "(1005,2,'prefix-abcdef-suffix','2024-01-02T00:00:01',''),"
                + "(1006,3,null,'2024-01-02T00:00:02',null)", context);
        for (boolean parquet : new boolean[]{false,true}) {
            if (parquet) engine.execute("alter table strings convert partition to parquet list '2024-01-01','2024-01-02'", context);
            for (String predicate : List.of("late like 'prefix-%'", "late not like '%tail'", "late=''", "v like '%suffix'", "v='é'", "v like 'abcdef%'", "v='abcdefg'")) {
                String sql = "select count() from strings where " + predicate;
                try (var factory = candidate(sql)) { check(factory, sql); }
            }
        }
        // Constant length affects data, not the generated comparison-loop body.
        String longValue = "αβ-shared-".repeat(8192);
        engine.execute("create table long_strings as (select cast(case when x=1 then null when x=2 then '' "
                + "when x=3 then '" + longValue + "' when x=4 then '" + longValue + "x' else 'x" + longValue
                + "' end as varchar) v from long_sequence(5))", context);
        for (String predicate : List.of("v='" + longValue + "'", "v like '" + longValue + "%'", "v like '%" + longValue + "'")) {
            String sql = "select count() from long_strings where " + predicate;
            try (var factory = candidate(sql)) { check(factory, sql); check(factory, sql); }
        }
    }

    @Test
    public void literalBudgetFailureEarlyCloseAndReopenReleaseSharedBytes() throws Exception {
        String sql = "select id from strings where v like 'prefix-longer%'";
        try (var factory = candidate(sql);
             var tracker = new io.questdb.std.MemoryTracker() {
                 @Override public long getQueryId() { return -1; }
                 @Override public io.questdb.std.MemoryTrackerWorkload getWorkload() { return io.questdb.std.MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }) {
            var physical = find(factory, AsyncJitFilteredRecordCursorFactory.class);
            context.setMemoryTracker(tracker);
            try {
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 1);
                assertThrows(io.questdb.cairo.CairoException.class, () -> {
                    try (var cursor = physical.getCursor(context)) { cursor.hasNext(); }
                });
                assertEquals(0, tracker.getUsed());
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                for (int repeat = 0; repeat < 2; repeat++) {
                    try (var cursor = physical.getCursor(context)) {
                        assertTrue(cursor.hasNext());
                        assertTrue(tracker.getUsed() >= 16);
                        cursor.toTop();
                        assertTrue(cursor.hasNext());
                    }
                    assertEquals(0, tracker.getUsed());
                }
                var breaker = new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine);
                context.with(breaker);
                breaker.cancel();
                assertThrows(io.questdb.cairo.CairoException.class, () -> {
                    try (var cursor = physical.getCursor(context)) { cursor.hasNext(); }
                });
                assertEquals(0, tracker.getUsed());
                context.with(new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine));
            } finally { context.setMemoryTracker(null); }
            check(factory, sql);
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    private RecordCursorFactory candidate(String sql) throws Exception {
        context.setQueryCodeGenerator(compiler);
        return Demo.compileSql(engine, context, sql, true);
    }

    private void check(RecordCursorFactory factory, String sql) throws Exception {
        var actual = Demo.readSql(factory, context, factory.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (var stock = Demo.compileSql(engine, context, sql, true)) {
            assertEquals(sql, Demo.readSql(stock, context, stock.getMetadata().getColumnCount()), actual);
        } finally {
            context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            context.setQueryCodeGenerator(compiler);
        }
    }

    private static <T> T find(RecordCursorFactory factory, Class<T> type) {
        for (; factory != null; factory = factory.getBaseFactory()) if (type.isInstance(factory)) return type.cast(factory);
        throw new AssertionError("Missing " + type.getSimpleName());
    }
}
