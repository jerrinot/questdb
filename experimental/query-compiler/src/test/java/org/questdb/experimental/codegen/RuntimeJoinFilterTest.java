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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.mp.*;
import io.questdb.std.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class RuntimeJoinFilterTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private QueryCodeGenerator compiler;
    private QueryCodeGenerator.JoinFilter generated;
    private long memory;
    private long limit;
    private int unavailableBuilds;
    private int compilations;
    private boolean failPublication;

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public long getQueryMemoryLimitBytes() { return limit; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        var artifacts = temporary.newFolder().toPath();
        compiler = new QueryCodeGenerator() {
            @Override public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions) { return null; }
            @Override public JoinFilter compileJoinFilter(ColumnTypes inputs, ObjList<IntJoinFilterSource> sources) {
                ObjList<IntJoinFilterSource> observed = new ObjList<>();
                for (int i = 0; i < sources.size(); i++) {
                    final int index = i;
                    var source = sources.getQuick(i);
                    observed.add(new IntJoinFilterSource(source.probeColumn(), source.mapKeyColumn(), () -> {
                        if (failPublication && index == 1) throw CairoException.nonCritical().put("publication failure");
                        var map = source.completedBuild().get();
                        if (map == null) unavailableBuilds++;
                        return map;
                    }));
                }
                try {
                    compilations++;
                    return generated = ResolvedJoinFilter.compile(inputs, observed, new LlvmBackend(), artifacts);
                } catch (IOException e) { throw new RuntimeException(e); }
            }
        };
        engine.execute("create table p as (select x id,cast(case when x%17=0 then null else x%101-50 end as int) k,"
                + "cast(x%23 as int) j,timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) "
                + "timestamp(ts) partition by day bypass wal", context);
        engine.execute("create table a as (select cast(case when x%17=0 then null else (x%51)*2-50 end as int) k,x v from long_sequence(143))", context);
        engine.execute("create table b as (select cast(x%13 as int) j,x v from long_sequence(39))", context);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void duplicatesNullsRewindEarlyCloseAndChangedBuilds() throws Exception {
        String sql = "select p.id,a.v,b.v from p join a on p.k=a.k join b on p.j=b.j";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertNotNull(generated);
            assertEquals(2003, generated.compiled().rows().getNativeInputRows());
            try (var cursor = factory.getCursor(context)) {
                long n = 0; while (cursor.hasNext()) n++;
                cursor.toTop();
                long repeated = 0; while (cursor.hasNext()) repeated++;
                assertTrue(n > 2003); assertEquals(n, repeated);
            }
            try (var cursor = factory.getCursor(context)) { assertTrue(cursor.hasNext()); }
            check(factory, sql);
            int compiled = compilations;
            engine.execute("truncate table a", context);
            check(factory, sql);
            engine.execute("insert into a values (null,1),(-1,2),(2147483647,3),(-2147483647,4)", context);
            engine.execute("insert into p values (2004,2147483647,1,'2024-01-01T02:00:00'),"
                    + "(2005,-2147483647,1,'2024-01-01T02:00:01')", context);
            check(factory, sql);
            assertEquals(compiled, compilations);
            engine.execute("truncate table p", context);
            check(factory, sql);
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test public void columnTopsAndParquetUseTheSamePublishedState() throws Exception {
        engine.execute("alter table p add column late int", context);
        engine.execute("insert into p values (2004,2,1,'2024-01-01T02:00:00',2)", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table p convert partition to parquet list '2024-01-01'", context);
            String sql = "select count(),sum(a.v+b.v) from p join a on p.late=a.k join b on p.j=b.j";
            try (var factory = candidate(sql)) {
                check(factory, sql);
                assertNotNull(generated);
            }
        }
    }

    @Test public void smallerProbeStillSwapsAndUnavailableBuildPassesThrough() throws Exception {
        // No designated timestamp: preserve the ordinary join's freedom to swap inputs.
        engine.execute("create table small as (select cast(x as int) k,cast(x as int) j from long_sequence(3))", context);
        String sql = "select small.k,a.v,b.v from small join a on small.k=a.k join b on small.j=b.j";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertNotNull(generated);
            assertTrue("Build orientation must survive the redundant filter", unavailableBuilds > 0);
        }
    }

    @Test public void commonProbeOnTheOtherSideOfTheFirstJoinPreservesRuntimeOrientation() throws Exception {
        String sql = "select count(),sum(p.id+a.v+b.v) from a join p on a.k=p.k join b on p.j=b.j";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertNotNull(generated);
            assertEquals(2003, generated.compiled().rows().getNativeInputRows());
            assertEquals("Completed build must be published after the ordinary swap", 0, unavailableBuilds);
            engine.execute("truncate table p", context);
            engine.execute("insert into p values (1,2,1,'2024-01-01'),(2,null,2,'2024-01-01T00:00:01')", context);
            check(factory, sql);
            assertTrue("The same cached factory can now build the common scan", unavailableBuilds > 0);
        }
    }

    @Test public void failureReleasesStateAndFactoryReopensWithWorkersOrOwnerOnly() throws Exception {
        engine.execute("truncate table a", context);
        engine.execute("insert into a select cast(x*2 as int),x from long_sequence(100)", context);
        String sql = "select count(),sum(p.id+a.v+b.v) from p join a on p.k=a.k join b on p.j=b.j";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            failPublication = true;
            var publication = assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 2));
            assertTrue(publication.getFlyweightMessage().toString().contains("publication failure"));
            failPublication = false;
            check(factory, sql);
            limit = 1024;
            var failure = assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 2));
            assertTrue(failure.isOutOfMemory());
            limit = 0;
            check(factory, sql);
            pool.halt();
            check(factory, sql);
        } finally { limit = 0; }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test public void outerAndDependentJoinsDoNotPublishUnsafeFilters() throws Exception {
        for (String sql : List.of(
                "select count(),sum(a.v+b.v) from p left join a on p.k=a.k left join b on p.j=b.j",
                "select count(),sum(a.v+b.v+c.v) from p join a on p.k=a.k join b on p.j=b.j join b c on a.k=c.j")) {
            generated = null;
            try (var factory = candidate(sql)) { check(factory, sql); assertNull(generated); }
        }
    }

    private RecordCursorFactory candidate(String sql) throws Exception {
        context.setQueryCodeGenerator(compiler);
        return engine.select(sql, context);
    }

    private void check(RecordCursorFactory factory, String sql) throws Exception {
        var actual = Demo.readSql(factory, context, factory.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var ordinary = engine.select(sql, context)) {
            assertEquals(sql, Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount()), actual);
        } finally { context.setQueryCodeGenerator(compiler); }
    }
}
