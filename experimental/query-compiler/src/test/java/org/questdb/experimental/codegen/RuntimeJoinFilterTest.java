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
    private final List<QueryCodeGenerator.JoinFilter> filters = new java.util.ArrayList<>();
    private long memory;
    private long nativeFunctions;
    private long limit;
    private int unavailableBuilds;
    private int compilations;
    private boolean failPublication;
    private boolean failNativePreparation;
    private boolean cancelAtPublication;
    private int unorderedMapMaxEntrySize = 16;

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        nativeFunctions = LlvmBackend.liveFunctions();
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public long getQueryMemoryLimitBytes() { return limit; }
            @Override public int getSqlUnorderedMapMaxEntrySize() { return unorderedMapMaxEntrySize; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        context.with(new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine) {
            @Override public void statefulThrowExceptionIfTrippedTimeThrottled() {
                if (failNativePreparation) throw CairoException.nonCritical().put("native preparation cancelled");
                super.statefulThrowExceptionIfTrippedTimeThrottled();
            }
        });
        var artifacts = temporary.newFolder().toPath();
        compiler = new QueryCodeGenerator() {
            @Override public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions) { return null; }
            @Override public JoinFilter compileJoinFilter(ColumnTypes inputs, ObjList<JoinFilterSource> sources) {
                ObjList<JoinFilterSource> observed = new ObjList<>();
                for (int i = 0; i < sources.size(); i++) {
                    final int index = i;
                    var source = sources.getQuick(i);
                    observed.add(new JoinFilterSource(source.probeColumns(), source.keyTypes(), source.mapKeyColumn(), () -> {
                        if (cancelAtPublication && index == 0) failNativePreparation = true;
                        if (failPublication && index == 1) throw CairoException.nonCritical().put("publication failure");
                        var map = source.completedBuild().get();
                        if (map == null) unavailableBuilds++;
                        return map;
                    }));
                }
                try {
                    compilations++;
                    generated = ResolvedJoinFilter.compile(inputs, observed, new LlvmBackend(), artifacts);
                    if (generated != null) filters.add(generated);
                    return generated;
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
        assertEquals(nativeFunctions, LlvmBackend.liveFunctions());
    }

    @Test public void duplicatesNullsRewindEarlyCloseAndChangedBuilds() throws Exception {
        String sql = "select p.id,a.v,b.v from p join a on p.k=a.k join b on p.j=b.j";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertNotNull(generated);
            assertTrue(nativeInputRows() >= 2003);
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

    @Test public void sparseIntMembershipDoesMoreThanRangeRejection() throws Exception {
        engine.execute("truncate table a", context);
        engine.execute("truncate table b", context);
        engine.execute("insert into a values (1,1),(1000000,2)", context);
        engine.execute("insert into b values (1,1),(1000000,2)", context);
        String sql = "select p.id,a.v,b.v from p join a on p.k=a.k join b on p.j=b.j";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertTrue(nativeInputRows() >= 2003);
            assertTrue("Sparse membership must reject keys within its min/max range", nativeOutputRows() < 20);
        }
    }

    @Test public void fullWidthLongMembership() throws Exception {
        checkTypedMembership("p.l=a.l", "p.l=b.l");
    }

    @Test public void scalarOrderedMapUsesItsActualStride() throws Exception {
        unorderedMapMaxEntrySize = 0;
        checkTypedMembership("p.l=a.l", "p.l=b.l");
    }

    @Test public void symbolMembershipUsesTheProbeDictionary() throws Exception {
        checkTypedMembership("p.s=a.s", "p.s=b.s");
    }

    @Test public void compositeMembershipKeepsTupleCorrelation() throws Exception {
        checkTypedMembership("p.k=a.k and p.l=a.l and p.s=a.s", "p.k=b.k and p.l=b.l and p.s=b.s");
    }

    private void checkTypedMembership(String first, String second) throws Exception {
        engine.execute("create table typed_p as (select x id,cast(x%7 as int) k,"
                + "case when x%23=0 then null else (x%17)*4294967296+x%3 end l,"
                + "cast(case when x%19=0 then null else concat('key',x%11) end as symbol) s,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) "
                + "timestamp(ts) partition by day bypass wal", context);
        engine.execute("create table typed_a as (select cast(x%7 as int) k,"
                + "case when x%23=0 then null else (x%17)*4294967296+x%3 end l,"
                + "cast(case when x%19=0 then null else concat('key',(110-x)%11) end as symbol) s,x v "
                + "from long_sequence(110))", context);
        engine.execute("create table typed_b as (select cast(x%7 as int) k,"
                + "case when x%23=0 then null else (x%17)*4294967296+x%3 end l,"
                + "cast(case when x%19=0 then null else concat('key',(110-x)%11) end as symbol) s,x v "
                + "from long_sequence(11))", context);
        String sql = "select p.id,a.v,b.v from typed_p p join typed_a a on " + first + " join typed_b b on " + second;
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertNotNull("Required typed runtime filter was not generated: " + sql, generated);
            assertTrue(nativeInputRows() >= 2003);
            assertTrue("Native membership must actually reject rows", nativeOutputRows() < nativeInputRows());
            engine.execute("insert into typed_p values (2004,2,9223372036854775807,'new-key','2024-01-01T02:00:00'),"
                    + "(2005,3,-9223372036854775807,'negative','2024-01-01T02:00:01')", context);
            engine.execute("insert into typed_a values (2,9223372036854775807,'new-key',111),"
                    + "(3,-9223372036854775807,'negative',112)", context);
            engine.execute("insert into typed_b values (2,9223372036854775807,'new-key',56),"
                    + "(3,-9223372036854775807,'negative',57)", context);
            check(factory, sql);
        }
        String reversed = "select p.id,a.v,b.v from typed_a a join typed_p p on " + first
                + " join typed_b b on " + second;
        try (var factory = candidate(reversed)) {
            check(factory, reversed);
            assertTrue(nativeInputRows() >= 2003);
            assertTrue(nativeOutputRows() < nativeInputRows());
        }
        String transfer = first.contains(" and ")
                ? "a.k=b.k and a.v=b.v and a.s=b.s" : "a.v=b.v";
        String dependent = "select p.id,a.v,b.v from typed_p p join typed_a a on " + first
                + " join typed_b b on " + transfer;
        try (var factory = candidate(dependent)) {
            check(factory, dependent);
            assertTrue("Typed transfer must filter the intermediate build and probe", filters.size() >= 2);
            assertTrue(nativeOutputRows() < nativeInputRows());
            for (var filter : filters) assertTrue(filter.compiled().rows().getNativeInputRows() > 0);
            engine.execute("insert into typed_p values (2006,2,9223372036854775807,'new-key','2024-01-02')", context);
            engine.execute("alter table typed_p convert partition to parquet list '2024-01-01'", context);
            // Cast-bearing frames use the existing Java fallback with the same published sets.
            check(factory, dependent);
        }
    }

    @Test public void columnTopsAndParquetUseTheSamePublishedState() throws Exception {
        engine.execute("alter table p add column late int", context);
        engine.execute("insert into p values (2004,2,1,'2024-01-01T02:00:00',2),"
                + "(2005,2,1,'2024-01-02T02:00:00',2)", context);
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
            assertTrue(nativeInputRows() >= 2003);
            assertTrue("The completed build must reject probe rows after the swap", nativeOutputRows() < nativeInputRows());
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
            cancelAtPublication = true;
            var cancelled = assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 2));
            assertTrue(cancelled.getFlyweightMessage().toString().contains("native preparation cancelled"));
            cancelAtPublication = false;
            failNativePreparation = false;
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

    @Test public void outerJoinsDoNotPublishUnsafeFilters() throws Exception {
        for (String sql : List.of(
                "select count(),sum(a.v+b.v) from p left join a on p.k=a.k left join b on p.j=b.j")) {
            generated = null;
            try (var factory = candidate(sql)) { check(factory, sql); assertNull(generated); }
        }
    }

    @Test public void dependentBranchesMustMatchTheSameProducerDuplicate() throws Exception {
        engine.execute("create table parent(k int,x long,y long)", context);
        engine.execute("insert into parent values (1,7,8),(1,8,7),(2,7,7),(2,7,7)", context);
        engine.execute("create table cx as (select 7L k from long_sequence(2))", context);
        engine.execute("create table cy as (select 7L k from long_sequence(3))", context);
        String sql = "select count(),sum(a.x),min(a.y),max(a.y),avg(a.x) from p "
                + "join parent a on p.k=a.k join cx on a.x=cx.k join cy on a.y=cy.k";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertEquals(2, filters.size());
            assertTrue("Only producer key 2 can survive both branches", nativeOutputRows() < 40);
        }
    }

    @Test public void dependentTransferPreservesVarcharAndDecimalPayloads() throws Exception {
        engine.execute("create table payload_a as (select cast(x%101-50 as int) k,x child,"
                + "cast('note'||x as varchar) note,cast(x*0.01 as decimal(10,2)) price from long_sequence(143))", context);
        String from = " from p join payload_a a on p.k=a.k join b on a.child=b.v";
        for (String sql : List.of("select p.id,a.note,cast(a.price as varchar)" + from,
                "select a.note,cast(sum(a.price*b.v) as varchar)" + from + " group by a.note")) {
            try (var factory = candidate(sql)) {
                check(factory, sql);
                assertTrue(filters.size() >= 2);
                assertTrue(nativeOutputRows() < nativeInputRows());
            }
        }
    }

    @Test public void dependentBuildsTransferTheirSurvivingKeys() throws Exception {
        String sql = "select count(),sum(a.v+b.v+c.v),min(a.v),max(b.v),avg(c.v) "
                + "from p join a on p.k=a.k join b on p.j=b.j join b c on a.k=c.j";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertTrue("Both the intermediate build and root scan must filter", filters.size() >= 2);
            assertTrue(nativeOutputRows() < nativeInputRows());
            for (var filter : filters) assertTrue(filter.compiled().rows().getNativeInputRows() > 0);
            engine.execute("truncate table b", context);
            check(factory, sql);
            engine.execute("insert into b values (2,1),(2,2)", context);
            check(factory, sql);
        }
    }

    private long nativeInputRows() {
        return filters.stream().mapToLong(f -> f.compiled().rows().getNativeInputRows()).sum();
    }

    private long nativeOutputRows() {
        return filters.stream().mapToLong(f -> f.compiled().rows().getNativeOutputRows()).sum();
    }

    @Test public void automaticRootChoicePreservesTypesPayloadsAndDuplicateBranches() throws Exception {
        createRootChoiceTables();
        for (String keys : List.of("p.k=a.k", "p.l=a.l", "p.s=a.s", "p.k=a.k and p.l=a.l and p.s=a.s")) {
            String from = " from root_b b join root_a a on a.j=b.j join root_p p on " + keys
                    + " join root_e e on p.j=e.k where a.v%3=0";
            String sql = "select count(),sum(p.id+a.v),min(p.id+a.v),max(p.id+a.v),avg(p.id+a.v),"
                    + "cast(sum(cast(p.id+a.v as decimal(18,0))) as varchar)" + from;
            try (var factory = candidate(sql)) {
                assertEquals("root_p", firstScan(factory));
                check(factory, sql);
                assertTrue(nativeInputRows() >= 100003);
                assertTrue(nativeOutputRows() < nativeInputRows());
                check(factory, sql);
            }
            String rows = "select p.id,a.v,b.j,e.k" + from + " order by 1,2,3,4";
            try (var factory = candidate(rows)) { check(factory, rows); }
        }
        String sql = "select count(),sum(p.id+a.v) from root_b b join root_a a on a.j=b.j "
                + "join root_p p on p.s=a.s join root_e e on p.j=e.k where a.v%3=0";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            engine.execute("insert into root_p values (100004,5,21474836485,'new',1)", context);
            engine.execute("insert into root_a values (5,21474836485,'new',1,9)", context);
            check(factory, sql);
            engine.execute("truncate table root_a", context);
            check(factory, sql);
        }
    }

    @Test public void automaticRootChoiceKeepsSelectiveAndOrderedBoundaries() throws Exception {
        createRootChoiceTables();
        String from = " from root_b b join root_a a on a.j=b.j join root_p p on p.k=a.k join root_e e on p.j=e.k";
        for (String sql : List.of(
                "select count(),sum(p.id+a.v)" + from + " where p.id=1",
                "select first(p.id),last(p.id),sum(a.v)" + from,
                "select count() from root_b b join root_a a on a.j=b.j left join root_p p on p.k=a.k join root_e e on p.j=e.k")) {
            try (var factory = candidate(sql)) {
                assertEquals("root_b", firstScan(factory));
                check(factory, sql);
            }
        }
        String limited = "select count() from root_b b join root_a a on a.j=b.j "
                + "join (select * from root_p limit 1) p on p.k=a.k join root_e e on p.j=e.k";
        try (var factory = candidate(limited)) {
            assertEquals("root_b", firstScan(factory));
            check(factory, limited);
        }
    }

    @Test public void automaticRootChoicePreservesCyclicKeysAndResiduals() throws Exception {
        createRootChoiceTables();
        String sql = "select count(),sum(p.id+a.v),min(p.id),max(p.id),"
                + "cast(sum(cast(p.id as decimal(18,0))) as varchar)"
                + " from root_b b join root_a a on a.j=b.j join root_p p on p.k=a.k and p.j=b.j"
                + " join root_e e on p.j=e.k and a.v>e.k where a.v%3=0";
        try (var factory = candidate(sql)) {
            assertEquals("root_p", firstScan(factory));
            check(factory, sql);
            check(factory, sql);
        }
    }

    @Test public void automaticRootChoiceDoesNotRequireNativeKeyTypes() throws Exception {
        createRootChoiceTables();
        for (String type : List.of("varchar", "string", "double")) {
            engine.execute("create table text_p as (select id,cast(k as " + type + ") k from root_p)", context);
            engine.execute("create table text_a as (select cast(k as " + type + ") k,j,v from root_a)", context);
            String sql = "select count(),sum(p.id+a.v),min(p.id),max(p.id),"
                    + "cast(sum(cast(p.id as decimal(18,0))) as varchar)"
                    + " from root_b b join text_a a on a.j=b.j join text_p p on p.k=a.k where a.v%3=0";
            try (var factory = candidate(sql)) {
                assertEquals(type.equals("double") ? "text_p" : "root_b", firstScan(factory));
                check(factory, sql);
            }
            engine.execute("drop table text_p", context);
            engine.execute("drop table text_a", context);
        }
    }

    @Test public void rootChoiceDeclinesAmbiguousDistinctKeyCost() throws Exception {
        engine.execute("create table near_p as (select 1 k from long_sequence(100003))", context);
        engine.execute("create table near_a as (select cast(x as int) k,0 j from long_sequence(90003))", context);
        engine.execute("create table near_b as (select 0 j from long_sequence(1))", context);
        String sql = "select count(),cast(sum(cast(p.k as decimal(18,0))) as varchar)"
                + " from near_a a join near_b b on a.j=b.j join near_p p on a.k=p.k";
        try (var factory = candidate(sql)) {
            assertEquals("near_a", firstScan(factory));
            check(factory, sql);
        }
    }

    @Test public void unorderedAggregatesCanReleaseInputTimestampOrder() throws Exception {
        createTimestampRootTables();
        for (String keys : List.of("p.k=a.k", "p.l=a.l", "p.s=a.s", "p.k=a.k and p.l=a.l and p.s=a.s")) {
            String from = " from time_a a join root_b b on a.j=b.j join root_p p on " + keys
                    + " where a.ts >= '2024-01-01' and a.ts < '2024-01-01T00:01:00'";
            for (String group : List.of("", "b.j,", "a.ts,")) {
                String sql = "select " + group + "count(),sum(p.id),min(p.id),max(p.id),avg(p.id),"
                        + "count_distinct(a.v),cast(sum(cast(p.id as decimal(18,0))) as varchar)" + from
                        + (group.isEmpty() ? "" : " order by 1 limit 4");
                try (var factory = candidate(sql)) {
                    assertEquals("root_p", firstScan(factory));
                    check(factory, sql);
                    check(factory, sql);
                    assertTrue(nativeInputRows() >= 100003);
                }
            }
        }
    }

    @Test public void timestampRootRetainsOrderedConsumersAndNestedLimits() throws Exception {
        createTimestampRootTables();
        String from = " from time_a a join root_b b on a.j=b.j join root_p p on p.k=a.k";
        for (String sql : List.of(
                "select first(p.id),last(p.id),sum(p.id)" + from,
                "select a.ts,sum(p.id)" + from + " sample by 10s",
                "select sum(id),count() from (select p.id" + from + " limit 7)",
                "select sum(id),count() from (select p.id, row_number() over(order by a.ts) n" + from + ") where n<=7")) {
            try (var factory = candidate(sql)) {
                assertEquals(sql, "time_a", firstScan(factory));
                check(factory, sql);
            }
        }
        engine.execute("create table clock as (select timestamp_sequence('2023-12-31',1000000) ts from long_sequence(86410)) timestamp(ts)", context);
        String sql = "select sum(p.id),count(c.ts)" + from + " asof join clock c";
        try (var factory = candidate(sql)) {
            assertEquals("time_a", firstScan(factory));
            check(factory, sql);
        }
    }

    @Test public void rootChoicePreservesExplicitSortAndTimestampGrouping() throws Exception {
        createTimestampRootTables();
        engine.execute("create table time_p as (select timestamp_sequence('2023-01-01',1000000) ts,* from root_p)"
                + " timestamp(ts) partition by day bypass wal", context);
        String from = " from time_a a join root_b b on a.j=b.j join time_p p on p.k=a.k";
        for (String sql : List.of(
                "select a.ts,p.id,a.v" + from + " order by a.ts,p.id,a.v limit 17",
                "select a.ts,count(),sum(p.id),min(p.ts),max(p.ts)" + from + " order by a.ts limit 4")) {
            try (var factory = candidate(sql)) {
                assertEquals(sql, "time_p", firstScan(factory));
                check(factory, sql);
            }
        }
    }

    private void createTimestampRootTables() throws Exception {
        createRootChoiceTables();
        engine.execute("create table time_a as (select timestamp_sequence('2024-01-01',1000000) ts,k,l,s,j,v from root_a)"
                + " timestamp(ts) partition by day bypass wal", context);
    }

    @Test public void laneSelectionAndCountAgreeWithFallbackAcrossTailsAndRepresentations() throws Exception {
        for (int[] types : List.of(new int[]{ColumnType.INT}, new int[]{ColumnType.LONG},
                new int[]{ColumnType.INT, ColumnType.LONG})) {
            var columns = new ArrayColumnTypes();
            for (int type : types) columns.add(type);
            try (var map = io.questdb.cairo.map.MapFactory.createUnorderedMap(engine.getConfiguration(), columns,
                    new ArrayColumnTypes().add(ColumnType.LONG));
                 var arena = java.lang.foreign.Arena.ofConfined()) {
                int[] indexes = new int[types.length];
                long[] data = new long[types.length];
                long pointers = arena.allocate(types.length * 8L, 8).address();
                long output = arena.allocate(68 * 8L, 8).address();
                for (int k = 0; k < types.length; k++) {
                    indexes[k] = k;
                    data[k] = arena.allocate(67 * 8L, 8).address();
                    Unsafe.putLong(pointers + k * 8L, data[k]);
                    for (int row = 0; row < 67; row++) {
                        long value = row % 11 - 3;
                        if (types[k] == ColumnType.INT) Unsafe.putInt(data[k] + row * 4L, (int) value);
                        else Unsafe.putLong(data[k] + row * 8L, value);
                    }
                }
                ObjList<QueryCodeGenerator.JoinFilterSource> sources = new ObjList<>();
                sources.add(new QueryCodeGenerator.JoinFilterSource(indexes, types, 1, () -> map));
                var filter = ResolvedJoinFilter.compile(columns, sources, new LlvmBackend(), temporary.newFolder().toPath());
                assertNotNull(filter);
                try (var rows = filter.compiled().rows(); var count = filter.compiled().count(); var fallback = filter.fallback()) {
                    for (int mode = 0; mode < 3; mode++) {
                        map.clear();
                        if (mode < 2) for (int i = 0; i < 5; i++) {
                            var key = map.withKey();
                            for (int type : types) {
                                long value = mode == 0 ? i : i * 4294967297L;
                                if (type == ColumnType.INT) key.putInt(mode == 0 ? i : i * 2);
                                else key.putLong(value);
                            }
                            key.createValue().putLong(0, i);
                        }
                        fallback.init(null, context);
                        rows.prepareForDispatch();
                        for (int size : new int[]{0, 1, 7, 8, 9, 63, 64, 65, 67}) {
                            for (int i = 0; i < 68; i++) Unsafe.putLong(output + i * 8L, -77);
                            long selected = rows.call(pointers, types.length, 0, 0, 0, output, size);
                            assertEquals(selected, count.call(pointers, types.length, 0, 0, 0, size));
                            long expected = 0;
                            for (int i = 0; i < size; i++) {
                                final int row = i;
                                var record = new io.questdb.cairo.sql.Record() {
                                    @Override public int getInt(int column) { return Unsafe.getInt(data[column] + row * 4L); }
                                    @Override public long getLong(int column) { return Unsafe.getLong(data[column] + row * 8L); }
                                };
                                if (fallback.getBool(record)) assertEquals(row, Unsafe.getLong(output + expected++ * 8L));
                            }
                            assertEquals(expected, selected);
                            assertEquals(-77, Unsafe.getLong(output + selected * 8L));
                        }
                    }
                }
            }
        }
    }

    @Test public void fixedKeyStorageBoundCoversRealMapGrowth() throws Exception {
        var values = new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.INT);
        for (int maxEntry : new int[]{0, 32}) {
            var configuration = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
                @Override public int getSqlUnorderedMapMaxEntrySize() { return maxEntry; }
                @Override public int getSqlSmallMapKeyCapacity() { return 17; }
                @Override public long getSqlSmallMapPageSize() { return 6144; }
                @Override public double getSqlFastMapLoadFactor() { return .41; }
            };
            for (int[] types : List.of(new int[]{ColumnType.INT}, new int[]{ColumnType.LONG},
                    new int[]{ColumnType.INT, ColumnType.LONG, ColumnType.SYMBOL})) {
                var keys = new ArrayColumnTypes();
                for (int type : types) keys.add(type);
                for (int count : new int[]{1, 65, 4097}) {
                    long bound = io.questdb.cairo.map.MapFactory.fixedKeyMemoryUpperBound(configuration, keys, 8, count);
                    try (var map = io.questdb.cairo.map.MapFactory.createUnorderedMap(configuration, keys, values)) {
                        map.close();
                        map.setMemoryLimit(bound);
                        map.reopen();
                        for (int i = 0; i < count; i++) {
                            var key = map.withKey();
                            for (int type : types) {
                                if (type == ColumnType.LONG) key.putLong(i * 4294967297L);
                                else key.putInt(i);
                            }
                            var value = key.createValue();
                            assertTrue(value.isNew());
                            value.putInt(0, i);
                            value.putInt(1, 1);
                        }
                        assertEquals(count, map.size());
                        long backing = map.getHeapSize();
                        if (map instanceof io.questdb.cairo.map.OrderedMap) backing += map.getKeyCapacity() * 8L;
                        assertTrue(backing <= bound);
                    }
                }
                assertEquals(Long.MAX_VALUE, io.questdb.cairo.map.MapFactory.fixedKeyMemoryUpperBound(configuration, keys, 8, Long.MAX_VALUE));
            }
            assertEquals(Long.MAX_VALUE, io.questdb.cairo.map.MapFactory.fixedKeyMemoryUpperBound(configuration,
                    new ArrayColumnTypes().add(ColumnType.VARCHAR), 8, 100));
        }
    }

    private void createRootChoiceTables() throws Exception {
        engine.execute("create table root_p as (select x id,cast(case when x%19=0 then null else x%67-33 end as int) k,"
                + "(x%67-33)*4294967297 l,cast(case when x%19=0 then null else concat('K',x%67-33) end as symbol) s,"
                + "cast(x%7 as int) j from long_sequence(100003))", context);
        engine.execute("create table root_a as (select cast(case when x%19=0 then null else (194-x)%67-33 end as int) k,"
                + "((194-x)%67-33)*4294967297 l,cast(case when x%19=0 then null else concat('K',(194-x)%67-33) end as symbol) s,"
                + "cast(x%11 as int) j,x%19 v from long_sequence(193))", context);
        engine.execute("create table root_b as (select cast(x%3*2+1 as int) j from long_sequence(5))", context);
        engine.execute("create table root_e as (select cast(x%4 as int) k from long_sequence(7))", context);
    }

    private String firstScan(RecordCursorFactory factory) {
        var sink = new io.questdb.griffin.TextPlanSink();
        sink.of(factory, context);
        for (int i = 1; i <= sink.getLineCount(); i++) {
            String line = sink.getLine(i).toString();
            if (line.contains("scan on: ")) return line.substring(line.indexOf("scan on: ") + 9).trim();
        }
        throw new AssertionError("No table scan in plan");
    }

    private RecordCursorFactory candidate(String sql) throws Exception {
        filters.clear();
        generated = null;
        context.setQueryCodeGenerator(compiler);
        return engine.select(sql, context);
    }

    private void check(RecordCursorFactory factory, String sql) throws Exception {
        var actual = Demo.readSql(factory, context, factory.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var ordinary = engine.select(sql, context)) {
            assertEquals(sql, ordinary.getMetadata().getColumnCount(), factory.getMetadata().getColumnCount());
            assertEquals(sql, ordinary.getMetadata().getTimestampIndex(), factory.getMetadata().getTimestampIndex());
            for (int i = 0; i < factory.getMetadata().getColumnCount(); i++) {
                assertEquals(sql, ordinary.getMetadata().getColumnName(i), factory.getMetadata().getColumnName(i));
                assertEquals(sql, ordinary.getMetadata().getColumnType(i), factory.getMetadata().getColumnType(i));
            }
            assertEquals(sql, Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount()), actual);
        } finally { context.setQueryCodeGenerator(compiler); }
    }
}
