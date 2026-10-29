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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
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
public class AggregateTypeTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

    public AggregateTypeTest(String name, Backend backend) { this.backend = backend; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()},
                new Object[]{"c", new NativeBackend()});
    }

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var cfg = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public int getGroupByShardingThreshold() { return 2; }
            @Override public int getGroupByMapCapacity() { return 16; }
            @Override public long getGroupByParallelTopKThreshold() { return 0; }
        };
        engine = new CairoEngine(cfg);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void symbolCountSharesOrdinaryStateAcrossMapsAndScalarMerge() throws Exception {
        engine.execute("create table counts as (select cast(x%7 as int) k,x id,"
                + "cast(case when x%7=0 then null else cast(x%17 as string) end as symbol) s,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(1003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select k,count(s),count() from counts group by k",
                "select s,count(s) from counts group by s", "select count(s) from counts",
                "select k,count(s) from counts where s in ('1','2',null) group by k")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table counts add column late symbol", context);
        String sql = "select k,count(s),count(late) from counts group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into counts values (1,1004,'new','2024-01-01T01:00:00','present')", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
        }
        engine.execute("alter table counts convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table counts", context);
        try (var candidate = compile("select count(s) from counts")) { check("select count(s) from counts", candidate); }
    }

    @Test public void dynamicSymbolCountKeepsOrdinaryNullAndKeyAssignmentSemantics() throws Exception {
        engine.execute("create table dynamic_counts as (select x id,cast(x%7 as int) k,"
                + "cast(case when x%3=0 then null else x%11 end as long) v from long_sequence(259))", context);
        String sql = "select k,count(cast(v as symbol)) from dynamic_counts group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            assertEquals(0, nativeRows(candidate));
        }
    }

    @Test public void intAndSymbolDistinctPreserveNullsGrowthMergeAndDictionaryReopen() throws Exception {
        engine.execute("create table distinct_types (k int,id long,i int,s symbol capacity 1024,ts timestamp) "
                + "timestamp(ts) partition by day bypass wal", context);
        engine.execute("insert into distinct_types select cast(x%7 as int) k,x id,"
                + "cast(case when x%7=0 then null else x%257-128 end as int) i,"
                + "cast(case when x%7=0 then null else cast(x%257 as string) end as symbol) s,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(4003)", context);
        for (String sql : List.of("select k,count_distinct(i),count_distinct(s),count_distinct(id) from distinct_types group by k",
                "select s,count_distinct(i),count_distinct(s),count() from distinct_types group by s",
                "select k,count_distinct(i),count_distinct(s) from distinct_types where s in ('1','2',null) group by k")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        for (String argument : List.of("i", "s")) {
            String sql = "select k,count_distinct(" + argument + ") n from distinct_types group by k order by n desc limit 7";
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                long collected = 0;
                for (var factory = candidate; factory != null; factory = factory.getBaseFactory()) {
                    if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                        for (int i = -1; i < 2; i++) {
                            if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot) collected += slot.topKCounters()[0];
                        }
                    }
                }
                assertTrue("Generated ranking reads finalized INT/SYMBOL counts", collected > 0);
            }
        }
        engine.execute("alter table distinct_types add column late symbol", context);
        String sql = "select k,count_distinct(i),count_distinct(s),count_distinct(late) from distinct_types group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into distinct_types values (1,4004,2147483647,'new','2024-01-02','present')", context);
            engine.execute("insert into distinct_types values (1,4005,-2147483647,'other','2024-01-02T00:00:01',null)", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
        }
        engine.execute("alter table distinct_types convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        String dynamic = "select k,count_distinct(cast(i as symbol)) from distinct_types group by k";
        try (var candidate = compile(dynamic)) { check(dynamic, candidate); assertEquals(0, nativeRows(candidate)); }
        engine.execute("truncate table distinct_types", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void integerAveragesUseFloatingSumsAndOrdinaryScalarResults() throws Exception {
        engine.execute("create table averages as (select cast(x%7 as short) g,cast(x%17 as int) k,"
                + "cast(case when x%7=0 or x<31 then null else x%257-128 end as int) i,"
                + "case when x%7=0 or x%13=0 then null else x*10000000 end l,cast(x%5 as short) s,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select g,avg(i),avg(l),avg(s) from averages group by g",
                "select g,avg(i+1),avg(l-1),count(i),sum(l) from averages where k>=0 group by g",
                "select avg(i),avg(l),avg(s) from averages where k>=0",
                "select avg(i+1),avg(l-1) from averages where k>0")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table averages add column late int", context);
        String sql = "select g,avg(i),avg(l),avg(late) from averages group by g";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into averages values (1,1,17,10000000000,1,'2024-01-02',42)", context);
            check(sql, candidate);
        }
        engine.execute("alter table averages convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table averages", context);
        sql = "select avg(i),avg(l),avg(s) from averages where k>=0";
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    private RecordCursorFactory compile(String sql) throws Exception {
        context.setQueryCodeGenerator(compiler);
        return Demo.compileSql(engine, context, sql, true);
    }

    private void check(String sql, RecordCursorFactory candidate) throws Exception {
        var actual = Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) {
            assertEquals(sql, Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount()), actual);
        } finally { context.setQueryCodeGenerator(compiler); }
    }

    private long nativeRows(RecordCursorFactory factory) {
        for (; factory != null; factory = factory.getBaseFactory()) {
            if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                long rows = 0;
                for (int i = -1; i < 2; i++)
                    if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot) rows += slot.updatedRows();
                return rows;
            }
            if (factory instanceof AsyncGroupByNotKeyedRecordCursorFactory group)
                return group.getAtom().getCompiledAggregateNativeRows();
        }
        return 0;
    }
}
