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
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static org.junit.Assert.*;

public class WeightedAverageTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend = new LlvmBackend();
    private CompilerOptions options = CompilerOptions.DEFAULT;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

    private void configure(CompilerOptions options) throws java.io.IOException {
        this.options = options;
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath(), options);
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
        context.setParallelFilterEnabled(true);
        context.setParallelHorizonJoinEnabled(false);
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath(), options);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void typedInputsComposeWithEveryAggregateProducer() throws Exception {
        engine.execute("create table weighted as (select cast(x%17 as int) k,cast('s'||(x%37) as symbol) s,"
                + "cast('v'||(x%19) as varchar) v,cast(x%31-15 as byte) b,cast(x%101-50 as short) h,"
                + "cast(case when x%13=0 then null else x%97-48 end as int) i,"
                + "case when x%13=0 then null else x%97-48 end l,"
                + "cast(case when x%13=0 then null else (x%97-48)*0.25 end as float) f,"
                + "case when x%13=0 then null else (x%97-48)*0.25 end d,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(16421)) timestamp(ts) partition by day bypass wal", context);
        engine.execute("create table dim as (select cast(x%17 as int) k,cast(x%3-1 as double) w from long_sequence(51))", context);
        for (String sample : List.of("b", "h", "i", "l", "f", "d")) {
            String aggregates = "count(),sum(k),weighted_avg(" + sample + ",k),weighted_avg("
                    + sample + ",case s when 's1' then d else -d end),min(" + sample + "),max(" + sample + ")";
            check("select " + aggregates + " from weighted", true);
            check("select " + aggregates + " from weighted where k>8", true);
            for (String key : List.of("k", "s", "k,v"))
                check("select " + key + "," + aggregates + " from weighted group by " + key, true);
            String joinAggregates = "count(),weighted_avg(a." + sample + ",b.w),weighted_avg(b.w,a.k),min(a." + sample + ")";
            check("select " + joinAggregates + " from weighted a join dim b on a.k=b.k", true);
            check("select a.k," + joinAggregates + " from weighted a join dim b on a.k=b.k group by a.k", true);
            check("select count(),weighted_avg(b." + sample + ",a.k) from weighted a asof join "
                    + "(select * from weighted where k>=0) b on a.k=b.k", true);
        }
        check("select k,weighted_avg(m,n),sum(n) from (select k,s,min(i) m,count() n from weighted group by k,s) group by k", true);
        check("select k,min(m),max(m),sum(n) from (select k,s,weighted_avg(f,k+1) m,count() n from weighted group by k,s) group by k", true);
        String dictionary = "select length(s) k,count(),weighted_avg(f,i),weighted_avg(d,l) from weighted group by k";
        check(dictionary, true);
        engine.execute("alter table weighted add column late double", context);
        engine.execute("insert into weighted select cast(x%17 as int),cast('s'||(x%37) as symbol),cast('v'||(x%19) as varchar),"
                + "cast(x%31-15 as byte),cast(x%101-50 as short),cast(x%97-48 as int),x%97-48,cast((x%97-48)*0.25 as float),"
                + "(x%97-48)*0.25,timestamp_sequence('2024-01-02',1000000),cast(x%7-3 as double) from long_sequence(1021)", context);
        check("select k,count(),weighted_avg(f,late),weighted_avg(late,i) from weighted group by k", true);
        engine.execute("alter table weighted convert partition to parquet list '2024-01-01'", context);
        check("select k,count(),weighted_avg(f,late),weighted_avg(late,i) from weighted group by k", true);
        check(dictionary, true);
        engine.execute("truncate table weighted", context);
        check("select count(),weighted_avg(f,i) from weighted", false);
    }

    @Test public void scalarAndStagedProbesRetainFirstRowsThroughPartitions() throws Exception {
        configure(options.withLocalCapacity(64));
        context.changePageFrameSizes(64, 2048);
        engine.execute("create table partitioned as (select cast(x%257 as int) k,cast(x%257 as varchar) s,"
                + "cast(case when x%13=0 then null else (x%97-48)*0.25 end as float) f,"
                + "cast(x%7-3 as double) w from long_sequence(16421))", context);
        for (String prefetch : List.of("0", "32")) {
            configure(options.withPrefetchRows(Integer.parseInt(prefetch)));
            for (String key : List.of("k", "s", "k,s"))
                check("select " + key + ",count(),weighted_avg(f,w),weighted_avg(w,k) from partitioned "
                        + "where k>0 group by " + key, true);
            check("select count(),weighted_avg(f,w),weighted_avg(w,k) from partitioned where k>128", true);
            check("select count(),weighted_avg(f,w) from partitioned where k<0", false);
        }
    }

    private void check(String sql, boolean generated) throws Exception {
        context.setQueryCodeGenerator(null);
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) {
            var expected = Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount());
            context.setQueryCodeGenerator(compiler);
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                for (int i = 0; i < 2; i++)
                    assertEquals(sql, expected, Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount()));
                if (generated) assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
    }

    private long nativeRows(RecordCursorFactory factory) {
        for (; factory != null; factory = factory.getBaseFactory()) {
            if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                long rows = 0;
                for (int i = -1; i < 2; i++)
                    if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot) rows += slot.updatedRows();
                if (rows > 0) return rows;
            }
            if (factory instanceof AsyncGroupByNotKeyedRecordCursorFactory group)
                return group.getAtom().getCompiledAggregateNativeRows();
            if (factory instanceof AsyncHashJoinGroupByRecordCursorFactory group)
                return group.getAtom().getCompiledJoinNativeRows();
            if (factory instanceof FilteredAsOfJoinFastRecordCursorFactory join) return join.getNativeAggregateRows();
        }
        return 0;
    }
}
