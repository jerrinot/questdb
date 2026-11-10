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

public class SwitchSelectorTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend = new LlvmBackend();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

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
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void typedSelectorsComposeAcrossAggregatesFiltersAndJoins() throws Exception {
        engine.execute("create table selectors (k int,f float,d double,dt date,u timestamp,n timestamp_ns,ip ipv4,ts timestamp) timestamp(ts) partition by day bypass wal", context);
        double[] values = {-0.0, 0.0, 1, Math.nextUp(1.0), 1.00000000005, Double.NaN,
                Double.longBitsToDouble(0xfff8000000000123L), Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, Double.MIN_VALUE, -Double.MIN_VALUE};
        try (var writer = engine.getWriter(engine.verifyTableName("selectors"), "test")) {
            for (int i = 0; i < 2053; i++) {
                var row = writer.newRow(1704067200000000L + i);
                row.putInt(0, i % 17);
                row.putFloat(1, i % 13 == 0 ? Float.intBitsToFloat(0xffc00123) : (float) values[i % values.length]);
                row.putDouble(2, values[i % values.length]);
                long time = switch (i % 4) { case 0 -> Long.MIN_VALUE; case 1 -> -1; case 2 -> 1000; default -> 0; };
                row.putDate(3, time); row.putTimestamp(4, time); row.putTimestamp(5, time);
                row.putIPv4(6, switch (i % 4) { case 0 -> 0; case 1 -> -1; case 2 -> Integer.MIN_VALUE; default -> 1; });
                row.append();
            }
            writer.commit();
        }
        engine.execute("create table dim as (select cast(x-1 as int) k from long_sequence(17))", context);
        for (String selector : selectors()) {
            String aggregates = "count(),sum(" + selector + "),min(" + selector + "),max(" + selector + "),avg(" + selector + ")";
            check("select " + aggregates + " from selectors", true);
            check("select k," + aggregates + " from selectors group by k", true);
            check("select k," + aggregates + " from selectors where " + selector + ">20 group by k", true);
            check("select k,sum(v),min(v),max(v) from (select k," + selector + " v,count() c from selectors group by k,v) group by k", true);
            String label = "case when " + selector + ">30 then '雪'::varchar else 'λ'::varchar end";
            check("select " + label + " label,count(),sum(" + selector + ") from selectors group by label", true);
            check("select " + aggregates.replace("switch(", "switch(a.") + " from selectors a join dim b on a.k=b.k", true);
            check("select " + aggregates.replace("switch(", "switch(b.") + " from selectors a asof join (select * from selectors where k>=0) b on a.k=b.k", true);
        }
        engine.execute("alter table selectors add column late float", context);
        engine.execute("insert into selectors values (1,1.0,1.0,-1,-1,-1,'255.255.255.255','2024-01-02',1.0)", context);
        String late = "select k,sum(switch(late,cast(null as float),11,cast(1 as float),22,33)),count() from selectors group by k";
        check(late, true);
        engine.execute("alter table selectors convert partition to parquet list '2024-01-01'", context);
        check(late, true);
        for (String selector : selectors()) check("select count(),sum(" + selector + ") from selectors", true);
        engine.execute("truncate table selectors", context);
        for (String selector : selectors()) check("select count(),sum(" + selector + ") from selectors", false);
    }

    private static List<String> selectors() {
        return List.of(
                "switch(f,cast(-0.0 as float),11,cast(0.0 as float),22,cast(1.0 as float),33,cast(null as float),44,55)",
                "switch(d,-0.0,11,0.0,22,1.0,33,cast(null as double),44,55)",
                "switch(dt,cast(-1 as date),11,cast(1000 as date),22,cast(null as date),33,44)",
                "switch(u,cast(1000000 as timestamp_ns),11,cast(-1 as timestamp),22,cast(null as timestamp),33,44)",
                "switch(n,cast(1 as timestamp),11,cast(-1 as timestamp_ns),22,cast(null as timestamp_ns),33,44)",
                "switch(ip,cast('255.255.255.255' as ipv4),11,cast('128.0.0.0' as ipv4),22,cast(null as ipv4),33,44)");
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
