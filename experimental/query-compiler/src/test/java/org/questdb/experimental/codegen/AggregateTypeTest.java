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
                "select s,count(s) from counts group by s", "select count(s) from counts", "select count(s),count() from counts",
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

    @Test public void doubleAggregatesPreserveNullableStateAcrossFramesAndMerges() throws Exception {
        engine.execute("create table doubles as (select cast(x%7 as short) g,cast(x%17 as int) k,"
                + "case when x%7=0 or x<31 then null else (x%257-128)*0.25 end d,"
                + "case when x%13=0 then null else (x%31-15)*1024.0 end e,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select g,count(d),sum(d),avg(d),min(d),max(d),avg(e) from doubles group by g",
                "select k,sum(d),avg(e),min(d),max(d) from doubles where k>=0 group by k",
                "select count(d),sum(d),avg(d),min(d),max(d) from doubles where k>=0")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table doubles add column late double", context);
        String sql = "select g,count(late),sum(d),avg(late),min(late),max(late) from doubles group by g";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into doubles values (1,1,17.25,1024,'2024-01-02',42.5)", context);
            check(sql, candidate);
        }
        engine.execute("alter table doubles convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table doubles", context);
        sql = "select count(d),sum(d),avg(d),min(d),max(d) from doubles where k>=0";
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void doubleExpressionsFuseWithFilteredGroupedAndScalarConsumers() throws Exception {
        engine.execute("create table expressions as (select cast(x%7 as short) g,cast(x%17 as int) k,"
                + "case when x%7=0 or x<31 then null else (x%257-128)*0.25 end d,"
                + "cast(case when x%13=0 then null else x%101-50 end as int) i,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select g,sum(d*1.5+0.25),avg(d) from expressions where d>1.0 and d<30.0 group by g",
                "select g,sum(d-cast(i as double)),avg(cast(i+1 as double)),count(d/0.0) from expressions group by g",
                "select g,sum(d/2.0),avg(d+0.25),count(d) from expressions where d!=null group by g",
                "select g,count() from expressions where d=i*0.25 group by g",
                "select g,count() from expressions where d!=i*0.25 group by g",
                "select sum(d*2.0+0.25),count() from expressions",
                "select avg(i+1),sum(d*2.0),count() from expressions",
                "select sum(d*2.0),avg(d) from expressions where d>=0.0 or d=null")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table expressions add column late double", context);
        String sql = "select g,sum(d+late),avg(late/2.0) from expressions where late=null or late>=0.0 group by g";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into expressions values (1,1,17.25,42,'2024-01-02',1.5)", context);
            check(sql, candidate);
        }
        engine.execute("alter table expressions convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table expressions", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void unsupportedComputedKeyRetainsOrdinaryAverageFinalization() throws Exception {
        engine.execute("create table fallback as (select cast(x%17-8 as int) i,(x%31-15)*0.25 d from long_sequence(1003))", context);
        String sql = "select abs(i) k,avg(i),avg(d) from fallback group by k";
        try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); }
    }

    @Test public void doubleKeysPreserveRawBitsCompositeLayoutsAndColumnTops() throws Exception {
        engine.execute("create table keys (v long,i int,s symbol,label varchar,ts timestamp) timestamp(ts) partition by day bypass wal", context);
        engine.execute("insert into keys select x,cast(x%3 as int),cast(x%3 as symbol),'old'::varchar,"
                + "timestamp_sequence('2024-01-01',1000000) from long_sequence(33)", context);
        engine.execute("alter table keys add column d double", context);
        double[] values = {0.0,-0.0,Double.NaN,Double.longBitsToDouble(0x7ff8000000001234L),
                Double.longBitsToDouble(0x7ff8000000005678L),Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE,-Double.MIN_VALUE,1,1.00000000005,-17.25};
        try (var writer = engine.getWriter(engine.verifyTableName("keys"), "test")) {
            for (int n = 0; n < 5003; n++) {
                var row = writer.newRow(1704153600000000L + n);
                row.putLong(0, n); row.putInt(1, n%3); row.putSym(2, Integer.toString(n%3));
                row.putVarchar(3, new io.questdb.std.str.Utf8String(n%2==0 ? "ascii" : "žluť"));
                row.putDouble(5, values[n%values.length]); row.append();
            }
            writer.commit();
        }
        String direct = "select d,count() n,sum(v) total from keys group by d";
        context.setQueryCodeGenerator(null);
        java.util.Map<Long,List<Long>> expected;
        try (var ordinary = engine.select(direct, context)) { expected = rawDoubleGroups(ordinary); }
        assertEquals(values.length, expected.size()); // Canonical NULL shares the column-top group.
        for (String sql : List.of(direct,
                "select d*2.0+0.25 k,count(),sum(v) from keys group by k",
                "select i,d,count(),sum(v) from keys group by i,d",
                "select d,s,count(),sum(v) from keys group by d,s",
                "select d,label,count(),sum(v) from keys group by d,label")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                assertTrue(sql, nativeMapRows(candidate) > 0);
                if (sql.equals(direct)) assertEquals(expected, rawDoubleGroups(candidate));
            }
        }
        try (var candidate = compile(direct + " limit 2")) {
            var selected = rawDoubleGroups(candidate);
            assertEquals(2, selected.size());
            for (var entry : selected.entrySet()) assertEquals(expected.get(entry.getKey()), entry.getValue());
            assertTrue(nativeMapRows(candidate) > 0);
            assertTrue("Admission retains only complete selected groups", nativeMapRows(candidate) < 5036);
        }
        engine.execute("alter table keys convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(direct)) { assertEquals(expected, rawDoubleGroups(candidate)); }
        engine.execute("alter table keys convert partition to parquet list '2024-01-02'", context);
        context.setQueryCodeGenerator(null);
        try (var ordinary = engine.select(direct, context)) { expected = rawDoubleGroups(ordinary); }
        try (var candidate = compile(direct)) { assertEquals(expected, rawDoubleGroups(candidate)); }
        engine.execute("truncate table keys", context);
        try (var candidate = compile(direct)) { assertTrue(rawDoubleGroups(candidate).isEmpty()); }
    }

    private java.util.Map<Long,List<Long>> rawDoubleGroups(RecordCursorFactory factory) throws Exception {
        var result = new java.util.TreeMap<Long,List<Long>>();
        try (var cursor = factory.getCursor(context)) {
            var record = cursor.getRecord();
            while (cursor.hasNext()) assertNull(result.put(Double.doubleToRawLongBits(record.getDouble(0)),
                    List.of(record.getLong(1), record.getLong(2))));
        }
        return result;
    }

    private long nativeMapRows(RecordCursorFactory factory) {
        for (; factory != null; factory = factory.getBaseFactory()) {
            if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                long rows = 0;
                for (int i = -1; i < 2; i++)
                    if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot) rows += slot.getNativeMapRows();
                return rows;
            }
        }
        return 0;
    }

    @Test public void varcharCountPreservesNullsEmptyStringsColumnTopsAndParquet() throws Exception {
        engine.execute("create table strings as (select cast(x%7 as int) k,x v,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(33)) timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table strings add column s varchar", context);
        engine.execute("insert into strings select cast(x%7 as int),x,timestamp_sequence('2024-01-02',1000000),"
                + "cast(case when x%11=0 then null when x%13=0 then '' when x%3=0 then '中' "
                + "else 'long-shared-prefix-'||cast(x%29 as string) end as varchar) from long_sequence(1003)", context);
        for (String sql : List.of("select k,count(s),count() from strings group by k",
                "select s,count(s),count() from strings group by s",
                "select k,s,count(s),sum(v) from strings group by k,s",
                "select count(s) from strings",
                "select count(s),count() from strings",
                "select sum(v),count() from strings",
                "select k,count(case when v<500 then s when v<750 then '' else null::varchar end) from strings group by k",
                "select count(case when k<3 then s else null::varchar end) from strings where v>100",
                "select k,count(s),min(s),max(s) from strings group by k")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                // Fully batched aggregates keep their existing execution.
                if (sql.equals("select sum(v),count() from strings")) assertEquals(0, nativeRows(candidate));
                else assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
        String sql = "select k,count(s),count() from strings group by k";
        for (String partition : List.of("2024-01-01", "2024-01-02")) {
            engine.execute("alter table strings convert partition to parquet list '" + partition + "'", context);
            try (var candidate = compile(sql)) { check(sql, candidate); assertTrue(nativeRows(candidate) > 0); }
            try (var candidate = compile("select count(s) from strings")) { check("select count(s) from strings", candidate); }
        }
        // A joined row has no VARCHAR descriptor ABI yet; retain ordinary execution.
        engine.execute("create table dim as (select cast(x as int) k from long_sequence(7))", context);
        String join = "select count(s) from strings join dim on strings.k=dim.k";
        try (var candidate = compile(join)) { check(join, candidate); }
        engine.execute("truncate table strings", context);
        try (var candidate = compile("select count(s) from strings")) { check("select count(s) from strings", candidate); }
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
