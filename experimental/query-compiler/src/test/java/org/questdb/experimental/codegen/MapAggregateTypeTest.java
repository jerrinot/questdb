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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
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
public class MapAggregateTypeTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private CompilerOptions options = CompilerOptions.DEFAULT.withLocalCapacity(64);
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

    public MapAggregateTypeTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()},
                new Object[]{"c", new NativeBackend()});
    }

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var cfg = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 513; }
            @Override public int getSqlPageFrameMinRows() { return 257; }
            @Override public int getGroupByShardingThreshold() { return 2; }
            @Override public int getGroupByMapCapacity() { return 16; }
        };
        engine = new CairoEngine(cfg);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine); pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath(), options);
        engine.execute("create table t as (select x%257 k,cast(x%11 as int) i,"
                + "cast(case when x%17=0 then null else cast(x%17 as string) end as symbol) s,"
                + "case when x%13=0 then null else (x%8191-4095)*0.25 end d,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(5003)) "
                + "timestamp(ts) partition by day bypass wal", context);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void finalizedNumericValuesAndProjectionsFeedTheExistingConsumer() throws Exception {
        for (String aggregate : List.of("min(d)", "max(d)", "avg(d)", "avg(i)")) {
            String sql = "select v+0.5 bucket,count() from (select k," + aggregate
                    + " v from t group by k) group by bucket";
            try (var candidate = compile(sql, true)) { check(sql, candidate); check(sql, candidate); }
        }
        for (String sql : List.of(
                "select i,count(),sum(v),min(v),max(v) from (select k,i,min(d) v from t group by k,i) group by i",
                "select d,sum(n) from (select k,d,count() n from t group by k,d) group by d",
                "select v,count() from (select k,min(i) v from t group by k) group by v",
                "select s,avg(v) from (select k,s,min(d) v from t group by k,s) group by s")) {
            try (var candidate = compile(sql, true)) { check(sql, candidate); }
        }
        for (String sql : List.of(
                "select v,count() from (select k,min(d) v from t group by k) where v>0 group by v")) {
            try (var candidate = compile(sql, false)) { check(sql, candidate); }
        }
    }

    @Test public void symbolConditionsUseTheOuterCursorsDictionary() throws Exception {
        String sql = "select i,count(case when s='new-ž' then v end),"
                + "sum(case s when '1' then v when 'new-ž' then v+0.5 end),"
                + "min(case when s in ('1',null) then v end),max(case when s!='1' then v end) "
                + "from (select k,i,s,min(d) v from t group by k,i,s) group by i";
        try (var candidate = compile(sql, true)) {
            check(sql, candidate);
            engine.execute("insert into t values (4,2,'new-ž',-7.25,'2024-01-02')", context);
            check(sql, candidate);
            engine.execute("truncate table t", context);
            check(sql, candidate);
            engine.execute("insert into t values (1,0,'new-ž',3.25,'2024-01-03'),"
                    + "(2,1,null,null,'2024-01-03'),(3,1,'1',-9.5,'2024-01-03')", context);
            check(sql, candidate);
        }
    }

    @Test public void statisticalConsumersShareFinalizationAndTypedReaders() throws Exception {
        // Bound the nested variance; DoubleStateTest checks exact states at large offsets.
        for (String aggregate : List.of("min(d/1024.0)", "var_pop(d/1024.0)", "var_samp(d/1024.0)",
                "stddev_pop(d/1024.0)", "stddev(d/1024.0)", "covar_pop(d/1024.0,d/2048.0)")) {
            String sql = "select i,count(v),avg(v),var_pop(v),var_samp(v),"
                    + "stddev_pop(v),stddev(v),min(v),max(v),covar_pop(v,k),covar_samp(k,v) "
                    + "from (select k,i," + aggregate + " v from t group by k,i) group by i";
            try (var candidate = compile(sql, true)) { check(sql, candidate, 9); check(sql, candidate, 9); }
        }
    }

    @Test public void outerAveragesFinalizeAfterAllProducerMapsMerge() throws Exception {
        engine.execute("create table nested_avg as (select cast(x%257 as int) k,cast(x%7 as int) i,"
                + "cast(x%17-8 as short) h,case when x%257%13=0 then null else x%257-128 end l,"
                + "cast(case when x%257%13=0 then null else (x%257-128)*0.25 end as float) f,"
                + "case when x%257%13=0 then null else (x%257-128)*0.25 end d "
                + "from long_sequence(5003))", context);
        for (String aggregate : List.of("min(k)", "min(l)", "min(h)", "max(h)", "sum(f)", "min(d)", "avg(l)", "avg(f)", "nsum(l)", "nsum(f)", "weighted_avg(l,i+1)", "weighted_avg(f,i+1)")) {
            String sql = "select i,avg(v),count(v),min(v),max(v),nsum(v),weighted_avg(v,i+1) from (select k,i," + aggregate
                    + " v from nested_avg group by k,i) group by i";
            try (var candidate = compile(sql, true)) { check(sql, candidate); check(sql, candidate); }
        }
        String sql = "select i,avg(h),sum(n) from (select k,i,h,count() n from nested_avg group by k,i,h) group by i";
        try (var candidate = compile(sql, true)) { check(sql, candidate); check(sql, candidate); }
    }

    @Test public void regressionResultsFeedNestedAggregatesAndRanking() throws Exception {
        engine.execute("create table nested_regression as (select cast(x%17 as int) k,cast(x%5 as symbol) s,"
                + "x%97-48 v from long_sequence(5003))",context);
        for(String aggregate:List.of("corr(v*2,v)","regr_slope(v*2,v)","regr_intercept(v*2,v)","regr_r2(v*2,v)")) {
            String sql="select s,count(r),min(r),max(r),avg(r),sum(r) from (select k,s,"+aggregate
                    + " r from nested_regression group by k,s) group by s order by s";
            try(var candidate=compile(sql,true)){check(sql,candidate);check(sql,candidate);}
        }
    }

    @Test public void floatResultsRetainTheirStateWidthInNestedConsumers() throws Exception {
        engine.execute("create table floats as (select cast(x%257 as int) k,cast(x%7 as int) i,"
                + "cast(case when x%13=0 then null else (x%31-15)*0.25 end as float) f "
                + "from long_sequence(5003))", context);
        for (String aggregate : List.of("sum", "min", "max")) {
            String sql = "select i,count(v),sum(v),min(v),max(v),avg(v),sum(v+cast(i as float)),"
                    + "min(v/cast(3 as float)) from (select k,i," + aggregate
                    + "(f) v from floats group by k,i) group by i";
            try (var candidate = compile(sql, true)) { check(sql, candidate); check(sql, candidate); }
        }
        String sql = "select i,count(v),min(v),max(v) from (select k,i,cast(min(d) as float) v from t group by k,i) group by i";
        try (var candidate = compile(sql, true)) { check(sql, candidate); check(sql, candidate); }
    }

    @Test public void finalizedOuterAveragesKeepRankingSharedReadersAndCursorLifecycle() throws Exception {
        String base = "select s,avg(v) mean from (select k,s,min(d) v from t group by k,s) group by s";
        for (String suffix : List.of("", " order by mean desc limit 5")) {
            String sql = base + suffix;
            try (var candidate = compile(sql, true)) {
                check(sql, candidate); check(sql, candidate);
            }
        }
        engine.execute("create table thresholds as (select x*100.0-1000 v from long_sequence(21))", context);
        String shared = "select o.s,o.mean,sub.v from (" + base + ") o "
                + "join lateral (select v from thresholds where v<o.mean) sub";
        try (var candidate = compile(shared, true)) { check(shared, candidate); check(shared, candidate); }
        try (var candidate = compile(base, true)) {
            try (var cursor = candidate.getCursor(context)) {
                var rows = new java.util.HashMap<Long, Long>();
                var record = cursor.getRecord();
                while (cursor.hasNext()) rows.put(record.getRowId(), Double.doubleToLongBits(record.getDouble(1)));
                cursor.toTop();
                int count = 0;
                while (cursor.hasNext()) {
                    assertEquals(rows.get(record.getRowId()).longValue(), Double.doubleToLongBits(record.getDouble(1)));
                    cursor.recordAt(cursor.getRecordB(), record.getRowId());
                    assertEquals(Double.doubleToLongBits(record.getDouble(1)), Double.doubleToLongBits(cursor.getRecordB().getDouble(1)));
                    count++;
                }
                assertEquals(rows.size(), count);
            }
            engine.execute("insert into t values (4,2,'new-ž',-7.25,'2024-01-02')", context);
            check(base, candidate);
            engine.execute("alter table t convert partition to parquet list '2024-01-01'", context);
            check(base, candidate);
            var breaker = new AtomicBooleanCircuitBreaker(engine); context.with(breaker);
            assertThrows(CairoException.class, () -> {
                try (var cursor = candidate.getCursor(context)) { breaker.cancel(); cursor.hasNext(); }
            });
            context.with(new AtomicBooleanCircuitBreaker(engine));
            check(base, candidate);
            engine.execute("truncate table t", context);
            check(base, candidate);
            engine.execute("insert into t values (1,0,'replacement',3.25,'2024-01-03'),"
                    + "(2,1,null,null,'2024-01-03'),(3,1,'new-ž',-9.5,'2024-01-03')", context);
            check(base, candidate);
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test public void integerDivisionAndRemainderFeedNestedConsumers() throws Exception {
        for(String type:List.of("int","long")) {
            String inner=" from (select k,i,cast(count() as "+type+") v from t group by k,i)";
            for(String sql:List.of("select i,count(v/i),min(v%i),max(v/(i-1)),sum(v%(-3))"+inner+" group by i",
                    "select v%(i-1) r,i,count(),min(v/i)"+inner+" group by r,i")) {
                try(var candidate=compile(sql,true)) {check(sql,candidate);check(sql,candidate);}
            }
        }
    }

    @Test public void floatingIntegerCastsFeedNestedConsumers() throws Exception {
        for(String source:List.of("d","d::float")) {
            String inner=" from (select k,i,min("+source+") v from t group by k,i)";
            for(String sql:List.of("select i,count(v::byte),min(v::short),max(v::int),bit_and(v::byte),bit_xor(v::long)"+inner+" group by i",
                    "select v::byte b,i,count(),min(v::short),max(v::int)"+inner+" group by b,i")) {
                try(var candidate=compile(sql,true)) {check(sql,candidate);check(sql,candidate);}
            }
        }
    }

    @Test public void bitwiseResultsFeedNestedConsumersAcrossWidths() throws Exception {
        engine.execute("create table bit_nested as (select cast(x%257 as int) k,cast(x%7 as int) i,"
                + "cast(x%127+1 as byte) b,cast(x%32767+1 as short) h,"
                + "cast(case when x%13=0 then null else x%65535+1 end as int) n,case when x%13=0 then null else x%65535+1 end l from long_sequence(2003))",context);
        for (String argument : List.of("b", "h", "n", "l", "l::int", "n::byte", "h::byte")) {
            for (String aggregate : List.of("bit_and", "bit_or", "bit_xor")) {
                String inner=" from (select k,i,"+aggregate+"("+argument+") v from bit_nested group by k,i)";
                for (String sql : List.of("select i,count(),bit_and(v),bit_or(v),bit_xor(v)"+inner+" group by i",
                        "select v,count(),bit_and(v),bit_or(v),bit_xor(v)"+inner+" group by v")) {
                    try (var candidate=compile(sql,true)) { check(sql,candidate); check(sql,candidate); }
                }
            }
        }
    }

    @Test public void ipv4ResultsAndComputedKeysFeedNestedConsumers() throws Exception {
        engine.execute("create table ip_nested as (select cast(x%257 as int) k,cast(x%7 as int) i,"
                + "cast(case when x%13=0 then null when x%5=0 then -1 when x%5=1 then -2147483647 "
                + "else x%991 end as int)::ipv4 ip from long_sequence(2003))",context);
        engine.execute("insert into ip_nested values (1,1,'128.0.0.0'),(1,1,null)",context);
        for (String aggregate : List.of("min", "max")) {
            String inner=" from (select k,i,"+aggregate+"(ip) v from ip_nested group by k,i)";
            for (String sql : List.of("select i,count(v),min(v),max(v),count(cast(v as int))"+inner+" group by i",
                    "select v,count(),min(v),max(v)"+inner+" group by v",
                    "select i,count(v-1),min(v-1),max(v-1)"+inner+" group by i")) {
                try (var candidate=compile(sql,true)) { check(sql,candidate); check(sql,candidate); }
            }
        }
        String distinct="select i,sum(n),min(n),max(n) from (select k,i,count_distinct(ip) n from ip_nested group by k,i) group by i";
        try (var candidate=compile(distinct,true)) { check(distinct,candidate); check(distinct,candidate); }
        String sql="select address,sum(n) from (select k,ip-1 address,count() n from ip_nested group by k,address) group by address";
        try (var candidate=compile(sql,true)) { check(sql,candidate); check(sql,candidate); }
    }

    @Test public void booleanResultsAndKeysFeedNestedConsumers() throws Exception {
        engine.execute("create table bool_nested as (select cast(x%257 as int) k,cast(x%7 as int) i,"
                + "x%3=0 b from long_sequence(2003))",context);
        for (String aggregate : List.of("bool_and", "bool_or")) {
            String inner = " from (select k,i," + aggregate + "(b) v from bool_nested group by k,i)";
            for (String sql : List.of("select i,count(),bool_and(v),bool_or(v),avg(v),min(v),max(v)" + inner + " group by i",
                    "select v,count(),bool_and(v),bool_or(v)" + inner + " group by v",
                    "select i,bool_and(not v),bool_or(v and i>2),sum(cast(v as long))" + inner + " group by i")) {
                try (var candidate = compile(sql,true)) { check(sql,candidate); check(sql,candidate); }
            }
        }
        String sql = "select b,sum(n),bool_and(b),avg(b) from (select k,b,count() n from bool_nested group by k,b) group by b";
        try (var candidate = compile(sql,true)) { check(sql,candidate); check(sql,candidate); }
    }

    @Test public void temporalResultsFeedNestedAggregateAndKeyConsumers() throws Exception {
        for (String type : List.of("date", "timestamp", "timestamp_ns")) {
            String table = "times_" + type;
            engine.execute("create table " + table + " as (select cast(x%257 as int) k,cast(x%7 as int) i,"
                    + "cast(case when x%7=0 then null else (x%991-495)*1000001 end as " + type + ") t "
                    + "from long_sequence(5003))", context);
            for (String sql : List.of("select i,count(v),min(v),max(v),sum(v),avg(v),sum(v::double),max(v::float) from (select k,i,min(t) v from "
                    + table + " group by k,i) group by i",
                    "select v,count() from (select k,i,min(t) v from " + table + " group by k,i) group by v")) {
                try (var candidate = compile(sql, true)) { check(sql, candidate); check(sql, candidate); }
            }
        }
    }

    @Test public void staticSymbolIdsKeepDictionaryOwnershipAcrossReopenAndTruncate() throws Exception {
        String sql = "select s,count(s),count(),sum(v),min(v),max(v) "
                + "from (select k,s,min(d) v from t group by k,s) group by s";
        try (var candidate = compile(sql, true)) {
            check(sql, candidate);
            try (var ignored = candidate.getCursor(context)) { }
            check(sql, candidate);
            engine.execute("insert into t values (4,2,'new-ž',-7.25,'2024-01-02')", context);
            check(sql, candidate);
            engine.execute("alter table t convert partition to parquet list '2024-01-01'", context);
            check(sql, candidate);
            engine.execute("truncate table t", context);
            check(sql, candidate);
            engine.execute("insert into t values (1,0,'replacement',3.25,'2024-01-03'),"
                    + "(2,1,null,null,'2024-01-03'),(3,1,'new-ž',-9.5,'2024-01-03')", context);
            check(sql, candidate);
            var breaker = new AtomicBooleanCircuitBreaker(engine); context.with(breaker);
            assertThrows(CairoException.class, () -> {
                try (var cursor = candidate.getCursor(context)) {
                    breaker.cancel();
                    cursor.hasNext();
                }
            });
            context.with(new AtomicBooleanCircuitBreaker(engine));
            check(sql, candidate);
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test public void floatGroupingPreservesRawKeysAcrossNestedConsumers() throws Exception {
        engine.execute("create table float_edges (k long,f float)", context);
        int[] bits = {0, 0x80000000, 0x7fc00000, 0x7fc01234, 0xffc05678, 0x7f800001, 0xff800001,
                0x7f800000, 0xff800000, 1, 0x80000001, 0x3f800000};
        try (var writer = engine.getWriter(engine.verifyTableName("float_edges"), "test")) {
            for (int n = 0; n < 1031; n++) {
                var row = writer.newRow(); row.putLong(0, n%257);
                row.putFloat(1, Float.intBitsToFloat(bits[n%bits.length])); row.append();
            }
            writer.commit();
        }
        for (String value : List.of("f", "-f", "case when k>0 then f else null::float end")) {
            String sql = "select g,sum(n),min(g),max(g),avg(g) from (select k," + value
                    + " g,count() n from float_edges group by k,g) group by g";
            try (var candidate = compile(sql, true)) {
                var actual = rawGroups(candidate);
                context.setQueryCodeGenerator(null);
                try (var ordinary = Demo.compileSql(engine, context, sql, true)) { assertEquals(rawGroups(ordinary), actual); }
                if (value.equals("f")) assertEquals(bits.length, actual.size());
                check(sql, candidate);
            }
        }
    }

    @Test public void doubleGroupingPreservesNonfiniteValuesAndRawKeyBits() throws Exception {
        engine.execute("create table edges (k long,d double)", context);
        double[] values = {0.0, -0.0, Double.NaN, Double.longBitsToDouble(0x7ff8000000001234L),
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, -Double.MIN_VALUE, 1, -17.25};
        try (var writer = engine.getWriter(engine.verifyTableName("edges"), "test")) {
            for (int n = 0; n < 1031; n++) {
                var row = writer.newRow(); row.putLong(0, n%257); row.putDouble(1, values[n%values.length]); row.append();
            }
            writer.commit();
        }
        String sql = "select d,sum(n) from (select k,d,count() n from edges group by k,d) group by d";
        try (var candidate = compile(sql, true)) {
            var actual = rawGroups(candidate);
            context.setQueryCodeGenerator(null);
            try (var ordinary = Demo.compileSql(engine, context, sql, true)) { assertEquals(rawGroups(ordinary), actual); }
            assertEquals(values.length, actual.size());
        }
    }

    private static long rawKey(RecordCursorFactory factory, io.questdb.cairo.sql.Record record) {
        return factory.getMetadata().getColumnType(0) == io.questdb.cairo.ColumnType.FLOAT
                ? Float.floatToRawIntBits(record.getFloat(0)) : Double.doubleToRawLongBits(record.getDouble(0));
    }

    private java.util.Map<Long,Long> rawGroups(RecordCursorFactory factory) throws Exception {
        var result = new java.util.TreeMap<Long,Long>();
        try (var cursor = factory.getCursor(context)) {
            var record = cursor.getRecord();
            while (cursor.hasNext()) assertNull(result.put(rawKey(factory, record), record.getLong(1)));
            cursor.toTop(); int count = 0;
            while (cursor.hasNext()) { assertEquals(result.get(rawKey(factory, record)).longValue(), record.getLong(1)); count++; }
            assertEquals(result.size(), count);
        }
        return result;
    }

    private RecordCursorFactory compile(String sql, boolean generated) throws Exception {
        context.setQueryCodeGenerator(compiler);
        var factory = Demo.compileSql(engine, context, sql, true);
        try {
            var sink = new TextPlanSink(); sink.of(factory, context);
            assertEquals(sink.getSink().toString(), generated, sink.getSink().toString().contains("compiledMapInput"));
            return factory;
        } catch (Throwable failure) { factory.close(); throw failure; }
    }

    private void check(String sql, RecordCursorFactory candidate) throws Exception { check(sql, candidate, 0); }

    private void check(String sql, RecordCursorFactory candidate, int statisticalColumns) throws Exception {
        var actual = Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) {
            var expected = Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount());
            if (statisticalColumns == 0) assertEquals(sql, expected, actual);
            else AggregateTypeTest.assertApproximateTail(sql, expected, actual, statisticalColumns);
        } finally { context.setQueryCodeGenerator(compiler); }
    }
}
