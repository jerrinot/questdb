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
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;
    private String previousCapacity;

    public MapAggregateTypeTest(String name, Backend backend) { this.backend = backend; }
    @Parameterized.Parameters(name = "{0}") public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()},
                new Object[]{"c", new NativeBackend()});
    }

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        previousCapacity = System.getProperty("query.codegen.localCapacity");
        System.setProperty("query.codegen.localCapacity", "64");
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
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
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
        if (previousCapacity == null) System.clearProperty("query.codegen.localCapacity");
        else System.setProperty("query.codegen.localCapacity", previousCapacity);
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
                "select v,count() from (select k,min(i) v from t group by k) group by v")) {
            try (var candidate = compile(sql, true)) { check(sql, candidate); }
        }
        for (String sql : List.of(
                "select v,count() from (select k,min(d) v from t group by k) where v>0 group by v",
                "select s,avg(v) from (select k,s,min(d) v from t group by k,s) group by s")) {
            try (var candidate = compile(sql, false)) { check(sql, candidate); }
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

    private java.util.Map<Long,Long> rawGroups(RecordCursorFactory factory) throws Exception {
        var result = new java.util.TreeMap<Long,Long>();
        try (var cursor = factory.getCursor(context)) {
            var record = cursor.getRecord();
            while (cursor.hasNext()) assertNull(result.put(Double.doubleToRawLongBits(record.getDouble(0)), record.getLong(1)));
            cursor.toTop(); int count = 0;
            while (cursor.hasNext()) { assertEquals(result.get(Double.doubleToRawLongBits(record.getDouble(0))).longValue(), record.getLong(1)); count++; }
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

    private void check(String sql, RecordCursorFactory candidate) throws Exception {
        var actual = Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) {
            assertEquals(sql, Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount()), actual);
        } finally { context.setQueryCodeGenerator(compiler); }
    }
}
