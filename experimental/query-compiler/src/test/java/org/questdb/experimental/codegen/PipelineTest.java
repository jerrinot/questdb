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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Pipeline.*;

@RunWith(Parameterized.class)
public class PipelineTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;

    public PipelineTest(String name, Backend backend) { this.backend = backend; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend(2)}, new Object[]{"native", new NativeBackend()}, new Object[]{"asmjit", new AsmJitBackend()});
    }

    @Before
    public void setUp() throws Exception {
        var configuration = new DefaultCairoConfiguration(temporary.newFolder("db").toString()) {
            @Override
            public int getSqlPageFrameMinRows() { return 16; }
            @Override
            public int getSqlPageFrameMaxRows() { return 32; }
        };
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        // Use the existing scalar path as a semantic oracle, independently of our generated code.
        context.setParallelGroupByEnabled(false);
    }

    @After
    public void tearDown() {
        if (context != null) { context.close(); }
        if (engine != null) { engine.close(); }
    }

    @Test
    public void q7AcrossFramesAndPartitions() throws Exception {
        engine.execute("""
                create table hits as (
                    select cast(x % 17 - 8 as short) AdvEngineID,
                           timestamp_sequence('2024-01-01', 3600000000L) ts
                    from long_sequence(1000)
                ) timestamp(ts) partition by day bypass wal
                """, context);
        Column key = new Column(0, "AdvEngineID", ColumnType.SHORT);
        Pipeline pipeline = new Pipeline(List.of(key), new Compare(Ir.Op.NE, key, new Constant(0)),
                List.of(key), List.of(new Count()));
        assertMatches("hits", pipeline, "select AdvEngineID, count(*) from hits where AdvEngineID <> 0 group by AdvEngineID", 32);
    }

    @Test
    public void nullsMultipleKeysExpressionsAndAggregates() throws Exception {
        engine.execute("create table t (a int, b long, v long)", context);
        engine.execute("""
                insert into t values
                    (1, 2, 10), (1, 2, -4), (1, 2, null),
                    (1, 3, null), (null, 2, 9), (null, 2, null),
                    (2, null, 7), (null, null, null), (-7, -9, 100)
                """, context);
        Column a = new Column(0, "a", ColumnType.INT);
        Column b = new Column(1, "b", ColumnType.LONG);
        Column v = new Column(2, "v", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(a, b, v), new All(), List.of(a, b),
                List.of(new Count(), new CountValue(v), new Sum(v), new Sum(new Add(v, new Constant(3)))));
        assertMatches("t", pipeline, "select a, b, count(*), count(v), sum(v), sum(v+3) from t group by a,b", 16);
    }

    @Test
    public void columnTopsAndNullRejectingFilter() throws Exception {
        engine.execute("create table t (k int)", context);
        engine.execute("insert into t values (1), (1), (2)", context);
        engine.execute("alter table t add column v long", context);
        engine.execute("insert into t values (1, 10), (2, 20), (2, null), (3, -3)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        var aggregates = List.<Aggregate>of(new Count(), new CountValue(v), new Sum(v));
        assertMatches("t", new Pipeline(List.of(k, v), new All(), List.of(k), aggregates),
                "select k, count(*), count(v), sum(v) from t group by k", 8);
        assertMatches("t", new Pipeline(List.of(k, v),
                        new And(new IsNotNull(v), new Compare(Ir.Op.GE, v, new Constant(0))), List.of(k), aggregates),
                "select k, count(*), count(v), sum(v) from t where v is not null and v >= 0 group by k", 8);
    }

    @Test
    public void emptyInputAndReexecutionAfterAppend() throws Exception {
        engine.execute("create table t (k int)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Pipeline pipeline = new Pipeline(List.of(k), new Compare(Ir.Op.NE, k, new Constant(0)), List.of(k), List.of(new Count()));
        try (var query = compile("t", pipeline)) {
            assertTrue(query.execute(context, 8).isEmpty());
            engine.execute("insert into t values (0), (0)", context);
            assertTrue(query.execute(context, 8).isEmpty());
            engine.execute("insert into t values (1), (1), (2)", context);
            var expected = sql("select k, count(*) from t where k <> 0 group by k", 1);
            assertEquals(expected, query.execute(context, 8));
            assertEquals(expected, query.execute(context, 8));
        }
    }

    @Test
    public void boundedTableFailureAndRecovery() throws Exception {
        engine.execute("create table t (k long)", context);
        engine.execute("insert into t values (1), (1), (2), (3), (4), (1), (4)", context);
        Column k = new Column(0, "k", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(k), new All(), List.of(k), List.of(new Count()));
        CompiledGroupBy query = compile("t", pipeline);
        try (query) {
            var failure = assertThrows(IllegalStateException.class, () -> query.execute(context, 1));
            assertTrue(failure.getMessage().contains("Group table is full"));
            assertThrows(IllegalArgumentException.class, () -> query.execute(context, 0));
            assertThrows(IllegalArgumentException.class, () -> query.execute(context, 3));
            var expected = sql("select k, count(*) from t group by k", 1);
            assertEquals(expected, query.execute(context, 4)); // Exactly full still supports existing keys.
            assertEquals(expected, query.execute(context, 16));
        }
        query.close();
        assertThrows(IllegalStateException.class, () -> query.execute(context, 16));
    }

    @Test
    public void randomizedGroupsAndSkew() throws Exception {
        engine.execute("""
                create table t as (
                    select cast(rnd_int(0, 31, 5) as int) k,
                           rnd_long(-100000, 100000, 7) v,
                           cast(x % 5 as byte) f
                    from long_sequence(5000)
                )
                """, context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Column f = new Column(2, "f", ColumnType.BYTE);
        Pipeline pipeline = new Pipeline(List.of(k, v, f), new Compare(Ir.Op.NE, f, new Constant(0)),
                List.of(k), List.of(new Count(), new Sum(v)));
        assertMatches("t", pipeline, "select k, count(*), sum(v) from t where f <> 0 group by k", 64);
    }

    @Test
    public void slicesLargeFramesWithoutLosingRows() throws Exception {
        context.changePageFrameSizes(200_000, 200_000);
        engine.execute("create table t as (select cast(x % 23 as int) k, x v from long_sequence(150000))", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        assertMatches("t", new Pipeline(List.of(k, v), new All(), List.of(k), List.of(new Count(), new Sum(v))),
                "select k, count(*), sum(v) from t group by k", 32);
    }

    @Test
    public void observesCancellationEvenForEmptyInput() throws Exception {
        engine.execute("create table t (k int)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Pipeline pipeline = new Pipeline(List.of(k), new All(), List.of(k), List.of(new Count()));
        try (var query = compile("t", pipeline)) {
            var breaker = new AtomicBooleanCircuitBreaker(engine);
            context.with(breaker);
            breaker.cancel();
            assertThrows(CairoException.class, () -> query.execute(context, 8));
        }
    }

    @Test
    public void rejectsParquetBeforeReadingRawAddresses() throws Exception {
        engine.execute("""
                create table t as (
                    select cast(x as int) k, timestamp_sequence('2024-01-01', 86400000000L) ts
                    from long_sequence(10)
                ) timestamp(ts) partition by day bypass wal
                """, context);
        engine.execute("alter table t convert partition to parquet where ts >= '2024-01-01'", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Pipeline pipeline = new Pipeline(List.of(k), new All(), List.of(k), List.of(new Count()));
        try (var query = compile("t", pipeline)) {
            var error = assertThrows(IllegalArgumentException.class, () -> query.execute(context, 16));
            assertTrue(error.getMessage().contains("Parquet"));
        }
    }

    @Test
    public void doesNotBypassViewFilters() throws Exception {
        engine.execute("create table t (k int)", context);
        engine.execute("create view v as select * from t where k = 1", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Pipeline pipeline = new Pipeline(List.of(k), new All(), List.of(k), List.of(new Count()));
        assertThrows(IllegalArgumentException.class, () -> compile("v", pipeline));
    }

    @Test
    public void groupedDistinctPreservesNullGroupsAndColumnTops() throws Exception {
        engine.execute("create table t (k int)", context);
        engine.execute("insert into t values (null), (-1), (1), (1)", context);
        engine.execute("alter table t add column v long", context);
        engine.execute("insert into t values (1, null), (1, 5), (1, 5), (null, 5), (2, null), (3, 10), (3, 20), (2, null)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(k, v), new All(), List.of(k), List.of(new CountDistinct(v)));
        assertDistinctMatches("t", pipeline, "select k, count_distinct(v) from t group by k", 8, 32);
        try (var control = CompiledGroupBy.withKernels(engine, context, "t", pipeline,
                HandwrittenKernels::distinct, HandwrittenKernels::collapse)) {
            assertEquals(sql("select k, count_distinct(v) from t group by k", 1), control.execute(context, 8, 32));
        }
    }

    @Test
    public void distinctAcrossPartitionsWithExpressionsAndFilters() throws Exception {
        engine.execute("""
                create table t as (
                    select cast(x % 7 as int) a, x % 3 b, x % 13 v,
                           timestamp_sequence('2024-01-01', 3600000000L) ts
                    from long_sequence(500)
                ) timestamp(ts) partition by day bypass wal
                """, context);
        Column a = new Column(0, "a", ColumnType.INT);
        Column b = new Column(1, "b", ColumnType.LONG);
        Column v = new Column(2, "v", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(a, b, v), new Compare(Ir.Op.NE, v, new Constant(0)),
                List.of(a, new Add(b, new Constant(3))), List.of(new CountDistinct(new Add(v, new Constant(1)))));
        assertDistinctMatches("t", pipeline,
                "select a, b+3, count_distinct(v+1) from t where v <> 0 group by a,b+3", 32, 512);
    }

    @Test
    public void distinctCapacityFailureInEitherPhaseAndRecovery() throws Exception {
        engine.execute("create table t (k int, v long)", context);
        engine.execute("insert into t values (1,1), (1,1), (1,2), (2,1), (2,null), (1,1), (2,null)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(k, v), new All(), List.of(k), List.of(new CountDistinct(v)));
        try (var query = compile("t", pipeline)) {
            var pairFailure = assertThrows(IllegalStateException.class, () -> query.execute(context, 2, 2));
            assertTrue(pairFailure.getMessage().contains("Distinct pair table is full"));
            var groupFailure = assertThrows(IllegalStateException.class, () -> query.execute(context, 1, 4));
            assertTrue(groupFailure.getMessage().contains("Group table is full"));
            assertThrows(IllegalArgumentException.class, () -> query.execute(context, 2, 3));
            var expected = sql("select k, count_distinct(v) from t group by k", 1);
            assertEquals(expected, query.execute(context, 2, 4));
            assertEquals(expected, query.execute(context, 8, 16));
        }
        assertThrows(IllegalArgumentException.class, () -> new Pipeline(List.of(k, v), new All(), List.of(k),
                List.of(new Count(), new CountDistinct(v))));
    }

    @Test
    public void distinctEmptyInputAndReexecution() throws Exception {
        engine.execute("create table t (k int, v long)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(k, v), new All(), List.of(k), List.of(new CountDistinct(v)));
        try (var query = compile("t", pipeline)) {
            assertTrue(query.execute(context, 8, 16).isEmpty());
            engine.execute("insert into t values (1,null), (1,null), (2,3), (2,3)", context);
            var expected = sql("select k, count_distinct(v) from t group by k", 1);
            assertEquals(expected, query.execute(context, 8, 16));
            assertEquals(expected, query.execute(context, 8, 16));
        }
    }

    @Test
    public void distinctCollapseSpansMultipleKernelCalls() throws Exception {
        context.changePageFrameSizes(200_000, 200_000);
        engine.execute("create table t as (select cast(x % 23 as int) k, x v from long_sequence(150000))", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(k, v), new All(), List.of(k), List.of(new CountDistinct(v)));
        assertDistinctMatches("t", pipeline, "select k, count_distinct(v) from t group by k", 32, 262_144);
    }

    @Test
    public void handwrittenMixedControlMatchesSql() throws Exception {
        engine.execute("create table t (k int, v long, f byte)", context);
        engine.execute("insert into t values (1,5,1), (1,null,1), (null,7,1), (2,-3,1), (2,5,0), (1,5,1)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Column f = new Column(2, "f", ColumnType.BYTE);
        Pipeline pipeline = new Pipeline(List.of(k, v, f), new Compare(Ir.Op.NE, f, new Constant(0)), List.of(k),
                List.of(new Count(), new CountValue(v), new Sum(new Add(v, new Constant(3)))));
        try (var query = CompiledGroupBy.withKernels(engine, context, "t", pipeline, HandwrittenKernels::mixed, null)) {
            assertEquals(sql("select k,count(*),count(v),sum(v+3) from t where f<>0 group by k", 1), query.execute(context, 8));
        }
    }

    @Test
    public void nestedDistinctMixedAggregateOrderAndIndependentNulls() throws Exception {
        engine.execute("create table t (k int)", context);
        engine.execute("insert into t values (1), (2), (null)", context);
        engine.execute("alter table t add column v long", context);
        engine.execute("alter table t add column m long", context);
        engine.execute("insert into t values (1,10,5), (1,10,null), (1,20,-5), (1,null,7), (2,null,null), (3,0,0), (3,-1,9), (null,10,-1)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Column m = new Column(2, "m", ColumnType.LONG);
        var columns = List.of(k, v, m);
        assertMatches("t", new Pipeline(columns, new All(), List.of(k),
                        List.of(new NestedCountDistinct(v), new Count(), new Sum(m), new CountValue(m))),
                "select k,count_distinct(v),count(*),sum(m),count(m) from t group by k", 8);
        assertMatches("t", new Pipeline(columns, new All(), List.of(k),
                        List.of(new Count(), new NestedCountDistinct(v), new Sum(m))),
                "select k,count(*),count_distinct(v),sum(m) from t group by k", 8);
        assertMatches("t", new Pipeline(columns, new Compare(Ir.Op.NE, k, new Constant(2)), List.of(k),
                        List.of(new Sum(new Add(m, new Constant(3))), new CountValue(v), new NestedCountDistinct(v))),
                "select k,sum(m+3),count(v),count_distinct(v) from t where k is not null and k <> 2 group by k", 8);
    }

    @Test
    public void nestedMixedGrowsAcrossLargeFramesAndMatchesControl() throws Exception {
        context.changePageFrameSizes(200_000, 200_000);
        engine.execute("""
                create table t as (
                    select cast(x % 23 as int) k,
                           case when x % 97 = 0 then null else x end v,
                           cast(x % 19 - 9 as short) a
                    from long_sequence(150000)
                )
                """, context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Column a = new Column(2, "a", ColumnType.SHORT);
        var pipeline = new Pipeline(List.of(k, v, a), new All(), List.of(k),
                List.of(new Sum(a), new Count(), new NestedCountDistinct(v)));
        String sql = "select k,sum(a),count(*),count_distinct(v) from t group by k";
        assertMatches("t", pipeline, sql, 32);
        try (var control = CompiledGroupBy.withKernels(engine, context, "t", pipeline, HandwrittenKernels.nestedMixed(3, 0.7), null)) {
            assertEquals(sql(sql, 1), control.execute(context, 32));
        }
    }

    @Test
    public void rejectsMismatchedOrUnsupportedSchema() throws Exception {
        engine.execute("create table t (k int)", context);
        Column wrong = new Column(0, "k", ColumnType.LONG);
        Pipeline pipeline = new Pipeline(List.of(wrong), new All(), List.of(wrong), List.of(new Count()));
        assertThrows(IllegalArgumentException.class, () -> compile("t", pipeline));
        assertThrows(IllegalArgumentException.class, () -> new Column(0, "s", ColumnType.VARCHAR));
        Column foreign = new Column(0, "different", ColumnType.LONG);
        assertThrows(IllegalArgumentException.class,
                () -> new Pipeline(List.of(wrong), new All(), List.of(foreign), List.of(new Count())).lower());
    }

    private void assertDistinctMatches(String table, Pipeline pipeline, String sql, int capacity, int pairCapacity) throws Exception {
        try (var query = compile(table, pipeline)) {
            var expected = sql(sql, pipeline.keys().size());
            assertEquals(expected, query.execute(context, capacity, pairCapacity));
            assertEquals(expected, query.execute(context, capacity, pairCapacity));
        }
        var argument = ((CountDistinct) pipeline.aggregates().getFirst()).argument();
        var nested = new Pipeline(pipeline.columns(), pipeline.filter(), pipeline.keys(), List.of(new NestedCountDistinct(argument)));
        try (var query = compile(table, nested)) {
            var expected = sql(sql, pipeline.keys().size());
            assertEquals(expected, query.execute(context, capacity));
            assertEquals(expected, query.execute(context, capacity));
        }
    }

    @Test
    public void nestedDistinctFailureCleanupAndRecovery() throws Exception {
        engine.execute("create table t (k int, v long)", context);
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        var pipeline = new Pipeline(List.of(k, v), new All(), List.of(k), List.of(new NestedCountDistinct(v)));
        try (var query = compile("t", pipeline)) {
            assertTrue(query.execute(context, 1).isEmpty());
            engine.execute("insert into t values (1,0), (1,1), (1,2), (1,3), (2,null), (2,null)", context);
            var failure = assertThrows(IllegalStateException.class, () -> query.execute(context, 1));
            assertTrue(failure.getMessage().contains("Group table is full"));
            assertEquals(sql("select k,count_distinct(v) from t group by k", 1), query.execute(context, 2));
            var breaker = new AtomicBooleanCircuitBreaker(engine);
            context.with(breaker);
            breaker.cancel();
            assertThrows(CairoException.class, () -> query.execute(context, 2));
        }
    }

    private void assertMatches(String table, Pipeline pipeline, String sql, int capacity) throws Exception {
        try (var query = compile(table, pipeline)) {
            var expected = sql(sql, pipeline.keys().size());
            assertEquals(expected, query.execute(context, capacity, capacity, new ExecutionTimings()));
            assertEquals(expected, query.execute(context, capacity));
        }
    }

    private CompiledGroupBy compile(String table, Pipeline pipeline) throws Exception {
        return CompiledGroupBy.compile(engine, context, table, pipeline, backend, temporary.getRoot().toPath().resolve("generated"));
    }

    private List<CompiledGroupBy.ResultRow> sql(String sql, int keyCount) throws Exception {
        List<CompiledGroupBy.ResultRow> result = new ArrayList<>();
        try (var compiler = engine.getSqlCompiler();
             var factory = compiler.compile(sql, context).getRecordCursorFactory();
             var cursor = factory.getCursor(context)) {
            while (cursor.hasNext()) {
                Record record = cursor.getRecord();
                List<Long> keys = new ArrayList<>();
                List<Long> values = new ArrayList<>();
                for (int i = 0; i < factory.getMetadata().getColumnCount(); i++) {
                    Long value = switch (factory.getMetadata().getColumnType(i)) {
                        case ColumnType.BYTE -> (long) record.getByte(i);
                        case ColumnType.SHORT -> (long) record.getShort(i);
                        case ColumnType.INT -> record.getInt(i) == Integer.MIN_VALUE ? null : (long) record.getInt(i);
                        case ColumnType.LONG -> record.getLong(i) == Long.MIN_VALUE ? null : record.getLong(i);
                        default -> throw new AssertionError("Unexpected result type");
                    };
                    (i < keyCount ? keys : values).add(value);
                }
                result.add(new CompiledGroupBy.ResultRow(keys, values));
            }
        }
        Collections.sort(result);
        return result;
    }
}
