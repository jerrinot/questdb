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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.join.CompiledAsOfJoin;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.std.ConcurrentAssociativeCache;
import io.questdb.std.DefaultConcurrentCacheConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class AsOfSqlTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private ResolvedQueryCompiler compiler;
    private long nativeMemory;
    private long functions;
    private int mapLimit = 1024 * 1024;

    @Before public void setUp() throws Exception {
        nativeMemory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        functions = LlvmBackend.liveFunctions();
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMinRows() { return 64; }
            @Override public int getSqlPageFrameMaxRows() { return 256; }
            @Override public int getSqlSmallMapKeyCapacity() { return 4; }
            @Override public long getSqlSmallMapPageSize() { return 1024; }
            @Override public int getSqlHashJoinValuePageSize() { return mapLimit; }
        };
        engine = new CairoEngine(config);
        context = new SqlExecutionContextImpl(engine, 1).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        context.setParallelFilterEnabled(true);
        context.setParallelHorizonJoinEnabled(false);
        compiler = new ResolvedQueryCompiler(new LlvmBackend(), temporary.newFolder().toPath());
        engine.execute("create table r as (select cast(x/3*1000000 as timestamp) ts,"
                + "cast(case when x%41=0 then null else x%97 end as int) k,"
                + "case when x%41=0 then null else (x%97-48)*4294967297 end kl,cast(case when x%41=0 then null else 's'||(x%97) end as symbol) ks,"
                + "cast('v'||(x%3) as varchar) text,x v from long_sequence(40000)) timestamp(ts) partition by hour", context);
        engine.execute("create table l as (select cast((10000+x)/3*1000000000 as timestamp_ns) ts,"
                + "cast(case when x%37=0 then null else x*31%97 end as int) k,"
                + "case when x%37=0 then null else (x*31%97-48)*4294967297 end kl,cast(case when x%37=0 then null else 's'||(x*31%97) end as symbol) ks "
                + "from long_sequence(2048)) timestamp(ts)", context);
    }

    @After public void tearDown() {
        if (context != null) context.close();
        if (engine != null) { assertEquals(0, engine.getBusyReaderCount()); engine.close(); }
        assertEquals(nativeMemory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(functions, LlvmBackend.liveFunctions());
    }

    @Test public void typedKeysProjectionAndReopen() throws Exception {
        for (String keys : new String[]{"k", "kl", "ks", "k,kl", "ks,kl"}) {
            matches("a.ts,b.v from l a asof join (select ks,v,kl,ts,k from r where v-v/3*3=0) b on ("
                    + keys + ")", true);
            matches("a.ts,b.v from l a asof join (select ks,v,kl,ts,k from r where v-v/3*3=0) b on ("
                    + keys + ") tolerance 20s", true);
            matches("a.ts,b.v from l a asof join (select ks,v,kl,ts,k from r where k-k/7*7=0) b on ("
                    + keys + ")", true);
        }
    }

    @Test public void backwardLookupTypedKeysToleranceAndProjection() throws Exception {
        for (String key : new String[]{"k", "kl", "ks"}) {
            for (String tolerance : new String[]{"", " tolerance 20s"}) {
                matches("a.ts,b.v from l a asof join (select ks,v,kl,ts,k from r where v-v/3*3=0) b on ("
                        + key + ")" + tolerance, false, true);
            }
        }
    }

    @Test public void immediateMatchesDoNotCompileBackwardLookup() throws Exception {
        engine.execute("create table instant as (select cast(x as timestamp) ts,cast(x as int) k,x v "
                + "from long_sequence(32)) timestamp(ts)", context);
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context,
                "select a.ts,b.v from instant a asof join (select * from instant where v>=0) b on (k)", true)) {
            RecordCursorFactory base = factory;
            while (base != null && !(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
            assertNotNull(base);
            var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
            long compiled = LlvmBackend.liveFunctions();
            for (int reopen = 0; reopen < 2; reopen++) {
                try (var cursor = factory.getCursor(context)) {
                    for (int rewind = 0; rewind < 2; rewind++) {
                        long rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                            assertEquals(rows, cursor.getRecord().getLong(1));
                        }
                        assertEquals(32, rows);
                        assertEquals(0, asof.getNativeLookupRows());
                        assertEquals(compiled, LlvmBackend.liveFunctions());
                        cursor.toTop();
                    }
                }
            }
        } finally { context.setQueryCodeGenerator(null); }
    }

    @Test public void cachedFactoryRetainsLookupCode() throws Exception {
        int[] compilations = {0};
        Backend backend = new Backend() {
            private final Backend delegate = new LlvmBackend();
            @Override public String name() { return delegate.name(); }
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path artifacts) throws java.io.IOException {
                compilations[0]++;
                return delegate.compile(program, artifacts);
            }
        };
        var generator = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
        for (String key : new String[]{"k", "kl", "ks"}) {
            String body = "a.ts,b.v from l a asof join (select * from r where v-v/3*3=0) b on (" + key + ")";
            String sql = "select " + body;
            List<String> expected;
            try (var reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true);
                 var cursor = reference.getCursor(context)) {
                expected = read(cursor);
            }
            long handles = LlvmBackend.liveFunctions();
            // Same factory cache and close-cursor/put/poll ownership used by HTTP SELECT.
            try (var cache = new ConcurrentAssociativeCache<RecordCursorFactory>(DefaultConcurrentCacheConfiguration.DEFAULT)) {
                context.setQueryCodeGenerator(generator);
                var factory = Demo.compileSql(engine, context, sql, true);
                cache.put(sql, factory);
                int compiled = -1;
                for (int execution = 0; execution < 3; execution++) {
                    var cached = cache.poll(sql);
                    assertSame(factory, cached);
                    try {
                        RecordCursorFactory base = cached;
                        while (base != null && !(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
                        assertNotNull(base);
                        try (var cursor = cached.getCursor(context)) {
                            assertEquals(expected, read(cursor));
                            assertTrue(((FilteredAsOfJoinFastRecordCursorFactory) base).getNativeLookupRows() > 0);
                        }
                        if (execution == 0) compiled = compilations[0];
                        else assertEquals("Cache hit must retain native code", compiled, compilations[0]);
                    } finally { cache.put(sql, cached); }
                }
                assertTrue(LlvmBackend.liveFunctions() > handles);
                cache.clear();
                assertEquals("Eviction must release native code", handles, LlvmBackend.liveFunctions());
            } finally { context.setQueryCodeGenerator(null); }
        }
    }

    @Test public void varcharPredicateAndParquetFrames() throws Exception {
        for (int storage = 0; storage < 2; storage++) {
            if (storage == 1) engine.execute("alter table r convert partition to parquet where ts < '1970-01-01T02'", context);
            matches("a.ts,b.v from l a asof join (select * from r where text='v0') b on (kl)", true);
            matches("a.ts,b.v from l a asof join (select * from r where ks in ('s0','s1','s2','s3')) b on (ks)", true);
        }
    }

    @Test public void boundedMapRefusalRetriesTheSameProbe() throws Exception {
        mapLimit = 4096;
        engine.execute("create table hr as (select cast(x as timestamp) ts,cast(x as int) k,x v from long_sequence(100000)) timestamp(ts)", context);
        engine.execute("create table hl as (select cast(90000+x as timestamp) ts,cast(case when x>512 then 90000+x else -1 end as int) k from long_sequence(1024)) timestamp(ts)", context);
        matches("a.ts,b.v from hl a asof join (select * from hr where v>0) b on (k)", true);
    }

    @Test public void columnTopFallsBackWithinTheSameExecution() throws Exception {
        engine.execute("alter table r add column added long", context);
        engine.execute("insert into r select cast((40000+x)/3*1000000 as timestamp),cast(x%97 as int),"
                + "(x%97-48)*4294967297,cast('s'||(x%97) as symbol),cast('v0' as varchar),40000+x,x from long_sequence(2000)", context);
        engine.execute("insert into l select cast((40000+x)/3*1000000000 as timestamp_ns),cast(x*31%97 as int),"
                + "(x*31%97-48)*4294967297,cast('s'||(x*31%97) as symbol) from long_sequence(1024)", context);
        matches("a.ts,b.v from l a asof join (select * from r where added is null or added>0) b on (kl)", true);
    }

    @Test public void filterBatchesUseExistingVariableAndFloatingKeySinks() throws Exception {
        engine.execute("create table rk as (select ts,cast(k as double) kd,cast(ks as varchar) kv,v from r) timestamp(ts)", context);
        engine.execute("create table lk as (select ts,cast(k as double) kd,cast(ks as varchar) kv from l) timestamp(ts)", context);
        for (String key : new String[]{"kd", "kv", "kd,kv"}) {
            matches("a.ts,b.v from lk a asof join (select * from rk where v-v/3*3=0) b on (" + key + ")", true);
        }
    }

    @Test public void batchAllocationFailureCancellationAndReopenReleaseMemory() throws Exception {
        context.setQueryCodeGenerator(compiler);
        String sql = "select a.ts,b.v from l a asof join (select * from r where text='v0') b on (kl)";
        try (RecordCursorFactory factory = Demo.compileSql(engine, context, sql, true);
             var tracker = new io.questdb.std.MemoryTracker() {
                 @Override public long getQueryId() { return -1; }
                 @Override public io.questdb.std.MemoryTrackerWorkload getWorkload() { return io.questdb.std.MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }) {
            RecordCursorFactory base = factory;
            while (base != null && !(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
            assertNotNull(base);
            var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
            context.setMemoryTracker(tracker);
            try {
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 1);
                assertThrows(io.questdb.cairo.CairoException.class, () -> {
                    try (var cursor = asof.getCursor(context)) { cursor.hasNext(); }
                });
                assertEquals(0, tracker.getUsed());
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                for (int attempt = 0; attempt < 2; attempt++) {
                    var breaker = new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine);
                    context.with(breaker);
                    try (var cursor = asof.getCursor(context)) {
                        for (int i = 0; i < 1024; i++) assertTrue(cursor.hasNext());
                        assertTrue(asof.getCompiledFilterRows() > 0);
                        assertTrue(tracker.getUsed() > 0);
                        breaker.cancel();
                        assertThrows(io.questdb.cairo.CairoException.class, cursor::hasNext);
                    }
                    assertEquals(0, tracker.getUsed());
                }
                context.with(new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine));
                try (var cursor = asof.getCursor(context)) {
                    int rows = 0;
                    while (cursor.hasNext()) rows++;
                    assertEquals(2048, rows);
                }
                assertEquals(0, tracker.getUsed());
            } finally { context.setMemoryTracker(null); }
        } finally { context.setQueryCodeGenerator(null); }
    }

    @Test public void floatingPredicatesAgreeWithScalarLookup() throws Exception {
        engine.execute("create table rf as (select ts,k,v,"
                + "case when v%7=0 then null else v%4*0.0000000001 end d,"
                + "cast(case when v%7=0 then null else v%4*0.0000000001 end as float) f from r) timestamp(ts)", context);
        for (String predicate : new String[]{"d=0.0", "f=0.0", "d>=0.0000000001", "f>=0.0000000001"}) {
            String body = "a.ts,b.v from l a asof join (select * from rf where " + predicate + ") b on (k)";
            context.setQueryCodeGenerator(null);
            context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED);
            try (var reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true)) {
                List<String> expected;
                try (var cursor = reference.getCursor(context)) { expected = read(cursor); }
                for (var generator : new ResolvedQueryCompiler[]{null, compiler}) {
                    context.setQueryCodeGenerator(generator);
                    context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
                    try (var candidate = Demo.compileSql(engine, context, "select " + body, true);
                         var cursor = candidate.getCursor(context)) {
                        List<String> actual = read(cursor);
                        String label = predicate + " backend=" + (generator == null ? "asmjit" : "llvm");
                        assertEquals(label, expected.size(), actual.size());
                        for (int i = 0; i < expected.size(); i++) assertEquals(label + " row=" + i, expected.get(i), actual.get(i));
                    }
                }
            } finally { context.setQueryCodeGenerator(null); }
        }
    }

    @Test public void filteredFrontierSkipsFutureRejectionsAndRefreshesBinds() throws Exception {
        engine.execute("create table gaps as (select cast(x/3*1000000 as timestamp) ts,"
                + "cast(x%97 as int) k,x v from long_sequence(40000)) timestamp(ts) partition by hour", context);
        context.with(new io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl(engine.getConfiguration()));
        context.getBindVariableService().setLong(0, 11000);
        for (String suffix : new String[]{"", " tolerance 20s", " limit 257"}) {
            String body = "a.ts,b.v from l a asof join (select * from gaps where v<$1) b on (k)" + suffix;
            context.setQueryCodeGenerator(null);
            context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true)) {
                context.setQueryCodeGenerator(compiler);
                context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
                try (RecordCursorFactory candidate = Demo.compileSql(engine, context, "select " + body, true)) {
                    for (long bound : new long[]{11000, 0, 40000, 11000}) {
                        context.getBindVariableService().setLong(0, bound);
                        try (RecordCursor expected = reference.getCursor(context); RecordCursor actual = candidate.getCursor(context)) {
                            assertEquals(read(expected), read(actual));
                            expected.toTop();
                            actual.toTop();
                            assertEquals(read(expected), read(actual));
                        }
                    }
                }
            } finally { context.setQueryCodeGenerator(null); }
        }
    }

    @Test public void aggregateRegionsEnterAfterJavaFirstRowAndReopen() throws Exception {
        engine.execute("create table first_row_l as (select * from l limit 48) timestamp(ts)", context);
        // ASOF aggregation must compose with either filtering path without a compiled forward scan.
        for (boolean batchFilter : new boolean[]{false, true}) {
            int[] calls = new int[2];
            var checked = new QueryCodeGenerator() {
                @Override public CompiledFilters compileFilters(ColumnTypes inputs, Function filter) {
                    return batchFilter ? compiler.compileFilters(inputs, filter) : null;
                }
                @Override public io.questdb.griffin.engine.groupby.GroupByKernel compile(ColumnTypes inputs,
                        ColumnTypes values, ObjList<GroupByFunction> functions) {
                    return compiler.compile(inputs, values, functions);
                }
                @Override public CompiledAsOfJoin compileAsOfJoin(CompiledAsOfJoin.Input input, Map layout, Function filter,
                        ColumnTypes values, ObjList<GroupByFunction> functions, MapValue value, GroupByAllocator allocator) {
                    assertTrue(functions.getQuick(0) instanceof io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction);
                    int countOffset = functions.getQuick(0).getValueIndex();
                    var delegate = compiler.compileAsOfJoin(input, layout, filter, values, functions, value, allocator);
                    assertNotNull(delegate);
                    return new CompiledAsOfJoin() {
                        private boolean prepared;
                        @Override public CharSequence getName() { return delegate.getName(); }
                        @Override public long getNativeRows() { return delegate.getNativeRows(); }
                        @Override public void prepareExecution(MemoryTracker tracker) {
                            assertFalse(prepared);
                            delegate.prepareExecution(tracker);
                            prepared = true;
                        }
                        @Override public void clearExecution() { delegate.clearExecution(); prepared = false; }
                        @Override public void close() { delegate.close(); }
                        private void check(MapValue state) {
                            assertTrue("ASOF must prepare each execution", prepared);
                            // Only this test uses COUNT to observe the Java update.
                            assertTrue("Java must consume the first row before either native strategy", state.getLong(countOffset) > 0);
                        }
                        @Override public void aggregate(PageFrameMemoryRecord master, TimeFrameCursor slave, Map map,
                                MapValue state, State progress, SqlExecutionCircuitBreaker breaker) {
                            check(state);
                            delegate.aggregate(master, slave, map, state, progress, breaker);
                            if (progress.nextMaster > progress.masterLo) calls[0]++;
                        }
                        @Override public void aggregateLookup(PageFrameMemoryRecord master, TimeFrameCursor slave,
                                MapValue state, State progress, SqlExecutionCircuitBreaker breaker) {
                            check(state);
                            delegate.aggregateLookup(master, slave, state, progress, breaker);
                            if (progress.nextMaster > progress.masterLo) calls[1]++;
                        }
                    };
                }
            };
            for (String master : List.of("l", "first_row_l")) {
                String body = "count(),stddev_pop(b.v),weighted_avg(b.v,a.kl),ksum(b.v),min(b.v),max(b.v) from "
                        + master + " a asof join (select * from r where v%3=0) b on (kl)";
                context.setQueryCodeGenerator(null);
                try (var reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true)) {
                    context.setQueryCodeGenerator(checked);
                    try (var candidate = Demo.compileSql(engine, context, "select " + body, true)) {
                        for (int reopen = 0; reopen < 2; reopen++) {
                            try (var expected = reference.getCursor(context); var actual = candidate.getCursor(context)) {
                                assertEquals(aggregateResult(expected, reference), aggregateResult(actual, candidate));
                            }
                        }
                    }
                } finally { context.setQueryCodeGenerator(null); }
            }
            assertTrue("Forward regions must be checked", calls[0] > 0);
            assertTrue("Lookup regions must be checked", calls[1] > 0);
        }
    }

    @Test public void invalidNativeProgressFailsBeforePublicationAndReleasesContext() throws Exception {
        var artifacts = temporary.newFolder().toPath();
        Backend corrupt = new Backend() {
            @Override public String name() { return "corrupt-progress"; }
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path path) throws java.io.IOException {
                var code = new LlvmBackend().compile(program, path);
                return new CompiledKernel((columns, rows, state) -> {
                    long result = code.run(columns, rows, state);
                    Unsafe.putLong(columns, Long.MAX_VALUE); // ASOF descriptor's LEFT publication word.
                    return result;
                }, code, null, 0);
            }
        };
        context.setQueryCodeGenerator(new QueryCodeGenerator() {
            @Override public io.questdb.griffin.engine.groupby.GroupByKernel compile(ColumnTypes inputs,
                    ColumnTypes values, ObjList<GroupByFunction> functions) { return null; }
            @Override public CompiledAsOfJoin compileAsOfJoin(CompiledAsOfJoin.Input input, Map layout, Function filter,
                    ColumnTypes values, ObjList<GroupByFunction> functions, MapValue value, GroupByAllocator allocator) {
                try { return ResolvedAsOfJoin.compile(input, layout, filter, values, functions, value, allocator, corrupt, artifacts); }
                catch (java.io.IOException failure) { throw new RuntimeException(failure); }
            }
        });
        String sql = "select count(),sum(b.v) from l a asof join (select * from r where v%3=0) b on (kl)";
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            for (int reopen = 0; reopen < 2; reopen++) {
                try (var cursor = factory.getCursor(context)) {
                    var failure = assertThrows(IllegalStateException.class, cursor::hasNext);
                    assertTrue(failure.getMessage().contains("Invalid ASOF aggregate progress"));
                    assertThrows(IllegalStateException.class, KernelMemory::allocator);
                }
            }
        } finally { context.setQueryCodeGenerator(null); }
    }

    @Test public void aggregateRegionTypedKeysPayloadsAndReopen() throws Exception {
        for (String type : new String[]{"long", "int", "double"}) {
            context.setQueryCodeGenerator(null);
            engine.execute("create table r_" + type + " as (select ts,k,kl,ks,cast(case when v%29=0 then null else v end as "
                    + type + ") v from r) timestamp(ts) partition by hour", context);
            for (String key : new String[]{"k", "kl", "ks"}) {
                aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                        + "from l a asof join (select ks,v,kl,ts,k from r_" + type + " where v-v/3*3=0) b on ("
                        + key + ")", true);
            }
        }
        aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                + "from l a asof join (select * from r where v-v/3*3=0) b on (kl) tolerance 20s", true);
    }

    @Test public void aggregateDistinctComposesAcrossTypesStrategiesAndReopen() throws Exception {
        engine.execute("create table short_distinct_l as (select * from l limit 48) timestamp(ts)",context);
        for(String master:List.of("l","short_distinct_l")) for(String key:List.of("k","kl","ks")) {
            String body="count(),count_distinct(b.v),count_distinct(b.k),count_distinct(b.ks),"
                    +"count_distinct(cast(b.k as ipv4)),count_distinct(a.ks),"
                    +"count_distinct(case a.ks when 's1' then b.v end),sum(b.v),min(b.v),max(b.v),bit_and(b.v::byte) from "
                    +master+" a asof join (select ks,v,kl,ts,k from r where v%3=0) b on ("+key+") tolerance 20s";
            aggregateMatches(body,true);
        }
        engine.execute("alter table r add column late long",context);
        aggregateMatches("count(),count_distinct(b.late),count_distinct(b.v) from l a asof join "
                +"(select * from r where v%3=0) b on (k)",false);
        engine.execute("alter table r convert partition to parquet where ts < '1970-01-01T02'",context);
        aggregateMatches("count(),count_distinct(b.v),count_distinct(b.k),count_distinct(b.ks) from l a asof join "
                +"(select * from r where v%3=0) b on (ks)",true);
        engine.execute("truncate table r",context);
        aggregateMatches("count(),count_distinct(b.v),count_distinct(b.k),count_distinct(b.ks),min(b.v) from l a asof join "
                +"(select * from r where v%3=0) b on (k)",false);
    }

    @Test public void aggregateDistinctReleasesMemoryAfterLimitAndReopens() throws Exception {
        String body = "count(),count_distinct(b.v),count_distinct(b.k),sum(b.v) from l a asof join "
                + "(select * from r where v%3=0) b on (kl)";
        String expected;
        try (var reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true);
             var cursor = reference.getCursor(context)) {
            expected = aggregateResult(cursor, reference);
        }
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, "select " + body, true);
             var tracker = new io.questdb.test.tools.LimitedMemoryTracker(0)) {
            RecordCursorFactory base = factory;
            while (!(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
            var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
            var owner = factory instanceof io.questdb.griffin.engine.QueryProgress ? factory.getBaseFactory() : factory;
            context.setMemoryTracker(tracker);
            boolean failedAfterNativeRows = false;
            for (long limit = 4096; limit <= (1 << 20) && !failedAfterNativeRows; limit *= 2) {
                tracker.setLimit(limit);
                try (var cursor = owner.getCursor(context)) {
                    aggregateResult(cursor, factory);
                } catch (io.questdb.cairo.CairoException failure) {
                    assertTrue(failure.isOutOfMemory());
                    failedAfterNativeRows = asof.getNativeAggregateRows() > 0;
                }
                assertEquals(0, tracker.getUsed());
            }
            assertTrue("Exercise memory failure after generated processing begins", failedAfterNativeRows);
            tracker.setLimit(0);
            try (var cursor = owner.getCursor(context)) {
                assertEquals(expected, aggregateResult(cursor, factory));
                cursor.toTop();
                assertEquals(expected, aggregateResult(cursor, factory));
            }
            assertEquals(0, tracker.getUsed());
        } finally { context.setMemoryTracker(null); context.setQueryCodeGenerator(null); }
    }

    @Test public void aggregateIntegerDivisionHandlesUnmatchedAndZeroDivisors() throws Exception {
        engine.execute("create table short_div_l as (select * from l limit 48) timestamp(ts)",context);
        for(String master:List.of("l","short_div_l")) for(String key:List.of("k","kl","ks")) {
            aggregateMatches("count(),count(b.v/a.kl),sum(b.v%(a.kl-1)),min(b.v/(a.kl-1)),"
                    +"max(b.k%(a.k-1)),sum(case a.ks when 's1' then b.v%(-3) end) from "
                    +master+" a asof join (select ks,v,kl,ts,k from r where v%3=0) b on ("+key+") tolerance 20s",true);
        }
    }

    @Test public void aggregateNarrowedPayloadsPreserveUnmatchedNullsAcrossStrategies() throws Exception {
        engine.execute("create table short_narrow_l as (select * from l limit 48) timestamp(ts)",context);
        for (String master : List.of("l", "short_narrow_l")) for (String key : List.of("k", "kl", "ks")) {
            aggregateMatches("count(b.v::byte),min(b.v::byte),max(b.v::byte),bit_and(b.v::byte),"
                    + "count(b.v::short),bit_or(b.v::short),count(b.v::int),bit_xor(b.v::int),"
                    + "bit_and(cast(case a.ks when 's1' then b.v end as byte)),count(cast(b.v::int as long)),bit_and(cast(b.v::float as byte)),bit_or(cast(b.v::double as short)),count(cast(b.v::float as int)),max(cast(b.v::double as long)) from "
                    +master+" a asof join (select ks,v,kl,ts,k from r where v-v/3*3=0) b on ("+key+") tolerance 20s",true);
        }
    }

    @Test public void aggregateBitwisePayloadsComposeAcrossWidthsAndStrategies() throws Exception {
        engine.execute("create table short_bit_l as (select * from l limit 48) timestamp(ts)",context);
        engine.execute("create table bit_r as (select ts,k,kl,ks,cast(v%127+1 as byte) b,cast(v%32767+1 as short) h,"
                + "cast(case when v%13=0 then null else v%65535+1 end as int) i,"
                + "case when v%13=0 then null else v%65535+1 end n from r) timestamp(ts) partition by hour",context);
        String aggregates="count(),bit_and(b.b),bit_or(b.b),bit_xor(b.b),bit_and(b.h),bit_or(b.h),bit_xor(b.h),"
                + "bit_and(b.i),bit_or(b.i),bit_xor(b.i),bit_and(b.n),bit_or(b.n),bit_xor(b.n),"
                + "bit_and(case a.ks when 's1' then b.b else cast(127 as byte) end)";
        for (String master : List.of("l", "short_bit_l")) for (String key : List.of("k", "kl", "ks")) {
            aggregateMatches(aggregates+" from "+master+" a asof join (select ks,b,h,i,n,kl,ts,k from bit_r "
                    + "where k-k/3*3=0) b on ("+key+") tolerance 20s",true);
        }
        engine.execute("alter table bit_r add column late byte",context);
        aggregateMatches("bit_and(b.late),bit_or(b.late),bit_xor(b.late) from l a asof join "
                + "(select * from bit_r where k-k/3*3=0) b on (k)",false);
        engine.execute("alter table bit_r convert partition to parquet where ts < '1970-01-01T02'",context);
        aggregateMatches(aggregates+" from l a asof join (select * from bit_r where k-k/3*3=0) b on (k)",true);
    }

    @Test public void aggregateIPv4PayloadsUseUnsignedExtremaAcrossStrategies() throws Exception {
        engine.execute("create table short_ip_l as (select * from l limit 48) timestamp(ts)",context);
        engine.execute("create table ip_r as (select ts,k,kl,ks,cast(case when v%13=0 then null "
                + "when v%3=0 then -1 when v%3=1 then -2147483647 else v end as int)::ipv4 ip "
                + "from r) timestamp(ts) partition by hour",context);
        String aggregates="count(b.ip),min(b.ip),max(b.ip),count(b.ip-1),min(b.ip-1),max(b.ip-1),"
                + "count(cast(b.ip as int)),min(case a.ks when 's1' then b.ip end)";
        for (String master : List.of("l", "short_ip_l")) for (String key : List.of("k", "kl", "ks")) {
            aggregateMatches(aggregates+" from "+master+" a asof join (select ks,ip,kl,ts,k from ip_r "
                    + "where k-k/3*3=0) b on ("+key+") tolerance 20s",true);
        }
        engine.execute("alter table ip_r add column late ipv4",context);
        aggregateMatches("count(b.late),min(b.late),max(b.late) from l a asof join "
                + "(select * from ip_r where k-k/3*3=0) b on (k)",false);
        engine.execute("alter table ip_r convert partition to parquet where ts < '1970-01-01T02'",context);
        aggregateMatches(aggregates+" from l a asof join (select * from ip_r where k-k/3*3=0) b on (k)",true);
    }

    @Test public void aggregateBooleanPayloadsPreserveUnmatchedFalseAcrossStrategies() throws Exception {
        engine.execute("create table bool_l as (select ts,k,kl,ks,k%5=0 flag from l) timestamp(ts)",context);
        engine.execute("create table short_bool_l as (select * from bool_l limit 48) timestamp(ts)",context);
        engine.execute("create table bool_r as (select ts,k,kl,ks,v%7=0 flag from r) timestamp(ts) partition by hour",context);
        engine.execute("alter table bool_r add column late boolean",context);
        String aggregates = "count(),bool_and(b.flag),bool_or(b.flag),bool_and(a.flag or b.flag),"
                + "min(b.flag),max(b.flag),avg(b.flag),sum(cast(b.flag as long)),"
                + "bool_or(case a.ks when 's1' then b.flag end)";
        for (String master : List.of("bool_l", "short_bool_l")) {
            for (String key : List.of("k", "kl", "ks")) {
                aggregateMatches(aggregates + " from " + master
                        + " a asof join (select ks,flag,late,kl,ts,k from bool_r where k-k/3*3=0) b on (" + key + ") tolerance 20s",true);
            }
        }
        // A missing physical page retains the ordinary top/conversion fallback.
        aggregateMatches("bool_and(b.late),bool_or(b.late),avg(b.late) from bool_l a asof join "
                + "(select * from bool_r where k-k/3*3=0) b on (k)",false);
        engine.execute("alter table bool_r convert partition to parquet where ts < '1970-01-01T02'",context);
        aggregateMatches(aggregates + " from bool_l a asof join (select * from bool_r where k-k/3*3=0) b on (k)",true);
    }

    @Test public void aggregateSymbolConditionsUseBothDictionariesAndStrategies() throws Exception {
        engine.execute("create table short_l as (select * from l limit 48) timestamp(ts)", context);
        for (String master : List.of("l", "short_l")) {
            for (String key : List.of("k", "kl", "ks")) {
                aggregateMatches("count(case when a.ks='s1' then b.v end),"
                        + "sum(case b.ks when 's1' then b.v when 's2' then b.v+1 end),"
                        + "min(case when b.ks in ('s3',null) then b.v end),"
                        + "max(case when a.ks!='s1' then b.v end),"
                        + "avg(case when b.ks='s4' then cast(b.v as double) end) from " + master
                        + " a asof join (select ks,v,kl,ts,k from r where v-v/3*3=0) b on (" + key + ") tolerance 20s", true);
            }
        }
    }

    @Test public void aggregateSymbolParametersRefreshAfterTruncate() throws Exception {
        String sql = "select count(case when a.ks='new' then b.v end),"
                + "sum(case b.ks when 'new' then b.v when 's1' then b.v+1 end),"
                + "min(case when b.ks is null then b.v end) "
                + "from l a asof join (select * from r where v-v/3*3=0) b on (k)";
        context.setQueryCodeGenerator(compiler);
        try (var candidate = Demo.compileSql(engine, context, sql, true)) {
            RecordCursorFactory base = candidate;
            while (base != null && !(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
            assertNotNull(base);
            var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
            for (int round = 0; round < 2; round++) {
                context.setQueryCodeGenerator(null);
                if (round > 0) {
                    engine.execute("truncate table l", context);
                    engine.execute("truncate table r", context);
                    engine.execute("insert into r select cast(x*1000000 as timestamp),cast(x%3 as int),x%3,"
                            + "cast(case when x%2=0 then 'new' else 's1' end as symbol),'text',x from long_sequence(4096)", context);
                    engine.execute("insert into l select cast((2048+x)*1000000000 as timestamp_ns),cast(x%3 as int),x%3,"
                            + "cast(case when x%2=0 then 's1' else 'new' end as symbol) from long_sequence(1024)", context);
                }
                try (var ordinary = Demo.compileSql(engine, context, sql, true);
                     var expected = ordinary.getCursor(context);
                     var actual = candidate.getCursor(context)) {
                    assertEquals(aggregateResult(expected, ordinary), aggregateResult(actual, candidate));
                    assertTrue(asof.getNativeAggregateRows() > 0);
                }
            }
        }
    }

    @Test public void aggregateFloatPayloadsShareTypedStatesAcrossStrategies() throws Exception {
        engine.execute("create table short_l as (select * from l limit 48) timestamp(ts)", context);
        engine.execute("create table floats as (select ts,k,kl,ks,"
                + "cast(case when v%29=0 then null else (v%31-15)*0.25 end as float) v "
                + "from r) timestamp(ts) partition by hour", context);
        for (String master : List.of("l", "short_l")) for (String key : List.of("k", "kl", "ks")) {
            aggregateMatches("count(b.v),sum(b.v),min(b.v),max(b.v),avg(b.v),sum(-b.v),min(-b.v),avg(-b.v),"
                    + "sum(b.v+cast(a.kl as float)),avg(b.v/cast(3 as float)),"
                    + "sum(case when b.ks='s1' then b.v else null::float end) "
                    + "from " + master + " a asof join (select ks,v,kl,ts,k from floats"
                    + " where k-k/3*3=0) b on (" + key + ") tolerance 20s", true);
        }
    }

    @Test public void aggregateMomentsUseOrdinaryStatesAcrossTypesAndStrategies() throws Exception {
        engine.execute("create table short_l as (select * from l limit 48) timestamp(ts)", context);
        int key = 0;
        for (String type : List.of("byte", "short", "int", "long", "float", "double")) {
            engine.execute("create table moment_" + type + " as (select ts,k,kl,ks,"
                    + "cast(case when v%29=0 then null else v%31-15 end as " + type + ") v "
                    + "from r) timestamp(ts) partition by hour", context);
            for (String master : List.of("l", "short_l")) {
                aggregateMatches("count(b.v),sum(cast(b.v as float)%cast(a.kl as float)),sum(cast(b.v as double)%cast(a.kl as double)),nsum(b.v),nsum(b.v+a.kl),weighted_avg(b.v,a.kl),weighted_avg(a.kl,b.v),var_pop(b.v),var_samp(b.v),stddev_pop(b.v),stddev(b.v),"
                        + "covar_pop(b.v,a.kl),covar_samp(a.kl,b.v),corr(b.v,a.kl),regr_slope(b.v,a.kl),regr_intercept(b.v,a.kl),regr_r2(b.v,a.kl),"
                        + "var_pop(case when b.ks='s1' then cast(b.v as double) end) from " + master
                        + " a asof join (select ks,v,kl,ts,k from moment_" + type
                        + " where k-k/3*3=0) b on (" + List.of("k", "kl", "ks").get(key % 3) + ") tolerance 20s", true);
            }
            key++;
        }
    }

    @Test public void aggregateNarrowPayloadsKeepUnmatchedZeroAndSignedWidths() throws Exception {
        engine.execute("create table narrow_l as (select *,cast(kl%256-128 as byte) b,"
                + "cast(kl*997-32768 as short) h from l) timestamp(ts)", context);
        engine.execute("create table short_narrow_l as (select * from narrow_l limit 48) timestamp(ts)", context);
        engine.execute("create table narrow_r as (select ts,k,kl,ks,cast(v%256-128 as byte) b,"
                + "cast(v%65536-32768 as short) h from r) timestamp(ts) partition by hour", context);
        String aggregates = "count(b.b),count(b.h),sum(b.b),min(b.b),max(b.b),avg(b.b),"
                + "sum(b.h),min(b.h),max(b.h),avg(b.h),sum(-b.h),"
                + "sum(a.b+b.h),sum(a.h+b.b),"
                + "sum(case when b.ks='s1' then b.h else a.h end)";
        for (String master : List.of("narrow_l", "short_narrow_l")) {
            for (String key : List.of("k", "kl", "ks")) {
                aggregateMatches(aggregates + " from " + master
                        + " a asof join (select ks,b,h,kl,ts,k from narrow_r where k-k/3*3=0) b on ("
                        + key + ") tolerance 20s", true);
            }
        }
        engine.execute("alter table narrow_r add column late short", context);
        aggregateMatches("count(b.late),sum(b.late),min(b.late),max(b.late),avg(b.late) "
                + "from narrow_l a asof join (select * from narrow_r where k-k/3*3=0) b on (k)", false);
        engine.execute("alter table narrow_r convert partition to parquet where ts < '1970-01-01T02'", context);
        aggregateMatches(aggregates + " from narrow_l a asof join "
                + "(select * from narrow_r where k-k/3*3=0) b on (k)", true);
    }

    @Test public void aggregateCharactersKeepUnsignedOrderAndUnmatchedNulls() throws Exception {
        engine.execute("create table short_l as (select * from l limit 48) timestamp(ts)", context);
        engine.execute("create table char_r as (select ts,k,kl,ks,cast(cast(case when v%29=0 then null "
                + "when v%7=0 then 0 when v%7=1 then 65535 when v%7=2 then 32768 else v%65536 end as int) as char) c "
                + "from r) timestamp(ts) partition by hour", context);
        for (String master : List.of("l", "short_l")) {
            for (String key : List.of("k", "kl", "ks")) {
                aggregateMatches("count(b.c),min(b.c),max(b.c),min(case b.ks when 's1' then b.c end),"
                        + "max(case when b.c>=cast(32768 as char) then b.c end) "
                        + "from " + master + " a asof join (select ks,c,kl,ts,k from char_r where k-k/3*3=0) b on ("+key+") tolerance 20s", true);
            }
        }
        engine.execute("alter table char_r add column late char",context);
        aggregateMatches("count(b.late),min(b.late),max(b.late),min(b.c),max(b.c) from l a asof join "
                + "(select * from char_r where k-k/3*3=0) b on (k)",false);
        engine.execute("alter table char_r convert partition to parquet where ts<'1970-01-01T02'",context);
        aggregateMatches("count(b.c),min(b.c),max(b.c) from l a asof join "
                + "(select * from char_r where k-k/3*3=0) b on (k)",true);
    }

    @Test public void aggregateTemporalPayloadsKeepUnitsAcrossStrategies() throws Exception {
        engine.execute("create table short_l as (select * from l limit 48) timestamp(ts)", context);
        for (String type : List.of("date", "timestamp", "timestamp_ns")) {
            String table = "temporal_" + type;
            engine.execute("create table " + table + " as (select ts,k,kl,ks,"
                    + "cast(case when v%29=0 then null else (v-20000)*1000001 end as " + type + ") v "
                    + "from r) timestamp(ts) partition by hour", context);
            for (String master : List.of("l", "short_l")) {
                aggregateMatches("count(b.v),min(b.v),max(b.v),sum(b.v),avg(b.v),sum(b.v::double),max(b.v::float),"
                        + "min(case when b.ks='s1' then b.v end) "
                        + "from " + master + " a asof join (select ks,v,kl,ts,k from " + table
                        + " where k-k/3*3=0) b on (k) tolerance 20s", true);
            }
        }
    }

    @Test public void aggregateRegionHistoryParquetColumnTopsAndLimits() throws Exception {
        for (int storage = 0; storage < 2; storage++) {
            if (storage == 1) engine.execute("alter table r convert partition to parquet where ts < '1970-01-01T02'", context);
            aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                    + "from l a asof join (select * from r where v<(k+1)*150) b on (k)", true);
            aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                    + "from l a asof join (select * from r where v-v/1000*1000<700) b on (k)", true);
            aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                    + "from (select * from l limit 100,1000) a asof join (select * from r where v-v/3*3=0) b on (ks)", false);
        }
        engine.execute("alter table r add column added long", context);
        aggregateMatches("sum(b.added),min(b.added),max(b.added),count(b.added),sum(b.added+a.kl),count(*) "
                + "from l a asof join (select * from r where v-v/3*3=0) b on (kl)", false);
        aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                + "from (select * from l limit 0) a asof join (select * from r where v>0) b on (kl)", false);
    }

    @Test public void aggregateRegionMapRefusalPreservesCompletedPrefix() throws Exception {
        engine.execute("create table mr as (select cast(x*1000000 as timestamp) ts,"
                + "cast(case when x<15000 then x%8 else x%1000 end as int) k,x v from long_sequence(20000)) timestamp(ts)", context);
        engine.execute("create table ml as (select cast((14000+x)*1000000 as timestamp) ts,"
                + "cast(case when x<1000 then (x+4)%8 else x%1000 end as int) k,x kl from long_sequence(5000)) timestamp(ts)", context);
        for (int limit : new int[]{1, 4096}) {
            mapLimit = limit;
            aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                    + "from ml a asof join (select * from mr where v>0) b on (k)", true);
        }
    }

    @Test public void aggregateRegionMissingSymbolsAndUnsupportedConsumer() throws Exception {
        engine.execute("create table ls as (select ts,k,kl,cast(case when k%7=0 then 'missing' else ks end as symbol) ks from l) timestamp(ts)", context);
        aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                + "from ls a asof join (select * from r where v-v/3*3=0) b on (ks)", true);
        aggregateMatches("count(b.ks),count(a.ks),count(b.k),count(b.kl) from ls a asof join "
                + "(select * from r where v-v/3*3=0) b on (ks)", true);
        aggregateMatches("avg(b.v),sum(b.v),count(b.v) from l a asof join (select * from r where v-v/3*3=0) b on (kl)", true);
        aggregateMatches("count(b.text),count(*) from l a asof join (select * from r where v-v/3*3=0) b on (kl)", false);
        aggregateMatches("sum(b.v),count(b.v),count(*) from l a asof join (select * from r where text='v0') b on (kl)", true);
    }

    @Test public void aggregateRegionCancellationReleasesQueryStateAndReopens() throws Exception {
        String body = "sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*),count_distinct(b.v),count_distinct(b.k) "
                + "from l a asof join (select * from r where v-v/3*3=0) b on (kl)";
        String expected;
        try (var reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true);
             var cursor = reference.getCursor(context)) {
            expected = aggregateResult(cursor, reference);
        }
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, "select " + body, true);
             var tracker = new io.questdb.std.MemoryTracker() {
                 @Override public long getQueryId() { return -1; }
                 @Override public io.questdb.std.MemoryTrackerWorkload getWorkload() { return io.questdb.std.MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }) {
            RecordCursorFactory base = factory;
            while (!(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
            var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
            var owner = factory instanceof io.questdb.griffin.engine.QueryProgress ? factory.getBaseFactory() : factory;
            context.setMemoryTracker(tracker);
            try {
                for (int attempt = 0; attempt < 2; attempt++) {
                    context.with(new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine) {
                        @Override public void statefulThrowExceptionIfTrippedOrYield() {
                            if (asof.getNativeAggregateRows() > 128) cancel();
                            super.statefulThrowExceptionIfTrippedOrYield();
                        }
                    });
                    try (var cursor = owner.getCursor(context)) {
                        assertThrows(io.questdb.cairo.CairoException.class, cursor::hasNext);
                        assertTrue(asof.getNativeAggregateRows() > 128);
                        assertTrue(asof.getNativeAggregateRows() < 2048);
                    }
                    assertEquals(0, tracker.getUsed());
                    context.with(new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine));
                    try (var cursor = owner.getCursor(context)) {
                        assertEquals(expected, aggregateResult(cursor, factory));
                    }
                    assertEquals(0, tracker.getUsed());
                }
            } finally { context.setMemoryTracker(null); }
        } finally { context.setQueryCodeGenerator(null); }
    }

    @Test public void backwardAggregateRegionCachesAndResumes() throws Exception {
        engine.execute("create table short_l as (select * from l limit 48) timestamp(ts)", context);
        for (String key : new String[]{"k", "kl", "ks"}) {
            for (String tolerance : new String[]{"", " tolerance 20s"}) {
                String body = "sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                        + "from short_l a asof join (select * from r where v-v/3*3=0) b on (" + key + ")" + tolerance;
                context.setQueryCodeGenerator(null);
                String expected;
                try (var reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true);
                     var cursor = reference.getCursor(context)) {
                    expected = aggregateResult(cursor, reference);
                }
                context.setQueryCodeGenerator(compiler);
                try (var tracker = new io.questdb.test.tools.LimitedMemoryTracker(32 * 1024);
                     var candidate = Demo.compileSql(engine, context, "select " + body, true)) {
                    RecordCursorFactory base = candidate;
                    while (base != null && !(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
                    assertNotNull(base);
                    var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
                    var owner = candidate instanceof io.questdb.griffin.engine.QueryProgress ? candidate.getBaseFactory() : candidate;
                    context.setMemoryTracker(tracker);
                    for (int reopen = 0; reopen < 3; reopen++) {
                        try (var cursor = owner.getCursor(context)) {
                            assertEquals(expected, aggregateResult(cursor, candidate));
                            assertTrue("No fused backward probes " + body, asof.getNativeAggregateRows() > 0);
                            assertEquals(0, asof.getNativeForwardRows());
                            cursor.toTop();
                            assertEquals(expected, aggregateResult(cursor, candidate));
                        }
                        assertEquals(0, tracker.getUsed());
                    }
                } finally { context.setMemoryTracker(null); context.setQueryCodeGenerator(null); }
            }
        }
    }

    private void aggregateMatches(String body, boolean requireNative) throws Exception {
        context.setQueryCodeGenerator(null);
        context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED);
        try (var reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true)) {
            context.setQueryCodeGenerator(compiler);
            context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
            try (var candidate = Demo.compileSql(engine, context, "select " + body, true)) {
                RecordCursorFactory base = candidate;
                while (base != null && !(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
                assertNotNull(base);
                var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
                for (int reopen = 0; reopen < 2; reopen++) {
                    try (var expected = reference.getCursor(context); var actual = candidate.getCursor(context)) {
                        for (int rewind = 0; rewind < 2; rewind++) {
                            long before = asof.getNativeAggregateRows();
                            assertEquals(body, aggregateResult(expected, reference), aggregateResult(actual, candidate));
                            if (requireNative && rewind == 0 && asof.getNativeAggregateRows() == before) {
                                var plan = new io.questdb.griffin.TextPlanSink(); plan.of(candidate, context);
                                StringBuilder text = new StringBuilder();
                                for (int i = 1; i <= plan.getLineCount(); i++) text.append(plan.getLine(i)).append('\n');
                                fail("No generated aggregate rows: " + body + "\n" + text);
                            }
                            if (rewind > 0) assertEquals(before, asof.getNativeAggregateRows());
                            expected.toTop(); actual.toTop();
                        }
                    }
                }
            }
        } finally { context.setQueryCodeGenerator(null); }
    }

    private static String aggregateResult(RecordCursor cursor, RecordCursorFactory factory) {
        StringBuilder result = new StringBuilder();
        while (cursor.hasNext()) {
            var record = cursor.getRecord();
            for (int i = 0; i < factory.getMetadata().getColumnCount(); i++) {
                int type = factory.getMetadata().getColumnType(i);
                if (type == io.questdb.cairo.ColumnType.BOOLEAN) result.append(record.getBool(i));
                else if (type == io.questdb.cairo.ColumnType.DOUBLE) result.append(record.getDouble(i));
                else if (type == io.questdb.cairo.ColumnType.FLOAT) result.append(record.getFloat(i));
                else if (type == io.questdb.cairo.ColumnType.INT) result.append(record.getInt(i));
                else if (type == io.questdb.cairo.ColumnType.CHAR) result.append((int) record.getChar(i));
                else if (type == io.questdb.cairo.ColumnType.IPv4) result.append(record.getIPv4(i));
                else result.append(record.getLong(i));
                result.append(',');
            }
            result.append(';');
        }
        return result.toString();
    }

    private void matches(String body, boolean requireNative) throws Exception {
        matches(body, requireNative, false);
    }

    private void matches(String body, boolean requireNative, boolean requireLookup) throws Exception {
        context.setQueryCodeGenerator(null);
        context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory reference = Demo.compileSql(engine, context, "select /*+ asof_linear(a b) */ " + body, true)) {
            context.setQueryCodeGenerator(compiler);
            context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
            try (RecordCursorFactory candidate = Demo.compileSql(engine, context, "select " + body, true)) {
                RecordCursorFactory base = candidate;
                while (base != null && !(base instanceof FilteredAsOfJoinFastRecordCursorFactory)) base = base.getBaseFactory();
                assertNotNull(base);
                var asof = (FilteredAsOfJoinFastRecordCursorFactory) base;
                for (int reopen = 0; reopen < 2; reopen++) {
                    try (RecordCursor expected = reference.getCursor(context); RecordCursor actual = candidate.getCursor(context)) {
                        for (int rewind = 0; rewind < 2; rewind++) {
                            assertEquals(body, read(expected), read(actual));
                            if (requireLookup) assertTrue("No native lookup: " + body, asof.getNativeLookupRows() > 0);
                            if (requireNative && asof.getNativeForwardRows() + asof.getCompiledFilterRows() == 0) {
                                var plan = new io.questdb.griffin.TextPlanSink();
                                plan.of(candidate, context);
                                var text = new StringBuilder();
                                for (int i = 1; i <= plan.getLineCount(); i++) text.append(plan.getLine(i)).append('\n');
                                fail("Native forward region was not exercised; forward=" + asof.isForwardScanMode() + "\\n" + text);
                            }
                            expected.toTop();
                            actual.toTop();
                        }
                    }
                }
            }
        } finally { context.setQueryCodeGenerator(null); }
    }

    private static List<String> read(RecordCursor cursor) {
        var values = new ArrayList<String>();
        var record = cursor.getRecord();
        while (cursor.hasNext()) values.add(record.getTimestamp(0) + ":" + record.getLong(1));
        return values;
    }
}
