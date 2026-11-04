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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.std.MemoryTag;
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
        mapLimit = 4096;
        engine.execute("create table mr as (select cast(x*1000000 as timestamp) ts,"
                + "cast(case when x<15000 then x%8 else x%1000 end as int) k,x v from long_sequence(20000)) timestamp(ts)", context);
        engine.execute("create table ml as (select cast((14000+x)*1000000 as timestamp) ts,"
                + "cast(case when x<1000 then (x+4)%8 else x%1000 end as int) k,x kl from long_sequence(5000)) timestamp(ts)", context);
        aggregateMatches("sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
                + "from ml a asof join (select * from mr where v>0) b on (k)", true);
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
        String body = "sum(b.v),min(b.v),max(b.v),count(b.v),sum(b.v+a.kl),count(*) "
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
                if (type == io.questdb.cairo.ColumnType.DOUBLE) result.append(record.getDouble(i));
                else if (type == io.questdb.cairo.ColumnType.INT) result.append(record.getInt(i));
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
