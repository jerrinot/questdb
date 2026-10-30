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
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SymbolExpressionTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private final io.questdb.mp.WorkerPoolMode mode;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

    public SymbolExpressionTest(String name, Backend backend, io.questdb.mp.WorkerPoolMode mode) {
        this.backend = backend;
        this.mode = mode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(
                new Object[]{"llvm-legacy", new LlvmBackend(), io.questdb.mp.WorkerPoolMode.LEGACY},
                new Object[]{"llvm-fiber", new LlvmBackend(), io.questdb.mp.WorkerPoolMode.FIBER_HOST},
                new Object[]{"asmjit", new AsmJitBackend(), io.questdb.mp.WorkerPoolMode.LEGACY},
                new Object[]{"c", new NativeBackend(), io.questdb.mp.WorkerPoolMode.LEGACY});
    }

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public int getGroupByShardingThreshold() { return 2; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
            @Override public io.questdb.mp.WorkerPoolMode getWorkerPoolMode() { return mode; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        context.with(new BindVariableServiceImpl(config));
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath());
        engine.execute("create table symbols as (select x id, cast(x%7 as int) k,"
                + "cast(case when x%11=0 then null when x%5=0 then '' when x%5=1 then 'ALPHA' "
                + "when x%5=2 then 'alpha' when x%5=3 then 'Žluť🙂' else 'İΣß' end as symbol) s,"
                + "case when x%13=0 then null else x*3 end v, timestamp_sequence('2024-01-01',1000000) ts "
                + "from long_sequence(1003)) timestamp(ts) partition by day bypass wal", context);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void dictionaryKeysComposeAndPreserveNullAndUnicodeSemantics() throws Exception {
        for (String expression : List.of("strpos(lower(s),'a')", "strpos(upper(lower(s)),'Ž')",
                "length(s)", "length(s::string)", "strpos(s,'')", "strpos(s,'🙂')")) {
            String sql = "select " + expression + " key,count(),sum(v),min(v),max(v),count(v) from symbols group by key";
            try (var factory = candidate(sql)) {
                check(factory, sql);
                assertEquals(1003, nativeRows(factory));
                assertEquals(1003, arrayRows(factory));
                try (var cursor = factory.getCursor(context)) {
                    assertTrue(cursor.hasNext());
                    cursor.toTop();
                    assertTrue(cursor.hasNext());
                }
                check(factory, sql);
            }
        }
        String sql = "select k,strpos(lower(s),'a') p,length(s) len,count(),sum(v) from symbols "
                + "group by k,p,len";
        try (var factory = candidate(sql)) { check(factory, sql); assertTrue(nativeRows(factory) > 0); assertEquals(0, arrayRows(factory)); }
    }

    @Test public void reopenRefreshesDictionaryAndColumnTopsRemainSafe() throws Exception {
        String sql = "select strpos(lower(s),'a') key,count(),sum(v) from symbols group by key";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            engine.execute("insert into symbols values (1004,1,'aaaa','1','2024-01-01T01:00:00')", context);
            check(factory, sql);
            engine.execute("truncate table symbols", context);
            engine.execute("insert into symbols values (1,1,'zzza',5,'2024-01-01'),(2,1,null,null,'2024-01-01T00:00:01')", context);
            check(factory, sql);
        }
        engine.execute("insert into symbols select x,1,'alpha',x,timestamp_sequence('2024-01-01T00:10:00',1) from long_sequence(1003)", context);
        engine.execute("alter table symbols add column late symbol", context);
        engine.execute("insert into symbols values (3,1,'x',7,'2024-01-01T01:00:00','abc')", context);
        String late = "select strpos(lower(late),'a') key,count(),sum(v) from symbols group by key";
        for (boolean parquet : new boolean[]{false,true}) {
            if (parquet) engine.execute("alter table symbols convert partition to parquet list '2024-01-01'", context);
            try (var factory = candidate(late)) {
                check(factory, late);
                if (!parquet) assertTrue(arrayRows(factory) > 0);
            }
        }
    }

    @Test public void arraysHandleNullDomainsAndDeclineSparseRanges() throws Exception {
        engine.execute("create table null_symbols as (select cast(null as symbol) s,x v from long_sequence(1003))", context);
        String sql = "select length(s) k,count(),sum(v) from null_symbols group by k";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertEquals(1003, arrayRows(factory));
            engine.execute("truncate table null_symbols", context);
            check(factory, sql);
        }
        engine.execute("create table sparse (s symbol,v long)", context);
        for (int length : new int[]{10000, 1000000}) {
            // The first range fits cache but exceeds the serial merge work budget;
            // the second also exceeds the array memory budget.
            String distant = "x".repeat(length);
            try (var writer = engine.getWriter(engine.verifyTableName("sparse"), "test")) {
                for (int i = 0; i < 1003; i++) {
                    var row = writer.newRow();
                    row.putSym(0, i % 2 == 0 ? "x" : distant);
                    row.putLong(1, i);
                    row.append();
                }
                writer.commit();
            }
            sql = "select length(s) k,count(),sum(v) from sparse group by k";
            try (var factory = candidate(sql)) {
                check(factory, sql);
                assertEquals(1003, nativeRows(factory));
                assertEquals(0, arrayRows(factory));
            }
            engine.execute("truncate table sparse", context);
        }
    }

    @Test public void arraysReuseIntegerAndDoubleAggregateStateAndFinalization() throws Exception {
        engine.execute("create table numbers as (select cast(x%11 as symbol) s,"
                + "cast(case when x%7=0 then null else x%97-48 end as int) i,"
                + "case when x%7=0 then null else x*17 end l,"
                + "case when x%7=0 then null else (x%97-48)*0.25 end d from long_sequence(1003))", context);
        String sql = "select length(s) k,count(),count(i),count(l),count(d),"
                + "sum(i),sum(l),sum(d),min(i),max(i),min(l),max(l),min(d),max(d),avg(i),avg(l),avg(d) "
                + "from numbers group by k";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            assertEquals(1003, arrayRows(factory));
            check(factory, sql);
        }
        String ordered = sql + " order by k desc limit 1";
        try (var factory = candidate(ordered)) { check(factory, ordered); assertEquals(1003, arrayRows(factory)); }
    }

    @Test public void arrayQueriesCompileOnlyUsedKernels() throws Exception {
        var compiled = new java.util.concurrent.atomic.AtomicInteger();
        var closed = new java.util.concurrent.atomic.AtomicInteger();
        Backend tracking = new Backend() {
            @Override public String name() { return backend.name(); }
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path artifacts) throws java.io.IOException {
                var code = backend.compile(program, artifacts);
                compiled.incrementAndGet();
                return new CompiledKernel(code, () -> { code.close(); closed.incrementAndGet(); },
                        code.artifacts(), code.compilationNanos());
            }
        };
        compiler = new ResolvedQueryCompiler(tracking, temporary.newFolder().toPath());
        String sql = "select length(s) k,count(),sum(v) from symbols group by k";
        try (var factory = candidate(sql)) {
            assertEquals("No scan path has been admitted yet", 0, compiled.get());
            for (int i = 0; i < 2; i++) {
                long before = arrayRows(factory);
                check(factory, sql);
                assertEquals(before + 1003, arrayRows(factory));
                // Scan plus merge if more than one slot received rows; no fallback maps.
                assertTrue("Unexpected fallback compilation: " + compiled.get(), compiled.get() >= 1 && compiled.get() <= 2);
            }
        }
        assertEquals(compiled.get(), closed.get());
    }

    @Test public void runtimeOrMultipleInputsKeepOrdinaryEvaluation() throws Exception {
        context.getBindVariableService().setStr(0, "a");
        for (String expression : List.of("strpos(s,$1)", "strpos(s,cast(k as string))", "length(cast(id as symbol))")) {
            String sql = "select " + expression + " key,count(),sum(v) from symbols group by key";
            try (var factory = candidate(sql)) { check(factory, sql); assertEquals(0, nativeRows(factory)); }
        }
        String filtered = "select strpos(lower(s),'a') key,count(),sum(v) from symbols where id=7 group by key";
        try (var factory = candidate(filtered)) { check(factory, filtered); assertEquals(0, nativeRows(factory)); }
        engine.execute("create table unique_symbols as (select cast(x as symbol) s,x v from long_sequence(1003))", context);
        String unique = "select strpos(s,'3') key,count(),sum(v) from unique_symbols group by key";
        try (var factory = candidate(unique)) { check(factory, unique); assertEquals(0, nativeRows(factory)); }
        engine.execute("insert into unique_symbols select * from unique_symbols", context);
        try (var factory = candidate(unique)) { check(factory, unique); assertEquals(0, nativeRows(factory)); }
    }

    @Test public void preparationReleasesMemoryOnBudgetFailureAndCancellation() throws Exception {
        try (var reader = engine.getReader("symbols");
             var tracker = new io.questdb.std.MemoryTracker() {
                 @Override public long getQueryId() { return -1; }
                 @Override public io.questdb.std.MemoryTrackerWorkload getWorkload() { return io.questdb.std.MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }) {
            var column = new io.questdb.griffin.engine.functions.columns.SymbolColumn(reader.getMetadata().getColumnIndexQuiet("s"), true);
            column.init(reader, context);
            try (var function = new io.questdb.griffin.engine.functions.str.StrPosFunctionFactory.ConstFunc(column, "a");
                 var expression = SymbolExpression.bind(function)) {
                var breaker = new io.questdb.cairo.sql.AtomicBooleanCircuitBreaker(engine);
                expression.prepare(tracker, breaker, reader.size());
                assertTrue(tracker.getUsed() > 0);
                expression.close();
                assertEquals(0, tracker.getUsed());
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 1);
                assertThrows(io.questdb.cairo.CairoException.class, () -> expression.prepare(tracker, breaker, reader.size()));
                assertEquals(0, tracker.getUsed());
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                breaker.cancel();
                assertThrows(io.questdb.cairo.CairoException.class, () -> expression.prepare(tracker, breaker, reader.size()));
                assertEquals(0, tracker.getUsed());
                expression.prepare(tracker, io.questdb.cairo.sql.SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER, reader.size());
                assertTrue(expression.address() != 0);
            }
            assertEquals(0, tracker.getUsed());
        }
    }

    private RecordCursorFactory candidate(String sql) throws Exception {
        context.setQueryCodeGenerator(compiler);
        return Demo.compileSql(engine, context, sql, true);
    }

    private void check(RecordCursorFactory factory, String sql) throws Exception {
        var actual = Demo.readSql(factory, context, factory.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var stock = Demo.compileSql(engine, context, sql, true)) {
            assertEquals(sql, Demo.readSql(stock, context, stock.getMetadata().getColumnCount()), actual);
        } finally { context.setQueryCodeGenerator(compiler); }
    }

    private long nativeRows(RecordCursorFactory factory) {
        return nativeRows(factory, false);
    }

    private long arrayRows(RecordCursorFactory factory) { return nativeRows(factory, true); }

    private long nativeRows(RecordCursorFactory factory, boolean arrays) {
        for (; factory != null; factory = factory.getBaseFactory()) {
            if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                long rows = 0;
                for (int i = -1; i < 2; i++) {
                    var updater = group.getAtom().getCompiledUpdater(i);
                    if (updater != null) rows += arrays ? ((AggregateSlot) updater).dictionaryRows() : updater.getNativeMapRows();
                }
                return rows;
            }
        }
        return 0;
    }
}
