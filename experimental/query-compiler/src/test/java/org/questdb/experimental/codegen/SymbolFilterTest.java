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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncTopKRecordCursorFactory;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SymbolFilterTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend;
    private final AtomicInteger compilations = new AtomicInteger();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

    public SymbolFilterTest(String name, Backend backend) { this.backend = backend; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{"llvm", new LlvmBackend()}, new Object[]{"asmjit", new AsmJitBackend()});
    }

    @Before
    public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public int getGroupByShardingThreshold() { return 2; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        context.with(new BindVariableServiceImpl(config));
        context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        compiler = new ResolvedQueryCompiler(new Backend() {
            @Override public String name() { return backend.name(); }
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path path) throws java.io.IOException {
                compilations.incrementAndGet();
                return backend.compile(program, path);
            }
        }, temporary.newFolder().toPath());
        engine.execute("create table symbols as (select x id, cast(x%7 as int) k,"
                + "cast(case when x%11=0 then null when x%3=0 then 'alpha' when x%3=1 then 'žluť' else 'X' end as symbol) s,"
                + "cast('value-'||x as varchar) v, timestamp_sequence('2024-01-01',1000000) ts "
                + "from long_sequence(1003)) timestamp(ts) partition by day bypass wal", context);
    }

    @After
    public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test
    public void equalityAndSmallSetsCompileWithNullMissingAndBooleanComposition() throws Exception {
        for (String predicate : List.of("s='alpha'", "s!='alpha'", "s='X'",
                "s in ('alpha','absent',null,'alpha')", "s not in ('alpha','absent',null)",
                "s in ('missing','also missing')", "s in ('žluť',null) or k=2",
                "not (s='alpha' or k<2)", "s='alpha' and s in ('alpha',null)",
                "s in ('žluť','X') and v is not null")) {
            try (var factory = candidate("select count() from symbols where " + predicate)) {
                check(factory, "select count() from symbols where " + predicate);
                var filtered = find(factory, AsyncJitFilteredRecordCursorFactory.class);
                assertEquals(predicate, backend.name(), filtered.getCompiledFilter().getBackendName());
                assertEquals(predicate, 1003, filtered.getCompiledCountOnlyFilter().getNativeInputRows());
            }
        }
    }

    @Test
    public void cachedFactoriesRebindValuesAndRefreshDictionaryIdsWithoutRecompiling() throws Exception {
        context.getBindVariableService().setStr(0, "alpha");
        context.getBindVariableService().setStr(1, "žluť");
        String in = "select count() from symbols where s in ($1,$2,'alpha')";
        String eq = "select count() from symbols where s='alpha'";
        try (var members = candidate(in); var equal = candidate(eq)) {
            int compiled = compilations.get();
            int originalId;
            try (var reader = engine.getReader("symbols")) { originalId = reader.getSymbolMapReader(2).keyOf("alpha"); }
            check(members, in); check(equal, eq);
            context.getBindVariableService().setStr(0, "absent");
            context.getBindVariableService().setStr(1, (CharSequence) null);
            check(members, in);
            engine.execute("insert into symbols values (1004,1,'absent','tail','2024-01-01T01:00:00')", context);
            check(members, in);
            engine.execute("truncate table symbols", context);
            engine.execute("insert into symbols values (1,1,'absent','one','2024-01-01'),"
                    + "(2,2,'alpha','two','2024-01-01T00:00:01'),(3,3,null,'three','2024-01-01T00:00:02')", context);
            check(members, in); check(equal, eq);
            try (var reader = engine.getReader("symbols")) {
                assertNotEquals("Exercise changed dictionary IDs", originalId, reader.getSymbolMapReader(2).keyOf("alpha"));
            }
            assertEquals("Runtime binding must not compile a new program", compiled, compilations.get());
            assertEquals(backend.name(), find(members, AsyncJitFilteredRecordCursorFactory.class).getCompiledFilter().getBackendName());
        }
    }

    @Test
    public void symbolColumnTopsAndParquetKeepNullSemantics() throws Exception {
        engine.execute("alter table symbols add column late symbol", context);
        engine.execute("insert into symbols values (1004,1,'alpha','tail','2024-01-01T01:00:00','new')", context);
        for (boolean parquet : new boolean[]{false, true}) {
            if (parquet) engine.execute("alter table symbols convert partition to parquet list '2024-01-01'", context);
            for (String predicate : List.of("late in (null,'new','missing')", "late not in ('new')", "s in ('alpha',null)")) {
                String sql = "select count() from symbols where " + predicate;
                try (var factory = candidate(sql)) {
                    check(factory, sql);
                    assertEquals(backend.name(), find(factory, AsyncJitFilteredRecordCursorFactory.class).getCompiledFilter().getBackendName());
                    // Column-top frames retain the executor's Java fallback in either format.
                    assertEquals(predicate + " parquet=" + parquet, predicate.startsWith("late") ? 1 : 1004,
                            find(factory, AsyncJitFilteredRecordCursorFactory.class).getCompiledCountOnlyFilter().getNativeInputRows());
                }
            }
        }
    }

    @Test
    public void aggregationAndTopKUseParameterizedFilters() throws Exception {
        String group = "select k,count() from symbols where s in ('alpha',null) and v is not null group by k";
        try (var factory = candidate(group)) {
            check(factory, group);
            var atom = find(factory, AsyncGroupByRecordCursorFactory.class).getAtom();
            long rows = 0;
            for (int i = -1; i < 2; i++) rows += ((AggregateSlot) atom.getCompiledUpdater(i)).filterCounters()[0];
            assertEquals(1003, rows);
        }
        String top = "select id,v from symbols where s in ('alpha',null) and v is not null order by v limit 7";
        try (var factory = candidate(top)) {
            check(factory, top);
            var code = (ResolvedTopK) find(factory, AsyncTopKRecordCursorFactory.class).getAtom().getCompiledTopK();
            assertTrue(code.getNativeRows() > 0);
            check(factory, top);
        }
    }

    @Test
    public void boundedSelectionAndLargeSetFallbackPreserveResults() throws Exception {
        String sql = "select id from symbols where s in ('alpha','žluť') order by ts limit 7";
        try (var factory = candidate(sql)) {
            check(factory, sql);
            var code = find(factory, AsyncJitFilteredRecordCursorFactory.class).getCompiledFilter();
            assertEquals(backend.name(), code.getBackendName());
            assertTrue(code.getNativeLimitRows() > 0);
        }
        String large = "select count() from symbols where s in ('alpha',"
                + java.util.stream.IntStream.range(0, 16).mapToObj(i -> "'missing-" + i + "'").collect(java.util.stream.Collectors.joining(",")) + ")";
        try (var factory = candidate(large)) {
            check(factory, large);
            for (RecordCursorFactory input = factory; input != null; input = input.getBaseFactory()) {
                if (input.getCompiledFilter() != null)
                    assertNull("Large sets retain existing filtering", input.getCompiledFilter().getBackendName());
            }
        }
    }

    private RecordCursorFactory candidate(String sql) throws Exception {
        context.setQueryCodeGenerator(compiler);
        return Demo.compileSql(engine, context, sql, true);
    }

    private void check(RecordCursorFactory factory, String sql) throws Exception {
        var actual = Demo.readSql(factory, context, factory.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (var stock = Demo.compileSql(engine, context, sql, true)) {
            assertEquals(sql, Demo.readSql(stock, context, stock.getMetadata().getColumnCount()), actual);
        } finally {
            context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            context.setQueryCodeGenerator(compiler);
        }
    }

    private static <T> T find(RecordCursorFactory factory, Class<T> type) {
        for (; factory != null; factory = factory.getBaseFactory()) if (type.isInstance(factory)) return type.cast(factory);
        throw new AssertionError("Missing " + type.getSimpleName());
    }
}
