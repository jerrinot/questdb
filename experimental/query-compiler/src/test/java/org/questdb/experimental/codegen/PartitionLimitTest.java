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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.mp.*;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class PartitionLimitTest {
    private static final long INPUT_ROWS = 50003;
    private static final String SQL = "select k,label,count() n,sum(v) from t group by k,label";
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final WorkerPoolMode mode;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private String previousCapacity;

    public PartitionLimitTest(WorkerPoolMode mode) { this.mode = mode; }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return List.of(new Object[]{WorkerPoolMode.LEGACY}, new Object[]{WorkerPoolMode.FIBER_HOST});
    }

    @Before public void setUp() throws Exception {
        previousCapacity = System.setProperty("query.codegen.localCapacity", "128");
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 2048; }
            @Override public int getSqlPageFrameMinRows() { return 256; }
            @Override public long getGroupByAllocatorDefaultChunkSize() { return 128 * 1024; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
            @Override public WorkerPoolMode getWorkerPoolMode() { return mode; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = new ResolvedQueryCompiler(new LlvmBackend(), temporary.newFolder().toPath());
        engine.execute("create table t as (select x%10007 k,"
                + "cast(case when (x%10007)%17=0 then null else 'ž-'||cast((x%10007)%31 as string) end as varchar) label,"
                + "case when x%13=0 then null else x*3 end v from long_sequence(" + INPUT_ROWS + "))", context);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        if (previousCapacity == null) System.clearProperty("query.codegen.localCapacity");
        else System.setProperty("query.codegen.localCapacity", previousCapacity);
    }

    @Test public void selectionCanGrowWithoutRescanningAndPreservesRowIds() throws Exception {
        Set<Row> expected = reference();
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, SQL, true);
             var rows = new DirectLongList(0, MemoryTag.NATIVE_RECORD_CHAIN)) {
            var physical = physical(factory);
            assertTrue(physical.prepareRowLimit());
            try (var cursor = physical.getCursor(context)) {
                assertTrue(cursor.selectRows(rows, 0, 10));
                assertEquals(10, rows.size());
                assertEquals("A partition prefix is not the full result", -1, cursor.size());
                assertEquals(INPUT_ROWS, nativeRows(physical));
                long partitionBytes = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                long[] firstIds = new long[10];
                Row[] first = new Row[10];
                for (int i = 0; i < 10; i++) {
                    firstIds[i] = rows.get(i);
                    cursor.recordAt(cursor.getRecord(), firstIds[i]);
                    first[i] = row(cursor.getRecord());
                    assertTrue(expected.contains(first[i]));
                }
                long state = cursor.preComputedStateSize();
                cursor.toTop();
                assertEquals(state, cursor.preComputedStateSize());
                assertEquals(-1, cursor.size());
                assertTrue(cursor.selectRows(rows, 700, 400));
                assertEquals(400, rows.size());
                assertEquals(-1, cursor.size());
                for (int i = 0; i < rows.size(); i++) {
                    cursor.recordAt(cursor.getRecordB(), rows.get(i));
                    assertTrue(expected.contains(row(cursor.getRecordB())));
                }
                for (int i = 0; i < 10; i++) {
                    cursor.recordAt(cursor.getRecord(), firstIds[i]);
                    assertEquals(first[i], row(cursor.getRecord()));
                }
                var count = new RecordCursor.Counter();
                cursor.calculateSize(context.getCircuitBreaker(), count);
                assertEquals(expected.size(), count.get());
                assertEquals(expected.size(), cursor.size());
                assertTrue("Consumed partition pages must be released before the cursor closes",
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_GROUP_BY_FUNCTION) <= partitionBytes - QuestPartitions.PAGE_BYTES);
                assertFalse(cursor.hasNext());
                assertEquals(INPUT_ROWS, nativeRows(physical));
                cursor.toTop();
                Set<Row> actual = new HashSet<>();
                while (cursor.hasNext()) assertTrue(actual.add(row(cursor.getRecord())));
                assertEquals(expected, actual);
                assertTrue(cursor.selectRows(rows, Long.MAX_VALUE - 1, 10));
                assertEquals(0, rows.size());
            }
        }
    }

    @Test public void partialCloseCancellationAndBudgetFailureAllowReuse() throws Exception {
        Set<Row> expected = reference();
        context.setQueryCodeGenerator(compiler);
        try (var tracker = new MemoryTracker() {
            @Override public long getQueryId() { return 1; }
            @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
            @Override public void close() { destroyNativeBlock(); }
        }; var factory = Demo.compileSql(engine, context, SQL, true);
             var rows = new DirectLongList(0, MemoryTag.NATIVE_RECORD_CHAIN)) {
            context.setMemoryTracker(tracker);
            var physical = physical(factory);
            assertTrue(physical.prepareRowLimit());
            try {
                for (int attempt = 0; attempt < 4; attempt++) {
                    var breaker = new AtomicBooleanCircuitBreaker(engine);
                    context.with(breaker);
                    try (var cursor = physical.getCursor(context)) {
                        assertTrue(cursor.selectRows(rows, 0, 10));
                        assertEquals(-1, cursor.size());
                        if (attempt == 1) {
                            breaker.cancel();
                            assertThrows(CairoException.class, () -> cursor.selectRows(rows, 0, 2000));
                        } else if (attempt == 2) {
                            Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, tracker.getUsed() + 1);
                            CairoException failure = assertThrows(CairoException.class, () -> cursor.selectRows(rows, 0, Long.MAX_VALUE));
                            assertTrue(failure.isOutOfMemory());
                        } else if (attempt == 3) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                assertTrue(expected.contains(row(cursor.getRecord())));
                                count++;
                            }
                            assertEquals(expected.size(), count);
                        }
                    }
                    Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                    assertEquals("Partial state must release its query allocation", 0, tracker.getUsed());
                }
            } finally {
                context.setMemoryTracker(null);
            }
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test public void sqlLimitsAndOrderedFallbackPreserveResults() throws Exception {
        Set<Row> expected = reference();
        context.setQueryCodeGenerator(compiler);
        for (String bounds : List.of("0", "10", "700,1100", "-10", "999999,1000009")) {
            try (var factory = Demo.compileSql(engine, context, SQL + " limit " + bounds, true)) {
                int wanted = switch (bounds) { case "0", "999999,1000009" -> 0; case "700,1100" -> 400; default -> 10; };
                for (int repeat = 0; repeat < 2; repeat++) {
                    try (var cursor = factory.getCursor(context)) {
                        int count = 0;
                        while (cursor.hasNext()) { assertTrue(expected.contains(row(cursor.getRecord()))); count++; }
                        assertEquals(wanted, count);
                        cursor.toTop();
                        int rewind = 0;
                        while (cursor.hasNext()) rewind++;
                        assertEquals(count, rewind);
                    }
                }
            }
        }
        String ordered = SQL + " order by n desc,k,label limit 10";
        context.setQueryCodeGenerator(null);
        try (var reference = Demo.compileSql(engine, context, ordered, true)) {
            var wanted = Demo.readSql(reference, context, 4);
            context.setQueryCodeGenerator(compiler);
            try (var candidate = Demo.compileSql(engine, context, ordered, true)) {
                assertEquals(wanted, Demo.readSql(candidate, context, 4));
            }
        }
        engine.execute("truncate table t", context);
        try (var empty = Demo.compileSql(engine, context, SQL + " limit 10", true);
             var cursor = empty.getCursor(context)) {
            assertFalse(cursor.hasNext());
        }
    }

    @Test public void runtimeAdmissionRefreshesKeysAndPreservesUnrestrictedCursor() throws Exception {
        var compilations = new java.util.concurrent.atomic.AtomicInteger();
        var llvm = new LlvmBackend();
        compiler = new ResolvedQueryCompiler(new Backend() {
            @Override public CompiledKernel compile(Ir.Program program, java.nio.file.Path artifacts) throws java.io.IOException {
                compilations.incrementAndGet();
                return llvm.compile(program, artifacts);
            }
            @Override public String name() { return llvm.name(); }
        }, temporary.newFolder().toPath());
        Set<Row> expected = reference();
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, SQL + " limit 10", true)) {
            assertLimited(factory, expected, 10, true);
            int compiled = compilations.get();
            context.setQueryCodeGenerator(null);
            engine.execute("truncate table t", context);
            engine.execute("insert into t select 1000000+x%10007,cast('new' as varchar),x*7 from long_sequence(" + INPUT_ROWS + ")", context);
            expected = reference();
            context.setQueryCodeGenerator(compiler);
            assertLimited(factory, expected, 10, true);
            assertEquals("Runtime keys must not cause recompilation", compiled, compilations.get());
            // Preparing LIMIT does not authorize admission for another cursor opened on its base.
            try (var cursor = physical(factory).getCursor(context)) {
                Set<Row> all = new HashSet<>();
                while (cursor.hasNext()) assertTrue(all.add(row(cursor.getRecord())));
                assertEquals(expected, all);
            }
            // Full consumption can compile the existing deferred partition kernels.
            compiled = compilations.get();
            context.setQueryCodeGenerator(null);
            engine.execute("truncate table t", context);
            engine.execute("insert into t select 7L,cast('hot' as varchar),x from long_sequence(" + INPUT_ROWS + ")", context);
            expected = reference();
            context.setQueryCodeGenerator(compiler);
            assertLimited(factory, expected, 1, false);
            assertEquals(compiled, compilations.get());
        }
    }

    @Test public void skewInsufficientDiscoveryAndSparseFiltersDecline() throws Exception {
        for (String key : List.of("case when x%100=0 then x else 0 end",
                "case when x<=4096 then x else 10 end", "cast(null as long)")) {
            engine.execute("truncate table t", context);
            engine.execute("insert into t select " + key + ",cast('a' as varchar),x from long_sequence(" + INPUT_ROWS + ")", context);
            Set<Row> expected = reference();
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, SQL + " limit 10", true)) {
                assertLimited(factory, expected, Math.min(10, expected.size()), false);
            }
        }
        engine.execute("truncate table t", context);
        engine.execute("insert into t select x,cast('a' as varchar),x from long_sequence(" + INPUT_ROWS + ")", context);
        String filtered = SQL.replace("group by", "where v>49995 group by");
        Set<Row> expected = reference(filtered);
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, filtered + " limit 10", true)) {
            assertLimited(factory, expected, 8, false);
        }
    }

    @Test public void admissionHandlesNullsFiltersAndOffsets() throws Exception {
        engine.execute("truncate table t", context);
        engine.execute("insert into t select case when x%300=1 then null else x%10007 end,"
                + "cast('a' as varchar),case when x%13=0 then null else x end from long_sequence(" + INPUT_ROWS + ")", context);
        for (String predicate : List.of("", "where v>0 ")) {
            String sql = SQL.replace("group by", predicate + "group by");
            Set<Row> expected = reference(sql);
            context.setQueryCodeGenerator(compiler);
            for (String bounds : List.of("10", "4,14", "14,4", "16")) {
                try (var factory = Demo.compileSql(engine, context, sql + " limit " + bounds, true)) {
                    Set<Row> selected = assertLimited(factory, expected, bounds.equals("16") ? 16 : 10, true);
                    if (bounds.equals("10")) assertTrue(selected.stream().anyMatch(r -> r.key == Long.MIN_VALUE));
                }
            }
        }
    }

    @Test public void denseLaterFramesAndCompositeFanoutKeepCompleteValues() throws Exception {
        for (String label : List.of("'a'", "cast(x as string)")) {
            engine.execute("truncate table t", context);
            engine.execute("insert into t select case when x>20000 and x<=24096 then 10 else x end,cast(" + label
                    + " as varchar),x from long_sequence(" + INPUT_ROWS + ")", context);
            Set<Row> expected = reference();
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, SQL + " limit 10", true)) {
                assertLimited(factory, expected, 10, true);
                long[] counters = admissionCounters(physical(factory));
                assertEquals(INPUT_ROWS, counters[0]);
                assertEquals(4096 + 10, counters[1]);
            }
        }
    }

    @Test public void narrowKeysAndColumnTopsUseResolvedStorageSemantics() throws Exception {
        for (String type : List.of("byte", "short", "int")) {
            engine.execute("drop table t", context);
            engine.execute("create table t as (select cast(x%103-50 as " + type
                    + ") k,cast('a' as varchar) label,x v from long_sequence(" + INPUT_ROWS + "))", context);
            String sql = "select k,label,count() n,sum(v) from t group by label,k";
            Set<Row> expected = reference(sql);
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql + " limit 10", true)) {
                assertLimited(factory, expected, 10, true);
            }
        }
        engine.execute("drop table t", context);
        engine.execute("create table t (label varchar,v long)", context);
        engine.execute("insert into t values ('a',1)", context);
        engine.execute("alter table t add column k long", context);
        engine.execute("insert into t select cast('a' as varchar),x,x%10007 from long_sequence(" + INPUT_ROWS + ")", context);
        Set<Row> expected = reference();
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, SQL + " limit 10", true)) {
            assertTrue(assertLimited(factory, expected, 10, true).stream().anyMatch(r -> r.key == Long.MIN_VALUE));
        }
    }

    @Test public void admissionCancellationAndAllocationFailureAllowReuse() throws Exception {
        Set<Row> expected = reference();
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, SQL + " limit 10", true);
             var tracker = new MemoryTracker() {
                 @Override public long getQueryId() { return 1; }
                 @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }) {
            var breaker = new AtomicBooleanCircuitBreaker(engine);
            context.with(breaker);
            context.setMemoryTracker(tracker);
            try {
                assertThrows(CairoException.class, () -> {
                    try (var cursor = factory.getCursor(context)) {
                        breaker.cancel();
                        cursor.hasNext();
                    }
                });
                breaker.reset();
                // The SQL execution owner releases its tracker on close. Inject the failure
                // directly at the physical cursor, as in the partition-merge budget test.
                context.setMemoryTracker(tracker);
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 1);
                assertThrows(CairoException.class, () -> {
                    try (var cursor = physical(factory).getCursor(context)) {
                        cursor.limitGroups(10);
                        cursor.hasNext();
                    }
                });
                assertEquals(0, tracker.getUsed());
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                assertLimited(factory, expected, 10, true);
                assertEquals(0, tracker.getUsed());
            } finally { context.setMemoryTracker(null); }
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    @Test public void discoveryAndScanUseTheSameSnapshot() throws Exception {
        Set<Row> before = reference();
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, SQL + " limit 10", true)) {
            try (var cursor = factory.getCursor(context)) {
                // Append after acquiring the scan snapshot but before discovery. Selected groups
                // must not mix the old input with discovery from a separately opened reader.
                engine.execute("insert into t select x%10007,"
                        + "cast(case when (x%10007)%17=0 then null else 'ž-'||cast((x%10007)%31 as string) end as varchar),"
                        + "x*3 from long_sequence(1000)", context);
                int count = 0;
                while (cursor.hasNext()) { assertTrue(before.contains(row(cursor.getRecord()))); count++; }
                assertEquals(10, count);
            }
            assertEquals(INPUT_ROWS, admissionCounters(physical(factory))[0]);
            Set<Row> after = reference();
            context.setQueryCodeGenerator(compiler);
            assertLimited(factory, after, 10, true);
            assertEquals(2 * INPUT_ROWS + 1000, admissionCounters(physical(factory))[0]);
        }
    }

    @Test public void downstreamConsumersAndLaterParquetFramesKeepFullInput() throws Exception {
        for (String sql : List.of(SQL + " order by k,label limit 10",
                "select * from (" + SQL + ") where n>4 limit 10", SQL + " limit -10")) {
            Set<Row> expected = reference(sql.substring(0, sql.lastIndexOf(" limit")));
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql, true)) {
                assertLimited(factory, expected, 10, false);
            }
        }
        engine.execute("create table p as (select x%10007 k,cast('a' as varchar) label,x v,"
                + "timestamp_sequence('2024-01-01',4000000L) ts from long_sequence(" + INPUT_ROWS
                + ")) timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table p convert partition to parquet list '2024-01-02'", context);
        String sql = SQL.replace("from t", "from p");
        Set<Row> expected = reference(sql);
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql + " limit 10", true)) {
            assertLimited(factory, expected, 10, false);
        }
    }

    @Test public void distinctWindowsAndSharedConsumersKeepCompleteGroups() throws Exception {
        engine.execute("truncate table t", context);
        engine.execute("insert into t select x%1000,cast('a' as varchar),1L from long_sequence(50000)", context);
        String grouped = "select k,label,count() n,sum(v) total from t group by k,label";
        for (String sql : List.of(
                "select distinct k%40 k,label,n,total from (" + grouped + ")",
                "select k,label,count() over() n,total from (" + grouped + ")",
                "select o.k,o.label,o.n,o.total from (" + grouped + ") o "
                        + "join lateral (select x from long_sequence(1) where x<=o.n) sub")) {
            Set<Row> expected = reference(sql);
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql + " limit 10", true)) {
                var plan = new TextPlanSink();
                plan.of(factory, context);
                String text = plan.getSink().toString();
                assertFalse(text, text.contains("groupAdmission: true"));
                if (sql.contains("lateral")) assertTrue(text, text.contains("Shared"));
                try (var cursor = factory.getCursor(context)) {
                    Set<Row> selected = new HashSet<>();
                    while (cursor.hasNext()) {
                        Row row = row(cursor.getRecord());
                        assertTrue("Incomplete group: " + row, expected.contains(row));
                        assertTrue(selected.add(row));
                    }
                    assertEquals(10, selected.size());
                }
            }
        }
    }

    private Set<Row> assertLimited(RecordCursorFactory factory, Set<Row> expected, int count, boolean admitted) throws Exception {
        var physical = physical(factory);
        long before = admissionCounters(physical)[0];
        Set<Row> result = new HashSet<>();
        int keyType = factory.getMetadata().getColumnType(0);
        try (var cursor = factory.getCursor(context)) {
            while (cursor.hasNext()) {
                Row row = row(cursor.getRecord(), keyType);
                assertTrue("Incomplete or unexpected group: " + row, expected.contains(row));
                assertTrue(result.add(row));
            }
            assertEquals(count, result.size());
            if (admitted || cursor.size() >= 0) assertEquals(count, cursor.size());
            cursor.toTop();
            Set<Row> replay = new HashSet<>();
            while (cursor.hasNext()) replay.add(row(cursor.getRecord(), keyType));
            assertEquals(result, replay);
        }
        var plan = new TextPlanSink();
        plan.of(factory, context);
        assertEquals("Unexpected admission decision: " + plan.getSink(), admitted, admissionCounters(physical)[0] > before);
        return result;
    }

    private static long[] admissionCounters(AsyncGroupByRecordCursorFactory factory) {
        long[] result = new long[2];
        for (int i = -1; i < 2; i++) {
            var updater = factory.getAtom().getCompiledUpdater(i);
            if (updater instanceof AggregateSlot slot) {
                long[] counters = slot.groupAdmissionCounters();
                result[0] += counters[0]; result[1] += counters[1];
            }
        }
        return result;
    }

    private Set<Row> reference() throws Exception {
        return reference(SQL);
    }

    private Set<Row> reference(String sql) throws Exception {
        context.setQueryCodeGenerator(null);
        Set<Row> rows = new HashSet<>();
        try (var factory = Demo.compileSql(engine, context, sql, true);
             var cursor = factory.getCursor(context)) {
            while (cursor.hasNext()) rows.add(row(cursor.getRecord(), factory.getMetadata().getColumnType(0)));
        }
        return rows;
    }

    private static Row row(Record record) {
        return row(record, ColumnType.LONG);
    }

    private static Row row(Record record, int keyType) {
        var text = record.getVarcharA(1);
        long key = switch (keyType) {
            case ColumnType.BYTE -> record.getByte(0);
            case ColumnType.SHORT -> record.getShort(0);
            case ColumnType.INT -> record.getInt(0);
            default -> record.getLong(0);
        };
        return new Row(key, text == null ? null : text.toString(), record.getLong(2), record.getLong(3));
    }

    private static AsyncGroupByRecordCursorFactory physical(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsyncGroupByRecordCursorFactory)) factory = factory.getBaseFactory();
        assertNotNull(factory);
        return (AsyncGroupByRecordCursorFactory) factory;
    }

    private static long nativeRows(AsyncGroupByRecordCursorFactory factory) {
        long rows = 0;
        for (int i = -1; i < 2; i++) rows += factory.getAtom().getCompiledUpdater(i).getNativeMapRows();
        return rows;
    }

    private record Row(long key, String label, long count, long sum) { }
}
