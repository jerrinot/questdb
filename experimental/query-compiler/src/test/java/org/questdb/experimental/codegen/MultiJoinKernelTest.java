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
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction;
import io.questdb.griffin.engine.functions.lt.LtLongFunctionFactory.LtLongFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.*;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class MultiJoinKernelTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private long memory;
    private int maxFrameRows = 65;

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var configuration = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return maxFrameRows; }
            @Override public int getSqlPageFrameMinRows() { return (maxFrameRows + 1) / 2; }
        };
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        engine.execute("create table p as (select cast(case when x%19=0 then null else x%31-15 end as int) k,"
                + "cast(case when x%13=0 then null else x%17-8 end as int) j,"
                + "case when x%11=0 then null else x end v from long_sequence(2003))", context);
        engine.execute("create table a as (select cast(case when x%19=0 then null else x%31-15 end as int) k,"
                + "case when x%5=0 then null else x*11 end v from long_sequence(93))", context);
        engine.execute("create table b as (select cast(case when x%13=0 then null else x%17-8 end as int) k,"
                + "case when x%7=0 then null else x*17 end v from long_sequence(51))", context);
    }

    @After public void tearDown() {
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void duplicateProductsPayloadsMapGrowthAndSelectionMatchSql() throws Exception {
        for (boolean copied : new boolean[]{false, true}) {
            for (boolean keyed : new boolean[]{false, true}) {
                check(2, keyed, copied, false);
                check(3, keyed, copied, true);
            }
        }
    }

    @Test public void reducedMembershipMatchesExistingBloomAcrossFullWidthKeys() throws Exception {
        try (var keys = new DirectLongList(64, MemoryTag.NATIVE_JIT);
             var set = new IntegralMembership()) {
            keys.add(Long.MIN_VALUE);
            keys.add(Long.MAX_VALUE);
            for (int i = 0; i < 64; i++) keys.add(Hash.hashLong64(i));
            set.prepareReducedKeys(keys, context.getMemoryTracker(), context.getCircuitBreaker());
            assertFalse(set.hasBitmap());
            Builder b = new Builder();
            b.when(IntegralMembership.containsJoin(b, IntegralMembership.bind(b, COLUMNS), load(at(COLUMNS, IntegralMembership.BYTES)), true), hit -> hit.exit(i64(1)));
            b.exit(i64(0));
            long descriptor = Unsafe.malloc(IntegralMembership.BYTES + 8, MemoryTag.NATIVE_JIT);
            try (var code = new LlvmBackend().compile(new Program(b.block()), temporary.newFolder().toPath())) {
                set.write(descriptor);
                for (long i = 0; i < keys.size(); i++) {
                    Unsafe.putLong(descriptor + IntegralMembership.BYTES, keys.get(i));
                    assertEquals(1, code.run(descriptor, 1, 0));
                }
                for (int i = 0; i < 10000; i++) {
                    long key = Hash.hashLong64(i);
                    Unsafe.putLong(descriptor + IntegralMembership.BYTES, key);
                    assertEquals("key=" + key, set.contains(key) ? 1 : 0, code.run(descriptor, 1, 0));
                }
                keys.clear();
                keys.add(-2); keys.add(0); keys.add(3);
                set.prepareReducedKeys(keys, context.getMemoryTracker(), context.getCircuitBreaker());
                set.write(descriptor);
                assertTrue(set.hasBitmap());
                for (long key = -4; key < 5; key++) {
                    Unsafe.putLong(descriptor + IntegralMembership.BYTES, key);
                    assertEquals(set.contains(key) ? 1 : 0, code.run(descriptor, 1, 0));
                }
            } finally { Unsafe.free(descriptor, IntegralMembership.BYTES + 8, MemoryTag.NATIVE_JIT); }
        }
    }

    @Test public void dependencyReductionBoundsPreparationAndReleasesScratch() throws Exception {
        engine.execute("create table budget_a as (select cast(x as int) k,cast(x%17 as int) v from long_sequence(31))", context);
        try (var a = new Input("budget_a", true); var b = new Input("b", true);
             var tracker = new MemoryTracker() {
                 @Override public long getQueryId() { return 1; }
                 @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             };
             var set = new IntegralMembership();
             var child = new JoinDependencyFilter.Lookup(b.frozen, new int[]{0}, new int[]{ColumnType.INT}, null, null)) {
            assertFalse(JoinDependencyFilter.affordable(a.frozen, -1, tracker));
            assertFalse(JoinDependencyFilter.affordable(a.frozen, a.frozen.getRowCount(), tracker));
            Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 64);
            assertFalse(JoinDependencyFilter.affordable(a.frozen, 10000, tracker));
            assertNull(JoinDependencyFilter.prepareLinks(a.frozen, new int[]{ColumnType.INT}, child,
                    10000, tracker, context.getCircuitBreaker()));
            Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 1024);
            assertTrue(JoinDependencyFilter.affordable(a.frozen, 10000, tracker));
            var children = new ObjList<JoinDependencyFilter.Lookup>();
            children.add(child);
            assertTrue(JoinDependencyFilter.prepare(a.frozen, ColumnType.INT, children, set, tracker, context.getCircuitBreaker()));
            assertTrue(tracker.getUsed() > 0);
            assertTrue("only retained filter remains", tracker.getUsed() <= a.frozen.getKeyCount() * 8 + 32);
            try (var probe = b.frozen.newProbe()) {
                a.frozen.forEachKey(key -> {
                    probe.findUnchecked(key % 17);
                    if (probe.hasNext()) assertTrue(set.contains(key));
                }, context.getCircuitBreaker());
            }
            set.close();
            assertEquals(0, tracker.getUsed());
            assertNull(JoinDependencyFilter.prepareLinks(a.frozen, new int[]{ColumnType.INT}, child,
                    -1, tracker, context.getCircuitBreaker()));
            try (var links = JoinDependencyFilter.prepareLinks(a.frozen, new int[]{ColumnType.INT}, child,
                    10000, tracker, context.getCircuitBreaker());
                 var parent = a.frozen.newProbe(); var probe = b.frozen.newProbe()) {
                assertNotNull(links);
                assertEquals(128, tracker.getUsed());
                for (long row = 0; row < a.frozen.getRowCount(); row++) {
                    parent.recordAt(a.frozen.getRowHandle(row));
                    probe.findUnchecked(parent.getRecord().getInt(0));
                    long head = Integer.toUnsignedLong(Unsafe.getInt(links.getAddress() + row * 4)) * 8;
                    assertEquals(probe.getNextRowLink(), head);
                }
            }
            assertEquals(0, tracker.getUsed());
            a.build.close();
            assertThrows(IllegalStateException.class, () -> a.frozen.getRowHandle(0));
        }
    }

    @Test public void uniqueAndEmptyBuildsMatchSql() throws Exception {
        engine.execute("truncate table a", context);
        engine.execute("truncate table b", context);
        engine.execute("insert into a select cast(x-15 as int),x from long_sequence(31)", context);
        engine.execute("insert into b select cast(x-8 as int),x from long_sequence(17)", context);
        check(2, false, false, false);
        check(3, true, true, false);
        engine.execute("truncate table b", context);
        check(2, false, false, false);
        check(2, true, false, false);
    }

    @Test public void cancellationInterruptsDuplicateProductsAndFreshSlotCanRun() throws Exception {
        engine.execute("truncate table p", context);
        engine.execute("truncate table a", context);
        engine.execute("truncate table b", context);
        engine.execute("insert into p values (1,1,1)", context);
        engine.execute("insert into a select 1,x from long_sequence(1001)", context);
        engine.execute("insert into b select 1,x from long_sequence(1001)", context);
        try (var p = new Input("p", false); var a = new Input("a", true); var b = new Input("b", true)) {
            var functions = functions(2); var types = valueTypes(functions);
            var polls = new java.util.concurrent.atomic.AtomicInteger();
            try (var value = new SimpleMapValue(types.getColumnCount());
                 var code = compile(p, functions, types, 2, value, null, -1, false);
                 var slot = code.newUpdater(() -> {
                     if (polls.incrementAndGet() == 3) throw new IllegalStateException("cancelled");
                 })) {
                for (int i = 0; i < functions.size(); i++) functions.getQuick(i).setEmpty(value);
                p.record.init(p.pool.navigateTo(0));
                var builds = new FrozenHashJoinBuild.IntKeyed[]{a.frozen, b.frozen};
                assertEquals("cancelled", assertThrows(IllegalStateException.class,
                        () -> slot.updateMulti(p.record, p.frames.getFrameRowCount(0), 0, builds, value)).getMessage());
                assertTrue(value.getLong(0) > 0);
                assertTrue(value.getLong(0) < 1_002_001);
                try (var retry = code.newUpdater(() -> { })) {
                    for (int i = 0; i < functions.size(); i++) functions.getQuick(i).setEmpty(value);
                    assertTrue(retry.updateMulti(p.record, p.frames.getFrameRowCount(0), 0, builds, value));
                    assertEquals(1_002_001, value.getLong(0));
                }
            }
        }
    }

    @Test public void unsupportedBindingDeclinesBeforeChangingAggregateState() throws Exception {
        try (var p = new Input("p", false); var a = new Input("a", true); var b = new Input("b", true)) {
            var functions = functions(2); var types = valueTypes(functions);
            try (var value = new SimpleMapValue(types.getColumnCount());
                 var code = compile(p, functions, types, 2, value, null, -1, false);
                 var slot = code.newUpdater(() -> { })) {
                value.putLong(0, 42);
                p.record.init(p.pool.navigateTo(0));
                assertFalse(slot.updateMulti(p.record, 1, 0, new FrozenHashJoinBuild.IntKeyed[]{a.frozen}, value));
                var unsupported = (FrozenHashJoinBuild.IntKeyed) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[]{FrozenHashJoinBuild.IntKeyed.class}, (proxy, method, args) ->
                                method.getName().equals("writeNativePayload") ? false : method.invoke(b.frozen, args));
                assertFalse(slot.updateMulti(p.record, 1, 0, new FrozenHashJoinBuild.IntKeyed[]{a.frozen, unsupported}, value));
                assertEquals(42, value.getLong(0));
                assertEquals(0, slot.getNativeRows());
            }
        }
    }

    @Test public void adaptiveFramesPreserveSelectiveProductsAcrossMapGrowthAndSelection() throws Exception {
        maxFrameRows = 16384;
        engine.execute("truncate table p", context);
        engine.execute("truncate table a", context);
        engine.execute("truncate table b", context);
        engine.execute("insert into p select cast(x%31-15 as int),cast(x%17-8 as int),x from long_sequence(200003)", context);
        engine.execute("insert into a select cast(x%16*2-15 as int),x from long_sequence(93)", context);
        engine.execute("insert into b select cast(x%9*2-8 as int),x from long_sequence(51)", context);
        check(2, true, false, false);
        check(3, false, true, true);
    }

    @Test public void frozenMembershipHandlesBitmapWideNullDomainAndEmptyBuilds() throws Exception {
        var b = new Ir.Builder();
        b.exit(new Ir.Select(IntegralMembership.containsJoin(b, IntegralMembership.bind(b, Ir.COLUMNS),
                Ir.binary(Ir.Op.SUB, Ir.ROWS, Ir.i64(2147483648L))), Ir.i64(1), Ir.i64(0)));
        try (var code = new LlvmBackend().compile(new Ir.Program(b.block()), temporary.newFolder().toPath())) {
            long descriptor = Unsafe.malloc(IntegralMembership.BYTES, MemoryTag.NATIVE_JIT);
            try {
                for (String keys : new String[]{"cast(x*2-50 as int)", "cast(x-25 as int)", "cast(x*101-50 as int)",
                        "cast(case when x=1 then null when x=2 then 2147483647 else x*1000000 end as int)"}) {
                    engine.execute("truncate table a", context);
                    engine.execute("insert into a select " + keys + ",x from long_sequence(51)", context);
                    try (var a = new Input("a", true); var set = new IntegralMembership()) {
                        var distinct = new java.util.HashSet<Integer>();
                        assertTrue(a.frozen.forEachKey(distinct::add, context.getCircuitBreaker()));
                        assertEquals(a.frozen.getKeyCount(), distinct.size());
                        set.prepareJoin(a.frozen, context.getMemoryTracker(), context.getCircuitBreaker(), 1024);
                        set.write(descriptor);
                        for (int key : distinct) {
                            assertTrue("no false negatives: " + key, set.contains(key));
                            assertEquals(1, code.run(descriptor, key + 2147483648L, 0));
                        }
                        for (int key = -60; key < 110; key++) {
                            if (!distinct.contains(Numbers.INT_NULL)) assertEquals(distinct.contains(key), set.contains(key));
                            assertEquals(set.contains(key) ? 1 : 0, code.run(descriptor, key + 2147483648L, 0));
                        }
                    }
                }
                engine.execute("truncate table a", context);
                try (var a = new Input("a", true); var set = new IntegralMembership()) {
                    set.prepareJoin(a.frozen, context.getMemoryTracker(), context.getCircuitBreaker(), 1024);
                    set.write(descriptor);
                    for (int key : new int[]{0, Numbers.INT_NULL, Integer.MAX_VALUE}) {
                        assertFalse(set.contains(key));
                        assertEquals(0, code.run(descriptor, key + 2147483648L, 0));
                    }
                }
            } finally {
                Unsafe.free(descriptor, IntegralMembership.BYTES, MemoryTag.NATIVE_JIT);
            }
        }
    }

    @Test public void conservativeBoundsRemainSafeWithoutKeyIteration() throws Exception {
        engine.execute("truncate table a", context);
        engine.execute("insert into a select cast(x*2-50 as int),x from long_sequence(51)", context);
        try (var a = new Input("a", true); var set = new IntegralMembership()) {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
            for (boolean defaults : new boolean[]{false, true}) {
                var boundsOnly = (FrozenHashJoinBuild.IntKeyed) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[]{FrozenHashJoinBuild.IntKeyed.class}, (proxy, method, args) -> {
                            if ((method.getName().equals("forEachKey") || method.getName().equals("forEachIntegralKey"))) return false;
                            if (defaults && method.isDefault()) return java.lang.reflect.InvocationHandler.invokeDefault(proxy, method, args);
                            return method.invoke(a.frozen, args);
                        });
                set.prepareJoin(boundsOnly, context.getMemoryTracker(), context.getCircuitBreaker());
                for (int key = -48; key <= 52; key += 2) assertTrue(set.contains(key));
                assertEquals(defaults, set.contains(-51));
                assertEquals(defaults, set.contains(53));
                assertEquals(defaults, set.isUniversal());
                assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
            }
            // A large producer can offer bounds without iteration. Do not allocate a 512MiB bitmap
            // just to discover that the optional iterator is unavailable.
            var large = (FrozenHashJoinBuild.IntKeyed) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{FrozenHashJoinBuild.IntKeyed.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "getKeyCount" -> 1L << 28;
                        case "getMinKey" -> Integer.MIN_VALUE;
                        case "getMinIntegralKey" -> (long) Integer.MIN_VALUE;
                        case "getMaxKey" -> Integer.MAX_VALUE;
                        case "getMaxIntegralKey" -> (long) Integer.MAX_VALUE;
                        case "forEachKey", "forEachIntegralKey" -> false;
                        default -> method.invoke(a.frozen, args);
                    });
            long mallocs = Unsafe.getMallocCount();
            set.prepareJoin(large, context.getMemoryTracker(), context.getCircuitBreaker());
            assertEquals("An unavailable iterator must not allocate a bitmap", mallocs, Unsafe.getMallocCount());
            assertTrue(set.isUniversal());
            assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        }
    }

    @Test public void expandedMembershipPreservesDuplicateRetriesAndSelectedRows() throws Exception {
        maxFrameRows = 4096;
        engine.execute("truncate table p", context);
        engine.execute("truncate table a", context);
        engine.execute("truncate table b", context);
        engine.execute("insert into p select cast(case when x%19=0 then null else x%1024*101 end as int),"
                + "cast(x%16384 as int),case when x%11=0 then null else x end from long_sequence(200003)", context);
        engine.execute("insert into a select cast(x%257*101 as int),"
                + "case when x%5=0 then null else x*11 end from long_sequence(771)", context);
        engine.execute("insert into b select cast(x%8192 as int),x from long_sequence(16384)", context);
        check(2, true, false, false);
        check(3, true, true, false);
        check(2, false, false, true);
    }

    @Test public void sharedMembershipReservesExistingFiltersAndReopensUnderTightBudgets() throws Exception {
        engine.execute("truncate table a", context);
        engine.execute("truncate table b", context);
        engine.execute("insert into a select cast(x*101 as int),x from long_sequence(51)", context);
        engine.execute("insert into b select cast(x*2 as int),x from long_sequence(201)", context);
        try (var p = new Input("p", false); var a = new Input("a", true); var b = new Input("b", true);
             var tracker = new MemoryTracker() {
                 @Override public long getQueryId() { return 1; }
                 @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }) {
            var functions = functions(2); var types = valueTypes(functions);
            try (var value = new SimpleMapValue(types.getColumnCount());
                 var code = compile(p, functions, types, 2, value, null, -1, false)) {
                long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
                var builds = new FrozenHashJoinBuild.IntKeyed[]{a.frozen, b.frozen};
                // The sparse first build must not consume the later build's existing bitmap.
                for (long[] admission : new long[][]{
                        {10000, 0, 688}, {-1, 0, 56}, {500, 0, 56}, {0, 0, 56},
                        {10000, 56, 56}, {10000, 680, 56}, {10000, 688, 688}}) {
                    code.clearExecution();
                    assertEquals(0, tracker.getUsed());
                    Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, admission[1]);
                    code.prepareBuilds(builds, admission[0], tracker, context.getCircuitBreaker());
                    assertEquals(admission[2], tracker.getUsed());
                    assertEquals(before + admission[2], Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
                }
                code.clearExecution();
                assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
                assertEquals(0, tracker.getUsed());
            }
        }
    }

    @Test public void failedMembershipPreparationReleasesEarlierBitmapAndCanRetry() throws Exception {
        engine.execute("truncate table a", context);
        engine.execute("insert into a select cast(x*2-50 as int),x from long_sequence(51)", context);
        engine.execute("truncate table b", context);
        engine.execute("insert into b select cast(x*2-50 as int),x from long_sequence(51)", context);
        try (var p = new Input("p", false); var a = new Input("a", true); var b = new Input("b", true)) {
            var functions = functions(2); var types = valueTypes(functions);
            try (var value = new SimpleMapValue(types.getColumnCount());
                 var code = compile(p, functions, types, 2, value, null, -1, false);
                 var slot = code.newUpdater(() -> { })) {
                var failing = (FrozenHashJoinBuild.IntKeyed) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[]{FrozenHashJoinBuild.IntKeyed.class}, (proxy, method, args) -> {
                            if ((method.getName().equals("forEachKey") || method.getName().equals("forEachIntegralKey"))) throw new IllegalStateException("cancel preparation");
                            return method.invoke(b.frozen, args);
                        });
                long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
                assertEquals("cancel preparation", assertThrows(IllegalStateException.class, () -> code.prepareBuilds(
                        new FrozenHashJoinBuild.IntKeyed[]{a.frozen, failing}, -1, context.getMemoryTracker(), context.getCircuitBreaker())).getMessage());
                assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
                var builds = new FrozenHashJoinBuild.IntKeyed[]{a.frozen, b.frozen};
                code.prepareBuilds(builds, -1, context.getMemoryTracker(), context.getCircuitBreaker());
                for (int i = 0; i < functions.size(); i++) functions.getQuick(i).setEmpty(value);
                for (int f = 0; f < p.frames.getFrameCount(); f++) {
                    p.record.init(p.pool.navigateTo(f));
                    assertTrue(slot.updateMulti(p.record, p.frames.getFrameRowCount(f), 0, builds, value));
                }
                try (var control = engine.select("select count(),sum(p.v),sum(a.v),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k", context);
                     var cursor = control.getCursor(context)) {
                    assertTrue(cursor.hasNext());
                    for (int i = 0; i < functions.size(); i++) assertEquals(cursor.getRecord().getLong(i), functions.getQuick(i).getLong(value));
                }
                code.clearExecution();
                assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
            }
        }
    }

    private ObjList<GroupByFunction> functions(int builds) {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountLongConstGroupByFunction());
        functions.add(new SumLongGroupByFunction(LongColumn.newInstance(2)));
        for (int i = 0; i < builds; i++) functions.add(new SumLongGroupByFunction(LongColumn.newInstance(3 + i)));
        return functions;
    }

    @Test public void countedRowsPreserveCountArithmetic() throws Exception {
        Builder b = new Builder();
        new Pipeline.Count().addRows(b, STATE, binary(Op.MUL, ROWS, load(COLUMNS)));
        b.exit(0);
        try (var code = new LlvmBackend().compile(new Program(b.block()), temporary.newFolder().toPath());
             var value = new SimpleMapValue(1); var ordinary = new SimpleMapValue(1);
             var factor = new DirectLongList(1, MemoryTag.NATIVE_JIT)) {
            var function = new CountLongConstGroupByFunction();
            function.initValueTypes(new ArrayColumnTypes());
            value.putLong(0, Long.MAX_VALUE - 2);
            ordinary.putLong(0, Long.MAX_VALUE - 2);
            Unsafe.putLong(factor.getAddress(), 1);
            code.run(factor.getAddress(), 5, value.getAddress(0));
            for (int i = 0; i < 5; i++) function.computeNext(ordinary, null, i);
            assertEquals(ordinary.getLong(0), value.getLong(0));
            for (long multiplicity : new long[]{0, 3, Long.MAX_VALUE, Long.MIN_VALUE}) {
                value.putLong(0, 7);
                Unsafe.putLong(factor.getAddress(), multiplicity);
                code.run(factor.getAddress(), 6, value.getAddress(0));
                assertEquals(7 + 6 * multiplicity, value.getLong(0));
            }
        }
    }

    @Test public void repeatedUpdateLengthsRejectCrossRootOverflow() throws Exception {
        try (var a = new Input("a", true); var b = new Input("b", true);
             var left = new DirectLongList(a.frozen.getRowCount(), MemoryTag.NATIVE_JIT);
             var right = new DirectLongList(b.frozen.getRowCount(), MemoryTag.NATIVE_JIT)) {
            for (long i = 0; i < a.frozen.getRowCount(); i++) left.add(0);
            for (long i = 0; i < b.frozen.getRowCount(); i++) right.add(0);
            var builds = new FrozenHashJoinBuild[]{a.frozen, b.frozen};
            var counts = new DirectLongList[]{left, right};
            assertTrue(JoinDependencyFilter.repetitionsFit(builds, counts, context.getCircuitBreaker()));
            left.set(0, Long.MAX_VALUE);
            right.set(0, 1);
            assertTrue(JoinDependencyFilter.repetitionsFit(builds, counts, context.getCircuitBreaker()));
            right.set(0, 2);
            assertFalse(JoinDependencyFilter.repetitionsFit(builds, counts, context.getCircuitBreaker()));
            left.set(0, -1);
            assertFalse(JoinDependencyFilter.repetitionsFit(builds, counts, context.getCircuitBreaker()));
        }
    }

    @Test public void countPreparationHonoursBudgetAndReleasesOnFailure() throws Exception {
        try (var p = new Input("p", false); var a = new Input("a", true); var b = new Input("b", true);
             var tracker = new MemoryTracker() {
                 @Override public long getQueryId() { return 1; }
                 @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }) {
            var functions = new ObjList<GroupByFunction>();
            functions.add(new CountLongConstGroupByFunction());
            var types = valueTypes(functions);
            try (var value = new SimpleMapValue(1);
                 var code = compile(p, functions, types, 2, value, null, -1, false)) {
                var builds = new FrozenHashJoinBuild[]{a.frozen, b.frozen};
                code.prepareBuilds(builds, -1, tracker, context.getCircuitBreaker());
                long filters = tracker.getUsed();
                code.clearExecution();
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, filters + 8);
                code.prepareBuilds(builds, 10000, tracker, context.getCircuitBreaker());
                assertEquals(filters, tracker.getUsed());
                code.clearExecution();
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                code.prepareBuilds(builds, 10000, tracker, context.getCircuitBreaker());
                assertEquals(filters + 8 * (a.frozen.getRowCount() + b.frozen.getRowCount()), tracker.getUsed());
                // These direct bindings retain payload rows even though COUNT observes no payload.
                // Their sixteen-byte heap stride must still index dense eight-byte counts correctly.
                functions.getQuick(0).setEmpty(value);
                try (var slot = code.newUpdater(() -> { })) {
                    for (int frame = 0; frame < p.frames.getFrameCount(); frame++) {
                        p.record.init(p.pool.navigateTo(frame));
                        assertTrue(slot.updateMulti(p.record, p.frames.getFrameRowCount(frame), 0, builds, value));
                    }
                }
                try (var control = engine.select("select count() from p join a on p.k=a.k join b on p.j=b.k", context);
                     var cursor = control.getCursor(context)) {
                    assertTrue(cursor.hasNext());
                    assertEquals(cursor.getRecord().getLong(0), value.getLong(0));
                }
                code.clearExecution();
                builds[0] = (FrozenHashJoinBuild.IntKeyed) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[]{FrozenHashJoinBuild.IntKeyed.class}, (proxy, method, args) -> {
                            if (method.getName().equals("copyKeyRowCounts")) throw new IllegalStateException("copy failed");
                            return method.invoke(a.frozen, args);
                        });
                assertThrows(IllegalStateException.class,
                        () -> code.prepareBuilds(builds, 10000, tracker, context.getCircuitBreaker()));
                assertEquals(0, tracker.getUsed());
                builds[0] = a.frozen;
                code.prepareBuilds(builds, 10000, tracker, context.getCircuitBreaker());
                code.clearExecution();
                assertEquals(0, tracker.getUsed());
            }
        }
    }

    @Test public void subtreePreparationIsTransactionalUnderBudgetFailureAndOverflow() throws Exception {
        try (var a = new Input("a", true); var b = new Input("b", true);
             var tracker = new MemoryTracker() {
                 @Override public long getQueryId() { return 1; }
                 @Override public MemoryTrackerWorkload getWorkload() { return MemoryTrackerWorkload.QUERY; }
                 @Override public void close() { destroyNativeBlock(); }
             }; var child = b.frozen.newProbe(); var root = a.frozen.newProbe()) {
            var builds = new FrozenHashJoinBuild[]{a.frozen, b.frozen, b.frozen};
            var descriptions = new CompiledJoin.Build[]{new CompiledJoin.Build(0, 1), new CompiledJoin.Build(3, 1), new CompiledJoin.Build(3, 1)};
            int[] parents = {-1, 0, 0};
            var counts = new DirectLongList[3];
            var links = new DirectLongList[3];
            try {
                child.findUnchecked(0);
                long head = child.getNextRowLink();
                assertTrue(head > 0);
                for (int i = 1; i < 3; i++) {
                    counts[i] = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
                    counts[i].setMemoryTracker(tracker);
                    counts[i].ensureCapacity(b.frozen.getRowCount());
                    assertTrue(b.frozen.copyKeyRowCounts(counts[i].getAddress()));
                    links[i] = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
                    links[i].setMemoryTracker(tracker);
                    links[i].ensureCapacity((a.frozen.getRowCount() + 1) / 2);
                    for (long row = 0; row < a.frozen.getRowCount(); row++) {
                        Unsafe.putInt(links[i].getAddress() + row * 4,
                                row == 0 ? 0 : io.questdb.griffin.engine.CompressedOffsets.compressAligned8(head));
                    }
                }
                long before = tracker.getUsed();
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, before + a.frozen.getRowCount() * 8 - 1);
                assertFalse(JoinDependencyFilter.prepareSubtrees(builds, descriptions, parents, counts, links, tracker, context.getCircuitBreaker()));
                assertEquals(before, tracker.getUsed());
                Unsafe.putLong(tracker.nativeAddress() + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, 0);
                // Product overflow, then a representable row product whose chain sum overflows.
                for (long weight : new long[]{Long.MAX_VALUE, Long.MAX_VALUE / 3}) {
                    for (long row = 0; row < b.frozen.getRowCount(); row++) counts[1].set(row, weight);
                    assertFalse(JoinDependencyFilter.prepareSubtrees(builds, descriptions, parents, counts, links, tracker, context.getCircuitBreaker()));
                    assertEquals(before, tracker.getUsed());
                    assertNull(counts[0]);
                    assertEquals(weight, counts[1].get(0));
                    assertNotNull(links[1]);
                }
                assertTrue(b.frozen.copyKeyRowCounts(counts[1].getAddress()));
                for (boolean fail : new boolean[]{false, true}) {
                    builds[0] = (FrozenHashJoinBuild.IntKeyed) java.lang.reflect.Proxy.newProxyInstance(
                            getClass().getClassLoader(), new Class<?>[]{FrozenHashJoinBuild.IntKeyed.class}, (proxy, method, args) -> {
                                if (method.getName().equals("sumKeyRowWeights")) {
                                    if (fail) throw new IllegalStateException("sum failed");
                                    return false;
                                }
                                return method.invoke(a.frozen, args);
                            });
                    if (fail) assertThrows(IllegalStateException.class,
                            () -> JoinDependencyFilter.prepareSubtrees(builds, descriptions, parents, counts, links, tracker, context.getCircuitBreaker()));
                    else assertFalse(JoinDependencyFilter.prepareSubtrees(builds, descriptions, parents, counts, links, tracker, context.getCircuitBreaker()));
                    assertEquals(before, tracker.getUsed());
                    assertNull(counts[0]);
                    assertNotNull(counts[1]);
                    assertNotNull(links[1]);
                }
                builds[0] = a.frozen;
                assertTrue(JoinDependencyFilter.prepareSubtrees(builds, descriptions, parents, counts, links, tracker, context.getCircuitBreaker()));
                assertEquals(a.frozen.getRowCount() * 8, tracker.getUsed());
                assertNull(counts[1]);
                assertNull(counts[2]);
                assertNull(links[1]);
                assertNull(links[2]);
                // Each matched producer row contributes 3*3; only heap row zero was rejected.
                for (int key = -16; key <= 15; key++) {
                    root.findUnchecked(key == -16 ? Numbers.INT_NULL : key);
                    long rootHead = root.getNextRowLink();
                    if (rootHead == 0) continue;
                    long expected = 0;
                    while (root.hasNext()) {
                        long ordinal = (root.getNextRowLink() - 8) >> 4;
                        expected += ordinal == 0 ? 0 : 9;
                        root.next();
                    }
                    assertEquals(expected, counts[0].get((rootHead - 8) >> 4));
                }
            } finally {
                for (var count : counts) if (count != null) count.close();
                for (var link : links) if (link != null) link.close();
            }
            assertEquals(0, tracker.getUsed());
        }
    }

    private ArrayColumnTypes valueTypes(ObjList<GroupByFunction> functions) {
        var types = new ArrayColumnTypes();
        for (int i = 0; i < functions.size(); i++) functions.getQuick(i).initValueTypes(types);
        return types;
    }

    private CompiledJoin compile(Input p, ObjList<GroupByFunction> functions, ArrayColumnTypes values,
                                 int count, SimpleMapValue value, Unordered8Map map, int groupKey, boolean filtered) throws Exception {
        var joined = new ArrayColumnTypes();
        for (int i = 0; i < 3; i++) joined.add(p.factory.getMetadata().getColumnType(i));
        var builds = new ObjList<CompiledJoin.Build>();
        for (int i = 0; i < count; i++) {
            joined.add(ColumnType.LONG);
            builds.add(new CompiledJoin.Build(i == 1 ? 1 : 0, 1));
        }
        var code = new ResolvedQueryCompiler(new LlvmBackend(), temporary.newFolder().toPath()).compileJoin(
                joined, 3, values, functions, builds, value,
                filtered ? new LtLongFunction(LongColumn.newInstance(3), LongColumn.newInstance(4)) : null,
                groupKey, map);
        assertNotNull(code);
        return code;
    }

    private void check(int count, boolean keyed, boolean copied, boolean selected) throws Exception {
        try (var p = new Input("p", false); var a = new Input("a", true); var b = new Input("b", true)) {
            if (copied) {
                a.frames.copyPayload(a.frozen, context.getCircuitBreaker());
                b.frames.copyPayload(b.frozen, context.getCircuitBreaker());
            }
            var builds = count == 2 ? new FrozenHashJoinBuild.IntKeyed[]{a.frozen, b.frozen}
                    : new FrozenHashJoinBuild.IntKeyed[]{a.frozen, b.frozen, a.frozen};
            var functions = functions(count); var types = valueTypes(functions);
            var updater = GroupByFunctionsUpdaterFactory.getInstance(new BytecodeAssembler(), functions);
            var actual = new ArrayList<List<Long>>();
            int groupKey = keyed ? (copied ? 2 + count : 2) : -1;
            String groupExpression = copied ? (count == 3 ? "c.v" : "b.v") : "p.v";
            try (var value = new SimpleMapValue(types.getColumnCount());
                 var map = keyed ? new Unordered8Map(ColumnType.LONG, types, 4, .5, 32) : null;
                 var code = compile(p, functions, types, count, value, map, groupKey, selected);
                 var slot = code.newUpdater(() -> { });
                 var rows = new DirectLongList(65, MemoryTag.NATIVE_JIT)) {
                long probeRows = 0;
                for (int f = 0; f < p.frames.getFrameCount(); f++) probeRows += p.frames.getFrameRowCount(f);
                code.prepareBuilds(builds, selected ? -1 : probeRows, context.getMemoryTracker(), context.getCircuitBreaker());
                updater.updateEmpty(value);
                value.setNew(true);
                long visited = 0;
                for (int f = 0; f < p.frames.getFrameCount(); f++) {
                    p.record.init(p.pool.navigateTo(f));
                    long n = p.frames.getFrameRowCount(f);
                    rows.clear();
                    if (selected) for (long row = 0; row < n; row++) {
                        p.record.setRowIndex(row);
                        long v = p.record.getLong(2);
                        if (v != Numbers.LONG_NULL && v%2 == 0) rows.add(row);
                    }
                    long input = selected ? rows.size() : n;
                    visited += input;
                    assertTrue(keyed ? slot.updateKeyedMulti(p.record, input, selected ? rows.getAddress() : 0, builds, map, null, updater)
                            : slot.updateMulti(p.record, input, selected ? rows.getAddress() : 0, builds, value));
                }
                if (a.frozen.getRowCount() > 0 && b.frozen.getRowCount() > 0) assertEquals(visited, slot.getNativeRows());
                if (keyed) {
                    try (var cursor = map.getCursor()) {
                        while (cursor.hasNext()) {
                            var record = cursor.getRecord();
                            var row = new ArrayList<Long>();row.add(record.getLong(types.getColumnCount()));
                            for (int i = 0; i < functions.size(); i++) row.add(functions.getQuick(i).getLong(record));
                            actual.add(row);
                        }
                    }
                    assertEquals(map.size(), slot.getNativeMapMisses());
                } else {
                    assertEquals(value.getLong(0) == 0, value.isNew());
                    var row = new ArrayList<Long>();
                    for (int i = 0; i < functions.size(); i++) row.add(functions.getQuick(i).getLong(value));
                    actual.add(row);
                }
            }
            String sql = "select " + (keyed ? groupExpression + "," : "") + "count(),sum(p.v),sum(a.v),sum(b.v)" + (count == 3 ? ",sum(c.v)" : "")
                    + " from p join a on p.k=a.k join b on p.j=b.k" + (count == 3 ? " join a c on p.k=c.k" : "")
                    + (selected ? " where p.v%2=0 and a.v<b.v" : "") + (keyed ? " group by " + groupExpression : "");
            var expected = new ArrayList<List<Long>>();
            try (var factory = engine.select(sql, context); var cursor = factory.getCursor(context)) {
                while (cursor.hasNext()) {
                    var row = new ArrayList<Long>();
                    for (int i = 0; i < factory.getMetadata().getColumnCount(); i++) row.add(cursor.getRecord().getLong(i));
                    expected.add(row);
                }
            }
            actual.sort(java.util.Comparator.comparing(Object::toString));
            expected.sort(java.util.Comparator.comparing(Object::toString));
            assertEquals(sql, expected, actual);
        }
        assertEquals(0, engine.getBusyReaderCount());
    }

    private final class Input implements AutoCloseable {
        final RecordCursorFactory factory;
        final HashJoinBuildFrames frames;
        final PageFrameMemoryPool pool = new PageFrameMemoryPool(engine.getConfiguration());
        final PageFrameMemoryRecord record = new PageFrameMemoryRecord();
        final IntHashJoinBuild build = new IntHashJoinBuild(true, 4, 4);
        final FrozenHashJoinBuild.IntKeyed frozen;
        Input(String table, boolean dimension) throws Exception {
            factory = engine.select(table, context);
            var payload = new IntList();if (dimension) payload.add(1);
            frames = new HashJoinBuildFrames(engine.getConfiguration(), payload, factory.getMetadata());
            frames.of(factory, context);
            pool.of(frames.getAddressCache());record.of(frames.getSymbolTableSource());
            if (dimension) {
                build.open(context.getMemoryTracker(), context.getCircuitBreaker());
                for (int f = 0; f < frames.getFrameCount(); f++) {
                    record.init(pool.navigateTo(f));
                    build.appendFrame(record, 0, frames.getFrameRowCount(f));
                }
                frozen = build.freeze(frames);
            } else frozen = null;
        }
        @Override public void close() {
            record.close();pool.close();build.close();frames.close();factory.close();
        }
    }
}
