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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinDenseRecordCursorFactoryBase;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class AsOfJoinAdaptiveTest extends AbstractCairoTest {
    @Test
    public void testPhaseChangesAcrossKeyTypesAndTolerance() throws Exception {
        assertPhaseChangesAcrossKeyTypesAndTolerance();
    }

    @Test
    public void testPhaseChangesWithFiniteMemoryBudget() throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
        assertPhaseChangesAcrossKeyTypesAndTolerance();
    }

    @Test
    public void testHighCardinalityHistoryUsesBoundedState() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 4096);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 1024);
        assertMemoryLeak(() -> {
            execute("create table r as (select cast(x*1000 as timestamp) ts,cast(x as int) k,"
                    + "x*4294967296+121 kl,cast('s_'||x as symbol) ks,x v "
                    + "from long_sequence(220000)) timestamp(ts)");
            execute("create table l as (select cast((case when x<=98 then 190000+x else 219998+x end)*1000 as timestamp) ts,"
                    + "cast(k as int) k,k*4294967296+121 kl,cast('s_'||k as symbol) ks "
                    + "from (select x,case when x<=2 or x>98 then 219999 else -x end k from long_sequence(100))) timestamp(ts)");
            long[] expected = new long[100];
            Arrays.fill(expected, Long.MIN_VALUE);
            expected[98] = expected[99] = 219999;
            for (String keys : new String[]{"k", "kl", "ks", "k,kl", "ks,kl"}) {
                for (String right : new String[]{"r", "(select * from r where v%3=0)"}) {
                    assertUnderNativeLimit("select b.v from l a asof join " + right + " b on (" + keys + ")", expected);
                }
            }
        });
    }

    @Test
    public void testOversizedVarcharKeyUsesLookup() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 4096);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 1024);
        assertMemoryLeak(() -> {
            execute("create table r as (select cast(x as timestamp) ts,cast('" + "x".repeat(8192)
                    + "'||x as varchar) k,x v from long_sequence(128)) timestamp(ts)");
            execute("create table l as (select cast(100 as timestamp) ts,cast('missing' as varchar) k "
                    + "from long_sequence(130)) timestamp(ts)");
            long[] expected = new long[130];
            Arrays.fill(expected, Long.MIN_VALUE);
            assertUnderNativeLimit("select b.v from l a asof join r b on (k)", expected);
        });
    }

    @Test
    public void testFilteredPhaseChangesAcrossKeyTypesAndTolerance() throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8 * 1024 * 1024L);
        assertPhaseChangesAcrossKeyTypesAndTolerance(true);
    }

    @Test
    public void testFilteredBindsAndStatefulPredicate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table r as (select cast(x as timestamp) ts,cast(x%97 as int) k,"
                    + "cast(x%97 as double) kd,cast('k'||(x%97) as varchar) kv,"
                    + "cast('k'||(x%97) as string) kt,x v from long_sequence(10000)) timestamp(ts)");
            execute("create table l as (select cast(3000+x as timestamp) ts,cast(x*31%97 as int) k,"
                    + "cast(x*31%97 as double) kd,cast('k'||(x*31%97) as varchar) kv,"
                    + "cast('k'||(x*31%97) as string) kt from long_sequence(512)) timestamp(ts)");
            bindVariableService.setLong(0, 0);
            for (String key : new String[]{"k", "kd", "kv", "kt"}) {
                String body = "a.ts,b.v from l a asof join (select v,ts," + key + " from r where v%3=$1) b on (" + key + ")";
                try (RecordCursorFactory reference = select("select /*+ asof_linear(a b) */ " + body);
                     RecordCursorFactory candidate = select("select " + body)) {
                    for (int remainder = 0; remainder < 3; remainder++) {
                        bindVariableService.setLong(0, remainder);
                        try (RecordCursor expected = reference.getCursor(sqlExecutionContext);
                             RecordCursor actual = candidate.getCursor(sqlExecutionContext)) {
                            TestUtils.assertEquals(expected, reference.getMetadata(), actual, candidate.getMetadata(), false);
                            AsOfJoinDenseRecordCursorFactoryBase base = adaptiveFactory(candidate);
                            Assert.assertTrue(base instanceof FilteredAsOfJoinFastRecordCursorFactory);
                            Assert.assertTrue("filtered scan transition", base.isForwardScanMode());
                            expected.toTop();
                            actual.toTop();
                            TestUtils.assertEquals(expected, reference.getMetadata(), actual, candidate.getMetadata(), false);
                        }
                    }
                }
            }
            try (RecordCursorFactory factory = select("select b.v from l a asof join "
                    + "(select * from r where rnd_double(0)>-1.0) b on (k)");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                AsOfJoinDenseRecordCursorFactoryBase base = adaptiveFactory(factory);
                Assert.assertTrue(base instanceof FilteredAsOfJoinFastRecordCursorFactory);
                int rows = 0;
                while (cursor.hasNext()) {
                    Assert.assertFalse("stateful predicate must not cache matches", base.isForwardScanMode());
                    rows++;
                }
                Assert.assertEquals(512, rows);
            }
        });
    }

    private static AsOfJoinDenseRecordCursorFactoryBase adaptiveFactory(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsOfJoinDenseRecordCursorFactoryBase)) {
            factory = factory.getBaseFactory();
        }
        Assert.assertNotNull(factory);
        return (AsOfJoinDenseRecordCursorFactoryBase) factory;
    }

    private void assertUnderNativeLimit(String sql, long[] expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            for (int reopen = 0; reopen < 2; reopen++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 2 * 1024 * 1024);
                    try {
                        for (int rewind = 0; rewind < 2; rewind++) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                Assert.assertTrue(count < expected.length);
                                Assert.assertEquals(expected[count++], cursor.getRecord().getLong(0));
                            }
                            Assert.assertEquals(expected.length, count);
                            cursor.toTop();
                        }
                    } finally {
                        Unsafe.setRssMemLimit(0);
                    }
                }
            }
        }
    }

    private void assertPhaseChangesAcrossKeyTypesAndTolerance() throws Exception {
        assertPhaseChangesAcrossKeyTypesAndTolerance(false);
    }

    private void assertPhaseChangesAcrossKeyTypesAndTolerance(boolean filtered) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 128);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 128);
        assertMemoryLeak(() -> {
            execute("create table r as (select cast(x/3*1000000 as timestamp) ts, "
                    + "cast(case when x%41=0 then null else x%97 end as int) k, "
                    + "x%17*4294967296+121 kl, "
                    + "cast(case when x%41=0 then null else 's_'||(x%97) end as symbol) ks, x v "
                    + "from long_sequence(180000)) timestamp(ts) partition by hour");
            String position = "case when x<=256 then 3000+x when x<=512 then 4000+(x-256)*600 "
                    + "when x<=1024 then 157600+(x-512) else 159136+(x-1024)*24 end";
            execute("create table l as (select cast(at/3*1000000000 as timestamp_ns) ts, "
                    + "cast(case when x>1024 then case when at%41=0 then null else at%97 end "
                    + "when x%37=0 then null when x%53=0 then -99 else x*31%97 end as int) k, "
                    + "(case when x>1024 then at%17 else x*11%17 end)*4294967296+121 kl, "
                    + "cast(case when x>1024 then case when at%41=0 then null else 's_'||(at%97) end "
                    + "when x%37=0 then null when x%53=0 then 'missing' else 's_'||(x*31%97) end as symbol) ks "
                    + "from (select x," + position + " at from long_sequence(1536))) timestamp(ts) partition by hour");
            for (int storage = 0; storage < 3; storage++) {
                if (storage == 1) {
                    execute("alter table r alter column ks add index");
                } else if (storage == 2) {
                    execute("alter table r convert partition to parquet where ts < '1970-01-01T12'");
                }
                for (String keys : new String[]{"k", "kl", "ks", "k,kl", "ks,kl"}) {
                    for (String tolerance : new String[]{"", " tolerance 20s"}) {
                        assertEquivalent("l", filtered ? "(select ks,v,kl,ts,k from r where v%3=0)" : "r",
                                " on (" + keys + ")" + tolerance, !filtered);
                    }
                }
            }
            // Identical timestamps and dictionaries also take the generic key path.
            assertEquivalent("r", "r", " on (k,kl) limit 1000", false);
        });
    }

    @Test
    public void testDeepMissAndFutureOnlyKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table r as (select cast(x as timestamp) ts, "
                    + "cast(case when x>99000 then 7 else x%3 end as int) k,x v "
                    + "from long_sequence(100000)) timestamp(ts)");
            execute("create table l as (select cast(90000+x as timestamp) ts, "
                    + "cast(case when x%2=0 then 7 else -1 end as int) k "
                    + "from long_sequence(10000)) timestamp(ts)");
            assertEquivalent("l", "r", " on (k)", false);
        });
    }

    @Test
    public void testMemoizedHistoryAfterSparseGap() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 4096);
        assertMemoryLeak(() -> {
            execute("create table r as (select cast(x as timestamp) ts,k,k*4294967296+121 kl,"
                    + "cast('s_'||k as symbol) ks,x v from (select x,"
                    + "cast(case when x>950000 then 7 when x%131072=0 then 0 else 1 end as int) k "
                    + "from long_sequence(1000000))) timestamp(ts) partition by hour");
            // The first miss builds history. The large gap returns to lookup; later probes
            // must reuse negative/positive history only after checking every newer row.
            execute("create table l as (select cast(at as timestamp) ts,k,k*4294967296+121 kl,"
                    + "cast('s_'||k as symbol) ks from (select "
                    + "case when x=1 then 90000 else 500000+(x-2)*10000 end at,"
                    + "cast(case when x%3=0 then 0 else 7 end as int) k "
                    + "from long_sequence(52))) timestamp(ts)");
            for (String keys : new String[]{"k", "kl", "ks", "k,kl", "ks,kl"}) {
                for (String tolerance : new String[]{"", " tolerance 100T"}) {
                    assertEquivalent("l", "r", " on (" + keys + ")" + tolerance, false);
                }
            }
        });
    }

    private void assertEquivalent(String left, String right, String suffix, boolean expectSwitch) throws Exception {
        String body = " a.ts,b.v from " + left + " a asof join " + right + " b" + suffix;
        try (RecordCursorFactory reference = select("select /*+ asof_linear(a b) */" + body);
             RecordCursorFactory candidate = select("select" + body)) {
            for (int reopen = 0; reopen < 2; reopen++) {
                try (RecordCursor expected = reference.getCursor(sqlExecutionContext);
                     RecordCursor actual = candidate.getCursor(sqlExecutionContext)) {
                    TestUtils.assertEquals(expected, reference.getMetadata(), actual, candidate.getMetadata(), false);
                    if (expectSwitch) {
                        RecordCursorFactory base = candidate;
                        while (!(base instanceof AsOfJoinDenseRecordCursorFactoryBase) && base != null) {
                            base = base.getBaseFactory();
                        }
                        Assert.assertNotNull(base);
                        AsOfJoinDenseRecordCursorFactoryBase adaptive = (AsOfJoinDenseRecordCursorFactoryBase) base;
                        boolean sawForward = false, sawLookupAfterForward = false;
                        actual.toTop();
                        while (actual.hasNext()) {
                            if (adaptive.isForwardScanMode()) {
                                sawForward = true;
                            } else if (sawForward) {
                                sawLookupAfterForward = true;
                            }
                        }
                        Assert.assertTrue("forward transition " + suffix, sawForward);
                        Assert.assertTrue("lookup transition " + suffix, sawLookupAfterForward);
                    }
                    expected.toTop();
                    actual.toTop();
                    TestUtils.assertEquals(expected, reference.getMetadata(), actual, candidate.getMetadata(), false);
                }
            }
        }
    }
}
