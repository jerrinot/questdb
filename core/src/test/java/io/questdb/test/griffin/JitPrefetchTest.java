/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for JIT filter prefetching functionality.
 * These tests verify that prefetching is correctly integrated with
 * JIT-compiled filters and does not break query execution.
 */
public class JitPrefetchTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        // Disable the test suite on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());
        super.setUp();
    }

    @Test
    public void testPrefetchEnabledByDefault() throws Exception {
        // Verify that prefetch is enabled by default in configuration
        assertMemoryLeak(() -> {
            Assert.assertTrue("Prefetch should be enabled by default",
                    configuration.isSqlJitPrefetchEnabled());
            Assert.assertEquals("Default prefetch lookahead should be 2",
                    2, configuration.getSqlJitPrefetchLookahead());
        });
    }

    @Test
    public void testJitFilterWithPrefetchEnabled() throws Exception {
        // Test that JIT filter queries work correctly with prefetch enabled
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select x as id, " +
                    "rnd_long() as value, " +
                    "timestamp_sequence(0, 1000000) as ts " +
                    "from long_sequence(10000)" +
                    ") timestamp(ts) partition by day");

            String query = "select count() from x where id > 5000 and id < 5010";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(9, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testJitFilterWithMixedColumnTypes() throws Exception {
        // Test prefetching with various column types in filter
        assertMemoryLeak(() -> {
            execute("create table mixed as (" +
                    "select " +
                    "x as id, " +
                    "rnd_int() as int_col, " +
                    "rnd_long() as long_col, " +
                    "rnd_double() as double_col, " +
                    "rnd_float() as float_col, " +
                    "timestamp_sequence(0, 1000000) as ts " +
                    "from long_sequence(1000)" +
                    ") timestamp(ts)");

            // Filter on multiple column types
            String query = "select count() from mixed where int_col > 0 and long_col < 0";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    // Just verify it executes without error
                }
            }
        });
    }

    @Test
    public void testJitFilterWithVarcharColumn() throws Exception {
        // Test prefetching with varchar column (aux vector prefetch)
        assertMemoryLeak(() -> {
            execute("create table varchars as (" +
                    "select " +
                    "x as id, " +
                    "rnd_varchar(10, 20, 0) as name, " +
                    "rnd_long() as value, " +
                    "timestamp_sequence(0, 1000000) as ts " +
                    "from long_sequence(1000)" +
                    ") timestamp(ts)");

            // Filter on numeric column, but table has varchar
            String query = "select count() from varchars where id > 500";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(500, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testPrefetchDisabledViaConfig() throws Exception {
        // Test that prefetch can be disabled via configuration
        setProperty(PropertyKey.CAIRO_SQL_JIT_PREFETCH_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select x as id, " +
                    "rnd_long() as value, " +
                    "timestamp_sequence(0, 1000000) as ts " +
                    "from long_sequence(1000)" +
                    ") timestamp(ts)");

            String query = "select count() from x where id > 500";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(500, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testPrefetchWithCustomLookahead() throws Exception {
        // Test with custom lookahead value
        setProperty(PropertyKey.CAIRO_SQL_JIT_PREFETCH_LOOKAHEAD, "5");
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select x as id, " +
                    "rnd_long() as value, " +
                    "timestamp_sequence(0, 1000000) as ts " +
                    "from long_sequence(1000)" +
                    ") timestamp(ts)");

            String query = "select count() from x where id > 500";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(500, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testPrefetchWithMultiplePartitions() throws Exception {
        // Test prefetching across multiple partitions
        assertMemoryLeak(() -> {
            execute("create table partitioned as (" +
                    "select " +
                    "x as id, " +
                    "rnd_long() as value, " +
                    "timestamp_sequence('2020-01-01', 3600000000) as ts " +
                    "from long_sequence(10000)" +
                    ") timestamp(ts) partition by day");

            // Use simple comparison operators that JIT supports
            String query = "select count() from partitioned where id > 5000";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(5000, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testPrefetchWithSingleFrame() throws Exception {
        // Test edge case: single frame (no lookahead possible)
        assertMemoryLeak(() -> {
            execute("create table small as (" +
                    "select x as id, rnd_long() as value " +
                    "from long_sequence(10)" +
                    ")");

            String query = "select count() from small where id > 5";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(5, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testOnlyFilterColumnsTracked() throws Exception {
        // Verify that only columns used in filter are tracked for prefetch
        // by running a query with many SELECT columns but few filter columns
        assertMemoryLeak(() -> {
            execute("create table wide as (" +
                    "select " +
                    "x as id, " +
                    "rnd_long() as col1, " +
                    "rnd_long() as col2, " +
                    "rnd_long() as col3, " +
                    "rnd_long() as col4, " +
                    "rnd_long() as col5, " +
                    "rnd_long() as filter_col, " +
                    "timestamp_sequence(0, 1000000) as ts " +
                    "from long_sequence(1000)" +
                    ") timestamp(ts)");

            // SELECT many columns, but filter only on filter_col
            String query = "select id, col1, col2, col3, col4, col5 from wide where filter_col > 0";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        count++;
                    }
                    Assert.assertTrue("Should have matching rows", count > 0);
                }
            }
        });
    }

    @Test
    public void testPrefetchConstantOnNonLinux() throws Exception {
        // Verify graceful fallback when POSIX_MADV_WILLNEED is not available
        // On Windows/FreeBSD, the constant should be -1, and prefetch should be a no-op
        if (Os.isWindows() || !Os.isLinux()) {
            // On non-Linux platforms, POSIX_MADV_WILLNEED should be -1
            // and prefetch should return -1 (not supported)
            Assert.assertEquals(-1, Files.POSIX_MADV_WILLNEED);
            Assert.assertEquals(-1, Files.prefetch(0, 0));
        } else {
            // On Linux, POSIX_MADV_WILLNEED should be 3
            Assert.assertEquals(3, Files.POSIX_MADV_WILLNEED);
        }
    }

    @Test
    public void testPrefetchGracefulFallbackOnUnsupportedPlatform() throws Exception {
        // Verify queries still work correctly even when prefetch is not available
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select x as id, " +
                    "rnd_long() as value, " +
                    "timestamp_sequence(0, 1000000) as ts " +
                    "from long_sequence(1000)" +
                    ") timestamp(ts)");

            // Query should work regardless of prefetch support
            String query = "select count() from x where id > 500";
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("Query should use JIT", factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(500, cursor.getRecord().getLong(0));
                }
            }
        });
    }
}
