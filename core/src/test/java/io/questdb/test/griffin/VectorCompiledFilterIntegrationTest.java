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

package io.questdb.test.griffin;

import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.jit.VectorCompiledFilter;

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class VectorCompiledFilterIntegrationTest extends AbstractCairoTest {
    @Test
    public void testEnabledModeUsesJavaBackend() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select x i64, cast(x as int) i32, timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(8)" +
                            ") timestamp(ts)"
            );

            final String query = "select i64, i32 from x where i32 + 1 > 3 and i32 in (2, 5, 7)";
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(factory.usesCompiledFilter());
                assertVectorCompiledFilter(factory);
                final long vectorApiExecutionCount = getVectorApiExecutionCountIfSelected(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
                assertVectorApiExecutedIfSelected(query, factory, vectorApiExecutionCount);
            }
        });
    }

    @Test
    public void testForceVectorModeUsesJavaBackend() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select x i64, cast(x as int) i32, timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(8)" +
                            ") timestamp(ts)"
            );

            final String query = "select i64, i32 from x where i32 + 1 > 3 and i32 in (2, 5, 7)";
            final String countQuery = "select count() from x where i32 + 1 > 3 and i32 in (2, 5, 7)";
            final StringSink actualSink = new StringSink();

            sink.clear();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
            }

            actualSink.clear();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_VECTOR);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(factory.usesCompiledFilter());
                assertVectorCompiledFilter(factory);
                final long vectorApiExecutionCount = getVectorApiExecutionCountIfSelected(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), actualSink);
                }
                assertVectorApiExecutedIfSelected(query, factory, vectorApiExecutionCount);
            }
            TestUtils.assertEquals("vector backend result mismatch", sink, actualSink);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            long expectedCount;
            try (RecordCursorFactory factory = select(countQuery)) {
                Assert.assertFalse(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    expectedCount = cursor.getRecord().getLong(0);
                }
            }

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_VECTOR);
            try (RecordCursorFactory factory = select(countQuery)) {
                Assert.assertTrue(factory.usesCompiledFilter());
                final long vectorApiExecutionCount = getVectorApiExecutionCountIfSelected(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(expectedCount, cursor.getRecord().getLong(0));
                }
                assertVectorApiExecutedIfSelected(countQuery, factory, vectorApiExecutionCount);
            }
        });
    }

    private static void assertVectorCompiledFilter(RecordCursorFactory factory) {
        RecordCursorFactory current = factory;
        while (current != null) {
            if (current instanceof AsyncJitFilteredRecordCursorFactory) {
                Assert.assertEquals(
                        VectorCompiledFilter.class.getName(),
                        ((AsyncJitFilteredRecordCursorFactory) current).getCompiledFilter().getClass().getName()
                );
                return;
            }
            current = current.getBaseFactory();
        }
        Assert.fail("compiled filter factory not found");
    }
}
