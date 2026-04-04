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
import io.questdb.cairo.JitBackend;
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
    public void testMixedLongIntDoubleQueryUsesVectorBytecodeBackend() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table jit_bench as (" +
                            "select rnd_long() l, rnd_double(0) d, rnd_int() i, timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(1024)" +
                            ") timestamp(ts)"
            );

            final String query = "select count() from jit_bench where l > 0 and i != 0 and d < 0.5 and l < 1000000";

            long expectedCount;
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    expectedCount = cursor.getRecord().getLong(0);
                }
            }

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setJitBackend(JitBackend.JAVA_VECTOR_COMPILED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(factory.usesCompiledFilter());
                assertVectorCompiledFilter(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(expectedCount, cursor.getRecord().getLong(0));
                }
                assertBytecodeCompiledIfJava(query, factory);
            }
        });
    }

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
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(factory.usesCompiledFilter());
                assertVectorCompiledFilter(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
                assertBytecodeCompiledIfJava(query, factory);
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
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
            }

            actualSink.clear();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_VECTOR);
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue(factory.usesCompiledFilter());
                assertVectorCompiledFilter(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), actualSink);
                }
                assertBytecodeCompiledIfJava(query, factory);
            }
            TestUtils.assertEquals("vector backend result mismatch", sink, actualSink);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            long expectedCount;
            try (RecordCursorFactory factory = select(countQuery)) {
                Assert.assertFalse(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    expectedCount = cursor.getRecord().getLong(0);
                }
            }

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_FORCE_VECTOR);
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            try (RecordCursorFactory factory = select(countQuery)) {
                Assert.assertTrue(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(expectedCount, cursor.getRecord().getLong(0));
                }
                assertBytecodeCompiledIfJava(countQuery, factory);
            }
        });
    }

    @Test
    public void testAutoBackendMixedSizePredicateUsesVectorPath() throws Exception {
        // Verifies that AUTO backend with mixed-size columns (I4 + F8) produces
        // straight-line IR (no short-circuit) so the vector compiler can accept it.
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE mixed AS (" +
                            "SELECT rnd_long() l, rnd_double(0) d, rnd_int() i, timestamp_sequence(0, 1_000_000) ts " +
                            "FROM long_sequence(1024)" +
                            ") TIMESTAMP(ts)"
            );

            final String query = "SELECT count() FROM mixed WHERE i > 0 AND d < 0.5";

            // Baseline: JIT disabled
            long expectedCount;
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    expectedCount = cursor.getRecord().getLong(0);
                }
            }

            // AUTO backend with mixed I4 + F8 (no I8): vector path should be taken
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("filter should be compiled", factory.usesCompiledFilter());
                assertVectorCompiledFilter(factory);
                assertVectorBytecodeUsed(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(expectedCount, cursor.getRecord().getLong(0));
                }
            }
        });
    }

    @Test
    public void testOversizedIntLiteralOrChainUsesVectorPath() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select case when x = 1 then 111111111 when x = 2 then 222222222 else cast(x as int) end i, " +
                            "timestamp_sequence(0, 1000000) ts " +
                            "from long_sequence(1024)" +
                            ") timestamp(ts)"
            );

            final String query = "select i from x where i = 111111111 or i = 222222222 or i = 3333333333";
            final StringSink actualSink = new StringSink();

            sink.clear();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            sqlExecutionContext.setJitBackend(JitBackend.AUTO);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertFalse(factory.usesCompiledFilter());
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                }
            }

            actualSink.clear();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setJitBackend(JitBackend.JAVA_VECTOR_COMPILED);
            try (RecordCursorFactory factory = select(query)) {
                Assert.assertTrue("filter should be compiled", factory.usesCompiledFilter());
                assertVectorCompiledFilter(factory);
                assertVectorBytecodeUsed(factory);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    CursorPrinter.println(cursor, factory.getMetadata(), actualSink);
                }
            }
            TestUtils.assertEquals("vector backend result mismatch", sink, actualSink);
        });
    }

    private static void assertVectorBytecodeUsed(RecordCursorFactory factory) {
        RecordCursorFactory current = factory;
        while (current != null) {
            if (current instanceof AsyncJitFilteredRecordCursorFactory ajf) {
                if (ajf.getCompiledFilter() instanceof VectorCompiledFilter vcf) {
                    Assert.assertTrue("expected vector bytecode, got scalar", vcf.usesVectorBytecode());
                    return;
                }
            }
            current = current.getBaseFactory();
        }
        Assert.fail("VectorCompiledFilter not found");
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
