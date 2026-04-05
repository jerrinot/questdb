/*******************************************************************************
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

package io.questdb.test.jit;

import io.questdb.cairo.JitBackend;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.jit.VectorCompiledFilter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Drives the Java vector-bytecode JIT through the real SQL execution path so
 * the generated filter can be C2-compiled and inspected with JVM diagnostics.
 *
 * Example:
 *   mvn -pl core -Dtest=VectorBytecodeC2Driver#dumpIntRange test
 */
public class VectorBytecodeC2Driver extends AbstractCairoTest {

    private static final String CREATE_TABLE_SQL = "CREATE TABLE jit_bench AS (" +
            "SELECT" +
            " x AS l," +
            " x * 1.5 AS d," +
            " CAST(x AS INT) AS i," +
            " timestamp_sequence(0, 1_000_000) ts" +
            " FROM long_sequence(4096)" +
            ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL";
    private static final int WARMUP_ITERATIONS = 20_000;

    @Test
    public void dumpIn5() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l IN (1, 2, 3, 4, 5)", true);
    }

    @Test
    public void dumpIntIn5() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE i IN (1, 2, 3, 4, 5)", true);
    }

    @Test
    public void dumpIntRange() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE i > 0 AND i < 100", true);
    }

    @Test
    public void dumpIntEqOrAllInt() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE i = 111111111 OR i = 222222222 OR i = 33_333_3333", true);
    }

    @Test
    public void dumpIntEqOrMixedIntLong() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE i = 111111111 OR i = 222222222 OR i = 333_333_3333", false);
    }

    @Test
    public void dumpLongEq42() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l = 42", true);
    }

    @Test
    public void dumpLongGt42() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l > 42", true);
    }

    @Test
    public void dumpLongGt42OrDoubleLt100() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l > 42 OR d < 100.0", true);
    }

    @Test
    public void dumpLongIn10() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)", false);
    }

    @Test
    public void dumpLongPlus10Gt42() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l + 10 > 42", true);
    }

    @Test
    public void dumpLongRange() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l > 42 AND l < 100", true);
    }

    @Test
    public void dumpLongTriple() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l > 42 AND l < 100 AND l != 77", true);
    }

    @Test
    public void dumpMixed() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l > 42 AND d < 100.0", true);
    }

    @Test
    public void dumpMixedLongInt() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE l > 42 AND i < 100", true);
    }

    @Test
    public void dumpNotLongGt42() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE NOT (l > 42)", true);
    }

    @Test
    public void dumpPureDoubleEq() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE d = 42.5", true);
    }

    @Test
    public void dumpPureDoubleRange() throws Exception {
        driveJavaVectorQuery("SELECT * FROM jit_bench WHERE d > 42.0 AND d < 100.0", true);
    }

    private static final class SilentSqlExecutionContext extends SqlExecutionContextImpl {
        private SilentSqlExecutionContext() {
            super(engine, 1);
        }

        @Override
        public boolean shouldLogSql() {
            return false;
        }
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
            if (current instanceof AsyncJitFilteredRecordCursorFactory ajf) {
                Assert.assertEquals(VectorCompiledFilter.class.getName(), ajf.getCompiledFilter().getClass().getName());
                return;
            }
            current = current.getBaseFactory();
        }
        Assert.fail("compiled filter factory not found");
    }

    private static long drainCursor(RecordCursor cursor) {
        long rows = 0;
        while (cursor.hasNext()) {
            rows++;
        }
        return rows;
    }

    private void driveJavaVectorQuery(String sql, boolean expectCompiledFilter) throws Exception {
        assertMemoryLeak(() -> {
            SilentSqlExecutionContext executionContext = new SilentSqlExecutionContext();
            executionContext.with(sqlExecutionContext.getSecurityContext(), sqlExecutionContext.getBindVariableService());
            executionContext.setParallelFilterEnabled(sqlExecutionContext.isParallelFilterEnabled());
            executionContext.setParallelGroupByEnabled(sqlExecutionContext.isParallelGroupByEnabled());
            executionContext.setParallelTopKEnabled(sqlExecutionContext.isParallelTopKEnabled());
            executionContext.setParallelHorizonJoinEnabled(sqlExecutionContext.isParallelHorizonJoinEnabled());
            executionContext.setParallelWindowJoinEnabled(sqlExecutionContext.isParallelWindowJoinEnabled());
            executionContext.setParallelReadParquetEnabled(sqlExecutionContext.isParallelReadParquetEnabled());
            executionContext.setParquetRowGroupPruningEnabled(sqlExecutionContext.isParquetRowGroupPruningEnabled());

            execute(CREATE_TABLE_SQL, executionContext);

            executionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            executionContext.setJitBackend(JitBackend.JAVA_VECTOR_COMPILED);

            System.out.println("=== " + sql + " ===");
            try (RecordCursorFactory factory = select(sql, executionContext)) {
                if (expectCompiledFilter) {
                    Assert.assertTrue("query should use a compiled filter", factory.usesCompiledFilter());
                    assertVectorCompiledFilter(factory);
                    assertVectorBytecodeUsed(factory);
                } else {
                    System.err.println("usesCompiledFilter=" + factory.usesCompiledFilter());
                }

                long rows = 0;
                for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                    try (RecordCursor cursor = factory.getCursor(executionContext)) {
                        rows = drainCursor(cursor);
                    }
                }
                System.err.println("Rows matched: " + rows);
            }
        });
    }
}
