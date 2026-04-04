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

import io.questdb.PropertyKey;
import io.questdb.cairo.JitBackend;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Dumps native asmjit-generated assembly for JIT-compiled filters.
 * The native backend logs assembly to stdout when
 * {@code cairo.sql.jit.debug.enabled=true}.
 *
 * Run individual filters:
 *   mvn -pl core -Dtest=NativeJitAsmDumpDriver#dumpLongGt42 test
 *   mvn -pl core -Dtest=NativeJitAsmDumpDriver#dumpMixed test
 *   mvn -pl core -Dtest=NativeJitAsmDumpDriver#dumpIn5 test
 */
public class NativeJitAsmDumpDriver extends AbstractCairoTest {
    private static final String CREATE_BENCH_SQL = "CREATE TABLE jit_bench AS (" +
            "SELECT" +
            " x AS l," +
            " x * 1.5 AS d," +
            " CAST(x AS INT) AS i," +
            " timestamp_sequence(0, 1_000_000) ts" +
            " FROM long_sequence(4096)" +
            ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL";

    @Test
    public void dumpIn5() throws Exception {
        dumpNativeAsm("l IN (1, 2, 3, 4, 5)");
    }

    @Test
    public void dumpIntRangeSelect() throws Exception {
        dumpNativeSelect("i > 0 AND i < 100");
    }

    @Test
    public void dumpIntEqOrAllIntSelect() throws Exception {
        dumpNativeSelect("i = 111111111 OR i = 222222222 OR i = 33_333_3333");
    }

    @Test
    public void dumpIntEqOrMixedIntLongSelect() throws Exception {
        dumpNativeSelect("i = 111111111 OR i = 222222222 OR i = 333_333_3333");
    }

    @Test
    public void dumpLongGt42() throws Exception {
        dumpNativeAsm("l > 42");
    }

    @Test
    public void dumpMixed() throws Exception {
        dumpNativeAsm("l > 42 AND d < 100.0");
    }

    private void dumpNativeAsm(String whereClause) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_DEBUG_ENABLED, true);
        node1.setProperty(PropertyKey.LOG_SQL_QUERY_PROGRESS_EXE, false);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE jit_dump AS (" +
                    "SELECT" +
                    " x AS l," +
                    " x * 1.5 AS d," +
                    " CAST(x AS INT) AS i," +
                    " timestamp_sequence(0, 1_000_000) ts" +
                    " FROM long_sequence(1000)" +
                    ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setJitBackend(JitBackend.CPP);

            String sql = "SELECT count(*) FROM jit_dump WHERE " + whereClause;
            System.out.println("=== " + whereClause + " ===");
            try (RecordCursorFactory factory = select(sql);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                if (cursor.hasNext()) {
                    System.err.println("Result: " + cursor.getRecord().getLong(0));
                }
            }
        });
    }

    private void dumpNativeSelect(String whereClause) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_JIT_DEBUG_ENABLED, true);
        node1.setProperty(PropertyKey.LOG_SQL_QUERY_PROGRESS_EXE, false);
        assertMemoryLeak(() -> {
            execute(CREATE_BENCH_SQL);

            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            sqlExecutionContext.setJitBackend(JitBackend.CPP);

            String sql = "SELECT * FROM jit_bench WHERE " + whereClause;
            System.out.println("=== " + sql + " ===");
            try (RecordCursorFactory factory = select(sql);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                long rows = 0;
                while (cursor.hasNext()) {
                    rows++;
                }
                System.err.println("Rows matched: " + rows);
            }
        });
    }
}
