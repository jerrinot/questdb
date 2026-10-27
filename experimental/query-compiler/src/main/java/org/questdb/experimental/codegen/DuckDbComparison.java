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
import io.questdb.griffin.SqlExecutionContextImpl;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Optional released DuckDB JDBC reference; no dependency in the prototype's default build. */
final class DuckDbComparison {
    static void measure(CairoEngine engine, SqlExecutionContextImpl context, Path artifacts,
                        String name, String sql, int workers, int warmups, int repeats) throws Exception {
        if (!sql.equals(CountSumPaths.SQL)) throw new UnsupportedPlan("DuckDB comparison currently covers the fixed COUNT/SUM fixture");
        Class.forName("org.duckdb.DuckDBDriver");
        long inputRows;
        try (var reader = engine.getReader("input")) { inputRows = reader.size(); }
        String ddl = ResolvedBatchMatrix.matchedSpec(name, inputRows, true).ddl()
                .replace("as short", "as smallint")
                .replace("long_sequence(" + inputRows + ")", "range(1," + Math.addExact(inputRows, 1) + ") as seq(x)");
        try (var expectedFactory = Demo.compileSql(engine, context, sql, true);
             var db = DriverManager.getConnection("jdbc:duckdb:" + artifacts.resolve("comparison.duckdb"));
             var setup = db.createStatement();
             var profile = new ResolvedBatchMatrix.ProfileControl()) {
            var expected = Demo.readSql(expectedFactory, context, 1);
            setup.execute("set threads=" + workers);
            int perfectBits = Integer.getInteger("query.codegen.duckdbPerfectBits", -1);
            if (perfectBits >= 0) setup.execute("set perfect_ht_threshold=" + perfectBits);
            String label = perfectBits >= 0 ? "duckdb-perfect" + perfectBits : "duckdb-default";
            print(db, "select version() version");
            print(db, "select name,value from duckdb_settings() where name in ('threads','memory_limit','perfect_ht_threshold','preserve_insertion_order','default_block_size','default_row_group_size','enable_external_access') order by name");
            System.out.println("DUCKDB CREATE " + ddl);
            long start = System.nanoTime();
            setup.execute(ddl);
            double createMillis = (System.nanoTime() - start) / 1e6;
            start = System.nanoTime();
            setup.execute("checkpoint");
            System.out.printf(Locale.ROOT, "DUCKDB LOAD create-ms=%.3f checkpoint-ms=%.3f%n", createMillis, (System.nanoTime() - start) / 1e6);
            try (var count = setup.executeQuery("select count(*) from input")) {
                if (!count.next() || count.getLong(1) != inputRows) throw new AssertionError("DuckDB actual input row count");
            }
            print(db, "describe input");
            print(db, "select column_name,segment_type,compression,count(*) segments from pragma_storage_info('input') group by all order by all");
            print(db, "select database_size,memory_usage,memory_limit from pragma_database_size()");
            print(db, "explain " + sql);
            start = System.nanoTime();
            try (var query = db.prepareStatement(sql)) {
                double preparationMillis = (System.nanoTime() - start) / 1e6;
                System.out.printf(Locale.ROOT, "SQL CASE %s slots=%d groups=%d hash=%d prepare-native=%.3f implementation=%s sql=%s%n",
                        name, workers, expected.size(), expected.hashCode(), preparationMillis, label, sql);
                double[] times = new double[repeats];
                var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                for (int round = -warmups; round < repeats; round++) {
                    var before = ScanMatrix.Resources.read();
                    long cpuStart = os.getProcessCpuTime();
                    if (round >= 0) profile.command("enable");
                    Run run;
                    try { run = read(query); }
                    finally { if (round >= 0) profile.command("disable"); }
                    long cpu = os.getProcessCpuTime() - cpuStart;
                    var after = ScanMatrix.Resources.read();
                    run.rows.sort(CompiledGroupBy.ResultRow::compareTo);
                    if (!run.rows.equals(expected)) throw new AssertionError("DuckDB result differs from QuestDB SQL");
                    if (round >= 0) times[round] = run.millis;
                    System.out.printf(Locale.ROOT, "%s %d %s ms=%.3f first-row-ms=%.3f process-cpu-ms=%.3f major-faults=%d read-bytes=%d groups=%d input-rows=%d%n",
                            round < 0 ? "warm" : "sample", round, label, run.millis, run.firstRowMillis, cpu / 1e6,
                            after.faults() - before.faults(), after.bytes() - before.bytes(), run.rows.size(), inputRows);
                }
                Arrays.sort(times);
                System.out.printf(Locale.ROOT, "SQL RESULT %s %s slots=%d median=%.3f range=[%.3f,%.3f]%n",
                        name, label, workers, times[repeats / 2], times[0], times[repeats - 1]);
            }
        }
    }

    private record Run(List<CompiledGroupBy.ResultRow> rows, double millis, double firstRowMillis) { }

    private static Run read(PreparedStatement query) throws Exception {
        long start = System.nanoTime();
        var rows = new ArrayList<CompiledGroupBy.ResultRow>();
        double firstRowMillis;
        try (var result = query.executeQuery()) {
            boolean hasRow = result.next();
            firstRowMillis = (System.nanoTime() - start) / 1e6;
            while (hasRow) {
                long key = result.getLong(1);
                if (result.wasNull()) throw new AssertionError("Fixture filter excludes NULL keys");
                var values = new ArrayList<Long>(5);
                for (int column = 2; column <= 6; column++) {
                    long v = result.getLong(column);
                    values.add(result.wasNull() ? null : v);
                }
                rows.add(new CompiledGroupBy.ResultRow(List.of(key), values));
                hasRow = result.next();
            }
        }
        return new Run(rows, (System.nanoTime() - start) / 1e6, firstRowMillis);
    }

    private static void print(Connection db, String sql) throws Exception {
        System.out.println("DUCKDB " + sql);
        try (var statement = db.createStatement(); var result = statement.executeQuery(sql)) {
            int columns = result.getMetaData().getColumnCount();
            while (result.next()) {
                for (int c = 1; c <= columns; c++) {
                    if (c > 1) System.out.print(" | ");
                    System.out.print(result.getString(c));
                }
                System.out.println();
            }
        }
    }
}
