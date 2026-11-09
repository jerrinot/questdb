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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.DataSource;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

/** Isolated fixed-shape execution reference, deliberately outside SQL planning. */
final class CountSumPaths {
    static final String SQL = "select k,count(),count(v),sum(v),sum(w),sum(s) from input where k>=0 group by k";
    private static final List<Pipeline.Column> COLUMNS = List.of(
            new Pipeline.Column(0, "k", ColumnType.LONG), new Pipeline.Column(1, "v", ColumnType.LONG),
            new Pipeline.Column(2, "w", ColumnType.INT), new Pipeline.Column(3, "s", ColumnType.SHORT));

    static CompiledKernel compile(Path artifacts) throws Exception {
        long started = System.nanoTime();
        Path module = Path.of(CountSumPaths.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent().getParent();
        Path directory = Files.createTempDirectory(artifacts, "count-sum-paths-").toAbsolutePath();
        Path library = directory.resolve("query.so"), log = directory.resolve("clang.log");
        Path jdk = Path.of(System.getProperty("java.home"));
        var command = List.of("clang++-21", "-O3", "-march=native", "-fwrapv", "-std=c++20", "-pthread", "-shared", "-fPIC",
                "-I" + jdk.resolve("include"), "-I" + jdk.resolve("include/linux"),
                "-I" + module.getParent().getParent().resolve("core/src/main/c/share"),
                module.resolve("src/main/c/count_sum_paths.cpp").toString(), "-o", library.toString());
        Files.writeString(directory.resolve("command.txt"), String.join("\n", command));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly().waitFor();
                throw new IOException("Reference compilation timed out: " + log);
            }
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw e;
        }
        if (process.exitValue() != 0) throw new IOException(Files.readString(log));
        Arena arena = Arena.ofShared();
        try {
            var symbol = SymbolLookup.libraryLookup(library, arena).find("query").orElseThrow();
            return new CompiledKernel(NativeCalls.bind(symbol, arena), arena, directory, System.nanoTime() - started);
        } catch (Throwable e) {
            arena.close();
            throw e;
        }
    }

    static RecordCursorFactory openScan(CairoEngine engine, SqlExecutionContextImpl context) throws Exception {
        // Keep every frame address valid until native workers have joined. This small
        // reference only supports a single native partition, with no column tops.
        try (var reader = engine.getReader("input")) {
            if (reader.getPartitionedBy() != PartitionBy.NONE) throw new UnsupportedPlan("Reference requires an unpartitioned table");
        }
        return CompiledGroupBy.openScan(engine, context, "input", COLUMNS);
    }

    record Run(List<CompiledGroupBy.ResultRow> rows, double millis, long discovery, long build, long merge,
               boolean dense, long accepted, long range, long stateBytes, long inputRows) { }

    static Run execute(CompiledKernel kernel, RecordCursorFactory scan, SqlExecutionContextImpl context,
                       int workers, boolean auto) throws Exception {
        long started = System.nanoTime();
        var result = new ArrayList<CompiledGroupBy.ResultRow>();
        long[] stats = new long[8];
        try (Arena arena = Arena.ofConfined();
             var cursor = scan.getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC)) {
            var descriptors = new ArrayList<long[]>();
            long total = 0;
            PageFrame frame;
            while ((frame = cursor.next()) != null) {
                long rows = frame.getPartitionHi() - frame.getPartitionLo();
                if (rows == 0) continue;
                if (rows < 0 || frame.getFormat() != PartitionFormat.NATIVE) throw new UnsupportedPlan("Reference requires native page frames");
                long[] descriptor = new long[6];
                for (var column : COLUMNS) {
                    int index = column.index();
                    long address = frame.getPageAddress(index);
                    if (frame.getColumnSource(index) != DataSource.DIRECT || address == 0
                            || frame.getPageSize(index) < Math.multiplyExact(rows, column.width().bytes)) {
                        throw new UnsupportedPlan("Reference requires direct columns without tops");
                    }
                    descriptor[index] = address;
                }
                descriptor[4] = rows;
                descriptor[5] = total;
                total = Math.addExact(total, rows);
                descriptors.add(descriptor);
            }
            var input = arena.allocate(Math.max(1, descriptors.size()) * 48L, 8);
            for (int f = 0; f < descriptors.size(); f++) {
                for (int c = 0; c < 6; c++) input.set(JAVA_LONG, f * 48L + c * 8L, descriptors.get(f)[c]);
            }
            var state = arena.allocate(12 * 8, 8);
            state.set(JAVA_LONG, 0, auto ? 1 : 0);
            state.set(JAVA_LONG, 8, workers);
            try {
                long status = kernel.run(input.address(), descriptors.size(), state.address());
                if (status != 0) throw new IllegalStateException("Native reference failed: " + status);
                long count = state.get(JAVA_LONG, 24);
                var output = MemorySegment.ofAddress(state.get(JAVA_LONG, 16)).reinterpret(Math.multiplyExact(count, 48));
                for (long r = 0; r < count; r++) {
                    var values = new ArrayList<Long>(5);
                    for (int a = 0; a < 5; a++) {
                        long v = output.get(JAVA_LONG, r * 48 + (a + 1) * 8L);
                        values.add(v == Long.MIN_VALUE ? null : v);
                    }
                    result.add(new CompiledGroupBy.ResultRow(List.of(output.get(JAVA_LONG, r * 48)), values));
                }
                for (int i = 0; i < stats.length; i++) stats[i] = state.get(JAVA_LONG, (i + 4) * 8L);
                if (stats[7] != total) throw new AssertionError("Native input count mismatch");
            } finally {
                state.set(JAVA_LONG, 0, -1);
                kernel.run(0, 0, state.address());
            }
        }
        return new Run(result, (System.nanoTime() - started) / 1e6, stats[0], stats[1], stats[2],
                stats[3] != 0, stats[4], stats[5], stats[6], stats[7]);
    }

    static void measure(CairoEngine engine, SqlExecutionContextImpl context, Path artifacts,
                        String name, String sql, int workers, int warmups, int repeats, String mode) throws Exception {
        if (!sql.equals(SQL)) throw new UnsupportedPlan("Reference supports only " + SQL);
        try (var expectedFactory = Demo.compileSql(engine, context, sql, true);
             var scan = openScan(engine, context);
             var kernel = compile(artifacts);
             var profile = new ResolvedBatchMatrix.ProfileControl()) {
            var expected = Demo.readSql(expectedFactory, context, 1);
            System.out.printf(Locale.ROOT, "SQL CASE %s slots=%d groups=%d hash=%d prepare-native=%.3f sql=%s%n",
                    name, workers, expected.size(), expected.hashCode(), kernel.compilationNanos() / 1e6, sql);
            System.out.println("Isolated Clang O3 native execution; Rosti LONG-key adapter; private maps; parallel merge; range discovery included for native-auto.");
            var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            double[] times = new double[repeats];
            for (int round = -warmups; round < repeats; round++) {
                var before = ScanMatrix.Resources.read();
                long cpuStart = os.getProcessCpuTime();
                Run run;
                if (round >= 0) profile.command("enable");
                try { run = execute(kernel, scan, context, workers, mode.equals("native-auto")); }
                finally { if (round >= 0) profile.command("disable"); }
                long cpu = os.getProcessCpuTime() - cpuStart;
                var after = ScanMatrix.Resources.read();
                run.rows.sort(CompiledGroupBy.ResultRow::compareTo);
                if (!run.rows.equals(expected)) throw new AssertionError(mode + " SQL mismatch");
                if (round >= 0) times[round] = run.millis;
                System.out.printf(Locale.ROOT, "%s %d %s ms=%.3f discovery-ms=%.3f build-ms=%.3f merge-ms=%.3f process-cpu-ms=%.3f major-faults=%d read-bytes=%d native-rows=%d accepted=%d dense=%s range=%d local-state-MiB=%.2f%n",
                        round < 0 ? "warm" : "sample", round, mode, run.millis, run.discovery / 1e6, run.build / 1e6,
                        run.merge / 1e6, cpu / 1e6, after.faults() - before.faults(), after.bytes() - before.bytes(),
                        run.inputRows, run.accepted, run.dense, run.range, run.stateBytes / 1048576.0);
            }
            Arrays.sort(times);
            System.out.printf(Locale.ROOT, "SQL RESULT %s %s slots=%d median=%.3f range=[%.3f,%.3f]%n",
                    name, mode, workers, times[repeats / 2], times[0], times[repeats - 1]);
        }
    }
}
