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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.log.LogFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static org.questdb.experimental.codegen.Pipeline.*;

/** Correctness-checked, interleaved smoke measurements; not a hardware bottleneck study. */
public final class Demo {
    private Demo() {
    }

    public static void main(String[] args) throws Exception {
        long rows = args.length > 0 ? Long.parseLong(args[0]) : 1_000_000;
        int repetitions = args.length > 1 ? Integer.parseInt(args[1]) : 9;
        String mode = args.length > 2 ? args[2] : "mixed";
        int groups = args.length > 3 ? Integer.parseInt(args[3]) : 1024;
        int valuesPerGroup = args.length > 4 ? Integer.parseInt(args[4]) : 128;
        if (rows < 1 || repetitions < 3 || groups < 1 || valuesPerGroup < 1 || args.length > 5) {
            throw new IllegalArgumentException("Usage: Demo [rows >= 1] [repetitions >= 3] [mixed|distinct|distinct-mixed] [groups >= 1] [values per group >= 1]");
        }
        Workload workload = switch (mode) {
            case "mixed" -> mixed(rows, groups);
            case "distinct" -> distinct(rows, groups, valuesPerGroup, false);
            case "distinct-mixed" -> distinct(rows, groups, valuesPerGroup, true);
            default -> throw new IllegalArgumentException("Unknown workload: " + mode);
        };
        Path workDirectory = Path.of("experimental/query-compiler/target/generated").toAbsolutePath();
        deleteDirectory(workDirectory);
        Files.createDirectories(workDirectory);
        Path database = Files.createTempDirectory("questdb-codegen-demo-");
        try {
            run(rows, repetitions, workload, database, workDirectory);
        } finally {
            LogFactory.haltInstance();
            deleteDirectory(database);
        }
    }

    private static void run(long rows, int repetitions, Workload workload, Path database, Path artifacts) throws Exception {
        var configuration = new DefaultCairoConfiguration(database.toString());
        try (var engine = new CairoEngine(configuration);
             var context = new SqlExecutionContextImpl(engine, 1).with(
                     configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null)) {
            engine.execute(workload.ddl(), context);
            Pipeline pipeline = workload.pipeline();
            Pipeline nested = pipeline.flatDistinct() ? new Pipeline(pipeline.columns(), pipeline.filter(), pipeline.keys(),
                    List.of(new NestedCountDistinct(((CountDistinct) pipeline.aggregates().getFirst()).argument(),
                            configuration.getCountDistinctCapacity(), configuration.getCountDistinctLoadFactor()))) : null;
            int groupCapacity = workload.groupCapacity();
            int pairCapacity = workload.pairCapacity();
            try (var baseline = compileSql(engine, context, workload.sql(), false);
                 var normal = compileSql(engine, context, workload.sql(), true);
                 var handwritten = CompiledGroupBy.withKernels(engine, context, "input", pipeline, workload.scan(), workload.finish());
                 var nativeQuery = CompiledGroupBy.compile(engine, context, "input", pipeline, new NativeBackend(), artifacts);
                 var llvm = CompiledGroupBy.compile(engine, context, "input", pipeline, new LlvmBackend(), artifacts);
                 var nestedHand = nested == null ? null : CompiledGroupBy.withKernels(engine, context, "input", nested,
                         HandwrittenKernels.nestedDistinct(configuration.getCountDistinctCapacity(), configuration.getCountDistinctLoadFactor()), null);
                 var nestedNative = nested == null ? null : CompiledGroupBy.compile(engine, context, "input", nested, new NativeBackend(), artifacts);
                 var nestedLlvm = nested == null ? null : CompiledGroupBy.compile(engine, context, "input", nested, new LlvmBackend(), artifacts)) {
                List<CompiledGroupBy.ResultRow> expected = readSql(baseline, context, 1);
                List<Case> cases = new ArrayList<>(List.of(
                        new Case("questdb-scalar", t -> readSql(baseline, context, 1, t)),
                        new Case("questdb-normal", t -> readSql(normal, context, 1, t)),
                        new Case("handwritten-java", t -> handwritten.execute(context, groupCapacity, pairCapacity, t)),
                        new Case("generated-native", t -> nativeQuery.execute(context, groupCapacity, pairCapacity, t)),
                        new Case("llvm-o2", t -> llvm.execute(context, groupCapacity, pairCapacity, t))));
                if (nested != null) {
                    cases.add(new Case("nested-handwritten", t -> nestedHand.execute(context, groupCapacity, groupCapacity, t)));
                    cases.add(new Case("nested-native", t -> nestedNative.execute(context, groupCapacity, groupCapacity, t)));
                    cases.add(new Case("nested-llvm-o2", t -> nestedLlvm.execute(context, groupCapacity, groupCapacity, t)));
                }
                double[][] elapsed = new double[cases.size()][repetitions];
                double[][] unsortedElapsed = new double[cases.size()][repetitions];
                double[][][] phases = new double[cases.size()][4][repetitions];
                int[] orderDescents = new int[cases.size()];
                ExecutionTimings timings = new ExecutionTimings();
                int warmups = 20;
                for (int round = -warmups; round < repetitions; round++) {
                    // Rotate order to avoid consistently favoring one implementation.
                    for (int offset = 0; offset < cases.size(); offset++) {
                        int index = Math.floorMod(round + offset, cases.size());
                        Case item = cases.get(index);
                        timings.inspectOrder = round == -warmups;
                        long start = System.nanoTime();
                        var actual = item.execute().run(timings);
                        long nanos = System.nanoTime() - start;
                        if (!expected.equals(actual)) {
                            throw new AssertionError("Result mismatch for " + item.name());
                        }
                        if (round == -warmups) { orderDescents[index] = timings.descents; }
                        if (round >= 0) {
                            elapsed[index][round] = nanos / 1_000_000.0;
                            unsortedElapsed[index][round] = (nanos - timings.sort) / 1e6;
                            phases[index][0][round] = timings.build / 1e6;
                            phases[index][1][round] = timings.copy / 1e6;
                            phases[index][2][round] = timings.sort / 1e6;
                            phases[index][3][round] = timings.release / 1e6;
                        }
                    }
                }
                System.out.println("SQL: " + workload.sql());
                System.out.printf(Locale.ROOT, "rows=%d, groups=%d, warmups=%d, measured=%d%n",
                        rows, expected.size(), warmups, repetitions);
                if (pipeline.distinct()) {
                    int distinctIndex = 0;
                    while (!(pipeline.aggregates().get(distinctIndex) instanceof CountDistinct)
                            && !(pipeline.aggregates().get(distinctIndex) instanceof NestedCountDistinct)) { distinctIndex++; }
                    long pairs = 0;
                    for (var row : expected) { pairs += row.values().get(distinctIndex); }
                    System.out.printf(Locale.ROOT, "unique non-null pairs=%d, group-capacity=%d%s%n", pairs, groupCapacity,
                            pipeline.flatDistinct() ? ", pair-capacity=" + pairCapacity : ", nested sets grow on demand");
                }
                System.out.println("All results matched. One execution thread; no background query workers.");
                printPlan(normal, context);
                System.out.println("Execution includes allocation, scan, aggregation, result copying and sorting; compilation is separate.");
                System.out.println("Phases: build=allocation/scan/aggregation, copy=output materialization, sort=canonical result sorting, release=cleanup.");
                System.out.println("Without canonical sort = each total sample minus its sort phase; still includes copying and cleanup. No SQL ORDER BY is requested.");
                System.out.printf(Locale.ROOT, "Native compile/load: %.3f ms%n", nativeQuery.compilationNanos() / 1e6);
                System.out.printf(Locale.ROOT, "LLVM-O2 compile/load: %.3f ms%n", llvm.compilationNanos() / 1e6);
                if (nested != null) {
                    System.out.printf(Locale.ROOT, "Nested native compile/load: %.3f ms%n", nestedNative.compilationNanos() / 1e6);
                    System.out.printf(Locale.ROOT, "Nested LLVM-O2 compile/load: %.3f ms%n", nestedLlvm.compilationNanos() / 1e6);
                }
                for (int i = 0; i < cases.size(); i++) {
                    Arrays.sort(elapsed[i]);
                    System.out.printf(Locale.ROOT, "%s: min %.3f ms, median %.3f ms, max %.3f ms%n",
                            cases.get(i).name(), elapsed[i][0], median(elapsed[i]), elapsed[i][repetitions - 1]);
                    Arrays.sort(unsortedElapsed[i]);
                    System.out.printf(Locale.ROOT, "  without canonical sort: median %.3f ms; output-order descents before sorting: %d%n",
                            median(unsortedElapsed[i]), orderDescents[i]);
                    for (double[] phase : phases[i]) { Arrays.sort(phase); }
                    System.out.printf(Locale.ROOT, "  phases median: build %.3f ms, copy %.3f ms, sort %.3f ms, release %.3f ms%n",
                            median(phases[i][0]), median(phases[i][1]), median(phases[i][2]), median(phases[i][3]));
                }
                System.out.println("Current generated code: " + artifacts);
            }
        }
    }

    private static Workload mixed(long rows, int groups) {
        Column k = new Column(0, "k", ColumnType.INT);
        Column v = new Column(1, "v", ColumnType.LONG);
        Column f = new Column(2, "f", ColumnType.BYTE);
        Pipeline pipeline = new Pipeline(List.of(k, v, f), new Compare(Ir.Op.NE, f, new Constant(0)), List.of(k),
                List.of(new Count(), new CountValue(v), new Sum(new Add(v, new Constant(3)))));
        String ddl = """
                create table input as (
                    select cast(x %% %d as int) k,
                           case when x %% 19 = 0 then null else x*3 end v,
                           cast(x %% 7 as byte) f
                    from long_sequence(%d)
                )
                """.formatted(groups, rows);
        return new Workload(pipeline, ddl, "select k,count(*),count(v),sum(v+3) from input where f<>0 group by k",
                HandwrittenKernels::mixed, null, capacity(Math.min(rows, groups)), 1);
    }

    private static Workload distinct(long rows, int groups, int valuesPerGroup, boolean mixed) {
        Column region = new Column(0, "RegionID", ColumnType.INT);
        Column user = new Column(1, "UserID", ColumnType.LONG);
        Column adv = new Column(2, "AdvEngineID", ColumnType.SHORT);
        Pipeline pipeline = mixed
                ? new Pipeline(List.of(region, user, adv), new All(), List.of(region),
                        List.of(new Sum(adv), new Count(), new NestedCountDistinct(user)))
                : new Pipeline(List.of(region, user), new All(), List.of(region), List.of(new CountDistinct(user)));
        String ddl = """
                create table input as (
                    select cast((x-1) %% %d as int) RegionID,
                           case when x %% 97 = 0 then null
                                else cast((x-1)/%d as long) %% %d end UserID%s
                    from long_sequence(%d)
                )
                """.formatted(groups, groups, valuesPerGroup,
                        mixed ? ", cast(x % 23 - 11 as short) AdvEngineID" : "", rows);
        long pairBound = Math.min(rows, Math.multiplyExact((long) groups, valuesPerGroup + 1L));
        if (mixed) {
            return new Workload(pipeline, ddl, "select RegionID,sum(AdvEngineID),count(*),count_distinct(UserID) from input group by RegionID",
                    HandwrittenKernels.nestedMixed(3, 0.7), null, capacity(Math.min(rows, groups)), 1);
        }
        return new Workload(pipeline, ddl, "select RegionID,count_distinct(UserID) from input group by RegionID",
                HandwrittenKernels::distinct, HandwrittenKernels::collapse, capacity(Math.min(rows, groups)), capacity(pairBound));
    }

    private static int capacity(long entries) {
        long wanted = Math.max(2, Math.addExact(entries, entries / 2 + 1));
        if (wanted > (1L << 30)) { throw new IllegalArgumentException("Prototype table capacity is too large"); }
        return (int) (Long.highestOneBit(wanted - 1) << 1);
    }

    static RecordCursorFactory compileSql(CairoEngine engine, SqlExecutionContextImpl context, String sql, boolean parallel) throws Exception {
        context.setParallelGroupByEnabled(parallel);
        try (var compiler = engine.getSqlCompiler()) {
            return compiler.compile(sql, context).getRecordCursorFactory();
        } finally {
            context.setParallelGroupByEnabled(true);
        }
    }

    private static void printPlan(RecordCursorFactory factory, SqlExecutionContextImpl context) {
        TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        System.out.println("QuestDB normal plan:");
        for (int i = 1; i <= sink.getLineCount(); i++) { System.out.println(sink.getLine(i)); }
    }

    static List<CompiledGroupBy.ResultRow> readSql(RecordCursorFactory factory, SqlExecutionContextImpl context, int keyCount) throws Exception {
        return readSql(factory, context, keyCount, null);
    }

    static List<CompiledGroupBy.ResultRow> readSql(RecordCursorFactory factory, SqlExecutionContextImpl context,
                                                int keyCount, ExecutionTimings timings) throws Exception {
        if (timings != null && timings.profileStart != null) timings.profileStart.run();
        long started = timings == null ? 0 : System.nanoTime();
        long releaseStarted = 0;
        if (timings != null) { timings.reset(); }
        List<CompiledGroupBy.ResultRow> result = new ArrayList<>();
        try (var cursor = factory.getCursor(context)) {
            // Both scalar and async GROUP BY build their complete map on the first hasNext().
            boolean hasRow = cursor.hasNext();
            if (timings != null) { timings.build = System.nanoTime() - started; started = System.nanoTime(); }
            while (hasRow) {
                var record = cursor.getRecord();
                List<Long> keys = new ArrayList<>();
                List<Long> values = new ArrayList<>();
                for (int i = 0; i < factory.getMetadata().getColumnCount(); i++) {
                    if (factory.getMetadata().getColumnType(i) == ColumnType.SYMBOL) {
                        // Compare dictionary contents exactly, including NULL and empty symbols.
                        var symbol = record.getSymA(i);
                        var target = i < keyCount ? keys : values;
                        target.add(symbol == null ? -1L : (long) symbol.length());
                        if (symbol != null) {
                            for (int offset = 0; offset < symbol.length(); offset += 4) {
                                long word = 0;
                                for (int j = 0; j < 4 && offset + j < symbol.length(); j++) word |= (long) symbol.charAt(offset + j) << (16 * j);
                                target.add(word);
                            }
                        }
                        continue;
                    }
                    if (factory.getMetadata().getColumnType(i) == ColumnType.VARCHAR) {
                        // Exact bytes, not a hash: keep the numeric harness's canonical row container.
                        var utf8 = record.getVarcharA(i);
                        var target = i < keyCount ? keys : values;
                        target.add(utf8 == null ? -1L : (long) utf8.size());
                        if (utf8 != null) {
                            for (int offset = 0; offset < utf8.size(); offset += 8) {
                                long word = 0;
                                for (int j = 0; j < 8 && offset + j < utf8.size(); j++) {
                                    word |= (utf8.byteAt(offset + j) & 255L) << (8 * j);
                                }
                                target.add(word);
                            }
                        }
                        continue;
                    }
                    Long value = switch (factory.getMetadata().getColumnType(i)) {
                        case ColumnType.BYTE -> (long) record.getByte(i);
                        case ColumnType.SHORT -> (long) record.getShort(i);
                        case ColumnType.INT -> record.getInt(i) == Integer.MIN_VALUE ? null : (long) record.getInt(i);
                        case ColumnType.LONG -> record.getLong(i) == Long.MIN_VALUE ? null : record.getLong(i);
                        case ColumnType.IPv4 -> (long) record.getIPv4(i);
                        case ColumnType.DATE -> record.getDate(i);
                        case ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> record.getTimestamp(i);
                        case ColumnType.DOUBLE -> Double.doubleToLongBits(record.getDouble(i));
                        default -> throw new IllegalArgumentException("Expected integer SQL result columns");
                    };
                    (i < keyCount ? keys : values).add(value);
                }
                result.add(new CompiledGroupBy.ResultRow(keys, values));
                hasRow = cursor.hasNext();
            }
            if (timings != null) {
                // Observe the completed map before cursor.close() clears the execution.
                var physical = factory;
                while (physical != null && !(physical instanceof io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory)) {
                    physical = physical.getBaseFactory();
                }
                if (physical instanceof io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory async) {
                    timings.shards = async.getAtom().isSharded() ? io.questdb.griffin.engine.table.GroupByMapFragment.NUM_SHARDS : 0;
                }
                timings.copy = System.nanoTime() - started;
                releaseStarted = System.nanoTime();
            }
        } finally {
            if (timings != null && releaseStarted != 0) { timings.release = System.nanoTime() - releaseStarted; }
            if (timings != null && timings.profileEnd != null) timings.profileEnd.run();
        }
        if (timings != null) { timings.inspect(result); started = System.nanoTime(); }
        Collections.sort(result);
        if (timings != null) { timings.sort = System.nanoTime() - started; }
        return result;
    }

    private static void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            try (var files = Files.walk(directory)) {
                for (Path path : files.sorted(Comparator.reverseOrder()).toList()) { Files.delete(path); }
            }
        }
    }

    private static double median(double[] sorted) {
        int mid = sorted.length / 2;
        return sorted.length % 2 == 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
    }

    @FunctionalInterface
    private interface Execution {
        List<CompiledGroupBy.ResultRow> run(ExecutionTimings timings) throws Exception;
    }

    private record Case(String name, Execution execute) { }

    private record Workload(Pipeline pipeline, String ddl, String sql, Kernel scan, Kernel finish,
                            int groupCapacity, int pairCapacity) { }
}
