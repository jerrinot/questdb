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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Fixed-work ASOF controls. Consumes matched payloads; never benchmarks count's size shortcut. */
public final class AsOfSelectionMatrix {
    public static void main(String[] args) throws Exception {
        if (args.length < 9 || args.length > 17) {
            throw new IllegalArgumentException("root rhsRows domain probes local|dense|phase|burst|late_burst|rare|packed|packed_rare|packed_local|filtered|filtered_local int|long|symbol|composite modes repeats warmups [indexed|unindexed|keep] [queryMemoryBytes] [mod3|arith3|arith16|arith256|arith256_spread|arith_all|arith_mixed|sin] [executionSlots] [jit|java] [outputLimit] [stock|llvm] [rows|aggregates]");
        }
        Path root = Path.of(args[0]);
        long rows = Long.parseLong(args[1]);
        int domain = Integer.parseInt(args[2]);
        long probes = Long.parseLong(args[3]);
        String shape = args[4], type = args[5];
        List<String> modes = List.of(args[6].split(","));
        int repeats = Integer.parseInt(args[7]), warmups = Integer.parseInt(args[8]);
        String access = args.length >= 10 ? args[9] : "keep";
        long queryMemoryBytes = args.length >= 11 ? Long.parseLong(args[10]) : 0;
        String predicate = args.length >= 12 ? args[11] : "mod3";
        int slots = args.length >= 13 ? Integer.parseInt(args[12]) : 1;
        String filterMode = args.length >= 14 ? args[13] : "jit";
        long limit = args.length >= 15 ? Long.parseLong(args[14]) : 0;
        String backend = args.length >= 16 ? args[15] : "stock";
        String consumer = args.length >= 17 ? args[16] : "rows";
        boolean aggregates = consumer.equals("aggregates");
        if (!List.of("rows", "aggregates").contains(consumer) || aggregates && limit != 0) {
            throw new IllegalArgumentException("Invalid consumer; aggregate controls require complete input");
        }
        if (!List.of("stock", "llvm").contains(backend)) throw new IllegalArgumentException("Invalid backend");
        if (!List.of("mod3", "arith3", "arith16", "arith256", "arith256_spread", "arith_all", "arith_mixed", "sin").contains(predicate)
                || slots < 1 || slots > 64 || !List.of("jit", "java").contains(filterMode) || limit < 0
                || shape.equals("filtered_local") && !List.of("mod3", "arith3").contains(predicate)) {
            throw new IllegalArgumentException("Invalid predicate, slots, JIT mode or output limit");
        }
        long outputRows = limit == 0 ? probes : Math.min(limit, probes);
        if (queryMemoryBytes < 0) throw new IllegalArgumentException("Negative query memory limit");
        if (!List.of("keep", "indexed", "unindexed").contains(access)
                || access.equals("unindexed") && modes.contains("index")) {
            throw new IllegalArgumentException("Invalid index control");
        }
        if (rows < 500_000_000L || domain < 2 || probes < 1 || probes > rows - domain - 1
                || repeats < 3 || warmups < 1 || !List.of("local", "dense", "phase", "burst", "late_burst", "rare", "packed", "packed_rare", "packed_local", "filtered", "filtered_local").contains(shape)
                || !List.of("int", "long", "symbol", "composite").contains(type)) {
            throw new IllegalArgumentException("Require 500M+ RHS rows, valid key/probe counts, >=3 repeats and >=1 warmup");
        }
        String signature = rows + ":" + domain;
        Path marker = root.resolve("asof-dataset.txt");
        if (Files.exists(root) && !Files.exists(marker)) {
            try (var files = Files.list(root)) {
                if (files.findAny().isPresent()) throw new IllegalArgumentException("Use an empty experiment root");
            }
        }
        Files.createDirectories(root);
        if (Files.exists(marker) && !Files.readString(marker).equals(signature)) {
            throw new IllegalArgumentException("Dataset parameters differ; use a separate empty root");
        }
        Files.writeString(marker, signature);
        System.out.printf("RUN args=%s java=%s classes=%s compiler_classes=%s java_options=%s consumer=%s%n",
                Arrays.toString(args), System.getProperty("java.runtime.version"),
                AsOfSelectionMatrix.class.getProtectionDomain().getCodeSource().getLocation(),
                System.getenv("ASOF_CLASSES"), System.getenv("JAVA_TOOL_OPTIONS"), consumer);
        boolean[] forceForward = {false};
        var config = new DefaultCairoConfiguration(root.toString()) {
            @Override
            public long getQueryMemoryLimitBytes() {
                return queryMemoryBytes;
            }

            @Override
            public long getSqlHorizonJoinBwdScanAbsoluteThreshold() {
                return forceForward[0] ? 0 : super.getSqlHorizonJoinBwdScanAbsoluteThreshold();
            }
        };
        try (var engine = new CairoEngine(config);
             var pool = slots > 1 ? new WorkerPool(() -> slots - 1) : null;
             var context = new SqlExecutionContextImpl(engine, Math.max(1, slots - 1)).with(
                     config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null)) {
            if (pool != null) {
                WorkerPoolUtils.setupQueryJobs(pool, engine);
                pool.start();
            }
            context.setJitMode(filterMode.equals("jit") ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
            // Filter stealing is exposed by the async filter factory. A one-slot run
            // reduces tasks on the query thread; larger runs use the existing query jobs.
            context.setParallelFilterEnabled(shape.startsWith("filtered"));
            context.setParallelGroupByEnabled(false);
            context.setParallelHorizonJoinEnabled(false);
            if (backend.equals("llvm")) context.setQueryCodeGenerator(new ResolvedQueryCompiler(new LlvmBackend(), root.resolve("codegen")));
            String rightTable = shape.endsWith("rare") ? "rhs_rare" : "rhs";
            long rarePeriod = (long) domain * domain;
            String rightKey = shape.endsWith("rare") ? "case when x%" + rarePeriod
                    + "=0 then 0 else 1+x%" + (domain - 1) + " end" : "x%" + domain;
            if (engine.getTableTokenIfExists(rightTable) == null) {
                System.out.printf("CREATE %s rows=%d domain=%d root=%s%n", rightTable, rows, domain, root);
                engine.execute("create table " + rightTable + " as (select cast(x*1000 as timestamp) ts,cast(" + rightKey
                        + " as int) k,(" + rightKey + ")*4294967296+123 kl,cast('s'||(" + rightKey
                        + ") as symbol) ks,x v from long_sequence(" + rows + ")) timestamp(ts) partition by hour", context);
            }
            try (var reader = engine.getReader(rightTable)) {
                if (reader.size() != rows) throw new AssertionError("Incomplete RHS dataset");
            }
            long step = shape.startsWith("packed") ? 1 : (rows - domain - 1) / probes;
            String position = "(" + domain + "+x*" + step + ")";
            if (shape.equals("filtered_local")) position = "(" + position + "/3*3)";
            String key = (shape.equals("local") || shape.equals("packed_local") || shape.equals("filtered_local")) ? position + "%" + domain : "x*997%" + domain;
            if (shape.equals("phase")) key = "case when x<=" + probes / 2 + " then x*997%" + domain
                    + " else " + position + "%" + domain + " end";
            long half = probes / 2;
            long closeStep = Math.max(1, step / 64);
            long farStep = (rows - domain - 1 - half * closeStep) / Math.max(1, probes - half);
            if (shape.equals("burst") || shape.equals("late_burst")) {
                boolean denseFirst = shape.equals("burst");
                long firstStep = denseFirst ? closeStep : farStep;
                long secondStep = denseFirst ? farStep : closeStep;
                position = "(" + domain + "+case when x<=" + half + " then x*" + firstStep
                        + " else " + half * firstStep + "+(x-" + half + ")*" + secondStep + " end)";
                key = "case when x" + (denseFirst ? "<=" : ">") + half + " then x*997%" + domain
                        + " else " + position + "%" + domain + " end";
            }
            if (shape.endsWith("rare")) key = "0";
            String probeName = "probe_" + shape + "_" + probes;
            if (engine.getTableTokenIfExists(probeName) == null) {
                engine.execute("create table " + probeName + " as (select cast(" + position
                        + "*1000 as timestamp) ts,cast(" + key + " as int) k,(" + key
                        + ")*4294967296+123 kl,cast('s'||(" + key + ") as symbol) ks from long_sequence("
                        + probes + ")) timestamp(ts) partition by hour", context);
            }
            String condition = switch (type) {
                case "int" -> "p.k=q.k";
                case "long" -> "p.kl=q.kl";
                case "symbol" -> "p.ks=q.ks";
                default -> "p.k=q.k and p.kl=q.kl";
            };
            long expectedSum = 0, expectedHash = 0;
            long aggregateSum = 0, aggregateMin = Long.MAX_VALUE, aggregateMax = Long.MIN_VALUE;
            long aggregateCount = 0, expressionSum = 0;
            for (long x = 1; x <= outputRows; x++) {
                long at = domain + x * step;
                if (shape.equals("filtered_local")) at = at / 3 * 3;
                if (shape.equals("burst") || shape.equals("late_burst")) {
                    long firstStep = shape.equals("burst") ? closeStep : farStep;
                    long secondStep = shape.equals("burst") ? farStep : closeStep;
                    at = domain + Math.min(x, half) * firstStep + Math.max(0, x - half) * secondStep;
                }
                boolean localPhase = shape.equals("burst") && x > half || shape.equals("late_burst") && x <= half;
                long k = localPhase || (shape.equals("local") || shape.equals("packed_local") || shape.equals("filtered_local")) || shape.equals("phase") && x > probes / 2 ? at % domain : x * 997 % domain;
                if (shape.endsWith("rare")) k = 0;
                long value = at - Math.floorMod(at - k, domain);
                if (shape.startsWith("filtered")) {
                    while (value > 0 && !accepts(predicate, value)) value -= domain;
                    if (value <= 0) value = Long.MIN_VALUE;
                }
                if (shape.endsWith("rare")) {
                    value = at / rarePeriod * rarePeriod;
                    if (value == 0) value = Long.MIN_VALUE;
                }
                expectedSum += value;
                expectedHash = Long.rotateLeft(expectedHash, 7) ^ value;
                if (value != Long.MIN_VALUE) {
                    aggregateSum += value;
                    aggregateMin = Math.min(aggregateMin, value);
                    aggregateMax = Math.max(aggregateMax, value);
                    aggregateCount++;
                    expressionSum += value + k;
                }
            }
            if (aggregates) {
                long[] expected = {aggregateCount == 0 ? Long.MIN_VALUE : aggregateSum,
                        aggregateCount == 0 ? Long.MIN_VALUE : aggregateMin, aggregateMax, aggregateCount,
                        aggregateCount == 0 ? Long.MIN_VALUE : expressionSum, outputRows};
                expectedSum = expectedHash = 0;
                for (long value : expected) {
                    expectedSum += value;
                    expectedHash = Long.rotateLeft(expectedHash, 7) ^ value;
                }
            }
            if (modes.contains("index") || !access.equals("keep")) {
                boolean indexed;
                try (var reader = engine.getReader(rightTable)) {
                    indexed = reader.getMetadata().isColumnIndexed(reader.getMetadata().getColumnIndex("ks"));
                }
                boolean wantIndex = !access.equals("unindexed");
                if (indexed != wantIndex) {
                    engine.execute("alter table " + rightTable + " alter column ks " + (wantIndex ? "add" : "drop") + " index", context);
                }
            }
            var factories = new ArrayList<RecordCursorFactory>();
            try (var profile = new ResolvedBatchMatrix.ProfileControl()) {
                for (String mode : modes) {
                    boolean horizon = mode.equals("horizon") || mode.equals("horizon_forward");
                    forceForward[0] = mode.equals("horizon_forward");
                    String hint = switch (mode) {
                        case "fast", "adaptive", "horizon", "horizon_forward" -> "";
                        case "dense", "linear", "memoized", "memoized_driveby", "index" -> "/*+ asof_" + mode + "(p q) */ ";
                        default -> throw new IllegalArgumentException("Unknown mode " + mode);
                    };
                    String rightInput = shape.startsWith("filtered") ? "(select * from " + rightTable + " where " + predicateSql(predicate) + ")" : rightTable;
                    String columns = aggregates ? "sum(q.v),min(q.v),max(q.v),count(q.v),sum(q.v+p.k),count(*)" : "p.ts,q.v";
                    String sql = "select " + hint + columns + " from " + probeName + " p "
                            + (horizon ? "horizon" : "asof") + " join " + rightInput + " q on (" + condition + ")"
                            + (horizon ? " list (0) as h" : "") + (limit > 0 ? " limit " + limit : "");
                    RecordCursorFactory factory;
                    long compileStart = System.nanoTime();
                    try (var compiler = engine.getSqlCompiler()) { factory = compiler.compile(sql, context).getRecordCursorFactory(); }
                    factories.add(factory);
                    System.out.printf(Locale.ROOT, "COMPILE backend=%s mode=%s ms=%.3f%n", backend, mode, (System.nanoTime() - compileStart) / 1e6);
                    var plan = new TextPlanSink();
                    plan.of(factory, context);
                    var text = new StringBuilder();
                    for (int i = 1; i <= plan.getLineCount(); i++) text.append(plan.getLine(i)).append('\n');
                    String required = switch (mode) {
                        case "fast" -> "AsOf Join Fast";
                        case "adaptive" -> shape.startsWith("filtered") ? "Filtered AsOf Join Adaptive" : "AsOf Join Adaptive";
                        case "dense" -> "AsOf Join Dense";
                        case "memoized", "memoized_driveby" -> "AsOf Join Memoized";
                        case "index" -> "AsOf Join Indexed";
                        case "horizon", "horizon_forward" -> "Horizon Join";
                        default -> "AsOf Join";
                    };
                    if (!text.toString().contains(required)) throw new AssertionError("Ignored algorithm " + mode + "\n" + text);
                    if (shape.startsWith("filtered") && mode.equals("linear")) {
                        boolean expectJit = filterMode.equals("jit") && predicate.startsWith("arith");
                        String requiredFilter = expectJit ? "Async JIT Filter" : "Async Filter";
                        if (!text.toString().contains(requiredFilter)) throw new AssertionError("Unexpected filter path\n" + text);
                    }
                    System.out.printf("PLAN %s %s %s%n%s", shape, type, mode, text);
                }
                double[][] times = new double[modes.size()][repeats];
                var threads = ManagementFactory.getThreadMXBean();
                var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                var compilation = ManagementFactory.getCompilationMXBean();
                boolean compilationClock = compilation != null && compilation.isCompilationTimeMonitoringSupported();
                System.out.printf("FILTER predicate=%s slots=%d mode=%s limit=%d frame_rows=%d..%d%n",
                        predicate, slots, filterMode, limit, config.getSqlPageFrameMinRows(), config.getSqlPageFrameMaxRows());
                System.out.printf("READY pid=%d rows=%d probes=%d step=%d domain=%d expected=%d/%d%n",
                        ProcessHandle.current().pid(), rows, probes, step, domain, expectedSum, expectedHash);
                for (int round = -warmups; round < repeats; round++) {
                    for (int j = 0; j < modes.size(); j++) {
                        int index = Math.floorMod(round + warmups + j, modes.size());
                        var before = ScanMatrix.Resources.read();
                        long sum = 0, hash = 0, count = 0;
                        long jvmCompileMs = compilationClock ? compilation.getTotalCompilationTime() : -1;
                        if (round >= 0) profile.command("enable");
                        long processCpu = os.getProcessCpuTime(), cpu = threads.getCurrentThreadCpuTime(), start = System.nanoTime();
                        double firstRowMs = Double.NaN;
                        try (var cursor = factories.get(index).getCursor(context)) {
                            var record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                if (count == 0) firstRowMs = (System.nanoTime() - start) / 1e6;
                                for (int col = aggregates ? 0 : 1; col < (aggregates ? 6 : 2); col++) {
                                    long value = record.getLong(col);
                                    sum += value;
                                    hash = Long.rotateLeft(hash, 7) ^ value;
                                }
                                count++;
                            }
                        }
                        double ms = (System.nanoTime() - start) / 1e6;
                        double cpuMs = (threads.getCurrentThreadCpuTime() - cpu) / 1e6;
                        double processCpuMs = (os.getProcessCpuTime() - processCpu) / 1e6;
                        if (round >= 0) profile.command("disable");
                        if (compilationClock) jvmCompileMs = compilation.getTotalCompilationTime() - jvmCompileMs;
                        var after = ScanMatrix.Resources.read();
                        if (count != (aggregates ? 1 : outputRows) || sum != expectedSum || hash != expectedHash) {
                            throw new AssertionError("Wrong result " + modes.get(index) + " " + count + "/" + sum + "/" + hash);
                        }
                        if (round >= 0) times[index][round] = ms;
                        System.out.printf(Locale.ROOT, "ASOF %s %s %s round=%d ms=%.3f cpu_ms=%.3f process_cpu_ms=%.3f first_row_ms=%.3f jvm_compile_ms=%d major_faults=%d read_bytes=%d checksum=%d/%d%n",
                                shape, type, modes.get(index), round, ms, cpuMs, processCpuMs, firstRowMs, jvmCompileMs, after.faults() - before.faults(), after.bytes() - before.bytes(), sum, hash);
                        if (backend.equals("llvm")) {
                            RecordCursorFactory scan = factories.get(index);
                            while (scan != null && !(scan instanceof io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory)) scan = scan.getBaseFactory();
                            if (scan instanceof io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory asof)
                                System.out.printf("NATIVE aggregate_probes=%d forward_rows=%d filter_rows=%d lookup_rows=%d%n",
                                        asof.getNativeAggregateRows(), asof.getNativeForwardRows(), asof.getCompiledFilterRows(),
                                        asof.getNativeLookupRows());
                        }
                    }
                }
                for (int i = 0; i < modes.size(); i++) {
                    Arrays.sort(times[i]);
                    System.out.printf(Locale.ROOT, "RESULT %s %s %s median_ms=%.3f range=%.3f..%.3f%n",
                            shape, type, modes.get(i), times[i][repeats / 2], times[i][0], times[i][repeats - 1]);
                }
            } finally { for (var factory : factories) factory.close(); }
        } finally { LogFactory.haltInstance(); }
    }

    private static boolean accepts(String predicate, long value) {
        return switch (predicate) {
            case "mod3", "arith3" -> value % 3 == 0;
            case "arith16" -> value % 1009 < 63;
            case "arith256" -> value % 1009 < 4;
            case "arith256_spread" -> value % 1009 % 256 == 0;
            case "arith_all" -> value > 0;
            case "arith_mixed" -> value * 6364136223846793005L > 0;
            case "sin" -> Math.sin(value) > 0.5;
            default -> throw new IllegalArgumentException(predicate);
        };
    }

    private static String predicateSql(String predicate) {
        return switch (predicate) {
            case "mod3" -> "v%3=0";
            case "arith3" -> "v-v/3*3=0";
            case "arith16" -> "v-v/1009*1009<63";
            case "arith256" -> "v-v/1009*1009<4";
            case "arith256_spread" -> "(v-v/1009*1009)-(v-v/1009*1009)/256*256=0";
            case "arith_all" -> "v>0";
            case "arith_mixed" -> "v*6364136223846793005>0";
            case "sin" -> "sin(v)>0.5";
            default -> throw new IllegalArgumentException(predicate);
        };
    }
}
