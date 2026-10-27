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

import io.questdb.BuildInformationHolder;
import io.questdb.FreeOnExit;
import io.questdb.PropServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTracker;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/** Existing SQL benchmark controls extended to one composed pipeline; no custom query executor. */
final class ResolvedJoinMatrix {
    static void run(String[] args) throws Exception {
        int repeats = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        String cases = args.length > 1 ? args[1] : "unique,filtered,duplicates";
        long rows = args.length > 2 ? Long.parseLong(args[2]) : 500_000_000L;
        int warmups = args.length > 3 ? Integer.parseInt(args[3]) : 5;
        String forced = args.length > 4 ? args[4] : "";
        int slots = args.length > 5 ? Integer.parseInt(args[5]) : 8;
        boolean concurrent = args.length > 6 && Boolean.parseBoolean(args[6]);
        if (rows < 500_000_000L || repeats < 3 || warmups < 1 || slots < 2 || slots > 64
                || !List.of("", "java", "llvm").contains(forced)) throw new IllegalArgumentException("Require 500M rows, >=3 samples, >=1 warmup, 2..64 slots; server defaults disable parallel joins with zero background workers");
        Path root = Files.createTempDirectory("query-resolved-join-");
        Path artifacts = Files.createTempDirectory("query-join-code-");
        try {
            var properties = new Properties();
            properties.setProperty("http.enabled", "false");
            properties.setProperty("shared.query.worker.count", Integer.toString(slots - 1));
            var server = new PropServerConfiguration(root.toString(), properties, java.util.Map.of(),
                    LogFactory.getLog(ResolvedJoinMatrix.class), new BuildInformationHolder());
            var config = server.getCairoConfiguration();
            Files.createDirectories(Path.of(config.getDbRoot().toString()));
            try (var free = new FreeOnExit(); var engine = new CairoEngine(config)) {
                server.init(engine, free);
                try (var pool = slots > 1 ? new WorkerPool(server.getSharedWorkerPoolQueryConfiguration()) : null;
                     var context = context(engine, slots)) {
                    if (pool != null) { WorkerPoolUtils.setupQueryJobs(pool, engine); pool.start(); }
                    System.out.printf("JOIN rows=%d slots=%d background-workers=%d warmups=%d samples=%d mode=%s concurrency=%s%n",
                            rows, slots, slots - 1, warmups, repeats, forced, concurrent);
                    System.out.printf("DEFAULTS frame=%d..%d parallel-build-min=%d partition-rows=%d memory-limit=%d worker-mode=%s%n",
                            config.getSqlPageFrameMinRows(), config.getSqlPageFrameMaxRows(),
                            config.getSqlParallelHashJoinGroupByBuildParallelMinRows(), config.getSqlParallelHashJoinGroupByBuildRowsPerPartition(),
                            config.getQueryMemoryLimitBytes(), server.getSharedWorkerPoolQueryConfiguration().getWorkerPoolMode());
                    System.out.println("CREATE 500M+ probe rows; data creation and compilation excluded from execution timing.");
                    boolean skew = cases.equals("keyed-skew");
                    if (!skew && cases.contains("keyed-skew")) throw new IllegalArgumentException("Run keyed-skew alone; it uses a different input distribution");
                    engine.execute("create table probe as (select cast(x*17%1048576-524288 as int) k,"
                            + "case when x%23=0 then null " + (skew ? "when x%5!=0 then 0 " : "") + "else x%100003-50001 end v,"
                            + "cast(x%1009-504 as int) w,cast(x%127-63 as short) s from long_sequence(" + rows + "))", context);
                    try (var reader = engine.getReader("probe")) { if (reader.size() != rows) throw new AssertionError("Probe row count"); }
                    for (String name : cases.split(",")) {
                        if (!List.of("unique", "filtered", "duplicates", "payload", "payload-duplicates", "post-filter", "keyed-duplicates", "keyed-high", "keyed-skew").contains(name)) throw new IllegalArgumentException("Unknown join case " + name);
                        long domain = name.endsWith("duplicates") ? 524288 : 1048576;
                        engine.execute("create table dimension as (select cast(x%" + domain + "-524288 as int) k,x%7 f from long_sequence(1048576))", context);
                        String sql = "select count(),count(p.v),sum(p.v),sum(p.w),sum(p.s)"
                                + (name.startsWith("payload") || name.equals("post-filter") ? ",sum(d.f)" : "")
                                + " from probe p join dimension d on p.k=d.k"
                                + (name.equals("filtered") ? " where p.w>=0 and d.f!=0" : name.equals("post-filter") ? " where p.w>d.f" : "");
                        if (name.startsWith("keyed")) {
                            String key = name.equals("keyed-duplicates") ? "d.f" : "p.v";
                            sql = "select " + key + ",count(),sum(p.v),sum(d.f) from probe p join dimension d on p.k=d.k group by " + key + " order by 1";
                        }
                        try {
                            measure(engine, slots, artifacts, name, sql, warmups, repeats, forced, concurrent);
                        } finally { engine.execute("drop table dimension", context); }
                    }
                }
            }
        } finally {
            LogFactory.haltInstance();
            WorkloadMatrix.clearDirectory(artifacts);
            WorkloadMatrix.clearDirectory(root);
        }
    }

    private static SqlExecutionContextImpl context(CairoEngine engine, int slots) {
        return new SqlExecutionContextImpl(engine, Math.max(1, slots - 1)).with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
    }

    private static void measure(CairoEngine engine, int slots, Path artifacts, String name, String sql,
                                int warmups, int repeats, String forced, boolean concurrent) throws Exception {
        var runs = new ArrayList<Run>();
        try (var profile = new ResolvedBatchMatrix.ProfileControl()) {
            // Always validate against the ordinary SQL path, even in a forced LLVM process.
            runs.add(new Run(engine, slots, artifacts, sql, false));
            long[] expected = runs.getFirst().execute().values();
            if (!forced.equals("java")) runs.add(new Run(engine, slots, artifacts, sql, true));
            var measured = runs.stream().filter(run -> forced.isEmpty() || run.mode.equals(forced)).toList();
            double[][] times = new double[measured.size()][repeats];
            System.out.println("CASE " + name + " sql=" + sql + " expected="
                    + (expected.length <= 16 ? Arrays.toString(expected) : expected.length + " values, hash=" + Arrays.hashCode(expected)));
            for (int round = -warmups; round < repeats; round++) {
                if (round == 0) profile.command("enable");
                for (int j = 0; j < measured.size(); j++) {
                    int index = Math.floorMod(round + j, measured.size());
                    Run run = measured.get(index);
                    Sample sample = run.execute();
                    if (!Arrays.equals(expected, sample.values)) throw new AssertionError("Join result mismatch: " + Arrays.toString(sample.values));
                    if (run.mode.equals("llvm") && sample.nativeRows == 0) throw new AssertionError("No native join coverage");
                    if (round >= 0) times[index][round] = sample.elapsed / 1e6;
                    System.out.printf(Locale.ROOT, "%s %s %s ms=%.3f build=%.3f probe=%.3f merge=%.3f sampled-peak-MiB=%.3f native-rows=%d map-misses=%d parallel-build=%s partitions=%d major-faults=%d read-bytes=%d%n",
                            round < 0 ? "warm" : "sample", name, run.mode, sample.elapsed / 1e6,
                            phaseMillis(sample.phases[0]), phaseMillis(sample.phases[1]), phaseMillis(sample.phases[2]),
                            sample.peak / 1048576.0, sample.nativeRows, sample.mapMisses, sample.parallel, sample.partitions, sample.faults, sample.reads);
                }
            }
            profile.command("disable");
            for (int i = 0; i < measured.size(); i++) {
                Arrays.sort(times[i]);
                System.out.printf(Locale.ROOT, "RESULT %s %s slots=%d median=%.3f range=[%.3f,%.3f]%n", name,
                        measured.get(i).mode, slots, median(times[i]), times[i][0], times[i][repeats - 1]);
            }
            if (concurrent) {
                if (!forced.isEmpty()) throw new IllegalArgumentException("Concurrent comparison needs both modes");
                try (var secondJava = new Run(engine, slots, artifacts, sql, false);
                     var secondLlvm = new Run(engine, slots, artifacts, sql, true)) {
                    paired(name, "java/java", runs.get(0), secondJava, expected, warmups, repeats);
                    paired(name, "java/llvm", runs.get(0), runs.get(1), expected, warmups, repeats);
                    paired(name, "llvm/llvm", runs.get(1), secondLlvm, expected, warmups, repeats);
                }
            }
        } finally { for (Run run : runs) run.close(); }
    }

    private static void paired(String name, String modes, Run first, Run second, long[] expected, int warmups, int repeats) throws Exception {
        try (var clients = Executors.newFixedThreadPool(2)) {
            double[] wall = new double[repeats];
            for (int round = -warmups; round < repeats; round++) {
                var barrier = new CyclicBarrier(3);
                var a = clients.submit(() -> { barrier.await(); return first.execute(); });
                var b = clients.submit(() -> { barrier.await(); return second.execute(); });
                long start = System.nanoTime();
                barrier.await();
                Sample x = a.get(), y = b.get();
                double elapsed = (System.nanoTime() - start) / 1e6;
                if (!Arrays.equals(x.values, expected) || !Arrays.equals(y.values, expected)) throw new AssertionError("Concurrent SQL result");
                if (round >= 0) wall[round] = elapsed;
                System.out.printf(Locale.ROOT, "PAIR %s %s %s wall=%.3f first=%.3f second=%.3f native-rows=%d/%d%n",
                        round < 0 ? "warm" : "sample", name, modes, elapsed, x.elapsed / 1e6, y.elapsed / 1e6, x.nativeRows, y.nativeRows);
            }
            Arrays.sort(wall);
            System.out.printf(Locale.ROOT, "PAIR-RESULT %s %s median=%.3f range=[%.3f,%.3f]%n", name, modes,
                    median(wall), wall[0], wall[repeats - 1]);
        }
    }

    private static double median(double[] sorted) { return (sorted[(sorted.length - 1) / 2] + sorted[sorted.length / 2]) / 2; }

    private static double phaseMillis(long nanos) { return nanos < 0 ? Double.NaN : nanos / 1e6; }

    private record Sample(long[] values, long elapsed, long[] phases, long peak, long nativeRows, long mapMisses,
                          boolean parallel, int partitions, long faults, long reads) { }

    private static final class Run implements AutoCloseable {
        final SqlExecutionContextImpl context;
        final RecordCursorFactory factory;
        final AsyncHashJoinGroupByAtom atom;
        final String mode;
        final Method timings;
        final Method nativeRows;
        final Method mapMisses;
        volatile MemoryTracker activeTracker;
        final AtomicLong peak = new AtomicLong();
        final Thread sampler;
        volatile boolean sampling, closed;

        Run(CairoEngine engine, int slots, Path artifacts, String sql, boolean llvm) throws Exception {
            mode = llvm ? "llvm" : "java";
            context = context(engine, slots);
            if (llvm) Compilation.install(context, artifacts);
            long start = System.nanoTime();
            factory = Demo.compileSql(engine, context, sql, true);
            double prepare = (System.nanoTime() - start) / 1e6;
            RecordCursorFactory base = factory;
            while (base != null && !(base instanceof AsyncHashJoinGroupByRecordCursorFactory)) base = base.getBaseFactory();
            if (llvm && base == null) throw new AssertionError("Expected compiled join planner path");
            atom = base == null ? null : ((AsyncHashJoinGroupByRecordCursorFactory) base).getAtom();
            Method timingMethod = null, rowsMethod = null;
            if (atom != null) try {
                atom.getClass().getMethod("collectExecutionTimings").invoke(atom);
                timingMethod = atom.getClass().getMethod("getExecutionTimings");
                rowsMethod = atom.getClass().getMethod("getCompiledJoinNativeRows");
            } catch (NoSuchMethodException stock) { if (llvm) throw stock; }
            timings = timingMethod;
            nativeRows = rowsMethod;
            Method missesMethod = null;
            if (atom != null) try {
                missesMethod = atom.getClass().getMethod("getCompiledJoinMapMisses");
            } catch (NoSuchMethodException stock) { if (llvm) throw stock; }
            mapMisses = missesMethod;
            var sink = new TextPlanSink(); sink.of(factory, context);
            StringBuilder plan = new StringBuilder();
            for (int i = 1; i <= sink.getLineCount(); i++) plan.append(sink.getLine(i)).append('\n');
            if (llvm != plan.toString().contains("compiledJoinPipeline")) throw new AssertionError(plan.toString());
            System.out.printf(Locale.ROOT, "PREPARE %s %.3f ms%n%s", mode, prepare, plan);
            // Sampling bounds the observed query memory, not an exact allocator high-water mark.
            sampler = Thread.ofPlatform().daemon().name("join-memory-sampler").start(() -> {
                while (!closed) {
                    MemoryTracker tracker = activeTracker;
                    if (sampling && tracker != null) peak.accumulateAndGet(tracker.getUsed(), Math::max);
                    LockSupport.parkNanos(1_000_000);
                }
            });
        }

        Sample execute() throws Exception {
            var output = new io.questdb.std.LongList();
            long beforeRows = nativeRows != null ? (long) nativeRows.invoke(atom) : 0;
            long beforeMisses = mapMisses != null ? (long) mapMisses.invoke(atom) : 0;
            var before = ScanMatrix.Resources.read();
            peak.set(0); sampling = true;
            boolean parallel; int partitions;
            long started = System.nanoTime();
            try (var cursor = factory.getCursor(context)) {
                activeTracker = context.getMemoryTracker();
                var record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0; i < factory.getMetadata().getColumnCount(); i++) output.add(record.getLong(i));
                }
                parallel = atom != null && atom.isBuiltInRounds(); partitions = atom == null ? 0 : atom.getBuildPartitionCount();
                if (activeTracker != null) peak.accumulateAndGet(activeTracker.getUsed(), Math::max);
            } finally { sampling = false; activeTracker = null; }
            long elapsed = System.nanoTime() - started;
            var after = ScanMatrix.Resources.read();
            long[] phases = timings != null ? (long[]) timings.invoke(atom) : new long[]{-1, -1, -1};
            long processed = nativeRows != null ? (long) nativeRows.invoke(atom) - beforeRows : 0;
            long misses = mapMisses != null ? (long) mapMisses.invoke(atom) - beforeMisses : 0;
            long[] values = new long[output.size()];
            for (int i = 0; i < values.length; i++) values[i] = output.getQuick(i);
            return new Sample(values, elapsed, phases, peak.get(), processed, misses, parallel, partitions,
                    after.faults() - before.faults(), after.bytes() - before.bytes());
        }

        @Override public void close() throws Exception {
            closed = true; sampler.interrupt(); sampler.join();
            try { factory.close(); } finally { context.close(); }
        }
    }

    // Stock core intentionally has no optional compiler API. Load it only for the LLVM candidate.
    private static final class Compilation {
        static void install(SqlExecutionContextImpl context, Path artifacts) {
            context.setQueryCodeGenerator(new ResolvedQueryCompiler(new LlvmBackend(), artifacts));
        }
    }
}
