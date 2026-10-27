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
import io.questdb.PropServerConfiguration;
import io.questdb.FreeOnExit;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/** Fixed-work filter/aggregate experiment. The caller supplies an isolated scratch database. */
public final class FilterFusionExperiment {
    private static final long ROWS = 500_000_000L;

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 7) throw new IllegalArgumentException(
                "root create|java|materialized|fused [threshold 0..1000] [slots] [samples] [warmups] [k|h]");
        Path root = Path.of(args[0]);
        String mode = args[1];
        int threshold = args.length > 2 ? Integer.parseInt(args[2]) : 500;
        int slots = args.length > 3 ? Integer.parseInt(args[3]) : 8;
        int samples = args.length > 4 ? Integer.parseInt(args[4]) : 5;
        int warmups = args.length > 5 ? Integer.parseInt(args[5]) : 3;
        String key = args.length > 6 ? args[6] : "k";
        if (!List.of("create", "java", "materialized", "fused").contains(mode)
                || !List.of("k", "h").contains(key) || threshold < 0 || threshold > 1000
                || slots < 2 || samples < 3 || warmups < 1) throw new IllegalArgumentException("Invalid experiment arguments");
        Files.createDirectories(root.resolve("db"));
        var props = new Properties();
        props.setProperty("http.enabled", "false");
        props.setProperty("telemetry.enabled", "false");
        props.setProperty("shared.query.worker.count", Integer.toString(slots - 1));
        var server = new PropServerConfiguration(root.toString(), props, Map.of(),
                LogFactory.getLog(FilterFusionExperiment.class), new BuildInformationHolder());
        var cfg = server.getCairoConfiguration();
        try (var free = new FreeOnExit(); var engine = new CairoEngine(cfg)) {
            server.init(engine, free);
            try (var pool = new WorkerPool(server.getSharedWorkerPoolQueryConfiguration());
                 var ctx = new SqlExecutionContextImpl(engine, slots - 1).with(
                         cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null)) {
                WorkerPoolUtils.setupQueryJobs(pool, engine);
                pool.start();
                if (mode.equals("create")) {
                    if (engine.getTableTokenIfExists("fusion_input") == null) {
                        System.out.println("CREATE 500M rows");
                        engine.execute("create table fusion_input as (select cast((x-1)%1024 as int) k,"
                                + "cast((x-1)%1048576 as int) h,cast((x*15485863)%1000 as int) p,"
                                + "case when x%13=0 then null else x%8191 end v from long_sequence(500000000))", ctx);
                    }
                    return;
                }
                try (var reader = engine.getReader("fusion_input")) {
                    if (reader.size() != ROWS) throw new AssertionError("Expected 500M rows");
                }
                String sql = "select " + key + ",count(),sum(v) from fusion_input where p<" + threshold + " group by " + key;
                List<CompiledGroupBy.ResultRow> expected;
                try (var control = Demo.compileSql(engine, ctx, sql, true)) { expected = Demo.readSql(control, ctx, 1); }
                long accepted = expected.stream().mapToLong(row -> row.values().getFirst()).sum();
                if (accepted != ROWS / 1000 * threshold) throw new AssertionError("Predicate cardinality: " + accepted);
                if (!mode.equals("java")) ctx.setQueryCodeGenerator(mode.equals("fused")
                        ? ResolvedQueryCompiler.filterFusionExperiment(new LlvmBackend(), root.resolve("code"))
                        : new ResolvedQueryCompiler(new LlvmBackend(), root.resolve("code")));
                long compileStart = System.nanoTime();
                try (var factory = Demo.compileSql(engine, ctx, sql, true);
                     var profile = new ResolvedBatchMatrix.ProfileControl()) {
                    System.out.printf(Locale.ROOT, "PREPARE mode=%s ms=%.3f rows=%d accepted=%d groups=%d slots=%d frame=%d batch=%d sql=%s%n",
                            mode, (System.nanoTime()-compileStart)/1e6, ROWS, accepted, expected.size(), slots,
                            cfg.getSqlPageFrameMaxRows(), cfg.getGroupByBatchSize(), sql);
                    var plan = new TextPlanSink(); plan.of(factory, ctx);
                    for (int i=1; i<=plan.getLineCount(); i++) System.out.println(plan.getLine(i));
                    var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                    for (int round=-warmups; round<samples; round++) {
                        var before = ScanMatrix.Resources.read();
                        long[] nativeBefore = counters(factory, slots);
                        long cpu = os.getProcessCpuTime();
                        var timing = new ExecutionTimings();
                        long gc = gcMillis();
                        if (round >= 0) {
                            timing.profileStart = () -> profile.commandUnchecked("enable");
                            timing.profileEnd = () -> profile.commandUnchecked("disable");
                        }
                        var actual = Demo.readSql(factory, ctx, 1, timing);
                        double ms = (timing.build + timing.copy + timing.release)/1e6;
                        double cpuMs = (os.getProcessCpuTime()-cpu)/1e6;
                        var after = ScanMatrix.Resources.read();
                        long[] nativeAfter = counters(factory, slots);
                        if (!actual.equals(expected)) throw new AssertionError("Results differ from ordinary SQL");
                        long filterRows = nativeAfter[0]-nativeBefore[0], selected = nativeAfter[1]-nativeBefore[1], updated = nativeAfter[2]-nativeBefore[2];
                        long fused = nativeAfter[3]-nativeBefore[3];
                        if (mode.equals("fused") && fused != ROWS) throw new AssertionError("Fusion coverage: " + fused);
                        if (!mode.equals("java") && (filterRows != ROWS || selected != accepted || updated != accepted)) {
                            throw new AssertionError("Native coverage: " + filterRows + "/" + selected + "/" + updated);
                        }
                        System.out.printf(Locale.ROOT, "RUN mode=%s key=%s threshold=%d warm=%s ms=%.3f build_ms=%.3f cpu_ms=%.3f gc_ms=%d filter=%d selected=%d updated=%d fused=%d faults=%d reads=%d hash=%d%n",
                                mode, key, threshold, round<0, ms, timing.build/1e6, cpuMs, gcMillis()-gc, filterRows, selected, updated, fused,
                                after.faults()-before.faults(), after.bytes()-before.bytes(), actual.hashCode());
                    }
                }
            }
        } finally { LogFactory.haltInstance(); }
    }

    private static long gcMillis() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum();
    }

    private static long[] counters(RecordCursorFactory factory, int slots) {
        while (!(factory instanceof AsyncGroupByRecordCursorFactory)) {
            factory = factory.getBaseFactory();
            if (factory == null) return new long[4];
        }
        var atom = ((AsyncGroupByRecordCursorFactory) factory).getAtom();
        long[] result = new long[4];
        for (int slot=-1; slot<slots-1; slot++) {
            var updater = atom.getCompiledUpdater(slot);
            long[] filters = ResolvedQueryCompiler.filterCounters(updater);
            result[0] += filters[0]; result[1] += filters[1];
            result[2] += ResolvedQueryCompiler.updatedRows(updater);
            if (updater instanceof AggregateSlot aggregate) result[3] += aggregate.fusedInputRows();
        }
        return result;
    }
}
