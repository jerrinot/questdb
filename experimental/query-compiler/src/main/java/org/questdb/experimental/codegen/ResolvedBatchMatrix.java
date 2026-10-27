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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.questdb.experimental.codegen.Pipeline.*;

/** Same-map diagnostic and ordinary SQL comparison reached through ScanMatrix's resolved modes. */
final class ResolvedBatchMatrix {
    static void run(String[] args) throws Exception {
        run(args, false);
    }

    static void run(String[] args, boolean integrated) throws Exception {
        run(args, integrated, false);
    }

    static void runThreeWay(String[] args) throws Exception {
        run(args, true, true);
    }

    private static void run(String[] args, boolean integrated, boolean threeWay) throws Exception {
        int repeats = args.length > 0 ? Integer.parseInt(args[0]) : 9;
        String cases = args.length > 1 ? args[1] : "count,mixed";
        long rows = args.length > 2 ? Long.parseLong(args[2]) : 500_000_000L;
        int requestedBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : threeWay ? 0 : 1024;
        int warmups = args.length > 4 ? Integer.parseInt(args[4]) : 5;
        String forced = args.length > 5 ? args[5] : "";
        if (!List.of("", "java", "llvm", "native-hash", "native-auto", "duckdb").contains(forced)) {
            throw new IllegalArgumentException("Mode must be java, llvm, native-hash, native-auto or duckdb");
        }
        if ((forced.startsWith("native-") || forced.equals("duckdb")) && !integrated) throw new IllegalArgumentException("External references require SQL mode");
        if (System.getProperty("query.codegen.varcharKeys") != null
                && (!integrated || threeWay || forced.startsWith("native-") || forced.equals("duckdb"))) {
            throw new IllegalArgumentException("VARCHAR fixture is supported by ordinary SQL modes only");
        }
        String sqlFile = System.getProperty("query.codegen.sqlFile");
        if (sqlFile != null && (!integrated || threeWay || forced.startsWith("native-") || forced.equals("duckdb"))) {
            throw new IllegalArgumentException("SQL file requires the ordinary SQL Java/LLVM comparison");
        }
        String sqlOverride = sqlFile == null ? null : Files.readString(Path.of(sqlFile));
        int workers = args.length > 6 ? Integer.parseInt(args[6]) : 8;
        String configuration = args.length > 7 ? args[7] : threeWay ? "server" : "test";
        int requestedShardingThreshold = args.length > 8 ? Integer.parseInt(args[8]) : 0;
        if (requestedShardingThreshold < 0) throw new IllegalArgumentException("Sharding threshold must be nonnegative; 0 uses the default");
        if (threeWay && !forced.isEmpty()) throw new IllegalArgumentException("Three-way mode compares all implementations");
        if (!configuration.equals("test") && !configuration.equals("server")) throw new IllegalArgumentException("Configuration must be test or server");
        if (!integrated && configuration.equals("server")) throw new IllegalArgumentException("Server configuration requires SQL mode");
        if (workers < 1 || workers > 64) throw new IllegalArgumentException("Execution slots must be 1..64");
        if (rows < 500_000_000L || repeats < 3 || warmups < 1 || requestedBatchSize < 0) {
            throw new IllegalArgumentException("Require at least 500M rows, 3 samples, 1 warmup; batch 0 uses the configuration default");
        }
        Path db = Files.createTempDirectory("query-resolved-batch-");
        Path artifacts = Files.createTempDirectory("query-resolved-code-");
        try {
            PropServerConfiguration server = null;
            CairoConfiguration config;
            WorkerPoolConfiguration workerConfig = () -> workers - 1;
            if (configuration.equals("server")) {
                var properties = new Properties();
                // No listener is started by this harness. Disabling HTTP avoids loading
                // the server distribution's mime.types into the temporary installation.
                properties.setProperty("http.enabled", "false");
                properties.setProperty("shared.query.worker.count", Integer.toString(workers - 1));
                if (requestedBatchSize != 0) {
                    properties.setProperty("cairo.sql.parallel.groupby.batch.size", Integer.toString(requestedBatchSize));
                }
                if (requestedShardingThreshold != 0) {
                    properties.setProperty("cairo.sql.parallel.groupby.sharding.threshold", Integer.toString(requestedShardingThreshold));
                }
                server = new PropServerConfiguration(db.toString(), properties, java.util.Map.of(),
                        LogFactory.getLog(ResolvedBatchMatrix.class), new BuildInformationHolder());
                config = server.getCairoConfiguration();
                workerConfig = server.getSharedWorkerPoolQueryConfiguration();
                Files.createDirectories(Path.of(config.getDbRoot().toString()));
            } else {
                config = new DefaultCairoConfiguration(db.toString()) {
                    @Override public int getGroupByBatchSize() {
                        return requestedBatchSize == 0 ? super.getGroupByBatchSize() : requestedBatchSize;
                    }
                    @Override public int getGroupByShardingThreshold() {
                        return requestedShardingThreshold == 0 ? super.getGroupByShardingThreshold() : requestedShardingThreshold;
                    }
                };
            }
            int batchSize = config.getGroupByBatchSize();
            try (var freeOnExit = new FreeOnExit(); var engine = new CairoEngine(config)) {
                if (server != null) server.init(engine, freeOnExit);
                try (var pool = integrated && workers > 1 ? new WorkerPool(workerConfig) : null;
                     var context = new SqlExecutionContextImpl(engine, integrated ? Math.max(1, workers - 1) : 1).with(
                            config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null)) {
                    if (pool != null) { WorkerPoolUtils.setupQueryJobs(pool, engine); pool.start(); }
                    System.out.printf("Resolved %s control: rows=%d batch=%d warmups=%d samples=%d; slots=%d.%n",
                            integrated ? "SQL" : "batch", rows, batchSize, warmups, repeats, integrated ? workers : 1);
                    System.out.println(forced.equals("duckdb") ? "DuckDB JDBC reference; matched data and exact QuestDB SQL result validation."
                            : forced.startsWith("native-")
                            ? "Fixed-shape handwritten Clang O3 reference; preparation reported separately."
                            : "Actual SQL-selected functions; native LLVM O2; preparation reported separately.");
                    System.out.printf("Configuration=%s sharding-threshold=%d dispatcher=%s jit-mode=%d frame-rows=[%d,%d]%n",
                            configuration, config.getGroupByShardingThreshold(), workerConfig.getWorkerPoolMode(),
                            config.getSqlJitMode(), config.getSqlPageFrameMinRows(), config.getSqlPageFrameMaxRows());
                    if (threeWay) {
                        System.out.printf("Matched SQL/data/core/config. Normal and integrated: %d workers + caller; full: %d workers + coordinator. All share process CPU affinity.%n", workers - 1, workers);
                        System.out.println("Timing includes allocation/build/merge/output/cleanup; only comparison sort and compilation are excluded. NaN means uninstrumented. Full retains its existing 512 MiB native-state budget.");
                    }
                    for (String name : cases.split(",")) {
                        if (!integrated && !List.of("count", "mixed", "high").contains(name)) throw new IllegalArgumentException("Use SQL mode for " + name);
                        var spec = matchedSpec(name, rows, integrated);
                        System.out.println("CREATE " + name);
                        engine.execute(spec.ddl(), context);
                        try {
                            try (var reader = engine.getReader("input")) {
                                if (reader.size() != rows) throw new AssertionError("Actual input row count");
                            }
                            if (threeWay) {
                                measureThreeWay(engine, context, artifacts, spec, rows, workers, warmups, repeats);
                            } else if (integrated) {
                                measureSql(engine, context, artifacts, name, sqlOverride == null ? spec.sql() : sqlOverride, workers, warmups, repeats, forced);
                            } else {
                                measure(engine, context, config, artifacts, name, spec.sql(), rows, batchSize, warmups, repeats, forced);
                            }
                        } finally {
                            engine.execute("drop table input", context);
                        }
                    }
                }
            }
        } finally {
            io.questdb.log.LogFactory.haltInstance();
            WorkloadMatrix.clearDirectory(artifacts);
            WorkloadMatrix.clearDirectory(db);
        }
    }

    private static void measure(CairoEngine engine, SqlExecutionContextImpl context, CairoConfiguration config,
                                Path artifacts, String name, String sql, long rows, int batchSize, int warmups, int repeats, String forced) throws Exception {
        // This fixture's nonnegative keys make the filter true for every row. It keeps normal
        // resolution on the generic async branch while isolating only its probe/update boundary.
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            RecordCursorFactory current = factory;
            while (!(current instanceof AsyncGroupByRecordCursorFactory)) {
                current = current.getBaseFactory();
                if (current == null) throw new IllegalStateException("Expected ordinary async group-by: " + sql);
            }
            var async = (AsyncGroupByRecordCursorFactory) current;
            var atom = async.getAtom();
            var functions = atom.getOwnerGroupByFunctions();
            var valueTypes = new ArrayColumnTypes();
            for (int i = 0; i < functions.size(); i++) {
                valueTypes.add(ColumnType.LONG);
                if (functions.getQuick(i).getClass() == io.questdb.griffin.engine.functions.groupby.CountDistinctLongGroupByFunction.class) valueTypes.add(ColumnType.LONG);
            }
            var binding = ResolvedAggregateKernel.bind(async.getBaseFactory().getMetadata(), valueTypes, functions);
            if (binding == null) throw new IllegalStateException("Resolved functions unsupported");
            var expected = Demo.readSql(factory, context, 1);
            System.out.printf("CASE %s groups=%d hash=%d SQL=%s%n", name, expected.size(), expected.hashCode(), sql);
            try (var code = binding.compile(new LlvmBackend(), artifacts); var slot = code.newSlot(); var profile = new ProfileControl()) {
                System.out.printf(Locale.ROOT, "COMPILE %.3f ms%n", code.compilationNanos() / 1e6);
                String[] modes = forced.isEmpty() ? new String[]{"java", "llvm"} : new String[]{forced};
                double[][] times = new double[modes.length][repeats];
                var allocationBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
                long threadId = Thread.currentThread().threadId();
                for (int round = -warmups; round < repeats; round++) {
                    if (round == 0) profile.command("enable");
                    for (int order = 0; order < modes.length; order++) {
                        int mode = Math.floorMod(round + order, modes.length);
                        boolean nativeCode = modes[mode].equals("llvm");
                        long beforeAllocated = allocationBean.getThreadAllocatedBytes(threadId);
                        var before = ScanMatrix.Resources.read();
                        long cpuStart = allocationBean.getCurrentThreadCpuTime();
                        long started = System.nanoTime();
                        var actual = new ArrayList<CompiledGroupBy.ResultRow>();
                        long processed = 0;
                        try (var map = MapFactory.createUnorderedMap(config, new ArrayColumnTypes().add(ColumnType.LONG), valueTypes);
                             var frames = async.getBaseFactory().getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC);
                             var cache = new PageFrameAddressCache(); var pool = new PageFrameMemoryPool(config);
                             var batch = new DirectLongList(batchSize, MemoryTag.NATIVE_DEFAULT);
                             var record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                            cache.of(async.getBaseFactory().getMetadata(), frames.getColumnMapping(), frames.isExternal());
                            var counts = new ArrayList<Long>();
                            PageFrame frame;
                            int frameIndex = 0;
                            while ((frame = frames.next()) != null) {
                                cache.add(frameIndex++, frame);
                                counts.add(frame.getPartitionHi() - frame.getPartitionLo());
                            }
                            pool.of(cache);
                            var mapValue = new FlyweightPackedMapValue(valueTypes);
                            map.setBatchEmptyValue(atom.getFunctionUpdater(-1));
                            for (int f = 0; f < frameIndex; f++) {
                                record.init(pool.navigateTo(f));
                                long frameRows = counts.get(f);
                                for (long lo = 0; lo < frameRows; lo += batchSize) {
                                    long hi = Math.min(frameRows, lo + batchSize);
                                    map.reserveCapacity(hi - lo);
                                    long base = map.probeBatch(record, atom.getMapSink(-1), lo, hi, batch.getAddress());
                                    if (!nativeCode || !slot.update(record, base, batch.getAddress(), hi - lo)) {
                                        for (int a = 0; a < functions.size(); a++) {
                                            functions.getQuick(a).computeKeyedBatch(record, mapValue, base, batch.getAddress(), hi - lo, 0);
                                        }
                                    }
                                    processed += hi - lo;
                                }
                            }
                            try (var cursor = map.getCursor()) {
                                while (cursor.hasNext()) {
                                    var r = cursor.getRecord();
                                    long key = r.getLong(valueTypes.getColumnCount());
                                    var output = new ArrayList<Long>();
                                    for (int a = 0; a < functions.size(); a++) {
                                        long v = functions.getQuick(a).getLong(r);
                                        output.add(v == Long.MIN_VALUE ? null : v);
                                    }
                                    actual.add(new CompiledGroupBy.ResultRow(List.of(key), output));
                                }
                            }
                        }
                        double elapsed = (System.nanoTime() - started) / 1e6;
                        double cpuMillis = (allocationBean.getCurrentThreadCpuTime() - cpuStart) / 1e6;
                        var after = ScanMatrix.Resources.read();
                        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - beforeAllocated;
                        actual.sort(CompiledGroupBy.ResultRow::compareTo);
                        if (processed != rows || !actual.equals(expected)) throw new AssertionError(name + " / " + modes[mode] + " result or input count");
                        if (round >= 0) times[mode][round] = elapsed;
                        System.out.printf(Locale.ROOT, "%s %d %s ms=%.3f cpu-ms=%.3f allocated=%d major-faults=%d read-bytes=%d rows=%d%n",
                                round < 0 ? "warm" : "sample", round, modes[mode], elapsed, cpuMillis, allocated,
                                after.faults() - before.faults(), after.bytes() - before.bytes(), processed);
                    }
                }
                profile.command("disable");
                for (int i = 0; i < modes.length; i++) {
                    Arrays.sort(times[i]);
                    System.out.printf(Locale.ROOT, "RESULT %s %s median=%.3f range=[%.3f,%.3f]%n", name, modes[i], times[i][repeats / 2], times[i][0], times[i][repeats - 1]);
                }
            }
        }
    }

    private static void measureSql(CairoEngine engine, SqlExecutionContextImpl context, Path artifacts,
                                   String name, String sql, int workers, int warmups, int repeats, String forced) throws Exception {
        if (forced.equals("duckdb")) {
            DuckDbComparison.measure(engine, context, artifacts, name, sql, workers, warmups, repeats);
            return;
        }
        if (forced.startsWith("native-")) {
            CountSumPaths.measure(engine, context, artifacts, name, sql, workers, warmups, repeats, forced);
            return;
        }
        // A forced Java control never references the optional context methods. The same harness
        // can therefore measure the unmodified core binary as well as the disabled candidate.
        boolean compile = !forced.equals("java");
        long started = System.nanoTime();
        try (var baseline = Demo.compileSql(engine, context, sql, true)) {
            double javaPrepare = (System.nanoTime() - started) / 1e6;
            if (compile) Compilation.install(context, artifacts);
            started = System.nanoTime();
            try (var candidate = compile ? Demo.compileSql(engine, context, sql, true) : null;
                 var profile = new ProfileControl()) {
                double nativePrepare = (System.nanoTime() - started) / 1e6;
                if (compile) Compilation.clear(context);
                if (Boolean.getBoolean("query.codegen.topKPhase")) {
                    boolean javaMeasured = forced.isEmpty() || forced.equals("java");
                    var expected = TopKPhaseExperiment.run(baseline, context, javaMeasured ? warmups : 0,
                            javaMeasured ? repeats : 1, "java", javaMeasured,
                            () -> profile.commandUnchecked("enable"), () -> profile.commandUnchecked("disable"));
                    if (candidate != null) {
                        var actual = TopKPhaseExperiment.run(candidate, context, warmups, repeats, "llvm", true,
                                () -> profile.commandUnchecked("enable"), () -> profile.commandUnchecked("disable"));
                        if (!actual.equals(expected)) throw new AssertionError("Top-K phase mismatch");
                    }
                    return;
                }
                var expected = Demo.readSql(baseline, context, 1);
                System.out.printf(Locale.ROOT, "SQL CASE %s slots=%d groups=%d hash=%d prepare-java=%.3f prepare-native=%.3f sql=%s%n",
                        name, workers, expected.size(), expected.hashCode(), javaPrepare, nativePrepare, sql);
                var sink = new TextPlanSink();
                sink.of(candidate != null ? candidate : baseline, context);
                for (int i = 1; i <= sink.getLineCount(); i++) System.out.println(sink.getLine(i));
                String[] modes = forced.isEmpty() ? new String[]{"java", "llvm"} : new String[]{forced};
                String nativeLabel = Boolean.getBoolean("query.codegen.handwritten") ? "handwritten-c" : "llvm";
                double[][] times = new double[modes.length][repeats];
                var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                for (int round = -warmups; round < repeats; round++) {
                    for (int order = 0; order < modes.length; order++) {
                        int mode = Math.floorMod(round + order, modes.length);
                        var before = ScanMatrix.Resources.read();
                        long nativeRowsBefore = nativeRows(candidate, context.getSharedQueryWorkerCount());
                        long mapRowsBefore = candidate != null ? Compilation.mapRows(candidate, context.getSharedQueryWorkerCount()) : 0;
                        long[] counters = candidate != null ? Compilation.profileCounters(candidate, context.getSharedQueryWorkerCount()) : null;
                        long[] topKBefore = candidate != null ? Compilation.topKCounters(candidate, context.getSharedQueryWorkerCount()) : null;
                        long[] filterBefore = candidate != null ? Compilation.filterCounters(candidate, context.getSharedQueryWorkerCount()) : null;
                        long cpuStart = os.getProcessCpuTime();
                        var timing = new ExecutionTimings();
                        if (round >= 0) {
                            timing.profileStart = () -> profile.commandUnchecked("enable");
                            timing.profileEnd = () -> profile.commandUnchecked("disable");
                        }
                        var actual = Demo.readSql(modes[mode].equals("llvm") ? candidate : baseline, context, 1, timing);
                        long cpu = os.getProcessCpuTime() - cpuStart;
                        long nativeRows = nativeRows(candidate, context.getSharedQueryWorkerCount()) - nativeRowsBefore;
                        var after = ScanMatrix.Resources.read();
                        if (!actual.equals(expected)) throw new AssertionError(name + " / " + modes[mode] + " SQL mismatch: expected "
                                + expected.subList(0, Math.min(10, expected.size())) + " actual " + actual.subList(0, Math.min(10, actual.size())));
                        double elapsed = (timing.build + timing.copy + timing.release) / 1e6;
                        if (round >= 0) times[mode][round] = elapsed;
                        System.out.printf(Locale.ROOT, "%s %d %s ms=%.3f first-row-ms=%.3f consume-ms=%.3f close-ms=%.3f process-cpu-ms=%.3f major-faults=%d read-bytes=%d native-rows=%d shards=%d map-rows=%d",
                                round < 0 ? "warm" : "sample", round, modes[mode].equals("llvm") ? nativeLabel : modes[mode], elapsed, timing.build / 1e6,
                                timing.copy / 1e6, timing.release / 1e6, cpu / 1e6,
                                after.faults() - before.faults(), after.bytes() - before.bytes(), nativeRows, timing.shards, candidate != null ? Compilation.mapRows(candidate, context.getSharedQueryWorkerCount()) - mapRowsBefore : 0);
                        if (candidate != null && Boolean.getBoolean("query.codegen.profile")) {
                            long[] afterCounters = Compilation.profileCounters(candidate, context.getSharedQueryWorkerCount());
                            System.out.printf(" kernel-calls=%d new-groups=%d probe-slots=%d key-comparisons=%d",
                                    afterCounters[0] - counters[0], afterCounters[1] - counters[1],
                                    afterCounters[2] - counters[2], afterCounters[3] - counters[3]);
                        }
                        if (candidate != null) {
                            long[] topKAfter = Compilation.topKCounters(candidate, context.getSharedQueryWorkerCount());
                            System.out.printf(" top-k-rows=%d top-k-merge-rows=%d", topKAfter[0] - topKBefore[0], topKAfter[1] - topKBefore[1]);
                            long[] filterAfter = Compilation.filterCounters(candidate, context.getSharedQueryWorkerCount());
                            System.out.printf(" filter-input-rows=%d filter-selected-rows=%d", filterAfter[0] - filterBefore[0], filterAfter[1] - filterBefore[1]);
                        }
                        System.out.println();
                    }
                }
                for (int i = 0; i < modes.length; i++) {
                    Arrays.sort(times[i]);
                    System.out.printf(Locale.ROOT, "SQL RESULT %s %s slots=%d median=%.3f range=[%.3f,%.3f]%n",
                            name, modes[i].equals("llvm") ? nativeLabel : modes[i], workers, times[i][repeats / 2], times[i][0], times[i][repeats - 1]);
                }
            } finally {
                if (compile) Compilation.clear(context);
            }
        }
    }

    static WorkloadMatrix.Spec matchedSpec(String name, long rows, boolean integrated) {
        if (name.equals("previous-mixed") || name.equals("previous-distinct")) {
            String previousName = name.equals("previous-mixed") ? "mixed" : "nested-repeated";
            var previous = WorkloadMatrix.specs().stream().filter(s -> s.name().equals(previousName)).findFirst().orElseThrow();
            String ddl = Pattern.compile("long_sequence\\((\\d+)\\)").matcher(previous.ddl()).replaceAll("long_sequence(" + rows + ")");
            return new WorkloadMatrix.Spec(name, ddl, previous.sql(), previous.pipeline(), null, null);
        }
        if (!List.of("count", "mixed", "high", "distinct", "distinct-high", "selective", "skew").contains(name)) {
            throw new IllegalArgumentException("Case: " + name);
        }
        int filterThreshold = Integer.getInteger("query.codegen.filterThreshold", 100);
        long groups = Long.getLong("query.codegen.groups", name.equals("high") ? 131072L : name.startsWith("distinct") ? 64L : integrated ? 512L : 1024L);
        String values = name.equals("count") ? "count()" : name.startsWith("distinct") ? "count(),sum(v),count_distinct(v)" : "count(),count(v),sum(v),sum(w),sum(s)";
        String key = name.equals("skew") ? "case when x%10!=0 then 0 else x%" + groups + " end"
                : Boolean.getBoolean("query.codegen.permuteKeys") ? "((x*198491317)%4294967291)%" + groups : "x%" + groups;
        long keyStride = Long.getLong("query.codegen.keyStride", 1L);
        if (keyStride < 1 || groups > Long.MAX_VALUE / keyStride) throw new IllegalArgumentException("Invalid benchmark key stride");
        if (keyStride != 1) key = "(" + key + ")*" + keyStride;
        boolean composite = Boolean.getBoolean("query.codegen.compositeKey");
        String logicalKey = key;
        if (composite) key = "(" + key + ")/256";
        String keyNames = composite ? "k,g" : "k";
        String value = name.startsWith("distinct") ? "(x/" + groups + ")%" + (name.equals("distinct-high") ? 1024 : 8) : "x*3";
        String ddl = "create table input as (select " + key + " k,case when x%13=0 then null else " + value + " end v,"
                + "cast(case when x%17=0 then null else x%100003 end as int) w,cast(x%31-15 as short) s" + (composite ? ",cast((" + logicalKey + ")%256 as int) g" : "") + " from long_sequence(" + rows + "))";
        String sql = "select " + keyNames + "," + values + " from input where k>=0" + (name.equals("selective") ? " and w<" + filterThreshold : "") + " group by " + keyNames;
        var k = new Column(0, "k", ColumnType.LONG);
        var v = new Column(1, "v", ColumnType.LONG);
        var w = new Column(2, "w", ColumnType.INT);
        var s = new Column(3, "s", ColumnType.SHORT);
        Predicate filter = new SqlCompare(Ir.Op.GE, k, new Constant(0));
        if (name.equals("selective")) filter = new And(filter, new SqlCompare(Ir.Op.LT, w, new Constant(filterThreshold)));
        List<Aggregate> aggregates = name.equals("count") ? List.of(new Count())
                : name.startsWith("distinct") ? List.of(new Count(), new Sum(v), new NestedCountDistinct(v))
                : List.of(new Count(), new CountValue(v), new Sum(v), new Sum(w), new Sum(s));
        // Vary aggregate state width independently of key distribution/cardinality. This
        // is a workload parameter, not an engine eligibility or map-layout override.
        String aggregateSet = System.getProperty("query.codegen.aggregateSet");
        if (aggregateSet != null) {
            var selected = new ArrayList<Aggregate>();
            var expressions = new ArrayList<String>();
            for (String operation : aggregateSet.split(",")) {
                switch (operation) {
                    case "count" -> { selected.add(new Count()); expressions.add("count()"); }
                    case "sum" -> { selected.add(new Sum(v)); expressions.add("sum(v)"); }
                    case "distinct" -> { selected.add(new NestedCountDistinct(v)); expressions.add("count_distinct(v)"); }
                    default -> throw new IllegalArgumentException("Unknown aggregate: " + operation);
                }
            }
            aggregates = selected;
            sql = "select " + keyNames + "," + String.join(",", expressions) + " from input where k>=0"
                    + (name.equals("selective") ? " and w<" + filterThreshold : "") + " group by " + keyNames;
        }
        int topK = Integer.getInteger("query.codegen.topKLimit", 0);
        if (topK > 0) {
            String direction = System.getProperty("query.codegen.topKDirection", "desc");
            if (!direction.equals("asc") && !direction.equals("desc")) throw new IllegalArgumentException("Top-K direction");
            int column = Integer.getInteger("query.codegen.topKColumn", aggregates.size() + (composite ? 2 : 1));
            if (column < (composite ? 3 : 2) || column > aggregates.size() + (composite ? 2 : 1)) throw new IllegalArgumentException("Top-K column");
            sql += " order by " + column + " " + direction + " limit " + topK;
        }
        String varcharKind = System.getProperty("query.codegen.varcharKeys");
        if (varcharKind != null) {
            if (!integrated || composite) throw new IllegalArgumentException("VARCHAR fixture requires ordinary resolved SQL");
            String textKey = switch (varcharKind) {
                case "inline" -> "cast(" + logicalKey + " as varchar)";
                case "prefix" -> "cast('shared-prefix-abcdefghijkl-'||cast(" + logicalKey + " as string) as varchar)";
                case "mixed" -> "cast(case when x%3=0 then cast(" + logicalKey + " as string) "
                        + "when x%3=1 then 'shared-prefix-abcdefghijkl-'||cast(" + logicalKey + " as string) "
                        + "else 'žluťoučký-'||cast(" + logicalKey + " as string) end as varchar)";
                default -> throw new IllegalArgumentException("Unknown VARCHAR fixture: " + varcharKind);
            };
            String layout = System.getProperty("query.codegen.varcharLayout", "single");
            String additional = "", grouping = "k", extraAggregates = "";
            if (layout.equals("wide")) {
                extraAggregates = ",count(v)";
            } else if (layout.equals("composite") || layout.equals("strings")) {
                // Hold logical cardinality independently of the number and width of key fields.
                textKey = textKey.replace(logicalKey, "((" + logicalKey + ")%256)");
                String upper = "((" + logicalKey + ")/256)";
                additional = layout.equals("strings") ? ",cast('second-'||cast(" + upper + " as string) as varchar) g"
                        : ",cast(" + upper + " as int) g";
                grouping = "k,g";
            } else if (!layout.equals("single")) throw new IllegalArgumentException("Unknown VARCHAR layout: " + layout);
            textKey = "case when x%29=0 then null when x%31=0 then cast('' as varchar) else " + textKey + " end";
            ddl = "create table input as (select " + textKey + " k,x v" + additional + " from long_sequence(" + rows + "))";
            sql = "select " + grouping + ",count(),sum(v) total" + extraAggregates
                    + " from input where k is not null group by " + grouping + " order by total desc limit 10";
        }
        var g = new Column(4, "g", ColumnType.INT);
        return new WorkloadMatrix.Spec(name, ddl, sql, new Pipeline(composite ? List.of(k, v, w, s, g) : List.of(k, v, w, s),
                filter, composite ? List.of(k, g) : List.of(k), aggregates), null, null);
    }

    private static void measureThreeWay(CairoEngine engine, SqlExecutionContextImpl context, Path artifacts,
                                        WorkloadMatrix.Spec spec, long rows, int workers, int warmups, int repeats) throws Exception {
        try (var reader = engine.getReader("input")) {
            if (reader.size() != rows) throw new AssertionError("Actual input row count");
        }
        long started = System.nanoTime();
        try (var normal = Demo.compileSql(engine, context, spec.sql(), true)) {
            double normalPrepare = (System.nanoTime() - started) / 1e6;
            Compilation.install(context, artifacts);
            started = System.nanoTime();
            try (var integrated = Demo.compileSql(engine, context, spec.sql(), true)) {
                double integratedPrepare = (System.nanoTime() - started) / 1e6;
                Compilation.clear(context);
                started = System.nanoTime();
                try (var full = ParallelGroupBy.compile(engine, context, "input", spec.pipeline(), new LlvmBackend(), artifacts, workers)) {
                    double fullPrepare = (System.nanoTime() - started) / 1e6;
                    var expected = Demo.readSql(normal, context, spec.pipeline().keys().size());
                    long pairs = -1;
                    for (int a = 0; a < spec.pipeline().aggregates().size(); a++) {
                        if (spec.pipeline().aggregates().get(a) instanceof NestedCountDistinct) {
                            final int index = a;
                            pairs = expected.stream().mapToLong(row -> row.values().get(index)).sum();
                        }
                    }
                    System.out.printf(Locale.ROOT, "MATCHED CASE %s rows=%d groups=%d pairs=%d hash=%d prepare-normal=%.3f prepare-integrated=%.3f prepare-full=%.3f sql=%s%n",
                            spec.name(), rows, expected.size(), pairs, expected.hashCode(), normalPrepare, integratedPrepare, fullPrepare, spec.sql());
                    for (var factory : List.of(normal, integrated)) {
                        var sink = new TextPlanSink();
                        sink.of(factory, context);
                        for (int i = 1; i <= sink.getLineCount(); i++) System.out.println(sink.getLine(i));
                    }
                    String[] modes = {"normal", "integrated", "full"};
                    double[][] times = new double[3][repeats];
                    var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                    for (int round = -warmups; round < repeats; round++) {
                        // All six permutations, so each implementation occupies every position
                        // and follows every other implementation equally over a full cycle.
                        int rotation = Math.floorMod(round, 3);
                        int direction = Math.floorMod(round, 6) < 3 ? 1 : -1;
                        for (int order = 0; order < 3; order++) {
                            int mode = Math.floorMod(rotation + direction * order, 3);
                            var t = new ExecutionTimings();
                            var before = ScanMatrix.Resources.read();
                            long nativeBefore = nativeRows(integrated, context.getSharedQueryWorkerCount());
                            long cpuStart = os.getProcessCpuTime();
                            long start = System.nanoTime();
                            var actual = mode == 2 ? full.execute(context, CompiledGroupBy.Options.DEFAULT, t)
                                    : Demo.readSql(mode == 0 ? normal : integrated, context, spec.pipeline().keys().size(), t);
                            double ms = (System.nanoTime() - start - t.sort) / 1e6;
                            long cpu = os.getProcessCpuTime() - cpuStart;
                            long nativeUpdates = nativeRows(integrated, context.getSharedQueryWorkerCount()) - nativeBefore;
                            var after = ScanMatrix.Resources.read();
                            if (!actual.equals(expected)) throw new AssertionError(spec.name() + "/" + modes[mode] + " SQL mismatch");
                            long nativeInput = t.workerRows == null ? 0 : Arrays.stream(t.workerRows).sum();
                            if (mode == 2 && nativeInput != rows) throw new AssertionError("Native scan input row count");
                            if (round >= 0) times[mode][round] = ms;
                            System.out.printf(Locale.ROOT, "%s %d %s ms=%.3f build-ms=%.3f scan-ms=%.3f merge-ms=%.3f consume-ms=%.3f close-ms=%.3f cpu-ms=%.3f shards=%d native-input-rows=%d native-update-rows=%d peak-native-MiB=%.2f major-faults=%d read-bytes=%d%n",
                                    round < 0 ? "warm" : "sample", round, modes[mode], ms, t.build / 1e6,
                                    mode == 2 ? t.scan / 1e6 : Double.NaN, mode == 2 ? t.merge / 1e6 : Double.NaN,
                                    t.copy / 1e6, t.release / 1e6, cpu / 1e6, t.shards, nativeInput, mode == 2 ? -1 : nativeUpdates,
                                    mode == 2 ? t.peakBytes / 1048576.0 : Double.NaN,
                                    after.faults() - before.faults(), after.bytes() - before.bytes());
                        }
                    }
                    for (int i = 0; i < 3; i++) {
                        Arrays.sort(times[i]);
                        System.out.printf(Locale.ROOT, "MATCHED RESULT %s %s median=%.3f range=[%.3f,%.3f]%n",
                                spec.name(), modes[i], times[i][repeats / 2], times[i][0], times[i][repeats - 1]);
                    }
                }
            } finally {
                Compilation.clear(context);
            }
        }
    }

    static long nativeRows(RecordCursorFactory factory, int workerCount) {
        if (factory == null) return 0;
        return Compilation.nativeRows(factory, workerCount);
    }

    // Keep optional API types out of the enclosing class's verifier dependencies,
    // so the Java control runs against an entirely unmodified core classpath.
    private static final class Compilation {
        static void install(SqlExecutionContextImpl context, Path artifacts) {
            context.setQueryCodeGenerator(new ResolvedQueryCompiler(new LlvmBackend(), artifacts,
                    Boolean.getBoolean("query.codegen.handwritten")));
        }

        static void clear(SqlExecutionContextImpl context) {
            context.setQueryCodeGenerator(null);
        }

        static long[] profileCounters(RecordCursorFactory factory, int workerCount) {
            while (!(factory instanceof AsyncGroupByRecordCursorFactory)) {
                factory = factory.getBaseFactory();
                if (factory == null) return new long[4];
            }
            var atom = ((AsyncGroupByRecordCursorFactory) factory).getAtom();
            long[] total = new long[4];
            for (int slot = -1; slot < workerCount; slot++) {
                long[] part = ResolvedQueryCompiler.profileCounters(atom.getCompiledUpdater(slot));
                for (int i = 0; i < total.length; i++) total[i] += part[i];
            }
            return total;
        }

        static long[] filterCounters(RecordCursorFactory factory, int workerCount) {
            while (!(factory instanceof AsyncGroupByRecordCursorFactory)) {
                factory = factory.getBaseFactory();
                if (factory == null) return new long[2];
            }
            var atom = ((AsyncGroupByRecordCursorFactory) factory).getAtom();
            long[] total = new long[2];
            for (int slot = -1; slot < workerCount; slot++) {
                long[] part = ResolvedQueryCompiler.filterCounters(atom.getCompiledUpdater(slot));
                for (int i = 0; i < total.length; i++) total[i] += part[i];
            }
            return total;
        }

        static long[] topKCounters(RecordCursorFactory factory, int workerCount) {
            while (!(factory instanceof AsyncGroupByRecordCursorFactory)) {
                factory = factory.getBaseFactory();
                if (factory == null) return new long[2];
            }
            var atom = ((AsyncGroupByRecordCursorFactory) factory).getAtom();
            long[] total = new long[2];
            for (int slot = -1; slot < workerCount; slot++) {
                long[] part = ResolvedQueryCompiler.topKCounters(atom.getCompiledUpdater(slot));
                for (int i = 0; i < total.length; i++) total[i] += part[i];
            }
            return total;
        }

        static long mapRows(RecordCursorFactory factory, int workerCount) {
            while (!(factory instanceof AsyncGroupByRecordCursorFactory)) {
                factory = factory.getBaseFactory();
                if (factory == null) return 0;
            }
            var atom = ((AsyncGroupByRecordCursorFactory) factory).getAtom();
            long rows = 0;
            for (int slot = -1; slot < workerCount; slot++) {
                var updater = atom.getCompiledUpdater(slot);
                if (updater != null) rows += updater.getNativeMapRows();
            }
            return rows;
        }

        static long nativeRows(RecordCursorFactory factory, int workerCount) {
            while (!(factory instanceof AsyncGroupByRecordCursorFactory)) {
                factory = factory.getBaseFactory();
                if (factory == null) return 0;
            }
            var atom = ((AsyncGroupByRecordCursorFactory) factory).getAtom();
            long rows = 0;
            for (int slot = -1; slot < workerCount; slot++) rows += ResolvedQueryCompiler.updatedRows(atom.getCompiledUpdater(slot));
            return rows;
        }
    }

    /** Optional perf FIFO handshake: counters cover only complete measured queries. */
    static final class ProfileControl implements AutoCloseable {
        private java.io.BufferedWriter control;
        private java.io.BufferedReader acknowledgements;

        ProfileControl() throws Exception {
            String path = System.getenv("QUERY_CODEGEN_PERF_CONTROL");
            if (path != null) {
                control = Files.newBufferedWriter(Path.of(path));
                acknowledgements = Files.newBufferedReader(Path.of(System.getenv("QUERY_CODEGEN_PERF_ACK")));
            }
        }

        void commandUnchecked(String command) {
            try { command(command); } catch (Exception failure) { throw new RuntimeException(failure); }
        }

        void command(String command) throws Exception {
            if (control != null) {
                control.write(command);
                control.newLine();
                control.flush();
                if (acknowledgements.readLine() == null) throw new IllegalStateException("perf control closed");
            }
        }

        @Override
        public void close() throws Exception {
            try { if (control != null) control.close(); }
            finally { if (acknowledgements != null) acknowledgements.close(); }
        }
    }
}
