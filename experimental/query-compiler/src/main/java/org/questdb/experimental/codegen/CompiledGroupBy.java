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
import io.questdb.cairo.sql.DataSource;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Experimental entry point. Owns a normal QuestDB projection factory and compiled
 * aggregate kernel. Page frames stay pinned throughout each call; no row copying.
 * Instances and execution contexts must not be shared between concurrent calls.
 */
public final class CompiledGroupBy implements AutoCloseable {
    private static final int ROWS_PER_CALL = 65_536;
    private final CompiledKernel kernel;
    private final CompiledKernel finish;
    private final Pipeline pipeline;
    private RecordCursorFactory scan;

    private CompiledGroupBy(Pipeline pipeline, RecordCursorFactory scan, CompiledKernel kernel, CompiledKernel finish) {
        this.pipeline = pipeline;
        this.scan = scan;
        this.kernel = kernel;
        this.finish = finish;
    }

    public static CompiledGroupBy compile(
            CairoEngine engine,
            SqlExecutionContext context,
            String table,
            Pipeline pipeline,
            Backend backend,
            Path artifacts
    ) throws SqlException, IOException {
        RecordCursorFactory scan = openScan(engine, context, table, pipeline);
        CompiledKernel kernel = null;
        try {
            kernel = pipeline.compileScan(backend, artifacts, false, false);
            CompiledKernel finish = pipeline.flatDistinct() ? backend.compile(pipeline.lowerFinish(), artifacts) : null;
            return new CompiledGroupBy(pipeline, scan, kernel, finish);
        } catch (IOException | RuntimeException | Error e) {
            if (kernel != null) { kernel.close(); }
            scan.close();
            throw e;
        }
    }

    static CompiledGroupBy withKernels(CairoEngine engine, SqlExecutionContext context, String table,
                                      Pipeline pipeline, Kernel scanKernel, Kernel finishKernel) throws SqlException {
        if (pipeline.flatDistinct() != (finishKernel != null)) {
            throw new IllegalArgumentException("Flat DISTINCT requires a collapse kernel");
        }
        return new CompiledGroupBy(pipeline, openScan(engine, context, table, pipeline),
                CompiledKernel.handwritten(scanKernel), finishKernel == null ? null : CompiledKernel.handwritten(finishKernel));
    }

    static RecordCursorFactory openScan(CairoEngine engine, SqlExecutionContext context, String table,
                                                Pipeline pipeline) throws SqlException {
        return openScan(engine, context, table, pipeline.columns());
    }

    static RecordCursorFactory openScan(CairoEngine engine, SqlExecutionContext context, String table,
                                       List<Pipeline.Column> columns) throws SqlException {
        // Construct the scan ourselves: accepting an arbitrary factory could bypass
        // filters whose row selections are not represented by raw page frames.
        var token = engine.getTableTokenIfExists(table);
        if (token != null && token.isView()) {
            throw new UnsupportedPlan("Prototype scans physical tables, not SQL views");
        }
        String projection = String.join(",", columns.stream().map(c -> quote(c.name())).toList());
        RecordCursorFactory scan;
        try (var compiler = engine.getSqlCompiler()) {
            scan = compiler.compile("select " + projection + " from " + quote(table), context).getRecordCursorFactory();
        }
        try {
            if (!columns.equals(Pipeline.schema(scan.getMetadata()))) {
                throw new IllegalArgumentException("Pipeline schema does not match the table projection");
            }
            if (!scan.supportsPageFrameCursor() || !scan.producesMaterializedPageFrames()) {
                throw new UnsupportedPlan("Prototype requires directly readable table page frames");
            }
            return scan;
        } catch (RuntimeException | Error e) {
            scan.close();
            throw e;
        }
    }

    public Path artifacts() { return kernel.artifacts(); }
    public long compilationNanos() { return kernel.compilationNanos() + (finish == null ? 0 : finish.compilationNanos()); }

    public record Options(int initialCapacity, int maxCapacity, long memoryLimit) {
        public static final Options DEFAULT = new Options(64, 1 << 26, 512L << 20);
        public Options {
            TableState.checkCapacity(initialCapacity);
            TableState.checkCapacity(maxCapacity);
            if (initialCapacity > maxCapacity || memoryLimit < 1) { throw new IllegalArgumentException("Invalid execution limits"); }
        }
    }

    /** Cardinality-independent execution: tables grow while retaining the compiled kernel. */
    public List<ResultRow> execute(SqlExecutionContext context) throws SqlException {
        return execute(context, Options.DEFAULT);
    }

    public List<ResultRow> execute(SqlExecutionContext context, Options options) throws SqlException {
        return execute(context, options, null);
    }

    List<ResultRow> execute(SqlExecutionContext context, Options options, ExecutionTimings timings) throws SqlException {
        return execute(context, options.initialCapacity(), options.initialCapacity(), options.maxCapacity(),
                options.maxCapacity(), options.memoryLimit(), timings, true);
    }

    // SQL imposes no output order for supported plans. Avoid the DSL's comparison sort.
    List<ResultRow> executeUnordered(SqlExecutionContext context) throws SqlException {
        Options options = Options.DEFAULT;
        return execute(context, options.initialCapacity(), options.initialCapacity(), options.maxCapacity(),
                options.maxCapacity(), options.memoryLimit(), null, false);
    }

    /** A fresh zeroed table for each execution; capacity is explicit and never silently truncated. */
    public List<ResultRow> execute(SqlExecutionContext context, int capacity) throws SqlException {
        return execute(context, capacity, capacity);
    }

    /** DISTINCT uses a separate capacity for unique pairs, including null-marker pairs. */
    public List<ResultRow> execute(SqlExecutionContext context, int capacity, int pairCapacity) throws SqlException {
        return execute(context, capacity, pairCapacity, null);
    }

    List<ResultRow> execute(SqlExecutionContext context, int capacity, int pairCapacity, ExecutionTimings timings) throws SqlException {
        return execute(context, capacity, pairCapacity, capacity, pairCapacity, Options.DEFAULT.memoryLimit(), timings, true);
    }

    private List<ResultRow> execute(SqlExecutionContext context, int capacity, int pairCapacity,
                                   int maxCapacity, int maxPairCapacity, long memoryLimit, ExecutionTimings timings, boolean canonicalOrder) throws SqlException {
        long started = timings == null ? 0 : System.nanoTime();
        long releaseStarted = 0;
        if (timings != null) { timings.reset(); }
        if (scan == null) {
            throw new IllegalStateException("Compiled query is closed");
        }
        checkCapacity(capacity);
        if (pipeline.flatDistinct()) { checkCapacity(pairCapacity); }
        context.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
        try (Arena arena = Arena.ofConfined();
             KernelMemory memory = KernelMemory.open(context, memoryLimit);
             var frames = scan.getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC)) {
            long state = TableState.create(arena, capacity, maxCapacity, pipeline.entryBytes());
            long inputState = state;
            if (pipeline.flatDistinct()) {
                inputState = TableState.create(arena, pairCapacity, maxPairCapacity, pipeline.inputEntryBytes());
            }
            long columns = arena.allocate(pipeline.columns().size() * 8L, 8).address();
            PageFrame frame;
            while ((frame = frames.next()) != null) {
                if (frame.getFormat() != PartitionFormat.NATIVE) {
                    throw new UnsupportedPlan("Prototype does not yet decode Parquet page frames");
                }
                long rows = frame.getPartitionHi() - frame.getPartitionLo();
                if (rows < 0) {
                    throw new IllegalStateException("Negative page-frame row count");
                }
                for (Pipeline.Column column : pipeline.columns()) {
                    int i = column.index();
                    if (frame.getColumnSource(i) != DataSource.DIRECT) {
                        throw new UnsupportedPlan("Prototype does not yet decode covered columns");
                    }
                    if (frame.getPageAddress(i) != 0
                            && frame.getPageSize(i) < Math.multiplyExact(rows, column.width().bytes)) {
                        throw new IllegalStateException("Column page is shorter than the frame");
                    }
                }
                for (long offset = 0; offset < rows; offset += ROWS_PER_CALL) {
                    context.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    long count = Math.min(ROWS_PER_CALL, rows - offset);
                    for (Pipeline.Column column : pipeline.columns()) {
                        long base = frame.getPageAddress(column.index());
                        long address = base == 0 ? 0 : base + offset * column.width().bytes;
                        Unsafe.getUnsafe().putLong(columns + column.index() * 8L, address);
                    }
                    long status = kernel.run(columns, count, inputState);
                    memory.throwIfFailed();
                    if (status == 2) { throw new IllegalStateException("Query allocation failed"); }
                    if (status == 3) { throw new IllegalStateException("Distinct set capacity overflow or corrupt set"); }
                    if (status == 4) { throw new IllegalStateException("Kernel checkpoint failed"); }
                    if (status != 0) {
                        throw tableFull(pipeline.flatDistinct() ? "Distinct pair" : "Group", TableState.capacity(inputState));
                    }
                }
            }
            if (finish != null) {
                long actualPairCapacity = TableState.capacity(inputState);
                for (long offset = 0; offset < actualPairCapacity; offset += ROWS_PER_CALL) {
                    context.getCircuitBreaker().statefulThrowExceptionIfTrippedNoThrottle();
                    long input = TableState.entries(inputState) + offset * pipeline.inputEntryBytes();
                    long status = finish.run(input, Math.min(ROWS_PER_CALL, actualPairCapacity - offset), state);
                    memory.throwIfFailed();
                    if (status != 0) {
                        if (status != 1) { throw new IllegalStateException("Collapse kernel failed: " + status); }
                        throw tableFull("Group", TableState.capacity(state));
                    }
                }
            }
            if (timings != null) {
                timings.build = System.nanoTime() - started;
                timings.peakBytes = memory.peakBytes();
                timings.grows = Unsafe.getUnsafe().getLong(state + TableState.GROWS)
                        + (state == inputState ? 0 : Unsafe.getUnsafe().getLong(inputState + TableState.GROWS));
            }
            var result = readResult(pipeline, state, TableState.capacity(state), timings, canonicalOrder);
            if (timings != null) { releaseStarted = System.nanoTime(); }
            return result;
        } finally {
            if (timings != null && releaseStarted != 0) { timings.release = System.nanoTime() - releaseStarted; }
        }
    }

    @Override
    public void close() {
        if (scan != null) {
            try {
                try {
                    kernel.close();
                } finally {
                    if (finish != null) { finish.close(); }
                }
            } finally {
                scan.close();
                scan = null;
            }
        }
    }

    public record ResultRow(List<Long> keys, List<Long> values) implements Comparable<ResultRow> {
        public ResultRow {
            keys = Collections.unmodifiableList(new ArrayList<>(keys));
            values = Collections.unmodifiableList(new ArrayList<>(values));
        }

        @Override
        public int compareTo(ResultRow other) {
            Comparator<Long> comparator = Comparator.nullsFirst(Long::compare);
            for (int i = 0; i < Math.min(keys.size(), other.keys.size()); i++) {
                int cmp = comparator.compare(keys.get(i), other.keys.get(i));
                if (cmp != 0) { return cmp; }
            }
            return Integer.compare(keys.size(), other.keys.size());
        }
    }

    static List<ResultRow> readResult(Pipeline pipeline, long state, int capacity, ExecutionTimings timings, boolean canonicalOrder) {
        long started = timings == null ? 0 : System.nanoTime();
        List<ResultRow> result = new ArrayList<>();
        long entryBytes = pipeline.entryBytes();
        long entries = TableState.entries(state);
        long[] offsets = new long[pipeline.aggregates().size()];
        for (int i = 0; i < offsets.length; i++) { offsets[i] = pipeline.aggregateOffset(i); }
        for (int slot = 0; slot < capacity; slot++) {
            long entry = entries + slot * entryBytes;
            if ((slot & 1023) == 0) { KernelMemory.poll(); KernelMemory.checkFailure(); }
            if (Unsafe.getUnsafe().getLong(entry) == 0) { continue; }
            List<Long> keys = new ArrayList<>();
            for (int i = 0; i < pipeline.keys().size(); i++) {
                keys.add(nullable(Unsafe.getUnsafe().getLong(entry + 8L * (i + 1))));
            }
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < pipeline.aggregates().size(); i++) {
                values.add(nullable(Unsafe.getUnsafe().getLong(entry + offsets[i])));
            }
            result.add(new ResultRow(keys, values));
        }
        if (result.isEmpty() && pipeline.keys().isEmpty()) {
            result.add(new ResultRow(List.of(), pipeline.aggregates().stream()
                    .map(a -> nullable(a.initialValue())).toList()));
        }
        if (timings != null) {
            timings.copy = System.nanoTime() - started;
            timings.inspect(result);
            started = System.nanoTime();
        }
        if (canonicalOrder) { Collections.sort(result); }
        if (timings != null) { timings.sort = System.nanoTime() - started; started = System.nanoTime(); }
        var immutable = List.copyOf(result);
        if (timings != null) { timings.copy += System.nanoTime() - started; }
        return immutable;
    }

    private static Long nullable(long value) { return value == Long.MIN_VALUE ? null : value; }

    private static void checkCapacity(int capacity) {
        if (capacity < 1 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Table capacity must be a positive power of two");
        }
    }

    private static IllegalStateException tableFull(String table, int capacity) {
        return new IllegalStateException(table + " table is full (capacity=" + capacity
                + "); rerun with a larger capacity. No partial result was returned.");
    }

    private static String quote(String name) {
        if (name.isEmpty() || name.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Invalid SQL identifier");
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }
}
