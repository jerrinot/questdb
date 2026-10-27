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
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

/** Generates the Java aggregation algorithms over the prototype's existing table layout.
 * Sharding/thresholds follow GroupByShardingContext and GroupByMapFragment; aggregate
 * merges follow the concrete GroupByFunctions. The executor is an isolated test harness,
 * not a replacement for QuestDB's page-frame dispatch or query worker pool.
 */
public final class ParallelGroupBy implements AutoCloseable {
    private static final int ROWS_PER_CALL = 65_536;
    private final Pipeline pipeline;
    private final int workers;
    private final CompiledKernel kernel;
    private final CompiledKernel shardedKernel;
    private final CompiledKernel shard;
    private final CompiledKernel merge;
    private final ExecutorService executor;
    private RecordCursorFactory scan;
    private boolean shardedHint;

    private ParallelGroupBy(Pipeline pipeline, int workers, RecordCursorFactory scan,
                            CompiledKernel kernel, CompiledKernel merge, CompiledKernel shard, CompiledKernel shardedKernel) {
        this.pipeline = pipeline;
        this.workers = workers;
        this.scan = scan;
        this.kernel = kernel;
        this.merge = merge;
        this.shard = shard;
        this.shardedKernel = shardedKernel;
        executor = Executors.newFixedThreadPool(workers,
                Thread.ofPlatform().daemon().name("query-codegen-", 0).factory());
    }

    public static ParallelGroupBy compile(CairoEngine engine, SqlExecutionContext context, String table,
                                          Pipeline pipeline, Backend backend, Path artifacts, int workers)
            throws SqlException, IOException {
        if (workers < 1 || workers > 64) { throw new IllegalArgumentException("Workers must be in [1,64]"); }
        if (pipeline.flatDistinct()) { throw new UnsupportedPlan("Parallel mode follows Java's nested DISTINCT algorithm"); }
        RecordCursorFactory scan = CompiledGroupBy.openScan(engine, context, table, pipeline);
        CompiledKernel kernel = null, merge = null, shard = null, shardedKernel = null;
        try {
            kernel = pipeline.compileScan(backend, artifacts, false, true);
            merge = backend.compile(pipeline.lowerMerge(), artifacts);
            shard = backend.compile(pipeline.lowerShard(), artifacts);
            shardedKernel = pipeline.compileScan(backend, artifacts, true, true);
            return new ParallelGroupBy(pipeline, workers, scan, kernel, merge, shard, shardedKernel);
        } catch (IOException | RuntimeException | Error failure) {
            for (AutoCloseable resource : new AutoCloseable[]{shardedKernel, shard, merge, kernel, scan}) {
                if (resource != null) {
                    try { resource.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                }
            }
            throw failure;
        }
    }

    public long compilationNanos() {
        return kernel.compilationNanos() + merge.compilationNanos() + shard.compilationNanos() + shardedKernel.compilationNanos();
    }

    public List<CompiledGroupBy.ResultRow> execute(SqlExecutionContext context) throws SqlException {
        return execute(context, CompiledGroupBy.Options.DEFAULT);
    }

    public List<CompiledGroupBy.ResultRow> execute(SqlExecutionContext context, CompiledGroupBy.Options options) throws SqlException {
        return execute(context, options, null);
    }

    synchronized List<CompiledGroupBy.ResultRow> execute(SqlExecutionContext context, CompiledGroupBy.Options options,
                                                       ExecutionTimings timings) throws SqlException {
        if (scan == null) { throw new IllegalStateException("Compiled query is closed"); }
        if (timings != null) { timings.reset(); timings.workerRows = new long[workers]; }
        long started = System.nanoTime(), releaseStarted = 0;
        var failure = new AtomicReference<Throwable>();
        Runnable coordinatorCheckpoint = context.getCircuitBreaker()::statefulThrowExceptionIfTrippedNoThrottle;
        Runnable workerCheckpoint = () -> {
            if (failure.get() != null) { throw new CancellationException("Another query task failed"); }
        };
        coordinatorCheckpoint.run();
        int threshold = context.getCairoEngine().getConfiguration().getGroupByShardingThreshold();
        try (var memory = new ParallelMemory(context, options.memoryLimit(), workers + 1);
             var frames = scan.getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC)) {
            var fragments = new ParallelFragment[workers];
            for (int w = 0; w < workers; w++) {
                try (var binding = KernelMemory.bind(memory.allocator(w), coordinatorCheckpoint)) {
                    fragments[w] = new ParallelFragment(pipeline, memory.arena, options);
                }
            }
            boolean sharded = shardedHint;
            if (sharded) { shardFragments(fragments, memory, workerCheckpoint, coordinatorCheckpoint, failure); }
            memory.observe();
            PageFrame frame;
            while ((frame = frames.next()) != null) {
                coordinatorCheckpoint.run();
                long rows = frame.getPartitionHi() - frame.getPartitionLo();
                long[] addresses = addresses(frame, rows);
                CompiledKernel scanCode = sharded ? shardedKernel : kernel;
                scanCode.runBatch(rows, selectedCode -> runTasks(workers, worker -> {
                    try (var binding = KernelMemory.bind(memory.allocator(worker), workerCheckpoint)) {
                        var fragment = fragments[worker];
                        long lo = boundary(rows, worker, workers), hi = boundary(rows, worker + 1, workers);
                        for (long offset = lo; offset < hi; offset += ROWS_PER_CALL) {
                            workerCheckpoint.run();
                            long count = Math.min(ROWS_PER_CALL, hi - offset);
                            for (Pipeline.Column column : pipeline.columns()) {
                                long base = addresses[column.index()];
                                put(fragment.columns + column.index() * 8L, base == 0 ? 0 : base + offset * column.width().bytes);
                            }
                            invoke(selectedCode, fragment.columns, count, fragment.root, binding);
                            if (timings != null) { timings.workerRows[worker] += count; }
                        }
                    }
                }, coordinatorCheckpoint, failure));
                // Like maybeEnableSharding: map cardinality OR aggregate cardinality.
                // The barrier also pins this frame until every native call has exited.
                if (!sharded) {
                    for (var fragment : fragments) {
                        if (fragment.groups() > threshold || fragment.cardinality() > 10L * threshold) { sharded = true; break; }
                    }
                    if (sharded) { shardFragments(fragments, memory, workerCheckpoint, coordinatorCheckpoint, failure); }
                }
                memory.observe();
            }
            long scanDone = System.nanoTime();
            ParallelFragment destination;
            try (var binding = KernelMemory.bind(memory.allocator(workers), coordinatorCheckpoint)) {
                destination = new ParallelFragment(pipeline, memory.arena, options);
                if (sharded) { destination.openShards(); }
            }
            if (sharded) {
                // Same independent shard jobs and per-slot allocator ownership as mergeShards.
                var slots = new PerWorkerLocks(context.getCairoEngine().getConfiguration(), workers);
                runTasks(GroupByMapFragment.NUM_SHARDS, shardIndex -> {
                    workerCheckpoint.run();
                    int slot = slots.acquireSlot(shardIndex % workers, () -> failure.get() != null);
                    try (var binding = KernelMemory.bind(memory.allocator(slot), workerCheckpoint)) {
                        for (var fragment : fragments) {
                            runTable(merge, fragment.shards[shardIndex], pipeline.entryBytes(),
                                    destination.shards[shardIndex], binding, workerCheckpoint);
                        }
                    } finally { slots.releaseSlot(slot); }
                }, coordinatorCheckpoint, failure);
            } else {
                try (var binding = KernelMemory.bind(memory.allocator(workers), coordinatorCheckpoint)) {
                    for (var fragment : fragments) {
                        runTable(merge, fragment.root, pipeline.entryBytes(), destination.root, binding, coordinatorCheckpoint);
                    }
                }
            }
            memory.observe();
            long cardinality = 0;
            for (var fragment : fragments) { cardinality += fragment.cardinality(); }
            shardedHint = destination.groups() > threshold || cardinality > 10L * threshold;
            if (timings != null) {
                long done = System.nanoTime();
                timings.scan = scanDone - started;
                timings.merge = done - scanDone;
                timings.build = done - started;
                timings.peakBytes = memory.peak();
                timings.shards = sharded ? GroupByMapFragment.NUM_SHARDS : 0;
                timings.grows = destination.grows();
                for (var fragment : fragments) { timings.grows += fragment.grows(); }
            }
            try (var binding = KernelMemory.bind(memory.allocator(workers), coordinatorCheckpoint)) {
                long copyStarted = System.nanoTime();
                var result = new ArrayList<CompiledGroupBy.ResultRow>();
                long[] outputs = sharded ? destination.shards : new long[]{destination.root};
                for (long output : outputs) {
                    if (get(output + 8) != 0) {
                        result.addAll(CompiledGroupBy.readResult(pipeline, output, TableState.capacity(output), null, false));
                    }
                }
                if (result.isEmpty() && pipeline.keys().isEmpty()) {
                    result.addAll(CompiledGroupBy.readResult(pipeline, outputs[0], TableState.capacity(outputs[0]), null, false));
                }
                if (timings != null) { timings.copy = System.nanoTime() - copyStarted; timings.inspect(result); }
                long sortStarted = System.nanoTime();
                Collections.sort(result);
                if (timings != null) { timings.sort = System.nanoTime() - sortStarted; }
                var immutable = List.copyOf(result);
                binding.throwIfFailed();
                releaseStarted = System.nanoTime();
                return immutable;
            }
        } finally {
            if (timings != null && releaseStarted != 0) { timings.release = System.nanoTime() - releaseStarted; }
        }
    }

    private void shardFragments(ParallelFragment[] fragments, ParallelMemory memory, Runnable workerCheckpoint,
                                Runnable coordinatorCheckpoint, AtomicReference<Throwable> failure) {
        runTasks(workers, worker -> {
            try (var binding = KernelMemory.bind(memory.allocator(worker), workerCheckpoint)) {
                var fragment = fragments[worker];
                fragment.openShards();
                runTable(shard, fragment.root, pipeline.entryBytes(), fragment.root, binding, workerCheckpoint);
                KernelMemory.free(TableState.entries(fragment.root), TableState.capacity(fragment.root) * pipeline.entryBytes());
                binding.throwIfFailed();
                put(fragment.root + TableState.ENTRIES, 0);
            }
        }, coordinatorCheckpoint, failure);
    }

    private void runTasks(int count, IntConsumer body, Runnable checkpoint, AtomicReference<Throwable> failure) {
        var tasks = new ArrayList<Future<?>>(count);
        try {
            for (int task = 0; task < count; task++) {
                final int index = task;
                tasks.add(executor.submit(() -> {
                    try { body.accept(index); }
                    catch (Throwable error) { failure.compareAndSet(null, error); throw error; }
                    finally { MemoryTracker.detachResourceMemoryCurrentThread(); }
                }));
            }
        } catch (Throwable error) { failure.compareAndSet(null, error); }
        await(tasks, checkpoint, failure);
    }

    private static void runTable(CompiledKernel code, long source, long stride, long destination,
                                 KernelMemory binding, Runnable checkpoint) {
        long capacity = TableState.capacity(source);
        for (long offset = 0; offset < capacity; offset += ROWS_PER_CALL) {
            checkpoint.run();
            invoke(code, TableState.entries(source) + offset * stride,
                    Math.min(ROWS_PER_CALL, capacity - offset), destination, binding);
        }
    }

    private long[] addresses(PageFrame frame, long rows) {
        if (frame.getFormat() != PartitionFormat.NATIVE) { throw new UnsupportedPlan("Prototype does not decode Parquet page frames"); }
        if (rows < 0) { throw new IllegalStateException("Negative page-frame row count"); }
        long[] addresses = new long[pipeline.columns().size()];
        for (Pipeline.Column column : pipeline.columns()) {
            int index = column.index();
            if (frame.getColumnSource(index) != DataSource.DIRECT) { throw new UnsupportedPlan("Prototype does not decode covered columns"); }
            long address = frame.getPageAddress(index);
            if (address != 0 && frame.getPageSize(index) < Math.multiplyExact(rows, column.width().bytes)) {
                throw new IllegalStateException("Column page is shorter than the frame");
            }
            addresses[index] = address;
        }
        return addresses;
    }

    private static long boundary(long rows, int worker, int workers) {
        return (rows / workers) * worker + Math.min(worker, rows % workers);
    }
    private static long get(long address) { return Unsafe.getUnsafe().getLong(address); }
    private static void put(long address, long value) { Unsafe.getUnsafe().putLong(address, value); }

    private static void invoke(Kernel code, long input, long rows, long state, KernelMemory memory) {
        long status = code.run(input, rows, state);
        memory.throwIfFailed();
        if (status != 0) { throw new IllegalStateException("Parallel kernel failed, status=" + status); }
    }

    private static void await(List<Future<?>> tasks, Runnable checkpoint, AtomicReference<Throwable> failure) {
        boolean interrupted = false;
        for (Future<?> task : tasks) {
            boolean complete = false;
            while (!complete) {
                if (Thread.interrupted()) {
                    interrupted = true;
                    failure.compareAndSet(null, new InterruptedException("Query coordinator interrupted"));
                }
                if (failure.get() == null) {
                    try { checkpoint.run(); } catch (Throwable error) { failure.compareAndSet(null, error); }
                }
                try { task.get(1, TimeUnit.MILLISECONDS); complete = true; }
                catch (TimeoutException ignored) { }
                catch (InterruptedException error) { interrupted = true; failure.compareAndSet(null, error); }
                catch (ExecutionException error) { failure.compareAndSet(null, error.getCause()); complete = true; }
            }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
        Throwable error = failure.get();
        if (error instanceof Error e) { throw e; }
        if (error instanceof RuntimeException e) { throw e; }
        if (error != null) { throw new IllegalStateException("Parallel execution interrupted", error); }
    }

    @Override
    public synchronized void close() {
        if (scan == null) { return; }
        executor.close();
        Throwable failure = null;
        for (AutoCloseable resource : new AutoCloseable[]{shardedKernel, shard, merge, kernel, scan}) {
            try { resource.close(); }
            catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } }
        }
        scan = null;
        if (failure instanceof Error e) { throw e; }
        if (failure != null) { throw new IllegalStateException("Could not close parallel query", failure); }
    }
}
