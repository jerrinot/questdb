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

import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;

/** Query-scoped bridge to QuestDB's allocator. Native callbacks must never unwind into C. */
public final class KernelMemory implements AutoCloseable {
    private static final ThreadLocal<KernelMemory> CURRENT = new ThreadLocal<>();
    private final GroupByAllocator allocator;
    private KernelMemory previous;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean bound;
    private Runnable phaseCheckpoint;
    private Throwable failure;
    private final Runnable checkpoint;
    private final boolean ownsAllocator;

    private KernelMemory(GroupByAllocator allocator) {
        this(allocator, () -> { });
    }

    private KernelMemory(GroupByAllocator allocator, Runnable checkpoint) {
        this(allocator, checkpoint, true);
    }

    private KernelMemory(GroupByAllocator allocator, Runnable checkpoint, boolean ownsAllocator) {
        this(allocator, checkpoint, ownsAllocator, true);
    }

    private KernelMemory(GroupByAllocator allocator, Runnable checkpoint, boolean ownsAllocator, boolean attach) {
        this.allocator = allocator;
        this.checkpoint = checkpoint;
        this.ownsAllocator = ownsAllocator;
        if (attach) enter(null);
    }

    /** Slot-owned bridge reused for batches; retains no execution breaker between calls. */
    static KernelMemory reusable(GroupByAllocator allocator) {
        return new KernelMemory(allocator, null, false, false);
    }

    static KernelMemory reusable(Runnable checkpoint) {
        return new KernelMemory(null, checkpoint, false, false);
    }

    void enter(SqlExecutionCircuitBreaker breaker) {
        if (bound) throw new IllegalStateException("Native allocation scope is already bound");
        previous = CURRENT.get();
        circuitBreaker = breaker;
        failure = null;
        bound = true;
        CURRENT.set(this);
    }

    void enterCheckpoint(Runnable checkpoint) {
        enter(null);
        phaseCheckpoint = checkpoint;
    }

    private void checkpoint() {
        // Native allocation/poll callbacks can occur once per group. Check cancellation and
        // timeout every time, but share the breaker's wall-clock throttle for socket syscalls.
        if (circuitBreaker != null) circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        else if (phaseCheckpoint != null) phaseCheckpoint.run();
        else if (checkpoint != null) checkpoint.run();
    }

    static KernelMemory open(GroupByAllocator allocator) { return new KernelMemory(allocator); }
    // A worker binds its allocator only while running. The query closes it after all workers join.
    static KernelMemory bind(GroupByAllocator allocator, Runnable checkpoint) {
        return new KernelMemory(allocator, checkpoint, false);
    }
    static KernelMemory open(SqlExecutionContext context, long budget) {
        return new KernelMemory(new QueryAllocator(context, budget),
                context.getCircuitBreaker()::statefulThrowExceptionIfTrippedTimeThrottled);
    }

    static void checkFailure() {
        KernelMemory current = CURRENT.get();
        if (current != null) { current.throwIfFailed(); }
    }

    long peakBytes() { return allocator instanceof QueryAllocator q ? q.peak() : allocator.allocated(); }

    public static long poll() {
        KernelMemory memory = CURRENT.get();
        if (memory == null) { return 1; }
        if (memory.failure != null) { return 1; }
        try { memory.checkpoint(); return 0; }
        catch (Throwable failure) { memory.failure = failure; return 1; }
    }

    static GroupByAllocator allocator() {
        KernelMemory memory = CURRENT.get();
        if (memory == null) { throw new IllegalStateException("No query allocator is bound"); }
        return memory.allocator;
    }

    public static long malloc(long bytes) {
        KernelMemory memory = CURRENT.get();
        if (memory == null || memory.failure != null) { return 0; }
        try {
            memory.checkpoint();
            if (bytes <= 0) { throw new IllegalArgumentException("Invalid allocation size: " + bytes); }
            return memory.allocator.malloc(bytes);
        } catch (Throwable failure) {
            memory.failure = failure;
            return 0;
        }
    }

    public static void free(long address, long bytes) {
        KernelMemory memory = CURRENT.get();
        if (memory == null || memory.failure != null) { return; }
        try {
            memory.allocator.free(address, bytes);
        } catch (Throwable failure) {
            memory.failure = failure;
        }
    }

    void throwIfFailed() {
        if (failure instanceof Error e) { throw e; }
        if (failure instanceof RuntimeException e) { throw e; }
        if (failure != null) { throw new IllegalStateException("Query allocation failed", failure); }
    }

    @Override
    public void close() {
        try {
            if (ownsAllocator) { allocator.close(); }
        } finally {
            if (bound) {
                // Retain a null-valued entry: remove/set would allocate an Entry per native batch.
                CURRENT.set(previous);
                previous = null;
                circuitBreaker = null;
                phaseCheckpoint = null;
                failure = null;
                bound = false;
            }
        }
    }
}
