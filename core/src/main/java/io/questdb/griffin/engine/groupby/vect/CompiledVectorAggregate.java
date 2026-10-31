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

package io.questdb.griffin.engine.groupby.vect;

import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;

/** Optional fused aggregation. The vector factory owns worker slots and finalizes exported Rosti groups. */
public interface CompiledVectorAggregate extends QuietCloseable {
    /** Bind the current reader snapshot; rowCount may be -1. False retains ordinary vector aggregation. */
    boolean open(int symbolCount, long rowCount, MemoryTracker memoryTracker);

    /** Called under the existing exclusive worker-slot lock. */
    void aggregate(PageFrameMemory frame, long rows, int slot, ExecutionCircuitBreaker circuitBreaker);

    /** All scans have drained. Merge private arrays, then publish final groups into empty Rosti. */
    boolean mergeAndExport(long rosti, ExecutionCircuitBreaker circuitBreaker);

    /** Release execution memory after all workers have joined; compiled code remains reusable. */
    void clear();

    CharSequence getName();

    long getNativeRows();
}
