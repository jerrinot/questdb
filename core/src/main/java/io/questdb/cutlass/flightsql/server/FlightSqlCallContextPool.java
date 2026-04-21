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

package io.questdb.cutlass.flightsql.server;

import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.io.Closeable;

/**
 * Fixed-capacity pool of {@link FlightSqlCallContext} instances, sized
 * to the H2 engine's {@code ourMaxConcurrentStreams} so every active
 * stream is guaranteed a context. Acquire / release is by stream id;
 * the pool maintains a sparse array-backed map from {@code streamId} to
 * the bound context. Wave 5 uses a linear scan for the map — at typical
 * concurrency (≤ 100 streams) this is cheaper than a hash and stays
 * branch-predictable. A higher concurrency tier can later upgrade to a
 * primitive-keyed hash without changing the public surface.
 */
public final class FlightSqlCallContextPool implements Closeable {

    private final ObjList<FlightSqlCallContext> contexts;
    private final int[] streamIdByIndex;
    private int activeCount;
    private boolean isClosed;

    public FlightSqlCallContextPool(int capacity, int maxMessageBytes, int memoryTag) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.contexts = new ObjList<>(capacity);
        this.streamIdByIndex = new int[capacity];
        for (int i = 0; i < capacity; i++) {
            contexts.add(new FlightSqlCallContext(maxMessageBytes, memoryTag));
            streamIdByIndex[i] = 0;
        }
    }

    /**
     * Binds a free context to {@code streamId}. Returns {@code null} if
     * the pool is exhausted or if {@code streamId} is already bound.
     */
    public FlightSqlCallContext acquire(int streamId) {
        if (streamId <= 0) {
            throw new IllegalArgumentException("streamId must be positive: " + streamId);
        }
        // Duplicate bind is a programming error; the listener pairs
        // acquire with onStreamClosed / release and never sees the same
        // stream id twice.
        for (int i = 0, n = contexts.size(); i < n; i++) {
            if (streamIdByIndex[i] == streamId) {
                return null;
            }
        }
        for (int i = 0, n = contexts.size(); i < n; i++) {
            if (streamIdByIndex[i] == 0) {
                streamIdByIndex[i] = streamId;
                activeCount++;
                return contexts.getQuick(i);
            }
        }
        return null;
    }

    public int capacity() {
        return contexts.size();
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        Misc.freeObjListAndClear(contexts);
    }

    public int getActiveCount() {
        return activeCount;
    }

    public FlightSqlCallContext lookup(int streamId) {
        if (streamId <= 0) {
            return null;
        }
        for (int i = 0, n = contexts.size(); i < n; i++) {
            if (streamIdByIndex[i] == streamId) {
                return contexts.getQuick(i);
            }
        }
        return null;
    }

    /**
     * Releases the context bound to {@code streamId}. No-op if the
     * stream is not currently bound (the listener releases idempotently
     * from {@code onStreamClosed}).
     */
    public void release(int streamId) {
        if (streamId <= 0) {
            return;
        }
        for (int i = 0, n = contexts.size(); i < n; i++) {
            if (streamIdByIndex[i] == streamId) {
                streamIdByIndex[i] = 0;
                activeCount--;
                contexts.getQuick(i).release();
                return;
            }
        }
    }
}
