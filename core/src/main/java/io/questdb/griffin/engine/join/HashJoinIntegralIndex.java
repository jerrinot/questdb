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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import java.util.function.LongConsumer;

/** A bounded replacement for an integral build's key tables; row heaps stay unchanged. */
final class HashJoinIntegralIndex implements QuietCloseable {
    private static final long MAX_BYTES = 1L << 48;
    private final HashJoinBuffer heads = new HashJoinBuffer(MAX_BYTES);
    private long first;
    private long last = -1;
    private int nullHead;
    private long nullKey;
    private boolean ready;

    @Override
    public void close() {
        ready = false;
        heads.close();
        first = 0;
        last = -1;
        nullHead = 0;
    }

    int find(long key) {
        if (key == nullKey) {
            return nullHead;
        }
        return key >= first && key <= last ? Unsafe.getInt(heads.address + (key - first) * Integer.BYTES) : 0;
    }

    void forEachKey(LongConsumer visitor, SqlExecutionCircuitBreaker breaker) {
        if (nullHead != 0) {
            visitor.accept(nullKey);
        }
        long span = last - first + 1;
        for (long i = 0; i < span; i++) {
            if ((i & 1023) == 0) {
                breaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
            if (Unsafe.getInt(heads.address + i * Integer.BYTES) != 0) {
                visitor.accept(first + i);
            }
        }
    }

    long getMaximum() {
        return nullHead != 0 ? Math.max(last, nullKey) : last;
    }

    long getMinimum() {
        return nullHead != 0 ? Math.min(first, nullKey) : first;
    }

    long getSizeInBytes() {
        return heads.capacity;
    }

    boolean isReady() {
        return ready;
    }

    boolean prepare(
            FrozenHashJoinBuild build,
            long probeRows,
            long nullKey,
            long replacedBytes,
            MemoryTracker tracker,
            SqlExecutionCircuitBreaker breaker,
            Runnable copyHeads
    ) {
        if (ready) {
            return false;
        }
        long keys = build.getKeyCount();
        // Bound construction by known probe work, and keep all byte arithmetic representable.
        if (keys <= 0 || probeRows <= 0 || keys > probeRows / 32) {
            return false;
        }
        this.nullKey = nullKey;
        first = build.getMinIntegralKey();
        last = build.getMaxIntegralKey();
        if (first > last || first <= nullKey && nullKey <= last) {
            first = Long.MAX_VALUE;
            last = Long.MIN_VALUE;
            if (!build.forEachIntegralKey(key -> {
                if (key != nullKey) {
                    first = Math.min(first, key);
                    last = Math.max(last, key);
                }
            }, breaker)) {
                return false;
            }
        }
        if (first > last) {
            return false;
        }
        long span = last - first + 1;
        // At most eight bytes per distinct key, including holes. Wrapped ranges decline.
        if (span <= 0 || span > keys * 2) {
            return false;
        }
        long bytes = ((span + 1) / 2) * Long.BYTES;
        if (bytes > MAX_BYTES || bytes > replacedBytes
                || tracker != null && tracker.getLimit() > 0 && bytes > tracker.getLimit() - tracker.getUsed()) {
            return false;
        }
        heads.of(tracker, breaker);
        try {
            heads.allocate(bytes, true);
            // The build copies occupied entries directly; looking their keys up again
            // adds hashing and dependent loads without discovering any new information.
            copyHeads.run();
            // Until this point iteration and lookups must still use the original tables.
            ready = true;
            return true;
        } catch (Throwable failure) {
            close();
            throw failure;
        }
    }

    /** Copies an occupied entry while prepare owns both representations. */
    void put(long key, int compressedHead) {
        assert !ready && compressedHead != 0;
        if (key == nullKey) {
            nullHead = compressedHead;
        } else {
            assert key >= first && key <= last;
            Unsafe.putInt(heads.address + (key - first) * Integer.BYTES, compressedHead);
        }
    }

    boolean write(long address, long heap) {
        if (!ready) {
            return false;
        }
        Unsafe.putLong(address, heads.address);
        Unsafe.putLong(address + 8, first);
        Unsafe.putLong(address + 16, last);
        Unsafe.putLong(address + 24, CompressedOffsets.uncompressAligned8(nullHead));
        Unsafe.putLong(address + 32, heap);
        return true;
    }
}
