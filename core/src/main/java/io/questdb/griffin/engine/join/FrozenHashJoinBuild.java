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

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.QuietCloseable;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Immutable lookup backing, borrowed from its builder for one execution. Publish
 * this object through the frame-task publication barrier before probing. The owner
 * must drain all probes and finish reading output symbols before closing the build.
 * Handles expire at close. Payload columns and their symbol tables come from the build's
 * input through its {@link HashJoinPayloadSource}, so they stay valid only while that input
 * stays open, and the owner closes the input after the build. A later partitioned
 * implementation can route keys without changing this contract.
 * <p>
 * A build hands out probes through the keyed sub-interface that matches its lookup:
 * {@link IntKeyed} for a single INT key and {@link RecordKeyed} for a key that a
 * {@link RecordSink} stages from the probe record. The iteration half of a probe is
 * the same either way, so consumers that only walk matches hold the base
 * {@link Probe}.
 */
public interface FrozenHashJoinBuild {
    int LOOKUP_BYTES = 40;

    /** Conservative integral bounds; min > max means unavailable. */
    default long getMinIntegralKey() {
        return Long.MAX_VALUE;
    }

    default long getMaxIntegralKey() {
        return Long.MIN_VALUE;
    }

    /** Optional distinct integral-key iteration, before worker publication. */
    default boolean forEachIntegralKey(LongConsumer visitor, SqlExecutionCircuitBreaker breaker) {
        return false;
    }

    /** Number of NativeMapView descriptors required by a staged-key native lookup. */
    default int getNativeMapCount() {
        return 0;
    }

    /** Writes immutable map snapshots on the owner before probe publication. */
    default void writeNativeMapViews(long address) {
        throw new UnsupportedOperationException();
    }

    /** Five-word lookup header; mapViews is borrowed until all frame workers drain. */
    default boolean writeLookup(long address, long mapViews) {
        return false;
    }

    /** Borrowed payload descriptor, with the same lifetime as the frozen lookup. */
    default boolean writeNativePayload(int column, long address) {
        return false;
    }

    /**
     * Replaces each key's chain of rows with the key's row count, for a query that only counts the
     * joined pairs: a probe then adds the rows a key matches without visiting them, through
     * {@link IntProbe#findRowCountUnchecked(int)} or {@link RecordProbe#findRowCountUnchecked(Record)}.
     * The count runs in place over the row heap, in one pass over the build's rows, and allocates
     * nothing. The owner calls it at most once per execution, after the build froze and before it
     * publishes the build to the probes' threads. A parallel build may count ranges of its partitions
     * on the workers instead, through its builder; see
     * {@link IntHashJoinBuild#getKeyRowCountTaskCount()}. Afterwards the build keeps no chain to walk, so
     * its probes answer those two lookups alone: no other lookup, and no {@link Probe#next()}.
     * On failure all execution allocations are released.
     */
    void countKeyRows();

    /**
     * Optionally copies each heap row's chain length into caller-owned LONG storage of
     * {@code getRowCount() * 8} bytes, indexed by heap ordinal. Preserves the ordinary links
     * and payloads. Called on the owner before worker publication; polls the build's breaker.
     * Returns false when unsupported or when links have already been replaced by counts.
     */
    default boolean copyKeyRowCounts(long address) {
        return false;
    }

    /**
     * Optionally replaces caller-owned, nonnegative row weights with their chain sums,
     * indexed as in {@link #copyKeyRowCounts(long)}. Preserves this build's links and payloads.
     * Called before worker publication and polls the build's breaker. Returns false when
     * unsupported, already counted, or a sum exceeds Long.MAX_VALUE; the caller must then
     * discard the partially written weights. Throws if this frozen build has expired.
     */
    default boolean sumKeyRowWeights(long address) {
        return false;
    }

    long getKeyCount();

    long getRowCount();

    /**
     * Optional opaque handle for a frozen heap ordinal, suitable for {@link Probe#recordAt(long)}.
     * The ordinal order is the same as {@link #getRowId(long)}. Returns -1 when unsupported
     * or when the heap's chains have been replaced with counts. Handles expire with this build.
     */
    default long getRowHandle(long ordinal) {
        return -1;
    }

    /**
     * The id of the build row that the build appended ordinal-th, counting from zero. Rows keep
     * the order the build appended them in, which is the build input's order, so a pass over
     * every ordinal reads the input forward. Only a build with payload columns stores ids.
     */
    long getRowId(long ordinal);

    /**
     * Allocated native bytes, including unused capacity.
     */
    long getSizeInBytes();

    /**
     * A build whose probes look up a single INT key.
     */
    interface IntKeyed extends FrozenHashJoinBuild {
        @Override
        default long getMinIntegralKey() {
            return getMinKey();
        }

        @Override
        default long getMaxIntegralKey() {
            return getMaxKey();
        }

        @Override
        default boolean forEachIntegralKey(LongConsumer visitor, SqlExecutionCircuitBreaker breaker) {
            return forEachKey(key -> visitor.accept(key), breaker);
        }

        @Override
        default boolean writeLookup(long address, long mapViews) {
            return writeLookup(address);
        }

        /**
         * Conservative inclusive bounds on this execution's keys, including the INT null sentinel.
         * A minimum greater than the maximum means bounds are unavailable, as in the defaults.
         * Bounds have no meaning when {@link #getKeyCount()} is zero.
         */
        default int getMaxKey() {
            return Integer.MIN_VALUE;
        }

        /** @see #getMaxKey() */
        default int getMinKey() {
            return Integer.MAX_VALUE;
        }

        /**
         * Visits each distinct key, including null, on the owner before probe publication.
         * Optional: false means unsupported and must not invoke the visitor. The snapshot
         * remains pinned for the whole visit; cancellation also covers empty table slots.
         */
        default boolean forEachKey(IntConsumer visitor, SqlExecutionCircuitBreaker circuitBreaker) {
            return false;
        }

        /**
         * Optional native lookup snapshot: five eight-byte words containing serial key-table
         * address, slot mask, partition directory address (zero for serial), partition shift and
         * row-heap address. Directory entries hold table address and mask. Table slots are an INT
         * key and an unsigned compressed head (head * 8 is the biased row link). A row begins with
         * its next biased byte link; zero ends the chain. Hashing is Hash.hashInt64, as in the Java
         * probe. All backing is immutable and borrowed until this execution drains, before close.
         * Returns false for an unsupported/chain-counted snapshot, before the consumer mutates state.
         */
        default boolean writeLookup(long address) {
            return false;
        }

        /**
         * Borrows the payload view described by {@link HashJoinPayloadSource#writeNativeColumn}.
         * A supported heap has 16-byte rows: next link followed by row id. The ordinal is
         * the unbiased byte offset divided by 16. This view has the lookup snapshot's lifetime.
         */
        default boolean writeNativePayload(int column, long address) {
            return false;
        }

        /**
         * Each acquired execution slot needs its own probe. Probes do not consult a circuit
         * breaker; callers check cancellation at frame boundaries and on a work budget.
         */
        IntProbe newProbe();
    }

    /**
     * Lookups keyed by a single INT. SYMBOL keys arrive translated into the build's key
     * domain, so they take this shape too.
     */
    interface IntProbe extends Probe {
        /**
         * Replaces the current duplicate iterator, including on a miss.
         */
        void find(int key);

        /**
         * The rows the build holds for this key, zero for a key it lacks, using the lookup
         * metadata cached at reopen. Only a build that counted its key rows answers it; see
         * {@link FrozenHashJoinBuild#countKeyRows()}. Positions neither the duplicate iterator
         * nor the payload.
         */
        long findRowCountUnchecked(int key);

        /**
         * Positions the payload directly for a build with exactly one row per key.
         * The caller must establish rowCount == keyCount for this execution and
         * check cancellation at frame boundaries. Clears the duplicate iterator;
         * read the payload only when this returns true.
         */
        boolean findSingleUnchecked(int key);

        /**
         * Replaces the current duplicate iterator using the lookup metadata cached at reopen.
         * Read payload columns only after advancing a matching row.
         */
        void findUnchecked(int key);
    }

    /**
     * Walks the matches of one lookup and reads their payload. Every build implementation
     * shares this half; the keyed sub-interfaces add the lookup itself.
     * <p>
     * A probe may hold native memory of its own, so its owner closes it at the end of the
     * execution that charged it, before releasing that execution's memory tracker.
     * {@link #reopen()} brings a closed probe back for the next execution.
     */
    interface Probe extends SymbolTableSource, QuietCloseable {
        /**
         * Optional native next-row link: byte offset plus eight, zero at the chain end,
         * or -1 when unavailable. Borrows the same immutable heap as {@link FrozenHashJoinBuild#writeLookup(long, long)};
         * does not advance the iterator. Valid only until the frozen build closes.
         */
        default long getNextRowLink() {
            return -1;
        }

        /**
         * The payload columns of the last row that {@link #next()}, a unique lookup or
         * {@link #recordAt(long)} positioned; null for a build without payload columns.
         */
        Record getRecord();

        boolean hasNext();

        /**
         * Advances the payload record and returns an opaque, execution-local handle.
         */
        long next();

        /**
         * Positions the payload record without changing the duplicate iterator.
         */
        void recordAt(long handle);

        /**
         * Explicitly bind this slot-owned view to a refreshed snapshot, after consumer drain.
         * This also takes fresh symbol tables from the build's input, so call it on the owner.
         */
        void reopen();
    }

    /**
     * A build whose probes stage the key from a probe record through a {@link RecordSink}.
     */
    interface RecordKeyed extends FrozenHashJoinBuild {
        /**
         * Each acquired execution slot needs its own probe, and its own key sink: sinks hold
         * scratch state and must not be shared across workers. Probes do not consult a circuit
         * breaker; callers check cancellation at frame boundaries and on a work budget.
         */
        RecordProbe newProbe(RecordSink probeKeySink);
    }

    /**
     * Lookups keyed by the probe record, staged through the probe's own key sink. The key
     * columns of the probe record must match the build key's column order and types.
     */
    interface RecordProbe extends Probe {
        /**
         * Replaces the current duplicate iterator, including on a miss.
         */
        void find(Record probeRecord);

        /**
         * The rows the build holds for the probe record's key, zero for a key it lacks, using the
         * lookup metadata cached at reopen. Only a build that counted its key rows answers it;
         * see {@link FrozenHashJoinBuild#countKeyRows()}. Positions neither the duplicate
         * iterator nor the payload.
         */
        long findRowCountUnchecked(Record probeRecord);

        /**
         * Positions the payload directly for a build with exactly one row per key.
         * The caller must establish rowCount == keyCount for this execution and
         * check cancellation at frame boundaries. Clears the duplicate iterator;
         * read the payload only when this returns true.
         */
        boolean findSingleUnchecked(Record probeRecord);

        /**
         * Replaces the current duplicate iterator using the lookup metadata cached at reopen.
         * Read payload columns only after advancing a matching row.
         */
        void findUnchecked(Record probeRecord);
    }
}
