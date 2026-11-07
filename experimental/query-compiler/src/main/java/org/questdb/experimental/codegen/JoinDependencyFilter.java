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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.std.DirectLongList;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/** Owner-side dependency preparation over existing probes and duplicate chains. */
final class JoinDependencyFilter {
    private JoinDependencyFilter() { }

    static boolean affordable(FrozenHashJoinBuild producer, long probeRows, MemoryTracker tracker) {
        long keys = producer.getKeyCount();
        // The existing preparation policy allows one build row per 32 known probe rows.
        // Reserve one key word plus at most one filter word per key, including rounding.
        return keys > 0 && keys <= (Integer.MAX_VALUE - 31L) / Long.BYTES
                && probeRows > 0 && producer.getRowCount() <= probeRows / 32
                && (tracker == null || tracker.getLimit() <= 0
                    || keys * 16 + 32 <= tracker.getLimit() - tracker.getUsed());
    }

    static boolean prepare(FrozenHashJoinBuild producer, int[] types, ObjList<Lookup> children,
                           IntegralMembership result, MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        try (Lookup parent = new Lookup(producer, new int[types.length], types, null, null);
             DirectLongList keys = new DirectLongList(0, MemoryTag.NATIVE_JIT, true)) {
            keys.setMemoryTracker(tracker);
            keys.ensureCapacity(producer.getKeyCount());
            boolean visited;
            if (types.length == 1) {
                visited = producer.forEachIntegralKey(key -> {
                    parent.findKey(key);
                    if (parent.survives(children, breaker)) keys.add(key);
                }, breaker);
            } else {
                visited = producer.forEachRecordKey((record, column) -> {
                    for (int i = 0; i < types.length; i++) parent.values[i] = types[i] == ColumnType.INT || types[i] == ColumnType.SYMBOL
                            ? record.getInt(column + i) : record.getLong(column + i);
                    parent.findKey(parent.values[0]);
                    if (parent.survives(children, breaker)) keys.add(parent.membershipKey());
                }, breaker);
            }
            if (!visited || keys.size() == producer.getKeyCount()) return false;
            result.prepareReducedKeys(keys, tracker, breaker);
            return true;
        }
    }

    /** One compressed child-chain head per producer row, without materializing duplicate products. */
    static DirectLongList prepareLinks(FrozenHashJoinBuild producer, int[] producerTypes, Lookup child,
                                       long probeRows, MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        if (!affordableLinks(producer, probeRows, tracker)
                || producer.getRowHandle(0) < 0 || child.probe.getNextRowLink() < 0) return null;
        long rows = producer.getRowCount();
        long words = (rows + 1) / 2;
        DirectLongList links = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
        links.setMemoryTracker(tracker);
        try (Lookup parent = new Lookup(producer, new int[producerTypes.length], producerTypes, null, null)) {
            if (parent.probe.getRecord() == null) {
                links.close();
                return null;
            }
            links.ensureCapacity(words);
            for (long ordinal = 0; ordinal < rows; ordinal++) {
                breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                parent.probe.recordAt(producer.getRowHandle(ordinal));
                long head = child.find(parent.probe.getRecord()) ? child.probe.getNextRowLink() : 0;
                Unsafe.putInt(links.getAddress() + ordinal * Integer.BYTES, CompressedOffsets.compressAligned8(head));
            }
            return links;
        } catch (Throwable failure) {
            links.close();
            throw failure;
        }
    }

    static boolean affordableLinks(FrozenHashJoinBuild producer, long probeRows, MemoryTracker tracker) {
        long rows = producer.getRowCount();
        return rows > 0 && probeRows > 0 && rows <= probeRows / 32
                && (tracker == null || tracker.getLimit() <= 0
                    || ((rows + 1) / 2) * Long.BYTES <= tracker.getLimit() - tracker.getUsed());
    }

    /**
     * Folds a closed unobserved forest through its cached heads. Old counts/links remain usable
     * until every root succeeds; only then replace them with immutable root weights.
     */
    static boolean prepareSubtrees(FrozenHashJoinBuild[] builds, CompiledJoin.Build[] descriptions,
                                   int[] parents, DirectLongList[] counts, DirectLongList[] links,
                                   MemoryTracker tracker, SqlExecutionCircuitBreaker breaker) {
        DirectLongList[] weights = new DirectLongList[builds.length];
        try {
            for (int producer = builds.length - 1; producer >= 0; producer--) {
                boolean hasChildren = false;
                for (int child = producer + 1; child < builds.length; child++) {
                    if (parents[child] != producer) continue;
                    if (links[child] == null) return false;
                    hasChildren = true;
                }
                if (!hasChildren) {
                    if (counts[producer] == null && builds[producer].getRowCount() != builds[producer].getKeyCount()) return false;
                    continue;
                }
                long rows = builds[producer].getRowCount();
                if (tracker != null && tracker.getLimit() > 0
                        && rows * Long.BYTES > tracker.getLimit() - tracker.getUsed()) return false;
                DirectLongList result = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
                weights[producer] = result;
                result.setMemoryTracker(tracker);
                result.ensureCapacity(rows);
                for (long row = 0; row < rows; row++) {
                    breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    long weight = 1;
                    for (int child = producer + 1; child < builds.length; child++) {
                        if (parents[child] != producer) continue;
                        long head = Integer.toUnsignedLong(Unsafe.getInt(links[child].getAddress() + row * Integer.BYTES)) << 3;
                        if (head == 0) { weight = 0; break; }
                        DirectLongList childWeights = weights[child] != null ? weights[child] : counts[child];
                        if (childWeights != null) {
                            int shift = descriptions[child].payloadColumnCount() > 0 ? 4 : 3;
                            long childWeight = Unsafe.getLong(childWeights.getAddress() + (((head - 8) >> shift) << 3));
                            weight = Math.multiplyExact(weight, childWeight);
                            if (weight == 0) break;
                        }
                    }
                    Unsafe.putLong(result.getAddress() + row * Long.BYTES, weight);
                }
                if (!builds[producer].sumKeyRowWeights(result.getAddress())) return false;
                // Each child belongs to one producer. Its temporary weights are now folded in.
                for (int child = producer + 1; child < builds.length; child++) {
                    if (parents[child] == producer && weights[child] != null) {
                        weights[child].close();
                        weights[child] = null;
                    }
                }
            }
            for (int i = 0; i < builds.length; i++) {
                if (parents[i] >= 0 || weights[i] != null) {
                    if (counts[i] != null) counts[i].close();
                    counts[i] = weights[i];
                    weights[i] = null;
                }
                if (links[i] != null) { links[i].close(); links[i] = null; }
            }
            return true;
        } catch (ArithmeticException overflow) {
            // Zero must mean no joined row. Keep the ordinary wrapping COUNT path on overflow.
            return false;
        } finally {
            for (DirectLongList weight : weights) if (weight != null) weight.close();
        }
    }

    /** Replaying aggregate updates requires a representable positive run length. */
    static boolean repetitionsFit(FrozenHashJoinBuild[] builds, DirectLongList[] counts, SqlExecutionCircuitBreaker breaker) {
        long product = 1;
        try {
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] == null) continue;
                long maximum = 1;
                for (long row = 0, n = builds[i].getRowCount(); row < n; row++) {
                    if ((row & 1023) == 0) breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    long count = counts[i].get(row);
                    if (count < 0) return false;
                    maximum = Math.max(maximum, count);
                }
                product = Math.multiplyExact(product, maximum);
            }
            return true;
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    static final class Lookup implements AutoCloseable {
        private final int[] columns;
        private final int[] types;
        private final SymbolKeyTranslator.View[] translators;
        private final long[] values;
        private final IntegralMembership membership;
        private final FrozenHashJoinBuild.Probe probe;

        Lookup(FrozenHashJoinBuild build, int[] columns, int[] types,
               SymbolKeyTranslator.View[] translators, IntegralMembership membership) {
            this.columns = columns;
            this.types = types;
            this.translators = translators;
            this.membership = membership;
            values = new long[types.length];
            if (build instanceof FrozenHashJoinBuild.IntKeyed narrow) {
                probe = narrow.newProbe();
            } else {
                probe = ((FrozenHashJoinBuild.RecordKeyed) build).newProbe(new RecordSink() {
                    @Override public void copy(Record record, RecordSinkSPI sink) {
                        for (int i = 0; i < values.length; i++) {
                            if (types[i] == ColumnType.INT || types[i] == ColumnType.SYMBOL) sink.putInt((int) values[i]);
                            else sink.putLong(values[i]);
                        }
                    }
                    @Override public void setFunctions(ObjList<Function> functions) { }
                });
            }
        }

        boolean find(Record row) {
            for (int i = 0; i < types.length; i++) {
                long value;
                if (types[i] == ColumnType.SYMBOL) {
                    value = translators[i].translate(row.getInt(columns[i]));
                    if (value == SymbolTable.VALUE_NOT_FOUND) return false;
                } else if (types[i] == ColumnType.INT) value = row.getInt(columns[i]);
                else value = row.getLong(columns[i]);
                values[i] = value;
            }
            if (membership != null && !membership.isUniversal() && !membership.contains(membershipKey())) return false;
            findKey(values[0]);
            return probe.hasNext();
        }

        private long membershipKey() {
            if (values.length == 1) return values[0];
            long hash = 0;
            for (long value : values) hash = Hash.hashLong128_64(hash, value);
            return hash;
        }

        private boolean survives(ObjList<Lookup> children, SqlExecutionCircuitBreaker breaker) {
            while (probe.hasNext()) {
                breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                probe.next();
                Record row = probe.getRecord();
                boolean survives = true;
                for (int i = 0; i < children.size(); i++) {
                    if (!children.getQuick(i).find(row)) {
                        survives = false;
                        break;
                    }
                }
                if (survives) return true; // All children must match the same producer duplicate.
            }
            return false;
        }

        private void findKey(long key) {
            values[0] = key;
            if (probe instanceof FrozenHashJoinBuild.IntProbe narrow) narrow.findUnchecked((int) key);
            else ((FrozenHashJoinBuild.RecordProbe) probe).findUnchecked(null);
        }

        @Override public void close() { probe.close(); }
    }
}
