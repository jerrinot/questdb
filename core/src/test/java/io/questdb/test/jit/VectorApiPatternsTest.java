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

package io.questdb.test.jit;

import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Exploratory tests for Java Vector API patterns that will be used
 * when generating vectorized bytecode from the lowered IR.
 * <p>
 * Each test exercises a specific pattern in isolation so we understand
 * the exact API calls and method signatures the bytecode generator
 * will need to emit.
 */
public class VectorApiPatternsTest {

    private static final ValueLayout.OfInt NATIVE_INT = ValueLayout.JAVA_INT.withOrder(ByteOrder.nativeOrder());
    private static final ValueLayout.OfLong NATIVE_LONG = ValueLayout.JAVA_LONG.withOrder(ByteOrder.nativeOrder());
    private static final ValueLayout.OfDouble NATIVE_DOUBLE = ValueLayout.JAVA_DOUBLE.withOrder(ByteOrder.nativeOrder());

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Float> FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

    // Off-heap memory for test data
    private long intColumnAddr;
    private long longColumnAddr;
    private long doubleColumnAddr;
    private long outputAddr;
    private int rowCount;
    private long outputSize;

    @Before
    public void setUp() {
        rowCount = LONG_SPECIES.length() * 3 + 2; // 3 full vectors + partial tail
        intColumnAddr = Unsafe.malloc(rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        longColumnAddr = Unsafe.malloc(rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        doubleColumnAddr = Unsafe.malloc(rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        // Output needs room for a full vector write past the last match
        outputSize = (rowCount + LONG_SPECIES.length()) * Long.BYTES;
        outputAddr = Unsafe.malloc(outputSize, MemoryTag.NATIVE_DEFAULT);

        for (int i = 0; i < rowCount; i++) {
            Unsafe.getUnsafe().putInt(intColumnAddr + (long) i * Integer.BYTES, i);
            Unsafe.getUnsafe().putLong(longColumnAddr + (long) i * Long.BYTES, i * 10L);
            Unsafe.getUnsafe().putDouble(doubleColumnAddr + (long) i * Double.BYTES, i * 0.1);
        }
    }

    @After
    public void tearDown() {
        Unsafe.free(intColumnAddr, rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(longColumnAddr, rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(doubleColumnAddr, rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(outputAddr, outputSize, MemoryTag.NATIVE_DEFAULT);
    }

    // ==================== Pattern 1: Load from raw address ====================

    @Test
    public void testLoadLongVectorFromRawAddress() {
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());

        // First vector should contain [0, 10, 20, 30, ...] for SPECIES_LENGTH lanes
        for (int i = 0; i < LONG_SPECIES.length(); i++) {
            Assert.assertEquals(i * 10L, v.lane(i));
        }
    }

    @Test
    public void testLoadIntVectorFromRawAddress() {
        MemorySegment seg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);

        IntVector v = IntVector.fromMemorySegment(INT_SPECIES, seg, 0, ByteOrder.nativeOrder());

        for (int i = 0; i < INT_SPECIES.length(); i++) {
            Assert.assertEquals(i, v.lane(i));
        }
    }

    // ==================== Pattern 2: Compare → mask ====================

    @Test
    public void testLongCompareGt() {
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        // Compare: value > 25  (rows 3+ should match since values are 0, 10, 20, 30, ...)
        VectorMask<Long> mask = v.compare(VectorOperators.GT, 25L);

        for (int i = 0; i < LONG_SPECIES.length(); i++) {
            boolean expected = (i * 10L) > 25L;
            Assert.assertEquals("lane " + i, expected, mask.laneIsSet(i));
        }
    }

    @Test
    public void testLongCompareBroadcast() {
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        // Compare against broadcast vector (equivalent to scalar compare)
        LongVector threshold = LongVector.broadcast(LONG_SPECIES, 25L);
        VectorMask<Long> mask = v.compare(VectorOperators.GT, threshold);

        Assert.assertEquals(v.compare(VectorOperators.GT, 25L), mask);
    }

    // ==================== Pattern 3: Mask boolean ops ====================

    @Test
    public void testMaskAndOr() {
        MemorySegment longSeg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, longSeg, 0, ByteOrder.nativeOrder());

        // Simulate: l > 15 AND l < 45
        VectorMask<Long> gt15 = v.compare(VectorOperators.GT, 15L);
        VectorMask<Long> lt45 = v.compare(VectorOperators.LT, 45L);
        VectorMask<Long> combined = gt15.and(lt45);

        // Values: 0, 10, 20, 30, 40, 50, ...
        // > 15 AND < 45: rows 2 (20), 3 (30), 4 (40) match
        for (int i = 0; i < LONG_SPECIES.length(); i++) {
            long val = i * 10L;
            boolean expected = val > 15 && val < 45;
            Assert.assertEquals("lane " + i + " (val=" + val + ")", expected, combined.laneIsSet(i));
        }
    }

    @Test
    public void testMaskNot() {
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        VectorMask<Long> eq0 = v.compare(VectorOperators.EQ, 0L);
        VectorMask<Long> ne0 = eq0.not();

        // Only lane 0 has value 0
        Assert.assertTrue(eq0.laneIsSet(0));
        Assert.assertFalse(ne0.laneIsSet(0));
        if (LONG_SPECIES.length() > 1) {
            Assert.assertFalse(eq0.laneIsSet(1));
            Assert.assertTrue(ne0.laneIsSet(1));
        }
    }

    // ==================== Pattern 4: Count-only — trueCount() ====================

    @Test
    public void testMaskTrueCount() {
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        long totalCount = 0;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> activeMask = LONG_SPECIES.indexInRange(row, rowCount);
            LongVector v = LongVector.fromMemorySegment(
                    LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder(), activeMask
            );
            VectorMask<Long> match = v.compare(VectorOperators.GT, 25L, activeMask);
            totalCount += match.trueCount();
        }

        // Values: 0, 10, 20, 30, 40, ... up to (rowCount-1)*10
        // Count of values > 25: rows 3, 4, 5, ... rowCount-1
        long expected = Math.max(0, rowCount - 3);
        Assert.assertEquals(expected, totalCount);
    }

    // ==================== Pattern 5: Row-ID extraction (lane-by-lane) ====================

    @Test
    public void testRowIdExtractionLaneByLane() {
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);
        MemorySegment out = MemorySegment.ofAddress(outputAddr)
                .reinterpret(outputSize);

        long filteredCount = 0;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> activeMask = LONG_SPECIES.indexInRange(row, rowCount);
            LongVector v = LongVector.fromMemorySegment(
                    LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder(), activeMask
            );
            VectorMask<Long> match = v.compare(VectorOperators.GT, 25L, activeMask);

            // Lane-by-lane extraction — this is what the current executor does
            for (int lane = 0; lane < LONG_SPECIES.length(); lane++) {
                if (match.laneIsSet(lane)) {
                    out.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                }
            }
        }

        long expected = Math.max(0, rowCount - 3);
        Assert.assertEquals(expected, filteredCount);

        // Verify extracted row IDs are correct
        for (long i = 0; i < filteredCount; i++) {
            long rowId = out.getAtIndex(NATIVE_LONG, i);
            long value = rowId * 10L;
            Assert.assertTrue("row " + rowId + " value " + value + " should be > 25", value > 25);
        }
    }

    // ==================== Pattern 6: Tail handling with indexInRange ====================

    @Test
    public void testTailHandlingIndexInRange() {
        // rowCount is deliberately not a multiple of SPECIES.length()
        Assert.assertNotEquals(0, rowCount % LONG_SPECIES.length());

        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        int iterations = 0;
        long lastActiveCount = -1;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> activeMask = LONG_SPECIES.indexInRange(row, rowCount);
            iterations++;

            if (row + LONG_SPECIES.length() > rowCount) {
                // This is the tail iteration — mask should be partial
                lastActiveCount = activeMask.trueCount();
                Assert.assertTrue(lastActiveCount < LONG_SPECIES.length());
                Assert.assertEquals(rowCount - row, lastActiveCount);
            } else {
                // Full iteration — all lanes active
                Assert.assertEquals(LONG_SPECIES.length(), activeMask.trueCount());
            }

            // Masked load — safe even for tail
            LongVector v = LongVector.fromMemorySegment(
                    LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder(), activeMask
            );
            // Inactive lanes are zero
            for (int lane = 0; lane < LONG_SPECIES.length(); lane++) {
                if (!activeMask.laneIsSet(lane)) {
                    Assert.assertEquals("inactive lane should be 0", 0L, v.lane(lane));
                }
            }
        }
        // We should have had at least one partial tail iteration
        Assert.assertTrue(lastActiveCount > 0);
        Assert.assertEquals(4, iterations); // 3 full + 1 partial
    }

    // ==================== Pattern 7: Int→Long promotion ====================

    @Test
    public void testIntToLongPromotion() {
        // When comparing int and long columns, we need to widen int→long.
        // Vector API provides castShape() / convertShape() for this.
        MemorySegment intSeg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);

        IntVector iv = IntVector.fromMemorySegment(INT_SPECIES, intSeg, 0, ByteOrder.nativeOrder());

        // Convert int vector to long vector — this may change the number of lanes.
        // If IntVector has 8 lanes and LongVector has 4 lanes, we get 2 long vectors.
        // The safe approach: convert with part index.
        int intLanes = INT_SPECIES.length();
        int longLanes = LONG_SPECIES.length();

        if (intLanes >= longLanes) {
            // Common case: int vector has more or equal lanes.
            // Extract the first longLanes worth of ints → longs
            LongVector lv = (LongVector) iv.castShape(LONG_SPECIES, 0);
            for (int i = 0; i < longLanes; i++) {
                Assert.assertEquals((long) iv.lane(i), lv.lane(i));
            }

            if (intLanes > longLanes) {
                // Second part
                LongVector lv2 = (LongVector) iv.castShape(LONG_SPECIES, 1);
                for (int i = 0; i < Math.min(longLanes, intLanes - longLanes); i++) {
                    Assert.assertEquals((long) iv.lane(longLanes + i), lv2.lane(i));
                }
            }
        }
    }

    // ==================== Pattern 8: Null sentinel detection ====================

    @Test
    public void testIntNullSentinelDetection() {
        // Write some NULL sentinels into the int column
        Unsafe.getUnsafe().putInt(intColumnAddr + 2L * Integer.BYTES, Numbers.INT_NULL);
        Unsafe.getUnsafe().putInt(intColumnAddr + 5L * Integer.BYTES, Numbers.INT_NULL);

        MemorySegment seg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);

        IntVector v = IntVector.fromMemorySegment(INT_SPECIES, seg, 0, ByteOrder.nativeOrder());
        IntVector nullSentinel = IntVector.broadcast(INT_SPECIES, Numbers.INT_NULL);
        VectorMask<Integer> isNull = v.compare(VectorOperators.EQ, nullSentinel);

        // Lane 2 and 5 (if within species length) should be null
        if (INT_SPECIES.length() > 2) {
            Assert.assertTrue("lane 2 should be null", isNull.laneIsSet(2));
        }
        if (INT_SPECIES.length() > 5) {
            Assert.assertTrue("lane 5 should be null", isNull.laneIsSet(5));
        }
        Assert.assertFalse("lane 0 should not be null", isNull.laneIsSet(0));
        Assert.assertFalse("lane 1 should not be null", isNull.laneIsSet(1));
    }

    @Test
    public void testLongNullSentinelDetection() {
        Unsafe.getUnsafe().putLong(longColumnAddr + 1L * Long.BYTES, Numbers.LONG_NULL);

        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        LongVector nullSentinel = LongVector.broadcast(LONG_SPECIES, Numbers.LONG_NULL);
        VectorMask<Long> isNull = v.compare(VectorOperators.EQ, nullSentinel);

        Assert.assertFalse(isNull.laneIsSet(0));
        Assert.assertTrue(isNull.laneIsSet(1));
    }

    @Test
    public void testDoubleNanDetection() {
        Unsafe.getUnsafe().putDouble(doubleColumnAddr + 3L * Double.BYTES, Double.NaN);

        MemorySegment seg = MemorySegment.ofAddress(doubleColumnAddr)
                .reinterpret((long) rowCount * Double.BYTES);

        DoubleVector v = DoubleVector.fromMemorySegment(DOUBLE_SPECIES, seg, 0, ByteOrder.nativeOrder());
        VectorMask<Double> isNan = v.test(VectorOperators.IS_NAN);

        Assert.assertFalse(isNan.laneIsSet(0));
        if (DOUBLE_SPECIES.length() > 3) {
            Assert.assertTrue("lane 3 should be NaN", isNan.laneIsSet(3));
        }
    }

    // ==================== Pattern 9: Null-aware comparison ====================

    @Test
    public void testNullAwareLongCompareLe() {
        // QuestDB semantics: NULL <= NULL → true, NULL <= x → false, x <= NULL → false
        Unsafe.getUnsafe().putLong(longColumnAddr + 0L * Long.BYTES, Numbers.LONG_NULL);
        Unsafe.getUnsafe().putLong(longColumnAddr + 1L * Long.BYTES, Numbers.LONG_NULL);
        Unsafe.getUnsafe().putLong(longColumnAddr + 2L * Long.BYTES, 50L);
        if (LONG_SPECIES.length() > 3) {
            Unsafe.getUnsafe().putLong(longColumnAddr + 3L * Long.BYTES, 30L);
        }

        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector lhs = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        // Compare lhs <= broadcast(LONG_NULL)
        LongVector rhs = LongVector.broadcast(LONG_SPECIES, Numbers.LONG_NULL);

        VectorMask<Long> lhsNull = lhs.compare(VectorOperators.EQ, rhs);
        VectorMask<Long> rhsNull = rhs.compare(VectorOperators.EQ, LongVector.broadcast(LONG_SPECIES, Numbers.LONG_NULL));
        VectorMask<Long> anyNull = lhsNull.or(rhsNull);
        VectorMask<Long> bothNull = lhsNull.and(rhsNull);

        // LE with null awareness: (lhs <= rhs where neither is null) OR (both are null)
        VectorMask<Long> leResult = lhs.compare(VectorOperators.LE, rhs, anyNull.not()).or(bothNull);

        // Lane 0: NULL <= NULL → true (bothNull)
        Assert.assertTrue("NULL <= NULL should be true", leResult.laneIsSet(0));
        // Lane 1: NULL <= NULL → true
        Assert.assertTrue("NULL <= NULL should be true", leResult.laneIsSet(1));
        // Lane 2: 50 <= NULL → false (rhs is null but lhs isn't)
        Assert.assertFalse("50 <= NULL should be false", leResult.laneIsSet(2));
    }

    // ==================== Pattern 10: Double epsilon comparison ====================

    @Test
    public void testDoubleEpsilonEquality() {
        double epsilon = 1e-10;

        // Write values that are "equal" within epsilon
        Unsafe.getUnsafe().putDouble(doubleColumnAddr + 0L * Double.BYTES, 1.0);
        Unsafe.getUnsafe().putDouble(doubleColumnAddr + 1L * Double.BYTES, 1.0 + 1e-11); // within epsilon
        Unsafe.getUnsafe().putDouble(doubleColumnAddr + 2L * Double.BYTES, 1.0 + 1e-9);  // outside epsilon
        if (DOUBLE_SPECIES.length() > 3) {
            Unsafe.getUnsafe().putDouble(doubleColumnAddr + 3L * Double.BYTES, 2.0);
        }

        MemorySegment seg = MemorySegment.ofAddress(doubleColumnAddr)
                .reinterpret((long) rowCount * Double.BYTES);

        DoubleVector v = DoubleVector.fromMemorySegment(DOUBLE_SPECIES, seg, 0, ByteOrder.nativeOrder());
        DoubleVector target = DoubleVector.broadcast(DOUBLE_SPECIES, 1.0);

        // Epsilon equality: |a - b| <= epsilon
        VectorMask<Double> eq = v.sub(target).abs()
                .compare(VectorOperators.LE, epsilon);

        Assert.assertTrue("1.0 == 1.0 within epsilon", eq.laneIsSet(0));
        Assert.assertTrue("1.0+1e-11 == 1.0 within epsilon", eq.laneIsSet(1));
        Assert.assertFalse("1.0+1e-9 != 1.0 outside epsilon", eq.laneIsSet(2));
        if (DOUBLE_SPECIES.length() > 3) {
            Assert.assertFalse("2.0 != 1.0", eq.laneIsSet(3));
        }
    }

    // ==================== Pattern 11: Full filter loop ====================

    @Test
    public void testFullFilterLoopCountOnly() {
        // End-to-end: count rows where l > 25 AND l < 85
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        long count = 0;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> active = LONG_SPECIES.indexInRange(row, rowCount);
            LongVector v = LongVector.fromMemorySegment(
                    LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder(), active
            );
            VectorMask<Long> gt25 = v.compare(VectorOperators.GT, 25L, active);
            VectorMask<Long> lt85 = v.compare(VectorOperators.LT, 85L, active);
            count += gt25.and(lt85).trueCount();
        }

        // Values: 0,10,20,30,40,50,60,70,80,90,...
        // >25 AND <85: 30,40,50,60,70,80 → rows 3-8
        long expected = 0;
        for (int i = 0; i < rowCount; i++) {
            long val = i * 10L;
            if (val > 25 && val < 85) {
                expected++;
            }
        }
        Assert.assertEquals(expected, count);
    }

    @Test
    public void testFullFilterLoopRowIds() {
        // End-to-end: collect row IDs where l > 25 AND l < 85
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);
        MemorySegment out = MemorySegment.ofAddress(outputAddr)
                .reinterpret(outputSize);

        long filteredCount = 0;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> active = LONG_SPECIES.indexInRange(row, rowCount);
            LongVector v = LongVector.fromMemorySegment(
                    LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder(), active
            );
            VectorMask<Long> gt25 = v.compare(VectorOperators.GT, 25L, active);
            VectorMask<Long> lt85 = v.compare(VectorOperators.LT, 85L, active);
            VectorMask<Long> match = gt25.and(lt85);

            for (int lane = 0; lane < LONG_SPECIES.length(); lane++) {
                if (match.laneIsSet(lane)) {
                    out.setAtIndex(NATIVE_LONG, filteredCount++, row + lane);
                }
            }
        }

        // Verify each collected row ID
        for (long i = 0; i < filteredCount; i++) {
            long rowId = out.getAtIndex(NATIVE_LONG, i);
            long val = rowId * 10L;
            Assert.assertTrue(val > 25 && val < 85);
        }
    }

    // ==================== Pattern 12: Mixed-type filter ====================

    @Test
    public void testMixedTypeLongAndDoubleFilter() {
        // Filter: l > 25 AND d < 0.5
        // Need to operate on different species — long and double have
        // the same element size (8 bytes), so same number of lanes.
        Assert.assertEquals(
                "long and double should have same lane count",
                LONG_SPECIES.length(), DOUBLE_SPECIES.length()
        );

        MemorySegment longSeg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);
        MemorySegment dblSeg = MemorySegment.ofAddress(doubleColumnAddr)
                .reinterpret((long) rowCount * Double.BYTES);

        long count = 0;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> activeLong = LONG_SPECIES.indexInRange(row, rowCount);
            VectorMask<Double> activeDouble = DOUBLE_SPECIES.indexInRange(row, rowCount);

            LongVector lv = LongVector.fromMemorySegment(
                    LONG_SPECIES, longSeg, row * Long.BYTES, ByteOrder.nativeOrder(), activeLong
            );
            DoubleVector dv = DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES, dblSeg, row * Double.BYTES, ByteOrder.nativeOrder(), activeDouble
            );

            VectorMask<Long> longMatch = lv.compare(VectorOperators.GT, 25L, activeLong);
            VectorMask<Double> doubleMatch = dv.compare(VectorOperators.LT, 0.5, activeDouble);

            // Combine masks across types via cast
            VectorMask<Long> combined = longMatch.and(doubleMatch.cast(LONG_SPECIES));
            count += combined.trueCount();
        }

        long expected = 0;
        for (int i = 0; i < rowCount; i++) {
            if (i * 10L > 25 && i * 0.1 < 0.5) {
                expected++;
            }
        }
        Assert.assertEquals(expected, count);
    }

    // ==================== Pattern 13: Arithmetic on vectors ====================

    @Test
    public void testVectorArithmetic() {
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        LongVector added = v.add(100L);
        LongVector negated = v.neg();

        for (int i = 0; i < LONG_SPECIES.length(); i++) {
            Assert.assertEquals(i * 10L + 100L, added.lane(i));
            Assert.assertEquals(-(i * 10L), negated.lane(i));
        }
    }

    @Test
    public void testVectorDivisionWithNullPreservation() {
        // Division by zero should be masked out, null sentinel preserved
        Unsafe.getUnsafe().putLong(longColumnAddr + 0L * Long.BYTES, 100L);
        Unsafe.getUnsafe().putLong(longColumnAddr + 1L * Long.BYTES, Numbers.LONG_NULL);
        Unsafe.getUnsafe().putLong(longColumnAddr + 2L * Long.BYTES, 200L);

        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector lhs = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        LongVector divisor = LongVector.broadcast(LONG_SPECIES, 10L);
        LongVector nullVec = LongVector.broadcast(LONG_SPECIES, Numbers.LONG_NULL);

        // Mask out null lanes before division
        VectorMask<Long> isNull = lhs.compare(VectorOperators.EQ, nullVec);
        VectorMask<Long> valid = isNull.not();

        LongVector result = lhs.div(divisor, valid); // Only divide valid lanes
        result = result.blend(nullVec, isNull);       // Restore nulls

        Assert.assertEquals(10L, result.lane(0));    // 100/10
        Assert.assertEquals(Numbers.LONG_NULL, result.lane(1)); // NULL preserved
        Assert.assertEquals(20L, result.lane(2));    // 200/10
    }

    // ==================== Pattern 14: Compress store (alternative to lane-by-lane) ====================

    @Test
    public void testCompressStore() {
        // compress() extracts only the matching lanes into a contiguous result.
        // This is the efficient alternative to lane-by-lane extraction.
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);
        MemorySegment out = MemorySegment.ofAddress(outputAddr)
                .reinterpret(outputSize);

        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        VectorMask<Long> match = v.compare(VectorOperators.GT, 25L);

        // compress() packs matching lanes to the front
        LongVector compressed = v.compress(match);
        int matchCount = match.trueCount();

        // Store the compressed result
        compressed.intoMemorySegment(out, 0, ByteOrder.nativeOrder());

        // Verify: the first matchCount lanes should be the matching values
        for (int i = 0; i < matchCount; i++) {
            long val = out.getAtIndex(NATIVE_LONG, i);
            Assert.assertTrue("compressed value " + val + " should be > 25", val > 25);
        }
    }

    // ==================== Pattern 15: Row-ID generation with iota ====================

    @Test
    public void testRowIdGenerationWithIota() {
        // For row-ID mode, we need a vector of [row+0, row+1, row+2, ...].
        // LongVector has no direct iota, but we can broadcast + add an index vector.
        long row = 42;
        long[] indices = new long[LONG_SPECIES.length()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        LongVector indexVec = LongVector.fromArray(LONG_SPECIES, indices, 0);
        LongVector rowIds = indexVec.add(row);

        for (int i = 0; i < LONG_SPECIES.length(); i++) {
            Assert.assertEquals(row + i, rowIds.lane(i));
        }

        // Compress + store for row-ID output
        VectorMask<Long> match = LONG_SPECIES.maskAll(true)
                .and(LONG_SPECIES.indexInRange(0, 3)); // Only first 3 match
        LongVector compressed = rowIds.compress(match);
        Assert.assertEquals(row, compressed.lane(0));
        Assert.assertEquals(row + 1, compressed.lane(1));
        Assert.assertEquals(row + 2, compressed.lane(2));
    }

    // ==================== Pattern 16: Compress + store with running offset ====================

    @Test
    public void testCompressStoreFullLoop() {
        // The complete row-ID output pattern:
        // 1. Build iota row-ID vector [row+0, row+1, ...]
        // 2. Evaluate filter → mask
        // 3. Compress row-IDs with mask
        // 4. Store compressed vector at output offset
        // 5. Advance offset by trueCount
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);
        MemorySegment out = MemorySegment.ofAddress(outputAddr)
                .reinterpret(outputSize);

        long[] iotaArray = new long[LONG_SPECIES.length()];
        for (int i = 0; i < iotaArray.length; i++) {
            iotaArray[i] = i;
        }
        LongVector iota = LongVector.fromArray(LONG_SPECIES, iotaArray, 0);

        long filteredCount = 0;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> active = LONG_SPECIES.indexInRange(row, rowCount);
            LongVector v = LongVector.fromMemorySegment(
                    LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder(), active
            );

            // Filter: value > 25
            VectorMask<Long> match = v.compare(VectorOperators.GT, 25L, active);
            int matchCount = match.trueCount();

            if (matchCount > 0) {
                // Build row IDs for this chunk: [row+0, row+1, ...]
                LongVector rowIds = iota.add(row);
                // Compress to pack matching IDs contiguously
                LongVector compressed = rowIds.compress(match);
                // Store at current output position
                compressed.intoMemorySegment(out, filteredCount * Long.BYTES, ByteOrder.nativeOrder());
                filteredCount += matchCount;
            }
        }

        // Verify
        long expected = 0;
        for (int i = 0; i < rowCount; i++) {
            if (i * 10L > 25) expected++;
        }
        Assert.assertEquals(expected, filteredCount);

        for (long i = 0; i < filteredCount; i++) {
            long rowId = out.getAtIndex(NATIVE_LONG, i);
            Assert.assertTrue(rowId >= 0 && rowId < rowCount);
            Assert.assertTrue(rowId * 10L > 25);
        }
    }

    // ==================== Pattern 17: Type conversions ====================

    @Test
    public void testIntToLongConvertShape() {
        // convertShape is the proper way to do int→long widening
        MemorySegment seg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);

        IntVector iv = IntVector.fromMemorySegment(INT_SPECIES, seg, 0, ByteOrder.nativeOrder());

        // Int has 2x the lanes of Long (same bit width).
        // convertShape splits into parts: part 0 = first half, part 1 = second half.
        LongVector lv0 = (LongVector) iv.convertShape(VectorOperators.I2L, LONG_SPECIES, 0);
        for (int i = 0; i < LONG_SPECIES.length(); i++) {
            Assert.assertEquals((long) iv.lane(i), lv0.lane(i));
        }

        // If int has more lanes, second part captures the rest
        if (INT_SPECIES.length() > LONG_SPECIES.length()) {
            LongVector lv1 = (LongVector) iv.convertShape(VectorOperators.I2L, LONG_SPECIES, 1);
            for (int i = 0; i < LONG_SPECIES.length(); i++) {
                int srcLane = LONG_SPECIES.length() + i;
                if (srcLane < INT_SPECIES.length()) {
                    Assert.assertEquals((long) iv.lane(srcLane), lv1.lane(i));
                }
            }
        }
    }

    @Test
    public void testLongToDoubleConversion() {
        // For mixed i64+f64 operations: convert long to double
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);

        LongVector lv = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        // Long and Double have same element size → same lane count, single part
        DoubleVector dv = (DoubleVector) lv.convertShape(VectorOperators.L2D, DOUBLE_SPECIES, 0);

        for (int i = 0; i < LONG_SPECIES.length(); i++) {
            Assert.assertEquals((double) lv.lane(i), dv.lane(i), 0.0);
        }
    }

    @Test
    public void testIntToFloatConversion() {
        // For mixed i32+f32 operations: convert int to float
        MemorySegment seg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);

        IntVector iv = IntVector.fromMemorySegment(INT_SPECIES, seg, 0, ByteOrder.nativeOrder());
        // Int and Float have same element size → same lane count
        FloatVector fv = (FloatVector) iv.convertShape(VectorOperators.I2F, FLOAT_SPECIES, 0);

        for (int i = 0; i < INT_SPECIES.length(); i++) {
            Assert.assertEquals((float) iv.lane(i), fv.lane(i), 0.0f);
        }
    }

    @Test
    public void testIntToDoubleConversion() {
        // i32 mixed with f64: both promote to f64. Int has 2x lanes of Double.
        MemorySegment seg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);

        IntVector iv = IntVector.fromMemorySegment(INT_SPECIES, seg, 0, ByteOrder.nativeOrder());
        // Widen: int → double, takes part index because lane counts differ
        DoubleVector dv0 = (DoubleVector) iv.convertShape(VectorOperators.I2D, DOUBLE_SPECIES, 0);

        for (int i = 0; i < DOUBLE_SPECIES.length(); i++) {
            Assert.assertEquals((double) iv.lane(i), dv0.lane(i), 0.0);
        }
    }

    // ==================== Pattern 18: ByteVector / ShortVector ====================

    @Test
    public void testByteVectorLoadAndCompare() {
        // I1 columns: GeoHash byte, boolean.
        // Byte species has the most lanes (e.g., 32 or 64 with AVX2/512).
        int byteCount = BYTE_SPECIES.length();
        long byteColumnAddr = Unsafe.malloc(byteCount, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < byteCount; i++) {
                Unsafe.getUnsafe().putByte(byteColumnAddr + i, (byte) (i % 127));
            }

            MemorySegment seg = MemorySegment.ofAddress(byteColumnAddr).reinterpret(byteCount);
            ByteVector v = ByteVector.fromMemorySegment(BYTE_SPECIES, seg, 0, ByteOrder.nativeOrder());
            VectorMask<Byte> gt10 = v.compare(VectorOperators.GT, (byte) 10);

            for (int i = 0; i < BYTE_SPECIES.length(); i++) {
                byte val = (byte) (i % 127);
                Assert.assertEquals("lane " + i, val > 10, gt10.laneIsSet(i));
            }

            // Lane count: byte vectors have the most lanes
            Assert.assertTrue(BYTE_SPECIES.length() >= INT_SPECIES.length());
        } finally {
            Unsafe.free(byteColumnAddr, byteCount, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testShortVectorLoadAndCompare() {
        // I2 columns: short, GeoHash short
        int shortCount = SHORT_SPECIES.length();
        long shortColumnAddr = Unsafe.malloc((long) shortCount * Short.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < shortCount; i++) {
                Unsafe.getUnsafe().putShort(shortColumnAddr + (long) i * Short.BYTES, (short) (i * 3));
            }

            MemorySegment seg = MemorySegment.ofAddress(shortColumnAddr)
                    .reinterpret((long) shortCount * Short.BYTES);
            ShortVector v = ShortVector.fromMemorySegment(SHORT_SPECIES, seg, 0, ByteOrder.nativeOrder());
            VectorMask<Short> gt20 = v.compare(VectorOperators.GT, (short) 20);

            for (int i = 0; i < SHORT_SPECIES.length(); i++) {
                short val = (short) (i * 3);
                Assert.assertEquals("lane " + i, val > 20, gt20.laneIsSet(i));
            }
        } finally {
            Unsafe.free(shortColumnAddr, (long) shortCount * Short.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Pattern 19: FloatVector ====================

    @Test
    public void testFloatVectorEpsilonCompare() {
        long floatColumnAddr = Unsafe.malloc((long) rowCount * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putFloat(floatColumnAddr + 0L * Float.BYTES, 1.0f);
            Unsafe.getUnsafe().putFloat(floatColumnAddr + 1L * Float.BYTES, 1.0f + 1e-11f);
            Unsafe.getUnsafe().putFloat(floatColumnAddr + 2L * Float.BYTES, 1.0f + 1e-5f);
            for (int i = 3; i < rowCount; i++) {
                Unsafe.getUnsafe().putFloat(floatColumnAddr + (long) i * Float.BYTES, (float) i);
            }

            MemorySegment seg = MemorySegment.ofAddress(floatColumnAddr)
                    .reinterpret((long) rowCount * Float.BYTES);

            FloatVector v = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, 0, ByteOrder.nativeOrder());
            FloatVector target = FloatVector.broadcast(FLOAT_SPECIES, 1.0f);
            float epsilon = 1e-10f;

            VectorMask<Float> eq = v.sub(target).abs().compare(VectorOperators.LE, epsilon);

            Assert.assertTrue("exact match", eq.laneIsSet(0));
            Assert.assertTrue("within epsilon", eq.laneIsSet(1));
            Assert.assertFalse("outside epsilon", eq.laneIsSet(2));
        } finally {
            Unsafe.free(floatColumnAddr, (long) rowCount * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testFloatNanDetection() {
        long floatColumnAddr = Unsafe.malloc((long) rowCount * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putFloat(floatColumnAddr + 0L * Float.BYTES, 1.0f);
            Unsafe.getUnsafe().putFloat(floatColumnAddr + 1L * Float.BYTES, Float.NaN);
            Unsafe.getUnsafe().putFloat(floatColumnAddr + 2L * Float.BYTES, 3.0f);

            MemorySegment seg = MemorySegment.ofAddress(floatColumnAddr)
                    .reinterpret((long) rowCount * Float.BYTES);

            FloatVector v = FloatVector.fromMemorySegment(FLOAT_SPECIES, seg, 0, ByteOrder.nativeOrder());
            VectorMask<Float> isNan = v.test(VectorOperators.IS_NAN);

            Assert.assertFalse(isNan.laneIsSet(0));
            Assert.assertTrue(isNan.laneIsSet(1));
            Assert.assertFalse(isNan.laneIsSet(2));
        } finally {
            Unsafe.free(floatColumnAddr, (long) rowCount * Float.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Pattern 20: Bind variable broadcast ====================

    @Test
    public void testBindVariableBroadcast() {
        // Bind variables are stored at varsAddress with 8-byte stride.
        // The filter loads a scalar and broadcasts it to a vector.
        int varCount = 3;
        long varsAddr = Unsafe.malloc((long) varCount * 8, MemoryTag.NATIVE_DEFAULT);
        try {
            // Write bind vars: long at offset 0, int at offset 8, double at offset 16
            Unsafe.getUnsafe().putLong(varsAddr, 42L);
            Unsafe.getUnsafe().putInt(varsAddr + 8, 99);
            Unsafe.getUnsafe().putDouble(varsAddr + 16, 3.14);

            // Load scalar, broadcast to vector — this is what generated code will do
            long longVar = Unsafe.getUnsafe().getLong(varsAddr);
            LongVector longBroadcast = LongVector.broadcast(LONG_SPECIES, longVar);
            for (int i = 0; i < LONG_SPECIES.length(); i++) {
                Assert.assertEquals(42L, longBroadcast.lane(i));
            }

            int intVar = Unsafe.getUnsafe().getInt(varsAddr + 8);
            IntVector intBroadcast = IntVector.broadcast(INT_SPECIES, intVar);
            for (int i = 0; i < INT_SPECIES.length(); i++) {
                Assert.assertEquals(99, intBroadcast.lane(i));
            }

            double doubleVar = Unsafe.getUnsafe().getDouble(varsAddr + 16);
            DoubleVector doubleBroadcast = DoubleVector.broadcast(DOUBLE_SPECIES, doubleVar);
            for (int i = 0; i < DOUBLE_SPECIES.length(); i++) {
                Assert.assertEquals(3.14, doubleBroadcast.lane(i), 0.0);
            }
        } finally {
            Unsafe.free(varsAddr, (long) varCount * 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // ==================== Pattern 21: Mask cast across types ====================

    @Test
    public void testMaskCastIntToFloat() {
        // When combining results from int and float operations,
        // we need to cast masks between species of the same lane count.
        Assert.assertEquals(
                "int and float must have same lane count for mask cast",
                INT_SPECIES.length(), FLOAT_SPECIES.length()
        );

        IntVector iv = IntVector.broadcast(INT_SPECIES, 5);
        VectorMask<Integer> intMask = iv.compare(VectorOperators.GT, 3);

        // Cast to float mask — works because same lane count
        VectorMask<Float> floatMask = intMask.cast(FLOAT_SPECIES);
        Assert.assertEquals(intMask.trueCount(), floatMask.trueCount());

        // Can AND with a float comparison result
        FloatVector fv = FloatVector.broadcast(FLOAT_SPECIES, 2.0f);
        VectorMask<Float> floatCompare = fv.compare(VectorOperators.GT, 1.0f);
        VectorMask<Float> combined = floatMask.and(floatCompare);

        // Both are all-true, so combined should be all-true
        Assert.assertEquals(FLOAT_SPECIES.length(), combined.trueCount());
    }

    @Test
    public void testMaskCastLongToDouble() {
        Assert.assertEquals(
                "long and double must have same lane count for mask cast",
                LONG_SPECIES.length(), DOUBLE_SPECIES.length()
        );

        LongVector lv = LongVector.broadcast(LONG_SPECIES, 100L);
        VectorMask<Long> longMask = lv.compare(VectorOperators.GT, 50L);

        VectorMask<Double> doubleMask = longMask.cast(DOUBLE_SPECIES);
        Assert.assertEquals(longMask.trueCount(), doubleMask.trueCount());
    }

    // ==================== Pattern 22: Species lane counts and relationships ====================

    @Test
    public void testSpeciesLaneCounts() {
        // Understanding the lane count ratios is critical for mixed-type code generation.
        // With PREFERRED species, all use the same vector bit width.
        VectorShape shape = LONG_SPECIES.vectorShape();
        int bits = shape.vectorBitSize();

        Assert.assertEquals(bits / Byte.SIZE, BYTE_SPECIES.length());
        Assert.assertEquals(bits / Short.SIZE, SHORT_SPECIES.length());
        Assert.assertEquals(bits / Integer.SIZE, INT_SPECIES.length());
        Assert.assertEquals(bits / Long.SIZE, LONG_SPECIES.length());
        Assert.assertEquals(bits / Float.SIZE, FLOAT_SPECIES.length());
        Assert.assertEquals(bits / Double.SIZE, DOUBLE_SPECIES.length());

        // Same-width types have same lane count
        Assert.assertEquals(INT_SPECIES.length(), FLOAT_SPECIES.length());
        Assert.assertEquals(LONG_SPECIES.length(), DOUBLE_SPECIES.length());

        // Wider types have fewer lanes
        Assert.assertEquals(BYTE_SPECIES.length(), 2 * SHORT_SPECIES.length());
        Assert.assertEquals(SHORT_SPECIES.length(), 2 * INT_SPECIES.length());
        Assert.assertEquals(INT_SPECIES.length(), 2 * LONG_SPECIES.length());
    }

    // ==================== Pattern 23: Multi-column load in one iteration ====================

    @Test
    public void testMultiColumnLoadSameIteration() {
        // Real filters load multiple columns per iteration.
        // Each column has its own MemorySegment but shares the same row index.
        MemorySegment longSeg = MemorySegment.ofAddress(longColumnAddr)
                .reinterpret((long) rowCount * Long.BYTES);
        MemorySegment intSeg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);
        MemorySegment dblSeg = MemorySegment.ofAddress(doubleColumnAddr)
                .reinterpret((long) rowCount * Double.BYTES);

        // Process at LONG_SPECIES granularity (fewest lanes).
        // Int column must be loaded at matching offsets but with int stride.
        long count = 0;
        for (long row = 0; row < rowCount; row += LONG_SPECIES.length()) {
            VectorMask<Long> active = LONG_SPECIES.indexInRange(row, rowCount);

            // Load long column
            LongVector lv = LongVector.fromMemorySegment(
                    LONG_SPECIES, longSeg, row * Long.BYTES, ByteOrder.nativeOrder(), active
            );

            // Load double column (same lane count as long)
            VectorMask<Double> activeDbl = active.cast(DOUBLE_SPECIES);
            DoubleVector dv = DoubleVector.fromMemorySegment(
                    DOUBLE_SPECIES, dblSeg, row * Double.BYTES, ByteOrder.nativeOrder(), activeDbl
            );

            // Filter: l > 25 AND d < 0.8
            VectorMask<Long> longMatch = lv.compare(VectorOperators.GT, 25L, active);
            VectorMask<Double> dblMatch = dv.compare(VectorOperators.LT, 0.8, activeDbl);
            VectorMask<Long> combined = longMatch.and(dblMatch.cast(LONG_SPECIES));

            count += combined.trueCount();
        }

        long expected = 0;
        for (int i = 0; i < rowCount; i++) {
            if (i * 10L > 25 && i * 0.1 < 0.8) expected++;
        }
        Assert.assertEquals(expected, count);
    }

    // ==================== Pattern 24: Blend for conditional assignment ====================

    @Test
    public void testBlendForNullCoercion() {
        // When widening int→long, INT_NULL (0x80000000) sign-extends to
        // 0xFFFFFFFF80000000, not LONG_NULL (0x8000000000000000).
        // Fix: detect the sign-extended sentinel in long land and blend LONG_NULL.
        MemorySegment seg = MemorySegment.ofAddress(intColumnAddr)
                .reinterpret((long) rowCount * Integer.BYTES);

        Unsafe.getUnsafe().putInt(intColumnAddr + 1L * Integer.BYTES, Numbers.INT_NULL);

        IntVector iv = IntVector.fromMemorySegment(INT_SPECIES, seg, 0, ByteOrder.nativeOrder());

        // Convert int→long (first part).
        // INT_NULL sign-extends to (long) Integer.MIN_VALUE = 0xFFFFFFFF80000000.
        LongVector lv = (LongVector) iv.convertShape(VectorOperators.I2L, LONG_SPECIES, 0);

        // Detect the sign-extended INT_NULL value in the long vector
        long signExtendedIntNull = (long) Numbers.INT_NULL; // 0xFFFFFFFF80000000
        VectorMask<Long> isNull = lv.compare(VectorOperators.EQ, signExtendedIntNull);
        lv = lv.blend(Numbers.LONG_NULL, isNull);

        Assert.assertEquals(0L, lv.lane(0));               // 0 stays 0
        Assert.assertEquals(Numbers.LONG_NULL, lv.lane(1)); // INT_NULL → LONG_NULL
    }

    // ==================== Pattern 25: Method signatures for bytecode ====================

    @Test
    public void testMethodSignatures() {
        // This test documents the exact method signatures the bytecode generator
        // needs to emit invokestatic/invokevirtual calls for.
        // Each assertion verifies the method exists and returns the expected type.

        // Static factory: LongVector.fromMemorySegment(species, seg, offset, order)
        MemorySegment seg = MemorySegment.ofAddress(longColumnAddr).reinterpret(64);
        LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder());
        Assert.assertNotNull(v);

        // Static factory with mask: LongVector.fromMemorySegment(species, seg, offset, order, mask)
        VectorMask<Long> mask = LONG_SPECIES.indexInRange(0, rowCount);
        LongVector vm = LongVector.fromMemorySegment(LONG_SPECIES, seg, 0, ByteOrder.nativeOrder(), mask);
        Assert.assertNotNull(vm);

        // Static factory: LongVector.broadcast(species, scalar)
        LongVector bcast = LongVector.broadcast(LONG_SPECIES, 42L);
        Assert.assertNotNull(bcast);

        // Instance: vector.compare(op, scalar) → VectorMask
        VectorMask<Long> cmp = v.compare(VectorOperators.GT, 0L);
        Assert.assertNotNull(cmp);

        // Instance: vector.compare(op, vector) → VectorMask
        VectorMask<Long> cmp2 = v.compare(VectorOperators.GT, bcast);
        Assert.assertNotNull(cmp2);

        // Instance: vector.compare(op, scalar, mask) → VectorMask
        VectorMask<Long> cmp3 = v.compare(VectorOperators.GT, 0L, mask);
        Assert.assertNotNull(cmp3);

        // Instance: mask.and(mask), mask.or(mask), mask.not()
        VectorMask<Long> andM = cmp.and(cmp2);
        VectorMask<Long> orM = cmp.or(cmp2);
        VectorMask<Long> notM = cmp.not();
        Assert.assertNotNull(andM);
        Assert.assertNotNull(orM);
        Assert.assertNotNull(notM);

        // Instance: mask.trueCount() → int
        int tc = cmp.trueCount();
        Assert.assertTrue(tc >= 0);

        // Instance: mask.cast(species) → VectorMask
        VectorMask<Double> castM = cmp.cast(DOUBLE_SPECIES);
        Assert.assertNotNull(castM);

        // Instance: vector.add/sub/mul/div/neg
        LongVector arith = v.add(bcast).sub(bcast).mul(bcast);
        Assert.assertNotNull(arith);
        LongVector neg = v.neg();
        Assert.assertNotNull(neg);

        // Instance: vector.div(vector, mask) — masked division
        LongVector safeDivisor = LongVector.broadcast(LONG_SPECIES, 1L);
        LongVector divResult = v.div(safeDivisor, mask);
        Assert.assertNotNull(divResult);

        // Instance: vector.blend(scalar, mask)
        LongVector blended = v.blend(0L, cmp);
        Assert.assertNotNull(blended);

        // Instance: vector.compress(mask)
        LongVector compressed = v.compress(cmp);
        Assert.assertNotNull(compressed);

        // Instance: vector.intoMemorySegment(seg, offset, order)
        MemorySegment out = MemorySegment.ofAddress(outputAddr).reinterpret(outputSize);
        compressed.intoMemorySegment(out, 0, ByteOrder.nativeOrder());

        // Instance: vector.convertShape(op, species, part) → Vector
        IntVector iv = IntVector.broadcast(INT_SPECIES, 7);
        LongVector converted = (LongVector) iv.convertShape(VectorOperators.I2L, LONG_SPECIES, 0);
        Assert.assertEquals(7L, converted.lane(0));

        // Static: species.indexInRange(offset, limit) → VectorMask
        VectorMask<Long> range = LONG_SPECIES.indexInRange(0L, 3L);
        Assert.assertNotNull(range);

        // Instance: vector.test(op) → VectorMask (for IS_NAN)
        DoubleVector dv = DoubleVector.broadcast(DOUBLE_SPECIES, Double.NaN);
        VectorMask<Double> nanTest = dv.test(VectorOperators.IS_NAN);
        Assert.assertEquals(DOUBLE_SPECIES.length(), nanTest.trueCount());
    }
}
