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

package io.questdb.test.cairo;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;


public class RostiTest extends AbstractCairoTest {

    @Test
    public void testIntSumMergeKeepsLongAccumulator() throws Exception {
        assertMemoryLeak(() -> {
            long a = integerState(false, 16);
            long b = integerState(false, 16);
            long aBytes = Rosti.getAllocMemory(a), bBytes = Rosti.getAllocMemory(b);
            long input = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                int[] keys = {1, 1, 2, 2};
                for (int i = 0; i < keys.length; i++) {
                    Unsafe.putInt(input + 4L * i, keys[i]);
                    Unsafe.putInt(input + 32 + 4L * i, i < 2 ? Integer.MAX_VALUE : -Integer.MAX_VALUE);
                }
                Assert.assertTrue(Rosti.keyedIntSumInt(b, input, input + 32, 4, 1));
                Unsafe.putInt(input + 32, 7);
                Assert.assertTrue(Rosti.keyedIntSumInt(a, input, input + 32, 1, 1));
                Assert.assertTrue(Rosti.keyedIntSumIntMerge(a, b, 1));
                Assert.assertEquals(2L * Integer.MAX_VALUE + 7, Unsafe.getLong(valueAddress(a, 1)));
                Assert.assertEquals(-2L * Integer.MAX_VALUE, Unsafe.getLong(valueAddress(a, 2)));
                Assert.assertEquals(3, Unsafe.getLong(valueAddress(a, 1) + 8));
                Assert.assertEquals(2, Unsafe.getLong(valueAddress(a, 2) + 8));
            } finally {
                Unsafe.free(input, 64, MemoryTag.NATIVE_DEFAULT);
                Rosti.updateMemoryUsage(a, aBytes);
                Rosti.updateMemoryUsage(b, bBytes);
                Rosti.free(a);
                Rosti.free(b);
            }
        });
    }

    @Test
    public void testIntegerAverageNullPartialAndWrapUpGrowth() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean wide : new boolean[]{false, true}) {
                for (boolean existingNull : new boolean[]{true, false}) {
                    long map = integerState(wide, 16);
                    long initialBytes = Rosti.getAllocMemory(map);
                    long input = Unsafe.malloc(192, MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (int i = 0; i < 14; i++) {
                            Unsafe.putInt(input + i * 4L, existingNull && i == 0 ? Numbers.INT_NULL : i + 1);
                            if (wide) Unsafe.putLong(input + 64 + i * 8L, existingNull && i == 0 ? Long.MAX_VALUE : 10L * (i + 1));
                            else Unsafe.putInt(input + 64 + i * 4L, 10 * (i + 1));
                        }
                        Assert.assertTrue(wide ? Rosti.keyedIntSumLongLong(map, input, input + 64, 14, 1)
                                : Rosti.keyedIntSumInt(map, input, input + 64, 14, 1));
                        if (wide && existingNull) {
                            Unsafe.putInt(input + 4, Numbers.INT_NULL);
                            Unsafe.putLong(input + 72, Long.MAX_VALUE);
                            Assert.assertTrue(Rosti.keyedIntSumLongLong(map, input, input + 64, 2, 1));
                        }
                        long capacity = Rosti.getCapacity(map);
                        Assert.assertTrue(wide ? Rosti.keyedIntAvgLongLongWrapUp(map, 1, 12, 2)
                                : Rosti.keyedIntAvgLongWrapUp(map, 1, 12, 2));
                        double expected = existingNull ? wide ? (3.0 * Long.MAX_VALUE + 12) / 5 : 22.0 / 3 : 6;
                        Assert.assertEquals(expected, Unsafe.getDouble(valueAddress(map, Numbers.INT_NULL)), 0);
                        if (!existingNull) Assert.assertTrue("NULL insertion must exercise map growth", Rosti.getCapacity(map) > capacity);
                        for (int key = existingNull ? 2 : 1; key <= 14; key++)
                            Assert.assertEquals(10.0 * key, Unsafe.getDouble(valueAddress(map, key)), 0);
                    } finally {
                        Unsafe.free(input, 192, MemoryTag.NATIVE_DEFAULT);
                        Rosti.updateMemoryUsage(map, initialBytes);
                        Rosti.free(map);
                    }
                }
            }
        });
    }

    private static long integerState(boolean wide, int capacity) {
        ArrayColumnTypes types = new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.LONG).add(ColumnType.LONG);
        if (wide) types.add(ColumnType.LONG);
        long map = Rosti.alloc(types, capacity);
        Unsafe.putInt(Rosti.getInitialValueSlot(map, 0), Numbers.INT_NULL);
        for (int i = 1; i < types.getColumnCount(); i++) Unsafe.putLong(Rosti.getInitialValueSlot(map, i), 0);
        return map;
    }

    private static long valueAddress(long map, int key) {
        for (long slot = 0; slot < Rosti.getCapacity(map); slot++) {
            if (Unsafe.getByte(Rosti.getCtrl(map) + slot) >= 0) {
                long entry = Rosti.getSlots(map) + (slot << Rosti.getSlotShift(map));
                if (Unsafe.getInt(entry) == key) return entry + Unsafe.getInt(Rosti.getValueOffsets(map) + 4);
            }
        }
        throw new AssertionError("Missing Rosti key: " + key);
    }

    @Test
    public void testPrintRosti() {
        long pRosti = Rosti.alloc(new SingleColumnType(ColumnType.INT), 1024);
        try {
            Rosti.printRosti(pRosti);
        } finally {
            Rosti.free(pRosti);
        }
    }
}
