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

package io.questdb.test.cutlass.arrow.column;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cutlass.arrow.column.ArrowColumnScratch;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ArrowColumnScratchTest extends AbstractCairoTest {

    @Test
    public void testAllNullLongValidityAllZero() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 16);
                for (int i = 0; i < 10; i++) {
                    s.appendLongOrNull(Numbers.LONG_NULL);
                }
                Assert.assertEquals(10, s.getRowCount());
                Assert.assertEquals(10, s.getNullCount());
                Assert.assertEquals(80, s.valuesLengthBytes());
                Assert.assertEquals(2, s.validityLengthBytes());

                long validityDst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                long valuesDst = Unsafe.malloc(80, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValidityTo(validityDst);
                    Assert.assertEquals(0, Unsafe.getUnsafe().getByte(validityDst) & 0xFF);
                    Assert.assertEquals(0, Unsafe.getUnsafe().getByte(validityDst + 1) & 0xFF);
                    s.flushValuesTo(valuesDst);
                    for (int i = 0; i < 80; i++) {
                        Assert.assertEquals(0, Unsafe.getUnsafe().getByte(valuesDst + i));
                    }
                } finally {
                    Unsafe.free(validityDst, 8, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(valuesDst, 80, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testAllValidLongOrNullKeepsEmptyValidity() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 8);
                for (int i = 0; i < 8; i++) {
                    s.appendLongOrNull(i + 1L);
                }
                Assert.assertEquals(8, s.getRowCount());
                Assert.assertEquals(0, s.getNullCount());
                Assert.assertEquals(0, s.validityLengthBytes());
                Assert.assertEquals(64, s.valuesLengthBytes());
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testAppendStringOrNullBasic() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.STRING, 4);
                s.appendStringOrNull("hi");
                s.appendStringOrNull("world");
                s.appendStringOrNull("");
                Assert.assertEquals(3, s.getRowCount());
                Assert.assertEquals(0, s.getNullCount());
                // 2 + 5 + 0 = 7 UTF-8 bytes total.
                Assert.assertEquals(7, s.valuesLengthBytes());
                // offsets buffer carries 4 * (rowCount + 1) = 16 bytes.
                Assert.assertEquals(16, s.offsetsLengthBytes());
                // all valid; no validity bytes emitted.
                Assert.assertEquals(0, s.validityLengthBytes());

                long offDst = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                long valDst = Unsafe.malloc(7, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushOffsetsTo(offDst);
                    Assert.assertEquals(0, Unsafe.getUnsafe().getInt(offDst));
                    Assert.assertEquals(2, Unsafe.getUnsafe().getInt(offDst + 4));
                    Assert.assertEquals(7, Unsafe.getUnsafe().getInt(offDst + 8));
                    Assert.assertEquals(7, Unsafe.getUnsafe().getInt(offDst + 12));
                    s.flushValuesTo(valDst);
                    byte[] expected = {'h', 'i', 'w', 'o', 'r', 'l', 'd'};
                    for (int i = 0; i < expected.length; i++) {
                        Assert.assertEquals("byte " + i, expected[i], Unsafe.getUnsafe().getByte(valDst + i));
                    }
                } finally {
                    Unsafe.free(valDst, 7, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(offDst, 16, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testAppendStringOrNullWithNullInMiddle() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.STRING, 4);
                s.appendStringOrNull("abc");
                s.appendStringOrNull(null);
                s.appendStringOrNull("de");
                Assert.assertEquals(3, s.getRowCount());
                Assert.assertEquals(1, s.getNullCount());
                Assert.assertEquals(5, s.valuesLengthBytes()); // 3 + 0 + 2
                Assert.assertEquals(16, s.offsetsLengthBytes());
                // first null allocates the validity bitmap; validity covers row 0..2.
                Assert.assertEquals(1, s.validityLengthBytes());

                long offDst = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                long valDst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                long bitDst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushOffsetsTo(offDst);
                    Assert.assertEquals(0, Unsafe.getUnsafe().getInt(offDst));
                    Assert.assertEquals(3, Unsafe.getUnsafe().getInt(offDst + 4));
                    Assert.assertEquals(3, Unsafe.getUnsafe().getInt(offDst + 8));
                    Assert.assertEquals(5, Unsafe.getUnsafe().getInt(offDst + 12));
                    s.flushValuesTo(valDst);
                    byte[] expected = {'a', 'b', 'c', 'd', 'e'};
                    for (int i = 0; i < expected.length; i++) {
                        Assert.assertEquals(expected[i], Unsafe.getUnsafe().getByte(valDst + i));
                    }
                    s.flushValidityTo(bitDst);
                    // bit0 = valid, bit1 = null, bit2 = valid -> 0b0000_0101.
                    Assert.assertEquals((byte) 0b0000_0101, Unsafe.getUnsafe().getByte(bitDst));
                } finally {
                    Unsafe.free(bitDst, 8, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(valDst, 8, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(offDst, 16, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testAppendVarcharOrNullDirect() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            GcUtf8String direct = new GcUtf8String("abc");
            try {
                // Sanity: direct-backed sequence must expose a valid ptr().
                Assert.assertTrue("GcUtf8String must be direct-backed", direct.ptr() >= 0);
                s.initFor(ColumnType.VARCHAR, 4);
                s.appendVarcharOrNull(direct);
                Assert.assertEquals(1, s.getRowCount());
                Assert.assertEquals(0, s.getNullCount());
                Assert.assertEquals(3, s.valuesLengthBytes());
                Assert.assertEquals(8, s.offsetsLengthBytes()); // offsets[0..1]

                long off = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                long val = Unsafe.malloc(3, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushOffsetsTo(off);
                    Assert.assertEquals(0, Unsafe.getUnsafe().getInt(off));
                    Assert.assertEquals(3, Unsafe.getUnsafe().getInt(off + 4));
                    s.flushValuesTo(val);
                    Assert.assertEquals((byte) 'a', Unsafe.getUnsafe().getByte(val));
                    Assert.assertEquals((byte) 'b', Unsafe.getUnsafe().getByte(val + 1));
                    Assert.assertEquals((byte) 'c', Unsafe.getUnsafe().getByte(val + 2));
                } finally {
                    Unsafe.free(val, 3, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(off, 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testAppendVarcharOrNullOnHeap() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                Utf8String onHeap = new Utf8String("xyz");
                // Sanity: plain Utf8String does not expose a native pointer.
                Assert.assertEquals(-1L, onHeap.ptr());
                s.initFor(ColumnType.VARCHAR, 4);
                s.appendVarcharOrNull(onHeap);
                Assert.assertEquals(1, s.getRowCount());
                Assert.assertEquals(3, s.valuesLengthBytes());
                long val = Unsafe.malloc(3, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValuesTo(val);
                    Assert.assertEquals((byte) 'x', Unsafe.getUnsafe().getByte(val));
                    Assert.assertEquals((byte) 'y', Unsafe.getUnsafe().getByte(val + 1));
                    Assert.assertEquals((byte) 'z', Unsafe.getUnsafe().getByte(val + 2));
                } finally {
                    Unsafe.free(val, 3, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testBoolBitPacked() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.BOOLEAN, 16);
                boolean[] vals = {true, false, true, false, true, false, true, false, true};
                for (boolean b : vals) {
                    s.appendBool(b);
                }
                Assert.assertEquals(9, s.getRowCount());
                Assert.assertEquals(0, s.getNullCount());
                Assert.assertEquals(2, s.valuesLengthBytes());
                Assert.assertEquals(0, s.validityLengthBytes());

                long dst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValuesTo(dst);
                    Assert.assertEquals((byte) 0b0101_0101, Unsafe.getUnsafe().getByte(dst));
                    Assert.assertEquals((byte) 0b0000_0001, Unsafe.getUnsafe().getByte(dst + 1));
                } finally {
                    Unsafe.free(dst, 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testByteAppendAndFlush() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.BYTE, 4);
                byte[] values = {1, 2, 3, 4};
                for (byte v : values) s.appendByte(v);
                Assert.assertEquals(4, s.getRowCount());
                Assert.assertEquals(4, s.valuesLengthBytes());
                long dst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValuesTo(dst);
                    for (int i = 0; i < 4; i++) {
                        Assert.assertEquals(values[i], Unsafe.getUnsafe().getByte(dst + i));
                    }
                } finally {
                    Unsafe.free(dst, 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testCloseFreesNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            s.initFor(ColumnType.LONG, 8);
            CursorRecord cur = new CursorRecord(new long[]{1, 2, 3}, null, null);
            while (cur.next()) {
                s.appendLong(cur.getLong(0));
            }
            s.close();
            s.close();
        });
    }

    @Test
    public void testCloseFreesValidityBuffer() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            s.initFor(ColumnType.LONG, 8);
            s.appendLongOrNull(1L);
            s.appendLongOrNull(Numbers.LONG_NULL);
            s.close();
        });
    }

    @Test
    public void testDoubleAppendAndFlush() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.DOUBLE, 4);
                double[] expected = {0.5, 1.0, 1.5, 2.0};
                CursorRecord cur = new CursorRecord(null, expected, null);
                while (cur.next()) {
                    s.appendDouble(cur.getDouble(0));
                }
                Assert.assertEquals(expected.length, s.getRowCount());
                Assert.assertEquals(expected.length * 8, s.valuesLengthBytes());

                long dst = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = s.flushValuesTo(dst);
                    Assert.assertEquals(dst + expected.length * 8L, end);
                    for (int i = 0; i < expected.length; i++) {
                        long bits = Unsafe.getUnsafe().getLong(dst + i * 8L);
                        Assert.assertEquals(Double.doubleToRawLongBits(expected[i]), bits);
                    }
                } finally {
                    Unsafe.free(dst, 64, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testDoubleInfinityDetectedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.DOUBLE, 4);
                s.appendDoubleOrNull(Double.POSITIVE_INFINITY);
                s.appendDoubleOrNull(Double.NEGATIVE_INFINITY);
                s.appendDoubleOrNull(Double.NaN);
                s.appendDoubleOrNull(1.5);
                Assert.assertEquals(4, s.getRowCount());
                Assert.assertEquals(3, s.getNullCount());
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testFloatAppendAndFlush() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.FLOAT, 3);
                float[] vals = {1.5f, -2.25f, 0.0f};
                for (float v : vals) s.appendFloat(v);
                Assert.assertEquals(3, s.getRowCount());
                Assert.assertEquals(12, s.valuesLengthBytes());
                long dst = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValuesTo(dst);
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals(Float.floatToRawIntBits(vals[i]),
                                Unsafe.getUnsafe().getInt(dst + i * 4L));
                    }
                } finally {
                    Unsafe.free(dst, 16, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testFloatNaNAndInfinityDetectedAsNull() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.FLOAT, 4);
                s.appendFloatOrNull(Float.NaN);
                s.appendFloatOrNull(Float.POSITIVE_INFINITY);
                s.appendFloatOrNull(1.5f);
                Assert.assertEquals(3, s.getRowCount());
                Assert.assertEquals(2, s.getNullCount());
                Assert.assertEquals(1, s.validityLengthBytes());
                long vdst = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValidityTo(vdst);
                    byte bits = Unsafe.getUnsafe().getByte(vdst);
                    Assert.assertEquals((byte) 0b0000_0100, bits);
                } finally {
                    Unsafe.free(vdst, 4, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testGrowValidityBufferPastInitialCap() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 128);
                int n = 10_000;
                for (int i = 0; i < n; i++) {
                    s.appendLongOrNull(Numbers.LONG_NULL);
                }
                Assert.assertEquals(n, s.getRowCount());
                Assert.assertEquals(n, s.getNullCount());
                Assert.assertEquals((n + 7) >>> 3, s.validityLengthBytes());
                // all-null: every byte in validity must be 0.
                int validityBytes = s.validityLengthBytes();
                long dst = Unsafe.malloc(validityBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValidityTo(dst);
                    for (int i = 0; i < validityBytes; i++) {
                        Assert.assertEquals("byte " + i, 0, Unsafe.getUnsafe().getByte(dst + i));
                    }
                } finally {
                    Unsafe.free(dst, validityBytes, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testGrowValuesBuffer() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 2);
                int n = 2048;
                long[] src = new long[n];
                for (int i = 0; i < n; i++) {
                    src[i] = 1_000L + i;
                }
                CursorRecord cur = new CursorRecord(src, null, null);
                while (cur.next()) {
                    s.appendLong(cur.getLong(0));
                }
                Assert.assertEquals(n, s.getRowCount());
                Assert.assertEquals(n * 8, s.valuesLengthBytes());

                long dst = Unsafe.malloc(n * 8L, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValuesTo(dst);
                    for (int i = 0; i < n; i++) {
                        Assert.assertEquals(src[i], Unsafe.getUnsafe().getLong(dst + i * 8L));
                    }
                } finally {
                    Unsafe.free(dst, n * 8L, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testHalfNullLongBits() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 8);
                // Row 0 valid, 1 null, 2 valid, 3 null, ... 6 valid, 7 null.
                for (int i = 0; i < 8; i++) {
                    if ((i & 1) == 0) {
                        s.appendLongOrNull(100L + i);
                    } else {
                        s.appendLongOrNull(Numbers.LONG_NULL);
                    }
                }
                Assert.assertEquals(8, s.getRowCount());
                Assert.assertEquals(4, s.getNullCount());
                Assert.assertEquals(1, s.validityLengthBytes());
                long dst = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValidityTo(dst);
                    // Bits: valid at 0,2,4,6 → 0b0101_0101 = 0x55.
                    Assert.assertEquals((byte) 0b0101_0101, Unsafe.getUnsafe().getByte(dst));
                } finally {
                    Unsafe.free(dst, 4, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testIntAppendAndFlush() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.INT, 5);
                int[] expected = {10, 20, 30, 40, 50};
                CursorRecord cur = new CursorRecord(null, null, expected);
                while (cur.next()) {
                    s.appendInt(cur.getInt(0));
                }
                Assert.assertEquals(expected.length, s.getRowCount());
                Assert.assertEquals(expected.length * 4, s.valuesLengthBytes());

                long dst = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = s.flushValuesTo(dst);
                    Assert.assertEquals(dst + expected.length * 4L, end);
                    for (int i = 0; i < expected.length; i++) {
                        Assert.assertEquals(expected[i], Unsafe.getUnsafe().getInt(dst + i * 4L));
                    }
                } finally {
                    Unsafe.free(dst, 64, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testLongAppendAndFlush() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 10);
                long[] expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
                CursorRecord cur = new CursorRecord(expected, null, null);
                while (cur.next()) {
                    s.appendLong(cur.getLong(0));
                }
                Assert.assertEquals(expected.length, s.getRowCount());
                Assert.assertEquals(expected.length * 8, s.valuesLengthBytes());

                long dst = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
                try {
                    long end = s.flushValuesTo(dst);
                    Assert.assertEquals(dst + expected.length * 8L, end);
                    for (int i = 0; i < expected.length; i++) {
                        long v = expected[i];
                        long dstBase = dst + i * 8L;
                        for (int b = 0; b < 8; b++) {
                            byte expectedByte = (byte) ((v >>> (b * 8)) & 0xFF);
                            Assert.assertEquals(expectedByte, Unsafe.getUnsafe().getByte(dstBase + b));
                        }
                    }
                } finally {
                    Unsafe.free(dst, 128, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testOffsetsMonotonic() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.STRING, 8);
                String[] rows = {"a", "", "bc", null, "defg", ""};
                for (String r : rows) {
                    s.appendStringOrNull(r);
                }
                int rowCount = s.getRowCount();
                Assert.assertEquals(rows.length, rowCount);
                int offsetsLen = s.offsetsLengthBytes();
                Assert.assertEquals(4 * (rowCount + 1), offsetsLen);
                long off = Unsafe.malloc(offsetsLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushOffsetsTo(off);
                    int prev = Unsafe.getUnsafe().getInt(off);
                    Assert.assertEquals(0, prev);
                    for (int i = 1; i <= rowCount; i++) {
                        int cur = Unsafe.getUnsafe().getInt(off + 4L * i);
                        Assert.assertTrue("offsets must be non-decreasing at " + i + ": " + prev + " -> " + cur,
                                cur >= prev);
                        prev = cur;
                    }
                    Assert.assertEquals("last offset must equal valuesPos",
                            s.valuesLengthBytes(), Unsafe.getUnsafe().getInt(off + 4L * rowCount));
                } finally {
                    Unsafe.free(off, offsetsLen, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testResetAfterNullThenAllValidEmitsEmptyValidity() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 8);
                s.appendLongOrNull(1L);
                s.appendLongOrNull(Numbers.LONG_NULL);
                s.appendLongOrNull(3L);
                Assert.assertEquals(1, s.getNullCount());
                Assert.assertEquals(1, s.validityLengthBytes());

                s.reset();
                Assert.assertEquals(0, s.getRowCount());
                Assert.assertEquals(0, s.getNullCount());
                Assert.assertEquals(0, s.validityLengthBytes());

                s.appendLongOrNull(10L);
                s.appendLongOrNull(20L);
                Assert.assertEquals(2, s.getRowCount());
                Assert.assertEquals(0, s.getNullCount());
                Assert.assertEquals(0, s.validityLengthBytes());
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testResetClearsCountersButKeepsBuffer() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.LONG, 8);
                CursorRecord cur = new CursorRecord(new long[]{100, 200, 300}, null, null);
                while (cur.next()) {
                    s.appendLong(cur.getLong(0));
                }
                Assert.assertEquals(3, s.getRowCount());
                Assert.assertEquals(24, s.valuesLengthBytes());

                s.reset();
                Assert.assertEquals(0, s.getRowCount());
                Assert.assertEquals(0, s.valuesLengthBytes());

                CursorRecord cur2 = new CursorRecord(new long[]{7, 8}, null, null);
                while (cur2.next()) {
                    s.appendLong(cur2.getLong(0));
                }
                Assert.assertEquals(2, s.getRowCount());
                Assert.assertEquals(16, s.valuesLengthBytes());

                long dst = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValuesTo(dst);
                    Assert.assertEquals(7L, Unsafe.getUnsafe().getLong(dst));
                    Assert.assertEquals(8L, Unsafe.getUnsafe().getLong(dst + 8));
                } finally {
                    Unsafe.free(dst, 16, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testResetClearsOffsetsForReuse() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.STRING, 4);
                s.appendStringOrNull("first");
                s.appendStringOrNull("second");
                Assert.assertEquals(2, s.getRowCount());

                s.reset();
                Assert.assertEquals(0, s.getRowCount());
                Assert.assertEquals(0, s.getNullCount());
                Assert.assertEquals(4, s.offsetsLengthBytes()); // 4 * (0 + 1) = 4
                Assert.assertEquals(0, s.valuesLengthBytes());

                s.appendStringOrNull("abc");
                Assert.assertEquals(1, s.getRowCount());
                Assert.assertEquals(3, s.valuesLengthBytes());
                int offLen = s.offsetsLengthBytes();
                long off = Unsafe.malloc(offLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushOffsetsTo(off);
                    Assert.assertEquals("offsets[0] must be 0 after reset", 0, Unsafe.getUnsafe().getInt(off));
                    Assert.assertEquals("offsets[1] must be byteLen of the first new row",
                            3, Unsafe.getUnsafe().getInt(off + 4));
                } finally {
                    Unsafe.free(off, offLen, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    @Test
    public void testShortAppendAndFlush() throws Exception {
        assertMemoryLeak(() -> {
            ArrowColumnScratch s = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
            try {
                s.initFor(ColumnType.SHORT, 3);
                short[] values = {1000, -2000, 30_000};
                for (short v : values) s.appendShort(v);
                Assert.assertEquals(3, s.getRowCount());
                Assert.assertEquals(6, s.valuesLengthBytes());
                long dst = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    s.flushValuesTo(dst);
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals(values[i], Unsafe.getUnsafe().getShort(dst + i * 2L));
                    }
                } finally {
                    Unsafe.free(dst, 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                s.close();
            }
        });
    }

    /**
     * Iterator-style stub Record. One of the three primitive arrays is populated
     * and {@code next()} advances the cursor one step at a time, mirroring how
     * {@code RecordCursor} drives a real record via {@code getRecord()} + {@code hasNext()}.
     */
    private static final class CursorRecord implements Record {
        private final double[] doubles;
        private final int[] ints;
        private final long[] longs;
        private final int size;
        private int pos = -1;

        CursorRecord(long[] longs, double[] doubles, int[] ints) {
            this.longs = longs;
            this.doubles = doubles;
            this.ints = ints;
            if (longs != null) {
                this.size = longs.length;
            } else if (doubles != null) {
                this.size = doubles.length;
            } else {
                this.size = ints == null ? 0 : ints.length;
            }
        }

        @Override
        public double getDouble(int col) {
            return doubles[pos];
        }

        @Override
        public int getInt(int col) {
            return ints[pos];
        }

        @Override
        public long getLong(int col) {
            return longs[pos];
        }

        boolean next() {
            pos++;
            return pos < size;
        }
    }
}
