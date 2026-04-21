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

import io.questdb.cutlass.arrow.column.Int64ColumnEmitter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class Int64ColumnEmitterTest {

    @Test
    public void testEmitEmpty() {
        long dst = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(dst, (byte) 0xAB);
            long end = Int64ColumnEmitter.INSTANCE.emit(dst, new long[]{1}, 0, 0);
            Assert.assertEquals(dst, end);
            Assert.assertEquals((byte) 0xAB, Unsafe.getUnsafe().getByte(dst));
        } finally {
            Unsafe.free(dst, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEmitThreeValues() {
        long dst = Unsafe.malloc(24, MemoryTag.NATIVE_DEFAULT);
        try {
            long[] src = {1L, 2L, 3L};
            long end = Int64ColumnEmitter.INSTANCE.emit(dst, src, 0, src.length);
            Assert.assertEquals(dst + 24, end);
            byte[] expected = {
                    0x01, 0, 0, 0, 0, 0, 0, 0,
                    0x02, 0, 0, 0, 0, 0, 0, 0,
                    0x03, 0, 0, 0, 0, 0, 0, 0,
            };
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("byte " + i, expected[i], Unsafe.getUnsafe().getByte(dst + i));
            }
        } finally {
            Unsafe.free(dst, 24, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEmitWithOffset() {
        long dst = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            long[] src = {-1L, Long.MAX_VALUE, Long.MIN_VALUE};
            long end = Int64ColumnEmitter.INSTANCE.emit(dst, src, 1, 2);
            Assert.assertEquals(dst + 16, end);
            byte[] expected = {
                    // Long.MAX_VALUE little-endian: 0x7F FF FF FF FF FF FF FF reversed
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F,
                    // Long.MIN_VALUE little-endian: 0x00 ... 0x80
                    0, 0, 0, 0, 0, 0, 0, (byte) 0x80,
            };
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("byte " + i, expected[i], Unsafe.getUnsafe().getByte(dst + i));
            }
        } finally {
            Unsafe.free(dst, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
