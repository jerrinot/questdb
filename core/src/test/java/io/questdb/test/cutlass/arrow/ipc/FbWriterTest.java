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

package io.questdb.test.cutlass.arrow.ipc;

import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FbWriterTest {

    private static final int BUF = 4096;

    @Test
    public void testEmptyTable() {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            w.startTable(0);
            int table = w.endTable();
            w.finish(table);
            byte[] bytes = extract(w);
            ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            // Root uoffset points at the table's soffset slot.
            int rootUoffset = bb.getInt(0);
            int tablePos = rootUoffset;
            int soffset = bb.getInt(tablePos);
            // soffset is positive (vtable precedes table in memory).
            Assert.assertTrue("soffset must be positive", soffset > 0);
            int vtablePos = tablePos - soffset;
            int vtLen = bb.getShort(vtablePos) & 0xFFFF;
            int tblLen = bb.getShort(vtablePos + 2) & 0xFFFF;
            // Empty table: vtable has only vt_len + tbl_len headers (4 bytes).
            Assert.assertEquals(4, vtLen);
            // tbl_len covers the 4-byte soffset only.
            Assert.assertEquals(4, tblLen);
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testTableWithInt32AndInt64Fields() {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            w.startTable(2);
            // Field 1: int64 at voffset 6 (field index 1 × 2 + 4 = 6)
            w.prependInt64(0x11_22_33_44_55_66_77_88L);
            w.slot(1, w.cursorFromEnd());
            // Field 0: int32 at voffset 4
            w.prependInt32(0x0A_0B_0C_0D);
            w.slot(0, w.cursorFromEnd());
            int table = w.endTable();
            w.finish(table);
            byte[] bytes = extract(w);
            ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int tablePos = bb.getInt(0);
            int soffset = bb.getInt(tablePos);
            int vtablePos = tablePos - soffset;
            int vtLen = bb.getShort(vtablePos) & 0xFFFF;
            int tblLen = bb.getShort(vtablePos + 2) & 0xFFFF;
            // vt_length = 4 header bytes + 2 fields × 2 bytes = 8.
            Assert.assertEquals(8, vtLen);
            // table body size: soffset(4) + int32(4) + maybe padding + int64(8).
            // Arrow/Flatbuffers lays out field 0 first then field 1, but
            // since we prepended int64 first and int32 second, the int32
            // lives closer to the soffset. Read vtable entries to find
            // voffsets.
            int voffset0 = bb.getShort(vtablePos + 4) & 0xFFFF;
            int voffset1 = bb.getShort(vtablePos + 6) & 0xFFFF;
            int v0 = bb.getInt(tablePos + voffset0);
            long v1 = bb.getLong(tablePos + voffset1);
            Assert.assertEquals(0x0A_0B_0C_0D, v0);
            Assert.assertEquals(0x11_22_33_44_55_66_77_88L, v1);
            Assert.assertTrue("table body length must be at least large enough for both fields",
                    tblLen >= 4 + 4 + 8);
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testWriteString() {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] utf8 = "col1".getBytes();
            long src = Unsafe.malloc(utf8.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < utf8.length; i++) {
                    Unsafe.getUnsafe().putByte(src + i, utf8[i]);
                }
                FbWriter w = new FbWriter();
                w.of(buf, buf + BUF);
                int strOff = w.writeString(src, utf8.length);
                w.finish(strOff);
                byte[] bytes = extract(w);
                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                int strPos = bb.getInt(0);
                int len = bb.getInt(strPos);
                Assert.assertEquals(utf8.length, len);
                for (int i = 0; i < utf8.length; i++) {
                    Assert.assertEquals(utf8[i], bytes[strPos + 4 + i]);
                }
                // null terminator follows.
                Assert.assertEquals(0, bytes[strPos + 4 + utf8.length]);
            } finally {
                Unsafe.free(src, utf8.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testVectorOfInt32() {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            int[] values = {1, 2, 3, 4};
            w.startVector(4, values.length, 4);
            for (int i = values.length - 1; i >= 0; i--) {
                w.prependInt32(values[i]);
            }
            int vec = w.endVector(values.length);
            w.finish(vec);
            byte[] bytes = extract(w);
            ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int vecPos = bb.getInt(0);
            int len = bb.getInt(vecPos);
            Assert.assertEquals(values.length, len);
            for (int i = 0; i < values.length; i++) {
                Assert.assertEquals(values[i], bb.getInt(vecPos + 4 + i * 4));
            }
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] extract(FbWriter w) {
        int len = w.finishedLen();
        byte[] bytes = new byte[len];
        long addr = w.finishedAddr();
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return bytes;
    }
}
