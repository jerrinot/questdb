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

package io.questdb.test.cutlass.hpack;

import io.questdb.cutlass.hpack.Hpack;
import io.questdb.cutlass.hpack.HpackDynamicTable;
import io.questdb.cutlass.hpack.HpackException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class HpackDynamicTableTest {

    @Test
    public void testEvictOnCapLower() {
        HpackDynamicTable table = new HpackDynamicTable(4096, 4096);
        try {
            insertLiteral(table, "a", "1");       // cost 34
            insertLiteral(table, "bb", "22");     // cost 36
            insertLiteral(table, "ccc", "333");   // cost 38
            Assert.assertEquals(34 + 36 + 38, table.currentSize());

            // Lower cap to 70: must evict two oldest (34, 36).
            table.setOperatingCap(70);
            Assert.assertEquals(38, table.currentSize());
            Assert.assertEquals(1, table.dynamicCount());
            assertEntry(table, HpackDynamicTable.FIRST_DYNAMIC_WIRE_INDEX, "ccc", "333");
        } finally {
            table.close();
        }
    }

    @Test
    public void testInsertEvictsOldestWhenCapExceeded() {
        // Cap is 100 bytes; entry cost is 32 + 3 + 5 = 40. Three entries cost 120 > 100,
        // so inserting the third must evict the first.
        HpackDynamicTable table = new HpackDynamicTable(100, 4096);
        try {
            insertLiteral(table, "aaa", "value");
            insertLiteral(table, "bbb", "valve");
            insertLiteral(table, "ccc", "vault");
            Assert.assertEquals(2, table.dynamicCount());
            Assert.assertEquals(80, table.currentSize());
            // Newest at 62, oldest at 63.
            assertEntry(table, 62, "ccc", "vault");
            assertEntry(table, 63, "bbb", "valve");
        } finally {
            table.close();
        }
    }

    @Test
    public void testInsertWireIndexing() {
        HpackDynamicTable table = new HpackDynamicTable(4096, 4096);
        try {
            insertLiteral(table, "a", "1");
            insertLiteral(table, "bb", "22");
            insertLiteral(table, "ccc", "333");

            // Per RFC 7541 sec. 2.3.3: newest entry at index 62; oldest at 61 + count.
            assertEntry(table, 62, "ccc", "333");
            assertEntry(table, 63, "bb", "22");
            assertEntry(table, 64, "a", "1");
        } finally {
            table.close();
        }
    }

    @Test
    public void testNameReferenceHazardSurvivesEviction() {
        // Pool sized tight so inserting forces evictions. Use an indexed-name reference
        // where the name source lives inside the table and gets evicted by the insert.
        HpackDynamicTable table = new HpackDynamicTable(4096, 4096);
        try {
            // Entry "x-custom-name" / "value-A".
            insertLiteral(table, "x-custom-name", "value-A");

            // Build the second insert as an indexed-name reuse of entry 62's name plus a new value.
            long name62Addr = table.entryNameAddr(62);
            int name62Len = table.entryNameLen(62);
            long newValueAddr = nativeCopy("value-B");
            int newValueLen = "value-B".length();

            // Lower cap so the next insert MUST evict entry 62 (whose name we're referencing).
            // Cost of new entry = 32 + 13 + 7 = 52. Cap set to 52 — exactly one entry.
            table.setOperatingCap(52);
            Assert.assertEquals(1, table.dynamicCount());

            try {
                // Insert with the indexed name reference. Step-1 staging must copy the name
                // into the private buffer BEFORE eviction overwrites it.
                table.insert(name62Addr, name62Len, newValueAddr, newValueLen);
                Assert.assertEquals(1, table.dynamicCount());
                assertEntry(table, 62, "x-custom-name", "value-B");
            } finally {
                Unsafe.free(newValueAddr, newValueLen, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            table.close();
        }
    }

    @Test
    public void testOversizedEntryClearsTable() {
        HpackDynamicTable table = new HpackDynamicTable(100, 4096);
        try {
            insertLiteral(table, "aa", "bb");
            insertLiteral(table, "cc", "dd");
            Assert.assertEquals(2, table.dynamicCount());

            // Single entry cost 32 + 60 + 40 = 132 > cap 100. Table must clear.
            long nameAddr = nativeCopy(repeat('n', 60));
            long valueAddr = nativeCopy(repeat('v', 40));
            try {
                table.insert(nameAddr, 60, valueAddr, 40);
            } finally {
                Unsafe.free(nameAddr, 60, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(valueAddr, 40, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertEquals(0, table.dynamicCount());
            Assert.assertEquals(0, table.currentSize());
        } finally {
            table.close();
        }
    }

    @Test
    public void testManyWrapsPreserveEntryBytes() {
        // Stress test: many inserts at varied sizes that force multiple pool wraps while the
        // cap is loose enough that sometimes the overlap check (not cap) must drive eviction
        // of a newer entry sitting at a low offset. The invariant asserted is that every
        // reported entry's bytes faithfully match what was inserted — no silent corruption
        // from the wrap/overlap loop.
        int pool = 256;
        HpackDynamicTable table = new HpackDynamicTable(pool, pool);
        try {
            // Known-plaintext marker: the k-th insert writes a name of form "name-k" (unique)
            // and a value whose every byte is (byte) (k & 0x7F). After the burst, walk every
            // live entry, parse k from the name, and verify the value's bytes all match k.
            for (int k = 1; k <= 500; k++) {
                String name = "name-" + k;
                int valueLen = 5 + (k * 37) % 64;  // varied, 5..68 bytes
                byte[] valueBytes = new byte[valueLen];
                java.util.Arrays.fill(valueBytes, (byte) (k & 0x7F));
                byte[] nameBytes = name.getBytes();
                long nameAddr = Unsafe.malloc(nameBytes.length, MemoryTag.NATIVE_DEFAULT);
                long valueAddr = Unsafe.malloc(Math.max(1, valueLen), MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < nameBytes.length; i++) {
                        Unsafe.getUnsafe().putByte(nameAddr + i, nameBytes[i]);
                    }
                    for (int i = 0; i < valueLen; i++) {
                        Unsafe.getUnsafe().putByte(valueAddr + i, valueBytes[i]);
                    }
                    table.insert(nameAddr, nameBytes.length, valueAddr, valueLen);
                } finally {
                    Unsafe.free(nameAddr, nameBytes.length, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(valueAddr, Math.max(1, valueLen), MemoryTag.NATIVE_DEFAULT);
                }

                // After every insert, every live entry must still report intact bytes.
                for (int w = 0; w < table.dynamicCount(); w++) {
                    int wireIdx = HpackDynamicTable.FIRST_DYNAMIC_WIRE_INDEX + w;
                    int nl = table.entryNameLen(wireIdx);
                    byte[] n = new byte[nl];
                    long na = table.entryNameAddr(wireIdx);
                    for (int i = 0; i < nl; i++) {
                        n[i] = Unsafe.getUnsafe().getByte(na + i);
                    }
                    String parsedName = new String(n);
                    Assert.assertTrue("name format at insert " + k + " wire " + wireIdx + ": " + parsedName,
                            parsedName.startsWith("name-"));
                    int entryK = Integer.parseInt(parsedName.substring(5));
                    int vl = table.entryValueLen(wireIdx);
                    long va = table.entryValueAddr(wireIdx);
                    byte expectedVal = (byte) (entryK & 0x7F);
                    for (int i = 0; i < vl; i++) {
                        byte got = Unsafe.getUnsafe().getByte(va + i);
                        Assert.assertEquals("corrupt byte at insert " + k + " wire " + wireIdx
                                        + " entryK=" + entryK + " byte " + i,
                                expectedVal, got);
                    }
                }
            }
        } finally {
            table.close();
        }
    }

    @Test
    public void testWireIndexOutOfRange() {
        HpackDynamicTable table = new HpackDynamicTable(4096, 4096);
        try {
            insertLiteral(table, "a", "1");
            try {
                table.entryNameAddr(63);
                Assert.fail("expected HpackException");
            } catch (HpackException expected) {
                // ok
            }
            try {
                table.entryNameAddr(61);
                Assert.fail("expected HpackException");
            } catch (HpackException expected) {
                // ok
            }
        } finally {
            table.close();
        }
    }

    @Test
    public void testResetRestoresCleanState() {
        HpackDynamicTable table = new HpackDynamicTable(4096, 8192);
        try {
            insertLiteral(table, "aa", "v1");
            insertLiteral(table, "bb", "v2");
            insertLiteral(table, "cc", "v3");
            Assert.assertEquals(3, table.dynamicCount());
            Assert.assertTrue(table.currentSize() > 0);

            table.reset(8192);
            Assert.assertEquals(0, table.dynamicCount());
            Assert.assertEquals(0, table.currentSize());
            Assert.assertEquals(8192, table.currentOperatingCap());

            // Reused instance accepts new inserts under the new cap, and wire indexing
            // starts fresh at 62 with the first post-reset entry.
            insertLiteral(table, "x", "y");
            Assert.assertEquals(1, table.dynamicCount());
            assertEntry(table, 62, "x", "y");
        } finally {
            table.close();
        }
    }

    @Test
    public void testZeroCapInsertShortCircuits() {
        // Milestone 1 encoder posture: selectedMax pinned at 0.
        HpackDynamicTable table = new HpackDynamicTable(0, 4096);
        try {
            insertLiteral(table, "a", "1");
            Assert.assertEquals(0, table.dynamicCount());
            Assert.assertEquals(0, table.currentSize());
        } finally {
            table.close();
        }
    }

    private static void assertEntry(HpackDynamicTable table, int wireIdx, String name, String value) {
        byte[] nameBytes = name.getBytes();
        byte[] valueBytes = value.getBytes();
        Assert.assertEquals(nameBytes.length, table.entryNameLen(wireIdx));
        Assert.assertEquals(valueBytes.length, table.entryValueLen(wireIdx));
        long n = table.entryNameAddr(wireIdx);
        for (int i = 0; i < nameBytes.length; i++) {
            Assert.assertEquals("name[" + i + "]", nameBytes[i], Unsafe.getUnsafe().getByte(n + i));
        }
        long v = table.entryValueAddr(wireIdx);
        for (int i = 0; i < valueBytes.length; i++) {
            Assert.assertEquals("value[" + i + "]", valueBytes[i], Unsafe.getUnsafe().getByte(v + i));
        }
    }

    private static void insertLiteral(HpackDynamicTable table, String name, String value) {
        byte[] nb = name.getBytes();
        byte[] vb = value.getBytes();
        long nameAddr = Unsafe.malloc(Math.max(1, nb.length), MemoryTag.NATIVE_DEFAULT);
        long valueAddr = Unsafe.malloc(Math.max(1, vb.length), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < nb.length; i++) {
                Unsafe.getUnsafe().putByte(nameAddr + i, nb[i]);
            }
            for (int i = 0; i < vb.length; i++) {
                Unsafe.getUnsafe().putByte(valueAddr + i, vb[i]);
            }
            table.insert(nameAddr, nb.length, valueAddr, vb.length);
        } finally {
            Unsafe.free(nameAddr, Math.max(1, nb.length), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueAddr, Math.max(1, vb.length), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long nativeCopy(String s) {
        byte[] b = s.getBytes();
        long addr = Unsafe.malloc(Math.max(1, b.length), MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < b.length; i++) {
            Unsafe.getUnsafe().putByte(addr + i, b[i]);
        }
        return addr;
    }

    private static String repeat(char c, int count) {
        char[] buf = new char[count];
        java.util.Arrays.fill(buf, c);
        return new String(buf);
    }

    // Exercise the Hpack.ENTRY_OVERHEAD constant to pin its value to 32.
    static {
        Assert.assertEquals(32, Hpack.ENTRY_OVERHEAD);
    }
}
