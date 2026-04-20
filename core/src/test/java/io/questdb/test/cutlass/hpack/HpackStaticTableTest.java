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

import io.questdb.cutlass.hpack.HpackStaticTable;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class HpackStaticTableTest {

    @Test
    public void testExactLookupsForStatusChain() {
        assertEntryMatchesIndex(8, ":status", "200");
        assertEntryMatchesIndex(9, ":status", "204");
        assertEntryMatchesIndex(10, ":status", "206");
        assertEntryMatchesIndex(11, ":status", "304");
        assertEntryMatchesIndex(12, ":status", "400");
        assertEntryMatchesIndex(13, ":status", "404");
        assertEntryMatchesIndex(14, ":status", "500");
    }

    @Test
    public void testFirstIndexWithNameReturnsLowest() {
        Assert.assertEquals(2, lookupName(":method"));
        Assert.assertEquals(8, lookupName(":status"));
        Assert.assertEquals(4, lookupName(":path"));
        Assert.assertEquals(6, lookupName(":scheme"));
        Assert.assertEquals(1, lookupName(":authority"));
        Assert.assertEquals(16, lookupName("accept-encoding"));
        Assert.assertEquals(32, lookupName("cookie"));
        Assert.assertEquals(61, lookupName("www-authenticate"));
    }

    @Test
    public void testIndexOfNameValueMisses() {
        Assert.assertEquals(-1, lookupNameValue(":method", "PUT"));
        Assert.assertEquals(-1, lookupNameValue(":status", "201"));
        Assert.assertEquals(-1, lookupNameValue("unknown-header", "whatever"));
    }

    @Test
    public void testNameChainMethod() {
        int idx = lookupName(":method");
        Assert.assertEquals(2, idx);
        int next = HpackStaticTable.nextIndexWithSameName(idx);
        Assert.assertEquals(3, next);
        Assert.assertEquals(-1, HpackStaticTable.nextIndexWithSameName(next));
    }

    @Test
    public void testNameChainStatus() {
        // :status chain covers indices 8..14 in order.
        int idx = lookupName(":status");
        Assert.assertEquals(8, idx);
        for (int expected = 9; expected <= 14; expected++) {
            idx = HpackStaticTable.nextIndexWithSameName(idx);
            Assert.assertEquals(expected, idx);
        }
        Assert.assertEquals(-1, HpackStaticTable.nextIndexWithSameName(idx));
    }

    @Test
    public void testSingleAppearanceNames() {
        Assert.assertEquals(-1, HpackStaticTable.nextIndexWithSameName(1));   // :authority
        Assert.assertEquals(-1, HpackStaticTable.nextIndexWithSameName(16));  // accept-encoding
        Assert.assertEquals(-1, HpackStaticTable.nextIndexWithSameName(61));  // www-authenticate
    }

    @Test
    public void testUnknownNameReturnsMinusOne() {
        Assert.assertEquals(-1, lookupName("x-custom-header"));
        Assert.assertEquals(-1, lookupName("grpc-status"));
    }

    private static void assertEntryMatchesIndex(int index, String expectedName, String expectedValue) {
        assertMatches(expectedName, HpackStaticTable.nameAddr(index), HpackStaticTable.nameLen(index));
        assertMatches(expectedValue, HpackStaticTable.valueAddr(index), HpackStaticTable.valueLen(index));

        long nameBuf = Unsafe.malloc(expectedName.length(), MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(Math.max(1, expectedValue.length()), MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] nameBytes = expectedName.getBytes();
            for (int i = 0; i < nameBytes.length; i++) {
                Unsafe.getUnsafe().putByte(nameBuf + i, nameBytes[i]);
            }
            byte[] valueBytes = expectedValue.getBytes();
            for (int i = 0; i < valueBytes.length; i++) {
                Unsafe.getUnsafe().putByte(valueBuf + i, valueBytes[i]);
            }
            Assert.assertEquals(index, HpackStaticTable.indexOfNameValue(
                    nameBuf, nameBytes.length, valueBuf, valueBytes.length));
        } finally {
            Unsafe.free(nameBuf, expectedName.length(), MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, Math.max(1, expectedValue.length()), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertMatches(String expected, long addr, int len) {
        byte[] bytes = expected.getBytes();
        Assert.assertEquals(bytes.length, len);
        for (int i = 0; i < len; i++) {
            Assert.assertEquals("mismatch at index " + i, bytes[i], Unsafe.getUnsafe().getByte(addr + i));
        }
    }

    private static int lookupName(String name) {
        byte[] bytes = name.getBytes();
        long buf = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, bytes[i]);
            }
            return HpackStaticTable.firstIndexWithName(buf, bytes.length);
        } finally {
            Unsafe.free(buf, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int lookupNameValue(String name, String value) {
        byte[] n = name.getBytes();
        byte[] v = value.getBytes();
        long nameBuf = Unsafe.malloc(n.length, MemoryTag.NATIVE_DEFAULT);
        long valueBuf = Unsafe.malloc(Math.max(1, v.length), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < n.length; i++) {
                Unsafe.getUnsafe().putByte(nameBuf + i, n[i]);
            }
            for (int i = 0; i < v.length; i++) {
                Unsafe.getUnsafe().putByte(valueBuf + i, v[i]);
            }
            return HpackStaticTable.indexOfNameValue(nameBuf, n.length, valueBuf, v.length);
        } finally {
            Unsafe.free(nameBuf, n.length, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(valueBuf, Math.max(1, v.length), MemoryTag.NATIVE_DEFAULT);
        }
    }
}
