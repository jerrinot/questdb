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

package io.questdb.test.cairo.map;

import io.questdb.PropertyKey;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapMemoryLimitException;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MapMemoryLimitTest extends AbstractCairoTest {
    @Test
    public void testStorageBoundIncludesRehashOverlapAndReopen() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4096);
        setProperty(PropertyKey.CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE, 64);
        TestUtils.assertMemoryLeak(() -> {
            int[] types = {ColumnType.INT, ColumnType.LONG, ColumnType.LONG, ColumnType.STRING, ColumnType.VARCHAR};
            for (int shape = 0; shape < types.length; shape++) {
                ArrayColumnTypes keys = new ArrayColumnTypes();
                keys.add(types[shape]);
                if (shape == 2) {
                    keys.add(ColumnType.INT);
                }
                final long budget = 16 * 1024;
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                     Map map = MapFactory.createUnorderedMap(configuration, keys, new SingleColumnType(ColumnType.LONG), budget)) {
                    map.setMemoryTracker(tracker);
                    for (int reopen = 0; reopen < 2; reopen++) {
                        // This independent guard catches a missed bound on a transient old/new
                        // allocation pair, even if the final resident map fits the budget.
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + budget);
                        int inserted = 0;
                        try {
                            map.reopen();
                            for (int i = 0; i < 10000; i++) {
                                MapKey key = map.withKey();
                                switch (types[shape]) {
                                    case ColumnType.INT -> key.putInt(i);
                                    case ColumnType.LONG -> key.putLong((long) i << 32);
                                    case ColumnType.STRING -> key.putStr("key" + i);
                                    case ColumnType.VARCHAR -> key.putVarchar(new Utf8String("key" + i));
                                    default -> throw new AssertionError();
                                }
                                if (shape == 2) {
                                    key.putInt(i);
                                }
                                key.createValue().putLong(0, i);
                                inserted++;
                                Assert.assertTrue(tracker.getUsed() <= budget);
                            }
                            Assert.fail("expected optional storage refusal");
                        } catch (MapMemoryLimitException expected) {
                            Assert.assertTrue("must exercise growth after opening", inserted > 0);
                            Assert.assertTrue(tracker.getUsed() <= budget);
                        } finally {
                            Unsafe.setRssMemLimit(0);
                            map.close();
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                }
            }
        });
    }

    @Test
    public void testOrderedMapBoundIncludesNativeKeyStaging() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4096);
        TestUtils.assertMemoryLeak(() -> {
            final long budget = 16 * 1024;
            long view = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 Map map = MapFactory.createUnorderedMap(configuration, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), budget)) {
                map.setMemoryTracker(tracker);
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + budget);
                try {
                    map.reopen();
                    OrderedMap ordered = (OrderedMap) map;
                    ordered.writeNativeKeyStagingView(view, 8192);
                    Assert.assertTrue(tracker.getUsed() <= budget);
                    try {
                        ordered.writeNativeKeyStagingView(view, budget);
                        Assert.fail("expected staging storage refusal");
                    } catch (MapMemoryLimitException expected) {
                        Assert.assertTrue(tracker.getUsed() <= budget);
                    }
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            } finally {
                Unsafe.free(view, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testInitialRefusalAndRealAllocationFailureAreDistinct() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 4096);
        TestUtils.assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                 Map map = MapFactory.createUnorderedMap(configuration, new SingleColumnType(ColumnType.STRING), new SingleColumnType(ColumnType.LONG), 1L)) {
                map.setMemoryTracker(tracker);
                try {
                    map.reopen();
                    Assert.fail("expected initial storage refusal");
                } catch (MapMemoryLimitException expected) {
                    Assert.assertEquals(0, tracker.getUsed());
                }
                map.close();
                map.setMemoryLimit(16 * 1024);
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                try {
                    map.reopen();
                    Assert.fail("expected real allocation failure");
                } catch (CairoException expected) {
                    Assert.assertTrue(expected.isOutOfMemory());
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }
        });
    }
}
