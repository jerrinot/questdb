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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Misc;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class SimpleMapValueTest {
    @Test
    public void testCacheLineIsolation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SimpleMapValue[] values = new SimpleMapValue[32];
            HashSet<Long> occupiedLines = new HashSet<>();
            try {
                for (int i = 0; i < values.length; i++) {
                    int columns = i % 9 + 1;
                    SimpleMapValue value = values[i] = new SimpleMapValue(columns);
                    long address = value.getAddress(0);
                    Assert.assertEquals(0, address % Misc.CACHE_LINE_SIZE);
                    long lastLine = (value.getAddress(columns - 1) + 31) / Misc.CACHE_LINE_SIZE;
                    for (long line = address / Misc.CACHE_LINE_SIZE; line <= lastLine; line++) {
                        Assert.assertTrue("independent values share a cache line", occupiedLines.add(line));
                    }
                }
            } finally {
                for (SimpleMapValue value : values) {
                    Misc.free(value);
                }
            }
        });
    }

    @Test
    public void testCopyAndClearWideValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Long256Impl input = new Long256Impl();
            for (int columns = 0; columns <= 9; columns++) {
                try (SimpleMapValue source = new SimpleMapValue(columns);
                     SimpleMapValue copy = new SimpleMapValue(columns)) {
                    for (int i = 0; i < columns; i++) {
                        input.setAll(i + 1, i + 2, i + 3, i + 4);
                        source.putLong256(i, input);
                    }
                    copy.copy(source);
                    source.clear();
                    for (int i = 0; i < columns; i++) {
                        Long256 value = copy.getLong256A(i);
                        Assert.assertEquals(i + 1, value.getLong0());
                        Assert.assertEquals(i + 2, value.getLong1());
                        Assert.assertEquals(i + 3, value.getLong2());
                        Assert.assertEquals(i + 4, value.getLong3());
                        value = source.getLong256A(i);
                        Assert.assertEquals(0, value.getLong0());
                        Assert.assertEquals(0, value.getLong1());
                        Assert.assertEquals(0, value.getLong2());
                        Assert.assertEquals(0, value.getLong3());
                    }
                }
            }
        });
    }
}
