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

package io.questdb.test.cutlass.flightsql;

import io.questdb.cutlass.flightsql.server.FlightSqlCallContext;
import io.questdb.cutlass.flightsql.server.FlightSqlCallContextPool;
import io.questdb.std.MemoryTag;
import org.junit.Assert;
import org.junit.Test;

public class FlightSqlCallContextPoolTest {

    @Test
    public void testAcquireReleaseLifecycle() {
        FlightSqlCallContextPool pool = new FlightSqlCallContextPool(3, 4096, MemoryTag.NATIVE_DEFAULT);
        try {
            FlightSqlCallContext a = pool.acquire(1);
            Assert.assertNotNull(a);
            Assert.assertEquals(1, pool.getActiveCount());
            Assert.assertSame(a, pool.lookup(1));

            FlightSqlCallContext b = pool.acquire(3);
            Assert.assertNotNull(b);
            Assert.assertNotSame(a, b);
            Assert.assertEquals(2, pool.getActiveCount());

            pool.release(1);
            Assert.assertNull(pool.lookup(1));
            Assert.assertEquals(1, pool.getActiveCount());

            // Release of unknown stream is a no-op, never throws.
            pool.release(999);
            Assert.assertEquals(1, pool.getActiveCount());
        } finally {
            pool.close();
        }
    }

    @Test
    public void testDoubleAcquireReturnsNull() {
        FlightSqlCallContextPool pool = new FlightSqlCallContextPool(2, 4096, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertNotNull(pool.acquire(5));
            Assert.assertNull("duplicate acquire for same stream must not silently reuse",
                    pool.acquire(5));
        } finally {
            pool.close();
        }
    }

    @Test
    public void testExhaustion() {
        FlightSqlCallContextPool pool = new FlightSqlCallContextPool(2, 4096, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertNotNull(pool.acquire(1));
            Assert.assertNotNull(pool.acquire(3));
            Assert.assertNull(pool.acquire(5));
            pool.release(1);
            FlightSqlCallContext c = pool.acquire(5);
            Assert.assertNotNull(c);
            Assert.assertSame(c, pool.lookup(5));
        } finally {
            pool.close();
        }
    }

    @Test
    public void testLookupUnknownReturnsNull() {
        FlightSqlCallContextPool pool = new FlightSqlCallContextPool(2, 4096, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertNull(pool.lookup(1));
            Assert.assertNull(pool.lookup(0));
        } finally {
            pool.close();
        }
    }
}
