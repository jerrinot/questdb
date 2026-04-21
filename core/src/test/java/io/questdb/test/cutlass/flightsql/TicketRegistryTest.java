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

import io.questdb.cutlass.flightsql.server.TicketRegistry;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class TicketRegistryTest {

    @Test
    public void testAcquireReleaseCycle() {
        try (TicketRegistry r = new TicketRegistry(2)) {
            long t1 = r.acquire();
            long t2 = r.acquire();
            Assert.assertEquals(1L, t1);
            Assert.assertEquals(2L, t2);
            Assert.assertEquals(2, r.inUseCount());
            r.release(t1);
            Assert.assertEquals(1, r.inUseCount());
            long t3 = r.acquire();
            Assert.assertEquals(3L, t3);
            Assert.assertNotNull(r.entryById(t3));
            Assert.assertNull(r.entryById(t1));
        }
    }

    @Test
    public void testCapExhaustionReturnsMinusOne() {
        try (TicketRegistry r = new TicketRegistry(1)) {
            long t1 = r.acquire();
            Assert.assertEquals(1L, t1);
            Assert.assertEquals(-1L, r.acquire());
            r.release(t1);
            long t2 = r.acquire();
            Assert.assertEquals(2L, t2);
        }
    }

    @Test
    public void testEntryByIdIgnoresReleasedTicket() {
        try (TicketRegistry r = new TicketRegistry(4)) {
            long t = r.acquire();
            Assert.assertNotNull(r.entryById(t));
            r.release(t);
            Assert.assertNull(r.entryById(t));
            Assert.assertNull(r.entryById(999));
            Assert.assertNull(r.entryById(-5));
        }
    }

    @Test
    public void testMonotonicIdsAcrossReuse() {
        try (TicketRegistry r = new TicketRegistry(1)) {
            for (int i = 1; i <= 5; i++) {
                long t = r.acquire();
                Assert.assertEquals((long) i, t);
                r.release(t);
            }
        }
    }

    @Test
    public void testSchemaAttachmentFreesOnRelease() {
        long schemaAddr = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try (TicketRegistry r = new TicketRegistry(2)) {
            long t = r.acquire();
            TicketRegistry.TicketEntry e = r.entryById(t);
            Assert.assertNotNull(e);
            e.setSchema(schemaAddr, 32, 32, 24, MemoryTag.NATIVE_DEFAULT);
            Assert.assertEquals(schemaAddr, e.getSchemaAddr());
            Assert.assertEquals(32, e.getSchemaLen());
            Assert.assertEquals(24, e.getRawSchemaLen());
            // release must free the schema memory
            r.release(t);
            Assert.assertNull(r.entryById(t));
            // schemaAddr was freed inside the entry; null our handle to
            // avoid the double-free in the finally block
            schemaAddr = 0;
        } finally {
            if (schemaAddr != 0) {
                Unsafe.free(schemaAddr, 32, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
