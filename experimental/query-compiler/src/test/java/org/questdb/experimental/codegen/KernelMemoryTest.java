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

package org.questdb.experimental.codegen;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.std.datetime.millitime.MillisecondClock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class KernelMemoryTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void nativeCallbacksThrottleOnlyConnectionChecksAndReleaseFailedScopes() throws Exception {
        AtomicLong clock = new AtomicLong(1_000);
        var config = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override public MillisecondClock getClock() { return clock::get; }
            @Override public long getQueryTimeout() { return 1_000; }
            @Override public long getCircuitBreakerConnectionCheckThrottle() { return 100; }
        };
        try (var engine = new CairoEngine(new DefaultCairoConfiguration(temporary.newFolder().toString()));
             var allocator = new FastGroupByAllocator(64, 1 << 20);
             var breaker = new NetworkSqlExecutionCircuitBreaker(engine, config) {
                 int probes;
                 boolean disconnected;
                 @Override protected boolean testConnection(long fd) { probes++; return disconnected; }
             }) {
            var scope = KernelMemory.reusable(allocator);
            breaker.of(7);
            breaker.resetTimer();
            scope.enter(breaker);
            try (scope) {
                for (int i = 0; i < 1_000; i++) assertEquals(0, KernelMemory.poll());
                long address = KernelMemory.malloc(8);
                assertNotEquals(0, address);
                address = KernelMemory.realloc(address, 8, 16);
                assertNotEquals(0, address);
                KernelMemory.free(address, 16);
                assertEquals("Allocation and poll share the breaker's connection window", 1, breaker.probes);
                breaker.cancel();
                assertEquals(1, KernelMemory.poll());
                assertThrows(CairoException.class, scope::throwIfFailed);
                assertEquals("Cancellation never waits for the socket window", 1, breaker.probes);
            }
            assertThrows(IllegalStateException.class, KernelMemory::allocator);

            breaker.resetTimer();
            scope.enter(breaker);
            try (scope) {
                assertEquals(0, KernelMemory.poll());
                long address = KernelMemory.malloc(8);
                int probes = breaker.probes;
                clock.addAndGet(1_001);
                assertEquals("Reallocation also checks timeout", 0, KernelMemory.realloc(address, 8, 16));
                assertEquals("A failed scope refuses later allocations", 0, KernelMemory.malloc(8));
                assertThrows(CairoException.class, scope::throwIfFailed);
                assertEquals("Timeout is checked before any socket syscall", probes, breaker.probes);
            }
            breaker.resetTimer();
            scope.enter(breaker);
            try (scope) {
                assertEquals(0, KernelMemory.poll());
                breaker.disconnected = true;
                clock.addAndGet(99);
                assertEquals(0, KernelMemory.poll());
                clock.incrementAndGet();
                assertEquals(1, KernelMemory.poll());
                assertThrows(CairoException.class, scope::throwIfFailed);
            }
            assertThrows(IllegalStateException.class, KernelMemory::allocator);
            assertNotEquals("Borrowed allocator survives failed native scopes", 0, allocator.malloc(8));
        }
    }
}
