/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.sql.async;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.async.AsyncPrefetchManager;
import io.questdb.std.IOURingFacadeImpl;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class AsyncPrefetchManagerTest extends AbstractTest {

    @Test
    public void testAsyncModeDisabledWhenNotLinux() throws Exception {
        Assume.assumeFalse(Os.isLinux());

        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public boolean isSqlJitPrefetchAsync() {
                    return true;  // Try to enable async
                }
            };

            try (AsyncPrefetchManager manager = new AsyncPrefetchManager(configuration)) {
                // Should fall back to sync on non-Linux
                Assert.assertFalse(manager.isAsyncEnabled());
            }
        });
    }

    @Test
    public void testAsyncModeDisabledWhenConfiguredOff() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public boolean isSqlJitPrefetchAsync() {
                    return false;  // Explicitly disable async
                }
            };

            try (AsyncPrefetchManager manager = new AsyncPrefetchManager(configuration)) {
                // Should be sync mode when explicitly disabled
                Assert.assertFalse(manager.isAsyncEnabled());
            }
        });
    }

    @Test
    public void testAsyncModeEnabledOnLinuxWithIoUring() throws Exception {
        Assume.assumeTrue(Os.isLinux());
        Assume.assumeTrue(IOURingFacadeImpl.INSTANCE.isAvailable());
        Assume.assumeTrue(IOURingFacadeImpl.INSTANCE.isMadviseSupported());

        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public boolean isSqlJitPrefetchAsync() {
                    return true;
                }

                @Override
                public int getSqlJitPrefetchRingCapacity() {
                    return 64;
                }

                @Override
                public long getSqlJitPrefetchMaxChunkBytes() {
                    return 64 * Numbers.SIZE_1MB;
                }
            };

            try (AsyncPrefetchManager manager = new AsyncPrefetchManager(configuration)) {
                // Should be async mode on Linux with io_uring
                Assert.assertTrue(manager.isAsyncEnabled());
            }
        });
    }

    @Test
    public void testDrainCompletionsNoOp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public boolean isSqlJitPrefetchAsync() {
                    return false;  // Use sync mode
                }
            };

            try (AsyncPrefetchManager manager = new AsyncPrefetchManager(configuration)) {
                // drainCompletions should be safe to call even in sync mode
                manager.drainCompletions();
            }
        });
    }

    @Test
    public void testCloseTwiceSafe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public boolean isSqlJitPrefetchAsync() {
                    return false;
                }
            };

            AsyncPrefetchManager manager = new AsyncPrefetchManager(configuration);
            manager.close();
            // Second close should be safe
            manager.close();
        });
    }
}
