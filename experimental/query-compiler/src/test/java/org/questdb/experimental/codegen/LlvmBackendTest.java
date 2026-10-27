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

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.std.Unsafe;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class LlvmBackendTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void preservesSemanticsAtEveryOptimizationLevel() throws Exception {
        Builder b = new Builder();
        Var value = b.let(load(COLUMNS)), index = b.let(i64(0)), total = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.poll(); // Phi inputs must name the continuation after this split block.
            loop.assign(total, add(total, binary(Op.XOR, binary(Op.MUL, value, i64(3)), binary(Op.USHR, value, index))));
            loop.assign(index, add(index, i64(1)));
            loop.poll();
        });
        b.store(STATE, total);
        b.store(MemoryType.I8, at(STATE, 8), value);
        b.store(MemoryType.I16, at(STATE, 9), value);
        b.store(MemoryType.I32, at(STATE, 11), value);
        b.exit(0);
        long before = LlvmBackend.liveFunctions();
        for (int level = 0; level <= 3; level++) {
            try (var kernel = new LlvmBackend(level, true).compile(new Program(b.block()), temporary.getRoot().toPath());
                 var memory = KernelMemory.open(new FastGroupByAllocator(1024, 1048576)); Arena arena = Arena.ofConfined()) {
                long input = arena.allocate(8, 8).address(), output = arena.allocate(16, 8).address();
                for (long v : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, -1, 0, 7, 255, 65535}) for (int n : new int[]{0, 1, 2, 65}) {
                    Unsafe.getUnsafe().putLong(input, v); long expected = 0;
                    for (int i = 0; i < n; i++) expected += (v * 3) ^ (v >>> i);
                    assertEquals(0, kernel.run(input, n, output)); memory.throwIfFailed();
                    assertEquals(expected, Unsafe.getUnsafe().getLong(output));
                    assertEquals((byte)v, Unsafe.getUnsafe().getByte(output + 8));
                    assertEquals((short)v, Unsafe.getUnsafe().getShort(output + 9));
                    assertEquals((int)v, Unsafe.getUnsafe().getInt(output + 11));
                }
                assertTrue(Files.size(kernel.artifacts().resolve("query.ll")) > 0);
                assertTrue(Files.size(kernel.artifacts().resolve("optimized.ll")) > 0);
            }
            assertEquals(before, LlvmBackend.liveFunctions());
        }
    }
    @Test public void rejectsInvalidModulesAndRecoversWithoutLeakingCode() throws Exception {
        long before = LlvmBackend.liveFunctions();
        String signature = "define i64 @query(i64 %a, i64 %b, i64 %c, i64 %d, i64 %e, i64 %f)";
        for (String source : new String[]{"", "not llvm", "define i32 @query() { ret i32 0 }",
                signature + " { entry: %x = add i64 %y, 1\n %y = add i64 %b, 2\n ret i64 %x }",
                "declare i64 @questdb_missing_llvm_test_symbol()\n" + signature + " { %x = call i64 @questdb_missing_llvm_test_symbol()\n ret i64 %x }"}) {
            assertThrows(IOException.class, () -> LlvmBackend.compileText(source, 2, null));
            assertEquals(before, LlvmBackend.liveFunctions());
        }
        assertThrows(IOException.class, () -> LlvmBackend.compileText(signature + " { ret i64 0 }", 4, null));
        // These Java buffers contain exactly the specified bytes, without a trailing NUL.
        for (int padding : new int[]{0, 1, 7, 63, 4095}) {
            var code = LlvmBackend.compileText(signature + " { ret i64 0 }" + " ".repeat(padding), 2, null);
            assertEquals(before + 1, LlvmBackend.liveFunctions()); LlvmBackend.release(code.handle());
        }
        assertEquals(before, LlvmBackend.liveFunctions());
    }
    @Test public void ownsIndependentCodeAndCacheLeases() throws Exception {
        Builder a = new Builder(); a.exit(17); Builder b = new Builder(); b.exit(29);
        long before = LlvmBackend.liveFunctions();
        try (var cache = new KernelCache(new LlvmBackend(), 1)) {
            var first = cache.compile(new Program(a.block()), temporary.getRoot().toPath());
            try (first; var second = cache.compile(new Program(b.block()), temporary.getRoot().toPath())) {
                assertEquals(before + 2, LlvmBackend.liveFunctions());
                assertEquals(17, first.run(0, 0, 0)); assertEquals(29, second.run(0, 0, 0));
                cache.close(); // Eviction/close must retain code while either lease is live.
                assertEquals(17, first.run(0, 0, 0)); assertEquals(29, second.run(0, 0, 0));
            }
            first.close(); assertThrows(IllegalStateException.class, () -> first.run(0, 0, 0));
        }
        assertEquals(before, LlvmBackend.liveFunctions());
    }
    @Test public void cancellationExitsBeforeLaterStores() throws Exception {
        Builder b = new Builder(); b.poll(); b.store(STATE, i64(99)); b.exit(0);
        try (var kernel = new LlvmBackend().compile(new Program(b.block()), temporary.getRoot().toPath())) {
            assertEquals(4, kernel.run(0, 0, 0)); // No KernelMemory binding: poll fails, the null store stays unreachable.
        }
    }
}
