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

import io.questdb.std.Unsafe;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.util.List;

import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class NativeCallTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    public static long mutate(long pointer, long a, long b, long c, long d, long e, long f, long g) {
        long value = Unsafe.getLong(pointer) + a + 2*b + 3*c + 4*d + 5*e + 6*f + 7*g;
        Unsafe.putLong(pointer, value);
        return value;
    }

    @Test
    public void matcherFallbackKeepsCancellationAndReleasesResources() throws Exception {
        assertNull(NativeCapture.Plan.bind("^" + "(?:)?".repeat(30) + "(a)$", 1));
        assertNull(NativeCapture.Plan.bind("^(a*)?(b)$", 1));
        long before = Unsafe.getMemUsedByTag(io.questdb.std.MemoryTag.NATIVE_JIT);
        var plan = NativeCapture.Plan.bind("^(a*)a*b$", 1);
        var cancelled = new IllegalStateException("cancelled capture");
        try (var arena = Arena.ofConfined(); var slot = new NativeCapture.Slot(plan, arena.allocate(16, 8).address())) {
            long input = arena.allocateFrom("aaaaab").address();
            try (var memory = KernelMemory.reusable(() -> { })) {
                memory.enter(null);
                assertEquals(5L << 32, slot.fallback(input, 6));
                memory.throwIfFailed();
            }
            try (var memory = KernelMemory.reusable(() -> { throw cancelled; })) {
                memory.enter(null);
                assertEquals(-3, slot.fallback(input, 6));
                assertSame(cancelled, assertThrows(IllegalStateException.class, memory::throwIfFailed));
            }
        }
        assertEquals(before, Unsafe.getMemUsedByTag(io.questdb.std.MemoryTag.NATIVE_JIT));
    }

    @Test
    public void callsPreserveEffectsArgumentsAndLiveValues() throws Exception {
        try (var arena = Arena.ofConfined()) {
            var handle = MethodHandles.lookup().findStatic(NativeCallTest.class, "mutate",
                    MethodType.methodType(long.class, long.class, long.class, long.class, long.class,
                            long.class, long.class, long.class, long.class));
            var signature = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
            long callback = Linker.nativeLinker().upcallStub(handle, signature, arena).address();
            long data = arena.allocate(24, 8).address();
            Builder b = new Builder();
            Var before = b.let(load(STATE));
            Var floating = b.let(new Load(MemoryType.F64, at(STATE, 8)));
            Expr function = new Offset(nil(), i64(callback));
            var args = List.<Expr>of(STATE, i64(1), i64(2), i64(3), i64(4), i64(5), i64(6), i64(7));
            // Identical calls must neither be CSE'd nor removed when the result is unused.
            b.let(new NativeCall(function, args));
            Var returned = b.let(new NativeCall(function, args));
            Var after = b.let(load(STATE));
            b.store(MemoryType.F64, at(STATE, 16), binary(Op.FADD, floating, f64(0.5)));
            b.exit(add(add(before, after), returned));
            Program program = new Program(b.block());
            Builder nested = new Builder();
            nested.exit(new NativeCall(function, List.of(STATE, load(STATE), new NativeCall(function, args),
                    i64(0), i64(0), i64(0), i64(0), i64(0))));
            var controls = CompilerOptions.PROCESS;
            for (Backend backend : List.of(new LlvmBackend(),
                    new AsmJitBackend(false, controls.withSsa(false)), new AsmJitBackend(false, controls.withSsa(true)),
                    new NativeBackend("cc", controls.withSsa(false)), new NativeBackend("cc", controls.withSsa(true)))) {
                try (var kernel = backend.compile(program, temporary.newFolder().toPath())) {
                    Unsafe.putLong(data, 10);
                    Unsafe.getUnsafe().putDouble(data + 8, 1.25);
                    assertEquals(590, kernel.run(0, 0, data));
                    assertEquals(290, Unsafe.getLong(data));
                    assertEquals(1.75, Unsafe.getUnsafe().getDouble(data + 16), 0);
                    try (var ordered = backend.compile(new Program(nested.block()), temporary.newFolder().toPath())) {
                        Unsafe.putLong(data, 10);
                        assertEquals(460, ordered.run(0, 0, data));
                        assertEquals(460, Unsafe.getLong(data));
                    }
                }
            }
        }
    }
}
