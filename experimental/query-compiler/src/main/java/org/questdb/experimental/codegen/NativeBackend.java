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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.questdb.experimental.codegen.Ir.*;

/** IR -> portable C -> shared library, called through the JDK foreign-function API. */
public final class NativeBackend implements Backend {
    private final String compiler;

    public NativeBackend() { this("cc"); }

    /** One executable path, not a shell command. */
    public NativeBackend(String compiler) { this.compiler = compiler; }

    @Override
    public String name() { return "native"; }

    @Override
    public CompiledKernel compile(Program program, Path artifactRoot) throws IOException {
        long start = System.nanoTime();
        var prepared = IrOptimizer.prepare(PortableVectorLowering.lower(HelperInlining.expand(program)));
        if (!System.getProperty("os.name").equals("Linux") || ValueLayout.ADDRESS.byteSize() != 8) {
            throw new IOException("The native prototype currently supports 64-bit Linux");
        }
        Files.createDirectories(artifactRoot);
        Path directory = Files.createTempDirectory(artifactRoot, "native-").toAbsolutePath();
        Path source = directory.resolve("query.c");
        Path library = directory.resolve("query.so");
        Path log = directory.resolve("compiler.log");
        Files.writeString(source, preamble() + "int64_t query(uint64_t v0, int64_t v1, uint64_t v2, uint64_t alloc_fn, uint64_t free_fn, uint64_t poll_fn, uint64_t realloc_fn) {\n"
                + "    (void)v0; (void)v1; (void)v2; (void)alloc_fn; (void)free_fn; (void)poll_fn; (void)realloc_fn;\n"
                + (prepared.ssa() == null ? new Emitter().body(prepared.structured()) : new Emitter().body(prepared.ssa())) + "}\n");
        Files.writeString(directory.resolve("query.ir"), prepared.dump());
        List<String> command = List.of(compiler, "-std=c11", "-O3", "-ffp-contract=off", "-fwrapv", "-fPIC", "-shared",
                "-Wall", "-Wextra", source.toString(), "-o", library.toString());
        Files.writeString(directory.resolve("command.txt"), String.join("\n", command) + "\n");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor();
                throw new IOException("Native compilation timed out: " + log);
            }
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IOException("Native compilation interrupted", e);
        }
        if (process.exitValue() != 0) {
            throw new IOException("Native compilation failed: " + log + "\n" + Files.readString(log));
        }
        Arena arena = Arena.ofShared();
        try {
            var address = SymbolLookup.libraryLookup(library, arena).find("query").orElseThrow();
            Kernel kernel = NativeCalls.bind(address, arena);
            return new CompiledKernel(kernel, arena, directory, System.nanoTime() - start);
        } catch (ReflectiveOperationException e) {
            arena.close();
            throw new IOException("Could not bind allocator callbacks", e);
        } catch (RuntimeException | Error e) {
            arena.close();
            throw e;
        }
    }

    private static String preamble() {
        StringBuilder source = new StringBuilder("""
                #include <stdint.h>
                #include <stdbool.h>
                #include <string.h>
                static inline double query_f64(uint64_t bits) { double v; memcpy(&v, &bits, 8); return v; }
                static inline int64_t query_ctz(uint64_t v) { return v ? __builtin_ctzll(v) : 64; }
                static inline int64_t query_cas(uint64_t p, int64_t expected, int64_t value) {
                    __atomic_compare_exchange_n((int64_t*)(uintptr_t)p, &expected, value, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
                    return expected;
                }
                _Static_assert(sizeof(void*) == 8, "64-bit addresses required");
                """);
        for (MemoryType width : MemoryType.values()) {
            String cType = width == MemoryType.F64 ? "double" : width == MemoryType.ADDRESS ? "uint64_t" : "int" + width.bytes * 8 + "_t";
            source.append("static inline ").append(cType).append(" load_").append(width)
                    .append("(uint64_t p) { ").append(cType)
                    .append(" v; memcpy(&v, (const void*)(uintptr_t)p, sizeof(v)); return v; }\n");
            source.append("static inline void store_").append(width).append("(uint64_t p, ")
                    .append(width == MemoryType.F64 ? "double" : width == MemoryType.ADDRESS ? "uint64_t" : "int64_t").append(" value) { ")
                    .append(cType).append(" v = (").append(cType)
                    .append(")value; memcpy((void*)(uintptr_t)p, &v, sizeof(v)); }\n");
        }
        return source.toString();
    }

    private static final class Emitter extends SourceEmitter {
        @Override String atomicLoad(String address, boolean acquire) { return "__atomic_load_n((int64_t*)(uintptr_t)(" + address + "), " + (acquire ? "__ATOMIC_ACQUIRE" : "__ATOMIC_RELAXED") + ")"; }
        @Override String atomicStore(String address, String value) { return "__atomic_store_n((int64_t*)(uintptr_t)(" + address + "), " + value + ", __ATOMIC_RELEASE)"; }
        @Override String atomicAdd(String address, String value) { return "__atomic_fetch_add((int64_t*)(uintptr_t)(" + address + "), " + value + ", __ATOMIC_RELAXED)"; }
        @Override String compareExchange(String address, String expected, String value) { return "query_cas(" + address + ", " + expected + ", " + value + ")"; }
        @Override String prefetch(String address, boolean write) {
            return "__builtin_prefetch((const void*)(uintptr_t)(" + address + "), " + (write ? 1 : 0) + ", 3)";
        }
        @Override String trailingZeros(String value) { return "query_ctz((uint64_t)(" + value + "))"; }
        @Override
        String binary(Op op, String left, String right) {
            if (op == Op.USHR) {
                return "((int64_t)((uint64_t)(" + left + ") >> ((" + right + ") & 63)))";
            }
            return super.binary(op, left, right);
        }

        @Override
        String literal(Literal l) {
            if (l.type() == Type.F64) return "query_f64(UINT64_C(0x" + Long.toHexString(l.value()) + "))";
            if (l.type() == Type.BOOL) { return Boolean.toString(l.value() != 0); }
            if (l.type() == Type.ADDRESS) { return "UINT64_C(0)"; }
            if (l.value() == Long.MIN_VALUE) { return "INT64_MIN"; }
            return l.value() < 0 ? "(-INT64_C(" + -l.value() + "))" : "INT64_C(" + l.value() + ")";
        }

        @Override
        String load(MemoryType width, String address) {
            // All integer loads widen immediately, so C's integer promotions cannot
            // introduce 32-bit overflow that differs from the IR's 64-bit arithmetic.
            return "((" + type(width.resultType) + ")load_" + width + "(" + address + "))";
        }

        @Override
        String offset(String address, String bytes) { return "(" + address + " + (uint64_t)(" + bytes + "))"; }
        @Override
        String store(MemoryType width, String address, String value) { return "store_" + width + "(" + address + ", " + value + ")"; }
        @Override
        String allocate(String bytes) { return "((uint64_t (*)(int64_t))(uintptr_t)alloc_fn)(" + bytes + ")"; }
        String reallocate(String address, String oldBytes, String newBytes) {
            return "((uint64_t (*)(uint64_t, int64_t, int64_t))(uintptr_t)realloc_fn)(" + address + ", " + oldBytes + ", " + newBytes + ")";
        }
        @Override
        String free(String address, String bytes) { return "((void (*)(uint64_t, int64_t))(uintptr_t)free_fn)(" + address + ", " + bytes + ")"; }
        @Override
        String poll() { return "((int64_t (*)(void))(uintptr_t)poll_fn)()"; }
        @Override
        String type(Type type) {
            return switch (type) {
                case BOOL -> "bool";
                case I64 -> "int64_t";
                case F64 -> "double";
                case ADDRESS -> "uint64_t";
            };
        }
    }
}
