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
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Typed SSA -> LLVM IR -> in-process ORC LLJIT. LLVM owns optimization and machine code. */
public final class LlvmBackend implements Backend {
    private final int optimizationLevel;
    private final boolean dump;

    public LlvmBackend() { this(2, false); }
    public LlvmBackend(int optimizationLevel) { this(optimizationLevel, false); }
    public LlvmBackend(int optimizationLevel, boolean dump) {
        if (optimizationLevel < 0 || optimizationLevel > 3) throw new IllegalArgumentException("LLVM optimization level must be 0..3");
        this.optimizationLevel = optimizationLevel; this.dump = dump;
    }
    @Override public String name() { return "llvm-o" + optimizationLevel; }

    @Override public CompiledKernel compile(Ir.Program program, Path artifacts) throws IOException {
        long started = System.nanoTime();
        if (!System.getProperty("os.name").equals("Linux") || ValueLayout.ADDRESS.byteSize() != 8)
            throw new IOException("The experimental LLVM backend requires 64-bit Linux");
        // Preserve SSA. Our scalar passes and out-of-SSA lowering are deliberately bypassed.
        var graph = SsaProgram.lower(program); SsaOptimizer.verify(graph);
        String source = LlvmEmitter.emit(graph);
        Path directory = null;
        if (dump) {
            Files.createDirectories(artifacts);
            directory = Files.createTempDirectory(artifacts, "llvm-o" + optimizationLevel + "-");
            Files.writeString(directory.resolve("query.ir"), graph.dump());
            Files.writeString(directory.resolve("query.ll"), source);
        }
        return compileSource(source, directory, started);
    }

    /** Shares optimization, JIT, callbacks and ownership with the handwritten C reference. */
    CompiledKernel compileSource(String source, Path directory, long started) throws IOException {
        NativeCode code = compileText(source, optimizationLevel, directory == null ? null : directory.resolve("optimized.ll"));
        Arena callbacks = Arena.ofShared();
        try {
            Kernel kernel = NativeCalls.bind(MemorySegment.ofAddress(code.address()), callbacks);
            AutoCloseable lifetime = () -> { try { release(code.handle()); } finally { callbacks.close(); } };
            return new CompiledKernel(kernel, lifetime, directory, System.nanoTime() - started);
        } catch (ReflectiveOperationException | RuntimeException | Error failure) {
            try { release(code.handle()); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            finally { callbacks.close(); }
            if (failure instanceof Error error) throw error;
            throw new IOException("Could not bind LLVM kernel", failure);
        }
    }
    record NativeCode(long handle, long address) { }
    static NativeCode compileText(String source, int optimizationLevel, Path optimizedDump) throws IOException {
        try (Arena temporary = Arena.ofConfined()) {
            byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
            var input = temporary.allocate(Math.max(1, bytes.length)); input.asSlice(0, bytes.length).copyFrom(MemorySegment.ofArray(bytes));
            var address = temporary.allocate(ValueLayout.JAVA_LONG);
            var error = temporary.allocate(4096);
            MemorySegment path = optimizedDump == null ? MemorySegment.NULL : temporary.allocateFrom(optimizedDump.toAbsolutePath().toString());
            var handle = (MemorySegment) Bridge.COMPILE.invokeExact(input, (long) bytes.length, optimizationLevel, address, path, error, 4096L);
            if (handle.address() == 0) throw new IOException("LLVM compilation failed: " + error.getString(0));
            return new NativeCode(handle.address(), address.get(ValueLayout.JAVA_LONG, 0));
        } catch (IOException e) { throw e; }
        catch (LinkageError e) { throw new IOException("Could not load the experimental LLVM bridge", e); }
        catch (Throwable e) { if (e instanceof Error error) throw error; throw new IOException("LLVM bridge failed", e); }
    }
    static void release(long handle) {
        try (Arena temporary = Arena.ofConfined()) {
            var error = temporary.allocate(4096);
            int status = (int) Bridge.RELEASE.invokeExact(MemorySegment.ofAddress(handle), error, 4096L);
            if (status != 0) throw new IllegalStateException("LLVM code release failed: " + error.getString(0));
        } catch (RuntimeException | Error e) { throw e; }
        catch (Throwable e) { throw new IllegalStateException("LLVM code release failed", e); }
    }
    static long liveFunctions() {
        try { return (long) Bridge.LIVE.invokeExact(); }
        catch (Throwable e) { throw new IllegalStateException(e); }
    }
    private static final class Bridge {
        static final MethodHandle COMPILE, RELEASE, LIVE;
        static {
            try {
                Path classes = Path.of(LlvmBackend.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                Path library = Path.of(System.getProperty("questdb.codegen.llvm.library",
                        classes.getParent().resolve("native/libquery_codegen_llvm.so").toString())).toAbsolutePath();
                var lookup = SymbolLookup.libraryLookup(library, Arena.global()); var linker = Linker.nativeLinker();
                COMPILE = linker.downcallHandle(lookup.find("query_llvm_compile").orElseThrow(), FunctionDescriptor.of(
                        ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
                RELEASE = linker.downcallHandle(lookup.find("query_llvm_release").orElseThrow(), FunctionDescriptor.of(
                        ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
                LIVE = linker.downcallHandle(lookup.find("query_llvm_live_functions").orElseThrow(), FunctionDescriptor.of(ValueLayout.JAVA_LONG));
            } catch (Exception e) { throw new ExceptionInInitializerError(e); }
        }
    }
}
