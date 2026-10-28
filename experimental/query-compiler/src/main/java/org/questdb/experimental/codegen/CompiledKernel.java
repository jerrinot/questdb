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
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Owns code and callbacks. Concurrent calls require independent state and must finish before close. */
public final class CompiledKernel implements Kernel, AutoCloseable {
    private final Path artifacts;
    private final long compilationNanos;
    private final AutoCloseable resource;
    private Kernel kernel;

    CompiledKernel(Kernel kernel, AutoCloseable resource, Path artifacts, long compilationNanos) {
        this.kernel = kernel;
        this.resource = resource;
        this.artifacts = artifacts;
        this.compilationNanos = compilationNanos;
    }

    public Path artifacts() { return artifacts; }
    public long compilationNanos() { return compilationNanos + (resource instanceof Deferred deferred ? deferred.compilationNanos : 0); }

    /** Compiles an optional operator on its first invocation; the ordinary query owner retains it. */
    static CompiledKernel defer(Backend backend, Supplier<Ir.Program> program, Path artifacts) {
        Deferred deferred = new Deferred(backend, program, artifacts);
        return new CompiledKernel(deferred, deferred, artifacts, 0);
    }

    static CompiledKernel handwritten(Kernel kernel) {
        return new CompiledKernel(kernel, () -> { }, null, 0);
    }

    /** All worker calls must have joined when work returns, including failures. */
    void runBatch(long rows, Consumer<Kernel> work) {
        if (kernel == null) throw new IllegalStateException("Compiled kernel is closed");
        if (rows < 0) throw new IllegalArgumentException("Negative row count");
        if (kernel instanceof AdaptiveKernel adaptive) adaptive.runBatch(rows, work);
        else work.accept(this);
    }

    @Override
    public long run(long columnAddresses, long rowCount, long stateAddress) {
        if (kernel == null) {
            throw new IllegalStateException("Compiled kernel is closed");
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("Negative row count");
        }
        return kernel.run(columnAddresses, rowCount, stateAddress);
    }

    @Override
    public void close() {
        if (kernel != null) {
            kernel = null;
            try {
                resource.close();
            } catch (Exception e) {
                throw new IllegalStateException("Could not close compiled kernel", e);
            }
        }
    }
    private static final class Deferred implements Kernel, AutoCloseable {
        private final Backend backend;
        private final Path artifacts;
        private Supplier<Ir.Program> program;
        private volatile CompiledKernel compiled;
        private volatile long compilationNanos;
        private boolean closed;

        private Deferred(Backend backend, Supplier<Ir.Program> program, Path artifacts) {
            this.backend = backend;
            this.program = program;
            this.artifacts = artifacts;
        }

        @Override public long run(long columns, long rows, long state) {
            CompiledKernel code = compiled;
            if (code == null) code = prepare();
            return code.run(columns, rows, state);
        }

        private synchronized CompiledKernel prepare() {
            if (closed) throw new IllegalStateException("Deferred kernel is closed");
            if (compiled == null) {
                try {
                    CompiledKernel code;
                    // Planning previously called the backend serially. Preserve that contract
                    // when distinct optional operators first become necessary on worker threads.
                    synchronized (backend) { code = backend.compile(program.get(), artifacts); }
                    compilationNanos = code.compilationNanos();
                    compiled = code;
                    program = null;
                } catch (IOException failure) {
                    throw io.questdb.cairo.CairoException.nonCritical()
                            .put("Deferred operator compilation failed: ").put(failure.getMessage());
                }
            }
            return compiled;
        }

        @Override public synchronized void close() {
            if (!closed) {
                closed = true;
                program = null;
                if (compiled != null) compiled.close();
            }
        }
    }

}
