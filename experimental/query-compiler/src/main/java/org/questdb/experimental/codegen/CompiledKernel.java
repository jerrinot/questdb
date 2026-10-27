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

import java.nio.file.Path;
import java.util.function.Consumer;

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
    public long compilationNanos() { return compilationNanos; }

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
}
