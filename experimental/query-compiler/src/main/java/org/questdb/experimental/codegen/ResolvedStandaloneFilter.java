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

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

/** Binds the shared scan IR to the engine's existing selection/count-only ABI. */
final class ResolvedStandaloneFilter {
    private ResolvedStandaloneFilter() { }

    static QueryCodeGenerator.CompiledFilters compile(ColumnTypes types, Function function,
                                                        Backend backend, Path artifacts) throws IOException {
        var binding = ResolvedFilter.bind(types, function);
        if (binding == null) return null;
        boolean descriptors = binding.varcharColumns().length != 0;
        CompiledKernel rows = backend.compile(descriptors
                ? ScanLowering.selection(binding.plan(), binding.residual(), false)
                : ScanLowering.selection(binding.plan(), binding.sourceIndexes(), false), artifacts);
        CompiledKernel count = null;
        try {
            count = backend.compile(descriptors
                    ? ScanLowering.selection(binding.plan(), binding.residual(), true)
                    : ScanLowering.selection(binding.plan(), binding.sourceIndexes(), true), artifacts);
            var frameBinding = descriptors ? binding : null;
            return new QueryCodeGenerator.CompiledFilters(new Rows(rows, backend.name(), frameBinding), new Count(count, frameBinding));
        } catch (Throwable failure) {
            try { rows.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            if (count != null) try { count.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static long run(CompiledKernel code, ResolvedFilter.Binding binding, long data, long aux,
                            long output, long rows, PageFrameMemory frame) {
        long bytes = binding.bytes();
        long addresses = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT);
        try {
            binding.prepare(frame, data, aux, rows, addresses);
            long count = code.run(addresses, rows, output);
            if (count < 0 || count > rows) throw new IllegalStateException("Invalid native filter count: " + count);
            return count;
        } finally {
            Unsafe.free(addresses, bytes, MemoryTag.NATIVE_JIT);
        }
    }

    private static final class Rows extends CompiledFilter {
        private final CompiledKernel code;
        private final ResolvedFilter.Binding binding;
        private final String backend;
        private final LongAdder inputRows = new LongAdder();

        private Rows(CompiledKernel code, String backend, ResolvedFilter.Binding binding) {
            this.code = code; this.backend = backend; this.binding = binding;
        }

        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long output, long rows,
                                   PageFrameMemory frame) {
            if (binding == null) return call(data, columnCount, aux, vars, variableCount, output, rows);
            long count = run(code, binding, data, aux, output, rows, frame);
            inputRows.add(rows);
            return count;
        }
        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long output, long rows) {
            if (binding != null) throw new IllegalStateException("VARCHAR filtering requires frame metadata");
            long count = code.run(data, rows, output);
            inputRows.add(rows);
            return count;
        }
        @Override public CharSequence getBackendName() { return backend; }
        @Override public long getNativeInputRows() { return inputRows.sum(); }
        @Override public void close() { code.close(); }
    }

    private static final class Count extends CompiledCountOnlyFilter {
        private final CompiledKernel code;
        private final ResolvedFilter.Binding binding;
        private final LongAdder inputRows = new LongAdder();

        private Count(CompiledKernel code, ResolvedFilter.Binding binding) { this.code = code; this.binding = binding; }

        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long rows,
                                   PageFrameMemory frame) {
            if (binding == null) return call(data, columnCount, aux, vars, variableCount, rows);
            long count = run(code, binding, data, aux, 0, rows, frame);
            inputRows.add(rows);
            return count;
        }
        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long rows) {
            if (binding != null) throw new IllegalStateException("VARCHAR filtering requires frame metadata");
            long count = code.run(data, rows, 0);
            inputRows.add(rows);
            return count;
        }
        @Override public long getNativeInputRows() { return inputRows.sum(); }
        @Override public void close() { code.close(); }
    }
}
