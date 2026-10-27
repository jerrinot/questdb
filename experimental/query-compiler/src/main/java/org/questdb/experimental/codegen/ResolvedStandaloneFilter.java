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
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.jit.CompiledCountOnlyFilter;
import io.questdb.jit.CompiledFilter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;

/** Binds the shared scan IR to the engine's existing selection/count-only ABI. */
final class ResolvedStandaloneFilter {
    private ResolvedStandaloneFilter() { }

    static QueryCodeGenerator.CompiledFilters compile(ColumnTypes types, Function function,
                                                        Backend backend, Path artifacts) throws IOException {
        var inputs = new ResolvedInputs(types);
        var predicate = inputs.predicate(function);
        if (predicate == null) return null;
        var plan = ScanPlan.of(inputs.columns(), predicate);
        CompiledKernel rows = backend.compile(ScanLowering.selection(plan, inputs.indexes(), false), artifacts);
        CompiledKernel count = null;
        try {
            count = backend.compile(ScanLowering.selection(plan, inputs.indexes(), true), artifacts);
            return new QueryCodeGenerator.CompiledFilters(new Rows(rows, backend.name()), new Count(count));
        } catch (Throwable failure) {
            try { rows.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            if (count != null) try { count.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static final class Rows extends CompiledFilter {
        private final CompiledKernel code;
        private final String backend;
        private final LongAdder inputRows = new LongAdder();

        private Rows(CompiledKernel code, String backend) { this.code = code; this.backend = backend; }

        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long output, long rows) {
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
        private final LongAdder inputRows = new LongAdder();

        private Count(CompiledKernel code) { this.code = code; }

        @Override public long call(long data, long columnCount, long aux, long vars, long variableCount, long rows) {
            long count = code.run(data, rows, 0);
            inputRows.add(rows);
            return count;
        }
        @Override public long getNativeInputRows() { return inputRows.sum(); }
        @Override public void close() { code.close(); }
    }
}
