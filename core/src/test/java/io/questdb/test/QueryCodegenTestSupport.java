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

package io.questdb.test;

import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryCodeGenerator;

import java.util.function.LongSupplier;

/** Optional test-runner wiring. Core tests have no dependency on a compiler backend. */
public final class QueryCodegenTestSupport {
    private static QueryCodeGenerator compiler;
    private static LongSupplier executions = () -> 0;

    private QueryCodegenTestSupport() {
    }

    /** Install before constructing tests or starting their workers; clear after they finish. */
    public static void install(QueryCodeGenerator compiler, LongSupplier executions) {
        QueryCodegenTestSupport.compiler = compiler;
        QueryCodegenTestSupport.executions = executions;
    }

    public static SqlExecutionContextImpl configure(SqlExecutionContextImpl context) {
        context.setQueryCodeGenerator(compiler);
        return context;
    }

    /** Native kernel invocations, not input rows: different kernels have different row ABIs. */
    public static long executions() {
        return executions.getAsLong();
    }
}
