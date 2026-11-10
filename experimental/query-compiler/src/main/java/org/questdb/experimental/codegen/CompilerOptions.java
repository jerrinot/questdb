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

/** A query's immutable planning controls. Deferred compilation retains the same snapshot. */
public record CompilerOptions(boolean expressions, Boolean bounded, Integer localCapacity, int prefetchRows,
                              boolean sharedVarchar, boolean orderedPipeline, boolean largeTopK) {
    static final CompilerOptions DEFAULT = fromSystemProperties();
    // These controls affect instrumentation and prefetch hints and retain process scope.
    static final Process PROCESS = new Process(Boolean.getBoolean("query.codegen.profile"),
            Boolean.getBoolean("query.codegen.probeCounters"), !Boolean.getBoolean("query.codegen.disablePrefetchHints"));

    public CompilerOptions {
        if (prefetchRows < 0 || prefetchRows > 1024 || (prefetchRows & (prefetchRows - 1)) != 0)
            throw new IllegalArgumentException("Prefetch rows must be zero or a power of two up to 1024");
        if (localCapacity != null && (localCapacity < 64 || localCapacity > 65536 || (localCapacity & (localCapacity - 1)) != 0))
            throw new IllegalArgumentException("Local aggregation capacity must be a power of two in [64,65536]");
    }

    public static CompilerOptions fromSystemProperties() {
        String bounded = System.getProperty("query.codegen.bounded");
        return new CompilerOptions(Boolean.parseBoolean(System.getProperty("query.codegen.expressions", "true")),
                bounded == null ? null : Boolean.parseBoolean(bounded), Integer.getInteger("query.codegen.localCapacity"),
                Integer.getInteger("query.codegen.prefetchRows", 32), Boolean.getBoolean("query.codegen.sharedVarchar"),
                Boolean.parseBoolean(System.getProperty("query.codegen.orderedPipeline", "true")),
                Boolean.parseBoolean(System.getProperty("query.codegen.largeTopK", "true")));
    }

    public CompilerOptions withLocalCapacity(Integer capacity) {
        return new CompilerOptions(expressions, bounded, capacity, prefetchRows, sharedVarchar, orderedPipeline, largeTopK);
    }
    public CompilerOptions withBounded(Boolean enabled) {
        return new CompilerOptions(expressions, enabled, localCapacity, prefetchRows, sharedVarchar, orderedPipeline, largeTopK);
    }
    public CompilerOptions withExpressions(boolean enabled) {
        return new CompilerOptions(enabled, bounded, localCapacity, prefetchRows, sharedVarchar, orderedPipeline, largeTopK);
    }
    public CompilerOptions withSharedVarchar(boolean enabled) {
        return new CompilerOptions(expressions, bounded, localCapacity, prefetchRows, enabled, orderedPipeline, largeTopK);
    }
    public CompilerOptions withPrefetchRows(int rows) {
        return new CompilerOptions(expressions, bounded, localCapacity, rows, sharedVarchar, orderedPipeline, largeTopK);
    }

    record Process(boolean profile, boolean probeCounters, boolean prefetchHints) { }
}
