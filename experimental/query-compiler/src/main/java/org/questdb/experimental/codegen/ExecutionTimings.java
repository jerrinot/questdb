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

import java.util.List;

/** Optional demo instrumentation, overwritten on each execution. No per-row clocks. */
final class ExecutionTimings {
    Runnable profileStart;
    Runnable profileEnd;
    long build;
    long scan;
    long merge;
    long[] workerRows;
    int shards;
    long copy;
    long sort;
    long release;
    boolean inspectOrder;
    int descents;
    long peakBytes;
    long grows;

    void reset() { build = scan = merge = copy = sort = release = peakBytes = grows = 0; descents = shards = 0; workerRows = null; }

    // Only enabled for an unmeasured warmup, to explain canonical-sort costs.
    void inspect(List<CompiledGroupBy.ResultRow> rows) {
        if (inspectOrder) {
            for (int i = 1; i < rows.size(); i++) {
                if (rows.get(i - 1).compareTo(rows.get(i)) > 0) { descents++; }
            }
        }
    }
}
