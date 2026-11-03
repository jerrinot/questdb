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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/** Factory-owned scan and filters; execution contexts borrow them until their workers drain. */
public final class HashJoinInput implements QuietCloseable {
    private RecordCursorFactory factory;
    private AsyncFilterContext filters;
    private Function onFilter;
    private ObjList<Function> workerOnFilters;

    public HashJoinInput(RecordCursorFactory factory, AsyncFilterContext filters,
                         Function onFilter, ObjList<Function> workerOnFilters) {
        this.factory = factory;
        this.filters = filters;
        this.onFilter = onFilter;
        this.workerOnFilters = workerOnFilters;
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeBestEffort(null, filters);
        filters = null;
        failure = Misc.freeBestEffort(failure, onFilter);
        onFilter = null;
        failure = Misc.freeObjListBestEffort(failure, workerOnFilters);
        workerOnFilters = null;
        failure = Misc.freeBestEffort(failure, factory);
        factory = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }
    public AsyncFilterContext getFilters() {
        return filters;
    }
    public Function getOnFilter() {
        return onFilter;
    }
    public ObjList<Function> getWorkerOnFilters() {
        return workerOnFilters;
    }
}
