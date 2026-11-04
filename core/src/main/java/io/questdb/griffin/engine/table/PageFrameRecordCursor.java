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

import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public interface PageFrameRecordCursor extends RecordCursor, Plannable {

    /**
     * Exclusive row bound of a contiguous span beginning at the record positioned by
     * hasNext(), or -1 when borrowing is unsupported. Coordinates are relative to that
     * record's frame. The span respects the current decode and LIMIT budget; borrowing
     * does not advance the cursor. No cursor navigation is allowed while it is borrowed.
     */
    default long getContiguousFrameRowHi() {
        return -1;
    }

    /**
     * Commits rows from a borrowed span, including the currently positioned record.
     * Zero preserves the current record for a fallback. Otherwise the record is left
     * on the last consumed row and the next hasNext() advances past it. Call hasNext()
     * before consuming another span. This does not change the post-skip LIMIT budget.
     */
    default void consumeFrameRows(long rowCount) {
        throw new UnsupportedOperationException();
    }

    PageFrameCursor getPageFrameCursor();

    @Override
    StaticSymbolTable getSymbolTable(int columnIndex);

    void of(PageFrameCursor cursor, SqlExecutionContext executionContext) throws SqlException;
}
