/*******************************************************************************
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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.PageFrameRecordCursor;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameSpanTest extends AbstractCairoTest {
    @Test
    public void testCommitPreservesFrameLimitAndRescan() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, timestamp_sequence(0, 1000000) ts from long_sequence(17)) timestamp(ts) partition by day");
            for (boolean parquet : new boolean[]{false, true}) {
                if (parquet) execute("alter table t convert partition to parquet where ts >= 0");
                try (RecordCursorFactory factory = select("select * from t")) {
                    ((SqlExecutionContextImpl) sqlExecutionContext).changePageFrameSizes(4, 4);
                    for (int reopen = 0; reopen < 2; reopen++) {
                        try (RecordCursor cursor = (factory instanceof QueryProgress ? factory.getBaseFactory() : factory).getCursor(sqlExecutionContext)) {
                            PageFrameRecordCursor frames = (PageFrameRecordCursor) cursor;
                            Assert.assertEquals(-1, frames.getContiguousFrameRowHi());
                            assertSpans(frames, 1, 17);
                            cursor.toTop();
                            RecordCursor.Counter skip = new RecordCursor.Counter();
                            skip.set(5);
                            cursor.skipRows(skip, 7);
                            Assert.assertEquals(0, skip.get());
                            Assert.assertEquals(-1, frames.getContiguousFrameRowHi());
                            assertSpans(frames, 6, 7);
                            cursor.toTop();
                            assertSpans(frames, 1, 17);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRecordAtDoesNotCommitConsumption() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, timestamp_sequence(0, 1000000) ts from long_sequence(10)) timestamp(ts) partition by day");
            try (RecordCursorFactory factory = select("select * from t"); RecordCursor cursor = (factory instanceof QueryProgress ? factory.getBaseFactory() : factory).getCursor(sqlExecutionContext)) {
                PageFrameRecordCursor frames = (PageFrameRecordCursor) cursor;
                Assert.assertTrue(cursor.hasNext());
                long first = cursor.getRecord().getRowId();
                Assert.assertTrue(cursor.hasNext());
                long second = cursor.getRecord().getRowId();
                cursor.recordAt(cursor.getRecord(), first);
                Assert.assertEquals(-1, frames.getContiguousFrameRowHi());
                Assert.assertThrows(IllegalArgumentException.class, () -> frames.consumeFrameRows(1));
                cursor.recordAt(cursor.getRecord(), second);
                frames.consumeFrameRows(2);
                Assert.assertEquals(3, cursor.getRecord().getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(4, cursor.getRecord().getLong(0));
            }
        });
    }

    @Test
    public void testIndexedAndBackwardStreamsDecline() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x, cast('s'||(x%3) as symbol) s, timestamp_sequence(0, 1000000) ts from long_sequence(10)) timestamp(ts) partition by day");
            execute("alter table t alter column s add index");
            for (String sql : new String[]{"select * from t where s='s1'", "select * from t order by ts desc"}) {
                try (RecordCursorFactory factory = select(sql); RecordCursor cursor = (factory instanceof QueryProgress ? factory.getBaseFactory() : factory).getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(cursor instanceof PageFrameRecordCursor);
                    Assert.assertEquals(-1, ((PageFrameRecordCursor) cursor).getContiguousFrameRowHi());
                }
            }
        });
    }

    private static void assertSpans(PageFrameRecordCursor cursor, long firstValue, long expectedRows) {
        long count = 0;
        while (cursor.hasNext()) {
            PageFrameMemoryRecord record = (PageFrameMemoryRecord) cursor.getRecord();
            long lo = Rows.toLocalRowID(record.getRowId());
            long hi = cursor.getContiguousFrameRowHi();
            Assert.assertTrue(hi > lo);
            long take = Math.min(3, hi - lo);
            for (long i = 0; i < take; i++) {
                Assert.assertEquals(firstValue + count + i, Unsafe.getLong(record.getPageAddress(0) + (lo + i) * 8));
            }
            cursor.consumeFrameRows(0);
            Assert.assertEquals(firstValue + count, record.getLong(0));
            Assert.assertThrows(IllegalArgumentException.class, () -> cursor.consumeFrameRows(hi - lo + 1));
            Assert.assertThrows(IllegalArgumentException.class, () -> cursor.consumeFrameRows(-1));
            cursor.consumeFrameRows(take);
            count += take;
            Assert.assertEquals(firstValue + count - 1, record.getLong(0));
        }
        Assert.assertEquals(expectedRows, count);
        Assert.assertEquals(-1, cursor.getContiguousFrameRowHi());
    }
}
