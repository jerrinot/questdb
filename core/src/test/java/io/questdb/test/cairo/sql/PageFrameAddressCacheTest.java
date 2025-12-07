/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameAddressCacheTest extends AbstractCairoTest {

    @Test
    public void testTableToQueryColumnIndexAllColumnsSelected() {
        // When all columns are selected, tableIdx == queryIdx
        int columnCount = 5;
        RecordMetadata metadata = createMetadata(columnCount);
        IntList columnIndexes = new IntList();
        for (int i = 0; i < columnCount; i++) {
            columnIndexes.add(i);
        }

        PageFrameAddressCache cache = new PageFrameAddressCache(configuration);
        cache.of(metadata, columnIndexes, false);

        // All columns should map to themselves
        for (int i = 0; i < columnCount; i++) {
            Assert.assertEquals(i, cache.tableToQueryColumnIndex(i));
        }
    }

    @Test
    public void testTableToQueryColumnIndexProjection() {
        // When only some columns are selected
        int columnCount = 5;
        RecordMetadata metadata = createMetadata(columnCount);
        // Select only columns 1 and 3 from table
        IntList columnIndexes = new IntList();
        columnIndexes.add(1);  // tableIdx 1 -> queryIdx 0
        columnIndexes.add(3);  // tableIdx 3 -> queryIdx 1

        PageFrameAddressCache cache = new PageFrameAddressCache(configuration);
        cache.of(metadata, columnIndexes, false);

        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(0));  // Not in query
        Assert.assertEquals(0, cache.tableToQueryColumnIndex(1));   // Maps to queryIdx 0
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(2));  // Not in query
        Assert.assertEquals(1, cache.tableToQueryColumnIndex(3));   // Maps to queryIdx 1
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(4));  // Not in query
    }

    @Test
    public void testTableToQueryColumnIndexNotFound() {
        // Column not in the query should return -1
        int columnCount = 5;
        RecordMetadata metadata = createMetadata(columnCount);
        IntList columnIndexes = new IntList();
        columnIndexes.add(0);
        columnIndexes.add(2);

        PageFrameAddressCache cache = new PageFrameAddressCache(configuration);
        cache.of(metadata, columnIndexes, false);

        // Column 1, 3, 4 are not in the query
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(1));
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(3));
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(4));
        // Column out of range should also return -1
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(100));
    }

    @Test
    public void testTableToQueryColumnIndexClearedOnClear() {
        int columnCount = 3;
        RecordMetadata metadata = createMetadata(columnCount);
        IntList columnIndexes = new IntList();
        columnIndexes.add(0);
        columnIndexes.add(1);
        columnIndexes.add(2);

        PageFrameAddressCache cache = new PageFrameAddressCache(configuration);
        cache.of(metadata, columnIndexes, false);

        // Verify mapping works
        Assert.assertEquals(0, cache.tableToQueryColumnIndex(0));
        Assert.assertEquals(1, cache.tableToQueryColumnIndex(1));

        // Clear and verify mappings are gone
        cache.clear();
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(0));
        Assert.assertEquals(-1, cache.tableToQueryColumnIndex(1));
    }

    private RecordMetadata createMetadata(int columnCount) {
        return new RecordMetadata() {
            @Override
            public int getColumnCount() {
                return columnCount;
            }

            @Override
            public int getColumnType(int columnIndex) {
                return ColumnType.LONG;
            }

            @Override
            public int getColumnIndex(CharSequence columnName) {
                return -1;
            }

            @Override
            public int getColumnIndexQuiet(CharSequence columnName) {
                return -1;
            }

            @Override
            public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
                return -1;
            }

            @Override
            public io.questdb.cairo.TableColumnMetadata getColumnMetadata(int columnIndex) {
                return null;
            }

            @Override
            public String getColumnName(int columnIndex) {
                return "col" + columnIndex;
            }

            @Override
            public int getWriterIndex(int columnIndex) {
                return columnIndex;
            }

            @Override
            public boolean hasColumn(int columnIndex) {
                return columnIndex >= 0 && columnIndex < columnCount;
            }

            @Override
            public boolean isColumnIndexed(int columnIndex) {
                return false;
            }

            @Override
            public int getIndexValueBlockCapacity(int columnIndex) {
                return 0;
            }

            @Override
            public boolean isSymbolTableStatic(int columnIndex) {
                return false;
            }

            @Override
            public int getTimestampIndex() {
                return -1;
            }

            @Override
            public boolean isDedupKey(int columnIndex) {
                return false;
            }
        };
    }
}
