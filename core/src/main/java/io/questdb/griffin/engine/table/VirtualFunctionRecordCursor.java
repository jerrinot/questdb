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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualFunctionRecord;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.engine.CompiledProjection;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class VirtualFunctionRecordCursor implements RecordCursor {
    protected final VirtualFunctionRecord recordA;
    private final ObjList<Function> functions;
    private final int memoizerCount;
    private final ObjList<MemoizerFunction> memoizers;
    private final PriorityMetadata priorityMetadata;
    private final VirtualFunctionRecord recordB;
    private final boolean supportsRandomAccess;
    protected RecordCursor baseCursor;
    private final CompiledProjection projection;
    private final long[] projectionRowIds;
    private final boolean projectionNeedsBaseRecord;
    private SqlExecutionCircuitBreaker projectionBreaker;
    private int projectedCount;
    private int projectedNext;

    public VirtualFunctionRecordCursor(
            @NotNull PriorityMetadata priorityMetadata,
            @NotNull ObjList<Function> functions,
            @NotNull ObjList<MemoizerFunction> memoizers,
            boolean supportsRandomAccess,
            int virtualColumnReservedSlots
    ) {
        this(priorityMetadata, functions, memoizers, supportsRandomAccess, virtualColumnReservedSlots, null);
    }

    public VirtualFunctionRecordCursor(PriorityMetadata priorityMetadata, ObjList<Function> functions,
                                      ObjList<MemoizerFunction> memoizers, boolean supportsRandomAccess,
                                      int virtualColumnReservedSlots,
                                      CompiledProjection projection) {
        this.projection = projection;
        this.projectionNeedsBaseRecord = projection != null && projection.needsBaseRecord();
        this.projectionRowIds = projection == null ? null : new long[projection.batchSize()];
        this.priorityMetadata = priorityMetadata;
        this.functions = functions;
        this.memoizers = memoizers;
        this.memoizerCount = memoizers.size();
        if (supportsRandomAccess) {
            this.recordA = projection == null ? new VirtualFunctionRecord(functions, virtualColumnReservedSlots)
                    : new ProjectedRecord(functions, virtualColumnReservedSlots, projection, this);
            this.recordB = projection == null ? new VirtualFunctionRecord(functions, virtualColumnReservedSlots)
                    : new ProjectedRecord(functions, virtualColumnReservedSlots, projection, this);
        } else {
            this.recordA = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
            this.recordB = null;
        }
        this.supportsRandomAccess = supportsRandomAccess;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        assert baseCursor != null;
        counter.add(projectedCount - projectedNext);
        projectedCount = projectedNext = 0;
        baseCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        projectedCount = projectedNext = 0;
        projectionBreaker = null;
        baseCursor = Misc.free(baseCursor);
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).cursorClosed();
        }
    }

    @Override
    public void expectLimitedIteration() {
        baseCursor.expectLimitedIteration();
    }

    public int getLongTopKColumnIndex(int columnIndex) {
        if (!supportsRandomAccess) {
            return -1;
        }
        ColumnFunction columnFunction = ColumnFunction.unwrap(functions.getQuick(columnIndex));
        if (columnFunction == null) {
            return -1;
        }
        final int virtualColumnIndex = columnFunction.getColumnIndex();
        final int columnType = priorityMetadata.getColumnType(virtualColumnIndex);
        if (columnType == ColumnType.LONG || ColumnType.isTimestamp(columnType)) {
            return priorityMetadata.getBaseColumnIndex(virtualColumnIndex);
        }
        return -1;
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        if (supportsRandomAccess) {
            return recordB;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) functions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (projection != null) {
            if (projectedNext == projectedCount) {
                projectedNext = projectedCount = 0;
                projectionBreaker.statefulThrowExceptionIfTripped();
                Record input = baseCursor.getRecord();
                while (projectedCount < projectionRowIds.length && baseCursor.hasNext()) {
                    projectionRowIds[projectedCount] = input.getRowId();
                    projection.putInput(input, projectedCount++);
                }
                if (projectedCount == 0) return false;
                projection.evaluate(0, projectedCount);
            }
            ProjectedRecord projected = (ProjectedRecord) recordA;
            projected.rowId = projectionRowIds[projectedNext];
            projected.row = projectedNext++;
            projected.positioned = false;
            if (projectionNeedsBaseRecord) projected.getBaseRecord();
            clearMemos();
            return true;
        }
        final boolean result = baseCursor.hasNext();
        if (result) {
            clearMemos();
        }
        return result;
    }

    @Override
    public boolean selectSortedRows(io.questdb.std.DirectLongLongSortedList sorted, io.questdb.std.DirectLongList rows, long skip, long take) {
        return baseCursor.selectSortedRows(sorted, rows, skip, take);
    }

    @Override
    public boolean selectRows(io.questdb.std.DirectLongList rows, long skip, long take) {
        return baseCursor.selectRows(rows, skip, take);
    }

    @Override
    public boolean sortLong(io.questdb.std.DirectLongList entries, io.questdb.std.DirectLongList scratch,
                            int columnIndex, boolean ascending, long maxEntries) {
        int baseColumn = getLongTopKColumnIndex(columnIndex);
        return baseColumn >= 0 && baseCursor.sortLong(entries, scratch, baseColumn, ascending, maxEntries);
    }

    @Override
    public void longTopK(DirectLongLongSortedList list, int columnIndex) {
        final int baseColumnIndex = getLongTopKColumnIndex(columnIndex);
        assert baseColumnIndex != -1;
        baseCursor.longTopK(list, baseColumnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
    }

    public void of(RecordCursor cursor) {
        of(cursor, io.questdb.cairo.sql.SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
    }

    public void of(RecordCursor cursor, SqlExecutionCircuitBreaker breaker) {
        projectionBreaker = breaker;
        projectedCount = projectedNext = 0;
        baseCursor = cursor;
        recordA.of(baseCursor.getRecord());
        if (recordB != null) {
            recordB.of(baseCursor.getRecordB());
        }
        cursor.toTop();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (supportsRandomAccess) {
            assert baseCursor != null;
            Record input = projection == null ? ((VirtualFunctionRecord) record).getBaseRecord()
                    : ((ProjectedRecord) record).inputRecord();
            baseCursor.recordAt(input, atRowId);
            if (projection != null) {
                ProjectedRecord projected = (ProjectedRecord) record;
                projected.rowId = atRowId;
                projected.positioned = true;
                projected.row = projection.batchSize() + (record == recordA ? 0 : 1);
                projection.putInput(projected.getBaseRecord(), projected.row);
                projection.evaluate(projected.row, 1);
            }
            clearMemos();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        if (baseCursor != null) {
            baseCursor.setParquetDecodeHint(hint);
        }
    }

    @Override
    public void setRecordAtRows(@Nullable RowIdSource source) {
        if (baseCursor != null) {
            baseCursor.setRecordAtRows(source);
        }
    }

    @Override
    public long size() {
        assert baseCursor != null;
        return baseCursor.size();
    }

    @Override
    public void skipRows(Counter rowCount, long maxRowsAfterSkip) {
        assert baseCursor != null;
        long buffered = Math.min(rowCount.get(), projectedCount - projectedNext);
        projectedNext += (int) buffered;
        rowCount.set(rowCount.get() - buffered);
        if (rowCount.get() > 0) baseCursor.skipRows(rowCount, maxRowsAfterSkip);
    }

    @Override
    public void toTop() {
        assert baseCursor != null;
        projectedCount = projectedNext = 0;
        baseCursor.toTop();
        GroupByUtils.toTop(functions);
    }

    private void clearMemos() {
        for (int i = 0; i < memoizerCount; i++) {
            memoizers.getQuick(i).clearMemo();
        }
    }
    private static class ProjectedRecord extends VirtualFunctionRecord {
        private final CompiledProjection projection;
        private final VirtualFunctionRecordCursor owner;
        private int row;
        private long rowId;
        private boolean positioned;

        ProjectedRecord(ObjList<Function> functions, int reserved,
                        CompiledProjection projection, VirtualFunctionRecordCursor owner) {
            super(functions, reserved);
            this.projection = projection;
            this.owner = owner;
        }

        private Record inputRecord() { return super.getBaseRecord(); }
        @Override public Record getBaseRecord() {
            if (!positioned) {
                owner.baseCursor.recordAt(inputRecord(), rowId);
                positioned = true;
            }
            return inputRecord();
        }
        @Override public long getRowId() { return rowId; }
        @Override public long getUpdateRowId() { return getBaseRecord().getUpdateRowId(); }
        @Override public byte getByte(int col) { return projection.isProjected(col) ? (byte) projection.getValue(row, col) : super.getByte(col); }
        @Override public short getShort(int col) { return projection.isProjected(col) ? (short) projection.getValue(row, col) : super.getShort(col); }
        @Override public int getInt(int col) {
            if (!projection.isProjected(col)) return super.getInt(col);
            long value = projection.getValue(row, col);
            return value == Long.MIN_VALUE ? Integer.MIN_VALUE : (int) value;
        }
        @Override public long getLong(int col) { return projection.isProjected(col) ? projection.getValue(row, col) : super.getLong(col); }
        @Override public int getIPv4(int col) { return projection.isProjected(col) ? (int) projection.getValue(row, col) : super.getIPv4(col); }
        @Override public long getDate(int col) { return projection.isProjected(col) ? projection.getValue(row, col) : super.getDate(col); }
        @Override public long getTimestamp(int col) { return projection.isProjected(col) ? projection.getValue(row, col) : super.getTimestamp(col); }
        @Override public float getFloat(int col) {
            if (!projection.isProjected(col)) return super.getFloat(col);
            long value = projection.getValue(row, col);
            return value == Long.MIN_VALUE ? Float.NaN : value;
        }
        @Override public double getDouble(int col) {
            if (!projection.isProjected(col)) return super.getDouble(col);
            long value = projection.getValue(row, col);
            return value == Long.MIN_VALUE ? Double.NaN : value;
        }
    }

}
