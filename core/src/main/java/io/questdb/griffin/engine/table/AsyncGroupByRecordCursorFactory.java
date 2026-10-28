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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

public class AsyncGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final UnorderedPageFrameReducer AGGREGATE = AsyncGroupByRecordCursorFactory::aggregate;
    private static final UnorderedPageFrameReducer FILTER_AND_AGGREGATE = AsyncGroupByRecordCursorFactory::filterAndAggregate;

    private RecordCursorFactory base;
    private AsyncGroupByRecordCursor cursor;
    private UnorderedPageFrameSequence<AsyncGroupByAtom> frameSequence;
    private ObjList<Function> recordFunctions; // includes groupByFunctions
    private @Nullable ObjList<ObjList<Function>> sharedRecordFunctions;
    private final int workerCount;
    private ObjList<AsyncGroupBySharedCursor> sharedCursors;

    public AsyncGroupByRecordCursorFactory(
            @NotNull CairoEngine engine,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata groupByMetadata,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount,
            @Nullable ObjList<ObjList<Function>> sharedRecordFunctions,
            @Nullable QueryCodeGenerator aggregateCompiler
    ) {
        super(groupByMetadata);
        try {
            this.base = base;
            this.recordFunctions = recordFunctions;
            this.sharedRecordFunctions = sharedRecordFunctions;
            AsyncGroupByAtom atom = new AsyncGroupByAtom(
                    asm,
                    configuration,
                    base.getMetadata(),
                    keyTypes,
                    valueTypes,
                    listColumnFilter,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    keyFunctions,
                    perWorkerKeyFunctions,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    workerCount
            );
            this.frameSequence = new UnorderedPageFrameSequence<>(
                    engine,
                    configuration,
                    messageBus,
                    atom,
                    filter != null ? FILTER_AND_AGGREGATE : AGGREGATE,
                    workerCount
            );
            this.workerCount = workerCount;
            if (aggregateCompiler != null) {
                atom.compileAggregates(aggregateCompiler, base.getMetadata(), valueTypes, keyTypes, listColumnFilter);
            }
            this.cursor = new AsyncGroupByRecordCursor(engine, messageBus, atom.outputFunctions(recordFunctions));
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    @TestOnly
    public AsyncGroupByAtom getAtom() {
        return frameSequence.getAtom();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        frameSequence.of(base, executionContext, order);
        try {
            cursor.of(frameSequence, executionContext);
            return cursor;
        } catch (Throwable th) {
            // On a mid-reopen breach, close() drains the partially reopened atom and resets isOpen
            // so the cached factory stays reusable.
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public RecordCursor getSharedCursor(SqlExecutionContext executionContext, int sharedId) {
        if (sharedCursors == null) {
            sharedCursors = new ObjList<>();
        }
        int idx = sharedId - 1;
        AsyncGroupBySharedCursor shared = sharedCursors.getQuiet(idx);
        if (shared == null) {
            assert sharedRecordFunctions != null;
            assert idx < sharedRecordFunctions.size();
            shared = new AsyncGroupBySharedCursor(frameSequence.getAtom().outputFunctions(sharedRecordFunctions.getQuick(idx)));
            sharedCursors.extendAndSet(idx, shared);
        }
        shared.of(cursor);
        return shared;
    }

    @Override
    public boolean prepareRowLimit() { return frameSequence.getAtom().prepareRowLimit(); }

    @Override
    public boolean prepareSortedRowLimit() { return frameSequence.getAtom().prepareSortedRowLimit(); }

    @Override
    public boolean prepareLongSort(int columnIndex) {
        return frameSequence.getAtom().prepareLongSort(recordFunctions.getQuick(columnIndex));
    }

    @Override
    public boolean recordCursorSupportsCompiledLongTopK(int columnIndex) {
        return frameSequence.getAtom().supportsCompiledLongTopK(recordFunctions.getQuick(columnIndex));
    }

    @Override
    public void prepareLongTopK(int columnIndex) {
        frameSequence.getAtom().prepareLongTopK(recordFunctions.getQuick(columnIndex));
    }

    @Override
    public void prepareLongTopK(int columnIndex, boolean ascending, int limit) {
        frameSequence.getAtom().prepareLongTopK(recordFunctions.getQuick(columnIndex), ascending, limit);
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        final int columnType = getMetadata().getColumnType(columnIndex);
        return columnType == ColumnType.LONG || ColumnType.isTimestamp(columnType);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsSharedCursors() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Group By");
        } else {
            sink.type("Async Group By");
        }
        sink.meta("workers").val(workerCount);
        CharSequence compiledAggregateName = frameSequence.getAtom().getCompiledAggregateName();
        if (compiledAggregateName != null) {
            sink.meta("compiledAggregateBatches").val(compiledAggregateName);
        }
        CharSequence compiledFilterName = frameSequence.getAtom().getCompiledAggregateFilterName();
        if (compiledFilterName != null) {
            sink.meta("compiledAggregateFilter").val(compiledFilterName);
        }
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("keyFunctions", frameSequence.getAtom().getOwnerKeyFunctions(), true);
        sink.optAttr("values", frameSequence.getAtom().getOwnerGroupByFunctions(), true);
        sink.optAttr("filter", frameSequence.getAtom(), true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getFilterContext().getCompiledFilter() != null;
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private static void aggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;
        @SuppressWarnings("unchecked") final AsyncGroupByAtom atom = ((UnorderedPageFrameSequence<AsyncGroupByAtom>) frameSequence).getAtom();

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final AsyncFilterContext filterCtx = atom.getFilterContext();
        final PageFrameMemoryPool frameMemoryPool = filterCtx.getMemoryPool(slotId);

        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final GroupByMapFragment fragment = atom.getFragment(slotId);
        final RecordSink mapSink = atom.getMapSink(slotId);
        // navigateTo() decodes the frame and can throw, so it must sit inside the try that
        // releases the slot, see PerWorkerLocks.acquireSlot().
        try {
            final PageFrameMemory frameMemory = frameMemoryPool.navigateTo(frameIndex);
            record.init(frameMemory);

            atom.resetLocalStats(slotId);

            if (atom.isSharded()) {
                fragment.shard(circuitBreaker::statefulThrowExceptionIfTrippedNoThrottle);
            }

            record.setRowIndex(0);
            long baseRowId = record.getRowId();

            if (fragment.isNotSharded()) {
                aggregateNonShardedBatched(record, frameRowCount, baseRowId, atom, slotId, fragment, mapSink, circuitBreaker);
            } else if (!aggregateShardedCompiled(record, frameRowCount, 0, atom, slotId, fragment, mapSink, circuitBreaker)) {
                aggregateSharded(record, frameRowCount, baseRowId, functionUpdater, fragment, mapSink);
            }

            atom.maybeEnableSharding(fragment);
        } finally {
            try {
                frameMemoryPool.releaseParquetBuffers();
            } finally {
                atom.release(slotId);
            }
        }
    }

    private static void aggregateFilteredNonSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            GroupByMapFragment fragment,
            RecordSink mapSink
    ) {
        final Map map = fragment.reopenMap();
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
            record.setFilteredRowIndex(r, p);

            final MapKey key = map.withKey();
            mapSink.copy(record, key);
            MapValue value = key.createValue();
            if (value.isNew()) {
                functionUpdater.updateNew(value, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(value, record, baseRowId + r);
            }
        }
    }

    private static void aggregateFilteredNonShardedBatched(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            AsyncGroupByAtom atom,
            int slotId,
            GroupByMapFragment fragment,
            RecordSink mapSink,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final Map map = fragment.reopenMap();
        final ObjList<GroupByFunction> functions = atom.getGroupByFunctions(slotId);
        final DirectLongList batchList = atom.getBatchList(slotId);
        final FlyweightPackedMapValue mapValue = atom.getBatchMapValue(slotId);
        final GroupByKernel.Updater compiledUpdater = atom.getCompiledUpdater(slotId);
        final int functionCount = functions.size();
        final int batchSize = atom.getBatchSize();
        final long rowCount = rows.size();
        final long rowIdsAddr = rows.getAddress();

        if (compiledUpdater != null && compiledUpdater.updateMap(record, rowCount, rowIdsAddr, map, batchSize, circuitBreaker)) {
            return;
        }

        // Size the batch buffer once — probeBatch writes via the raw base address, never advancing pos.
        // ensureCapacity reopens the list if it's closed.
        batchList.ensureCapacity(batchSize);
        final long batchAddr = batchList.getAddress();

        for (long batchStart = 0; batchStart < rowCount; batchStart += batchSize) {
            final long batchEnd = Math.min(batchStart + batchSize, rowCount);
            final long batchRows = batchEnd - batchStart;

            map.reserveCapacity(batchRows);

            // Probe phase — rowIdsAddr[batchStart..batchEnd) holds the filtered frame-relative row ids.
            final long baseValueAddress = map.probeBatchFiltered(
                    record,
                    mapSink,
                    rowIdsAddr,
                    batchStart,
                    batchEnd,
                    batchAddr
            );

            if (compiledUpdater != null && compiledUpdater.update(record, baseValueAddress, batchAddr, batchRows, circuitBreaker)) {
                continue;
            }
            for (int i = 0; i < functionCount; i++) {
                functions.getQuick(i).computeKeyedBatch(
                        record,
                        mapValue,
                        baseValueAddress,
                        batchAddr,
                        batchRows,
                        baseRowId
                );
            }
        }
    }

    private static void aggregateFilteredSharded(
            PageFrameMemoryRecord record,
            DirectLongList rows,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            GroupByMapFragment fragment,
            RecordSink mapSink
    ) {
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long p = 0, n = rows.size(); p < n; p++) {
            long r = rows.get(p);
            record.setFilteredRowIndex(r, p);

            final MapKey lookupKey = lookupShard.withKey();
            mapSink.copy(record, lookupKey);
            lookupKey.commit();
            final long hashCode = lookupKey.hash();

            final Map shard = fragment.getShardMap(hashCode);
            final MapKey shardKey;
            if (shard != lookupShard) {
                shardKey = shard.withKey();
                shardKey.copyFrom(lookupKey);
            } else {
                shardKey = lookupKey;
            }

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }

    private static void aggregateNonShardedBatched(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            AsyncGroupByAtom atom,
            int slotId,
            GroupByMapFragment fragment,
            RecordSink mapSink,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final Map map = fragment.reopenMap();
        final ObjList<GroupByFunction> functions = atom.getGroupByFunctions(slotId);
        final DirectLongList batchList = atom.getBatchList(slotId);
        final FlyweightPackedMapValue mapValue = atom.getBatchMapValue(slotId);
        final GroupByKernel.Updater compiledUpdater = atom.getCompiledUpdater(slotId);
        final int functionCount = functions.size();
        final int batchSize = atom.getBatchSize();

        if (compiledUpdater != null && compiledUpdater.updateMap(record, frameRowCount, 0, map, batchSize, circuitBreaker)) {
            return;
        }

        // Size the batch buffer once — probeBatch writes via the raw base address, never advancing pos.
        // ensureCapacity reopens the list if it's closed.
        batchList.ensureCapacity(batchSize);
        final long batchAddr = batchList.getAddress();

        for (long batchStart = 0; batchStart < frameRowCount; batchStart += batchSize) {
            final long batchEnd = Math.min(batchStart + batchSize, frameRowCount);
            final long batchRows = batchEnd - batchStart;

            // Must reserve capacity before probeBatch: probeBatch packs entry offsets relative to the
            // current memStart, so a mid-batch rehash would break the offsets for unordered maps.
            map.reserveCapacity(batchRows);

            // Probe phase — delegate to the map for inlined probe loop.
            final long baseValueAddress = map.probeBatch(record, mapSink, batchStart, batchEnd, batchAddr);

            // Update phase — one call per function per sub-batch. The encoded rowIndex in each
            // batch entry is already the frame-relative row id, so computeKeyedBatch computes
            // the global row id as baseRowId + rowIndex.
            if (compiledUpdater != null && compiledUpdater.update(record, baseValueAddress, batchAddr, batchRows, circuitBreaker)) {
                continue;
            }
            for (int i = 0; i < functionCount; i++) {
                functions.getQuick(i).computeKeyedBatch(
                        record,
                        mapValue,
                        baseValueAddress,
                        batchAddr,
                        batchRows,
                        baseRowId
                );
            }
        }
    }

    private static boolean aggregateShardedCompiled(
            PageFrameMemoryRecord record,
            long rowCount,
            long rowIdsAddress,
            AsyncGroupByAtom atom,
            int slotId,
            GroupByMapFragment fragment,
            RecordSink mapSink,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final GroupByKernel.Updater updater = atom.getCompiledUpdater(slotId);
        return updater != null && updater.updateSharded(record, rowCount, rowIdsAddress,
                fragment.getShards(), mapSink, atom.getFunctionUpdater(slotId), circuitBreaker);
    }

    private static void aggregateSharded(
            PageFrameMemoryRecord record,
            long frameRowCount,
            long baseRowId,
            GroupByFunctionsUpdater functionUpdater,
            GroupByMapFragment fragment,
            RecordSink mapSink
    ) {
        // The first map is used to write keys.
        final Map lookupShard = fragment.getShards().getQuick(0);
        for (long r = 0; r < frameRowCount; r++) {
            record.setRowIndex(r);

            final MapKey lookupKey = lookupShard.withKey();
            mapSink.copy(record, lookupKey);
            lookupKey.commit();
            final long hashCode = lookupKey.hash();

            final Map shard = fragment.getShardMap(hashCode);
            final MapKey shardKey;
            if (shard != lookupShard) {
                shardKey = shard.withKey();
                shardKey.copyFrom(lookupKey);
            } else {
                shardKey = lookupKey;
            }

            MapValue shardValue = shardKey.createValue(hashCode);
            if (shardValue.isNew()) {
                functionUpdater.updateNew(shardValue, record, baseRowId + r);
            } else {
                functionUpdater.updateExisting(shardValue, record, baseRowId + r);
            }
        }
    }

    private static void filterAndAggregate(
            int workerId,
            @NotNull PageFrameMemoryRecord record,
            int frameIndex,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @NotNull UnorderedPageFrameSequence<?> frameSequence,
            @Nullable UnorderedPageFrameSequence<?> stealingFrameSequence
    ) {
        @SuppressWarnings("unchecked") final AsyncGroupByAtom atom = ((UnorderedPageFrameSequence<AsyncGroupByAtom>) frameSequence).getAtom();
        final long frameRowCount = frameSequence.getFrameRowCount(frameIndex);
        assert frameRowCount > 0;

        final boolean owner = stealingFrameSequence == frameSequence;
        final int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);
        final AsyncFilterContext filterCtx = atom.getFilterContext();
        final PageFrameAddressCache addressCache = frameSequence.getPageFrameAddressCache();
        final boolean isParquetFrame = addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET;
        final boolean useLateMaterialization = filterCtx.shouldUseLateMaterialization(slotId, isParquetFrame);

        final PageFrameMemoryPool frameMemoryPool = filterCtx.getMemoryPool(slotId);

        final DirectLongList rows = filterCtx.getFilteredRows(slotId);
        rows.clear();

        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(slotId);
        final GroupByMapFragment fragment = atom.getFragment(slotId);
        final CompiledFilter compiledFilter = filterCtx.getCompiledFilter();
        final Function filter = filterCtx.getFilter(slotId);
        final RecordSink mapSink = atom.getMapSink(slotId);
        // navigateTo() can throw; it must sit inside the try that releases the slot. See
        // aggregate() for why a leaked slot is permanent.
        try {
            final PageFrameMemory frameMemory;
            if (useLateMaterialization) {
                frameMemory = frameMemoryPool.navigateTo(frameIndex, filterCtx.getFilterUsedColumnIndexes());
            } else {
                frameMemory = frameMemoryPool.navigateTo(frameIndex);
            }
            record.init(frameMemory);

            atom.resetLocalStats(slotId);

            final GroupByKernel.Updater compiledUpdater = atom.getCompiledUpdater(slotId);
            if (compiledUpdater != null && compiledUpdater.hasFusedFilter() && !useLateMaterialization
                    && !frameMemory.hasColumnTops() && !frameMemory.hasColumnTypeCasts()) {
                if (atom.isSharded()) fragment.shard(circuitBreaker::statefulThrowExceptionIfTrippedNoThrottle);
                boolean local = fragment.isNotSharded();
                long selected = compiledUpdater.updateFiltered(record, frameRowCount,
                        local ? fragment.reopenMap() : null, local ? null : fragment.getShards(),
                        atom.getBatchSize(), rows, circuitBreaker);
                if (selected >= 0) {
                    if (isParquetFrame) filterCtx.getSelectivityStats(slotId).update(selected, frameRowCount);
                    atom.maybeEnableSharding(fragment);
                    return;
                }
            }
            final boolean filtered = compiledUpdater != null && !frameMemory.hasColumnTops() && !frameMemory.hasColumnTypeCasts()
                    && compiledUpdater.filter(record, frameRowCount, rows, circuitBreaker);
            if (!filtered && (compiledFilter == null || frameMemory.hasColumnTops() || frameMemory.hasColumnTypeCasts())) {
                AsyncFilterUtils.applyFilter(filter, rows, record, frameRowCount);
            } else if (!filtered) {
                AsyncFilterUtils.applyCompiledFilter(
                        compiledFilter,
                        filterCtx.getBindVarMemory(),
                        filterCtx.getBindVarFunctions(),
                        frameMemory,
                        addressCache,
                        filterCtx.getDataAddresses(slotId),
                        filterCtx.getAuxAddresses(slotId),
                        rows,
                        frameRowCount
                );
            }

            if (isParquetFrame) {
                filterCtx.getSelectivityStats(slotId).update(rows.size(), frameRowCount);
            }
            // Late materialization installs a PageFrameFilteredMemoryRecord that requires both
            // the absolute and the compacted row index via setFilteredRowIndex. The batched
            // probe/update path bottoms out in Map.probeBatch* and computeKeyedBatch, which call
            // setRowIndex(long) and have no way to supply a compacted index, so fall back to the
            // per-row path in that case.
            boolean lateMaterialized = false;
            if (useLateMaterialization && frameMemory.populateRemainingColumns(filterCtx.getFilterUsedColumnIndexes(), rows, false)) {
                PageFrameFilteredMemoryRecord filteredMemoryRecord = filterCtx.getPageFrameFilteredMemoryRecord(slotId);
                filteredMemoryRecord.of(frameMemory, record, filterCtx.getFilterUsedColumnIndexes());
                record = filteredMemoryRecord;
                lateMaterialized = true;
            }

            if (atom.isSharded()) {
                fragment.shard(circuitBreaker::statefulThrowExceptionIfTrippedNoThrottle);
            }

            long baseRowId = Rows.toRowID(frameIndex, 0);

            if (fragment.isNotSharded()) {
                if (lateMaterialized) {
                    aggregateFilteredNonSharded(record, rows, baseRowId, functionUpdater, fragment, mapSink);
                } else {
                    aggregateFilteredNonShardedBatched(record, rows, baseRowId, atom, slotId, fragment, mapSink, circuitBreaker);
                }
            } else if (!aggregateShardedCompiled(record, rows.size(), rows.getAddress(), atom, slotId, fragment, mapSink, circuitBreaker)) {
                aggregateFilteredSharded(record, rows, baseRowId, functionUpdater, fragment, mapSink);
            }

            atom.maybeEnableSharding(fragment);
        } finally {
            try {
                frameMemoryPool.releaseParquetBuffers();
            } finally {
                atom.release(slotId);
            }
        }
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final AsyncGroupByRecordCursor cursor = this.cursor;
        this.cursor = null;
        final UnorderedPageFrameSequence<AsyncGroupByAtom> frameSequence = this.frameSequence;
        this.frameSequence = null;
        final ObjList<Function> recordFunctions = this.recordFunctions;
        this.recordFunctions = null;
        final ObjList<AsyncGroupBySharedCursor> sharedCursors = this.sharedCursors;
        this.sharedCursors = null;
        final ObjList<ObjList<Function>> sharedRecordFunctions = this.sharedRecordFunctions;
        this.sharedRecordFunctions = null;

        Throwable cleanupFailure = Misc.freeBestEffort(null, base);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, cursor);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, frameSequence);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, recordFunctions); // groupByFunctions are included in recordFunctions
        cleanupFailure = GroupByRecordCursorFactory.freeSharedRecordFunctionsBestEffort(
                cleanupFailure,
                sharedRecordFunctions
        );
        // Shared cursors hold no native memory; primary state freed above covers it.
        Misc.clear(sharedCursors);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }
}
