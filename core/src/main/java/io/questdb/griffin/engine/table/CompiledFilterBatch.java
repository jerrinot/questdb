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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** A scalar predicate with a bounded compiled path over borrowed page frames. */
public class CompiledFilterBatch extends BooleanFunction implements UnaryFunction {
    public static final int BATCH_SIZE = 2048;
    private static final ThreadLocal<BatchObserver> TEST_BATCH_OBSERVER = new ThreadLocal<>();
    private final DirectLongList auxAddresses;
    private final MemoryCARW bindVarMemory;
    private final DirectLongList dataAddresses;
    private final IntList filterColumnIndexes;
    private final DirectLongList filteredRows;
    private BatchObserver batchObserver;
    private ObjList<Function> bindVarFunctions;
    private CompiledFilter compiledFilter;
    private Function filter;
    private boolean executionPrepared;
    private long inputRows;
    private boolean isFrameCompiled;
    private int preparedFrameIndex = -1;

    public CompiledFilterBatch(
            CairoConfiguration configuration,
            Function filter,
            CompiledFilter compiledFilter,
            ObjList<Function> bindVarFunctions,
            IntList filterColumnIndexes
    ) {
        this.filter = filter;
        this.compiledFilter = compiledFilter;
        this.bindVarFunctions = bindVarFunctions;
        this.filterColumnIndexes = filterColumnIndexes;
        bindVarMemory = Vm.getCARWInstance(configuration.getSqlJitBindVarsMemoryPageSize(),
                configuration.getSqlJitBindVarsMemoryMaxPages(), MemoryTag.NATIVE_JIT);
        filteredRows = new DirectLongList(Math.min(BATCH_SIZE, configuration.getPageFrameReduceRowIdListCapacity()), MemoryTag.NATIVE_OFFLOAD, true);
        dataAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD, true);
        auxAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD, true);
    }

    public static @Nullable DirectLongList apply(
            Function filter,
            PageFrameMemoryPool memoryPool,
            PageFrameAddressCache addressCache,
            int frameIndex,
            long rowLo,
            long rowHi
    ) {
        assert rowHi > rowLo && rowHi - rowLo <= BATCH_SIZE;
        if (filter instanceof CompiledFilterBatch jit) {
            try {
                PageFrameMemory memory = memoryPool.navigateTo(frameIndex);
                return jit.apply(memory, addressCache.getColumnTypes(), rowLo, rowHi);
            } finally {
                memoryPool.releaseFrameMemory();
            }
        }
        return null;
    }

    public @Nullable DirectLongList apply(PageFrameMemory memory, IntList columnTypes, long rowLo, long rowHi) {
        assert rowHi > rowLo && rowHi - rowLo <= BATCH_SIZE;
        if (memory.getFrameIndex() != preparedFrameIndex) {
            prepareFrame(memory, columnTypes.size());
            preparedFrameIndex = memory.getFrameIndex();
        }
        if (isFrameCompiled) {
            if (batchObserver != null) {
                batchObserver.onJitBatch();
            }
            applyBatch(memory, columnTypes, rowLo, rowHi - rowLo);
            inputRows += rowHi - rowLo;
            return filteredRows;
        }
        if (batchObserver != null) {
            batchObserver.onJavaFallbackFrame();
        }
        return null;
    }

    public void clearExecution() {
        Throwable failure = null;
        if (executionPrepared) {
            executionPrepared = false;
            try {
                compiledFilter.clearExecution();
            } catch (Throwable th) {
                failure = th;
            }
        }
        failure = closeBuffers(failure);
        filteredRows.setMemoryTracker(null);
        dataAddresses.setMemoryTracker(null);
        auxAddresses.setMemoryTracker(null);
        bindVarMemory.setMemoryTracker(null);
        preparedFrameIndex = -1;
        CairoException.rethrowCleanupFailure(failure);
    }

    public CharSequence getBackendName() {
        CharSequence name = compiledFilter.getBackendName();
        return name == null ? "asmjit" : name;
    }

    public long getInputRows() {
        return inputRows;
    }

    @TestOnly
    public static void clearBatchObserverForTesting() {
        TEST_BATCH_OBSERVER.remove();
    }

    @TestOnly
    public static void setBatchObserverForTesting(BatchObserver observer) {
        TEST_BATCH_OBSERVER.set(observer);
    }

    @Override
    public void close() {
        Throwable failure = null;
        try {
            clearExecution();
        } catch (Throwable th) {
            failure = th;
        }
        final Function filter = this.filter;
        this.filter = null;
        final CompiledFilter compiledFilter = this.compiledFilter;
        this.compiledFilter = null;
        final ObjList<Function> bindVarFunctions = this.bindVarFunctions;
        this.bindVarFunctions = null;
        failure = Misc.freeBestEffort(failure, compiledFilter);
        failure = Misc.freeObjListBestEffort(failure, bindVarFunctions);
        failure = Misc.freeBestEffort(failure, filter);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void cursorClosed() {
        try {
            if (filter != null) {
                filter.cursorClosed();
            }
        } finally {
            clearExecution();
        }
    }

    @Override
    public Function getArg() {
        return filter;
    }

    @Override
    public boolean getBool(Record record) {
        return filter.getBool(record);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        clearExecution();
        batchObserver = TEST_BATCH_OBSERVER.get();
        preparedFrameIndex = -1;
        inputRows = 0;
        final var tracker = executionContext == null ? null : executionContext.getMemoryTracker();
        filteredRows.setMemoryTracker(tracker);
        dataAddresses.setMemoryTracker(tracker);
        auxAddresses.setMemoryTracker(tracker);
        bindVarMemory.setMemoryTracker(tracker);
        filteredRows.reopen();
        dataAddresses.reopen();
        auxAddresses.reopen();
        filter.init(symbolTableSource, executionContext);
        Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
        AsyncFilterUtils.prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        executionPrepared = true;
        compiledFilter.prepareExecution(tracker);
        compiledFilter.prepareForDispatch();
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        filter.toPlan(sink);
    }

    private void applyBatch(PageFrameMemory memory, IntList columnTypes, long rowLo, long rowCount) {
        for (int i = 0, n = filterColumnIndexes.size(); i < n; i++) {
            final int columnIndex = filterColumnIndexes.getQuick(i);
            final int columnType = columnTypes.getQuick(columnIndex);
            if (ColumnType.isVarSize(columnType)) {
                dataAddresses.set(columnIndex, memory.getPageAddress(columnIndex));
                auxAddresses.set(columnIndex, memory.getAuxPageAddress(columnIndex)
                        + ColumnType.getDriver(columnType).getAuxVectorOffset(rowLo));
            } else {
                dataAddresses.set(columnIndex, memory.getPageAddress(columnIndex) + (rowLo << ColumnType.pow2SizeOf(columnType)));
            }
        }
        if (filteredRows.getCapacity() < rowCount) {
            filteredRows.setCapacity(rowCount);
        }
        long hi = compiledFilter.call(
                dataAddresses.getAddress(),
                dataAddresses.size(),
                auxAddresses.getAddress(),
                bindVarMemory.getAddress(),
                bindVarFunctions.size(),
                filteredRows.getAddress(),
                rowCount,
                memory
        );
        filteredRows.setPos(hi);
    }

    private Throwable closeBuffers(Throwable failure) {
        failure = Misc.freeBestEffort(failure, filteredRows);
        failure = Misc.freeBestEffort(failure, dataAddresses);
        failure = Misc.freeBestEffort(failure, auxAddresses);
        return Misc.freeBestEffort(failure, bindVarMemory);
    }

    private void prepareFrame(PageFrameMemory memory, int columnCount) {
        isFrameCompiled = false;
        if (memory.hasColumnTypeCasts()) {
            return;
        }
        for (int i = 0, n = filterColumnIndexes.size(); i < n; i++) {
            final int columnIndex = filterColumnIndexes.getQuick(i);
            if (memory.getPageAddress(columnIndex) == 0 && memory.getAuxPageAddress(columnIndex) == 0) {
                return;
            }
        }
        // An allocation failure can leave only one address table complete.
        if (dataAddresses.size() != columnCount || auxAddresses.size() != columnCount) {
            dataAddresses.clear();
            auxAddresses.clear();
            for (int i = 0; i < columnCount; i++) {
                dataAddresses.add(0);
                auxAddresses.add(0);
            }
        }
        isFrameCompiled = true;
    }

    static void addJitAttr(PlanSink sink, Function filter) {
        if (filter instanceof CompiledFilterBatch) {
            sink.attr("jit").val(true);
        }
    }

    public interface BatchObserver {
        void onJavaFallbackFrame();

        void onJitBatch();
    }
}
