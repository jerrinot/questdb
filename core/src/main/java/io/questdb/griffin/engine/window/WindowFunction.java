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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

/**
 * Execution contract for SQL window functions.
 * <p>
 * Window functions are planned into one of two executor shapes:
 * <ul>
 *     <li>The streaming fast path ({@link WindowRecordCursorFactory}), used when the input cursor is already in an
 *     order that satisfies the window definition and the function explicitly opts into streaming via
 *     {@link #supportsStreamingFastPath()}. In this mode the executor advances the function row-by-row via
 *     {@link #computeNext(Record)} directly on the base cursor and emits each row immediately.</li>
 *     <li>The cached executor ({@link CachedWindowRecordCursorFactory}), used when window evaluation must be decoupled
 *     from the base cursor iteration order. This happens when ordered windows need their own traversal order, when a
 *     function needs to revisit already seen rows, when evaluation is naturally backward, or when a second pass is
 *     required. In this mode the executor first copies the base cursor into a record chain, appends fixed-width result
 *     and scratch columns to that chain, builds any required ordered row-id trees, runs one primary cached traversal
 *     and an optional secondary cached pass, and only then serves result rows from the cached chain.</li>
 * </ul>
 * <p>
 * "Caching" here means materializing the input rows into executor-owned storage so window evaluation can read them
 * again in a different order than they arrived from the base cursor. That cached representation is what makes
 * `lead()`, backward `last_value()`, rank-style ordered traversals, and two-pass functions such as `percent_rank()`
 * possible without constraining result delivery to the computation order.
 * <p>
 * A function instance is reused across cursor openings. Executors initialize it through {@link Function#init(ObjList,
 * io.questdb.cairo.sql.SymbolTableSource, io.questdb.griffin.SqlExecutionContext, io.questdb.cairo.sql.BindVariableService)},
 * may call {@link Function#toTop()} when the cursor rewinds, may call {@link io.questdb.cairo.Reopenable#reopen()} on
 * reopenable implementations when reopening a cursor, and finally call {@link #reset()} when closing the cursor and
 * discarding the execution state. Implementations should treat {@code computeNext()}, cached traversal methods, and
 * scratch columns as per-execution state, and {@code reset()} as the point where any native buffers or other retained
 * state must be released or returned to the initial state.
 * <p>
 * Implementations should declare their execution behavior through the executor-facing methods below instead of relying
 * on planner or executor internals:
 * <ul>
 *     <li>Use {@link #isPrimaryCachedTraversalStreamable()} for functions whose primary cached traversal is just
 *     {@link #computeNext(Record)} plus materializing the current value through the getter implied by
 *     {@link #getType()}.</li>
 *     <li>Override {@link #processPrimaryCachedRow(Record, long, WindowSPI, CachedFunctionContext)} when cached
 *     execution needs custom traversal logic.</li>
 *     <li>Declare private fixed-width scratch storage through {@link #getScratchColumnCount()} and
 *     {@link #getScratchColumnType(int)} when cached execution needs temporary per-row state.</li>
 *     <li>Opt into a second cached traversal through {@link #needsSecondaryCachedPass()},
 *     {@link #prepareSecondaryCachedPass(CachedFunctionContext)}, and
 *     {@link #processSecondaryCachedRow(Record, long, WindowSPI, CachedFunctionContext)}.</li>
 * </ul>
 * Scratch columns are executor-managed implementation details. They are appended to the cached record-chain layout,
 * are visible only through {@link CachedFunctionContext}, and never become part of the query result schema.
 */
public interface WindowFunction extends Function {
    /**
     * Planner-owned cached layout for a single window function result column plus its private scratch columns.
     */
    final class CachedFunctionLayout {
        private final int resultColumnIndex;
        private final IntList scratchColumnIndexes = new IntList();
        private final IntList scratchColumnTypes = new IntList();

        public CachedFunctionLayout(int resultColumnIndex) {
            this.resultColumnIndex = resultColumnIndex;
        }

        public void addScratchColumnType(int columnType) {
            scratchColumnTypes.add(columnType);
            scratchColumnIndexes.add(-1);
        }

        public int getResultColumnIndex() {
            return resultColumnIndex;
        }

        public int getScratchColumnCount() {
            return scratchColumnTypes.size();
        }

        public int getScratchColumnIndex(int index) {
            return scratchColumnIndexes.getQuick(index);
        }

        public int getScratchColumnType(int index) {
            return scratchColumnTypes.getQuick(index);
        }

        public void setScratchColumnIndex(int index, int columnIndex) {
            scratchColumnIndexes.setQuick(index, columnIndex);
        }
    }

    /**
     * Bound cached-executor columns for a single window-function instance.
     * <p>
     * The context is created by the cached executor after it finalizes the record-chain layout. Result and scratch
     * columns refer to fixed-width storage owned by that executor.
     */
    final class CachedFunctionContext {
        private final WindowSPI.FixedSizeColumn resultColumn;
        private final ObjList<WindowSPI.FixedSizeColumn> scratchColumns;

        public CachedFunctionContext(WindowSPI.FixedSizeColumn resultColumn, ObjList<WindowSPI.FixedSizeColumn> scratchColumns) {
            this.resultColumn = resultColumn;
            this.scratchColumns = scratchColumns;
        }

        /**
         * Returns the bound output column for this function.
         */
        public WindowSPI.FixedSizeColumn getResultColumn() {
            return resultColumn;
        }

        /**
         * Returns the bound scratch column declared by {@link #getScratchColumnType(int)}.
         *
         * @param index scratch column index in the range {@code [0, getScratchColumnCount())}
         */
        public WindowSPI.FixedSizeColumn getScratchColumn(int index) {
            if (scratchColumns == null) {
                throw new IndexOutOfBoundsException("scratch column is not bound");
            }
            return scratchColumns.getQuick(index);
        }
    }

    enum PrimaryCachedTraversalDirection {
        /**
         * Visit rows in natural cached order.
         */
        FORWARD,
        /**
         * Visit rows in reverse cached order.
         */
        BACKWARD
    }

    /**
     * Advances the function by one input row.
     * <p>
     * This method is used by the streaming fast path and by the default implementation of
     * {@link #processPrimaryCachedRow(Record, long, WindowSPI, CachedFunctionContext)} for functions that declare
     * {@link #isPrimaryCachedTraversalStreamable()}.
     */
    default void computeNext(Record record) {
    }

    @Override
    default ArrayView getArray(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default BinarySequence getBin(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getBinLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean getBool(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default char getChar(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDate(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal128(Record rec, Decimal128 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getDecimal16(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getDecimal256(Record rec, Decimal256 sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getDecimal32(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getDecimal64(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getDecimal8(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default double getDouble(Record rec) {
        // unused
        throw new UnsupportedOperationException();
    }

    @Override
    default float getFloat(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default byte getGeoByte(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getGeoInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getGeoLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getGeoShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getIPv4(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getInt(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default @NotNull Interval getInterval(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Hi(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getLong128Lo(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void getLong256(Record rec, CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256A(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Long256 getLong256B(Record rec) {
        throw new UnsupportedOperationException();
    }

    /**
     * Declares the direction used by the cached executor for the primary traversal.
     * <p>
     * Return {@link PrimaryCachedTraversalDirection#BACKWARD} only when primary cached evaluation must see later rows
     * before earlier ones, for example `lead()`-style logic or backward-looking `last_value()` implementations.
     * This does not by itself make the function eligible for the streaming fast path.
     */
    default PrimaryCachedTraversalDirection getPrimaryCachedTraversalDirection() {
        return PrimaryCachedTraversalDirection.FORWARD;
    }

    /**
     * Declares how many private scratch columns the cached executor must allocate for this function.
     * <p>
     * Scratch columns are fixed-width record-chain columns used only during cached execution. They are not visible in
     * query results and are accessed only through {@link CachedFunctionContext}.
     */
    default int getScratchColumnCount() {
        return 0;
    }

    /**
     * Returns the type of the scratch column at {@code index}.
     * <p>
     * This is called only for {@code index < getScratchColumnCount()}. Implementations must return a fixed-width
     * column type supported by {@link WindowSPI.FixedSizeColumn}.
     *
     * @param index scratch column index in the range {@code [0, getScratchColumnCount())}
     */
    default int getScratchColumnType(int index) {
        throw new UnsupportedOperationException();
    }

    /**
     * Declares whether the primary cached traversal can be implemented by calling {@link #computeNext(Record)} and
     * materializing the current value through the getter implied by {@link #getType()}.
     * <p>
     * When this returns {@code true}, the default
     * {@link #processPrimaryCachedRow(Record, long, WindowSPI, CachedFunctionContext)} implementation is sufficient.
     * When it returns {@code false}, the function must override
     * {@link #processPrimaryCachedRow(Record, long, WindowSPI, CachedFunctionContext)}.
     * <p>
     * This only describes cached primary traversal. A function may still require cached execution even if this returns
     * {@code true}, for example because it needs backward traversal or a secondary cached pass.
     */
    default boolean isPrimaryCachedTraversalStreamable() {
        return false;
    }

    /**
     * Planner capability check for the streaming fast path.
     * <p>
     * The default implementation requires:
     * <ul>
     *     <li>a streamable primary cached traversal,</li>
     *     <li>forward primary traversal direction, and</li>
     *     <li>no secondary cached pass.</li>
     * </ul>
     * Most implementations should express their behavior through the other contract methods and inherit this default.
     */
    default boolean supportsStreamingFastPath() {
        return isPrimaryCachedTraversalStreamable()
                && getPrimaryCachedTraversalDirection() == PrimaryCachedTraversalDirection.FORWARD
                && !needsSecondaryCachedPass();
    }

    @Override
    default RecordCursorFactory getRecordCursorFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    default short getShort(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getStrB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getStrLen(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbol(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CharSequence getSymbolB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getTimestamp(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharA(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default Utf8Sequence getVarcharB(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getVarcharSize(Record rec) {
        throw new UnsupportedOperationException();
    }

    default void initRecordComparator(
            SqlCodeGenerator sqlGenerator,
            RecordMetadata metadata,
            ArrayColumnTypes chainTypes,
            IntList orderIndices,
            ObjList<ExpressionNode> orderBy,
            IntList orderByDirections
    ) throws SqlException {
    }

    default boolean isIgnoreNulls() {
        return false;
    }

    /**
     * Declares whether cached execution requires a second traversal after the primary cached traversal completes.
     */
    default boolean needsSecondaryCachedPass() {
        return false;
    }

    /**
     * Called once after the primary cached traversal and before the secondary cached pass begins.
     * <p>
     * Implementations can use this hook to finalize aggregate state, derive constants from scratch storage, or prepare
     * secondary-pass state.
     */
    default void prepareSecondaryCachedPass(CachedFunctionContext context) {
    }

    /**
     * Processes one row during the primary cached traversal.
     * <p>
     * {@code recordOffset} identifies the current row inside {@code spi}. {@code context} provides the bound result
     * column and any scratch columns declared by this function.
     * <p>
     * The default implementation is valid only for functions that declare
     * {@link #isPrimaryCachedTraversalStreamable()}: it calls {@link #computeNext(Record)} and writes the current
     * value into {@link CachedFunctionContext#getResultColumn()} using the getter implied by {@link #getType()}.
     * Non-streamable cached implementations must override this method.
     */
    default void processPrimaryCachedRow(Record record, long recordOffset, WindowSPI spi, CachedFunctionContext context) {
        if (isPrimaryCachedTraversalStreamable()) {
            computeNext(record);
            context.getResultColumn().putValue(recordOffset, this, record);
            return;
        }
        throw new UnsupportedOperationException();
    }

    /**
     * Processes one row during the secondary cached pass.
     * <p>
     * This method is called only when {@link #needsSecondaryCachedPass()} returns {@code true}. Implementations can
     * read rows through {@code spi}, consume scratch state via {@code context}, and emit final results into
     * {@link CachedFunctionContext#getResultColumn()}.
     */
    default void processSecondaryCachedRow(Record record, long recordOffset, WindowSPI spi, CachedFunctionContext context) {
        throw new UnsupportedOperationException();
    }

    /**
     * Releases native memory and resets internal state to default/initial.
     * It differs from close() in that it doesn't release memory held by metadata, e.g. partition by key functions.
     * This means function may still be used after calling reopen().
     * <p>
     * Executors call this when tearing down the current cursor state. Unlike {@link Function#toTop()}, which rewinds an
     * in-flight execution, {@code reset()} must leave the function ready for a fresh {@link Function#init(ObjList,
     * io.questdb.cairo.sql.SymbolTableSource, io.questdb.griffin.SqlExecutionContext,
     * io.questdb.cairo.sql.BindVariableService)} / execution cycle.
     **/
    void reset();
}
