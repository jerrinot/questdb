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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.cast.CastSymbolToStrFunctionFactory;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.str.LengthStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.LengthSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.str.StrPosFunctionFactory;
import io.questdb.griffin.engine.functions.str.ToLowercaseFunctionFactory;
import io.questdb.griffin.engine.functions.str.ToUppercaseFunctionFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;

/** Borrowed resolved expression; immutable lookup results live only for the current cursor. */
final class SymbolExpression implements AutoCloseable {
    private final Function function;
    private final SymbolColumn symbol;
    private final DirectLongList values = new DirectLongList(0, MemoryTag.NATIVE_JIT, true);
    private int minimum;
    private int maximum;

    private SymbolExpression(Function function, SymbolColumn symbol) {
        this.function = function;
        this.symbol = symbol;
    }

    static SymbolExpression bind(Function function) {
        if (function.getType() != ColumnType.INT) return null;
        Class<?> type = function.getClass();
        if (type != StrPosFunctionFactory.ConstFunc.class && type != LengthStrFunctionFactory.Func.class
                && type != LengthSymbolFunctionFactory.LengthSymbolVFunc.class) return null;
        Function input = ((UnaryFunction) function).getArg();
        // Only these total, deterministic functions may be evaluated on unused dictionary entries.
        // All leaves must be the same static SYMBOL; arbitrary functions and runtime arguments decline.
        for (int depth = 0; depth < 64; depth++) {
            if (input.getClass() == SymbolColumn.class) {
                var column = (SymbolColumn) input;
                return column.isSymbolTableStatic() ? new SymbolExpression(function, column) : null;
            }
            type = input.getClass();
            if (type != CastSymbolToStrFunctionFactory.Func.class
                    && type != ToLowercaseFunctionFactory.ToLowercaseFunc.class
                    && type != ToUppercaseFunctionFactory.ToUppercaseFunc.class) return null;
            input = ((UnaryFunction) input).getArg();
        }
        return null;
    }

    SymbolColumn symbol() { return symbol; }
    long address() { return values.getAddress(); }
    int minimum() { return minimum; }
    long range() { return minimum > maximum ? 0 : (long) maximum - minimum + 1; }

    void prepare(MemoryTracker tracker, SqlExecutionCircuitBreaker breaker, long evaluationBudget) {
        close();
        values.setMemoryTracker(tracker);
        var dictionary = symbol.getStaticSymbolTable();
        if (dictionary == null) throw new IllegalStateException("Static SYMBOL lost its dictionary");
        int count = dictionary.getSymbolCount();
        // Serial preparation must do less expression work than one parallel scan slot.
        // Unknown/filtered row counts decline instead of assuming all table rows survive.
        if (evaluationBudget < 0 || count + 1L >= evaluationBudget) return;
        var record = new DictionaryRecord(symbol.getColumnIndex());
        minimum = Integer.MAX_VALUE;
        maximum = Integer.MIN_VALUE;
        try {
            breaker.statefulThrowExceptionIfTripped();
            values.ensureCapacity(((long) count + 2) / 2);
            for (long entry = 0; entry <= count; entry++) {
                if ((entry & 1023) == 0) breaker.statefulThrowExceptionIfTripped();
                record.key = entry == 0 ? io.questdb.cairo.sql.SymbolTable.VALUE_IS_NULL : (int) entry - 1;
                int value = function.getInt(record);
                Unsafe.putInt(values.getAddress() + entry * 4, value);
                if (value != Integer.MIN_VALUE) {
                    minimum = Math.min(minimum, value);
                    maximum = Math.max(maximum, value);
                }
            }
        } catch (Throwable failure) {
            close();
            throw failure;
        }
    }

    @Override public void close() { values.close(); }

    private static final class DictionaryRecord implements Record {
        private final int column;
        private int key;
        private DictionaryRecord(int column) { this.column = column; }
        @Override public int getInt(int column) {
            if (column != this.column) throw new IllegalArgumentException("Expression reads another column");
            return key;
        }
    }
}
