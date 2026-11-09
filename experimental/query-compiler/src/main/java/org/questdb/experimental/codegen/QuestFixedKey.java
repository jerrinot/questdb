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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.map.OrderedMap;
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.MapKernelState.*;

/** Fixed-width RecordSink encoding; direct columns followed by resolved computed keys. */
record QuestFixedKey(ResolvedVarchar.Binding frameInputs,
                     java.util.List<Pipeline.Scalar> expressions, MemoryType[] widths, long[] nullValues, int bytes,
                     java.util.List<SymbolExpression> dictionaries) {
    /** Raw RecordSink bytes, including floating-point payload bits. */
    static MemoryType width(int type) {
        return switch (type) {
            case ColumnType.BOOLEAN, ColumnType.BYTE -> MemoryType.I8;
            case ColumnType.SHORT, ColumnType.CHAR -> MemoryType.I16;
            case ColumnType.INT, ColumnType.SYMBOL, ColumnType.IPv4, ColumnType.FLOAT -> MemoryType.I32;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO, ColumnType.DOUBLE -> MemoryType.I64;
            default -> null;
        };
    }

    static long nullValue(int type) {
        return switch (type) {
            case ColumnType.FLOAT -> Float.floatToRawIntBits(Float.NaN);
            case ColumnType.DOUBLE -> Double.doubleToRawLongBits(Double.NaN);
            default -> Pipeline.nullValueOf(type);
        };
    }

    static QuestFixedKey bind(ColumnTypes inputs, ColumnTypes types, ListColumnFilter columns, OrderedMap map) {
        return bind(inputs, types, columns, null, map);
    }

    static QuestFixedKey bind(ColumnTypes inputTypes, ColumnTypes types, ListColumnFilter columns,
                             io.questdb.std.ObjList<io.questdb.cairo.sql.Function> functions, io.questdb.cairo.map.Map map) {
        return bind(new ResolvedInputs(inputTypes), types, columns, functions, map);
    }

    static QuestFixedKey bind(ResolvedInputs inputs, ColumnTypes types, ListColumnFilter columns,
                             io.questdb.std.ObjList<io.questdb.cairo.sql.Function> functions, io.questdb.cairo.map.Map map) {
        ColumnTypes inputTypes = inputs.types();
        int computed = functions == null ? 0 : functions.size(), count = types.getColumnCount();
        if (count == 0 || count != columns.size() + computed) return null;
        var strings = inputs.strings();
        var expressions = new java.util.ArrayList<Pipeline.Scalar>();
        var dictionaries = new java.util.ArrayList<SymbolExpression>();
        var dictionaryPositions = new java.util.ArrayList<Integer>();
        MemoryType[] widths = new MemoryType[count];
        long[] nullValues = new long[count];
        int bytes = 0;
        for (int i = 0; i < count; i++) {
            int type = types.getColumnType(i);
            nullValues[i] = nullValue(type);
            widths[i] = width(type);
            if (widths[i] == null) return null;
            Pipeline.Scalar expression;
            if (i < columns.size()) {
                int index = columns.getQuick(i) - 1;
                if (index < 0 || index >= inputTypes.getColumnCount() || inputTypes.getColumnType(index) != type) return null;
                expression = inputs.column(index, type);
            } else {
                var function = functions.getQuick(i - columns.size());
                if (function.getType() != type) return null;
                expression = inputs.scalar(function, type);
                if (expression == null) {
                    var dictionary = SymbolExpression.bind(function);
                    if (dictionary == null) return null;
                    expression = inputs.column(dictionary.symbol(), ColumnType.SYMBOL);
                    if (expression == null) return null;
                    dictionaries.add(dictionary);
                    dictionaryPositions.add(i);
                }
            }
            if (expression == null) return null;
            expressions.add(expression);
            bytes += widths[i].bytes;
        }
        if (map instanceof OrderedMap ordered) {
            if (bytes != ordered.getFixedKeySize()) return null;
        } else if (!(count == 1 && (io.questdb.cairo.map.Unordered4Map.isSupportedKeyType(types.getColumnType(0)) && map instanceof io.questdb.cairo.map.Unordered4Map
                || io.questdb.cairo.map.Unordered8Map.isSupportedKeyType(types.getColumnType(0))
                && map instanceof io.questdb.cairo.map.Unordered8Map))) return null;
        for (int i = 0; i < dictionaries.size(); i++) {
            int position = dictionaryPositions.get(i);
            Expr pointers = new Load(MemoryType.ADDRESS, at(STATE, KEY_COLUMNS));
            Expr table = new Load(MemoryType.ADDRESS, at(pointers, strings.binding().bytes() + i * 8L));
            expressions.set(position, new Pipeline.DictionaryLookup(expressions.get(position), table));
        }
        return new QuestFixedKey(strings.binding(), java.util.List.copyOf(expressions), widths, nullValues, bytes,
                java.util.List.copyOf(dictionaries));
    }

    int[] columns() { return frameInputs.numericColumns(); }
    java.util.List<Pipeline.Column> inputs() { return frameInputs.numericInputs(); }
    long descriptorBytes() { return frameInputs.bytes() + dictionaries.size() * 8L; }

    void prepareExecution(io.questdb.std.MemoryTracker tracker, io.questdb.cairo.sql.SqlExecutionCircuitBreaker breaker, long evaluationBudget) {
        try {
            for (var dictionary : dictionaries) dictionary.prepare(tracker, breaker, evaluationBudget);
        } catch (Throwable failure) {
            clearExecution();
            throw failure;
        }
    }

    void clearExecution() { for (var dictionary : dictionaries) dictionary.close(); }

    boolean prepare(io.questdb.cairo.sql.PageFrameMemoryRecord record, long pointers) {
        if (!frameInputs.prepare(record, pointers)) return false;
        for (int i = 0; i < dictionaries.size(); i++) {
            if (dictionaries.get(i).address() == 0) return false;
            io.questdb.std.Unsafe.putLong(pointers + frameInputs.bytes() + i * 8L, dictionaries.get(i).address());
        }
        return true;
    }

    private Pipeline.Row loadRow(Builder b, Expr row) {
        Var pointers = b.let(new Load(MemoryType.ADDRESS, at(STATE, KEY_COLUMNS)));
        return frameInputs.frame(ResolvedInputs.loadFrameRow(b, inputs(), pointers, row), row, pointers).numeric(b);
    }

    private Expr storedKey(Pipeline.Row row, int index) {
        var value = expressions.get(index).emit(row);
        // RecordSink stores raw IEEE bits, including NaN payloads and zero's sign.
        if (value.data().type() == Type.F64 || value.data().type() == Type.F32) return new BitCast(value.data());
        // Widening casts retain the input expression's NULL flag, but the packed key
        // must use the result width's sentinel (INT NULL is not LONG NULL).
        return widths[index] == MemoryType.I32 || widths[index] == MemoryType.I64
                ? new Select(value.isNull(), i64(nullValues[index]), value.data()) : value.data();
    }

    Var scalar(Builder b, Expr index) { return scalar(b, loadRow(b, index)); }
    Var scalar(Builder b, Pipeline.Row row) { return b.let(storedKey(row, 0)); }

    Var stage(Builder b, Expr index, Expr destination) { return stage(b, loadRow(b, index), destination); }
    Var stage(Builder b, Pipeline.Row row, Expr destination) {
        if (java.nio.ByteOrder.nativeOrder() != java.nio.ByteOrder.LITTLE_ENDIAN) {
            int offset = 0;
            for (int i = 0; i < widths.length; i++) {
                b.store(widths[i], at(destination, offset), storedKey(row, i));
                offset += widths[i].bytes;
            }
            return b.let(destination);
        }
        // Hashing reads packed words, which can straddle SQL fields. Assemble those
        // words in registers: a wide load spanning separate narrow stores cannot
        // forward from the store queue. The RecordSink byte layout stays identical.
        Var[] values = new Var[widths.length];
        for (int i = 0; i < values.length; i++) values[i] = b.let(storedKey(row, i));
        int field = 0, fieldByte = 0, offset = 0;
        for (MemoryType wordWidth : new MemoryType[]{MemoryType.I64, MemoryType.I32, MemoryType.I16, MemoryType.I8}) {
            while (offset + wordWidth.bytes <= bytes) {
                Expr word = i64(0);
                int wordByte = 0;
                while (wordByte < wordWidth.bytes) {
                    int take = Math.min(wordWidth.bytes - wordByte, widths[field].bytes - fieldByte);
                    Expr part = values[field];
                    if (fieldByte > 0) part = binary(Op.USHR, part, i64(fieldByte * 8));
                    if (take < 8) part = binary(Op.AND, part, i64((1L << (take * 8)) - 1));
                    if (wordByte > 0) part = binary(Op.MUL, part, i64(1L << (wordByte * 8)));
                    word = binary(Op.XOR, word, part);
                    wordByte += take;
                    fieldByte += take;
                    if (fieldByte == widths[field].bytes) { field++; fieldByte = 0; }
                }
                b.store(wordWidth, at(destination, offset), word);
                offset += wordWidth.bytes;
            }
        }
        return b.let(destination);
    }

    Var hash(Builder b, Expr key) {
        Var h = b.let(i64(0));
        int offset = 0;
        for (; offset + 8 <= bytes; offset += 8) fold(b, h, load(at(key, offset)));
        if (offset + 4 <= bytes) { fold(b, h, new Load(MemoryType.I32, at(key, offset))); offset += 4; }
        for (; offset < bytes; offset++) fold(b, h, new Load(MemoryType.I8, at(key, offset)));
        b.assign(h, binary(Op.XOR, h, binary(Op.USHR, h, i64(37))));
        b.assign(h, binary(Op.MUL, h, i64(0x165667919E3779F9L)));
        b.assign(h, binary(Op.XOR, h, binary(Op.USHR, h, i64(32))));
        return h;
    }

    Expr equal(Expr left, Expr right) {
        Expr same = bool(true); int offset = 0;
        for (; offset + 8 <= bytes; offset += 8) same = binary(Op.BOTH, same, eq(load(at(left, offset)), load(at(right, offset))));
        if (offset + 4 <= bytes) {
            same = binary(Op.BOTH, same, eq(new Load(MemoryType.I32, at(left, offset)), new Load(MemoryType.I32, at(right, offset)))); offset += 4;
        }
        for (; offset < bytes; offset++) same = binary(Op.BOTH, same, eq(new Load(MemoryType.I8, at(left, offset)), new Load(MemoryType.I8, at(right, offset))));
        return same;
    }

    private static void fold(Builder b, Var h, Expr next) { b.assign(h, add(binary(Op.MUL, h, i64(0x517cc1b727220a95L)), next)); }
}
