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

/** Fixed-width RecordSink encoding; direct columns followed by resolved computed keys. */
record QuestFixedKey(int[] columns, java.util.List<Pipeline.Column> inputs,
                     java.util.List<Pipeline.Scalar> expressions, MemoryType[] widths, int bytes) {
    static QuestFixedKey bind(ColumnTypes inputs, ColumnTypes types, ListColumnFilter columns, OrderedMap map) {
        return bind(inputs, types, columns, null, map);
    }

    static QuestFixedKey bind(ColumnTypes inputTypes, ColumnTypes types, ListColumnFilter columns,
                             io.questdb.std.ObjList<io.questdb.cairo.sql.Function> functions, io.questdb.cairo.map.Map map) {
        int computed = functions == null ? 0 : functions.size(), count = types.getColumnCount();
        if (count == 0 || count != columns.size() + computed) return null;
        var inputs = new ResolvedInputs(inputTypes);
        var expressions = new java.util.ArrayList<Pipeline.Scalar>();
        MemoryType[] widths = new MemoryType[count];
        int bytes = 0;
        for (int i = 0; i < count; i++) {
            int type = types.getColumnType(i);
            widths[i] = switch (type) {
                case ColumnType.BYTE -> MemoryType.I8;
                case ColumnType.SHORT -> MemoryType.I16;
                case ColumnType.INT, ColumnType.SYMBOL, ColumnType.IPv4 -> MemoryType.I32;
                case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> MemoryType.I64;
                default -> null;
            };
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
                if (expression == null) return null;
            }
            expressions.add(expression);
            bytes += widths[i].bytes;
        }
        if (map instanceof OrderedMap ordered) {
            if (bytes != ordered.getFixedKeySize()) return null;
        } else if (!(count == 1 && ((types.getColumnType(0) == ColumnType.INT || types.getColumnType(0) == ColumnType.SYMBOL) && map instanceof io.questdb.cairo.map.Unordered4Map
                || types.getColumnType(0) == ColumnType.LONG && map instanceof io.questdb.cairo.map.Unordered8Map))) return null;
        return new QuestFixedKey(inputs.indexes(), inputs.columns(), java.util.List.copyOf(expressions), widths, bytes);
    }

    private Pipeline.Row loadRow(Builder b, Expr row) {
        Var pointers = b.let(new Load(MemoryType.ADDRESS, at(STATE, 16)));
        return ResolvedInputs.loadFrameRow(b, inputs, pointers, row);
    }

    private Expr storedKey(Pipeline.Row row, int index) {
        var value = expressions.get(index).emit(row);
        // Widening casts retain the input expression's NULL flag, but the packed key
        // must use the result width's sentinel (INT NULL is not LONG NULL).
        long nullValue = widths[index] == MemoryType.I64 ? Long.MIN_VALUE
                : expressions.get(index) instanceof Pipeline.Column column ? column.nullValue() : Integer.MIN_VALUE;
        return widths[index] == MemoryType.I32 || widths[index] == MemoryType.I64
                ? new Select(value.isNull(), i64(nullValue), value.data()) : value.data();
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
