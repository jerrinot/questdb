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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.std.ObjList;

import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** OrderedMap.VarSizeKey encoding: total byte length, then RecordSink-ordered fields. */
record QuestVariableKey(ResolvedVarchar.Binding inputs, List<Part> parts) {
    static final int NEED_STAGING = 8;
    record Part(MemoryType width, Pipeline.Scalar scalar, ResolvedVarchar.Expression varchar) { }

    static QuestVariableKey bind(ColumnTypes input, ColumnTypes types, ListColumnFilter columns,
                                 ObjList<Function> functions, io.questdb.cairo.map.Map map) {
        int computed = functions == null ? 0 : functions.size();
        if (!(map instanceof OrderedMap ordered) || ordered.getFixedKeySize() != -1
                || types.getColumnCount() != columns.size() + computed) return null;
        var resolved = new ResolvedInputs(input);
        var parts = new ArrayList<Part>();
        var strings = new ResolvedVarchar(input, resolved);
        boolean variable = false;
        for (int i = 0; i < types.getColumnCount(); i++) {
            int type = types.getColumnType(i);
            if (type == ColumnType.VARCHAR) {
                ResolvedVarchar.Expression expression = i < columns.size()
                        ? strings.column(columns.getQuick(i) - 1) : strings.bind(functions.getQuick(i - columns.size()));
                if (expression == null) return null;
                parts.add(new Part(null, null, expression));
                variable = true;
            } else {
                MemoryType width = switch (type) {
                    case ColumnType.BYTE -> MemoryType.I8;
                    case ColumnType.SHORT -> MemoryType.I16;
                    case ColumnType.INT, ColumnType.SYMBOL, ColumnType.IPv4 -> MemoryType.I32;
                    case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> MemoryType.I64;
                    default -> null;
                };
                if (width == null) return null;
                Pipeline.Scalar scalar;
                if (i < columns.size()) {
                    int index = columns.getQuick(i) - 1;
                    if (index < 0 || index >= input.getColumnCount() || input.getColumnType(index) != type) return null;
                    scalar = resolved.column(index, type);
                } else {
                    Function function = functions.getQuick(i - columns.size());
                    if (function.getType() != type) return null;
                    scalar = resolved.scalar(function, type);
                }
                if (scalar == null) return null;
                parts.add(new Part(width, scalar, null));
            }
        }
        if (!variable) return null;
        return new QuestVariableKey(strings.binding(), List.copyOf(parts));
    }

    long descriptorBytes() { return inputs.bytes(); }

    boolean prepare(PageFrameMemoryRecord record, long descriptor) {
        return inputs.prepare(record, descriptor);
    }

    Var stage(Builder b, Expr row) {
        Var descriptors = b.let(new Load(MemoryType.ADDRESS, at(STATE, 16)));
        var source = ResolvedInputs.loadFrameRow(b, inputs.numericInputs(), descriptors, row);
        var frame = inputs.frame(source, row, descriptors);
        var strings = new ArrayList<Pipeline.Utf8Value>();
        Expr bytes = i64(4);
        for (Part part : parts) {
            var value = part.varchar == null ? null : part.varchar.emit(b, frame);
            strings.add(value);
            bytes = add(bytes, part.width != null ? i64(part.width.bytes) : add(i64(4), value.size()));
        }
        Var size = b.let(bytes);
        b.when(binary(Op.GE, size, i64(Integer.MAX_VALUE)), bad -> bad.exit(-8));
        Var used = b.let(load(at(STATE, 80)));
        b.when(binary(Op.LT, load(at(STATE, 72)), add(used, size)), full -> {
            full.store(at(STATE, 88), add(used, size));
            full.exit(NEED_STAGING); // The producer saved the first unconsumed position at STATE+32.
        });
        Var key = b.let(new Offset(new Load(MemoryType.ADDRESS, at(STATE, 64)), used));
        b.store(MemoryType.I32, key, binary(Op.SUB, size, i64(4)));
        Var offset = b.let(i64(4));
        for (int i = 0; i < parts.size(); i++) {
            Part part = parts.get(i);
            Expr field = new Offset(key, offset);
            if (part.width != null) {
                var value = part.scalar.emit(source);
                long nullValue = part.scalar instanceof Pipeline.Column column ? column.nullValue()
                        : part.width == MemoryType.I32 ? Integer.MIN_VALUE : Long.MIN_VALUE;
                Expr data = part.width == MemoryType.I32 || part.width == MemoryType.I64
                        ? new Select(value.isNull(), i64(nullValue), value.data()) : value.data();
                b.store(part.width, field, data);
                b.assign(offset, add(offset, i64(part.width.bytes)));
            } else {
                var value = strings.get(i);
                // VarSizeKey.putVarchar deliberately erases ASCII: equality compares encoded bytes.
                b.store(MemoryType.I32, field, new Select(value.isNull(), i64(-1), value.size()));
                QuestMapMutation.copy(b, value.address(), at(field, 4), value.size());
                b.assign(offset, add(offset, add(i64(4), value.size())));
            }
        }
        b.store(at(STATE, 80), add(used, size));
        return key;
    }

    static Expr bytes(Expr key) { return add(i64(4), new Load(MemoryType.I32, key)); }
    static Expr value(Expr key) { return new Offset(key, bytes(key)); }
    static Expr stride(Expr key, Expr valueBytes) { return binary(Op.AND, add(add(bytes(key), valueBytes), i64(7)), i64(-8)); }
    static Var hash(Builder b, Expr key) { return QuestVarchar.hash(b, at(key, 4), new Load(MemoryType.I32, key)); }

    static Var equal(Builder b, Expr left, Expr right) {
        Var length = b.let(new Load(MemoryType.I32, left));
        Var same = b.let(eq(length, new Load(MemoryType.I32, right)));
        b.when(same, compare -> {
            compare.assign(same, ByteSequences.equal(compare, at(left, 4), at(right, 4), length));
        });
        return same;
    }
}
