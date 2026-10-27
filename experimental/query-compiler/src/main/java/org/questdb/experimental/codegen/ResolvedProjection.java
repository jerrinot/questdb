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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.CompiledProjection;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.questdb.experimental.codegen.Ir.*;

/** Bounded projection batches. Ordinary records own strings, symbols, row IDs and their lifetime. */
final class ResolvedProjection implements CompiledProjection {
    private static final int BATCH = 128;
    private static final int ROWS = BATCH + 2; // Independent random-access A/B slots.
    private final CompiledKernel code;
    private final int[] indexes;
    private final List<Pipeline.Column> columns;
    private final int[] outputs;
    private final String name;
    private final long bytes;
    private final long outputOffset;
    private long buffer;
    private long nativeRows;

    private ResolvedProjection(CompiledKernel code, ResolvedInputs inputs, int columnOffset, int[] outputs, String name) {
        this.code = code;
        this.columns = inputs.columns();
        this.indexes = inputs.indexes();
        for (int i = 0; i < indexes.length; i++) indexes[i] -= columnOffset;
        this.outputs = outputs;
        this.name = name;
        this.outputOffset = indexes.length * ROWS * 8L;
        int count = 0;
        for (int output : outputs) if (output >= 0) count++;
        this.bytes = Math.max(8, outputOffset + count * ROWS * 8L);
        this.buffer = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT);
    }

    static ResolvedProjection compile(ColumnTypes types, int offset, ObjList<Function> functions,
                                      Backend backend, Path artifacts) throws IOException {
        if (functions.size() > 64) return null;
        ResolvedInputs inputs = new ResolvedInputs(types, offset);
        var expressions = new ArrayList<Pipeline.Scalar>();
        var outputTypes = new ArrayList<Integer>();
        int[] outputs = new int[functions.size()];
        java.util.Arrays.fill(outputs, -1);
        for (int i = 0; i < functions.size(); i++) {
            Function function = functions.getQuick(i);
            while (function instanceof MemoizerFunction memoizer) function = memoizer.getArg();
            ColumnFunction column = ColumnFunction.unwrap(function);
            if (column != null) {
                if (column.getColumnIndex() < offset) return null;
                continue;
            }
            Pipeline.Scalar expression = inputs.scalar(function, function.getType());
            if (expression == null) return null;
            outputs[i] = expressions.size();
            expressions.add(expression);
            outputTypes.add(function.getType());
        }
        if (expressions.isEmpty()) return null;
        Builder b = new Builder();
        Var row = b.let(i64(0));
        b.loop(binary(Op.LT, row, Ir.ROWS), loop -> {
            var values = new ArrayList<Pipeline.Value>();
            for (int i = 0; i < inputs.columns().size(); i++) {
                Pipeline.Column column = inputs.columns().get(i);
                Var value = loop.let(load(new Offset(at(COLUMNS, i * ROWS * 8L), binary(Op.MUL, row, i64(8)))));
                values.add(new Pipeline.Value(value, column.nullable() ? eq(value, i64(column.nullValue())) : bool(false)));
            }
            Pipeline.Row input = new Pipeline.Row(inputs.columns(), values);
            for (int i = 0; i < expressions.size(); i++) {
                Pipeline.Value value = expressions.get(i).emit(input);
                long absent = outputTypes.get(i) == ColumnType.IPv4 ? 0 : Long.MIN_VALUE;
                loop.store(new Offset(at(STATE, i * ROWS * 8L), binary(Op.MUL, row, i64(8))),
                        new Select(value.isNull(), i64(absent), value.data()));
            }
            loop.assign(row, add(row, i64(1)));
        });
        b.exit(0);
        CompiledKernel code = backend.compile(new Program(b.block()), artifacts);
        try { return new ResolvedProjection(code, inputs, offset, outputs, backend.name()); }
        catch (Throwable failure) { code.close(); throw failure; }
    }

    @Override public int batchSize() { return BATCH; }
    @Override public boolean isProjected(int column) { return outputs[column] >= 0; }
    @Override public boolean needsBaseRecord() {
        for (int output : outputs) if (output < 0) return true;
        return false;
    }
    @Override public CharSequence getName() { return name; }
    @Override public long getNativeRows() { return nativeRows; }

    @Override public void putInput(Record record, int row) {
        for (int i = 0; i < indexes.length; i++) {
            int column = indexes[i];
            long value = switch (columns.get(i).columnType()) {
                case ColumnType.BYTE -> record.getByte(column);
                case ColumnType.SHORT -> record.getShort(column);
                case ColumnType.INT -> record.getInt(column);
                case ColumnType.IPv4 -> record.getIPv4(column);
                case ColumnType.LONG -> record.getLong(column);
                case ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> record.getTimestamp(column);
                default -> throw new IllegalStateException("Unsupported projection input");
            };
            Unsafe.putLong(buffer + (i * ROWS + row) * 8L, value);
        }
    }

    @Override public void evaluate(int start, int count) {
        code.run(buffer + start * 8L, count, buffer + outputOffset + start * 8L);
        nativeRows += count;
    }

    @Override public long getValue(int row, int column) {
        return Unsafe.getLong(buffer + outputOffset + (outputs[column] * ROWS + row) * 8L);
    }

    @Override public void close() {
        if (buffer != 0) buffer = Unsafe.free(buffer, bytes, MemoryTag.NATIVE_JIT);
        code.close();
    }
}
