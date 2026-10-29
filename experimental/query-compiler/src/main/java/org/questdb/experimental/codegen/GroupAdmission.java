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
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;

import static org.questdb.experimental.codegen.Ir.*;

/** A frozen subset of a direct grouping column. Every worker retains complete selected groups. */
final class GroupAdmission implements AutoCloseable {
    // The same small-set regime as constant IN lowering; larger limits keep ordinary aggregation.
    private static final int MAX_KEYS = 16;
    private static final int SAMPLE_ROWS = 4096;
    private static final int SAMPLE_BLOCK_ROWS = 64;
    static final int SCRATCH_BYTES = 16 + MAX_KEYS * 8;
    private final int column;
    private final int type;
    private final int limit;
    private final CompiledKernel code;
    // Published before dispatch and cleared only after the sequence has drained.
    private long[] keys;

    private GroupAdmission(int column, int type, int limit, CompiledKernel code) {
        this.column = column;
        this.type = type;
        this.limit = limit;
        this.code = code;
    }

    static GroupAdmission compile(QuestMapProbe probe, long limit, Backend backend, Path artifacts) throws IOException {
        if (limit <= 0 || limit > MAX_KEYS || probe == null || !probe.mutable()) return null;
        int column = -1, type = -1;
        if (probe.variable()) {
            var key = probe.variableKey();
            for (var part : key.parts()) {
                if (part.scalar() instanceof Pipeline.Column c) {
                    column = key.inputs().numericColumns()[c.index()];
                    type = c.columnType();
                    break;
                }
            }
        } else if (probe.fixedKey() != null) {
            var key = probe.fixedKey();
            for (var expression : key.expressions()) {
                if (expression instanceof Pipeline.Column c) {
                    column = key.columns()[c.index()];
                    type = c.columnType();
                    break;
                }
            }
        } else if (!probe.varchar()) {
            column = probe.column();
            type = probe.keyType();
        }
        if (column < 0) return null;
        MemoryType width = switch (ColumnType.sizeOf(type)) {
            case 1 -> MemoryType.I8;
            case 2 -> MemoryType.I16;
            case 4 -> MemoryType.I32;
            case 8 -> MemoryType.I64;
            default -> throw new IllegalArgumentException("Non-integral group admission key");
        };
        return new GroupAdmission(column, type, (int) limit, backend.compile(program(width, (int) limit), artifacts));
    }

    boolean active() { return keys != null; }
    boolean supports(long requiredGroups) { return requiredGroups == limit; }
    void clear() { keys = null; }
    long compilationNanos() { return code.compilationNanos(); }

    @Override
    public void close() { keys = null; code.close(); }

    boolean discover(long requiredGroups, UnorderedPageFrameSequence<?> frames, PageFrameMemoryPool pool,
                     Function filter, SqlExecutionCircuitBreaker breaker) {
        if (!supports(requiredGroups)) return false;
        if (keys != null) return true;
        long total = 0;
        for (int i = 0; i < frames.getFrameCount(); i++) {
            if ((i & 1023) == 0) breaker.statefulThrowExceptionIfTripped();
            // Compacted Parquet records have a different row-coordinate contract. Decline before
            // dispatch, including when a later frame (not the discovery frame) is Parquet.
            if (frames.getPageFrameAddressCache().getFrameFormat(i) != PartitionFormat.NATIVE) return false;
            total += frames.getFrameRowCount(i);
        }
        if (total <= SAMPLE_ROWS) return false;
        long[] candidates = new long[limit];
        int[] frequencies = new int[limit];
        int distinct = 0, qualifying = 0, admitted = 0;
        try (var record = new PageFrameMemoryRecord()) {
            record.of(frames.getSymbolTableSource());
            // Spread bounded, contiguous blocks over the snapshot. A prefix alone misses a
            // common phase change: diverse early keys followed by one dominant selected key.
            final int blocks = SAMPLE_ROWS / SAMPLE_BLOCK_ROWS;
            final long stride = (total - SAMPLE_BLOCK_ROWS) / (blocks - 1);
            int frame = 0;
            long frameStart = 0;
            for (int block = 0; block < blocks; block++) {
                breaker.statefulThrowExceptionIfTripped();
                long start = block == blocks - 1 ? total - SAMPLE_BLOCK_ROWS : block * stride;
                while (start >= frameStart + frames.getFrameRowCount(frame)) {
                    if ((frame & 1023) == 0) breaker.statefulThrowExceptionIfTripped();
                    frameStart += frames.getFrameRowCount(frame++);
                }
                record.init(pool.navigateTo(frame));
                for (int i = 0; i < SAMPLE_BLOCK_ROWS; i++) {
                    while (start + i >= frameStart + frames.getFrameRowCount(frame)) {
                        frameStart += frames.getFrameRowCount(frame++);
                        record.init(pool.navigateTo(frame));
                    }
                    record.setRowIndex(start + i - frameStart);
                    if (filter != null && !filter.getBool(record)) continue;
                    qualifying++;
                    long value = read(record);
                    int candidate = 0;
                    while (candidate < distinct && candidates[candidate] != value) candidate++;
                    if (candidate == distinct && distinct < limit) candidates[distinct++] = value;
                    if (candidate < distinct) { frequencies[candidate]++; admitted++; }
                }
            }
        }
        // This is a cost guard, not a statistical correctness assumption. Membership is exact
        // for every input row even if the sample is completely unrepresentative.
        if (distinct != limit || admitted * 2 >= qualifying) return false;
        // One common key can admit a whole SIMD block without evaluating the remaining keys.
        int common = 0;
        for (int i = 1; i < limit; i++) if (frequencies[i] > frequencies[common]) common = i;
        long first = candidates[0]; candidates[0] = candidates[common]; candidates[common] = first;
        keys = candidates;
        return true;
    }

    long select(PageFrameMemoryRecord record, long count, boolean filtered, DirectLongList rows,
                long scratch, KernelMemory memory, SqlExecutionCircuitBreaker breaker) {
        if (keys == null || count == 0) return -1;
        rows.setCapacity(Math.max(rows.getCapacity(), count));
        long page = record.getPageAddress(column);
        long selected;
        if (page != 0) {
            Unsafe.putLong(scratch, page);
            Unsafe.putLong(scratch + 8, filtered ? rows.getAddress() : 0);
            for (int i = 0; i < keys.length; i++) Unsafe.putLong(scratch + 16L + i * 8L, keys[i]);
            memory.enter(breaker);
            try {
                selected = code.run(scratch, count, rows.getAddress());
                memory.throwIfFailed();
            } finally { memory.close(); }
        } else {
            // Column tops and lazy casts retain the ordinary record's null/conversion semantics.
            selected = 0;
            boolean prefix = true;
            for (long i = 0; i < count; i++) {
                if ((i & 1023) == 0) breaker.statefulThrowExceptionIfTripped();
                long row = filtered ? rows.get(i) : i;
                record.setRowIndex(row);
                long value = read(record);
                boolean match = false;
                for (long key : keys) if (value == key) { match = true; break; }
                if (!match && prefix) {
                    if (!filtered) for (long j = 0; j < i; j++) {
                        if ((j & 1023) == 0) breaker.statefulThrowExceptionIfTripped();
                        rows.set(j, j);
                    }
                    prefix = false;
                }
                if (match) { if (!prefix) rows.set(selected, row); selected++; }
            }
            if (prefix) selected = -1;
        }
        if (selected >= 0) rows.setPos(selected);
        return selected;
    }

    private long read(PageFrameMemoryRecord record) {
        return switch (type) {
            case ColumnType.BYTE -> record.getByte(column);
            case ColumnType.SHORT -> record.getShort(column);
            case ColumnType.INT, ColumnType.SYMBOL -> record.getInt(column);
            case ColumnType.IPv4 -> record.getIPv4(column);
            case ColumnType.DATE -> record.getDate(column);
            case ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> record.getTimestamp(column);
            default -> record.getLong(column);
        };
    }

    private static Program program(MemoryType width, int limit) {
        Builder b = new Builder();
        Var page = b.let(new Load(MemoryType.ADDRESS, COLUMNS));
        Var rowIds = b.let(new Load(MemoryType.ADDRESS, at(COLUMNS, 8)));
        Var[] keys = new Var[limit];
        for (int i = 0; i < limit; i++) keys[i] = b.let(load(at(COLUMNS, 16L + i * 8L)));
        Var index = b.let(i64(0)), count = b.let(i64(0)), prefix = b.let(bool(true));
        b.when(eq(rowIds, nil()), contiguous -> {
            contiguous.loop(binary(Op.GE, binary(Op.SUB, ROWS, index), i64(8)), batch -> {
                batch.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                Expr value = new Load(width, new Offset(page, binary(Op.MUL, index, i64(width.bytes))));
                Var mask = batch.let(new VectorMask(8, eq(value, keys[0])));
                if (limit > 1) batch.when(ne(mask, i64(255)), remaining -> {
                    Expr predicate = eq(value, keys[1]);
                    for (int i = 2; i < limit; i++) predicate = binary(Op.EITHER, predicate, eq(value, keys[i]));
                    Var more = remaining.let(new VectorMask(8, predicate));
                    remaining.assign(mask, binary(Op.XOR, binary(Op.XOR, mask, more), binary(Op.AND, mask, more)));
                });
                batch.when(binary(Op.BOTH, prefix, ne(mask, i64(255))), gap -> {
                    materializePrefix(gap, index);
                    gap.assign(prefix, bool(false));
                });
                batch.when(eq(prefix, bool(false)), compact -> compact.assign(count, add(count,
                        new SelectRows(8, mask, index, new Offset(STATE, binary(Op.MUL, count, i64(8)))))));
                batch.when(prefix, full -> full.assign(count, add(count, i64(8))));
                batch.assign(index, add(index, i64(8)));
            });
        });
        b.loop(binary(Op.LT, index, ROWS), scalar -> {
            scalar.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = scalar.let(index);
            scalar.when(ne(rowIds, nil()), indirect -> indirect.assign(row,
                    load(new Offset(rowIds, binary(Op.MUL, index, i64(8))))));
            Var value = scalar.let(new Load(width, new Offset(page, binary(Op.MUL, row, i64(width.bytes)))));
            Expr predicate = eq(value, keys[0]);
            for (int i = 1; i < limit; i++) predicate = binary(Op.EITHER, predicate, eq(value, keys[i]));
            Var match = scalar.let(predicate);
            scalar.when(binary(Op.BOTH, prefix, eq(match, bool(false))), gap -> {
                gap.when(eq(rowIds, nil()), fresh -> materializePrefix(fresh, index));
                gap.assign(prefix, bool(false));
            });
            scalar.when(match, accepted -> {
                accepted.when(eq(prefix, bool(false)), compact ->
                        compact.store(new Offset(STATE, binary(Op.MUL, count, i64(8))), row));
                accepted.assign(count, add(count, i64(1)));
            });
            scalar.assign(index, add(index, i64(1)));
        });
        b.exit(new Select(prefix, i64(-1), count));
        return new Program(b.block());
    }

    private static void materializePrefix(Builder b, Var end) {
        Var row = b.let(i64(0));
        b.loop(binary(Op.LT, row, end), fill -> {
            fill.when(eq(binary(Op.AND, row, i64(1023)), i64(0)), Builder::poll);
            fill.store(new Offset(STATE, binary(Op.MUL, row, i64(8))), row);
            fill.assign(row, add(row, i64(1)));
        });
    }
}
