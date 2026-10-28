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

import static org.questdb.experimental.codegen.Ir.*;

/** Cache-sized subpartitions; storage belongs to the existing acquired slot allocator. */
final class QuestRepartition {
    static final int PARTS = 64;
    static final int MASK_OFFSET = PARTS * QuestPartitions.BUCKET_BYTES;
    static final int SHIFT_OFFSET = MASK_OFFSET + Long.BYTES;
    static final int STATE_BYTES = SHIFT_OFFSET + Long.BYTES;

    private static Var bucket(Builder b, Expr hash) {
        Var part = b.let(binary(Op.AND, binary(Op.USHR, hash,
                load(at(STATE, SHIFT_OFFSET))), load(at(STATE, MASK_OFFSET))));
        return b.let(new Offset(STATE, binary(Op.MUL, part, i64(QuestPartitions.BUCKET_BYTES))));
    }

    static Program rows(QuestMapProbe probe, long rowBytes) {
        Builder b = new Builder();
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(rowBytes))));
            Var key = probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            var output = QuestPartitions.reserve(loop, bucket(loop, probe.hash(loop, key)), rowBytes);
            QuestMapMutation.copy(loop, row, output.row(), rowBytes);
            output.commit(loop);
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

    static Program variableRows(long payloadBytes, boolean inputs) {
        Builder b = new Builder();
        Var index = b.let(i64(0)), offset = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, offset));
            Var key = loop.let(at(row, 8));
            Var bytes = loop.let(add(i64(8), inputs ? add(QuestVariableKey.bytes(key), i64(payloadBytes))
                    : QuestVariableKey.stride(key, i64(payloadBytes))));
            var output = QuestPartitions.reserveVariable(loop, bucket(loop, load(row)), bytes);
            QuestMapMutation.copy(loop, row, output.row(), bytes);
            output.commit(loop);
            loop.assign(offset, add(offset, bytes));
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

}
