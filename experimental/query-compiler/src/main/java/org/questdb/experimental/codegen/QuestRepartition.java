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

import io.questdb.griffin.engine.table.GroupByMapFragment;
import static org.questdb.experimental.codegen.Ir.*;

/** Cache-sized subpartitions; storage belongs to the existing acquired slot allocator. */
final class QuestRepartition {
    static final int PARTS = 64;
    static final int STATE_BYTES = PARTS * 16 + 16;

    private static QuestPartitions.Append append(Builder b, Expr hash, long rowBytes) {
        Var part = b.let(binary(Op.AND, binary(Op.USHR, hash,
                load(at(STATE, PARTS * 16L + 8))), load(at(STATE, PARTS * 16L))));
        Var bucket = b.let(new Offset(STATE, binary(Op.MUL, part, i64(16))));
        return QuestPartitions.reserve(b, bucket, rowBytes);
    }

    static Program rows(QuestMapProbe probe, long rowBytes) {
        Builder b = new Builder();
        Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
            Var row = loop.let(new Offset(COLUMNS, binary(Op.MUL, index, i64(rowBytes))));
            Var key = probe.orderedNative() ? row : loop.let(new Load(probe.keyBytes() == 4 ? MemoryType.I32 : MemoryType.I64, row));
            var output = append(loop, probe.hash(loop, key), rowBytes);
            QuestMapMutation.copy(loop, row, output.row(), rowBytes);
            output.commit(loop);
            loop.assign(index, add(index, i64(1)));
        });
        b.exit(0);
        return new Program(b.block());
    }

}
