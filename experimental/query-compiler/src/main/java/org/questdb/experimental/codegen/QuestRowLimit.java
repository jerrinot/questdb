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

/** Positive LIMIT/OFFSET selection in actual map cursor order. No copied payloads. */
final class QuestRowLimit {
    static final int SELECTED_OFFSET = 8;
    static final int SKIP_OFFSET = 16;
    static final int TAKE_OFFSET = 24;
    static final int SHARD_OFFSET = 32;

    private QuestRowLimit() { }

    /** Owner-only admission of a borrowed frame prefix. STATE holds the remaining budget. */
    static Program frame() {
        Builder b = new Builder();
        Var remaining = b.let(load(STATE));
        Var take = b.let(prefix(ROWS, remaining));
        b.store(STATE, binary(Op.SUB, remaining, take));
        b.exit(take);
        return new Program(b.block());
    }
    /** Both the available count and the limit are nonnegative. */
    static Expr prefix(Expr count, Expr limit) { return new Select(binary(Op.LT, count, limit), count, limit); }

    // STATE starts with the output address, followed by the fields above.
    static Program program(QuestMapProbe probe) {
        Builder b = new Builder();
        Var output = b.let(new Load(MemoryType.ADDRESS, STATE));
        Var selected = b.let(load(at(STATE, SELECTED_OFFSET)));
        Var skip = b.let(load(at(STATE, SKIP_OFFSET)));
        Var take = b.let(load(at(STATE, TAKE_OFFSET)));
        // A nonempty sorted list supplies ROWS; ordinary map selection passes zero.
        b.when(ne(ROWS, i64(0)), sorted -> {
            sorted.poll();
            sorted.loop(binary(Op.LT, selected, take), copy -> {
                Expr source = new Offset(COLUMNS, add(binary(Op.MUL, add(skip, selected), i64(16)), i64(8)));
                copy.store(new Offset(output, binary(Op.MUL, selected, i64(8))), load(source));
                copy.assign(selected, add(selected, i64(1)));
                copy.when(eq(binary(Op.AND, selected, i64(1023)), i64(0)), Builder::poll);
            });
            sorted.store(at(STATE, SELECTED_OFFSET), selected);
            sorted.exit(0);
        });
        Var shard = b.let(load(at(STATE, SHARD_OFFSET)));
        QuestMapScan.emit(b, probe, COLUMNS, true, (entryBuilder, entry) -> {
            Var skipped = entryBuilder.let(binary(Op.LT, i64(0), skip));
            entryBuilder.when(skipped, next -> next.assign(skip, binary(Op.SUB, skip, i64(1))));
            entryBuilder.when(eq(skipped, bool(false)), keep -> {
                Var target = keep.let(new Offset(output, binary(Op.MUL, selected, i64(8))));
                keep.store(MemoryType.ADDRESS, target, entry);
                keep.store(target, add(load(target), shard));
                keep.assign(selected, add(selected, i64(1)));
                keep.when(eq(selected, take), done -> {
                    done.store(at(STATE, SELECTED_OFFSET), selected);
                    done.store(at(STATE, SKIP_OFFSET), skip);
                    done.exit(0);
                });
            });
        });
        b.store(at(STATE, SELECTED_OFFSET), selected);
        b.store(at(STATE, SKIP_OFFSET), skip);
        b.exit(0);
        return new Program(b.block());
    }
}
