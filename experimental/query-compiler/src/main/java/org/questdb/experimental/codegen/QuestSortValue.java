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

import static org.questdb.experimental.codegen.Ir.*;

/** SortKeyEncoder's numeric order represented as a signed LONG key. */
final class QuestSortValue {
    private QuestSortValue() { }

    static Expr load(Builder b, Expr address, int type) {
        return switch (type) {
            case ColumnType.LONG -> b.let(new Load(MemoryType.I64, address));
            case ColumnType.SHORT -> b.let(new Load(MemoryType.I16, address));
            case ColumnType.INT -> {
                Var value = b.let(new Load(MemoryType.I32, address));
                yield b.let(new Select(eq(value, i64(Integer.MIN_VALUE)), i64(Long.MIN_VALUE), value));
            }
            case ColumnType.DOUBLE -> {
                Var value = b.let(new Load(MemoryType.F64, address));
                // Double.doubleToLongBits canonicalizes every NaN, preserving signed zeros/infinities.
                Var bits = b.let(new Select(binary(Op.FNE, value, value),
                        i64(Double.doubleToLongBits(Double.NaN)), new BitCast(value)));
                yield b.let(binary(Op.XOR, bits, new Select(binary(Op.LT, bits, i64(0)), i64(Long.MAX_VALUE), i64(0))));
            }
            default -> throw new IllegalArgumentException("Unsupported numeric sort type: " + type);
        };
    }
}
