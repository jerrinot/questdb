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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;

import static org.questdb.experimental.codegen.Ir.*;

/** Generated probe over the core build's optional bounded integral lookup. */
final class IntegralJoinIndex {
    static final int BYTES = FrozenHashJoinBuild.INTEGRAL_LOOKUP_BYTES;

    private IntegralJoinIndex() { }

    private static long nullKey(int type) {
        return type == ColumnType.SYMBOL ? SymbolTable.VALUE_IS_NULL
                : type == ColumnType.INT ? Integer.MIN_VALUE : Long.MIN_VALUE;
    }

    record Descriptor(Var address, Var first, Var last, Var nullHead) { }
    static Descriptor bind(Builder b, Expr address) {
        return new Descriptor(b.let(new Load(MemoryType.ADDRESS, address)),
                b.let(load(at(address, 8))), b.let(load(at(address, 16))), b.let(load(at(address, 24))));
    }
    static Var find(Builder b, Descriptor index, Expr key, int type) {
        Var head = b.let(i64(0));
        b.when(eq(key, i64(nullKey(type))), missing -> missing.assign(head, index.nullHead));
        b.when(binary(Op.BOTH, binary(Op.GE, key, index.first), binary(Op.GE, index.last, key)), inside -> {
            Expr offset = binary(Op.MUL, binary(Op.SUB, key, index.first), i64(Integer.BYTES));
            Var compressed = inside.let(new Load(MemoryType.I32, new Offset(index.address, offset)));
            inside.assign(head, binary(Op.MUL, binary(Op.AND, compressed, i64(0xffff_ffffL)), i64(8)));
        });
        return head;
    }
}
