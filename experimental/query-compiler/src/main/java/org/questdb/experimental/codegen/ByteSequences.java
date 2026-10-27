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

/** Shared exact byte equality. Every vector and tail load stays inside both slices. */
final class ByteSequences {
    private ByteSequences() { }

    static Var equal(Builder b, Expr left, Expr right, Expr length) {
        Var same = b.let(bool(true));
        b.when(binary(Op.GE, length, i64(64)), large -> {
            Var offset = large.let(i64(0));
            Expr last = binary(Op.SUB, length, i64(64));
            large.loop(binary(Op.BOTH, same, binary(Op.LT, offset, last)), block -> {
                block.assign(same, equalBlock(left, right, offset, 64));
                block.assign(offset, add(offset, i64(64)));
            });
            // The final block may overlap the previous block, but cannot cross
            // either slice's end. Avoid decoding the remainder's individual bits.
            large.when(same, tail -> tail.assign(same, equalBlock(left, right, last, 64)));
        });
        b.when(binary(Op.LT, length, i64(64)), small -> equalSmall(small, left, right, length, same, 32));
        return same;
    }

    private static void equalSmall(Builder b, Expr left, Expr right, Expr length, Var same, int bytes) {
        if (bytes == 1) {
            b.when(ne(length, i64(0)), one -> one.assign(same, equalBlock(left, right, i64(0), 1)));
            return;
        }
        // For bytes <= length < 2*bytes, first and last blocks cover the whole
        // slice. Both reads are bounded, even when a slice touches a guard page.
        b.when(binary(Op.GE, length, i64(bytes)), pair -> pair.assign(same,
                binary(Op.BOTH, equalBlock(left, right, i64(0), bytes),
                        equalBlock(left, right, binary(Op.SUB, length, i64(bytes)), bytes))));
        b.when(binary(Op.LT, length, i64(bytes)), smaller -> equalSmall(smaller, left, right, length, same, bytes / 2));
    }

    private static Expr equalBlock(Expr left, Expr right, Expr offset, int bytes) {
        MemoryType width = switch (bytes) {
            case 1 -> MemoryType.I8;
            case 2 -> MemoryType.I16;
            case 4 -> MemoryType.I32;
            default -> MemoryType.I64;
        };
        Expr equal = eq(new Load(width, new Offset(left, offset)), new Load(width, new Offset(right, offset)));
        int lanes = bytes / width.bytes;
        return lanes == 1 ? equal : eq(new VectorMask(lanes, equal), i64((1L << lanes) - 1));
    }
}
