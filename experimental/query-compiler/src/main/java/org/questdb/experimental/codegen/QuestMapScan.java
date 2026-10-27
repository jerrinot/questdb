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
import io.questdb.cairo.map.NativeMapView;

import java.util.function.BiConsumer;

import static org.questdb.experimental.codegen.Ir.*;

/** Existing map cursor order over a borrowed probe/mutation view. No Java call per entry. */
final class QuestMapScan {
    private QuestMapScan() { }

    static void emit(Builder b, QuestMapProbe probe, Expr view, BiConsumer<Builder, Expr> consumer) {
        emit(b, probe, view, false, consumer);
    }

    static void emit(Builder b, QuestMapProbe probe, Expr view, boolean zeroLast, BiConsumer<Builder, Expr> consumer) {
        Var base = b.let(new Load(MemoryType.ADDRESS, view));
        Var stride = b.let(load(at(view, NativeMapView.STRIDE)));
        if (probe.orderedNative()) {
            Var count = b.let(load(at(view, NativeMapView.SIZE)));
            Var index = b.let(i64(0));
            Var byteOffset = probe.variable() ? b.let(i64(0)) : null;
            b.loop(binary(Op.LT, index, count), loop -> {
                loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                Var entry = loop.let(new Offset(base, probe.variable() ? byteOffset : binary(Op.MUL, index, stride)));
                consumer.accept(loop, entry);
                if (probe.variable()) loop.assign(byteOffset, add(byteOffset, QuestVariableKey.stride(entry, stride)));
                loop.assign(index, add(index, i64(1)));
            });
        } else {
            Var zero = b.let(new Load(MemoryType.ADDRESS, at(view, NativeMapView.ZERO_ENTRY)));
            Var end = b.let(add(load(at(view, NativeMapView.MASK)), i64(2)));
            Var index = b.let(i64(probe.varchar() || zeroLast ? 1 : 0));
            MemoryType width = probe.keyType() == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
            b.loop(binary(Op.LT, index, end), loop -> {
                loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                Var entry = loop.let(zero);
                loop.when(ne(index, i64(0)), physical -> physical.assign(entry,
                        new Offset(base, binary(Op.MUL, binary(Op.SUB, index, i64(1)), stride))));
                Var occupied = loop.let(ne(new Load(width, entry), i64(0)));
                loop.when(eq(index, i64(0)), z -> z.assign(occupied, ne(load(at(view, NativeMapView.HAS_ZERO)), i64(0))));
                loop.when(occupied, present -> consumer.accept(present, entry));
                loop.assign(index, add(index, i64(1)));
            });
            if (zeroLast && !probe.varchar()) b.when(ne(load(at(view, NativeMapView.HAS_ZERO)), i64(0)), present -> consumer.accept(present, zero));
        }
    }
}
