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
            if (!probe.varchar() && !zeroLast) emitZero(b, view, zero, consumer);
            Var end = b.let(add(load(at(view, NativeMapView.MASK)), i64(1)));
            Var index = b.let(i64(0));
            MemoryType width = probe.keyType() == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
            // Unordered4/8/VarcharMap capacities are powers of two, at least 16.
            // Keep their runtime stride and cursor order; no extra occupancy metadata.
            b.loop(binary(Op.LT, index, end), loop -> {
                loop.when(eq(binary(Op.AND, index, i64(1023)), i64(0)), Builder::poll);
                Var chunk = loop.let(new Offset(base, binary(Op.MUL, index, stride)));
                Expr bits = i64(0);
                for (int lane = 0; lane < 8; lane++) {
                    Expr entry = new Offset(chunk, binary(Op.MUL, i64(lane), stride));
                    bits = binary(Op.ADD, bits, new Select(ne(new Load(width, entry), i64(0)), i64(1L << lane), i64(0)));
                }
                Var mask = loop.let(bits);
                loop.loop(ne(mask, i64(0)), present -> {
                    Var lane = present.let(new TrailingZeros(mask));
                    Var entry = present.let(new Offset(chunk, binary(Op.MUL, lane, stride)));
                    consumer.accept(present, entry);
                    present.assign(mask, binary(Op.AND, mask, binary(Op.SUB, mask, i64(1))));
                });
                loop.assign(index, add(index, i64(8)));
            });
            if (!probe.varchar() && zeroLast) emitZero(b, view, zero, consumer);
        }
    }

    private static void emitZero(Builder b, Expr view, Expr zero, BiConsumer<Builder, Expr> consumer) {
        b.when(ne(load(at(view, NativeMapView.HAS_ZERO)), i64(0)), present -> {
            present.poll();
            consumer.accept(present, zero);
        });
    }
}
