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

import java.util.function.UnaryOperator;
import static org.questdb.experimental.codegen.Ir.*;

/** Verifies the safe, read-only lane region and rewrites its consecutive loads. */
final class LaneExpressions {
    private LaneExpressions() { }

    static void verify(Expr e) {
        switch (e) {
            case Literal l -> { if (l.type() == Type.ADDRESS || l.type() == Type.F64) reject(); }
            case Var v -> { if (v.type() == Type.ADDRESS || v.type() == Type.F64) reject(); }
            case Binary b -> { verify(b.left()); verify(b.right()); }
            case Select s -> { verify(s.condition()); verify(s.yes()); verify(s.no()); }
            case Load l -> {
                if (l.width() == MemoryType.ADDRESS || l.width() == MemoryType.F64) reject();
                invariantAddress(l.address());
            }
            default -> reject();
        }
    }

    private static void invariantAddress(Expr e) {
        switch (e) {
            case Literal ignored -> { }
            case Var ignored -> { }
            case Offset o -> { invariantAddress(o.address()); invariantAddress(o.bytes()); }
            case Binary b -> { invariantAddress(b.left()); invariantAddress(b.right()); }
            case Select s -> { invariantAddress(s.condition()); invariantAddress(s.yes()); invariantAddress(s.no()); }
            default -> reject(); // No pointer chasing, nested loads, allocation or callbacks.
        }
    }

    static Expr mapLoads(Expr e, UnaryOperator<Load> mapper) {
        return switch (e) {
            case Load l -> mapper.apply(l);
            case Binary b -> new Binary(b.op(), mapLoads(b.left(), mapper), mapLoads(b.right(), mapper));
            case Select s -> new Select(mapLoads(s.condition(), mapper), mapLoads(s.yes(), mapper), mapLoads(s.no(), mapper));
            case Var v -> v;
            case Literal l -> l;
            default -> throw new IllegalArgumentException("Not a lane expression: " + e);
        };
    }

    private static void reject() { throw new IllegalArgumentException("Lane predicates require pure arithmetic and consecutive integer reads"); }
}
