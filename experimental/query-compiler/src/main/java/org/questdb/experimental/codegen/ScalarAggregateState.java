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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.questdb.experimental.codegen.Ir.*;

final class ScalarAggregateState {
    private final LinkedHashMap<Load, Var> registers = new LinkedHashMap<>();
    static ScalarAggregateState bind(Builder b, AggregateUpdates updates) {
        for (var update : updates.updates()) if (update.allocates() || update.scalarState().isEmpty()) return null;
        var state = new ScalarAggregateState();
        for (var update : updates.updates()) for (var cell : update.scalarState()) {
            var load = new Load(cell.width(), at(STATE, cell.offset()));
            if (state.registers.putIfAbsent(load, b.let(load)) != null) throw new IllegalArgumentException("Overlapping aggregate state");
        }
        return state;
    }
    Block promote(Block block) { return promote(block, registers); }
    Block publishExits(Block block) {
        var result = new ArrayList<Statement>();
        for (var statement : block.statements()) {
            if (statement instanceof Return || statement instanceof Poll) for (var entry : registers.entrySet())
                result.add(new Store(entry.getKey().width(), entry.getKey().address(), entry.getValue()));
            result.add(switch (statement) {
                case If branch -> new If(branch.condition(), publishExits(branch.body()));
                case While loop -> new While(loop.condition(), publishExits(loop.body()));
                default -> statement;
            });
        }
        return new Block(result);
    }
    // Reuse the existing update definitions. Only their known state loads/stores become registers;
    // unexpected effects fail compilation instead of silently changing an aggregate's semantics.
    private static Block promote(Block block, Map<Load, Var> registers) {
        var result = new ArrayList<Statement>();
        for (Statement statement : block.statements()) result.add(switch (statement) {
            case Assign a -> new Assign(a.variable(), promote(a.value(), registers));
            case While loop -> new While(promote(loop.condition(), registers), promote(loop.body(), registers));
            case Poll poll -> poll;
            case Define d -> new Define(d.variable(), promote(d.value(), registers));
            case Store s -> new Assign(register(registers, new Load(s.width(), s.address())),
                    promote(s.value(), registers));
            case If branch -> new If(promote(branch.condition(), registers), promote(branch.body(), registers));
            default -> throw new IllegalArgumentException("Unsupported scalar aggregate effect: " + statement);
        });
        return new Block(result);
    }

    private static Expr promote(Expr expression, Map<Load, Var> registers) {
        return switch (expression) {
            case Var v -> v;
            case Literal literal -> literal;
            case Load load -> register(registers, load);
            case Binary binary -> new Binary(binary.op(), promote(binary.left(), registers), promote(binary.right(), registers));
            case Select select -> new Select(promote(select.condition(), registers), promote(select.yes(), registers), promote(select.no(), registers));
            case ToDouble cast -> new ToDouble(promote(cast.value(), registers));
            case ToInteger cast -> new ToInteger(promote(cast.value(), registers), cast.bits());
            case ToFloat cast -> new ToFloat(promote(cast.value(), registers));
            case Sqrt cast -> new Sqrt(promote(cast.value(), registers));
            case BitCast cast -> new BitCast(promote(cast.value(), registers), cast.type());
            default -> throw new IllegalArgumentException("Unsupported scalar aggregate expression: " + expression);
        };
    }

    private static Var register(Map<Load, Var> registers, Load load) {
        Var value = registers.get(load);
        if (value == null) throw new IllegalArgumentException("Unbound scalar aggregate state: " + load);
        return value;
    }

}
