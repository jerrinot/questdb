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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.questdb.experimental.codegen.Ir.*;

/** Preserve helper semantics on backends that emit a single native function. */
final class HelperInlining {
    private HelperInlining() { }

    static Program expand(Program program) {
        if (!hasCalls(program.body())) return program;
        Builder out = new Builder();
        Map<Var, Expr> scope = new HashMap<>();
        for (Var parameter : List.of(COLUMNS, ROWS, STATE)) scope.put(parameter, parameter);
        emit(program.body(), out, scope);
        return new Program(out.block());
    }

    private static boolean hasCalls(Block block) {
        for (Statement s : block.statements()) {
            if (s instanceof Call) return true;
            if (s instanceof If i && hasCalls(i.body())) return true;
            if (s instanceof While w && hasCalls(w.body())) return true;
        }
        return false;
    }

    private static Expr expression(Expr value, Map<Var, Expr> scope) {
        return SsaProgram.map(value, v -> scope.get((Var) v));
    }

    private static void emit(Block block, Builder out, Map<Var, Expr> scope) {
        for (Statement s : block.statements()) {
            switch (s) {
                case Define d -> scope.put(d.variable(), out.let(expression(d.value(), scope)));
                case Assign a -> out.assign((Var) scope.get(a.variable()), expression(a.value(), scope));
                case Call c -> {
                    Map<Var, Expr> parameters = new HashMap<>();
                    for (int i = 0; i < c.arguments().size(); i++) {
                        parameters.put(c.helper().parameters().get(i), out.let(expression(c.arguments().get(i), scope)));
                    }
                    var body = c.helper().body().statements();
                    // The helper contract permits only failure exits before its final success.
                    // Those failures already have the enclosing kernel's return semantics.
                    out.when(bool(true), nested -> emit(new Block(body.subList(0, body.size() - 1)), nested, parameters));
                }
                case If i -> out.when(expression(i.condition(), scope), nested -> emit(i.body(), nested, new HashMap<>(scope)));
                case While w -> out.loop(expression(w.condition(), scope), nested -> emit(w.body(), nested, new HashMap<>(scope)));
                case Return r -> out.exit(expression(r.value(), scope));
                default -> out.append(new Block(List.of(SsaProgram.map(s, v -> scope.get((Var) v)))));
            }
        }
    }
}
