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

import java.util.*;
import java.util.function.UnaryOperator;
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.SsaProgram.*;

/** Validates SSA definitions, dominance, types and phi predecessor sets. */
final class SsaVerifier {
    private SsaVerifier() { }

    /** Reject invalid definitions, dominance, types or phi predecessor sets before backend lowering. */
    static void verify(SsaProgram g) {
        Analysis flow = new Analysis(g);
        record Definition(Var value, int block, int position) { }
        Map<Integer, Definition> definitions = new HashMap<>();
        for (Var p : g.parameters) definitions.put(p.id(), new Definition(p, 0, -2));
        for (BasicBlock b : g.blocks) {
            for (Phi p : b.phis) {
                if (!p.inputs.keySet().equals(flow.predecessors.get(b.id))) throw new IllegalArgumentException("Phi predecessors in b" + b.id);
                if (definitions.put(p.result.id(), new Definition(p.result, b.id, -1)) != null) throw new IllegalArgumentException("Duplicate SSA definition");
                for (Expr value : p.inputs.values()) if (value.type() != p.result.type()) throw new IllegalArgumentException("Phi type mismatch");
            }
            for (int i = 0; i < b.code.size(); i++) if (b.code.get(i) instanceof Define d)
                if (definitions.put(d.variable().id(), new Definition(d.variable(), b.id, i)) != null) throw new IllegalArgumentException("Duplicate SSA definition");
        }
        for (BasicBlock b : g.blocks) {
            for (Phi p : b.phis) for (var edge : p.inputs.entrySet()) for (Var v : reads(edge.getValue())) {
                Definition d = definitions.get(v.id());
                if (d == null || !d.value().equals(v) || !flow.dominators[edge.getKey()].get(d.block())) throw new IllegalArgumentException("Invalid phi input " + v);
            }
            for (int i = 0; i <= b.code.size(); i++) {
                final int position = i;
                UnaryOperator<Expr> check = e -> {
                    Var v = (Var) e; Definition d = definitions.get(v.id());
                    if (d == null || !d.value().equals(v) || !flow.dominators[b.id].get(d.block())
                            || d.block() == b.id && d.position() >= position) throw new IllegalArgumentException("Non-dominating SSA value " + v + " in b" + b.id);
                    return e;
                };
                if (i == b.code.size()) map(b.end, check); else map(b.code.get(i), check);
            }
        }
    }
    static final class Analysis {
        final List<Set<Integer>> predecessors = new ArrayList<>();
        final BitSet[] dominators;
        Analysis(SsaProgram g) {
            int count = g.blocks.size(); dominators = new BitSet[count];
            for (int i = 0; i < count; i++) predecessors.add(new HashSet<>());
            for (BasicBlock b : g.blocks) for (int target : successors(b.end)) predecessors.get(target).add(b.id);
            for (int i = 0; i < count; i++) { dominators[i] = new BitSet(count); if (i == 0) dominators[i].set(0); else dominators[i].set(0, count); }
            boolean changed;
            do {
                changed = false;
                for (int i = 1; i < count; i++) {
                    BitSet next = new BitSet(count); next.set(0, count);
                    if (predecessors.get(i).isEmpty()) throw new IllegalArgumentException("Unreachable SSA block " + i);
                    for (int p : predecessors.get(i)) next.and(dominators[p]);
                    next.set(i);
                    if (!next.equals(dominators[i])) { dominators[i] = next; changed = true; }
                }
            } while (changed);
        }
    }
}
