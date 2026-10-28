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
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.SsaProgram.*;

/** Phi coalescing using interference over SSA edge uses; never merge overlapping lifetimes. */
final class SsaLiveness {
    private SsaLiveness() { }
    static Map<Var, Var> coalesce(SsaProgram g) {
        int count = g.blocks.size();
        List<Set<Var>> in = new ArrayList<>(), out = new ArrayList<>(), used = new ArrayList<>(), defined = new ArrayList<>();
        for (BasicBlock b : g.blocks) {
            Set<Var> uses = new HashSet<>(), defs = new HashSet<>();
            for (Phi p : b.phis) defs.add(p.result);
            for (Statement s : b.code) {
                for (Var v : reads(s)) if (!defs.contains(v)) uses.add(v);
                if (s instanceof Define d) defs.add(d.variable());
            }
            map(b.end, e -> { if (!defs.contains(e)) uses.add((Var)e); return e; });
            in.add(new HashSet<>()); out.add(new HashSet<>()); used.add(uses); defined.add(defs);
        }
        boolean changed;
        do {
            changed = false;
            for (int id = count - 1; id >= 0; id--) {
                BasicBlock b = g.blocks.get(id); Set<Var> exit = new HashSet<>();
                for (int target : successors(b.end)) {
                    exit.addAll(in.get(target));
                    for (Phi p : g.blocks.get(target).phis) exit.addAll(SsaProgram.reads(p.inputs.get(id)));
                }
                Set<Var> entry = new HashSet<>(exit); entry.removeAll(defined.get(id)); entry.addAll(used.get(id));
                if (!entry.equals(in.get(id)) || !exit.equals(out.get(id))) { in.set(id, entry); out.set(id, exit); changed = true; }
            }
        } while (changed);
        Map<Var, Set<Var>> interference = new HashMap<>();
        for (BasicBlock b : g.blocks) {
            Set<Var> live = new HashSet<>(out.get(b.id));
            map(b.end, e -> { live.add((Var)e); return e; });
            for (int i = b.code.size() - 1; i >= 0; i--) {
                Statement s = b.code.get(i);
                if (s instanceof Define d) { interfere(d.variable(), live, interference); live.remove(d.variable()); }
                live.addAll(reads(s));
            }
            for (Phi p : b.phis) interfere(p.result, live, interference);
        }
        Map<Var, Var> aliases = new HashMap<>();
        Map<Var, Set<Var>> members = new HashMap<>();
        for (BasicBlock b : g.blocks) for (Phi p : b.phis) for (Expr input : p.inputs.values()) {
            if (!(input instanceof Var v) || v.id() < 3) continue;
            Var left = aliases.getOrDefault(p.result, p.result), right = aliases.getOrDefault(v, v);
            if (left.equals(right)) continue;
            Set<Var> a = members.getOrDefault(left, Set.of(left)), c = members.getOrDefault(right, Set.of(right));
            boolean conflict = a.stream().anyMatch(x -> c.stream().anyMatch(y -> interference.getOrDefault(x, Set.of()).contains(y)));
            if (conflict) continue;
            Set<Var> merged = new HashSet<>(a); merged.addAll(c);
            for (Var x : merged) aliases.put(x, left);
            members.put(left, merged); members.remove(right);
        }
        return aliases;
    }
    private static Set<Var> reads(Statement s) {
        Set<Var> result = new HashSet<>();
        Ir.collectReads(s, result);
        return result;
    }
    private static void interfere(Var definition, Set<Var> live, Map<Var, Set<Var>> graph) {
        for (Var v : live) if (!definition.equals(v)) {
            graph.computeIfAbsent(definition, unused -> new HashSet<>()).add(v);
            graph.computeIfAbsent(v, unused -> new HashSet<>()).add(definition);
        }
    }
}
