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

/** Dominance-scoped value numbering and conservative loop motion. No alias assumptions. */
final class SsaOptimizer {
    enum Effect { PURE, READ, WRITE, CALL }
    private SsaOptimizer() { }

    static Effect effect(Statement s) {
        return switch (s) {
            case Define d -> (d.value() instanceof AtomicLoad || d.value() instanceof CompareExchange || d.value() instanceof AtomicAdd) ? Effect.CALL : (d.value() instanceof Load || d.value() instanceof VectorMask) ? Effect.READ : (d.value() instanceof Allocate || d.value() instanceof Reallocate) ? Effect.CALL : d.value() instanceof SelectRows ? Effect.WRITE : Effect.PURE;
            case Store ignored -> Effect.WRITE;
            case AtomicStore ignored -> Effect.CALL;
            case Prefetch ignored -> Effect.READ;
            case Free ignored -> Effect.CALL;
            case Poll ignored -> Effect.CALL;
            case Call ignored -> Effect.CALL;
            default -> throw new IllegalArgumentException("Not SSA: " + s);
        };
    }
    static void optimize(SsaProgram graph) {
        boolean changed;
        do {
            foldBranches(graph);
            simplifyPhis(graph);
            numberValues(graph);
            changed = foldBranches(graph);
        } while (changed);
        eliminateDeadValues(graph);
        hoist(graph);
        numberValues(graph);
        simplifyPhis(graph);
        eliminateDeadValues(graph);
    }
    private static boolean foldBranches(SsaProgram g) {
        boolean changed = false;
        for (BasicBlock b : g.blocks) if (b.end instanceof Branch branch && branch.condition() instanceof Literal l) {
            b.end = new Jump(l.value() != 0 ? branch.yes() : branch.no()); changed = true;
        }
        if (!changed) return false;
        Set<Integer> live = new HashSet<>(); var pending = new ArrayDeque<Integer>(); pending.add(0);
        while (!pending.isEmpty()) { int id = pending.removeFirst(); if (live.add(id)) pending.addAll(successors(g.blocks.get(id).end)); }
        Map<Integer, Integer> ids = new HashMap<>();
        for (BasicBlock b : g.blocks) if (live.contains(b.id)) ids.put(b.id, ids.size());
        Map<Integer, Set<Integer>> predecessors = new HashMap<>();
        for (BasicBlock b : g.blocks) if (live.contains(b.id)) for (int target : successors(b.end))
            predecessors.computeIfAbsent(target, unused -> new HashSet<>()).add(b.id);
        List<BasicBlock> compact = new ArrayList<>();
        for (BasicBlock old : g.blocks) if (live.contains(old.id)) {
            BasicBlock b = new BasicBlock(ids.get(old.id)); b.code.addAll(old.code);
            for (Phi oldPhi : old.phis) {
                Phi phi = new Phi(oldPhi.result);
                oldPhi.inputs.forEach((edge, value) -> { if (predecessors.getOrDefault(old.id, Set.of()).contains(edge)) phi.inputs.put(ids.get(edge), value); });
                b.phis.add(phi);
            }
            b.end = switch (old.end) {
                case Jump j -> new Jump(ids.get(j.target()));
                case Branch c -> new Branch(c.condition(), ids.get(c.yes()), ids.get(c.no()));
                case Exit e -> e;
            };
            compact.add(b);
        }
        List<Loop> loops = new ArrayList<>();
        for (Loop loop : g.loops) if (live.contains(loop.header()) && live.contains(loop.preheader())) {
            Set<Integer> members = new HashSet<>();
            for (int id : loop.members()) if (live.contains(id)) members.add(ids.get(id));
            loops.add(new Loop(ids.get(loop.header()), ids.get(loop.preheader()), Set.copyOf(members)));
        }
        List<Integer> layout = g.layout.stream().filter(live::contains).map(ids::get).toList();
        g.blocks.clear(); g.blocks.addAll(compact); g.loops.clear(); g.loops.addAll(loops);
        g.layout.clear(); g.layout.addAll(layout);
        return true;
    }
    private static Expr resolve(Expr e, Map<Var, Expr> replacements) {
        while (e instanceof Var v && replacements.containsKey(v)) e = replacements.get(v);
        return e;
    }
    private static void rewrite(SsaProgram g, Map<Var, Expr> replacements) {
        UnaryOperator<Expr> f = e -> resolve(e, replacements);
        for (BasicBlock b : g.blocks) {
            for (Phi p : b.phis) p.inputs.replaceAll((edge, value) -> map(value, f));
            b.code.replaceAll(s -> map(s, f));
            b.end = map(b.end, f);
        }
    }
    private static void simplifyPhis(SsaProgram g) {
        boolean changed;
        do {
            Map<Var, Expr> replacements = new HashMap<>(); changed = false;
            for (BasicBlock b : g.blocks) {
                var iterator = b.phis.iterator();
                while (iterator.hasNext()) {
                    Phi p = iterator.next(); Expr single = null; boolean different = false;
                    for (Expr input : p.inputs.values()) {
                        input = resolve(input, replacements);
                        if (input.equals(p.result)) continue;
                        if (single == null) single = input;
                        else if (!single.equals(input)) { different = true; break; }
                    }
                    if (!different && single != null) { replacements.put(p.result, single); iterator.remove(); changed = true; }
                }
            }
            rewrite(g, replacements);
        } while (changed);
    }
    private static void numberValues(SsaProgram g) {
        Analysis flow = new Analysis(g);
        Map<Var, Expr> replacements = new HashMap<>();
        number(g, 0, flow, new HashMap<>(), replacements);
        rewrite(g, replacements);
    }
    private static void number(SsaProgram g, int id, Analysis flow, Map<Expr, Var> inherited, Map<Var, Expr> replacements) {
        BasicBlock b = g.blocks.get(id);
        Map<Expr, Var> available = new HashMap<>(inherited);
        Map<Expr, Var> reads = new HashMap<>(); // A new block is a conservative memory barrier.
        List<Statement> output = new ArrayList<>();
        for (Statement original : b.code) {
            Statement s = map(original, e -> resolve(e, replacements));
            Effect effect = effect(s);
            if (effect == Effect.WRITE || effect == Effect.CALL) reads.clear();
            if (s instanceof Define d && effect != Effect.CALL) {
                Expr value = IrOptimizer.simplify(d.value(), Map.of());
                // Canonicalize commutative values; literals on the right also enable immediates.
                if (value instanceof Binary x && switch (x.op()) { case ADD, MUL, AND, XOR, EQ, NE -> true; default -> false; }) {
                    if (x.left() instanceof Literal && x.right() instanceof Var
                            || x.left() instanceof Var a && x.right() instanceof Var c && a.id() > c.id())
                        value = new Binary(x.op(), x.right(), x.left());
                }
                if (value instanceof Var || value instanceof Literal) { replacements.put(d.variable(), value); continue; }
                Map<Expr, Var> table = effect == Effect.READ ? reads : available;
                Var prior = table.get(value);
                if (prior != null) { replacements.put(d.variable(), prior); continue; }
                table.put(value, d.variable()); s = new Define(d.variable(), value);
            }
            output.add(s);
        }
        b.code.clear(); b.code.addAll(output);
        for (int child : flow.children.get(id)) number(g, child, flow, available, replacements);
    }
    private static void eliminateDeadValues(SsaProgram g) {
        Map<Var, Set<Var>> dependencies = new HashMap<>();
        Set<Var> live = new HashSet<>();
        for (BasicBlock b : g.blocks) {
            for (Phi p : b.phis) {
                Set<Var> used = new HashSet<>(); p.inputs.values().forEach(e -> used.addAll(reads(e)));
                dependencies.put(p.result, used);
            }
            for (Statement s : b.code) {
                Set<Var> used = new HashSet<>(); map(s, e -> { used.add((Var) e); return e; });
                if (s instanceof Define d) dependencies.put(d.variable(), used);
                if (effect(s) != Effect.PURE) live.addAll(used);
            }
            map(b.end, e -> { live.add((Var) e); return e; });
        }
        var pending = new ArrayDeque<>(live);
        while (!pending.isEmpty()) for (Var input : dependencies.getOrDefault(pending.removeFirst(), Set.of()))
            if (live.add(input)) pending.add(input);
        for (BasicBlock b : g.blocks) {
            b.phis.removeIf(p -> !live.contains(p.result));
            b.code.removeIf(s -> s instanceof Define d && effect(s) == Effect.PURE && !live.contains(d.variable()));
        }
    }
    private static void hoist(SsaProgram g) {
        Analysis flow = new Analysis(g);
        Map<Var, Integer> owners = new HashMap<>();
        for (Var p : List.of(COLUMNS, ROWS, STATE)) owners.put(p, 0);
        for (BasicBlock b : g.blocks) {
            for (Phi p : b.phis) owners.put(p.result, b.id);
            for (Statement s : b.code) if (s instanceof Define d) owners.put(d.variable(), b.id);
        }
        // Inner loops were recorded first. Outer loops can then lift their invariants again.
        for (Loop loop : g.loops) {
            BasicBlock preheader = g.blocks.get(loop.preheader());
            boolean changed;
            do {
                changed = false;
                for (BasicBlock b : g.blocks) if (loop.members().contains(b.id)) {
                    var instructions = b.code.iterator();
                    while (instructions.hasNext()) {
                        Statement s = instructions.next();
                        if (!(s instanceof Define d) || effect(s) != Effect.PURE) continue;
                        boolean invariant = reads(d.value()).stream().allMatch(v -> {
                            int owner = owners.get(v);
                            return !loop.members().contains(owner) && flow.dominators[loop.preheader()].get(owner);
                        });
                        if (invariant) {
                            preheader.code.add(s); owners.put(d.variable(), preheader.id); instructions.remove(); changed = true;
                        }
                    }
                }
            } while (changed);
        }
    }

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
        final List<List<Integer>> children = new ArrayList<>();
        final BitSet[] dominators;
        Analysis(SsaProgram g) {
            int count = g.blocks.size(); dominators = new BitSet[count];
            for (int i = 0; i < count; i++) { predecessors.add(new HashSet<>()); children.add(new ArrayList<>()); }
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
            for (int i = 1; i < count; i++) {
                int parent = 0;
                for (int d = dominators[i].nextSetBit(0); d >= 0; d = dominators[i].nextSetBit(d + 1))
                    if (d != i && dominators[d].cardinality() > dominators[parent].cardinality()) parent = d;
                children.get(parent).add(i);
            }
        }
    }
}
