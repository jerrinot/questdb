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

/** Typed SSA between operator lowering and either backend. Control flow and phi edges are explicit. */
final class SsaProgram {
    sealed interface End permits Jump, Branch, Exit { }
    record Jump(int target) implements End { }
    record Branch(Expr condition, int yes, int no) implements End { }
    record Exit(Expr value) implements End { }
    static final class Phi {
        final Var result;
        final Map<Integer, Expr> inputs = new LinkedHashMap<>();
        Phi(Var result) { this.result = result; }
    }
    static final class BasicBlock {
        final int id;
        final List<Phi> phis = new ArrayList<>();
        final List<Statement> code = new ArrayList<>();
        End end;
        BasicBlock(int id) { this.id = id; }
    }
    record Loop(int header, int preheader, Set<Integer> members) { }
    final List<BasicBlock> blocks = new ArrayList<>();
    final List<Loop> loops = new ArrayList<>();
    final List<Integer> layout = new ArrayList<>();
    int nextValue = 3;
    List<Var> parameters = List.of(COLUMNS, ROWS, STATE);

    static SsaProgram lower(Program input) {
        return lower(input.body(), List.of(COLUMNS, ROWS, STATE));
    }
    static SsaProgram lower(Helper helper) {
        return lower(helper.body(), helper.parameters());
    }
    private static SsaProgram lower(Block body, List<Var> parameters) {
        SsaProgram graph = new SsaProgram();
        graph.parameters = parameters;
        graph.nextValue = parameters.stream().mapToInt(Var::id).max().orElse(-1) + 1;
        graph.new Translator().translate(body);
        return graph;
    }
    Var fresh(Type type) { return new Var(nextValue++, type); }
    BasicBlock block() { BasicBlock b = new BasicBlock(blocks.size()); blocks.add(b); return b; }
    static List<Integer> successors(End end) {
        return switch (end) {
            case Jump j -> List.of(j.target());
            case Branch b -> b.yes() == b.no() ? List.of(b.yes()) : List.of(b.yes(), b.no());
            case Exit ignored -> List.of();
        };
    }
    static Expr map(Expr e, UnaryOperator<Expr> leaf) {
        return switch (e) {
            case Var v -> leaf.apply(v);
            case Literal l -> l;
            case Binary b -> new Binary(b.op(), map(b.left(), leaf), map(b.right(), leaf));
            case Offset o -> new Offset(map(o.address(), leaf), map(o.bytes(), leaf));
            case Load l -> new Load(l.width(), map(l.address(), leaf));
            case AtomicLoad l -> new AtomicLoad(map(l.address(), leaf), l.acquire());
            case AtomicAdd a -> new AtomicAdd(map(a.address(), leaf), map(a.value(), leaf));
            case CompareExchange c -> new CompareExchange(map(c.address(), leaf), map(c.expected(), leaf), map(c.value(), leaf));
            case Allocate a -> new Allocate(map(a.bytes(), leaf));
            case Reallocate r -> new Reallocate(map(r.address(), leaf), map(r.oldBytes(), leaf), map(r.newBytes(), leaf));
            case ToDouble c -> new ToDouble(map(c.value(), leaf));
            case VectorMask m -> new VectorMask(m.lanes(), map(m.predicate(), leaf));
            case PopulationCount z -> new PopulationCount(map(z.value(), leaf));
            case TrailingZeros z -> new TrailingZeros(map(z.value(), leaf));
            case SelectRows s -> new SelectRows(s.lanes(), map(s.mask(), leaf), map(s.base(), leaf), map(s.destination(), leaf));
            case Select s -> new Select(map(s.condition(), leaf), map(s.yes(), leaf), map(s.no(), leaf));
        };
    }
    static Statement map(Statement s, UnaryOperator<Expr> f) {
        return switch (s) {
            case Define d -> new Define(d.variable(), map(d.value(), f));
            case AtomicStore atomic -> new AtomicStore(map(atomic.address(), f), map(atomic.value(), f));
            case Store w -> new Store(w.width(), map(w.address(), f), map(w.value(), f));
            case Free w -> new Free(map(w.address(), f), map(w.bytes(), f));
            case Poll p -> p;
            case Call c -> new Call(c.helper(), c.arguments().stream().map(e -> map(e, f)).toList());
            case Prefetch p -> new Prefetch(map(p.address(), f), p.write());
            default -> throw new IllegalArgumentException("Not an SSA instruction: " + s);
        };
    }
    static End map(End end, UnaryOperator<Expr> f) {
        return switch (end) {
            case Jump j -> j;
            case Branch b -> new Branch(map(b.condition(), f), b.yes(), b.no());
            case Exit e -> new Exit(map(e.value(), f));
        };
    }
    static Set<Var> reads(Expr e) {
        Set<Var> result = new HashSet<>();
        map(e, value -> { result.add((Var) value); return value; });
        return result;
    }
    String dump() {
        StringBuilder out = new StringBuilder("SSA kernel(v0: ADDRESS columns, v1: I64 rows, v2: ADDRESS state)\n");
        for (BasicBlock b : blocks) {
            out.append("b").append(b.id).append(":\n");
            for (Phi p : b.phis) out.append("  ").append(p.result).append(" = phi ").append(p.inputs).append('\n');
            for (Statement s : b.code) out.append("  ")
                    .append(s instanceof Call c ? "call " + c.helper().name() + c.arguments() : s).append('\n');
            out.append("  ").append(b.end).append('\n');
        }
        return out.toString();
    }

    /** Out-of-SSA uses edge-local parallel copies. Snapshot every input before overwriting any phi. */
    List<BasicBlock> executableBlocks() {
        rematerializeCheapValues();
        aliases = SsaLiveness.coalesce(this);
        List<BasicBlock> output = new ArrayList<>();
        List<BasicBlock> edges = new ArrayList<>();
        nextEdge = blocks.size();
        Map<Var, Integer> uses = new HashMap<>(), useBlocks = new HashMap<>();
        for (BasicBlock b : blocks) {
            UnaryOperator<Expr> count = e -> {
                Var v = (Var) e; uses.merge(v, 1, Integer::sum); useBlocks.put(v, b.id); return e;
            };
            for (Phi p : b.phis) for (Expr value : p.inputs.values()) map(value, e -> {
                uses.merge((Var) e, 1, Integer::sum); useBlocks.put((Var) e, -1); return e;
            });
            for (Statement s : b.code) map(s, count);
            map(b.end, count);
        }
        for (int blockId : layout) {
            BasicBlock original = blocks.get(blockId);
            BasicBlock b = new BasicBlock(original.id);
            Map<Var, Expr> trees = new HashMap<>();
            UnaryOperator<Expr> inline = e -> trees.getOrDefault((Var) e, e);
            for (Statement s : original.code) {
                Statement rewritten = map(s, inline);
                // Fold single-use pure values locally for instruction selection, without undoing loop motion.
                if (rewritten instanceof Define d && SsaOptimizer.effect(s) == SsaOptimizer.Effect.PURE
                        && uses.getOrDefault(d.variable(), 0) == 1 && useBlocks.get(d.variable()) == b.id)
                    trees.put(d.variable(), d.value());
                else b.code.add(rewritten);
            }
            b.end = switch (map(original.end, inline)) {
                case Jump j -> { copies(original.id, j.target(), b); yield j; }
                case Branch c -> new Branch(c.condition(), edge(original.id, c.yes(), edges), edge(original.id, c.no(), edges));
                case Exit e -> e;
            };
            output.add(b);
            output.addAll(edges); edges.clear();
        }
        // Keep a return last for the native wire verifier; execution always follows explicit edges.
        output.sort(Comparator.comparingInt(b -> b.end instanceof Exit ? 1 : 0));
        for (BasicBlock b : output) {
            b.code.replaceAll(s -> switch (s) {
                case Define d -> new Define(alias(d.variable()), map(d.value(), e -> alias((Var)e)));
                case Assign a -> new Assign(alias(a.variable()), map(a.value(), e -> alias((Var)e)));
                default -> map(s, e -> alias((Var)e));
            });
            b.code.removeIf(s -> s instanceof Assign a && a.variable().equals(a.value()));
            b.end = map(b.end, e -> alias((Var)e));
        }
        Map<Integer, Integer> redirects = new HashMap<>();
        for (BasicBlock b : output) if (b.id != 0 && b.code.isEmpty() && b.end instanceof Jump j && b.id != j.target()) redirects.put(b.id, j.target());
        // Do not rewrite cycles of empty blocks; they still represent nontermination.
        java.util.function.IntUnaryOperator destination = id -> {
            Set<Integer> seen = new HashSet<>(); int next = id;
            while (redirects.containsKey(next)) { if (!seen.add(next)) return id; next = redirects.get(next); }
            return next;
        };
        for (BasicBlock b : output) b.end = switch (b.end) {
            case Jump j -> new Jump(destination.applyAsInt(j.target()));
            case Branch c -> new Branch(c.condition(), destination.applyAsInt(c.yes()), destination.applyAsInt(c.no()));
            case Exit e -> e;
        };
        output.removeIf(b -> b.id != destination.applyAsInt(b.id));
        return output;
    }
    private Map<Var, Var> aliases = Map.of();
    private Var alias(Var v) { return aliases.getOrDefault(v, v); }
    private void rematerializeCheapValues() {
        Map<Var, Expr> cheap = new HashMap<>();
        for (BasicBlock b : blocks) for (Statement s : b.code)
            if (s instanceof Define d && (d.value() instanceof Offset o && o.bytes() instanceof Literal
                    || d.value() instanceof Binary x && switch (x.op()) { case EQ, NE, LT, GE -> true; default -> false; }))
                cheap.put(d.variable(), d.value());
        UnaryOperator<Expr> replace = new UnaryOperator<>() {
            public Expr apply(Expr e) { return cheap.containsKey(e) ? map(cheap.get(e), this) : e; }
        };
        for (BasicBlock b : blocks) {
            b.code.removeIf(s -> s instanceof Define d && cheap.containsKey(d.variable()));
            b.code.replaceAll(s -> map(s, replace));
            b.end = map(b.end, replace);
            for (Phi p : b.phis) p.inputs.replaceAll((edge, value) -> map(value, replace));
        }
    }
    private int nextEdge;
    private int edge(int source, int target, List<BasicBlock> edges) {
        if (blocks.get(target).phis.isEmpty()) return target;
        BasicBlock edge = new BasicBlock(nextEdge++);
        copies(source, target, edge);
        if (edge.code.isEmpty()) return target;
        edge.end = new Jump(target); edges.add(edge);
        return edge.id;
    }
    private void copies(int source, int target, BasicBlock edge) {
        List<Assign> writes = new ArrayList<>();
        for (Phi p : blocks.get(target).phis) {
            Expr input = Objects.requireNonNull(p.inputs.get(source), "Missing phi predecessor");
            if (input instanceof Var v && alias(v).equals(alias(p.result))) continue;
            Var temp = fresh(input.type());
            edge.code.add(new Define(temp, input));
            writes.add(new Assign(p.result, temp));
        }
        edge.code.addAll(writes);
    }

    private final class Translator {
        BasicBlock current;
        Map<Var, Expr> values = new HashMap<>();
        void enter(BasicBlock b) { current = b; layout.add(b.id); }
        void translate(Block input) {
            enter(block());
            for (Var p : parameters) values.put(p, p);
            statements(input);
        }
        Expr emit(Expr expression) {
            Expr value = switch (expression) {
                case Var v -> Objects.requireNonNull(values.get(v));
                case Literal l -> l;
                case Binary b -> {
                    if (b.op() == Op.BOTH) yield select(b.left(), b.right(), bool(false));
                    if (b.op() == Op.EITHER) yield select(b.left(), bool(true), b.right());
                    yield new Binary(b.op(), emit(b.left()), emit(b.right()));
                }
                case Offset o -> new Offset(emit(o.address()), emit(o.bytes()));
                case Load l -> new Load(l.width(), emit(l.address()));
                case AtomicLoad l -> new AtomicLoad(emit(l.address()), l.acquire());
                case AtomicAdd a -> new AtomicAdd(emit(a.address()), emit(a.value()));
                case CompareExchange c -> new CompareExchange(emit(c.address()), emit(c.expected()), emit(c.value()));
                case Allocate a -> new Allocate(emit(a.bytes()));
                case Reallocate r -> new Reallocate(emit(r.address()), emit(r.oldBytes()), emit(r.newBytes()));
                case ToDouble c -> new ToDouble(emit(c.value()));
                // Keep the lane region intact. Extracting its loads here would turn
                // them into scalar, eager reads outside the vector operation.
                case VectorMask m -> new VectorMask(m.lanes(), map(m.predicate(), e -> Objects.requireNonNull(values.get((Var) e))));
                case PopulationCount z -> new PopulationCount(emit(z.value()));
                case TrailingZeros z -> new TrailingZeros(emit(z.value()));
                case SelectRows s -> new SelectRows(s.lanes(), emit(s.mask()), emit(s.base()), emit(s.destination()));
                case Select s -> select(s.condition(), s.yes(), s.no());
            };
            if (value instanceof Var || value instanceof Literal) return value;
            Var result = fresh(value.type()); current.code.add(new Define(result, value)); return result;
        }
        Expr select(Expr condition, Expr yes, Expr no) {
            Expr test = emit(condition);
            BasicBlock taken = block(), untaken = block(), merge = block();
            current.end = new Branch(test, taken.id, untaken.id);
            Phi p = new Phi(fresh(yes.type()));
            enter(taken); Expr y = emit(yes); p.inputs.put(current.id, y); current.end = new Jump(merge.id);
            enter(untaken); Expr n = emit(no); p.inputs.put(current.id, n); current.end = new Jump(merge.id);
            merge.phis.add(p); enter(merge); return p.result;
        }
        void statements(Block input) {
            for (Statement s : input.statements()) {
                if (current == null) break;
                switch (s) {
                    case Define d -> values.put(d.variable(), emit(d.value()));
                    case Assign a -> values.put(a.variable(), emit(a.value()));
                    case AtomicStore atomic -> { Expr address = emit(atomic.address()), value = emit(atomic.value()); current.code.add(new AtomicStore(address, value)); }
                    case Store w -> { Expr address = emit(w.address()), value = emit(w.value()); current.code.add(new Store(w.width(), address, value)); }
                    case Free f -> { Expr address = emit(f.address()), bytes = emit(f.bytes()); current.code.add(new Free(address, bytes)); }
                    case Poll p -> current.code.add(p);
                    case Call c -> current.code.add(new Call(c.helper(), c.arguments().stream().map(this::emit).toList()));
                    case Prefetch p -> current.code.add(new Prefetch(emit(p.address()), p.write()));
                    case Return r -> { Expr value = emit(r.value()); current.end = new Exit(value); current = null; }
                    case If i -> conditional(i);
                    case While w -> loop(w);
                }
            }
        }
        void branch(Expr test, BasicBlock yes, BasicBlock no) {
            if (test instanceof Binary binary && (binary.op() == Op.BOTH || binary.op() == Op.EITHER)) {
                BasicBlock next = block();
                if (binary.op() == Op.BOTH) branch(binary.left(), next, no);
                else branch(binary.left(), yes, next);
                enter(next); branch(binary.right(), yes, no);
            } else {
                Expr value = emit(test); current.end = new Branch(value, yes.id, no.id);
            }
        }
        void conditional(If statement) {
            BasicBlock taken = block(), merge = block();
            branch(statement.condition(), taken, merge);
            List<Integer> untaken = blocks.stream().filter(b -> b.end != null && successors(b.end).contains(merge.id)).map(b -> b.id).toList();
            Map<Var, Expr> saved = new HashMap<>(values);
            enter(taken); statements(statement.body());
            Map<Var, Expr> after = values; values = new HashMap<>(saved);
            if (current != null) {
                current.end = new Jump(merge.id);
                for (Var v : saved.keySet().stream().sorted(Comparator.comparingInt(Var::id)).toList()) if (!saved.get(v).equals(after.get(v))) {
                    Phi p = new Phi(fresh(v.type()));
                    for (int edge : untaken) p.inputs.put(edge, saved.get(v));
                    p.inputs.put(current.id, after.get(v));
                    merge.phis.add(p); values.put(v, p.result);
                }
            }
            enter(merge);
        }
        void loop(While statement) {
            BasicBlock before = current, header = block();
            before.end = new Jump(header.id);
            Map<Var, Expr> saved = new HashMap<>(values);
            Set<Var> writes = new HashSet<>(); collectWrites(statement.body(), writes);
            Map<Var, Phi> carried = new LinkedHashMap<>();
            // Stable ordering keeps generated code reproducible.
            writes.stream().filter(saved::containsKey).sorted(Comparator.comparingInt(Var::id)).forEach(v -> {
                Phi p = new Phi(fresh(v.type())); p.inputs.put(before.id, saved.get(v));
                header.phis.add(p); carried.put(v, p); values.put(v, p.result);
            });
            Map<Var, Expr> headValues = new HashMap<>(values);
            enter(header);
            BasicBlock body = block(), exit = block(); branch(statement.condition(), body, exit);
            enter(body); statements(statement.body());
            if (current != null) {
                current.end = new Jump(header.id);
                for (var entry : carried.entrySet()) entry.getValue().inputs.put(current.id, values.get(entry.getKey()));
            }
            Set<Integer> members = new HashSet<>();
            for (int id = header.id; id < blocks.size(); id++) if (id != exit.id) members.add(id);
            loops.add(new Loop(header.id, before.id, Set.copyOf(members)));
            values = headValues; enter(exit);
        }
    }
    private static void collectWrites(Block body, Set<Var> result) {
        for (Statement s : body.statements()) switch (s) {
            case Assign a -> result.add(a.variable());
            case If i -> collectWrites(i.body(), result);
            case While w -> collectWrites(w.body(), result);
            default -> { }
        };
    }
}
