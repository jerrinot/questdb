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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.questdb.experimental.codegen.Ir.*;

/** Effect-aware simplification of the shared structured IR, before any target lowering. */
public final class IrOptimizer {
    private static final boolean ENABLED = !Boolean.getBoolean("questdb.codegen.disableOptimization");

    private IrOptimizer() { }

    record Prepared(Program structured, SsaProgram ssa) {
        String dump() { return ssa == null ? SourceEmitter.dump(structured) : ssa.dump(); }
    }

    static Prepared prepare(Program program) {
        Program structured = ENABLED ? optimize(program) : program;
        if (!Boolean.getBoolean("questdb.codegen.ssa")) return new Prepared(structured, null);
        SsaProgram graph = SsaProgram.lower(structured);
        if (ENABLED && !Boolean.getBoolean("questdb.codegen.disableSsaOptimization")) SsaOptimizer.optimize(graph);
        SsaOptimizer.verify(graph);
        return new Prepared(structured, graph);
    }

    public static Program optimize(Program program) {
        Block result = propagate(program.body(), new HashMap<>());
        // Propagation can leave chains of unused local definitions. Remove to a fixed point.
        while (true) {
            Set<Var> reads = new HashSet<>();
            collectReads(result, reads);
            Set<Var> assigned = new HashSet<>();
            collectRetainedAssignments(result, reads, assigned);
            Block next = removeDeadLocals(result, reads, assigned);
            if (next.equals(result)) return new Program(result);
            result = next;
        }
    }

    private static Block propagate(Block block, Map<Var, Expr> facts) {
        List<Statement> output = new ArrayList<>();
        for (Statement statement : block.statements()) {
            switch (statement) {
                case Define d -> {
                    Expr value = simplify(d.value(), facts);
                    output.add(new Define(d.variable(), value));
                    bind(facts, d.variable(), value);
                }
                case Assign a -> {
                    Expr value = simplify(a.value(), facts);
                    if (!value.equals(a.variable())) output.add(new Assign(a.variable(), value));
                    bind(facts, a.variable(), value);
                }
                case AtomicStore atomic -> output.add(new AtomicStore(simplify(atomic.address(), facts), simplify(atomic.value(), facts)));
                case Store s -> output.add(new Store(s.width(), simplify(s.address(), facts), simplify(s.value(), facts)));
                case Prefetch p -> output.add(new Prefetch(simplify(p.address(), facts), p.write()));
                case Free f -> output.add(new Free(simplify(f.address(), facts), simplify(f.bytes(), facts)));
                case Poll p -> output.add(p);
                case Call ignored -> throw new IllegalArgumentException("Expand helpers before structured lowering");
                case Return r -> output.add(new Return(simplify(r.value(), facts)));
                case If i -> {
                    Expr condition = simplify(i.condition(), facts);
                    if (condition.equals(bool(true))) {
                        output.addAll(propagate(i.body(), facts).statements());
                    } else if (!condition.equals(bool(false))) {
                        Map<Var, Expr> taken = new HashMap<>(facts);
                        Block body = propagate(i.body(), taken);
                        if (!body.statements().isEmpty() || !discardable(condition)) output.add(new If(condition, body));
                        // A fact must hold on both the taken and untaken paths.
                        facts.entrySet().removeIf(e -> !e.getValue().equals(taken.get(e.getKey())));
                    }
                }
                case While w -> {
                    Set<Var> writes = new HashSet<>();
                    collectWrites(w.body(), writes);
                    Map<Var, Expr> loopFacts = new HashMap<>(facts);
                    invalidate(loopFacts, writes);
                    Expr condition = simplify(w.condition(), loopFacts);
                    if (!condition.equals(bool(false))) {
                        output.add(new While(condition, propagate(w.body(), loopFacts)));
                        // Zero or many iterations: never carry an iteration's values across the backedge.
                        invalidate(facts, writes);
                    }
                }
            }
            if (!output.isEmpty() && output.getLast() instanceof Return) break;
        }
        return new Block(output);
    }

    private static void bind(Map<Var, Expr> facts, Var target, Expr value) {
        invalidate(facts, Set.of(target));
        if ((value instanceof Literal || value instanceof Var) && !target.equals(value)) facts.put(target, value);
    }

    private static void invalidate(Map<Var, Expr> facts, Set<Var> writes) {
        // Copy facts always point directly to an unbound variable, never through another copy.
        facts.entrySet().removeIf(e -> writes.contains(e.getKey()) || e.getValue() instanceof Var v && writes.contains(v));
    }

    static Expr simplify(Expr expression, Map<Var, Expr> facts) {
        return switch (expression) {
            case Var v -> facts.getOrDefault(v, v);
            case Literal l -> l;
            case Load l -> new Load(l.width(), simplify(l.address(), facts));
            case AtomicLoad l -> new AtomicLoad(simplify(l.address(), facts), l.acquire());
            case AtomicAdd a -> new AtomicAdd(simplify(a.address(), facts), simplify(a.value(), facts));
            case CompareExchange c -> new CompareExchange(simplify(c.address(), facts), simplify(c.expected(), facts), simplify(c.value(), facts));
            case Allocate a -> new Allocate(simplify(a.bytes(), facts));
            case ToDouble c -> {
                Expr value = simplify(c.value(), facts);
                yield value instanceof Literal l ? f64((double) l.value()) : new ToDouble(value);
            }
            case VectorMask m -> new VectorMask(m.lanes(), simplify(m.predicate(), facts));
            case SelectRows s -> new SelectRows(s.lanes(), simplify(s.mask(), facts), simplify(s.base(), facts), simplify(s.destination(), facts));
            case PopulationCount z -> { Expr v = simplify(z.value(), facts); yield v instanceof Literal l ? i64(Long.bitCount(l.value())) : new PopulationCount(v); }
            case TrailingZeros z -> { Expr v = simplify(z.value(), facts); yield v instanceof Literal l ? i64(Long.numberOfTrailingZeros(l.value())) : new TrailingZeros(v); }
            case Offset o -> {
                Expr address = simplify(o.address(), facts), bytes = simplify(o.bytes(), facts);
                if (bytes.equals(i64(0))) yield address;
                if (address instanceof Offset inner && inner.bytes() instanceof Literal a && bytes instanceof Literal b)
                    yield new Offset(inner.address(), i64(a.value() + b.value()));
                yield new Offset(address, bytes);
            }
            case Select s -> {
                Expr condition = simplify(s.condition(), facts);
                if (condition instanceof Literal l) yield simplify(l.value() != 0 ? s.yes() : s.no(), facts);
                Expr yes = simplify(s.yes(), facts), no = simplify(s.no(), facts);
                if (yes.equals(no) && discardable(condition)) yield yes;
                if (yes.equals(bool(true)) && no.equals(bool(false))) yield condition;
                if (yes.equals(bool(false)) && no.equals(bool(true))) yield negate(condition);
                yield new Select(condition, yes, no);
            }
            case Binary b -> {
                Expr left = simplify(b.left(), facts);
                // Simplify the left operand first: the right operand may never execute.
                if (b.op() == Op.BOTH && left.equals(bool(false))) yield left;
                if (b.op() == Op.EITHER && left.equals(bool(true))) yield left;
                yield binary(b.op(), left, simplify(b.right(), facts));
            }
        };
    }

    private static Expr binary(Op op, Expr left, Expr right) {
        if (left instanceof Literal a && right instanceof Literal b) {
            long x = a.value(), y = b.value();
            return switch (op) {
                case FADD -> f64(Double.longBitsToDouble(x) + Double.longBitsToDouble(y));
                case FSUB -> f64(Double.longBitsToDouble(x) - Double.longBitsToDouble(y));
                case FMUL -> f64(Double.longBitsToDouble(x) * Double.longBitsToDouble(y));
                case FDIV -> f64(Double.longBitsToDouble(x) / Double.longBitsToDouble(y));
                case ADD -> i64(x + y); case SUB -> i64(x - y); case MUL -> i64(x * y); case SDIV -> i64(x / y);
                case AND -> i64(x & y); case XOR -> i64(x ^ y); case USHR -> i64(x >>> y);
                case EQ -> bool(x == y); case NE -> bool(x != y); case LT -> bool(x < y); case GE -> bool(x >= y);
                case BOTH -> bool(x != 0 && y != 0); case EITHER -> bool(x != 0 || y != 0);
            };
        }
        if ((op == Op.EQ || op == Op.NE) && left.type() == Type.BOOL) {
            if (right instanceof Literal l) return (l.value() != 0) == (op == Op.EQ) ? left : negate(left);
            if (left instanceof Literal l) return (l.value() != 0) == (op == Op.EQ) ? right : negate(right);
        }
        if (op == Op.BOTH || op == Op.EITHER) {
            boolean and = op == Op.BOTH;
            if (left.equals(bool(and))) return right;
            if (right.equals(bool(and))) return left;
            if (right.equals(bool(!and)) && discardable(left)) return right;
            Expr comparison = combineComparisons(op, left, right);
            if (comparison != null) return comparison;
        }
        if (right instanceof Literal r) {
            if (r.value() == 0 && (op == Op.ADD || op == Op.SUB || op == Op.XOR)) return left;
            if ((r.value() == 1 && (op == Op.MUL || op == Op.SDIV)) || (r.value() == -1 && op == Op.AND)) return left;
            if (op == Op.USHR && (r.value() & 63) == 0) return left;
            if (r.value() == 0 && (op == Op.MUL || op == Op.AND) && discardable(left)) return i64(0);
        }
        if (left instanceof Literal l) {
            if (l.value() == 0 && (op == Op.ADD || op == Op.XOR)) return right;
            if ((l.value() == 1 && op == Op.MUL) || (l.value() == -1 && op == Op.AND)) return right;
            if (l.value() == 0 && (op == Op.MUL || op == Op.AND) && discardable(right)) return i64(0);
        }
        if (left.equals(right) && discardable(left)) {
            switch (op) {
                case SUB, XOR: return i64(0);
                case AND, BOTH, EITHER: return left;
                case EQ, GE: return bool(true);
                case NE, LT: return bool(false);
                default: break;
            }
        }
        return new Binary(op, left, right);
    }

    private static Expr negate(Expr expression) {
        if (expression instanceof Literal l) return bool(l.value() == 0);
        if (expression instanceof Binary b) {
            Op inverse = switch (b.op()) {case EQ -> Op.NE; case NE -> Op.EQ; case LT -> Op.GE; case GE -> Op.LT; default -> null;};
            if (inverse != null) return new Binary(inverse, b.left(), b.right());
        }
        return new Binary(Op.EQ, expression, bool(false));
    }

    /** Boolean algebra over the three possible integer comparison outcomes: less, equal, greater. */
    private static Expr combineComparisons(Op op, Expr left, Expr right) {
        if (!(left instanceof Binary a) || !(right instanceof Binary b) || !a.left().equals(b.left())
                || !a.right().equals(b.right()) || !discardable(a.left()) || !discardable(a.right())) return null;
        int x = comparisonMask(a.op()), y = comparisonMask(b.op());
        if (x < 0 || y < 0) return null;
        return switch (op == Op.BOTH ? x & y : x | y) {
            case 0 -> bool(false); case 7 -> bool(true);
            case 1 -> new Binary(Op.LT, a.left(), a.right());
            case 2 -> new Binary(Op.EQ, a.left(), a.right());
            case 3 -> new Binary(Op.GE, a.right(), a.left());
            case 4 -> new Binary(Op.LT, a.right(), a.left());
            case 5 -> new Binary(Op.NE, a.left(), a.right());
            case 6 -> new Binary(Op.GE, a.left(), a.right());
            default -> throw new AssertionError();
        };
    }
    private static int comparisonMask(Op op) {
        return switch (op) {case LT -> 1; case EQ -> 2; case NE -> 5; case GE -> 6; default -> -1;};
    }

    /** Loads may fault and allocations have side effects. Do not duplicate, move or discard either. */
    private static boolean discardable(Expr expression) {
        return switch (expression) {
            case Var ignored -> true;
            case Literal ignored -> true;
            case Load ignored -> false;
            case AtomicLoad ignored -> false;
            case AtomicAdd ignored -> false;
            case CompareExchange ignored -> false;
            case VectorMask ignored -> false;
            case SelectRows ignored -> false;
            case PopulationCount z -> discardable(z.value());
            case TrailingZeros z -> discardable(z.value());
            case Allocate ignored -> false;
            case ToDouble c -> discardable(c.value());
            case Binary b -> discardable(b.left()) && discardable(b.right());
            case Offset o -> discardable(o.address()) && discardable(o.bytes());
            case Select s -> discardable(s.condition()) && discardable(s.yes()) && discardable(s.no());
        };
    }

    private static void collectWrites(Block block, Set<Var> writes) {
        for (Statement s : block.statements()) switch (s) {
            case Assign a -> writes.add(a.variable());
            case If i -> collectWrites(i.body(), writes);
            case While w -> collectWrites(w.body(), writes);
            default -> { }
        };
    }
    private static void collectReads(Block block, Set<Var> reads) {
        for (Statement s : block.statements()) switch (s) {
            case Call call -> call.arguments().forEach(argument -> collectReads(argument, reads));
            case Define d -> collectReads(d.value(), reads);
            case Assign a -> collectReads(a.value(), reads);
            case AtomicStore atomic -> { collectReads(atomic.address(), reads); collectReads(atomic.value(), reads); }
            case Store m -> {collectReads(m.address(), reads); collectReads(m.value(), reads);}
            case Prefetch p -> collectReads(p.address(), reads);
            case Free f -> {collectReads(f.address(), reads); collectReads(f.bytes(), reads);}
            case Return r -> collectReads(r.value(), reads);
            case If i -> {collectReads(i.condition(), reads); collectReads(i.body(), reads);}
            case While w -> {collectReads(w.condition(), reads); collectReads(w.body(), reads);}
            case Poll ignored -> { }
        };
    }
    private static void collectReads(Expr expression, Set<Var> reads) {
        switch (expression) {
            case Var v -> reads.add(v);
            case Binary b -> {collectReads(b.left(), reads); collectReads(b.right(), reads);}
            case Load l -> collectReads(l.address(), reads);
            case AtomicLoad l -> collectReads(l.address(), reads);
            case AtomicAdd a -> { collectReads(a.address(), reads); collectReads(a.value(), reads); }
            case CompareExchange c -> { collectReads(c.address(), reads); collectReads(c.expected(), reads); collectReads(c.value(), reads); }
            case Allocate a -> collectReads(a.bytes(), reads);
            case ToDouble c -> collectReads(c.value(), reads);
            case VectorMask m -> collectReads(m.predicate(), reads);
            case SelectRows s -> { collectReads(s.mask(), reads); collectReads(s.base(), reads); collectReads(s.destination(), reads); }
            case PopulationCount z -> collectReads(z.value(), reads);
            case TrailingZeros z -> collectReads(z.value(), reads);
            case Offset o -> {collectReads(o.address(), reads); collectReads(o.bytes(), reads);}
            case Select s -> {collectReads(s.condition(), reads); collectReads(s.yes(), reads); collectReads(s.no(), reads);}
            case Literal ignored -> { }
        }
    }
    private static void collectRetainedAssignments(Block block, Set<Var> reads, Set<Var> targets) {
        for (Statement s : block.statements()) switch (s) {
            case Assign a -> {if (reads.contains(a.variable()) || !discardable(a.value())) targets.add(a.variable());}
            case If i -> collectRetainedAssignments(i.body(), reads, targets);
            case While w -> collectRetainedAssignments(w.body(), reads, targets);
            default -> { }
        };
    }
    private static Block removeDeadLocals(Block block, Set<Var> reads, Set<Var> targets) {
        List<Statement> output = new ArrayList<>();
        for (Statement s : block.statements()) switch (s) {
            case Define d -> {if (reads.contains(d.variable()) || targets.contains(d.variable()) || !discardable(d.value())) output.add(d);}
            case Assign a -> {if (reads.contains(a.variable()) || !discardable(a.value())) output.add(a);}
            case If i -> {
                Block body = removeDeadLocals(i.body(), reads, targets);
                if (!body.statements().isEmpty() || !discardable(i.condition())) output.add(new If(i.condition(), body));
            }
            case While w -> output.add(new While(w.condition(), removeDeadLocals(w.body(), reads, targets)));
            default -> output.add(s);
        };
        return new Block(output);
    }
}
