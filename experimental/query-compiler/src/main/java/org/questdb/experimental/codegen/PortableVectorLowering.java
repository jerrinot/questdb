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
import java.util.List;
import static org.questdb.experimental.codegen.Ir.*;

/** Scalar implementation of lane masks, preserving lazy control flow and read effects. */
final class PortableVectorLowering {
    private int next;
    private PortableVectorLowering() { }

    static Program lower(Program program) {
        var lowering = new PortableVectorLowering();
        lowering.next = 3;
        lowering.findIds(program.body());
        return new Program(lowering.block(program.body()));
    }

    private void findIds(Block b) {
        for (Statement s : b.statements()) {
            if (s instanceof Define d) next = Math.max(next, d.variable().id() + 1);
            else if (s instanceof If i) findIds(i.body());
            else if (s instanceof While w) findIds(w.body());
        }
    }
    private Var let(List<Statement> out, Expr value) {
        Var v = new Var(next++, value.type()); out.add(new Define(v, value)); return v;
    }
    private Block block(Block input) {
        List<Statement> out = new ArrayList<>();
        for (Statement s : input.statements()) {
            switch (s) {
                case Define d -> out.add(new Define(d.variable(), expr(d.value(), out)));
                case Assign a -> out.add(new Assign(a.variable(), expr(a.value(), out)));
                case AtomicStore atomic -> { Expr address = expr(atomic.address(), out); if (contains(atomic.value())) address = let(out, address); out.add(new AtomicStore(address, expr(atomic.value(), out))); }
                case Store w -> {
                    Expr address = expr(w.address(), out);
                    if (contains(w.value())) address = let(out, address);
                    out.add(new Store(w.width(), address, expr(w.value(), out)));
                }
                case Free f -> {
                    Expr address = expr(f.address(), out);
                    if (contains(f.bytes())) address = let(out, address);
                    out.add(new Free(address, expr(f.bytes(), out)));
                }
                case Return r -> out.add(new Return(expr(r.value(), out)));
                case Poll p -> out.add(p);
                case Call ignored -> throw new IllegalArgumentException("Expand helpers before structured lowering");
                case Prefetch p -> out.add(new Prefetch(expr(p.address(), out), p.write()));
                case If i -> out.add(new If(expr(i.condition(), out), block(i.body())));
                case While w -> {
                    if (!contains(w.condition())) out.add(new While(w.condition(), block(w.body())));
                    else {
                        Var condition = let(out, expr(w.condition(), out));
                        List<Statement> body = new ArrayList<>(block(w.body()).statements());
                        // A terminal return needs no further condition evaluation.
                        if (body.isEmpty() || !(body.getLast() instanceof Return))
                            body.add(new Assign(condition, expr(w.condition(), body)));
                        out.add(new While(condition, new Block(body)));
                    }
                }
            }
        }
        return new Block(out);
    }
    private Expr expr(Expr e, List<Statement> out) {
        if (!contains(e)) return e;
        return switch (e) {
            case VectorMask m -> {
                Var mask = let(out, i64(0)), lane = let(out, i64(0)), bit = let(out, i64(1));
                Expr predicate = LaneExpressions.mapLoads(m.predicate(), l -> new Load(l.width(),
                        new Offset(l.address(), binary(Op.MUL, lane, i64(l.width().bytes)))));
                out.add(new While(binary(Op.LT, lane, i64(m.lanes())), new Block(List.of(
                        new Assign(mask, add(mask, new Select(predicate, bit, i64(0)))),
                        new Assign(bit, add(bit, bit)), new Assign(lane, add(lane, i64(1)))))));
                yield mask;
            }
            case SelectRows s -> {
                Var mask = let(out, binary(Op.AND, expr(s.mask(), out), i64(s.lanes() == 64 ? -1L : (1L << s.lanes()) - 1)));
                Var base = let(out, expr(s.base(), out)), destination = let(out, expr(s.destination(), out)), count = let(out, i64(0));
                out.add(new While(ne(mask, i64(0)), new Block(List.of(
                        new Store(MemoryType.I64, new Offset(destination, binary(Op.MUL, count, i64(8))), add(base, new TrailingZeros(mask))),
                        new Assign(count, add(count, i64(1))),
                        new Assign(mask, binary(Op.AND, mask, binary(Op.SUB, mask, i64(1))))))));
                yield count;
            }
            case Select s -> select(s.condition(), s.yes(), s.no(), out);
            case Binary b -> {
                if (b.op() == Op.BOTH) yield select(b.left(), b.right(), bool(false), out);
                if (b.op() == Op.EITHER) yield select(b.left(), bool(true), b.right(), out);
                Expr left = expr(b.left(), out);
                if (contains(b.right())) left = let(out, left);
                yield new Binary(b.op(), left, expr(b.right(), out));
            }
            case Offset o -> {
                Expr address = expr(o.address(), out);
                if (contains(o.bytes())) address = let(out, address);
                yield new Offset(address, expr(o.bytes(), out));
            }
            case Load l -> new Load(l.width(), expr(l.address(), out));
            case AtomicLoad l -> new AtomicLoad(expr(l.address(), out), l.acquire());
            case AtomicAdd a -> new AtomicAdd(expr(a.address(), out), expr(a.value(), out));
            case CompareExchange c -> new CompareExchange(expr(c.address(), out), expr(c.expected(), out), expr(c.value(), out));
            case Allocate a -> new Allocate(expr(a.bytes(), out));
            case ToDouble c -> new ToDouble(expr(c.value(), out));
            case PopulationCount z -> {
                Var value = let(out, expr(z.value(), out));
                out.add(new Assign(value, binary(Op.SUB, value, binary(Op.AND, binary(Op.USHR, value, i64(1)), i64(0x5555555555555555L)))));
                out.add(new Assign(value, add(binary(Op.AND, value, i64(0x3333333333333333L)), binary(Op.AND, binary(Op.USHR, value, i64(2)), i64(0x3333333333333333L)))));
                out.add(new Assign(value, binary(Op.AND, add(value, binary(Op.USHR, value, i64(4))), i64(0x0f0f0f0f0f0f0f0fL))));
                yield binary(Op.USHR, binary(Op.MUL, value, i64(0x0101010101010101L)), i64(56));
            }
            case TrailingZeros z -> new TrailingZeros(expr(z.value(), out));
            default -> e;
        };
    }
    private Expr select(Expr test, Expr yes, Expr no, List<Statement> out) {
        Var condition = let(out, expr(test, out));
        Var result = let(out, new Literal(0, yes.type()));
        List<Statement> y = new ArrayList<>(), n = new ArrayList<>();
        y.add(new Assign(result, expr(yes, y))); n.add(new Assign(result, expr(no, n)));
        out.add(new If(condition, new Block(y))); out.add(new If(eq(condition, bool(false)), new Block(n)));
        return result;
    }
    private static boolean contains(Expr e) {
        return switch (e) {
            case VectorMask ignored -> true;
            case SelectRows ignored -> true;
            case Binary b -> contains(b.left()) || contains(b.right());
            case Select s -> contains(s.condition()) || contains(s.yes()) || contains(s.no());
            case Offset o -> contains(o.address()) || contains(o.bytes());
            case Load l -> contains(l.address());
            case AtomicLoad l -> contains(l.address());
            case AtomicAdd a -> contains(a.address()) || contains(a.value());
            case CompareExchange c -> contains(c.address()) || contains(c.expected()) || contains(c.value());
            case Allocate a -> contains(a.bytes());
            case ToDouble c -> contains(c.value());
            case PopulationCount ignored -> true;
            case TrailingZeros z -> contains(z.value());
            default -> false;
        };
    }
}
