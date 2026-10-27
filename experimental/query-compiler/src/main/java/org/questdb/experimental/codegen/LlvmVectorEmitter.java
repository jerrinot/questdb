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
import java.util.Map;
import java.util.function.Function;
import static org.questdb.experimental.codegen.Ir.*;

/** Legalizes a typed, read-only lane region to LLVM vectors, independent of SQL. */
final class LlvmVectorEmitter {
    private final int lanes;
    private final Function<String, String> temp;
    private final Map<Expr, String> values = new HashMap<>();
    private final Map<Expr, String> invariants = new HashMap<>();

    LlvmVectorEmitter(int lanes, Function<String, String> temp) { this.lanes = lanes; this.temp = temp; }

    String mask(Expr predicate) {
        String bits = temp.apply("bitcast " + vector(Type.BOOL) + " " + value(predicate) + " to i" + lanes);
        return lanes == 64 ? bits : temp.apply("zext i" + lanes + " " + bits + " to i64");
    }
    private String vector(Type type) { return "<" + lanes + " x " + scalarType(type) + ">"; }
    private static String scalarType(Type type) { return type == Type.BOOL ? "i1" : "i64"; }
    private String value(Expr e) {
        String old = values.get(e); if (old != null) return old;
        String result = switch (e) {
            case Var ignored -> splat(e);
            case Literal ignored -> splat(e);
            case Load l -> {
                String pointer = temp.apply("inttoptr i64 " + invariant(l.address()) + " to ptr");
                String memoryType = "<" + lanes + " x i" + l.width().bytes * 8 + ">";
                String loaded = temp.apply("load " + memoryType + ", ptr " + pointer + ", align 1");
                yield l.width().bytes == 8 ? loaded : temp.apply("sext " + memoryType + " " + loaded + " to " + vector(Type.I64));
            }
            case Binary b -> {
                String left = value(b.left()), right = value(b.right());
                if (b.op() == Op.USHR) right = temp.apply("and " + vector(Type.I64) + " " + right + ", " + value(i64(63)));
                yield temp.apply(op(b.op()) + " " + vector(b.left().type()) + " " + left + ", " + right);
            }
            case Select s -> temp.apply("select " + vector(Type.BOOL) + " " + value(s.condition()) + ", "
                    + vector(s.type()) + " " + value(s.yes()) + ", " + vector(s.type()) + " " + value(s.no()));
            default -> throw new IllegalArgumentException("Not a lane expression: " + e);
        };
        values.put(e, result); return result;
    }
    private String splat(Expr e) {
        String type = vector(e.type());
        String first = temp.apply("insertelement " + type + " poison, " + scalarType(e.type()) + " " + invariant(e) + ", i64 0");
        return temp.apply("shufflevector " + type + " " + first + ", " + type + " poison, <" + lanes + " x i32> zeroinitializer");
    }
    private String invariant(Expr e) {
        String old = invariants.get(e); if (old != null) return old;
        String result = switch (e) {
            case Var v -> "%" + v.name();
            case Literal l -> l.type() == Type.BOOL ? Boolean.toString(l.value() != 0) : Long.toString(l.value());
            case Offset o -> temp.apply("add i64 " + invariant(o.address()) + ", " + invariant(o.bytes()));
            case Binary b -> {
                String left = invariant(b.left()), right = invariant(b.right());
                if (b.op() == Op.USHR) right = temp.apply("and i64 " + right + ", 63");
                yield temp.apply(op(b.op()) + " " + scalarType(b.left().type()) + " " + left + ", " + right);
            }
            case Select s -> temp.apply("select i1 " + invariant(s.condition()) + ", " + scalarType(s.type()) + " "
                    + invariant(s.yes()) + ", " + scalarType(s.type()) + " " + invariant(s.no()));
            default -> throw new IllegalArgumentException("Lane load address must be invariant: " + e);
        };
        invariants.put(e, result); return result;
    }
    private static String op(Op op) {
        return switch (op) {
            case FADD, FSUB, FMUL, FDIV -> throw new IllegalArgumentException("Floating lanes are not bound yet");
            case ADD -> "add"; case SUB -> "sub"; case MUL -> "mul"; case SDIV -> "sdiv";
            case AND, BOTH -> "and"; case XOR -> "xor"; case EITHER -> "or"; case USHR -> "lshr";
            case EQ -> "icmp eq"; case NE -> "icmp ne"; case LT -> "icmp slt"; case GE -> "icmp sge";
        };
    }
}
