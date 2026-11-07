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

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Consumer;
import static org.questdb.experimental.codegen.Ir.*;

/** Legalizes a typed, read-only lane region to LLVM vectors, independent of SQL. */
final class LlvmVectorEmitter {
    private final int lanes;
    private final Function<String, String> temp;
    private final Consumer<String> declaration;
    private final Map<Expr, String> values = new HashMap<>();
    private final Map<Expr, String> invariants = new HashMap<>();

    LlvmVectorEmitter(int lanes, Function<String, String> temp, Consumer<String> declaration) {
        this.lanes = lanes; this.temp = temp; this.declaration = declaration;
    }

    String mask(Expr predicate) {
        String bits = temp.apply("bitcast " + vector(Type.BOOL) + " " + value(predicate) + " to i" + lanes);
        return lanes == 64 ? bits : temp.apply("zext i" + lanes + " " + bits + " to i64");
    }
    private String vector(Type type) { return "<" + lanes + " x " + scalarType(type) + ">"; }
    private static String scalarType(Type type) { return type == Type.BOOL ? "i1" : type == Type.F64 ? "double" : "i64"; }
    private String value(Expr e) {
        String old = values.get(e); if (old != null) return old;
        String result = switch (e) {
            case Var ignored -> splat(e);
            case Literal ignored -> splat(e);
            case Load l -> {
                String pointer = temp.apply("inttoptr i64 " + invariant(l.address()) + " to ptr");
                String memoryType = "<" + lanes + " x " + LlvmEmitter.memoryType(l.width()) + ">";
                String loaded = temp.apply("load " + memoryType + ", ptr " + pointer + ", align 1");
                yield l.width().bytes == 8 ? loaded : temp.apply((l.width() == MemoryType.F32 ? "fpext " : "sext ")
                        + memoryType + " " + loaded + " to " + vector(l.type()));
            }
            case Gather g -> {
                String addresses = temp.apply("add " + vector(Type.I64) + " " + splat(g.base()) + ", " + value(g.bytes()));
                String pointers = temp.apply("inttoptr " + vector(Type.I64) + " " + addresses + " to <" + lanes + " x ptr>");
                String memoryType = "<" + lanes + " x " + LlvmEmitter.memoryType(g.width()) + ">";
                String intrinsic = "@llvm.masked.gather.v" + lanes
                        + (g.width().resultType == Type.F64 ? "f" : "i") + g.width().bytes * 8 + ".v" + lanes + "p0";
                declaration.accept("declare " + memoryType + " " + intrinsic + "(<" + lanes + " x ptr>, i32 immarg, "
                        + vector(Type.BOOL) + ", " + memoryType + ")");
                String loaded = temp.apply("call " + memoryType + " " + intrinsic + "(<" + lanes + " x ptr> "
                        + pointers + ", i32 1, " + vector(Type.BOOL) + " " + value(bool(true)) + ", " + memoryType + " poison)");
                yield g.width().bytes == 8 ? loaded : temp.apply((g.width() == MemoryType.F32 ? "fpext " : "sext ")
                        + memoryType + " " + loaded + " to " + vector(g.type()));
            }
            case Binary b -> {
                if (b.op() == Op.SDIV && Long.bitCount(((Literal) b.right()).value()) != 1)
                    yield value(divide(b.left(), ((Literal) b.right()).value()));
                String left = value(b.left()), right = value(b.right());
                if (b.op() == Op.USHR) right = temp.apply("and " + vector(Type.I64) + " " + right + ", " + value(i64(63)));
                yield temp.apply(op(b.op()) + " " + vector(b.left().type()) + " " + left + ", " + right);
            }
            case Select s -> temp.apply("select " + vector(Type.BOOL) + " " + value(s.condition()) + ", "
                    + vector(s.type()) + " " + value(s.yes()) + ", " + vector(s.type()) + " " + value(s.no()));
            case ToDouble c -> temp.apply("sitofp " + vector(Type.I64) + " " + value(c.value()) + " to " + vector(Type.F64));
            case BitCast c -> temp.apply("bitcast " + vector(c.value().type()) + " " + value(c.value()) + " to " + vector(c.type()));
            default -> throw new IllegalArgumentException("Not a lane expression: " + e);
        };
        values.put(e, result); return result;
    }

    /** Avoid scalar lane extraction when the target has no vector 64-bit high multiply. */
    private static Expr divide(Expr numerator, long divisor) {
        // A sign-extended INT load proves |n| <= 2^31, without profiling or a guard.
        // Expressions over INT columns may exceed that range and retain the wide path.
        int bits = numerator instanceof Load load && load.width() == MemoryType.I32 ? 32 : 64;
        if (bits == 32 && divisor > (1L << 31)) return i64(0);
        int shift = 64 - Long.numberOfLeadingZeros(divisor - 1);
        // M = ceil(2^(bits+shift)/d) has bits+1 bits. For |n| <= 2^(bits-1), adding |n|
        // to high(|n| * lowBits(M)) cannot overflow, including the signed minimum.
        // The reciprocal's rounding error is < 1/2^(shift+1) < 1/d, so truncating
        // the scaled product gives the exact quotient. Restore the sign last.
        long multiplier = BigInteger.ONE.shiftLeft(bits + shift).add(BigInteger.valueOf(divisor - 1))
                .divide(BigInteger.valueOf(divisor)).longValue();
        Expr negative = binary(Op.LT, numerator, i64(0));
        Expr magnitude = new Select(negative, binary(Op.SUB, i64(0), numerator), numerator);
        Expr low = binary(Op.AND, magnitude, i64(0xffff_ffffL));
        Expr productHigh;
        if (bits == 32) {
            productHigh = binary(Op.USHR, binary(Op.MUL, low, i64(multiplier & 0xffff_ffffL)), i64(32));
        } else {
            Expr high = binary(Op.USHR, magnitude, i64(32));
            Expr lowMultiplier = i64(multiplier & 0xffff_ffffL), highMultiplier = i64(multiplier >>> 32);
            Expr middle = add(binary(Op.MUL, high, lowMultiplier),
                    binary(Op.USHR, binary(Op.MUL, low, lowMultiplier), i64(32)));
            Expr carry = add(binary(Op.MUL, low, highMultiplier), binary(Op.AND, middle, i64(0xffff_ffffL)));
            productHigh = add(binary(Op.MUL, high, highMultiplier),
                    add(binary(Op.USHR, middle, i64(32)), binary(Op.USHR, carry, i64(32))));
        }
        Expr quotient = binary(Op.USHR, add(magnitude, productHigh), i64(shift));
        return new Select(negative, binary(Op.SUB, i64(0), quotient), quotient);
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
            case Literal l -> l.type() == Type.BOOL ? Boolean.toString(l.value() != 0)
                    : l.type() == Type.F64 ? "0x" + String.format("%016X", l.value()) : Long.toString(l.value());
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
            case FADD -> "fadd"; case FSUB -> "fsub"; case FMUL -> "fmul"; case FDIV -> "fdiv";
            case FEQ -> "fcmp oeq"; case FNE -> "fcmp une"; case FLT -> "fcmp olt"; case FGE -> "fcmp oge";
            case ADD -> "add"; case SUB -> "sub"; case MUL -> "mul"; case SDIV -> "sdiv";
            case AND, BOTH -> "and"; case XOR -> "xor"; case EITHER -> "or"; case USHR -> "lshr";
            case EQ -> "icmp eq"; case NE -> "icmp ne"; case LT -> "icmp slt"; case GE -> "icmp sge";
        };
    }
}
