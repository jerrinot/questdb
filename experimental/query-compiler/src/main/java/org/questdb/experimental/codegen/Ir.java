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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A structured, typed, mutable-register IR. Memory widths are explicit; integer
 * operations have Java long wraparound semantics. This is trusted compiler IR,
 * not a memory-safe language for executing arbitrary user-supplied programs.
 */
public final class Ir {
    public static final Var COLUMNS = new Var(0, Type.ADDRESS);
    public static final Var ROWS = new Var(1, Type.I64);
    public static final Var STATE = new Var(2, Type.ADDRESS);

    private Ir() {
    }

    public enum Type { I64, BOOL, ADDRESS, F64 }

    public enum MemoryType {
        I8(1, Type.I64), I16(2, Type.I64), I32(4, Type.I64), I64(8, Type.I64), F64(8, Type.F64), ADDRESS(8, Type.ADDRESS);

        public final int bytes;
        public final Type resultType;

        MemoryType(int bytes, Type resultType) {
            this.bytes = bytes;
            this.resultType = resultType;
        }
    }

    public enum Op { ADD, SUB, MUL, AND, XOR, USHR, EQ, NE, LT, GE, BOTH, EITHER, FADD, FSUB, FMUL, FDIV, SDIV }

    public sealed interface Expr permits Var, Literal, Binary, Load, Offset, Select, Allocate, Reallocate, VectorMask, TrailingZeros, PopulationCount, SelectRows, ToDouble, AtomicLoad, CompareExchange, AtomicAdd {
        Type type();
    }

    /**
     * Bit j is the predicate for lane j. Every integer load reads its address plus
     * j * width. Other values are invariant. The caller must establish that ALL
     * lanes of EVERY load are readable, including both arms of a select.
     * This is a read effect, not pure scalar arithmetic.
     */
    public record VectorMask(int lanes, Expr predicate) implements Expr {
        public VectorMask {
            if (lanes < 1 || lanes > 64) throw new IllegalArgumentException("Mask requires 1..64 lanes");
            require(predicate, Type.BOOL);
            LaneExpressions.verify(predicate);
        }
        @Override public Type type() { return Type.I64; }
    }

    /** Defined for zero: trailingZeros(0) == 64 on every backend. */
    /** Write base+lane for each set low mask bit, in order; return the number written.
     * Destination must have room for lanes longs. This is a memory WRITE effect.
     */
    public record SelectRows(int lanes, Expr mask, Expr base, Expr destination) implements Expr {
        public SelectRows {
            if (lanes < 1 || lanes > 64) throw new IllegalArgumentException("Selection requires 1..64 lanes");
            require(mask, Type.I64); require(base, Type.I64); require(destination, Type.ADDRESS);
        }
        @Override public Type type() { return Type.I64; }
    }

    /** Signed integer to IEEE double, with Java's round-to-nearest conversion. */
    public record ToDouble(Expr value) implements Expr {
        public ToDouble { require(value, Type.I64); }
        @Override public Type type() { return Type.F64; }
    }

    public record TrailingZeros(Expr value) implements Expr {
        public TrailingZeros { require(value, Type.I64); }
        @Override public Type type() { return Type.I64; }
    }

    public record PopulationCount(Expr value) implements Expr {
        public PopulationCount { require(value, Type.I64); }
        @Override public Type type() { return Type.I64; }
    }

    public record Var(int id, Type type) implements Expr {
        public Var {
            if (id < 0 || type == null) {
                throw new IllegalArgumentException("Invalid variable");
            }
        }

        public String name() {
            return "v" + id;
        }
    }

    public record Literal(long value, Type type) implements Expr {
        public Literal {
            if (type == null || (type == Type.BOOL && value != 0 && value != 1)
                    || (type == Type.ADDRESS && value != 0)) {
                throw new IllegalArgumentException("Invalid typed literal");
            }
        }
    }

    public record Binary(Op op, Expr left, Expr right) implements Expr {
        public Binary {
            require(right, left.type());
            // Positive compile-time divisors exclude division by zero and MIN/-1.
            // This is an integer primitive, not SQL's nullable division operator.
            if (op == Op.SDIV && (!(right instanceof Literal divisor) || divisor.value() <= 0)) {
                throw new IllegalArgumentException("Signed division requires a positive literal divisor");
            }
            switch (op) {
                case EQ, NE -> {
                    if (left.type() == Type.F64) throw new IllegalArgumentException("Floating comparisons are not bound yet");
                }
                case FADD, FSUB, FMUL, FDIV -> require(left, Type.F64);
                case BOTH, EITHER -> require(left, Type.BOOL);
                default -> require(left, Type.I64);
            }
        }

        @Override
        public Type type() {
            return switch (op) {
                case EQ, NE, LT, GE, BOTH, EITHER -> Type.BOOL;
                case FADD, FSUB, FMUL, FDIV -> Type.F64;
                default -> Type.I64;
            };
        }
    }

    public record Load(MemoryType width, Expr address) implements Expr {
        public Load {
            require(address, Type.ADDRESS);
        }

        @Override
        public Type type() {
            return width.resultType;
        }
    }

    /** Naturally aligned 64-bit atomic load; acquire observes release-published payloads. */
    public record AtomicLoad(Expr address, boolean acquire) implements Expr {
        public AtomicLoad(Expr address) { this(address, false); }
        public AtomicLoad { require(address, Type.ADDRESS); }
        @Override public Type type() { return Type.I64; }
    }

    /** Naturally aligned 64-bit relaxed addition, returning the previous value. */
    public record AtomicAdd(Expr address, Expr value) implements Expr {
        public AtomicAdd { require(address, Type.ADDRESS); require(value, Type.I64); }
        @Override public Type type() { return Type.I64; }
    }

    /** Strong relaxed compare-exchange; returns the observed old value on success or failure. */
    public record CompareExchange(Expr address, Expr expected, Expr value) implements Expr {
        public CompareExchange {
            require(address, Type.ADDRESS); require(expected, Type.I64); require(value, Type.I64);
        }
        @Override public Type type() { return Type.I64; }
    }

    public record Offset(Expr address, Expr bytes) implements Expr {
        public Offset {
            require(address, Type.ADDRESS);
            require(bytes, Type.I64);
        }

        @Override
        public Type type() {
            return Type.ADDRESS;
        }
    }

    /** Only the selected arm is evaluated, including its memory loads. */
    public record Select(Expr condition, Expr yes, Expr no) implements Expr {
        public Select {
            require(condition, Type.BOOL);
            require(no, yes.type());
        }

        @Override
        public Type type() {
            return yes.type();
        }
    }

    /** Runtime allocation; emit into a local and check for nil before accessing it. */
    public record Allocate(Expr bytes) implements Expr {
        public Allocate { require(bytes, Type.I64); }
        @Override
        public Type type() { return Type.ADDRESS; }
    }

    /** Preserves the old allocation on failure; check for nil before publishing the result. */
    public record Reallocate(Expr address, Expr oldBytes, Expr newBytes) implements Expr {
        public Reallocate { require(address, Type.ADDRESS); require(oldBytes, Type.I64); require(newBytes, Type.I64); }
        @Override
        public Type type() { return Type.ADDRESS; }
    }

    public sealed interface Statement permits Define, Assign, Store, AtomicStore, Prefetch, Free, Poll, Call, If, While, Return { }

    /** Naturally aligned 64-bit release store, publishing preceding payload writes. */
    public record AtomicStore(Expr address, Expr value) implements Statement {
        public AtomicStore { require(address, Type.ADDRESS); require(value, Type.I64); }
    }

    /** Non-faulting cache hint; evaluating the address must still be safe. */
    public record Prefetch(Expr address, boolean write) implements Statement {
        public Prefetch { require(address, Type.ADDRESS); }
    }

    /** Cooperative cancellation and deferred runtime failures. */
    public record Poll() implements Statement { }

    /** A typed, memory-effecting helper. Zero completes it; nonzero exits fail the enclosing kernel.
     * Success must be its final statement, so backends may expand it without changing control flow.
     */
    public record Helper(String name, List<Var> parameters, Block body) {
        public Helper {
            if (name == null || !name.matches("[A-Za-z_][A-Za-z_0-9]*") || name.equals("query")) {
                throw new IllegalArgumentException("Invalid helper name");
            }
            parameters = List.copyOf(parameters);
            Set<Integer> ids = new HashSet<>();
            for (Var p : parameters) if (!ids.add(p.id())) throw new IllegalArgumentException("Duplicate helper parameter");
            verify(body, new HashSet<>(parameters), ids);
            var statements = body.statements();
            if (statements.isEmpty() || !(statements.getLast() instanceof Return r) || !r.value().equals(i64(0))) {
                throw new IllegalArgumentException("Helper must finish with success");
            }
            verifyHelperExits(new Block(statements.subList(0, statements.size() - 1)));
        }
    }

    /** Arguments are evaluated once, in order. Calls may read/write memory and poll cancellation. */
    public record Call(Helper helper, List<Expr> arguments) implements Statement {
        public Call {
            arguments = List.copyOf(arguments);
            if (arguments.size() != helper.parameters().size()) throw new IllegalArgumentException("Helper argument count");
            for (int i = 0; i < arguments.size(); i++) require(arguments.get(i), helper.parameters().get(i).type());
        }
    }

    public record Define(Var variable, Expr value) implements Statement {
        public Define { require(value, variable.type()); }
    }

    public record Assign(Var variable, Expr value) implements Statement {
        public Assign { require(value, variable.type()); }
    }

    public record Store(MemoryType width, Expr address, Expr value) implements Statement {
        public Store(Expr address, Expr value) { this(MemoryType.I64, address, value); }
        public Store {
            require(address, Type.ADDRESS);
            require(value, width.resultType);
        }
    }

    public record Free(Expr address, Expr bytes) implements Statement {
        public Free { require(address, Type.ADDRESS); require(bytes, Type.I64); }
    }

    public record If(Expr condition, Block body) implements Statement {
        public If { require(condition, Type.BOOL); }
    }

    public record While(Expr condition, Block body) implements Statement {
        public While { require(condition, Type.BOOL); }
    }

    public record Return(Expr value) implements Statement {
        public Return { require(value, Type.I64); }
    }

    public record Block(List<Statement> statements) {
        public Block { statements = List.copyOf(statements); }
    }

    public record Program(Block body) {
        public Program {
            Set<Var> parameters = Set.of(COLUMNS, ROWS, STATE);
            verify(body, new HashSet<>(parameters), new HashSet<>(Set.of(0, 1, 2)));
            if (body.statements().isEmpty() || !(body.statements().getLast() instanceof Return)) {
                throw new IllegalArgumentException("Program must end with a return");
            }
        }
    }

    public static final class Builder {
        private final AtomicInteger ids;
        private final List<Statement> statements = new ArrayList<>();

        public Builder() { this(new AtomicInteger(3)); }
        Builder(List<Var> parameters) { this(new AtomicInteger(parameters.stream().mapToInt(Var::id).max().orElse(-1) + 1)); }

        private Builder(AtomicInteger ids) { this.ids = ids; }

        public void assign(Var variable, Expr value) { statements.add(new Assign(variable, value)); }
        public Block block() { return new Block(statements); }
        // Compiler-time regions share IDs, then are attached in dominance order.
        Builder fork() { return new Builder(ids); }
        void append(Block block) { statements.addAll(block.statements()); }
        public void exit(long status) { statements.add(new Return(i64(status))); }
        public void exit(Expr value) { statements.add(new Return(value)); }

        public void when(Expr condition, Consumer<Builder> body) {
            statements.add(new If(condition, nested(body)));
        }

        public Var let(Expr value) {
            Var variable = new Var(ids.getAndIncrement(), value.type());
            statements.add(new Define(variable, value));
            return variable;
        }

        public void loop(Expr condition, Consumer<Builder> body) {
            statements.add(new While(condition, nested(body)));
        }

        public void store(Expr address, Expr value) { statements.add(new Store(address, value)); }
        public void atomicStore(Expr address, Expr value) { statements.add(new AtomicStore(address, value)); }
        public void store(MemoryType width, Expr address, Expr value) { statements.add(new Store(width, address, value)); }
        public void free(Expr address, Expr bytes) { statements.add(new Free(address, bytes)); }
        public void poll() { statements.add(new Poll()); }
        public void call(Helper helper, List<Expr> arguments) { statements.add(new Call(helper, arguments)); }
        public void prefetch(Expr address, boolean write) {
            if (!Boolean.getBoolean("query.codegen.disablePrefetchHints")) statements.add(new Prefetch(address, write));
        }

        private Block nested(Consumer<Builder> emit) {
            Builder builder = new Builder(ids);
            emit.accept(builder);
            return builder.block();
        }
    }

    public static Expr binary(Op op, Expr left, Expr right) { return new Binary(op, left, right); }
    public static Expr i64(long value) { return new Literal(value, Type.I64); }
    public static Expr f64(double value) { return new Literal(Double.doubleToRawLongBits(value), Type.F64); }
    public static Expr bool(boolean value) { return new Literal(value ? 1 : 0, Type.BOOL); }
    public static Expr nil() { return new Literal(0, Type.ADDRESS); }
    public static Expr add(Expr a, Expr b) { return binary(Op.ADD, a, b); }
    public static Expr eq(Expr a, Expr b) { return binary(Op.EQ, a, b); }
    public static Expr ne(Expr a, Expr b) { return binary(Op.NE, a, b); }
    public static Expr at(Expr address, long bytes) { return new Offset(address, i64(bytes)); }
    public static Expr load(Expr address) { return new Load(MemoryType.I64, address); }

    private static void require(Expr expression, Type type) {
        if (expression.type() != type) {
            throw new IllegalArgumentException("Expected " + type + ", got " + expression.type());
        }
    }

    private static void verify(Block block, Set<Var> scope, Set<Integer> ids) {
        boolean returned = false;
        for (Statement statement : block.statements()) {
            if (returned) {
                throw new IllegalArgumentException("Unreachable statement after return");
            }
            switch (statement) {
                case Define d -> {
                    verify(d.value(), scope);
                    if (!ids.add(d.variable().id())) {
                        throw new IllegalArgumentException("Duplicate variable: " + d.variable());
                    }
                    scope.add(d.variable());
                }
                case Assign a -> {
                    verify(a.variable(), scope);
                    verify(a.value(), scope);
                }
                case Store s -> { verify(s.address(), scope); verify(s.value(), scope); }
                case AtomicStore atomic -> { verify(atomic.address(), scope); verify(atomic.value(), scope); }
                case Prefetch p -> verify(p.address(), scope);
                case Free f -> { verify(f.address(), scope); verify(f.bytes(), scope); }
                case Poll ignored -> { }
                case Call call -> call.arguments().forEach(e -> verify(e, scope));
                case If i -> { verify(i.condition(), scope); verify(i.body(), new HashSet<>(scope), ids); }
                case While w -> { verify(w.condition(), scope); verify(w.body(), new HashSet<>(scope), ids); }
                case Return r -> { verify(r.value(), scope); returned = true; }
            }
        }
    }

    private static void verifyHelperExits(Block block) {
        for (Statement s : block.statements()) {
            if (s instanceof Return r && (!(r.value() instanceof Literal l) || l.value() == 0)) {
                throw new IllegalArgumentException("Helper early exits must be nonzero literal status codes");
            }
            if (s instanceof If i) verifyHelperExits(i.body());
            else if (s instanceof While w) verifyHelperExits(w.body());
        }
    }

    private static void verify(Expr expression, Set<Var> scope) {
        switch (expression) {
            case Var v -> {
                if (!scope.contains(v)) {
                    throw new IllegalArgumentException("Variable is not in scope: " + v);
                }
            }
            case Literal ignored -> { }
            case Binary b -> { verify(b.left(), scope); verify(b.right(), scope); }
            case Load l -> verify(l.address(), scope);
            case AtomicLoad l -> verify(l.address(), scope);
            case AtomicAdd a -> { verify(a.address(), scope); verify(a.value(), scope); }
            case CompareExchange c -> { verify(c.address(), scope); verify(c.expected(), scope); verify(c.value(), scope); }
            case Allocate a -> verify(a.bytes(), scope);
            case Reallocate r -> { verify(r.address(), scope); verify(r.oldBytes(), scope); verify(r.newBytes(), scope); }
            case ToDouble c -> verify(c.value(), scope);
            case VectorMask m -> verify(m.predicate(), scope);
            case PopulationCount z -> verify(z.value(), scope);
            case TrailingZeros z -> verify(z.value(), scope);
            case SelectRows s -> { verify(s.mask(), scope); verify(s.base(), scope); verify(s.destination(), scope); }
            case Offset o -> { verify(o.address(), scope); verify(o.bytes(), scope); }
            case Select s -> { verify(s.condition(), scope); verify(s.yes(), scope); verify(s.no(), scope); }
        }
    }
}
