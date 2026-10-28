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
import java.util.stream.Collectors;
import static org.questdb.experimental.codegen.Ir.*;

/** SSA -> LLVM text. Addresses retain wrapping i64 arithmetic; dereferences use opaque pointers. */
final class LlvmEmitter {
    private final StringBuilder out = new StringBuilder();
    private final Map<Integer, String> exits = new HashMap<>();
    private int temporary;

    static String emit(SsaProgram graph) {
        StringBuilder source = new StringBuilder(new LlvmEmitter().module(graph, "query", false));
        Map<String, Helper> helpers = new java.util.LinkedHashMap<>();
        collectHelpers(graph, helpers);
        for (Helper helper : helpers.values()) {
            SsaProgram body = SsaProgram.lower(helper);
            SsaOptimizer.verify(body);
            source.append(new LlvmEmitter().module(body, helper.name(), true));
        }
        return source.toString();
    }

    private static void collectHelpers(SsaProgram graph, Map<String, Helper> helpers) {
        for (var block : graph.blocks) for (Statement s : block.code) if (s instanceof Call c) {
            Helper existing = helpers.putIfAbsent(c.helper().name(), c.helper());
            if (existing == null) collectHelpers(SsaProgram.lower(c.helper()), helpers);
            else if (!existing.equals(c.helper())) throw new IllegalArgumentException("Conflicting helper definitions: " + existing.name());
        }
    }

    private String module(SsaProgram graph, String name, boolean helper) {
        boolean hasPoll = false;
        for (var b : graph.blocks) {
            long polls = b.code.stream().filter(s -> s instanceof Poll || s instanceof Call).count();
            exits.put(b.id, polls == 0 ? "b" + b.id : "b" + b.id + "p" + polls);
            hasPoll |= polls != 0;
        }
        if (!helper) {
            out.append("declare i64 @llvm.cttz.i64(i64, i1)\n\n");
            out.append("declare i64 @llvm.ctpop.i64(i64)\n");
            out.append("declare void @llvm.masked.compressstore.v8i64(<8 x i64>, ptr, <8 x i1>)\n\n");
            out.append("declare void @llvm.prefetch.p0(ptr, i32 immarg, i32 immarg, i32 immarg)\n\n");
        }
        out.append("define ").append(helper ? "internal " : "").append("i64 @").append(name).append("(");
        out.append(graph.parameters.stream().map(p -> type(p.type()) + " " + operand(p)).collect(Collectors.joining(", ")));
        if (!graph.parameters.isEmpty()) out.append(", ");
        out.append("i64 %alloc_arg, i64 %free_arg, i64 %poll_arg, i64 %realloc_arg)").append(helper ? " noinline" : "").append(" {\n");
        for (var b : graph.blocks) {
            out.append('b').append(b.id).append(":\n");
            if (b.id == 0) {
                line("%alloc_fn = inttoptr i64 %alloc_arg to ptr");
                line("%free_fn = inttoptr i64 %free_arg to ptr");
                line("%poll_fn = inttoptr i64 %poll_arg to ptr");
                line("%realloc_fn = inttoptr i64 %realloc_arg to ptr");
            }
            for (var p : b.phis) {
                String incoming = p.inputs.entrySet().stream().map(e -> "[ " + operand(e.getValue()) + ", %" + exits.get(e.getKey()) + " ]")
                        .collect(Collectors.joining(", "));
                line(operand(p.result) + " = phi " + type(p.result.type()) + " " + incoming);
            }
            int polls = 0;
            for (var statement : b.code) switch (statement) {
                case Define d -> define(operand(d.variable()), d.value());
                case Store s -> store(s);
                case AtomicStore atomic -> line("store atomic i64 " + operand(atomic.value()) + ", ptr " + temp("inttoptr i64 " + operand(atomic.address()) + " to ptr") + " release, align 8");
                case Prefetch p -> line("call void @llvm.prefetch.p0(ptr "
                        + temp("inttoptr i64 " + operand(p.address()) + " to ptr")
                        + ", i32 " + (p.write() ? 1 : 0) + ", i32 3, i32 1)");
                case Free f -> line("call void %free_fn(i64 " + operand(f.address()) + ", i64 " + operand(f.bytes()) + ")");
                case Call call -> {
                    String args = call.arguments().stream().map(a -> type(a.type()) + " " + operand(a)).collect(Collectors.joining(", "));
                    if (!args.isEmpty()) args += ", ";
                    String status = temp("call i64 @" + call.helper().name() + "(" + args + "i64 %alloc_arg, i64 %free_arg, i64 %poll_arg, i64 %realloc_arg)");
                    String failed = temp("icmp ne i64 " + status + ", 0");
                    String continuation = "b" + b.id + "p" + ++polls;
                    line("br i1 " + failed + ", label %" + continuation + "failed, label %" + continuation);
                    out.append(continuation).append("failed:\n");
                    line("ret i64 " + status);
                    out.append(continuation).append(":\n");
                }
                case Poll ignored -> {
                    String status = temp("call i64 %poll_fn()"), failed = temp("icmp ne i64 " + status + ", 0");
                    String continuation = "b" + b.id + "p" + ++polls;
                    line("br i1 " + failed + ", label %poll_failed, label %" + continuation);
                    out.append(continuation).append(":\n");
                }
                default -> throw new IllegalArgumentException("Unexpected SSA statement: " + statement);
            };
            switch (b.end) {
                case SsaProgram.Jump j -> line("br label %b" + j.target());
                case SsaProgram.Branch c -> line("br i1 " + operand(c.condition()) + ", label %b" + c.yes() + ", label %b" + c.no());
                case SsaProgram.Exit e -> line("ret i64 " + operand(e.value()));
            }
        }
        if (hasPoll) out.append("poll_failed:\n  ret i64 4\n");
        return out.append("}\n").toString();
    }
    private void define(String result, Expr expression) {
        switch (expression) {
            case Binary b -> {
                String left = operand(b.left()), right = operand(b.right());
                if (b.op() == Op.USHR) right = temp("and i64 " + right + ", 63");
                String op = switch (b.op()) {
                    case ADD -> "add"; case SUB -> "sub"; case MUL -> "mul";
                    case FADD -> "fadd"; case FSUB -> "fsub"; case FMUL -> "fmul"; case FDIV -> "fdiv"; case SDIV -> "sdiv";
                    case AND, BOTH -> "and"; case XOR -> "xor"; case EITHER -> "or"; case USHR -> "lshr";
                    case EQ -> "icmp eq"; case NE -> "icmp ne"; case LT -> "icmp slt"; case GE -> "icmp sge";
                };
                line(result + " = " + op + " " + type(b.left().type()) + " " + left + ", " + right);
            }
            case Offset o -> line(result + " = add i64 " + operand(o.address()) + ", " + operand(o.bytes()));
            case Load l -> {
                String pointer = temp("inttoptr i64 " + operand(l.address()) + " to ptr");
                String memoryType = l.width() == MemoryType.F64 ? "double" : "i" + l.width().bytes * 8;
                String load = "load " + memoryType + ", ptr " + pointer + ", align 1";
                if (l.width().bytes < 8) line(result + " = sext " + memoryType + " " + temp(load) + " to i64");
                else line(result + " = " + load);
            }
            case AtomicLoad l -> {
                String pointer = temp("inttoptr i64 " + operand(l.address()) + " to ptr");
                line(result + " = load atomic i64, ptr " + pointer + (l.acquire() ? " acquire" : " monotonic") + ", align 8");
            }
            case AtomicAdd a -> line(result + " = atomicrmw add ptr " + temp("inttoptr i64 " + operand(a.address()) + " to ptr") + ", i64 " + operand(a.value()) + " monotonic, align 8");
            case CompareExchange c -> {
                String pointer = temp("inttoptr i64 " + operand(c.address()) + " to ptr");
                String pair = temp("cmpxchg ptr " + pointer + ", i64 " + operand(c.expected()) + ", i64 " + operand(c.value()) + " monotonic monotonic, align 8");
                line(result + " = extractvalue { i64, i1 } " + pair + ", 0");
            }
            case VectorMask m -> line(result + " = or i64 " + new LlvmVectorEmitter(m.lanes(), this::temp).mask(m.predicate()) + ", 0");
            case PopulationCount z -> line(result + " = call i64 @llvm.ctpop.i64(i64 " + operand(z.value()) + ")");
            case TrailingZeros z -> line(result + " = call i64 @llvm.cttz.i64(i64 " + operand(z.value()) + ", i1 false)");
            case SelectRows s -> selectRows(result, s);
            case ToDouble c -> line(result + " = sitofp i64 " + operand(c.value()) + " to double");
            case Allocate a -> line(result + " = call i64 %alloc_fn(i64 " + operand(a.bytes()) + ")");
            case Reallocate r -> line(result + " = call i64 %realloc_fn(i64 " + operand(r.address()) + ", i64 " + operand(r.oldBytes()) + ", i64 " + operand(r.newBytes()) + ")");
            default -> throw new IllegalArgumentException("Unexpected SSA expression: " + expression);
        }
    }
    private void selectRows(String result, SelectRows selection) {
        String base = temp("insertelement <8 x i64> poison, i64 " + operand(selection.base()) + ", i64 0");
        base = temp("shufflevector <8 x i64> " + base + ", <8 x i64> poison, <8 x i32> zeroinitializer");
        String count = "0";
        for (int offset = 0; offset < selection.lanes(); offset += 8) {
            int lanes = Math.min(8, selection.lanes() - offset);
            String bits = offset == 0 ? operand(selection.mask()) : temp("lshr i64 " + operand(selection.mask()) + ", " + offset);
            bits = temp("and i64 " + bits + ", " + ((1 << lanes) - 1));
            String mask = temp("bitcast i8 " + temp("trunc i64 " + bits + " to i8") + " to <8 x i1>");
            var indexes = new StringBuilder("<");
            for (int i = 0; i < 8; i++) { if (i > 0) indexes.append(", "); indexes.append("i64 ").append(offset + i); }
            String rows = temp("add <8 x i64> " + base + ", " + indexes.append('>'));
            String address = temp("add i64 " + operand(selection.destination()) + ", " + temp("mul i64 " + count + ", 8"));
            String pointer = temp("inttoptr i64 " + address + " to ptr");
            line("call void @llvm.masked.compressstore.v8i64(<8 x i64> " + rows + ", ptr " + pointer + ", <8 x i1> " + mask + ")");
            count = temp("add i64 " + count + ", " + temp("call i64 @llvm.ctpop.i64(i64 " + bits + ")"));
        }
        line(result + " = add i64 " + count + ", 0");
    }
    private void store(Store s) {
        String pointer = temp("inttoptr i64 " + operand(s.address()) + " to ptr");
        String memoryType = s.width() == MemoryType.F64 ? "double" : "i" + s.width().bytes * 8, value = operand(s.value());
        if (s.width().bytes < 8) value = temp("trunc i64 " + value + " to " + memoryType);
        line("store " + memoryType + " " + value + ", ptr " + pointer + ", align 1");
    }
    private String temp(String expression) { String name = "%t" + temporary++; line(name + " = " + expression); return name; }
    private void line(String text) { out.append("  ").append(text).append('\n'); }
    private static String type(Type type) { return type == Type.F64 ? "double" : type == Type.BOOL ? "i1" : "i64"; }
    private static String operand(Expr e) {
        return switch (e) {
            case Var v -> "%" + v.name();
            case Literal l -> l.type() == Type.F64 ? "0x" + String.format("%016X", l.value()) : l.type() == Type.BOOL ? Boolean.toString(l.value() != 0) : Long.toString(l.value());
            default -> throw new IllegalArgumentException("SSA operand must be a value: " + e);
        };
    }
}
