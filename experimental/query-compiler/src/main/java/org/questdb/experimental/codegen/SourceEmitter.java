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

import static org.questdb.experimental.codegen.Ir.*;

/** Structured-control-flow traversal for C source and diagnostic IR dumps. */
abstract class SourceEmitter {
    final String body(Program program) {
        StringBuilder out = new StringBuilder(); block(program.body(), out, 1); return out.toString();
    }
    private void block(Block block, StringBuilder out, int indent) {
        for (Statement s : block.statements()) {
            out.append("    ".repeat(indent));
            switch (s) {
                case Define d -> out.append(type(d.variable().type())).append(' ').append(d.variable().name()).append(" = ").append(expression(d.value())).append(";\n");
                case Assign a -> out.append(a.variable().name()).append(" = ").append(expression(a.value())).append(";\n");
                case Store w -> out.append(store(w.width(), expression(w.address()), expression(w.value()))).append(";\n");
                case AtomicStore atomic -> out.append(atomicStore(expression(atomic.address()), expression(atomic.value()))).append(";\n");
                case Prefetch p -> out.append(prefetch(expression(p.address()), p.write())).append(";\n");
                case Free f -> out.append(free(expression(f.address()), expression(f.bytes()))).append(";\n");
                case Poll ignored -> out.append("if (").append(poll()).append(" != 0) { return 4; }\n");
                case Call ignored -> throw new IllegalArgumentException("Expand helpers before structured lowering");
                case Return r -> out.append("return ").append(expression(r.value())).append(";\n");
                case If i -> nested("if", i.condition(), i.body(), out, indent);
                case While w -> nested("while", w.condition(), w.body(), out, indent);
            }
        }
    }
    private void nested(String keyword, Expr condition, Block body, StringBuilder out, int indent) {
        out.append(keyword).append(" (").append(expression(condition)).append(") {\n");
        block(body, out, indent + 1); out.append("    ".repeat(indent)).append("}\n");
    }
    static String dump(Program program) {
        return "kernel(v0: ADDRESS columns, v1: I64 rows, v2: ADDRESS state) -> I64 status {\n" + new SourceEmitter() {
            String literal(Literal l) { return l.type() + "(" + l.value() + ")"; }
            String load(MemoryType w, String a) { return "load." + w + "(" + a + ")"; }
            String store(MemoryType w, String a, String v) { return "store." + w + "(" + a + ", " + v + ")"; }
            String allocate(String b) { return "allocate(" + b + ")"; }
            String free(String a, String b) { return "free(" + a + ", " + b + ")"; }
            String poll() { return "poll()"; }
            String type(Type t) { return t.name(); }
            String offset(String a, String b) { return "offset(" + a + ", " + b + ")"; }
        }.body(program) + "}\n";
    }

    final String body(SsaProgram graph) {
        var blocks = graph.executableBlocks();
        var variables = new java.util.TreeMap<Integer, Type>();
        for (var b : blocks) for (var statement : b.code) if (statement instanceof Define d)
            variables.put(d.variable().id(), d.variable().type());
        for (var b : blocks) for (var statement : b.code) if (statement instanceof Assign a)
            variables.put(a.variable().id(), a.variable().type());
        StringBuilder out = new StringBuilder();
        variables.forEach((id, kind) -> out.append("    ").append(type(kind)).append(" v").append(id).append(";\n"));
        out.append("    goto b0;\n");
        for (var b : blocks) {
            out.append('b').append(b.id).append(":;\n");
            for (var statement : b.code) {
                out.append("    ");
                switch (statement) {
                    case Define d -> out.append(d.variable().name()).append(" = ").append(expression(d.value()));
                    case Assign a -> out.append(a.variable().name()).append(" = ").append(expression(a.value()));
                    case Store w -> out.append(store(w.width(), expression(w.address()), expression(w.value())));
                    case AtomicStore atomic -> out.append(atomicStore(expression(atomic.address()), expression(atomic.value())));
                    case Prefetch p -> out.append(prefetch(expression(p.address()), p.write()));
                    case Free f -> out.append(free(expression(f.address()), expression(f.bytes())));
                    case Poll ignored -> out.append("if (").append(poll()).append(" != 0) { return 4; }");
                    default -> throw new IllegalArgumentException("Not lowered SSA: " + statement);
                }
                out.append(";\n");
            }
            switch (b.end) {
                case SsaProgram.Jump j -> out.append("    goto b").append(j.target()).append(";\n");
                case SsaProgram.Branch c -> out.append("    if (").append(expression(c.condition()))
                        .append(") goto b").append(c.yes()).append("; else goto b").append(c.no()).append(";\n");
                case SsaProgram.Exit e -> out.append("    return ").append(expression(e.value())).append(";\n");
            }
        }
        return out.toString();
    }

    final String expression(Expr expression) {
        return switch (expression) {
            case Var v -> v.name();
            case Literal l -> literal(l);
            case Binary b -> binary(b.op(), expression(b.left()), expression(b.right()));
            case Load l -> load(l.width(), expression(l.address()));
            case AtomicLoad l -> atomicLoad(expression(l.address()), l.acquire());
            case AtomicAdd a -> atomicAdd(expression(a.address()), expression(a.value()));
            case CompareExchange c -> compareExchange(expression(c.address()), expression(c.expected()), expression(c.value()));
            case Allocate a -> allocate(expression(a.bytes()));
            case ToDouble c -> "((double)(" + expression(c.value()) + "))";
            case SelectRows s -> "selectRows[" + s.lanes() + "](" + expression(s.mask()) + ", " + expression(s.base()) + ", " + expression(s.destination()) + ")";
            case VectorMask m -> "mask[" + m.lanes() + "](" + expression(m.predicate()) + ")";
            case PopulationCount z -> "__builtin_popcountll((uint64_t)(" + expression(z.value()) + "))";
            case TrailingZeros z -> trailingZeros(expression(z.value()));
            case Offset o -> offset(expression(o.address()), expression(o.bytes()));
            case Select s -> "(" + expression(s.condition()) + " ? " + expression(s.yes()) + " : " + expression(s.no()) + ")";
        };
    }

    String prefetch(String address, boolean write) { return "prefetch(" + address + ", " + write + ")"; }

    String binary(Op op, String left, String right) {
        String operator = switch (op) {
            case ADD, FADD -> "+";
            case SUB, FSUB -> "-";
            case MUL, FMUL -> "*";
            case FDIV, SDIV -> "/";
            case AND -> "&";
            case XOR -> "^";
            case USHR -> ">>>";
            case EQ -> "==";
            case NE -> "!=";
            case LT -> "<";
            case GE -> ">=";
            case BOTH -> "&&";
            case EITHER -> "||";
        };
        return "(" + left + " " + operator + " " + right + ")";
    }

    String trailingZeros(String value) { return "trailingZeros(" + value + ")"; }

    String atomicLoad(String address, boolean acquire) { return "atomicLoad(" + address + ", " + acquire + ")"; }
    String atomicStore(String address, String value) { return "atomicStoreRelease(" + address + ", " + value + ")"; }
    String atomicAdd(String address, String value) { return "atomicAdd(" + address + ", " + value + ")"; }
    String compareExchange(String address, String expected, String value) { return "compareExchange(" + address + ", " + expected + ", " + value + ")"; }
    abstract String literal(Literal literal);
    abstract String load(MemoryType width, String address);
    abstract String store(MemoryType width, String address, String value);
    abstract String allocate(String bytes);
    abstract String free(String address, String bytes);
    abstract String poll();
    abstract String type(Type type);

    String offset(String address, String bytes) { return "(" + address + " + " + bytes + ")"; }

}
