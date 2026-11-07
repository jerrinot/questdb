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
import java.util.BitSet;
import java.util.List;
import java.util.IdentityHashMap;
import java.util.Map;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Typed scan/filter/project stage, before row access and execution strategy are chosen. */
record ScanPlan(List<Column> input, Predicate predicate, List<Column> output, List<Scalar> bindings, List<Parameter> parameters) {
    // Bounds expression duplication during projection substitution, independent of data.
    static final int MAX_FUSED_EXPRESSION_NODES = 256;

    ScanPlan(List<Column> input, Predicate predicate, List<Column> output, List<Scalar> bindings) {
        this(input, predicate, output, bindings, List.of());
    }

    ScanPlan {
        input = List.copyOf(input); output = List.copyOf(output); bindings = List.copyOf(bindings);
        parameters = List.copyOf(parameters);
        for (int i = 0; i < parameters.size(); i++)
            if (parameters.get(i).index() != i) throw new IllegalArgumentException("Parameter order mismatch");
        if (output.size() != bindings.size()) throw new IllegalArgumentException("Binding arity mismatch");
        // Validate all references, including currently unused projected expressions.
        var checked = new IdentityHashMap<Scalar, Boolean>();
        validate(predicate, input, parameters, checked);
        for (Scalar expression : bindings) validate(expression, input, parameters, checked);
    }
    static ScanPlan of(List<Column> columns, Predicate filter) {
        return new ScanPlan(columns, filter, columns, new ArrayList<Scalar>(columns));
    }
    static ScanPlan from(PhysicalPlan node) {
        return switch (node) {
            case PhysicalPlan.Scan scan -> of(scan.columns(), new All());
            case PhysicalPlan.Filter filter -> {
                ScanPlan child = from(filter.input());
                if (child == null) yield null;
                Predicate next = rewrite(filter.predicate(), child.output, child.bindings);
                Predicate combined = child.predicate instanceof All ? next : new And(child.predicate, next);
                yield fits(combined, child.bindings) ? new ScanPlan(child.input, combined, child.output, child.bindings, child.parameters) : null;
            }
            case PhysicalPlan.Project project -> {
                ScanPlan child = from(project.input());
                if (child == null) yield null;
                var expressions = project.expressions().stream().map(e -> rewrite(e, child.output, child.bindings)).toList();
                yield fits(child.predicate, expressions) ? new ScanPlan(child.input, child.predicate, project.columns(), expressions, child.parameters) : null;
            }
            default -> null;
        };
    }
    BitSet predicateColumns() {
        BitSet used = new BitSet(); collect(predicate, used); return used;
    }

    private static boolean fits(Predicate predicate, List<Scalar> bindings) {
        var memo = new IdentityHashMap<Scalar, Integer>();
        int count = cost(predicate, memo);
        for (var binding : bindings) {
            count += cost(binding, memo);
            if (count > MAX_FUSED_EXPRESSION_NODES) return false;
        }
        return count <= MAX_FUSED_EXPRESSION_NODES;
    }
    private static int cost(Scalar s, Map<Scalar, Integer> memo) {
        Integer previous = memo.get(s); if (previous != null) return previous;
        int value = s instanceof ToFloating a ? 2 + cost(a.argument(), memo)
                : s instanceof Divide a ? 3 + cost(a.left(), memo) + cost(a.right(), memo)
                : s instanceof Add a ? 1 + cost(a.left(), memo) + cost(a.right(), memo)
                : s instanceof Multiply a ? 1 + cost(a.left(), memo) + cost(a.right(), memo)
                : s instanceof IPv4Subtract a ? 4 + cost(a.left(), memo) + cost(a.right(), memo)
                : s instanceof TimestampPart a ? 12 + cost(a.argument(), memo)
                : s instanceof TimestampYear a ? 48 + cost(a.argument(), memo)
                : s instanceof Conditional a ? 2 + cost(a.condition(), memo) + cost(a.whenTrue(), memo) + cost(a.whenFalse(), memo)
                : s instanceof Subtract a ? 1 + cost(a.left(), memo) + cost(a.right(), memo) : 1;
        value = Math.min(MAX_FUSED_EXPRESSION_NODES + 1, value); memo.put(s, value); return value;
    }
    private static int cost(Predicate p, Map<Scalar, Integer> memo) {
        return switch (p) {
            case All ignored -> 1;
            case Compare c -> 1 + cost(c.left(), memo) + cost(c.right(), memo);
            case SqlCompare c -> 1 + cost(c.left(), memo) + cost(c.right(), memo);
            case FloatingCompare c -> 8 + cost(c.left(), memo) + cost(c.right(), memo);
            case IsNotNull n -> 1 + cost(n.value(), memo);
            case Not n -> 1 + cost(n.value(), memo);
            case Or a -> Math.min(MAX_FUSED_EXPRESSION_NODES + 1, 1 + cost(a.left(), memo) + cost(a.right(), memo));
            case And a -> Math.min(MAX_FUSED_EXPRESSION_NODES + 1, 1 + cost(a.left(), memo) + cost(a.right(), memo));
        };
    }
    private static void validate(Scalar s, List<Column> schema, List<Parameter> parameters, Map<Scalar, Boolean> checked) {
        if (checked.put(s, true) != null) return;
        switch (s) {
            case Column c -> { if (c.index() >= schema.size() || !c.equals(schema.get(c.index()))) throw new IllegalArgumentException("Foreign column: " + c); }
            case Add a -> { validate(a.left(), schema, parameters, checked); validate(a.right(), schema, parameters, checked); }
            case Multiply a -> { validate(a.left(), schema, parameters, checked); validate(a.right(), schema, parameters, checked); }
            case IPv4Subtract a -> { validate(a.left(), schema, parameters, checked); validate(a.right(), schema, parameters, checked); }
            case Subtract a -> { validate(a.left(), schema, parameters, checked); validate(a.right(), schema, parameters, checked); }
            case TimestampPart a -> validate(a.argument(), schema, parameters, checked);
            case TimestampYear a -> validate(a.argument(), schema, parameters, checked);
            case Conditional a -> { validate(a.condition(), schema, parameters, checked); validate(a.whenTrue(), schema, parameters, checked); validate(a.whenFalse(), schema, parameters, checked); }
            case DictionaryLookup a -> validate(a.symbol(), schema, parameters, checked);
            case FrameScalar ignored -> { }
            case ToFloating a -> validate(a.argument(), schema, parameters, checked);
            case Divide a -> { validate(a.left(), schema, parameters, checked); validate(a.right(), schema, parameters, checked); }
            case Parameter p -> { if (p.index() >= parameters.size() || !p.equals(parameters.get(p.index()))) throw new IllegalArgumentException("Foreign parameter: " + p); }
            case Constant ignored -> { }
            case FloatingConstant ignored -> { }
        }
    }
    private static void validate(Predicate p, List<Column> schema, List<Parameter> parameters, Map<Scalar, Boolean> checked) {
        switch (p) {
            case All ignored -> { }
            case Compare c -> { validate(c.left(), schema, parameters, checked); validate(c.right(), schema, parameters, checked); }
            case SqlCompare c -> { validate(c.left(), schema, parameters, checked); validate(c.right(), schema, parameters, checked); }
            case FloatingCompare c -> { validate(c.left(), schema, parameters, checked); validate(c.right(), schema, parameters, checked); }
            case IsNotNull n -> validate(n.value(), schema, parameters, checked);
            case Not n -> validate(n.value(), schema, parameters, checked);
            case Or a -> { validate(a.left(), schema, parameters, checked); validate(a.right(), schema, parameters, checked); }
            case And a -> { validate(a.left(), schema, parameters, checked); validate(a.right(), schema, parameters, checked); }
        }
    }

    private static Scalar rewrite(Scalar e, List<Column> schema, List<Scalar> bindings) {
        return switch (e) {
            case Column c -> {
                if (c.index() >= schema.size() || !c.equals(schema.get(c.index()))) throw new IllegalArgumentException("Foreign column: " + c);
                yield bindings.get(c.index());
            }
            case Constant c -> c;
            case FloatingConstant c -> c;
            case ToFloating a -> new ToFloating(rewrite(a.argument(), schema, bindings));
            case Divide a -> new Divide(rewrite(a.left(), schema, bindings), rewrite(a.right(), schema, bindings), a.columnType());
            case Parameter p -> p;
            case TimestampPart a -> new TimestampPart(rewrite(a.argument(), schema, bindings), a.unitTicks(), a.period());
            case TimestampYear a -> new TimestampYear(rewrite(a.argument(), schema, bindings), a.timestampType());
            case Conditional a -> new Conditional(rewrite(a.condition(), schema, bindings), rewrite(a.whenTrue(), schema, bindings), rewrite(a.whenFalse(), schema, bindings));
            case DictionaryLookup a -> new DictionaryLookup(rewrite(a.symbol(), schema, bindings), a.table());
            case FrameScalar ignored -> throw new IllegalArgumentException("Frame expressions cannot be substituted into a numeric scan");
            case Add a -> new Add(rewrite(a.left(), schema, bindings), rewrite(a.right(), schema, bindings), a.columnType());
            case Multiply a -> new Multiply(rewrite(a.left(), schema, bindings), rewrite(a.right(), schema, bindings), a.columnType());
            case IPv4Subtract a -> new IPv4Subtract(rewrite(a.left(), schema, bindings), rewrite(a.right(), schema, bindings));
            case Subtract a -> new Subtract(rewrite(a.left(), schema, bindings), rewrite(a.right(), schema, bindings), a.columnType());
        };
    }
    private static Predicate rewrite(Predicate p, List<Column> schema, List<Scalar> bindings) {
        return switch (p) {
            case All a -> a;
            case Compare c -> new Compare(c.op(), rewrite(c.left(), schema, bindings), rewrite(c.right(), schema, bindings));
            case SqlCompare c -> new SqlCompare(c.op(), rewrite(c.left(), schema, bindings), rewrite(c.right(), schema, bindings));
            case FloatingCompare c -> new FloatingCompare(c.op(), rewrite(c.left(), schema, bindings), rewrite(c.right(), schema, bindings));
            case IsNotNull n -> new IsNotNull(rewrite(n.value(), schema, bindings));
            case Not n -> new Not(rewrite(n.value(), schema, bindings));
            case Or a -> new Or(rewrite(a.left(), schema, bindings), rewrite(a.right(), schema, bindings));
            case And a -> new And(rewrite(a.left(), schema, bindings), rewrite(a.right(), schema, bindings));
        };
    }
    private static void collect(Scalar e, BitSet used) {
        switch (e) {
            case Column c -> used.set(c.index());
            case TimestampPart a -> collect(a.argument(), used);
            case TimestampYear a -> collect(a.argument(), used);
            case Conditional a -> { collect(a.condition(), used); collect(a.whenTrue(), used); collect(a.whenFalse(), used); }
            case DictionaryLookup a -> collect(a.symbol(), used);
            case FrameScalar expression -> expression.collectColumns(used);
            case ToFloating a -> collect(a.argument(), used);
            case Divide a -> { collect(a.left(), used); collect(a.right(), used); }
            case Add a -> { collect(a.left(), used); collect(a.right(), used); }
            case Multiply a -> { collect(a.left(), used); collect(a.right(), used); }
            case IPv4Subtract a -> { collect(a.left(), used); collect(a.right(), used); }
            case Subtract a -> { collect(a.left(), used); collect(a.right(), used); }
            case Constant ignored -> { }
            case FloatingConstant ignored -> { }
            case Parameter ignored -> { }
        }
    }
    static void collect(Predicate p, BitSet used) {
        switch (p) {
            case All ignored -> { }
            case Compare c -> { collect(c.left(), used); collect(c.right(), used); }
            case SqlCompare c -> { collect(c.left(), used); collect(c.right(), used); }
            case FloatingCompare c -> { collect(c.left(), used); collect(c.right(), used); }
            case IsNotNull n -> collect(n.value(), used);
            case Not n -> collect(n.value(), used);
            case Or a -> { collect(a.left(), used); collect(a.right(), used); }
            case And a -> { collect(a.left(), used); collect(a.right(), used); }
        }
    }
}
