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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.bool.NotFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.regex.AbstractLikeVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.StartsWithVarcharFunctionFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.IntSupplier;

import static org.questdb.experimental.codegen.Ir.*;

/** Resolved predicates shared by standalone filtering and aggregate selection. */
final class ResolvedFilter implements AutoCloseable {
    private final CompiledKernel code;
    private final Binding binding;
    private boolean closed;

    private ResolvedFilter(CompiledKernel code, Binding binding) {
        this.code = code;
        this.binding = binding;
    }

    static ResolvedFilter compile(ColumnTypes types, Function function, Backend backend, Path artifacts) throws IOException {
        Binding binding = bind(types, function);
        if (binding == null) return null;
        var program = ScanLowering.selectionRange(binding.plan(), binding.residual(), binding.bytes());
        var code = backend.compile(program, artifacts);
        try { return new ResolvedFilter(code, binding); }
        catch (Throwable failure) { code.close(); throw failure; }
    }

    static Binding bind(ColumnTypes types, Function function) {
        var parameters = new java.util.ArrayList<IntSupplier>();
        var inputs = new ResolvedInputs(types, parameters);
        var leaves = new java.util.ArrayList<Function>();
        if (!conjuncts(function, leaves, 0)) return null;
        var residuals = new ResidualBinder(types, inputs, parameters);
        var tests = new java.util.ArrayList<ScanLowering.RowPredicate>();
        Pipeline.Predicate numeric = new Pipeline.All();
        for (Function leaf : leaves) {
            var predicate = inputs.predicate(leaf);
            if (predicate != null) {
                numeric = numeric instanceof Pipeline.All ? predicate : new Pipeline.And(numeric, predicate);
            } else {
                var test = residuals.bind(leaf, 0);
                if (test == null) return null;
                tests.add(test);
            }
        }
        // Only top-level numeric conjuncts may reject a row before the residual tree.
        // Numeric leaves inside OR must stay in that branch.
        ScanLowering.RowPredicate residual = tests.isEmpty() ? null : combine(tests, false);
        var parameterTypes = java.util.stream.IntStream.range(0, parameters.size())
                .mapToObj(i -> new Pipeline.Parameter(i, ColumnType.SYMBOL)).toList();
        var plan = new ScanPlan(inputs.columns(), numeric, inputs.columns(),
                new java.util.ArrayList<Pipeline.Scalar>(inputs.columns()), parameterTypes);
        return new Binding(plan, inputs.indexes(), residuals.strings.stream().mapToInt(Integer::intValue).toArray(), residual,
                List.copyOf(parameters), List.copyOf(residuals.symbols), residuals.literals.toByteArray(), new DirectLongList(0, MemoryTag.NATIVE_JIT, true));
    }

    private static ScanLowering.RowPredicate combine(List<ScanLowering.RowPredicate> tests, boolean disjunction) {
        return (builder, descriptors, row) -> {
            Var accepted = builder.let(bool(!disjunction));
            for (var test : tests) builder.when(eq(accepted, bool(!disjunction)), check ->
                    check.assign(accepted, test.emit(check, descriptors, row)));
            return accepted;
        };
    }

    /** Compiler-time descriptor indexes; offsets are final when the bound tree emits IR. */
    private static final class ResidualBinder {
        private final ColumnTypes types;
        private final ResolvedInputs inputs;
        private final List<IntSupplier> parameters;
        private final java.util.ArrayList<Integer> strings = new java.util.ArrayList<>();
        private final java.util.ArrayList<SymbolMembership> symbols = new java.util.ArrayList<>();
        private final java.io.ByteArrayOutputStream literals = new java.io.ByteArrayOutputStream();

        private ResidualBinder(ColumnTypes types, ResolvedInputs inputs, List<IntSupplier> parameters) {
            this.types = types;
            this.inputs = inputs;
            this.parameters = parameters;
        }

        private long stringOffset() { return (inputs.columns().size() + parameters.size()) * 8L; }
        private long symbolOffset() { return stringOffset() + strings.size() * 48L; }
        private long literalOffset() { return symbolOffset() + symbols.size() * SymbolMembership.BYTES; }

        private ScanLowering.RowPredicate bind(Function function, int depth) {
            if (depth > 64) return null;
            var numeric = inputs.predicate(function);
            if (numeric != null) return (builder, descriptors, row) -> {
                var columns = inputs.columns();
                var addresses = new java.util.ArrayList<Expr>();
                for (int i = 0; i < columns.size(); i++) addresses.add(new Load(MemoryType.ADDRESS, at(descriptors, i * 8L)));
                var values = new java.util.ArrayList<Expr>();
                for (int i = 0; i < parameters.size(); i++) values.add(new Load(MemoryType.I32, at(descriptors, (columns.size() + i) * 8L)));
                return numeric.emit(ScanLowering.row(columns, addresses, row, true, values));
            };
            if (function.getClass() == NotFunctionFactory.Func.class) {
                var child = bind(((NotFunctionFactory.Func) function).getArg(), depth + 1);
                return child == null ? null : (builder, descriptors, row) -> eq(child.emit(builder, descriptors, row), bool(false));
            }
            var arguments = ResolvedInputs.andArguments(function);
            boolean disjunction = function.getClass() == io.questdb.griffin.engine.functions.bool.OrFunctionFactory.MyBooleanFunction.class;
            if (disjunction) {
                var binary = (BinaryFunction) function;
                arguments = new Function[]{binary.getLeft(), binary.getRight()};
            }
            if (arguments != null) {
                var tests = new java.util.ArrayList<ScanLowering.RowPredicate>();
                for (Function argument : arguments) {
                    var child = bind(argument, depth + 1);
                    if (child == null) return null;
                    tests.add(child);
                }
                return combine(List.copyOf(tests), disjunction);
            }
            if (function instanceof SymbolKeySetProvider source) {
                Function argument = function instanceof UnaryFunction unary ? unary.getArg()
                        : function instanceof BinaryFunction binary ? binary.getLeft() : null;
                if (argument == null) return null;
                var column = inputs.column(argument, ColumnType.SYMBOL);
                if (column == null) return null;
                int index = symbols.size();
                symbols.add(new SymbolMembership(source));
                return (builder, descriptors, row) -> {
                    Expr base = new Load(MemoryType.ADDRESS, at(descriptors, column.index() * 8L));
                    Var key = builder.let(new Load(MemoryType.I32, new Offset(base, binary(Op.MUL, row, i64(4)))));
                    return SymbolMembership.contains(builder, at(descriptors, symbolOffset() + index * SymbolMembership.BYTES), key);
                };
            }
            StringPredicate test;
            int column;
            if (function.getClass() == EqVarcharFunctionFactory.NullCheckFunc.class) {
                var check = (EqVarcharFunctionFactory.NullCheckFunc) function;
                column = varcharColumn(types, check.getArg());
                test = (builder, descriptor, row, literalBase) -> eq(ne(binary(Op.AND,
                        QuestVarchar.header(builder, descriptor, row), i64(4)), i64(0)), bool(!check.isNegated()));
            } else if (function.getClass() == AbstractLikeVarcharFunctionFactory.ConstContainsSwarVarcharFunction.class) {
                var contains = (AbstractLikeVarcharFunctionFactory.ConstContainsSwarVarcharFunction) function;
                column = varcharColumn(types, contains.getArg());
                var patternValue = contains.getPattern();
                byte[] pattern = new byte[patternValue.size()];
                for (int i = 0; i < pattern.length; i++) pattern[i] = patternValue.byteAt(i);
                test = (builder, descriptor, row, literalBase) -> {
                    var value = QuestVarchar.decode(builder, descriptor, row);
                    Var found = builder.let(bool(false));
                    builder.when(eq(value.isNull(), bool(false)), present ->
                            present.assign(found, ByteSequences.contains(present, value.address(), value.size(), pattern)));
                    return found;
                };
            } else {
                LiteralMatch literal = literalMatch(function, literals);
                if (literal == null) return null;
                column = varcharColumn(types, ((UnaryFunction) function).getArg());
                test = literal::emit;
            }
            if (column < 0) return null;
            int index = strings.size();
            strings.add(column);
            return (builder, descriptors, row) -> test.emit(builder, at(descriptors, stringOffset() + index * 48L), row,
                    new Load(MemoryType.ADDRESS, at(descriptors, literalOffset())));
        }
    }

    private enum LiteralKind { EQUAL, PREFIX, SUFFIX }

    private static LiteralMatch literalMatch(Function function, java.io.ByteArrayOutputStream bytes) {
        if (function == null) return null;
        io.questdb.std.str.Utf8Sequence literal;
        LiteralKind kind;
        boolean negate = false;
        if (function.getClass() == EqVarcharFunctionFactory.ConstCheckFunc.class) {
            var equal = (EqVarcharFunctionFactory.ConstCheckFunc) function;
            literal = equal.getConstant();
            negate = equal.isNegated();
            kind = LiteralKind.EQUAL;
        } else if (function.getClass() == StartsWithVarcharFunctionFactory.ConstFunc.class
                || function.getClass() == AbstractLikeVarcharFunctionFactory.ConstStartsWithVarcharFunction.class) {
            literal = ((StartsWithVarcharFunctionFactory.ConstFunc) function).getPrefix();
            kind = LiteralKind.PREFIX;
        } else if (function.getClass() == AbstractLikeVarcharFunctionFactory.ConstEndsWithVarcharFunction.class) {
            literal = ((AbstractLikeVarcharFunctionFactory.ConstEndsWithVarcharFunction) function).getPattern();
            kind = LiteralKind.SUFFIX;
        } else return null;
        int offset = bytes.size(), size = literal == null ? -1 : literal.size();
        long prefix = 0;
        for (int i = 0; i < size; i++) {
            byte value = literal.byteAt(i);
            bytes.write(value);
            if (i < 6) prefix |= (value & 255L) << (i * 8);
        }
        return new LiteralMatch(kind, offset, size, prefix, negate);
    }

    private record LiteralMatch(LiteralKind kind, int offset, int size, long prefix, boolean negate) {
        Expr emit(Builder b, Expr descriptor, Expr row, Expr literals) {
            Var found = b.let(bool(false));
            if (size >= 0) {
                var value = QuestVarchar.decode(b, descriptor, row);
                Expr length = kind == LiteralKind.EQUAL ? eq(value.size(), i64(size)) : binary(Op.GE, value.size(), i64(size));
                b.when(binary(Op.BOTH, eq(value.isNull(), bool(false)), length), valid -> {
                    if (size == 0) valid.assign(found, bool(true));
                    else if (kind == LiteralKind.SUFFIX) {
                        Expr start = new Offset(value.address(), binary(Op.SUB, value.size(), i64(size)));
                        valid.assign(found, ByteSequences.equal(valid, start, valid.let(at(literals, offset)), i64(size)));
                    } else {
                        long mask = (1L << (Math.min(6, size) * 8)) - 1;
                        Var actual = QuestVarchar.sixPrefix(valid, descriptor, row, value);
                        valid.assign(found, eq(binary(Op.AND, actual, i64(mask)), i64(prefix)));
                        if (size > 6) valid.when(found, tail -> tail.assign(found,
                                ByteSequences.equal(tail, at(value.address(), 6), tail.let(at(literals, offset + 6)), i64(size - 6))));
                    }
                });
            }
            return negate ? eq(found, bool(false)) : found;
        }
    }

    private static int varcharColumn(ColumnTypes types, Function function) {
        if (function.getClass() != VarcharColumn.class) return -1;
        int column = ((VarcharColumn) function).getColumnIndex();
        return column >= 0 && column < types.getColumnCount() && types.getColumnType(column) == ColumnType.VARCHAR ? column : -1;
    }

    @FunctionalInterface
    private interface StringPredicate {
        Expr emit(Builder builder, Expr descriptor, Expr row, Expr literalBase);
    }

    record Binding(ScanPlan plan, int[] sourceIndexes, int[] varcharColumns, ScanLowering.RowPredicate residual,
                   List<IntSupplier> parameters, List<SymbolMembership> symbols, byte[] literals, DirectLongList literalMemory) implements AutoCloseable {
        long stringOffset() { return (sourceIndexes.length + parameters.size()) * 8L; }
        long symbolOffset() { return stringOffset() + varcharColumns.length * 48L; }
        long literalOffset() { return symbolOffset() + symbols.size() * SymbolMembership.BYTES; }
        long bytes() { return literalOffset() + (literals.length == 0 ? 0 : Long.BYTES); }

        void prepareExecution(MemoryTracker tracker) {
            try {
                for (var symbol : symbols) symbol.prepare(tracker);
                literalMemory.close();
                literalMemory.setMemoryTracker(tracker);
                if (literals.length != 0) {
                    literalMemory.ensureCapacity((literals.length + 7L) / 8);
                    Unsafe.copyMemory(literals, Unsafe.BYTE_OFFSET, null, literalMemory.getAddress(), literals.length);
                }
            } catch (Throwable failure) { close(); throw failure; }
        }

        @Override public void close() {
            for (var symbol : symbols) symbol.close();
            literalMemory.close();
            literalMemory.setMemoryTracker(null);
        }

        private void prepareParameters(long addresses) {
            if (literals.length != 0) {
                if (literalMemory.getAddress() == 0) throw new IllegalStateException("Filter literals are not prepared");
                Unsafe.putLong(addresses + literalOffset(), literalMemory.getAddress());
            }
            // Borrow resolved keys after Function.init; they are immutable while tasks run.
            for (int i = 0; i < parameters.size(); i++)
                Unsafe.putInt(addresses + (sourceIndexes.length + i) * 8L, parameters.get(i).getAsInt());
            for (int i = 0; i < symbols.size(); i++)
                symbols.get(i).write(addresses + symbolOffset() + i * SymbolMembership.BYTES);
        }

        boolean prepare(PageFrameMemoryRecord record, long addresses) {
            if (record instanceof PageFrameFilteredMemoryRecord) return false;
            for (int i = 0; i < sourceIndexes.length; i++) {
                long address = record.getPageAddress(sourceIndexes[i]);
                // Preserve ordinary getters for column tops and lazy casts.
                if (address == 0) return false;
                Unsafe.putLong(addresses + i * 8L, address);
            }
            prepareParameters(addresses);
            for (int i = 0; i < varcharColumns.length; i++)
                if (!record.writeNativeVarcharView(varcharColumns[i], addresses + stringOffset() + i * 48L)) return false;
            return true;
        }

        void prepare(PageFrameMemory frame, long data, long aux, long rows, long addresses) {
            for (int i = 0; i < sourceIndexes.length; i++)
                Unsafe.putLong(addresses + i * 8L, Unsafe.getLong(data + sourceIndexes[i] * 8L));
            prepareParameters(addresses);
            for (int i = 0; i < varcharColumns.length; i++) {
                int column = varcharColumns[i];
                long descriptor = addresses + stringOffset() + i * 48L;
                // Latest-by can supply a shifted aux vector for a bounded row slice.
                Unsafe.putLong(descriptor, Unsafe.getLong(aux + column * 8L));
                Unsafe.putLong(descriptor + 8, Unsafe.getLong(data + column * 8L));
                Unsafe.putLong(descriptor + 16, rows * 16L);
                Unsafe.putLong(descriptor + 24, frame.getPageSize(column));
                Unsafe.putLong(descriptor + 32, frame.getFrameFormat() == PartitionFormat.PARQUET ? 1 : 0);
                Unsafe.putLong(descriptor + 40, 0); // predicates never retain input pointers
            }
        }
    }

    private static boolean conjuncts(Function function, java.util.List<Function> leaves, int depth) {
        if (depth > 64) return false;
        var arguments = ResolvedInputs.andArguments(function);
        if (arguments == null) leaves.add(function);
        else for (Function argument : arguments) if (!conjuncts(argument, leaves, depth + 1)) return false;
        return true;
    }

    ScanPlan fusedPlan() {
        // The map-fused ABI currently carries column pointers only.
        return binding.residual() == null && binding.parameters().isEmpty() ? binding.plan() : null;
    }

    void prepareExecution(MemoryTracker tracker) { binding.prepareExecution(tracker); }
    void clearExecution() { binding.close(); }

    long compilationNanos() { return code.compilationNanos(); }

    Slot newSlot() {
        if (closed) throw new IllegalStateException("Filter code is closed");
        return new Slot();
    }

    @Override
    public void close() {
        if (!closed) { closed = true; try { code.close(); } finally { binding.close(); } }
    }

    final class Slot implements AutoCloseable {
        private final KernelMemory memory = KernelMemory.reusable((Runnable) null);
        private final long bytes = binding.bytes() + 8;
        private long addresses = Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT);
        private long inputRows;
        private long selectedRows;

        /** Borrowed pointer array; zero declines before the caller mutates aggregate state. */
        long prepare(PageFrameMemoryRecord record) {
            if (closed || addresses == 0) throw new IllegalStateException("Filter slot is closed");
            if (!binding.prepare(record, addresses)) return 0;
            Unsafe.putLong(addresses + bytes - 8, 0);
            return addresses;
        }

        boolean filter(PageFrameMemoryRecord record, long rowCount, DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
            if (prepare(record) == 0) return false;
            filterRange(0, rowCount, rows, breaker);
            return true;
        }

        /** Reuse prepared frame pointers and the caller's bounded selection scratch. */
        long filterRange(long begin, long end, DirectLongList rows, SqlExecutionCircuitBreaker breaker) {
            rows.clearAndEnsureCapacity(end - begin);
            Unsafe.putLong(addresses + bytes - 8, begin);
            memory.enter(breaker);
            try {
                long count = code.run(addresses, end, rows.getAddress());
                memory.throwIfFailed();
                if (count < 0 || count > end - begin) throw new IllegalStateException("Invalid native filter count: " + count);
                rows.setPos(count);
                inputRows += end - begin;
                selectedRows += count;
                return count;
            } finally { memory.close(); }
        }

        long[] counters() { return new long[]{inputRows, selectedRows}; }

        @Override
        public void close() {
            if (addresses != 0) addresses = Unsafe.free(addresses, bytes, MemoryTag.NATIVE_JIT);
        }
    }
}
