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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.*;
import java.lang.invoke.*;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import static org.questdb.experimental.codegen.Ir.*;

/** Anchored byte capture. Immutable plans are shared; mutable matcher state belongs to one slot. */
final class NativeCapture {
    static final int DESCRIPTOR_BYTES = 16;

    record Plan(String pattern, int group, byte[] prefix) {
        static Plan bind(String pattern, int group) {
            try {
                Parser parser = new Parser(pattern);
                parser.sequence(false);
                if (parser.position != pattern.length() - 1 || group < 1 || group > parser.groups) return null;
                return new Plan(pattern, group, NativeCapture.prefix(pattern));
            } catch (IllegalArgumentException unsupported) { return null; }
        }
    }

    record Expression(ResolvedVarchar.Expression argument, Plan plan, int index) implements ResolvedVarchar.Expression {
        @Override public Expr isNull(Builder b, ResolvedVarchar.Frame frame) { return argument.isNull(b, frame); }
        @Override public Pipeline.Utf8Value emit(Builder b, ResolvedVarchar.Frame frame) {
            var input = frame.value(b, argument);
            Var pointer = b.let(input.address()), size = b.let(input.size());
            b.when(eq(input.isNull(), bool(false)), present -> {
                present.when(prefixMatches(present, pointer, size, plan.prefix), match -> {
                    Expr context = at(frame.helpers(), (long) index * DESCRIPTOR_BYTES);
                    Var bounds = match.let(new NativeCall(new Offset(nil(), i64(Bridge.MATCH.address())),
                            List.of(pointer, size, new Load(MemoryType.ADDRESS, context))));
                    match.when(eq(bounds, i64(-3)), slow -> slow.assign(bounds,
                            new NativeCall(new Load(MemoryType.ADDRESS, at(context, 8)), List.of(pointer, size))));
                    match.when(eq(bounds, i64(-3)), failed -> failed.exit(-11));
                    match.when(ne(bounds, i64(-2)), captured -> {
                        Expr start = binary(Op.AND, bounds, i64(0xffff_ffffL));
                        Expr end = binary(Op.USHR, bounds, i64(32));
                        captured.assign(pointer, new Offset(pointer, start));
                        captured.assign(size, binary(Op.SUB, end, start));
                    });
                });
            });
            // ASCII is a sufficient condition. A slice of non-ASCII input may be ASCII;
            // leaving the flag false only makes consumers validate/count the bytes.
            return new Pipeline.Utf8Value(pointer, size, input.isNull(), input.isAscii(), input.stable());
        }
    }

    static final class Slot implements AutoCloseable {
        private final Arena arena = Arena.ofShared();
        private final java.util.regex.Matcher matcher;
        private final ByteView view = new ByteView();
        private final int group;
        private Throwable allocationFailure;
        private long handle;

        Slot(Plan plan, long descriptor) {
            matcher = Pattern.compile(plan.pattern).matcher("");
            group = plan.group;
            try {
                var lookup = MethodHandles.lookup();
                long allocate = stub(lookup.findVirtual(Slot.class, "allocate", MethodType.methodType(long.class, long.class)),
                        FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
                long free = stub(lookup.findVirtual(Slot.class, "release", MethodType.methodType(void.class, long.class, long.class)),
                        FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
                long fallback = stub(lookup.findVirtual(Slot.class, "fallback", MethodType.methodType(long.class, long.class, long.class)),
                        FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
                byte[] pattern = plan.pattern.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                long address = arena.allocateFrom(ValueLayout.JAVA_BYTE, pattern).address();
                handle = (long) Bridge.CREATE.invokeExact(address, (long) pattern.length, (long) group, allocate, free);
                if (allocationFailure != null) throw allocationFailure;
                if (handle == 0) throw new IllegalStateException("PCRE2 JIT could not prepare admitted capture");
                Unsafe.putLong(descriptor, handle);
                Unsafe.putLong(descriptor + 8, fallback);
            } catch (Throwable failure) {
                try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                if (failure instanceof Error error) throw error;
                if (failure instanceof RuntimeException error) throw error;
                throw new IllegalStateException("Could not prepare capture helper", failure);
            }
        }

        private long stub(MethodHandle method, FunctionDescriptor descriptor) {
            return Linker.nativeLinker().upcallStub(method.bindTo(this), descriptor, arena).address();
        }
        private long allocate(long bytes) {
            if (allocationFailure != null) return 0;
            try { return Unsafe.malloc(bytes, MemoryTag.NATIVE_JIT); }
            catch (Throwable failure) { allocationFailure = failure; return 0; }
        }
        private void release(long address, long bytes) {
            try { Unsafe.free(address, bytes, MemoryTag.NATIVE_JIT); }
            catch (Throwable failure) { if (allocationFailure == null) allocationFailure = failure; }
        }
        long fallback(long address, long size) {
            try {
                view.address = address; view.size = Math.toIntExact(size); view.reads = 0;
                matcher.reset(view);
                if (!matcher.find()) return -2;
                int start = matcher.start(group), end = matcher.end(group);
                return start < 0 ? 0 : ((long) end << 32) | start;
            } catch (Throwable failure) {
                KernelMemory.fail(failure);
                return -3;
            } finally { view.address = 0; view.size = 0; }
        }
        @Override public void close() {
            if (!arena.scope().isAlive()) return;
            try {
                if (handle != 0) { Bridge.FREE.invokeExact(handle); handle = 0; }
            } catch (Throwable failure) { throw new IllegalStateException("Could not close capture helper", failure); }
            finally { arena.close(); }
        }
    }

    private static final class ByteView implements CharSequence {
        long address;
        int size, reads;
        @Override public int length() { return size; }
        @Override public char charAt(int index) {
            if ((reads++ & 1023) == 0 && KernelMemory.poll() != 0) KernelMemory.checkFailure();
            // Match SingleGroupAsciiFunc's signed-byte-to-char contract.
            return (char) Unsafe.getByte(address + index);
        }
        @Override public CharSequence subSequence(int lo, int hi) { throw new UnsupportedOperationException(); }
    }

    /** Conservative shared Java/PCRE language; reject syntax with different byte semantics. */
    private static final class Parser {
        final String pattern;
        int position = 1, groups, nodes;
        Parser(String pattern) {
            this.pattern = pattern;
            if (pattern.length() < 3 || pattern.length() > 256 || !pattern.startsWith("^") || !pattern.endsWith("$"))
                throw new IllegalArgumentException();
        }
        int sequence(boolean nested) {
            int minimum = 0;
            while (position < pattern.length() - 1 && pattern.charAt(position) != ')') {
                if (++nodes > 64) throw new IllegalArgumentException();
                char c = pattern.charAt(position++);
                if (c == '(') {
                    if (pattern.startsWith("?:", position)) position += 2;
                    else groups++;
                    int groupMinimum = sequence(true);
                    if (position >= pattern.length() - 1 || pattern.charAt(position++) != ')') throw new IllegalArgumentException();
                    if (position < pattern.length() - 1 && pattern.charAt(position) == '?') {
                        // Nullable optional groups can backtrack exponentially without reading
                        // a character, bypassing the fallback's CharSequence cancellation polls.
                        if (groupMinimum == 0) throw new IllegalArgumentException();
                        position++;
                    } else minimum += groupMinimum;
                    continue;
                }
                if (c == '[') {
                    if (position < pattern.length() - 1 && pattern.charAt(position) == '^') position++;
                    int terms = 0;
                    while (position < pattern.length() - 1 && pattern.charAt(position) != ']') {
                        int lo = literal(pattern.charAt(position++));
                        if (lo == '&') throw new IllegalArgumentException(); // Java class intersection.
                        if (position + 1 < pattern.length() - 1 && pattern.charAt(position) == '-' && pattern.charAt(position + 1) != ']') {
                            position++;
                            if (literal(pattern.charAt(position++)) < lo) throw new IllegalArgumentException();
                        }
                        terms++;
                    }
                    if (terms == 0 || position >= pattern.length() - 1 || pattern.charAt(position++) != ']') throw new IllegalArgumentException();
                } else if (c != '.') literal(c);
                int count = 1;
                if (position < pattern.length() - 1 && "?*+".indexOf(pattern.charAt(position)) >= 0) {
                    if (pattern.charAt(position) != '+') count = 0;
                    position++;
                }
                minimum += count;
            }
            if (!nested && position != pattern.length() - 1) throw new IllegalArgumentException();
            return minimum;
        }
        int literal(char c) {
            if (c == '\\') {
                if (position >= pattern.length() - 1) throw new IllegalArgumentException();
                c = pattern.charAt(position++);
                if (Character.isLetterOrDigit(c)) throw new IllegalArgumentException();
            } else if ("^$|?*+{}()[]".indexOf(c) >= 0) throw new IllegalArgumentException();
            if (c > 127) throw new IllegalArgumentException();
            return c;
        }
    }

    private static byte[] prefix(String pattern) {
        var prefix = new ByteArrayOutputStream();
        for (int i = 1; i < pattern.length() - 1;) {
            char c = pattern.charAt(i++);
            if (c == '\\') c = pattern.charAt(i++); // Parser already validated escapes.
            else if (".^$[]()|?*+{}".indexOf(c) >= 0) break;
            if (i < pattern.length() - 1 && "?*".indexOf(pattern.charAt(i)) >= 0) break;
            prefix.write(c);
            if (i < pattern.length() - 1 && pattern.charAt(i) == '+') break;
        }
        return prefix.toByteArray();
    }

    private static Expr prefixMatches(Builder b, Expr address, Expr size, byte[] prefix) {
        Var matches = b.let(binary(Op.GE, size, i64(prefix.length)));
        for (int i = 0; i < prefix.length;) {
            int remaining = prefix.length - i;
            MemoryType width = remaining >= 8 ? MemoryType.I64 : remaining >= 4 ? MemoryType.I32 : remaining >= 2 ? MemoryType.I16 : MemoryType.I8;
            long bits = 0;
            for (int j = 0; j < width.bytes; j++) bits |= (prefix[i + j] & 255L) << (8 * j);
            long constant = width.bytes == 8 ? bits : width.bytes == 4 ? (int) bits : width.bytes == 2 ? (short) bits : (byte) bits;
            int offset = i;
            b.when(matches, check -> check.assign(matches, eq(new Load(width, at(address, offset)), i64(constant))));
            i += width.bytes;
        }
        return matches;
    }

    private static final class Bridge {
        static final MethodHandle CREATE, FREE;
        static final MemorySegment MATCH;
        static {
            try {
                Path classes = Path.of(LlvmBackend.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                Path llvm = Path.of(System.getProperty("questdb.codegen.llvm.library",
                        classes.getParent().resolve("native/libquery_codegen_llvm.so").toString())).toAbsolutePath();
                var lookup = SymbolLookup.libraryLookup(llvm.resolveSibling("libquery_codegen_regex.so"), Arena.global());
                var linker = Linker.nativeLinker();
                CREATE = linker.downcallHandle(lookup.find("query_capture_create").orElseThrow(), FunctionDescriptor.of(
                        ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
                FREE = linker.downcallHandle(lookup.find("query_capture_free").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));
                MATCH = lookup.find("query_capture_match").orElseThrow();
            } catch (Exception failure) { throw new ExceptionInInitializerError(failure); }
        }
    }
}
