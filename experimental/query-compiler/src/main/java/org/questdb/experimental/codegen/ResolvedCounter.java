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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.CompiledCounter;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;

import static org.questdb.experimental.codegen.Ir.*;

/** Batches frame/shard cardinalities through the existing cursor Counter contract. */
final class ResolvedCounter extends CompiledCounter {
    private static final int CAPACITY = 256;
    private static final long BYTES = (CAPACITY + 1L) * Long.BYTES;
    private final CompiledKernel code;
    private final String name;
    private long buffer;
    private int pending;
    private long nativePartials;

    private ResolvedCounter(CompiledKernel code, String name) {
        this.code = code;
        this.name = name;
        buffer = Unsafe.malloc(BYTES, MemoryTag.NATIVE_JIT);
        clear();
    }

    static ResolvedCounter compile(RecordCursorFactory input, Backend backend, Path artifacts) throws IOException {
        // Negative LIMIT uses a different cursor; never replace an O(1) size with row enumeration.
        if (input.implementsLimit() || !(input instanceof AsyncFilteredRecordCursorFactory
                || input instanceof AsyncJitFilteredRecordCursorFactory
                || input instanceof AsyncGroupByRecordCursorFactory)) return null;
        Builder b = new Builder();
        Var total = b.let(load(at(STATE, 0)));
        Var row = b.let(i64(0));
        b.loop(binary(Op.LT, row, ROWS), loop -> {
            loop.assign(total, Ir.add(total, load(new Offset(COLUMNS, binary(Op.MUL, row, i64(8))))));
            loop.assign(row, Ir.add(row, i64(1)));
        });
        b.store(at(STATE, 0), total);
        b.exit(0);
        CompiledKernel code = backend.compile(new Program(b.block()), artifacts);
        try { return new ResolvedCounter(code, backend.name()); }
        catch (Throwable failure) {
            try { code.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    @Override public void add(long value) {
        Unsafe.putLong(buffer + (pending + 1L) * Long.BYTES, value);
        if (++pending == CAPACITY) reduce();
    }

    @Override public void clear() {
        pending = 0;
        nativePartials = 0;
        Unsafe.putLong(buffer, 0);
    }

    @Override public void close() {
        if (buffer != 0) buffer = Unsafe.free(buffer, BYTES, MemoryTag.NATIVE_JIT);
        code.close();
    }

    @Override public void dec() { add(-1); }
    @Override public void dec(long value) { add(-value); }
    @Override public long get() { reduce(); return Unsafe.getLong(buffer); }
    @Override public CharSequence getName() { return name; }
    @Override public long getNativePartials() { return nativePartials; }
    @Override public void inc() { add(1); }

    @Override public void set(long value) {
        pending = 0;
        Unsafe.putLong(buffer, value);
    }

    private void reduce() {
        if (pending > 0) {
            // Bounded work, no allocation or callbacks; the producing cursor owns cancellation/yield.
            code.run(buffer + Long.BYTES, pending, buffer);
            nativePartials += pending;
            pending = 0;
        }
    }
}
