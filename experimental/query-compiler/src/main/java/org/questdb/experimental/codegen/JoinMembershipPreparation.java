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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.NativeMapView;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.questdb.experimental.codegen.Ir.*;

/** Owner-only export of completed map keys into the existing bounded membership representation. */
final class JoinMembershipPreparation implements java.io.Closeable {
    private static final int MODE = IntegralMembership.BYTES;
    static final int SCRATCH_BYTES = NativeMapView.MUTATION_BYTES + MODE + Long.BYTES;
    private final int[] types;
    private final Class<?> mapClass;
    private final CompiledKernel code;
    private final KernelMemory memory = KernelMemory.reusable(() -> { });

    private JoinMembershipPreparation(int[] types, Map map, CompiledKernel code) {
        this.types = types.clone();
        this.mapClass = map.getClass();
        this.code = code;
    }

    static JoinMembershipPreparation compile(int[] types, Map map, Backend backend, Path artifacts) {
        var columns = new ListColumnFilter();
        var columnTypes = new ArrayColumnTypes();
        for (int i = 0; i < types.length; i++) {
            if (types[i] != ColumnType.INT && types[i] != ColumnType.LONG) return null;
            columns.add(i + 1);
            columnTypes.add(types[i]);
        }
        var key = QuestFixedKey.bind(columnTypes, columnTypes, columns, null, map);
        if (key == null) return null;
        var probe = QuestMapProbe.fixed(key, types[0], map instanceof OrderedMap);
        try {
            // Like other deferred operators, first execution may compile outside the planner.
            synchronized (backend) {
                return new JoinMembershipPreparation(types, map, backend.compile(program(types, probe), artifacts));
            }
        } catch (IOException failure) {
            throw CairoException.nonCritical().put("Join membership compilation failed: ").put(failure.getMessage());
        }
    }

    boolean matches(int[] types, Map map) {
        return map.getClass() == mapClass && Arrays.equals(types, this.types);
    }

    void prepare(IntegralMembership set, Map map, int keyBytes, MemoryTracker tracker,
                 SqlExecutionCircuitBreaker breaker) {
        long view = Unsafe.malloc(SCRATCH_BYTES, MemoryTag.NATIVE_JIT, tracker);
        try {
            // The owner holds the map throughout both read-only scans. The larger view supplies
            // SIZE and the ordered entry stride; no mutation needs publishing back to the map.
            map.writeNativeMutationView(view);
            long state = view + NativeMapView.MUTATION_BYTES;
            Unsafe.putLong(state + MODE, 0);
            memory.enter(breaker);
            try {
                code.run(view, map.size(), state);
                memory.throwIfFailed();
                set.prepareJoin(map.size(), Unsafe.getLong(state), Unsafe.getLong(state + 8), keyBytes, tracker);
                set.write(state);
                if (Unsafe.getLong(state + 24) != 0) {
                    Unsafe.putLong(state + MODE, 1);
                    code.run(view, map.size(), state);
                    memory.throwIfFailed();
                }
            } finally {
                memory.close();
            }
        } finally {
            Unsafe.free(view, SCRATCH_BYTES, MemoryTag.NATIVE_JIT, tracker);
        }
    }

    private static Program program(int[] types, QuestMapProbe probe) {
        Builder b = new Builder();
        Var mode = b.let(load(at(STATE, MODE)));
        b.when(eq(mode, i64(0)), bounds -> {
            Var first = bounds.let(i64(Long.MAX_VALUE)), last = bounds.let(i64(Long.MIN_VALUE));
            QuestMapScan.emit(bounds, probe, COLUMNS, (scan, entry) -> {
                Var key = scan.let(key(scan, types, entry));
                scan.assign(first, new Select(binary(Op.LT, key, first), key, first));
                scan.assign(last, new Select(binary(Op.LT, last, key), key, last));
            });
            bounds.store(STATE, first);
            bounds.store(at(STATE, 8), last);
        });
        b.when(ne(mode, i64(0)), fill -> {
            var set = IntegralMembership.bind(fill, STATE);
            QuestMapScan.emit(fill, probe, COLUMNS, (scan, entry) ->
                    IntegralMembership.insertJoin(scan, set, key(scan, types, entry)));
        });
        b.exit(i64(0));
        return new Program(b.block());
    }

    private static Expr key(Builder b, int[] types, Expr entry) {
        Expr hash = i64(0);
        int offset = 0;
        for (int type : types) {
            MemoryType width = type == ColumnType.INT ? MemoryType.I32 : MemoryType.I64;
            Var value = b.let(new Load(width, at(entry, offset)));
            hash = types.length == 1 ? value : QuestMapProbe.hashLong128(b, hash, value);
            offset += width.bytes;
        }
        return hash;
    }

    @Override public void close() { code.close(); }
}
