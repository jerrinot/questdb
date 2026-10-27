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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.CountDistinctLongGroupByFunction;
import io.questdb.std.ObjList;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.*;

public class CompiledAggregateOwnershipTest {
    @Test
    public void partialCompilationClosesEveryKernelAndPreservesPrimaryFailure() throws Exception {
        for (boolean failClose : new boolean[]{false, true}) {
            for (int failAt = 1; failAt <= 8; failAt++) {
                var backend = new TrackingBackend(failAt, failClose);
                try (var map = new Unordered8Map(ColumnType.LONG, types(), 16, 0.6, 30)) {
                    var binding = binding(map);
                    IOException failure = assertThrows(IOException.class, () -> binding.compile(backend, Path.of("unused"), true));
                    assertSame(backend.failure, failure);
                    assertEquals(failAt - 1, backend.closed);
                }
            }
        }
    }

    @Test
    public void allKernelsContributeToAccountingAndCloseExactlyOnce() throws Exception {
        var backend = new TrackingBackend(Integer.MAX_VALUE, true);
        try (var map = new Unordered8Map(ColumnType.LONG, types(), 16, 0.6, 30)) {
            var prepared = binding(map).compile(backend, Path.of("unused"), true);
            prepared.prepareRowLimit();
            assertEquals(9, backend.created);
            assertEquals(backend.totalNanos, prepared.compilationNanos());
            assertThrows(IllegalStateException.class, prepared::close);
            assertEquals(backend.created, backend.closed);
            prepared.close();
            assertEquals(backend.created, backend.closed);
        }
    }

    private static ArrayColumnTypes types() {
        return new ArrayColumnTypes().add(ColumnType.LONG).add(ColumnType.LONG);
    }

    private static ResolvedAggregateKernel.Binding binding(Unordered8Map map) {
        var functions = new ObjList<GroupByFunction>();
        functions.add(new CountDistinctLongGroupByFunction(LongColumn.newInstance(0), 16, 0.6));
        var values = new ArrayColumnTypes();
        functions.getQuick(0).initValueTypes(values);
        return ResolvedAggregateKernel.bind(new ArrayColumnTypes().add(ColumnType.LONG), values, functions,
                QuestMapProbe.bind(0, ColumnType.LONG, map));
    }

    private static final class TrackingBackend implements Backend {
        final IOException failure = new IOException("compile failure");
        final int failAt;
        final boolean failClose;
        int calls;
        int created;
        int closed;
        long totalNanos;

        TrackingBackend(int failAt, boolean failClose) { this.failAt = failAt; this.failClose = failClose; }
        @Override public String name() { return "tracking"; }
        @Override public CompiledKernel compile(Ir.Program program, Path artifacts) throws IOException {
            if (++calls == failAt) throw failure;
            long nanos = ++created * 100L;
            totalNanos += nanos;
            return new CompiledKernel((columns, rows, state) -> 0, () -> {
                closed++;
                if (failClose) throw new IllegalStateException("close failure");
            }, null, nanos);
        }
    }
}
