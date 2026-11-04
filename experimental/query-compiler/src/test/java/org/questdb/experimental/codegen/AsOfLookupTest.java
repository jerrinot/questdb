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
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class AsOfLookupTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void saturatingTimestampBounds() throws Exception {
        long functions = LlvmBackend.liveFunctions();
        long memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        long address = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        int checks = 0;
        try {
            var record = new PageFrameMemoryRecord() {
                @Override public long getPageAddress(int column) { return address + column * 8L; }
            };
            for (int type : new int[]{ColumnType.INT, ColumnType.LONG, ColumnType.SYMBOL}) {
                var inputs = new ArrayColumnTypes().add(ColumnType.TIMESTAMP).add(type);
                for (long scale : new long[]{1, 1000}) {
                    try (var code = ResolvedAsOfLookup.compile(inputs, BooleanConstant.TRUE, 1, 0, scale, true,
                            new LlvmBackend(), temporary.newFolder().toPath())) {
                        assertNotNull(code);
                        code.prepareExecution(null);
                        Unsafe.putLong(address + 8, 17);
                        for (long ts : new long[]{Long.MIN_VALUE, Long.MIN_VALUE / 1000 - 1, Long.MIN_VALUE / 1000,
                                -1, 0, 1, Long.MAX_VALUE / 1000, Long.MAX_VALUE / 1000 + 1, Long.MAX_VALUE}) {
                            Unsafe.putLong(address, ts);
                            for (long min : new long[]{Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
                                boolean expected = AbstractAsOfJoinFastRecordCursor.scaleTimestamp(ts, scale) >= min;
                                var actual = code.scan(record, 0, 1, min, 17, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                                assertNotNull(actual);
                                assertEquals("type=" + type + " timestamp=" + ts + " scale=" + scale + " min=" + min,
                                        expected, actual.matched);
                                assertEquals(0, actual.row);
                                assertEquals(expected ? 1 : 0, actual.examined);
                                checks++;
                            }
                        }
                    }
                }
            }
        } finally { Unsafe.free(address, 16, MemoryTag.NATIVE_DEFAULT); }
        assertEquals(functions, LlvmBackend.liveFunctions());
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(270, checks);
    }
}
