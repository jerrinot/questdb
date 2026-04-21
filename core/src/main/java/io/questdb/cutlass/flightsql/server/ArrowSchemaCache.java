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

package io.questdb.cutlass.flightsql.server;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cutlass.arrow.ipc.ArrowSchemaWriter;
import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Cached, dispatcher-scoped Arrow IPC {@code Schema} message bytes for
 * the placeholder Wave 6a response shape: a single {@code Int64} column
 * named {@code "col1"}. Built once at dispatcher construction; read-only
 * after that.
 * <p>
 * Wave 6b retains this cache for the scaffolded GetFlightInfo path that
 * still returns the cached single-column schema; the cursor-driven
 * schema derivation from a compiled {@code RecordCursorFactory} lands
 * with the {@code SqlCompiler} integration step.
 * <p>
 * The buffer is native-allocated under the supplied memory tag. Callers
 * treat the returned {@code (addr, len)} pair as borrowed; the cache
 * keeps ownership and frees on {@link #close()}.
 */
public final class ArrowSchemaCache implements Closeable {

    private static final int SCHEMA_BUILD_CAP = 4096;
    private final int cap;
    private final int memoryTag;
    private final long schemaAddr;
    private final int schemaLen;
    private boolean isClosed;

    public ArrowSchemaCache(String columnName, int memoryTag) {
        if (columnName == null) {
            throw new IllegalArgumentException("columnName must be non-null");
        }
        this.memoryTag = memoryTag;
        this.cap = SCHEMA_BUILD_CAP;
        long scratch = Unsafe.malloc(cap, memoryTag);
        long nameScratch = Unsafe.malloc(256, memoryTag);
        try {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata(columnName, ColumnType.LONG));
            FbWriter w = new FbWriter();
            w.of(scratch, scratch + cap);
            int len = ArrowSchemaWriter.writeSchemaMessage(w, nameScratch, 256, metadata);
            if (len <= 0) {
                throw new IllegalStateException("schema scratch too small");
            }
            long dst = Unsafe.malloc(len, memoryTag);
            Unsafe.getUnsafe().copyMemory(w.finishedAddr(), dst, len);
            this.schemaAddr = dst;
            this.schemaLen = len;
        } finally {
            Unsafe.free(nameScratch, 256, memoryTag);
            Unsafe.free(scratch, cap, memoryTag);
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        Unsafe.free(schemaAddr, schemaLen, memoryTag);
    }

    public long getSchemaAddr() {
        return schemaAddr;
    }

    public int getSchemaLen() {
        return schemaLen;
    }
}
