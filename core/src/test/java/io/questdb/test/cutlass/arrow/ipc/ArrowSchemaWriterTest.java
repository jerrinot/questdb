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

package io.questdb.test.cutlass.arrow.ipc;

import io.questdb.cutlass.arrow.ipc.ArrowSchemaWriter;
import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flatbuf.Type;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ArrowSchemaWriterTest {

    private static final int BUF = 8192;

    @Test
    public void testSchemaIsDecodableByArrowJava() {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long nameScratch = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            int len = ArrowSchemaWriter.writeInt64SchemaMessage(w, nameScratch, 64, "col1", 64, true);
            Assert.assertTrue(len > 0);
            byte[] bytes = new byte[len];
            long addr = w.finishedAddr();
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            Message msg = Message.getRootAsMessage(bb);
            Assert.assertEquals(MetadataVersion.V5, msg.version());
            Assert.assertEquals(MessageHeader.Schema, msg.headerType());
            Assert.assertEquals(0L, msg.bodyLength());
            Schema schema = (Schema) msg.header(new Schema());
            Assert.assertNotNull(schema);
            Assert.assertEquals(1, schema.fieldsLength());
            org.apache.arrow.flatbuf.Field field = schema.fields(0);
            Assert.assertEquals("col1", field.name());
            Assert.assertFalse(field.nullable());
            Assert.assertEquals(Type.Int, field.typeType());
            Int intType = (Int) field.type(new Int());
            Assert.assertEquals(64, intType.bitWidth());
            Assert.assertTrue(intType.isSigned());
        } finally {
            Unsafe.free(nameScratch, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testSchemaWithDifferentColumnName() {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long nameScratch = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            int len = ArrowSchemaWriter.writeInt64SchemaMessage(w, nameScratch, 64,
                    "the_value_column", 64, true);
            Assert.assertTrue(len > 0);
            byte[] bytes = new byte[len];
            long addr = w.finishedAddr();
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            Message msg = Message.getRootAsMessage(ByteBuffer.wrap(bytes));
            Schema schema = (Schema) msg.header(new Schema());
            Assert.assertEquals("the_value_column", schema.fields(0).name());
        } finally {
            Unsafe.free(nameScratch, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
