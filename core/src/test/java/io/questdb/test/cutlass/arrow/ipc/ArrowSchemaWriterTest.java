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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cutlass.arrow.ipc.ArrowSchemaWriter;
import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.cutlass.arrow.ipc.UnsupportedColumnTypeException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.apache.arrow.flatbuf.FloatingPoint;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flatbuf.Type;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ArrowSchemaWriterTest {

    private static final int BUF = 8192;
    private static final int NAME_SCRATCH = 64;

    @Test
    public void testSchemaMessageOneColumnInt64() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("col1", ColumnType.LONG));
        byte[] bytes = writeToBytes(metadata);
        Message msg = Message.getRootAsMessage(ByteBuffer.wrap(bytes));
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
    }

    @Test
    public void testSchemaMessageTwoColumnsLongDouble() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("y", ColumnType.DOUBLE));
        byte[] bytes = writeToBytes(metadata);
        Message msg = Message.getRootAsMessage(ByteBuffer.wrap(bytes));
        Schema schema = (Schema) msg.header(new Schema());
        Assert.assertEquals(2, schema.fieldsLength());

        Assert.assertEquals("x", schema.fields(0).name());
        Assert.assertEquals(Type.Int, schema.fields(0).typeType());
        Int i0 = (Int) schema.fields(0).type(new Int());
        Assert.assertEquals(64, i0.bitWidth());

        Assert.assertEquals("y", schema.fields(1).name());
        Assert.assertEquals(Type.FloatingPoint, schema.fields(1).typeType());
        FloatingPoint f = (FloatingPoint) schema.fields(1).type(new FloatingPoint());
        Assert.assertEquals(Precision.DOUBLE, f.precision());
    }

    @Test
    public void testSchemaMessageThreeColumnsLongDoubleInt() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("y", ColumnType.DOUBLE));
        metadata.add(new TableColumnMetadata("z", ColumnType.INT));
        byte[] bytes = writeToBytes(metadata);
        Message msg = Message.getRootAsMessage(ByteBuffer.wrap(bytes));
        Schema schema = (Schema) msg.header(new Schema());
        Assert.assertEquals(3, schema.fieldsLength());

        Int i0 = (Int) schema.fields(0).type(new Int());
        Assert.assertEquals(64, i0.bitWidth());

        FloatingPoint f = (FloatingPoint) schema.fields(1).type(new FloatingPoint());
        Assert.assertEquals(Precision.DOUBLE, f.precision());

        Assert.assertEquals(Type.Int, schema.fields(2).typeType());
        Int i2 = (Int) schema.fields(2).type(new Int());
        Assert.assertEquals(32, i2.bitWidth());
        Assert.assertTrue(i2.isSigned());

        Assert.assertEquals("z", schema.fields(2).name());
    }

    @Test(expected = UnsupportedColumnTypeException.class)
    public void testSchemaMessageThrowsOnUnsupportedStringColumn() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("s", ColumnType.STRING));
        writeToBytes(metadata);
    }

    private static byte[] writeToBytes(GenericRecordMetadata metadata) {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long nameScratch = Unsafe.malloc(NAME_SCRATCH, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            int len = ArrowSchemaWriter.writeSchemaMessage(w, nameScratch, NAME_SCRATCH, metadata);
            if (len <= 0) {
                throw new AssertionError("writeSchemaMessage returned " + len);
            }
            byte[] bytes = new byte[len];
            long addr = w.finishedAddr();
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            return bytes;
        } finally {
            Unsafe.free(nameScratch, NAME_SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
