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

import io.questdb.cutlass.arrow.ipc.ArrowRecordBatchWriter;
import io.questdb.cutlass.arrow.ipc.ArrowSchemaWriter;
import io.questdb.cutlass.arrow.ipc.FbWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ArrowRecordBatchWriterTest {

    private static final int BUF = 8192;

    @Test
    public void testRecordBatchIsDecodableByArrowJava() {
        long[] values = {1L, 2L, 3L};
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            int len = ArrowRecordBatchWriter.writeInt64RecordBatchMessage(w,
                    values.length, 0L, values.length * 8L);
            Assert.assertTrue(len > 0);
            byte[] bytes = new byte[len];
            long addr = w.finishedAddr();
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            Message msg = Message.getRootAsMessage(ByteBuffer.wrap(bytes));
            Assert.assertEquals(MessageHeader.RecordBatch, msg.headerType());
            Assert.assertEquals(values.length * 8L, msg.bodyLength());

            RecordBatch rb = (RecordBatch) msg.header(new RecordBatch());
            Assert.assertNotNull(rb);
            Assert.assertEquals(values.length, rb.length());
            Assert.assertEquals(1, rb.nodesLength());
            FieldNode node = rb.nodes(0);
            Assert.assertEquals(values.length, node.length());
            Assert.assertEquals(0L, node.nullCount());
            Assert.assertEquals(2, rb.buffersLength());
            Buffer validity = rb.buffers(0);
            Assert.assertEquals(0L, validity.offset());
            Assert.assertEquals(0L, validity.length());
            Buffer valuesBuffer = rb.buffers(1);
            Assert.assertEquals(0L, valuesBuffer.offset());
            Assert.assertEquals(values.length * 8L, valuesBuffer.length());
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRecordBatchRoundTripThroughMessageSerializer() throws Exception {
        long[] values = {1L, 2L, 3L};
        long metadataBuf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long schemaBuf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long nameScratch = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        long bodyBuf = Unsafe.malloc(values.length * 8L, MemoryTag.NATIVE_DEFAULT);
        try {
            // Build the schema message first (needed to interpret the batch).
            FbWriter sw = new FbWriter();
            sw.of(schemaBuf, schemaBuf + BUF);
            int schemaLen = ArrowSchemaWriter.writeInt64SchemaMessage(sw, nameScratch, 64,
                    "col1", 64, true);
            byte[] schemaBytes = new byte[schemaLen];
            for (int i = 0; i < schemaLen; i++) {
                schemaBytes[i] = Unsafe.getUnsafe().getByte(sw.finishedAddr() + i);
            }
            Schema schemaFb = (Schema) Message.getRootAsMessage(ByteBuffer.wrap(schemaBytes)).header(new Schema());
            org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    org.apache.arrow.vector.types.pojo.Schema.convertSchema(schemaFb);

            // Build the record batch metadata.
            FbWriter mw = new FbWriter();
            mw.of(metadataBuf, metadataBuf + BUF);
            int rbLen = ArrowRecordBatchWriter.writeInt64RecordBatchMessage(mw,
                    values.length, 0L, values.length * 8L);
            byte[] metadataBytes = new byte[rbLen];
            for (int i = 0; i < rbLen; i++) {
                metadataBytes[i] = Unsafe.getUnsafe().getByte(mw.finishedAddr() + i);
            }

            // Populate the body buffer with little-endian int64 values.
            for (int i = 0; i < values.length; i++) {
                long v = values[i];
                long a = bodyBuf + i * 8L;
                for (int j = 0; j < 8; j++) {
                    Unsafe.getUnsafe().putByte(a + j, (byte) ((v >>> (j * 8)) & 0xFF));
                }
            }

            // Compose an "encapsulated IPC message" in memory: the
            // standard Arrow stream form has a 4-byte continuation
            // marker + 4-byte metadata length prefix + metadata + body.
            // MessageSerializer.deserializeMessageBatch expects this
            // shape. Easier: call MessageSerializer.deserializeRecordBatch
            // with a pre-parsed Message + ArrowBuf for the body.
            byte[] bodyBytes = new byte[values.length * 8];
            for (int i = 0; i < bodyBytes.length; i++) {
                bodyBytes[i] = Unsafe.getUnsafe().getByte(bodyBuf + i);
            }
            try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
                 org.apache.arrow.vector.VectorSchemaRoot root =
                         org.apache.arrow.vector.VectorSchemaRoot.create(pojoSchema, alloc)) {
                org.apache.arrow.memory.ArrowBuf body = alloc.buffer(bodyBytes.length);
                body.setBytes(0, bodyBytes);
                Message msg = Message.getRootAsMessage(ByteBuffer.wrap(metadataBytes));
                ArrowRecordBatch arb = MessageSerializer.deserializeRecordBatch(msg, body);
                try {
                    Assert.assertEquals(values.length, arb.getLength());
                    Assert.assertEquals(1, arb.getNodes().size());
                    Assert.assertEquals(2, arb.getBuffers().size());
                    org.apache.arrow.vector.VectorLoader loader = new org.apache.arrow.vector.VectorLoader(root);
                    loader.load(arb);
                } finally {
                    arb.close();
                }
                BigIntVector col = (BigIntVector) root.getVector("col1");
                long[] got = new long[values.length];
                for (int i = 0; i < values.length; i++) {
                    got[i] = col.get(i);
                }
                Assert.assertArrayEquals(values, got);
            }
        } finally {
            Unsafe.free(bodyBuf, values.length * 8L, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(nameScratch, 64, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(schemaBuf, BUF, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(metadataBuf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
