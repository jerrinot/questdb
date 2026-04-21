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
import io.questdb.cutlass.arrow.column.ArrowColumnScratch;
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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ArrowRecordBatchWriterTest {

    private static final int BUF = 8192;
    private static final int NAME_SCRATCH = 64;

    @Test
    public void testAlignTo8() {
        Assert.assertEquals(0L, ArrowRecordBatchWriter.alignTo8(0L));
        Assert.assertEquals(8L, ArrowRecordBatchWriter.alignTo8(1L));
        Assert.assertEquals(8L, ArrowRecordBatchWriter.alignTo8(8L));
        Assert.assertEquals(16L, ArrowRecordBatchWriter.alignTo8(9L));
        Assert.assertEquals(24L, ArrowRecordBatchWriter.alignTo8(20L));
    }

    @Test
    public void testOneColumnInt64Metadata() {
        int[] types = {ColumnType.LONG};
        long rowCount = 3;
        long[] validityLens = {0L};
        long[] valuesLens = {24L};
        long[] nullCounts = {0L};
        long bodyBytes = ArrowRecordBatchWriter.computeBodyBytes(validityLens, valuesLens);
        Assert.assertEquals(24L, bodyBytes);
        byte[] metadata = writeMetadata(rowCount, types, nullCounts, validityLens, valuesLens, bodyBytes);
        Message msg = Message.getRootAsMessage(ByteBuffer.wrap(metadata));
        Assert.assertEquals(MessageHeader.RecordBatch, msg.headerType());
        Assert.assertEquals(bodyBytes, msg.bodyLength());
        RecordBatch rb = (RecordBatch) msg.header(new RecordBatch());
        Assert.assertEquals(rowCount, rb.length());
        Assert.assertEquals(1, rb.nodesLength());
        FieldNode node = rb.nodes(0);
        Assert.assertEquals(rowCount, node.length());
        Assert.assertEquals(0L, node.nullCount());
        Assert.assertEquals(2, rb.buffersLength());
        Buffer validity = rb.buffers(0);
        Assert.assertEquals(0L, validity.offset());
        Assert.assertEquals(0L, validity.length());
        Buffer values = rb.buffers(1);
        Assert.assertEquals(0L, values.offset());
        Assert.assertEquals(24L, values.length());
    }

    @Test
    public void testThreeColumnAlignmentAndOffsets() {
        int[] types = {ColumnType.LONG, ColumnType.DOUBLE, ColumnType.INT};
        long rowCount = 5;
        long[] validityLens = {0L, 0L, 0L};
        long[] valuesLens = {40L, 40L, 20L};
        long[] nullCounts = {0L, 0L, 0L};
        // col0: LONG, 5 * 8 = 40 bytes. Running aligned total = 40.
        // col1: DOUBLE, 40 bytes starting at offset 40. Aligned total = 80.
        // col2: INT, 20 bytes starting at offset 80. Aligned total = 104.
        long expectedBody = 104L;
        Assert.assertEquals(expectedBody, ArrowRecordBatchWriter.computeBodyBytes(validityLens, valuesLens));
        Assert.assertEquals(0L, ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 0));
        Assert.assertEquals(40L, ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 1));
        Assert.assertEquals(80L, ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 2));

        byte[] metadata = writeMetadata(rowCount, types, nullCounts, validityLens, valuesLens, expectedBody);
        Message msg = Message.getRootAsMessage(ByteBuffer.wrap(metadata));
        RecordBatch rb = (RecordBatch) msg.header(new RecordBatch());
        Assert.assertEquals(3, rb.nodesLength());
        Assert.assertEquals(6, rb.buffersLength());
        Assert.assertEquals(0L, rb.buffers(1).offset());
        Assert.assertEquals(40L, rb.buffers(1).length());
        Assert.assertEquals(40L, rb.buffers(3).offset());
        Assert.assertEquals(40L, rb.buffers(3).length());
        Assert.assertEquals(80L, rb.buffers(5).offset());
        Assert.assertEquals(20L, rb.buffers(5).length());
    }

    @Test
    public void testThreeColumnBatchDecodedByVectorLoader() throws Exception {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("x", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("y", ColumnType.DOUBLE));
        metadata.add(new TableColumnMetadata("z", ColumnType.INT));

        long[] longs = {1L, 2L, 3L, 4L, 5L};
        double[] doubles = {0.5, 1.0, 1.5, 2.0, 2.5};
        int[] ints = {10, 20, 30, 40, 50};
        int rowCount = longs.length;
        int[] types = {ColumnType.LONG, ColumnType.DOUBLE, ColumnType.INT};
        long[] validityLens = {0L, 0L, 0L};
        long[] valuesLens = {40L, 40L, 20L};
        long[] nullCounts = {0L, 0L, 0L};
        long bodyBytes = ArrowRecordBatchWriter.computeBodyBytes(validityLens, valuesLens);

        long body = Unsafe.malloc(bodyBytes, MemoryTag.NATIVE_DEFAULT);
        ArrowColumnScratch longScratch = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
        ArrowColumnScratch doubleScratch = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
        ArrowColumnScratch intScratch = new ArrowColumnScratch(MemoryTag.NATIVE_DEFAULT);
        try {
            longScratch.initFor(ColumnType.LONG, rowCount);
            for (long v : longs) longScratch.appendLong(v);
            doubleScratch.initFor(ColumnType.DOUBLE, rowCount);
            for (double v : doubles) doubleScratch.appendDouble(v);
            intScratch.initFor(ColumnType.INT, rowCount);
            for (int v : ints) intScratch.appendInt(v);

            long off0 = ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 0);
            longScratch.flushValuesTo(body + off0);
            long off1 = ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 1);
            doubleScratch.flushValuesTo(body + off1);
            long off2 = ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 2);
            intScratch.flushValuesTo(body + off2);

            byte[] schemaBytes = writeSchemaBytes(metadata);
            byte[] batchBytes = writeMetadata(rowCount, types, nullCounts, validityLens, valuesLens, bodyBytes);

            Schema schemaFb = (Schema) Message.getRootAsMessage(ByteBuffer.wrap(schemaBytes)).header(new Schema());
            org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    org.apache.arrow.vector.types.pojo.Schema.convertSchema(schemaFb);

            try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
                 VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, alloc)) {
                ArrowBuf bodyBuf = alloc.buffer(bodyBytes);
                byte[] bodyBytesArr = new byte[(int) bodyBytes];
                for (int i = 0; i < bodyBytesArr.length; i++) {
                    bodyBytesArr[i] = Unsafe.getUnsafe().getByte(body + i);
                }
                bodyBuf.setBytes(0, bodyBytesArr);
                Message msg = Message.getRootAsMessage(ByteBuffer.wrap(batchBytes));
                ArrowRecordBatch arb = MessageSerializer.deserializeRecordBatch(msg, bodyBuf);
                try {
                    VectorLoader loader = new VectorLoader(root);
                    loader.load(arb);
                } finally {
                    arb.close();
                }
                BigIntVector xv = (BigIntVector) root.getVector("x");
                Float8Vector yv = (Float8Vector) root.getVector("y");
                IntVector zv = (IntVector) root.getVector("z");
                Assert.assertEquals(rowCount, xv.getValueCount());
                for (int i = 0; i < rowCount; i++) {
                    Assert.assertEquals(longs[i], xv.get(i));
                    Assert.assertEquals(doubles[i], yv.get(i), 0.0);
                    Assert.assertEquals(ints[i], zv.get(i));
                }
            }
        } finally {
            longScratch.close();
            doubleScratch.close();
            intScratch.close();
            Unsafe.free(body, bodyBytes, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testValidityBufferEmittedForNullColumn() {
        int[] types = {ColumnType.LONG, ColumnType.LONG};
        long rowCount = 5;
        long[] validityLens = {1L, 0L};
        long[] valuesLens = {40L, 40L};
        long[] nullCounts = {2L, 0L};
        long expectedBody = ArrowRecordBatchWriter.computeBodyBytes(validityLens, valuesLens);
        // col0 validity 1B + pad(7) + values 40B = 48. col1 validity 0 + values 40 = 40. total 88.
        Assert.assertEquals(88L, expectedBody);
        Assert.assertEquals(0L, ArrowRecordBatchWriter.computeColumnValidityOffset(validityLens, valuesLens, 0));
        Assert.assertEquals(8L, ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 0));
        Assert.assertEquals(48L, ArrowRecordBatchWriter.computeColumnValidityOffset(validityLens, valuesLens, 1));
        Assert.assertEquals(48L, ArrowRecordBatchWriter.computeColumnValuesOffset(validityLens, valuesLens, 1));

        byte[] metadata = writeMetadata(rowCount, types, nullCounts, validityLens, valuesLens, expectedBody);
        Message msg = Message.getRootAsMessage(ByteBuffer.wrap(metadata));
        RecordBatch rb = (RecordBatch) msg.header(new RecordBatch());
        Assert.assertEquals(2L, rb.nodes(0).nullCount());
        Assert.assertEquals(0L, rb.buffers(0).offset());
        Assert.assertEquals(1L, rb.buffers(0).length());
        Assert.assertEquals(8L, rb.buffers(1).offset());
        Assert.assertEquals(40L, rb.buffers(1).length());
        Assert.assertEquals(48L, rb.buffers(2).offset());
        Assert.assertEquals(0L, rb.buffers(2).length());
        Assert.assertEquals(48L, rb.buffers(3).offset());
        Assert.assertEquals(40L, rb.buffers(3).length());
    }

    private static byte[] writeMetadata(long rowCount, int[] types, long[] nullCounts,
                                        long[] validityLens, long[] valuesLens, long bodyBytes) {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            int len = ArrowRecordBatchWriter.writeRecordBatchMessage(w, rowCount, types,
                    nullCounts, validityLens, valuesLens, bodyBytes);
            Assert.assertTrue(len > 0);
            byte[] bytes = new byte[len];
            long addr = w.finishedAddr();
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            return bytes;
        } finally {
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] writeSchemaBytes(GenericRecordMetadata metadata) {
        long buf = Unsafe.malloc(BUF, MemoryTag.NATIVE_DEFAULT);
        long name = Unsafe.malloc(NAME_SCRATCH, MemoryTag.NATIVE_DEFAULT);
        try {
            FbWriter w = new FbWriter();
            w.of(buf, buf + BUF);
            int len = ArrowSchemaWriter.writeSchemaMessage(w, name, NAME_SCRATCH, metadata);
            Assert.assertTrue(len > 0);
            byte[] bytes = new byte[len];
            long addr = w.finishedAddr();
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
            }
            return bytes;
        } finally {
            Unsafe.free(name, NAME_SCRATCH, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(buf, BUF, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
