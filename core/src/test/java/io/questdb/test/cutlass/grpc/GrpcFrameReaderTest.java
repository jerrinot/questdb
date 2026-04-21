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

package io.questdb.test.cutlass.grpc;

import io.questdb.cutlass.grpc.GrpcFrameReader;
import io.questdb.cutlass.grpc.GrpcFrameWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class GrpcFrameReaderTest {

    private static final int MAX_MSG = 4096;

    @Test
    public void testCompressedFlagRejected() {
        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 1);
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 4, (byte) 3);
            Unsafe.getUnsafe().putByte(buf + 5, (byte) 'a');
            Unsafe.getUnsafe().putByte(buf + 6, (byte) 'b');
            Unsafe.getUnsafe().putByte(buf + 7, (byte) 'c');

            r = new GrpcFrameReader(MAX_MSG, MemoryTag.NATIVE_DEFAULT);
            Assert.assertTrue(r.feed(buf, 8));
            Assert.assertEquals(GrpcFrameReader.READ_ERROR_COMPRESSED, r.tryReadMessage());
        } finally {
            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }

    @Test
    public void testDeclaredLengthAboveCap() {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0);
            // 0x00 00 01 00 = 256 bytes declared
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 0);
            Unsafe.getUnsafe().putByte(buf + 3, (byte) 1);
            Unsafe.getUnsafe().putByte(buf + 4, (byte) 0);

            r = new GrpcFrameReader(128, MemoryTag.NATIVE_DEFAULT);
            Assert.assertTrue(r.feed(buf, 5));
            Assert.assertEquals(GrpcFrameReader.READ_ERROR_TOO_LARGE, r.tryReadMessage());
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }

    @Test
    public void testFeedOverflow() {
        // maxMessageBytes=4 → buffer cap = 9. Feed 10 bytes → overflow.
        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            for (int i = 0; i < 32; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) i);
            }
            r = new GrpcFrameReader(4, MemoryTag.NATIVE_DEFAULT);
            Assert.assertFalse(r.feed(buf, 10));
            Assert.assertEquals(GrpcFrameReader.READ_ERROR_TOO_LARGE, r.tryReadMessage());
        } finally {
            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }

    @Test
    public void testPartialAcrossThreeFeeds() {
        long msgBuf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            byte[] body = "ABCDEFGH".getBytes();
            long cursor = GrpcFrameWriter.writePrefix(msgBuf, body.length);
            for (int i = 0; i < body.length; i++) {
                Unsafe.getUnsafe().putByte(cursor + i, body[i]);
            }
            int total = 5 + body.length;

            r = new GrpcFrameReader(MAX_MSG, MemoryTag.NATIVE_DEFAULT);
            Assert.assertTrue(r.feed(msgBuf, 2));
            Assert.assertEquals(GrpcFrameReader.READ_NEED_MORE, r.tryReadMessage());
            Assert.assertTrue(r.feed(msgBuf + 2, 6));
            Assert.assertEquals(GrpcFrameReader.READ_NEED_MORE, r.tryReadMessage());
            Assert.assertTrue(r.feed(msgBuf + 8, total - 8));
            Assert.assertEquals(GrpcFrameReader.READ_MESSAGE_READY, r.tryReadMessage());
            Assert.assertEquals(body.length, r.lastMessageLen());
            for (int i = 0; i < body.length; i++) {
                Assert.assertEquals(body[i], Unsafe.getUnsafe().getByte(r.lastMessageAddr() + i));
            }
        } finally {
            Unsafe.free(msgBuf, 32, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }

    @Test
    public void testSingleMessageInOneFeed() {
        long msgBuf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            byte[] body = "hello".getBytes();
            long cursor = GrpcFrameWriter.writePrefix(msgBuf, body.length);
            for (int i = 0; i < body.length; i++) {
                Unsafe.getUnsafe().putByte(cursor + i, body[i]);
            }
            int total = 5 + body.length;

            r = new GrpcFrameReader(MAX_MSG, MemoryTag.NATIVE_DEFAULT);
            Assert.assertTrue(r.feed(msgBuf, total));
            Assert.assertEquals(GrpcFrameReader.READ_MESSAGE_READY, r.tryReadMessage());
            Assert.assertEquals(body.length, r.lastMessageLen());
            for (int i = 0; i < body.length; i++) {
                Assert.assertEquals(body[i], Unsafe.getUnsafe().getByte(r.lastMessageAddr() + i));
            }
            Assert.assertEquals(GrpcFrameReader.READ_NEED_MORE, r.tryReadMessage());
        } finally {
            Unsafe.free(msgBuf, 64, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }

    @Test
    public void testSingleMessageSplitAcrossTwoFeeds() {
        long msgBuf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            byte[] body = "hello Flight SQL".getBytes();
            long cursor = GrpcFrameWriter.writePrefix(msgBuf, body.length);
            for (int i = 0; i < body.length; i++) {
                Unsafe.getUnsafe().putByte(cursor + i, body[i]);
            }
            int total = 5 + body.length;

            r = new GrpcFrameReader(MAX_MSG, MemoryTag.NATIVE_DEFAULT);
            Assert.assertTrue(r.feed(msgBuf, 3));
            Assert.assertEquals(GrpcFrameReader.READ_NEED_MORE, r.tryReadMessage());
            Assert.assertTrue(r.feed(msgBuf + 3, total - 3));
            Assert.assertEquals(GrpcFrameReader.READ_MESSAGE_READY, r.tryReadMessage());
            Assert.assertEquals(body.length, r.lastMessageLen());
            for (int i = 0; i < body.length; i++) {
                Assert.assertEquals(body[i], Unsafe.getUnsafe().getByte(r.lastMessageAddr() + i));
            }
        } finally {
            Unsafe.free(msgBuf, 64, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }

    @Test
    public void testTwoMessagesInOneFeed() {
        long msgBuf = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            byte[] first = "first".getBytes();
            byte[] second = "second-msg".getBytes();
            long c = GrpcFrameWriter.writePrefix(msgBuf, first.length);
            for (int i = 0; i < first.length; i++) {
                Unsafe.getUnsafe().putByte(c + i, first[i]);
            }
            c = GrpcFrameWriter.writePrefix(c + first.length, second.length);
            for (int i = 0; i < second.length; i++) {
                Unsafe.getUnsafe().putByte(c + i, second[i]);
            }
            int total = 10 + first.length + second.length;

            r = new GrpcFrameReader(MAX_MSG, MemoryTag.NATIVE_DEFAULT);
            Assert.assertTrue(r.feed(msgBuf, total));

            Assert.assertEquals(GrpcFrameReader.READ_MESSAGE_READY, r.tryReadMessage());
            Assert.assertEquals(first.length, r.lastMessageLen());
            for (int i = 0; i < first.length; i++) {
                Assert.assertEquals(first[i], Unsafe.getUnsafe().getByte(r.lastMessageAddr() + i));
            }

            Assert.assertEquals(GrpcFrameReader.READ_MESSAGE_READY, r.tryReadMessage());
            Assert.assertEquals(second.length, r.lastMessageLen());
            for (int i = 0; i < second.length; i++) {
                Assert.assertEquals(second[i], Unsafe.getUnsafe().getByte(r.lastMessageAddr() + i));
            }

            Assert.assertEquals(GrpcFrameReader.READ_NEED_MORE, r.tryReadMessage());
        } finally {
            Unsafe.free(msgBuf, 128, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }

    @Test
    public void testZeroLengthMessage() {
        long msgBuf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        GrpcFrameReader r = null;
        try {
            GrpcFrameWriter.writePrefix(msgBuf, 0);
            r = new GrpcFrameReader(MAX_MSG, MemoryTag.NATIVE_DEFAULT);
            Assert.assertTrue(r.feed(msgBuf, 5));
            Assert.assertEquals(GrpcFrameReader.READ_MESSAGE_READY, r.tryReadMessage());
            Assert.assertEquals(0, r.lastMessageLen());
            Assert.assertEquals(GrpcFrameReader.READ_NEED_MORE, r.tryReadMessage());
        } finally {
            Unsafe.free(msgBuf, 16, MemoryTag.NATIVE_DEFAULT);
            if (r != null) {
                r.close();
            }
        }
    }
}
