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

package io.questdb.cutlass.grpc;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Per-stream gRPC message reassembler. Incoming HTTP/2 DATA bytes (which
 * may split a single gRPC message across frames, or pack several
 * messages into one frame) are appended via {@link #feed(long, int)};
 * {@link #tryReadMessage()} drains one fully-buffered message at a time
 * and exposes it via {@link #lastMessageAddr()} / {@link #lastMessageLen()}.
 * <p>
 * Ownership: the reassembly buffer is a native allocation owned by the
 * reader. Callers reuse one reader per stream and call
 * {@link #clear()} between requests when the reader is pooled back
 * onto its parent context.
 * <p>
 * Error semantics:
 * <ul>
 *   <li>Non-zero compression flag &rarr; {@link #READ_ERROR_COMPRESSED}
 *       (Wave 5 does not negotiate per-message compression).</li>
 *   <li>Declared message length &gt; {@code maxMessageBytes} &rarr;
 *       {@link #READ_ERROR_TOO_LARGE}.</li>
 *   <li>{@link #feed} that would push {@code writeOffset} past the
 *       buffer cap &rarr; reader transitions to sticky overflow; next
 *       {@link #tryReadMessage} returns {@link #READ_ERROR_TOO_LARGE}.</li>
 * </ul>
 */
public final class GrpcFrameReader implements Closeable {

    /**
     * {@link #tryReadMessage()} saw a non-zero compression flag.
     * Caller converts to {@code grpc-status: UNIMPLEMENTED}.
     */
    public static final int READ_ERROR_COMPRESSED = -1;
    /**
     * {@link #tryReadMessage()} saw a declared length above the
     * reader's cap, or {@link #feed(long, int)} overflowed the
     * reassembly buffer. Caller converts to
     * {@code grpc-status: RESOURCE_EXHAUSTED}.
     */
    public static final int READ_ERROR_TOO_LARGE = -2;
    public static final int READ_MESSAGE_READY = 1;
    public static final int READ_NEED_MORE = 0;
    private final long bufferAddr;
    private final int bufferCap;
    private final int maxMessageBytes;
    private final int memoryTag;
    private boolean isOverflow;
    private long lastMessageAddr;
    private int lastMessageLen;
    private int readOffset;
    private int writeOffset;

    public GrpcFrameReader(int maxMessageBytes, int memoryTag) {
        if (maxMessageBytes <= 0) {
            throw new IllegalArgumentException("maxMessageBytes must be positive: " + maxMessageBytes);
        }
        this.maxMessageBytes = maxMessageBytes;
        this.memoryTag = memoryTag;
        // Buffer holds at most one in-flight message + prefix. Multiple
        // messages in one DATA frame are handled by draining via
        // tryReadMessage() between feed calls (or the listener's onData
        // loops feed / tryReadMessage per chunk).
        this.bufferCap = maxMessageBytes + GrpcFrameWriter.PREFIX_LEN;
        this.bufferAddr = Unsafe.malloc(bufferCap, memoryTag);
    }

    /**
     * Resets the reassembly state for a fresh request on the same
     * (pooled) reader.
     */
    public void clear() {
        readOffset = 0;
        writeOffset = 0;
        lastMessageAddr = 0;
        lastMessageLen = 0;
        isOverflow = false;
    }

    @Override
    public void close() {
        Unsafe.free(bufferAddr, bufferCap, memoryTag);
    }

    /**
     * Appends {@code len} bytes from {@code srcAddr} to the reassembly
     * buffer, compacting out already-consumed bytes first if necessary.
     * Returns {@code true} on success, {@code false} on overflow (in
     * which case the next {@link #tryReadMessage()} returns
     * {@link #READ_ERROR_TOO_LARGE}).
     */
    public boolean feed(long srcAddr, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be non-negative: " + len);
        }
        if (isOverflow) {
            return false;
        }
        if (len == 0) {
            return true;
        }
        // Compact if the incoming chunk won't fit at writeOffset but
        // would fit after dropping consumed bytes.
        if (writeOffset + len > bufferCap) {
            compact();
        }
        if (writeOffset + len > bufferCap) {
            isOverflow = true;
            return false;
        }
        Unsafe.getUnsafe().copyMemory(srcAddr, bufferAddr + writeOffset, len);
        writeOffset += len;
        return true;
    }

    public int getMaxMessageBytes() {
        return maxMessageBytes;
    }

    public boolean hasBufferedBytes() {
        return writeOffset > readOffset;
    }

    public long lastMessageAddr() {
        return lastMessageAddr;
    }

    public int lastMessageLen() {
        return lastMessageLen;
    }

    /**
     * Attempts to extract one gRPC message. Returns
     * {@link #READ_MESSAGE_READY} when a full message is available (its
     * body is staged on {@link #lastMessageAddr()} /
     * {@link #lastMessageLen()} until the next {@code tryReadMessage}
     * or {@link #feed} invalidates the staging). Returns
     * {@link #READ_NEED_MORE} when more bytes are needed and
     * {@code READ_ERROR_*} on structural failure.
     */
    public int tryReadMessage() {
        if (isOverflow) {
            return READ_ERROR_TOO_LARGE;
        }
        int available = writeOffset - readOffset;
        if (available < GrpcFrameWriter.PREFIX_LEN) {
            return READ_NEED_MORE;
        }
        long prefixAddr = bufferAddr + readOffset;
        byte flag = Unsafe.getUnsafe().getByte(prefixAddr);
        int msgLen = ((Unsafe.getUnsafe().getByte(prefixAddr + 1) & 0xFF) << 24)
                | ((Unsafe.getUnsafe().getByte(prefixAddr + 2) & 0xFF) << 16)
                | ((Unsafe.getUnsafe().getByte(prefixAddr + 3) & 0xFF) << 8)
                | (Unsafe.getUnsafe().getByte(prefixAddr + 4) & 0xFF);
        if (msgLen < 0 || msgLen > maxMessageBytes) {
            isOverflow = true;
            return READ_ERROR_TOO_LARGE;
        }
        int totalLen = GrpcFrameWriter.PREFIX_LEN + msgLen;
        if (available < totalLen) {
            return READ_NEED_MORE;
        }
        if (flag != 0) {
            return READ_ERROR_COMPRESSED;
        }
        lastMessageAddr = prefixAddr + GrpcFrameWriter.PREFIX_LEN;
        lastMessageLen = msgLen;
        readOffset += totalLen;
        return READ_MESSAGE_READY;
    }

    public static GrpcFrameReader newInstanceForTesting(int maxMessageBytes) {
        return new GrpcFrameReader(maxMessageBytes, MemoryTag.NATIVE_DEFAULT);
    }

    private void compact() {
        int remaining = writeOffset - readOffset;
        if (readOffset == 0 || remaining == 0) {
            readOffset = 0;
            writeOffset = remaining;
            lastMessageAddr = 0;
            lastMessageLen = 0;
            return;
        }
        Unsafe.getUnsafe().copyMemory(bufferAddr + readOffset, bufferAddr, remaining);
        readOffset = 0;
        writeOffset = remaining;
        // Staging pointer into the buffer is no longer valid after
        // compacting; clients must consume lastMessageAddr between
        // tryReadMessage and the next feed / tryReadMessage anyway.
        lastMessageAddr = 0;
        lastMessageLen = 0;
    }
}
