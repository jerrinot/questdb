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

package io.questdb.cutlass.http2;

import io.questdb.std.Unsafe;

/**
 * HTTP/2 frame writer (RFC 7540 sec. 4 and 6).
 * <p>
 * Each method writes exactly one frame into a caller-owned native buffer
 * {@code [addr, limit)}. If the remaining space is insufficient for the full
 * frame, the method returns {@code -1}; the caller drains the buffer and
 * retries the same call unchanged (same {@code QwpEgressFrameWriter}
 * backpressure contract). Otherwise the new write pointer is returned.
 * <p>
 * Frame payload fragmentation (a gRPC message longer than the peer's
 * {@code SETTINGS_MAX_FRAME_SIZE}, or a header block longer than the same
 * bound) is the caller's responsibility: the caller slices the payload into
 * chunks of at most {@code peerMaxFrameSize} bytes and emits a sequence of
 * {@link #writeData}, {@link #writeHeaders}, or {@link #writeContinuation}
 * calls, setting the terminal flag ({@code END_STREAM}, {@code END_HEADERS})
 * only on the final chunk. This keeps the writer stateless and aligned with
 * the resumption model documented in {@code HTTP2_FRAME_CODEC.md} sec. 8.
 */
public final class Http2FrameWriter {

    private Http2FrameWriter() {
    }

    /**
     * Writes a CONTINUATION frame.
     *
     * @param blockAddr  native address of the (possibly partial) header block
     * @param blockLen   number of header block bytes to carry in this frame
     */
    public static long writeContinuation(long addr, long limit, int streamId,
                                         boolean endHeaders, long blockAddr, int blockLen) {
        requireStreamScoped(streamId, "CONTINUATION");
        if (blockLen < 0 || blockLen > Http2Settings.MAX_FRAME_SIZE_UPPER) {
            throw new IllegalArgumentException("blockLen out of range: " + blockLen);
        }
        long end = addr + Http2FrameHeader.SIZE + blockLen;
        if (end > limit) {
            return -1;
        }
        byte flags = endHeaders ? Http2Flags.END_HEADERS : Http2Flags.NONE;
        Http2FrameHeader.write(addr, blockLen, Http2FrameType.CONTINUATION, flags, streamId);
        if (blockLen > 0) {
            Unsafe.getUnsafe().copyMemory(blockAddr, addr + Http2FrameHeader.SIZE, blockLen);
        }
        return end;
    }

    /**
     * Writes a DATA frame carrying {@code payloadLen} bytes from
     * {@code payloadAddr}. The caller must set {@code endStream} only on the
     * final chunk of a logical gRPC message.
     *
     * @return new write pointer, or {@code -1} if the remaining buffer space
     * is insufficient for header + payload
     */
    public static long writeData(long addr, long limit, int streamId,
                                 boolean endStream, long payloadAddr, int payloadLen) {
        requireStreamScoped(streamId, "DATA");
        if (payloadLen < 0 || payloadLen > Http2Settings.MAX_FRAME_SIZE_UPPER) {
            throw new IllegalArgumentException("payloadLen out of range: " + payloadLen);
        }
        long end = addr + Http2FrameHeader.SIZE + payloadLen;
        if (end > limit) {
            return -1;
        }
        byte flags = endStream ? Http2Flags.END_STREAM : Http2Flags.NONE;
        Http2FrameHeader.write(addr, payloadLen, Http2FrameType.DATA, flags, streamId);
        if (payloadLen > 0) {
            Unsafe.getUnsafe().copyMemory(payloadAddr, addr + Http2FrameHeader.SIZE, payloadLen);
        }
        return end;
    }

    /**
     * Writes a GOAWAY frame. Debug data is optional and may be empty.
     *
     * @param lastStreamId last peer-initiated stream id the server processed
     * @param errorCode    one of {@link Http2ErrorCode}
     * @param debugAddr    optional ASCII debug data, or 0
     * @param debugLen     length of debug data, or 0
     */
    public static long writeGoAway(long addr, long limit, int lastStreamId, int errorCode,
                                   long debugAddr, int debugLen) {
        if (lastStreamId < 0 || lastStreamId > 0x7FFFFFFF) {
            throw new IllegalArgumentException("lastStreamId out of range: " + lastStreamId);
        }
        if (debugLen < 0 || debugLen > Http2Settings.MAX_FRAME_SIZE_UPPER - 8) {
            throw new IllegalArgumentException("debugLen out of range: " + debugLen);
        }
        int payloadLen = 8 + debugLen;
        long end = addr + Http2FrameHeader.SIZE + payloadLen;
        if (end > limit) {
            return -1;
        }
        Http2FrameHeader.write(addr, payloadLen, Http2FrameType.GOAWAY, Http2Flags.NONE, 0);
        long p = addr + Http2FrameHeader.SIZE;
        putU32(p, lastStreamId & 0x7FFFFFFF);
        putU32(p + 4, errorCode);
        if (debugLen > 0) {
            Unsafe.getUnsafe().copyMemory(debugAddr, p + 8, debugLen);
        }
        return end;
    }

    /**
     * Writes a HEADERS frame carrying (part of) an HPACK-encoded header block.
     * The caller is responsible for splitting an oversize block across
     * {@code HEADERS} + one or more {@link #writeContinuation} frames; only
     * the last in the sequence should have {@code endHeaders = true}.
     * <p>
     * This writer never emits the {@code PADDED} or {@code PRIORITY} flags.
     */
    public static long writeHeaders(long addr, long limit, int streamId,
                                    boolean endStream, boolean endHeaders,
                                    long blockAddr, int blockLen) {
        requireStreamScoped(streamId, "HEADERS");
        if (blockLen < 0 || blockLen > Http2Settings.MAX_FRAME_SIZE_UPPER) {
            throw new IllegalArgumentException("blockLen out of range: " + blockLen);
        }
        long end = addr + Http2FrameHeader.SIZE + blockLen;
        if (end > limit) {
            return -1;
        }
        byte flags = Http2Flags.NONE;
        if (endStream) {
            flags |= Http2Flags.END_STREAM;
        }
        if (endHeaders) {
            flags |= Http2Flags.END_HEADERS;
        }
        Http2FrameHeader.write(addr, blockLen, Http2FrameType.HEADERS, flags, streamId);
        if (blockLen > 0) {
            Unsafe.getUnsafe().copyMemory(blockAddr, addr + Http2FrameHeader.SIZE, blockLen);
        }
        return end;
    }

    /**
     * Writes a PING frame.
     *
     * @param ack        {@code true} to echo a peer PING, {@code false} to
     *                   originate one
     * @param opaqueData 8-byte opaque identifier; echoed verbatim by the peer
     */
    public static long writePing(long addr, long limit, boolean ack, long opaqueData) {
        long end = addr + Http2FrameHeader.SIZE + 8;
        if (end > limit) {
            return -1;
        }
        byte flags = ack ? Http2Flags.ACK : Http2Flags.NONE;
        Http2FrameHeader.write(addr, 8, Http2FrameType.PING, flags, 0);
        putU64(addr + Http2FrameHeader.SIZE, opaqueData);
        return end;
    }

    /**
     * Writes a RST_STREAM frame.
     */
    public static long writeRstStream(long addr, long limit, int streamId, int errorCode) {
        requireStreamScoped(streamId, "RST_STREAM");
        long end = addr + Http2FrameHeader.SIZE + 4;
        if (end > limit) {
            return -1;
        }
        Http2FrameHeader.write(addr, 4, Http2FrameType.RST_STREAM, Http2Flags.NONE, streamId);
        putU32(addr + Http2FrameHeader.SIZE, errorCode);
        return end;
    }

    /**
     * Writes a SETTINGS frame carrying {@code count} identifier/value pairs.
     * Arrays are caller-owned and read but not retained.
     */
    public static long writeSettings(long addr, long limit, short[] ids, int[] values, int count) {
        if (count < 0 || count > ids.length || count > values.length
                || count > Http2Settings.MAX_FRAME_SIZE_UPPER / 6) {
            throw new IllegalArgumentException("count out of range");
        }
        // Validate each (id, value) against the setting's legal range before
        // committing bytes; a peer would trip a connection PROTOCOL_ERROR /
        // FLOW_CONTROL_ERROR on the offending value, so we reject the send
        // outright instead of emitting a frame we know the peer must close on.
        // SETTINGS values are unsigned 32-bit on the wire (RFC 7540 sec. 6.5.2),
        // so the caller's int values[] encode those unsigned bits directly; widen
        // through the unsigned conversion before range-checking so a top-bit-set
        // raw int like 0x80000000 validates as the positive 2_147_483_648 rather
        // than as the negative int that the signed widening would produce.
        for (int i = 0; i < count; i++) {
            int code = Http2Settings.validate(ids[i], values[i] & 0xFFFFFFFFL);
            if (code != Http2ErrorCode.NO_ERROR) {
                throw new IllegalArgumentException("invalid SETTINGS id=" + ids[i] + " value=" + values[i]
                        + " (" + Http2ErrorCode.nameOf(code) + ")");
            }
        }
        int payloadLen = count * 6;
        long end = addr + Http2FrameHeader.SIZE + payloadLen;
        if (end > limit) {
            return -1;
        }
        Http2FrameHeader.write(addr, payloadLen, Http2FrameType.SETTINGS, Http2Flags.NONE, 0);
        long p = addr + Http2FrameHeader.SIZE;
        for (int i = 0; i < count; i++) {
            short id = ids[i];
            int value = values[i];
            Unsafe.getUnsafe().putByte(p, (byte) ((id >>> 8) & 0xFF));
            Unsafe.getUnsafe().putByte(p + 1, (byte) (id & 0xFF));
            putU32(p + 2, value);
            p += 6;
        }
        return end;
    }

    /**
     * Writes a SETTINGS frame with the ACK flag set (empty payload).
     */
    public static long writeSettingsAck(long addr, long limit) {
        long end = addr + Http2FrameHeader.SIZE;
        if (end > limit) {
            return -1;
        }
        Http2FrameHeader.write(addr, 0, Http2FrameType.SETTINGS, Http2Flags.ACK, 0);
        return end;
    }

    /**
     * Writes a WINDOW_UPDATE frame.
     *
     * @param streamId  0 for the connection-level window, otherwise a stream id
     * @param increment strictly positive; RFC 7540 sec. 6.9 forbids 0
     */
    public static long writeWindowUpdate(long addr, long limit, int streamId, int increment) {
        if (streamId < 0 || streamId > 0x7FFFFFFF) {
            throw new IllegalArgumentException("streamId out of range: " + streamId);
        }
        if (increment <= 0 || increment > 0x7FFFFFFF) {
            throw new IllegalArgumentException("increment out of range");
        }
        long end = addr + Http2FrameHeader.SIZE + 4;
        if (end > limit) {
            return -1;
        }
        Http2FrameHeader.write(addr, 4, Http2FrameType.WINDOW_UPDATE, Http2Flags.NONE, streamId);
        putU32(addr + Http2FrameHeader.SIZE, increment & 0x7FFFFFFF);
        return end;
    }

    private static void putU32(long addr, int v) {
        Unsafe.getUnsafe().putByte(addr, (byte) ((v >>> 24) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 1, (byte) ((v >>> 16) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 2, (byte) ((v >>> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(addr + 3, (byte) (v & 0xFF));
    }

    private static void putU64(long addr, long v) {
        putU32(addr, (int) (v >>> 32));
        putU32(addr + 4, (int) v);
    }

    private static void requireStreamScoped(int streamId, String frameName) {
        if (streamId <= 0 || streamId > 0x7FFFFFFF) {
            throw new IllegalArgumentException(frameName + " requires a positive stream id, got " + streamId);
        }
    }

}
