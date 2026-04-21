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
 * HTTP/2 frame reader (RFC 7540 sec. 4 and 6).
 * <p>
 * Parses the 9-byte frame header, enforces structural invariants per-frame
 * (header fields + payload-level checks that don't require HPACK), and tracks
 * the sliver of cross-frame state needed to reject malformed CONTINUATION
 * sequences (RFC 7540 sec. 6.10).
 * <p>
 * One instance per connection. Callers must invoke {@link #tryReadNext} on
 * every successfully framed byte range; skipping frames would desynchronise
 * the continuation tracker.
 * <p>
 * Per the first-milestone buffer model in {@code HTTP2_FRAME_CODEC.md} sec. 8,
 * the caller guarantees the receive buffer is at least
 * {@code 9 + inboundMaxFrameSize} bytes, so a frame is either fully present or
 * fully absent in the buffer. The reader returns the whole frame size on
 * success (header + payload) and {@code 0} if either the header or the payload
 * is not yet fully buffered.
 * <p>
 * HPACK decoding of HEADERS / CONTINUATION payloads lives in a separate
 * layer; this class validates only what is visible on the 9-byte header plus
 * the fixed-field invariants from sec. 6.
 */
public final class Http2FrameReader {

    /**
     * {@code -1} when the previous frame did not leave an open header block;
     * otherwise the stream id of the pending HEADERS / CONTINUATION sequence.
     */
    private int continuationStreamId = -1;

    public static int readGoAwayErrorCode(long payloadAddr) {
        return readU32(payloadAddr + 4);
    }

    public static int readGoAwayLastStreamId(long payloadAddr) {
        return readU32(payloadAddr) & 0x7FFFFFFF;
    }

    public static int readRstStreamErrorCode(long payloadAddr) {
        return readU32(payloadAddr);
    }

    public static short readSettingsId(long entryAddr) {
        int b0 = Unsafe.getUnsafe().getByte(entryAddr) & 0xFF;
        int b1 = Unsafe.getUnsafe().getByte(entryAddr + 1) & 0xFF;
        return (short) ((b0 << 8) | b1);
    }

    /**
     * Reads the 32-bit SETTINGS value as an unsigned long. SETTINGS values are
     * unsigned on the wire (RFC 7540 sec. 6.5.2); returning {@code long} keeps
     * top-bit-set values positive so downstream validation and clamping see
     * the correct magnitude.
     */
    public static long readSettingsValue(long entryAddr) {
        return readU32(entryAddr + 2) & 0xFFFFFFFFL;
    }

    /**
     * Reads the 32-bit WINDOW_UPDATE increment. The high bit is reserved
     * (sent 0, ignored on read) per RFC 7540 sec. 6.9.
     */
    public static int readWindowUpdateIncrement(long payloadAddr) {
        return readU32(payloadAddr) & 0x7FFFFFFF;
    }

    public void reset() {
        continuationStreamId = -1;
    }

    /**
     * Attempts to parse one frame starting at {@code addr}.
     *
     * @param addr                receive buffer start (first un-consumed byte)
     * @param limit               one past the last buffered byte
     * @param header              out-parameter; populated on success
     * @param inboundMaxFrameSize our advertised {@code SETTINGS_MAX_FRAME_SIZE}
     * @return bytes consumed ({@code 9 + payloadLength}) or {@code 0} if the
     * frame is not yet fully buffered
     * @throws Http2ConnectionException connection-level protocol or size error
     * @throws Http2StreamException     stream-level error with the offending stream id
     */
    public int tryReadNext(long addr, long limit, Http2FrameHeader header, int inboundMaxFrameSize) {
        long available = limit - addr;
        if (available < Http2FrameHeader.SIZE) {
            return 0;
        }
        header.readFrom(addr);
        // RFC 7540 sec. 6.10: if a HEADERS / CONTINUATION block is open, the
        // very next frame on the connection MUST be a CONTINUATION on the same
        // stream. The check runs off the 9-byte header alone and *before*
        // every other validation — including the max-frame-size bound — so a
        // peer can't mask the sequence violation by oversizing the offending
        // frame, nor hide it behind a type-specific structural error, nor
        // stall the reader by sending only the header.
        checkContinuationSequence(header);
        int payloadLength = header.getPayloadLength();
        if (payloadLength > inboundMaxFrameSize) {
            throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "frame exceeds SETTINGS_MAX_FRAME_SIZE");
        }
        int total = Http2FrameHeader.SIZE + payloadLength;
        if (available < total) {
            return 0;
        }
        // Buffered-check precedes the structural / payload validators so a
        // stream-level throw (PRIORITY length != 5, HEADERS self-dependency,
        // stream-scoped WINDOW_UPDATE increment 0) is only ever raised when
        // the entire frame is in the caller's buffer. Callers can then
        // unconditionally advance {@code cursor += total} on stream error
        // without risking a cursor overrun on partial frames.
        validateStructure(header);
        // Continuation-tracker advance must precede payload validation so a
        // stream error from {@link #validatePayload} (e.g. HEADERS with
        // self-dependency + END_HEADERS=0) does not leave the tracker
        // un-armed. Without this ordering the peer's follow-up CONTINUATION
        // would be rejected as "CONTINUATION without HEADERS" — escalating
        // a stream error to a connection error.
        updateContinuationState(header);
        validatePayload(header);
        return total;
    }

    /**
     * Big-endian 64-bit read. Symmetric with {@code Http2FrameWriter.putU64}.
     * Use this when echoing wire-octet sequences back to the peer
     * byte-for-byte (e.g. PING opaque), since {@link Unsafe#getLong} reads
     * in platform byte order and would corrupt big-endian wire data on
     * little-endian hosts.
     */
    public static long readU64(long addr) {
        long hi = readU32(addr) & 0xFFFF_FFFFL;
        long lo = readU32(addr + 4) & 0xFFFF_FFFFL;
        return (hi << 32) | lo;
    }

    static int readU32(long addr) {
        int b0 = Unsafe.getUnsafe().getByte(addr) & 0xFF;
        int b1 = Unsafe.getUnsafe().getByte(addr + 1) & 0xFF;
        int b2 = Unsafe.getUnsafe().getByte(addr + 2) & 0xFF;
        int b3 = Unsafe.getUnsafe().getByte(addr + 3) & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    private static void validateStructure(Http2FrameHeader header) {
        byte type = header.getType();
        int streamId = header.getStreamId();
        int len = header.getPayloadLength();
        byte flags = header.getFlags();
        switch (type) {
            case Http2FrameType.DATA:
                if (streamId == 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "DATA on stream 0");
                }
                if (Http2Flags.hasPadded(flags) && len < 1) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "DATA PADDED with empty payload");
                }
                return;
            case Http2FrameType.HEADERS:
                if (streamId == 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "HEADERS on stream 0");
                }
                int headersMin = 0;
                if (Http2Flags.hasPadded(flags)) {
                    headersMin++;
                }
                if (Http2Flags.hasPriority(flags)) {
                    headersMin += 5;
                }
                if (len < headersMin) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "HEADERS length too short for flags");
                }
                return;
            case Http2FrameType.PRIORITY:
                if (streamId == 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "PRIORITY on stream 0");
                }
                if (len != 5) {
                    throw Http2StreamException.instance(streamId, Http2ErrorCode.FRAME_SIZE_ERROR);
                }
                return;
            case Http2FrameType.RST_STREAM:
                if (streamId == 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "RST_STREAM on stream 0");
                }
                if (len != 4) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "RST_STREAM length != 4");
                }
                return;
            case Http2FrameType.SETTINGS:
                if (streamId != 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "SETTINGS on non-zero stream");
                }
                if (Http2Flags.hasAck(flags)) {
                    if (len != 0) {
                        throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "SETTINGS ACK with payload");
                    }
                } else if ((len % 6) != 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "SETTINGS length not multiple of 6");
                }
                return;
            case Http2FrameType.PUSH_PROMISE:
                throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "PUSH_PROMISE received");
            case Http2FrameType.PING:
                if (streamId != 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "PING on non-zero stream");
                }
                if (len != 8) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "PING length != 8");
                }
                return;
            case Http2FrameType.GOAWAY:
                if (streamId != 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "GOAWAY on non-zero stream");
                }
                if (len < 8) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "GOAWAY length < 8");
                }
                return;
            case Http2FrameType.WINDOW_UPDATE:
                if (len != 4) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.FRAME_SIZE_ERROR, "WINDOW_UPDATE length != 4");
                }
                return;
            case Http2FrameType.CONTINUATION:
                if (streamId == 0) {
                    throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "CONTINUATION on stream 0");
                }
                return;
            default:
                // RFC 7540 sec. 4.1: unknown frame types are discarded by the caller.
        }
    }

    /**
     * Runs checks that require inspecting the payload. Precondition: the full
     * frame is buffered and {@link #validateStructure} has already passed.
     */
    private void validatePayload(Http2FrameHeader header) {
        byte type = header.getType();
        byte flags = header.getFlags();
        int len = header.getPayloadLength();
        int streamId = header.getStreamId();
        long payload = header.getPayloadAddr();

        switch (type) {
            case Http2FrameType.DATA:
                if (Http2Flags.hasPadded(flags)) {
                    int padLen = Unsafe.getUnsafe().getByte(payload) & 0xFF;
                    // Pad length byte is part of payload; len must be > 1 + padLen.
                    if (padLen >= len) {
                        throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "DATA pad length exceeds payload");
                    }
                }
                return;
            case Http2FrameType.HEADERS:
                int priorityOffset = 0;
                if (Http2Flags.hasPadded(flags)) {
                    int padLen = Unsafe.getUnsafe().getByte(payload) & 0xFF;
                    int consumed = 1 + (Http2Flags.hasPriority(flags) ? 5 : 0);
                    if (padLen > len - consumed) {
                        throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "HEADERS pad length exceeds payload");
                    }
                    priorityOffset = 1;
                }
                if (Http2Flags.hasPriority(flags)) {
                    // RFC 7540 sec. 5.3.1: a stream cannot depend on itself.
                    int dependencyId = readU32(payload + priorityOffset) & 0x7FFFFFFF;
                    if (dependencyId == streamId) {
                        throw Http2StreamException.instance(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                    }
                }
                return;
            case Http2FrameType.PRIORITY:
                // RFC 7540 sec. 5.3.1: a stream cannot depend on itself.
                int priorityDepId = readU32(payload) & 0x7FFFFFFF;
                if (priorityDepId == streamId) {
                    throw Http2StreamException.instance(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                }
                return;
            case Http2FrameType.WINDOW_UPDATE:
                int increment = readU32(payload) & 0x7FFFFFFF;
                if (increment == 0) {
                    // RFC 7540 sec. 6.9: 0-increment is connection error on stream 0,
                    // stream error otherwise.
                    if (streamId == 0) {
                        throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "WINDOW_UPDATE increment 0");
                    }
                    throw Http2StreamException.instance(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                }
                return;
            case Http2FrameType.SETTINGS:
                if (!Http2Flags.hasAck(flags)) {
                    int entries = len / 6;
                    for (int i = 0; i < entries; i++) {
                        long entry = payload + (long) i * 6;
                        short id = readSettingsId(entry);
                        long value = readSettingsValue(entry);
                        int code = Http2Settings.validate(id, value);
                        if (code != Http2ErrorCode.NO_ERROR) {
                            throw Http2ConnectionException.instance(code, "SETTINGS value out of range");
                        }
                    }
                }
                // return;
        }
    }

    /**
     * Header-only check: rejects any frame that violates RFC 7540 sec. 6.10's
     * "CONTINUATION must immediately follow HEADERS / CONTINUATION on the same
     * stream" rule. Runs before structural / payload validation so a peer can
     * neither hide the violation behind a second structural error nor stall
     * the reader with a half-sent non-CONTINUATION frame.
     */
    private void checkContinuationSequence(Http2FrameHeader header) {
        byte type = header.getType();
        int streamId = header.getStreamId();
        if (continuationStreamId != -1) {
            if (type != Http2FrameType.CONTINUATION) {
                throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "expected CONTINUATION");
            }
            if (streamId != continuationStreamId) {
                throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "CONTINUATION on wrong stream");
            }
            return;
        }
        if (type == Http2FrameType.CONTINUATION) {
            throw Http2ConnectionException.instance(Http2ErrorCode.PROTOCOL_ERROR, "CONTINUATION without HEADERS");
        }
    }

    /**
     * Commits the continuation tracker update. Runs only after structural and
     * payload validation have passed, so a rejected frame leaves the tracker
     * untouched.
     */
    private void updateContinuationState(Http2FrameHeader header) {
        byte type = header.getType();
        byte flags = header.getFlags();
        int streamId = header.getStreamId();
        if (continuationStreamId != -1) {
            // Must be a CONTINUATION on the same stream (guaranteed by checkContinuationSequence).
            if (Http2Flags.hasEndHeaders(flags)) {
                continuationStreamId = -1;
            }
            return;
        }
        if (type == Http2FrameType.HEADERS && !Http2Flags.hasEndHeaders(flags)) {
            continuationStreamId = streamId;
        }
    }
}
