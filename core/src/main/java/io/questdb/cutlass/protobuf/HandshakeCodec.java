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

package io.questdb.cutlass.protobuf;

/**
 * Hand-rolled codec for the Flight {@code HandshakeRequest} and
 * {@code HandshakeResponse} messages.
 * <p>
 * The {@code Flight.proto} shape (unchanged since Arrow 0.15):
 * <pre>
 *   message HandshakeRequest {
 *     uint64 protocol_version = 1;
 *     bytes  payload          = 2;
 *   }
 *   message HandshakeResponse {
 *     uint64 protocol_version = 1;
 *     bytes  payload          = 2;
 *   }
 * </pre>
 * Wave 5 accepts any (protocol_version, payload) the client sends
 * without interpretation, and always replies with {@code protocol_version=0}
 * and an empty {@code payload} — the handshake RPC is exposed so that
 * the later auth layer has a hook; Wave 5 performs no authentication.
 */
public final class HandshakeCodec {

    public static final int FIELD_PAYLOAD = 2;
    public static final int FIELD_PROTOCOL_VERSION = 1;

    private HandshakeCodec() {
    }

    /**
     * Decodes a {@code HandshakeRequest} at {@code [addr, limit)} into
     * {@code out}. Unknown fields are skipped. The payload slice exposed
     * via {@code out.payloadAddr} / {@code out.payloadLen} points into
     * the caller-owned input buffer and is stable only as long as the
     * caller retains that buffer — the codec never copies.
     */
    public static void decodeHandshakeRequest(long addr, long limit, Fields out) {
        if (out == null) {
            throw new IllegalArgumentException("out must be non-null");
        }
        out.clear();
        ProtobufReader r = out.reader;
        r.of(addr, limit);
        while (r.hasMore()) {
            int tag = r.readTag();
            int fieldNumber = ProtobufWireFormat.fieldNumberOf(tag);
            int wireType = ProtobufWireFormat.wireTypeOf(tag);
            switch (fieldNumber) {
                case FIELD_PROTOCOL_VERSION:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_VARINT) {
                        throw ProtobufException.instance("protocol_version wrong wire type");
                    }
                    out.protocolVersion = r.readVarint64();
                    break;
                case FIELD_PAYLOAD:
                    if (wireType != ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED) {
                        throw ProtobufException.instance("payload wrong wire type");
                    }
                    r.readLengthDelimited();
                    out.payloadAddr = r.lastValueAddr();
                    out.payloadLen = r.lastValueLen();
                    break;
                default:
                    r.skipField(wireType);
                    break;
            }
        }
    }

    /**
     * Encodes a {@code HandshakeResponse} into {@code [dstAddr, dstLimit)}.
     * Returns the new cursor on success or {@code -1} on buffer overflow
     * (nothing written, atomic). A zero {@code protocolVersion} is
     * emitted explicitly rather than omitted — all Wave 5 consumers
     * tolerate either shape and the explicit form gives deterministic
     * wire bytes for tests.
     */
    public static long encodeHandshakeResponse(long dstAddr, long dstLimit,
                                               long protocolVersion, long payloadAddr, int payloadLen) {
        if (payloadLen < 0) {
            throw new IllegalArgumentException("payloadLen must be non-negative: " + payloadLen);
        }
        if (dstLimit < dstAddr) {
            throw new IllegalArgumentException("dstLimit < dstAddr");
        }
        // Pre-measure so the method is atomic: either all bytes land or none do.
        long tag1 = ProtobufWireFormat.makeTag(FIELD_PROTOCOL_VERSION, ProtobufWireFormat.WIRE_TYPE_VARINT);
        long tag2 = ProtobufWireFormat.makeTag(FIELD_PAYLOAD, ProtobufWireFormat.WIRE_TYPE_LENGTH_DELIMITED);
        long needed = ProtobufWireFormat.varintSize(tag1)
                + ProtobufWireFormat.varintSize(protocolVersion)
                + ProtobufWireFormat.varintSize(tag2)
                + ProtobufWireFormat.varintSize(payloadLen)
                + payloadLen;
        if (dstAddr + needed > dstLimit) {
            return -1;
        }
        long c = ProtobufWriter.staticWriteVarint64Field(dstAddr, dstLimit, FIELD_PROTOCOL_VERSION, protocolVersion);
        if (c < 0) {
            return -1;
        }
        return ProtobufWriter.staticWriteLengthDelimitedField(c, dstLimit, FIELD_PAYLOAD, payloadAddr, payloadLen);
    }

    /**
     * Reusable holder for decoded {@code HandshakeRequest} fields.
     * Callers allocate one instance and reuse across requests via
     * {@link #clear()} (called automatically by
     * {@link HandshakeCodec#decodeHandshakeRequest}).
     * <p>
     * The {@link ProtobufReader} is owned here so the decode is a single
     * allocation-free call — callers that want their own reader can
     * recreate it via {@link ProtobufReader#of(long, long)}.
     */
    public static final class Fields {
        final ProtobufReader reader = new ProtobufReader();
        public long payloadAddr;
        public int payloadLen;
        public long protocolVersion;

        public void clear() {
            protocolVersion = 0;
            payloadAddr = 0;
            payloadLen = 0;
        }
    }
}
