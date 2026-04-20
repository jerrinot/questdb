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

package io.questdb.cutlass.hpack;

import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.io.Closeable;

/**
 * Per-connection HPACK decoder (RFC 7541 sec. 6). Walks the five
 * representation kinds, drives {@link HpackListener}, maintains a dynamic
 * table, and enforces the size-update-at-block-start rule.
 * <p>
 * The decoder owns one {@link HpackDynamicTable} (inbound direction) and one
 * native scratch buffer reused across every {@link #decodeBlock} call for
 * Huffman-decoded strings and staged dynamic-table name references. Both
 * are freed by {@link #close()}.
 * <p>
 * The SETTINGS plumbing splits caps across three quantities on the decoder
 * side, following the directional rules in {@code HPACK_CODEC.md} §13:
 * <ul>
 *   <li>{@code localAdvertisedCap} — the cap we advertise via our own
 *       {@code SETTINGS_HEADER_TABLE_SIZE}, applied here only after the
 *       peer has ACKed it via
 *       {@link #onLocalAdvertisedCapChanged}. Any inbound Dynamic Table
 *       Size Update exceeding this ceiling is rejected.</li>
 *   <li>{@code peerSelectedMax} — the peer encoder's operating cap, tracked
 *       from the most recent inbound HPACK size update. Initialised at the
 *       HTTP/2 default {@code 4096} before any SETTINGS exchange.</li>
 *   <li>Peer's {@code currentSize} — byte occupancy of the peer's encoder
 *       table, mirrored in our dynamic-table instance. Not part of the
 *       size-update obligation logic.</li>
 * </ul>
 * <p>
 * Not thread-safe.
 */
public final class HpackDecoder implements Closeable {

    private static final int DEFAULT_SCRATCH_CAPACITY = 16 * 1024;
    private static final int NO_PENDING = -1;
    private final HpackDynamicTable dynamicTable;
    private final long scratchAddr;
    private final int scratchCapacity;
    private int localAdvertisedCap;
    // Smallest interim cap the peer encoder must signal on its next header block, or
    // NO_PENDING when no pending obligation exists. Tracking a minimum (not just a
    // boolean) enforces RFC 7541 sec. 4.2's "when multiple changes to the maximum size
    // are signaled, the smallest of them MUST be reported" rule — if we lower to X,
    // raise to Y (> X) before the peer's next block, the peer still owes an update
    // <= X, not just <= Y.
    private int pendingMinCap = NO_PENDING;
    private int peerSelectedMax;

    public HpackDecoder(int initialEffectiveInboundCap, int poolCapacityBytes) {
        this(initialEffectiveInboundCap, poolCapacityBytes, DEFAULT_SCRATCH_CAPACITY);
    }

    public HpackDecoder(int initialEffectiveInboundCap, int poolCapacityBytes, int scratchCapacity) {
        if (scratchCapacity < 1) {
            throw new IllegalArgumentException("scratchCapacity must be positive: " + scratchCapacity);
        }
        // The HTTP/2 default HPACK dynamic-table size is 4096 (RFC 7541 sec. 4.2) and the
        // peer is free to fill the table up to that in pre-ACK header blocks, before we
        // get a chance to lower our advertised cap via SETTINGS. Require the pool to
        // cover the default so a legal peer pre-ACK block does not fail inside
        // HpackDynamicTable.setOperatingCap; smaller pools would violate the design-doc
        // §10 invariant. Callers that really want a smaller runtime cap should construct
        // with the 4096-capacity pool and lower via onLocalAdvertisedCapChanged after
        // SETTINGS ACK.
        if (poolCapacityBytes < Hpack.DEFAULT_TABLE_SIZE) {
            throw new IllegalArgumentException("poolCapacityBytes (" + poolCapacityBytes
                    + ") below HTTP/2 default HPACK table size (" + Hpack.DEFAULT_TABLE_SIZE + ")");
        }
        this.dynamicTable = new HpackDynamicTable(initialEffectiveInboundCap, poolCapacityBytes);
        this.peerSelectedMax = initialEffectiveInboundCap;
        this.localAdvertisedCap = Hpack.DEFAULT_TABLE_SIZE;
        this.scratchCapacity = scratchCapacity;
        this.scratchAddr = Unsafe.malloc(scratchCapacity, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        dynamicTable.close();
        Unsafe.free(scratchAddr, scratchCapacity, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * Decodes a single assembled HEADERS + CONTINUATION field block fragment.
     * Drives {@code listener} once per decoded header. Throws
     * {@link HpackException} on any structural error; the caller (the HTTP/2
     * request parser) converts this into a connection-level
     * {@code COMPRESSION_ERROR}.
     */
    public void decodeBlock(long addr, long limit, HpackListener listener) {
        long cursor = addr;
        boolean anyHeaderSeen = false;
        boolean pendingSatisfied = pendingMinCap == NO_PENDING;

        while (cursor < limit) {
            int b = Unsafe.getUnsafe().getByte(cursor) & 0xFF;
            if ((b & 0x80) != 0) {
                // Indexed header field (RFC 7541 sec. 6.1).
                if (!pendingSatisfied) {
                    throw HpackException.instance("missing mandatory dynamic table size update");
                }
                long packed = HpackIntCodec.decode(cursor, limit, 0x7F);
                int idx = Numbers.decodeLowInt(packed);
                cursor += Numbers.decodeHighInt(packed);
                if (idx == 0) {
                    throw HpackException.instance("indexed header field with index 0");
                }
                emitIndexed(idx, listener);
                anyHeaderSeen = true;
            } else if ((b & 0x40) != 0) {
                // Literal, incremental indexing (0x40-prefix, 6-bit name index).
                if (!pendingSatisfied) {
                    throw HpackException.instance("missing mandatory dynamic table size update");
                }
                cursor = decodeLiteral(cursor, limit, listener, 0x3F, true, false);
                anyHeaderSeen = true;
            } else if ((b & 0xE0) == 0x20) {
                // Dynamic table size update (001xxxxx, 5-bit prefix).
                if (anyHeaderSeen) {
                    throw HpackException.instance("size update after header field");
                }
                long packed = HpackIntCodec.decode(cursor, limit, 0x1F);
                int newCap = Numbers.decodeLowInt(packed);
                cursor += Numbers.decodeHighInt(packed);
                if (newCap > localAdvertisedCap) {
                    throw HpackException.instance("size update exceeds advertised cap");
                }
                if (pendingMinCap != NO_PENDING && !pendingSatisfied) {
                    // The first pending-clearing update must be <= the smallest interim cap
                    // we ever advertised while peerSelectedMax exceeded it (RFC 7541 sec. 4.2
                    // last paragraph). A larger value would let the peer skip the interim
                    // commitment and silently preserve entries that should have been evicted.
                    if (newCap > pendingMinCap) {
                        throw HpackException.instance("size update above pending interim minimum");
                    }
                    pendingMinCap = NO_PENDING;
                    pendingSatisfied = true;
                }
                dynamicTable.setOperatingCap(newCap);
                peerSelectedMax = newCap;
            } else if ((b & 0xF0) == 0x10) {
                // Literal, never indexed (0001xxxx, 4-bit prefix).
                if (!pendingSatisfied) {
                    throw HpackException.instance("missing mandatory dynamic table size update");
                }
                cursor = decodeLiteral(cursor, limit, listener, 0x0F, false, true);
                anyHeaderSeen = true;
            } else {
                // Literal, no indexing (0000xxxx, 4-bit prefix).
                if (!pendingSatisfied) {
                    throw HpackException.instance("missing mandatory dynamic table size update");
                }
                cursor = decodeLiteral(cursor, limit, listener, 0x0F, false, false);
                anyHeaderSeen = true;
            }
        }

        if (cursor != limit) {
            throw HpackException.instance("truncated block");
        }
        if (!pendingSatisfied) {
            throw HpackException.instance("block ended without mandatory size update");
        }
    }

    public HpackDynamicTable dynamicTable() {
        return dynamicTable;
    }

    public int localAdvertisedCap() {
        return localAdvertisedCap;
    }

    /**
     * Called when the peer has ACKed our SETTINGS_HEADER_TABLE_SIZE change.
     * From this point on, any inbound HPACK Dynamic Table Size Update with
     * a value greater than {@code newCap} is a decoding error.
     * <p>
     * If the peer's most recently signalled {@code peerSelectedMax} exceeds
     * {@code newCap} (or a pending obligation is already armed), the decoder
     * records {@code newCap} into a ratcheting {@code pendingMinCap}. RFC
     * 7541 sec. 4.2 last paragraph requires the peer to report the smallest
     * cap among multiple interim changes on its next header block, so
     * {@code pendingMinCap} only narrows — a later raise back above the
     * smallest interim value does not relieve the peer of the commitment,
     * and a later drop tightens the minimum it must report.
     * {@code pendingMinCap} clears when a qualifying inbound size update
     * (value &le; {@code pendingMinCap}) arrives; a non-qualifying update,
     * a block that opens with a header representation before clearing it,
     * or a block that ends without any qualifying update all throw
     * {@link HpackException}. See {@code HPACK_CODEC.md} §10 and §13 for
     * the full contract.
     * <p>
     * Fails fast with {@link IllegalArgumentException} when
     * {@code newCap > poolCapacityBytes}: the decoder's dynamic-table pool
     * is sized at construction to cover every cap the caller intends to
     * advertise, and letting a larger value through here would surface
     * later as a cryptic error from the table's {@code setOperatingCap}
     * after an otherwise-legal inbound size update already cleared the
     * {@code localAdvertisedCap} ceiling.
     */
    public void onLocalAdvertisedCapChanged(int newCap) {
        if (newCap < 0) {
            throw new IllegalArgumentException("newCap must be non-negative: " + newCap);
        }
        if (newCap > dynamicTable.poolCapacityBytes()) {
            throw new IllegalArgumentException("newCap (" + newCap
                    + ") exceeds dynamic-table poolCapacityBytes (" + dynamicTable.poolCapacityBytes() + ")");
        }
        localAdvertisedCap = newCap;
        if (pendingMinCap != NO_PENDING) {
            if (newCap < pendingMinCap) {
                pendingMinCap = newCap;
            }
        } else if (peerSelectedMax > newCap) {
            pendingMinCap = newCap;
        }
    }

    public int peerSelectedMax() {
        return peerSelectedMax;
    }

    /**
     * Resets all per-connection state so this decoder can be handed to a new
     * connection without re-allocating the native scratch buffer or the
     * dynamic-table pool / staging / ring arrays. The server-wide config
     * ({@code scratchCapacity}, pool capacity) is preserved. Call when
     * returning the instance to a connection pool.
     *
     * @param initialEffectiveInboundCap the cap the peer's encoder is bound
     *                                   by at the start of the new connection
     *                                   (HTTP/2 default {@code 4096} before
     *                                   any SETTINGS exchange)
     */
    public void reset(int initialEffectiveInboundCap) {
        dynamicTable.reset(initialEffectiveInboundCap);
        this.peerSelectedMax = initialEffectiveInboundCap;
        // Pool floor invariant (constructor): poolCapacityBytes >= DEFAULT_TABLE_SIZE.
        this.localAdvertisedCap = Hpack.DEFAULT_TABLE_SIZE;
        this.pendingMinCap = NO_PENDING;
    }

    private long copyNameToScratch(int nameIdx, long scratchCursor) {
        long srcAddr;
        int srcLen;
        if (nameIdx <= HpackStaticTable.SIZE) {
            srcAddr = HpackStaticTable.nameAddr(nameIdx);
            srcLen = HpackStaticTable.nameLen(nameIdx);
        } else {
            srcAddr = dynamicTable.entryNameAddr(nameIdx);
            srcLen = dynamicTable.entryNameLen(nameIdx);
        }
        if (scratchCursor + srcLen > scratchAddr + scratchCapacity) {
            throw HpackException.instance("scratch exhausted");
        }
        Vect.memcpy(scratchCursor, srcAddr, srcLen);
        return Numbers.encodeLowHighInts((int) (scratchCursor - scratchAddr), srcLen);
    }

    private long decodeLiteral(long cursor, long limit, HpackListener listener,
                               int prefixMask, boolean insertIntoTable, boolean neverIndexed) {
        long packed = HpackIntCodec.decode(cursor, limit, prefixMask);
        int nameIdx = Numbers.decodeLowInt(packed);
        cursor += Numbers.decodeHighInt(packed);

        long scratchCursor = scratchAddr;
        long nameAddr;
        int nameLen;
        if (nameIdx == 0) {
            // Read the name as a string (Huffman or literal). Materialise directly into scratch.
            long p = decodeString(cursor, limit, scratchCursor, scratchAddr + scratchCapacity);
            cursor = cursorFromPacked(p, cursor);
            int len = lenFromPacked(p);
            nameAddr = scratchCursor;
            nameLen = len;
            scratchCursor += len;
        } else {
            // Indexed name. Always stage into scratch so the listener's pointers
            // survive any subsequent dynamic-table insert / eviction hazard.
            long packedCopy = copyNameToScratch(nameIdx, scratchCursor);
            int offsetInScratch = Numbers.decodeLowInt(packedCopy);
            int len = Numbers.decodeHighInt(packedCopy);
            nameAddr = scratchAddr + offsetInScratch;
            nameLen = len;
            scratchCursor += len;
        }

        // Value string.
        long p = decodeString(cursor, limit, scratchCursor, scratchAddr + scratchCapacity);
        cursor = cursorFromPacked(p, cursor);
        int valueLen = lenFromPacked(p);
        long valueAddr = scratchCursor;
        scratchCursor += valueLen;

        if (insertIntoTable) {
            dynamicTable.insert(nameAddr, nameLen, valueAddr, valueLen);
        }
        listener.onHeader(nameAddr, nameLen, valueAddr, valueLen, neverIndexed);
        return cursor;
    }

    /**
     * Decodes an HPACK string starting at {@code srcAddr}, materialising the
     * raw bytes at {@code dstAddr}. Returns a packed {@code (nextSrc, len)}
     * with {@code nextSrc - srcAddr} in the low 32 bits and {@code len} in
     * the high 32 bits.
     */
    private long decodeString(long srcAddr, long srcLimit, long dstAddr, long dstLimit) {
        // Bounds-check before the first byte read: HpackIntCodec.decode would throw on an
        // empty input, but dereferencing the byte here first would already be a wild read.
        if (srcAddr >= srcLimit) {
            throw HpackException.instance("truncated string header");
        }
        int first = Unsafe.getUnsafe().getByte(srcAddr) & 0xFF;
        boolean huffman = (first & Hpack.FLAG_STRING_HUFFMAN) != 0;
        long packed = HpackIntCodec.decode(srcAddr, srcLimit, 0x7F);
        int encodedLen = Numbers.decodeLowInt(packed);
        long cursor = srcAddr + Numbers.decodeHighInt(packed);
        if (cursor + encodedLen > srcLimit) {
            throw HpackException.instance("truncated string");
        }
        int len;
        if (huffman) {
            int decoded = HpackHuffman.decode(cursor, encodedLen, dstAddr, dstLimit);
            if (decoded < 0) {
                throw HpackException.instance("scratch exhausted");
            }
            len = decoded;
        } else {
            if (dstAddr + encodedLen > dstLimit) {
                throw HpackException.instance("scratch exhausted");
            }
            Vect.memcpy(dstAddr, cursor, encodedLen);
            len = encodedLen;
        }
        cursor += encodedLen;
        return Numbers.encodeLowHighInts((int) (cursor - srcAddr), len);
    }

    private void emitIndexed(int idx, HpackListener listener) {
        if (idx <= HpackStaticTable.SIZE) {
            listener.onHeader(
                    HpackStaticTable.nameAddr(idx), HpackStaticTable.nameLen(idx),
                    HpackStaticTable.valueAddr(idx), HpackStaticTable.valueLen(idx),
                    false
            );
        } else {
            listener.onHeader(
                    dynamicTable.entryNameAddr(idx), dynamicTable.entryNameLen(idx),
                    dynamicTable.entryValueAddr(idx), dynamicTable.entryValueLen(idx),
                    false
            );
        }
    }

    private static long cursorFromPacked(long packed, long base) {
        return base + Numbers.decodeLowInt(packed);
    }

    private static int lenFromPacked(long packed) {
        return Numbers.decodeHighInt(packed);
    }
}
