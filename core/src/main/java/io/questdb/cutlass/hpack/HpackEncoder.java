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

import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.io.Closeable;

/**
 * Per-connection HPACK encoder (RFC 7541 sec. 6). Milestone 1 posture: emits
 * static-indexed headers and literals-without-indexing with plain-octet
 * strings (no Huffman, no dynamic-table writes). The first block starts with
 * a Dynamic Table Size Update to {@code 0} pinning {@code selectedMax} at
 * {@code 0} for the life of the connection (see {@code HPACK_CODEC.md}
 * §16.1).
 * <p>
 * {@link #encode} returns {@code -1} when the caller-owned output buffer
 * cannot hold the next representation. Preflight atomicity: the
 * {@code -1} path leaves the encoder's observable state — the output buffer
 * byte contents, the internal queued-size-update list, the dynamic-table
 * ring / pool, and the staging buffer — exactly as they were on entry. The
 * caller drains and retries with the same arguments.
 * <p>
 * Not thread-safe.
 */
public final class HpackEncoder implements Closeable {

    /**
     * {@code hint} value: no hint. The encoder looks up name / value in the
     * static table and picks the best representation.
     */
    public static final int HINT_NONE = 0;

    /**
     * {@code hint} value bit: the low 28 bits name the static-table index for
     * an indexed header field. Callers construct the hint as
     * {@code HpackEncoder.HINT_STATIC_INDEX | idx}.
     */
    public static final int HINT_STATIC_INDEX = 0x40000000;

    /**
     * {@code hint} value bit: the low 28 bits name the static-table index
     * for a literal with static-indexed name. Callers construct the hint as
     * {@code HpackEncoder.HINT_STATIC_NAME | idx}.
     */
    public static final int HINT_STATIC_NAME = 0x20000000;

    private static final int HINT_INDEX_MASK = 0x0FFFFFFF;
    private static final int HINT_KIND_MASK = HINT_STATIC_INDEX | HINT_STATIC_NAME;
    private static final int NO_QUEUED_UPDATE = -1;
    private final int bufferFloor;
    private final HpackDynamicTable dynamicTable;
    private final int localPreferredCap;
    private final int maxOutboundFieldBytes;
    private final int outputBufferCapacityBytes;
    private boolean blockOpen;
    private int peerAdvertisedCap;
    // Queued size updates emitted at the next beginBlock. A value of -1 means "none".
    // The interim value precedes the final one on the wire (RFC 7541 sec. 4.2 last paragraph).
    private int queuedInterimMin = NO_QUEUED_UPDATE;
    private int queuedSizeUpdate = NO_QUEUED_UPDATE;
    private int selectedMax;

    /**
     * @param initialPeerAdvertisedCap the peer's SETTINGS_HEADER_TABLE_SIZE as of
     *                                 connection setup (HTTP/2 default {@code 4096}).
     *                                 SETTINGS values are unsigned 32-bit on the wire
     *                                 (RFC 7540 sec. 6.5.2) so this is typed
     *                                 {@code long}; the value is clamped internally
     *                                 to {@code localPreferredCap} because any peer
     *                                 cap above that upper-bounds {@code selectedMax}
     *                                 at {@code localPreferredCap} anyway.
     * @param localPreferredCap        policy upper bound on {@code selectedMax}
     * @param poolCapacityBytes        dynamic-table pool size
     * @param maxOutboundFieldBytes    cap on a single outbound field's
     *                                 {@code nameLen + valueLen}
     * @param outputBufferCapacityBytes byte capacity of the caller-owned
     *                                 output buffer handed to {@link #beginBlock}
     *                                 and {@link #encode}; must be at least
     *                                 the computed {@code bufferFloor}
     */
    public HpackEncoder(long initialPeerAdvertisedCap,
                        int localPreferredCap,
                        int poolCapacityBytes,
                        int maxOutboundFieldBytes,
                        int outputBufferCapacityBytes) {
        if (initialPeerAdvertisedCap < 0 || initialPeerAdvertisedCap > 0xFFFFFFFFL) {
            throw new IllegalArgumentException("initialPeerAdvertisedCap out of unsigned 32-bit range: "
                    + initialPeerAdvertisedCap);
        }
        if (localPreferredCap < 0) {
            throw new IllegalArgumentException("localPreferredCap must be non-negative");
        }
        if (maxOutboundFieldBytes < 0) {
            throw new IllegalArgumentException("maxOutboundFieldBytes must be non-negative");
        }
        // Design doc §11: pool must cover every selectedMax the connection can ever operate
        // at, which is bounded by localPreferredCap. Catch the misconfiguration at construction
        // rather than letting it surface on a Milestone 2 cap raise when the dynamic table
        // rejects the operating cap.
        if (poolCapacityBytes < localPreferredCap) {
            throw new IllegalArgumentException("poolCapacityBytes (" + poolCapacityBytes
                    + ") below localPreferredCap (" + localPreferredCap + ")");
        }
        // Clamp the peer's advertised cap to our policy ceiling: any value above
        // localPreferredCap yields identical encoder behaviour (selectedMax is always
        // bounded by localPreferredCap per §11), so we can safely narrow to int for
        // all downstream comparisons.
        this.peerAdvertisedCap = (int) Math.min(initialPeerAdvertisedCap, localPreferredCap);
        this.localPreferredCap = localPreferredCap;
        this.maxOutboundFieldBytes = maxOutboundFieldBytes;
        // Milestone 1: selectedMax pinned at 0. Pool is sized by the caller; we construct
        // the table but never admit entries.
        this.selectedMax = 0;
        this.dynamicTable = new HpackDynamicTable(0, poolCapacityBytes);
        this.bufferFloor = computeBufferFloor(localPreferredCap, maxOutboundFieldBytes);
        if (outputBufferCapacityBytes < bufferFloor) {
            throw new IllegalArgumentException(
                    "outputBufferCapacityBytes (" + outputBufferCapacityBytes
                            + ") below computed bufferFloor (" + bufferFloor + ")");
        }
        this.outputBufferCapacityBytes = outputBufferCapacityBytes;
        // Queue the initial Milestone 1 pin: size update -> 0 at the very first beginBlock.
        this.queuedSizeUpdate = 0;
    }

    /**
     * Opens a new header block. Emits any queued Dynamic Table Size Update
     * instructions before the first header field. Must be paired with
     * {@link #endBlock}. Returns the new write pointer, or {@code -1} if the
     * buffer cannot hold the queued updates (preflight atomic: no bytes
     * written, no queued state dequeued).
     */
    public long beginBlock(long addr, long limit) {
        if (blockOpen) {
            throw new IllegalStateException("block already open");
        }
        // Preflight: compute the combined encoded length of the queued size updates in
        // long arithmetic, then reject with -1 if the caller-owned buffer cannot hold
        // them both. Writing the interim first and only discovering the final doesn't fit
        // later would leave partial bytes in the caller buffer, breaking the documented
        // retry-atomicity of beginBlock.
        long needed = 0;
        if (queuedInterimMin != NO_QUEUED_UPDATE) {
            needed += HpackIntCodec.encodedLength(5, queuedInterimMin);
        }
        if (queuedSizeUpdate != NO_QUEUED_UPDATE) {
            needed += HpackIntCodec.encodedLength(5, queuedSizeUpdate);
        }
        if (limit - addr < needed) {
            return -1;
        }
        long cursor = addr;
        if (queuedInterimMin != NO_QUEUED_UPDATE) {
            cursor = HpackIntCodec.encode(cursor, limit, 0x1F, Hpack.PATTERN_SIZE_UPDATE, queuedInterimMin);
        }
        if (queuedSizeUpdate != NO_QUEUED_UPDATE) {
            cursor = HpackIntCodec.encode(cursor, limit, 0x1F, Hpack.PATTERN_SIZE_UPDATE, queuedSizeUpdate);
        }
        queuedInterimMin = NO_QUEUED_UPDATE;
        queuedSizeUpdate = NO_QUEUED_UPDATE;
        blockOpen = true;
        return cursor;
    }

    @Override
    public void close() {
        dynamicTable.close();
    }

    public HpackDynamicTable dynamicTable() {
        return dynamicTable;
    }

    /**
     * Closes the header block opened by {@link #beginBlock}. Purely a
     * state-machine transition; no bytes are written.
     */
    public void endBlock() {
        if (!blockOpen) {
            throw new IllegalStateException("no block open");
        }
        blockOpen = false;
    }

    /**
     * Encodes one header into the caller-owned buffer. Must be called between
     * {@link #beginBlock} and {@link #endBlock}. The caller-boundary field
     * cap is enforced here: any field whose
     * {@code nameLen + valueLen > maxOutboundFieldBytes} throws
     * {@link IllegalArgumentException}. Returns the new write pointer, or
     * {@code -1} if the buffer cannot hold the encoded representation.
     */
    public long encode(long addr, long limit,
                       long nameAddr, int nameLen,
                       long valueAddr, int valueLen,
                       int hint) {
        if (!blockOpen) {
            throw new IllegalStateException("no block open");
        }
        if (nameLen < 0 || valueLen < 0) {
            throw new IllegalArgumentException("negative length");
        }
        // Long arithmetic guards against int overflow on pathological lengths.
        if ((long) nameLen + valueLen > maxOutboundFieldBytes) {
            throw new IllegalArgumentException("field exceeds maxOutboundFieldBytes");
        }
        // Reject stray bits up front, before dispatching on kind — so e.g.
        // HINT_STATIC_INDEX | 0x10000000 | 8 (a recognized kind combined with garbage
        // bits outside both masks) doesn't slip through as a valid static-indexed emit.
        if ((hint & ~(HINT_KIND_MASK | HINT_INDEX_MASK)) != 0) {
            throw new IllegalArgumentException("unrecognized hint bits: 0x" + Integer.toHexString(hint));
        }
        int kind = hint & HINT_KIND_MASK;
        int idx = hint & HINT_INDEX_MASK;
        if (kind == HINT_STATIC_INDEX) {
            if (idx < 1 || idx > HpackStaticTable.SIZE) {
                throw new IllegalArgumentException("HINT_STATIC_INDEX requires idx in [1, "
                        + HpackStaticTable.SIZE + "]: " + idx);
            }
            return encodeIndexed(addr, limit, idx);
        }
        if (kind == HINT_STATIC_NAME) {
            if (idx < 1 || idx > HpackStaticTable.SIZE) {
                throw new IllegalArgumentException("HINT_STATIC_NAME requires idx in [1, "
                        + HpackStaticTable.SIZE + "]: " + idx);
            }
            return encodeLiteralIndexedName(addr, limit, idx, valueAddr, valueLen);
        }
        // Reject both-kind-bits-set (kind == HINT_KIND_MASK, neither above branch matches)
        // and any other non-HINT_NONE value reachable after the stray-bits guard.
        if (hint != HINT_NONE) {
            throw new IllegalArgumentException("unrecognized hint bits: 0x" + Integer.toHexString(hint));
        }
        // HINT_NONE: one combined probe + chain walk over the static table.
        long lookup = HpackStaticTable.lookup(nameAddr, nameLen, valueAddr, valueLen);
        int exact = Numbers.decodeHighInt(lookup);
        if (exact > 0) {
            return encodeIndexed(addr, limit, exact);
        }
        int nameOnly = Numbers.decodeLowInt(lookup);
        if (nameOnly > 0) {
            return encodeLiteralIndexedName(addr, limit, nameOnly, valueAddr, valueLen);
        }
        return encodeLiteralNewName(addr, limit, nameAddr, nameLen, valueAddr, valueLen);
    }

    public int localPreferredCap() {
        return localPreferredCap;
    }

    /**
     * Called when the peer's SETTINGS_HEADER_TABLE_SIZE has changed. Accepts the
     * unsigned 32-bit SETTINGS value as {@code long}: HTTP/2 SETTINGS values run
     * {@code [0, 2^32 - 1]} on the wire (RFC 7540 sec. 6.5.2), and a top-bit-set
     * peer setting that arrived as {@code int} would otherwise surface here as a
     * negative value and reject a legal peer frame.
     * <p>
     * The argument is clamped to {@code localPreferredCap} before storage: any
     * value above that ceiling produces identical encoder behaviour because
     * {@code selectedMax} is always bounded by {@code localPreferredCap} (§11).
     * The clamp lets the rest of the encoder continue to compare against the
     * peer cap as {@code int}.
     * <p>
     * In Milestone 1 {@code selectedMax} is pinned at {@code 0}, so the clamped
     * value is simply recorded; Milestone 2 will use it to recompute
     * {@code selectedMax} and queued size-update updates.
     */
    public void onPeerAdvertisedCapChanged(long newCap) {
        if (newCap < 0 || newCap > 0xFFFFFFFFL) {
            throw new IllegalArgumentException("newCap out of unsigned 32-bit range: " + newCap);
        }
        peerAdvertisedCap = (int) Math.min(newCap, localPreferredCap);
    }

    public int peerAdvertisedCap() {
        return peerAdvertisedCap;
    }

    /**
     * Resets all per-connection state so this encoder can be handed to a new
     * connection without re-allocating the dynamic-table pool / staging / ring
     * arrays. The server-wide config ({@code localPreferredCap},
     * {@code maxOutboundFieldBytes}, {@code outputBufferCapacityBytes},
     * pool capacity, computed {@code bufferFloor}) is preserved. Re-queues the
     * Milestone 1 pinning size update to {@code 0} so the new connection's
     * first block starts with it. Call when returning the instance to a
     * connection pool.
     *
     * @param initialPeerAdvertisedCap the peer's SETTINGS_HEADER_TABLE_SIZE as
     *                                 of the new connection's setup (HTTP/2
     *                                 default {@code 4096} before any SETTINGS
     *                                 exchange). Typed {@code long} for
     *                                 unsigned 32-bit safety; clamped to
     *                                 {@code localPreferredCap} internally.
     */
    public void reset(long initialPeerAdvertisedCap) {
        if (initialPeerAdvertisedCap < 0 || initialPeerAdvertisedCap > 0xFFFFFFFFL) {
            throw new IllegalArgumentException("initialPeerAdvertisedCap out of unsigned 32-bit range: "
                    + initialPeerAdvertisedCap);
        }
        dynamicTable.reset(0);
        this.peerAdvertisedCap = (int) Math.min(initialPeerAdvertisedCap, localPreferredCap);
        this.selectedMax = 0;
        this.queuedInterimMin = NO_QUEUED_UPDATE;
        this.queuedSizeUpdate = 0;
        this.blockOpen = false;
    }

    public int selectedMax() {
        return selectedMax;
    }

    private static int computeBufferFloor(int localPreferredCap, int maxOutboundFieldBytes) {
        long sizeUpdateBytes = HpackIntCodec.encodedLength(5, localPreferredCap);
        // maxRepresentationBytes: conservative upper bound for the Milestone 1 surface.
        // First-byte representation flag (1 byte) + name index or length prefix (up to 6 bytes)
        // + string length prefix for the value (up to 6 bytes) + name / value bytes. Long
        // arithmetic prevents silent overflow if maxOutboundFieldBytes is misconfigured.
        long maxRepresentationBytes = 1L + 6 + 6 + maxOutboundFieldBytes;
        long floor = 2 * sizeUpdateBytes + maxRepresentationBytes;
        if (floor > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("computed bufferFloor " + floor
                    + " exceeds Integer.MAX_VALUE; reduce maxOutboundFieldBytes");
        }
        return (int) floor;
    }

    private long encodeIndexed(long addr, long limit, int idx) {
        // 7-bit prefix with the Indexed Header Field pattern (high bit set).
        return HpackIntCodec.encode(addr, limit, 0x7F, 0x80, idx);
    }

    private long encodeLiteralIndexedName(long addr, long limit, int nameIdx, long valueAddr, int valueLen) {
        // Literal, no indexing (0000xxxx): 4-bit prefix carries the name index.
        // Long arithmetic keeps the preflight safe even if valueLen is pathological.
        long headerLen = (long) HpackIntCodec.encodedLength(4, nameIdx)
                + HpackIntCodec.encodedLength(7, valueLen) + valueLen;
        if (limit - addr < headerLen) {
            return -1;
        }
        long cursor = HpackIntCodec.encode(addr, limit, 0x0F, Hpack.PATTERN_LITERAL_NO_INDEXING, nameIdx);
        cursor = encodePlainString(cursor, limit, valueAddr, valueLen);
        return cursor;
    }

    private long encodeLiteralNewName(long addr, long limit,
                                      long nameAddr, int nameLen,
                                      long valueAddr, int valueLen) {
        long totalLen = 1L
                + HpackIntCodec.encodedLength(7, nameLen) + nameLen
                + HpackIntCodec.encodedLength(7, valueLen) + valueLen;
        if (limit - addr < totalLen) {
            return -1;
        }
        // Literal, no indexing, new name: first byte is 0x00.
        Unsafe.getUnsafe().putByte(addr, (byte) Hpack.PATTERN_LITERAL_NO_INDEXING);
        long cursor = addr + 1;
        cursor = encodePlainString(cursor, limit, nameAddr, nameLen);
        cursor = encodePlainString(cursor, limit, valueAddr, valueLen);
        return cursor;
    }

    private long encodePlainString(long addr, long limit, long srcAddr, int srcLen) {
        long cursor = HpackIntCodec.encode(addr, limit, 0x7F, 0x00, srcLen);
        if (cursor == -1) {
            return -1;
        }
        Vect.memcpy(cursor, srcAddr, srcLen);
        return cursor + srcLen;
    }
}
