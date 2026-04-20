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
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.io.Closeable;

/**
 * Per-direction HPACK dynamic table (RFC 7541 sec. 4).
 * <p>
 * A FIFO where the most recently inserted entry gets the lowest wire index
 * ({@code 62}) and the oldest gets {@code 61 + dynamicCount}. Storage is a
 * native byte pool plus a ring of
 * {@code (nameOffset, nameLen, valueOffset, valueLen)} tuples.
 * <p>
 * Two cap concepts operate on the table and must not be conflated:
 * <ul>
 *   <li>{@code poolCapacityBytes} — pool size in bytes, fixed at construction.
 *       Governs pool wrap / overlap arithmetic and the staging buffer size.</li>
 *   <li>{@code currentOperatingCap} — HPACK cap currently enforced for eviction
 *       and oversized-entry decisions. For the encoder's instance this is
 *       {@code selectedMax}; for the decoder's that's the cap from the most
 *       recent inbound HPACK Dynamic Table Size Update. Mutable via
 *       {@link #setOperatingCap}.</li>
 * </ul>
 * <p>
 * Insert follows the six-step routine in {@code HPACK_CODEC.md} §7:
 * <ol>
 *   <li>Resolve + stage back-references — if the caller-supplied name
 *       {@code (srcAddr, srcLen)} points into this table's own pool, copy it
 *       into a private staging buffer before any eviction.</li>
 *   <li>Oversized-entry short-circuit — if
 *       {@code entryCost > currentOperatingCap}, empty the table and return
 *       without inserting.</li>
 *   <li>Pick target region — check wrap at {@code poolCursor}.</li>
 *   <li>Overlap eviction — evict live entries whose pool range overlaps the
 *       target region, oldest first.</li>
 *   <li>Operating-cap eviction — continue evicting oldest until
 *       {@code currentSize + entryCost <= currentOperatingCap}.</li>
 *   <li>Commit — write the bytes, advance {@code poolCursor} and the ring
 *       pointers, bump {@code currentSize}.</li>
 * </ol>
 * <p>
 * Not thread-safe. Each connection owns two distinct instances (inbound for
 * the decoder, outbound for the encoder).
 */
public final class HpackDynamicTable implements Closeable {

    /**
     * First wire index addressing the dynamic table (RFC 7541 sec. 2.3.3);
     * indices {@code 1..61} address the static table.
     */
    public static final int FIRST_DYNAMIC_WIRE_INDEX = Hpack.STATIC_TABLE_SIZE + 1;

    private static final int EMPTY_SLOT = -1;

    private final int[] nameLen;
    // Entry metadata is held in parallel int[] arrays indexed by ring slot;
    // each slot holds (nameOffset, nameLen, valueOffset, valueLen). Offsets are
    // relative to poolAddr.
    private final int[] nameOffset;
    private final long poolAddr;
    private final int poolCapacityBytes;
    private final int ringSize;
    // Staging buffer for the name-reference hazard (§7 step 1). Sized to
    // poolCapacityBytes so any single entry's name fits.
    private final long stagingAddr;
    private final int[] valueLen;
    private final int[] valueOffset;
    private int currentOperatingCap;
    // Live bytes under the RFC size accounting (includes 32-byte overhead per entry).
    private int currentSize;
    private int dynamicCount;
    private int newestSlot = EMPTY_SLOT;
    private int oldestSlot = EMPTY_SLOT;
    private int poolCursor;

    /**
     * @param initialOperatingCap initial {@code currentOperatingCap}, bounded by
     *                            {@code poolCapacityBytes}
     * @param poolCapacityBytes   byte capacity of the native pool (must be
     *                            {@code >= initialOperatingCap}); see
     *                            {@code HPACK_CODEC.md} §4 for sizing rules
     */
    public HpackDynamicTable(int initialOperatingCap, int poolCapacityBytes) {
        if (poolCapacityBytes < 0) {
            throw new IllegalArgumentException("poolCapacityBytes must be non-negative: " + poolCapacityBytes);
        }
        if (initialOperatingCap < 0) {
            throw new IllegalArgumentException("initialOperatingCap must be non-negative: " + initialOperatingCap);
        }
        // §4 / §7 invariant: poolCapacityBytes >= currentOperatingCap. The pool must be
        // large enough to hold every entry the cap admits; the overlap-eviction loop and
        // the ring sizing both assume this. Enforcing it here prevents subtle corruption
        // whereby a wrap-target [0, n) could fall between entries in pool order that are
        // not contiguous in ring order.
        if (initialOperatingCap > poolCapacityBytes) {
            throw new IllegalArgumentException("initialOperatingCap (" + initialOperatingCap
                    + ") exceeds poolCapacityBytes (" + poolCapacityBytes + ")");
        }
        this.poolCapacityBytes = poolCapacityBytes;
        this.currentOperatingCap = initialOperatingCap;
        // Native allocations: pool + staging buffer (same size). Pool must not be zero
        // so poolAddr is a valid range even when poolCapacityBytes == 0; use a 1-byte sentinel.
        int alloc = Math.max(1, poolCapacityBytes);
        this.poolAddr = Unsafe.malloc(alloc, MemoryTag.NATIVE_DEFAULT);
        this.stagingAddr = Unsafe.malloc(alloc, MemoryTag.NATIVE_DEFAULT);
        // Ring size: an entry's RFC cost is >= 32, so max live entries <= poolCapacityBytes / 32.
        // Add one slot of slack so the ring can never saturate at the exact bound.
        this.ringSize = Math.max(1, poolCapacityBytes / Hpack.ENTRY_OVERHEAD + 1);
        this.nameOffset = new int[ringSize];
        this.nameLen = new int[ringSize];
        this.valueOffset = new int[ringSize];
        this.valueLen = new int[ringSize];
    }

    @Override
    public void close() {
        int alloc = Math.max(1, poolCapacityBytes);
        Unsafe.free(poolAddr, alloc, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(stagingAddr, alloc, MemoryTag.NATIVE_DEFAULT);
    }

    public int currentOperatingCap() {
        return currentOperatingCap;
    }

    public int currentSize() {
        return currentSize;
    }

    public int dynamicCount() {
        return dynamicCount;
    }

    /**
     * Returns the name-bytes native address for the entry at dynamic wire
     * index {@code wireIdx} (must be in
     * {@code [FIRST_DYNAMIC_WIRE_INDEX, FIRST_DYNAMIC_WIRE_INDEX + dynamicCount - 1]}).
     * Out-of-range indices throw {@link HpackException}.
     */
    public long entryNameAddr(int wireIdx) {
        return poolAddr + nameOffset[slotForWireIndex(wireIdx)];
    }

    public int entryNameLen(int wireIdx) {
        return nameLen[slotForWireIndex(wireIdx)];
    }

    public long entryValueAddr(int wireIdx) {
        return poolAddr + valueOffset[slotForWireIndex(wireIdx)];
    }

    public int entryValueLen(int wireIdx) {
        return valueLen[slotForWireIndex(wireIdx)];
    }

    /**
     * Inserts a new entry. {@code srcNameAddr} may point into this table's
     * own pool (dynamic-index name reference); the routine stages those bytes
     * before any eviction so the reference survives the insert. The value
     * bytes must not point into the pool; callers deliver them from a
     * separate scratch buffer.
     * <p>
     * If {@code 32 + nameLen + valueLen > currentOperatingCap}, the routine
     * empties the table and returns without committing — per RFC 7541 sec.
     * 4.4 an oversized entry causes the full dynamic table to be cleared.
     */
    public void insert(long srcNameAddr, int nameLenIn, long valueAddr, int valueLenIn) {
        if (nameLenIn < 0 || valueLenIn < 0) {
            throw new IllegalArgumentException("negative length");
        }
        // Step 2 first, using long arithmetic, so that an out-of-range length or cost
        // cannot overflow int before the short-circuit test runs. Doing the check here
        // also lets us skip the staging copy in the clear-and-return path. The RFC 7541
        // sec. 4.4 short-circuit does not use the name bytes, so step 1's ordering
        // obligation (stage before eviction) does not apply.
        long neededLong = (long) nameLenIn + valueLenIn;
        long entryCostLong = Hpack.ENTRY_OVERHEAD + neededLong;
        if (neededLong > poolCapacityBytes || entryCostLong > currentOperatingCap) {
            clear();
            return;
        }
        int needed = (int) neededLong;
        int entryCost = (int) entryCostLong;

        // Step 1: stage the name if it points into this table's own pool.
        long nameAddr = srcNameAddr;
        if (srcNameAddr >= poolAddr && srcNameAddr < poolAddr + poolCapacityBytes) {
            Vect.memcpy(stagingAddr, srcNameAddr, nameLenIn);
            nameAddr = stagingAddr;
        }

        // Step 3: pick target region, possibly wrapping. Long arithmetic on the wrap
        // decision so configurations with pool capacity near Integer.MAX_VALUE can't
        // overflow the sum into a false "fits" result.
        int targetStart;
        if ((long) poolCursor + needed <= poolCapacityBytes) {
            targetStart = poolCursor;
        } else {
            poolCursor = 0;
            targetStart = 0;
        }
        int targetEnd = targetStart + needed;

        // Steps 4 + 5: overlap + operating-cap eviction, oldest first. The overlap scan
        // covers every live entry, not just the oldest: after a prior wrap, a high-offset
        // oldest entry can sit outside the target region while a low-offset newer entry
        // (written post-wrap) sits inside it. Evicting only the oldest would then let the
        // commit overwrite the newer entry's bytes. FIFO order still applies — to free a
        // newer entry we evict every older one first. The cap comparison runs in long
        // arithmetic so a misconfigured cap near Integer.MAX_VALUE can't overflow the
        // sum and short-circuit the check with the wrong branch.
        while (dynamicCount > 0) {
            boolean mustEvict = (long) currentSize + entryCost > currentOperatingCap;
            if (!mustEvict) {
                int slot = oldestSlot;
                for (int i = 0; i < dynamicCount; i++) {
                    int sStart = nameOffset[slot];
                    int sEnd = valueOffset[slot] + valueLen[slot];
                    if (sStart < targetEnd && sEnd > targetStart) {
                        mustEvict = true;
                        break;
                    }
                    slot = (slot + 1) % ringSize;
                }
            }
            if (!mustEvict) {
                break;
            }
            evictOldest();
        }

        // Step 6: commit. Write name then value contiguously via bulk memcpy so long
        // strings (cookie values, URL paths, compressed blobs) don't tie up the decode
        // loop in per-byte Unsafe calls.
        Vect.memcpy(poolAddr + targetStart, nameAddr, nameLenIn);
        Vect.memcpy(poolAddr + targetStart + nameLenIn, valueAddr, valueLenIn);
        poolCursor = targetStart + needed;
        currentSize += entryCost;

        int slot;
        if (dynamicCount == 0) {
            slot = 0;
            newestSlot = 0;
            oldestSlot = 0;
        } else {
            slot = (newestSlot + 1) % ringSize;
            newestSlot = slot;
        }
        nameOffset[slot] = targetStart;
        nameLen[slot] = nameLenIn;
        valueOffset[slot] = targetStart + nameLenIn;
        valueLen[slot] = valueLenIn;
        dynamicCount++;
    }

    public int poolCapacityBytes() {
        return poolCapacityBytes;
    }

    /**
     * Resets all per-connection state — ring pointers, pool cursor, live-size
     * accounting, and operating cap — so this instance can be handed to a new
     * connection without re-allocating the native pool or staging buffer or
     * the ring-metadata arrays. Call when returning the instance to a
     * connection pool.
     *
     * @param initialOperatingCap operating cap for the new connection
     *                            (encoder: starting {@code selectedMax};
     *                            decoder: pre-SETTINGS effective inbound cap)
     */
    public void reset(int initialOperatingCap) {
        if (initialOperatingCap < 0) {
            throw new IllegalArgumentException("initialOperatingCap must be non-negative: " + initialOperatingCap);
        }
        if (initialOperatingCap > poolCapacityBytes) {
            throw new IllegalArgumentException("initialOperatingCap (" + initialOperatingCap
                    + ") exceeds poolCapacityBytes (" + poolCapacityBytes + ")");
        }
        clear();
        this.currentOperatingCap = initialOperatingCap;
    }

    /**
     * Updates {@code currentOperatingCap}. If {@code newCap} is lower than
     * the current cap, evicts oldest entries until
     * {@code currentSize <= newCap}. Never reallocates the pool — the caller
     * has already sized the pool to the maximum cap the connection can ever
     * operate at.
     */
    public void setOperatingCap(int newCap) {
        if (newCap < 0) {
            throw new IllegalArgumentException("operatingCap must be non-negative: " + newCap);
        }
        if (newCap > poolCapacityBytes) {
            throw new IllegalArgumentException("operatingCap (" + newCap
                    + ") exceeds poolCapacityBytes (" + poolCapacityBytes + ")");
        }
        currentOperatingCap = newCap;
        while (currentSize > newCap && dynamicCount > 0) {
            evictOldest();
        }
    }

    private void clear() {
        oldestSlot = EMPTY_SLOT;
        newestSlot = EMPTY_SLOT;
        dynamicCount = 0;
        currentSize = 0;
        poolCursor = 0;
    }

    private void evictOldest() {
        int cost = Hpack.ENTRY_OVERHEAD + nameLen[oldestSlot] + valueLen[oldestSlot];
        currentSize -= cost;
        dynamicCount--;
        if (dynamicCount == 0) {
            oldestSlot = EMPTY_SLOT;
            newestSlot = EMPTY_SLOT;
        } else {
            oldestSlot = (oldestSlot + 1) % ringSize;
        }
    }

    private int slotForWireIndex(int wireIdx) {
        int offset = wireIdx - FIRST_DYNAMIC_WIRE_INDEX;
        if (offset < 0 || offset >= dynamicCount) {
            throw HpackException.instance("dynamic index out of range");
        }
        int slot = newestSlot - offset;
        if (slot < 0) {
            slot += ringSize;
        }
        return slot;
    }
}
