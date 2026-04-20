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

import java.nio.charset.StandardCharsets;

/**
 * HPACK static table (RFC 7541 Appendix A). 61 entries, 1-indexed on the wire.
 * <p>
 * All name and value bytes live in a single native buffer allocated at class
 * init and never freed — the buffer has JVM-lifetime scope. Per-entry
 * {@code (nameOffset, nameLen, valueOffset, valueLen)} tuples are stored in
 * parallel {@code int[]} arrays indexed by the static wire index (slot 0 is
 * unused).
 * <p>
 * The class also exposes a small open-addressed index over entry names, so
 * the encoder can find a matching static entry in near-constant time. Several
 * names appear at multiple indices (e.g. {@code :method} at 2 and 3, or
 * {@code :status} at 8..14); {@link #firstIndexWithName} returns the lowest
 * matching static index and {@link #nextIndexWithSameName} walks the rest of
 * the chain.
 */
public final class HpackStaticTable {

    /**
     * Number of entries in the static table.
     */
    public static final int SIZE = 61;

    private static final long BUF;
    private static final int BUF_LEN;
    // FNV-1a 32-bit constants.
    private static final int FNV_OFFSET_BASIS = 0x811c9dc5;
    private static final int FNV_PRIME = 0x01000193;
    // 1-based index into HASH_SLOTS; 0 means "empty slot".
    private static final int[] HASH_SLOTS;
    // Bit mask selecting hash slot (HASH_SLOTS.length - 1; power of two).
    private static final int HASH_SLOT_MASK;
    // NAME_CHAIN_NEXT[i] = next static index sharing the same name as entry i, or 0 if end of chain.
    private static final int[] NAME_CHAIN_NEXT;
    private static final int[] NAME_LEN = new int[SIZE + 1];
    private static final int[] NAME_OFFSET = new int[SIZE + 1];
    private static final int[] VALUE_LEN = new int[SIZE + 1];
    private static final int[] VALUE_OFFSET = new int[SIZE + 1];

    static {
        String[] names = new String[SIZE + 1];
        String[] values = new String[SIZE + 1];

        names[1]  = ":authority";                    values[1]  = "";
        names[2]  = ":method";                       values[2]  = "GET";
        names[3]  = ":method";                       values[3]  = "POST";
        names[4]  = ":path";                         values[4]  = "/";
        names[5]  = ":path";                         values[5]  = "/index.html";
        names[6]  = ":scheme";                       values[6]  = "http";
        names[7]  = ":scheme";                       values[7]  = "https";
        names[8]  = ":status";                       values[8]  = "200";
        names[9]  = ":status";                       values[9]  = "204";
        names[10] = ":status";                       values[10] = "206";
        names[11] = ":status";                       values[11] = "304";
        names[12] = ":status";                       values[12] = "400";
        names[13] = ":status";                       values[13] = "404";
        names[14] = ":status";                       values[14] = "500";
        names[15] = "accept-charset";                values[15] = "";
        names[16] = "accept-encoding";               values[16] = "gzip, deflate";
        names[17] = "accept-language";               values[17] = "";
        names[18] = "accept-ranges";                 values[18] = "";
        names[19] = "accept";                        values[19] = "";
        names[20] = "access-control-allow-origin";   values[20] = "";
        names[21] = "age";                           values[21] = "";
        names[22] = "allow";                         values[22] = "";
        names[23] = "authorization";                 values[23] = "";
        names[24] = "cache-control";                 values[24] = "";
        names[25] = "content-disposition";           values[25] = "";
        names[26] = "content-encoding";              values[26] = "";
        names[27] = "content-language";              values[27] = "";
        names[28] = "content-length";                values[28] = "";
        names[29] = "content-location";              values[29] = "";
        names[30] = "content-range";                 values[30] = "";
        names[31] = "content-type";                  values[31] = "";
        names[32] = "cookie";                        values[32] = "";
        names[33] = "date";                          values[33] = "";
        names[34] = "etag";                          values[34] = "";
        names[35] = "expect";                        values[35] = "";
        names[36] = "expires";                       values[36] = "";
        names[37] = "from";                          values[37] = "";
        names[38] = "host";                          values[38] = "";
        names[39] = "if-match";                      values[39] = "";
        names[40] = "if-modified-since";             values[40] = "";
        names[41] = "if-none-match";                 values[41] = "";
        names[42] = "if-range";                      values[42] = "";
        names[43] = "if-unmodified-since";           values[43] = "";
        names[44] = "last-modified";                 values[44] = "";
        names[45] = "link";                          values[45] = "";
        names[46] = "location";                      values[46] = "";
        names[47] = "max-forwards";                  values[47] = "";
        names[48] = "proxy-authenticate";            values[48] = "";
        names[49] = "proxy-authorization";           values[49] = "";
        names[50] = "range";                         values[50] = "";
        names[51] = "referer";                       values[51] = "";
        names[52] = "refresh";                       values[52] = "";
        names[53] = "retry-after";                   values[53] = "";
        names[54] = "server";                        values[54] = "";
        names[55] = "set-cookie";                    values[55] = "";
        names[56] = "strict-transport-security";     values[56] = "";
        names[57] = "transfer-encoding";             values[57] = "";
        names[58] = "user-agent";                    values[58] = "";
        names[59] = "vary";                          values[59] = "";
        names[60] = "via";                           values[60] = "";
        names[61] = "www-authenticate";              values[61] = "";

        int total = 0;
        for (int i = 1; i <= SIZE; i++) {
            total += names[i].length() + values[i].length();
        }
        BUF_LEN = total;
        BUF = Unsafe.malloc(total, MemoryTag.NATIVE_DEFAULT);

        int cursor = 0;
        for (int i = 1; i <= SIZE; i++) {
            byte[] n = names[i].getBytes(StandardCharsets.US_ASCII);
            NAME_OFFSET[i] = cursor;
            NAME_LEN[i] = n.length;
            for (int k = 0; k < n.length; k++) {
                Unsafe.getUnsafe().putByte(BUF + cursor, n[k]);
                cursor++;
            }
            byte[] v = values[i].getBytes(StandardCharsets.US_ASCII);
            VALUE_OFFSET[i] = cursor;
            VALUE_LEN[i] = v.length;
            for (int k = 0; k < v.length; k++) {
                Unsafe.getUnsafe().putByte(BUF + cursor, v[k]);
                cursor++;
            }
        }

        // Open-addressed name index. 128 slots cover 54 unique names at ~42% load.
        HASH_SLOTS = new int[128];
        HASH_SLOT_MASK = HASH_SLOTS.length - 1;
        NAME_CHAIN_NEXT = new int[SIZE + 1];

        for (int i = 1; i <= SIZE; i++) {
            int hash = fnv1aHash(BUF + NAME_OFFSET[i], NAME_LEN[i]);
            int slot = hash & HASH_SLOT_MASK;
            while (true) {
                int occupant = HASH_SLOTS[slot];
                if (occupant == 0) {
                    // Empty slot: claim it with this entry as the chain head.
                    HASH_SLOTS[slot] = i;
                    break;
                }
                if (NAME_LEN[occupant] == NAME_LEN[i]
                        && Vect.memeq(BUF + NAME_OFFSET[occupant], BUF + NAME_OFFSET[i], NAME_LEN[i])) {
                    // Same name: append to the chain tail. First (lowest) index stays at the head.
                    int tail = occupant;
                    while (NAME_CHAIN_NEXT[tail] != 0) {
                        tail = NAME_CHAIN_NEXT[tail];
                    }
                    NAME_CHAIN_NEXT[tail] = i;
                    break;
                }
                slot = (slot + 1) & HASH_SLOT_MASK;
            }
        }
    }

    private HpackStaticTable() {
    }

    /**
     * Returns the first static index whose name matches the given byte range,
     * or {@code -1} if no static entry carries that name. "First" means the
     * lowest wire index among entries sharing the name (e.g. 2 for
     * {@code :method}, 8 for {@code :status}).
     */
    public static int firstIndexWithName(long nameAddr, int nameLen) {
        int hash = fnv1aHash(nameAddr, nameLen);
        int slot = hash & HASH_SLOT_MASK;
        while (true) {
            int occupant = HASH_SLOTS[slot];
            if (occupant == 0) {
                return -1;
            }
            if (NAME_LEN[occupant] == nameLen
                    && Vect.memeq(BUF + NAME_OFFSET[occupant], nameAddr, nameLen)) {
                return occupant;
            }
            slot = (slot + 1) & HASH_SLOT_MASK;
        }
    }

    /**
     * Returns the static index of an exact {@code (name, value)} match, or
     * {@code -1} if none. Intended for encoder emission of fields whose both
     * name and value sit in the static table (e.g. {@code :status 200}).
     */
    public static int indexOfNameValue(long nameAddr, int nameLen, long valueAddr, int valueLen) {
        int idx = firstIndexWithName(nameAddr, nameLen);
        while (idx > 0) {
            if (VALUE_LEN[idx] == valueLen
                    && Vect.memeq(BUF + VALUE_OFFSET[idx], valueAddr, valueLen)) {
                return idx;
            }
            idx = NAME_CHAIN_NEXT[idx];
        }
        return -1;
    }

    /**
     * Single-pass lookup combining {@link #indexOfNameValue} and
     * {@link #firstIndexWithName}. Returns a packed long whose low 32 bits hold
     * the first name-only match (or {@code 0} if no name matches) and whose
     * high 32 bits hold the exact name+value match (or {@code 0}). The encoder
     * uses this to replace two hash probes + two chain walks with a single
     * probe + one chain walk.
     * <p>
     * Use {@link Numbers#decodeLowInt(long)} for {@code nameOnlyIdx} and
     * {@link Numbers#decodeHighInt(long)} for {@code exactIdx}.
     */
    public static long lookup(long nameAddr, int nameLen, long valueAddr, int valueLen) {
        int head = firstIndexWithName(nameAddr, nameLen);
        if (head <= 0) {
            return Numbers.encodeLowHighInts(0, 0);
        }
        for (int idx = head; idx > 0; idx = NAME_CHAIN_NEXT[idx]) {
            if (VALUE_LEN[idx] == valueLen
                    && Vect.memeq(BUF + VALUE_OFFSET[idx], valueAddr, valueLen)) {
                return Numbers.encodeLowHighInts(head, idx);
            }
        }
        return Numbers.encodeLowHighInts(head, 0);
    }

    /**
     * Returns the native address of the name bytes for static entry
     * {@code index} (1..{@value SIZE}).
     */
    public static long nameAddr(int index) {
        return BUF + NAME_OFFSET[index];
    }

    public static int nameLen(int index) {
        return NAME_LEN[index];
    }

    /**
     * Walks the chain of static indices sharing the same name. Used after
     * {@link #firstIndexWithName} when the encoder wants to locate an exact
     * name + value match. Returns {@code -1} at end of chain.
     */
    public static int nextIndexWithSameName(int currentIndex) {
        int next = NAME_CHAIN_NEXT[currentIndex];
        return next == 0 ? -1 : next;
    }

    public static long valueAddr(int index) {
        return BUF + VALUE_OFFSET[index];
    }

    public static int valueLen(int index) {
        return VALUE_LEN[index];
    }

    static int bufLen() {
        return BUF_LEN;
    }

    private static int fnv1aHash(long addr, int len) {
        int hash = FNV_OFFSET_BASIS;
        for (int i = 0; i < len; i++) {
            hash ^= Unsafe.getUnsafe().getByte(addr + i) & 0xFF;
            hash *= FNV_PRIME;
        }
        return hash;
    }
}
