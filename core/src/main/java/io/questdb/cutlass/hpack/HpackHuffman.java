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

import io.questdb.std.Unsafe;

/**
 * HPACK Huffman codec (RFC 7541 Appendix B).
 * <p>
 * Encodes and decodes using the canonical 257-symbol code table over
 * {@code 0..255 + EOS}. Codes range from 5 to 30 bits. The encoder packs
 * MSB-first and pads the final partial byte with {@code 1} bits (a prefix of
 * the EOS code, which the decoder recognises as padding).
 * <p>
 * Decode is a bit-by-bit walk over a flat binary tree stored in parallel
 * {@link #TREE_LEFT} / {@link #TREE_RIGHT} arrays. Each entry is either a
 * non-negative internal-node index or a negative leaf whose symbol value is
 * {@code ~entry}. Per RFC 7541 sec. 5.2, three distinct padding conditions
 * are rejected as {@link HpackException}: more than 7 trailing bits, trailing
 * bits that are not a prefix of the EOS code, and the EOS symbol appearing
 * as a proper symbol rather than as padding.
 */
public final class HpackHuffman {

    /**
     * Bits per Huffman symbol, indexed by symbol {@code 0..256}. Symbol 256
     * is the EOS sentinel.
     */
    public static final int[] BITS = new int[257];

    /**
     * Canonical Huffman code bits, right-aligned, indexed by symbol
     * {@code 0..256}.
     */
    public static final int[] CODES = new int[257];

    private static final int EOS_SYMBOL = Hpack.HUFFMAN_EOS_SYMBOL;
    private static final int[] TREE_LEFT;
    private static final int[] TREE_RIGHT;

    static {
        // RFC 7541 Appendix B code table. Code bit widths range from 5 to 30.
        set(0, 0x1ff8, 13);
        set(1, 0x7fffd8, 23);
        set(2, 0xfffffe2, 28);
        set(3, 0xfffffe3, 28);
        set(4, 0xfffffe4, 28);
        set(5, 0xfffffe5, 28);
        set(6, 0xfffffe6, 28);
        set(7, 0xfffffe7, 28);
        set(8, 0xfffffe8, 28);
        set(9, 0xffffea, 24);
        set(10, 0x3ffffffc, 30);
        set(11, 0xfffffe9, 28);
        set(12, 0xfffffea, 28);
        set(13, 0x3ffffffd, 30);
        set(14, 0xfffffeb, 28);
        set(15, 0xfffffec, 28);
        set(16, 0xfffffed, 28);
        set(17, 0xfffffee, 28);
        set(18, 0xfffffef, 28);
        set(19, 0xffffff0, 28);
        set(20, 0xffffff1, 28);
        set(21, 0xffffff2, 28);
        set(22, 0x3ffffffe, 30);
        set(23, 0xffffff3, 28);
        set(24, 0xffffff4, 28);
        set(25, 0xffffff5, 28);
        set(26, 0xffffff6, 28);
        set(27, 0xffffff7, 28);
        set(28, 0xffffff8, 28);
        set(29, 0xffffff9, 28);
        set(30, 0xffffffa, 28);
        set(31, 0xffffffb, 28);
        set(32, 0x14, 6);
        set(33, 0x3f8, 10);
        set(34, 0x3f9, 10);
        set(35, 0xffa, 12);
        set(36, 0x1ff9, 13);
        set(37, 0x15, 6);
        set(38, 0xf8, 8);
        set(39, 0x7fa, 11);
        set(40, 0x3fa, 10);
        set(41, 0x3fb, 10);
        set(42, 0xf9, 8);
        set(43, 0x7fb, 11);
        set(44, 0xfa, 8);
        set(45, 0x16, 6);
        set(46, 0x17, 6);
        set(47, 0x18, 6);
        set(48, 0x0, 5);
        set(49, 0x1, 5);
        set(50, 0x2, 5);
        set(51, 0x19, 6);
        set(52, 0x1a, 6);
        set(53, 0x1b, 6);
        set(54, 0x1c, 6);
        set(55, 0x1d, 6);
        set(56, 0x1e, 6);
        set(57, 0x1f, 6);
        set(58, 0x5c, 7);
        set(59, 0xfb, 8);
        set(60, 0x7ffc, 15);
        set(61, 0x20, 6);
        set(62, 0xffb, 12);
        set(63, 0x3fc, 10);
        set(64, 0x1ffa, 13);
        set(65, 0x21, 6);
        set(66, 0x5d, 7);
        set(67, 0x5e, 7);
        set(68, 0x5f, 7);
        set(69, 0x60, 7);
        set(70, 0x61, 7);
        set(71, 0x62, 7);
        set(72, 0x63, 7);
        set(73, 0x64, 7);
        set(74, 0x65, 7);
        set(75, 0x66, 7);
        set(76, 0x67, 7);
        set(77, 0x68, 7);
        set(78, 0x69, 7);
        set(79, 0x6a, 7);
        set(80, 0x6b, 7);
        set(81, 0x6c, 7);
        set(82, 0x6d, 7);
        set(83, 0x6e, 7);
        set(84, 0x6f, 7);
        set(85, 0x70, 7);
        set(86, 0x71, 7);
        set(87, 0x72, 7);
        set(88, 0xfc, 8);
        set(89, 0x73, 7);
        set(90, 0xfd, 8);
        set(91, 0x1ffb, 13);
        set(92, 0x7fff0, 19);
        set(93, 0x1ffc, 13);
        set(94, 0x3ffc, 14);
        set(95, 0x22, 6);
        set(96, 0x7ffd, 15);
        set(97, 0x3, 5);
        set(98, 0x23, 6);
        set(99, 0x4, 5);
        set(100, 0x24, 6);
        set(101, 0x5, 5);
        set(102, 0x25, 6);
        set(103, 0x26, 6);
        set(104, 0x27, 6);
        set(105, 0x6, 5);
        set(106, 0x74, 7);
        set(107, 0x75, 7);
        set(108, 0x28, 6);
        set(109, 0x29, 6);
        set(110, 0x2a, 6);
        set(111, 0x7, 5);
        set(112, 0x2b, 6);
        set(113, 0x76, 7);
        set(114, 0x2c, 6);
        set(115, 0x8, 5);
        set(116, 0x9, 5);
        set(117, 0x2d, 6);
        set(118, 0x77, 7);
        set(119, 0x78, 7);
        set(120, 0x79, 7);
        set(121, 0x7a, 7);
        set(122, 0x7b, 7);
        set(123, 0x7ffe, 15);
        set(124, 0x7fc, 11);
        set(125, 0x3ffd, 14);
        set(126, 0x1ffd, 13);
        set(127, 0xffffffc, 28);
        set(128, 0xfffe6, 20);
        set(129, 0x3fffd2, 22);
        set(130, 0xfffe7, 20);
        set(131, 0xfffe8, 20);
        set(132, 0x3fffd3, 22);
        set(133, 0x3fffd4, 22);
        set(134, 0x3fffd5, 22);
        set(135, 0x7fffd9, 23);
        set(136, 0x3fffd6, 22);
        set(137, 0x7fffda, 23);
        set(138, 0x7fffdb, 23);
        set(139, 0x7fffdc, 23);
        set(140, 0x7fffdd, 23);
        set(141, 0x7fffde, 23);
        set(142, 0xffffeb, 24);
        set(143, 0x7fffdf, 23);
        set(144, 0xffffec, 24);
        set(145, 0xffffed, 24);
        set(146, 0x3fffd7, 22);
        set(147, 0x7fffe0, 23);
        set(148, 0xffffee, 24);
        set(149, 0x7fffe1, 23);
        set(150, 0x7fffe2, 23);
        set(151, 0x7fffe3, 23);
        set(152, 0x7fffe4, 23);
        set(153, 0x1fffdc, 21);
        set(154, 0x3fffd8, 22);
        set(155, 0x7fffe5, 23);
        set(156, 0x3fffd9, 22);
        set(157, 0x7fffe6, 23);
        set(158, 0x7fffe7, 23);
        set(159, 0xffffef, 24);
        set(160, 0x3fffda, 22);
        set(161, 0x1fffdd, 21);
        set(162, 0xfffe9, 20);
        set(163, 0x3fffdb, 22);
        set(164, 0x3fffdc, 22);
        set(165, 0x7fffe8, 23);
        set(166, 0x7fffe9, 23);
        set(167, 0x1fffde, 21);
        set(168, 0x7fffea, 23);
        set(169, 0x3fffdd, 22);
        set(170, 0x3fffde, 22);
        set(171, 0xfffff0, 24);
        set(172, 0x1fffdf, 21);
        set(173, 0x3fffdf, 22);
        set(174, 0x7fffeb, 23);
        set(175, 0x7fffec, 23);
        set(176, 0x1fffe0, 21);
        set(177, 0x1fffe1, 21);
        set(178, 0x3fffe0, 22);
        set(179, 0x1fffe2, 21);
        set(180, 0x7fffed, 23);
        set(181, 0x3fffe1, 22);
        set(182, 0x7fffee, 23);
        set(183, 0x7fffef, 23);
        set(184, 0xfffea, 20);
        set(185, 0x3fffe2, 22);
        set(186, 0x3fffe3, 22);
        set(187, 0x3fffe4, 22);
        set(188, 0x7ffff0, 23);
        set(189, 0x3fffe5, 22);
        set(190, 0x3fffe6, 22);
        set(191, 0x7ffff1, 23);
        set(192, 0x3ffffe0, 26);
        set(193, 0x3ffffe1, 26);
        set(194, 0xfffeb, 20);
        set(195, 0x7fff1, 19);
        set(196, 0x3fffe7, 22);
        set(197, 0x7ffff2, 23);
        set(198, 0x3fffe8, 22);
        set(199, 0x1ffffec, 25);
        set(200, 0x3ffffe2, 26);
        set(201, 0x3ffffe3, 26);
        set(202, 0x3ffffe4, 26);
        set(203, 0x7ffffde, 27);
        set(204, 0x7ffffdf, 27);
        set(205, 0x3ffffe5, 26);
        set(206, 0xfffff1, 24);
        set(207, 0x1ffffed, 25);
        set(208, 0x7fff2, 19);
        set(209, 0x1fffe3, 21);
        set(210, 0x3ffffe6, 26);
        set(211, 0x7ffffe0, 27);
        set(212, 0x7ffffe1, 27);
        set(213, 0x3ffffe7, 26);
        set(214, 0x7ffffe2, 27);
        set(215, 0xfffff2, 24);
        set(216, 0x1fffe4, 21);
        set(217, 0x1fffe5, 21);
        set(218, 0x3ffffe8, 26);
        set(219, 0x3ffffe9, 26);
        set(220, 0xffffffd, 28);
        set(221, 0x7ffffe3, 27);
        set(222, 0x7ffffe4, 27);
        set(223, 0x7ffffe5, 27);
        set(224, 0xfffec, 20);
        set(225, 0xfffff3, 24);
        set(226, 0xfffed, 20);
        set(227, 0x1fffe6, 21);
        set(228, 0x3fffe9, 22);
        set(229, 0x1fffe7, 21);
        set(230, 0x1fffe8, 21);
        set(231, 0x7ffff3, 23);
        set(232, 0x3fffea, 22);
        set(233, 0x3fffeb, 22);
        set(234, 0x1ffffee, 25);
        set(235, 0x1ffffef, 25);
        set(236, 0xfffff4, 24);
        set(237, 0xfffff5, 24);
        set(238, 0x3ffffea, 26);
        set(239, 0x7ffff4, 23);
        set(240, 0x3ffffeb, 26);
        set(241, 0x7ffffe6, 27);
        set(242, 0x3ffffec, 26);
        set(243, 0x3ffffed, 26);
        set(244, 0x7ffffe7, 27);
        set(245, 0x7ffffe8, 27);
        set(246, 0x7ffffe9, 27);
        set(247, 0x7ffffea, 27);
        set(248, 0x7ffffeb, 27);
        set(249, 0xffffffe, 28);
        set(250, 0x7ffffec, 27);
        set(251, 0x7ffffed, 27);
        set(252, 0x7ffffee, 27);
        set(253, 0x7ffffef, 27);
        set(254, 0x7fffff0, 27);
        set(255, 0x3ffffee, 26);
        set(EOS_SYMBOL, 0x3fffffff, 30);

        // Kraft-inequality sanity check; a well-formed canonical Huffman code satisfies
        // sum(2^-bits[i]) == 1.0 exactly.
        double kraft = 0.0;
        for (int i = 0; i <= EOS_SYMBOL; i++) {
            kraft += Math.pow(2.0, -BITS[i]);
        }
        if (Math.abs(kraft - 1.0) > 1e-9) {
            throw new AssertionError("HPACK Huffman table fails Kraft inequality, sum=" + kraft);
        }

        // Build the decoding tree. Node 0 is the root; each internal node
        // reserves two consecutive slots in TREE_LEFT / TREE_RIGHT. A negative
        // slot value encodes a leaf with symbol {@code ~value}.
        int[] left = new int[2 * (EOS_SYMBOL + 1)];
        int[] right = new int[2 * (EOS_SYMBOL + 1)];
        int[] nodeCount = {1};
        for (int sym = 0; sym <= EOS_SYMBOL; sym++) {
            insertSymbol(left, right, nodeCount, sym, CODES[sym], BITS[sym]);
        }
        // Trim to actual size. Most nodes are used; some extras from the over-allocation
        // above stay unused and we just copy the prefix.
        int used = nodeCount[0];
        TREE_LEFT = new int[used];
        TREE_RIGHT = new int[used];
        System.arraycopy(left, 0, TREE_LEFT, 0, used);
        System.arraycopy(right, 0, TREE_RIGHT, 0, used);
    }

    private HpackHuffman() {
    }

    /**
     * Decodes {@code srcLen} Huffman-encoded bytes starting at {@code srcAddr}
     * into the buffer at {@code dstAddr}, stopping before {@code dstLimit}.
     * Returns the number of decoded bytes, or {@code -1} if the decoded
     * output would not fit in {@code [dstAddr, dstLimit)}.
     *
     * @throws HpackException on malformed input: EOS symbol emitted, trailing
     *                        bits that are not an EOS-code prefix, or more
     *                        than 7 bits of trailing padding
     */
    public static int decode(long srcAddr, int srcLen, long dstAddr, long dstLimit) {
        int node = 0;
        // Bits consumed since the last symbol emission. Equal to the depth of {@code node}
        // in the tree. Tracking it alongside the walk saves a trailing-path scan and lets
        // the end-of-stream padding check run allocation-free.
        int depth = 0;
        long dstCursor = dstAddr;
        for (int i = 0; i < srcLen; i++) {
            int b = Unsafe.getUnsafe().getByte(srcAddr + i) & 0xFF;
            for (int bit = 7; bit >= 0; bit--) {
                int child = ((b >>> bit) & 1) == 0 ? TREE_LEFT[node] : TREE_RIGHT[node];
                if (child < 0) {
                    int sym = ~child;
                    if (sym == EOS_SYMBOL) {
                        throw HpackException.instance("huffman: EOS symbol");
                    }
                    if (dstCursor >= dstLimit) {
                        return -1;
                    }
                    Unsafe.getUnsafe().putByte(dstCursor, (byte) sym);
                    dstCursor++;
                    node = 0;
                    depth = 0;
                } else {
                    node = child;
                    depth++;
                }
            }
        }
        // Padding must fit in the (up-to-7) trailing bits of the last byte and must
        // match the leading bits of the EOS code (all 1s at positions corresponding
        // to the remaining depth from root).
        if (node != 0) {
            if (depth > 7) {
                throw HpackException.instance("huffman: trailing bits exceed 7");
            }
            if (!isEosPrefixPath(node, depth)) {
                throw HpackException.instance("huffman: trailing bits do not match EOS prefix");
            }
        }
        return (int) (dstCursor - dstAddr);
    }

    /**
     * Returns the exact number of bytes {@link #encode} would produce for the
     * given input, without touching any buffer.
     */
    public static int encodedLength(long srcAddr, int srcLen) {
        long bits = 0;
        for (int i = 0; i < srcLen; i++) {
            int sym = Unsafe.getUnsafe().getByte(srcAddr + i) & 0xFF;
            bits += BITS[sym];
        }
        return (int) ((bits + 7) >>> 3);
    }

    private static void insertSymbol(int[] left, int[] right, int[] nodeCount, int symbol, int code, int bits) {
        int node = 0;
        for (int bit = bits - 1; bit >= 0; bit--) {
            int go = (code >>> bit) & 1;
            int[] arr = go == 0 ? left : right;
            if (bit == 0) {
                // Leaf.
                arr[node] = ~symbol;
            } else {
                int next = arr[node];
                if (next == 0) {
                    int newNode = nodeCount[0]++;
                    arr[node] = newNode;
                    node = newNode;
                } else if (next < 0) {
                    throw new AssertionError("HPACK Huffman: code collides with existing leaf at symbol=" + symbol);
                } else {
                    node = next;
                }
            }
        }
    }

    private static boolean isEosPrefixPath(int node, int depth) {
        // Trailing bits must match the leading bits of the EOS code, which are all 1s.
        // That means the path from root to {@code node} must follow only right edges, and
        // since {@code depth} is the exact depth of {@code node}, walking {@code depth}
        // right-edges from the root lands exactly on {@code node} iff it is on the EOS
        // prefix. Allocation-free: pure integer index walk.
        int cur = 0;
        for (int i = 0; i < depth; i++) {
            int child = TREE_RIGHT[cur];
            if (child < 0) {
                return false;
            }
            cur = child;
        }
        return cur == node;
    }

    private static void set(int symbol, int code, int bits) {
        CODES[symbol] = code;
        BITS[symbol] = bits;
    }

    /**
     * Encodes {@code srcLen} raw bytes at {@code srcAddr} as an HPACK
     * Huffman-encoded bit stream starting at {@code dstAddr}. Packing is
     * MSB-first; the final partial byte is padded with {@code 1} bits (the
     * EOS-code leading bits).
     *
     * @return the new write pointer (one past the last byte written), or
     * {@code -1} if the buffer is too small. Preflight atomicity: the
     * {@code -1} path writes no bytes.
     */
    public static long encode(long dstAddr, long dstLimit, long srcAddr, int srcLen) {
        int needed = encodedLength(srcAddr, srcLen);
        if (dstLimit - dstAddr < needed) {
            return -1;
        }
        long bitBuffer = 0;
        int bitsInBuffer = 0;
        long cursor = dstAddr;
        for (int i = 0; i < srcLen; i++) {
            int sym = Unsafe.getUnsafe().getByte(srcAddr + i) & 0xFF;
            int code = CODES[sym];
            int nBits = BITS[sym];
            bitBuffer = (bitBuffer << nBits) | (code & ((1L << nBits) - 1));
            bitsInBuffer += nBits;
            while (bitsInBuffer >= 8) {
                bitsInBuffer -= 8;
                Unsafe.getUnsafe().putByte(cursor, (byte) ((bitBuffer >>> bitsInBuffer) & 0xFF));
                cursor++;
            }
        }
        if (bitsInBuffer > 0) {
            int pad = 8 - bitsInBuffer;
            bitBuffer = (bitBuffer << pad) | ((1L << pad) - 1); // pad with 1s (EOS prefix)
            Unsafe.getUnsafe().putByte(cursor, (byte) (bitBuffer & 0xFF));
            cursor++;
        }
        return cursor;
    }
}
