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

/**
 * Shared HPACK (RFC 7541) constants: representation prefix bytes, prefix
 * widths, default table size, static-table size, Huffman EOS sentinel.
 * <p>
 * Every HPACK field-block representation starts with a pattern in the high
 * bits of the first byte (RFC 7541 sec. 6):
 * <pre>
 *   1xxxxxxx                       Indexed header field (7-bit prefix)
 *   01xxxxxx                       Literal with incremental indexing (6-bit prefix)
 *   0000xxxx                       Literal without indexing (4-bit prefix)
 *   0001xxxx                       Literal never indexed (4-bit prefix)
 *   001xxxxx                       Dynamic table size update (5-bit prefix)
 * </pre>
 */
public final class Hpack {

    /**
     * Entry overhead in bytes, per RFC 7541 sec. 4.1. An entry's cost is
     * {@code 32 + len(name) + len(value)}.
     */
    public static final int ENTRY_OVERHEAD = 32;

    /**
     * Default dynamic-table size in bytes, per RFC 7541 sec. 4.2.
     */
    public static final int DEFAULT_TABLE_SIZE = 4096;

    /**
     * Huffman string-length prefix high bit. Set means the string bytes are
     * Huffman-encoded; clear means plain octets.
     */
    public static final int FLAG_STRING_HUFFMAN = 0x80;

    /**
     * Huffman EOS symbol index (257 in a 257-symbol alphabet 0..255 + EOS).
     * Exposed only for table build + padding detection; never emitted as a
     * proper decoded symbol.
     */
    public static final int HUFFMAN_EOS_SYMBOL = 256;

    /**
     * Prefix pattern bits for Literal, Incremental Indexing (6-bit prefix).
     * Set after clearing the low 6 bits of the first byte.
     */
    public static final int PATTERN_LITERAL_INCREMENTAL = 0x40;

    /**
     * Prefix pattern bits for Literal, Never Indexed (4-bit prefix).
     */
    public static final int PATTERN_LITERAL_NEVER_INDEXED = 0x10;

    /**
     * Prefix pattern bits for Literal, No Indexing (4-bit prefix). All four
     * high bits are zero on the wire.
     */
    public static final int PATTERN_LITERAL_NO_INDEXING = 0x00;

    /**
     * Prefix pattern bits for Dynamic Table Size Update (5-bit prefix).
     */
    public static final int PATTERN_SIZE_UPDATE = 0x20;

    /**
     * Prefix width (in bits) for Literal, Incremental Indexing.
     */
    public static final int PREFIX_BITS_INCREMENTAL = 6;

    /**
     * Prefix width (in bits) for an Indexed Header Field.
     */
    public static final int PREFIX_BITS_INDEXED = 7;

    /**
     * Prefix width (in bits) for Literal, No Indexing and Literal, Never
     * Indexed.
     */
    public static final int PREFIX_BITS_LITERAL = 4;

    /**
     * Prefix width (in bits) for Dynamic Table Size Update.
     */
    public static final int PREFIX_BITS_SIZE_UPDATE = 5;

    /**
     * Prefix width (in bits) for a string length.
     */
    public static final int PREFIX_BITS_STRING = 7;

    /**
     * First dynamic-table wire index. Indices 1..61 address the static table,
     * {@code STATIC_TABLE_SIZE + 1} onward address the dynamic table
     * (RFC 7541 sec. 2.3.3).
     */
    public static final int STATIC_TABLE_SIZE = 61;

    private Hpack() {
    }
}
