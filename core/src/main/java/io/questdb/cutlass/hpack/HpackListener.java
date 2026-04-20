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
 * Visitor callback from {@link HpackDecoder}. Called once per decoded
 * {@code (name, value)} header field.
 * <p>
 * All four address / length arguments point into caller-visible memory that
 * is valid only until {@link #onHeader} returns. The listener must copy
 * anything it wants to keep past the call. Name and value bytes live either
 * in the HPACK static-table native buffer (for pure indexed fields whose
 * name / value sit in the static table) or in the decoder's per-block
 * scratch buffer (for all literals and for dynamic-table-referenced names
 * staged to dodge eviction hazards).
 * <p>
 * {@code neverIndexed} is {@code true} for the HPACK
 * {@code literal-never-indexed} representation (RFC 7541 sec. 6.2.3). The
 * bit matters for intermediaries that must preserve the never-indexed
 * signal when re-encoding; for a leaf server it is advisory.
 */
@FunctionalInterface
public interface HpackListener {

    void onHeader(long nameAddr, int nameLen, long valueAddr, int valueLen, boolean neverIndexed);
}
