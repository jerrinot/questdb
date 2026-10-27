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

package io.questdb.cairo.map;

/**
 * Borrowed native map descriptor. Offsets are bytes; fields are 64-bit words.
 * The owning map writes and publishes this view under an exclusive reducer slot.
 * Growth invalidates it. This describes map storage, not allocation or ownership.
 */
public final class NativeMapView {
    public static final int ENTRIES = 0;
    public static final int OFFSETS = 8;
    public static final int MASK = 16;
    /** Unordered entry stride; ordered fixed stride or variable-key value size. */
    public static final int STRIDE = 24;
    public static final int ZERO_ENTRY = 32;
    public static final int HAS_ZERO = 40;
    public static final int FREE = 48;
    /** Unordered maps exclude the separately stored zero key. */
    public static final int SIZE = 56;
    public static final int PROBE_BYTES = 48;
    public static final int MUTATION_BYTES = 64;

    /** Ordered maps reuse the zero-key words for their append-only heap. */
    public static final class Ordered {
        public static final int APPEND = 32;
        public static final int LIMIT = 40;

        private Ordered() {
        }
    }

    private NativeMapView() {
    }
}
