/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | |/ _ \/ __| __| | | |  _ \
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

package org.questdb.experimental.codegen;

/**
 * Slot-owned control block shared by Java and generated map kernels. Offsets are
 * bytes relative to STATE; every field is a 64-bit word. NativeMapView descriptors
 * follow the selected key layout's control block, then slot-owned prefetch scratch.
 *
 * Scan and merge callers supply START_ROW. Join probes keep their own iteration
 * state and share the miss fields and map views. A resumable exit publishes
 * RESUME_ROW in the caller's input coordinates, before updating that row's
 * aggregates. The caller services the request, refreshes any invalidated map
 * views, and retries there.
 * A VARCHAR scan may retain its prepared batch across NEED_COPY, which does not
 * move map entries or release the input frame. Other retries rebuild scratch.
 * Return zero means completion; other values outside the requests below are failures,
 * not retries.
 * Ownership of maps, key storage and allocation remains with the caller.
 */
final class MapKernelState {
    static final int START_ROW = 0;
    /** Optional row-ID array, or predicate descriptors for a fused filter. */
    static final int SELECTION = 8;
    /** Key page or descriptors, as selected by QuestMapProbe's bound layout. */
    static final int KEY_COLUMNS = 16;
    static final int SHARD = 24;
    static final int RESUME_ROW = 32;
    static final int PROBE_COUNT = 40;
    static final int KEY_COMPARISONS = 48;
    /** Zero for a single map; otherwise NUM_SHARDS - 1. */
    static final int SHARD_MASK = 56;
    static final int CONTROL_BYTES = 64;

    /** Cursor constants, also available when COLUMNS is a captured input page. */
    static final int ARGUMENT_PARAMETERS = -24;
    /** Present only for fused filters, before the aggregate descriptor and deferred-DISTINCT words. */
    static final int FILTERED_ROWS = -32;

    /** Insert a missing key through Java, or grow a mutating map; SHARD identifies it. */
    static final int NEED_KEY = 4;
    /** Reserve a batch in the unsharded destination map. */
    static final int NEED_CAPACITY = 5;
    static final int NEED_COPY = 6;
    static final int NEED_STAGING = 8;
    /** Flush bounded local state before consuming RESUME_ROW. */
    static final int NEED_FLUSH = 10;

    /** UnorderedVarcharMap copies unstable key bytes before insertion. */
    static final class VarcharKey {
        /** Source on NEED_COPY; the caller replaces it with the owned copy. */
        static final int COPY_ADDRESS = 64;
        static final int COPY_BYTES = 72;
        /** Row whose copy is available; -1 until copied, and again after insertion. */
        static final int COPIED_ROW = 80;
        /** Nonzero only when a scan left a prepared batch on NEED_COPY. */
        static final int BATCH_END = 88;
        static final int BATCH_SCRATCH = 96;
        static final int BATCH_CAPACITY_END = 104;
        static final int BATCH_START = 112;
        static final int BATCH_MASK = 120;
        static final int CONTROL_BYTES = 128;

        private VarcharKey() {
        }
    }

    /** OrderedMap variable keys borrow its staging buffer; they do not use VarcharKey. */
    static final class VariableKey {
        /** Address/capacity pair written by OrderedMap.writeNativeKeyStagingView. */
        static final int STAGING_ADDRESS = 64;
        static final int STAGING_CAPACITY = 72;
        static final int STAGING_USED = 80;
        /** Staging capacity on NEED_STAGING; destination entry bytes on NEED_KEY. */
        static final int REQUIRED_BYTES = 88;
        /** Byte offset of the unconsumed variable record when resuming a merge. */
        static final int SOURCE_OFFSET = 96;
        static final int CONTROL_BYTES = 104;

        private VariableKey() {
        }
    }

    private MapKernelState() {
    }
}
