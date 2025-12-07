/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.sql.PageFrameAddressCache;

/**
 * Interface for atoms that support prefetching of page frame data.
 * <p>
 * Prefetching uses madvise(MADV_WILLNEED) to hint the OS to load
 * memory pages ahead of time, reducing page faults on cold data.
 */
public interface PrefetchableAtom {

    /**
     * Prefetches data for the specified frame index.
     * <p>
     * Implementation should issue prefetch hints for columns used in
     * the filter expression, not all columns in the query.
     *
     * @param frameIndex the index of the frame to prefetch
     * @param cache the address cache containing page addresses and sizes
     */
    void prefetchFrame(int frameIndex, PageFrameAddressCache cache);

    /**
     * Returns the number of frames to prefetch ahead of the current frame.
     *
     * @return lookahead count, 0 or negative to disable prefetching
     */
    int getPrefetchLookahead();

    /**
     * Returns whether prefetching is enabled for this atom.
     *
     * @return true if prefetching is enabled
     */
    boolean isPrefetchEnabled();
}
