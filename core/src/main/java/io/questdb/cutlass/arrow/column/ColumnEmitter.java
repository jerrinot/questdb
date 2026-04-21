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

package io.questdb.cutlass.arrow.column;

/**
 * Narrow contract shared by all Arrow column emitters. An emitter knows
 * how to transpose {@code count} rows of its column from a source view
 * into Arrow's columnar buffer layout, writing directly into a native
 * destination buffer at the given address. Implementations return the
 * byte cursor that sits immediately after the last byte written, so
 * multi-buffer columns (for example {@code Utf8}, which needs an
 * offsets buffer and a bytes buffer) can chain writes.
 * <p>
 * Wave 6a only implements {@link Int64ColumnEmitter}. The interface
 * exists so Wave 7's {@code Float64}, {@code Utf8}, and dictionary
 * emitters slot in without touching higher-level code.
 */
public interface ColumnEmitter {

    /**
     * Writes {@code count} rows from {@code src} into the Arrow values
     * buffer starting at {@code dstAddr}. Returns the byte cursor at
     * one past the last byte written.
     *
     * @param dstAddr destination native address (must have capacity for
     *                at least the emitter's advertised bytes-per-row)
     * @param src     source row values; interpretation depends on the
     *                emitter (e.g. {@code long[]} for
     *                {@link Int64ColumnEmitter})
     * @param offset  index of the first row to emit from {@code src}
     * @param count   number of rows to emit
     * @return the address one past the last written byte
     */
    long emit(long dstAddr, long[] src, int offset, int count);
}
