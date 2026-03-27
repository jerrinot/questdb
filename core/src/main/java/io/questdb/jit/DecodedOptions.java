/*******************************************************************************
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

package io.questdb.jit;

/**
 * Structured decomposition of the packed options word produced by
 * {@link CompiledFilterIRSerializer#serialize}.
 * <p>
 * Layout:
 * <pre>
 * Bit 0     : debug
 * Bits 1-3  : log2(max column type size)
 * Bits 4-5  : execution hint (0=scalar, 1=single-size, 2=mixed-size)
 * Bit 6     : null checks enabled
 * </pre>
 */
public record DecodedOptions(
        boolean isDebug,
        int maxColumnTypeSizeLog2,
        int executionHint,
        boolean isNullChecksEnabled
) {
    public static final int EXEC_HINT_MIXED_SIZE = 2;
    public static final int EXEC_HINT_SCALAR = 0;
    public static final int EXEC_HINT_SINGLE_SIZE = 1;

    public static DecodedOptions decode(int options) {
        boolean debug = (options & 1) != 0;
        int maxColTypeSizeLog2 = (options >> 1) & 0x7;
        int execHint = (options >> 4) & 0x3;
        boolean nullChecks = (options & (1 << 6)) != 0;
        return new DecodedOptions(debug, maxColTypeSizeLog2, execHint, nullChecks);
    }

    public boolean isScalarOnly() {
        return executionHint == EXEC_HINT_SCALAR;
    }

    public boolean isSingleSize() {
        return executionHint == EXEC_HINT_SINGLE_SIZE;
    }

    public int maxColumnTypeSize() {
        return 1 << maxColumnTypeSizeLog2;
    }
}
