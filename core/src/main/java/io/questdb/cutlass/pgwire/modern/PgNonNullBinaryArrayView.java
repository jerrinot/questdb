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

package io.questdb.cutlass.pgwire.modern;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;

final class PgNonNullBinaryArrayView implements ArrayView, Mutable {
    private final IntList dimSizes = new IntList();
    private final IntList strides = new IntList();
    private long ptr;


    void addDimSize(int size) {
        dimSizes.add(size);
    }

    void setPtrAndCalculateStrides(MemoryA mem) {
        int stride = 1;
        for (int i = dimSizes.size() - 1; i > 0; i--) {
            strides.add(stride);
            stride *= dimSizes.getQuick(i);
        }
        strides.add(stride);
    }


    @Override
    public void appendWithDefaultStrides(MemoryA mem) {

    }

    @Override
    public void clear() {
        dimSizes.clear();
        strides.clear();
    }

    @Override
    public int getDimCount() {
        return 0;
    }

    @Override
    public int getDimSize(int dim) {
        return 0;
    }

    @Override
    public double getDoubleAssumingDefaultStrides(int flatIndex) {
        return 0;
    }

    @Override
    public long getLongAssumingDefaultStrides(int flatIndex) {
        return 0;
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public int getStride(int dimension) {
        return 0;
    }

    @Override
    public int getType() {
        return 0;
    }
}
