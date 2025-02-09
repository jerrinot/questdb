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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Mutable;

public final class ArrayBindVariable extends ArrayFunction implements Mutable {

    public ArrayBindVariable() {
        this.type = SillyArrayView.INSTANCE.getType();
    }

    public void fromView(ArrayView view) {
        // do nothing, we are a silly array after all
    }

    @Override
    public ArrayView getArray(Record rec) {
        return SillyArrayView.INSTANCE;
    }

    public void parseArray(CharSequence value) {
        // do nothing, we are a silly array after all
    }

    private static class SillyArrayView implements ArrayView {
        private static final SillyArrayView INSTANCE = new SillyArrayView();

        @Override
        public void appendWithDefaultStrides(MemoryA mem) {
            for (int i = 0, n = getSize(); i < n; i++) {
                mem.putLong(getLongAssumingDefaultStrides(i));
            }
        }

        @Override
        public int getDimCount() {
            return 1;
        }

        @Override
        public int getDimSize(int dim) {
            return 5;
        }

        @Override
        public double getDoubleAssumingDefaultStrides(int flatIndex) {
            return 0;
        }

        @Override
        public long getLongAssumingDefaultStrides(int flatIndex) {
            return flatIndex;
        }

        @Override
        public int getSize() {
            return 5;
        }

        @Override
        public int getStride(int dimension) {
            return 1;
        }

        @Override
        public int getType() {
            return ColumnType.encodeArrayType(ColumnType.LONG, 1);
        }
    }
}
