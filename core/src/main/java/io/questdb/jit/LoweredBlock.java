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

import io.questdb.std.ObjList;

/**
 * A basic block in the lowered CFG. Contains an ordered list of
 * {@link LoweredOp} operations followed by exactly one {@link Terminator}.
 */
public final class LoweredBlock {
    private final int id;
    private final ObjList<LoweredOp> ops = new ObjList<>();
    private Terminator terminator;

    LoweredBlock(int id) {
        this.id = id;
    }

    public void addOp(LoweredOp op) {
        ops.add(op);
    }

    public int getId() {
        return id;
    }

    public LoweredOp getOp(int index) {
        return ops.getQuick(index);
    }

    public int getOpCount() {
        return ops.size();
    }

    public Terminator getTerminator() {
        return terminator;
    }

    public void setTerminator(Terminator terminator) {
        this.terminator = terminator;
    }
}
