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

package org.questdb.experimental.codegen;

import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
import io.questdb.std.MemoryTracker;

import static org.questdb.experimental.codegen.Ir.*;

/** Dictionary match-set binding; representation and generated lookup are shared with INT joins. */
final class SymbolMembership implements AutoCloseable {
    static final int BYTES = IntegralMembership.BYTES;
    private final SymbolKeySetProvider source;
    private final IntegralMembership membership = new IntegralMembership();

    SymbolMembership(SymbolKeySetProvider source) { this.source = source; }

    void prepare(MemoryTracker tracker) { membership.prepare(source.getMatchedSymbolKeys(), tracker); }
    void write(long descriptor) { membership.write(descriptor); }
    static Expr contains(Builder b, Expr descriptor, Expr key) { return IntegralMembership.contains(b, descriptor, key); }
    @Override public void close() { membership.close(); }
}
