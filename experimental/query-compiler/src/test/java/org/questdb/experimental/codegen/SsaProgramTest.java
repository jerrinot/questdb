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

import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class SsaProgramTest {
    @Test public void rejectsMissingPhiEdgesAndNonDominatingValues() {
        Builder b = new Builder(); Var value = b.let(i64(0));
        b.when(eq(ROWS, i64(1)), yes -> yes.assign(value, load(COLUMNS)));
        b.store(STATE, value); b.exit(0);
        var graph = SsaProgram.lower(new Program(b.block())); SsaVerifier.verify(graph);
        var merge = graph.blocks.stream().filter(x -> !x.phis.isEmpty()).findFirst().orElseThrow();
        var phi = merge.phis.getFirst();
        int edge = phi.inputs.keySet().iterator().next(); Expr saved = phi.inputs.remove(edge);
        assertThrows(IllegalArgumentException.class, () -> SsaVerifier.verify(graph));
        phi.inputs.put(edge, saved);
        Var branchOnly = (Var) phi.inputs.values().stream().filter(x -> x instanceof Var).findFirst().orElseThrow();
        graph.blocks.getFirst().code.add(new Store(STATE, branchOnly));
        assertThrows(IllegalArgumentException.class, () -> SsaVerifier.verify(graph));
    }
}
