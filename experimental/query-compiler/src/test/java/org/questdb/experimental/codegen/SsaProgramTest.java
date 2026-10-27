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
    @Test public void reusesLoadsOnlyWithinAnUnmodifiedMemoryEpoch() {
        Builder b = new Builder();
        Var a = b.let(load(COLUMNS)), same = b.let(load(COLUMNS));
        b.store(STATE, add(a, same));
        b.store(COLUMNS, ROWS);
        Var afterStore = b.let(load(COLUMNS)); b.poll();
        Var afterPoll = b.let(load(COLUMNS));
        Var allocated = b.let(new Allocate(i64(64)));
        Var afterAllocate = b.let(load(COLUMNS)); b.free(allocated, i64(64));
        Var afterFree = b.let(load(COLUMNS));
        b.store(STATE, add(add(afterStore, afterPoll), add(afterAllocate, afterFree))); b.exit(0);
        var graph = SsaProgram.lower(new Program(b.block())); SsaOptimizer.optimize(graph); SsaOptimizer.verify(graph);
        assertEquals(5, graph.blocks.stream().flatMap(x -> x.code.stream()).filter(s -> s instanceof Define d && d.value() instanceof Load).count());
    }
    @Test public void hoistsArithmeticButNeverSpeculatesLoadsOrAllocations() {
        Builder b = new Builder(); Var index = b.let(i64(0));
        b.loop(binary(Op.LT, index, ROWS), loop -> {
            Var invariant = loop.let(binary(Op.MUL, ROWS, i64(7)));
            loop.store(STATE, add(load(COLUMNS), invariant));
            loop.let(new Allocate(i64(8))); loop.assign(index, add(index, i64(1)));
        }); b.exit(0);
        var graph = SsaProgram.lower(new Program(b.block())); SsaOptimizer.optimize(graph); SsaOptimizer.verify(graph);
        var preheader = graph.blocks.get(graph.loops.getFirst().preheader());
        assertTrue(preheader.code.stream().anyMatch(s -> s instanceof Define d && d.value() instanceof Binary x && x.op() == Op.MUL));
        assertTrue(preheader.code.stream().allMatch(s -> SsaOptimizer.effect(s) == SsaOptimizer.Effect.PURE));
        assertEquals(1, graph.blocks.stream().flatMap(x -> x.code.stream()).filter(s -> s instanceof Define d && d.value() instanceof Load).count());
        assertEquals(1, graph.blocks.stream().flatMap(x -> x.code.stream()).filter(s -> s instanceof Define d && d.value() instanceof Allocate).count());
    }
    @Test public void numbersDominatingExpressionsButKeepsDistinctBranchValues() {
        Builder b = new Builder(); Var base = b.let(add(ROWS, i64(7))), result = b.let(i64(0));
        b.when(eq(load(COLUMNS), i64(1)), yes -> {
            yes.assign(result, add(add(ROWS, i64(7)), i64(13)));
            yes.store(STATE, add(base, i64(13)));
        });
        b.when(eq(load(COLUMNS), i64(2)), yes -> yes.assign(result, add(base, i64(13))));
        b.store(STATE, result); b.exit(0);
        var graph = SsaProgram.lower(new Program(b.block())); SsaOptimizer.optimize(graph); SsaOptimizer.verify(graph);
        long additions = graph.blocks.stream().flatMap(x -> x.code.stream()).filter(s -> s instanceof Define d && d.value() instanceof Binary x && x.op() == Op.ADD).count();
        assertEquals(3, additions); // Shared base; one add per sibling branch, with reuse inside the first branch.
    }
    @Test public void rejectsMissingPhiEdgesAndNonDominatingValues() {
        Builder b = new Builder(); Var value = b.let(i64(0));
        b.when(eq(ROWS, i64(1)), yes -> yes.assign(value, load(COLUMNS)));
        b.store(STATE, value); b.exit(0);
        var graph = SsaProgram.lower(new Program(b.block())); SsaOptimizer.verify(graph);
        var merge = graph.blocks.stream().filter(x -> !x.phis.isEmpty()).findFirst().orElseThrow();
        var phi = merge.phis.getFirst();
        int edge = phi.inputs.keySet().iterator().next(); Expr saved = phi.inputs.remove(edge);
        assertThrows(IllegalArgumentException.class, () -> SsaOptimizer.verify(graph));
        phi.inputs.put(edge, saved);
        Var branchOnly = (Var) phi.inputs.values().stream().filter(x -> x instanceof Var).findFirst().orElseThrow();
        graph.blocks.getFirst().code.add(new Store(STATE, branchOnly));
        assertThrows(IllegalArgumentException.class, () -> SsaOptimizer.verify(graph));
    }
}
