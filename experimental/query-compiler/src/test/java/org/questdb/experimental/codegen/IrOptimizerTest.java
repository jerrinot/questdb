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
import java.util.List;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class IrOptimizerTest {
    @Test public void propagatesConstantsThroughAssignmentsAndDropsDeadDefinitions() {
        Builder b = new Builder();
        Var x = b.let(i64(0));
        b.assign(x, binary(Op.XOR, x, binary(Op.USHR, x, i64(33))));
        b.assign(x, binary(Op.MUL, x, i64(-49064778989728563L)));
        b.store(STATE, x);
        b.exit(0);
        assertEquals(new Program(new Block(List.of(new Store(STATE, i64(0)), new Return(i64(0))))),
                IrOptimizer.optimize(new Program(b.block())));
    }

    @Test public void preservesLoadsAllocationsAndTheirOrder() {
        Builder b = new Builder();
        b.let(load(COLUMNS)); // May fault even if its value is unused.
        b.let(new Allocate(i64(64)));
        Var unused = b.let(i64(0));
        b.assign(unused, load(STATE)); // Its declaration must survive too.
        b.poll();
        b.exit(0);
        Program program = new Program(b.block());
        assertEquals(program, IrOptimizer.optimize(program));
    }

    @Test public void removesUnselectedEffectsAndTruncatesAfterKnownReturn() {
        Builder b = new Builder();
        b.let(new Select(bool(true), i64(42), load(nil())));
        b.when(bool(false), dead -> dead.let(new Allocate(i64(64))));
        b.when(bool(true), taken -> taken.exit(17));
        b.store(nil(), i64(0));
        b.exit(0);
        assertEquals(new Program(new Block(List.of(new Return(i64(17))))), IrOptimizer.optimize(new Program(b.block())));
    }

    @Test public void doesNotEraseEvaluatedOperandsOfBooleanAndArithmeticExpressions() {
        Builder b = new Builder();
        b.let(binary(Op.MUL, load(COLUMNS), i64(0)));
        b.when(binary(Op.BOTH, eq(new Allocate(i64(64)), nil()), bool(false)), ignored -> ignored.exit(9));
        b.exit(0);
        Program program = new Program(b.block());
        assertEquals(program, IrOptimizer.optimize(program));
    }

    @Test public void respectsLongOverflowAndMaskedShiftCountsWhenFolding() {
        Builder b = new Builder();
        b.store(STATE, add(i64(Long.MAX_VALUE), i64(1)));
        b.store(at(STATE, 8), binary(Op.USHR, i64(Long.MIN_VALUE), i64(65)));
        b.store(at(STATE, 16), binary(Op.MUL, i64(Long.MAX_VALUE), i64(3)));
        b.exit(0);
        Program result = IrOptimizer.optimize(new Program(b.block()));
        assertEquals(i64(Long.MIN_VALUE), ((Store)result.body().statements().get(0)).value());
        assertEquals(i64(Long.MIN_VALUE >>> 65), ((Store)result.body().statements().get(1)).value());
        assertEquals(i64(Long.MAX_VALUE * 3), ((Store)result.body().statements().get(2)).value());
    }
}
