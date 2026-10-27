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

import static org.junit.Assert.assertThrows;
import static org.questdb.experimental.codegen.Ir.*;

public class IrTest {
    @Test
    public void rejectsTypeErrors() {
        assertThrows(IllegalArgumentException.class, () -> add(i64(1), bool(true)));
        assertThrows(IllegalArgumentException.class, () -> new Load(MemoryType.I64, i64(0)));
        assertThrows(IllegalArgumentException.class, () -> new Store(STATE, bool(false)));
        assertThrows(IllegalArgumentException.class, () -> new Select(i64(0), i64(1), i64(2)));
        assertThrows(IllegalArgumentException.class, () -> new Select(bool(true), nil(), i64(0)));
        assertThrows(IllegalArgumentException.class, () -> binary(Op.LT, STATE, COLUMNS));
    }

    @Test
    public void rejectsUseBeforeDefinitionAndEscapedLocals() {
        Var undefined = new Var(3, Type.I64);
        assertThrows(IllegalArgumentException.class,
                () -> new Program(new Block(List.of(new Return(undefined)))));
        Builder builder = new Builder();
        Var[] nested = new Var[1];
        builder.when(bool(true), b -> nested[0] = b.let(i64(1)));
        builder.store(STATE, nested[0]);
        builder.exit(0);
        assertThrows(IllegalArgumentException.class, () -> new Program(builder.block()));
    }

    @Test
    public void rejectsDuplicateIdsAndMissingReturn() {
        Var a = new Var(3, Type.I64);
        Var b = new Var(3, Type.BOOL);
        assertThrows(IllegalArgumentException.class, () -> new Program(new Block(List.of(
                new Define(a, i64(0)), new Define(b, bool(true)), new Return(i64(0))))));
        assertThrows(IllegalArgumentException.class, () -> new Program(new Block(List.of())));
        assertThrows(IllegalArgumentException.class, () -> new Program(new Block(List.of(
                new Return(i64(0)), new Return(i64(1))))));
    }
}
