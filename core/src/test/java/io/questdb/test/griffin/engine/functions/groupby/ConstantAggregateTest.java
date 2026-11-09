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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ConstantAggregateTest extends AbstractCairoTest {
    @Test
    public void testBooleanConstantsKeepEmptyInputAndSingleResultRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table constants as (select x, true t, false f from long_sequence(4))");
            for (int rows = 0; rows <= 4; rows++) {
                String from = " from constants where x<=" + rows;
                String result = "a\tb\tc\td\n" + (rows == 0 ? "false" : "true") + "\tfalse\t"
                        + (rows == 0 ? "false" : "true") + "\tfalse\n";
                assertQuery("select bool_and(true) a,bool_and(false) b,bool_or(true) c,bool_or(false) d" + from)
                        .noLeakCheck().inferRandomAccess().expectSize().returns(result);
                assertSqlCursors("select bool_and(t),bool_and(f),bool_or(t),bool_or(f)" + from,
                        "select bool_and(true),bool_and(false),bool_or(true),bool_or(false)" + from);
            }
        });
    }

    @Test
    public void testBitwiseConstantsKeepParityAndEmptyInputAcrossWidths() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table constants as (select x,cast(7 as byte) b,cast(7 as short) h,cast(7 as int) i,7L l from long_sequence(4))");
            String[] types = {"byte", "short", "int", "long"};
            String[] columns = {"b", "h", "i", "l"};
            for (int t = 0; t < types.length; t++) {
                for (int rows = 0; rows <= 4; rows++) {
                    String value = "7::" + types[t], from = " from constants where x<=" + rows;
                    String empty = t < 2 ? "0" : "null";
                    String same = rows == 0 ? empty : "7";
                    String xor = rows == 0 ? empty : rows % 2 == 0 ? "0" : "7";
                    assertQuery("select bit_and(" + value + ") a,bit_or(" + value + ") o,bit_xor(" + value + ") x" + from)
                            .noLeakCheck().inferRandomAccess().expectSize().returns("a\to\tx\n" + same + "\t" + same + "\t" + xor + "\n");
                    assertSqlCursors("select bit_xor(" + columns[t] + ")" + from, "select bit_xor(" + value + ")" + from);
                }
            }
        });
    }

    @Test
    public void testGeometricMeanConstantKeepsEmptyInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table constants as (select x,1.0 v from long_sequence(4))");
            for (int rows = 0; rows <= 4; rows++) {
                String from = " from constants where x<=" + rows;
                assertQuery("select geomean(1.0) g" + from).noLeakCheck().inferRandomAccess().expectSize()
                        .returns("g\n" + (rows == 0 ? "null" : "1.0") + "\n");
                assertSqlCursors("select geomean(v)" + from, "select geomean(1.0)" + from);
            }
        });
    }
}
