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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class CountSumPathsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void matchesSqlAcrossRangesNullsFramesAndReopen() throws Exception {
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 128; }
            @Override public int getSqlPageFrameMinRows() { return 64; }
        };
        try (var engine = new CairoEngine(config);
             var context = new SqlExecutionContextImpl(engine, 1).with(
                     config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
             var kernel = CountSumPaths.compile(temporary.newFolder().toPath())) {
            String[] keys = {"x%257-5", "4000000000+x%257", "(x%257)*10000003"};
            for (int shape = 0; shape < keys.length; shape++) {
                engine.execute("create table input as (select case when x%29=0 then null else " + keys[shape] + " end k,"
                        + "case when x%13=0 or x%257=7 then null else x*3-70000 end v,"
                        + "cast(case when x%17=0 or x%257=7 then null else x%100003 end as int) w,"
                        + "cast(x%31-15 as short) s from long_sequence(50003))", context);
                try (var sql = Demo.compileSql(engine, context, CountSumPaths.SQL, true);
                     var scan = CountSumPaths.openScan(engine, context)) {
                    var expected = Demo.readSql(sql, context, 1);
                    for (int workers : new int[]{1, 3}) {
                        for (boolean auto : new boolean[]{false, true}) {
                            var actual = CountSumPaths.execute(kernel, scan, context, workers, auto);
                            actual.rows().sort(CompiledGroupBy.ResultRow::compareTo);
                            assertEquals(expected, actual.rows());
                            assertEquals(50003, actual.inputRows());
                            assertEquals(auto && shape < 2, actual.dense());
                        }
                    }
                }
                engine.execute("truncate table input", context);
                try (var scan = CountSumPaths.openScan(engine, context)) {
                    assertTrue(CountSumPaths.execute(kernel, scan, context, 3, true).rows().isEmpty());
                }
                engine.execute("drop table input", context);
            }
        }
    }
}
