/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.line.tcp;

import org.junit.Ignore;
import org.junit.Test;

public class LineTcpReceiverFuzzTest extends AbstractLineTcpReceiverFuzzTest {

    // there seem to be an issue with the transactionality of adding new columns
    // when the issue is fixed 'newColumnFactor' can be used and this test should be enabled
    @Ignore
    @Test
    public void testAddColumns() throws Exception {
        initLoadParameters(15, 2, 2, 5, 100);
        initFuzzParameters(-1, -1, -1, 4, -1, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumns() throws Exception {
        initLoadParameters(100, 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(100, 5, 5, 5, 50);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, 5, 5, 5, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(100, 5, 5, 5, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false);
        runTest();
    }
}
