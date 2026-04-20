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

package io.questdb.test.cutlass.http2;

import io.questdb.cutlass.http2.Http2FlowController;
import org.junit.Assert;
import org.junit.Test;

public class Http2FlowControllerTest {

    @Test
    public void testAdjustNegativeDeltaCanGoNegative() {
        // RFC 9113 sec. 6.9.2 explicitly permits a negative window after a
        // SETTINGS_INITIAL_WINDOW_SIZE decrease.
        long result = Http2FlowController.adjustOnInitialWindowChange(100L, -500L);
        Assert.assertEquals(-400L, result);
        Assert.assertNotEquals(Http2FlowController.OVERFLOW, result);
    }

    @Test
    public void testAdjustOverflowReturnsSentinel() {
        long result = Http2FlowController.adjustOnInitialWindowChange(
                Http2FlowController.WINDOW_MAX, 1L);
        Assert.assertEquals(Http2FlowController.OVERFLOW, result);
    }

    @Test
    public void testAdjustPositiveDeltaAtMaxBoundary() {
        long result = Http2FlowController.adjustOnInitialWindowChange(
                Http2FlowController.WINDOW_MAX - 10L, 10L);
        Assert.assertEquals(Http2FlowController.WINDOW_MAX, result);
    }

    @Test
    public void testCreditAtMaxReturnsSentinel() {
        long result = Http2FlowController.credit(Http2FlowController.WINDOW_MAX, 1L);
        Assert.assertEquals(Http2FlowController.OVERFLOW, result);
    }

    @Test
    public void testCreditPositiveBelowMax() {
        Assert.assertEquals(1_100L, Http2FlowController.credit(1_000L, 100L));
        Assert.assertEquals(Http2FlowController.WINDOW_MAX,
                Http2FlowController.credit(Http2FlowController.WINDOW_MAX - 5L, 5L));
    }

    @Test
    public void testDebitBelowZeroReturnsSignedNegative() {
        // Caller (peer DATA path) maps negative to FLOW_CONTROL_ERROR; the
        // helper itself just subtracts.
        long result = Http2FlowController.debit(5L, 100L);
        Assert.assertEquals(-95L, result);
    }

    @Test
    public void testDebitPositive() {
        Assert.assertEquals(100L, Http2FlowController.debit(1_000L, 900L));
        Assert.assertEquals(0L, Http2FlowController.debit(500L, 500L));
    }
}
