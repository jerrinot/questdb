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

import io.questdb.WorkerPoolAwareConfiguration;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;

public interface LineTcpReceiverConfiguration {

    int getAggressiveReadRetryCount();

    String getAuthDbPath();

    CairoSecurityContext getCairoSecurityContext();

    int getConnectionPoolInitialCapacity();

    int getDefaultPartitionBy();

    WorkerPoolAwareConfiguration getIOWorkerPoolConfiguration();

    /**
     * Interval in milliseconds to perform writer maintenance. Such maintenance can
     * incur cost of commit (in case of using "lag"), load rebalance and writer release.
     *
     * @return interval in milliseconds
     */
    long getMaintenanceInterval();

    /**
     * After this timeout elapsed all uncommitted rows will be committed
     *
     * @return timeout in milliseconds
     */
    long getCommitTimeout();

    int getMaxMeasurementSize();

    MicrosecondClock getMicrosecondClock();

    MillisecondClock getMillisecondClock();

    IODispatcherConfiguration getNetDispatcherConfiguration();

    int getNetMsgBufferSize();

    NetworkFacade getNetworkFacade();

    long getSymbolCacheWaitUsBeforeReload();

    LineProtoTimestampAdapter getTimestampAdapter();

    long getWriterIdleTimeout();

    int getWriterQueueCapacity();

    WorkerPoolAwareConfiguration getWriterWorkerPoolConfiguration();

    boolean isEnabled();
}
