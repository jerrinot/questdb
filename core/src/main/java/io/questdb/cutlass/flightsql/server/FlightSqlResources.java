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

package io.questdb.cutlass.flightsql.server;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;

/**
 * Adapter exposing the subset of owning-connection state that Flight SQL
 * handlers need: the {@code CairoEngine} to compile SQL, the connection's
 * security principal, a per-connection circuit breaker bound to the fd,
 * a per-connection {@code SqlExecutionContextImpl}, the raw fd, and the
 * shared-query worker count used when opening record cursors.
 * <p>
 * Implemented by {@code HttpConnectionContext}. Handlers receive an
 * instance via the Flight SQL dispatcher and never talk to
 * {@code HttpConnectionContext} directly.
 */
public interface FlightSqlResources {

    CairoEngine getCairoEngine();

    long getFd();

    NetworkSqlExecutionCircuitBreaker getOrCreateCircuitBreaker();

    SqlExecutionContextImpl getOrCreateSqlExecutionContext();

    SecurityContext getSecurityContext();

    int getSharedWorkerCount();
}
