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

package io.questdb.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.AssociativeCache;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

/**
 * The request / response handle a processor sees.
 * <p>
 * Today the handle is always the outer {@link HttpConnectionContext} (HTTP/1.x
 * plus the WebSocket upgrade mode). The HTTP/2 integration introduces a
 * per-stream adapter that implements the same surface so a processor can run
 * unchanged on top of a multiplexed connection — see
 * {@code HTTP2_INTEGRATION.md} §7.
 * <p>
 * The set of accessors is a union of what current processors read off the
 * context (request / response header pair, response sinks, session and
 * security state, SQL execution context, per-request metric counters). It
 * intentionally does not include connection-lifecycle hooks such as
 * {@link HttpConnectionContext#scheduleRetry} or the retry-attempt state —
 * those remain on the H1 context and do not belong to the per-stream view.
 */
public interface HttpRequestContext extends Locality {

    long getAuthenticationNanos();

    HttpChunkedResponse getChunkedResponse();

    NetworkSqlExecutionCircuitBreaker getCircuitBreaker();

    HttpCookieHandler getCookieHandler();

    long getFd();

    long getLastRequestBytesSent();

    @Override
    LocalValueMap getMap();

    Metrics getMetrics();

    int getNCompletedRequests();

    NetworkSqlExecutionCircuitBreaker getOrCreateCircuitBreaker(CairoEngine engine);

    SqlExecutionContextImpl getOrCreateSqlExecutionContext(CairoEngine engine, int workerCount);

    CharSequenceObjHashMap<CharSequence> getParsedCookiesMap();

    HttpRawSocket getRawResponseSocket();

    RejectProcessor getRejectProcessor();

    HttpRequestHeader getRequestHeader();

    HttpResponseHeader getResponseHeader();

    SecurityContext getSecurityContext();

    AssociativeCache<RecordCursorFactory> getSelectCache();

    @NotNull
    StringSink getSessionIdSink();

    SqlExecutionContextImpl getSqlExecutionContext();

    long getTotalBytesSent();

    long getTotalReceived();

    void resumeResponseSend() throws PeerIsSlowToReadException, PeerDisconnectedException;

    SimpleResponse simpleResponse();
}
