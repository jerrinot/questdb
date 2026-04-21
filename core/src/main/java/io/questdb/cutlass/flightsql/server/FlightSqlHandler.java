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

/**
 * Flight SQL RPC handler. One instance is installed per route at startup;
 * the dispatcher invokes exactly one of the three {@code on*} callbacks
 * per incoming RPC, selected by the route's server-side cardinality:
 * <ul>
 *   <li>Unary: single request, single response (e.g. {@code GetSchema}).</li>
 *   <li>Client-streaming: stream of request messages, single response
 *       (e.g. {@code DoPut}; {@code Handshake} is modelled as
 *       client-streaming because it is bidirectional but Wave 5 responds
 *       once, after the client's {@code END_STREAM}).</li>
 *   <li>Server-streaming: single request, stream of response messages
 *       (e.g. {@code DoGet}).</li>
 * </ul>
 * <p>
 * Wave 5 exercises only the client-streaming path.
 */
public interface FlightSqlHandler {

    /**
     * Invoked per client-streaming request message, and finally with
     * {@code endOfStream == true} once {@code END_STREAM} has arrived
     * (possibly with {@code messageLen == 0} if the client already
     * closed its side mid-stream between messages).
     */
    void onClientStreaming(FlightSqlCallContext ctx, long messageAddr, int messageLen, boolean endOfStream);

    /**
     * Invoked for server-streaming RPCs with the single incoming request
     * body. Wave 5 never calls this.
     */
    default void onServerStreaming(FlightSqlCallContext ctx, long requestAddr, int requestLen) {
        throw new UnsupportedOperationException("server-streaming not implemented in Wave 5");
    }

    /**
     * Invoked for unary RPCs with the single incoming request body.
     * Wave 5 never calls this.
     */
    default void onUnary(FlightSqlCallContext ctx, long requestAddr, int requestLen) {
        throw new UnsupportedOperationException("unary not implemented in Wave 5");
    }
}
