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

import io.questdb.cutlass.grpc.GrpcFrameWriter;
import io.questdb.cutlass.grpc.GrpcStatus;
import io.questdb.cutlass.protobuf.HandshakeCodec;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

/**
 * Wave 5 Flight {@code Handshake} handler. Treats the RPC as
 * client-streaming: the server ignores any incoming
 * {@code HandshakeRequest} messages and, upon {@code END_STREAM},
 * replies with a single empty {@code HandshakeResponse} followed by
 * {@code grpc-status: 0}. The RPC exists so the later auth layer has a
 * hook to plug into; Wave 5 deliberately performs no authentication.
 */
public final class HandshakeHandler implements FlightSqlHandler {

    private static final Log LOG = LogFactory.getLog(HandshakeHandler.class);

    @Override
    public void onClientStreaming(FlightSqlCallContext ctx, long messageAddr, int messageLen, boolean endOfStream) {
        if (!endOfStream) {
            // Wave 5 ignores the request body; wait for client to close.
            return;
        }
        int r = ctx.emitResponseHeaders();
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("handshake response headers emit failed [rc=").$(r).I$();
            return;
        }
        long bodyWriteAddr = ctx.getResponseBodyAddr() + GrpcFrameWriter.PREFIX_LEN;
        long bodyLimit = ctx.getResponseBodyAddr() + ctx.getResponseBodyCap();
        long end = HandshakeCodec.encodeHandshakeResponse(bodyWriteAddr, bodyLimit, 0, 0, 0);
        if (end < 0) {
            LOG.error().$("handshake response scratch too small").$();
            return;
        }
        int bodyLen = (int) (end - bodyWriteAddr);
        r = ctx.emitDataMessage(bodyWriteAddr, bodyLen);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("handshake data emit failed [rc=").$(r).I$();
            return;
        }
        r = ctx.emitTrailers(GrpcStatus.OK, null);
        if (r != FlightSqlCallContext.EMIT_OK) {
            LOG.error().$("handshake trailers emit failed [rc=").$(r).I$();
        }
    }
}
