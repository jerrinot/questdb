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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.DenyAllSecurityContext;
import io.questdb.cairo.security.PrincipalContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.ex.BufferOverflowException;
import io.questdb.cutlass.http.ex.NotEnoughLinesException;
import io.questdb.cutlass.http.ex.RetryFailedOperationException;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.http.ex.TooFewBytesReceivedException;
import io.questdb.cutlass.http.processors.RejectProcessor;
import io.questdb.cutlass.http2.Http2ConnectionConfig;
import io.questdb.cutlass.http2.Http2ConnectionContext;
import io.questdb.cutlass.http2.Http2Preface;
import io.questdb.cutlass.http2.Http2RequestHeadersView;
import io.questdb.cutlass.http2.Http2StreamListener;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.HeartBeatException;
import io.questdb.network.IOContext;
import io.questdb.network.IOOperation;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.AssociativeCache;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjectPool;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StdoutSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.HttpResponseSink.HTTP_TOO_MANY_REQUESTS;
import static io.questdb.network.IODispatcher.*;
import static java.net.HttpURLConnection.*;

public class HttpConnectionContext extends IOContext<HttpConnectionContext>
        implements Locality, Retry, HttpRequestContext {
    private static final String FALSE = "false";
    private static final Log LOG = LogFactory.getLog(HttpConnectionContext.class);
    private static final byte MODE_H1 = 1;
    private static final byte MODE_H2 = 3;
    private static final byte MODE_H2_PREFACE_PENDING = 2;
    private static final byte MODE_SNIFFING = 0;
    // 64 KiB covers one max-size DATA frame (peer default MAX_FRAME_SIZE is
    // 16 KiB, ours is 16 KiB too) plus several control frames in the same
    // tick. When the engine is wired to real responses in B.6 this may need
    // to grow to accommodate HEADERS + CONTINUATION + DATA combined; for
    // Wave 2 only control frames flow.
    private static final int H2_SEND_BUFFER_CAP = 64 * 1024;
    private static final int NO_RESUME_PROCESSOR = Integer.MIN_VALUE;
    private static final int PEEK_SCRATCH_SIZE = Http2Preface.LENGTH;
    private static final String TRUE = "true";
    private final ActiveConnectionTracker activeConnectionTracker;
    private final HttpAuthenticator authenticator;
    private final ChunkedContentParser chunkedContentParser = new ChunkedContentParser();
    private final HttpServerConfiguration configuration;
    private final HttpCookieHandler cookieHandler;
    private final ObjectPool<DirectUtf8String> csPool;
    private final boolean dumpNetworkTraffic;
    private final int forceFragmentationReceiveChunkSize;
    private final HttpHeaderParser headerParser;
    private final LocalValueMap localValueMap = new LocalValueMap();
    private final Metrics metrics;
    private final HttpHeaderParser multipartContentHeaderParser;
    private final HttpMultipartContentParser multipartContentParser;
    private final long multipartIdleSpinCount;
    private final MultipartParserState multipartParserState = new MultipartParserState();
    private final NetworkFacade nf;
    private final CharSequenceObjHashMap<CharSequence> parsedCookies = new CharSequenceObjHashMap<>();
    private final boolean preAllocateBuffers;
    private final RejectProcessor rejectProcessor;
    private final HttpRequestValidator requestValidator = new HttpRequestValidator();
    private final HttpResponseSink responseSink;
    private final RetryAttemptAttributes retryAttemptAttributes = new RetryAttemptAttributes();
    private final RescheduleContext retryRescheduleContext = retry -> {
        LOG.info().$("Retry is requested after successful writer allocation. Retry will be re-scheduled [thread=").$(Thread.currentThread().getId()).I$();
        throw RetryOperationException.INSTANCE;
    };
    private final AssociativeCache<RecordCursorFactory> selectCache;
    private final StringSink sessionIdSink = new StringSink();
    private final HttpSessionStore sessionStore;
    private long authenticationNanos = 0L;
    private boolean connectionCounted;
    private int currentHandlerId = HttpRequestProcessorSelector.REJECT_PROCESSOR_ID;
    private boolean forceDisconnectOnComplete;
    private Http2ConnectionContext h2;
    private Http2StreamListener h2Listener;
    private int h2PrefaceBytesDrained;
    private long h2SendBuffer;
    private int h2SendBufferCap;
    // Bytes already written into h2SendBuffer but not yet handed to
    // socket.send — preserved across ticks so a short-write on WRITE
    // drains the remainder without replaying the engine.
    private int h2SendBufferPos;
    private int h2SendBufferLimit;
    private NetworkSqlExecutionCircuitBreaker httpCircuitBreaker;
    private SqlExecutionContextImpl httpSqlExecutionContext;
    private boolean isProtocolSwitched = false;  // WebSocket protocol switch flag
    private int nCompletedRequests;
    private long peekScratchAddr;
    private boolean pendingRetry = false;
    private String processorName;
    // MODE_SNIFFING until the first READ classifies the connection. The
    // H1 / WebSocket paths live at MODE_H1; MODE_H2_PREFACE_PENDING is a
    // transient state between a positive sniff match and the preface
    // drain in a later wave (§6.1 / §6.2 of HTTP2_INTEGRATION.md).
    private byte protocolMode = MODE_SNIFFING;
    private int receivedBytes;
    private long recvBuffer;
    private int recvBufferReadSize;
    private int recvBufferSize;
    private long recvPos;
    private int resumeHandlerId = NO_RESUME_PROCESSOR;
    private SecurityContext securityContext;
    private long totalBytesSent;
    private long totalReceived;

    @TestOnly
    public HttpConnectionContext(
            HttpServerConfiguration configuration,
            SocketFactory socketFactory
    ) {
        this(
                configuration,
                socketFactory,
                HttpServer.NO_OP_CACHE,
                ActiveConnectionTracker.NO_TRACKING
        );
    }

    public HttpConnectionContext(
            HttpServerConfiguration configuration,
            SocketFactory socketFactory,
            AssociativeCache<RecordCursorFactory> selectCache,
            ActiveConnectionTracker activeConnectionTracker
    ) {
        super(
                socketFactory,
                configuration.getHttpContextConfiguration().getNetworkFacade(),
                LOG
        );
        this.configuration = configuration;
        this.cookieHandler = configuration.getFactoryProvider().getHttpCookieHandler();
        this.sessionStore = configuration.getFactoryProvider().getHttpSessionStore();
        this.activeConnectionTracker = activeConnectionTracker;
        final HttpContextConfiguration contextConfiguration = configuration.getHttpContextConfiguration();
        this.nf = contextConfiguration.getNetworkFacade();
        this.csPool = new ObjectPool<>(DirectUtf8String.FACTORY, contextConfiguration.getConnectionStringPoolCapacity());
        this.headerParser = configuration.getFactoryProvider().getHttpHeaderParserFactory().newParser(contextConfiguration.getRequestHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(contextConfiguration.getMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.responseSink = new HttpResponseSink(configuration);
        this.recvBufferSize = configuration.getRecvBufferSize();
        this.preAllocateBuffers = configuration.preAllocateBuffers();
        if (preAllocateBuffers) {
            recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
            peekScratchAddr = Unsafe.malloc(PEEK_SCRATCH_SIZE, MemoryTag.NATIVE_HTTP_CONN);
            h2SendBuffer = Unsafe.malloc(H2_SEND_BUFFER_CAP, MemoryTag.NATIVE_HTTP_CONN);
            h2SendBufferCap = H2_SEND_BUFFER_CAP;
            this.responseSink.open(configuration.getSendBufferSize());
        }
        this.multipartIdleSpinCount = contextConfiguration.getMultipartIdleSpinCount();
        this.dumpNetworkTraffic = contextConfiguration.getDumpNetworkTraffic();
        // This is default behaviour until the security context is overridden with correct principal.
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.metrics = contextConfiguration.getMetrics();
        this.authenticator = contextConfiguration.getFactoryProvider().getHttpAuthenticatorFactory().getHttpAuthenticator();
        this.rejectProcessor = contextConfiguration.getFactoryProvider().getRejectProcessorFactory().getRejectProcessor(this);
        this.forceFragmentationReceiveChunkSize = contextConfiguration.getForceRecvFragmentationChunkSize();
        this.recvBufferReadSize = Math.min(forceFragmentationReceiveChunkSize, recvBufferSize);
        this.selectCache = selectCache;
    }

    // called when returning the context back to a pool (=connection closed)
    @Override
    public void clear() {
        LOG.debug().$("clear [fd=").$(getFd()).I$();
        decrementActiveConnections(getFd());
        super.clear();
        reset();
        if (this.pendingRetry) {
            LOG.error().$("reused context with retry pending").$();
        }
        this.pendingRetry = false;
        if (h2 != null) {
            h2.close();
            h2 = null;
        }
        if (h2Listener instanceof java.io.Closeable) {
            Misc.free((java.io.Closeable) h2Listener);
        }
        if (!preAllocateBuffers) {
            this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
            this.peekScratchAddr = Unsafe.free(peekScratchAddr, PEEK_SCRATCH_SIZE, MemoryTag.NATIVE_HTTP_CONN);
            this.h2SendBuffer = Unsafe.free(h2SendBuffer, h2SendBufferCap, MemoryTag.NATIVE_HTTP_CONN);
            this.h2SendBufferCap = 0;
            this.responseSink.close();
            this.headerParser.close();
            this.multipartContentHeaderParser.close();
        }
        this.forceDisconnectOnComplete = false;
        this.localValueMap.disconnect();
        // SECURITY: these unconditional resets are the safety net for the conditional
        // skip in reset(), which preserves securityContext while isProtocolSwitched is true.
        // Both fields MUST be reset here to prevent security context leaks between pooled
        // connections. Do not make these conditional.
        this.isProtocolSwitched = false;
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.protocolMode = MODE_SNIFFING;
        this.h2PrefaceBytesDrained = 0;
        this.h2SendBufferPos = 0;
        this.h2SendBufferLimit = 0;
        this.h2Listener = null;
    }

    @Override
    public void close() {
        final long fd = getFd();
        LOG.debug().$("close [fd=").$(fd).I$();
        decrementActiveConnections(fd);
        super.close();
        if (this.pendingRetry) {
            this.pendingRetry = false;
            LOG.info().$("closed context with retry pending [fd=").$(getFd()).I$();
        }
        this.nCompletedRequests = 0;
        this.totalBytesSent = 0;
        this.csPool.clear();
        this.multipartContentParser.close();
        this.multipartContentHeaderParser.close();
        this.headerParser.close();
        this.localValueMap.close();
        this.httpCircuitBreaker = Misc.free(httpCircuitBreaker);
        this.httpSqlExecutionContext = Misc.free(httpSqlExecutionContext);
        if (h2 != null) {
            h2.close();
            h2 = null;
        }
        if (h2Listener instanceof java.io.Closeable) {
            Misc.free((java.io.Closeable) h2Listener);
        }
        this.recvBuffer = Unsafe.free(recvBuffer, recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.peekScratchAddr = Unsafe.free(peekScratchAddr, PEEK_SCRATCH_SIZE, MemoryTag.NATIVE_HTTP_CONN);
        this.h2SendBuffer = Unsafe.free(h2SendBuffer, h2SendBufferCap, MemoryTag.NATIVE_HTTP_CONN);
        this.h2SendBufferCap = 0;
        this.responseSink.close();
        this.receivedBytes = 0;
        this.securityContext = DenyAllSecurityContext.INSTANCE;
        this.sessionIdSink.clear();
        this.authenticator.close();
        LOG.debug().$("closed [fd=").$(fd).I$();
    }

    @Override
    public void fail(HttpRequestProcessorSelector selector, HttpException e) throws PeerIsSlowToReadException, ServerDisconnectException {
        LOG.info().$("failed to retry query [fd=").$(getFd()).I$();
        HttpRequestProcessor processor = getHttpRequestProcessor(selector);
        failProcessor(processor, e, DISCONNECT_REASON_RETRY_FAILED);
    }

    @Override
    public RetryAttemptAttributes getAttemptDetails() {
        return retryAttemptAttributes;
    }

    public long getAuthenticationNanos() {
        return authenticationNanos;
    }

    public HttpChunkedResponse getChunkedResponse() {
        return responseSink.getChunkedResponse();
    }

    public NetworkSqlExecutionCircuitBreaker getCircuitBreaker() {
        return httpCircuitBreaker;
    }

    public HttpCookieHandler getCookieHandler() {
        return cookieHandler;
    }

    public long getLastRequestBytesSent() {
        return responseSink.getTotalBytesSent();
    }

    @Override
    public LocalValueMap getMap() {
        return localValueMap;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public int getNCompletedRequests() {
        return nCompletedRequests;
    }

    public NetworkSqlExecutionCircuitBreaker getOrCreateCircuitBreaker(CairoEngine engine) {
        if (httpCircuitBreaker == null) {
            httpCircuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine,
                    engine.getConfiguration().getCircuitBreakerConfiguration(),
                    MemoryTag.NATIVE_CB3
            );
        }
        return httpCircuitBreaker;
    }

    public SqlExecutionContextImpl getOrCreateSqlExecutionContext(CairoEngine engine, int workerCount) {
        if (httpSqlExecutionContext == null) {
            httpSqlExecutionContext = new SqlExecutionContextImpl(engine, workerCount);
        }
        return httpSqlExecutionContext;
    }

    public CharSequenceObjHashMap<CharSequence> getParsedCookiesMap() {
        return parsedCookies;
    }

    public HttpRawSocket getRawResponseSocket() {
        return responseSink.getRawSocket();
    }

    /**
     * Returns the receive buffer address for protocol-switched connections.
     */
    public long getRecvBuffer() {
        return recvBuffer;
    }

    /**
     * Returns the receive buffer size for protocol-switched connections.
     */
    public int getRecvBufferSize() {
        return recvBufferSize;
    }

    @SuppressWarnings("unused")
    public RejectProcessor getRejectProcessor() {
        return rejectProcessor;
    }

    public HttpRequestHeader getRequestHeader() {
        return headerParser;
    }

    public HttpResponseHeader getResponseHeader() {
        return responseSink.getHeader();
    }

    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public AssociativeCache<RecordCursorFactory> getSelectCache() {
        return selectCache;
    }

    public @NotNull StringSink getSessionIdSink() {
        return sessionIdSink;
    }

    /**
     * Returns the underlying socket for direct I/O after protocol switch (e.g., WebSocket).
     */
    public Socket getSocket() {
        return socket;
    }

    public SqlExecutionContextImpl getSqlExecutionContext() {
        return httpSqlExecutionContext;
    }

    public long getTotalBytesSent() {
        return totalBytesSent;
    }

    public long getTotalReceived() {
        return totalReceived;
    }

    @TestOnly
    public String getProtocolModeForTests() {
        return switch (protocolMode) {
            case MODE_SNIFFING -> "sniffing";
            case MODE_H1 -> "h1";
            case MODE_H2_PREFACE_PENDING -> "h2_preface_pending";
            case MODE_H2 -> "h2";
            default -> throw new IllegalStateException("unknown mode " + protocolMode);
        };
    }

    public boolean handleClientOperation(int operation, HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext)
            throws HeartBeatException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        if (protocolMode == MODE_SNIFFING) {
            sniffAndSelectMode();
        }
        if (protocolMode == MODE_H2_PREFACE_PENDING) {
            drainH2Preface();
        }
        if (protocolMode == MODE_H2) {
            return handleH2Operation(operation);
        }
        boolean keepGoing = switch (operation) {
            case IOOperation.READ -> handleClientRecv(selector, rescheduleContext);
            case IOOperation.WRITE -> handleClientSend(selector);
            case IOOperation.HEARTBEAT -> throw registerDispatcherHeartBeat();
            default -> throw registerDispatcherDisconnect(DISCONNECT_REASON_UNKNOWN_OPERATION);
        };

        boolean useful = keepGoing;
        if (keepGoing && protocolMode == MODE_H1) {
            if (keepConnectionAlive()) {
                do {
                    keepGoing = handleClientRecv(selector, rescheduleContext);
                } while (keepGoing);
            } else {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_KEEPALIVE_OFF);
            }
        }
        return useful;
    }

    @Override
    public boolean invalid() {
        return pendingRetry || receivedBytes > 0 || this.socket == null;
    }

    // called between requests on the same connections
    public void reset() {
        LOG.debug().$("reset [fd=").$(getFd()).$(']').$();
        this.totalBytesSent += responseSink.getTotalBytesSent();
        this.responseSink.clear();
        this.nCompletedRequests++;
        // Preserve resumeHandlerId for protocol-switched connections (e.g., WebSocket)
        if (!isProtocolSwitched) {
            this.resumeHandlerId = NO_RESUME_PROCESSOR;
        }
        this.headerParser.clear();
        this.multipartContentParser.clear();
        this.multipartContentHeaderParser.clear();
        this.csPool.clear();
        this.localValueMap.clear();
        if (httpCircuitBreaker != null) {
            httpCircuitBreaker.clear();
        }
        this.multipartParserState.multipartRetry = false;
        this.retryAttemptAttributes.waitStartTimestamp = 0;
        this.retryAttemptAttributes.lastRunTimestamp = 0;
        this.retryAttemptAttributes.attempt = 0;
        this.receivedBytes = 0;
        this.authenticationNanos = 0L;
        // Preserve securityContext for protocol-switched connections (e.g., WebSocket).
        // The security context was configured during the initial HTTP request and should
        // persist for the lifetime of the WebSocket connection.
        //
        // SECURITY: this conditional skip is safe ONLY because clear() unconditionally
        // resets both isProtocolSwitched and securityContext when the context returns to
        // the pool. The pool (WeakObjectPoolBase.push) always calls clear() before reuse.
        // Do not add code paths that return a context to the pool without calling clear().
        if (!isProtocolSwitched) {
            this.securityContext = DenyAllSecurityContext.INSTANCE;
        }
        this.sessionIdSink.clear();
        this.authenticator.clear();
        this.totalReceived = 0;
        this.chunkedContentParser.clear();
        this.recvPos = recvBuffer;
        this.rejectProcessor.clear();
    }

    public void resumeResponseSend() throws PeerIsSlowToReadException, PeerDisconnectedException {
        responseSink.resumeSend();
    }

    public void scheduleRetry(HttpRequestProcessor processor, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, ServerDisconnectException {
        try {
            pendingRetry = true;
            rescheduleContext.reschedule(this);
        } catch (RetryFailedOperationException e) {
            failProcessor(processor, e, DISCONNECT_REASON_RETRY_FAILED);
        }
    }

    public HttpResponseSink.SimpleResponseImpl simpleResponse() {
        return responseSink.simpleResponse();
    }

    @TestOnly
    public void sniffAndSelectModeForTests() throws PeerIsSlowToWriteException, ServerDisconnectException {
        sniffAndSelectMode();
    }

    /**
     * Switches the connection to a different protocol (e.g., WebSocket).
     * After calling this, normal HTTP parsing is bypassed and the processor
     * handles raw socket I/O directly. The processor is resolved via
     * {@code currentHandlerId} which was set during request routing.
     */
    public void switchProtocol() {
        this.isProtocolSwitched = true;
        this.resumeHandlerId = currentHandlerId;
    }

    @Override
    public boolean tryRerun(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
        if (pendingRetry) {
            pendingRetry = false;
            HttpRequestProcessor processor = getHttpRequestProcessor(selector);
            try {
                LOG.info().$("retrying query [fd=").$(getFd()).I$();
                processor.onRequestRetry(this);
                if (multipartParserState.multipartRetry) {
                    if (continueConsumeMultipart(
                            socket,
                            multipartParserState.start,
                            multipartParserState.buf,
                            multipartParserState.bufRemaining,
                            (HttpMultipartContentProcessor) processor,
                            retryRescheduleContext
                    )) {
                        LOG.info().$("success retried multipart import [fd=").$(getFd()).I$();
                        busyRcvLoop(selector, rescheduleContext);
                    } else {
                        LOG.info().$("retry success but import not finished [fd=").$(getFd()).I$();
                    }
                } else {
                    busyRcvLoop(selector, rescheduleContext);
                }
            } catch (RetryOperationException e2) {
                pendingRetry = true;
                return false;
            } catch (PeerDisconnectedException ignore) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_RERUN);
            } catch (PeerIsSlowToReadException e2) {
                LOG.info().$("peer is slow on running the rerun [fd=").$(getFd())
                        .$(", thread=").$(Thread.currentThread().getId()).I$();
                processor.parkRequest(this, false);
                resumeHandlerId = (processor instanceof RejectProcessor)
                        ? HttpRequestProcessorSelector.REJECT_PROCESSOR_ID : currentHandlerId;
                throw registerDispatcherWrite();
            } catch (ServerDisconnectException e) {
                LOG.info().$("kicked out [fd=").$(getFd()).I$();
                throw registerDispatcherDisconnect(DISCONNECT_REASON_KICKED_OUT_AT_RERUN);
            }
        }
        return true;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void allocateH2EngineIfNeeded() {
        if (h2 != null) {
            return;
        }
        final Http2ConnectionConfig h2Config = Http2ConnectionConfig.defaults();
        if (configuration.getHttpContextConfiguration().isFlightSqlEnabled()) {
            final io.questdb.cutlass.flightsql.server.FlightSqlCallContextPool pool =
                    new io.questdb.cutlass.flightsql.server.FlightSqlCallContextPool(
                            h2Config.ourMaxConcurrentStreams,
                            io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener.DEFAULT_MAX_MESSAGE_BYTES,
                            io.questdb.std.MemoryTag.NATIVE_HTTP_CONN);
            final io.questdb.cutlass.flightsql.server.HandshakeHandler handshake =
                    new io.questdb.cutlass.flightsql.server.HandshakeHandler();
            final io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener dispatcher =
                    new io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener(pool, handshake);
            h2 = new Http2ConnectionContext(dispatcher, h2Config);
            dispatcher.bind(h2);
            h2Listener = dispatcher;
        } else {
            h2Listener = new NoopH2Listener();
            h2 = new Http2ConnectionContext(h2Listener, h2Config);
        }
    }

    private void busyRcvLoop(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext)
            throws PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        reset();
        if (keepConnectionAlive()) {
            while (handleClientRecv(selector, rescheduleContext)) ;
        } else {
            throw registerDispatcherDisconnect(DISCONNECT_REASON_KEEPALIVE_OFF);
        }
    }

    private HttpRequestProcessor checkConnectionLimit(HttpRequestProcessor processor) {
        processorName = processor.getName();
        final int connectionLimit = activeConnectionTracker.getLimit(processorName);
        final long numOfConnections = activeConnectionTracker.inc(processorName);
        connectionCounted = true;

        if (connectionLimit != ActiveConnectionTracker.UNLIMITED) {
            assert processorName != null;
            if (numOfConnections > connectionLimit) {
                rejectProcessor.getMessageSink()
                        .put("exceeded connection limit [name=").put(processorName)
                        .put(", numOfConnections=").put(numOfConnections)
                        .put(", connectionLimit=").put(connectionLimit)
                        .put(", fd=").put(getFd())
                        .put(']');
                decrementActiveConnections(getFd());
                forceDisconnectOnComplete = true;
                return rejectProcessor.withShutdownWrite().reject(HTTP_TOO_MANY_REQUESTS);
            }
            if (processor.reservedOneAdminConnection() && numOfConnections == connectionLimit && !securityContext.isSystemAdmin()) {
                rejectProcessor.getMessageSink()
                        .put("non-admin user reached connection limit [name=").put(processorName)
                        .put(", numOfConnections=").put(numOfConnections)
                        .put(", connectionLimit=").put(connectionLimit)
                        .put(", fd=").put(getFd())
                        .put(']');
                decrementActiveConnections(getFd());
                forceDisconnectOnComplete = true;
                return rejectProcessor.withShutdownWrite().reject(HTTP_TOO_MANY_REQUESTS);
            }
            LOG.debug().$("counted connection [name=").$(processorName)
                    .$(", numOfConnections=").$(numOfConnections)
                    .$(", connectionLimit=").$(connectionLimit)
                    .$(", fd=").$(getFd())
                    .I$();
        }
        return processor;
    }

    private void completeRequest(
            HttpRequestProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        LOG.debug().$("complete [fd=").$(getFd()).I$();
        try {
            processor.onRequestComplete(this);
            reset();
        } catch (RetryOperationException e) {
            pendingRetry = true;
            scheduleRetry(processor, rescheduleContext);
        }
    }

    private boolean configureSecurityContext() {
        if (securityContext == DenyAllSecurityContext.INSTANCE) {
            final Clock clock = configuration.getHttpContextConfiguration().getNanosecondClock();
            final long authenticationStart = clock.getTicks();

            final CharSequence sessionId = cookieHandler.processSessionCookie(this);
            HttpSessionStore.SessionInfo sessionInfo = null;
            if (sessionId != null) {
                sessionInfo = sessionStore.verifySessionId(sessionId, this);
            }

            final PrincipalContext principalContext;
            if (authenticator.authenticate(headerParser)) {
                principalContext = authenticator;
            } else if (sessionInfo != null) {
                principalContext = sessionInfo;
            } else {
                // authenticationNanos stays 0, when it fails this value is irrelevant
                return false;
            }

            // auth successful, create security context from auth info
            final SecurityContextFactory scf = configuration.getFactoryProvider().getSecurityContextFactory();
            securityContext = scf.getInstance(principalContext, SecurityContextFactory.HTTP);

            if (configuration.getHttpContextConfiguration().areCookiesEnabled()) {
                // the client can request a session by sending 'session=true',
                // and close the session by sending 'session=false'
                // we do not create a session for clients by default to avoid excessive session creating
                // for clients which do not support/care about cookies, such as apps using the REST API
                final DirectUtf8Sequence sessionParam = getRequestHeader().getUrlParam(URL_PARAM_SESSION);

                // create session if
                // - we do not have one yet and the client requested one with 'session=true' or
                // - the client sent an expired/evicted session id or
                // - changed credentials without sending logout
                if (
                        (Utf8s.equalsNcAscii(TRUE, sessionParam) && sessionId == null)
                                || (sessionId != null && sessionInfo == null)
                                || (sessionInfo != null && !Chars.equals(sessionInfo.getPrincipal(), securityContext.getSessionPrincipal()))
                ) {
                    sessionStore.createSession(authenticator, this);
                } else if (Utf8s.equalsNcAscii(FALSE, sessionParam) && sessionInfo != null) {
                    // close session if client requested it
                    // note that this request is still going to be processed
                    sessionStore.destroySession(sessionInfo.getSessionId(), this);
                }
            }
            authenticationNanos = clock.getTicks() - authenticationStart;
        }
        return true;
    }

    private boolean consumeChunked(
            HttpPostPutProcessor processor,
            long headerEnd,
            long read,
            boolean newRequest
    ) throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException, PeerIsSlowToWriteException {
        if (!newRequest) {
            processor.resumeRecv(this);
        }

        while (true) {
            long lo, hi;
            int bufferLenLeft = (int) (recvBuffer + recvBufferSize - recvPos);
            if (newRequest) {
                processor.onHeadersReady(this);
                totalReceived -= headerEnd - recvBuffer;
                lo = headerEnd;
                hi = recvBuffer + read;
                newRequest = false;
            } else {
                read = socket.recv(recvPos, Math.min(forceFragmentationReceiveChunkSize, bufferLenLeft));
                lo = recvBuffer;
                hi = recvPos + read;
            }

            if (read > 0) {
                lo = chunkedContentParser.handleRecv(lo, hi, processor);
                if (lo == Long.MAX_VALUE) {
                    // done
                    processor.onRequestComplete(this);
                    reset();
                    if (keepConnectionAlive()) {
                        return true;
                    } else {
                        return disconnectHttp(processor, DISCONNECT_REASON_KEEPALIVE_OFF_RECV);
                    }
                } else if (lo == Long.MIN_VALUE) {
                    // protocol violation
                    LOG.error().$("cannot parse chunk length, chunked protocol violation, disconnecting [fd=").$(getFd()).I$();
                    return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                } else if (lo != hi) {
                    lo = -lo;
                    assert lo >= recvBuffer && lo <= hi && lo < recvBuffer + recvBufferSize;
                    if (lo != recvBuffer) {
                        // Compact recv buffer
                        Vect.memmove(recvBuffer, lo, hi - lo);
                    }
                    recvPos = recvBuffer + (hi - lo);
                } else {
                    recvPos = recvBuffer;
                }
            }

            if (read == 0 || read == forceFragmentationReceiveChunkSize) {
                // Schedule for read
                throw registerDispatcherRead();
            } else if (read < 0) {
                // client disconnected
                return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
            }
        }
    }

    private boolean consumeContent(
            long contentLength,
            Socket socket,
            HttpPostPutProcessor processor,
            long headerEnd,
            int read,
            boolean newRequest
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        if (!newRequest) {
            processor.resumeRecv(this);
        }

        while (true) {
            long lo;
            if (newRequest) {
                processor.onHeadersReady(this);
                totalReceived -= headerEnd - recvBuffer;
                lo = headerEnd;
                newRequest = false;
            } else {
                read = socket.recv(recvBuffer, recvBufferReadSize);
                lo = recvBuffer;
            }

            if (read > 0) {
                if (totalReceived + read > contentLength) {
                    // HTTP protocol violation
                    // client sent more data than it promised in Content-Length header
                    // we will disconnect client and roll back
                    return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                }

                processor.onChunk(lo, recvBuffer + read);
                totalReceived += read;

                if (totalReceived == contentLength) {
                    // we have received all content, commit
                    // check that client has not disconnected
                    read = socket.recv(recvBuffer, recvBufferSize);
                    if (read < 0) {
                        // client disconnected, don't commit, rollback instead
                        return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
                    } else if (read > 0) {
                        // HTTP protocol violation
                        // client sent more data than it promised in Content-Length header
                        // we will disconnect client and roll back
                        return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES);
                    }

                    processor.onRequestComplete(this);
                    reset();
                    if (keepConnectionAlive()) {
                        return true;
                    } else {
                        return disconnectHttp(processor, DISCONNECT_REASON_KEEPALIVE_OFF_RECV);
                    }
                }
            }

            if (read == 0 || read == forceFragmentationReceiveChunkSize) {
                // Schedule for read
                throw registerDispatcherRead();
            } else if (read < 0) {
                // client disconnected
                return disconnectHttp(processor, DISCONNECT_REASON_KICKED_OUT_AT_RECV);
            }
        }
    }

    private boolean consumeMultipart(
            Socket socket,
            HttpMultipartContentProcessor processor,
            long headerEnd,
            int read,
            boolean newRequest,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        if (newRequest) {
            if (!headerParser.hasBoundary()) {
                LOG.error().$("Bad request. Form data in multipart POST expected.").$(". Disconnecting [fd=").$(getFd()).I$();
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PROTOCOL_VIOLATION);
            }
            processor.onHeadersReady(this);
            multipartContentParser.of(headerParser.getBoundary());
        }

        processor.resumeRecv(this);

        final long bufferEnd = recvBuffer + read;

        LOG.debug().$("multipart").$();

        // read socket into buffer until there is nothing to read
        long start;
        long buf;
        int bufRemaining;

        if (headerEnd < bufferEnd) {
            start = headerEnd;
            buf = bufferEnd;
            bufRemaining = (int) (recvBufferSize - (bufferEnd - recvBuffer));
        } else {
            start = recvBuffer;
            buf = start + receivedBytes;
            bufRemaining = recvBufferSize - receivedBytes;
            receivedBytes = 0;
        }

        return continueConsumeMultipart(socket, start, buf, bufRemaining, processor, rescheduleContext);
    }

    private boolean continueConsumeMultipart(
            Socket socket,
            long start,
            long buf,
            int bufRemaining,
            HttpMultipartContentProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, PeerIsSlowToWriteException {
        boolean keepGoing = false;

        if (buf > start) {
            try {
                if (parseMultipartResult(start, buf, bufRemaining, processor, rescheduleContext)) {
                    return true;
                }

                buf = start = recvBuffer;
                bufRemaining = recvBufferSize;
            } catch (TooFewBytesReceivedException e) {
                start = multipartContentParser.getResumePtr();
            }
        }

        long spinsRemaining = multipartIdleSpinCount;

        while (true) {
            final int n = socket.recv(buf, bufRemaining);
            if (n < 0) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_MULTIPART_RECV);
            }

            if (n == 0) {
                // Text loader needs as big of a data chunk as possible
                // to analyse columns and delimiters correctly. To make sure we
                // can deliver large data chunk we have to implement mini-Nagle
                // algorithm by accumulating small data chunks client could be
                // sending into our receive buffer. To make sure we don't
                // sit around accumulating for too long we have spin limit
                if (spinsRemaining-- > 0) {
                    continue;
                }

                // do we have anything in the buffer?
                if (buf > start) {
                    try {
                        if (parseMultipartResult(start, buf, bufRemaining, processor, rescheduleContext)) {
                            keepGoing = true;
                            break;
                        }

                        buf = start = recvBuffer;
                        bufRemaining = recvBufferSize;
                        continue;
                    } catch (TooFewBytesReceivedException e) {
                        start = multipartContentParser.getResumePtr();
                        shiftReceiveBufferUnprocessedBytes(start, (int) (buf - start));
                        throw registerDispatcherRead();
                    }
                }

                LOG.debug().$("peer is slow [multipart]").$();
                throw registerDispatcherRead();
            }

            LOG.debug().$("multipart recv [len=").$(n).I$();

            dumpBuffer(buf, n);

            bufRemaining -= n;
            buf += n;

            if (bufRemaining == 0) {
                try {
                    if (buf - start > 1) {
                        if (parseMultipartResult(start, buf, bufRemaining, processor, rescheduleContext)) {
                            keepGoing = true;
                            break;
                        }
                    }

                    buf = start = recvBuffer;
                    bufRemaining = recvBufferSize;
                } catch (TooFewBytesReceivedException e) {
                    start = multipartContentParser.getResumePtr();
                    int unprocessedSize = (int) (buf - start);
                    // Shift to start
                    if (unprocessedSize < recvBufferSize) {
                        start = multipartContentParser.getResumePtr();
                        shiftReceiveBufferUnprocessedBytes(start, unprocessedSize);
                        throw registerDispatcherRead();
                    } else {
                        // Header does not fit receive buffer
                        failProcessor(processor, BufferOverflowException.INSTANCE, DISCONNECT_REASON_MULTIPART_HEADER_TOO_BIG);
                    }
                    break;
                }
            }
        }
        return keepGoing;
    }

    private void decrementActiveConnections(long fd) {
        if (processorName != null && connectionCounted) {
            long activeConnections = activeConnectionTracker.dec(processorName);
            LOG.debug().$("decrementing active connections [name=").$(processorName)
                    .$(", activeConnections=").$(activeConnections)
                    .$(", fd=").$(fd)
                    .I$();
            processorName = null;
        }
        connectionCounted = false;
    }

    private boolean disconnectHttp(HttpRequestProcessor processor, int reason) throws ServerDisconnectException {
        processor.onConnectionClosed(this);
        reset();
        throw registerDispatcherDisconnect(reason);
    }

    private void drainH2Preface() throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
        // Cursor h2PrefaceBytesDrained persists across ticks: when recv short-reads
        // and we throw registerDispatcherRead, the next READ resumes at the same
        // offset without replaying bytes we already validated. Bytes land in
        // peekScratchAddr — the 24-byte buffer doubles as the drain window and is
        // discarded once the preface finishes.
        while (h2PrefaceBytesDrained < Http2Preface.LENGTH) {
            final int toRead = Http2Preface.LENGTH - h2PrefaceBytesDrained;
            final int n = socket.recv(peekScratchAddr + h2PrefaceBytesDrained, toRead);
            if (n < 0) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV);
            }
            if (n == 0) {
                throw registerDispatcherRead();
            }
            if (!Http2Preface.matches(peekScratchAddr, h2PrefaceBytesDrained, h2PrefaceBytesDrained + n)) {
                LOG.error().$("h2 preface mismatch [fd=").$(getFd()).I$();
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PROTOCOL_VIOLATION);
            }
            h2PrefaceBytesDrained += n;
        }
        allocateH2EngineIfNeeded();
        final long written = h2.emitInitialSettings(h2SendBuffer, h2SendBuffer + h2SendBufferCap);
        h2SendBufferPos = 0;
        h2SendBufferLimit = (int) (written - h2SendBuffer);
        flushH2Send();
        protocolMode = MODE_H2;
        LOG.info().$("h2 upgrade complete [fd=").$(getFd()).I$();
    }

    private void dumpBuffer(long buffer, int size) {
        if (dumpNetworkTraffic && size > 0) {
            StdoutSink.INSTANCE.put('>');
            Net.dump(buffer, size);
        }
    }

    private void failProcessor(HttpRequestProcessor processor, HttpException e, int reason) throws PeerIsSlowToReadException, ServerDisconnectException {
        pendingRetry = false;
        boolean canReset = true;
        try {
            LOG.info()
                    .$("failed query result cannot be delivered. Kicked out [fd=").$(getFd())
                    .$(", error=").$safe(e.getFlyweightMessage())
                    .I$();
            processor.failRequest(this, e);
            throw registerDispatcherDisconnect(reason);
        } catch (PeerDisconnectedException peerDisconnectedException) {
            throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
        } catch (PeerIsSlowToReadException peerIsSlowToReadException) {
            LOG.info().$("peer is slow to receive failed to retry response [fd=").$(getFd()).I$();
            processor.parkRequest(this, false);
            resumeHandlerId = (processor instanceof RejectProcessor)
                    ? HttpRequestProcessorSelector.REJECT_PROCESSOR_ID : currentHandlerId;
            canReset = false;
            throw registerDispatcherWrite();
        } finally {
            if (canReset) {
                reset();
            }
        }
    }

    private void flushH2Send() throws PeerIsSlowToReadException {
        while (h2SendBufferPos < h2SendBufferLimit) {
            final int remaining = h2SendBufferLimit - h2SendBufferPos;
            final int sent = socket.send(h2SendBuffer + h2SendBufferPos, remaining);
            if (sent < 0) {
                LOG.info().$("h2 peer disconnected on send [fd=").$(getFd()).I$();
                // Promote to ServerDisconnectException at the caller; for Wave 2
                // signal via slow-to-read re-register so WRITE tick picks it up.
                throw registerDispatcherWrite();
            }
            if (sent == 0) {
                throw registerDispatcherWrite();
            }
            h2SendBufferPos += sent;
        }
        h2SendBufferPos = 0;
        h2SendBufferLimit = 0;
    }

    private HttpRequestProcessor getHttpRequestProcessor(HttpRequestProcessorSelector selector) {
        final HttpRequestProcessor processor = selector.select(headerParser);
        this.currentHandlerId = selector.getLastSelectedHandlerId();
        return requestValidator.validateRequestType(processor, rejectProcessor);
    }

    private boolean handleClientRecv(HttpRequestProcessorSelector selector, RescheduleContext rescheduleContext) throws PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
        // Handle protocol-switched connections (e.g., WebSocket)
        if (isProtocolSwitched && resumeHandlerId != NO_RESUME_PROCESSOR) {
            return handleProtocolSwitchedRecv(selector);
        }

        boolean busyRecv = true;
        try {
            // this is address of where header ended in our receiving buffer
            // we need to process request content starting from this address
            long headerEnd = recvBuffer;
            int read = 0;
            final boolean newRequest = headerParser.isIncomplete();
            if (newRequest) {
                while (headerParser.isIncomplete()) {
                    // read headers
                    read = socket.recv(recvBuffer, recvBufferReadSize);
                    LOG.debug().$("recv [fd=").$(getFd()).$(", count=").$(read).I$();
                    if (read < 0 && !headerParser.onRecvError(read)) {
                        LOG.debug()
                                .$("done [fd=").$(getFd())
                                .$(", errno=").$(nf.errno())
                                .I$();
                        // peer disconnect
                        throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV);
                    }

                    if (read == 0) {
                        // client is not sending anything
                        throw registerDispatcherRead();
                    }

                    dumpBuffer(recvBuffer, read);
                    headerEnd = headerParser.parse(recvBuffer, recvBuffer + read, true, false);
                }
                requestValidator.of(headerParser);
            }

            requestValidator.validateRequestHeader(rejectProcessor);
            HttpRequestProcessor processor = rejectProcessor.isRequestBeingRejected() ? rejectProcessor : getHttpRequestProcessor(selector);

            DirectUtf8Sequence acceptEncoding = headerParser.getHeader(HEADER_CONTENT_ACCEPT_ENCODING);
            if (configuration.getHttpContextConfiguration().allowDeflateBeforeSend()
                    && acceptEncoding != null
                    && Utf8s.containsAscii(acceptEncoding, "gzip")) {
                // re-read send buffer size in case the config was reloaded
                responseSink.setDeflateBeforeSend(true, configuration.getSendBufferSize());
            }

            try {
                if (newRequest) {
                    final boolean cookiesEnabled = configuration.getHttpContextConfiguration().areCookiesEnabled();
                    if (cookiesEnabled) {
                        if (!cookieHandler.parseCookies(this)) {
                            processor = rejectProcessor;
                        }
                    }

                    try {
                        if (processor.requiresAuthentication() && !configureSecurityContext()) {
                            final byte requiredAuthType = processor.getRequiredAuthType();
                            processor = rejectProcessor.withAuthenticationType(requiredAuthType).reject(HTTP_UNAUTHORIZED);
                        }
                    } catch (CairoException e) {
                        processor = rejectProcessor.reject(HTTP_INTERNAL_ERROR, e.getFlyweightMessage());
                    }

                    if (cookiesEnabled) {
                        if (!processor.processServiceAccountCookie(this, securityContext)) {
                            processor = rejectProcessor;
                        }
                    }

                    try {
                        securityContext.checkEntityEnabled();
                    } catch (CairoException e) {
                        processor = rejectProcessor.reject(HTTP_FORBIDDEN, e.getFlyweightMessage());
                    }
                }

                if (!connectionCounted && !processor.ignoreConnectionLimitCheck()) {
                    processor = checkConnectionLimit(processor);
                }

                final long contentLength = headerParser.getContentLength();
                final boolean chunked = HttpKeywords.isChunked(headerParser.getHeader(HEADER_TRANSFER_ENCODING));
                final boolean multipartRequest = HttpKeywords.isContentTypeMultipartFormData(headerParser.getContentType())
                        || HttpKeywords.isContentTypeMultipartMixed(headerParser.getContentType());

                if (multipartRequest) {
                    busyRecv = consumeMultipart(socket, (HttpMultipartContentProcessor) processor, headerEnd, read, newRequest, rescheduleContext);
                } else if (chunked) {
                    busyRecv = consumeChunked((HttpPostPutProcessor) processor, headerEnd, read, newRequest);
                } else if (contentLength > 0) {
                    busyRecv = consumeContent(contentLength, socket, (HttpPostPutProcessor) processor, headerEnd, read, newRequest);
                } else {
                    // Do not expect any more bytes to be sent to us before
                    // we respond back to client. We will disconnect the client when
                    // they abuse protocol. In addition, we will not call processor
                    // if client has disconnected before we had a chance to reply.
                    read = socket.recv(recvBuffer, 1);

                    if (read != 0) {
                        dumpBuffer(recvBuffer, read);
                        LOG.info().$("disconnect after request [fd=").$(getFd()).$(", read=").$(read).I$();
                        int reason = read > 0 ? DISCONNECT_REASON_KICKED_OUT_AT_EXTRA_BYTES : DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV;
                        throw registerDispatcherDisconnect(reason);
                    } else {
                        processor.onHeadersReady(this);
                        LOG.debug().$("good [fd=").$(getFd()).I$();
                        processor.onRequestComplete(this);
                        // Don't clear resumeHandlerId for protocol-switched connections (e.g., WebSocket)
                        if (!isProtocolSwitched) {
                            resumeHandlerId = NO_RESUME_PROCESSOR;
                        }
                        reset();
                    }
                }
            } catch (RetryOperationException e) {
                pendingRetry = true;
                scheduleRetry(processor, rescheduleContext);
                busyRecv = false;
            } catch (PeerDisconnectedException e) {
                return disconnectHttp(processor, DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV);
            } catch (PeerIsSlowToReadException e) {
                LOG.debug().$("peer is slow reader [two]").$();
                // it is important to assign resume handler ID before we fire
                // event off to dispatcher
                processor.parkRequest(this, false);
                resumeHandlerId = (processor instanceof RejectProcessor)
                        ? HttpRequestProcessorSelector.REJECT_PROCESSOR_ID : currentHandlerId;
                throw registerDispatcherWrite();
            }
        } catch (ServerDisconnectException | PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
            throw e;
        } catch (HttpException e) {
            LOG.error().$("http error [fd=").$(getFd()).$(", e=`").$safe(e.getFlyweightMessage()).$("`]").$();
            throw registerDispatcherDisconnect(DISCONNECT_REASON_PROTOCOL_VIOLATION);
        } catch (Throwable e) {
            LOG.error().$("internal error [fd=").$(getFd()).$(", e=`").$(e).$("`]").$();
            throw registerDispatcherDisconnect(DISCONNECT_REASON_SERVER_ERROR);
        }
        return busyRecv;
    }

    private boolean handleClientSend(HttpRequestProcessorSelector selector) throws PeerIsSlowToReadException, ServerDisconnectException {
        if (resumeHandlerId != NO_RESUME_PROCESSOR) {
            final HttpRequestProcessor proc = resolveResumeProcessor(selector);
            try {
                proc.resumeSend(this);
                reset();
                return true;
            } catch (PeerIsSlowToReadException ignore) {
                proc.parkRequest(this, false);
                LOG.debug().$("peer is slow reader").$();
                throw registerDispatcherWrite();
                // resumeHandlerId stays set (re-park with same ID)
            } catch (PeerDisconnectedException ignore) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_SEND);
            } catch (ServerDisconnectException ignore) {
                LOG.info().$("kicked out [fd=").$(getFd()).I$();
                throw registerDispatcherDisconnect(DISCONNECT_REASON_KICKED_OUT_AT_SEND);
            }
        } else {
            LOG.error().$("spurious write request [fd=").$(getFd()).I$();
        }
        return false;
    }

    private boolean handleH2Operation(int operation) throws HeartBeatException, PeerIsSlowToReadException, PeerIsSlowToWriteException, ServerDisconnectException {
        // Flush any deferred send bytes first. A prior tick may have short-written
        // on a slow-reader peer; draining here keeps the outbound order intact
        // before the engine emits anything new.
        if (h2SendBufferPos < h2SendBufferLimit) {
            flushH2Send();
        }
        boolean useful = false;
        if (operation == IOOperation.READ || operation == IOOperation.WRITE) {
            if (operation == IOOperation.READ) {
                final int read = socket.recv(recvBuffer, recvBufferSize);
                if (read < 0) {
                    throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_RECV);
                }
                if (read > 0) {
                    h2.processReceivedBytes(recvBuffer, recvBuffer + read);
                    useful = true;
                }
            }
            final long written = h2.writePending(h2SendBuffer, h2SendBuffer + h2SendBufferCap);
            final int drainedBytes = (int) (written - h2SendBuffer);
            if (drainedBytes > 0) {
                h2SendBufferPos = 0;
                h2SendBufferLimit = drainedBytes;
                flushH2Send();
                useful = true;
            }
            if (h2.getState() == Http2ConnectionContext.STATE_CLOSED && h2SendBufferLimit == h2SendBufferPos) {
                throw registerDispatcherDisconnect(DISCONNECT_REASON_UNKNOWN_OPERATION);
            }
            if (operation == IOOperation.READ) {
                throw registerDispatcherRead();
            }
            return useful;
        }
        if (operation == IOOperation.HEARTBEAT) {
            throw registerDispatcherHeartBeat();
        }
        throw registerDispatcherDisconnect(DISCONNECT_REASON_UNKNOWN_OPERATION);
    }

    /**
     * Handles receive for protocol-switched connections (e.g., WebSocket).
     * Instead of parsing HTTP, delegates to the processor's resumeRecv.
     */
    private boolean handleProtocolSwitchedRecv(HttpRequestProcessorSelector selector) throws PeerIsSlowToWriteException, ServerDisconnectException, PeerIsSlowToReadException {
        final HttpRequestProcessor processor = resolveResumeProcessor(selector);
        try {
            processor.resumeRecv(this);
            // resumeRecv is not designed to complete normally (has a while-true loop). This line is unreachable.
            return true;
        } catch (PeerIsSlowToReadException | PeerIsSlowToWriteException e) {
            // Need more data from/to peer
            throw e;
        } catch (ServerDisconnectException e) {
            // Connection should be closed
            LOG.info().$("protocol-switched connection closing [fd=").$(getFd()).I$();
            processor.onConnectionClosed(this);
            throw e;
        } catch (Throwable e) {
            // Any other error, close connection
            LOG.error().$("error in protocol-switched recv [fd=").$(getFd()).$(", e=").$(e).I$();
            processor.onConnectionClosed(this);
            throw registerDispatcherDisconnect(DISCONNECT_REASON_SERVER_ERROR);
        }
    }

    private boolean keepConnectionAlive() {
        return !forceDisconnectOnComplete && configuration.getHttpContextConfiguration().getServerKeepAlive();
    }

    private boolean parseMultipartResult(
            long start,
            long buf,
            int bufRemaining,
            HttpMultipartContentProcessor processor,
            RescheduleContext rescheduleContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException, TooFewBytesReceivedException {
        boolean parseResult;
        try {
            parseResult = multipartContentParser.parse(start, buf, processor);
        } catch (RetryOperationException e) {
            this.multipartParserState.saveFdBufferPosition(multipartContentParser.getResumePtr(), buf, bufRemaining);
            throw e;
        } catch (NotEnoughLinesException e) {
            failProcessor(processor, e, DISCONNECT_REASON_KICKED_TXT_NOT_ENOUGH_LINES);
            parseResult = false;
        }

        if (parseResult) {
            // request is complete
            completeRequest(processor, rescheduleContext);
            return true;
        }
        return false;
    }

    private HttpRequestProcessor resolveResumeProcessor(HttpRequestProcessorSelector selector) {
        if (resumeHandlerId == HttpRequestProcessorSelector.REJECT_PROCESSOR_ID) {
            return rejectProcessor;
        }
        HttpRequestProcessor processor = selector.resolveProcessorById(resumeHandlerId, headerParser);
        return processor != null ? processor : rejectProcessor;
    }

    private void shiftReceiveBufferUnprocessedBytes(long start, int receivedBytes) {
        // Shift to start
        this.receivedBytes = receivedBytes;
        Vect.memmove(recvBuffer, start, receivedBytes);
        LOG.debug().$("peer is slow, waiting for bigger part to parse [multipart]").$();
    }

    private void sniffAndSelectMode() throws PeerIsSlowToWriteException, ServerDisconnectException {
        if (!configuration.getHttpContextConfiguration().isH2Enabled()) {
            protocolMode = MODE_H1;
            return;
        }
        final int peeked = nf.peekRaw(socket.getFd(), peekScratchAddr, PEEK_SCRATCH_SIZE);
        if (peeked < 0) {
            throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV);
        }
        if (peeked == 0) {
            throw registerDispatcherRead();
        }
        final int classification = Http2Preface.detect(peekScratchAddr, peeked);
        if (classification == Http2Preface.MATCH) {
            protocolMode = MODE_H2_PREFACE_PENDING;
            return;
        }
        if (classification == Http2Preface.INCOMPLETE) {
            throw registerDispatcherRead();
        }
        // NO_MATCH: peeked bytes are non-H2 request bytes. MSG_PEEK left
        // them in the kernel buffer; the H1 parser's first recv consumes them.
        protocolMode = MODE_H1;
    }

    @Override
    protected void doInit() throws TlsSessionInitFailedException {
        // the context is obtained from the pool, so we should initialize the memory
        if (recvBuffer == 0) {
            // re-read recv buffer size in case the config was reloaded
            recvBufferSize = configuration.getRecvBufferSize();
            recvBufferReadSize = Math.min(forceFragmentationReceiveChunkSize, recvBufferSize);
            recvBuffer = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_HTTP_CONN);
        }
        if (peekScratchAddr == 0) {
            peekScratchAddr = Unsafe.malloc(PEEK_SCRATCH_SIZE, MemoryTag.NATIVE_HTTP_CONN);
        }
        if (h2SendBuffer == 0) {
            h2SendBuffer = Unsafe.malloc(H2_SEND_BUFFER_CAP, MemoryTag.NATIVE_HTTP_CONN);
            h2SendBufferCap = H2_SEND_BUFFER_CAP;
        }
        // re-read buffer sizes in case the config was reloaded
        responseSink.of(socket, configuration.getSendBufferSize());
        headerParser.reopen(configuration.getHttpContextConfiguration().getRequestHeaderBufferSize());
        multipartContentHeaderParser.reopen(configuration.getHttpContextConfiguration().getMultipartHeaderBufferSize());

        if (socket.supportsTls()) {
            socket.startTlsSession(null);
        }
        connectionCounted = false;
    }

    /**
     * Logging no-op {@link Http2StreamListener}. Installed on every new
     * {@link Http2ConnectionContext} so the H2 engine has a valid listener
     * to call; the Flight SQL handler surface (M2) substitutes its own
     * listener at bind time. No response bytes ever leave the connection
     * under this listener — the point is to let the engine accept
     * connections, drain the preface, parse HEADERS / DATA / trailers, and
     * observe the callback traffic without wiring a dispatch path that
     * Wave 4 has explicitly retired.
     */
    private final class NoopH2Listener implements Http2StreamListener {
        @Override
        public boolean onData(int streamId, long addr, int dataLen, boolean endStream, int generationToken) {
            LOG.debug().$("h2 onData [sid=").$(streamId)
                    .$(", len=").$(dataLen)
                    .$(", endStream=").$(endStream).I$();
            return true;
        }

        @Override
        public void onRequestHeader(int streamId, long nameAddr, int nameLen, long valueAddr, int valueLen, boolean neverIndexed) {
        }

        @Override
        public void onRequestHeaders(int streamId, Http2RequestHeadersView view, boolean endStream) {
            LOG.debug().$("h2 onRequestHeaders [sid=").$(streamId)
                    .$(", endStream=").$(endStream).I$();
        }

        @Override
        public void onStreamClosed(int streamId, int cause) {
            LOG.debug().$("h2 onStreamClosed [sid=").$(streamId)
                    .$(", cause=").$(cause).I$();
        }

        @Override
        public void onStreamWritable(int streamId) {
        }

        @Override
        public void onTrailers(int streamId, boolean endStream) {
        }
    }
}
