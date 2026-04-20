# HTTP/2 Integration

How the HTTP/2 stack (`HTTP2_FRAME_CODEC.md`, `HPACK_CODEC.md`,
`STREAM_STATE_MACHINE.md`) plugs into QuestDB's existing HTTP
machinery — the server, the dispatcher, the processor pipeline,
authentication, and the response sinks.

Completes the Stage 2 surface defined in `FLIGHT_SQL_DESIGN.md`
and hands Stage 3 (gRPC / Flight SQL) a coherent request / response
boundary to build against.

## Table of Contents

1. [Scope](#1-scope)
2. [Non-Goals](#2-non-goals)
3. [References](#3-references)
4. [Layered Architecture](#4-layered-architecture)
5. [Threading Model](#5-threading-model)
6. [IO Dispatcher and Preface Branching](#6-io-dispatcher-and-preface-branching)
7. [HttpRequestContext Interface Extraction](#7-httprequestcontext-interface-extraction)
8. [Per-Stream Request Context](#8-per-stream-request-context)
9. [Request Reading Path](#9-request-reading-path)
10. [Response Writing Path](#10-response-writing-path)
11. [LocalValue and Per-Stream State](#11-localvalue-and-per-stream-state)
12. [Park / Resume](#12-park--resume)
13. [Authentication and Security](#13-authentication-and-security)
14. [Error Mapping](#14-error-mapping)
15. [Milestone Scoping](#15-milestone-scoping)
16. [Open Questions](#16-open-questions)

---

## 1. Scope

This document specifies how the HTTP/2 stack — the frame codec,
HPACK codec, and stream state machine — integrates with the rest
of the QuestDB HTTP server so that existing request processors
(`HttpRequestProcessor` implementations such as `JsonQueryProcessor`,
`ExportQueryProcessor`, `TextImportProcessor`, etc.) run unmodified
over HTTP/2 streams.

Covered:

- The IO layer split: `Http2ConnectionContext` as a pure
  byte-in / byte-out protocol engine, wrapped by
  `Http2ServerContext extends IOContext<Http2ServerContext>` that
  owns the socket, buffers, and dispatcher integration.
- Preface detection at accept time; how the same `IODispatcher`
  queue services both HTTP/1.x and HTTP/2 connections.
- Extracting `HttpRequestContext` as the minimal interface that
  `HttpRequestProcessor` needs, implemented by both the existing
  `HttpConnectionContext` and the new per-stream adapter.
- Per-stream request-context adapter `Http2StreamRequestContext`
  that presents one HTTP/2 stream to a processor as if it were a
  dedicated connection.
- Response writing: `Http2ChunkedResponse` + `Http2ResponseSink`
  that translate the same processor-facing API shape as
  `HttpChunkedResponse` into HEADERS / DATA frames subject to
  per-stream and connection-level flow control.
- `LocalValue` semantics under multiplexing; park / resume at
  stream scope rather than connection scope.
- Error-class translation (`PeerIsSlowToReadException`,
  `PeerDisconnectedException`, `ServerDisconnectException`) into
  the RST_STREAM / GOAWAY vocabulary from
  `STREAM_STATE_MACHINE.md` §12.

Not covered here: gRPC framing, Arrow Flight SQL semantics, or
any Stage 3 dispatch logic. Those live on top of this layer and
are described in `FLIGHT_SQL_DESIGN.md`.

## 2. Non-Goals

- **No changes to processor business logic.** A processor that
  emits `application/json` body bytes via `HttpChunkedResponse`
  over HTTP/1.x should run unchanged over HTTP/2. The integration
  layer adapts the request and response surfaces around it.
- **No H2-specific processor API in M1.** Extended CONNECT
  (RFC 8441) and gRPC processors land as a separate interface in
  Stage 3; Milestone 1 and 2 focus on existing GET / POST /
  PUT processors.
- **No protocol-level priority.** RFC 9113 sec. 5.3.2 deprecates
  it. `STREAM_STATE_MACHINE.md` §7 specifies round-robin
  scheduling; no processor-facing priority hook.
- **No transparent HTTP/1.x to HTTP/2 upgrade.** Clients use
  prior knowledge (`curl --http2-prior-knowledge`, gRPC clients
  that negotiate via ALPN). The 101-Switching-Protocols upgrade
  dance is not supported in M1; may be added in M3 once ALPN is
  wired.

## 3. References

- `HTTP2_FRAME_CODEC.md` — frame reader / writer, frame types.
- `HPACK_CODEC.md` — header compression.
- `STREAM_STATE_MACHINE.md` — per-stream FSM, flow control,
  listener contract, error handling.
- `FLIGHT_SQL_DESIGN.md` — Stage boundaries and the Stage 3
  handoff.
- `core/src/main/java/io/questdb/cutlass/http/HttpConnectionContext.java`
  — the existing HTTP/1.x IOContext; reference surface that the
  per-stream adapter must present.
- `core/src/main/java/io/questdb/cutlass/http/HttpRequestProcessor.java`
  — the processor interface that becomes
  `HttpRequestContext`-keyed.

## 4. Layered Architecture

Three layers, strictly separated:

```
+----------------------------------------------------------+
| Processor layer (unchanged)                              |
| HttpRequestProcessor subclasses                          |
| - JsonQueryProcessor, ExportQueryProcessor, ...          |
+----------------------------------------------------------+
                          |  HttpRequestContext interface
                          v
+----------------------------------------------------------+
| Per-stream adapter layer (new)                           |
| Http2StreamRequestContext                                |
|  - implements HttpRequestContext                         |
|  - owns per-stream request header view, body buffer,     |
|    response sink, LocalValueMap, park/resume slot        |
+----------------------------------------------------------+
                          |  Http2StreamListener
                          v
+----------------------------------------------------------+
| Protocol core (STREAM_STATE_MACHINE.md)                  |
| Http2ConnectionContext                                   |
|  - byte-in / byte-out                                    |
|  - drives Http2StreamListener per frame                  |
|  - NO socket, NO dispatcher reference                    |
+----------------------------------------------------------+
                          |  processReceivedBytes(addr,len)
                          |  writePending(addr,cap)
                          v
+----------------------------------------------------------+
| IO wrapper (new)                                         |
| Http2ServerContext extends IOContext<Http2ServerContext> |
|  - owns socket, native receive/send buffers              |
|  - owns the Http2ConnectionContext + stream adapters     |
|  - handleClientOperation(READ/WRITE/HEARTBEAT)           |
+----------------------------------------------------------+
                          |
                          v
                  IODispatcher, HttpServer
```

**Why the protocol core stays socket-less.** HTTP/2 has a dense
set of edge cases around flow-control accounting, HPACK dynamic-
table coherence after malformed blocks, per-stream state machine
transitions, content-length pre-dispatch suppression, and
deferred-credit settlement. `STREAM_STATE_MACHINE.md` §13 lists
dozens of regression tests that craft exact byte sequences and
assert on internal state at frame granularity. Running those tests
through real sockets adds TCP buffering, worker scheduling, and
flake risk for no benefit. Keeping `Http2ConnectionContext`
socket-free makes the tests deterministic and single-threaded.

**Why the wrapper is real code, not a convention.** If
`Http2ConnectionContext` directly extended `IOContext`, the
socket-free property would rely on "don't call socket APIs from
inside the protocol engine" — a discipline, not a constraint.
Splitting the class physically makes the boundary a compile-time
check: `Http2ConnectionContext` has no socket field, so it cannot
touch one.

**Class inventory** (all new unless noted):

| File                                         | Purpose                                                                                             |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `Http2ServerContext.java`                    | `IOContext<Http2ServerContext>`. Owns socket + receive/send buffers + `Http2ConnectionContext`. Its `handleClientOperation` is the boundary between network and protocol. |
| `Http2ServerConnectionFactory.java`          | Creates `Http2ServerContext` instances, sized per configuration. Registered with `IODispatcher` via the branch in §6. |
| `Http2StreamRequestContext.java`             | Per-stream adapter implementing `HttpRequestContext`. Pooled inside `Http2ServerContext`, sized exactly to the configured inbound `SETTINGS_MAX_CONCURRENT_STREAMS` so every LIVE stream slot in `Http2StreamPool` has a paired adapter (§8 below). |
| `Http2StreamRequestContextListener.java`     | Implementation of `Http2StreamListener` that the server context hands to `Http2ConnectionContext`. Maps stream events to adapter lifecycle. |
| `Http2ResponseSink.java`                     | Buffered response-header + body emitter. Converts the `HttpResponseSink`-shaped API into HEADERS (via `HpackEncoder`) + DATA frames bounded by per-stream / connection windows. |
| `Http2ChunkedResponse.java`                  | Thin wrapper over `Http2ResponseSink` presenting the `HttpChunkedResponse` surface processors already use. |
| `Http2RequestHeaderView.java` (existing)     | Already in the tree — exposes captured pseudo-headers plus staged regular headers as `HttpRequestHeader`-shaped lookups. |
| `HttpRequestContext.java` (refactor)         | Extracted interface that was `HttpConnectionContext`; §7 below. |

## 5. Threading Model

QuestDB's HTTP server hands each connection to one IO worker at a
time, but the worker **can change** across park / resume
boundaries — see `HttpRequestProcessor` javadoc. HTTP/2 inherits
the same model with some multiplex-specific wrinkles that the
integration layer has to preserve.

**Invariants.**

1. **One owner worker per connection at any instant.** The
   `IODispatcher` guarantees that only one worker ever calls
   `Http2ServerContext.handleClientOperation(...)` for a given
   context at the same time.
2. **`Http2ConnectionContext` is single-threaded by contract**
   (`STREAM_STATE_MACHINE.md` §4, §10). Every mutation — frame
   read, frame emit, SETTINGS apply, stream-pool slot change,
   flow-control window update — must run on the current owner
   worker. A call from any other thread is undefined behaviour.
3. **Every `Http2StreamListener` callback fires on the owner
   worker**, inside the same `processReceivedBytes` dispatch that
   decoded the frame. Adapter lifecycle transitions
   (`onRequestHeader`, `onRequestHeaders`, `onData`,
   `onTrailers`, `onStreamClosed`) are therefore also single-
   threaded per-connection.
4. **Every processor callback fires on the owner worker.** Just
   like HTTP/1.x, processor methods (`onHeadersReady`,
   `onRequestComplete`, `resumeSend`, `failRequest`, ...) are
   invoked from inside a dispatcher tick. A processor that
   offloads work to a different thread cannot touch the context
   or the response sink from that thread directly — it has to
   marshal back.

**Worker migration.** When a stream parks on
`PeerIsSlowToReadException`, the dispatcher re-queues the
connection for WRITE; a different worker may pick up the next
event. The `Http2ServerContext` travels with the connection as a
unit — the adapter pool, the parked-streams map, the HPACK
codecs, everything — because it's one `IOContext` object that the
dispatcher hands around. The new worker resolves the right
processor instance via
`HttpRequestProcessorSelector.resolveProcessorById`, exactly like
HTTP/1.x (§12). The adapter must hold **only** the processor's
handler id (an `int`), not a reference to any one worker's
processor object; otherwise it would resume against the wrong
instance after migration.

**Per-stream vs per-connection scope.** A single connection tick
may process events for many streams in arbitrary order (DATA on
stream 3, HEADERS on stream 5, WINDOW_UPDATE on stream 1, etc.
— the frame reader's output). The adapter must therefore treat
every stream as independent state: its `LocalValueMap` is per-
stream (§11), its `SecurityContext` is per-stream (§13), its
deferred-credit counter (`Http2Stream.outstandingInboundCredit`)
is per-stream, and its generation-token identity-guard is per-
stream (§7 step 6 of the state-machine doc).

**Foreign-thread marshalling — the critical case.** A processor
that returns `false` from the adapter's body-consumption callback
typically offloads the body bytes to a different thread (e.g.
`TextImportProcessor` writes into `TableWriter`'s ingest path).
When that thread finishes, it cannot call
`Http2ConnectionContext.onBytesConsumed(streamId, genToken, n)`
directly — that would mutate context state from outside the
owner worker, corrupting the flow-control windows, the coalescing
credit counter, and the outbound frame queue. Instead:

- `Http2ServerContext` exposes a thread-safe
  `MPSC`-style **pending-ops queue**. Each stream may contribute
  multiple entries over its lifetime — `outstandingInboundCredit`
  accumulates across successive `onData` calls that return `false`,
  so a handler may produce one ack per offloaded chunk rather
  than one per stream. The queue is bounded (say, 4 ×
  `MAX_CONCURRENT_STREAMS` slots — open question §16 on the exact
  bound); if a foreign thread finds the queue full it falls back
  to a blocking enqueue until the owner worker drains, which
  naturally backpressures the offload pipeline. The worker-thread
  ack pushes an entry `{streamId, generationToken, n}` and signals
  the dispatcher to wake the connection.
- On the next owner-worker tick, `handleClientOperation` drains
  the pending-ops queue **before** reading bytes; each entry
  turns into a `Http2ConnectionContext.onBytesConsumed(streamId,
  genToken, n)` call on the owner thread.
- The dispatcher wake-up signal is a per-connection mechanism
  (e.g. an `eventfd` on Linux, or the existing
  `IODispatcher.heartbeat` path). Open question §16 tracks the
  exact API — QuestDB's dispatcher needs a "wake this
  connection" primitive if it does not already have one.

The same marshalling rule applies to any async response-write
path: a processor that asynchronously produces response bytes
must push them (or a "please call resumeSend") onto the pending-
ops queue, not write them directly from the producing thread.
In practice, M1 and M2 processors are all synchronous inside the
IO tick (they may block on CairoEngine work, but they do so on
the owner worker), and the only foreign-thread case is deferred
body consumption.

**Race-free stream close.** `onStreamClosed` fires on the owner
worker as part of state-machine transition processing. The
adapter releases its pool slot and clears its `LocalValueMap` on
that same tick. A pending-ops entry whose `generationToken` no
longer matches the (now-bumped) `Http2Stream.generation` is
silently dropped on drain — the same guard as §7 step 6 of the
state-machine doc.

## 6. IO Dispatcher and Preface Branching

One TCP listen socket, one `IODispatcher`, two IOContext types
behind a common supertype.

RFC 9113 sec. 3.4: an HTTP/2 client with prior knowledge opens
the TCP connection and sends the 24-byte preface
`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` as its first bytes, before any
frames. An HTTP/1.x client sends `METHOD path HTTP/1.x\r\n...`.
These are disjoint on the first byte range.

**Branch point.** Define `HttpServerContext` as the common
supertype for both:

```java
public abstract class HttpServerContext<C extends HttpServerContext<C>>
        extends IOContext<C> {
    // shared: peer address, auth state, metrics bindings
}

public class HttpConnectionContext
        extends HttpServerContext<HttpConnectionContext> { ... }

public final class Http2ServerContext
        extends HttpServerContext<Http2ServerContext> { ... }
```

The accept flow:

1. `IODispatcher` accepts, hands the socket to a
   **preface-sniffing IOContext** (`HttpServerStartContext`,
   short-lived).
2. First READ event: `Net.peek(fd, scratch, 24)` — QuestDB's
   socket layer wraps `MSG_PEEK`, so this reads up to 24 bytes
   without consuming them from the socket buffer. Three outcomes:
   - Fewer than 24 bytes available and what arrived is a proper
     prefix of the preface (`PRI`, `PRI `, …): return to the
     dispatcher and wait for the next READ event; try peek
     again.
   - 24 bytes available, full match against the preface: swap
     the context for `Http2ServerContext`. The preface bytes
     stay in the socket buffer and the H2 context's first
     `recv` consumes them naturally — no handoff required.
   - Anything else: swap for `HttpConnectionContext`. The H1
     parser's first `recv` picks up the bytes from the socket
     buffer, also without a handoff.
3. Both successor contexts are `IOContext` subtypes, so
   `IODispatcher<HttpServerContext>` routes `handleClientOperation`
   events uniformly.

The swap requires an `IOContext` handle that the dispatcher can
replace in place. The existing dispatcher supports this via
`context.reshuffle(...)` or similar (confirm when wiring — open
question §16); if not, the alternative is to have a single
`HttpServerContext` that internally holds either an HTTP/1.x
parser or an H2 engine, with the choice made on first-byte
inspection and sticky thereafter. Both shapes work; pick whichever
fits the existing dispatcher API with the least disruption.

**No 101 Upgrade in M1.** Pure HTTP/2 prior-knowledge only, which
is what `curl --http2-prior-knowledge` and every gRPC client use.
M3 may add the `Upgrade: h2c` handshake if a real caller needs
it.

**TLS / ALPN** (M3). The TLS layer selects `h2` or `http/1.1` via
ALPN before any bytes reach this branch; the dispatcher's socket
wrapper exposes the negotiated protocol, and the branch uses that
directly instead of the preface sniff. For M1 / M2 we stay on
cleartext.

## 7. HttpRequestContext Interface Extraction

Currently `HttpRequestProcessor` takes `HttpConnectionContext`:

```java
void onHeadersReady(HttpConnectionContext context);
void onRequestComplete(HttpConnectionContext context);
void resumeSend(HttpConnectionContext context);
// ... etc
```

This ties every processor to the HTTP/1.x context type. For
multiplexed HTTP/2 we need processors to operate on a per-stream
handle. The refactor extracts the surface that processors
actually use into `HttpRequestContext`:

```java
// Retry already extends Closeable; Locality is the LocalValue anchor
public interface HttpRequestContext extends Locality, Retry {
    HttpRequestHeader getRequestHeader();
    HttpResponseHeader getResponseHeader();
    HttpChunkedResponse getChunkedResponse();
    HttpRawSocket getRawResponseSocket();

    LocalValueMap getMap();             // LocalValue storage
    SecurityContext getSecurityContext();
    NetworkSqlExecutionCircuitBreaker getCircuitBreaker();
    HttpCookieHandler getCookieHandler();
    CharSequenceObjHashMap<CharSequence> getParsedCookiesMap();
    Metrics getMetrics();
    RejectProcessor getRejectProcessor();

    // SqlExecutionContext accessors used by query processors
    SqlExecutionContextImpl getOrCreateSqlExecutionContext(
            CairoEngine engine, int workerCount);
    SqlExecutionContextImpl getSqlExecutionContext();
    AssociativeCache<RecordCursorFactory> getSelectCache();

    // Connection-scope bookkeeping that the processors read
    long getFd();                       // for log correlation
    long getTotalBytesSent();
    long getTotalReceived();
    RetryAttemptAttributes getAttemptDetails();
}
```

Both `HttpConnectionContext` and `Http2StreamRequestContext`
implement this interface. The processor interface becomes:

```java
public interface HttpRequestProcessor {
    void onHeadersReady(HttpRequestContext ctx);
    void onRequestComplete(HttpRequestContext ctx);
    void resumeSend(HttpRequestContext ctx);
    // ...
}
```

**Refactor plan.** This is a large-scoped, mechanical rename that
touches every processor. To keep reviewable diffs:

1. Introduce `HttpRequestContext` as an interface with exactly
   the methods above, implemented only by `HttpConnectionContext`.
   No processor signature changes yet. Lands as one PR.
2. Change `HttpRequestProcessor` parameter types from
   `HttpConnectionContext` to `HttpRequestContext`. Every
   processor compiles unchanged because
   `HttpConnectionContext` implements the new interface. Lands
   as a second PR; diff is mostly import changes.
3. Any processor that reached into HTTP/1.x-specific state
   (e.g. `context.getRequestHeader()` casting the
   `HttpRequestHeader` to a concrete type) gets cleaned up in
   the same PR. Expected to be zero cases, but Grep first.

After step 2, `Http2StreamRequestContext` can be added as a
second implementor without touching any processor.

**Methods that do NOT belong on `HttpRequestContext`.** Anything
that exposes HTTP/1.x framing state: the `HttpHeaderParser`
instance, chunked-encoding state, keep-alive bookkeeping, the
request-line byte range. Those live on `HttpConnectionContext`
only. A processor that needs them is HTTP/1.x-only by
definition.

## 8. Per-Stream Request Context

`Http2StreamRequestContext` presents one HTTP/2 stream to a
processor as if it were a dedicated connection. Pooled by
`Http2ServerContext`; the pool is sized to exactly
`SETTINGS_MAX_CONCURRENT_STREAMS` (the same number as the
LIVE-slot budget of the `Http2StreamPool` inside
`Http2ConnectionContext` — `STREAM_STATE_MACHINE.md` §4). The
two pools are 1:1: every LIVE stream has exactly one adapter,
and DISCARDING_BLOCK / TOMBSTONE slots do not consume adapter
capacity, so the adapter pool cannot be exhausted while the
state machine still admits streams.

The adapter pool lives on `Http2ServerContext` and migrates with
the connection across workers (§5). Neither the pool itself nor
any adapter is ever shared across connections or across workers
— all adapter access is single-threaded on the current owner
worker.

**Fields.** Every field is pre-allocated once and reused across
requests — the "zero-GC on the hot path" rule applies here too.

- `int streamId` — the HTTP/2 stream id this adapter is bound to
  for the current request.
- `int streamGeneration` — the `Http2Stream.generation` captured
  at bind time. Used when the processor defers a DATA ack; the
  generation goes into `context.onBytesConsumed(streamId,
  generationToken, n)` verbatim (see
  `STREAM_STATE_MACHINE.md` §7 step 6).
- `Http2RequestHeaderView headerView` — an `HttpRequestHeader`
  implementation that reads from the captured pseudo-header
  slots plus the staged regular-header tuple table
  (`HEADER_STAGING_BYTES` / `HEADER_STAGING_TUPLES`). Already
  in the tree.
- `LocalValueMap localValueMap` — per-stream, not per-connection
  (see §11).
- `SecurityContext securityContext` — set by the auth path (§13)
  during `onRequestHeaders` dispatch.
- `Http2ResponseSink responseSink` — the response buffer and
  frame emitter.
- `Http2ChunkedResponse chunkedResponse` — processor-facing
  response API; wraps `responseSink`.
- Deferred-body-ack bookkeeping: the existing HTTP/1.x body
  dispatch tests the processor's type at request-dispatch time
  (`instanceof HttpPostPutProcessor`) and routes body chunks
  through the processor-specific body API (e.g.
  `HttpMultipartContentProcessor.onChunk`). H2 reuses that
  dispatch logic — whatever the HTTP/1 path does to hand body
  bytes to the processor, the adapter does the same thing with
  the bytes `onData` delivered. When the processor signals "all
  N bytes consumed" (either synchronously on return or
  asynchronously via whatever callback pattern the HTTP/1 path
  uses), the adapter translates that into
  `Http2ConnectionContext.onBytesConsumed(streamId,
  streamGeneration, N)` — on the owner worker directly for
  synchronous acks, via the pending-ops queue for foreign-thread
  acks (§5).
- Park / resume slot — see §12.

**Lifecycle.**

1. `onRequestHeaders(streamId, view, endStream)` on the
   `Http2StreamListener` implementation:
   - Acquire a pooled `Http2StreamRequestContext`; bind to
     `(streamId, streamGeneration)`.
   - Populate `headerView` from the captured pseudo-header slots.
   - Run the authentication pipeline (§13); populate
     `securityContext`.
   - Select the processor via
     `HttpRequestProcessorSelector.select(headerView)` (the
     selector already operates on `HttpRequestHeader`, so no
     changes needed).
   - Call `processor.onHeadersReady(ctx)`.
   - If `endStream == true`, follow up with
     `processor.onRequestComplete(ctx)` immediately — no body
     will arrive.
2. `onRequestHeader(...)` fires before `onRequestHeaders` per
   `STREAM_STATE_MACHINE.md` §11. The adapter does not need to
   observe these individually; the `headerView` reads the staged
   tuple table at lookup time.
3. `onData(streamId, addr, dataLen, endStream, genToken)`:
   - Route body bytes to the processor's `onChunk`-equivalent
     entry point. For a POST / PUT processor, this is where
     `HttpMultipartContentProcessor` / `HttpPostPutProcessor`
     consume bytes.
   - Return `true` if synchronously consumed, `false` if the
     processor needs to defer. On deferral, the adapter stashes
     `genToken` and produces it on the later
     `onBytesConsumed` call.
   - If `endStream == true`, call
     `processor.onRequestComplete(ctx)`.
4. `onTrailers(streamId, endStream)` (M2): trailers not currently
   consumed by any HTTP/1.x-legacy processor, so the M2 path
   delivers them through a new optional
   `processor.onTrailers(ctx, trailerView)` hook that defaults to
   no-op.
5. `onStreamClosed(streamId, cause)`:
   - Call `processor.onConnectionClosed(ctx)` (rename pending
     — see §16 open question; the semantics match per-stream
     close).
   - Release the adapter back to the pool.

**Binding rules.**

- At most one `Http2StreamRequestContext` per stream id at a
  time. On `onStreamClosed` the id unbinds and the adapter is
  re-usable.
- The adapter's identity does NOT track a specific
  `Http2Stream` pool slot. It holds `(streamId, generation)`
  values only, matching the handler-side rule in
  `STREAM_STATE_MACHINE.md` §4 (handler holds pair, not object
  reference).

## 9. Request Reading Path

Concrete flow for an inbound POST with a body:

```
peer bytes
  -> Http2ServerContext.handleClientOperation(READ)
    -> read socket into receiveBuffer
    -> Http2ConnectionContext.processReceivedBytes(addr, len)
      -> Http2FrameReader splits frames
      -> HEADERS + CONTINUATION sequence assembled in block scratch
      -> HpackDecoder.decodeBlock(...)
      -> HpackListener copies pseudo + regular into per-stream
         staging (STREAM_STATE_MACHINE.md §11)
      -> at END_HEADERS, STREAM_STATE_MACHINE.md §11 validators
         run; on success:
         Http2StreamListener.onRequestHeader(...) per field
         Http2StreamListener.onRequestHeaders(streamId, view,
                                              endStream)
      -> context adapter selects processor, calls
         processor.onHeadersReady(ctx)
      -> subsequent DATA frames:
         Http2StreamListener.onData(streamId, addr, dataLen,
                                     endStream, genToken)
         -> adapter delivers to processor's body-consumer
         -> on endStream=true:
            processor.onRequestComplete(ctx)
```

The response path is interleaved (not physically concurrent)
with any remaining inbound DATA — HTTP/2 is logically
full-duplex but the protocol core runs single-threaded on the
owner worker (§5), so inbound frame processing and outbound
frame emission interleave within one tick. The processor can
begin emitting response headers + DATA via
`getChunkedResponse()` from inside `onHeadersReady` or
`onRequestComplete` on the same worker.

**Body consumption contract.** Two paths:

- **Synchronous.** The processor finishes its work inside
  `onData` and returns `true`. The adapter returns `true` from
  `Http2StreamListener.onData`, and `Http2ConnectionContext`
  credits the windows immediately — all on the owner worker, no
  marshalling.
- **Deferred (foreign-thread).** The processor hands the bytes
  off to a different thread (e.g. `TextImportProcessor` writing
  to `TableWriter`) and returns `false`. The adapter records
  `(streamId, streamGeneration, dataLen)` on itself. When the
  foreign thread finishes, it **MUST NOT** call
  `Http2ConnectionContext.onBytesConsumed(...)` directly — that
  violates the single-owner-thread invariant (§5). Instead it
  pushes the ack entry onto the `Http2ServerContext`'s pending-
  ops queue and signals the dispatcher. The next tick on the
  owner worker drains the queue and invokes
  `Http2ConnectionContext.onBytesConsumed(streamId, genToken,
  n)` on the owner thread. Generation mismatch on drain is a
  silent no-op per `STREAM_STATE_MACHINE.md` §7 step 6 —
  handles the case where the stream was reset or recycled
  between the foreign thread pushing and the owner worker
  draining.

**Content-length enforcement** is inside
`Http2ConnectionContext` per §11 of the state-machine doc, so
the adapter and processors never see a body over or under the
declared length — the state machine rejects before dispatching.

## 10. Response Writing Path

The existing `HttpChunkedResponse` / `HttpResponseSink` pair
streams response bytes through HTTP/1.1 chunked encoding. The
H2 replacement pair exposes the same processor-facing API
surface.

**Processor-facing contract.** Unchanged. A processor writes to
`ctx.getChunkedResponse()`; the sink buffers, flushes, and
throws `PeerIsSlowToReadException` when its output buffer is
full. The park / resume machinery re-schedules the processor
for a later WRITE event.

**Under the hood for H2.**

`Http2ResponseSink` owns a per-stream native output buffer sized
to peer `SETTINGS_MAX_FRAME_SIZE` (or a small multiple of it).
Its emit path:

1. **Response headers.** On the first `flush()` / `sendHeaders()`
   call, encode `:status` and user-set headers via the
   connection's shared `HpackEncoder` into the sink's own
   buffer, then split into a HEADERS frame (+ CONTINUATION if
   the encoded block exceeds peer `MAX_FRAME_SIZE`) and enqueue
   on the outbound writer. Concurrency constraint: `HpackEncoder`
   is one instance per connection and its dynamic table must
   advance in a well-defined order, so two streams cannot
   interleave encoding. The listener layer serialises encode
   calls per connection — a stream that starts encoding headers
   holds the encoder until it finishes emitting the HEADERS (+
   any CONTINUATION) bytes into the sink's buffer. This is
   trivial in practice because everything runs on the single
   owner worker (§5), so there is no true concurrency; the
   "serialise" wording is just to rule out reentrancy from a
   nested `flush`.
2. **Response body.** Body writes accumulate into the sink
   buffer. On `flush()` or buffer full, the bytes are handed
   to the outbound scheduler as a
   `(streamId, payloadAddr, payloadLen, endStream)` tuple
   (see `STREAM_STATE_MACHINE.md` §7 "Outbound accounting on
   send"). The scheduler is connection-scoped and round-robins
   across ready streams.
3. **Flow-control backpressure.** There are two independent
   queues in play: the sink's own byte buffer (inside
   `Http2ResponseSink`) and the state machine's per-stream
   outbound tuple queue (inside `Http2ConnectionContext`,
   `STREAM_STATE_MACHINE.md` §7 "Outbound accounting on send").
   A write fills the sink buffer; on flush, the sink hands a
   tuple to the state machine, which queues it until the
   scheduler emits a DATA frame under available window. When
   the outbound window is zero the tuple simply sits in the
   state-machine queue — the sink buffer is free to accept more
   writes. `PeerIsSlowToReadException` fires only when
   **both** buffers are at their configured bound:
   - The sink's buffer has no room for another write, AND
   - The state-machine's per-stream outbound tuple queue has
     already absorbed a configurable maximum (a new per-stream
     cap, e.g. 256 KiB worth of queued tuple payload, to bound
     memory per stream under zero-window).

   At that point the adapter calls the existing `parkRequest`
   path (see §12). When `WINDOW_UPDATE` arrives and the
   scheduler drains enough tuples to bring the queued payload
   back below the cap, `Http2ConnectionContext` raises a new
   callback `Http2StreamListener.onStreamWritable(streamId)`
   (not in the current listener interface — see open question
   §16) which schedules resume for this stream.
4. **End of response.** On the final `flush()` call carrying the
   last bytes, the tuple's `endStream=true` flag fires;
   `Http2ConnectionContext` emits DATA with `END_STREAM` and
   transitions the stream per §5 of the state-machine doc.
   `onStreamClosed` fires on the listener after the frame is
   flushed to the socket.

**Trailers** (M2): a handler that wants to emit trailers (gRPC
pattern — `grpc-status` + `grpc-message` after the body)
invokes `ctx.getChunkedResponse().sendTrailers(...)`. The sink
builds a second HEADERS frame via the encoder and emits it with
`END_STREAM` set.

**Raw socket access.** `HttpRequestContext.getRawResponseSocket()`
does not make sense for H2 — there is no single byte stream
behind a stream. For H2, `getRawResponseSocket()` either
returns `null` (and processors that use it are H2-incompatible
and must guard on the protocol type) or returns a thin adapter
that converts raw byte writes into DATA frames with the same
flow-control contract. Pick the latter if any production
processor depends on it; M1 can return `null` and treat raw-
socket processors as HTTP/1.x-only.

## 11. LocalValue and Per-Stream State

`LocalValue<T>` is a `static final` index that lets a processor
attach typed state to a context object. Today it keys on
`HttpConnectionContext.getMap() -> LocalValueMap`.

**H2 rule: LocalValue keys on the per-stream adapter.**
`Http2StreamRequestContext.getMap()` returns a per-stream
`LocalValueMap`. Two concurrent streams on the same connection
have independent maps. This is the only behaviour consistent
with multiplexing — per-connection state would leak between
concurrent requests.

**Pool reuse.** When a stream closes and the adapter returns to
the `Http2ServerContext`'s adapter pool, its `LocalValueMap` is
cleared through the same teardown path `HttpConnectionContext`
uses for HTTP/1.x request end (`LocalValue` entries get their
close hook invoked for `Closeable` values, then the map resets
to empty). A subsequent request reuses the adapter and starts
with an empty map, matching the post-`onConnectionClosed`
lifecycle on the HTTP/1.x side.

**Connection-scope state.** If a processor truly needs state that
spans all streams on one TCP connection (no current processor
does, but it's conceivable — e.g. a connection-wide auth cache),
it attaches via a `LocalValue` on `Http2ServerContext` directly.
This requires a second `LocalValueMap` on the server context;
M1 omits it since no processor needs it. The §16 open question
tracks when to add it.

## 12. Park / Resume

HTTP/1.x park / resume is per-connection: a processor fills the
send buffer, the context parks the whole connection for WRITE
readiness, a different worker resumes the processor via
`HttpRequestProcessorSelector.resolveProcessorById` on the same
connection context.

**H2 park / resume is per-stream.** The connection can keep
servicing other streams even while one stream is parked on its
own outbound window.

The adapter keeps a **parked-streams set** on `Http2ServerContext`:
`IntObjHashMap<Http2StreamRequestContext>` keyed by stream id,
populated when a stream throws `PeerIsSlowToReadException` out
of its sink. Resume triggers:

- Inbound `WINDOW_UPDATE` on the stream id → the state machine
  raises `onStreamWritable(streamId)` → the wrapper wakes that
  one stream's adapter and calls
  `processor.resumeSend(adapter)`.
- Inbound `WINDOW_UPDATE(0)` on the connection + any parked
  stream with `outboundStreamWindow > 0` → wake the stream(s)
  and call resume on each.

**Processor id correlation across worker migration.** Each
worker has its own `HttpRequestProcessor` instance
(`HttpRequestProcessor` javadoc "Threading model": "processors
are not thread-safe; each worker thread has its own private
processor instance"). A stream that parks on worker A may resume
on worker B, which has a *different* processor object. The
adapter must therefore hold **only the processor's handler id**
(an `int`), never a reference to the processor instance itself —
otherwise resume on worker B would call into worker A's
processor, violating the "one worker at a time" invariant on
that instance.

Call-site shape:

- At `select` time in `onRequestHeaders`:
  `int handlerId = selector.select(headerView).getHandlerId();`
  stored on the adapter.
- At resume time, whether this tick is on worker A or B:
  `HttpRequestProcessor p = selector.resolveProcessorById(
  handlerId);` then `p.resumeSend(adapter)`.

This matches HTTP/1.x's park / resume discipline exactly; H2
differs only in that the resume target is a *stream-scoped*
adapter, not the whole connection context. Per-request
scratchpad state that the processor built up on worker A is lost
on migration — that's the existing HTTP/1.x rule, and the fix is
the existing one: durable per-request state lives in
`LocalValue` on the adapter (§11), not in processor instance
fields.

**Park of inbound.** A processor that cannot consume body bytes
as fast as they arrive defers via the `onData` return-false
path (§8). That is not a park in the HTTP/1.x sense — the
stream is NOT re-queued for READ, the connection just keeps
buffering per the inbound flow-control window. When the
handler eventually acks via `onBytesConsumed`, the window
credits forward and peer resumes sending. No processor
lifecycle event fires on inbound resume; the data just flows
again.

## 13. Authentication and Security

HTTP/1.x currently runs authentication in
`HttpConnectionContext` during header parsing, populating
`SecurityContext` before `processor.onHeadersReady` fires.

**H2 auth: per stream.** Every HTTP/2 stream is an independent
request with its own headers (`:authority`, cookies,
`authorization`, etc.). Even though multiple streams share a
TCP connection, they can carry different credentials — a client
sending two concurrent requests may legitimately use different
auth tokens per stream (edge case: rare in practice but
standard-compliant). Auth evaluation therefore runs **per
stream** during the `onRequestHeaders` dispatch, before the
processor is called.

The `HttpAuthenticator` surface works off `HttpRequestHeader`
already, so the adapter calls it identically to HTTP/1.x.
`SecurityContext` is stored on the per-stream adapter.

**Cookie parsing** runs per stream on the per-stream
`CharSequenceObjHashMap<CharSequence>` already referenced by
`HttpRequestContext`.

**Connection-scope auth caching** (optional, deferred): a
connection-scope auth-token-to-`SecurityContext` cache on
`Http2ServerContext` would skip re-evaluating the same bearer
token on every stream. Worth measuring before implementing —
the per-stream path is already fast.

## 14. Error Mapping

The processor pipeline throws a small set of checked exceptions:

| Exception                    | HTTP/1.x meaning                                        | HTTP/2 meaning                                                                                      |
|------------------------------|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `PeerDisconnectedException`  | Socket closed / reset                                   | Maps to connection-level socket close. Entire `Http2ServerContext` tears down (all streams lost). |
| `PeerIsSlowToReadException`  | Send buffer full, park for WRITE                        | Stream-scope park (see §12). The connection stays up; other streams proceed.                        |
| `PeerIsSlowToWriteException` | Receive buffer empty, park for READ                     | Effectively never fires on H2. HTTP/1.x throws this when the processor is parsing headers / body and needs more bytes that haven't arrived. Under H2, headers are fully assembled by `Http2ConnectionContext` before the processor is invoked, and body bytes are surfaced one DATA frame at a time through `onData`; a processor that needs more body simply waits for the next `onData` invocation. The adapter should treat `PeerIsSlowToWriteException` as a programming error (log and `RST_STREAM(INTERNAL_ERROR)`) in M1; the state machine's flow-control buffering subsumes its semantics. |
| `ServerDisconnectException`  | Server-initiated shutdown                               | For a stream-scope error: `RST_STREAM(INTERNAL_ERROR)` on the stream, connection survives. For a server-wide shutdown: `GOAWAY(NO_ERROR)` followed by drain per `STREAM_STATE_MACHINE.md` §12. |
| `HttpException` (processor)  | Response reset mid-body, close connection              | `RST_STREAM(INTERNAL_ERROR)` on the stream. Connection survives so other streams are unaffected.   |

The adapter catches these at the `processor.*` call sites and
translates. An uncaught `RuntimeException` from a processor is
also stream-scope: `RST_STREAM(INTERNAL_ERROR)`, log, and
`onStreamClosed` fires. Connection-scope errors are only the
RFC 9113 sec. 5.4.1 set (frame-size violation, compression
failure, flow-control overrun, etc.) handled inside
`Http2ConnectionContext` per §12 of the state-machine doc;
processors do not surface those.

## 15. Milestone Scoping

### 15.1 Milestone 1 — single processor end-to-end

- `HttpRequestContext` interface extraction (§7 steps 1 + 2).
- `Http2ServerContext` + preface-sniffing accept branch.
- `Http2StreamRequestContext` adapter + listener
  implementation, wired to exactly one processor:
  `StaticContentProcessor` or a hello-world handler (easier to
  exercise end-to-end than the real query pipeline).
- `Http2ResponseSink` / `Http2ChunkedResponse` covering status
  line + headers + a single-frame body.
- Per-stream `LocalValueMap`, per-stream auth evaluation.
- Park / resume at stream scope (§12) wired for
  `PeerIsSlowToReadException`.
- Tier 4 smoke: `curl --http2-prior-knowledge http://host/health`
  round-trips a 200 OK with a small body.
- No trailers, no chunked response beyond one DATA frame's
  worth, no GOAWAY-driven graceful shutdown, no raw-socket
  processor support.

### 15.2 Milestone 2 — production shape

- All read-path processors (`JsonQueryProcessor`,
  `ExportQueryProcessor`) run over H2.
- Streaming response bodies with real flow-control backpressure
  (multi-frame bodies, `PeerIsSlowToReadException` park /
  resume under load).
- POST / PUT processors (`TextImportProcessor`,
  `HttpPostPutProcessor`) — deferred body-ack path, per-stream
  circuit-breaker integration.
- Trailers on both sides (optional per-processor hook).
- GOAWAY graceful-shutdown flow — server shutdown triggers
  `Http2ConnectionContext` to emit GOAWAY; adapter drains
  active streams per §12 of the state-machine doc.
- h2spec full conformance via the harness already sketched in
  `HTTP2_FRAME_CODEC.md` §12.5.

### 15.3 Milestone 3 — TLS and Flight SQL

- ALPN negotiation in the TLS layer; drop the preface sniff.
- Extended CONNECT (RFC 8441) carve-out in the state machine;
  gRPC-specific processor surface.
- Arrow Flight SQL `DoGet` / `DoPut` handlers emitting
  length-prefixed protobuf over DATA frames.

### 15.4 Build order within Milestone 1

Each step lands with its unit tests and is independently
reviewable.

1. **`HttpRequestContext` interface.** Extract from
   `HttpConnectionContext`. No processor signature changes.
   Tests unchanged.
2. **`HttpRequestProcessor` parameter retype.** Change every
   processor method signature from `HttpConnectionContext` to
   `HttpRequestContext`. Mechanical diff, every compile-error
   is an import fix.
3. **`Http2ServerContext` skeleton.** Socket ownership,
   receive/send buffers, `handleClientOperation` routing to
   `Http2ConnectionContext` already under test. No listener
   yet — listener is a no-op that just logs callbacks.
4. **Preface-branch in the dispatcher.** Accept flow picks
   between `Http2ServerContext` and `HttpConnectionContext` on
   first bytes.
5. **`Http2StreamRequestContext` pool + `Http2RequestHeaderView`
   population.** Adapter construction / teardown tested in
   isolation via a mock `Http2StreamListener` feeding the
   adapter synthetic events.
6. **`Http2ResponseSink` sending a single HEADERS + DATA
   frame.** No flow-control backpressure path yet — the M1
   response bodies fit in one frame.
7. **Wire to `StaticContentProcessor`** (or a hello-world
   stub); `curl --http2-prior-knowledge` smoke.
8. **Park / resume** wired for the response sink's
   `PeerIsSlowToReadException` path, then re-test with a
   response large enough to drain the initial window.

## 16. Open Questions

1. **Dispatcher context swap.** §6 step 2 swaps the short-lived
   `HttpServerStartContext` for either `HttpConnectionContext`
   or `Http2ServerContext` after `Net.peek` decides the
   protocol. Does `IODispatcher` currently support replacing the
   `IOContext` bound to a socket in place, or does the API force
   one type for the whole connection lifetime? If the latter,
   fold both parsers under a single `HttpServerContext` whose
   `handleClientOperation` internally dispatches to an H1 or H2
   engine selected at first peek. Verify when wiring.
2. **Connection-scope `LocalValue`.** No current processor needs
   state that spans all streams on a TCP connection. Add the
   facility (a second `LocalValueMap` on `Http2ServerContext`)
   only when a real caller asks for it.
3. **Raw-socket processor support.** Does any M1 / M2 processor
   depend on `HttpRawSocket` access? If yes, build the
   DATA-frame-wrapping adapter (§10); if no, return `null` from
   `Http2StreamRequestContext.getRawResponseSocket()` and
   document raw-socket processors as HTTP/1.x-only until M3.
   Grep the processor tree to close this.
4. **Trailers for existing processors.** HTTP/1.x processors
   never see trailers. Is the M2 trailer hook worth the
   interface churn, or is the gRPC-only pathway the right home
   for it? Defer pending the Stage 3 design.
5. **Per-connection auth cache.** Measure whether re-running
   the authenticator per stream is a meaningful overhead under
   the expected H2 workload (browser-driven query bursts, gRPC
   long-lived channels). If yes, add the cache on
   `Http2ServerContext`; if no, skip.
6. **`onConnectionClosed` rename.** The processor hook
   `onConnectionClosed(ctx)` fires on stream close under H2,
   not connection close. Rename to `onRequestClosed(ctx)` in
   the refactor PR, or keep the historical name and document
   the semantics. Minor but touches every processor.
7. **Dispatcher wake-up for pending-ops queue.** §5 specifies
   that foreign threads (e.g. a `TextImportProcessor` worker
   finishing deferred body consumption) push entries onto a
   per-connection MPSC pending-ops queue and signal the
   `IODispatcher` to wake the connection. The exact wake-up
   primitive depends on what QuestDB's dispatcher exposes —
   `eventfd`-style wake, re-queue via `heartbeat`, or a
   dedicated signal channel. Confirm when wiring §5; if no
   primitive exists, adding one is a prerequisite for M2
   (M1 can get away without it since the hello-world fixture
   is fully synchronous).
8. **`Http2StreamListener.onStreamWritable(streamId)`.** §10
   introduces this new callback so the adapter layer can resume
   a stream that parked on its outbound window. It is **not**
   in the current `Http2StreamListener` Java interface; adding
   it is part of the M1 work. The trigger inside
   `Http2ConnectionContext` is "a parked stream's outbound
   queue dropped below its per-stream cap after a `WINDOW_UPDATE`
   was applied" — specify the exact firing condition when
   implementing, and decide whether the callback is raised
   synchronously during `processReceivedBytes` (simplest) or
   deferred to the end of the tick (batches multiple wakes).
9. **Per-stream outbound tuple-queue cap.** §10 references a
   "configurable maximum, e.g. 256 KiB worth of queued tuple
   payload" as the threshold above which
   `PeerIsSlowToReadException` fires. Pick the number after
   measuring realistic response-size distributions against
   `INITIAL_WINDOW_SIZE`. Too low and long-poll / streaming
   responses bounce into park/resume under moderate load; too
   high and a single stalled peer can tie up unbounded memory.
10. **Pending-ops queue bound and overflow policy.** §5 proposes
    `4 × MAX_CONCURRENT_STREAMS` entries with a blocking
    enqueue as the overflow fallback. Blocking inside the
    foreign thread feeds backpressure into the offload pipeline
    (e.g. the `TextImportProcessor` writer slows its reads),
    which is probably right; alternatively, a bounded queue
    with a non-blocking "drop and let the state machine settle
    on stream close" policy would avoid any risk of deadlocking
    the offload pipeline at the cost of re-crediting more
    bytes via the settlement path. Measure under a deferred-
    body-heavy workload to pick.
