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
4. [Architecture](#4-architecture)
5. [Threading Model](#5-threading-model)
6. [Preface Detection and Mode Selection](#6-preface-detection-and-mode-selection)
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

**Pivot note (2026-04-20):** earlier revisions of this doc aimed
to run the existing `HttpRequestProcessor` surface
(`JsonQueryProcessor`, `ExportQueryProcessor`,
`TextImportProcessor`, `HealthCheckProcessor`, ...) unmodified
over H2. That goal was retired once it became clear that the
only near-term consumer of H2 is Arrow Flight SQL, which does
not reuse any of the H1 processor plumbing — gRPC has its own
framing, its own auth model, its own routing, and its own
streaming semantics. The H1-mimicry layer
(`Http2StreamRequestContext`, `Http2ResponseSink`,
`Http2ChunkedResponse`, `Http2SimpleResponse`,
`HttpRequestContext` interface extraction, `SimpleResponse`
interface extraction, `LocalValueMap.release`,
`HttpRequestProcessorSelector` wiring on H2) is dead weight
under the new framing and is removed in Wave 4. §7 and §8 of
this doc describe that layer for historical context only; §15
reflects the current milestone shape.

The rest of the H2 engine work — frame codec, HPACK codec,
stream state machine, outbound arena, scheduler, park / resume,
preface detection — remains load-bearing. It's exactly the
subset gRPC needs.

This document now specifies how the HTTP/2 stack integrates
with the rest of the QuestDB HTTP server as the transport
substrate for gRPC / Arrow Flight SQL handlers. The gRPC
framing, Flight SQL RPC surface, and handler dispatch are
described in `FLIGHT_SQL_DESIGN.md`.

Covered:

- Extending `HttpConnectionContext` with an H2 mode selected at
  the first READ event. No new `IOContext` subclass, no dispatcher
  changes, no accept-time context swap.
- The preface sniff on the existing HTTP listen socket, gated by
  a config toggle; lazy allocation of the H2 engine on a positive
  match. The stream-adapter pool, response sink, and chunked-
  response adapter are gone — the Flight SQL handler surface
  binds directly to `Http2StreamListener` / `Http2ConnectionContext`.
- Pseudo-header capture for `:method`, `:scheme`, `:path`,
  `:authority`, and `content-type` into per-stream staging so a
  router can dispatch without touching the H1 request-header
  type. Full §11 validator work deferred — only the rules the
  HPACK decoder + stream state machine already enforce hold in
  this layer.
- Response writing: direct use of
  `Http2ConnectionContext.emitResponseHeaders` /
  `enqueueData` / trailer HEADERS emission by the Flight SQL
  handler. No adapter layer.
- Park / resume at stream scope driven by
  `Http2StreamListener.onStreamWritable` — required for DoGet
  result-set streaming.
- Error-class translation (`PeerIsSlowToReadException`,
  `PeerDisconnectedException`, `ServerDisconnectException`) into
  the RST_STREAM / GOAWAY vocabulary from
  `STREAM_STATE_MACHINE.md` §12.
- TLS + ALPN wiring so Flight SQL clients negotiate `h2` at the
  TLS layer and skip the preface sniff (Milestone 2).

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
- **No new `IOContext` subclass for HTTP/2.** `HttpConnectionContext`
  remains the only HTTP-facing `IOContext`, extended with a mode
  bit. Rationale in §4.

## 3. References

- `HTTP2_FRAME_CODEC.md` — frame reader / writer, frame types.
- `HPACK_CODEC.md` — header compression.
- `STREAM_STATE_MACHINE.md` — per-stream FSM, flow control,
  listener contract, error handling.
- `FLIGHT_SQL_DESIGN.md` — Stage boundaries and the Stage 3
  handoff.
- `core/src/main/java/io/questdb/cutlass/http/HttpConnectionContext.java`
  — the HTTP/1.x + WebSocket `IOContext`; gains H2 as a third mode.
- `core/src/main/java/io/questdb/cutlass/http/HttpRequestProcessor.java`
  — the processor interface that becomes
  `HttpRequestContext`-keyed.
- `core/src/main/java/io/questdb/cutlass/http2/Http2ConnectionContext.java`
  — the socket-free H2 protocol engine; held by composition on
  `HttpConnectionContext`.

## 4. Architecture

**One `IOContext` per fd; protocol mode selected inline.**
`HttpConnectionContext` stays the single HTTP-facing `IOContext`
subclass. It gains a `protocolMode` field with three values
(`SNIFFING`, `H1`, `H2`) and dispatches inside its
`handleClientOperation` to either the existing H1 code path, the
existing WebSocket code path, or a new H2 code path that drives
the socket-free `Http2ConnectionContext` engine.

No new `IOContext` subclass, no changes to `IODispatcher` /
`IOContextFactory` / `HttpContextFactory`, no dispatcher context
swap. The H2 engine and stream-adapter pool live as composed
fields on `HttpConnectionContext` and are lazy-allocated on a
successful preface match.

### 4.1 Why this shape

Two in-tree precedents establish the pattern:

1. **pgwire TLS upgrade** — `PGConnectionContext.handleClientOperation`
   (`PGConnectionContext.java:382-398`) runs `handleTlsRequest()`
   inline, flips `tlsSessionStarting`, calls
   `socket.startTlsSession(null)` on the same fd, and keeps going.
   One class, no IOContext swap, mode bit tracks the phase.
2. **HTTP WebSocket upgrade** — `HttpConnectionContext.switchProtocol`
   (`HttpConnectionContext.java:463-466`) flips
   `isProtocolSwitched` and stores the resume handler id;
   `handleClientRecv` branches on the bit at the top
   (`HttpConnectionContext.java:962-966`). One class, no new
   `IOContext`.

Every cutlass `IOContext` subclass is a fat class that owns its
fd's full protocol state (HTTP 1221 lines, pgwire 1899 lines, ILP
444 lines). None of them splits a dispatcher-facing shell from a
protocol engine via composition. Option A follows that pattern;
it is both the lowest-friction path and the idiomatic one.

The alternative considered — a shell class owning two engines via
composition — was rejected because (a) it has no in-tree
precedent, (b) the dispatcher's self-typed
`IODispatcher<C extends IOContext<C>>` generic forbids hosting
two distinct `IOContext` subclasses under a common supertype
without a refactor, and (c) the `FLIGHT_SQL_DESIGN.md` §2
requirement "one port, one I/O stack" forces shared-port anyway,
which is exactly what in-class mode switching is for.

### 4.2 Layering

```
+----------------------------------------------------------+
| Processor layer (unchanged)                              |
| HttpRequestProcessor subclasses                          |
| - JsonQueryProcessor, ExportQueryProcessor, ...          |
+----------------------------------------------------------+
                          |  HttpRequestContext interface
                          v
+----------------------------------------------------------+
| Per-stream adapter layer (new; H2 only)                  |
| Http2StreamRequestContext                                |
|  - implements HttpRequestContext                         |
|  - per-stream header view, LocalValueMap,                |
|    SecurityContext, response sink, handlerId,            |
|    park/resume slot                                      |
+----------------------------------------------------------+
                          |  Http2StreamListener (in-class impl)
                          v
+----------------------------------------------------------+
| Protocol core (STREAM_STATE_MACHINE.md)                  |
| Http2ConnectionContext (io.questdb.cutlass.http2)        |
|  - socket-free, byte-in / byte-out                       |
|  - drives Http2StreamListener per frame                  |
+----------------------------------------------------------+
                          |  processReceivedBytes / writePending
                          |  / onBytesConsumed
                          v
+----------------------------------------------------------+
| IOContext (existing, extended)                           |
| HttpConnectionContext                                    |
|  - owns socket, recv/send buffers                        |
|  - protocolMode: SNIFFING | H1 | H2 (+ isProtocolSwitched|
|    for WebSocket, preserved)                             |
|  - H1: existing parsers, response sink, dispatch         |
|  - H2: Http2ConnectionContext, adapter pool,             |
|    parked-streams set, pending-ops queue, H2 send buffer |
+----------------------------------------------------------+
                          |
                          v
                  IODispatcher (unchanged), HttpServer (unchanged)
```

**Why `Http2ConnectionContext` stays socket-free.** HTTP/2 has a
dense set of edge cases around flow-control accounting, HPACK
dynamic-table coherence after malformed blocks, per-stream state
machine transitions, content-length pre-dispatch suppression, and
deferred-credit settlement. `STREAM_STATE_MACHINE.md` §13 lists
dozens of regression tests that craft exact byte sequences and
assert on internal state at frame granularity. Running those
tests through real sockets adds TCP buffering, worker scheduling,
and flake risk for no benefit. Keeping the engine socket-free
makes the tests deterministic and single-threaded, and the
`HttpConnectionContext` shell is thin enough that the boundary
between "holds the fd" and "drives the protocol" stays clean by
structure, not by discipline alone.

### 4.3 Class inventory

| File                             | Status          | Purpose                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `HttpConnectionContext.java`     | extended        | Adds `protocolMode` state, preface sniff, lazy H2 engine + adapter pool + H2 send buffer, H2 listener implementation, parked-streams set, pending-ops queue.                                                                                                                                                                                          |
| `Http2ConnectionContext.java`    | extended        | Gains the response-emission / outbound scheduling surface listed in §4.4. The M1 engine covers inbound framing + HPACK + stream state + WINDOW_UPDATE accounting; response HEADERS encoding, outbound DATA tuple queuing, the per-connection round-robin scheduler, and the `onStreamWritable` listener callback are net-new work for this milestone. |
| `Http2StreamRequestContext.java` | new             | Per-stream adapter implementing `HttpRequestContext`. Pooled on `HttpConnectionContext`, sized to `SETTINGS_MAX_CONCURRENT_STREAMS` (§8).                                                                                                                                                                                                             |
| `Http2RequestHeader.java`        | new             | Implements `HttpRequestHeader` on top of the pseudo-header slots from `Http2RequestHeadersView` plus the per-stream staged regular-header tuple table. Parses `:path` into URL + query, maps `:method` to `isGetRequest`/`isPostRequest`/`isPutRequest`, resolves `content-length` / `content-type` / `host` / arbitrary header lookup. See §8.       |
| `Http2ResponseSink.java`         | new             | Buffered response-header + body emitter. Converts the `HttpResponseSink`-shaped API into HEADERS (via `HpackEncoder`) + DATA frames bounded by per-stream / connection windows.                                                                                                                                                                       |
| `Http2ChunkedResponse.java`      | new             | Thin wrapper over `Http2ResponseSink` presenting the `HttpChunkedResponse` surface processors already use.                                                                                                                                                                                                                                            |
| `Http2RequestHeadersView.java`   | existing        | Exposes captured pseudo-headers (address / length). Remains the stream-listener-facing view; `Http2RequestHeader` above wraps it for processor consumption.                                                                                                                                                                                           |
| `Http2StreamListener.java`       | extended        | Gains `onStreamWritable(int streamId)` so the adapter can resume streams whose outbound window cleared (§10).                                                                                                                                                                                                                                         |
| `HttpRequestContext.java`        | new (interface) | Extracted from `HttpConnectionContext`; §7 below.                                                                                                                                                                                                                                                                                                     |

No `Http2ServerContext`, no `Http2ServerConnectionFactory`, no
`HttpServerContext` common supertype. The `IODispatcher` and
`HttpContextFactory` wiring at `HttpServer.java:104, 409` is
untouched.

### 4.4 New `Http2ConnectionContext` APIs

The engine today
(`Http2ConnectionContext.java:346, 400, 310, 193`) exposes
`processReceivedBytes` / `writePending` / `onBytesConsumed` /
`emitInitialSettings` and drains a small queue of control frames
(SETTINGS_ACK, PING_ACK, GOAWAY, RST_STREAM, WINDOW_UPDATE)
through `writePending`. There is no public response path yet.
M1 adds:

- **Response HEADERS emit.** A method with this shape (names
  indicative):

  ```java
  // Returns 0 on success, a negative error code on generation
  // mismatch, stream-closed, or header-list-size overrun.
  int emitResponseHeaders(
          int streamId,
          int generation,
          Http2HeadersWriter writer,  // callback with HpackEncoder
          boolean endStream);

  @FunctionalInterface
  interface Http2HeadersWriter {
      // Called once with the encoder already inside an open
      // block on engine-owned scratch. The writer appends the
      // response's headers (status pseudo-header first, then
      // regular headers) by calling HpackEncoder.encode(...),
      // threading the returned cursor through each put, and
      // returning the final cursor (or -1 on scratch overflow).
      // Must not retain the encoder reference past the callback
      // and must not call beginBlock / endBlock.
      long write(HpackEncoder encoder, long cursor, long limit);
  }
  ```

  `HpackEncoder.encode(...)` is cursor-stateless by design (see
  `HPACK_CODEC.md`), so the writer threads `cursor` and `limit`
  through each `encode` call and returns the final cursor rather
  than relying on encoder-internal position state.

  The engine serialises access to the shared `HpackEncoder`
  (one instance per connection; its dynamic table must advance
  in a deterministic order — see §10), encodes the block into
  engine-owned storage (not the caller's buffer), splits the
  encoded bytes across HEADERS + CONTINUATION by peer
  `MAX_FRAME_SIZE`, and hands the framed bytes to the outbound
  scheduler on `streamId`.
- **Outbound DATA tuple enqueue.** A method
  `enqueueData(streamId, generation, payloadAddr, payloadLen,
  endStream)` that appends a tuple to the stream's outbound
  queue inside the existing `Http2Stream`. Fails on window
  overflow beyond the per-stream queue cap (§10).

  **Ownership on enqueue.** Tuples point at bytes the engine
  must keep valid until the scheduler frames them. Caller-owned
  sink buffers are about to be reused by the next write, so one
  of the following must hold:

    1. **Engine copies on enqueue** into a per-stream outbound
       arena owned by `Http2Stream`. Simplest; pays one memcpy
       per enqueue. Under zero-window conditions the arena is
       bounded by the per-stream queue cap (§10), so memory is
       predictable.
    2. **Sink hand-off with ownership transfer.**
       `Http2ResponseSink` holds a small ring of output chunks
       (two or three chunk-sized native buffers); each `flush`
       passes the current chunk's `(addr, len)` to `enqueueData`
       and rotates to the next free ring slot. The engine
       releases the chunk back to the sink's ring when the
       scheduler has emitted the last DATA frame that referenced
       it. No copy, but needs explicit "release" callback and
       ring-slot accounting.

  **M1 picks option 1** (copy-on-enqueue). It keeps the engine
  self-contained, avoids a new release path on the sink, and
  the extra memcpy is small vs. the socket write that follows.
  M2 may revisit once real throughput numbers are in. The
  same rule applies to HEADERS / CONTINUATION bytes emitted
  below: `emitResponseHeaders` copies the encoded block into
  engine-owned storage before returning, so the caller's
  encoder buffer is free to re-use.
- **Round-robin outbound scheduler.** `writePending` grows to
  drain the control-frame queue first, then walk the set of
  streams with queued outbound tuples + non-zero stream and
  connection windows in round-robin order, emitting DATA frames
  up to `peerMaxFrameSize` and up to available windows, until
  either the send buffer fills or no stream is eligible. The
  scheduler's "which stream is next" cursor is connection-
  scoped per `STREAM_STATE_MACHINE.md` §7.
- **`onStreamWritable` callback.** After a WINDOW_UPDATE applies
  (connection- or stream-scoped), streams whose outbound queue
  was parked above the per-stream cap and is now below fire the
  new listener callback exactly once per transition. Firing
  condition is specified in the open question in §16.
- **Preface consumption hook.** The engine assumes frames from
  the first byte; the connection-layer preface drain stays
  outside it (§6.2 below).

These additions are **not optional shims** — the integration
can't proceed past M1 step 6 without them. They are scoped into
M1's build order as step 4 (lazy H2 engine) and step 6
(response sink) in §15.4.

## 5. Threading Model

QuestDB's HTTP server hands each connection to one IO worker at a
time, but the worker **can change** across park / resume
boundaries — see `HttpRequestProcessor` javadoc. HTTP/2 inherits
the same model with some multiplex-specific wrinkles that the
integration layer has to preserve.

**Invariants.**

1. **One owner worker per connection at any instant.** The
   `IODispatcher` guarantees that only one worker ever calls
   `HttpConnectionContext.handleClientOperation(...)` for a given
   context at the same time. Same invariant as today; nothing in
   the H2 mode weakens it.
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
event. The `HttpConnectionContext` travels with the connection
as a unit — the adapter pool, the parked-streams map, the HPACK
codecs, the pending-ops queue — because it's one `IOContext`
object that the dispatcher hands around. The new worker resolves
the right processor instance via
`HttpRequestProcessorSelector.resolveProcessorById`, exactly like
HTTP/1.x (§12). The adapter must hold **only** the processor's
handler id (an `int`), not a reference to any one worker's
processor object; otherwise it would resume against the wrong
instance after migration.

**Per-stream vs per-connection scope.** A single connection tick
may process events for many streams in arbitrary order (DATA on
stream 3, HEADERS on stream 5, WINDOW_UPDATE on stream 1, etc.
— the frame reader's output). The adapter must therefore treat
every stream as independent state: its `LocalValueMap` is
per-stream (§11), its `SecurityContext` is per-stream (§13), its
deferred-credit counter (`Http2Stream.outstandingInboundCredit`)
is per-stream, and its generation-token identity-guard is
per-stream (§7 step 6 of the state-machine doc).

**Foreign-thread marshalling — the critical case.** A processor
that returns `false` from the adapter's body-consumption callback
typically offloads the body bytes to a different thread (e.g.
`TextImportProcessor` writes into `TableWriter`'s ingest path).
When that thread finishes, it cannot call
`Http2ConnectionContext.onBytesConsumed(streamId, genToken, n)`
directly — that would mutate context state from outside the
owner worker, corrupting the flow-control windows, the coalescing
credit counter, and the outbound frame queue. Instead:

- `HttpConnectionContext` exposes a thread-safe `MPSC`-style
  **pending-ops queue**. Each stream may contribute multiple
  entries over its lifetime — `outstandingInboundCredit`
  accumulates across successive `onData` calls that return
  `false`, so a handler may produce one ack per offloaded chunk
  rather than one per stream. The queue is bounded (say,
  4 × `MAX_CONCURRENT_STREAMS` slots — open question on the
  exact bound); if a foreign thread finds the queue full it
  falls back to a blocking enqueue until the owner worker
  drains, which naturally backpressures the offload pipeline.
  The worker-thread ack pushes an entry
  `{streamId, generationToken, n}` and signals the dispatcher
  to wake the connection.
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
must push them (or a "please call resumeSend") onto the
pending-ops queue, not write them directly from the producing
thread. In practice, M1 and M2 processors are all synchronous
inside the IO tick (they may block on CairoEngine work, but they
do so on the owner worker), and the only foreign-thread case is
deferred body consumption.

**Race-free stream close.** `onStreamClosed` fires on the owner
worker as part of state-machine transition processing. The
adapter releases its pool slot and clears its `LocalValueMap` on
that same tick. A pending-ops entry whose `generationToken` no
longer matches the (now-bumped) `Http2Stream.generation` is
silently dropped on drain — the same guard as §7 step 6 of the
state-machine doc.

## 6. Preface Detection and Mode Selection

One TCP listen socket, one `IODispatcher`, one `IOContext`
subclass. Protocol mode is decided on first READ, inside
`HttpConnectionContext.handleClientOperation`.

RFC 9113 sec. 3.4: an HTTP/2 client with prior knowledge opens
the TCP connection and sends the 24-byte preface
`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` as its first bytes, before
any frames. An HTTP/1.x client sends
`METHOD path HTTP/1.x\r\n...`. The first four bytes (`PRI ` vs
any H1 method token) are disjoint; the 24-byte full match rules
out accidental matches from pathological H1 requests.

### 6.1 State

A new `byte protocolMode` field on `HttpConnectionContext`:

```
MODE_SNIFFING           = 0  // initial — peek on next READ decides
MODE_H1                 = 1  // HTTP/1.x (covers the WebSocket upgrade
                             //   path via the existing `isProtocolSwitched`)
MODE_H2_PREFACE_PENDING = 2  // H2 selected (sniff match, dedicated port,
                             //   or ALPN); 24-byte client preface not yet
                             //   drained
MODE_H2                 = 3  // H2 active, preface drained, frames only
```

`MODE_H2_PREFACE_PENDING` is required because **every HTTP/2
connection sends the 24-byte client preface regardless of how
the protocol was selected** (RFC 9113 §3.4: "The client MUST send
the client connection preface as the first application data
octets of a connection"). Skipping the sniff on a dedicated H2
port or under ALPN skips *detection*, not the preface itself;
the engine still cannot be handed bytes until those 24 octets
are consumed.

Transitions:

```
SNIFFING --peek match--> H2_PREFACE_PENDING --24 bytes drained--> H2
SNIFFING --peek miss--->  H1
doInit() (dedicated H2 port / ALPN=h2) --> H2_PREFACE_PENDING
doInit() (ALPN=http/1.1)               --> H1
```

All H2 code paths flow through `H2_PREFACE_PENDING` once; it is
not a shared-port-only optimisation. The existing
`isProtocolSwitched` bit stays and continues to be orthogonal to
`protocolMode`; WebSocket can only activate from `MODE_H1`.

A small (24-byte) native scratch buffer `peekScratchAddr` is
allocated in `doInit()` alongside the existing buffers. Freed in
`close()`.

### 6.2 Sniff logic

At the top of `handleClientOperation`:

```java
if(protocolMode ==MODE_SNIFFING){

sniffAndSelectMode();               // may throw to re-register for READ
}
        if(protocolMode ==MODE_H2_PREFACE_PENDING){

drainH2Preface();                   // may throw to re-register for READ
// drainH2Preface transitions to MODE_H2 once 24 bytes are consumed;
// also lazy-allocates the H2 engine + adapter pool + H2 send buffer
// and emits the initial SETTINGS.
}
        return switch(protocolMode){
        case MODE_H1 ->

handleH1Operation(op, selector, resched);  // existing body
    case MODE_H2 ->

handleH2Operation(op, selector, resched);  // new

default      ->throw

registerDispatcherDisconnect(DISCONNECT_REASON_UNKNOWN_OPERATION);
};
```

`sniffAndSelectMode()` uses
`NetworkFacade.peekRaw(socket.getFd(), peekScratchAddr, 24)`
(`NetworkFacadeImpl.java:183` — wraps `MSG_PEEK`). Outcomes:

| Peek result                       | Action                                                                                                                                                               |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `< 0` (error / disconnect)        | `throw registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV)`.                                                                              |
| `0` (no bytes yet)                | `throw registerDispatcherRead()` — identical idiom to `HttpConnectionContext.java:991`.                                                                              |
| `1..23`, proper prefix of preface | `throw registerDispatcherRead()` — wait for more bytes; next READ re-runs the sniff.                                                                                 |
| `>= 24`, full preface match       | `protocolMode = MODE_H2_PREFACE_PENDING`; return to `handleClientOperation`, which proceeds to `drainH2Preface()` below.                                             |
| anything else                     | `protocolMode = MODE_H1`; fall through to the existing H1 path. Peeked bytes stay in the kernel socket buffer and the H1 parser's first `socket.recv` consumes them. |

`sniffAndSelectMode` does not itself drain the preface or
allocate the H2 engine. It only classifies the connection.
All H2 setup — drain, engine allocation, initial SETTINGS —
lives in `drainH2Preface`, which also runs unmodified on
dedicated-port and ALPN paths that never see the sniff.

**Preface drain (`drainH2Preface`).**
`Http2ConnectionContext.processReceivedBytes` starts frame
parsing from the first byte; it does not recognise the 24-byte
preface (`HTTP2_FRAME_CODEC.md` §7 explicitly says to consume
it before handing bytes off).
`drainH2Preface` is the single chokepoint where this happens,
reached from three distinct entry conditions:

- **Shared-port sniff.** `sniffAndSelectMode` set
  `MODE_H2_PREFACE_PENDING` after matching the preface bytes
  via `MSG_PEEK`.
- **Dedicated H2 port.** `doInit()` set
  `MODE_H2_PREFACE_PENDING` eagerly based on the listener's
  configured protocol; no peek ran. The client still sends the
  preface as its first bytes.
- **TLS + ALPN (M3).** `doInit()` after `startTlsSession`
  completes and ALPN returns `h2` sets
  `MODE_H2_PREFACE_PENDING`; the TLS layer has handed the
  connection off without peeking. Again, the client's first
  plaintext bytes are the preface.

The drain itself:

```java
// Consume exactly 24 bytes from the socket, matching each byte
// against the fixed preface. Mismatch on any byte is a protocol
// error (someone opened H2 port / negotiated ALPN=h2 but then
// sent non-preface bytes).
int drained = h2PrefaceBytesDrained;  // persisted across ticks
while(drained< 24){
int n = socket.recv(peekScratchAddr + drained, 24 - drained);
    if(n< 0)throw

registerDispatcherDisconnect(DISCONNECT_REASON_PEER_DISCONNECT_AT_HEADER_RECV);
    if(n ==0){
// Not enough bytes yet. Persist progress, wait for READ.
h2PrefaceBytesDrained =drained;
        throw

registerDispatcherRead();
    }
            // Validate each byte against PREFACE[drained .. drained+n).
            if(!

matchesPreface(peekScratchAddr +drained, n, drained)){
        throw

registerDispatcherDisconnect(DISCONNECT_REASON_PROTOCOL_VIOLATION);
    }
drained +=n;
}
h2PrefaceBytesDrained =24;

allocateH2EngineIfNeeded();      // lazy: h2, adapter pool, send buffer
h2.

emitInitialSettings(sendBuffer, sendBufferLimit);

protocolMode =MODE_H2;
```

The validation step on every recv byte catches cases where the
peek is absent (dedicated port / ALPN) and the client
misbehaves, as well as partial-preface failures where the sniff
saw fewer than 24 bytes. `peekScratchAddr` doubles as the drain
buffer; the bytes are discarded once consumed.

The H1 path needs no equivalent drain — its parser reads from
the first byte, and the peek was non-consuming, so the bytes
are still in the kernel buffer waiting for the H1 parser's
first `socket.recv`.

**TLS caveat verified.** `Socket` (`Socket.java:85-96`) exposes
`recv`/`send` only; `peek` is raw-fd via `Net.peek`. Under TLS
the peek would return encrypted bytes, so preface sniffing is
plaintext-only. M3 replaces the sniff with ALPN-driven mode
selection: after `socket.startTlsSession` completes, the
negotiated protocol is read off the socket and `protocolMode` is
set directly, bypassing the peek.

### 6.3 Config gate

The sniff costs one `peek` syscall on the first READ of every
connection. A new `http.h2.enabled` config key (default `false`
until M2 stabilises) short-circuits the sniff to `MODE_H1` when
false. This also serves as the kill-switch for deployments that
do not want H2 enabled.

A dedicated H2 port (optional per `FLIGHT_SQL_DESIGN.md` §9 Q1)
does **not** require a new class. The shared-port deployment
uses the preface sniff as above; a strict-isolation deployment
that runs a second `HttpServer` on a dedicated port can set
`protocolMode = MODE_H2_PREFACE_PENDING` eagerly in `doInit()`
(gated by the port's configuration), skipping the sniff but
**not** skipping `drainH2Preface` — the client still sends the
24-byte preface first. Same class, same code, different
trigger. This is also how ALPN is wired in M3.

### 6.4 What stays unchanged

- `IODispatcher`, `AbstractIODispatcher`, `IODispatcherLinux/Osx/Windows`.
- `IOContext`, `IOContextFactory`, `IOContextFactoryImpl`.
- `HttpContextFactory` at `HttpServer.java:409`.
- `IORequestProcessor` wiring at `HttpServer.java:115-117`.
- `HttpRequestProcessorSelector`.
- The H1 / WebSocket code paths inside `handleClientRecv` /
  `handleClientSend` (`HttpConnectionContext.java:962-1129`) —
  they become the `MODE_H1` branch.

No 101 Upgrade in M1 / M2. Pure HTTP/2 prior-knowledge only,
which is what `curl --http2-prior-knowledge` and every gRPC
client use. M3 may add the `Upgrade: h2c` handshake if a real
caller needs it.

## 7. HttpRequestContext Interface Extraction

Currently `HttpRequestProcessor` takes `HttpConnectionContext`:

```java
void onHeadersReady(HttpConnectionContext context);

void onRequestComplete(HttpConnectionContext context);

void resumeSend(HttpConnectionContext context);
// ... etc
```

This ties every processor to the outer IOContext. For
multiplexed HTTP/2 we need processors to operate on a per-stream
handle. The refactor extracts the surface that processors
actually use into `HttpRequestContext`:

```java
// Locality is the LocalValue anchor. Retry is NOT on this
// interface — see "Retries and WaitProcessor" below.
public interface HttpRequestContext extends Locality {
    HttpRequestHeader getRequestHeader();

    HttpResponseHeader getResponseHeader();

    HttpChunkedResponse getChunkedResponse();

    SimpleResponse simpleResponse();                          // new interface — see below

    HttpRawSocket getRawResponseSocket();                     // null in H2 mode; see §10

    void resumeResponseSend() throws PeerIsSlowToReadException, PeerDisconnectedException;

    LocalValueMap getMap();             // LocalValue storage

    SecurityContext getSecurityContext();

    NetworkSqlExecutionCircuitBreaker getCircuitBreaker();

    NetworkSqlExecutionCircuitBreaker getOrCreateCircuitBreaker(CairoEngine engine);

    HttpCookieHandler getCookieHandler();

    CharSequenceObjHashMap<CharSequence> getParsedCookiesMap();

    Metrics getMetrics();

    RejectProcessor getRejectProcessor();

    // SqlExecutionContext accessors used by query processors
    SqlExecutionContextImpl getOrCreateSqlExecutionContext(
            CairoEngine engine, int workerCount);

    SqlExecutionContextImpl getSqlExecutionContext();

    AssociativeCache<RecordCursorFactory> getSelectCache();

    // Connection / request bookkeeping the processors read
    long getFd();                       // for log correlation

    long getTotalBytesSent();

    long getTotalReceived();

    long getLastRequestBytesSent();     // per-request counter, used by metrics

    int getNCompletedRequests();        // per-connection counter, used by metrics

    // Per-request counters consumed by query processors for timing / trace
    long getAuthenticationNanos();      // JsonQueryProcessorState.java:1271

    @NotNull
    StringSink getSessionIdSink();  // JsonQueryProcessor.java:865
}
```

The surface is wider than a minimal "protocol-adaptation"
interface because real processors reach into connection and
response state beyond the request / response header pair.
Verified callers that drive the method list:

- `LineHttpPingProcessor.java:63` calls `context.simpleResponse()`.
- `SqlValidationProcessor.java:164` calls `resumeResponseSend()`
  and `getOrCreateCircuitBreaker()`.
- `SqlValidationProcessor.java:477` reads
  `getLastRequestBytesSent()` and `getNCompletedRequests()` for
  metrics.
- `StaticContentProcessor.java:180, 182` calls
  `resumeResponseSend()` and `getRawResponseSocket()`.
- `JsonQueryProcessor.java:865` reads `getSessionIdSink()`.
- `JsonQueryProcessorState.java:1271` reads
  `getAuthenticationNanos()`.

Every additional method on the interface is a real call-site in
the existing processor tree, not speculative breadth.

**`SimpleResponse` extraction.** Today the simple-response path
returns `HttpResponseSink.SimpleResponseImpl`
(`HttpResponseSink.java:712`), a **non-static inner class** on
the H1 sink that emits `HTTP/1.1 ` status lines directly through
H1 internals (`headerImpl.status("HTTP/1.1 ", ...)` at line 708).
An H2 adapter cannot return that concrete type. The refactor
extracts a `SimpleResponse` interface with exactly the methods
the processors call (`sendStatusJsonContent` variants,
`sendStatusNoContent`, `sendStatusTextContent`, `clear()`, plus
the small-body variants used by `LineHttpPingProcessor` and the
reject path). The H1 sink's `SimpleResponseImpl` implements it
unchanged; `Http2ResponseSink` ships a parallel implementation
that emits via the H2 HEADERS + DATA path.

**Cookie handler signature.**
`HttpCookieHandler.processServiceAccountCookie(HttpConnectionContext,
SecurityContext)` (`HttpCookieHandler.java:34`) takes the
concrete H1 context today. Retype its first argument to
`HttpRequestContext` as part of step 2 of the refactor; the
only implementation in-tree (`DefaultHttpCookieHandler`) needs
the same-shape edit, and the call sites on
`HttpConnectionContext` / `Http2StreamRequestContext` both have
the requisite accessors on the interface. Without this change,
step 2's "every processor compiles unchanged" claim fails at
the cookie path.

**Processor-state classes that store `HttpConnectionContext`
directly.** Grep picked up several state-holder classes in
`io.questdb.cutlass.http.processors.*` that keep a strongly-
typed `HttpConnectionContext` field (e.g.
`JsonQueryProcessorState` holding `httpConnectionContext`).
These are not processor classes — they are per-request state
attached via `LocalValue` — but they must be retyped to
`HttpRequestContext` in the same PR that retypes the processor
signatures, otherwise the constructor calls stop compiling.
The change is mechanical (field type + constructor arg type),
but it expands the refactor footprint beyond the
`HttpRequestProcessor` interface itself.

**Refactor is largely mechanical but its footprint is wider
than "processor signatures only".** Step 2 lands the
`HttpRequestProcessor` retype, the `HttpCookieHandler`
signature change, the state-holder retypes, and the
`SimpleResponse` interface extraction together — they share a
single compile-unit of churn. Processors that reach into
HTTP/1.x-concrete state (`HttpHeaderParser`, chunked-encoding
state, keep-alive bookkeeping, `switchProtocol`, request-line
byte range) keep their `HttpConnectionContext` references and
are flagged as HTTP/1.x-only until M3. Grep the tree before
step 2 to classify each processor.

**Scope for "all read-path processors over H2" in M2.** The M2
target (§15.2) lists `JsonQueryProcessor` and
`ExportQueryProcessor`. Both rely on `JsonQueryProcessorState`
and related state classes that currently pin to
`HttpConnectionContext`; the M2 work must include the state-
class retype described above, not just processor-side imports.
If a state class proves too tangled to retype in one pass, M2
ships the retypeable processors first and defers the rest
rather than rushing.

**Retries and `WaitProcessor`.** The existing retry path
(`WaitProcessor.java:205-223`) casts any `Retry` back to
`HttpConnectionContext` on all three slow / disconnect
branches. If an H2 stream adapter implements `Retry` itself,
those casts break. The integration keeps retries owned by the
outer `HttpConnectionContext` — `HttpRequestContext` does
**not** extend `Retry` and `Http2StreamRequestContext` is
**not** a `Retry`. Under H2, stream-scoped errors either fail
fast (`RST_STREAM(INTERNAL_ERROR)`, no retry) or are retried
via a new adapter-aware path.

**This is a hard prerequisite for M2, not background work.**
`JsonQueryProcessor` — named in §15.2 as a target of the M2
"all read-path processors over H2" goal — actively uses the
retry machinery:
`JsonQueryProcessor.java:214` catches `EntryUnavailableException`
and throws `RetryOperationException.INSTANCE`;
`JsonQueryProcessor.java:295` implements
`onRequestRetry(HttpConnectionContext)`. Additional retry sites
at lines 551, 585, 688, 824 cover ALTER / UPDATE paths that
also flow through read-path processors. Any of these firing on
an H2 stream under the current `WaitProcessor` cast would
corrupt the dispatcher's view of the context on rerun.

M2 cannot enable `JsonQueryProcessor` over H2 without one of:

1. **`WaitProcessor` retype.** Parametrise the retry path on
   `Retry` + a `RetryContext` abstraction, drop the cast to
   `HttpConnectionContext`. `HttpConnectionContext` and
   `Http2StreamRequestContext` both implement `Retry`; the
   per-stream retry state rides on the adapter.
2. **Dedicated H2 retry path.** Park the adapter in a
   per-stream retry queue; the owner worker reruns on the
   next tick without going through `WaitProcessor`. Avoids
   the shared retry queue but duplicates some bookkeeping.

Option 1 is cleaner; option 2 is smaller scope. Either way,
the retry refactor lands before the M2 JsonQueryProcessor /
ExportQueryProcessor enablement, not after. Tracked as
prerequisite in §15.2.

Both `HttpConnectionContext` (H1 mode, where the outer context
itself is the request handle) and `Http2StreamRequestContext`
(H2 mode, where each stream gets its own handle) implement this
interface. The processor interface becomes:

```java
public interface HttpRequestProcessor {
    void onHeadersReady(HttpRequestContext ctx);

    void onRequestComplete(HttpRequestContext ctx);

    void resumeSend(HttpRequestContext ctx);
    // ...
}
```

**Refactor plan.** Three PRs. Only step 1 is purely mechanical;
steps 2 and 3 carry the real scope discussed above.

1. **Introduce `HttpRequestContext` interface** with the method
   set above, implemented only by `HttpConnectionContext`. No
   processor or cookie / selector / state-class signature
   changes yet. Tests unchanged. Mechanical.
2. **Surface changes and processor retype (one PR).** Bundle
   together because they share a single compile unit:
    - Extract `SimpleResponse` interface; `HttpResponseSink.SimpleResponseImpl`
      implements it unchanged.
    - Retype `HttpCookieHandler.processServiceAccountCookie`'s
      first parameter from `HttpConnectionContext` to
      `HttpRequestContext` (`HttpCookieHandler.java:34`).
    - Retype every `HttpRequestProcessor` method parameter from
      `HttpConnectionContext` to `HttpRequestContext`.
    - Retype processor-state classes that hold a concrete
      `HttpConnectionContext` field (e.g.
      `JsonQueryProcessorState.httpConnectionContext` at
      `JsonQueryProcessorState.java:1271`).
    - Classify each processor: mostly-H1-only (keeps
      `HttpConnectionContext` where it reaches into H1 framing
      state) versus interface-only (runs over H2). Grep first;
      the concrete known cases are the state-class retypes
      just above and the processors that touch
      `HttpHeaderParser`, chunked-encoding state, keep-alive
      bookkeeping, `switchProtocol`, or the request-line byte
      range.
3. **Add `Http2StreamRequestContext`** as a second implementor
   of `HttpRequestContext`. Touches no processors, no
   state-holders — only the adapter plus the H2 listener
   implementation on `HttpConnectionContext`.

After step 2, each processor file is either compiled against
the interface only (H2-eligible) or still imports
`HttpConnectionContext` (H1-only until retyped or reworked in a
later PR). The expected split is biased toward H1-only in M1 —
M2 migrates the read-path processors off the H1 concretes via
incremental state-class retypes.

**Methods that do NOT belong on `HttpRequestContext`.** Anything
that exposes HTTP/1.x framing state: the `HttpHeaderParser`
instance, chunked-encoding state, keep-alive bookkeeping, the
request-line byte range, the `switchProtocol` WebSocket hook.
Those live on `HttpConnectionContext` only. A processor that
needs them is HTTP/1.x-only by definition.

## 8. Per-Stream Request Context

`Http2StreamRequestContext` presents one HTTP/2 stream to a
processor as if it were a dedicated connection. Pooled by
`HttpConnectionContext`; the pool is sized to exactly
`SETTINGS_MAX_CONCURRENT_STREAMS` (the same number as the
LIVE-slot budget of the `Http2StreamPool` inside
`Http2ConnectionContext` — `STREAM_STATE_MACHINE.md` §4). The
two pools are 1:1: every LIVE stream has exactly one adapter,
and DISCARDING_BLOCK / TOMBSTONE slots do not consume adapter
capacity, so the adapter pool cannot be exhausted while the
state machine still admits streams.

The adapter pool lives on `HttpConnectionContext` and migrates
with the connection across workers (§5). Neither the pool itself
nor any adapter is ever shared across connections or across
workers — all adapter access is single-threaded on the current
owner worker.

**Fields.** Every field is pre-allocated once and reused across
requests — the "zero-GC on the hot path" rule applies here too.

- `int streamId` — the HTTP/2 stream id this adapter is bound to
  for the current request.
- `int streamGeneration` — the `Http2Stream.generation` captured
  at bind time. Used when the processor defers a DATA ack; the
  generation goes into `context.onBytesConsumed(streamId,
  generationToken, n)` verbatim (see
  `STREAM_STATE_MACHINE.md` §7 step 6).
- `Http2RequestHeader header` — the adapter's
  `HttpRequestHeader` implementation. This is a new class, not
  the existing `Http2RequestHeadersView`: the view
  (`Http2RequestHeadersView.java:45-66`) exposes only pseudo-
  header address/length slots, whereas `HttpRequestHeader`
  (`HttpRequestHeader.java:33-70`) requires URL + query
  resolution, method helpers (`isGetRequest`, `isPostRequest`,
  `isPutRequest`), content-length / content-type lookups, and
  a general `getHeader(Utf8Sequence)` over arbitrary regular
  headers.

  **Header lifetime — copy on bind.** The view's javadoc
  (`Http2RequestHeadersView.java:35-40`) states that
  `(addr, len)` pairs are stable only for the duration of
  `onRequestHeaders`, but processors read
  `ctx.getRequestHeader()` on every later callback
  (`onData`, `resumeSend`, `onRequestComplete`,
  `onConnectionClosed`). To bridge the two lifetimes, the
  adapter **copies** pseudo-header slots and regular-header
  tuples into adapter-owned native storage inside the
  `onRequestHeaders` dispatch, before the processor is called.
  Storage is a fixed-size per-adapter native buffer sized to
  the H2 `MAX_HEADER_LIST_SIZE` policy cap (a single allocation
  at adapter construction, reused across requests — zero-GC on
  the hot path). `Http2RequestHeader` then reads from that
  adapter-owned copy, not from the view, for the rest of the
  stream's lifetime.

  The state-machine contract is unchanged; the copy is a
  layer concern. Alternative — extending the state machine's
  stream-close lifetime contract to keep staging alive until
  `onStreamClosed` — was considered but rejected: it would
  couple `Http2ConnectionContext`'s stream-pool reuse rules
  to the adapter's read pattern and complicate §11 of the
  state-machine doc.

  The `Http2RequestHeader` then computes:
    - `:path` → `getUrl()` + `getQuery()` (split on the first `?`).
    - `:method` → `isGetRequest` / `isPostRequest` / `isPutRequest`.
    - regular-header tuple table → `getHeader(name)`,
      `getHeaderNames()`, `getContentLength()`, `getContentType()`,
      `getBoundary()`, `getCharset()`, `getContentDisposition*`,
      `getStatementTimeout()`.
    - `getMethodLine()` is synthesised from `:method` + `:path` +
      a fixed `HTTP/2.0` tail only where log correlation needs it;
      processors that care about the exact method line are
      HTTP/1.x-only.
- `LocalValueMap localValueMap` — per-stream, not per-connection
  (see §11).
- `SecurityContext securityContext` — set by the auth path (§13)
  during `onRequestHeaders` dispatch.
- `Http2ResponseSink responseSink` — the response buffer and
  frame emitter.
- `Http2ChunkedResponse chunkedResponse` — processor-facing
  response API; wraps `responseSink`.
- `int handlerId` — the id returned by
  `HttpRequestProcessorSelector.getLastSelectedHandlerId()`
  immediately after `selector.select(...)` at request-dispatch
  time (`HttpRequestProcessorSelector.java:35-42`). Used for
  park/resume across worker migration (§12).
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
   `Http2StreamListener` implementation (hosted by
   `HttpConnectionContext`):
    - Acquire a pooled `Http2StreamRequestContext`; bind to
      `(streamId, streamGeneration)`.
    - Populate the adapter's `Http2RequestHeader` from `view` +
      the per-stream staged regular-header tuple table.
    - Run the authentication pipeline (§13); populate
      `securityContext`.
    - Select the processor:
      ```java
      HttpRequestProcessor processor = selector.select(adapter.getRequestHeader());
      adapter.handlerId = selector.getLastSelectedHandlerId();
      ```
      `select` returns the processor instance; the id is read via
      `selector.getLastSelectedHandlerId()` afterwards
      (`HttpRequestProcessorSelector.java:35, 41`,
      `HttpServer.java:466-476`). The selector already operates
      on `HttpRequestHeader`, so no selector changes needed.
    - Call `processor.onHeadersReady(ctx)`.
    - If `endStream == true`, follow up with
      `processor.onRequestComplete(ctx)` immediately — no body
      will arrive.
2. `onRequestHeader(...)` fires before `onRequestHeaders` per
   `STREAM_STATE_MACHINE.md` §11. The adapter does not need to
   observe these individually; the per-field callbacks exist
   so that other (non-adapter) listeners can stream headers if
   they wish. Once `onRequestHeaders` fires, the adapter copies
   every staged field into its own native storage as described
   above, so header reads during later callbacks go through the
   copy, not the staging table.
3. `onData(streamId, addr, dataLen, endStream, genToken)`:
    - Route body bytes to the processor's `onChunk`-equivalent
      entry point. For a POST / PUT processor, this is where
      `HttpMultipartContentProcessor` / `HttpPostPutProcessor`
      consume bytes.
    - Return `true` if synchronously consumed, `false` if the
      processor needs to defer. On deferral, the adapter stashes
      `genToken` and produces it on the later
      `onBytesConsumed` call.
    - **Terminal callback timing.** If `endStream == true` and
      the body consumer returned `true` (synchronous), call
      `processor.onRequestComplete(ctx)` right away. If
      `endStream == true` and the consumer returned `false`
      (deferred), the adapter **must not** call
      `onRequestComplete` yet — firing it would tell the
      processor "request done" before the final DATA bytes have
      actually been processed, which matters for POST / PUT
      paths (e.g. `TextImportProcessor` commits only after the
      last chunk lands in the `TableWriter`). The adapter sets
      a sticky `pendingTerminalComplete` flag and the
      pending-ops drain (§5, §9) calls
      `processor.onRequestComplete(ctx)` when the final
      `onBytesConsumed` ack for the terminal frame arrives and
      the flag is clear-to-fire. A stream reset before the ack
      lands drops the flag silently — `onStreamClosed` takes
      over via `processor.onConnectionClosed(ctx)`.
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

Concrete flow for an inbound POST with a body, in `MODE_H2`:

```
peer bytes
  -> HttpConnectionContext.handleClientOperation(READ)
    -> drain pending-ops queue (foreign-thread inbound acks)
    -> read socket into recvBuffer
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
      -> adapter selects processor, calls
         processor.onHeadersReady(ctx)
      -> subsequent DATA frames:
         Http2StreamListener.onData(streamId, addr, dataLen,
                                     endStream, genToken)
         -> adapter delivers to processor's body-consumer
         -> on endStream=true:
            if body-consumer returned true (sync):
              processor.onRequestComplete(ctx)
            if body-consumer returned false (deferred):
              set pendingTerminalComplete on adapter; fire
              onRequestComplete from the pending-ops drain
              after the final onBytesConsumed ack lands
              (see §8 lifecycle step 3)
    -> Http2ConnectionContext.writePending(sendBuffer, cap)
    -> socket.send(sendBuffer, n)
```

The listener implementation is an inner class (or a package-
private helper) on `HttpConnectionContext` — same owner worker,
same heap, no cross-component marshalling.

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
  pushes the ack entry onto the `HttpConnectionContext`'s
  pending-ops queue and signals the dispatcher. The next tick on
  the owner worker drains the queue and invokes
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
surface and lives on the per-stream adapter, not on
`HttpConnectionContext` directly.

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
   `(payloadAddr, payloadLen)` tuple to `enqueueData`, which
   copies the bytes into engine-owned per-stream storage (see
   §4.4) so the sink buffer is immediately free to accept more
   writes. The tuple then sits in the state-machine queue until
   the scheduler emits a DATA frame under available window.
   `PeerIsSlowToReadException` fires only when **both** buffers
   are at their configured bound:
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
4. **End of response.** The processor signals end-of-body via
   an explicit `done()` call on the sink, mirroring the H1
   `HttpChunkedResponse.done()` shape. Three variants:
    - **Body only.** `done()` flushes any buffered bytes; the
      last `enqueueData` call carries `endStream=true`, and the
      scheduler emits DATA with `END_STREAM` set.
    - **No body.** A processor that returns an empty-body
      response calls `done()` without any prior body writes.
      The sink emits the HEADERS frame with `END_STREAM` set
      and no DATA frame at all.
    - **Body + trailers** (gRPC shape — M2). `sendTrailers(...)`
      replaces `done()`. The final DATA frame does **not**
      carry `END_STREAM`; instead a second HEADERS frame (built
      via `emitResponseHeaders` with `endStream=true`) carries
      the trailer fields. A processor that calls `sendTrailers`
      after `done()` has already fired the end-of-stream marker
      is a programming error — the M2 sink asserts on it.

   `onStreamClosed` fires on the listener after the final
   END_STREAM-carrying frame flushes to the socket, regardless
   of which variant closed the response.

**H2 send buffer.** `HttpResponseSink` is H1-chunked-encoding-
specific (`HttpConnectionContext.java:167`). M1 allocates a
separate native send buffer on `HttpConnectionContext` for H2
mode, sized to peer `SETTINGS_MAX_FRAME_SIZE` plus control-frame
headroom. Sharing the existing response-sink buffer would
require `HttpResponseSink` to become protocol-aware; not worth
it.

**Trailers** (M2). M1 does not ship the trailer path; the
`sendTrailers` method is defined on the `Http2ChunkedResponse`
surface but throws `UnsupportedOperationException` until M2
wires it up. M2 builds the trailing HEADERS frame via the
engine's `emitResponseHeaders(streamId, generation, writer,
endStream=true)` call (§4.4); the trailer-specific validation
(no pseudo-headers allowed in trailers per RFC 9113 sec. 8.1)
lives in the writer callback, not in the engine.

**Raw socket access.** `HttpRequestContext.getRawResponseSocket()`
does not make sense for H2 — there is no single byte stream
behind a stream. For H2, `getRawResponseSocket()` either
returns `null` (and processors that use it are H2-incompatible
and must guard on the protocol type) or returns a thin adapter
that converts raw byte writes into DATA frames with the same
flow-control contract. Pick the latter if any production
processor depends on it; M1 can return `null` and treat
raw-socket processors as HTTP/1.x-only.

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

**Pool reuse and close semantics.** The adapter pool reuses
each adapter across many streams on the same connection, so the
cleanup cadence must match *H1 connection end*, not *H1 request
end*. Verified against source:

- `LocalValueMap.clear()` (`LocalValueMap.java:45-53`) only
  calls `.clear()` on values that implement `Mutable`. It does
  **not** invoke `close()` on `Closeable` values.
- `LocalValueMap.close()` (`LocalValueMap.java:55-65`) is the
  method that calls `Misc.freeIfCloseable` on every entry.
- H1 `HttpConnectionContext.reset()` (called per request,
  `HttpConnectionContext.java:411`) uses `clear()`. Closeable
  LocalValue entries on H1 therefore survive across requests
  on the same connection and are freed only on
  `HttpConnectionContext.close()` at connection end
  (`HttpConnectionContext.java:229`).

H2 stream reuse is not an H1 request boundary; it's an adapter
re-binding to a new logical request. Closeable per-request
state attached via `LocalValue` on the adapter MUST be released
at stream close, otherwise it leaks across stream reuses within
one TCP connection. The adapter therefore calls
`localValueMap.close()` (not `clear()`) inside `onStreamClosed`,
followed by a fresh `LocalValueMap` allocation on next bind OR
a reusable map implementation with an explicit
"release-closeables-and-empty" method added to
`LocalValueMap`. M1 picks the second option — add
`LocalValueMap.release()` that iterates entries, calls
`Misc.freeIfCloseable` on the value, nulls the slot, and keeps
the backing table for reuse. H1 paths keep calling `clear()`
(unchanged behaviour).

**Adapter pool teardown.** On connection close
(`HttpConnectionContext.close`), every adapter in the pool —
whether bound or free — has `localValueMap.close()` called,
followed by native buffer frees for the per-adapter header
copy, response sink, and HPACK encoder buffer.

**Connection-scope state falls out of the design.** The
`localValueMap` field already on `HttpConnectionContext`
(`HttpConnectionContext.java:93`) continues to exist in H2 mode,
but nothing in the per-stream dispatch path writes to it. A
processor that needs state that spans all streams on one TCP
connection (none do today — e.g. a connection-wide auth cache)
attaches via a `LocalValue` keyed on the outer
`HttpConnectionContext.getMap()`. This resolves the
connection-scope-LocalValue question that used to be open: no
second map is needed; the existing one plays the role.

## 12. Park / Resume

HTTP/1.x park / resume is per-connection: a processor fills the
send buffer, the context parks the whole connection for WRITE
readiness, a different worker resumes the processor via
`HttpRequestProcessorSelector.resolveProcessorById` on the same
connection context.

**H2 park / resume is per-stream.** The connection can keep
servicing other streams even while one stream is parked on its
own outbound window.

The per-stream state lives on a **parked-streams set** on
`HttpConnectionContext`:
`IntObjHashMap<Http2StreamRequestContext>` keyed by stream id,
populated when a stream throws `PeerIsSlowToReadException` out
of its sink. The set exists because the state machine only
knows "this stream has a non-empty outbound queue"; it does not
know which streams have an adapter currently parked and waiting
for a resume callback. The set is the adapter layer's view on
top of the engine's outbound accounting.

Resume fires on any event that grows a parked stream's
outbound-eligible window back under the per-stream queue cap:

- Inbound `WINDOW_UPDATE` on the stream id.
- Inbound `WINDOW_UPDATE(0)` on the connection, combined with
  any parked stream that has non-zero `outboundStreamWindow`.
- Inbound `SETTINGS` that raises `SETTINGS_INITIAL_WINDOW_SIZE`
  (the state machine sweeps every outbound-active stream and
  adjusts windows — `Http2ConnectionContext.java:504-537`),
  potentially unparking streams that were blocked on
  per-stream (not connection) window.

In all three cases the state machine raises
`onStreamWritable(streamId)` on the listener; the listener
looks the adapter up in the parked-streams set and calls
`processor.resumeSend(adapter)`.

Even when one or more streams parked in a given tick,
`HttpConnectionContext` re-registers the connection for WRITE
exactly once at the dispatcher level — the connection's
readiness is a single bit as far as `IODispatcher` is concerned;
the per-stream resume fan-out happens on the next tick when the
state machine applies the incoming `WINDOW_UPDATE`(s).

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

Call-site shape (verified against
`HttpRequestProcessorSelector.java:35-42` and
`HttpServer.java:466-476`):

- At `select` time in `onRequestHeaders`:
  ```java
  HttpRequestProcessor p = selector.select(adapter.getRequestHeader());
  adapter.handlerId = selector.getLastSelectedHandlerId();
  ```
  `select` returns the processor instance; the handler id is
  published as side-effect state on the selector and read
  afterwards via `getLastSelectedHandlerId()`.
- At resume time, whether this tick is on worker A or B:
  ```java
  HttpRequestProcessor p = selector.resolveProcessorById(
          adapter.handlerId, adapter.getRequestHeader());
  p.resumeSend(adapter);
  ```
  `resolveProcessorById` takes the request header as its second
  argument so the handler can pick the right processor variant
  based on request state (the H1 path at
  `HttpConnectionContext.java:956-960` does the same).

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
`HttpConnectionContext` would skip re-evaluating the same bearer
token on every stream. Worth measuring before implementing —
the per-stream path is already fast.

**Per-stream circuit breakers.** H1 has a single
`NetworkSqlExecutionCircuitBreaker` per connection
(`HttpConnectionContext.java:117`, lazy-allocated via
`getOrCreateCircuitBreaker`). Under H2, cancelling one stream
via `RST_STREAM(CANCEL)` must not abort SQL executing for other
streams on the same connection, so each adapter owns its own
circuit breaker.

- `Http2StreamRequestContext.getOrCreateCircuitBreaker(engine)`
  lazily builds an adapter-scoped
  `NetworkSqlExecutionCircuitBreaker` on first call, reusing it
  across streams bound to the same adapter (cleared on stream
  close via the `clear()` on the breaker).
- Native memory owned by each breaker is freed when the adapter
  itself is closed during connection teardown (§11 adapter pool
  teardown), not at stream end — the breaker object is reusable
  across bindings.
- `RST_STREAM(CANCEL)` inbound trips the breaker for that
  adapter only, leaving other streams' breakers untouched.
- H1's single-breaker shape is preserved for H1 mode; the
  interface method `getOrCreateCircuitBreaker(engine)` is the
  same call site on both contexts, just with different scoping
  semantics.

**Per-stream reject processor.** `RejectProcessor` is a stateful
per-request object (`HttpConnectionContext.java:102`, reset in
`reset()`). Each H2 adapter owns its own RejectProcessor
instance, built during adapter construction from the same
factory `HttpContextConfiguration.getRejectProcessorFactory()`
uses on H1. Sharing a single RejectProcessor across concurrent
streams would corrupt its per-request state.

## 14. Error Mapping

The processor pipeline throws a small set of checked exceptions:

| Exception                    | HTTP/1.x meaning                          | HTTP/2 meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|------------------------------|-------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `PeerDisconnectedException`  | Socket closed / reset                     | Connection-level socket close. `HttpConnectionContext` tears down (all streams lost).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `PeerIsSlowToReadException`  | Send buffer full, park for WRITE          | Stream-scope park (see §12). The connection stays up; other streams proceed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `PeerIsSlowToWriteException` | Receive buffer empty, park for READ       | Effectively never fires on H2. HTTP/1.x throws this when the processor is parsing headers / body and needs more bytes that haven't arrived. Under H2, headers are fully assembled by `Http2ConnectionContext` before the processor is invoked, and body bytes are surfaced one DATA frame at a time through `onData`; a processor that needs more body simply waits for the next `onData` invocation. The adapter should treat `PeerIsSlowToWriteException` as a programming error (log and `RST_STREAM(INTERNAL_ERROR)`) in M1; the state machine's flow-control buffering subsumes its semantics. |
| `ServerDisconnectException`  | Server-initiated shutdown                 | For a stream-scope error: `RST_STREAM(INTERNAL_ERROR)` on the stream, connection survives. For a server-wide shutdown: `GOAWAY(NO_ERROR)` followed by drain per `STREAM_STATE_MACHINE.md` §12.                                                                                                                                                                                                                                                                                                                                                                                                      |
| `HttpException` (processor)  | Response reset mid-body, close connection | `RST_STREAM(INTERNAL_ERROR)` on the stream. Connection survives so other streams are unaffected.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

The adapter catches these at the `processor.*` call sites and
translates. An uncaught `RuntimeException` from a processor is
also stream-scope: `RST_STREAM(INTERNAL_ERROR)`, log, and
`onStreamClosed` fires. Connection-scope errors are only the
RFC 9113 sec. 5.4.1 set (frame-size violation, compression
failure, flow-control overrun, etc.) handled inside
`Http2ConnectionContext` per §12 of the state-machine doc;
processors do not surface those.

**Connection idle-timeout under H2.** `invalid()`
(`HttpConnectionContext.java:393`) is **not** a "connection
still has useful work" signal — it's read by the dispatcher's
`doDisconnect` (`AbstractIODispatcher.java:454-457`) as "do not
tear down this context now", and the Linux idle scan at
`IODispatcherLinux.java:146-153` calls `doDisconnect` directly
without consulting it for idle reaping. Overloading `invalid()`
with `h2.getActiveStreamCount() > 0` would skip legitimate
cleanup paths (disconnect-on-error, shutdown) while still
allowing the idle scan to reap a mid-stream connection — the
worst of both.

M1 keeps `invalid()` unchanged and introduces a dedicated H2
idle policy instead:

- Every successful `handleH2Operation` tick refreshes the
  connection's idle-timer anchor whenever a frame is read or a
  byte is written, so a connection with healthy stream traffic
  never ages into the idle scan.
- A `HEARTBEAT` operation from the dispatcher
  (`HttpConnectionContext.handleClientOperation`'s
  `IOOperation.HEARTBEAT` case at line 375) gets a new H2
  branch: if `protocolMode == MODE_H2` and any stream has
  made progress since the previous heartbeat, re-register for
  READ to keep the timer fresh; otherwise fall through to the
  existing heartbeat behaviour (which lets the dispatcher apply
  its idle-timeout).
- Long-running streams with no traffic (gRPC long-polls, Flight
  SQL blocked on a slow query) need the heartbeat-anchor reset
  from the *processor* side via an explicit "stream alive"
  signal. Deferred to M2 alongside the dispatcher wake-up
  primitive (§16); M1 processors all complete within one tick.

This keeps `invalid()` semantically consistent with how the
dispatcher uses it today and isolates the "active streams"
concern to the heartbeat / idle-refresh path where it belongs.

**Pipelined inner recv loop.** The existing
`handleClientOperation` body (`HttpConnectionContext.java:379-388`)
re-enters `handleClientRecv` in a loop while
`keepConnectionAlive()` — an H1-pipelining optimisation. For H2
the single-tick `handleH2Operation` already drains what it can;
gate the loop with `if (protocolMode == MODE_H1 &&
keepConnectionAlive())`.

## 15. Milestone Scoping

The milestone shape reflects the Flight-SQL-only framing from
§1. The earlier three-milestone plan (M1 HealthCheck end-to-end,
M2 full REST-over-H2, M3 TLS + Flight SQL) is superseded.

### 15.1 Milestone 1 — H2 engine fit for gRPC

**Already landed (Waves 1–2):**

- Frame codec, HPACK codec, stream state machine, outbound
  arena with copy-on-enqueue ownership, per-stream tuple queue,
  round-robin scheduler, park machinery, `onStreamWritable`
  listener, HPACK snapshot / restore for PARK rollback.
- `protocolMode` on `HttpConnectionContext`,
  `sniffAndSelectMode`, `drainH2Preface`,
  `allocateH2EngineIfNeeded`, `handleH2Operation` skeleton.
  `http.h2.enabled` config gate (default off).

**Landed but slated for removal (Wave 4):** the H1-mimicry
layer from Wave 3 — `Http2StreamRequestContext`,
`Http2StreamRequestContextPool`, `Http2ResponseSink`,
`Http2ChunkedResponse`, `Http2SimpleResponse`,
`Http2RequestHeader`, `Http2StreamRequestContextListener`,
`HttpRequestContext` interface extraction, `SimpleResponse`
interface extraction, `LocalValueMap.release`. Built on the
assumption that existing H1 processors would run over H2;
Flight SQL does not reuse any of it.

**Remaining (Wave 4):**

- Remove the H1-mimicry layer listed above. Replace the in-
  class listener with a slim no-op placeholder that the
  Flight SQL handler surface (M2) will substitute at bind
  time.
- Pseudo-header capture for `:method`, `:scheme`, `:path`,
  `:authority`, and `content-type` only. Per-stream header
  staging buffer on `Http2Stream`, real `HpackListener`
  replacing `NOOP_HPACK_LISTENER`, concrete
  `Http2RequestHeadersView` backed by the staging buffer.
  Scope is deliberately narrow: just enough for a gRPC router
  to see the RPC path and content-type. Full §11 validator
  (ordering, forbidden-headers, content-length, uppercase-
  name, TE restrictions, connection ban) deferred to M2.
- Trailer emission. `Http2ConnectionContext.emitTrailers(
  streamId, generation, writer, endStream=true)` as a peer of
  `emitResponseHeaders`, sharing the same HPACK-snapshot-and-
  restore discipline for PARK rollback. gRPC requires trailers
  for every RPC — this moves up from the old M2.
- Park / resume wired end-to-end at the engine level — the
  listener callback is already in place; Wave 4 adds a
  generic test-only consumer that drives PARK via tiny
  `SETTINGS_INITIAL_WINDOW_SIZE` + `WINDOW_UPDATE` + tuple-
  ring saturation and asserts resume actually drains.

**M1 exit criterion:** a synthetic HEADERS + DATA + trailers
round-trip test at the engine level — no HTTP processor, no
gRPC framer yet — proves the engine can carry a gRPC-shaped
request and response including trailers and park / resume.

### 15.2 Milestone 2 — TLS, ALPN, gRPC framing, Flight SQL handlers

- ALPN in the TLS layer. `socket.startTlsSession` negotiates
  `h2`; the TLS code path pre-sets
  `protocolMode = MODE_H2_PREFACE_PENDING` in `doInit()` and
  skips the MSG_PEEK sniff. The 24-byte preface drain still
  runs — RFC 9113 §3.4 requires every H2 client to send the
  preface regardless of negotiation path.
- gRPC framing layer. 5-byte prefix (`compressed` flag +
  4-byte length) over DATA frames; message reassembly across
  frame boundaries; `grpc-timeout`, `grpc-encoding`,
  `grpc-accept-encoding` header handling.
- Flight SQL handler surface. A narrow `FlightSqlHandler`
  interface bound to the engine via a custom
  `Http2StreamListener` that routes on `:path` — one RPC per
  path per `arrow.flight.protocol.FlightService/{Method}`.
  Initial handlers: `Handshake`, `GetFlightInfo`, `DoGet`,
  `DoPut`, `DoAction`. Result-set streaming in `DoGet`
  exercises Wave 4's park / resume for real.
- §11 validator subset required for gRPC correctness:
  reject requests missing required pseudo-headers, reject
  non-POST, reject bad `content-type`, reject uppercase header
  names. The full RFC 9113 §8.1.2 set is not needed — what's
  on the wire is a closed set of known-good clients.
- Error mapping. RPC-level failure maps to HTTP 200 with a
  trailer `grpc-status` and optional `grpc-message`. Transport
  failure emits `RST_STREAM` / `GOAWAY` per §14.

### 15.3 Milestone 3 — Production polish

Deferred until after Flight SQL is working end-to-end:

- GOAWAY graceful-shutdown. Server shutdown emits
  `GOAWAY(NO_ERROR)` with `lastProcessedStreamId`; new streams
  refused, existing streams allowed to drain per
  `STREAM_STATE_MACHINE.md` §12.
- h2spec full conformance via the harness in
  `HTTP2_FRAME_CODEC.md` §12.5.
- Full §11 validator (content-length reconciliation, TE
  restrictions, connection ban).
- Extended CONNECT (RFC 8441) if any Flight client ends up
  needing bidirectional streaming over a single long-lived
  stream.

### 15.4 Deliberately out of scope

The following pre-pivot items are retired and will not land
unless a future non-Flight-SQL consumer re-raises the need:

- `JsonQueryProcessor` / `ExportQueryProcessor` /
  `HealthCheckProcessor` / any existing REST processor running
  over H2. The existing H1 listen socket keeps serving them.
- `WaitProcessor` retry-path refactor for H2.
- Foreign-thread dispatcher wake-up primitive for H2 body
  consumption (Flight SQL body consumption happens on the
  same worker that drives the H2 engine).
- `HttpCookieHandler.parseCookies` retype.
- `RejectProcessorFactory` retype.
- Cookie parsing over H2.
- `HttpRequestContext` as a processor-facing abstraction for
  H2 (Wave 4 rips out the H2 side; the H1 path keeps the
  interface it already has, or reverts to the concrete type —
  see Wave 4 §15.5 step 1).
- `Http2StreamRequestContext` adapter layer, response sink,
  chunked / simple response adapters, `LocalValueMap.release`.

### 15.5 Build order for Wave 4

Single PR, or split at the reviewer's discretion. The ordering
inside is:

1. **Rip out the H1-mimicry layer.** Delete
   `Http2StreamRequestContext`,
   `Http2StreamRequestContextPool`, `Http2ResponseSink`,
   `Http2ChunkedResponse`, `Http2SimpleResponse`,
   `Http2RequestHeader`. Delete
   `Http2StreamRequestContextListener` from
   `HttpConnectionContext` and the `h2AdapterPool`
   field / lifecycle. Delete `LocalValueMap.release`. Revert
   the `HttpRequestContext` interface extraction if no
   current caller still needs it — check H1 first; if H1 now
   takes `HttpRequestContext` everywhere, leaving the
   interface in place is harmless and pure refactoring churn
   to revert, so leave it. Revert the `SimpleResponse`
   interface extraction on the same principle. Delete the
   Wave 3 tests
   (`Http2StreamAdapterPoolTest`,
   `Http2RequestHeaderCopyOnBindTest`,
   `Http2ResponseSinkSingleFrameTest`,
   `LocalValueMapReleaseTest`).
2. **Pseudo-header capture.** Per-stream staging buffer on
   `Http2Stream` (8 KiB default,
   `Http2ConnectionConfig.headerStagingBytesPerStream`); real
   `HpackListener` replacing `NOOP_HPACK_LISTENER`; concrete
   `Http2RequestHeadersView`; overflow → `PROTOCOL_ERROR`
   reset. Slots: `:method`, `:scheme`, `:path`, `:authority`,
   `content-type`. No `Host` capture — H1-ism that Flight SQL
   doesn't use. All other headers consumed and dropped.
3. **Trailer emission.**
   `emitTrailers(streamId, generation, writer, endStream=true)`
   on `Http2ConnectionContext` with the same HPACK snapshot /
   restore discipline as `emitResponseHeaders`. Tuple kind
   `TUPLE_KIND_TRAILERS` on `Http2Stream`; scheduler treats
   trailer HEADERS identically to response HEADERS except
   that trailers always carry `END_STREAM` and the state
   machine transitions accordingly. State-machine update per
   `STREAM_STATE_MACHINE.md` §5.
4. **Park / resume end-to-end test.** Test-only listener that
   emits a multi-DATA-frame response, drives PARK via tiny
   `SETTINGS_INITIAL_WINDOW_SIZE`, asserts resume on
   `WINDOW_UPDATE`. Same shape for tuple-ring saturation and
   `WINDOW_UPDATE(0)`. Multi-park cases.
5. **Slim listener placeholder.** Replace the deleted
   `Http2StreamRequestContextListener` with a tiny logging no-
   op listener inside `HttpConnectionContext` that just
   records callbacks. M2 replaces this with the Flight SQL
   listener proper.

### 15.6 Pre-pivot build order (historical)

Two phases, with Phase A and Phase B steps 1–2 runnable in
parallel. Phase B steps 3 onwards depend on Phase A landing.
Each step lands with its unit tests and is independently
reviewable.

#### Phase A — Response core in `Http2ConnectionContext`

Pure engine work. Socket-free, driven by native `(addr, limit)`
buffers per `Http2ConnectionContext`'s existing contract (§4.2).
Tests live under `core/src/test/java/io/questdb/test/cutlass/http2/`
and drive the engine directly without touching the HTTP
integration layer. The reason this phase goes first: without the
response-side primitives, the integration layer would have to
fake them or bake ownership / backpressure decisions into the
adapter, where they don't belong.

**A.1 — Outbound state on `Http2Stream`.** Extend
`Http2Stream.java` (385 LOC today) with the per-stream outbound
queue, the copy-on-enqueue arena (native, sized to the per-stream
outbound tuple-queue cap — §16 Q7), per-stream-cap accounting,
and the END_STREAM-carrying flag on the final tuple. `Http2StreamPool`
teardown frees the arena when a slot moves to TOMBSTONE.

**A.2 — `enqueueData` with copy-on-enqueue ownership.** Add the
engine API shape from §4.4: `enqueueData(streamId, generation,
payloadAddr, payloadLen, endStream)` copies bytes into the
stream's outbound arena, rejects on generation mismatch or
stream-closed, rejects when the per-stream cap is exceeded
(returns the sentinel that tells the caller to park).

**A.3 — `emitResponseHeaders` with the shared `HpackEncoder`.**
Add the `Http2HeadersWriter` callback interface and the
`emitResponseHeaders(streamId, generation, writer, endStream)`
method. Encoder serialisation per §10 rule 1; copied encoded
block handed to the outbound scheduler on `streamId`.

**A.4 — Round-robin outbound scheduler in `writePending`.**
Extend `writePending` to drain the control-frame queue first,
then walk ready streams in round-robin order, emitting DATA
frames up to `peerMaxFrameSize` and up to available stream +
connection windows, stopping when the send buffer fills or no
stream is eligible. Connection-scoped "next stream" cursor per
`STREAM_STATE_MACHINE.md` §7.

**A.5 — `Http2StreamListener.onStreamWritable(streamId)`.**
New listener method. Fires after any event that grows a parked
stream's outbound-eligible window below the per-stream queue
cap — `WINDOW_UPDATE` on the stream id, `WINDOW_UPDATE(0)` on
the connection, and `SETTINGS_INITIAL_WINDOW_SIZE` increases
(`Http2ConnectionContext.java:504-537`). Decide synchronously-
during-`processReceivedBytes` vs. end-of-tick batching (§16
Q6); pick synchronous for simplicity unless profiling says
otherwise.

**A.6 — Response-side END_STREAM transitions.** Wire
`emitResponseHeaders(..., endStream=true)` and DATA-with-
END_STREAM through the state-machine transitions per
`STREAM_STATE_MACHINE.md` §5. Ensure `onStreamClosed` fires on
the listener after the frame flushes.

**A.7 — Focused tests.** Land alongside the implementation
PRs. The set:

- HEADERS-only response (status + empty body, END_STREAM on
  HEADERS).
- HEADERS + DATA + END_STREAM (happy path).
- DATA payload split across multiple DATA frames by peer
  `MAX_FRAME_SIZE`.
- Zero stream outbound window → DATA queued, no emission;
  `WINDOW_UPDATE` unparks and DATA flows.
- Zero connection outbound window with stream window > 0 →
  DATA queued; `WINDOW_UPDATE(0)` unparks.
- `SETTINGS_INITIAL_WINDOW_SIZE` increase unparks streams
  blocked only on per-stream window.
- Per-stream cap overrun on `enqueueData` returns the park
  sentinel and does not accept more bytes.
- `enqueueData` / `onBytesConsumed` / `emitResponseHeaders`
  with stale generation reject the call silently (per §7 step
  6 of the state-machine doc).
- Peer `RST_STREAM(CANCEL)` lands while HEADERS / DATA are
  queued for that stream → scheduler drops queued tuples, the
  per-stream arena is freed, no further frames emit on that
  stream. **This test catches ownership bugs at the cheapest
  point.**
- Round-robin fairness: two streams with queued DATA, both
  under their windows, alternate DATA emissions tick by tick.

#### Phase B — Integration

**B.1 — `HttpRequestContext` interface extraction.** Parallel
with Phase A. Pure mechanical — extracts the interface from
`HttpConnectionContext`; no processor signature changes yet.
Tests unchanged. (§7 refactor plan step 1.)

**B.2 — Surface changes + processor retype.** Parallel with
Phase A. Per §7 refactor plan step 2 — **not** a pure
import-fix pass. Bundle: `SimpleResponse` interface extraction,
`HttpCookieHandler.processServiceAccountCookie` first-arg
retype, `HttpRequestProcessor` parameter retype across every
implementation, and processor-state-class retypes (e.g.
`JsonQueryProcessorState.httpConnectionContext`). Classify
each processor as H2-eligible vs. H1-only and leave H1-only
processors importing the concrete context. Grep before
starting to size the change.

**B.3 — `protocolMode` + sniff path.** Depends on nothing;
could land any time after B.1. Adds the mode bit, the
peek-scratch allocation in `doInit()`, `sniffAndSelectMode`,
and the `MODE_H1` / `MODE_H2_PREFACE_PENDING` branches at the
top of `handleClientOperation`.
`MODE_H2_PREFACE_PENDING` initially short-circuits to
`registerDispatcherDisconnect` with a not-implemented reason.
Validates that the sniff doesn't regress H1 under the full
HTTP test suite.

**B.4 — Lazy H2 engine + preface drain + handleH2Operation
skeleton.** Depends on Phase A (the engine must already have
the response-emit surface) and B.3 (the mode bit). On entry
to `MODE_H2_PREFACE_PENDING`, run `drainH2Preface` (§6.2),
allocate `Http2ConnectionContext`, H2 send buffer, and emit
the initial SETTINGS; transition to `MODE_H2`.
`handleH2Operation` drains recv → `processReceivedBytes` →
`writePending` → `socket.send` with a no-op
`Http2StreamListener` that just logs callbacks.
`curl --http2-prior-knowledge` completes the preface exchange
cleanly; no actual responses yet.

**B.5 — Adapter pool + `Http2RequestHeader` population.**
Depends on B.4 (the engine is actually driving the listener
now). Adapter construction / teardown tested in isolation via
a mock `Http2StreamListener` feeding the adapter synthetic
events. Exercises the copy-on-bind path (§8) and the
per-stream `LocalValueMap` release-on-close semantics (§11).

**B.6 — `Http2ResponseSink` + `Http2ChunkedResponse` sending
a single HEADERS + DATA frame.** Depends on B.5 + Phase A.
Uses `emitResponseHeaders` and `enqueueData` directly — the
engine already has them tested. No flow-control backpressure
path yet — the M1 response bodies fit in one frame.

**B.7 — Wire to `HealthCheckProcessor`** (or a hello-world
stub). Depends on B.2 + B.6. `curl --http2-prior-knowledge`
smoke returns 200 OK. Avoid `StaticContentProcessor` until
the raw-socket DATA-frame adapter lands (§16 Q1).

**B.8 — Park / resume.** Depends on B.7. Wire the response
sink's `PeerIsSlowToReadException` path through the
parked-streams set on `HttpConnectionContext`, driven by
`onStreamWritable` from Phase A.5. Re-test with a response
large enough to drain the initial window.

## 16. Open Questions

1. **Raw-socket processor support.** Does any M1 / M2 processor
   depend on `HttpRawSocket` access? If yes, build the
   DATA-frame-wrapping adapter (§10); if no, return `null` from
   `Http2StreamRequestContext.getRawResponseSocket()` and
   document raw-socket processors as HTTP/1.x-only until M3.
   Grep the processor tree to close this.
2. **Trailers for existing processors.** HTTP/1.x processors
   never see trailers. Is the M2 trailer hook worth the
   interface churn, or is the gRPC-only pathway the right home
   for it? Defer pending the Stage 3 design.
3. **Per-connection auth cache.** Measure whether re-running
   the authenticator per stream is a meaningful overhead under
   the expected H2 workload (browser-driven query bursts, gRPC
   long-lived channels). If yes, add the cache via a
   `LocalValue` on the outer `HttpConnectionContext.getMap()`
   (§11); if no, skip.
4. **`onConnectionClosed` rename.** The processor hook
   `onConnectionClosed(ctx)` fires on stream close under H2,
   not connection close. Rename to `onRequestClosed(ctx)` in
   the refactor PR, or keep the historical name and document
   the semantics. Minor but touches every processor.
5. **Dispatcher wake-up for pending-ops queue.** §5 specifies
   that foreign threads (e.g. a `TextImportProcessor` worker
   finishing deferred body consumption) push entries onto a
   per-connection MPSC pending-ops queue and signal the
   `IODispatcher` to wake the connection. The exact wake-up
   primitive depends on what QuestDB's dispatcher exposes —
   `eventfd`-style wake, re-queue via `heartbeat`, or a
   dedicated signal channel. No such primitive exists on
   `IODispatcher` today; adding one is a prerequisite for M2
   (M1 can get away without it since the hello-world fixture
   is fully synchronous).
6. **`Http2StreamListener.onStreamWritable(streamId)`.** §10
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
7. **Per-stream outbound tuple-queue cap.** §10 references a
   "configurable maximum, e.g. 256 KiB worth of queued tuple
   payload" as the threshold above which
   `PeerIsSlowToReadException` fires. Pick the number after
   measuring realistic response-size distributions against
   `INITIAL_WINDOW_SIZE`. Too low and long-poll / streaming
   responses bounce into park/resume under moderate load; too
   high and a single stalled peer can tie up unbounded memory.
8. **Pending-ops queue bound and overflow policy.** §5 proposes
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
9. **Lazy H1 buffer allocation on H2-only connections.** When
   `preAllocateBuffers = true` (`HttpConnectionContext.java:169`),
   the H1 `recvBuffer`, `responseSink` send buffer,
   `headerParser` etc. are allocated in the constructor and
   sit unused on H2-only connections. For de/cployments that run
   H2 heavily (Flight SQL, gRPC), either flip the default off
   or split pre-allocation by mode. M1 defers; M2 worth
   measuring.
