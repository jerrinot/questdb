# Arrow Flight SQL — High-Level Design

A map of the subsystems required to expose QuestDB query results over Arrow
Flight SQL, implemented in pure Java in QuestDB's zero-dependency, zero-GC
idiom. This document scopes the **big blocks**; each block will have its own
detailed design doc before implementation starts.

**Pivot note (2026-04-20):** the HTTP/2 integration layer was re-scoped to
serve Flight SQL only — see `HTTP2_INTEGRATION.md` §1. Earlier revisions of
that doc had planned to run existing `HttpRequestProcessor` implementations
over H2 (HealthCheck, JsonQuery, TextImport, ...) so that Flight SQL would
sit on top of a general-purpose H2 server. That goal is retired. Flight SQL
binds directly to `Http2StreamListener` +
`Http2ConnectionContext.{emitResponseHeaders,enqueueData,emitTrailers}`; no
`HttpRequestProcessor` adapter layer sits between them. The protocol stack
is still Flight SQL-agnostic at the engine level (any future gRPC service
could substitute a different listener), but the H2 integration work is sized
for exactly what gRPC needs and nothing more.

Companion documents:
- [`HTTP2_INTEGRATION.md`](HTTP2_INTEGRATION.md) — H2 protocol-mode switch
  on the existing HTTP listener, pseudo-header capture, response emit
  surface, park / resume, trailers.
- [`HTTP2_FRAME_CODEC.md`](HTTP2_FRAME_CODEC.md) — frame-level codec.
- [`HPACK_CODEC.md`](HPACK_CODEC.md) — header compression.
- [`STREAM_STATE_MACHINE.md`](STREAM_STATE_MACHINE.md) — per-stream FSM
  and flow control.

## Table of Contents

1. [Goal](#1-goal)
2. [Non-Goals](#2-non-goals)
3. [Transport Security](#3-transport-security)
4. [Stack](#4-stack)
5. [The Big Blocks](#5-the-big-blocks)
6. [Dependency Graph and Build Order](#6-dependency-graph-and-build-order)
7. [What We Reuse from QWP Egress](#7-what-we-reuse-from-qwp-egress)
8. [Testing Strategy at a Glance](#8-testing-strategy-at-a-glance)
9. [Open Questions](#9-open-questions)

---

## 1. Goal

Serve Apache Arrow Flight SQL from QuestDB so that off-the-shelf clients — the
Flight SQL JDBC driver, the ADBC Flight SQL driver (Python, Go, C/C++),
BI tools that speak these drivers (Tableau, DBeaver, Dremio) — connect to
QuestDB natively with columnar throughput and without a translation layer.

The scope of "Flight SQL" for this project is **read-heavy**: SQL in, columnar
result batches out. Write paths (`CommandStatementUpdate`,
`CommandStatementIngest`) are included because Flight SQL clients expect them,
but the performance focus is on `DoGet`.

## 2. Non-Goals

- **No Netty, no grpc-java, no protobuf codegen, no Flatbuffers codegen, no
  `org.apache.arrow:*`**. Every byte on the wire is produced by code in this
  repository. Test-only dependencies are fine (Netty's HTTP/2 codec is already
  proposed as a test oracle in the HTTP/2 doc).
- **No separate network listener for Flight SQL.** HTTP/2 is multiplexed onto
  the existing HTTP endpoint via preface detection (h2c) or ALPN (h2). One
  port, one I/O stack.
- **No server push, no DoExchange, no Substrait, no federation across nodes.**
  A single-node Flight SQL server returns one `FlightEndpoint` pointing at
  itself.
- **No transactional semantics** in the first milestone. Autocommit only.
  Flight SQL's transaction actions can be stubbed with `UNIMPLEMENTED`.

## 3. Transport Security

**OSS posture: plaintext-first.** Open-source QuestDB does not currently
terminate TLS on any listener, so the first milestone exposes Flight SQL over
HTTP/2 cleartext (h2c) only. The frame codec's preface-sniffing path
(`HTTP2_FRAME_CODEC.md` §7) handles routing on the existing plain HTTP port;
no ALPN is required.

**gRPC / Flight SQL over plaintext is the mainstream configuration**, not a
workaround. Every major client supports it explicitly:

| Client                         | Plaintext connection string                                |
|--------------------------------|------------------------------------------------------------|
| Flight SQL JDBC driver         | `jdbc:arrow-flight-sql://host:9000?useEncryption=false`    |
| ADBC Flight SQL (Python)       | `adbc_driver_flightsql.connect({"uri": "grpc://host:9000"})` |
| ADBC Flight SQL (Go, C++)      | `Location::ForGrpcTcp("host", 9000)`                       |
| `grpcurl`                      | `grpcurl -plaintext host:9000 list`                        |

Precedent: InfluxDB 3.0's self-hosted Flight SQL endpoint defaults to
plaintext, Dremio's dev docs do the same, and every Apache Arrow Flight SQL
example is plaintext. Clients default to TLS, so the key piece of user-facing
documentation is **"pass the plaintext flag"** — without it, tools report
connection failures that look like server bugs.

**Extension point for TLS.** QuestDB Enterprise does support TLS on the HTTP
listener, so the design must leave a clean hook even though the OSS milestone
doesn't exercise it:

- The TLS termination layer is already separate from `HttpConnectionContext`
  — the HTTP/2 codec receives decrypted bytes either way.
- ALPN negotiation at the TLS handshake selects `h2` vs `http/1.1`; the
  selected protocol is passed to the context factory, which picks
  `Http2ConnectionContext` or the HTTP/1.1 path. No preface sniffing needed
  on TLS connections.
- `FlightInfo.Location` returned by `GetFlightInfo` uses
  `arrow-flight-reuse-connection://` — the client reuses the current gRPC
  channel whether it's plaintext or TLS. This keeps the server out of the
  business of advertising its own external hostname / scheme, and avoids a
  configuration pitfall where a TLS-terminated reverse proxy fronts a
  plaintext QuestDB.

Full TLS / ALPN wiring is deferred. Only the ALPN callback contract needs to
be defined now so the Enterprise layer can plug in without restructuring.

## 4. Stack

```
 +--------------------------------------------------------+
 |                Flight SQL command handlers             |   "business logic"
 |   CommandStatementQuery / CommandPreparedStatement /   |
 |   CommandGet* metadata / Statement Update / Ingest     |
 +--------------------------------------------------------+
 |  Flight RPC dispatcher  (GetFlightInfo, DoGet,         |
 |  DoPut, GetSchema, DoAction, Handshake)                |
 +--------------------------------------------------------+
 |  Arrow IPC encoder  (Flatbuffers Schema / RecordBatch  |
 |  / DictionaryBatch + encapsulated-message framing)     |
 +--------------------------------------------------------+
 |  Arrow column emitters  (null bitmap, offsets, buffers,|
 |  dictionary encoding, type mapping)                    |
 +--------------------------------------------------------+
 |  Protobuf codec  (hand-rolled, fixed Flight + Flight   |
 |  SQL message schema)                                   |
 +--------------------------------------------------------+
 |  gRPC framing  (length-prefix, trailers-only status,   |
 |  content-type, deadlines)                              |
 +--------------------------------------------------------+
 |  HTTP/2  (frame codec + HPACK + stream state machine + |
 |  flow control)  -- see HTTP2_FRAME_CODEC.md            |
 +--------------------------------------------------------+
 |  TLS + ALPN routing  (existing QuestDB TLS stack,      |
 |  extended to negotiate `h2` vs `http/1.1`)             |
 +--------------------------------------------------------+
 |  TCP / IODispatcher  (existing; unchanged)             |
 +--------------------------------------------------------+
```

Horizontal cut between "protocol" (everything below the dispatcher) and
"business logic" (everything above). The protocol stack is Flight SQL-agnostic
— it's a generic HTTP/2 + gRPC server that happens to serve Flight SQL as its
first tenant. Future uses (gRPC-Web, other gRPC services) sit alongside the
Flight SQL dispatcher without re-implementing the protocol stack.

## 5. The Big Blocks

### 5.1 HTTP/2 transport

**Scope.** Frame codec, HPACK, stream state machine, connection-level and
stream-level flow control, preface detection on the HTTP listener,
pseudo-header capture, response emit surface (HEADERS + DATA + trailers),
park / resume on flow control, ALPN hook in the TLS handshake.

**Module.** `io.questdb.cutlass.http2.*`

**Status.** Landed as of commit `428062d08b`:

- Frame codec (`HTTP2_FRAME_CODEC.md`) — full frame surface.
- HPACK codec (`HPACK_CODEC.md`) — static + dynamic table + Huffman, with
  encoder snapshot / restore for PARK rollback.
- Stream state machine (`STREAM_STATE_MACHINE.md`) — 7-state FSM with
  `END_STREAM` / `RST_STREAM` transitions.
- Per-connection + per-stream flow control with SETTINGS-driven initial
  values and round-robin scheduler across ready streams.
- Preface sniff on the existing HTTP listen socket
  (`HttpConnectionContext.protocolMode`), lazy H2 engine allocation,
  preface drain, all gated by `http.h2.enabled` (default off).
- Response emit: `emitResponseHeaders`, `enqueueData`, `emitTrailers`,
  with per-stream outbound arena (copy-on-enqueue) and tuple ring.
- Pseudo-header capture for `:method`, `:scheme`, `:path`, `:authority`,
  and `content-type` into per-stream staging buffers — exactly the set
  a gRPC router needs. Overflow → `PROTOCOL_ERROR`. Full §11 validator
  (ordering, forbidden-headers, content-length reconciliation) deferred
  to the gRPC framing layer where the error path is trailers-in-HEADERS.
- Park / resume end-to-end: listener gets `onStreamWritable` on
  `WINDOW_UPDATE`, `SETTINGS_INITIAL_WINDOW_SIZE` increase, and tuple-
  ring drain. Tested with tiny initial windows and multi-park cycles.
- Slim placeholder `Http2StreamListener` inside
  `HttpConnectionContext` logs callbacks and never emits — the Flight
  SQL listener substitutes this in §5.6.

**Remaining.** TLS + ALPN (§5.14). Full §11 validator subset required
for gRPC correctness moves into §5.2 alongside `content-type`
enforcement.

**Size estimate.** ~8.5k LOC landed (framing, HPACK, state machine,
integration, tests). Remaining: ~300 LOC for TLS + ALPN wiring.

### 5.2 gRPC framing

**Scope.** The thin layer gRPC places on top of HTTP/2:

- Method dispatch on `:path` header
  (`/arrow.flight.protocol.FlightService/DoGet`).
- Content-type negotiation (`application/grpc`,
  `application/grpc+proto`). Non-conforming requests get trailers-only
  `grpc-status: UNIMPLEMENTED`.
- Request validation subset: reject non-POST, reject uppercase header
  names, reject requests missing required pseudo-headers. The full §11
  HTTP/2 validator (forbidden-headers, content-length reconciliation,
  TE restrictions, connection ban) is not implemented — gRPC clients are
  a closed set and won't send any of those, and the error path for a bad
  request under gRPC is an HTTP 200 with trailers, not an HTTP-level
  error code.
- 5-byte message prefix: 1-byte compressed flag + 4-byte big-endian
  length. Message reassembly across DATA frame boundaries.
- Trailers-only status (`grpc-status`, `grpc-message`) — responses that
  error early use trailers on the empty HEADERS frame; normal responses
  write trailers after the last DATA frame.
- `grpc-timeout` header → per-request deadline, plumbed through to the
  cursor cancellation path.
- gRPC status codes (`OK`, `CANCELLED`, `UNAVAILABLE`,
  `INVALID_ARGUMENT`, `UNIMPLEMENTED`, `INTERNAL`, `PERMISSION_DENIED`,
  ...).

**Module.** `io.questdb.cutlass.grpc.*`

**Binding.** This layer is a subclass or composition of
`Http2StreamListener` rather than an `HttpRequestProcessor`. It reads
pseudo-headers via `Http2RequestHeadersView`, consumes request DATA
directly from the engine's listener callback, and emits response HEADERS
/ DATA / trailers via `Http2ConnectionContext.emitResponseHeaders` /
`enqueueData` / `emitTrailers`. No bridge to the existing
`HttpRequestProcessor` surface — that's exactly the H1-mimicry layer the
H2 integration pivot retired.

**Size estimate.** ~1k LOC. Thin layer — mostly a `switch` on `:path`,
header reads, and the message-prefix reader. The state shows up in how
it dovetails with HTTP/2 streams (trailers-only requires branching on
whether any DATA has been sent; the engine's outbound arena already
knows).

**Status.** Wave 5 shipped a narrow subset of gRPC framing scoped to
the `Handshake` RPC: `GrpcStatus` constants, `GrpcFrameReader`
(reassembler with compressed-flag + oversize rejection),
`GrpcFrameWriter.writePrefix`, and `GrpcTrailerWriter` (percent-encoded
`grpc-message`). The Flight SQL dispatcher at
`io.questdb.cutlass.flightsql.server.FlightSqlDispatchListener` routes
`:path` exactly, with one live route
(`/arrow.flight.protocol.FlightService/Handshake`) and trailers-only
rejection for non-POST, non-`application/grpc*` content-types, and
unknown paths. Per-message compression, `grpc-timeout`, `grpc-encoding`,
`te: trailers`, and bidirectional streaming land in later waves.

**Non-obvious concerns.**

- **Compression is per-message**, not per-stream. The 1-byte flag on
  each gRPC message indicates compression. Flight IPC bodies have their
  own separate compression negotiated via IPC options — they do not use
  gRPC's.
- **Error responses that precede any DATA frame** write trailers on the
  initial HEADERS frame with `END_STREAM` set; the client never sees an
  empty DATA frame. The gRPC layer must know the difference — ask the
  engine "has any DATA been enqueued yet?" before picking the error
  path. `Http2Stream` exposes this via its outbound queue.

### 5.3 Protobuf codec

**Scope.** A hand-written protobuf codec for the ~30 Flight + Flight SQL
message types. The `.proto` schemas are stable and public:
[`Flight.proto`](https://github.com/apache/arrow/blob/main/format/Flight.proto),
[`FlightSql.proto`](https://github.com/apache/arrow/blob/main/format/FlightSql.proto).

**Module.** `io.questdb.cutlass.protobuf.*`

**What it is.** A per-message hand-coded encoder and decoder using tag-length-
value + varint encoding. Zero reflection, zero code generation, zero message
objects on the hot path (decode into caller-provided primitive fields; encode
from caller-provided values). Field tag constants are hand-transcribed from
the `.proto` files once.

**What it is not.** Not a general-purpose protobuf implementation. It handles
the exact wire subset Flight SQL uses: `int32`, `int64`, `bool`, `string`,
`bytes`, `repeated` of the same, `Any` (which is itself just a
`{type_url, value}` pair), and nested messages.

**Size estimate.** ~2k LOC. Roughly 60-100 LOC per message × ~30 messages
(~half request, half response). One shared varint + length-delimited helper.

**Non-obvious concerns.**

- **`google.protobuf.Any` wrapping.** Every Flight SQL command is packed into
  `FlightDescriptor.cmd` as an `Any`. Dispatch reads the `type_url` string to
  pick the concrete message decoder.
- **`CommandGetSqlInfo` results** encode as a dense variant union inside a
  `Union` column in the Arrow response. The protobuf side is just the request
  shape.

### 5.4 Arrow column emitters

**Scope.** Produce per-column Arrow buffers from a QuestDB `Record` or
`PageFrame`. This is the same transposer shape as `QwpResultBatchBuffer`, with
a different output layout.

**Module.** `io.questdb.cutlass.arrow.column.*`

**Outputs per column.** Arrow's columnar layout:

- **Validity bitmap** (1 bit per row, LSB-first, 1 = non-null). Materialised
  from QuestDB's sentinel-value null convention (`Int.MIN_VALUE`,
  `Long.MIN_VALUE`, NaN, `Numbers.IPv4_NULL`, etc.).
- **Fixed-width types.** Dense array of primitives; byte representation is
  identical to QuestDB's native storage for BYTE/SHORT/INT/LONG/FLOAT/DOUBLE/
  DATE/TIMESTAMP.
- **Variable-length (STRING, VARCHAR, BINARY).** `(N+1) × int32` offsets +
  UTF-8 / opaque byte heap.
- **Dictionary-encoded (SYMBOL).** Dictionary buffer (offsets + UTF-8) +
  per-row int32 indices. Natural fit for QuestDB's symbol table; the symbol
  key exposed by `SymbolTable` maps directly to the dictionary index.
- **Fixed-size list (arrays).** `FixedSizeList` when dimensions are
  statically known; `List<...>` recursively when not.

**Size estimate.** ~3k LOC, mostly analogous to `QwpColumnScratch` +
`QwpResultBatchBuffer`. The per-column-type switch is ~20 cases, same as QWP
egress. Reuse the native scratch-buffer pattern.

**Page-frame fast path.** When the factory produces columnar page frames
(table scans, simple filters), fixed-width columns can be emitted with a
single `Unsafe.copyMemory` from the page into the Arrow buffer — zero
transposition. Null-bitmap materialisation is the only non-trivial work on
this path; this is where SIMD in Rust / C could later win on very wide
tables. Measure first.

### 5.5 Arrow IPC encoder

**Scope.** Wrap Arrow column buffers into the Arrow IPC message format:

- **Schema message** (once per stream on the first `DoGet` batch).
- **DictionaryBatch message** (when a column uses dictionary encoding).
- **RecordBatch message** per result batch.
- **Encapsulated-message framing**: 4-byte continuation marker `0xFFFFFFFF`,
  4-byte metadata length (little-endian), Flatbuffers metadata, padding to
  8-byte alignment, body.

**Module.** `io.questdb.cutlass.arrow.ipc.*`

**Flatbuffers encoding.** Hand-rolled for the exact Arrow format. The
[Arrow `Schema.fbs`, `Message.fbs`](https://github.com/apache/arrow/tree/main/format)
files define the types; Flatbuffers wire format is well-specified and simpler
than protobuf. Only a small subset of the Flatbuffers format is needed
(objects + vectors + enums + inline scalars + strings); tables with offsets
are straight-line code. ~2k LOC for the full Arrow message surface.

**Non-obvious concerns.**

- **Flatbuffers is write-backwards-then-patch** (the root table offset points
  at data written earlier). A single reusable native buffer + offset cursor
  mirrors QuestDB's existing frame-writer idiom.
- **Dictionary batches precede the RecordBatch** that first references them.
  Dictionary deltas (incremental updates) are an Arrow 4.0+ feature; Flight SQL
  clients generally handle them but some drivers prefer replacement
  dictionaries. Start with replacement; consider deltas as an optimisation.
- **Body buffer padding to 8-byte alignment** is mandatory. Arrow readers
  assert on it.

### 5.6 Flight RPC dispatcher

**Scope.** Route incoming Flight service method calls to Flight SQL command
handlers.

**Module.** `io.questdb.cutlass.flightsql.server.*`

**Binding.** The dispatcher is the concrete `Http2StreamListener` that
replaces the `NoopH2Listener` placeholder currently inside
`HttpConnectionContext`. On `MODE_H2` allocation, `HttpConnectionContext`
instantiates the Flight SQL listener (when `flight.sql.enabled=true`)
and passes it to `Http2ConnectionContext`'s constructor. One listener
instance per TCP connection; per-stream state lives in the dispatcher's
own stream table.

**RPC surface required for a usable server.**

| RPC               | Purpose                                      | Required? |
|-------------------|----------------------------------------------|-----------|
| `Handshake`       | Auth token mint (bidirectional stream).      | If auth is enabled. |
| `GetFlightInfo`   | SQL query / metadata command entry point.    | Yes |
| `GetSchema`       | Schema-only response for JDBC prepare.       | Yes |
| `DoGet`           | Server-streamed Arrow result batches.        | Yes |
| `DoPut`           | Client-streamed ingest + prepared binds.     | Yes |
| `DoAction`        | Prepared statement lifecycle, cancel.        | Yes |
| `ListFlights`     | List open flights.                           | Optional (stub empty). |
| `ListActions`     | Advertise supported actions.                 | Optional (static list). |
| `PollFlightInfo`  | Progress polling for long queries.           | Optional, later. |
| `DoExchange`      | Bidirectional stream for compute offload.    | Skip. |

~500 LOC for the dispatcher itself (it's a `switch` on `:path`); the work is
in the handlers it calls.

**Ticket format.** The `Ticket` returned in `GetFlightInfo.endpoints[0]` is an
opaque server blob. We pack: `{query_id:int64, schema_id:int32, expiry:int64}`,
sign with an HMAC so clients can't forge one, and keep the compiled factory +
open cursor in a per-connection registry keyed by `query_id`. `DoGet`
retrieves the entry and streams.

Tickets must survive across a `GetFlightInfo` → `DoGet` pair. In the
single-endpoint single-node case both RPCs ride the same gRPC connection, so
the per-connection registry is sufficient.

### 5.7 Flight SQL command handlers

**Scope.** The handlers behind the dispatcher. Each command is a concrete
handler class; `GetFlightInfo` picks the handler based on the `Any`-wrapped
command type in the `FlightDescriptor.cmd`.

**Module.** `io.questdb.cutlass.flightsql.commands.*`

**Commands to implement** (in rough order of ADBC client importance):

1. **`CommandGetSqlInfo`** — the single most important metadata command.
   ADBC/JDBC clients probe this on every connect to learn capability bits
   (SQL dialect, keywords, quoting, max lengths, ...). A thin implementation
   makes QuestDB look broken in every tool. Produce a complete `SqlInfo`
   response modeled on Dremio's and InfluxDB 3's.
2. **`CommandStatementQuery`** — the `SELECT` path. Compile → factory →
   `DoGet` stream.
3. **`CommandGetTables`, `CommandGetTableTypes`, `CommandGetCatalogs`,
   `CommandGetDbSchemas`** — schema browser metadata. Straight queries
   against `information_schema` (or the QuestDB equivalent) returning fixed
   Arrow schemas.
4. **`CommandGetPrimaryKeys`, `CommandGetImportedKeys`,
   `CommandGetExportedKeys`, `CommandGetCrossReference`** — FK metadata.
   Return empty rowsets; QuestDB has no FKs. Fixed Arrow schemas still
   required.
5. **`CommandGetXdbcTypeInfo`** — JDBC type catalog. Static table keyed to
   QuestDB's type system.
6. **`CommandPreparedStatementQuery` + `ActionCreate/Close
   PreparedStatementRequest`** — prepared statements. Opaque handle registry
   (below).
7. **`CommandStatementUpdate`** — non-`SELECT` via `DoPut`. Returns row count.
8. **`CommandStatementIngest`** — bulk ingest via `DoPut`. Later milestone.

**Size estimate.** `CommandGetSqlInfo` alone is ~500 LOC (it's a large static
table, tediously). Other commands are ~100-300 LOC each. ~4-5k LOC total
for the full command surface.

### 5.8 Prepared statement registry

**Scope.** Issue, look up, and expire opaque prepared-statement handles.

**Module.** `io.questdb.cutlass.flightsql.prepared.*`

**Behaviour.**

- `ActionCreatePreparedStatementRequest` compiles the SQL, captures the
  resulting schema, stores the compiled factory keyed by a new handle, and
  returns the handle + schemas (request schema for binds, result schema for
  output).
- `CommandPreparedStatementQuery` + `DoPut` stream receives parameter
  bindings, binds them to the factory's bind-variable service, and returns a
  fresh handle if the bind types narrow the signature (Flight SQL convention).
- `CommandPreparedStatementQuery` on `GetFlightInfo` produces a ticket for
  `DoGet` to stream results.
- `ActionClosePreparedStatementRequest` releases the factory and frees the
  handle.
- Handles have a TTL; a janitor thread reclaims orphaned ones.
- Per-connection handle cap prevents misbehaved clients from exhausting
  memory.

**Size estimate.** ~500 LOC.

### 5.9 Cursor streaming driver

**Scope.** Drive a `RecordCursorFactory` to completion, producing Arrow
batches on the wire, with HTTP/2-aware flow control.

**Module.** `io.questdb.cutlass.flightsql.server.FlightSqlStreamer`

**Behaviour.** Almost a direct port of `QwpEgressProcessorState`:

- Two cursor paths: `PageFrameCursor` fast path (when
  `factory.supportsPageFrameCursor()`), `RecordCursor` fallback for joins /
  aggregates / projections.
- Batch size capped at `MAX_ROWS_PER_BATCH` (tune for Arrow; likely larger
  than QWP's 4096).
- Per-batch loop: start a RecordBatch, transpose N rows, emit, send as gRPC
  DATA message inside one or more HTTP/2 DATA frames.
- When the stream's send window is exhausted, throw
  `PeerIsSlowToReadException`, park the connection, resume on
  `WINDOW_UPDATE`. Same idiom QWP egress already uses.
- On cursor exhaustion, write gRPC trailers (`grpc-status: 0`) with
  `END_STREAM` and release cursor resources.

**Size estimate.** ~1k LOC including page-frame wiring. Significant reuse from
`QwpEgressProcessorState`; the differences are the emit layer and the flow
control coupling.

### 5.10 Type mapping

**Scope.** The full QuestDB → Arrow mapping table, with null conventions.

**Module.** `io.questdb.cutlass.arrow.type.*`

| QuestDB type        | Arrow type                          | Notes                                            |
|---------------------|-------------------------------------|--------------------------------------------------|
| BOOLEAN             | `Bool`                              | Bit-packed; Arrow's layout matches.              |
| BYTE                | `Int8`                              |                                                  |
| SHORT               | `Int16`                             | NULL sentinel `Short.MIN_VALUE`.                 |
| CHAR                | `UInt16`                            | No direct Arrow `Char`.                          |
| INT                 | `Int32`                             | NULL sentinel `Int.MIN_VALUE`.                   |
| LONG                | `Int64`                             | NULL sentinel `Long.MIN_VALUE`.                  |
| FLOAT               | `Float32`                           | NaN-as-NULL (same as QWP).                       |
| DOUBLE              | `Float64`                           | NaN-as-NULL.                                     |
| DATE                | `Date64`                            |                                                  |
| TIMESTAMP           | `Timestamp(MICROSECOND, "UTC")`     | QuestDB stores micros UTC.                       |
| TIMESTAMP_NANOS     | `Timestamp(NANOSECOND, "UTC")`      |                                                  |
| STRING              | `Utf8`                              | `int32` offsets.                                 |
| VARCHAR             | `Utf8`                              | Near-identical aux+data layout; minor repack.    |
| SYMBOL              | `Dictionary<Int32, Utf8>`           | `SymbolTable` supplies the dictionary.           |
| IPv4                | `Extension<Ipv4, UInt32>`           | Extension type over `UInt32`.                    |
| UUID                | `Extension<UUID, FixedSizeBinary[16]>` | Canonical Arrow extension.                    |
| LONG256             | `Extension<Long256, FixedSizeBinary[32]>` |                                             |
| GEOHASH             | `Extension<GeoHash, *>`             | Underlying type follows precision bits.          |
| DECIMAL64           | `Decimal128(precision, scale)`      | Widen; Arrow has no Decimal64.                   |
| DECIMAL128          | `Decimal128(precision, scale)`      |                                                  |
| DECIMAL256          | `Decimal256(precision, scale)`      |                                                  |
| BINARY              | `Binary`                            | `int32` offsets.                                 |
| DOUBLE[], LONG[]    | `FixedSizeList` or `List<Float64/Int64>` | Dimensionality encoded in type.             |

Extension types are declared via Arrow `Field.metadata`:
`{"ARROW:extension:name": "questdb.geohash", "ARROW:extension:metadata": "<precision>"}`.
Clients that don't know the extension fall through to the base type.

**Size estimate.** ~500 LOC for the mapping + extension type registry.

### 5.11 Auth

**Scope.** Plug Flight SQL auth into QuestDB's existing security context.

**Module.** `io.questdb.cutlass.flightsql.auth.*`

**Flow.**

- `Handshake` RPC exchanges credentials (basic-auth header or bearer) and
  mints a token returned in an `authorization: bearer <token>` header on
  subsequent calls.
- Per-call authenticator validates the bearer token against QuestDB's
  existing security context (same one QWP egress consumes via
  `SqlExecutionContextImpl`).
- On failure, respond with gRPC status `UNAUTHENTICATED`.

**Size estimate.** ~500 LOC. Straightforward once the security context is
threaded through.

### 5.12 Cancellation

**Scope.** End-to-end cancellation: client-side gRPC cancel,
`CancelFlightInfo` action, server-side deadline expiry — all converge on the
same "trip the active cursor" path.

**Module.** spans `grpc`, `flightsql.server`, and `flightsql.prepared`.

**Flow.** gRPC cancellation surfaces as an `RST_STREAM(CANCEL)` on HTTP/2.
The stream state machine sees it, looks up the associated cursor, sets a
circuit-breaker flag. The cursor (or the factory) periodically checks the
flag and throws. The HTTP/2 stream was already closed by the client; the
server just needs to release resources.

This is a gap in QWP egress (see `QWP_EGRESS_PHASE2_BACKLOG.md` item #2).
The cancellation machinery built here is shared with QWP egress: both use
`SqlExecutionContextImpl`'s existing circuit-breaker hooks.

**Size estimate.** ~500 LOC net-new; benefits QWP egress for free.

### 5.13 Compression (optional, later)

**Scope.** Arrow IPC body compression (LZ4 Frame, Zstd), negotiated per
stream via Arrow `BodyCompression` metadata in the IPC options. Not gRPC-level
compression — they're separate.

**Module.** `io.questdb.cutlass.arrow.compression.*`

**Implementation.** This is the clearest candidate for Rust / C via the
existing native-code infrastructure: call vendored `lz4` or `zstd` once per
buffer, per batch. No per-row or per-cell JNI traffic. Pure bulk transform
on already-materialised native memory.

Defer to a later milestone; ship the baseline uncompressed.

### 5.14 Server registration and configuration

**Scope.** Make the whole thing discoverable and configurable.

**Module.** changes to `io.questdb.cutlass.http.HttpServer`,
`HttpFullFatServerConfiguration`, and the config loader.

**Surface.**

- Config key `flight.sql.enabled` (default `false` until GA).
- Config key `flight.sql.max.concurrent.queries.per.connection`.
- Config key `flight.sql.prepared.statement.ttl.secs`.
- Config key `http.h2.enabled` (gating HTTP/2 in general).
- ALPN advertisement added to the TLS configuration.

**Size estimate.** ~300 LOC.

## 6. Dependency Graph and Build Order

Build bottom-up, validating each layer against real clients before adding
the next one.

```
Stage 1:  HTTP/2 framing (HTTP2_FRAME_CODEC.md)                 [DONE]
          |
          v
Stage 2:  HPACK  +  Stream state machine  +  Flow control       [DONE]
          +  Integration layer: protocolMode, preface sniff,
             pseudo-header capture, emit surface, trailers,
             park / resume
          |
          v
Stage 3:  gRPC framing layer (direct Http2StreamListener bind;  [NEXT]
          no HttpRequestProcessor adapter)
          |
          v
Stage 4:  Protobuf codec (Flight + Flight SQL messages)
          |
          v
Stage 5:  Flight RPC dispatcher skeleton
          +  CommandGetSqlInfo / CommandGetTableTypes stubs
          |
          v
Stage 6:  Arrow IPC encoder (Schema + RecordBatch)
          +  Arrow column emitters (scalar types only)
          +  Type mapping for scalars
          |
          v
Stage 7:  CommandStatementQuery end-to-end
          (first real SELECT via Flight SQL JDBC)
          |
          v
Stage 8:  Cursor streaming driver + flow-control coupling
          |
          v
Stage 9:  Prepared statements  +  DoPut  +  StatementUpdate
          |
          v
Stage 10: Auth  +  Cancellation
          |
          v
Stage 11: Metadata commands (full set)
          +  extension types, arrays, dictionary encoding
          |
          v
Stage 12: Page-frame fast path  +  compression  +  dictionary deltas
```

**TLS + ALPN runs in parallel** with Stages 3–5; it touches only the
socket init path in `HttpConnectionContext.doInit()` and is independent
of the gRPC / Flight SQL code paths.

Client validation at each stage:

| Stage | Client used as oracle                                |
|-------|------------------------------------------------------|
| 1–2   | `curl --http2-prior-knowledge`, `nghttp -v` — both working against the current build |
| 3     | `grpcurl -plaintext` against a hello-world service   |
| 5     | ADBC Python: `adbc_driver_flightsql.connect(...)` + `list_table_types()` |
| 7+    | Flight SQL JDBC driver + ADBC Python `execute()`     |
| 11+   | DBeaver / Tableau via Flight SQL JDBC                |

## 7. What We Reuse from QWP Egress

Phase 1 of QWP egress already solved roughly half of the Flight SQL
server-side problem. Direct reuse:

- **Per-column type dispatch** — the `switch` in
  `QwpResultBatchBuffer.appendRow` is 95% identical; only the emit step
  differs. Refactor the shared parts into a `ColumnTransposer` base with
  QWP and Arrow emit subclasses.
- **Native scratch buffer pattern** (`QwpColumnScratch`). The null bitmap
  tracking, value-stream heap, growth policy all port directly.
- **Two-path cursor iteration** (`QwpEgressProcessorState`'s
  `beginStreamingPageFrame` vs `beginStreaming`). Arrow layer needs the
  identical decision and state management.
- **Symbol table native-key fast path** — the `appendSymbolKey(int, SymbolTable)`
  path maps directly to Arrow dictionary encoding.
- **Bind variable plumbing** — the `DoPut`-driven prepared statement binds
  reuse the `BindVariableServiceImpl` integration from QWP egress.
- **Error classification** — `SqlException` → parse error, `CairoException`
  `isAuthorizationError` → permission denied, others → internal. Same mapping
  as QWP egress; different status codes.
- **Security context threading** — already plumbed from
  `HttpConnectionContext` into `SqlExecutionContextImpl`.

And one gap shared with QWP egress that we close here: **cancellation**. QWP
egress Phase 1 parses CANCEL and logs it; the cursor circuit breaker this
work adds serves both protocols.

## 8. Testing Strategy at a Glance

Each big block owns its own tests, with oracles chosen per layer:

| Block                             | Oracle                                       |
|-----------------------------------|----------------------------------------------|
| HTTP/2 framing, HPACK             | Netty codec (unit diff) + h2spec (integration) — see HTTP2_FRAME_CODEC.md |
| gRPC framing                      | `grpcurl`, differential against `io.grpc:grpc-core` in test scope |
| Protobuf codec                    | Google `protobuf-java` in test scope — encode with ours, decode with theirs and vice versa |
| Arrow IPC encoder + column emitters | `org.apache.arrow:arrow-vector` in test scope — decode our output, assert schema + per-column values match the source rows. Also Python `pyarrow` in an E2E smoke test |
| Flight SQL end-to-end             | `org.apache.arrow:flight-sql-jdbc-driver` in test scope + ADBC Python (subprocess smoke test) |
| CommandGetSqlInfo completeness    | ADBC Python connect + `get_objects()` + capability introspection |

Test-scope dependencies on `core/pom.xml`:

- Added: `io.netty:netty-codec-http2` — HTTP/2 frame-level differential
  (currently wired; `Http2NettyDifferentialTest` exercises every frame
  type). Reused by the planned HPACK Tier 3 differential once
  `DefaultHttp2HeadersEncoder` / `Decoder` are wired up — pending in
  `HPACK_CODEC.md` §15.4.
- Pending: `com.google.protobuf:protobuf-java` — protobuf differential
  (needed once the gRPC framing stage begins).
- Pending: `org.apache.arrow:arrow-vector` + `arrow-flight-sql-jdbc-driver`
  — Arrow IPC and Flight SQL end-to-end differential (needed for the
  later Arrow + Flight stages).

Precedent: `org.postgresql:postgresql` is already a test-only oracle for
pgwire, and `org.questdb:questdb-client` is already a test-only oracle for
the wire protocol clients. Same pattern, new subjects.

## 9. Open Questions

1. **Single HTTP port for h1.1 + h2, or a dedicated h2 port?** Resolved:
   the shared-port path is already implemented via preface sniffing in
   `HttpConnectionContext.protocolMode` and gated by
   `http.h2.enabled`. A dedicated h2 port remains a cheap config toggle
   if operators ask for strict isolation, but there's nothing in the
   current design that requires one.
2. **Batch size default.** Arrow readers are happy with 64k-row batches;
   QWP ships 4096. Likely a higher default is better for throughput but
   increases latency-to-first-byte. Benchmark once Stage 7 is live.
3. **`CommandStatementIngest` positioning.** Conceptually overlaps with ILP
   and QWP ingress. The Flight SQL ingest path is useful for BI tools that
   want to push intermediate results back, but positioning it alongside the
   existing ingest paths needs a product call.
4. **Rust for compression and SIMD bitmap**, per the earlier discussion. Defer
   to Stage 12; no block above Stage 12 depends on it.
5. **gRPC-Web (HTTP/1.1-compatible gRPC)** as a follow-on. Requires only a
   small shim once the gRPC + Flight SQL stack exists; would enable browser
   clients. Out of scope for the first milestone.
6. **Multi-endpoint / parallel `DoGet`** for future sharded deployments. The
   protocol supports it (`FlightInfo.endpoints` is a list); a single-node
   server returns one. No design debt incurred by ignoring multi-endpoint for
   now.
