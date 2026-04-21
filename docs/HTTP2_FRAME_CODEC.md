# HTTP/2 Frame Codec

Design for an HTTP/2 frame codec in QuestDB's zero-dependency, zero-GC Java
idiom. This is the foundational layer for the planned Arrow Flight
SQL endpoint (gRPC runs on HTTP/2) and for any future in-tree HTTP/2 uses
(gRPC-Web, HTTP/3 via a later upgrade). This document covers **framing only** —
HPACK and the per-stream request/response state machine each get their own
documents.

## Table of Contents

1. [Scope](#1-scope)
2. [Non-Goals](#2-non-goals)
3. [RFC References](#3-rfc-references)
4. [Module Layout](#4-module-layout)
5. [Frame Support Matrix](#5-frame-support-matrix)
6. [Wire Format](#6-wire-format)
7. [Connection Preface](#7-connection-preface)
8. [Buffer and Allocation Model](#8-buffer-and-allocation-model)
9. [Flight SQL Transport Implications](#9-flight-sql-transport-implications)
10. [Error Handling](#10-error-handling)
11. [Integration with `HttpConnectionContext`](#11-integration-with-httpconnectioncontext)
12. [Testing Strategy](#12-testing-strategy)
13. [Open Questions](#13-open-questions)

---

## 1. Scope

The codec reads and writes HTTP/2 frames (RFC 7540 sec. 4 and 6) between a
caller-owned byte buffer and an in-memory frame representation. It covers:

- Parsing the 9-byte frame header.
- Validating length, type, flag, and stream-id constraints.
- Reading and writing payload bytes for every frame type required by gRPC
  (`DATA`, `HEADERS`, `CONTINUATION`, `SETTINGS`, `WINDOW_UPDATE`,
  `RST_STREAM`, `GOAWAY`, `PING`).
- Enforcing our advertised inbound `SETTINGS_MAX_FRAME_SIZE` bound.
- Producing framed output into a caller-owned wire buffer with the same
  backpressure model used by `QwpEgressFrameWriter` (return -1 when the buffer
  is too small; caller drains and retries).

It does **not** cover:

- HPACK encoding or decoding of header block fragments (separate doc).
- Stream state machines (idle/open/half-closed/closed; separate doc).
- Connection- and stream-level flow-control accounting (WINDOW_UPDATE is parsed
  and serialised here; the window math lives with the stream state machine).
- TLS termination and ALPN negotiation. The first milestone ships clear-text
  h2c only, so the codec sees plaintext bytes straight from the socket.
  ALPN-driven `h2` / `http/1.1` selection is deferred to the TLS + ALPN
  track that runs in parallel with the gRPC / Flight SQL stages (see
  [`FLIGHT_SQL_DESIGN.md`](FLIGHT_SQL_DESIGN.md) §5.14 and the
  dependency-graph note in §6).

## 2. Non-Goals

- **No server push.** We decode `PUSH_PROMISE` enough to reject it; we never
  emit it.
- **No stream prioritisation.** `PRIORITY` frames and the `PRIORITY` flag on
  `HEADERS` are parsed, validated, and ignored (RFC 7540 allows this; HTTP/2
  priority is deprecated by RFC 9218 and unused by gRPC).
- **No extension frames.** Unknown frame types are discarded per RFC 7540 sec.
  4.1 ("Implementations MUST ignore and discard any frame that has a type that
  is unknown").
- **No HTTP/1.1 upgrade path (RFC 7540 sec. 3.2).** The path used by modern
  clients is either ALPN (`h2` over TLS) or the clear-text preface
  (`h2c` with prior knowledge). QuestDB's HTTP server would detect the preface
  before handing the connection to the HTTP/2 codec.

## 3. RFC References

Local plain-text copies of the RFCs live under `docs/rfc/` (see
`docs/rfc/README.md`) so design-review cross-checks can `grep` the
source directly without a network round-trip.

- [RFC 7540 — HTTP/2](https://www.rfc-editor.org/rfc/rfc7540)
  (`docs/rfc/rfc7540.txt`; framing layer; still the canonical
  reference even though 9113 supersedes it).
- [RFC 9113 — HTTP/2 (revision)](https://www.rfc-editor.org/rfc/rfc9113)
  (`docs/rfc/rfc9113.txt`; clarifications; where 9113 diverges
  from 7540, follow 9113).
- [RFC 7541 — HPACK](https://www.rfc-editor.org/rfc/rfc7541)
  (`docs/rfc/rfc7541.txt`; for the subsequent HPACK doc).
- [gRPC HTTP/2 usage](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
  — profiles the subset of HTTP/2 gRPC actually uses and the specific headers
  it expects. This is the primary conformance target for the Flight SQL work;
  generic HTTP/2 correctness is not enough.

## 4. Module Layout

All new code lives under `core/src/main/java/io/questdb/cutlass/http2/`.

| File                          | Purpose                                                              |
|-------------------------------|----------------------------------------------------------------------|
| `Http2FrameType.java`         | Byte constants (`DATA = 0x00`, `HEADERS = 0x01`, ...).               |
| `Http2Flags.java`             | Flag bit constants (`END_STREAM = 0x01`, `END_HEADERS = 0x04`, ...). |
| `Http2ErrorCode.java`         | `NO_ERROR = 0x0`, `PROTOCOL_ERROR = 0x1`, ... (RFC 7540 sec. 7).     |
| `Http2Settings.java`          | SETTINGS id constants, defaults, and `validate(short id, long value)` for range-checking unsigned 32-bit values (RFC 7540 sec. 6.5.2). No per-connection state; current-value tracking lives with the connection context in a separate layer. |
| `Http2FrameHeader.java`       | Parsed view of the 9-byte header (length, type, flags, streamId).   |
| `Http2FrameReader.java`       | Parse frame header, enforce per-frame structural + payload invariants, track the open-CONTINUATION sequence. Caller branches on `Http2FrameHeader.getType()` to dispatch. |
| `Http2FrameWriter.java`       | Emit frames into a caller-owned native buffer (mirrors `QwpEgressFrameWriter`). |
| `Http2ConnectionException.java`| Thrown for connection-level errors; maps to GOAWAY + error code.    |
| `Http2StreamException.java`   | Thrown for stream-level errors; maps to RST_STREAM + error code.    |
| `Http2Preface.java`           | 24-byte client connection preface; detection + consumption helper.  |

The codec follows QuestDB's existing pattern: static constants, no OO hierarchy
for frame kinds (one `Http2FrameReader` with a `switch` on type; payload
parsing is specialised inline), one reusable reader and writer per connection
context.

## 5. Frame Support Matrix

gRPC uses a narrow subset of HTTP/2. Implement-receive means the codec parses
and validates; implement-send means the codec can emit it.

| Type           | Code  | Receive | Send | Notes                                                                 |
|----------------|-------|---------|------|-----------------------------------------------------------------------|
| DATA           | 0x00  | Yes     | Yes  | gRPC payload carrier. Honour `END_STREAM` and `PADDED`.               |
| HEADERS        | 0x01  | Yes     | Yes  | Request and response headers. Honour `END_STREAM`, `END_HEADERS`, `PADDED`, `PRIORITY` (parse + discard priority fields). |
| PRIORITY       | 0x02  | Parse+ignore | No | RFC 7540 allows discarding. Never emitted.                          |
| RST_STREAM     | 0x03  | Yes     | Yes  | 4-byte payload. Used by gRPC `CANCELLED`.                             |
| SETTINGS       | 0x04  | Yes     | Yes  | 6-byte entries. `ACK` flag. Enforce setting-specific value ranges.    |
| PUSH_PROMISE   | 0x05  | Reject  | No   | Treat as `PROTOCOL_ERROR`; clients are not permitted to send server-push promises. |
| PING           | 0x06  | Yes     | Yes  | 8-byte opaque. Must echo with `ACK` flag set.                         |
| GOAWAY         | 0x07  | Yes     | Yes  | Connection teardown. Carries last-stream-id and error code.           |
| WINDOW_UPDATE  | 0x08  | Yes     | Yes  | 4-byte increment. Stream-id 0 is connection-level.                    |
| CONTINUATION   | 0x09  | Yes     | Yes  | Spillover of oversize HEADERS. Must immediately follow HEADERS/CONTINUATION on same stream. |
| ALTSVC, ORIGIN | 0x0A, 0x0C | Ignore | No | Unknown type per RFC; silently discarded.                         |

## 6. Wire Format

```
 +-----------------------------------------------+
 |                 Length (24)                   |
 +---------------+---------------+---------------+
 |   Type (8)    |   Flags (8)   |
 +-+-------------+---------------+-------------------------------+
 |R|                 Stream Identifier (31)                      |
 +=+=============================================================+
 |                   Frame Payload (0...)                      ...
 +---------------------------------------------------------------+
```

Big-endian throughout. The reserved high bit of the stream id (`R`) is
set to zero on send and ignored on receive per RFC 7540 sec. 4.1.

Max frame size is directional and negotiated per endpoint via
`SETTINGS_MAX_FRAME_SIZE` (default 16384, legal range 16384..16777215):

- The peer's advertised value caps the largest frame payload we may send to it.
- Our advertised value caps the largest frame payload we will accept from the
  peer.
- We advertise our own limit (default 16384, configurable up to 1 MiB) to bound
  per-frame memory. The peer cannot raise this inbound bound by advertising a
  larger value.

SETTINGS values are validated per setting:

- `SETTINGS_HEADER_TABLE_SIZE`: any unsigned 32-bit value is syntactically valid;
  the HPACK layer may clamp to its configured table bound.
- `SETTINGS_ENABLE_PUSH`: only `0` or `1` is valid. Server-side code never emits
  `PUSH_PROMISE`; if a future client role is added, it should disable peer push
  by sending `0`.
- `SETTINGS_MAX_CONCURRENT_STREAMS`: any unsigned 32-bit value is valid.
- `SETTINGS_INITIAL_WINDOW_SIZE`: max `0x7FFFFFFF`; larger is
  `FLOW_CONTROL_ERROR`.
- `SETTINGS_MAX_FRAME_SIZE`: must be `16384..16777215`; otherwise
  `PROTOCOL_ERROR`.
- `SETTINGS_MAX_HEADER_LIST_SIZE`: any unsigned 32-bit value is syntactically
  valid; header-list enforcement belongs with HPACK/request handling.

## 7. Connection Preface

Clients send exactly 24 bytes on stream open before any frames:

```
0x505249202a20485454502f322e300d0a0d0a534d0d0a0d0a
=  "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
```

Detection strategy in the existing HTTP/1.1 listener:

1. `Http2Preface.detect(buf, len)` returns one of three int constants:
   `Http2Preface.MATCH`, `Http2Preface.NO_MATCH`, or
   `Http2Preface.INCOMPLETE`. It returns `INCOMPLETE` iff the first
   `min(24, len)` bytes match the preface prefix but fewer than 24 bytes are
   available; the caller continues reading.
2. HTTP/1.1 methods (`GET`, `POST`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`,
   `CONNECT`, `TRACE`, `PATCH`) never start with `'P'` followed by `'R'` + `'I'`
   + `' '` — the `PRI` verb was chosen by RFC design to be unambiguous.
3. On match, consume 24 bytes and hand the connection to
   `Http2ConnectionContext`. The server must then immediately send its own
   `SETTINGS` frame (RFC 7540 sec. 3.5).

TLS / ALPN is **not** part of the first milestone. Milestone 1 is clear-text
h2c only: the HTTP/1.1 listener sniffs the preface and, on a positive match,
flips a `protocolMode` bit on the same `HttpConnectionContext` that owns the
fd. The context keeps the fd and dispatches subsequent reads into a composed
`Http2ConnectionContext` engine held as a lazy-allocated field
(`HTTP2_INTEGRATION.md` §4). There is no context swap and no dispatcher
change — `Http2ConnectionContext` is socket-free by contract.

The TLS / ALPN path (ALPN selection of `h2` vs `http/1.1` during the
handshake, pre-setting `protocolMode = MODE_H2_PREFACE_PENDING` from the
negotiated protocol, no MSG_PEEK sniff) lands alongside the Flight SQL TLS
work; see [`FLIGHT_SQL_DESIGN.md`](FLIGHT_SQL_DESIGN.md) §5.14 and the
dependency-graph TLS note in §6.

## 8. Buffer and Allocation Model

- All parsing operates on `long addr` + `long limit` pairs; no `byte[]` on the
  hot path.
- The 9-byte frame header is parsed in place via per-byte `Unsafe.getByte`
  loads assembled into the length / type / flags / stream-id fields. Per-byte
  is used instead of a single `Unsafe.getInt` + swap because the 24-bit length
  and the reserved-bit-masked stream id don't align to 4-byte boundaries; the
  simpler form reads cleanly and the JIT folds the shifts.
- `Http2FrameHeader` is a single long-lived instance per connection, mutated
  in place on each read. Callers must consume it before the next read.
- `Http2FrameWriter` writes one wire frame at a time. Fixed-size/control frames
  (`SETTINGS`, `PING`, `RST_STREAM`, `WINDOW_UPDATE`, `GOAWAY`) return the new
  write pointer, or `-1` if the caller-supplied buffer cannot hold the complete
  frame.
- Variable-size writers (`DATA`, `HEADERS`, `CONTINUATION`) must never require
  the entire logical gRPC message or header block to fit in the send buffer.
  The caller supplies an offset and the writer emits at most one frame payload
  chunk bounded by `peerMaxFrameSize` and available buffer space. The caller
  persists the offset across `PeerIsSlowToReadException` resumes.
- The receive path either guarantees `recvBufferSize >= 9 + inboundMaxFrameSize`
  or uses an explicit in-progress payload/discard state. The first milestone
  should keep the simpler invariant with the default 16 KiB inbound frame size;
  raising the advertised inbound max to 1 MiB also raises the per-connection
  receive-buffer commitment.
- No iterator objects, no `ByteBuffer.duplicate()`, no enum values for frame
  types in hot paths (byte constants instead).

## 9. Flight SQL Transport Implications

`FLIGHT_SQL_DESIGN.md` makes the frame codec load-bearing for `DoGet`: each
Arrow `RecordBatch` is serialized as a gRPC message and then carried by one or
more HTTP/2 `DATA` frames. The frame codec therefore needs these concrete
properties before it can support real Flight SQL traffic:

- A gRPC message may be larger than both `peerMaxFrameSize` and QuestDB's native
  socket send buffer. The HTTP/2 layer fragments the byte stream into multiple
  `DATA` frames; it does not force the Arrow or gRPC layer to materialize a
  frame-sized copy.
- `END_STREAM` belongs only on the final `DATA` frame for client request bodies,
  or on the trailing `HEADERS` frame for normal gRPC responses. Intermediate
  `DATA` frames in a large `DoGet` response do not change stream state.
- Outbound `HEADERS` and `CONTINUATION` follow the same rule as `DATA`: split
  header blocks at frame boundaries and resume from a persisted header-block
  offset after backpressure.
- The frame codec parses and serializes `WINDOW_UPDATE`, but the stream state
  machine owns the window counters. `FlightSqlStreamer` parks when the stream or
  connection send window is exhausted and resumes only after the HTTP/2 layer
  applies a valid `WINDOW_UPDATE`.
- Unknown extension frames can be discarded only after their full payload has
  been skipped. This uses the same receive-buffer invariant or in-progress
  discard state as oversized-but-valid known frames.

## 10. Error Handling

Two exception hierarchies, matching RFC 7540's distinction between
connection-level and stream-level errors:

- `Http2ConnectionException(int errorCode, CharSequence debug)` — terminal.
  Handler emits a single `GOAWAY` frame with the error code and closes the
  connection.
- `Http2StreamException(int streamId, int errorCode)` — per-stream. Handler
  emits `RST_STREAM` and continues serving the connection.

Connection-level triggers (per RFC 7540 sec. 5.4.1 + 6):

- Malformed frame size (payload length does not match the frame type's fixed
  size, is too small for the enabled flags, or exceeds our advertised
  `SETTINGS_MAX_FRAME_SIZE`) → `FRAME_SIZE_ERROR`.
- `PUSH_PROMISE` received from a client → `PROTOCOL_ERROR`.
- `SETTINGS` with stream id != 0 → `PROTOCOL_ERROR`.
- `SETTINGS` with `ACK` flag and non-zero length → `FRAME_SIZE_ERROR`.
- `SETTINGS` without `ACK` and length not divisible by 6 → `FRAME_SIZE_ERROR`.
- `SETTINGS` value outside the setting-specific legal range → error code
  specified in the per-setting validation table in §6.
- `PING` with stream id != 0 → `PROTOCOL_ERROR`.
- `PING` with length != 8 → `FRAME_SIZE_ERROR`.
- `PRIORITY` with stream id 0 → `PROTOCOL_ERROR`.
- `RST_STREAM` with stream id 0 or length != 4 → `PROTOCOL_ERROR` /
  `FRAME_SIZE_ERROR`.
- `GOAWAY` with stream id != 0 or length < 8 → `PROTOCOL_ERROR` /
  `FRAME_SIZE_ERROR`.
- `WINDOW_UPDATE` with length != 4 → `FRAME_SIZE_ERROR`.
- `WINDOW_UPDATE` with increment 0 on stream id 0 → `PROTOCOL_ERROR`.
- `HEADERS` on stream 0 → `PROTOCOL_ERROR`.
- `DATA` on stream 0 → `PROTOCOL_ERROR`.
- `CONTINUATION` not immediately preceded by `HEADERS` / `CONTINUATION` on
  same stream → `PROTOCOL_ERROR`.

Stream-level triggers:

- `DATA` on closed stream → `STREAM_CLOSED`.
- `PRIORITY` with length != 5 on a non-zero stream → `FRAME_SIZE_ERROR`
  (sec. 6.3: malformed PRIORITY is a stream error).
- `WINDOW_UPDATE` with increment 0 on a specific stream → `PROTOCOL_ERROR`
  (sec. 6.9: zero increment is connection error if stream id is 0, stream
  error otherwise).

All debug strings are ASCII-only per project convention.

## 11. Integration with `HttpConnectionContext`

The frame codec runs inside the H2 branch of `HttpConnectionContext`, not as
a separate `IOContext`. On a positive preface match,
`HttpConnectionContext` flips `protocolMode` to `MODE_H2_PREFACE_PENDING`,
lazy-allocates an `Http2ConnectionContext` engine (composition, not
inheritance), drains the 24-byte preface, transitions to `MODE_H2`, and
dispatches subsequent reads through the engine. The engine never touches
the socket — it reads from caller-provided `(addr, limit)` buffers and
writes to a caller-provided wire buffer. See
[`HTTP2_INTEGRATION.md`](HTTP2_INTEGRATION.md) §4 for the full
architecture.

Shared I/O plumbing:

- Same `IODispatcher`, same worker pool, same socket buffer pool.
- `HttpConnectionContext` keeps the fd and drives recv / send; `Http2ConnectionContext` is a composed field that receives decrypted-but-protocol-encoded bytes.
- Receive path: `HttpConnectionContext` reads into its existing native recv
  buffer, then calls `Http2ConnectionContext.processReceivedBytes(addr, limit)`
  which internally invokes `Http2FrameReader.tryReadNext(...)` frame by
  frame. `tryReadNext` returns the number of bytes consumed
  (`9 + payloadLength`) on success, or `0` if the frame is not yet fully
  buffered. Protocol violations surface as `Http2ConnectionException` /
  `Http2StreamException`; the engine handles them per §10 (GOAWAY /
  RST_STREAM emission). Partial frame bytes stay in the recv buffer for the
  next read.
- Send path: `HttpConnectionContext` calls
  `Http2ConnectionContext.writePending(addr, limit)` which emits frames via
  `Http2FrameWriter` into a native send buffer, then `socket.send`s the
  buffer. When the buffer fills, the engine returns with bytes still
  queued; `HttpConnectionContext` drains and retries on the next write tick.
  Per-stream outbound arenas (`Http2Stream.tryEnqueueOutbound`) hold
  application-level DATA / HEADERS / trailer tuples with copy-on-enqueue
  ownership; the scheduler drains them round-robin across streams subject to
  per-stream and connection-level flow-control windows.

## 12. Testing Strategy

Four tiers, each with a distinct oracle. The mix is deliberate: **the same bit
error is unlikely to be missed by all four.**

### 12.1 Oracles

| Oracle                                | Used for                                  | Scope            |
|---------------------------------------|-------------------------------------------|------------------|
| Netty `io.netty.handler.codec.http2`  | Differential frame roundtrip              | Unit             |
| `hpack-test-case` JSON corpus         | HEADERS payload decode — owned by HPACK_CODEC.md §15.3 (pending) | Unit (pending) |
| `summerwind/h2spec`                   | Full RFC 7540 / 7541 conformance          | Integration      |
| `curl --http2-prior-knowledge`, `nghttp` | End-to-end smoke                       | Integration      |
| `grpcurl`                             | Confirms the next layer up still works    | Integration      |

**Why Netty for Tier 1.** Netty's HTTP/2 codec is the reference Java
implementation. It backs gRPC-Java, Armeria, Reactor Netty, Jetty HTTP/2,
and effectively every JVM gRPC deployment in production. If our codec
produces frames Netty can't parse, or fails to parse frames Netty can emit,
we're the one that's wrong. Test-only scope is already an established pattern
in this codebase (postgresql JDBC driver is a test-only oracle for pgwire).

**Why h2spec for Tier 3.** `h2spec` (Go binary,
<https://github.com/summerwind/h2spec>) is the de facto HTTP/2 conformance
tester. It implements ~146 test cases drawn directly from RFC 7540 and RFC
7541. nginx, envoy, h2o, and nghttp2 themselves run it in CI. It's shipped as
a single static binary — no runtime dependency, just a CI download.

### 12.2 Test-only Maven dependency

Add to `core/pom.xml` in the test-scope block where `junit` and the
postgresql driver live today:

```xml
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-codec-http2</artifactId>
    <version>4.1.115.Final</version>
    <scope>test</scope>
</dependency>
```

`netty-codec-http2` transitively pulls `netty-codec-http`, `netty-codec`,
`netty-buffer`, `netty-common`, and `netty-handler` — all test-scope only, so
they never appear on the production classpath. Total test-jar size is
reasonable (~3–4 MB added).

### 12.3 Tier 1 — unit differential against Netty

Located at `core/src/test/java/io/questdb/test/cutlass/http2/`.

**Encoder fidelity** (`Http2FrameEncoderDifferentialTest`):

For each frame type, build a canonical instance, encode with our
`Http2FrameWriter` into a native buffer, copy the written bytes into a Netty
direct `ByteBuf`, decode with Netty's `Http2FrameReader`, and assert every
field matches. The copy is test-only and keeps production code free of Netty
types.

```java
// Pseudo-code for the DATA frame differential.
long base = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
int n = Http2FrameWriter.writeData(
        base, base + 64,
        /*streamId*/ 3,
        /*endStream*/ true,
        payloadAddr, payloadLen);
ByteBuf bb = Unpooled.directBuffer(n);
for (int i = 0; i < n; i++) {
    bb.writeByte(Unsafe.getUnsafe().getByte(base + i));
}
RecordingHttp2FrameListener listener = new RecordingHttp2FrameListener();
new DefaultHttp2FrameReader().readFrame(ctx, bb, listener);
listener.assertDataRead(3, payload, 0, true);
```

**Decoder fidelity** (`Http2FrameDecoderDifferentialTest`):

Encode with Netty's `Http2FrameWriter` into a `ByteBuf`, copy to native
memory, run our `Http2FrameReader` over it, assert fields match the original
inputs. Cross-validates that our parser accepts every valid frame Netty emits.

**Property-based**: a small generator for each frame type emits 1000
permutations of `(streamId, flags, payload)` with boundary values (0, 1,
`MAX_VALUE`, empty payload, max-size payload, every valid flag combination).
Roundtrip QuestDB → Netty and Netty → QuestDB for each.

**Malformed input**: a table of crafted byte sequences with the expected
error code. Assert QuestDB produces the same error class Netty does for each
case.

```java
@Test
public void rejectsZeroLengthHeadersFrameOnStream0() {
    // RFC 7540 sec. 6.2: HEADERS MUST be associated with a stream.
    // stream_id == 0 must be a PROTOCOL_ERROR.
    assertRejected(frame(HEADERS, flags=0, streamId=0, payload=empty),
            Http2ErrorCode.PROTOCOL_ERROR);
}
```

### 12.4 Tier 2 — HPACK corpus

Covered in the HPACK doc. Reference only here:
<https://github.com/http2jp/hpack-test-case> ships JSON vectors contributed by
multiple implementations (nghttp2, Go, Erlang, Haskell, Python, Swift). Each
file contains an encoded stream and the expected decoded header list. Standard
cross-validation corpus.

### 12.5 Tier 3 — h2spec conformance

A JUnit test (`Http2ConformanceTest`) that:

1. Starts `ServerMain` with `http.h2.enabled=true` on a random port. The
   H2 listener binds a minimal test-only handler (pre-Flight-SQL this is
   the `NoopH2Listener` placeholder in `HttpConnectionContext`; once
   Flight SQL is wired, a minimal route on the Flight SQL dispatcher
   suffices — h2spec does not exercise anything above the HTTP/2
   protocol layer).
2. Spawns `h2spec -h 127.0.0.1 -p $port --strict -j h2spec.json`.
3. Parses the JSON report and asserts zero failures.

Boot-strap the h2spec binary via a `.github/scripts/install-h2spec.sh` helper
that downloads the correct release for the CI runner OS. Pin the version
(`h2spec v2.6.0` or current). Skip gracefully on developer machines where the
binary is missing; require it on CI.

Begin with a scoped h2spec run — not all 146 tests, but the subset relevant
to the gRPC profile:

```
h2spec generic/4   # starting HTTP/2
h2spec http2/3.5   # HTTP/2 connection preface
h2spec http2/4.1   # frame format
h2spec http2/4.2   # frame size
h2spec http2/4.3   # header compression and decompression
h2spec http2/5.1   # stream states
h2spec http2/6.*   # frame definitions (DATA, HEADERS, SETTINGS, etc.)
h2spec http2/7     # error codes
h2spec hpack/*     # HPACK conformance
```

Expand coverage as the implementation matures.

### 12.6 Tier 4 — real-client smoke

Small JUnit tests that exec real clients and assert exit status + stdout.
These catch mismatches that slip through the other tiers (e.g., handshake
ordering quirks that h2spec doesn't exercise).

- `curl --http2-prior-knowledge http://127.0.0.1:$port/ping` against a
  hello-world handler. Asserts `200 OK` and body matches.
- `nghttp -v http://127.0.0.1:$port/ping`. `nghttp`'s verbose mode dumps every
  frame; capture and sanity-check.
- `grpcurl -plaintext 127.0.0.1:$port list` once the gRPC layer lands. Smoke
  test for the reflection service.

These are `@Ignore`-by-default when the client binaries are unavailable, so
local development doesn't require the full toolchain installation. CI
installs all three.

### 12.7 CI integration

One `http2-conformance` job in the existing GitHub Actions workflow:

- Installs h2spec, nghttp2-client, curl, grpcurl on the runner.
- Runs the frame-level `Tier 1` tests (Netty differential) as part of the
  normal `mvn test` invocation — no external dependencies required.
- Runs `Http2ConformanceTest` (Tier 3) and the smoke tests (Tier 4) as a
  separate integration-test target.

Tier 1 is fast (pure Java, no subprocess, no network): it runs on every
PR. Tier 3 and Tier 4 run on every PR as well but are isolated so failures
are easy to distinguish from general test regressions.

Tier 2 coverage for HPACK payload decode lives with the HPACK codec
(HPACK_CODEC.md §15.3) and is currently pending there: no
`core/src/test/resources/hpack-test-case/` content is wired, so this
tier is **not yet running in CI** despite the oracle listing above. The
row in §12.1 flags the dependency; the HTTP/2 frame codec itself is
exercised by Tier 1 alone on every PR.

## 13. Open Questions

1. **Our advertised inbound `SETTINGS_MAX_FRAME_SIZE` value.** Default 16 KiB
   bounds memory use per frame; 1 MiB reduces CPU by lowering frame count but
   raises the per-connection memory ceiling. Pick after measuring realistic
   gRPC / Flight SQL batch sizes.
2. **Connection-level `WINDOW_UPDATE` strategy** — immediate replenishment on
   every DATA frame versus batched updates at 50% window consumption. gRPC's
   BDP algorithm is one known-good strategy; implementing it is out of scope
   for the frame codec but worth flagging.
3. **Dynamic `SETTINGS` changes mid-connection**. The codec handles the frame;
   the stream state machine must decide whether to apply new values atomically
   at the `ACK` boundary or lazily as frames arrive.
4. **`netty-codec-http2` license**. Apache 2.0 — compatible, test-scope only.
   No issue; noted here for completeness.
