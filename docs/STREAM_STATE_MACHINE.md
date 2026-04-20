# HTTP/2 Stream State Machine

Design for the per-stream finite state machine, flow-control accounting,
and connection-wide stream bookkeeping that sits between the frame codec
(`HTTP2_FRAME_CODEC.md`) + HPACK codec (`HPACK_CODEC.md`) and the
request-dispatch layer above. Completes the Stage 2 surface defined in
`FLIGHT_SQL_DESIGN.md`.

## Table of Contents

1. [Scope](#1-scope)
2. [Non-Goals](#2-non-goals)
3. [RFC References](#3-rfc-references)
4. [Module Layout](#4-module-layout)
5. [Stream State Model](#5-stream-state-model)
6. [Stream Identifier Rules](#6-stream-identifier-rules)
7. [Flow Control](#7-flow-control)
8. [Concurrency Limits](#8-concurrency-limits)
9. [Frame → Stream Dispatch](#9-frame--stream-dispatch)
10. [Connection Context](#10-connection-context)
11. [Integration with HPACK](#11-integration-with-hpack)
12. [Error Handling](#12-error-handling)
13. [Testing Strategy](#13-testing-strategy)
14. [Milestone Scoping and Build Order](#14-milestone-scoping-and-build-order)
15. [Open Questions](#15-open-questions)

---

## 1. Scope

This document specifies:

- The RFC 7540 sec. 5.1 / RFC 9113 sec. 5.1 seven-state per-stream FSM.
- Transition triggers: HEADERS / CONTINUATION / DATA / RST_STREAM /
  WINDOW_UPDATE / END_STREAM flag observations, plus connection-level
  GOAWAY fallout on open streams.
- Per-stream and connection-level flow-control window accounting
  (`SETTINGS_INITIAL_WINDOW_SIZE`, inbound / outbound `WINDOW_UPDATE`,
  the RFC 7540 sec. 6.9.2 "only DATA frames consume the window" rule).
- Concurrency limits: `SETTINGS_MAX_CONCURRENT_STREAMS` and the
  first-unused stream id tracking required to enforce it.
- Stream id validation (monotonic, odd-or-even origin) and the
  RFC 7540 sec. 5.1.1 "idle to closed transition" shortcut.
- The connection context that owns the frame reader / writer, the two
  HPACK codecs, the stream map, and the connection-level flow-control
  windows.
- Dispatch from frame events to per-stream handlers that assemble the
  full request (pseudo-headers + headers + body) and hand it up to the
  existing request-pipeline layer.

It does **not** cover:

- Stream prioritisation (RFC 7540 sec. 5.3 is deprecated by RFC 9113;
  priority frames parse cleanly at the frame codec but are ignored).
- PUSH_PROMISE (we never push server-side streams; inbound push is
  disabled via `SETTINGS_ENABLE_PUSH = 0`).
- Connection preface detection (handled in the listener before this
  layer sees bytes; see `FLIGHT_SQL_DESIGN.md` §4).
- TLS / ALPN (also handled lower in the stack).
- HTTP/1.1 compatibility shim (a separate branch of the listener
  dispatches to the existing HTTP/1.1 code path on non-preface input).

## 2. Non-Goals

- **No request-level parsing.** Turning a decoded header list into an
  `HttpRequest` / gRPC envelope is the layer above. This layer delivers
  a stream of `onHeader` callbacks plus byte ranges for body DATA.
- **No response generation.** The dispatched handler emits response
  headers / body through the outbound side of this layer; shaping the
  response itself (status codes, Content-Type, etc.) is the handler's
  job.
- **No priority scheduling.** `PRIORITY` frames are parsed and
  dropped.
- **No opportunistic server push.** `SETTINGS_ENABLE_PUSH` is always
  advertised as `0`; a peer that sends `PUSH_PROMISE` is a protocol
  error.
- **No stream-id recycling across connections.** Each connection starts
  its id counter at the RFC 7540 minimum (1 for client-initiated).

## 3. RFC References

Local plain-text copies of the RFCs live under `docs/rfc/`
(see `docs/rfc/README.md`) so design-review cross-checks can
`grep` the source directly without a network round-trip.

- [RFC 9113 — HTTP/2](https://www.rfc-editor.org/rfc/rfc9113)
  (`docs/rfc/rfc9113.txt`) — the normative reference. Sec. 5 is the
  stream state model, sec. 6 the frame types, sec. 8 the request /
  response semantics. Priority is deprecated in sec. 5.3.2 and is
  parsed-and-discarded per §5.
- [RFC 7540 — HTTP/2](https://www.rfc-editor.org/rfc/rfc7540)
  (`docs/rfc/rfc7540.txt`) — the original spec. Cited below only
  for language that 9113 inherited verbatim or for section numbers
  that readers tracking the older document will expect; when the
  two disagree, 9113 wins.
- [gRPC HTTP/2 usage](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
  constrains trailers (`HEADERS` after final DATA, `END_STREAM` on the
  trailers frame) and END_STREAM handling on the request side.

## 4. Module Layout

All new code lives under `core/src/main/java/io/questdb/cutlass/http2/`.

Production code:

| File                          | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Http2StreamState.java`       | Enum of the seven FSM states (IDLE, RESERVED_LOCAL, RESERVED_REMOTE, OPEN, HALF_CLOSED_LOCAL, HALF_CLOSED_REMOTE, CLOSED).                                                                                                                                                                                                                                                                                                                                                            |
| `Http2Stream.java`            | Per-stream state: current `Http2StreamState`, inbound + outbound flow-control windows, `outstandingInboundCredit` (bytes the handler hasn't acked via `onBytesConsumed` — see §7 step 5 / 6), `generation` (monotonic `int`, incremented on every LIVE → tombstone promotion and every recycle, used as the token the handler must echo on `onBytesConsumed` so late acks after reset / recycle are silently dropped — §7 step 6 late-ack guard), the `initialHeadersSeen` flag (distinguishes request HEADERS from trailers), the `refusingCurrentBlock` flag (M1 trailer refusal substate — §8), a fixed-size native byte buffer (`HEADER_STAGING_BYTES` = 16 KiB, §11) that holds only the copied name/value bytes of pseudo-header slots plus staged regular-header fields (with `hostSlot` as an aliased view); **separate** plain-Java `int[]` tables (`HEADER_STAGING_TUPLES` = 600 entries × 5 ints ≈ 12 KiB of heap) that index into the byte buffer — the tuple table is **not** native and **not** an `ObjList<Object>`; per-stream pseudo-header slots filled during HPACK decode, `policySum` (running `32 + nameLen + valueLen` counter against `HEADER_LIST_POLICY_CAP`), the request handler's stream-scoped state slot. Does **not** own a HPACK-block assembly buffer — that lives on the connection context (§11).                                                                                                           |
| `Http2StreamPool.java`        | Fixed-size slot array sized to our advertised `SETTINGS_MAX_CONCURRENT_STREAMS` plus `N` tombstone slots for recently-closed streams plus a small reserve for in-flight discarding-block entries (§5 grace window). Each slot is a tagged union of three kinds: LIVE (full `Http2Stream`, counts against `activeStreamCount`), DISCARDING_BLOCK (id + fixed reason + block-bytes accumulator, doesn't count, lives only until END_HEADERS then promotes to LOCAL_RESET tombstone; field validators do not run because the reason is already final — see §11 sticky-error precedence), or TOMBSTONE ({CLEAN, LOCAL_RESET} + closeTick, doesn't count). The pool is sized purely by our inbound policy — the peer's `MAX_CONCURRENT_STREAMS` limits streams we may initiate, and since the server never pushes, it never constrains allocation here. A free-list of slot indices tracks available slots; a `streamId → slotIndex` primitive int map (`IntIntHashMap`) keyed by the 31-bit id locates all three kinds. Closed streams first become tombstones, then return to the free-list once the grace window rolls off, so post-warmup requests allocate nothing. |
| `Http2ConnectionContext.java` | Owns the frame reader / writer, both HPACK codecs, the stream pool, the connection-level flow-control windows, SETTINGS tracking, the HEADERS / CONTINUATION block-assembly scratch, and the two RFC 9113 sec. 5.1.1 / sec. 6.8 peer-stream-id counters (`highestPeerStreamIdSeen` for monotonic-id validation, `lastAcceptedPeerStreamId` for GOAWAY last-stream-id — see §6). Not thread-safe; one per connection, driven by the single owner thread.                                |
| `Http2StreamListener.java`    | Visitor the request-dispatch layer above implements. Four kinds of callback: **per-field at END_HEADERS** — `onRequestHeader(streamId, nameAddr, nameLen, valueAddr, valueLen, neverIndexed)` invoked once per staged regular header, relayed from per-stream scratch after the block passes validation (§11); a rejected block produces zero per-field callbacks; **per-block at END_HEADERS** — `onRequestHeaders(streamId, pseudoHeaderSlots, endStream)` fires once after the initial HEADERS block passes validation, with `endStream=true` indicating a request whose terminal edge is the HEADERS frame itself (no body, no trailers will follow); **body and trailers** — `onData(streamId, addr, dataLen, endStream, generationToken)` (post-strip body bytes only; padding has already been debited and credited back by §7; `generationToken` is the current `Http2Stream.generation` that the handler must echo on any subsequent `onBytesConsumed`) and `onTrailers(streamId, ..., endStream)`; **teardown** — `onStreamClosed(streamId, cause)`. The dispatch contract: the handler learns "the request is complete" on exactly one callback per stream — whichever of `onRequestHeaders`, `onData`, or `onTrailers` carries `endStream=true`. There is no separate `onRequestComplete`; the terminal edge is always the `endStream` flag on the last callback. `onData` takes ownership of the payload semantically but not memory: the `(addr, dataLen)` pair is stable only for the duration of the call (it points into the frame reader's receive buffer); the handler must copy any bytes it needs to retain, and returns `true` to signal synchronous consumption (context credits the `dataLen` portion of both inbound windows and may emit `WINDOW_UPDATE` immediately) or `false` to defer. Deferred credit flows through the **connection context**, not through a handler-held `Http2Stream` reference: the handler holds only the `(streamId, generationToken)` pair it received on `onData`, and when `dataLen` bytes' worth of deferred work completes it calls `context.onBytesConsumed(streamId, generationToken, n)`. `outstandingInboundCredit` tracks **bytes the handler still owes an ack for** (on every deferred `onData`, the context accumulates `outstandingInboundCredit += dataLen`). On each `onBytesConsumed(streamId, generationToken, n)` call the context first does its lookup (slot must be LIVE, `generation` must match the handler's token; anything else is a silent no-op), then validates `0 < n && n <= outstandingInboundCredit` — if `n` falls outside that range the call is also a silent no-op, with no window change, to make double-ack and oversized-ack inflation impossible. On a valid ack the context **subtracts** `n` from `outstandingInboundCredit` and **adds** `n` to both inbound windows via the coalesced `WINDOW_UPDATE` path. This design deliberately prevents the handler from holding a pooled `Http2Stream` reference across its own deferred work — a stale reference could otherwise credit an unrelated stream after the slot recycled, even with the token guard. If the stream is reset or torn down before the handler acks, the context's `cancelDeferredCredit(streamId)` internal path credits whatever remains in `outstandingInboundCredit` back to the connection window (not the stream window) and zeroes the counter — see §7 step 6. See §7 for the coalescing rules on deferred credit. One instance per connection context. |
| `Http2FlowController.java`    | Stateless helpers for window arithmetic: `debit`, `credit`, `adjustOnInitialWindowChange`. Takes the current window as a parameter and returns the updated window or a signed overflow/underflow signal. State lives on `Http2ConnectionContext` (connection windows) and `Http2Stream` (per-stream windows).                                                                                                                                                                         |

Test code:

| File                        | Purpose                                                                                                                                               |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Http2ConformanceTest.java` | Test harness running `h2spec`'s stream-state + flow-control subsets against our connection context. Already sketched in `HTTP2_FRAME_CODEC.md` §12.5. |

Follows the frame codec / HPACK codec pattern: static constants, one reusable
per-connection context, no OO hierarchy over frame types. **Zero-GC on the
hot data path.** All per-connection state is allocated once at connection
setup and reused across requests; `Http2Stream` instances are returned to
the pool's free-list on close. The per-request allocation budget is
zero — measured the same way the HPACK codec is.

## 5. Stream State Model

Per RFC 7540 sec. 5.1, every stream moves through a subset of seven states.
Only the transitions relevant to a server are exercised.

```
                          +--------+
                  send PP |        | recv PP
                 ,--------|  idle  |--------.
                /         |        |         \
               v          +--------+          v
        +----------+          |           +----------+
        |          |          | send H /  |          |
 ,------| reserved |          | recv H    | reserved |------.
 |      | (local)  |          v           | (remote) |      |
 |      +----------+      +--------+      +----------+      |
 |          |             |        |             |          |
 |          |     send H  |        | recv H      |          |
 |          |     ,-------|  open  |-------.     |          |
 |          |    /  endS  |        |  endS  \    |          |
 |          |   v         +--------+         v   |          |
 |          | +----------+    |    +----------+  |          |
 |          | | half-    |    |    | half-    |  |          |
 |          | | closed   |    |    | closed   |  |          |
 |          | | (remote) |    |    | (local)  |  |          |
 |          | +----------+    |    +----------+  |          |
 |          |      |          |          |       |          |
 |          |      | send endS| recv endS|       |          |
 |          |      | or R     | or R     |       |          |
 |          |      v          v          v       |          |
 |          |  +-----------------------------+   |          |
 |          |  |                             |   |          |
 `----------'->|           closed            |<--'----------'
  send R / R   |                             |  send R / R
  recv endS    +-----------------------------+  recv endS
     (H = HEADERS, R = RST_STREAM, endS = END_STREAM, PP = PUSH_PROMISE)
```

**Server-side transitions we actually implement.** Both directions: the
client sends the request so recv events drive the request side, and the
server sends the response so send events drive the response side. `H` is
HEADERS (initial or trailers, distinguished per-stream by the
`initialHeadersSeen` flag — see §11), `endS` is the END_STREAM flag on
the frame that carries it.

| From                 | Event                             | To                   |
|----------------------|-----------------------------------|----------------------|
| idle                 | recv H                            | open                 |
| idle                 | recv H + endS                     | half-closed (remote) |
| open                 | recv endS (on DATA or trailers H) | half-closed (remote) |
| open                 | send endS (on DATA or trailers H) | half-closed (local)  |
| open                 | recv / send RST_STREAM            | closed               |
| half-closed (remote) | send H (response)                 | half-closed (remote) |
| half-closed (remote) | send DATA (without endS)          | half-closed (remote) |
| half-closed (remote) | send endS (on DATA or trailers H) | closed               |
| half-closed (remote) | send / recv RST_STREAM            | closed               |
| half-closed (local)  | recv DATA (without endS)          | half-closed (local)  |
| half-closed (local)  | recv endS (on DATA or trailers H) | closed               |
| half-closed (local)  | recv / send RST_STREAM            | closed               |

The typical server-side sweep of a request / response is:
`idle → open (recv request H) → half-closed (remote) (recv endS) →
send response H → send DATA* → send endS → closed`. Send events on
HEADERS / DATA that don't carry `endS` stay in the same state; the
transitions in the table fire only on the end-of-message boundary.

**Transitions a server never observes** and must reject as a protocol
error on the receive side:

- idle → reserved (local) / reserved (remote): server never sends
  PUSH_PROMISE, client never does either.
- recv PUSH_PROMISE at any state: `PROTOCOL_ERROR`.

**GOAWAY does not force streams closed.** RFC 9113 sec. 6.8: GOAWAY
means "the sender will not process streams beyond `lastStreamId`",
not "all open streams closed now". The `lastStreamId` is direction-
specific — it names the highest id **initiated by the peer** that the
sender of GOAWAY might still act on:

- **We send GOAWAY.** `lastStreamId = lastAcceptedPeerStreamId`
  (highest client-initiated id we actually admitted to the pool;
  REFUSED_STREAM ids are excluded so the client can safely retry
  them — see §6). Streams with `id ≤ lastAcceptedPeerStreamId`
  complete through their normal transitions; any `HEADERS`
  arriving with a higher id after our GOAWAY flushes are refused
  with `RST_STREAM(REFUSED_STREAM)` **and** the frame's HPACK /
  flow-control state still advances per the §12 minimal-processing
  rule.
- **Peer (client) sends GOAWAY.** Its `lastStreamId` names the
  highest **server-initiated** stream the client would still process
  — i.e., pushes the client acknowledges acting on. Because this
  server never pushes (`SETTINGS_ENABLE_PUSH = 0`), `lastStreamId`
  is effectively advisory for us: it does **not** cancel in-flight
  client-initiated requests, and we do **not** `RST_STREAM(CANCEL)`
  streams with higher ids. We enter `DRAINING`, finish in-flight
  client-initiated streams normally, refuse any new `HEADERS` that
  arrive after the GOAWAY with `REFUSED_STREAM`, and close the
  socket when `activeStreamCount == 0`.

The shutdown flow is in §12.

**Per-state frame legality.** Each state permits a specific subset of
frame types on the receive side; everything else is a protocol error.
The table below is enforced by `Http2ConnectionContext.dispatch()`:

| State                         | Permitted inbound frame types                                                                                                |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| idle                          | HEADERS, PRIORITY                                                                                                            |
| open                          | HEADERS (trailers only, with END_STREAM), CONTINUATION (during open header block), DATA, RST_STREAM, WINDOW_UPDATE, PRIORITY |
| half-closed (remote)          | WINDOW_UPDATE, RST_STREAM, PRIORITY                                                                                          |
| half-closed (local)           | HEADERS (trailers), CONTINUATION, DATA, RST_STREAM, WINDOW_UPDATE, PRIORITY                                                  |
| closed (clean END_STREAM)     | WINDOW_UPDATE, PRIORITY, RST_STREAM — grace window, all others `STREAM_CLOSED` (see below)                                   |
| closed (locally-reset)        | Any frame type, minimally processed — grace window, see below                                                                |

RFC 9113 sec. 5.1: an endpoint MUST NOT send frames other than
PRIORITY on a closed stream, and after receiving a frame with
`END_STREAM` any subsequent frame is a connection error of type
`STREAM_CLOSED`. But frames can race on the wire, and the
**grace window** covers that.

Two flavours of closed:

- **Clean close.** The stream reached `closed` via matching
  `END_STREAM` on both sides without an RST_STREAM. `WINDOW_UPDATE`,
  `PRIORITY`, and `RST_STREAM` are tolerated in the grace window
  (RFC 9113 sec. 5.1 calls out each of these as frames that can
  legitimately race after we send `END_STREAM`). Any `HEADERS`,
  `CONTINUATION`, or `DATA` for a cleanly-closed stream is
  `STREAM_CLOSED` (stream or connection scope per RFC).
- **Locally reset.** We sent `RST_STREAM` from `open` or
  `half-closed-local`. The peer may still be in flight with
  `HEADERS` / `CONTINUATION` / `DATA` that crossed our reset. RFC
  9113 sec. 5.1 requires **minimal processing** of those late frames
  rather than rejection: `HEADERS` / `CONTINUATION` must still be
  HPACK-decoded to keep the shared dynamic table coherent (an
  ignored block would desynchronise the decoder for every future
  stream on the same connection). `DATA` is debited at the top of
  dispatch per §7 step 1; the payload is discarded, and the
  connection window is credited back via §7 step 5 so unrelated
  live streams on the same connection are not starved by in-flight
  bytes we never delivered. The stream window stays dead (the
  stream is closed). Decoded header callbacks and DATA payloads
  are discarded — only the HPACK state is advanced here.

Both flavours need per-id state to remember which rule applies.
`Http2StreamPool` holds a short-lived **tombstone** for each
recently-closed stream: `{streamId, closeKind ∈ {CLEAN, LOCAL_RESET}`,
closeTick}`. Tombstones occupy the same slot array as live streams
but do not count against `activeStreamCount`; their HPACK state is
already shared on the connection, so the tombstone only needs the
close-kind flag and the close timestamp. When the tombstone window
overflows the configured bound (§15), the oldest tombstones roll
off.

**Dispatch precedence for a received frame** (order matters, and
this is the contract referenced from §6 and §12):

1. **Live-stream match.** If the frame's id matches a live stream,
   per-state rules from the permitted-inbound-frames table apply.
   If the LIVE stream has `refusingCurrentBlock == true` (§8 M1
   trailer refusal), the CONTINUATION frames of the in-flight
   trailer block feed the HPACK-discard pipeline on the LIVE slot
   rather than surfacing per-field callbacks — see §8 and §11
   listener wiring.
2. **Live-tombstone match.** If the frame's id matches a live
   tombstone, the tombstone's close-kind (CLEAN / LOCAL_RESET)
   selects between the two flavours above.
3. **Live discarding-block match.** If the frame's id matches a
   live discarding-block entry (§8 concurrency-refused / §12
   post-GOAWAY — see "pool entry kinds" below), feed `CONTINUATION`
   into the block's HPACK-discard pipeline; any other frame type is
   a connection `PROTOCOL_ERROR` because the frame reader already
   enforces CONTINUATION sequencing (§9 / see
   `HTTP2_FRAME_CODEC.md` §9.3), so while a discarding block is
   live the only frame that can legally arrive on that id is another
   CONTINUATION. `DATA` for a discarded stream arrives only **after**
   END_HEADERS has promoted this entry to a `LOCAL_RESET` tombstone,
   and is handled by the tombstone path in rule 2.
4. **No match, id > `highestPeerStreamIdSeen`.** Monotonic-ok; the
   frame names a new stream. Dispatch by frame type:
    - `HEADERS` — advance `highestPeerStreamIdSeen = id`, run the
      idle-state admission path: concurrency check (§8) →
      GOAWAY-draining check (§12) → allocate a live `Http2Stream` on
      success or a discarding-block entry on refusal.
    - `PRIORITY` — legal on an idle id per RFC 9113 sec. 5.1 (the
      idle row in §5's permitted-frames table already lists it).
      Parse the payload and discard it (priority is deprecated per
      RFC 9113 sec. 5.3.2). **Do not advance
      `highestPeerStreamIdSeen`** — the counter tracks HEADERS per
      its definition in §6, and RFC 9113 sec. 5.1 is explicit that
      PRIORITY does not change stream state, so a later valid
      HEADERS on a lower id must not be rejected as a monotonic
      violation. In practice HTTP/2 clients rarely emit idle
      PRIORITY at all, but the framing has to stay correct.
    - Any other frame type on an idle id is a connection
      `PROTOCOL_ERROR` per the idle row in §5.
5. **No match, id ≤ `highestPeerStreamIdSeen`.** The id has been
   seen before and its live / tombstone / discarding-block entry
   has rolled off. Two sub-cases:
    - `HEADERS` / `CONTINUATION` — connection `PROTOCOL_ERROR`.
      The monotonic rule is structural: once a tombstone has rolled
      off we no longer have the state needed to distinguish "late
      trailer HEADERS for a stream we just forgot" from "new-stream
      HEADERS attempting to reuse a lower id", so we collapse both
      into the stricter case and let the peer recover on a new
      connection. The tombstone window bound (§15) should be sized
      to cover the peer's normal wire latency so this branch is
      effectively unreachable in practice.
    - `WINDOW_UPDATE`, `RST_STREAM`, `PRIORITY` — silently ignored
      (the RFC explicitly forbids treating late `WINDOW_UPDATE` as
      a stream error; we apply the same tolerance to RST_STREAM /
      PRIORITY).
    - `DATA` — the §7 step 1 debit has already run at the top of
      dispatch; payload discarded, stream error `STREAM_CLOSED`;
      the connection window is credited back via §7 step 5 so
      healthy streams keep their share.

**Pool entry kinds.** `Http2StreamPool` holds three kinds of entry
per slot, all indexed by stream id:

- **LIVE.** Full `Http2Stream`; counts against `activeStreamCount`.
- **DISCARDING_BLOCK.** `{streamId, reason,
  blockBytesAccumulated}`. Holds an id whose HEADERS block is being
  HPACK-decoded with upward callbacks suppressed — refused for
  concurrency (§8) or rejected because we are draining after GOAWAY
  (§12). Does not count against `activeStreamCount`. Lives until
  END_HEADERS, at which point it converts into a `LOCAL_RESET`
  tombstone, `RST_STREAM(reason)` is emitted, and any subsequent
  `DATA` that reaches the converted tombstone follows the
  `LOCAL_RESET` minimal-processing path. M1 trailer refusal is
  **not** a DISCARDING_BLOCK — see §8 for the LIVE-substate
  treatment.
- **TOMBSTONE.** `{streamId, closeKind ∈ {CLEAN, LOCAL_RESET},
  closeTick}`. Does not count against `activeStreamCount`. Rolls
  off when the tombstone window overflows (§15).

All three share the slot array and the `streamId → slotIndex` map;
the slot's tagged type determines behaviour. A refused HEADERS
block that spans CONTINUATION frames keeps hitting rule 3 above
until END_HEADERS, then promotes to a tombstone.

## 6. Stream Identifier Rules

RFC 7540 sec. 5.1.1:

- Client-initiated streams use odd ids; server-initiated use even.
- Ids are monotonically increasing on each side. Observing a new
  HEADERS with an id ≤ the highest previously seen is a connection
  `PROTOCOL_ERROR`.
- A lower-id idle stream is implicitly closed when a higher-id stream
  is opened. That "implicit idle → closed" transition is what makes
  the state model safe against stale-id DoS — we never have an
  unbounded pool of idle streams.

**Two distinct id counters.** RFC 9113 sec. 5.1.1 (monotonic
validation) and sec. 6.8 (GOAWAY last-stream-id) observe different
events:

- `highestPeerStreamIdSeen` — highest id the peer has used on any
  HEADERS, whether or not we accepted it. Used **only** for the
  monotonic-id rule: a HEADERS with `id ≤ highestPeerStreamIdSeen`
  for an id that is not the currently-open one is a connection
  `PROTOCOL_ERROR`. It MUST NOT appear in GOAWAY; including a
  REFUSED_STREAM id here would incorrectly tell the client the
  stream was processed and thus not safe to retry.
- `lastAcceptedPeerStreamId` — highest id we accepted into the
  stream pool (i.e., allocated an `Http2Stream` for and started
  dispatching). Advanced only after the concurrency check and
  state-table entry into `open` / `half-closed-remote`; NOT
  advanced for streams we `RST_STREAM(REFUSED_STREAM)`-rejected.
  This is the value we put in GOAWAY.
- `lastOurStreamId` — highest id we've used for a server-initiated
  stream (always 0 on the server side since we never push).

RFC 9113 sec. 6.8 is specific about "last-stream-id" meaning
"streams the sender has taken or will take action on", which matches
`lastAcceptedPeerStreamId` — a REFUSED_STREAM by definition has not
been acted on. The two counters only diverge when concurrency /
shutdown refusal is happening, but the divergence is exactly what
preserves request-retry safety for the client.

**Bound.** Stream ids are carried as a 32-bit field on the wire with
the high bit reserved (sent `0`, ignored on read per RFC 7540 sec. 4.1);
the effective range is `[0, 2^31 − 1]`. When `highestPeerStreamIdSeen`
approaches `2^31 − 1`, RFC 9113 sec. 5.1.1 requires us to emit GOAWAY
and refuse further streams on this connection. The client then opens
a new connection.

## 7. Flow Control

Per RFC 7540 sec. 5.2, only DATA frames consume flow-control windows.
Every endpoint maintains two **separate** windows per stream (send
and receive) plus two connection-level windows. Each DATA frame
debits both the stream window and the connection window on the
receive side; a WINDOW_UPDATE credits exactly one.

**Windows.**

- `inboundConnectionWindow` — bytes the peer may still send us.
  Initial value: 65535 (RFC 7540 sec. 6.9.2). We raise this up front
  via a connection-level WINDOW_UPDATE (delta chosen per §15).
- `outboundConnectionWindow` — bytes we may still send the peer.
  Initial value: 65535. Peer raises it via WINDOW_UPDATE.
- `inboundStreamWindow` (per stream) — bytes the peer may still send
  on this stream. Initial value: `ourApplied[INITIAL_WINDOW_SIZE]`
  at the moment the stream transitions to `open` (default 65535;
  equals `ourAdvertised` once the peer has ACKed our SETTINGS).
  Using `ourApplied` rather than `ourAdvertised` matters when a
  stream opens after we emit new SETTINGS but before the ACK
  arrives: the peer's encoder is still running against the old
  limit, so we must admit DATA against the old limit too, otherwise
  a legitimate in-flight DATA frame sized to the old window would
  flip to `FLOW_CONTROL_ERROR`. When the ACK arrives the §10
  compute-delta-then-commit flow adds the delta to every
  already-open stream (§7 initial-window-size adjustment), so the
  new window catches up.
- `outboundStreamWindow` (per stream) — bytes we may still send on
  this stream. Initial value: `peerAdvertised[INITIAL_WINDOW_SIZE]`
  at the moment the stream opens (default 65535). Peer SETTINGS
  apply immediately on receipt (§10 peer-apply rule), so no
  ACK-vs-advertised split exists on this side.

**Inbound accounting on DATA.** Two measurements per DATA frame:

- `paddedLen` — the full flow-controlled payload size: 1 byte for
  the Pad Length field (if PADDED flag set) + data + padding
  bytes. RFC 9113 sec. 6.9.1 requires flow-control accounting to
  use this total. The frame reader exposes it alongside the
  stripped data range.
- `dataLen` — data bytes only, i.e. what the handler's `onData`
  will see. `dataLen = paddedLen - (paddingOverhead)` where
  `paddingOverhead` is `1 + paddingByteCount` for PADDED frames,
  `0` otherwise.

Processing runs in this fixed order:

1. **Connection-window debit + immediate underflow check
   (against `paddedLen`).** Debit `inboundConnectionWindow -=
   paddedLen`. This debit happens **exactly once per DATA frame**,
   at the top of the dispatch path, before any per-id lookup.
   Every subsequent error path (locally-reset tombstone, rolled-off
   tombstone, discarding-block, content-length pre-dispatch,
   stream-window underflow) relies on this one debit having
   already happened and never re-debits. Immediately after the
   debit, check for underflow against `paddedLen`, **not** against
   the net-of-padding `dataLen` — symmetric with the stream-window
   rule in step 3. RFC 9113 sec. 6.9.1 requires the *full* DATA
   payload, including padding, to fit within the available
   connection window; a frame with `paddedLen >
   inboundConnectionWindow` must kill the connection even if
   `dataLen <= inboundConnectionWindow` would have fit. On
   underflow: `GOAWAY(FLOW_CONTROL_ERROR)`, skip the padding
   credit-back of step 2 (the connection is torn down anyway),
   skip all remaining steps.
2. **Immediate padding credit-back (connection window).** If
   underflow did NOT occur in step 1 and `paddingOverhead > 0`,
   credit `paddingOverhead` bytes back to the connection window
   through the coalescing path (§15 threshold) immediately.
   Padding is wire overhead that never reaches the handler; only
   the `dataLen` portion is subject to deferred handler
   consumption. The stream-window side of the padding credit
   happens in step 3 below, in the same per-id lookup pass that
   decides whether a stream-window debit even applies.
3. **Per-id lookup + stream-window debit, underflow check,
   padding credit (in that order).** Resolve the id per §5 dispatch
   precedence. Only when the lookup finds a **live stream whose
   inbound direction is still active** (states `open` or
   `half-closed-local`) do we touch the stream window. The order
   matters and is non-interchangeable:

   1. Debit `inboundStreamWindow -= paddedLen`.
   2. **Immediately check for underflow** against `paddedLen`,
      **not** against the net-of-padding `dataLen`. RFC 9113 sec.
      6.9.1 requires the *full* DATA payload, including padding,
      to fit within the available stream window; a frame with
      `paddedLen > inboundStreamWindow` must reset the stream
      even if `dataLen <= inboundStreamWindow` would have fit.
      If underflow occurred, emit
      `RST_STREAM(FLOW_CONTROL_ERROR)`, do NOT credit padding
      back to the (now-dead) stream window, and proceed to step
      4 routing with the stream reset; the `dataLen`-sized
      connection-window credit-back happens per the
      "stream-window underflow" row of the step 5 table.
   3. If underflow did NOT occur, credit `paddingOverhead` back
      to the stream window, symmetrically with the connection-
      window padding credit in step 2. The net effect on a
      live-active stream that fits its window is a `dataLen`
      debit of both windows, with the padding overhead cycling
      through both without lingering.

   Debiting a stream window for a tombstoned / discarded /
   post-END_STREAM id is meaningless: the window either doesn't
   exist (tombstone / rolled-off / discarding block) or is frozen
   (`half-closed-remote`, `closed`), so those arrivals skip the
   stream-window touch entirely.
4. **Route to per-stream logic.** The per-stream path may suppress
   handler delivery but does not touch the window.
5. **Credit-back invariant — `dataLen` portion.** Every
   connection-debited DATA frame either closes the connection or
   eventually produces a `dataLen`-sized connection-window credit.
   The per-arrival-path rules:

   | Arrival path                                              | Connection-window credit-back (`dataLen`) | Stream-window credit-back (`dataLen`) |
   |-----------------------------------------------------------|-------------------------------------------|----------------------------------------|
   | Live stream, handler accepts (`onData` returns `true`)    | Immediate via coalesced `WINDOW_UPDATE`   | Immediate via coalesced `WINDOW_UPDATE`|
   | Live stream, handler defers (`onData` returns `false`)    | On `context.onBytesConsumed(streamId, generationToken, n)` | On `context.onBytesConsumed(streamId, generationToken, n)` |
   | Live stream, stream-window underflow → stream reset       | **Immediate** via coalesced `WINDOW_UPDATE(0)` at the moment of underflow | None (stream now closed)               |
   | Live stream, content-length declared too small (excess body, §11 pre-dispatch drop) | Immediate via coalesced `WINDOW_UPDATE(0)` | None (stream reset)                    |
   | Live stream, content-length declared too large (early `END_STREAM`, §11 terminal check) | **Immediate** via coalesced `WINDOW_UPDATE(0)` for this frame's `dataLen`, before the stream is reset — the terminal `onData(..., endStream=true)` was suppressed so the handler never acked these bytes | None (stream now closed)               |
   | Locally-reset tombstone DATA (§5 rule 2 LOCAL_RESET)       | Immediate via coalesced `WINDOW_UPDATE(0)` | N/A (no stream window)                |
   | Rolled-off tombstone DATA (§5 rule 5)                      | Immediate via coalesced `WINDOW_UPDATE(0)` | N/A                                    |
   | Clean-closed stream DATA (both sides reached `closed` via matching `END_STREAM`) | Immediate via coalesced `WINDOW_UPDATE(0)`; `STREAM_CLOSED` stream error, connection stays alive | N/A (stream window gone)               |
   | `half-closed-remote` stream receiving DATA (peer's inbound side closed, our outbound may still be open) | Immediate via coalesced `WINDOW_UPDATE(0)`; `STREAM_CLOSED` stream error | N/A (inbound window frozen)            |
   | Connection-window underflow (step 1)                      | Connection closes via GOAWAY; no further accounting | —                                      |

   **Not** in this table: DATA on a stream in `half-closed-local`
   (we sent `END_STREAM` on the response body but the peer can
   still finish its request body). That is the normal live-active
   inbound path — peer DATA is legal, handler delivery is legal,
   covered by the "Live stream, handler accepts / defers" rows at
   the top of the table.

   `WINDOW_UPDATE(streamId=0)` credits only the connection window
   and is the mechanism every non-handler-delivery path uses to
   restore connection capacity without pretending the stream is
   still there.

6. **Deferred-credit stream-close settlement.** When a stream that
   has deferred handler credit (`onData` returned `false`, so
   `Http2Stream.outstandingInboundCredit > 0`) is reset or closed
   before the handler acks — via `RST_STREAM(sent)`,
   `RST_STREAM(received)`, handler teardown, local stream error,
   or connection shutdown — the context credits whatever remains
   in `outstandingInboundCredit` back to the connection window
   via coalesced `WINDOW_UPDATE(0)` and zeroes the counter.
   Without this, a resettable stream holding deferred credit
   would permanently leak connection-window capacity every time
   its handler failed to ack. The stream-side credit is dropped
   (the stream window is gone). The context exposes an internal
   `cancelDeferredCredit(streamId)` path for the close handlers
   to call; the method is idempotent.

   **Late-ack guard via stream generation.** A handler that has
   already received `onStreamClosed` may still call
   `context.onBytesConsumed(streamId, generationToken, n)` — racy
   handler code, slow release of a deferred task after the stream
   was reset by the peer, or a handler that held the
   `(streamId, generationToken)` pair past the slot's recycle
   point. Without a guard the ack would either (a) emit a
   spurious second `WINDOW_UPDATE(0)` on top of the settlement
   credit, or (b) credit an unrelated stream that now owns the
   slot. Both are silent corruption.

   `Http2Stream` carries a monotonically-increasing `generation`
   (`int`, incremented by the pool on every LIVE → tombstone
   promotion and on every recycle back from the free-list). The
   handler learns the current generation at dispatch time and
   passes it back on each ack:

   ```java
   // context → handler (handler-facing listener signature, §4):
   listener.onData(streamId, addr, dataLen, endStream,
                   generationToken);
   // handler later, after deferred work completes:
   context.onBytesConsumed(streamId, generationToken, n);
   ```

   The handler holds only the `(streamId, generationToken)` pair
   — no reference to the pooled `Http2Stream` — so a stale handler
   task cannot corrupt a recycled slot even if it keeps the pair
   past the stream's lifetime. Padding accounting
   (`paddedLen`, `paddingOverhead`) stays on the internal
   `stream.onData(...)` call (§9 dispatch table) and is not
   exposed to the handler.

   `context.onBytesConsumed(streamId, generationToken, n)` runs
   three checks, in order, and no-ops on any failure (no window
   move, no `WINDOW_UPDATE` emitted):

   1. **Slot + generation.** Look up `streamId` in the pool map.
      Must resolve to a LIVE slot with matching `generation`.
      TOMBSTONE, DISCARDING_BLOCK, unknown id, or bumped
      generation → no-op.
   2. **Ack magnitude.** Must satisfy `0 < n && n <=
      outstandingInboundCredit`. An `n` of zero, negative, or
      larger than the outstanding balance → no-op. This is what
      makes double-acks and oversized acks harmless: without the
      bound, a double ack would credit flow-control windows
      without a matching debit and leave `outstandingInboundCredit`
      permanently non-zero (to be re-credited later by the
      settlement path — double counting).
   3. **Apply.** Subtract `n` from `outstandingInboundCredit`,
      credit `n` to the connection window and (if the stream is
      still inbound-direction-active — `open` or `half-closed-local`)
      the stream window, through the coalesced `WINDOW_UPDATE`
      path.

   `cancelDeferredCredit` is therefore doubly safe: it credits
   the remaining balance once and zeroes the counter, and any
   later handler-driven ack fails check 1 (generation bumped or
   slot gone) before it can reach check 2 or 3.

**Outbound accounting on send.** The handler hands the writer a
`(streamId, payloadAddr, payloadLen, endStream)` tuple; the writer
enqueues it on the stream's per-stream send queue and signals the
connection-level outbound scheduler that this stream is ready. The
scheduler, not the enqueue path, is where DATA frames are emitted —
that's what makes the multiplexing paragraph below effective.

**Zero-length END_STREAM is not flow-controlled.** A tuple with
`payloadLen == 0 && endStream == true` emits a zero-length DATA
frame with `END_STREAM` set even when either outbound window is zero.
RFC 9113 sec. 6.9.1: zero-length DATA frames do not consume flow-
control capacity. This is the common case of a handler signalling
"response body done" after a prior partial write has drained the
window; blocking on `WINDOW_UPDATE` here would deadlock the stream.

**Multiplexed scheduler.** The outbound scheduler runs one pass per
write-readiness tick. Per pass it walks a round-robin cursor over the
set of streams whose per-stream send queue is non-empty AND whose
`outboundStreamWindow > 0` (or whose head tuple is a zero-length
`END_STREAM`, per the previous paragraph):

1. Pick the next ready stream in round-robin order.
2. Compute `sendable = min(outboundConnectionWindow, outboundStreamWindow,
   peerMaxFrameSize)` for that stream. Cap also by the stream's head
   tuple's remaining payload.
3. Emit one DATA frame of that size (or a zero-length `END_STREAM`).
   Debit both windows by the frame's payload length. Preserve the
   tuple's `endStream` flag only on the frame that carries its final
   byte; once flagged the stream advances its FSM per §5.
4. Advance the round-robin cursor, repeat until either the connection
   window is zero or no stream is ready.

A single large response tuple therefore yields the floor frequently
enough that other streams get airtime under backpressure; one pass
per ready stream per pass, not one pass per tuple. Blocked streams
sit in their own per-stream queue and are returned to the ready set
by the matching inbound `WINDOW_UPDATE` (§9). Priority (RFC 7540 sec.
5.3) is explicitly not honoured — RFC 9113 sec. 5.3.2 deprecates it,
and round-robin is the documented gRPC fallback.

**SETTINGS_INITIAL_WINDOW_SIZE change.** RFC 9113 sec. 6.9.2 applies
the adjustment only to "streams with active flow-control windows":
per-stream windows whose direction is still live. The connection
window is never affected. Direction matters asymmetrically:

- `outboundStreamWindow` is live only in states where we can still
  send DATA — `open` and `half-closed-remote`. In
  `half-closed-local` we have sent END_STREAM, so our outbound
  direction is closed and the window is frozen.
- `inboundStreamWindow` is live only in states where the peer can
  still send DATA — `open` and `half-closed-local`. In
  `half-closed-remote` the peer has sent END_STREAM, so the
  inbound direction is closed and the window is frozen.

Two directions, two adjustment moments, each over the appropriate
active set:

- **Peer changes its advertised value (affects our outbound per-stream
  window).** When the peer's SETTINGS frame arrives with a new
  `INITIAL_WINDOW_SIZE`, compute `delta = new - peerAdvertised[...]`
  and add `delta` to `outboundStreamWindow` on every stream in
  `open` or `half-closed-remote`. Adjustment is immediate, before
  we emit the SETTINGS ACK (per §10 peer-apply rule).
- **We change our advertised value (affects our inbound per-stream
  window).** When our SETTINGS ACK arrives from the peer, compute
  `delta = ourAdvertised[...] - ourApplied[...]` and add `delta` to
  `inboundStreamWindow` on every stream in `open` or
  `half-closed-local`. The adjustment happens only on ACK, not on
  emit (§10 our-apply rule): until then the peer's encoder is still
  running against the old value.

Two adjustment outcomes, asymmetric:

- **Negative window after adjustment — acceptable.** RFC 9113 sec.
  6.9.2 explicitly permits this; the stream simply cannot send /
  receive new DATA on the affected direction until a subsequent
  WINDOW_UPDATE catches up. Our `Http2FlowController` helpers are
  already signed-`long` precisely so we can represent the transient
  negative state without truncation.
- **Window past `2^31 − 1` after adjustment — connection
  `FLOW_CONTROL_ERROR`.** RFC 9113 sec. 6.9.2 ("an endpoint MUST
  treat a change to SETTINGS_INITIAL_WINDOW_SIZE that causes any
  flow-control window to exceed the maximum size as a connection
  error"). Because the check has to run before any adjustment is
  applied (we can't roll back per-stream windows once written), the
  apply-initial-window-delta helper sweeps the **direction-active**
  set only (per the state list above): `maxWindowAfter =
  maxOf(window) + delta` over that set first, then emit
  `GOAWAY(FLOW_CONTROL_ERROR)` if the bound is breached, and only
  then commit the deltas. Streams whose direction is frozen are
  excluded from both the precheck and the commit. Tier 2 tests
  (§13.2) cover the negative-window case and this overflow case
  beside the existing WINDOW_UPDATE overflow test.

**Validation.** RFC 7540 sec. 6.9.1: a WINDOW_UPDATE with
`increment == 0` is a stream error (`PROTOCOL_ERROR`) at stream
scope, or a connection error at connection scope. A WINDOW_UPDATE
that would push the recipient's window past `2^31 - 1` is a
`FLOW_CONTROL_ERROR`.

## 8. Concurrency Limits

`SETTINGS_MAX_CONCURRENT_STREAMS` caps the number of **open + partially
closed** streams. The server advertises a limit; the peer opens up to
that many concurrent streams. Streams in `idle` and `closed` do not
count.

**Two independent ceilings, one direction each.**

- **Inbound ceiling (ours).** `SETTINGS_MAX_CONCURRENT_STREAMS` that
  we advertise caps how many client-initiated streams the peer may
  have open against us. Enforcement lives in `Http2StreamPool`.
- **Outbound ceiling (peer's).** The peer's advertised
  `SETTINGS_MAX_CONCURRENT_STREAMS` caps server-initiated streams
  (pushes). Because `SETTINGS_ENABLE_PUSH = 0` is hard-coded, we
  never open a server-initiated stream and this value is recorded
  in `peerAdvertised[MAX_CONCURRENT_STREAMS]` but never read. It
  does not size any data structure.

**Tracking.** `Http2StreamPool` maintains:

- `activeStreamCount` — streams in `open` / `half-closed-local` /
  `half-closed-remote`.
- `maxConcurrentStreams` — the value **we** advertise (default 100;
  revisit in §15). The pool's slot array is sized to
  `maxConcurrentStreams + tombstoneSlack` (§5).

When the peer tries to open a new stream (HEADERS on an idle id)
while `activeStreamCount >= maxConcurrentStreams`, we respond with
`RST_STREAM(REFUSED_STREAM)` — RFC 7540 sec. 5.1.2 explicitly calls
out that REFUSED_STREAM is safe for the peer to retry, preserving
request-retry semantics.

**The refused HEADERS block must still decode.** RFC 9113 sec. 4.3
requires every received HEADERS / CONTINUATION block to be
HPACK-decoded to keep the shared dynamic table coherent, even when
the stream itself is going to be discarded — skipping decode would
desynchronise the decoder and force the whole connection to
`COMPRESSION_ERROR`. The refusal path therefore mirrors the
post-GOAWAY path (§12) and the pseudo-header sticky-error path
(§11):

1. Allocate a `DISCARDING_BLOCK` pool entry for the id (§5
   pool-entry-kinds) and do NOT advance `lastAcceptedPeerStreamId`.
   `highestPeerStreamIdSeen` does advance, to preserve monotonic-id
   validation (§6).
2. Pipe the block through `HpackDecoder.decodeBlock` with a
   listener that suppresses every upward callback. Field
   validators (forbidden-header, header-list-size, name, value,
   content-length) do **not** run — the refusal reason is already
   final (see §11 "sticky-error precedence"), so there is no error
   code to escalate. HPACK decode itself proceeds normally to keep
   the dynamic table coherent.
3. At END_HEADERS emit `RST_STREAM(REFUSED_STREAM)` and promote the
   `DISCARDING_BLOCK` entry to a `LOCAL_RESET` tombstone (§5) so
   that DATA the peer may have pipelined before seeing the reset
   debits the connection-level inbound window and is then
   discarded.

**M1 trailer refusal — LIVE-stream substate, not DISCARDING_BLOCK.**
A second HEADERS arriving on a stream while `initialHeadersSeen ==
true` during Milestone 1 is rejected as "trailers not supported in
M1". The refusal cannot use a `DISCARDING_BLOCK` pool entry because
the id is already occupied by the LIVE slot for this stream
(dispatch rule 1 in §5 wins over rule 3, and the `streamId →
slotIndex` map has one entry per id). Instead the LIVE stream
carries a boolean substate flag `refusingCurrentBlock`:

1. On the trailer HEADERS that triggers the refusal, the stream
   sets `refusingCurrentBlock = true`, records
   `pendingStreamError = (PROTOCOL_ERROR, "trailers not supported
   in M1")`, and feeds the fragment into the block-assembly scratch
   as usual.
2. While `refusingCurrentBlock == true`, the HPACK listener
   (§11 "listener wiring") drops every emission — per-field,
   pseudo-header-slot captures, and aggregate. Forbidden-header /
   header-list-size / field validators do not run (the decision is
   already final, see §11 sticky-error precedence).
3. At END_HEADERS the stream emits
   `RST_STREAM(pendingStreamError.code)`, decrements
   `activeStreamCount`, frees its handler state (by calling
   `onStreamClosed(streamId, cause = STREAM_REFUSED_TRAILERS)`),
   and the pool slot converts LIVE → `LOCAL_RESET` tombstone. Any
   DATA the peer pipelined before seeing the reset is then handled
   by the tombstone path in §5 rule 2.

The substate keeps the LIVE-slot invariant (one slot per id)
intact. `DISCARDING_BLOCK` is reserved for refusals that happen on
an id we never admitted to LIVE in the first place: concurrency
(§8) and post-GOAWAY (§12).

**Why not PROTOCOL_ERROR for over-limit streams?** Some clients
optimistically pipeline N+1 streams before ACKing our SETTINGS; a
hard protocol error would break them. REFUSED_STREAM is the
RFC-blessed soft-rejection.

## 9. Frame → Stream Dispatch

`Http2ConnectionContext` runs one loop per connection readiness tick:

```
while more bytes in receive buffer:
    try reader.tryReadNext(addr, limit, header, inboundMaxFrameSize)
    if header incomplete: break
    dispatch(header)
    advance addr past this frame
```

`dispatch(header)` routes by frame type. Stream-scoped entries look up
the stream in the pool (creating on idle HEADERS, rejecting if the pool
is full); connection-scoped entries act on the context directly.

| Type                         | Scope      | Target                                                                                                             |
|------------------------------|------------|--------------------------------------------------------------------------------------------------------------------|
| DATA                         | stream     | `stream.onData(addr, dataLen, paddedLen, paddingOverhead, endStream)` internal to the context (flow-control accounted per §7); relayed to the handler as `listener.onData(streamId, addr, dataLen, endStream, generationToken)` with the current `Http2Stream.generation` |
| HEADERS                      | stream     | `stream.onHeadersStart(...)` — append fragment to block scratch                                                    |
| PRIORITY                     | stream     | Parse + discard (RFC 9113 sec. 5.3.2 deprecates priority)                                                          |
| RST_STREAM                   | stream     | `stream.onRstStream(errorCode)`                                                                                    |
| SETTINGS (ACK = 0)           | connection | apply peer values per §10, emit SETTINGS(ACK)                                                                      |
| SETTINGS (ACK = 1)           | connection | compute-delta-then-commit `ourAdvertised → ourApplied` per §10                                                     |
| PUSH_PROMISE                 | —          | `PROTOCOL_ERROR` (server never accepts push)                                                                       |
| PING (ACK = 0)               | connection | Validate `streamId == 0` (else `PROTOCOL_ERROR`); echo the 8-byte opaque payload back in a PING frame with ACK set |
| PING (ACK = 1)               | connection | Correlate with outstanding outbound PING (if any); no response                                                     |
| GOAWAY                       | connection | Mark connection `DRAINING`; finish all in-flight client-initiated streams; refuse new HEADERS with `REFUSED_STREAM` (§12)    |
| WINDOW_UPDATE (streamId = 0) | connection | Credit `outboundConnectionWindow`; validate increment in §7                                                        |
| WINDOW_UPDATE (streamId > 0) | stream     | Credit `stream.outboundStreamWindow`; validate per §7                                                              |
| CONTINUATION                 | stream     | `stream.onContinuation(...)` — append fragment to block scratch                                                    |

The CONTINUATION sequencing (one HEADERS followed by 0+ CONTINUATIONs
on the same stream, all before any other frame type) is already
enforced by `Http2FrameReader`; `dispatch()` only sees a structurally
legal sequence.

## 10. Connection Context

`Http2ConnectionContext` owns:

- `Http2FrameReader reader` + `Http2FrameWriter writer` (bytes → frame
  header + payload; frame → bytes).
- `HpackDecoder hpackDecoder` + `HpackEncoder hpackEncoder` (header
  block → `(name, value)` callbacks and back).
- `Http2StreamPool streamPool`.
- `long inboundConnectionWindow`, `long outboundConnectionWindow`.
- Per-SETTINGS identifier: three `long[7]` arrays indexed by wire
  id with slot 0 unused — `ourAdvertised` (value we last put on
  the wire), `ourApplied` (value currently enforced locally; may
  lag `ourAdvertised` for ACK-gated and tighten-pending
  identifiers per the bucket rules below), `peerAdvertised` (value
  the peer last advertised, applied locally on receipt). `long`
  throughout since SETTINGS values are unsigned 32-bit on the
  wire (`HTTP2_FRAME_CODEC.md` §2).
- `boolean settingsFrameOutstanding` — tracks the explicit "one
  SETTINGS frame in flight" invariant independently of
  `ourApplied == ourAdvertised`, so that immediate-only SETTINGS
  frames still block a second emit until the first ACK lands.
- `Http2StreamListener listener` — upward visitor the dispatch layer
  supplies.
- `long highestPeerStreamIdSeen` — highest id the peer has used on
  any HEADERS, accepted or not (§6). Used for monotonic-id
  validation, not for GOAWAY.
- `long lastAcceptedPeerStreamId` — highest id we accepted into the
  pool and started dispatching (§6). This is the value we emit in
  GOAWAY's last-stream-id field.
- Connection state: `ACTIVE`, `DRAINING` (after GOAWAY send/receive),
  `CLOSED`.

**SETTINGS exchange.** On connection start the context immediately
emits our SETTINGS frame (empty-payload ACK-flagged SETTINGS is sent
separately in response to the peer's SETTINGS). Our SETTINGS carries:

- `SETTINGS_HEADER_TABLE_SIZE` = our advertised HPACK cap.
- `SETTINGS_ENABLE_PUSH = 0`.
- `SETTINGS_MAX_CONCURRENT_STREAMS` = our policy.
- `SETTINGS_INITIAL_WINDOW_SIZE` = our per-stream inbound window
  initial.
- `SETTINGS_MAX_FRAME_SIZE` = our policy.
- `SETTINGS_MAX_HEADER_LIST_SIZE` = our policy (M2; M1 omits this
  identifier from the SETTINGS frame, see §11 and §14.1).

Per-identifier tracking. Three `long[7]` arrays indexed directly by
wire setting id (`HEADER_TABLE_SIZE = 1`..`MAX_HEADER_LIST_SIZE = 6`),
with slot 0 unused so that `arr[MAX_HEADER_LIST_SIZE]` does not
overrun. `long` for every entry — SETTINGS values are unsigned 32-bit
on the wire (RFC 9113 sec. 6.5.2), so `long` fits every legal value
with no truncation and keeps the array declaration uniform:

```java
long[] ourAdvertised;  // value we last put on the wire
long[] ourApplied;     // value currently in effect locally
long[] peerAdvertised; // value the peer last advertised, applied locally on receipt
```

**Outstanding-SETTINGS discipline.** RFC 9113 sec. 6.5.3: SETTINGS ACKs
are delivered in order, so an ACK always applies to the oldest
unacknowledged SETTINGS frame we sent. We serialise this by allowing
**at most one SETTINGS frame outstanding per connection**, tracked by
a dedicated `boolean settingsFrameOutstanding` (not derived from
`ourAdvertised != ourApplied`). Rationale: a frame carrying only
pure-immediate identifiers — say `MAX_CONCURRENT_STREAMS` — commits
`ourApplied` on emit, so `ourAdvertised == ourApplied` the instant
the frame leaves the wire even though the ACK is still in flight.
If we used the equality as the outstanding marker we could emit a
second SETTINGS (say an ACK-gated `INITIAL_WINDOW_SIZE` change)
before the first ACK returns, and when that first ACK arrived we
would incorrectly apply the second frame's pending changes against
it. The explicit boolean avoids that: `emitSettings()` refuses to
send while `settingsFrameOutstanding == true` and instead queues
the requested change; on ACK receipt we clear the boolean and
flush the queue. The implementation stays trivial (no snapshot
ring, no ACK-to-frame correlation), the common case (SETTINGS at
connection start, rare reconfiguration thereafter) is unaffected,
and the pathological case (rapid-fire SETTINGS reconfiguration) is
bounded by a single queued snapshot. M2 may relax this to an FIFO
of snapshots if a use case appears.

Two directions, two apply rules. The timing is **per-identifier**,
not "everything on ACK" — some settings we can enforce immediately
when our SETTINGS frame leaves the wire, others must wait for ACK:

| Identifier                     | When we enforce it                                   | Why                                                                                                                                                                                                             |
|--------------------------------|------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `MAX_CONCURRENT_STREAMS`       | Immediately (pool sized / enforced at connection start and on every change) | Over-limit streams get `RST_STREAM(REFUSED_STREAM)` — retry-safe, so pre-ACK optimistic-pipelining clients are not broken. Pool sizing is structural and can't be deferred anyway (§4).                         |
| `MAX_FRAME_SIZE`               | Widen immediately, tighten on ACK (`max(old, new)` until ACK) | Widening is strictly permissive — accepting a larger frame can never hurt. Tightening before ACK would reject frames the peer legitimately sized to the old limit, surfacing as a false `FRAME_SIZE_ERROR`. We keep both `ourApplied` (old cap) and `ourAdvertised` (new cap) and reject only frames that exceed `max(ourApplied, ourAdvertised)` until the ACK lets us assign `ourApplied = ourAdvertised`. |
| `MAX_HEADER_LIST_SIZE` (M2)    | Widen immediately, tighten on ACK                    | Same argument as MAX_FRAME_SIZE: the peer encoder may already have a block in flight under the old limit. (Not advertised in M1 — §11.)                                                                         |
| `HEADER_TABLE_SIZE`            | On ACK only (`HpackDecoder.onLocalAdvertisedCapChanged`) | The peer's encoder controls dynamic-table size updates; it only knows our new cap once it has processed our SETTINGS. Touching the decoder before ACK would desynchronise the table and cause `COMPRESSION_ERROR`. |
| `INITIAL_WINDOW_SIZE` (inbound)| On ACK only (delta applied to active streams)       | Peer's flow-control accounting runs against the old value until it processes our SETTINGS; a pre-ACK tightening would cause legitimate DATA to overrun a window that the peer still thinks is larger.           |
| `ENABLE_PUSH = 0`              | Connection-start constant, never changes            | We never advertise anything else.                                                                                                                                                                               |

- **Settings we send.** Per-identifier
  apply rule, matching the timing table above. **Invariant: every
  emitted SETTINGS sets `ourAdvertised[id] = newValue`** for every
  identifier it carries — that is what `ourAdvertised` means, "value
  we last put on the wire" (see the field declaration above). Only
  the `ourApplied` update timing varies per bucket:

    - **Pure-immediate identifiers** (`MAX_CONCURRENT_STREAMS`,
      `ENABLE_PUSH`). Set `ourAdvertised = newValue` and
      `ourApplied = newValue` both on emit. After the emit the two
      slots stay equal for this identifier.
    - **Widen-immediately, tighten-on-ACK identifiers**
      (`MAX_FRAME_SIZE`, `MAX_HEADER_LIST_SIZE`). Set
      `ourAdvertised = newValue` on emit unconditionally. If
      `newValue >= ourApplied[id]` (widening or equal), also set
      `ourApplied = newValue` on emit — the peer's old-cap traffic
      remains legal under the new larger cap, and the two slots
      are equal again immediately. If `newValue < ourApplied[id]`
      (tightening), leave `ourApplied` at the old cap; inbound
      validation uses `max(ourApplied, ourAdvertised)` until ACK,
      and the ACK handler below catches up by assigning `ourApplied
      = ourAdvertised`.
    - **Pure-ACK-gated identifiers** (`HEADER_TABLE_SIZE`,
      `INITIAL_WINDOW_SIZE`). Set `ourAdvertised = newValue` on
      emit; leave `ourApplied` at the old value unconditionally
      until the ACK — these settings have no safe pre-ACK
      enforcement at all.

  In all three cases, emitting the SETTINGS frame sets
  `settingsFrameOutstanding = true`. On receipt of the peer's
  SETTINGS ACK we propagate every identifier where `ourApplied !=
  ourAdvertised` (i.e., the ACK-gated ones plus any tighten-pending
  widen-immediately ones) in **compute-delta-then-commit** order:
  read `delta = ourAdvertised[id] - ourApplied[id]` first, apply
  the change against that delta (for `HEADER_TABLE_SIZE`
  `HpackDecoder.onLocalAdvertisedCapChanged`; for
  `INITIAL_WINDOW_SIZE` add `delta` to `inboundStreamWindow` on the
  inbound-direction-active set `open` / `half-closed-local` per
  §7; for `MAX_FRAME_SIZE` / `MAX_HEADER_LIST_SIZE` the tightening
  is a plain field update — the old cap is now retired), then
  assign `ourApplied[id] = ourAdvertised[id]`. The ordering matters
  — copying first would leave `delta` stuck at zero and existing
  per-stream windows would silently fail to adjust. Once every
  pending identifier has been propagated, clear
  `settingsFrameOutstanding = false` and flush any queued outbound
  SETTINGS (per the outstanding-discipline note above).
- **Settings we receive.** On receipt of
  the peer's SETTINGS frame we walk every `(id, value)` entry. RFC
  9113 sec. 6.5.2 requires us to **ignore unknown setting
  identifiers** — the 16-bit id space extends well beyond our
  `{1..6}` range and future spec extensions live there. Each entry
  is filtered through `isKnownSettingId(id)` (checks `id >= 1 &&
  id <= MAX_HEADER_LIST_SIZE`) before any array access; unknown
  ids are silently dropped. For known ids we run
  compute-delta-then-commit: read `delta = newValue -
  peerAdvertised[id]` first, apply
  (`HpackEncoder.onPeerAdvertisedCapChanged` for
  `HEADER_TABLE_SIZE`; `outboundStreamWindow += delta` on the
  outbound-direction-active set `open` / `half-closed-remote` per
  §7 for `INITIAL_WINDOW_SIZE`; direct field updates for the
  others), then assign `peerAdvertised[id] = newValue`. Only after
  all applies complete do we emit the SETTINGS ACK so the wire
  order reflects our state.

  An unknown id with a value that would have been malformed under
  a known id (e.g. `ENABLE_PUSH = 2` would have been
  `PROTOCOL_ERROR` if id were `ENABLE_PUSH`, but the same byte
  pattern at id `0x0f` is simply ignored) is **not** a protocol
  error. This is the forward-compatibility surface the RFC carves
  out, and the test in §13.1 covers it directly.

## 11. Integration with HPACK

HEADERS / CONTINUATION payloads carry HPACK field block fragments
(possibly wrapped by PADDED / PRIORITY framing bytes — the frame
reader already strips those; see `HPACK_CODEC.md` §13).

**Block assembly.** A single **connection-scoped** native scratch
buffer (`blockScratchAddr`, capacity 16 KiB by default, configurable)
holds the concatenated field-block fragments while the block is
assembling. Per-connection is sufficient — RFC 7540 sec. 6.10 +
the frame reader's CONTINUATION sequencing forbid interleaving HEADERS /
CONTINUATION across streams, so at most one block is under assembly at
any moment. On each HEADERS / CONTINUATION the context appends the
fragment to the scratch; on END_HEADERS it hands
`(blockScratchAddr, blockScratchAddr + accumulatedLen)` to
`hpackDecoder.decodeBlock(...)` with an inline `HpackListener` that
routes each `onHeader` call (see below). The accumulated length is
tracked in the context; a fragment that would push it past the
scratch capacity throws `Http2ConnectionException(COMPRESSION_ERROR)`
before any more bytes are copied.

This scratch is **distinct** from the two scratch buffers owned by
the HPACK codec: (1) `HpackDecoder`'s per-decode literal /
name-staging scratch (`HPACK_CODEC.md` §12) — configured to
**24 KiB** for this layer so a single literal whose decoded size
is at or just above the 16 KiB `HEADER_LIST_POLICY_CAP` still fits
the HPACK buffer, letting the stream-layer policy counter fire
first (see "Decoded-header-list hard cap" below for the sizing
discipline); this is a configuration contract between the two
layers — if `HpackDecoder` is built with the older default 16 KiB
literal scratch, the policy-cap-first regression in §13.1 will
fail with a connection `COMPRESSION_ERROR` instead of the expected
stream `PROTOCOL_ERROR`. And (2) the dynamic table's internal
staging buffer. This context-level scratch only concatenates
fragment bytes; HPACK decode itself uses the decoder's own
scratch once decode starts.

**Listener wiring.** The `HpackListener` the context supplies
branches by slot kind (§5) and, for LIVE slots, by
`initialHeadersSeen`:

- **DISCARDING_BLOCK slot (concurrency-refused / post-GOAWAY), or
  LIVE slot with `refusingCurrentBlock == true` (§8 M1 trailer
  refusal).** Every emission is dropped: no pseudo-slot capture, no
  upward callback, no per-stream accounting. Field validators
  (forbidden-header, header-list-size, name, value, content-length)
  also do not run — the refusal reason is final (see §11
  "sticky-error precedence"), so there is nothing to escalate to.
  HPACK decode itself proceeds normally to keep the dynamic table
  coherent.
- **LIVE slot, `initialHeadersSeen == false` (initial HEADERS).**
  Pseudo-headers (names starting with `:`) are captured into
  per-stream slots (`methodSlot`, `schemeSlot`, `pathSlot`,
  `authoritySlot`) for dispatch. `HPACK_CODEC.md` §12 only
  guarantees that the `(nameAddr, valueAddr)` pointers it hands to
  `onHeader` are stable for the duration of that **one callback**
  — the decoder's scratch can rewind or overwrite between fields.
  So the copy into each per-stream slot happens **synchronously
  inside `onHeader`**, before the call returns: append the
  `(name, value)` bytes to a per-stream scratch buffer and store
  `(slotAddr, slotLen)` pointing into that per-stream scratch.
  The pseudo-header slots are then valid for the entire block and
  can be read safely at END_HEADERS for required-set validation
  and `:authority` / Host reconciliation.

  Regular fields (non-`:`) are **not** relayed to the listener
  per-field during decode. Instead they are staged into the same
  per-stream scratch as the pseudo-headers — append a
  `(nameOffset, nameLen, valueOffset, valueLen, flags)` row to
  the `HEADER_STAGING_TUPLES` `int[]` table (see §11 "Decoded-
  header-list hard cap" for the split between the native
  `HEADER_STAGING_BYTES` buffer and the primitive-int tuple
  table) synchronously inside `onHeader`. At
  END_HEADERS, the aggregate dispatch reads `pendingStreamError`;
  on `NONE` it walks the staging list and fires
  `streamListener.onRequestHeader(...)` per field **then**
  `onRequestHeaders(..., endStream)`. On a non-`NONE` sticky
  error — including the terminal content-length equality check
  that suppresses the aggregate callback — the staging list is
  simply dropped, so `onRequestHeader` never fires on an
  eventually-rejected request. This keeps the contract "a
  rejected initial HEADERS block surfaces zero per-field
  callbacks" honest, at the cost of one `long[]` pointer list per
  in-flight initial-HEADERS block.

  Trade-off acknowledged: staging costs the small list allocation
  (reusable across requests since `Http2Stream` is pool-backed)
  and a second pass over the decoded fields. The alternative — a
  "per-field fires during decode, aggregate may not" contract —
  forces every handler to track partial state and discard on
  `onStreamClosed`, which is error-prone enough that paying the
  staging cost here is the right call. The handler still observes
  the one-callback-lifetime rule on the relayed `onHeader`
  pointers (they point into per-stream scratch, stable for the
  relay dispatch but not beyond).
- **LIVE slot, `initialHeadersSeen == true` (trailers, M2 only).**
  Pseudo-headers are illegal in trailers per RFC 9113 sec. 8.1 —
  set `pendingStreamError` (§11 validation rules). Regular fields
  are **not** relayed through `onRequestHeader`; instead they are
  captured into the same split `(HEADER_STAGING_BYTES,
  HEADER_STAGING_TUPLES)` storage pair used for initial headers
  — the buffer is reused between the initial-header phase and
  the trailers phase since they are temporally disjoint on a
  single stream. The aggregate `onTrailers` callback at
  END_HEADERS walks this staging structure; if the sticky error
  flag tripped, `onTrailers` is suppressed entirely. This prevents
  malformed-trailer fields from surfacing as request headers before
  the trailer block is validated.

Both LIVE paths feed through the same sticky-error flag, so a
validation failure on either an initial header or a trailer
suppresses the matching aggregate callback without unwinding HPACK
decode.

**Validation is deferred, not aborting.** The listener must never
throw `Http2StreamException` out of `onHeader`: doing so would
unwind `HpackDecoder.decodeBlock` mid-block, leaving indexed and
literal-with-incremental-indexing entries that follow the offending
field un-applied to the shared dynamic table. Every subsequent
stream on the connection would then decode against a desynchronised
table and the whole connection would have to be torn down as
`COMPRESSION_ERROR` — a stream-scope fault escalated to a
connection-scope fault.

Instead the context carries a **sticky per-block stream-error flag**
`pendingStreamError ∈ {NONE, code+debug}` cleared at the start of
each HEADERS block. Any stream-scope validation failure sets this
flag and returns from the `onHeader` callback normally. Decode
continues to END_HEADERS so the dynamic table keeps advancing
through the remaining fields of the block. At END_HEADERS the
context inspects the flag:

- `NONE` — dispatch per-stream header / trailers normally.
- Non-NONE — discard the stream's collected pseudo-header slots,
  skip `onRequestHeaders` / `onTrailers` dispatch, and emit
  `RST_STREAM(pendingStreamError.code)`. `activeStreamCount`
  rolls back, a `LOCAL_RESET` tombstone is installed per §5 so
  any DATA already in flight on the stream gets the minimal-
  processing treatment.

While `pendingStreamError != NONE` the context also suppresses
further upward `onRequestHeader` / pseudo-slot captures for the
same block (pointless work — we know the block will be
discarded), but still runs HPACK decode. Validators
(forbidden-header, header-list-size, name, value) **stop running
once `pendingStreamError` has been set** — the first validation
failure wins and the error code does not change afterwards. This
makes the error-code attribution deterministic (a test asserting
"missing `:path` yields `PROTOCOL_ERROR`" stays stable even if a
later field would also have tripped a validator) and matches the
same rule used for refused / `refusingCurrentBlock` slots (§5
pool-entry-kinds).

**Sticky-error precedence — one rule, three cases:**

| Slot kind                                | Error code source        | Validators run?                                                                |
|------------------------------------------|--------------------------|--------------------------------------------------------------------------------|
| LIVE (normal)                            | `pendingStreamError` set by first validator failure; later failures no-op | Yes until first failure, stop once `pendingStreamError != NONE`                |
| LIVE with `refusingCurrentBlock == true` | refusal reason (fixed)   | No — final reason is M1-trailer `PROTOCOL_ERROR`                               |
| DISCARDING_BLOCK                         | slot's `reason` (fixed)  | No — final reason is the refusal cause (`REFUSED_STREAM`, `ENHANCE_YOUR_CALM`, …) |

In all three cases HPACK decode itself proceeds to END_HEADERS so
the dynamic table stays coherent.

**Pseudo-header validation rules.** RFC 9113 sec. 8.3 (supersedes
7540 sec. 8.1.2.1). All five of these are "malformed request"
detections, and RFC 9113 sec. 8.1.1 specifies malformed requests
MUST be treated as stream errors of type `PROTOCOL_ERROR`. Each
failure below sets `pendingStreamError = (PROTOCOL_ERROR, "...")`:

- Pseudo-headers must precede regular headers. Enforced by tracking
  `regularHeaderSeen` per block.
- An unknown `:`-prefixed name is `PROTOCOL_ERROR`.
- A duplicate pseudo-header is `PROTOCOL_ERROR`.
- Request pseudo-headers required — the rule branches on
  `:method`:
    - **CONNECT method** (RFC 9113 sec. 8.5): `:method == CONNECT`.
      Required: `:method` and `:authority`. Forbidden: `:scheme`
      and `:path` (their presence on a CONNECT request is
      malformed). The request travels to the handler layer,
      which decides whether to honour it (QuestDB's HTTP handler
      currently does not implement CONNECT tunnelling and returns
      an appropriate HTTP status — but that is a request-layer
      concern, not a malformed-HEADERS-block concern. This layer
      must accept a well-formed CONNECT and let the handler
      respond.)
    - **All other methods**: required `:method`, `:scheme`,
      `:path`. `:authority` is allowed; RFC 9113 sec. 8.3.1 now
      makes it recommended rather than strictly required, but we
      accept
      either.

  Missing any of the required-set for a given method, or
  presence of any method-forbidden pseudo-header (`:scheme` /
  `:path` on CONNECT), sets
  `pendingStreamError = (PROTOCOL_ERROR, ...)` — checked at
  END_HEADERS before dispatch, on the same dispatch path that
  reads `pendingStreamError`.
- Response-side checks only apply to PUSH_PROMISE, which the server
  never receives.

**`:authority` syntax and `Host` reconciliation.** RFC 9113 sec.
8.3.1 gives us two closely-related rules to enforce on the HEADERS
block, both in the malformed-request bucket:

- **No userinfo in `:authority`.** For `http` and `https` schemes
  the `userinfo` component (the `name@` prefix inside a
  `host[:port]` authority) is forbidden. A `:authority` whose
  bytes contain `@` is malformed.
- **`:authority` and `Host` must agree** when both are present.

This layer owns malformed-request detection, so both rules fire as
sticky stream errors (`PROTOCOL_ERROR`) at END_HEADERS.

The `HpackListener` captures the first `Host` header it sees into
a per-stream `hostSlot`. The `Host` field is **also** a regular
header, so the synchronous copy that stages it for the listener
relay (the `(nameAddr, nameLen, valueAddr, valueLen)` tuple in
`HEADER_STAGING_BYTES`) is the single copy — `hostSlot.addr`
and `hostSlot.len` point **at the same bytes inside the staged
regular-header tuple**, not at a duplicate copy. This keeps
storage sizing predictable: `HEADER_STAGING_BYTES` does not
need extra room for the Host value, and the same-pointer sharing
means a large Host header counts against the policy cap exactly
once. The slot's `(addr, len)` stays valid for the whole block
because the staged tuple it aliases does. At END_HEADERS, on the
dispatch path that already reads `pendingStreamError`:

1. **Userinfo check.** Scan `authoritySlot` for `0x40` ("@"); any
   occurrence sets
   `pendingStreamError = (PROTOCOL_ERROR, ":authority contains
   userinfo")`. Cheap linear byte scan.
2. **Pair-presence reconciliation.** If `authoritySlot` is present
   and `hostSlot` is present, compare them under a **minimal
   normalization rule** sufficient for HTTP/2 field equality:
   case-insensitive ASCII byte equality (RFC 3986 says the host
   subcomponent is case-insensitive). The comparison stops at the
   first mismatch; a mismatch sets
   `pendingStreamError = (PROTOCOL_ERROR, ":authority and host
   disagree")`.
3. **Absence case.** If `authoritySlot` is absent and `hostSlot`
   is absent, accept here — the request layer above decides
   whether the request is resolvable, because that decision needs
   the full URI / target context.

**Scope of the comparison.** ASCII case-fold byte equality is the
authoritative rule at this layer: if the bytes differ after that
minimal fold, the request is malformed and we `RST_STREAM`.
Default-port elision (`:80` for http, `:443` for https),
percent-encoding folding, and IDN (Unicode / punycode) conversion
are **not** performed — a request whose `:authority` and `Host`
differ only by one of those also gets rejected here. This is the
stricter of the two possible postures (the other being "always
capture, never reject, defer all comparison to the handler") and
matches how other HTTP/2 servers behave in practice. Clients that
round-trip a request through any reasonable URL library emit
byte-identical values for both fields; a mismatch is almost
always a client bug or an attack. RFC 9113 sec. 8.3.1's phrase
"normalization before comparison" gives implementations latitude
on the algorithm; we take the cheap end of that latitude.

**Repeated `Host`.** Two `Host` fields with differing values →
`PROTOCOL_ERROR` via the same sticky-error path as differing
`content-length`. Two identical `Host` values are accepted (RFC
9110 sec. 5.3 allows repeated field lines with identical values).
A single `Host` value containing an internal comma is not parsed
here — the upstream handler sees it verbatim, matching how we
treat other free-form header values.

**Connection-scope HPACK failures are different.** `HpackException`
thrown by the decoder itself (structural corruption: invalid prefix
integer, dynamic-table size update above the advertised cap,
truncated string, etc.) is `COMPRESSION_ERROR` at connection scope
— the dynamic table is *already* desynchronised and there is
nothing to salvage. That exception does unwind `decodeBlock`; the
context wraps it as `Http2ConnectionException(COMPRESSION_ERROR)`
and emits GOAWAY.

**Forbidden regular headers.** RFC 9113 sec. 8.2.2 disallows
`Connection`, `Keep-Alive`, `Proxy-Connection`, `Transfer-Encoding`,
and `Upgrade` in HTTP/2 messages. `TE` is allowed only with the
exact value `trailers`. The context checks these at the
`HpackListener.onHeader` callback by comparing `(nameAddr, nameLen)`
against a small FNV-indexed table of forbidden names; a match sets
`pendingStreamError = (PROTOCOL_ERROR, "forbidden HTTP/2 header")`
via the same sticky-flag path as pseudo-header validation, so the
decode still runs to END_HEADERS.

**Field name and value validation.** RFC 9113 sec. 8.2.1 requires
field names to be lowercase ASCII and to contain only the HTTP
token characters (sec. 5.1 of RFC 9110), and forbids control
characters (specifically NUL, CR, LF) in field values. HPACK
transport is case-preserving, so the wire may legally carry an
uppercase name that the decoder faithfully delivers; it is this
layer's job to reject it. `HpackListener.onHeader` runs two
post-decode checks on every emitted field:

- **Name validation.** The bytes at `(nameAddr, nameLen)` must be
  non-empty and consist entirely of lowercase ASCII letters,
  digits, or the token punctuation ``!#$%&'*+-.^_`|~`` (RFC 9110
  sec. 5.6.2 tchar set minus uppercase). The `:`-prefix for
  pseudo-headers is permitted as the leading byte.
  Any other byte (any uppercase ASCII, any non-ASCII, any ASCII
  control, any non-token punctuation) sets
  `pendingStreamError = (PROTOCOL_ERROR, "invalid field name")`.
- **Value validation.** The bytes at `(valueAddr, valueLen)` must
  not contain `0x00` (NUL), `0x0A` (LF), or `0x0D` (CR). A
  violation sets
  `pendingStreamError = (PROTOCOL_ERROR, "invalid field value")`.

Both run through the sticky-flag path so HPACK decode continues to
END_HEADERS and the dynamic table stays coherent. Both loops are
byte-scan over native memory ranges — cheap, and off the HPACK
hot path.

**Content-length accounting.** RFC 9113 sec. 8.1.1 (pointing at
RFC 9110 sec. 8.6): when a request carries `content-length`, the
sum of DATA payload sizes (excluding padding) MUST equal the
declared value; a mismatch is a malformed request (stream error
`PROTOCOL_ERROR`). Two multi-valued edge cases:

- **Repeated `content-length`.** If a request contains two
  `content-length` fields with different values, it is malformed.
  HPACK decodes each occurrence as a separate `onHeader` call, so
  the context tracks the first value seen and stream-errors on any
  subsequent different value. Two fields with identical values are
  accepted (RFC 9110 sec. 8.6.2 permits this).
- **Non-numeric, comma-bearing, or overflowing value.** A value
  whose bytes are not exclusively ASCII digits is malformed
  (stream `PROTOCOL_ERROR`). The value is parsed into
  `declaredContentLength` digit-by-digit into `long` with an
  explicit overflow check before each multiply-by-10 — a value
  that would exceed `Long.MAX_VALUE` is also malformed, stream
  `PROTOCOL_ERROR`. The overflow check is necessary because
  downstream accounting (`receivedBodyLen`, comparison against
  `declaredContentLength`) is `long`; silently wrapping would let
  a maliciously-crafted content-length header match a much
  smaller DATA stream. This rejection also covers the comma-list
  form permitted by RFC 9110 sec. 8.6.2 ("42, 42"): it is rare in
  practice, complicates the validator, and nothing in gRPC /
  Flight SQL emits it. A handler that cares can be added in M2 if
  a real client needs it.

Enforcement has two arrival paths, both **before** any handler
callback dispatches: a **per-DATA pre-dispatch check** that catches
excess body bytes (the declared length is too small for the
body actually arriving), and a **terminal-edge equality check** that
catches early END_STREAM (the declared length is too large for the
body the peer ended up sending). In both cases a mismatch must
suppress the terminal callback — otherwise the handler can
observe `endStream=true` on a frame that we're about to
`RST_STREAM`, breaking the listener contract's "exactly one
`endStream=true` callback per completed request" promise.

1. `HpackListener.onHeader` captures the declared value into a
   per-stream `declaredContentLength` (default `-1` meaning absent)
   while validating it.
2. `stream.onData(addr, dataLen, paddedLen, paddingOverhead,
   endStream)` runs the **pre-dispatch excess-body check**
   (declared length too small) **before** the terminal-edge
   equality check and **before** relaying to the listener. Only
   `dataLen` matters for content-length accounting — `paddedLen` /
   `paddingOverhead` are consumed by §7 flow-control, not by the
   handler. The comparison must avoid `long` overflow — a
   malicious `content-length` near `Long.MAX_VALUE` plus enough
   DATA could otherwise wrap `receivedBodyLen + dataLen` and bypass
   the check. Written safely:

   ```java
   if (declaredContentLength >= 0 &&
           dataLen > declaredContentLength - receivedBodyLen) {
       // excess body, declared length too small: malformed
   }
   ```

   This works because `receivedBodyLen` is maintained in the
   invariant range `0 <= receivedBodyLen <= declaredContentLength`
   (we reset the stream and stop accumulating the moment the
   pre-dispatch check fires), so `declaredContentLength -
   receivedBodyLen` is always non-negative and does not overflow.
   When the check trips, the frame-level flow-control debit (§7
   step 1) has already run at the top of the dispatch path, so
   there is nothing to debit here. Suppress the upward
   `streamListener.onData` call so the handler never sees the
   excess bytes, emit `RST_STREAM(PROTOCOL_ERROR)`, install a
   `LOCAL_RESET` tombstone, and drop the payload. Credit the
   discarded-byte count back to the connection window through the
   normal coalescing path (§7 step 5) so the stream-level abuse
   doesn't starve unrelated live streams; the per-stream window
   stays dead because the stream is now reset. Otherwise
   accumulate `receivedBodyLen += dataLen` (`dataLen` excludes
   padding per §7) and proceed to step 3.
3. **Pre-dispatch terminal equality check.** On any frame that
   would drive the terminal callback with `endStream=true`
   (initial HEADERS with `END_STREAM`, DATA with `END_STREAM`,
   trailer HEADERS with `END_STREAM`), evaluate
   `declaredContentLength >= 0 && receivedBodyLen !=
   declaredContentLength` **before the callback fires**. A
   mismatch at this edge means the peer ended early (declared
   length too large for the body actually sent); emit
   `RST_STREAM(PROTOCOL_ERROR)`, install a `LOCAL_RESET`
   tombstone, and **suppress** the terminal callback. For the
   DATA-with-`END_STREAM` shape specifically, this frame's
   `dataLen` has already debited the connection window (§7
   step 1) but is never delivered to the handler, so credit
   `dataLen` back to the connection window through the coalesced
   `WINDOW_UPDATE(0)` path **before** the reset lands — §7 step 5
   table row "content-length declared too large" formalises this.
   The three arrival shapes:

    - **Initial HEADERS with `END_STREAM`** (no body). Check
      `declaredContentLength <= 0`. On mismatch, suppress
      `onRequestHeaders` and reset — the handler never learns
      about this request. On match (or no `content-length`
      advertised) dispatch `onRequestHeaders(..., endStream=true)`
      as the request-complete signal.
    - **DATA with `END_STREAM`**. Fire step 2's excess-body check
      first (it's already part of this frame's processing).
      Then check `receivedBodyLen == declaredContentLength`; if
      the peer ended early (`receivedBodyLen <
      declaredContentLength`), suppress the `onData(...,
      endStream=true)` delivery and reset. Only a clean equality
      (or no `content-length`) lets the terminal `onData` reach
      the handler.
    - **Trailer HEADERS with `END_STREAM`** (M2). Step 2's
      excess-body check has already caught the too-small case
      during the preceding DATA frames. The terminal check fires
      on early end; a mismatch suppresses `onTrailers` via the
      sticky-error path (§11) and emits the RST_STREAM at trailer
      block completion.

The running sum is per-stream `long` so a large request cannot
overflow an int.

**Trailers detection.** The stream's `initialHeadersSeen` boolean
distinguishes the two HEADERS arrivals:

- First HEADERS on a stream: `initialHeadersSeen` was `false`,
  validate pseudo-headers + required-set, set `initialHeadersSeen =
  true`, dispatch to `onRequestHeaders`.
- Second HEADERS on a stream: `initialHeadersSeen` was `true`, this
  is trailers per RFC 9113 sec. 8.1. Trailers MUST carry `endStream`;
  a trailers frame without it sets
  `pendingStreamError = (PROTOCOL_ERROR, "trailers missing END_STREAM")`.
  Trailers MUST NOT contain pseudo-headers; a `:`-prefixed name in
  trailers sets `pendingStreamError = (PROTOCOL_ERROR, ...)`. On
  clean exit trailers dispatch to `onTrailers`.
- Third HEADERS on a stream: never legal. Rejected at state-table
  dispatch (§5 — only `half-closed (local)` permits trailers and the
  transition moves the stream to `closed`).

**Decoded-header-list hard cap (M1).** RFC 9113 sec. 6.5.2 defines
`MAX_HEADER_LIST_SIZE` as a decoded-bytes policy measured as
`32 + name_len + value_len` summed across every field line. The 32
bytes per field account for the map-entry overhead a downstream
HTTP request object would pay, even though they are not bytes on
the wire. We need three distinct quantities, each capped
independently at the stream layer so a misbehaving peer is caught
as a stream error rather than escalating to the HPACK connection-
error path:

- **`HEADER_LIST_POLICY_CAP` (16 KiB in M1).** The RFC-metric
  policy: `policySum += 32 + nameLen + valueLen` on every field
  (pseudo-header, regular header, Host). This is the number we
  will advertise as `SETTINGS_MAX_HEADER_LIST_SIZE` in M2.
  Overflow on this counter sets
  `pendingStreamError = (PROTOCOL_ERROR, "header list too large")`
  via the sticky-flag path.
- **`HEADER_STAGING_BYTES` (native byte buffer).** Per-stream
  contiguous **native** scratch that holds only the copied name
  and value bytes of pseudo-header slots plus staged regular-
  header fields. The `hostSlot` is an aliased view into the
  staged `host` tuple's value range, not a duplicate (see "Scope
  of the comparison" below). Sized to `HEADER_LIST_POLICY_CAP`
  bytes so any block that fits under the policy cap physically
  fits. Overflow of the native buffer (impossible under a
  well-behaved peer if the policy cap fires first; defensive)
  also sets `pendingStreamError`.
- **`HEADER_STAGING_TUPLES` (primitive int array, 600 tuples in
  M1).** Separate, **non-native** `int[]`-backed table indexing
  into `HEADER_STAGING_BYTES`. One tuple is four ints —
  `nameOffset`, `nameLen`, `valueOffset`, `valueLen` — stored
  contiguously in a flat `int[HEADER_STAGING_TUPLES * 4]`, with
  a separate `int[HEADER_STAGING_TUPLES]` for per-field flags
  (e.g. `neverIndexed`). The two arrays are plain Java primitive
  storage owned by `Http2Stream`; they are **not** native memory
  and **not** an `ObjList<Object>` (which would be per-field
  allocations on the hot path). The cap of 600 tuples is chosen
  so the policy cap (`HEADER_LIST_POLICY_CAP`, 16 KiB) binds
  first even when every field is minimum-size: worst case is
  one-byte-name + one-byte-value fields, where `32 + 1 + 1 = 34`
  bytes of policy per field, so `16 KiB / 34 ≈ 482` fields
  suffice to exhaust the policy cap. 600 gives enough headroom
  for realistic blocks with a few dozen headers without ever
  being the binding constraint. Overflow of the tuple table
  (defensive path, same argument as byte overflow) → stream
  `PROTOCOL_ERROR` via `pendingStreamError`.

The two parts are sized and allocated independently — the native
byte buffer is 16 KiB; the Java `int[]` tuple tables add
approximately `600 × 5 × 4 B = 12 KiB` of heap, allocated once
per `Http2Stream` and reused across requests. Total per-stream
steady-state footprint for header staging is ≈ 28 KiB, no
per-request allocations.

**HPACK-decoder scratch sizing.** `HPACK_CODEC.md` §12 describes a
per-decode literal / name-staging scratch that the decoder throws
`HpackException` on overflow, which §12 of this doc maps to
**connection** `COMPRESSION_ERROR`. To keep oversized-header-list
violations in the stream-error bucket, the HPACK decoder's
literal scratch is sized strictly larger than
`HEADER_LIST_POLICY_CAP` (M1: 24 KiB vs 16 KiB policy) so the
decoder can always hold one field whose decoded size is at or
just over the policy cap. The stream-layer policy counter then
trips first on the oversized block and converts it to a stream
error; the HPACK scratch-cap overflow path only fires on a
genuinely structural failure (an individual literal so malformed
that even the generous HPACK buffer can't hold it), which is
correctly a connection `COMPRESSION_ERROR`. This sizing discipline
is what lets us describe "decoded-list too large" as a
stream error consistently.

**Encoded block-assembly scratch.** Separate from the decoder's
literal scratch and independent of the staging caps: the 16 KiB
connection-level block-assembly scratch (§11 "Block assembly")
bounds the encoded HPACK bytes before decode starts. A block
whose encoded fragment stream overflows it is a connection
`COMPRESSION_ERROR`, because the peer has violated either our
MAX_FRAME_SIZE advertisement or spread one header block across
an unreasonable number of CONTINUATIONs.

Enforcement order inside each `onHeader` callback (for staged
slots — pseudo-headers, Host, regular initial-header fields):

1. Compute `addend = 32 + nameLen + valueLen`.
2. If `policySum + addend > HEADER_LIST_POLICY_CAP` → set
   `pendingStreamError`, drop this field's copy, return.
3. If storage or tuple capacity would overflow → same sticky
   error, return. (Not reachable under a compliant peer given
   step 2 fires first; kept as defence-in-depth.)
4. Copy bytes into staging, append tuple, update `policySum`,
   update per-stream storage cursor.

HPACK decode runs to END_HEADERS regardless so the dynamic table
stays coherent; `RST_STREAM(PROTOCOL_ERROR)` fires at the block
boundary along with every other malformed-request outcome.

M1 does not advertise `MAX_HEADER_LIST_SIZE` on the wire, but the
policy is enforced internally, so there is no "M1 gap" for
memory safety. An adversarial peer cannot blow past memory via
indexed references to short names — the expansion trips the
policy counter long before the physical staging runs out.

**M2 refinement.** Emit `SETTINGS_MAX_HEADER_LIST_SIZE` matching
`HEADER_LIST_POLICY_CAP` (§15 picks the exact value — the 16 KiB
M1 default is likely the M2 advertised number too) so compliant
clients avoid sending blocks we will reject. The enforcement
mechanism is unchanged — the per-field policy counter already
runs in M1; M2 just makes the policy visible on the wire.

## 12. Error Handling

Two scopes.

**Stream errors (RFC 7540 sec. 5.4.2).** Affect one stream; recover
by sending `RST_STREAM(errorCode)`. Examples: over-limit
concurrency (`REFUSED_STREAM`), flow-control underflow on a single
stream, malformed HEADERS on a single stream
(`PROTOCOL_ERROR`-as-stream-error if the rest of the connection is
salvageable).

**Connection errors (RFC 7540 sec. 5.4.1).** Kill the entire
connection; emit `GOAWAY(lastAcceptedPeerStreamId, errorCode, debug)`
(see §6 for why that is not `highestPeerStreamIdSeen`) then
close the socket after a short flush window (§15). Examples: preface
mismatch, frame-size violation, HPACK `COMPRESSION_ERROR`,
misordered CONTINUATION, flow-control overflow on the connection
window, PROTOCOL_ERROR on frames without a stream scope.

**Mapping our exceptions.** Already set up:

- `Http2ConnectionException` — carries an error code + ASCII debug
  string; handler emits GOAWAY. `HpackException` (HPACK structural
  failure) is the only non-connection-family exception that maps
  into this bucket: at the context boundary the decoder's throw is
  caught and re-thrown as `Http2ConnectionException(COMPRESSION_ERROR,
  hpackException.getDebug())`.
- `Http2StreamException` — carries an error code + stream id;
  handler emits RST_STREAM.

**Graceful shutdown — we send GOAWAY.** Triggered by a connection
error, server shutdown, or hitting the stream-id ceiling (§6). The
context emits `GOAWAY(lastAcceptedPeerStreamId, errorCode, debug)`.
`lastAcceptedPeerStreamId` (§6) is the highest client-initiated id
we admitted into the pool — streams we already `REFUSED_STREAM`-ed
are deliberately excluded so the client knows they are safe to
retry on a new connection. After the GOAWAY is flushed we wait up
to the §15 flush window for in-flight responses to drain, then
close the socket. While waiting:

- streams already in flight with `id ≤ lastAcceptedPeerStreamId`
  continue through their normal transitions;
- new `HEADERS` with `id > lastAcceptedPeerStreamId` are refused
  with `RST_STREAM(REFUSED_STREAM)`, but the stream's frames must
  still be **minimally processed** per RFC 9113 sec. 6.8:
  `HEADERS` / `CONTINUATION` run through HPACK to keep the
  dynamic table coherent; any `DATA` that the peer may have
  already launched debits the connection-level inbound window;
  callbacks and payloads are discarded. This is the same
  discard-but-process path as the locally-reset-tombstone rule
  in §5. `highestPeerStreamIdSeen` still advances so the
  monotonic-id rule keeps holding.

**Graceful shutdown — peer sends GOAWAY.** The peer is a client,
and its `lastStreamId` names the highest **server-initiated** stream
it will still process (see §5 GOAWAY paragraph). Because we never
push, this value is effectively advisory: it does not identify any
live client-initiated request. On receipt we enter `DRAINING`:

- Continue servicing every in-flight client-initiated stream to
  normal completion. We do **not** `RST_STREAM(CANCEL)` based on
  `peer.lastStreamId` — that would cancel the client's own
  requests, which is the opposite of the GOAWAY semantics.
- Refuse any new client `HEADERS` that arrive after receipt with
  `RST_STREAM(REFUSED_STREAM)` — the client is clearly winding the
  connection down.
- Close the socket when `activeStreamCount == 0`.

If `peer.errorCode != NO_ERROR` (peer is aborting, not draining),
treat the remaining active streams as unrecoverable: we're allowed
to `RST_STREAM(CANCEL)` them to release handler state, since the
peer is about to close the socket regardless.

## 13. Testing Strategy

Tiered, consistent with the frame-codec and HPACK doc patterns.

### 13.1 Tier 1 — unit state-machine

Direct unit tests over `Http2Stream` and `Http2ConnectionContext` with
hand-crafted frame sequences. Cover:

- Every legal transition in §5 once.
- Every illegal-in-state transition → expected error code (stream or
  connection).
- The five state-change edges that have distinct "send" vs "recv"
  triggers (open → half-closed-local vs half-closed-remote, etc.).
- Stream-id rules: monotonic (against `highestPeerStreamIdSeen`),
  odd-client, implicit idle → closed, GOAWAY carries
  `lastAcceptedPeerStreamId` and **not** ids we REFUSED_STREAM-ed.
- CONTINUATION ordering (frame reader tested; context tested via a
  sequence that exercises the handoff).
- Refused-stream decode coherence: a block refused for concurrency
  (§8) or M1 trailers (§11) still advances the dynamic table, so a
  subsequent well-formed request on a fresh stream decodes
  correctly after indexed references.
- Field-name / value / content-length validation (§11): uppercase
  name, token-violating name, NUL in value, LF in value, CR in
  value, two differing `content-length` fields, non-numeric
  `content-length`, overflowing `content-length` (value past
  `Long.MAX_VALUE`).
- Header-list policy-cap enforcement (§11
  `HEADER_LIST_POLICY_CAP`): construct an initial HEADERS block
  whose encoded bytes fit the 16 KiB connection-level block-
  assembly scratch but whose RFC-metric sum `32 + nameLen +
  valueLen` across all fields exceeds 16 KiB (indexed references
  to short names expand to many long-value fields, or many
  small fields where each field's 32-byte overhead dominates).
  Assert `RST_STREAM(PROTOCOL_ERROR)` at block boundary, no
  `onRequestHeader` / `onRequestHeaders` dispatch, HPACK decode
  still runs to END_HEADERS, and a subsequent well-formed stream
  decodes correctly. Include the "many small fields" shape — 483
  fields each with 1-byte name and 1-byte value, yielding
  `483 × (32 + 1 + 1) = 16 422 > 16 384` — to prove the 32-byte
  overhead per field participates in the counter. This shape fits
  under the 600-tuple `HEADER_STAGING_TUPLES` cap so the policy
  counter is the binding constraint, not the tuple cap (which
  would have been the case under an earlier 128-tuple design).
- Stream-error priority over HPACK scratch (§11 sizing
  discipline): a single literal field whose decoded size is
  larger than `HEADER_LIST_POLICY_CAP` (e.g. a 20 KiB decoded
  header value in an otherwise empty block). The encoded block
  must fit the 16 KiB connection-level block-assembly scratch
  (§11 "Block assembly") or the connection tears down with
  `COMPRESSION_ERROR` before HPACK decode even starts — choose
  a highly Huffman-compressible value (e.g. a 20 KiB run of a
  single ASCII letter encodes to ~12.5 KiB under HPACK Huffman,
  well under 16 KiB). Assert this trips the stream-layer policy
  counter as `RST_STREAM(PROTOCOL_ERROR)`, **not** the HPACK
  decoder's literal scratch as connection `COMPRESSION_ERROR`,
  and **not** the encoded-block-assembly scratch as connection
  `COMPRESSION_ERROR`. Regression guard that the HPACK decoder
  scratch is sized strictly larger than the policy cap (24 KiB
  vs 16 KiB per §11).
- Content-length **declared-too-small** (excess body) — the
  pre-dispatch check:
  - `content-length: 5`, first DATA of length 3 delivered to
    `onData` once, second DATA of length 4 triggers
    `RST_STREAM(PROTOCOL_ERROR)` before `onData` is called for
    that frame. Assert exactly one `onData` callback for the
    whole stream.
  - `content-length: 5`, a single DATA frame of length 8 with
    `END_STREAM=true`. The check fires before the terminal
    callback, so `onData` is **never** called with
    `endStream=true`; `onStreamClosed` reports the reset. No
    request-complete signal reaches the handler.
- Content-length **declared-too-large** (early end) — the
  terminal equality check:
  - `content-length: 10`, DATA of length 5 with `END_STREAM=true`.
    Assert `RST_STREAM(PROTOCOL_ERROR)` fires, and `onData` is
    **not** called with `endStream=true`. Exactly zero terminal
    callbacks observe `endStream=true` on this stream — this is
    the regression guard for the "completion callback leaks
    through declared-too-large body" bug.
  - Initial HEADERS with `END_STREAM` and non-zero
    `content-length` (e.g. `content-length: 7`), carrying at
    least one regular header (e.g. `x-foo: bar`) to exercise
    the staging path. Assert `RST_STREAM(PROTOCOL_ERROR)`, **no**
    `onRequestHeaders` dispatch, and **no** `onRequestHeader`
    dispatch for the staged regular header — the entire request
    is suppressed from the handler, including per-field
    callbacks. Regression guard for the per-field-callback-leak
    bug.
  - Trailer HEADERS with `END_STREAM` and `receivedBodyLen <
    declaredContentLength` (M2). Assert `RST_STREAM` fires at
    trailer block completion and `onTrailers` is not called.
- HPACK state preserved on sticky stream-error path: after any of
  the above rejections, a subsequent well-formed request on a new
  stream decodes correctly against the shared dynamic table
  (regression guard for the "malformed header aborts decode
  mid-block" class of bugs).
- Unknown SETTINGS id tolerance (§10 peer-apply rule): peer
  sends a SETTINGS frame containing a known identifier (e.g.
  `INITIAL_WINDOW_SIZE`) paired with an **unknown identifier**
  `0x0f` carrying an arbitrary value. Assert the context emits
  SETTINGS ACK, the known identifier is applied, and no array
  access for id `0x0f` occurs (regression guard against
  off-by-one `long[7]` indexing).
- CONNECT method validation (§11 required-set rule):
  well-formed CONNECT (`:method: CONNECT`, `:authority:
  host:port`, no `:scheme`, no `:path`) → accepted by this
  layer (handler can then reject with an HTTP status);
  CONNECT missing `:authority` → stream `PROTOCOL_ERROR`;
  CONNECT with `:scheme` → stream `PROTOCOL_ERROR`;
  CONNECT with `:path` → stream `PROTOCOL_ERROR`; non-CONNECT
  method missing `:scheme` or `:path` → stream
  `PROTOCOL_ERROR` (regression check that the method branch
  didn't widen the rule for non-CONNECT).
- Host / `:authority` reconciliation (§11): `:authority` only →
  accepted; `Host` only → accepted; both present and
  byte-identical → accepted; both present and differing only in
  ASCII case (`Example.COM` vs `example.com`) → accepted (minimal
  case-fold); both present and genuinely different → stream
  `PROTOCOL_ERROR` via sticky-error path; `:authority` containing
  `@` (userinfo) → stream `PROTOCOL_ERROR`; two `Host` fields
  with differing values → stream `PROTOCOL_ERROR`; two `Host`
  fields with identical values → accepted.

### 13.2 Tier 2 — flow-control exhaustion

- Peer sends DATA past `inboundStreamWindow` → stream error.
- Peer sends DATA past `inboundConnectionWindow` → connection error.
- We hold outbound DATA until a `WINDOW_UPDATE` arrives.
- `SETTINGS_INITIAL_WINDOW_SIZE` change adjusts only the direction-
  active set (peer change → outbound on `open` / `half-closed-remote`;
  our change on ACK → inbound on `open` / `half-closed-local`);
  positive and negative delta cases.
- `SETTINGS_INITIAL_WINDOW_SIZE` adjustment that would push any
  existing stream window past `2^31 - 1` → connection
  `FLOW_CONTROL_ERROR` (§7).
- `WINDOW_UPDATE` with increment 0 → `PROTOCOL_ERROR`.
- `WINDOW_UPDATE` pushing window > `2^31 - 1` → `FLOW_CONTROL_ERROR`.
- **Deferred-credit leak on RST_STREAM.** Open stream A, send
  DATA that `onData` handles by returning `false` (deferring the
  credit). Locally `RST_STREAM(CANCEL)` A before calling
  `onBytesConsumed`. Assert the coalesced
  `WINDOW_UPDATE(streamId=0)` fires for the deferred bytes —
  without the stream-close settlement rule (§7 step 6) the
  credit would permanently leak.
- **Over-ack and double-ack are silent no-ops.** Open stream A,
  `onData(dataLen=100)` returning `false` →
  `outstandingInboundCredit = 100`. Handler calls
  `context.onBytesConsumed(A, token, 80)` → counter drops to 20,
  connection window credit-back of 80. Now have the handler call
  `context.onBytesConsumed(A, token, 50)` (asks for more than
  remains — either buggy handler or double counting). Assert:
  `outstandingInboundCredit` stays at 20, no additional
  `WINDOW_UPDATE` fires, the call returns without error. Repeat
  with `n = 0` and `n = -1` — both also no-ops. Then
  `context.onBytesConsumed(A, token, 20)` finishes the balance
  cleanly: counter reaches 0, credit-back of 20 fires. A
  further `context.onBytesConsumed(A, token, 10)` after the
  balance is zero is a no-op (no `0 < n <= 0` satisfied).
  Regression guard for the "double-ack inflates flow-control"
  failure mode.
- **Late-ack after reset is a no-op (generation-token guard).**
  Same fixture as above: stream A with deferred credit, handler
  captures the `(streamId, generationToken)` pair from `onData`.
  Reset A via `RST_STREAM`; the settlement credit fires (one
  `WINDOW_UPDATE(0)`). Now have the handler call
  `context.onBytesConsumed(A.streamId, oldGenerationToken, n)`
  as a "late ack". Assert: no additional `WINDOW_UPDATE` is
  emitted, the connection window is unchanged from its
  post-settlement value, and the call returns a no-op without
  error. The lookup outcome varies by pool state at the moment
  of the late ack — LIVE slot gone (promoted to TOMBSTONE), or
  TOMBSTONE, or slot already freed — and all three map to the
  same silent no-op.

  Then exercise the recycle path: stream ids are monotonically
  increasing (§6), so a second LIVE stream cannot reuse A's
  `streamId`. Open stream C with a higher client-odd id which
  the pool returns into **the same pooled slot** as A (the
  free-list reuses slots across requests — §4), carrying a
  bumped `generation`. Call
  `context.onBytesConsumed(A.streamId, oldGenerationToken, n)`
  again — the handler still holds A's pair, not C's. The context
  looks up `A.streamId`: by the time of the recycle, A's id is
  no longer in the live-map (it was promoted to TOMBSTONE, then
  rolled off when C reused the slot), so the lookup either
  returns nothing (free slot) or finds C at a different id than
  A.streamId — either way, the call is a silent no-op. C's
  `outstandingInboundCredit`, C's stream window, and the
  connection window are all unchanged.
- **Padding flow-control credit-back (both windows).** Peer sends
  a PADDED DATA frame to a live `open` stream with `paddedLen =
  100` (`dataLen = 20`, `paddingOverhead = 80`). Assert both the
  connection window and the stream window each see a net `dataLen
  = 20` debit: the immediate `paddingOverhead = 80` credit-back
  (§7 step 2 for the connection window and the symmetric
  stream-window credit in §7 step 3) offsets the `paddedLen = 100`
  debit. Send enough padded DATA to prove the per-stream window
  does not drift downward one frame at a time.
- **DATA on clean-closed stream debits then credits.** Open
  stream A, send request body with `END_STREAM`, handler sends
  response with `END_STREAM`; stream reaches `closed` via
  matching END_STREAM from both sides. Now have the peer send
  one more DATA frame for A (wire race). Assert the connection
  window debits by the frame's `paddedLen`, the stream errors
  with `STREAM_CLOSED`, and the coalesced `WINDOW_UPDATE(0)`
  eventually credits the bytes back.
- **DATA on `half-closed-remote` stream debits then credits.**
  Peer sent `END_STREAM` on the request body; we're still sending
  response. A follow-up DATA from peer is `STREAM_CLOSED`;
  connection window still credited back via `WINDOW_UPDATE(0)`.
- **DATA on `half-closed-local` stream is normal inbound.**
  **Not** the same as the two cases above. We sent `END_STREAM`
  on our response but the peer can still finish its request body
  — this is the normal live-active inbound case (regression
  guard for the earlier mis-classification). Peer DATA reaches
  `onData`; the coalesced `WINDOW_UPDATE` credits back on
  handler ack.
- **Stream-window underflow: immediate connection credit-back.**
  Set a tight per-stream inbound window. Peer sends DATA whose
  `paddedLen` exceeds the stream window (but not the connection
  window). Assert `RST_STREAM(FLOW_CONTROL_ERROR)` fires, and
  the coalesced `WINDOW_UPDATE(0)` immediately includes
  `dataLen` bytes of connection-window credit-back (§7 step 5
  "stream-window underflow" row). The frame is never delivered
  to the handler, so it cannot flow through the deferred-credit
  settlement of step 6 — this is a regression guard for the
  underflow → no-settlement gap the earlier doc had.
- **Stream-window underflow check uses `paddedLen`, not
  `dataLen`.** Set the per-stream inbound window to `dataLen`
  bytes exactly (e.g. `inboundStreamWindow = 20`). Peer sends a
  PADDED DATA frame with `paddedLen = 100`, `dataLen = 20`,
  `paddingOverhead = 80`. The `dataLen` alone would fit, but the
  full `paddedLen` does not. Assert
  `RST_STREAM(FLOW_CONTROL_ERROR)` fires (regression guard
  against a "net-then-check" implementation that would wrongly
  accept this frame). Assert the stream window was debited to
  `-80` before the reset and padding was NOT credited back to it.
- **Connection-window underflow check uses `paddedLen`, not
  `dataLen`.** Same shape one level up: set the connection
  inbound window to `dataLen` bytes (e.g.
  `inboundConnectionWindow = 20`, arranged via the test fixture
  because SETTINGS can't shrink the connection window below its
  RFC default — see §13.2 starvation test for the hook). Peer
  sends PADDED DATA with `paddedLen = 100`, `dataLen = 20`,
  `paddingOverhead = 80`. Assert `GOAWAY(FLOW_CONTROL_ERROR)`
  fires and the connection tears down — regression guard that
  the connection-window underflow check is symmetric with the
  stream-window one.
- **Terminal Content-Length mismatch credits back this frame.**
  `content-length: 10`, peer sends DATA len=5 with
  `END_STREAM=true`. Assert: (a) `RST_STREAM(PROTOCOL_ERROR)`
  fires; (b) `onData(..., endStream=true)` is **never** called;
  (c) the coalesced `WINDOW_UPDATE(0)` credits back 5 bytes to
  the connection window so the last frame's `dataLen` does not
  permanently consume connection capacity. Regression guard for
  the "terminal-too-large row leaks credit" bug.
- **SETTINGS ACK correlation through queued tighten.** Emit an
  immediate-only SETTINGS frame (e.g. `MAX_CONCURRENT_STREAMS:
  200`), so `settingsFrameOutstanding = true` while `ourApplied ==
  ourAdvertised` for every identifier. Before the peer ACKs, call
  `emitSettings()` again with an ACK-gated tightening (e.g.
  `INITIAL_WINDOW_SIZE: 32 KiB` down from 64 KiB). Assert the
  second frame is **queued**, not emitted. Receive the ACK for the
  first frame; assert `settingsFrameOutstanding` clears, then the
  queued second frame emits and `ourAdvertised[INITIAL_WINDOW_SIZE]
  == 32 KiB` while `ourApplied` remains at 64 KiB. Receive the
  ACK for the second frame; assert `ourApplied[INITIAL_WINDOW_SIZE]
  == 32 KiB` and the window delta was applied to every
  inbound-direction-active stream. This proves the first ACK
  cannot mis-apply the second frame's pending changes (the P1
  failure mode fixed by the explicit boolean).
- **Discarded-DATA does not starve healthy streams.** Open two
  streams A and B on the same connection. Both the connection
  inbound window and the per-stream inbound windows default to
  65535 bytes (RFC 9113) — connection and stream
  `INITIAL_WINDOW_SIZE` are separate settings, and the
  connection-level window is only movable by `WINDOW_UPDATE` (no
  SETTINGS identifier shrinks it below the default). Per-stream
  inbound windows use `ourApplied[INITIAL_WINDOW_SIZE]`, not
  `peerAdvertised`; `peerAdvertised[INITIAL_WINDOW_SIZE]` controls
  **outbound** windows on our side. The simplest fixture that
  isolates the connection-window bottleneck: send exactly 65535
  bytes on stream A in five frames of sizes 16384, 16384, 16384,
  16383, 0 (keeping each frame ≤ `MAX_FRAME_SIZE = 16384` and the
  total ≤ the default stream window of 65535, so stream A's
  window is drained to 0 at the same time as the connection
  window). Locally `RST_STREAM` A. Without the §7 step 5
  credit-back, stream B's DATA attempt stalls on the connection
  window regardless of B's fresh per-stream window. Assert the
  coalesced `WINDOW_UPDATE(streamId=0)` fires for the discarded
  bytes and B can then send DATA that fills the restored
  connection window. Verify the same for the content-length
  declared-too-small path (same fixture but with `content-length:
  4` on A followed by the 65535-byte burst, which triggers the
  §11 pre-dispatch excess-body check).

### 13.3 Tier 3 — h2spec conformance

`summerwind/h2spec` covers 5.1 (stream states), 5.1.1 (stream identifier
rules), 6.5.3 (SETTINGS ACK), 6.9 (flow control), 8.1 (HTTP semantics).
The `Http2ConformanceTest` sketched in `HTTP2_FRAME_CODEC.md` §12.5
already runs the framing subset; extend it to include the 5.1 / 6.9
sections once this layer is in place.

### 13.4 Tier 4 — end-to-end

- `curl --http2-prior-knowledge http://localhost:9009/` — round-trips
  a real request.
- `nghttp -v` — gives a detailed dump for manual verification.
- `grpcurl` — exercises the gRPC framing on top (moves into Stage 3).

## 14. Milestone Scoping and Build Order

### 14.1 Milestone 1 — request / response with single DATA frame

Smallest viable server that responds to an inbound HTTP/2 GET with a
single-frame response. No trailers on either side, no flow-control
window raises beyond the initial values. `MAX_CONCURRENT_STREAMS`
starts at `100` — high enough that multi-stream client warmups (gRPC
channel, concurrent curl invocations) work, without committing to the
full production ceiling. The M1 bar is protocol-conformant
single-request behaviour, not the narrowest possible operating mode.

M1 deliverables:

- All seven FSM states, and every server-side transition from §5
  **except** trailers (the `half-closed-local → closed via recv
  trailers H + endS` edge): arrival of a second `HEADERS` on a
  stream is rejected as `RST_STREAM(PROTOCOL_ERROR)` until M2, with
  the block still decoded to END_HEADERS to keep HPACK state
  coherent (§8 M1-trailer refusal path).
- Inbound + outbound window accounting from §7 **without** the
  coalesced-WINDOW_UPDATE policy (every DATA immediately credits
  back).
- `SETTINGS` exchange with `ENABLE_PUSH = 0`,
  `MAX_CONCURRENT_STREAMS = 100`, `MAX_HEADER_LIST_SIZE` not
  advertised but enforced internally as a 16 KiB per-stream RFC-
  metric policy cap (§11 `HEADER_LIST_POLICY_CAP`, measured as
  `sum(32 + nameLen + valueLen)`), default window / frame sizes.
- HEADERS + CONTINUATION block assembly into HPACK scratch.
- Full sticky-error validator set per §11: pseudo-header rules,
  forbidden regular headers, field-name / value byte validation,
  `content-length` accounting.
- Listener delivery for pseudo-headers + regular headers. The
  terminal-edge signal is the `endStream` flag on the last callback
  (`onRequestHeaders` / `onData` / `onTrailers`) per §4, not a
  separate request-complete callback.
- Stream-error and connection-error mapping from §12.
- Tier 1 unit tests + Tier 3 h2spec 5.1 / 6.9 subset.
- Tier 4 `curl --http2-prior-knowledge` smoke.

### 14.2 Milestone 2 — production shape

- `MAX_CONCURRENT_STREAMS` configurable (default stays at 100, revisit
  upward once real workloads exist — §15).
- Advertise `MAX_HEADER_LIST_SIZE` on the wire matching the M1
  hard cap (§11 M2 refinement); enforcement mechanism is
  unchanged from M1.
- Coalesced `WINDOW_UPDATE` (half-window threshold).
- Trailers support (second HEADERS with END_STREAM).
- Peer `SETTINGS_INITIAL_WINDOW_SIZE` change → open-stream window
  adjustment (§7).
- GOAWAY graceful-shutdown flow.
- h2spec full conformance on sections 5.x + 6.x.
- ADBC Flight SQL smoke (Stage 3 integration).

### 14.3 Build order within Milestone 1

Each step lands with its unit tests in the same commit; nothing
advances until the prior step's tests are green. Follows the HPACK
build-order convention in `HPACK_CODEC.md` §16.3.

1. **Http2StreamState enum + state-table.** The lookup-only table
   from §5. No runtime logic; just the permitted-frames matrix.
   Tests: assert the matrix entries match the table, assert illegal
   (state, frame-type) pairs return the expected error scope.
2. **Http2Stream.** Per-stream state + windows + `initialHeadersSeen`
   flag. Pure state transitions. Tests: every legal transition in §5
   once; every illegal-in-state transition → expected error code and
   scope; trailers-vs-initial-headers selection via
   `initialHeadersSeen`.
3. **Http2FlowController.** Window arithmetic helpers. Stateless
   statics. Tests: `debit` into positive / exactly-zero / would-go-negative;
   `credit` into max / would-overflow-2^31-1; `adjustOnInitialWindowChange`
   positive / negative / would-go-negative delta.
4. **Http2StreamPool.** Fixed-size array + free-list + streamId-to-slot
   map per §4. Tests: open up to `MAX_CONCURRENT_STREAMS`, open one
   more → `REFUSED_STREAM`, close + reopen reuses a slot, id-monotonic
   rule, implicit-idle-to-closed on higher-id open.
5. **Http2ConnectionContext skeleton.** Frame reader / writer +
   HPACK codecs wired in; block-assembly scratch allocated; stream
   pool allocated; `dispatch(header)` as a stub returning
   `UNIMPLEMENTED`. Tests: setup / teardown memory accounting (native
   pool + staging sizes), reset-across-connection reuses state.
6. **Dispatch wiring, one frame type at a time.** SETTINGS → PING →
   HEADERS + CONTINUATION → DATA → WINDOW_UPDATE → RST_STREAM →
   GOAWAY. Each type gets its own frame-sequence unit tests before
   the next one starts: both the normal-case dispatch and the most
   common RFC-mandated error paths (see §12 / §13.1).
7. **Listener wiring.** `Http2StreamListener` invoked on dispatch
   completion events; per §11, the full sticky-error validator set:
   pseudo-header rules, forbidden regular headers, field-name /
   value byte validation, `content-length` accounting, trailers
   detection (reject-with-preserve in M1). Tests: golden-path
   request HEADERS; missing `:path`; pseudo-after-regular; duplicate
   pseudo; `TE: trailers` allowed; `Connection: keep-alive` rejected;
   uppercase field name rejected; NUL / CR / LF in value rejected;
   differing `content-length` fields rejected; non-numeric
   `content-length` rejected; DATA total != `content-length`
   rejected at END_STREAM; refused-stream block still progresses
   HPACK state (follow-up stream decodes correctly);
   concurrency-refused HEADERS block decodes to END_HEADERS.
8. **End-to-end smoke.** `curl --http2-prior-knowledge` against a
   `HelloWorldHandler` that returns `:status 200` + `"hello\n"` in
   one DATA frame; `nghttp -v` for a detailed wire dump; Tier 3
   h2spec subset (5.1, 6.9).

### 14.4 Size estimate

- State table + `Http2Stream` + pool: ~400 LOC.
- Flow controller: ~100 LOC.
- Connection context + dispatch: ~800 LOC.
- Listener + integration glue: ~300 LOC.
- Tier 1 + Tier 2 tests: ~1 kLOC.
- h2spec harness extension: ~200 LOC.

**Milestone 1 total: ~1.5 – 2 kLOC production + ~1.2 kLOC tests.**

## 15. Open Questions

1. **Our advertised `SETTINGS_INITIAL_WINDOW_SIZE`.** Default 65535
   matches RFC; 1 MiB raises throughput for large responses (a single
   Flight SQL `DoGet` stream can be 10s of MiB) but grows per-stream
   memory. Pick after measuring realistic response sizes.
2. **Our advertised `SETTINGS_MAX_CONCURRENT_STREAMS`.** Default 100
   is typical; gRPC-Java defaults to `Integer.MAX_VALUE` (effectively
   unlimited). A tight limit caps memory; a loose one matches gRPC
   expectations.
3. **Coalesced WINDOW_UPDATE threshold.** Half the initial window is
   common but arbitrary. Netty uses 50% with a minimum absolute
   delta. Measure before committing.
4. **Closed-stream grace window.** How many recently-closed streams
   to remember so in-flight WINDOW_UPDATE / RST_STREAM don't trip
   protocol errors. gRPC-Java defaults to 100.
5. **Trailers on request side.** gRPC uses them. RFC 7540 is silent
   on whether a server MUST buffer them before invoking the handler,
   or MAY deliver body then trailers. We pick "deliver body first,
   trailers after" to minimise memory; that's what most servers do.
6. **Receive-buffer sizing and ownership.** The dispatch loop in §9
   reads from a per-connection receive buffer. Sized to the larger
   of `SETTINGS_MAX_FRAME_SIZE` and the block-assembly scratch
   (16 KiB default) with slack for half-arrived frames? Owned by
   the context or by the network layer below? Likely the network
   layer with a pointer handed to the context, matching the
   existing HTTP/1.1 pattern — confirm when wiring.
7. **Outbound write scheduler tick budget.** §7 specifies one DATA
   frame per ready stream per round-robin pass. Question remaining:
   how many passes per connection-readiness tick before we yield
   back to the reactor? Unbounded passes maximise throughput when
   many streams are ready but starve other connections on the same
   worker. A per-tick byte or frame-count budget (e.g. 64 KiB, or
   `N * peerMaxFrameSize`) bounds that. Measure once a real
   workload exists.
8. **GOAWAY flush window.** §12 references a "short flush window"
   before socket close after emitting GOAWAY. Netty defaults to
   30 seconds; gRPC-Java to configurable, default around 1 s.
   Pick after seeing real shutdown traces.
9. **Listener-callback exception handling.** If
   `onRequestHeaders` / `onData` / `onTrailers` throws, is it a
   stream error (`RST_STREAM(INTERNAL_ERROR)`) or a connection
   error (`GOAWAY(INTERNAL_ERROR)`)? Defaulting to stream error
   keeps the connection usable; an application bug on one stream
   shouldn't kill the others. Confirm with the dispatch layer's
   expectations when wiring.
10. **ADBC Flight SQL handler integration.** Once M2 lands, the
    request-dispatch layer above this must understand gRPC framing
    (length-prefixed protobuf) and Arrow Flight SQL semantics. That
    design is Stage 3; listed here only to confirm the boundary.
