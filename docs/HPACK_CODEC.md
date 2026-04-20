# HPACK Codec

Design for an HPACK (RFC 7541) encoder + decoder in QuestDB's zero-dependency,
zero-GC Java idiom. HPACK is the header compression format HTTP/2 uses to
shrink request / response header lists; it sits between the frame codec
(`HTTP2_FRAME_CODEC.md`) and the per-stream state machine (future doc).

## Table of Contents

1. [Scope](#1-scope)
2. [Non-Goals](#2-non-goals)
3. [RFC References](#3-rfc-references)
4. [Module Layout](#4-module-layout)
5. [Wire Format Primer](#5-wire-format-primer)
6. [Static Table](#6-static-table)
7. [Dynamic Table](#7-dynamic-table)
8. [Huffman Codec](#8-huffman-codec)
9. [Integer Codec](#9-integer-codec)
10. [Decoder API and Delivery Model](#10-decoder-api-and-delivery-model)
11. [Encoder API and Strategy](#11-encoder-api-and-strategy)
12. [Buffer and Allocation Model](#12-buffer-and-allocation-model)
13. [Integration with the Frame Codec](#13-integration-with-the-frame-codec)
14. [Error Handling](#14-error-handling)
15. [Testing Strategy](#15-testing-strategy)
16. [Milestone Scoping and Build Order](#16-milestone-scoping-and-build-order)
17. [Open Questions](#17-open-questions)

---

## 1. Scope

The codec encodes and decodes HPACK header blocks between caller-owned native
byte buffers and a callback-delivered stream of `(name, value)` header fields.
It covers:

- The four HPACK representation kinds (indexed header field, literal with
  incremental indexing, literal without indexing, literal never indexed) and
  the dynamic-table size-update instruction.
- HPACK integer encoding with any prefix width (4, 5, 6, 7 bits).
- Huffman encoding / decoding using the static code table in RFC 7541
  Appendix B.
- A dynamic table with RFC-specified size accounting (32 bytes of overhead
  per entry plus `len(name) + len(value)`) and FIFO eviction on insert.
- Per-connection state for both the decoder (inbound table) and the encoder
  (outbound table), each with its own configurable capacity.
- Honouring `SETTINGS_HEADER_TABLE_SIZE` updates and the paired size-update
  instructions.

It does **not** cover:

- Header validation beyond HPACK's own structural rules. Semantic checks
  (pseudo-headers before regular headers, forbidden connection-specific
  headers, malformed `:path`) live in the HTTP/2 request parser at the
  layer above.
- Assembly of HEADERS + CONTINUATION frame payloads into one contiguous
  header block. The frame codec enforces the sequencing; the HTTP/2
  request parser concatenates the payloads and hands a single `addr, limit`
  pair to the HPACK decoder.
- String interning, case folding, or any `String` / `byte[]` allocation on
  the decode or encode hot path.

## 2. Non-Goals

- **No Unicode normalisation.** HPACK strings are opaque octet sequences;
  the codec preserves bytes exactly and does not convert to or from
  `char` / `String`.
- **No incremental / streaming decode across header-block boundaries.** The
  decoder consumes one complete HPACK block per call. CONTINUATION assembly
  happens in the frame handler.
- **No hpack-bomb defences beyond the size cap.** A peer can fill our
  decoder's dynamic table with max-size entries up to the cap *we*
  advertised via our own `SETTINGS_HEADER_TABLE_SIZE` — the peer's
  SETTINGS cannot enlarge our decoder's table; it only constrains the
  peer's own encoder. Higher-level DoS controls (connection rate
  limits, per-request compressed-block-bytes caps, decoded-header-list
  caps) live with request-handler policy; the decoder enforces only
  its own local scratch / assembly hard cap from §12.
- **No Huffman re-encoding heuristics.** The encoder picks Huffman vs
  literal per string with a simple "Huffman if the encoded form is
  shorter" rule; we do not revisit the choice after peer feedback.

## 3. RFC References

Local plain-text copies of the RFCs live under `docs/rfc/`
(see `docs/rfc/README.md`) so design-review cross-checks can
`grep` the source directly without a network round-trip.

- [RFC 7541 — HPACK: Header Compression for HTTP/2](https://www.rfc-editor.org/rfc/rfc7541)
  (`docs/rfc/rfc7541.txt`; canonical reference; Appendix B is the
  Huffman table, Appendix C the worked examples).
- [RFC 9113 — HTTP/2 (revision)](https://www.rfc-editor.org/rfc/rfc9113)
  (`docs/rfc/rfc9113.txt`; sec. 4.3 and 8 describe how HTTP/2 uses
  HPACK: header-block assembly, `COMPRESSION_ERROR` on decoder
  failure, `SETTINGS_HEADER_TABLE_SIZE`,
  `SETTINGS_MAX_HEADER_LIST_SIZE`).
- [gRPC HTTP/2 usage](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
  specifies the pseudo-header and regular-header set gRPC requires
  (`:method POST`, `:scheme`, `:path`, `te: trailers`, `content-type`,
  `grpc-timeout`, `user-agent`, etc.). This drives the encoder's
  static-table-index choices in §11.

## 4. Module Layout

All new code lives under `core/src/main/java/io/questdb/cutlass/hpack/`.

| File                         | Purpose                                                              |
|------------------------------|----------------------------------------------------------------------|
| `Hpack.java`                 | Shared constants: representation prefix bytes, max integer width, default table size, Huffman EOS sentinel. |
| `HpackStaticTable.java`      | Static 61-entry table from RFC 7541 App. A. Per-entry `nameAddr` / `nameLen` / `valueAddr` / `valueLen` into a single immutable native buffer populated at class init. |
| `HpackDynamicTable.java`     | Ring-buffer per-direction dynamic table. Tracks RFC size (bytes) and evicts FIFO on insert. Owns a native byte pool sized to a configured maximum capacity set at construction — large enough to cover every `selectedMax` / advertised cap that may apply during the connection, not the transient operating cap. For the decoder that means `max(Hpack.DEFAULT_TABLE_SIZE, initialEffectiveInboundCap, maxAdvertisedInboundCap)` — the HTTP/2 default (4096) is a hard floor because the peer is free to fill the table up to it in pre-ACK blocks regardless of what we plan to advertise. For the encoder it's `localPreferredCap`, the upper bound on any `selectedMax` we ever intend to run at. `selectedMax` can rise later within the pool without reallocation. |
| `HpackIntCodec.java`         | Prefix-integer encode / decode (RFC 7541 sec. 5.1). Stateless static helpers. |
| `HpackHuffman.java`          | Fixed-table Huffman encode / decode using RFC 7541 App. B. Decode is a bit-by-bit walk over a flat binary tree stored in parallel `TREE_LEFT` / `TREE_RIGHT` `int[]` arrays built at class init; see §8. |
| `HpackDecoder.java`          | Stateful per-connection decoder. Owns one `HpackDynamicTable`. Drives an `HpackListener`. |
| `HpackEncoder.java`          | Stateful per-connection encoder. Owns one `HpackDynamicTable`. Writes directly to a caller-owned native buffer with `-1`-on-overflow semantics. |
| `HpackListener.java`         | Visitor interface: single `onHeader(long, int, long, int, boolean)` callback. |
| `HpackException.java`        | Thrown for any RFC-defined decode failure. Maps to HTTP/2 `COMPRESSION_ERROR` one layer up. |

Follows the frame codec pattern: static constants, per-connection reusable
reader / writer instances, no OO hierarchy over representation kinds (the
decoder is one `switch` on the first byte's high bits).

## 5. Wire Format Primer

Every header field representation starts with a pattern in the high bits
of the first byte (RFC 7541 sec. 6).

| Pattern      | Kind                              | Prefix width | Effect                                                |
|--------------|-----------------------------------|--------------|-------------------------------------------------------|
| `1xxxxxxx`   | Indexed header field              | 7 bits       | Emit the table entry at index N.                      |
| `01xxxxxx`   | Literal, incremental indexing     | 6 bits       | Emit; append `(name, value)` to the dynamic table.    |
| `0000xxxx`   | Literal, no indexing              | 4 bits       | Emit; do not add to table.                            |
| `0001xxxx`   | Literal, never indexed            | 4 bits       | Emit; do not add; intermediaries must preserve this bit. |
| `001xxxxx`   | Dynamic table size update         | 5 bits       | Resize the decoder's dynamic table to N bytes.        |

For every literal form the prefix integer N encodes either `0` (new name
follows as a string) or a table index for the name; a string follows for
the value. Each string is preceded by:

```
+---+---+---+---+---+---+---+---+
| H |    String Length (7+)     |
+---+---------------------------+
```

`H` (high bit) signals Huffman; the 7-bit prefix integer gives the encoded
octet length.

## 6. Static Table

Populated once at class init from RFC 7541 Appendix A. 61 entries, 1-indexed
on the wire:

- `1: :authority`
- `2: :method GET`
- `3: :method POST`
- ...
- `61: www-authenticate`

Storage strategy: a single native byte buffer holds every static name and
value back-to-back; `HpackStaticTable` exposes `nameAddr(i)`, `nameLen(i)`,
`valueAddr(i)`, `valueLen(i)` as O(1) array lookups. Entries with an empty
value (e.g. `1: :authority`, `19: accept-charset`) use `valueLen = 0`.

The static table is immutable and shared across all connections. No
per-connection copy.

Name-only lookup (for encode-side "literal with indexed name" when we don't
want to add to the dynamic table) uses a pre-computed FNV-1a hash map keyed
by name, with chaining — collisions are rare enough in a 61-entry space that
a linear probe is fine. Built once, read-only.

## 7. Dynamic Table

Per RFC 7541 sec. 4, the dynamic table is:

- A FIFO where the **most recently added** entry gets the **lowest** dynamic
  index (62 for the newest entry, 62 + dynamicTableCount - 1 for the oldest).
- Sized in bytes: each entry costs `32 + len(name) + len(value)`.
- Bounded by a per-side configurable cap. The cap can be lowered mid-stream
  via `SETTINGS_HEADER_TABLE_SIZE` + the HPACK size-update instruction.

**Storage.** A native byte pool plus an `int[]` ring of
`(nameOffset, nameLen, valueOffset, valueLen)` tuples. Two slot pointers
govern the ring:

- `newestSlot` — ring index of the most recently inserted entry. Maps
  to dynamic wire index `62`.
- `oldestSlot` — ring index of the oldest live entry, i.e. the next to
  evict. Maps to dynamic wire index `61 + dynamicCount`.

Insertions bump `newestSlot` forward (`(newestSlot + 1) mod ringSize`).
Evictions bump `oldestSlot` forward. `dynamicCount` equals the number of
populated ring slots between the two, wrapping around.

Two distinct "cap" concepts operate on the table and should not be
confused:

- `poolCapacityBytes` — the pool's allocated size in bytes, fixed at
  construction. Governs address arithmetic (wrap, overlap) and the
  staging-buffer sizing. Never changes during the connection.
- `currentOperatingCap` — the HPACK cap currently enforced for
  eviction and oversized-entry decisions. For the encoder's instance
  that's `selectedMax`; for the decoder's that's the cap from the
  most recent inbound HPACK Dynamic Table Size Update (bounded by
  `localAdvertisedCap`). Can change mid-connection without touching
  pool allocation.

Pool bytes are written from an append cursor (`poolCursor`) that wraps
at `poolCapacityBytes`; every entry's bytes occupy one contiguous
`[start, start + nameLen + valueLen)` range so `(nameAddr, nameLen)`
and `(valueAddr, valueLen)` are always deliverable as simple
`addr, len` pairs without scatter-gather.

**Pool allocation routine.** On each insert, compute
`needed = nameLen + valueLen` and `entryCost = needed + 32` (per RFC
7541 sec. 4.1). The insert sequence must resolve any dynamic-table
back-references *before* the first eviction step — eviction advances
`oldestSlot` and removes slot metadata from the live set, after which
the referenced `(srcAddr, srcLen)` pair is no longer safely
addressable even if the underlying bytes have not yet been
overwritten.

1. **Resolve + stage back-references.** If the new entry's *name* is
   referenced by dynamic index (literal-with-incremental-indexing,
   indexed-name form), resolve the index to `(srcAddr, srcLen)` against
   the *current* live table. If `srcAddr` points into this table's own
   dynamic pool, copy those bytes into the table's internal staging
   buffer now. The buffer is owned by `HpackDynamicTable` itself and
   is sized to the maximum single-entry byte cost the table could
   ever hold, which is `poolCapacityBytes` (see §12); the same
   staging path serves both the decoder's and the encoder's
   `HpackDynamicTable` instance, so Milestone 2 dynamic indexing on
   the encoder side reuses it without a separate scratch. Static-table
   references are immune — the static buffer is immutable and
   separate. The bookkeeping is cheap (one branch check on the
   resolved `srcAddr`), so the hot path skips the copy when it isn't
   needed.
2. **Oversized-entry short-circuit.** If
   `entryCost > currentOperatingCap`, the entry cannot be held at
   all: empty the table (set `oldestSlot` and `newestSlot` to the
   empty-ring sentinel, `dynamicCount` → 0, `currentSize` → 0,
   `poolCursor` → 0) and return without running steps 3–6. This
   implements the RFC 7541 sec. 4.4 rule; skipping the short-circuit
   would cause the operating-cap eviction loop in step 5 to never
   terminate because `0 + entryCost > currentOperatingCap` stays
   true after the table is fully drained. The construction-time
   invariant `poolCapacityBytes >= currentOperatingCap` (§4, §7, §12)
   guarantees that a fitting entry (`entryCost <= currentOperatingCap`)
   also satisfies `needed <= poolCapacityBytes`, so this branch also
   subsumes the "entry longer than the whole pool" case.
3. **Pick the target region.** If `poolCursor + needed <=
   poolCapacityBytes`, the target region is
   `[poolCursor, poolCursor + needed)`. Otherwise wrap: set
   `poolCursor = 0` and target `[0, needed)`.
4. **Overlap eviction.** Evict every live entry whose pool range
   overlaps the target region, oldest first. Each eviction advances
   `oldestSlot`, subtracts the evicted entry's cost from
   `currentSize`, and decrements `dynamicCount`. When a decrement
   drops `dynamicCount` to `0`, reset both `oldestSlot` and
   `newestSlot` to the empty-ring sentinel — otherwise `oldestSlot`
   would point one past the last-evicted slot and the next insert's
   index math would be off by one. Necessary regardless of the
   operating-cap check: after a wrap, the target region can straddle
   live bytes near pool offset 0.
5. **Operating-cap eviction.** While `currentSize + entryCost >
   currentOperatingCap`, continue evicting oldest entries under the
   same bookkeeping (advance `oldestSlot`, decrement `dynamicCount`,
   subtract cost from `currentSize`, and collapse both pointers to
   the empty-ring sentinel when `dynamicCount` hits `0`). Guaranteed
   to terminate because step 2 already excluded
   `entryCost > currentOperatingCap`. (Combined "overlap +
   operating-cap" into a single loop is fine — the loop invariant is
   "evict oldest until both the region is clear of live bytes and
   `currentSize + entryCost <= currentOperatingCap`".)
6. **Commit.** Write the staged name (from step 1) and the literal
   value bytes starting at the target region. Advance `poolCursor` by
   `needed`, add `entryCost` to `currentSize`, and install a new
   slot:
   - If `dynamicCount == 0` (the empty-ring case, whether because
     the table has never been written or because step 2 / 4 / 5 just
     emptied it), set both `newestSlot` and `oldestSlot` to the slot
     `0` of the ring and set `dynamicCount = 1`.
   - Otherwise set `newestSlot = (newestSlot + 1) mod ringSize`,
     leave `oldestSlot` unchanged, and increment `dynamicCount`.

**Index mapping.** A dynamic wire index `i` (62 ≤ i ≤ 61 +
`dynamicCount`) maps to ring slot
`(newestSlot - (i - 62) + ringSize) mod ringSize`. So `i = 62` →
`newestSlot`, `i = 61 + dynamicCount` → `oldestSlot`. The bounds
check `i = 0 || i > 61 + dynamicCount` fails decode with
`HpackException`, which is why `dynamicCount` must be maintained
exactly — step 4/5 decrements expose the newly-oldest entry, step 6
increment admits the newest entry, and the oversized-entry clear in
step 2 resets `dynamicCount` to `0`.

**Name matching for encode (Milestone 2).** The encoder will walk a small
hash index over the currently-live dynamic entries to find a match for a
candidate header's name (and, optionally, both name + value). Hash chains
are intrusive `int[]` slots to avoid per-entry objects. This is deferred
until Milestone 2 because the Milestone 1 encoder pins `selectedMax` at
`0` (§16.1), so the dynamic table is always empty and no dynamic-side
lookup is needed. Milestone 1 probes only the static table, which has
its own dedicated FNV-1a index inside `HpackStaticTable`.

## 8. Huffman Codec

RFC 7541 Appendix B defines a static canonical Huffman code over the 257
symbols 0..255 + EOS (end-of-string). Symbol lengths range from 5 to 30
bits.

**Encode.** For each input byte, look up the `(code, codeBits)` pair and
pack into a MSB-first bit stream. Pad the final partial byte with `1` bits
(which correspond to a prefix of the EOS code — the decoder recognises this
as padding).

Encode size is known up front: sum of `codeBits[input[i]]` for i ∈ 0..n-1,
divided by 8, rounded up. The encoder computes this twice in the inner
"Huffman-or-literal" decision — once for the candidate, once to commit. No
allocation.

**Decode.** Bit-by-bit walk over a flat binary tree stored in parallel
`TREE_LEFT` / `TREE_RIGHT` `int[]` arrays. Each slot is either a
non-negative internal-node index or a negative leaf whose symbol value
is `~slot`. The tree is built at class init from the RFC Appendix B
code table. Decode tracks a running `node` pointer plus a `depth`
counter (bits consumed since the last symbol emission) that the
end-of-stream padding check uses without any allocation:

```
node = 0; depth = 0
for each input byte, from MSB to LSB:
    child = ((byte >> bit) & 1) == 0 ? TREE_LEFT[node] : TREE_RIGHT[node]
    if child < 0:
        emit(~child); node = 0; depth = 0
    else:
        node = child; depth += 1
```

This is noticeably slower than nghttp2's byte-at-a-time 2-level
decision table (~8× the ops per symbol), and a future optimisation
may upgrade to that shape. For Milestone 1 the bit-by-bit walker is
adequate and considerably simpler; see §17 Open Questions for the
upgrade trigger.

**Padding rejection.** Per RFC 7541 sec. 5.2, the decoder enforces three
separate conditions, each an `HpackException`:

1. **Padding length &le; 7 bits.** Any stream whose last symbol ends
   with 8 or more padding bits is malformed — a decoder that only
   checked "trailing bits are EOS-prefix" would silently accept, for
   example, a full trailing `0xFF` byte (8 bits of `1`, matching the
   EOS prefix). The tracked `depth > 7` after the last byte rejects.
2. **Trailing bits are an EOS prefix.** The up-to-7 remaining bits must
   equal the leading bits of the EOS code (all `1`s). The decoder
   confirms by walking `depth` right-edges from root and checking we
   land on `node` — allocation-free, no BFS.
3. **No EOS symbol may appear as a proper symbol** (only as padding).
   If a decode emission would be `~EOS_SYMBOL` we throw.

Size estimate: tree arrays are ~2 KiB of static data plus ~400 LOC of
build + decode.

## 9. Integer Codec

Per RFC 7541 sec. 5.1, an HPACK integer occupies a variable-length tail
after a prefix that fills the low `N` bits of the first byte (`N ∈ {4, 5,
6, 7}`, determined by the representation kind).

If the prefix value is `< (1 << N) - 1`, that's the integer. Otherwise
`(1 << N) - 1` is stored in the prefix and the remaining value is written
as a sequence of continuation bytes: low 7 bits of each byte are digit
bits (base 128), the high bit is 1 on all but the last byte.

Our codec:

- Caps decoded integers at `Integer.MAX_VALUE`. A continuation sequence
  that would overflow throws `HpackException`.
- Caps encoded integers likewise; the encoder is caller-driven and never
  sees values larger than header-field lengths / table sizes.
- Exposes three stateless static helpers:
  - `decode(long addr, long limit, int prefixMask) -> long packed` —
    parse. The return is a packed `long` where the low 32 bits are the
    decoded value and the high 32 bits are the bytes-read count;
    callers unpack via `Numbers.decodeLowInt` / `Numbers.decodeHighInt`.
  - `encode(long addr, long limit, int prefixMask, int firstByteFlags,
    int value) -> newAddr | -1` — emit. Returns `-1` when the buffer
    can't hold the encoded integer (preflight atomic: no bytes written).
  - `encodedLength(int prefixBits, int value) -> int` — return the
    exact byte length `encode` would produce for the given prefix
    width and value, without touching any buffer. Used by the §11
    buffer-sizing invariant to preflight queued Dynamic Table Size
    Update lengths (1 byte for small values, up to 6 bytes near
    `Integer.MAX_VALUE`).

The packed-long return avoids per-call allocation without the
caller-owned `int[]` out-params — zero boxing, pure primitive
arithmetic, and the caller's unpack is a pair of bit operations.

## 10. Decoder API and Delivery Model

```java
public interface HpackListener {
    /**
     * Called once per decoded header. All four address / length arguments
     * point into caller-visible scratch that is valid until onHeader
     * returns; the listener must copy anything it wants to keep.
     *
     * neverIndexed == true if the representation was literal-never-indexed.
     * Matters for forwarding through intermediaries; for a leaf server it
     * is advisory.
     */
    void onHeader(long nameAddr, int nameLen,
                  long valueAddr, int valueLen,
                  boolean neverIndexed);
}

public final class HpackDecoder {

    /**
     * @param initialEffectiveInboundCap the cap the peer's encoder is
     *   currently bound by, <em>not</em> our local preferred cap. Before
     *   we send any SETTINGS, that's the HTTP/2 default {@code 4096}
     *   (RFC 7541 sec. 4.2). Our own {@code SETTINGS_HEADER_TABLE_SIZE}
     *   — whatever we plan to advertise, default or otherwise — is
     *   applied separately through {@link #onLocalAdvertisedCapChanged}
     *   only after the peer's SETTINGS ACK arrives. Enforcing a larger
     *   or smaller pre-ACK cap here would reject legal pre-ACK header
     *   blocks or accept out-of-range size updates too early.
     * @param poolCapacityBytes the byte capacity to allocate for the
     *   dynamic-table pool. Must satisfy
     *   {@code poolCapacityBytes >= max(Hpack.DEFAULT_TABLE_SIZE,
     *   initialEffectiveInboundCap, maxAdvertisedInboundCap)}. All three
     *   bounds matter:
     *   <ul>
     *     <li>{@code Hpack.DEFAULT_TABLE_SIZE} (4096) is the HTTP/2
     *         protocol floor. Before our SETTINGS ACK the peer may fill
     *         the table up to this default regardless of what we
     *         ultimately advertise, and the constructor rejects
     *         configurations below it.</li>
     *     <li>{@code initialEffectiveInboundCap} upper-bounds the
     *         pre-SETTINGS-exchange fill.</li>
     *     <li>{@code maxAdvertisedInboundCap} upper-bounds the
     *         post-ACK fill.</li>
     *   </ul>
     *   Sizing to the maximum of the three prevents both pre-ACK
     *   under-running and post-ACK reallocation.
     */
    HpackDecoder(int initialEffectiveInboundCap, int poolCapacityBytes);

    /**
     * Called when our *local* SETTINGS_HEADER_TABLE_SIZE has changed — that
     * is, the peer has ACKed our new SETTINGS value via a SETTINGS frame
     * with the ACK flag set. From this point on the decoder caps the
     * maximum value it will accept in an inbound HPACK Dynamic Table Size
     * Update instruction at {@code newCap}; a size update exceeding that
     * ceiling is a decoding error.
     *
     * <p>The decoder tracks the peer encoder's most recently signalled
     * <em>selected max</em> — the value from the last inbound HPACK
     * Dynamic Table Size Update, separate from the table's current byte
     * occupancy. If {@code newCap} drops below that selected max, per
     * RFC 7541 sec. 4.2 the peer MUST begin its next header block with
     * a size update &le; {@code newCap}. The decoder enforces this by
     * recording a <em>pending minimum</em>: the smallest
     * {@code newCap} seen since the obligation first armed. Per RFC 7541
     * sec. 4.2 last paragraph, when multiple cap changes accumulate
     * between blocks the peer must report the smallest of them first;
     * the pending minimum ratchets downward on every subsequent drop and
     * does not relax when {@code localAdvertisedCap} later rises back up.
     * Satisfaction of the pending minimum is checked both at the first
     * non-size-update representation <em>and</em> at block end: opening
     * the block with a header representation, finishing the block
     * without any qualifying size update (including the zero-length-block
     * case), or accepting an inbound size update above the pending
     * minimum all throw {@code HpackException}. The pending minimum
     * clears as soon as a qualifying inbound size update (value &le;
     * pending minimum) arrives.
     *
     * <p>If the obligation was never armed (the peer's current selected
     * max is already &le; {@code localAdvertisedCap}), no inbound size
     * update is required; the peer can continue encoding against its
     * prior selected max.
     */
    void onLocalAdvertisedCapChanged(int newCap);

    /**
     * Decodes a single assembled HEADERS + CONTINUATION field block fragment
     * (see sec. 13 for fragment extraction). Throws HpackException on any
     * structural error.
     */
    void decodeBlock(long addr, long limit, HpackListener listener);
}
```

**Delivery semantics.**

- For an indexed header, the listener receives pointers straight into the
  static table's native buffer or into the dynamic table's pool. Both are
  stable for the duration of the `onHeader` call.
- For a literal header, the listener receives pointers into a
  per-decoder scratch buffer that is reset at the start of every
  `decodeBlock`. Huffman-decoded strings materialise here.
- Literal-with-incremental-indexing also *copies* the new entry into the
  dynamic-table pool as a separate step; the listener still sees the
  scratch-buffer copy so the call is uniform.

This keeps HpackListener pointer-stability simple: "valid until the method
returns". Anything longer is the listener's copy obligation. The pgwire
query handler already uses this convention for bind-variable delivery.

**Size update instructions must appear only at the start of a block** (RFC
7541 sec. 4.2). The decoder enforces this: once any header representation
has been emitted, a subsequent size-update is `HpackException`.

## 11. Encoder API and Strategy

```java
public final class HpackEncoder {

    /**
     * @param initialPeerAdvertisedCap the peer's SETTINGS_HEADER_TABLE_SIZE
     *   as of connection setup. Before the peer sends any SETTINGS, that's
     *   the HTTP/2 default {@code 4096} (RFC 7541 sec. 4.2). Typed
     *   {@code long} because SETTINGS values are unsigned 32-bit on the
     *   wire (RFC 7540 sec. 6.5.2); the encoder rejects values outside
     *   {@code [0, 2^32 - 1]} and otherwise clamps to
     *   {@code localPreferredCap} before storage (see
     *   {@link #peerAdvertisedCap()} for the exposed effective value).
     * @param localPreferredCap our policy upper bound on the encoder's
     *   {@code selectedMax}. Bounds how large a dynamic table we are
     *   willing to maintain on the outbound side even when the peer
     *   permits more. In Milestone 1 this is effectively unused because
     *   {@code selectedMax} is pinned at {@code 0}; Milestone 2 uses it
     *   in the cap-raise decision.
     * @param poolCapacityBytes byte capacity of the encoder's dynamic-table
     *   pool. The constructor enforces {@code poolCapacityBytes >=
     *   localPreferredCap} so the pool never needs reallocation when
     *   Milestone 2 raises {@code selectedMax} to its policy ceiling;
     *   configurations that violate this are rejected up front.
     * @param maxOutboundFieldBytes cap on a single outbound header field's
     *   {@code (nameLen + valueLen)}, per §11 "Outbound header-field cap".
     *   Used at setup to derive {@code maxRepresentationBytes} and thus
     *   the {@code bufferFloor}. Fields exceeding this cap are rejected
     *   at the caller boundary before {@code encode} is invoked.
     *   `outputBufferCapacityBytes` is the byte capacity of the
     *   caller-owned HPACK output buffer the writer passes to
     *   {@link #beginBlock} and {@link #encode} via {@code (addr, limit)}.
     *   The constructor computes {@code bufferFloor} from
     *   {@code localPreferredCap} + {@code maxOutboundFieldBytes} (see
     *   §11) and rejects any configuration where
     *   {@code outputBufferCapacityBytes < bufferFloor}. Per-call,
     *   {@code beginBlock} and {@code encode} return {@code -1} when the
     *   *current slice* cannot hold the pending write, so a rolling-buffer
     *   caller can drain and retry without exception. Livelock freedom
     *   relies on the setup-time invariant: once the buffer is drained,
     *   the slice the caller offers next must be at least
     *   {@code outputBufferCapacityBytes} wide, which is what the setup
     *   check guarantees.
     */
    HpackEncoder(long initialPeerAdvertisedCap,
                 int localPreferredCap,
                 int poolCapacityBytes,
                 int maxOutboundFieldBytes,
                 int outputBufferCapacityBytes);

    /**
     * Called when the peer has advertised a new SETTINGS_HEADER_TABLE_SIZE —
     * that is, the peer has just told us the maximum dynamic-table size it
     * is willing to maintain when decoding header blocks we send. Typed
     * {@code long} because SETTINGS values are unsigned 32-bit on the wire
     * (RFC 7540 sec. 6.5.2); the encoder rejects values outside
     * {@code [0, 2^32 - 1]} and otherwise clamps to
     * {@code localPreferredCap} before storage. The clamp is
     * information-preserving for the encoder: {@code selectedMax} is
     * always bounded by {@code localPreferredCap} per §11, so a peer cap
     * above that ceiling produces identical encoder behaviour.
     *
     * <p>The next header block emits a Dynamic Table Size Update <em>only
     * if</em> {@code selectedMax} actually changes as a result. Recording
     * a lower {@code peerAdvertisedCap} while {@code selectedMax} already
     * sits at or below it is a no-op on the wire — §13 covers this case.
     * Milestone 1 pins {@code selectedMax} at {@code 0}, so every call
     * here after the initial-block update is a pure state update.
     *
     * <p>If {@code selectedMax} changes more than once between our
     * blocks — for example a drop followed by a policy-driven raise,
     * or two successive drops — the encoder records the minimum
     * interim value and emits two updates at the next block start:
     * first the interim minimum, then the final {@code selectedMax}
     * (RFC 7541 sec. 4.2 last paragraph).
     */
    void onPeerAdvertisedCapChanged(long newCap);

    /**
     * Opens a new header block. Emits any queued Dynamic Table Size Update
     * instructions (zero, one, or two of them — see §11 block lifecycle
     * and §13) before the first header field. Must be paired with
     * {@link #endBlock}. A header block corresponds to exactly one HEADERS
     * (optionally followed by CONTINUATION) or one PUSH_PROMISE on the wire.
     *
     * <p>No size update is emitted when {@code selectedMax} has not
     * changed since the previous block; in steady state the method is a
     * no-op.
     *
     * <p>Returns the new write pointer, or {@code -1} if the
     * caller-owned buffer cannot hold the queued size updates. A
     * {@code -1} return is <em>atomic</em>: no size updates are
     * dequeued, no bytes are written, and the encoder's internal
     * state (queued size-update bookkeeping) is exactly as it was
     * on entry. The caller drains the output buffer and retries
     * with the same arguments; see the retry-atomicity rule in
     * this section.
     */
    long beginBlock(long addr, long limit);

    /**
     * Encodes one header into the caller-owned buffer. Must be called
     * between {@link #beginBlock} and {@link #endBlock}. Returns the
     * new write pointer, or {@code -1} if the buffer cannot hold the
     * encoded header. A {@code -1} return is <em>atomic</em>: no
     * bytes are written, the dynamic table is unchanged (Milestone 2),
     * and any name-reference staging is rolled back. The caller
     * drains the output buffer and retries with the same arguments.
     */
    long encode(long addr, long limit,
                long nameAddr, int nameLen,
                long valueAddr, int valueLen,
                int hint);

    /** Closes the header block opened by {@link #beginBlock}. */
    void endBlock();
}
```

**Retry atomicity.** Every `-1` return from `beginBlock` / `encode`
MUST leave the encoder's observable state — both the caller's write
pointer (no partial bytes in `[addr, returnedAddr)`) and the encoder's
internal state (queued size-update list, dynamic-table slot ring +
pool cursor + `currentSize`, `selectedMax`, staging buffer) — exactly
as it was on entry. The canonical implementation strategy is
**preflight**: compute the exact byte cost of the representation
first (prefix integer lengths, literal or Huffman string length, size
update(s)), fail with `-1` if the cost exceeds `limit - addr`, and
only then mutate the table and write bytes. Rolling back a partial
mutation is also acceptable but adds complexity; preflight is
preferred because HPACK byte costs are cheap to compute.

Without this rule, a drain-and-retry cycle after a `-1` can duplicate
or skip size updates, or desynchronise the dynamic table from the
bytes actually emitted on the wire.

**Buffer-sizing invariant.** Preflight atomicity is sound only when
`drain + retry` can actually make progress — that is, only when the
caller's output buffer is guaranteed to fit at least one worst-case
encoded representation once drained. Without that invariant, a single
header field larger than the buffer produces an endless `-1` loop.
Milestone 1 picks the simplest guarantee that makes it true:

- **Output-buffer size floor.** The caller-owned HPACK output buffer
  must be sized so that, when empty, it fits the maximum encoded
  representation the encoder can produce plus the worst-case queued
  size-update prelude. A Dynamic Table Size Update is a 5-bit-prefix
  integer per RFC 7541 sec. 5.1; its encoded length runs from 1 byte
  for values < 31 up to 6 bytes for values near `Integer.MAX_VALUE`.

  The invariant is split across two distinct checks:

  - **Setup-time floor** — a one-shot check at connection / encoder
    construction. Computed against the *worst case* across every
    size update the encoder could ever queue, not the currently
    pending value (which is typically `0`):

    ```
    bufferFloor =
          2 * HpackIntCodec.encodedLength(5, localPreferredCap)
        + maxRepresentationBytes
    ```

    `localPreferredCap` is the tight upper bound on `selectedMax`:
    policy always enforces `selectedMax <= localPreferredCap`
    regardless of `peerAdvertisedCap` movements (Milestone 1 pins
    `selectedMax = 0`; Milestone 2 may raise to any value `<=
    min(localPreferredCap, peerAdvertisedCap)` as a policy choice).
    So no legitimately signalled size update will ever exceed
    `localPreferredCap`. Two size updates is the maximum per RFC
    7541 sec. 4.2 (interim minimum + final `selectedMax`).

    `maxRepresentationBytes` is defined as **the encoded size of the
    largest representation admitted by the outbound header-field
    cap** (see the next bullet). That cap is the single
    configuration knob; `maxRepresentationBytes` is derived from it
    once at setup. For the Milestone 1 encoder surface
    (static-indexed + literal-without-indexing + plain-octet
    strings) it expands to: one prefix integer for the name index
    (or literal name length), one prefix integer for the value
    length, `maxEncodedNameBytes + maxEncodedValueBytes` payload
    bytes implied by the cap, plus a small constant for
    representation flags. Because the cap is what defines
    admissibility, any header field that fits the cap also fits
    `maxRepresentationBytes` in an empty buffer — the two are
    consistent by construction.

    Sizing at the `localPreferredCap` ceiling rather than at the
    currently pending size-update value is deliberate: peer-driven
    `selectedMax` churn can queue previously-zero size updates
    without reaching setup again, and a buffer sized to the smaller
    current value would reintroduce the `-1` retry livelock the
    invariant exists to prevent. Default `bufferFloor`: 8 KiB,
    configurable, but the encoder rejects a caller-supplied buffer
    smaller than the computed floor.
  - **Per-call preflight** — computed from the *actual* currently
    pending updates and the *actual* header field, as already
    described by the retry-atomicity rule. Always `<=` the
    setup-time floor because of how the floor is computed.
- **Outbound header-field cap.** A single configuration value,
  `maxOutboundFieldBytes`, bounds the maximum admissible
  `(nameLen + valueLen)` for any outbound header field. Enforced at
  the caller boundary before `encode` is invoked: any field whose
  `nameLen + valueLen > maxOutboundFieldBytes` is rejected with an
  explicit error rather than handed to `encode`. This cap is the
  single authoritative input to `maxRepresentationBytes` above; the
  encoder configuration stores `maxOutboundFieldBytes` directly,
  and `maxRepresentationBytes` is computed from it once at setup.
  For gRPC / Flight SQL responses the worst-case fields
  (`:status`, `content-type: application/grpc`, `grpc-status`,
  `grpc-message`) are small and well under any reasonable cap; the
  cap exists to fail loudly on pathological values rather than
  livelock. Default: 8 KiB, configurable.

Milestone 2 optionally relaxes this by adding mid-representation
resume (the encoder tracks how many bytes / bits of a string it has
already emitted, and `encode` returns the number of bytes consumed
rather than the simple `-1`). That change is only worth making if a
real workload produces outbound headers larger than the output
buffer; none of the response fields Flight SQL emits today get close.

**Block lifecycle.** HPACK Dynamic Table Size Update instructions are
legal only at the start of a header block (RFC 7541 sec. 4.2). The
explicit `beginBlock` / `endBlock` pair lets the encoder batch any
queued size-update instructions at exactly the right spot regardless
of how many `encode` calls follow. The trigger is a change to
`selectedMax`, not to `peerAdvertisedCap` alone: if peer SETTINGS
arrive between blocks but leave `selectedMax` unchanged (e.g. the new
`peerAdvertisedCap` is still ≥ `selectedMax`), `beginBlock` emits no
size update. When multiple peer SETTINGS between blocks do move
`selectedMax`, `beginBlock` emits two updates — the minimum interim
value and the final `selectedMax` — as RFC 7541 sec. 4.2 requires.

**Hint values.** The encoder consults `hint` to skip cost computations:

- `HpackEncoder.HINT_STATIC_INDEX | idx` — the caller knows the header
  maps to a specific static index (e.g. `:status 200` → 8). Emit as
  indexed.
- `HpackEncoder.HINT_STATIC_NAME | nameIdx` — the caller knows the name
  is in the static table but the value is dynamic (e.g. `:path` + request
  path). Emit as literal with static-indexed name.
- `HpackEncoder.HINT_NONE` — the encoder looks up the name / value in
  both tables and picks the cheapest representation.

This keeps the response-emission hot path allocation-free *and* index-free
for pseudo-headers (`:status`, `content-type: application/grpc`), which
covers the most-repeated fields in a gRPC response.

**Indexing policy for unknown headers.**

- Values of `:path`, `cookie`, `authorization`, `grpc-timeout` are not
  indexed. They are request-unique and would just churn the table.
- Other literals use incremental indexing when both name and value fit.
- When the entry cost `nameLen + valueLen + 32` exceeds the encoder's
  current `selectedMax` (the HPACK operating cap communicated to the
  peer's decoder, not `peerAdvertisedCap` and not the pool's byte
  capacity), we emit as literal-without-indexing and do not touch the
  table — it would be an immediate eviction otherwise.

**Huffman policy.** For each string we would otherwise emit as a literal
octet sequence, compute the Huffman-encoded length; use Huffman iff it's
strictly shorter. Computing the Huffman length is `O(len)` and needs only
a length lookup per input byte; not on a cache-missing path.

## 12. Buffer and Allocation Model

- All encode / decode operations run on `long addr` + `long limit` pairs.
  No `byte[]` and no `ByteBuffer` on the hot path.
- The static table's name / value bytes live in one native buffer allocated
  at class init and never freed.
- Each connection owns two `HpackDynamicTable` instances (one per direction),
  each with its own native pool sized to `poolCapacityBytes` fixed at
  construction (§4, §7), plus a small internal staging buffer for the
  name-reference hazard in §7 (sized to `poolCapacityBytes`, the
  largest single entry the table could hold). Pool bytes are governed
  by `poolCapacityBytes` for allocation / wrap / overlap; entry
  admission and eviction are governed by the table's
  `currentOperatingCap` (the encoder's `selectedMax`, the decoder's
  most-recent inbound size-update value). The staging buffer serves
  inserts from both sides: the decoder exercises it from day one on
  inbound literal-with-incremental-indexing; the encoder picks it up
  in Milestone 2 when encoder-side dynamic writes turn on.
- The decoder owns a single per-instance scratch native buffer for
  Huffman-decoded strings, sized to a **local hard cap** configured at
  connection setup (default 16 KiB; see §17 open questions for tuning).
  This is *not* driven by `SETTINGS_MAX_HEADER_LIST_SIZE` — that setting
  is advisory and directional (RFC 9113 sec. 6.5.2) and offers no hard
  budget for our own memory. If a header block's decoded field bytes
  would exceed the cap, the decoder throws `HpackException` and the
  caller converts that to a connection close or stream reset as it sees
  fit. Scratch is reset by cursor rewind at the top of every `decodeBlock`.
- The encoder does not need decoder-style scratch in Milestone 1:
  literal values are copied from caller-owned pointers straight to the
  output buffer and never materialised anywhere else. In Milestone 2
  the dynamic-table name-reference hazard is handled entirely inside
  `HpackDynamicTable`'s internal staging buffer, so the encoder still
  does not need to allocate a separate scratch. Huffman encoding
  preflights: it computes the encoded byte length from the RFC 7541
  App. B code-length table in a single pass over the input, returns
  `-1` if the result would not fit in the remaining buffer (per the
  retry-atomicity rule in §11), and only then writes the bits directly
  into the caller's buffer. No partial-write-then-rollback.
- No iterator / cursor objects. No per-header `HeaderField` instance. The
  listener callback is the sole per-header delivery point.

## 13. Integration with the Frame Codec

`Http2FrameReader` + `Http2FrameWriter` deliver HEADERS / CONTINUATION
frames; their payloads carry the HPACK **field block fragment** possibly
wrapped by Pad Length / Priority / Padding bytes (RFC 9113 sec. 6.2).
The HPACK codec operates only on the assembled *field block*, not on raw
frame payloads.

**Field-block extraction.** The request parser must strip non-HPACK bytes
from every HEADERS frame payload before assembly:

- If `PADDED` is set, byte 0 is the Pad Length. The last `padLength`
  bytes of the payload are padding.
- If `PRIORITY` is set, the 5 bytes that immediately follow the Pad
  Length byte (or start the payload, if `PADDED` is clear) are the
  stream-dependency + weight. (The frame reader already rejects
  stream self-dependency.)
- The HPACK field block fragment occupies the remaining bytes:
  `start = padLenByte(1 if PADDED else 0) + priorityBlock(5 if PRIORITY else 0)`,
  `end = payloadLen - padLength(if PADDED else 0)`, field block is
  `payload[start .. end)`.
- **Layout guard.** The invariant
  `payloadLen >= padLenByte + priorityBlock + padLength` is already
  enforced by the frame reader (`Http2FrameReader` validates both the
  Pad Length byte's presence and the consumed-byte total, including
  the 5-byte priority block when the `PRIORITY` flag is set, before
  handing the payload to the HPACK layer). A naive `padLength <
  payloadLen` check would let e.g. `payloadLen = 6`,
  `PADDED | PRIORITY`, `padLength = 1` through with `start = 6,
  end = 5` and a negative-length field block; the reader's actual
  check blocks that case. The HPACK layer receives a well-formed
  field block fragment by contract and does not re-validate.

CONTINUATION frames carry only the field block fragment — no Pad Length,
no Priority, no padding (RFC 9113 sec. 6.10).

**Inbound flow.**

```
  frame reader -> request parser  ->  HEADERS  (strip Pad Length, Priority,
                                                trailing padding; block=[...])
                                      CONTINUATION(block=[...])
                                      CONTINUATION(block=[...], END_HEADERS)
                                 |
                                 v
                  concatenate field block fragments into one native buffer
                  (or pass a scatter-gather list; see below)
                                 |
                                 v
                  HpackDecoder.decodeBlock(addr, limit, listener)
                                 |
                                 v
                  listener.onHeader(...)  per field
```

**Concatenation strategy.** Two options:

1. *Copy into a per-connection scratch.* Simple, wastes one memcpy per
   CONTINUATION. Sized by the same local hard cap as the decode scratch
   (§12), not by `SETTINGS_MAX_HEADER_LIST_SIZE`.
2. *Virtual contiguous view via scatter-gather.* Saves the memcpy but
   needs a `HpackDecoder` that tolerates address discontinuities
   mid-string. More code in the hottest decode loop.

First milestone: pick option (1) with a 16 KiB default scratch and a
configurable cap. The parser rejects any block whose assembled bytes
would exceed the cap with an `HpackException` — upgraded to a connection
close at the frame layer. Revisit if benchmarks show the memcpy is
measurable.

**Outbound flow.** The HPACK encoder writes into a caller-owned
rolling buffer that the response writer drains as it fills. An HPACK
header block is open from `beginBlock` until `endBlock`; the block is
a single byte stream on the wire that the HTTP/2 writer fragments
into one HEADERS frame followed by zero or more CONTINUATION frames
on the same stream, setting `END_HEADERS` only on the frame that
carries the tail of the block emitted after `endBlock`.

Flow per block:

1. Call `beginBlock(addr, limit)`. On `-1` the caller drains (see
   step 3) and retries until the size-update prelude fits.
2. Call `encode(addr, limit, ...)` for each header field. On `-1` the
   caller drains the already-encoded bytes as a mid-block frame
   (step 3), then retries the same `encode` call against the now-
   empty buffer. Preflight atomicity (§11) guarantees no partial
   representation is in the buffer at the moment of drain, so a
   mid-block frame boundary always lines up with a representation
   boundary — but the HPACK layer does not require it to, because
   HPACK is boundary-insensitive (RFC 7541 sec. 4.1).
3. **Draining mid-block.** The writer emits the accumulated bytes as
   either the first HEADERS frame for this block (no `END_HEADERS`
   flag) or a CONTINUATION frame (no `END_HEADERS` flag). The
   buffer is reset and encoding resumes. Block-in-progress state
   (which stream, whether HEADERS has already been emitted) lives
   on the HTTP/2 writer, not on the HPACK encoder.
4. Call `endBlock()`. Drain the remaining bytes with `END_HEADERS`
   set. The frame kind is picked from the writer's `headersEmitted`
   flag for this stream:
   - `headersEmitted == false` — no HEADERS has gone out yet, either
     because the block is empty or because every prior drain in
     steps 1–3 produced zero bytes. The writer emits a single
     HEADERS frame carrying the remaining bytes (possibly zero) with
     `END_HEADERS` set. The empty-block case collapses into this
     branch as a zero-length HEADERS with `END_HEADERS` — a
     zero-length CONTINUATION on a stream that never saw its
     HEADERS is a `PROTOCOL_ERROR` on the peer.
   - `headersEmitted == true` — HEADERS has already gone out for
     this block. The writer emits a CONTINUATION frame carrying the
     remaining bytes (possibly zero) with `END_HEADERS` set.

This is the only contract consistent with the `-1`-and-retry model in
§11. The earlier "buffer the whole block, then hand it to the frame
writer" contract is withdrawn — it contradicts mid-block drain and
would force the output buffer to be sized to the worst-case *block*,
not the worst-case *representation* (§11 buffer-sizing invariant).

Splitting happens at arbitrary byte boundaries inside the encoded
block, including mid-representation if preferred, though our
implementation splits only between representations (aligning with
the retry-atomicity drain points).

**SETTINGS_HEADER_TABLE_SIZE coupling.** The setting is directional
(RFC 9113 sec. 6.5.2). Two separate acknowledgment mechanisms live side
by side, and it's important not to conflate them:

- **HTTP/2 SETTINGS ACK.** A SETTINGS frame with the ACK flag confirms
  at the HTTP/2 layer that the peer has received and applied our
  SETTINGS. It is *not* an HPACK instruction.
- **HPACK Dynamic Table Size Update.** An in-band HPACK instruction at
  the start of a header block that changes the *sender's* (i.e. the
  encoder's) *selected maximum* dynamic-table size — the operating cap
  the encoder chooses to run at, which must be ≤ the peer-advertised
  cap. It is not an acknowledgment of SETTINGS; it communicates a
  change to `selectedMax`, not to the table's current occupancy
  `currentSize`.

**Three distinct quantities on the encoder side.** Track them separately:

- `peerAdvertisedCap` — the *effective* upper bound on our
  `selectedMax` that the peer's decoder will accept, recorded from
  the peer's `SETTINGS_HEADER_TABLE_SIZE`. The raw setting is unsigned
  32-bit (RFC 7540 sec. 6.5.2), but the encoder clamps it to
  `localPreferredCap` before storing because any value above that
  ceiling produces identical encoder behaviour (`selectedMax` is
  always bounded by `localPreferredCap`). The getter returns this
  clamped value, not the raw peer setting; callers that need the raw
  wire value can track it outside the encoder. We never choose a
  `selectedMax` above `peerAdvertisedCap`.
- `selectedMax` — the encoder's operating cap. The peer's decoder
  applies this value from the most recent HPACK size update as the
  actual ceiling on its dynamic table; it is *not* inferred from our
  byte occupancy. `selectedMax` must be signalled on the wire via an
  HPACK size update whenever it changes.
- `currentSize` — how many bytes of entries are actually live right
  now. Always ≤ `selectedMax`.

Splitting the directions:

- A **peer** `SETTINGS_HEADER_TABLE_SIZE` advertises
  `peerAdvertisedCap`. On receipt, the frame-codec SETTINGS parser
  calls `HpackEncoder.onPeerAdvertisedCapChanged(newCap)`. The encoder
  compares the new cap against its current `selectedMax`:
  - If `newCap < selectedMax`, the encoder lowers `selectedMax` to
    `min(localPreferredCap, newCap)`, evicts oldest entries until
    `currentSize <= selectedMax`, and queues a Dynamic Table Size
    Update for the next `beginBlock`. This is required by RFC 7541
    sec. 4.2 regardless of `currentSize`: the peer decoder's ceiling
    is `selectedMax`, not our byte occupancy.
  - If `newCap >= selectedMax`, no size update is required. (A policy
    choice: the encoder may still *optionally* raise `selectedMax` up
    to `min(localPreferredCap, newCap)` to make more room for future
    entries; that too is signalled via a size update. Milestone 2
    decision, §16.)
  - If `selectedMax` changes more than once between our blocks —
    whether by successive peer-cap drops, a drop followed by a
    policy-driven raise, or any other combination — the encoder
    records the minimum interim value and emits two updates at the
    next `beginBlock`: first the interim minimum, then the final
    `selectedMax` (RFC 7541 sec. 4.2 last paragraph).
- **Our** `SETTINGS_HEADER_TABLE_SIZE` advertises the maximum dynamic
  table size *we* will maintain when decoding frames *the peer sends*.
  It constrains **our decoder**. The same three-quantity split applies
  on this side, just with the roles reversed:
  - `localAdvertisedCap` — the cap we advertised in our own SETTINGS.
    The decoder enforces any inbound HPACK size update against it
    (value &gt; `localAdvertisedCap` → `HpackException`).
  - `peerSelectedMax` — the peer encoder's operating cap as it last
    signalled via an inbound HPACK Dynamic Table Size Update. The
    decoder tracks this; it is the cap the peer's encoder is actually
    running at, not the peer's current byte occupancy.
  - Peer's currentSize — byte occupancy of the peer's encoder table,
    mirrored in our decoder's dynamic-table instance. Not directly
    relevant to size-update obligations.

  When we lower `localAdvertisedCap` via a SETTINGS change, and the
  peer ACKs, the peer's encoder is required (RFC 7541 sec. 4.2) to
  reduce its `selectedMax` to &le; the new cap and signal the change
  via a Dynamic Table Size Update at the start of its next header
  block. The decoder enforces this obligation by recording a
  **pending minimum** — `pendingMinCap` — initialised to `newCap`
  when the drop first arms it (i.e. when `peerSelectedMax > newCap`).
  RFC 7541 sec. 4.2 last paragraph requires that when multiple cap
  changes accumulate between blocks, the peer must report the
  smallest of them first; `pendingMinCap` therefore only ratchets
  downward across subsequent changes, and a later raise to
  `localAdvertisedCap` does not relax it — the peer still owes us a
  size update &le; the smallest interim value. A qualifying inbound
  Dynamic Table Size Update (value &le; `pendingMinCap`) clears the
  pending minimum; an inbound update above it throws
  `HpackException` for violating the interim commitment. Any decode
  path that would let a block pass without clearing the pending
  minimum — the first representation being a header representation
  (not a size update), or the block ending with no representation at
  all (the empty-block case) — throws `HpackException`. If
  `peerSelectedMax &le; localAdvertisedCap` already, the pending
  minimum is never armed and the peer is free to send its next block
  with no size update at all.

`HpackDecoder.onLocalAdvertisedCapChanged` is invoked by the SETTINGS
emitter (not the SETTINGS parser) once our local change has been put on
the wire and the peer's HTTP/2 SETTINGS ACK has arrived — the standard
"settings take effect on ACK" pattern.

## 14. Error Handling

Any RFC 7541 structural failure throws `HpackException`. The caller (the
HTTP/2 request parser) converts this into a connection-level
`Http2ConnectionException(COMPRESSION_ERROR)` which the frame layer
translates into a `GOAWAY` and connection close.

Specific triggers:

- **Integer overflow** during prefix-int decode.
- **Index out of bounds**: `0`, or `> 61 + dynamicCount`.
- **Size update mid-block** (after any header representation has been
  emitted in the same block).
- **Size update exceeding cap**: peer tried to set the dynamic table
  larger than the cap we advertised via `SETTINGS_HEADER_TABLE_SIZE`.
- **Truncated header field**: buffer ends mid-integer or mid-string.
- **Huffman EOS symbol** appearing as a proper symbol (not just the
  padding prefix).
- **Huffman trailing bits** that do not match a prefix of EOS padding.

Debug strings are ASCII-only per project convention.

## 15. Testing Strategy

Four tiers, aligning with the frame codec's layered approach.

### 15.1 Oracles

| Oracle                                                                   | Used for                                                                | Scope       |
|--------------------------------------------------------------------------|-------------------------------------------------------------------------|-------------|
| RFC 7541 Appendix C worked examples                                      | Canonical encode + decode correctness                                   | Unit        |
| `http2jp/hpack-test-case` JSON corpus                                    | Cross-implementation decode (nghttp2, Go, Erlang, Haskell, Python, Swift) | Unit      |
| Netty `DefaultHttp2HeadersEncoder` / `DefaultHttp2HeadersDecoder`        | Differential encode / decode roundtrip                                  | Unit        |
| End-to-end h2c + gRPC client                                             | Real-world request / response header flows                              | Integration |

`hpack-test-case` is the standard corpus; nghttp2, Envoy, Go, and Netty all
consume it. Each JSON case lists the wire bytes and the expected decoded
header list, optionally per sequence-of-blocks on a shared dynamic table.

### 15.2 Tier 1 — RFC 7541 Appendix C

Hand-transcribe every worked example. Each is small (≤ 10 headers) and
exercises a specific representation kind. Useful for one-off debugging
during implementation and as a regression safety net.

### 15.3 Tier 2 — hpack-test-case corpus

Stored under `core/src/test/resources/hpack-test-case/`. A JUnit
parameterised test walks every JSON vector, feeds the wire bytes to our
decoder, and compares the emitted header list to the expected one. For
cases that share a dynamic table across blocks, the same `HpackDecoder`
instance drives the sequence.

### 15.4 Tier 3 — Netty differential

`netty-codec-http2` already pulled in as test scope (`HTTP2_FRAME_CODEC.md`
sec. 12.2). Mirrors the frame-codec approach, but through Netty's **public**
surfaces — the internal `HpackDecoder` / `HpackEncoder` in Netty 4.1.x are
package-private so the test harness uses `DefaultHttp2HeadersEncoder` and
`DefaultHttp2HeadersDecoder` (which wrap the internal HPACK implementations
and expose `ByteBuf`-in / `Http2Headers`-out semantics we can drive from
outside the `io.netty.handler.codec.http2` package).

Implemented in `HpackNettyDifferentialTest`:

- Encode a header block with our encoder; feed the wire bytes to Netty's
  `DefaultHttp2HeadersDecoder`; assert the decoded header list matches.
- Encode with Netty's `DefaultHttp2HeadersEncoder`; feed the wire bytes to
  our decoder; assert the decoded header list matches.
- Property-based: generate random header lists (name / value lengths and
  byte distributions that exercise both Huffman and literal encoding),
  roundtrip in both directions.

### 15.5 Tier 4 — end-to-end

Once HTTP/2 + HPACK + a hello-world handler are wired, drive
`curl --http2-prior-knowledge` and capture `nghttp -v` output to verify
real clients decode our responses and we decode their requests. Extend the
existing `Http2ConformanceTest` to run a scoped subset of `h2spec hpack/*`.

## 16. Milestone Scoping and Build Order

The codec is split asymmetrically across milestones because HPACK imposes
asymmetric requirements on decoder vs. encoder. A receiver MUST handle every
representation kind the sender chooses; a sender is free to emit only the
simplest forms and still produce valid HPACK.

### 16.1 Milestone 1 — decoder complete, encoder simple

The decoder is substantially **complete** on day one. Real clients
(`grpcurl`, ADBC-flightsql, Netty / gRPC-Java, nghttp-via-curl) use every
representation kind: indexed header fields, literals with incremental
indexing, size updates, Huffman strings. A partial decoder would fail
unpredictably against mainstream tooling.

The encoder ships **conservative**: static-indexed headers plus
literal-without-indexing for everything else. That is valid HPACK and is
the form most widely used by server implementations (Go's `net/http2`
notably makes the same choice by default). No dynamic table *writes*, no
Huffman encode.

**Encoder dynamic-table posture.** To keep the Milestone 1 encoder out of
the size-update-emission business entirely, the first block the encoder
emits begins with a single HPACK Dynamic Table Size Update to `0`,
pinning the encoder's `selectedMax` at `0` for the life of the
connection (see §13 for the `selectedMax` / `peerAdvertisedCap` /
`currentSize` split). With `selectedMax = 0`, `currentSize` is forced
to `0` and the encoder's table is inert. `0` is ≤ any peer
`peerAdvertisedCap`, so later peer SETTINGS changes never require a
further encoder-side size update. Milestone 2 raises `selectedMax`
when dynamic indexing + Huffman encode go in.

**Decoder dynamic-table posture.** The decoder fully implements size
updates, eviction, and name-reference handling from day one — the peer
will exercise them.

Milestone 1 deliverables:

| Block                                                                 | Status                                                                                                               |
|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| Integer codec (§9)                                                    | Complete                                                                                                             |
| Static table (§6)                                                     | Complete                                                                                                             |
| Huffman decode (§8) with EOS + 7-bit padding checks                   | Complete                                                                                                             |
| Dynamic table (§7) with size accounting, eviction, name-ref hazard    | Complete (decoder uses it fully; encoder's instance stays empty because `selectedMax` is pinned at 0)                |
| Decoder for all four representation kinds + size-update (§10)         | Complete                                                                                                             |
| Encoder: static-indexed + literal-without-indexing (§11)              | Minimal (skip Huffman encode, skip dynamic writes, pin `selectedMax` at 0 via the initial size update)               |
| `HpackEncoder.beginBlock` / `endBlock` lifecycle (§11)                | Complete; `beginBlock` emits the initial `size update → 0` on the very first block and nothing thereafter            |
| SETTINGS_HEADER_TABLE_SIZE plumbing (§13)                             | Complete on the decoder side (record `localAdvertisedCap` on peer ACK, track `peerSelectedMax`, enforce the sequencing rule). Peer SETTINGS changes are recorded in the encoder but take no action because `selectedMax = 0 ≤ any peerAdvertisedCap`. |
| Frame-codec fragment extraction (§13)                                 | Pending (§13 wiring happens at build-order step 8)                                                                   |
| Tier 1 tests (RFC 7541 App. C worked examples)                        | Complete (C.1 prefix-ints, C.2.1..C.2.4 representation kinds, C.3 request sequence, C.4.1 Huffman roundtrip)         |
| Tier 2 `hpack-test-case` JSON corpus                                  | Pending (resources under `core/src/test/resources/hpack-test-case/` not yet wired up)                                |
| Tier 3 HPACK Netty differential                                       | Complete (`HpackNettyDifferentialTest`: 6 hand-written cases covering pseudo-headers, gRPC request/response, custom headers, plus 50-trial fuzz in both directions against `DefaultHttp2HeadersEncoder` / `DefaultHttp2HeadersDecoder`) |

### 16.2 Milestone 2 — encoder optimisation

Added once the wider Flight SQL stack is running and we have real header
patterns to measure against. Lifting the Milestone 1 "pin `selectedMax`
at 0" stance is the first step; everything else here depends on having
a live encoder-side dynamic table.

- Drop the initial `size update → 0` pin. The encoder now chooses a
  non-zero `selectedMax` up to `min(localPreferredCap,
  peerAdvertisedCap)` and signals it via a Dynamic Table Size Update
  at the next `beginBlock`.
- Huffman encode with the `huffman_len < literal_len` decision rule.
- Dynamic-table writes from the encoder with the indexing policy in §11.
- Dynamic-table name lookup (hash index over live entries) for the
  "literal with indexed name" fast path on repeated custom headers;
  the name-reference hazard (§7) is handled inside
  `HpackDynamicTable`'s staging buffer.
- Encoder-side size-update emission whenever `selectedMax` changes. In
  particular: when the peer lowers `SETTINGS_HEADER_TABLE_SIZE` below
  the encoder's current `selectedMax`, the encoder lowers
  `selectedMax`, evicts oldest entries until `currentSize ≤
  selectedMax`, and queues a size update for the next `beginBlock`
  — regardless of whether `currentSize` was already under the new
  cap. The two-updates-at-next-block rule from §11 covers multiple
  peer changes between our blocks.

### 16.3 Build order within Milestone 1

Bottom-up, each step validated against Netty or the RFC corpus before the
next one starts:

1. **Integer codec.** 4 / 5 / 6 / 7-bit prefix, overflow rejection. RFC
   7541 sec. 5.1 worked values as unit tests.
2. **Static table.** One native buffer populated at class init; the FNV-1a
   name index built from it.
3. **Huffman decode.** Build a flat binary tree at class init from the
   Appendix B symbol listing and walk it bit-by-bit on decode (§8). EOS
   + 7-bit padding + no-proper-EOS checks are mandatory for this step
   to be "done". A byte-at-a-time table-driven implementation stays on
   the roadmap for later; see §17 Open Questions.
4. **Dynamic table.** Ring + pool with the staged-copy insert path
   (§7). Unit tests that force the name-reference eviction edge case
   (insert that evicts the entry whose name is referenced).
5. **Full decoder.** Walks the representation kinds and drives
   `HpackListener`. Drop the `hpack-test-case` corpus in at this point;
   it's the first step that passes the full suite.
6. **Minimal encoder.** `beginBlock` / `endBlock`, static-index emission,
   literal-without-indexing, plain-octet string encoding (no Huffman).
   Net-new code on top of steps 1–2.
7. **SETTINGS plumbing.** Wire `onLocalAdvertisedCapChanged` /
   `onPeerAdvertisedCapChanged` into the frame-codec SETTINGS path. On
   our outbound SETTINGS change, defer the decoder-cap enforcement
   update until the peer's HTTP/2 SETTINGS ACK arrives; from that
   point on the decoder enforces both the inbound-size-update
   ceiling and the "if `peerSelectedMax > localAdvertisedCap`, the
   next block must start with a size update" rule from §10. On peer
   SETTINGS change, record the new `peerAdvertisedCap` in the
   encoder's state — and stop there for Milestone 1: the encoder
   emits exactly one HPACK size update, `→ 0`, from the very first
   `beginBlock` call, pinning `selectedMax` at `0` for the life of
   the connection. Since `0 ≤ any peerAdvertisedCap`, no further
   encoder-side updates are ever queued. Milestone 2 is where peer
   cap reductions start driving `selectedMax` changes and size-update
   emission.
8. **Frame-codec integration.** HEADERS + CONTINUATION payload
   extraction, block assembly into the per-connection scratch, hand off
   to `HpackDecoder.decodeBlock`.

### 16.4 Size estimate

- Integer codec: ~150 LOC.
- Static table: ~300 LOC (mostly the hand-transcribed entry list).
- Huffman decode: ~400 LOC (table build + decode loop) + ~10 KiB of
  static data.
- Dynamic table: ~400 LOC.
- Decoder: ~500 LOC.
- Minimal encoder: ~300 LOC.
- Plumbing + listener + exceptions: ~200 LOC.

**Milestone 1 total: ~2 - 2.5k LOC production + roughly the same in tests.**
Milestone 2 (Huffman encode + dynamic indexing + name lookup) is
another ~500 - 1000 LOC.

Milestone 1 is a decoder-complete, encoder-simple HPACK that produces
valid wire output against every mainstream client and parses every
representation the RFC defines. It is the smallest surface that
interoperates without surprise.

## 17. Open Questions

1. **Encoder indexing default for `grpc-encoding` / `grpc-accept-encoding`.**
   Repeated verbatim on every response; indexing is an easy win but would
   evict static-named headers if the table is tight. Benchmark once the
   whole pipeline exists.
2. **Huffman decision threshold.** Strict "shorter wins" or a small margin
   (`huffman_len + k ≤ literal_len` with `k ∈ {0, 1, 2}`) to avoid
   pathological CPU-for-minimal-bytes trades on short strings. Default to
   strict shorter-wins for the first milestone; revisit on CPU profiling.
3. **Scatter-gather vs copy for CONTINUATION assembly.** Default is copy
   per §13; measure if the copy shows up on a flamegraph after Stage 7.
4. **Our advertised inbound `SETTINGS_HEADER_TABLE_SIZE`.** Default
   4096 bytes per HPACK; 16 KiB reduces index size on the wire for
   verbose APIs but grows per-connection memory. Pick after measuring
   realistic Flight SQL response headers.
5. **Dynamic-table hash index width.** Start with a flat `int[]` sized
   to `poolCapacityBytes / minEntryCost` rounded up to a power of two,
   chained by intrusive slots. (The pool's allocated byte capacity —
   not the encoder's current `selectedMax` — gives the upper bound on
   how many entries can ever be resident, so the index doesn't need
   resizing when `selectedMax` changes.) Revisit if encode lookup
   shows up hot.
