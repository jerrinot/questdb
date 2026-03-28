# Stage 5: Gap Attribution and Optimization Backlog

Date: 2026-03-28

## Benchmark snapshot (from investigation plan)

| Backend | `l > 42` | `l > 42 AND d < 100.0` | `l IN (1,2,3,4,5)` |
|---|---:|---:|---:|
| `NATIVE_SIMD` | 137.930 | 170.305 | 29.115 |
| `JAVA_VECTOR_BYTECODE` | 248.977 | 339.395 | 162.120 |
| Ratio (Java/Native) | 1.80x | 1.99x | 5.57x |

## Gap attribution

### `l > 42` — 1.8x gap

The hot loop machine code is near-optimal AVX-512 (8 longs/chunk). C2
inlines everything. The generated x86 matches what a hand-written AVX-512
implementation would produce.

**The gap is NOT in the hot loop.** Evidence:
- Java uses AVX-512 (8 longs/chunk) vs native AVX2 (4 longs/chunk)
- Java has fewer instructions per element than native
- Java's vpcompressq is faster than native's PEXT+PDEP+VPERMPS emulation

Attributed causes:
1. **Per-call MemorySegment construction** (~20-30% of gap): Each
   `filterRows()` call constructs 3 MemorySegment objects from raw
   addresses. `reinterpretInternal` (61 bytes) fails to inline. The
   native path takes raw pointers and starts computing immediately.
   **Category: QuestDB codegen issue** — could cache segments or use
   Unsafe directly.

2. **Safepoint poll** (~5-10% of gap): One `testl %eax, (%r9)` per
   loop iteration. Unavoidable in JVM code.
   **Category: JVM limitation** — cannot be eliminated.

3. **Benchmark measurement scope** (unknown): The JMH benchmark may
   include more overhead in the Java path (interface dispatch,
   generated class lookup) than in the native path (direct JNI call).
   **Category: benchmark-path issue** — needs validation.

4. **AVX-512 throttling** (speculative): On some Intel CPUs, heavy
   AVX-512 usage causes frequency throttling. The hot loop uses ZMM
   registers for every operation. If the CPU is throttling, the 2x
   wider vector width may not translate to 2x throughput.
   **Category: HotSpot/hardware interaction** — measure with perf
   counters to validate.

### `l > 42 AND d < 100.0` — 2.0x gap

Same as above, plus:
5. **Mask cast overhead** (~5-10% additional): 2 mask conversions per
   chunk (Long↔Double) that native doesn't need. These compile to
   `VectorSupport::convert` intrinsics but still cost 2-4 instructions.
   **Category: Vector API design cost** — unavoidable without
   same-species optimization.

### `l IN (1, 2, 3, 4, 5)` — 5.6x gap

6. **No vectorization for short-circuit programs** (90%+ of gap): The
   Java vector compiler rejects `IN()` because it generates
   short-circuit IR (`hasControlFlow() == true`). Falls back to scalar
   bytecode. Native uses AVX2 SIMD with 4-wide parallel OR.
   **Category: QuestDB codegen issue** — needs vectorized IN()
   implementation.

## Ranked optimization backlog

### Priority 1: High payoff, low risk

**1. Vectorize IN() predicates**

- Impact: 5.6x benchmark gap → should close to ~1.5-2x
- Type: QuestDB codegen issue
- Approach: Convert IN(v1, v2, ..., vN) from short-circuit OR chain to
  parallel mask OR. For each value, broadcast and compare against the
  column vector, OR the results. No control flow needed.
- Risk: Low — straight-line program, same codegen path
- Effort: Medium — needs IR transformation or new lowering for IN()

**2. Cache MemorySegments across calls**

- Impact: ~20-30% of `l > 42` gap
- Type: QuestDB codegen issue
- Approach: Instead of constructing MemorySegments from raw addresses
  on every `filterRows()` call, accept pre-constructed segments or cache
  them across calls for the same column addresses.
- Alternative: Use `Unsafe.getUnsafe().getLong/putLong` directly instead
  of MemorySegment for the vector loads and stores. This avoids the
  entire MemorySegment abstraction.
- Risk: Low — changes only the setup path, not the hot loop
- Effort: Medium — requires interface change or caching strategy

### Priority 2: High payoff, medium risk

**3. Skip redundant mask.cast() for same-species loads**

- Impact: Eliminates 1 method call per column load per chunk
- Type: QuestDB codegen issue
- Approach: In `emitLoadColumn()`, only emit `mask.cast(targetSpecies)`
  when the target species differs from the loop species. For pure I8
  or pure F8 programs, the activeMask already matches the column type.
- Risk: Low — bytecode change only, C2 already eliminates it
- Effort: Low — simple condition in the compiler
- Note: C2 already eliminates this (the cast is a no-op that resolves to
  the identity), so runtime impact may be zero. But it reduces bytecode
  size and simplifies the generated method.

**4. Vectorize short-circuit programs for simple AND chains**

- Impact: Enables vector path for `l > 0 AND i != 0 AND ...` filters
- Type: QuestDB codegen issue
- Approach: When all short-circuit branches jump to the same target
  (common for AND chains), merge them into a straight-line program with
  mask AND operations. The vector compiler already handles AND of masks.
- Risk: Medium — needs IR analysis to determine when short-circuit can
  be safely converted to straight-line
- Effort: Medium-High

### Priority 3: Speculative / HotSpot-limited

**5. Avoid MemorySegment entirely (use Unsafe for vector ops)**

- Impact: Eliminates all MemorySegment overhead
- Type: QuestDB codegen issue
- Approach: Instead of `LongVector.fromMemorySegment()`, use
  `LongVector.fromArray()` on a scratch array filled via Unsafe, or
  use VectorSupport intrinsics directly.
- Risk: High — may break Vector API contracts; depends on internal APIs
- Effort: High

**6. Measure and mitigate AVX-512 throttling**

- Impact: Unknown — may explain why 2x wider vectors don't give 2x
  throughput
- Type: Hardware interaction
- Approach: Run benchmarks with `perf stat` to check frequency, IPC,
  and port utilization. Consider falling back to AVX2 (256-bit) if
  throttling is detected.
- Risk: Low (measurement) / Medium (mitigation)
- Effort: Low (measurement) / High (runtime adaptation)

**7. Generate vectorized null check for I8 (avoid helper call)**

- Impact: Reduces code in hot loop for null-aware I8 comparisons
- Type: QuestDB codegen issue
- Approach: Instead of calling `FilterHelpers.longNullGt()`, emit the
  null check inline: `lhs.compare(EQ, nullVec).or(rhs.compare(EQ,
  nullVec)).not()` directly in the bytecode. C2 already inlines the
  helper, so runtime impact is likely zero.
- Risk: Low
- Effort: Low
- Note: C2 already inlines longNullGt (36 bytes). This would only
  reduce bytecode size, not runtime performance.

## Investigation conclusions

### Why is JAVA_VECTOR_BYTECODE slower than NATIVE_SIMD?

The hot loop machine code is near-optimal. The gap comes from:

1. **Per-call setup overhead** (MemorySegment construction) — largest
   controllable factor for simple filters
2. **IN() not vectorized** — largest gap for IN()-heavy workloads
3. **Safepoint poll** — unavoidable JVM cost, ~5% overhead
4. **Possible AVX-512 frequency throttling** — unconfirmed

### What part of the gap is in QuestDB-controlled code generation?

- IN() vectorization: **100% QuestDB-controlled**
- MemorySegment setup: **100% QuestDB-controlled** (can cache or avoid)
- Redundant mask.cast(): **100% QuestDB-controlled** (but C2 already
  handles it)

### What part is in HotSpot / Vector API behavior?

- Safepoint poll: **HotSpot limitation**
- MemorySegment.reinterpretInternal too big to inline: **JDK limitation**
  (could be fixed in future JDK versions with smaller implementation)
- AVX-512 throttling: **hardware limitation** (if confirmed)

### Top 3 optimization opportunities

1. **Vectorize IN() predicates** — eliminates scalar fallback for a
   common SQL pattern
2. **Cache or eliminate MemorySegment construction** — reduces per-call
   overhead for all filter types
3. **Vectorize simple AND short-circuit chains** — expands the set of
   programs eligible for vector bytecode

### Where should further work focus?

1. **Vectorized IN()** — highest impact single item
2. **MemorySegment optimization** — broadest impact across all filters
3. **Benchmark validation** — confirm whether the measured gap accurately
   reflects real query workloads (per-page-frame calls with segment
   reuse vs. the JMH synthetic benchmark)
