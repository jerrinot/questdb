# Stage 5: Gap Attribution and Optimization Backlog

Date: 2026-03-28

## Benchmark snapshot (from investigation plan)

| Backend | `l > 42` | `l > 42 AND d < 100.0` | `l IN (1,2,3,4,5)` |
|---|---:|---:|---:|
| `NATIVE_SIMD` | 137.930 | 170.305 | 29.115 |
| `JAVA_VECTOR_BYTECODE` | 248.977 | 339.395 | 162.120 |
| Ratio (Java/Native) | 1.80x | 1.99x | 5.57x |

## Evidence basis

- Stage 1: Generated JVM bytecode reviewed via `javap -c -p`
- Stage 2: C2 inlining verified via `-XX:+PrintCompilation -XX:+PrintInlining`
- Stage 3: HotSpot machine code captured via `-XX:+PrintAssembly` with hsdis
- Stage 4: Native asmjit assembly captured via `cairo.sql.jit.debug.enabled`
- No per-cause timing isolation was performed (no targeted microbenchmarks
  or perf counters). The attribution below is structural — based on
  instruction-level comparison of the two codegen outputs — not measured.

## Gap attribution

### `l > 42` — 1.8x gap

The Java hot loop (AVX-512, 8 longs/chunk) has fewer instructions per
element than native (AVX2, 4 longs/chunk) and uses `vpcompressq` (1 insn)
vs PEXT+PDEP+VPERMPS (6 insns). Despite this, native is faster.

**The gap is NOT in the hot loop instruction quality.** Both paths generate
clean vectorized code. The Java path is structurally more efficient per
element.

Identified structural differences (not yet measured individually):

1. **Per-call MemorySegment construction**: Each `filterRows()` call
   constructs 3 MemorySegment objects from raw addresses.
   `reinterpretInternal` (61 bytes) fails to inline (Stage 2). The
   native path takes raw pointers and starts computing immediately.
   **Category: QuestDB codegen issue** — could cache segments or use
   Unsafe directly.

2. **Safepoint poll**: One `testl %eax, (%r9)` per loop iteration in
   Java. None in native.
   **Category: JVM limitation** — cannot be eliminated.

3. **AVX-512 frequency throttling**: On some Intel CPUs, heavy AVX-512
   usage causes frequency downclocking. The Java path uses ZMM registers
   exclusively. If throttling occurs, the 2x wider vector width may not
   translate to 2x throughput. Not measured.
   **Category: hardware interaction** — needs `perf stat` to confirm.

4. **Benchmark call path differences**: The JMH benchmark invokes
   different code paths for native (JNI → asmjit function) vs Java
   (interface dispatch → generated class). Relative overhead unknown.
   **Category: benchmark-path issue** — needs validation.

### `l > 42 AND d < 100.0` — 2.0x gap

Same structural differences as above, plus:

5. **Mask cast overhead**: 2 mask conversions per chunk (Long↔Double)
   in Java that native doesn't need. These compile to
   `VectorSupport::convert` intrinsics. Impact not measured.
   **Category: Vector API design cost**

### `l IN (1, 2, 3, 4, 5)` — 5.6x gap

IN() IS vectorized for single-size columns (all LONG). The IR serializer
emits straight-line `EQ` + `OR` ops, and both backends generate SIMD loops.

What the machine-code comparison actually shows:

6. **Both backends reload the column once per IN value.**
   Java does 5 masked `vmovdqu64` loads per chunk; native does 5 `vmovdqu`
   loads per chunk. So column-load deduplication is a real optimization
   opportunity, but it is **not** the main explanation of the current
   Java-vs-native gap by itself.
   **Category: shared codegen inefficiency**

7. **Java hits materially higher register pressure on the 5-way OR shape.**
   C2 reports `out of virtual registers in linear scan` before retrying at
   tier 4 for both generated `IN()` methods. Native shows no corresponding
   signal because asmjit controls register allocation directly.
   **Category: Java backend / HotSpot interaction**

8. **Java row-ID still pays setup overhead that native does not.**
   The Java path constructs `MemorySegment` wrappers and carries object/type
   validation before entering the loop. Native enters directly with raw
   pointers.
   **Category: QuestDB codegen issue**

9. **Java count-only uses per-chunk `popcnt`; native uses vector
   accumulation.**
   This is directly visible in the native `IN()` dump (`vpsubq` accumulator +
   one horizontal reduction) versus the Java tier-4 dump (`popcntq` every
   chunk).
   **Category: QuestDB codegen issue**

## Ranked optimization backlog

### Priority 1: High payoff, evidence-backed

**1. Reduce setup-path `MemorySegment` overhead**

- Impact: applies to all Java vector-bytecode filters, including the simple
  `l > 42` case where the hot loop already looks close to ideal
- Type: QuestDB codegen issue
- Evidence:
  - Stage 2: `reinterpretInternal` does not inline
  - Stage 3/4: tier-4 code still constructs and validates segments before
    entering the hot loop
- Approach:
  - cache segments across calls when addresses are stable, or
  - move to a lower-overhead access path that still preserves Vector API use
- Risk: Medium
- Effort: Medium

**2. Add load-CSE for straight-line `IN()` lowering/codegen**

- Impact: reduces absolute work in both row-ID and count-only `IN()` paths
- Type: QuestDB codegen issue
- Evidence:
  - Stage 1 bytecode shows 5 separate `MEM` loads
  - Stage 3/4 machine code confirms those 5 loads survive into both Java and
    native loops
- Approach: load each referenced column vector once per chunk, cache it in a
  temp, and reuse it for all compares in that block
- Risk: Low
- Effort: Medium

**3. Add a vector-accumulation count-only path**

- Impact: directly targets the observed Java/native count-only structural gap
- Type: QuestDB codegen issue
- Evidence:
  - Stage 4 native `countRows` uses `vpsubq` accumulation + one horizontal sum
  - Stage 3 Java `countRows` still does `popcntq` per chunk
- Approach: generate a count-only loop that accumulates match masks in-vector
  and reduces once at loop exit
- Risk: Medium
- Effort: Medium

### Priority 2: High payoff, medium risk

**4. Reshape generated `IN()` loops to lower register pressure**

- Impact: directly targets the observed C2 `out of virtual registers in linear
  scan` warning
- Type: Java backend / HotSpot interaction
- Evidence:
  - Stage 2 reports register-pressure failure before tier-4 retry on both
    generated `IN()` methods
  - Stage 3/4 show 5 broadcast constants plus OR-chain state live in the loop
- Approach:
  - shorten live ranges
  - group compare/or steps more locally
  - consider two-phase compare reduction instead of a single long OR chain
- Risk: Medium
- Effort: Medium-High

**5. Skip redundant mask.cast() for same-species loads**

- Impact: Eliminates 1 method call per column load per chunk; C2 already
  eliminates this at runtime, so measured impact may be zero
- Type: QuestDB codegen issue
- Approach: In `emitLoadColumn()`, only emit `mask.cast(targetSpecies)`
  when the target species differs from the loop species.
- Risk: Low
- Effort: Low

**6. Support I4 (INT) columns in vector compiler**

- Impact: Enables vector path for filters involving INT columns (e.g.,
  the fourth benchmark filter `l > 0 AND i != 0 AND d < 0.5 AND l < 1000000`
  is currently rejected because `i` is I4)
- Type: QuestDB codegen issue
- Approach: Widen INT to LONG before comparison, or handle different
  lane counts.
- Risk: Medium — lane count mismatch
- Effort: Medium-High

### Priority 3: Speculative / needs measurement

**7. Measure AVX-512 throttling**

- Impact: Unknown — may partially explain why 2x wider vectors don't
  give 2x throughput
- Type: Hardware interaction
- Approach: Run benchmarks with `perf stat` to check frequency, IPC,
  and port utilization.
- Risk: Low (measurement only)
- Effort: Low

**8. Avoid MemorySegment entirely (use Unsafe for vector ops)**

- Impact: Eliminates all MemorySegment overhead
- Type: QuestDB codegen issue
- Risk: High — may break Vector API contracts
- Effort: High

**9. Generate null check inline (skip helper call)**

- Impact: C2 already inlines `longNullGt` (36 bytes), so measured
  impact is likely zero. Would only reduce bytecode size.
- Type: QuestDB codegen issue
- Risk: Low
- Effort: Low

## Investigation conclusions

### Why is JAVA_VECTOR_BYTECODE slower than NATIVE_SIMD?

The answer now depends on the filter shape:

1. **For simple predicates like `l > 42`**, the Java hot loop is already
   close to ideal. The strongest remaining explanations are setup-path
   overhead (`MemorySegment` construction/validation), safepoint polling,
   and possible AVX-512 frequency effects.
2. **For larger straight-line vector programs like `IN()`**, the gap is no
   longer “lack of vectorization.” The main Java-specific issues are:
   - setup-path overhead
   - higher register pressure in the generated loop shape
   - weaker count-only reduction strategy
3. **Repeated column loads in `IN()` are real**, but they are shared by the
   native backend too. They are worth fixing for absolute performance, but
   they do not by themselves explain why Java loses to native.

### What part is QuestDB-controlled?

- MemorySegment caching/avoidance: **100%**
- `IN()` load-CSE / loop reshaping: **100%**
- Count-only vector accumulation strategy: **100%**
- I4 column support: **100%**
- Redundant mask.cast: **100%** (but C2 handles it already)

### What part is HotSpot / Vector API / hardware?

- Safepoint poll: **HotSpot**
- `reinterpretInternal` too big to inline: **JDK**
- AVX-512 throttling: **hardware** (if confirmed)

### Top 3 optimization opportunities

1. **MemorySegment caching or avoidance** — eliminate per-call setup
2. **Load-CSE plus loop reshaping for straight-line `IN()`** — reduce both
   repeated work and register pressure
3. **Vector-accumulating count-only path** — close the observed native
   structural advantage in count-only loops

### Caveat

No per-cause timing isolation was performed. The ranking above is based
on structural analysis of the generated code, not on measured
contribution to the overall gap. Targeted microbenchmarks would be needed
to validate the relative impact of each cause.
