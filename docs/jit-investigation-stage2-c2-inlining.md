# Stage 2: C2 Inlining and Compilation Review

Date: 2026-03-28

## Context

JDK: Amazon Corretto 25.0.2 (build 25.0.2+10-LTS)
Host: AVX-512 capable (512-bit vector width, 8 longs per vector)

Driver: `VectorBytecodeC2Driver` (20,000 warm-up iterations per method)
Flags: `--add-modules jdk.incubator.vector --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -XX:+PrintInlining`

## Filter 1: `l > 42` (pure I8, no null checks)

### Compilation status

| Method | Bytecode size | C2 tier | OSR | Notes |
|--------|--------------|---------|-----|-------|
| `filterRows` | 214 bytes | 4 | Yes (@ 71, loop header) | Full C2 optimization |
| `countRows` | 156 bytes | 4 | Yes (@ 53, loop header) | Full C2 optimization |

Both methods are well under the 8KB HugeMethodLimit. C2 compiles them at
tier 4 (maximum optimization) after tier 3 warm-up.

### Inlining status — hot loop operations

| Operation | Bytecode offset | Inlined? | Mechanism |
|-----------|----------------|----------|-----------|
| `indexInRange` | 85 | Yes | force inline by annotation |
| `VectorMask.cast` | 110 | Yes | VectorSupport::convert intrinsic |
| `LongVector.fromMemorySegment` | 113 | Yes | ScopedMemoryAccess::loadFromMemorySegmentMasked intrinsic |
| `LongVector.compare(GT, ...)` | 131 | Yes | force inline by annotation |
| `VectorMask.and` | 143 | Yes | force inline by annotation |
| `VectorMask.trueCount` | 150 | Yes | force inline by annotation |
| `LongVector.add(long)` | 164 | Yes | force inline by annotation |
| `LongVector.compress(mask)` | 169 | Yes | force inline by annotation |
| `FilterHelpers.writeCompressedRows` | 184 | Yes | inline (hot) |
| `VectorSpecies.length` | 199 | Yes | accessor |

**All hot-loop operations are fully inlined.** C2 resolves concrete types
(`Long512Vector`, `Long512Mask`) and intrinsifies the underlying
`VectorSupport` calls.

### Inlining status — setup operations

| Operation | Inlined? | Notes |
|-----------|----------|-------|
| `longSpecies()` | Yes | inline (hot) |
| `nativeByteOrder()` | Yes | inline (hot) |
| `columnSegment()` | Yes | inline (hot) |
| `segment()` | Yes | inline (hot) |
| `iotaVector()` | Yes | VectorSupport::load intrinsic |
| `broadcast(42L)` | Yes | VectorSupport::fromBitsCoerced intrinsic |

### Failed to inline

| Method | Size | Reason | Impact |
|--------|------|--------|--------|
| `reinterpretInternal` | 61 bytes | too big | Setup only — runs once per call, not per chunk |

## Filter 2: `l > 25 AND d < 0.5` (mixed I8+F8, null checks)

### Compilation status

| Method | Bytecode size | C2 tier | OSR | Notes |
|--------|--------------|---------|-----|-------|
| `filterRows` | 313 bytes | 4 | Yes (@ 109) | Full C2 optimization |

Still well under 8KB. C2 applies full optimization.

### Helper inlining — the critical question

| Helper | Bytecode size | Inlined? | Mechanism |
|--------|--------------|----------|-----------|
| `FilterHelpers.longNullGt` | 36 bytes | Yes | inline (hot) |
| `FilterHelpers.doubleVecLt` | 60 bytes | Yes | inline (hot) |
| `FilterHelpers.longNullVector` | 8 bytes | Yes | inline (hot) |
| `FilterHelpers.writeCompressedRows` | 24 bytes | Yes | inline (hot) |

**All helpers are fully inlined.** The helper call boundaries do NOT block
C2 optimization. The concern raised in the performance investigation plan
(Stage 3, "verify relevant FilterHelpers methods are or are not inlined")
is resolved: they are.

### Mask cast operations

The mixed I8+F8 filter requires two mask cast operations per chunk:
1. `activeMask.cast(DoubleVector.SPECIES_PREFERRED)` for the F8 column load
2. `doubleVecLt result.cast(LongVector.SPECIES_PREFERRED)` to normalize back

Both are inlined via `VectorSupport::convert` intrinsic. These are
functionally required (different element types → different mask types).

The redundant `activeMask.cast(LongVector.SPECIES_PREFERRED)` for the I8
column load (same species → no-op) is also inlined and should be eliminated
by C2 as dead code.

## Filter 3: `l IN (1, 2, 3, 4, 5)` (pure I8, straight-line OR)

### Compilation status

| Method | Bytecode size | C2 tier | OSR | Notes |
|--------|--------------|---------|-----|-------|
| `filterRows` | 542 bytes | 4 | Yes (@ 163) | Tier-3 compile initially skipped due register pressure |
| `countRows` | 484 bytes | 4 | Yes (@ 145) | Tier-3 compile initially skipped due register pressure |

Observed C2 log lines:

```text
io.questdb.jit.vgen/...::filterRows (542 bytes)
  COMPILE SKIPPED: out of virtual registers in linear scan (retry at different tier)

io.questdb.jit.vgen/...::countRows (484 bytes)
  COMPILE SKIPPED: out of virtual registers in linear scan (retry at different tier)
```

This is the first strong backend-specific signal that the current vector
bytecode shape is stressing HotSpot's register allocator for the five-way
`IN()` OR chain.

### Inlining status — hot loop operations

| Operation | Inlined? | Notes |
|-----------|----------|-------|
| `indexInRange` | Yes | force inline by annotation |
| 5x `LongVector.fromMemorySegment` | Yes | intrinsic path preserved for every load |
| 5x `LongVector.compare(EQ, ...)` | Yes | intrinsic path preserved |
| 4x `VectorMask.or` | Yes | force inline by annotation |
| `VectorMask.trueCount` | Yes | intrinsic path preserved |
| `LongVector.add(long)` | Yes | force inline by annotation |
| `LongVector.compress(mask)` | Yes | force inline by annotation |
| `FilterHelpers.writeCompressedRows` | Yes | inline (hot) |

So the `IN()` gap is not caused by failed inlining. The generated method is
large enough and wide enough in live values that HotSpot struggles with
register allocation before eventually producing tier-4 code.

## Key findings

1. **No inlining failures in the hot loop.** All Vector API operations and
   all `FilterHelpers` methods are fully inlined by C2.

2. **Helper boundaries are not a bottleneck.** `longNullGt` (36 bytes) and
   `doubleVecLt` (60 bytes) are well within C2's inlining threshold.

3. **VectorSupport intrinsics are reached.** The key operations —
   `load`, `compare`, `compress`, `convert`, `fromBitsCoerced` — all
   resolve to `VectorSupport` intrinsics and are late-inlined by C2.

4. **Method sizes are safe.** 214 bytes (pure I8) and 313 bytes (mixed
   I8+F8) are far below the 8KB threshold. `IN()` is bigger at 542 / 484
   bytes, but still nowhere near HugeMethodLimit.

5. **The only failed inline is `reinterpretInternal` (61 bytes)** —
   `MemorySegment.reinterpret()` in the setup path. This runs once per
   `filterRows()` call (not per chunk) and is a negligible cost.

6. **`IN()` exposes real register-pressure costs.** The first tier-3 C2
   attempts for both generated `IN()` methods are skipped with `out of
   virtual registers in linear scan`. That does not block tier-4 codegen,
   but it is hard evidence that the current five-broadcast / five-compare /
   OR-chain shape is materially heavier for HotSpot than the simpler filters.

## Implications for the performance gap

Since C2 inlines everything in the hot loop, the remaining gap between
`JAVA_VECTOR_BYTECODE` and `NATIVE_SIMD` is NOT caused by:
- helper call overhead (eliminated by inlining)
- generic Object temps / checkcast (eliminated by C2 type proofs)
- getstatic on static final fields (folded as constants)

The gap must come from either:
- the quality of machine code C2 generates from the inlined Vector API
  (register allocation, spills, instruction selection)
- fundamental Vector API overhead (MemorySegment access path, mask
  handling, compress intrinsic quality)
- register pressure in larger straight-line vector programs such as `IN()`
- row-ID compaction strategy differences (compress+masked store vs.
  native scatter/pack)
- or simply that native SIMD has less overhead per loop iteration

**Stage 3 (machine code inspection) will resolve this.**

## Reproduction

```bash
export JAVA_HOME=/home/jara/.sdkman/candidates/java/25.0.2-amzn
mvn -pl core -DskipTests test-compile

# Pure I8 filter
$JAVA_HOME/bin/java --add-modules jdk.incubator.vector \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  -XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -XX:+PrintInlining \
  -cp core/target/classes:core/target/test-classes \
  io.questdb.test.jit.VectorBytecodeC2Driver l_gt_42 > /tmp/c2_l_gt_42.log 2>&1

# Mixed I8+F8 filter
$JAVA_HOME/bin/java --add-modules jdk.incubator.vector \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  -XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -XX:+PrintInlining \
  -cp core/target/classes:core/target/test-classes \
  io.questdb.test.jit.VectorBytecodeC2Driver mixed > /tmp/c2_mixed.log 2>&1
```
