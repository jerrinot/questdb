# Stage 1: Generated Vector Bytecode Review

Date: 2026-03-28

## Context

JDK: Amazon Corretto 25.0.2 (build 25.0.2+10-LTS)
Dumped via: `-Dquestdb.jit.vector.dump=/tmp/<name>.class`, disassembled with
`javap -c -p`

Test filters from `JitBackendBenchmark.java`:
- `l > 42` — pure I8
- `l > 42 AND d < 100.0` — mixed I8+F8
- `l IN (1, 2, 3, 4, 5)` — pure I8, short-circuit

## Filter 1: `l > 42` (pure I8, no null checks)

Source: `VectorBytecodeFilterCompilerTest#testLongGt`
Dump: `/tmp/vbc_l_gt_42.class` (6254 bytes)

### Generated filterRows() structure

```
SETUP (offsets 0-69):
  filteredCount = 0L                        (slot 15)
  row = 0L                                  (slot 17)
  species = longSpecies()                   (slot 19)
  order = nativeByteOrder()                 (slot 20)
  colSeg[0] = columnSegment(dataAddr, 0)    (slot 25)
  varsSeg = segment(varsAddr)               (slot 26)  <-- always allocated
  outputSeg = segment(filteredRowsAddr)     (slot 22)
  iota = iotaVector(longSpecies())          (slot 23)  <-- calls longSpecies() again
  activeMask = null                         (slot 21)
  matchCount = 0                            (slot 24, int)
  temps[0..2] = null                        (slots 27-29)
  [HOISTED] temp[0] = broadcast(SPECIES_PREFERRED, 42L)   (slot 27)

LOOP (offsets 71-208):
  if (row >= rowsCount) goto EXIT
  activeMask = species.indexInRange(row, rowsCount)
  temp[1] = LongVector.fromMemorySegment(
      SPECIES_PREFERRED,                    <-- getstatic inside loop
      colSeg, row*8, order,
      activeMask.cast(SPECIES_PREFERRED))   <-- redundant cast: mask already Long
  temp[2] = temp[1].compare(GT, temp[0])    <-- 2 checkcast before compare
  activeMask = temp[2].and(activeMask)      <-- 1 checkcast before and()
  matchCount = activeMask.trueCount()       <-- called once, good
  if (matchCount == 0) goto NEXT
  compressed = iota.add(row).compress(activeMask)
  writeCompressedRows(compressed, matchCount, outputSeg, filteredCount*8, order)
  filteredCount += matchCount

NEXT:
  row += species.length()
  goto LOOP

EXIT:
  return filteredCount
```

### Findings in hot loop

| # | Issue | Severity | Note |
|---|-------|----------|------|
| 1 | `activeMask.cast(SPECIES_PREFERRED)` at every column load | Medium | activeMask comes from `VectorSpecies<Long>.indexInRange()` — already `VectorMask<Long>`. Casting to `SPECIES_PREFERRED` (also Long) is a no-op method call per chunk. C2 may inline and eliminate it. |
| 2 | 2x `getstatic SPECIES_PREFERRED` inside loop (offsets 92, 107) | Low | C2 treats static final fields as constants, so these fold away. But they are bytecode noise — the species is already in slot 19. |
| 3 | 3x `checkcast` per chunk (offsets 120, 128, 138) | Low | Object-typed locals → repeated cast to LongVector/VectorMask. C2 eliminates these after proving type consistency. |
| 4 | `longSpecies()` called 3x in setup (offsets 6, 37, 45) | Low | Only matters at method entry, not per-chunk. |
| 5 | `varsSeg` always allocated even when no bind vars | Low | One-time cost, negligible. |

### What looks good

- Immediate 42 hoisted before loop (broadcast at offset 60-69)
- `trueCount()` called exactly once per chunk
- `matchCount` stored in int local, reused for skip/store/counter
- `writeCompressedRows` uses masked store (prevents overflow)
- Tail handled via `indexInRange` mask
- Loop structure is tight: compare → AND → count → compress → store → advance

## Filter 2: `l > 42 AND d < 100.0` (mixed I8+F8, null checks)

Source: `VectorBytecodeFilterCompilerTest#testMixedLongAndDouble`
Dump: `/tmp/vbc_mixed.class`

### Generated filterRows() hot loop

```
LOOP:
  activeMask = species.indexInRange(row, rowsCount)

  ;; Load I8 column
  temp[1] = LongVector.fromMemorySegment(SPECIES_PREFERRED, colSeg[0], row*8,
              order, activeMask.cast(SPECIES_PREFERRED))    <-- redundant cast

  ;; Compare: longNullGt(temp[1], temp[0], nullVec)
  temp[2] = FilterHelpers.longNullGt(col, imm_25, nullVec)
  ;; 2 checkcast before helper call

  ;; Load F8 column
  temp[4] = DoubleVector.fromMemorySegment(DOUBLE_SPECIES_PREFERRED, colSeg[1],
              row*8, order, activeMask.cast(DOUBLE_SPECIES_PREFERRED))
  ;; mask.cast(DoubleVector.SPECIES_PREFERRED) — REQUIRED for cross-type

  ;; Compare: doubleVecLt(temp[4], temp[3])
  temp[5] = FilterHelpers.doubleVecLt(col, imm_0_5)
  ;; result mask is VectorMask<Double>

  ;; Cast Double mask → Long mask for AND
  temp[5] = temp[5].cast(SPECIES_PREFERRED)                <-- REQUIRED

  ;; AND the two comparison masks
  temp[6] = temp[5].and(temp[2])                           <-- 2 checkcast

  ;; AND with activeMask
  activeMask = temp[6].and(activeMask)                     <-- 1 checkcast

  ;; rest same as pure I8
```

### Findings in mixed hot loop

| # | Issue | Severity | Note |
|---|-------|----------|------|
| 1 | `activeMask.cast(SPECIES_PREFERRED)` for I8 load | Medium | Same redundant cast as pure I8 — mask is already Long. |
| 2 | `activeMask.cast(DOUBLE_SPECIES_PREFERRED)` for F8 load | Required | Cross-type: Long mask must become Double mask for DoubleVector load. This is unavoidable. |
| 3 | `doubleVecLt` result → `.cast(SPECIES_PREFERRED)` | Required | Must normalize Double mask back to Long for boolean AND. |
| 4 | 5x `checkcast` in loop body | Low | C2 eliminates. More than pure I8 due to 2 columns + 2 comparisons + AND. |
| 5 | `longNullGt` helper call boundary | Medium | Helper does 3 vector ops (2 compares + OR + AND + NOT). If C2 doesn't inline it, these remain as a call with spills. |
| 6 | `doubleVecLt` helper call boundary | Medium | Helper does epsilon subtract + abs + compare. Same inlining concern. |
| 7 | Both immediates hoisted (25L and 0.5d) | Good | Broadcast before loop. |

## Filter 3: `l IN (1, 2, 3, 4, 5)` — NOT vectorized

`IN()` uses short-circuit evaluation → `hasControlFlow() == true` →
`VectorBytecodeFilterCompiler.isSupported()` returns `false` → falls back to
scalar bytecode.

This is a **significant finding**: one of the four benchmark filters does not
use vector bytecode at all. The benchmark comparison between
`JAVA_VECTOR_BYTECODE` and `NATIVE_SIMD` for this filter is comparing scalar
bytecode against native AVX2 — the vector compiler is not even in play.

### Impact on benchmarks

| Filter | Vector bytecode? | Competitor |
|--------|-----------------|------------|
| `l > 42` | Yes (pure I8) | NATIVE_SIMD uses full AVX2 vector loop |
| `l > 42 AND d < 100.0` | Yes (mixed I8+F8) | NATIVE_SIMD uses AVX2 |
| `l IN (1, 2, 3, 4, 5)` | **No** (scalar fallback) | NATIVE_SIMD uses AVX2 |
| `l > 0 AND i != 0 AND d < 0.5 AND l < 1000000` | Depends on IR shape | NATIVE_SIMD uses AVX2 |

## Summary of hot-loop bytecode patterns

### Redundant work (QuestDB codegen issue)

1. **`mask.cast(same_species)` on I8 column loads**: The activeMask from
   `indexInRange` is already `VectorMask<Long>`. Casting it to
   `LongVector.SPECIES_PREFERRED` is a no-op call. Emit `cast()` only when
   the target species differs from the loop species.

2. **`getstatic SPECIES_PREFERRED` inside loop**: The species is already stored
   in a local slot from setup. Use `aload` instead of `getstatic`. C2 likely
   folds this, but it bloats bytecode.

### Required work (not reducible without Vector API changes)

1. **Cross-type mask casts**: Mixed I8+F8 programs must cast masks between
   Long and Double species. This is fundamental to Java Vector API.

2. **Helper call boundaries**: `longNullGt`, `doubleVecLt`, etc. Each is a
   static method call. Whether C2 inlines these is a Stage 2 question.

3. **Tail handling via `indexInRange`**: Correct and necessary. The native
   backend uses a different strategy (separate scalar tail loop or AVX mask).

### Deferred (C2 handles)

1. **`checkcast` on Object-typed temps**: C2 proves type consistency and
   eliminates these. Changing this requires per-temp type tracking in
   StackMapTable, which is complex for marginal gain.

## Optimization opportunities identified (not yet validated)

| Priority | Opportunity | Type | Expected impact |
|----------|-------------|------|-----------------|
| 1 | Vectorize `IN()` predicates (short-circuit → parallel OR) | Codegen | High: currently scalar-only; native SIMD is 5x faster |
| 2 | Skip `mask.cast()` when loop species matches column species | Codegen | Medium: one fewer method call per column load per chunk |
| 3 | Use species local instead of `getstatic` inside loop | Codegen | Low: C2 folds static final, but reduces bytecode size |
| 4 | Verify helper inlining (Stage 2 dependency) | Investigation | Unknown: if helpers aren't inlined, this is high priority |

## Reproduction

```bash
export JAVA_HOME=/home/jara/.sdkman/candidates/java/25.0.2-amzn
mvn -pl core -Dtest=VectorBytecodeFilterCompilerTest#testLongGt \
  "-DargLine=--add-modules jdk.incubator.vector -Dquestdb.jit.vector.dump=/tmp/vbc_l_gt_42.class" test
$JAVA_HOME/bin/javap -c -p /tmp/vbc_l_gt_42.class
```
