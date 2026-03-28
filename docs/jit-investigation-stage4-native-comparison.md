# Stage 4: Native asmjit Backend Comparison

Date: 2026-03-28

## Context

The native JIT backend uses asmjit to generate x86 machine code at runtime.
The SIMD path targets AVX2 (256-bit YMM registers). **There is no AVX-512
path in the native backend** — the codebase has zero references to AVX-512,
ZMM registers, or 512-bit operations.

On AVX-512-capable hardware (like the test machine), the Java Vector API
path uses 512-bit ZMM registers while the native path uses 256-bit YMM
registers. This means the Java path processes 2x more data per loop
iteration.

## Filter 1: `l > 42` (pure I8, null checks)

### Native SIMD (AVX2) hot loop

The native backend processes 4 longs per iteration using YMM registers:

```
vmovdqu     ymmword [rdi + idx*8], ymm0     ; load 4 I64 values
vpcmpgtb    ymm1, ymm0, broadcast_42       ; compare GT (all lanes)
vmovmskpd   r8d, ymm1                       ; extract 4 mask bits
test        r8d, r8d                        ; short-circuit if no match
jz          skip
; compress_register: PEXT + PDEP + VPERMPS sequence
vpermps     ymm2, ymm1, row_ids_reg         ; gather matching row IDs
vmovdqu     [out + output_idx*8], ymm2       ; store (unmasked, overwrite ok)
popcnt      r8d, r8d                        ; count matches
add         output_idx, r8                  ; advance output
skip:
vpaddq      row_ids_reg, row_ids_step       ; increment row ID vector by 4
add         idx, 4
cmp         idx, stop
jl          loop
```

Tail: separate scalar loop for remaining 1-3 elements.

### Java Vector Bytecode (AVX-512) hot loop

Processes 8 longs per iteration using ZMM registers:

```
vmovdqu64   (%r10), %zmm3 {%k7} {z}        ; masked load 8 I64 values
vpcmpnleq   %zmm2, %zmm3, %k6              ; compare GT → mask register
kmovq       %k6, %r10                       ; mask → GPR
popcntq     %r10, %rcx                      ; count matches
testl       %ecx, %ecx                      ; short-circuit if no match
je          skip
vpbroadcastq %rsi, %zmm3                    ; broadcast current row offset
vpaddq      %zmm3, %zmm1, %zmm3            ; rowIds = iota + offset
vpcompressq %zmm3, %zmm3 {%k6} {z}         ; compress row IDs (AVX-512)
vmovdqu32   %zmm3, (%r10)                   ; store (fast path: all lanes)
addq        %r9, %r11                       ; filteredCount += trueCount
skip:
; safepoint poll: testl %eax, (%r9)
add         rsi, 8                          ; advance by 8 rows
```

### Side-by-side comparison

| Aspect | Native AVX2 | Java AVX-512 |
|--------|------------|--------------|
| Vector width | 256-bit (4 longs) | 512-bit (8 longs) |
| Core instructions/chunk | ~15 (match) / ~5 (no match) | ~10 (match) / ~5 (no match) |
| Rows per chunk | 4 | 8 |
| Compress strategy | PEXT+PDEP+VPERMPS (3-4 insns) | vpcompressq (1 insn) |
| Store | Unmasked vmovdqu (256-bit) | Fast: unmasked vmovdqu32 (512-bit); Slow: masked |
| Row-ID tracking | YMM register += [4,4,4,4] | broadcast(row) + iota.add(row) |
| Safepoint poll | None | 1 instruction per iteration |
| Tail handling | Scalar loop | indexInRange mask |
| Null handling | cmp vs LONG_NULL per-element pre-scan | longNullGt helper (inline: cmp + mask AND) |
| Bounds checks in loop | None | None (hoisted by C2) |

### Key differences

1. **Java uses AVX-512, native uses AVX2.** The Java path processes 2x
   more data per iteration. This is a significant advantage for Java on
   AVX-512 hardware.

2. **vpcompressq vs PEXT+PDEP+VPERMPS.** The Java path uses a single
   AVX-512 compress instruction while native must emulate it with 3-4
   AVX2 instructions.

3. **Safepoint poll.** The Java path adds one `testl` instruction per
   loop iteration for safepoint polling. This is unavoidable in JVM code.

4. **Row-ID computation.** Native tracks row IDs in a YMM register
   (vpaddq per chunk). Java broadcasts the current row offset and adds
   iota per chunk. Both approaches are similar cost.

5. **MemorySegment setup.** Every `filterRows()` call constructs
   MemorySegment objects from raw addresses. The `reinterpretInternal`
   (61 bytes) fails to inline. This is per-call overhead that native
   doesn't have.

## Filter 2: `l > 42 AND d < 100.0` (mixed I8+F8, null checks)

### Native SIMD path

The mixed I8+F8 filter has `exec_hint = 1` (single-size, both 8 bytes).
The native backend DOES use SIMD for this — both I8 and F8 fit in 256-bit
YMM registers with the same element count (4 per register). The loop
processes both column comparisons vectorized and combines results with
`vpand`.

### Java Vector Bytecode path

Uses AVX-512 with separate Long and Double vector loads. Mask cast overhead
per chunk:
1. `activeMask.cast(DoubleVector.SPECIES_PREFERRED)` for F8 load
   (Long mask → Double mask via VectorSupport::convert)
2. `doubleVecLt result.cast(LongVector.SPECIES_PREFERRED)` to normalize
   (Double mask → Long mask via VectorSupport::convert)

These compile to `VectorSupport::convert` intrinsics which C2 lowers to
`kunpck`/`kmov` instructions. This is 2 extra mask conversion instructions
per chunk that native doesn't need (native keeps everything in 256-bit
register space with integer masks).

### Side-by-side comparison (mixed)

| Aspect | Native AVX2 | Java AVX-512 |
|--------|------------|--------------|
| Loop type | SIMD (both types vectorized) | SIMD (both types vectorized) |
| Rows per chunk | 4 | 8 |
| Mask conversions | 0 (uniform 256-bit) | 2 per chunk (Long↔Double) |
| Null handling | Inlined per-element check | longNullGt helper (C2-inlined) |

## Filter 3: `l IN (1, 2, 3, 4, 5)` (straight-line OR)

### Native SIMD path

Uses AVX2 SIMD with 4-element chunks. Each value in the IN list is
compared vectorized, and results are OR'd together.

### Java Vector Bytecode path

**Vectorized.** For single-size columns (all LONG), the IR serializer
emits plain `EQ` + `OR` ops (no short-circuit). The vector compiler
accepts the straight-line program and generates AVX-512 code that:
1. Loads the column once per IN value (5 loads for 5 values)
2. Broadcasts each constant and compares
3. ORs all masks together

The IN() path is vectorized but loads the column 5 times per chunk
(once per IN value) instead of once. The native backend similarly
loads the column per value but at 256-bit width. The Java path
processes 8 elements per load vs native's 4, but does 5 loads per
chunk vs potentially fewer in native with register reuse.

The 5.6x gap for IN() may come from:
- Column re-loading overhead (5 vector loads vs 1 needed)
- MemorySegment construction per call (same as other filters)
- Possibly lower data cache efficiency with 5x 512-bit loads

## Summary

| Filter | Native | Java | Gap source |
|--------|--------|------|------------|
| `l > 42` | AVX2 (4 wide) | AVX-512 (8 wide) | Per-call setup, safepoint |
| `l > 42 AND d < 100.0` | AVX2 (4 wide) | AVX-512 (8 wide) + mask cast | Setup + 2 mask casts/chunk |
| `l IN (1,2,3,4,5)` | AVX2 (4 wide) | AVX-512 (8 wide), 5 loads/chunk | Column re-loads, setup |
