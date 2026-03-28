# Stage 3: HotSpot Machine Code Review

Date: 2026-03-28

## Context

JDK: Amazon Corretto 25.0.2 (build 25.0.2+10-LTS)
Host: AVX-512 capable (ZMM registers, 512-bit = 8 longs per vector)
Disassembler: hsdis-amd64.so (capstone 5.0.6)

Driver: `VectorBytecodeC2Driver` (20,000 warm-up iterations)
Flags: `--add-modules jdk.incubator.vector -XX:+UnlockDiagnosticVMOptions
-XX:+PrintAssembly -XX:CompileCommand=print,io.questdb.jit.vgen::filterRows
-XX:CompileCommand=print,io.questdb.jit.vgen::countRows`

## Filter: `l > 42` (pure I8, no null checks)

### countRows hot loop — core sequence

```asm
vmovdqu64   (%r8), %zmm2 {%k7} {z}     ; masked load 8 longs from column
vpcmpnleq   %zmm1, %zmm2, %k6          ; compare: k6 = (col > 42)
kmovq       %k6, %rcx                   ; mask → GPR for popcount
popcntq     %rcx, %r9                   ; trueCount = popcount(mask)
addq        %rax, %r9                   ; count += trueCount
; loop control (safepoint poll, stride by 8, back edge)
```

**5 core instructions per chunk.** This is near-optimal for a count-only
AVX-512 filter. The loop body has been unrolled 2x by C2.

The `zmm1` register holds the broadcast constant 42 (loop-invariant, hoisted
by C2). The `k7` mask register holds the tail mask from `indexInRange`.

### filterRows hot loop — core sequence

```asm
; === VECTOR LOAD + COMPARE ===
vmovdqu64   (%r10), %zmm3 {%k7} {z}    ; masked load 8 longs
vpcmpnleq   %zmm2, %zmm3, %k6          ; compare GT → mask k6
kmovq       %k6, %r10                   ; mask → GPR
popcntq     %r10, %rcx                  ; trueCount

; === SKIP CHECK ===
testl       %ecx, %ecx                  ; if (trueCount == 0)
je          skip                        ;   skip output entirely

; === ROW-ID COMPACTION ===
vpbroadcastq %rsi, %zmm3                ; broadcast current row offset
vpaddq      %zmm3, %zmm1, %zmm3        ; rowIds = iota + offset
vpcompressq %zmm3, %zmm3 {%k6} {z}     ; compress to contiguous lanes

; === MASKED STORE (fast path: trueCount == 8) ===
cmpl        $8, %ecx                    ; if (trueCount == lanes)
jl          slow_store                  ;   goto masked store
vmovdqu32   %zmm3, (%r10)              ; unmasked full-vector store

; === UPDATE COUNTER ===
addq        %r9, %r11                   ; filteredCount += trueCount
```

**10 core instructions per chunk** (load + compare + popcount + broadcast +
add + compress + store + counter update, plus skip branch).

C2 split `writeCompressedRows` into two paths:
- **Fast path** (`trueCount == 8`): unmasked `vmovdqu32` store — no mask
  overhead, 1 store instruction
- **Slow path** (`trueCount < 8`): creates mask via `indexInRange(0,
  trueCount)`, uses masked `vmovdqu32 {%k}` store — correct but adds
  mask setup cost

The fast path dominates when selectivity is high (most rows match).

Loop body unrolled 2x by C2.

### MemorySegment access path

The `vmovdqu64 (%r10), %zmm3 {%k7} {z}` instruction shows that C2
successfully eliminated all MemorySegment boundary checks from the hot path.
The `ScopedMemoryAccess::loadFromMemorySegmentMasked` intrinsic compiled
down to a single masked vector load instruction.

In the setup (before the loop), C2 emits the segment bounds check
(`cmpq %r9, %rbx; jg slow_path`) and the scope state check
(`testl %ecx, %ecx; jne slow_path`). If these pass, the loop body runs
with direct memory access.

### What C2 eliminated

From the original bytecode, C2 eliminated:
- All `checkcast` instructions (type proven consistent)
- The `getstatic SPECIES_PREFERRED` loads (folded as compile-time constants)
- The redundant `mask.cast(SPECIES_PREFERRED)` (dead code — same species)
- `VectorSpecies.length()` (folded as constant 8)
- All Object-typed local store/load overhead
- MemorySegment scope checks from the hot loop (hoisted to setup)

### Observations

1. **The machine code is clean.** C2 generates the expected AVX-512
   instruction sequence with no visible overhead from the Java Vector API
   abstraction.

2. **No spills in the hot loop.** The core sequence uses registers only.
   ZMM1 (iota), ZMM2 (broadcast constant), ZMM3 (working register), K6
   (result mask), K7 (tail mask) — all stay in registers.

3. **C2 unrolled the loop 2x.** Two copies of the core loop body appear
   in the compiled code. The second copy processes the next 8-element chunk
   without returning to the loop header.

4. **writeCompressedRows is split into fast/slow paths.** C2 hoisted the
   `trueCount == lanes` check and emits an unmasked store for the common
   case (all lanes match). The masked store path runs only for partial
   matches. This is correct and efficient.

5. **The safepoint poll is present** (`testl %eax, (%r9)` at the back
   edge), adding one instruction per loop iteration. This is unavoidable
   in JVM hot loops.

## Filter: `l IN (1, 2, 3, 4, 5)` (pure I8, straight-line OR)

### C2 output summary

HotSpot emits a real vectorized `IN()` loop for both row-ID and count-only
paths. The loop is not scalarized and still uses AVX-512 `ZMM` registers.

The key new facts versus the simpler `l > 42` filter are:

- the hot loop performs **5 separate masked loads** of the same column per
  chunk
- it performs **5 separate equality compares**
- it accumulates the result with **4 `korb` mask ORs**
- both generated methods were first seen by C2 as
  `COMPILE SKIPPED: out of virtual registers in linear scan`

### filterRows hot loop — observed sequence

Excerpt from the tier-4 dump:

```asm
vmovdqu64   (%r11), %zmm0 {%k4} {z}
vpcmpeqq    %zmm4, %zmm0, %k7

vmovdqu64   (%r11), %zmm0 {%k4} {z}
vpcmpeqq    %zmm8, %zmm0, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r11), %zmm0 {%k4} {z}
vpcmpeqq    %zmm7, %zmm0, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r11), %zmm0 {%k4} {z}
vpcmpeqq    %zmm6, %zmm0, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r10), %zmm0 {%k4} {z}
vpcmpeqq    %zmm5, %zmm0, %k6
korb        %k6, %k7, %k7

kmovq       %k7, %r11
popcntq     %r11, %r10
vpbroadcastq %r13, %zmm0
vpaddq      %zmm0, %zmm3, %zmm0
vpcompressq %zmm0, %zmm10 {%k7} {z}
```

This confirms that the bytecode-level repeated `MEM col0` operations survive
all the way into machine code. C2 does **not** collapse the five loads into a
single load plus multiple compares.

### countRows hot loop — observed sequence

Excerpt from the tier-4 dump:

```asm
vmovdqu64   (%r8), %zmm0 {%k4} {z}
vpcmpeqq    %zmm2, %zmm0, %k7

vmovdqu64   (%rcx), %zmm0 {%k4} {z}
vpcmpeqq    %zmm6, %zmm0, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r8), %zmm8 {%k4} {z}
vpcmpeqq    %zmm9, %zmm8, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r8), %zmm8 {%k4} {z}
vpcmpeqq    %zmm4, %zmm8, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r9), %zmm0 {%k4} {z}
vpcmpeqq    %zmm3, %zmm0, %k6
korb        %k6, %k7, %k7

kmovq       %k7, %r9
popcntq     %r9, %rcx
addq        %rdi, %r9
```

The count-only `IN()` path still uses **per-chunk `popcnt`**. It does not
adopt the native AVX2 strategy of accumulating mask results in a vector and
doing one horizontal reduction after the main loop.

### What this means

1. **Repeated loads are real**, not just a bytecode artifact.
2. **Register pressure is also real.** The five broadcast constants plus the
   live mask/iota/setup state are enough to trigger the linear-scan
   register-pressure warning before tier-4 retry.
3. **The Java `IN()` gap is not explained by lack of vectorization.** The
   loop is fully vectorized, but it is still a heavy straight-line program.
4. **Count-only has a separate weakness.** The Java path pays `popcnt`
   every chunk, while the native count-only backend accumulates in-vector.

## Comparison with expected native AVX2/AVX-512 equivalent

For reference, the ideal native AVX-512 loop for `col > 42` would be:

```asm
vmovdqu64   (%rsi), %zmm0               ; load 8 longs
vpcmpnleq   broadcast_42, %zmm0, %k1    ; compare GT
; (count-only: popcnt + add)
; (row-ID: broadcast + add + compress + store)
```

The Java vector bytecode output matches this structure exactly. The
additional overhead per chunk is:
- 1 instruction: safepoint poll (unavoidable JVM cost)
- 1 instruction: scope state check (hoisted to setup, not per-chunk)
- ~2 instructions: masked store path logic (fast/slow split)

The native backend would not have the safepoint poll or the scope check.
For a hot loop processing 128M rows, the safepoint poll adds ~16M extra
instructions total — a measurable but small overhead (~3-5%).

## Implications

The machine code quality is **not** the primary source of the performance
gap between `JAVA_VECTOR_BYTECODE` and `NATIVE_SIMD`. The generated x86
instructions are nearly identical to what a hand-written AVX-512
implementation would produce.

Possible remaining sources of the gap:
1. **Safepoint poll overhead** — ~3-5% for simple filters, unavoidable
2. **MemorySegment setup cost** — per-call segment construction and bounds
   checking (before the loop)
3. **Register pressure in larger vectorized programs** — confirmed for `IN()`
   by `out of virtual registers in linear scan`
4. **Repeated column loads for straight-line `IN()`** — confirmed to survive
   into machine code
5. **Row-ID compaction strategy** — still needs direct side-by-side review
   against the native loop
6. **Benchmark mode differences** — the JMH benchmark may measure the full
   call path including segment setup, not just the hot loop

**Stage 4 (native comparison) must now distinguish simple-predicate gaps from
`IN()`-specific gaps.**

## Reproduction

```bash
export JAVA_HOME=/home/jara/.sdkman/candidates/java/25.0.2-amzn
mvn -pl core -DskipTests test-compile

LD_LIBRARY_PATH=/tmp/capstone-5.0.6 $JAVA_HOME/bin/java \
  --add-modules jdk.incubator.vector \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  -XX:+UnlockDiagnosticVMOptions -XX:+PrintAssembly \
  "-XX:CompileCommand=print,io.questdb.jit.vgen::filterRows" \
  "-XX:CompileCommand=print,io.questdb.jit.vgen::countRows" \
  -cp core/target/classes:core/target/test-classes \
  io.questdb.test.jit.VectorBytecodeC2Driver l_gt_42 > /tmp/asm_l_gt_42.log 2>&1
```
