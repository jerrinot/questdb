# Stage 4: Native asmjit Backend Comparison

Date: 2026-03-28

## Context

The native JIT backend uses asmjit to generate x86 machine code at runtime.
The SIMD path targets AVX2 (256-bit YMM registers). **There is no AVX-512
path in the native backend.**

On AVX-512-capable hardware (like the test machine), the Java Vector API
path uses 512-bit ZMM registers while the native path uses 256-bit YMM
registers.

## Method

Both outputs are actual machine code dumps:
- **Java Vector Bytecode**: captured via `-XX:+PrintAssembly` (Stage 3)
- **Native asmjit**: captured via `cairo.sql.jit.debug.enabled=true`,
  which activates asmjit's `FileLogger(stdout)` in `compiler.cpp:924`

Driver: `NativeJitAsmDumpDriver` test with `SqlJitMode.JIT_MODE_ENABLED`
and `JitBackend.CPP`.

Important: `SqlJitMode.JIT_MODE_ENABLED = 0` (not 1, which is
`FORCE_SCALAR`). Setting `setJitMode(1)` produces scalar output.

## Filter 1: `l > 42` (pure I8, null checks)

### Native SIMD (AVX2) — actual asmjit dump

filterRows hot loop (4 longs per YMM iteration):

```asm
L3:
vmovdqu    ymm0, ymmword ptr [r11+r8*8]   ; load 4 I64 values
vpcmpgtq   ymm2, ymm0, ymm4               ; col > 42
vpcmpeqq   ymm1, ymm0, ymmword ptr [L2+96]; col == LONG_NULL
vpcmpeqq   ymm0, ymm4, ymmword ptr [L2+96]; 42 == LONG_NULL (always false)
vpor       ymm1, ymm1, ymm0               ; anyNull = lhsNull | rhsNull
vpcmpeqd   ymm0, ymm0, ymm0               ; ymm0 = all-ones (-1)
vpxor      ymm0, ymm1, ymm0               ; neitherNull = NOT anyNull
vpand      ymm0, ymm2, ymm0               ; result = (col>42) AND neitherNull
vmovmskpd  esi, ymm0                       ; extract 4 mask bits → GPR
test       esi, esi                        ; short-circuit if no match
jz         L5
; compress_register (PEXT+PDEP+VPERMPS):
vpmovmskb  ecx, ymm0                      ; extract byte mask
pext       edx, 1985229328, ecx            ; parallel bits extract
pdep       rdx, rdx, 1085102592571150095   ; parallel bits deposit
vmovq      xmm0, rdx                      ; move to XMM
vpmovzxbd  ymm0, xmm0                     ; unpack to 32-bit indices
vpermps    ymm0, ymm0, ymm3               ; gather matching row IDs
vmovdqu    ymmword ptr [r9+rax*8], ymm0    ; store 4 row IDs (unmasked)
popcnt     esi, esi                        ; count matches
add        rax, rsi                        ; output_count += matches
L5:
vpaddq     ymm3, ymm3, ymm5               ; row_ids += [4,4,4,4]
add        r8, 4                           ; index += 4
cmp        r8, rdi                         ; index < stop
short jl   L3
```

Tail (scalar, handles remaining 1-3 rows):
```asm
L6:
mov        rdi, qword ptr [r11+r8*8]      ; load 1 value
movabs     rcx, -9223372036854775808       ; LONG_NULL
xor        rsi, rsi
cmp        rdi, rcx
rex setnz  sil                             ; lhsNotNull
xor        rdx, rdx
cmp        r10, rcx
setnz      dl                             ; rhsNotNull (42 vs NULL, always 1)
and        rdx, rsi                        ; bothNotNull
xor        rcx, rcx
cmp        rdi, r10
setnle     cl                             ; col > 42
and        rcx, rdx                        ; result = gt AND bothNotNull
test       ecx, ecx
jz         L8
mov        qword ptr [r9+rax*8], r8        ; store row ID
add        rax, 1
L8:
add        r8, 1
cmp        r8, rbx
short jl   L6
```

countRows hot loop (no row-ID output):
```asm
L3:
vmovdqu    ymm0, ymmword ptr [r11+r8*8]   ; load 4 I64
vpcmpgtq   ymm3, ymm0, ymm4               ; col > 42
vpcmpeqq   ymm1, ymm0, ymmword ptr [L2+32]; col == LONG_NULL
vpcmpeqq   ymm0, ymm4, ymmword ptr [L2+32]; 42 == LONG_NULL
vpor       ymm1, ymm1, ymm0               ; anyNull
vpcmpeqd   ymm0, ymm0, ymm0               ; all-ones
vpxor      ymm0, ymm1, ymm0               ; neitherNull
vpand      ymm0, ymm3, ymm0               ; result
vpsubq     ymm2, ymm2, ymm0               ; acc -= mask (adds 1 per true lane)
add        r8, 4
cmp        r8, rcx
short jl   L3
; horizontal sum:
vextracti128 xmm0, ymm2, 1
vpaddq     xmm2, xmm2, xmm0
vpshufd    xmm0, xmm2, 78
vpaddq     xmm2, xmm2, xmm0
vmovq      rax, xmm2
```

### Java Vector Bytecode (AVX-512) — observed in Stage 3

filterRows hot loop (8 longs per ZMM iteration):

```asm
vmovdqu64   (%r10), %zmm3 {%k7} {z}    ; masked load 8 I64 values
vpcmpnleq   %zmm2, %zmm3, %k6          ; compare GT → mask register k6
kmovq       %k6, %r10                   ; mask → GPR
popcntq     %r10, %rcx                  ; count matches
testl       %ecx, %ecx                  ; short-circuit
je          skip
vpbroadcastq %rsi, %zmm3                ; broadcast current row offset
vpaddq      %zmm3, %zmm1, %zmm3        ; rowIds = iota + offset
vpcompressq %zmm3, %zmm3 {%k6} {z}     ; compress row IDs (single insn)
vmovdqu32   %zmm3, (%r10)              ; store (fast path: all lanes)
addq        %r9, %r11                   ; filteredCount += trueCount
```
Plus safepoint poll (`testl %eax, (%r9)`) per iteration.

Note: the Java path uses `longNullGt()` helper for null-aware GT. C2
inlines this to equivalent `vpcmpeqq` + mask logic, similar to the native
null check sequence. The Java hot loop shown above is the non-null-check
variant (from Stage 3 driver which used no null checks). The benchmark
runs with null checks, so the actual Java hot loop includes similar null
sentinel detection instructions.

### Side-by-side comparison

| Aspect | Native AVX2 (actual dump) | Java AVX-512 (actual dump) |
|--------|--------------------------|---------------------------|
| Vector width | 256-bit (4 longs) | 512-bit (8 longs) |
| Rows per iteration | 4 | 8 |
| Compare | `vpcmpgtq` | `vpcmpnleq` |
| Null check | 4 insns: 2x `vpcmpeqq` + `vpor` + `vpxor` + `vpand` | Similar via C2-inlined `longNullGt` |
| Mask extract | `vmovmskpd` → 4-bit GPR | `kmovq` → 8-bit k-register |
| Compress | 6 insns: `vpmovmskb`+`pext`+`pdep`+`vmovq`+`vpmovzxbd`+`vpermps` | 1 insn: `vpcompressq` |
| Store | Unmasked `vmovdqu` (256-bit) | Fast: unmasked; slow: masked |
| Row-ID tracking | YMM += [4,4,4,4] per chunk | broadcast(row) + iota.add per chunk |
| Safepoint poll | None | 1 instruction per iteration |
| Tail handling | Scalar loop | `indexInRange` mask (no scalar tail) |
| Constant fold | `42 == LONG_NULL` computed every chunk (wasted) | C2 may fold if inlined |
| Count-only | `vpsubq` accumulator (no memory writes) | `trueCount` + `i2l` + `ladd` |

### Key observations

1. **Java processes 2x more data per iteration** (8 vs 4 longs), but with
   AVX-512 frequency throttling this may not translate to 2x throughput.

2. **Native wastes work on constant null check**: `vpcmpeqq ymm0, ymm4,
   [L2+96]` compares 42 against LONG_NULL every iteration — the result is
   always false. The asmjit compiler doesn't constant-fold this.

3. **Compress cost**: native uses 6 instructions (PEXT+PDEP+VPERMPS chain)
   vs Java's single `vpcompressq`. This is a significant per-chunk cost
   when many rows match.

4. **Count-only path**: native accumulates via `vpsubq` (subtracting the
   all-ones mask adds 1 per true lane), then does a 3-instruction
   horizontal sum at loop exit. Java calls `trueCount()` per chunk which
   C2 likely compiles to `popcnt`.

5. **Safepoint poll**: Java adds 1 instruction per iteration. Native has
   zero such overhead.

## Filter 2: `l > 42 AND d < 100.0` (mixed I8+F8)

Not yet dumped. Both I8 and F8 are 8 bytes, so `exec_hint = 1`
(single-size). The native backend should use SIMD for this filter too.
To be captured in a follow-up.

## Filter 3: `l IN (1, 2, 3, 4, 5)` (pure I8, straight-line OR)

### Native SIMD (AVX2) — actual asmjit dump

filterRows hot loop:

```asm
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm4, ymm0, ymm11
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm3, ymm0, ymm10
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm2, ymm0, ymm9
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm1, ymm0, ymm8
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm0, ymm0, ymm7
vpor       ymm0, ymm0, ymm1
vpor       ymm0, ymm0, ymm2
vpor       ymm0, ymm0, ymm3
vpor       ymm0, ymm0, ymm4
vmovmskpd  esi, ymm0
test       esi, esi
jz         L5
; compress_register:
vpmovmskb  ecx, ymm0
pext       edx, 1985229328, ecx
pdep       rdx, rdx, 1085102592571150095
vpmovzxbd  ymm0, xmm0
vpermps    ymm0, ymm0, ymm5
vmovdqu    ymmword ptr [r9+rax*8], ymm0
popcnt     esi, esi
add        rax, rsi
```

countRows hot loop:

```asm
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm5, ymm0, ymm10
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm4, ymm0, ymm9
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm2, ymm0, ymm8
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm1, ymm0, ymm7
vmovdqu    ymm0, ymmword ptr [rbx+r8*8]
vpcmpeqq   ymm0, ymm0, ymm6
vpor       ymm0, ymm0, ymm1
vpor       ymm0, ymm0, ymm2
vpor       ymm0, ymm0, ymm4
vpor       ymm0, ymm0, ymm5
vpsubq     ymm3, ymm3, ymm0
```

The native `IN()` path therefore also reloads the same column once per value.
That matters: repeated loads are an optimization opportunity, but they are **not
the main explanation of the Java-vs-native gap**, because both backends do it.

### Java Vector Bytecode (AVX-512) — actual tier-4 dump

filterRows hot loop:

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

countRows hot loop:

```asm
vmovdqu64   (%r8), %zmm0 {%k4} {z}
vpcmpeqq    %zmm2, %zmm0, %k7

vmovdqu64   (%rcx), %zmm0 {%k4} {z}
vpcmpeqq    %zmm6, %zmm0, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r8), %zmm8 {%k4} {z}
vpcmpeqq    %zmm9, %zmm8, %k6
korb        %k6, %k7, %k7

vmovdqu64   (%r9), %zmm0 {%k4} {z}
vpcmpeqq    %zmm3, %zmm0, %k6
korb        %k6, %k7, %k7

kmovq       %k7, %r9
popcntq     %r9, %rcx
```

C2 also reports, for both generated `IN()` methods:

```text
COMPILE SKIPPED: out of virtual registers in linear scan (retry at different tier)
```

### Side-by-side comparison for `IN()`

| Aspect | Native AVX2 | Java AVX-512 |
|--------|-------------|--------------|
| Vector width | 256-bit / 4 longs | 512-bit / 8 longs |
| Column loads per chunk | 5 | 5 |
| Compares per chunk | 5 `vpcmpeqq` | 5 `vpcmpeqq` |
| Mask OR chain | 4 `vpor` | 4 `korb` |
| Row compaction | `vmovmskpd` + `pext` + `pdep` + `vpermps` | `kmovq` + `vpcompressq` |
| Count-only reduction | vector accumulation (`vpsubq`) | per-chunk `popcntq` |
| Setup path | raw pointers | `MemorySegment` construction and checks |
| Register-pressure warning | none observed | yes, from C2 linear scan |

### What changed in the investigation

1. **`IN()` is definitely vectorized on both sides.**
2. **Both backends reload the column once per compare.** Column-load
   deduplication is still worth doing, but it no longer explains the relative
   benchmark gap by itself.
3. **Java's two clearest backend-specific disadvantages are now visible:**
   - heavier setup through `MemorySegment`
   - higher register pressure in the five-way straight-line OR shape
4. **Count-only is structurally weaker on Java for `IN()`.** Native uses
   vector accumulation; Java still does `popcnt` every chunk.

## Reproduction

```bash
# Run from IDE: NativeJitAsmDumpDriver#dumpLongGt42
# Or via Maven (stdout captured by surefire, check output file):
export JAVA_HOME=/home/jara/.sdkman/candidates/java/25.0.2-amzn
mvn -pl core -Dtest=NativeJitAsmDumpDriver#dumpLongGt42 \
  "-DargLine=--add-modules jdk.incubator.vector" \
  -Dmaven.test.redirectTestOutputToFile=true test

# Important: use SqlJitMode.JIT_MODE_ENABLED (= 0), not 1 (= FORCE_SCALAR)
```
