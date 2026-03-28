# Review: Vector Bytecode Code Quality

Date: 2026-03-28

Scope:
- `core/src/main/java/io/questdb/jit/VectorBytecodeFilterCompiler.java`
- `core/src/main/java/io/questdb/jit/FilterHelpers.java`
- `core/src/test/java/io/questdb/test/jit/VectorBytecodeFilterCompilerTest.java`

## Findings

### ~~1. Redundant `maskCast` for same-species column loads~~ RESOLVED

`emitLoadColumn` emitted `mask.cast(targetSpecies)` for ALL column loads in
non-pureF8 programs, including I8 loads where the mask was already
`VectorMask<Long>` (matching the loop species). This added 2 bytecode
instructions (getstatic + invokevirtual) per I8 load per chunk. C2
eliminates the no-op cast at runtime, so the measured impact is likely
small, but it adds unnecessary bytecode noise and method size.

Fix: only emit `maskCast` when the column type differs from the loop
species type (`!pureF8 && lc.type() == F8_TYPE`).

Tests: all 173 JIT tests pass. Mixed I8+F8 paths (including F8-first load
order) covered by `testMixedLongAndDouble` and
`testMixedDoubleFirstThenLongNullOrdered`.

### ~~2. `species.length()` called per iteration~~ RESOLVED

The loop increment called `species.length()` via `invokeInterface` every
iteration, followed by `i2l` to convert to long. This is a constant value
(e.g., 8 for AVX-512 longs) that `VectorSpecies.length()` returns.

Fix: added a `strideSlot` (long) to the local variable layout. The stride
is computed once before the loop (`species.length()` → `i2l` → `lstore`)
and reused via `lload` in the loop increment. Saves 3 bytecodes per
iteration (aload + invokeInterface + i2l → lload).

The StackMapTable full_frame declares `strideSlot` as `ITEM_Long` between
`row` and the Object locals, matching the slot layout order.

Tests: all 173 JIT tests pass.

### 3. Temps declared as `Object` in StackMapTable

Each IR temporary maps to one JVM local of type `Object` in the
StackMapTable full_frame (line 508: `putITEM_Object(objectClassIndex)`).
Every use site emits `aload` + `checkcast` to the expected vector/mask
type. C2 eliminates these casts after proving type consistency (which is
always the case in straight-line code).

**Why not fix:** Precise typing of temps in the full_frame is feasible in
principle (each temp has exactly one definition site with a known type).
However:

1. C2 already eliminates the checkcasts at runtime. The Stage 3 machine
   code review confirmed no checkcast overhead survives into the hot loop.
2. Tracking per-temp types requires a type inference pass over the lowered
   ops, adding complexity for zero measured benefit.
3. The current `Object` typing keeps StackMapTable generation simple and
   correct for all program shapes.

**Status:** Won't fix. Documented as a known bytecode-level imperfection
with no runtime impact.

## Loop-Invariant Hoisting Status

The compiler already hoists all loop-invariant values before the loop:
- Vector immediates (`LoadImm`): hoisted at line 316-317
- Bind variables (`LoadVar`): hoisted at line 318-319
- Column `MemorySegment` objects: hoisted at line 251-255
- Vars `MemorySegment`: hoisted at line 257-259
- Output `MemorySegment` and iota vector: hoisted at line 262-268
- Null sentinel vector: hoisted at line 271-278
- Species and byte order: hoisted at line 241-249
- Stride (species.length()): hoisted at line 250-253

Column loads inside the loop use a `loadCache` (HashMap) to deduplicate
repeated loads of the same column+type within a single iteration. The
`tryEmitLongInEqOrChain` optimization further reduces IN()-style EQ+OR
chains to a single column load.

## Row-ID Hot-Path Assessment

The filterRows terminator path is minimal:
1. `iota.add(row)` — 1 invokevirtual
2. `compress(mask)` — 1 invokevirtual (vpcompressq on AVX-512)
3. `writeCompressedRows(...)` — 1 invokestatic (masked store)
4. `filteredCount += matchCount` — iload + i2l + lload + ladd + lstore

The `filteredCount * 8` byte offset multiply (ldc2_w + lmul) could be
eliminated by tracking the byte offset directly, but this is a minor
constant-factor savings that adds code complexity.

**Status:** No redundant row-ID hot-path work remains.
