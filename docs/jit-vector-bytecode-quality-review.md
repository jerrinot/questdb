# Review: Vector Bytecode Codegen Quality

Date: 2026-03-28

Scope:
- `core/src/main/java/io/questdb/jit/VectorBytecodeFilterCompiler.java`
- `core/src/main/java/io/questdb/jit/FilterHelpers.java`

## Findings

### ~~High: loop-invariant vector constants are rebuilt inside the hot loop~~ RESOLVED

`emitMethod()` now scans the block's ops and emits `LoadImm` and `LoadVar`
ops before the loop start, in the setup section. The loop body skips these
ops via `instanceof` checks. Vectorized immediates and bind variables are
materialized once per call, not per chunk.

### ~~High: pure `F8` programs pay unnecessary mask/species normalization costs~~ RESOLVED

`isPureF8()` detects programs with only F8 data types (no I8 columns, vars,
immediates, arithmetic, or casts). Pure F8 programs use
`DoubleVector.SPECIES_PREFERRED` for the loop species, keeping masks as
`VectorMask<Double>` throughout the hot path.

Changes:
- `emitMethod()` uses `helpersDoubleSpecies` instead of `helpersLongSpecies`
  for pure F8 programs
- `emitLoadColumn()` skips the mask cast when the loop species already
  matches the column type
- `emitCompare()` skips the `VectorMask<Double>` → `VectorMask<Long>` cast
  for pure F8 programs
- `nullVecSlot` is not allocated for pure F8 programs (F8 comparisons
  detect NaN internally via IS_NAN helpers)
- Row-ID output: mask cast from Double → Long only happens at the compress
  boundary (one cast total, not one per compare)

### ~~Medium: the row-id path does redundant per-chunk work~~ RESOLVED

`matchCount` is now computed once via `mask.trueCount()` and stored in a
dedicated `int` local (`matchCountSlot`). The stored value is reused for
the skip branch (`ifeq`), the `writeCompressedRows` masked store parameter,
and the `filteredCount += matchCount` update. Total `trueCount()` calls per
chunk reduced from 3 to 1.

The mask cast before `compress()` now keys on `pureF8` instead of
`primaryType == F8_TYPE`. Mixed I8+F8 programs already normalize masks to
`VectorMask<Long>` in `emitCompare`, so the cast was redundant for them.
Pure F8 programs cast `VectorMask<Double>` → `VectorMask<Long>` once at
the compress boundary only.

### Medium: temporary locals are still typed as generic `Object`

The generated method stores all temps as generic object locals in the
`StackMapTable`, then repeatedly `checkcast`s them back to concrete vector
types before operations.

Lowering already knows each temp's exact type. Using more specific temp typing
would reduce emitted casts. However, the benefit is marginal: C2 eliminates
the casts after proving type consistency in straight-line code, and changing
StackMapTable types for temps would require tracking per-temp vector class
indices in the constant pool, complicating the code generator for minimal
runtime gain.

Deferred: C2 handles this well enough that the complexity isn't justified.

## What Looks Good

The compiler is using the right broad Vector API mechanisms:

- masked tail handling via `VectorSpecies.indexInRange(...)`
- direct masked `fromMemorySegment(...)` vector loads
- `LongVector.compress(...)` for row-id compaction
- masked store via `writeCompressedRows()` to prevent buffer overflow
- helper methods only for the semantically tricky cases:
  - `F8` epsilon / NaN comparisons
  - `F8` division-by-zero semantics
  - `I8` null-aware arithmetic and ordered comparisons
  - `I8` → `F8` null-aware cast (LONG_NULL → NaN)
- loop-invariant hoisting of immediates and bind variables
- native double species for pure F8 programs

## Overall Assessment

The generated vector code is now well-aligned with Java Vector API best
practices. The main hot-path inefficiencies (per-chunk constant rebuilding,
unnecessary mask normalization) have been resolved. The remaining `checkcast`
overhead from generic Object temp typing is handled by C2 and not worth the
code generator complexity to eliminate.
