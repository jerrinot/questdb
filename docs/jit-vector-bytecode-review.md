# Review: Vector Bytecode Filter Compiler

Date: 2026-03-28

Scope:
- `core/src/main/java/io/questdb/jit/VectorBytecodeFilterCompiler.java`
- `core/src/main/java/io/questdb/jit/VectorCompiledFilter.java`
- `core/src/main/java/io/questdb/jit/VectorCompiledCountOnlyFilter.java`
- `core/src/main/java/io/questdb/jit/FilterHelpers.java`
- `core/src/test/java/io/questdb/test/jit/VectorBytecodeFilterCompilerTest.java`

This was a read-only review. No code was executed.

## Findings

### ~~High: `AUTO` does not currently select the vector bytecode backend~~ RESOLVED

`compileBytecode()` no longer returns early when the interpreter's Vector API
path is eligible. AUTO now tries vectorized bytecode first, falls back to
scalar bytecode, then interpreter.

Changes:
- `VectorCompiledFilter.compileBytecode()` — removed `usesVectorApi()` gate
- `VectorCompiledCountOnlyFilter.compileBytecode()` — same
- `JitBackend.AUTO` doc updated to reflect three-tier: vector bytecode → scalar
  bytecode → interpreter
- `AbstractCairoTest.getVectorApiExecutionCountIfSelected()` — skips counter
  when `usesBytecode()` is true
- `VectorBytecodeFilterCompiler.isSupported()` — added 1500-op limit to avoid
  64KB method size overflow (testHugeFilter regression)
- Tests: `testAutoSelectsVectorBytecodeForEligibleProgram`,
  `testAutoFallsBackToScalarForControlFlow`

### ~~High: vector arithmetic does not preserve current JIT semantics~~ RESOLVED

`emitArithmetic` now dispatches to vectorized helper methods that match
QuestDB JIT semantics:
- F8: `FilterHelpers.doubleVecArithmetic` handles div-by-zero → NaN and
  NaN propagation via `IS_NAN` + masked `div` + `blend`.
- I8 with null checks: `FilterHelpers.longVecArithmeticNull` preserves
  LONG_NULL via sentinel detection + masked division + blend.
- I8 without null checks: raw vector arithmetic (correct for non-null data).

Tests: `testLongArithmeticNullAware`, `testDoubleArithmeticDivByZero`.

### ~~High: mixed `F8` + `I8` ordered null-aware comparisons can use the wrong null vector type~~ RESOLVED

`nullVecSlot` is now always populated with a LongVector regardless of
`primaryType`. F8 comparisons detect NaN internally via `IS_NAN` in the
`doubleVecEq/Ne/Lt/Le/Gt/Ge` helpers and never use `nullVecSlot`.
StackMapTable declares `nullVecSlot` as LongVector.

Test: `testMixedDoubleFirstThenLongNullOrdered` — F8 column loaded first,
then I8 ordered null comparison with LONG_NULL values.

### ~~Medium: the implementation advertises a broader support surface than the gate actually allows~~ RESOLVED

Class-level Javadoc now says "I8, F8, and mixed I8+F8" and explicitly
notes that I4/F4 are not yet supported due to lane count mismatch.

## Coverage Notes

The dedicated tests provide good parity checks for:

- `I8` comparisons
- `I8` boolean composition
- `I8` count-only execution
- `F8` comparisons with epsilon and `NaN`
- one mixed `I8` + `F8` case

However, I did not find dedicated vector-bytecode tests for:

- floating-point arithmetic semantics, especially division
- null-aware arithmetic semantics
- mixed programs where `F8` is the first loaded type and `I8` ordered
  null-aware comparison appears later
- default `AUTO` selection of the vector bytecode backend

## Open Questions

1. Is `AUTO` intentionally supposed to keep preferring the legacy Vector API
   interpreter path for now, despite the `JitBackend` contract saying
   bytecode-first?
2. Is the intended scope of the current vector bytecode phase really only
   `I8`/`F8`? If yes, the compiler header and surrounding expectations should
   be tightened to that subset.

## Overall Assessment

The direction is sound. Generating Vector API bytecode from `LoweredProgram`
is a better long-term architecture than keeping a handwritten vector executor.

The main blockers visible in the current implementation are:

- integration preference still favoring the interpreter path in `AUTO`
- arithmetic semantics gaps
- the mixed-type null-vector bug described above

Those issues look fixable, but they are correctness and integration issues,
not cosmetic cleanup.

## Interpreter Removal Readiness

### ~~1. Runtime fallback~~ RESOLVED

The interpreter is no longer reachable in the AUTO runtime path.
`ScalarBytecodeFilterCompiler.isSupported()` returns true for all programs,
so `bytecodeFilter` is always set. The interpreter fallback in `call()` is
dead code for AUTO. The interpreter is only used when `JitBackend.JAVA_INTERPRETED`
is explicitly forced (benchmark-only mode).

### 2. Test oracle — NOT YET RESOLVED

`ScalarBytecodeFilterCompilerTest` uses `interpreterFilter()` /
`interpreterCount()` as the semantic oracle in 6 tests. Removing the
interpreter requires migrating these to use non-JIT SQL execution as the
oracle (like `CompiledFilterRegressionTest` does). This is a separate task
that does not block the current review.

### ~~3. Default integration path~~ RESOLVED

AUTO uses compiled backends in the correct order: vectorized bytecode →
scalar bytecode → interpreter (dead fallback). Resolved as part of
Finding #1.
