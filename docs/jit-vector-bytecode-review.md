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

### Medium: the implementation advertises a broader support surface than the gate actually allows

The class-level comment says the compiler supports:

- `I4`, `I8`, `F4`, `F8`
- same-width mixed pairs (`I4+F4`, `I8+F8`)

Reference:
- `core/src/main/java/io/questdb/jit/VectorBytecodeFilterCompiler.java`

But the current gate only admits:

- `I8`
- `F8`

Reference:
- `core/src/main/java/io/questdb/jit/VectorBytecodeFilterCompiler.java`
  `isSupportedVectorType()`

This mismatch is already visible in the tests:

- `testIntNotSupportedYet()` correctly expects `I4` fallback
- the rest of the file is effectively an `I8`/`F8` test suite

The implementation itself is fine being narrow, but the advertised support
surface should match the actual gate.

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

Resolving the findings above would put the vector bytecode backend much closer
to replacing the interpreted backend, but it is not the only condition for
deletion.

Before `VectorFilterInterpreter` can be fully removed, the remaining roles it
still serves need to disappear:

1. Runtime fallback

   Today the interpreter still exists as a fallback for shapes not yet covered
   by the compiled backends. Full removal requires scalar bytecode and vector
   bytecode together to cover the production surface that currently falls back
   to interpretation.

2. Test oracle

   Some dedicated compiler tests still use the interpreter as the semantic
   oracle. Full removal requires those tests to compare against non-JIT truth
   or another stable backend reference instead of relying on the interpreter.

3. Default integration path

   The intended default backend selection must be settled first. In
   particular, `AUTO` needs to use the compiled backends in the final
   intended order, without depending on the interpreter path.

Practical deletion sequence:

1. Remove the interpreter from the default runtime path.
2. Remove the interpreter as a fallback path.
3. Remove the interpreter as a test oracle.
4. Delete the backend implementation.

So the findings in this review are necessary to clear, but they are not by
themselves sufficient for deleting the interpreted backend. They get the
vectorized compiled path much closer to production readiness; full interpreter
removal still depends on complete compiled-backend coverage and test-oracle
transition.
