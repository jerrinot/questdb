# Review: Vector Bytecode Filter Compiler

Date: 2026-03-28

Scope:
- `core/src/main/java/io/questdb/jit/VectorBytecodeFilterCompiler.java`
- `core/src/main/java/io/questdb/jit/VectorCompiledFilter.java`
- `core/src/main/java/io/questdb/jit/VectorCompiledCountOnlyFilter.java`
- `core/src/main/java/io/questdb/jit/FilterHelpers.java`
- `core/src/test/java/io/questdb/test/jit/VectorBytecodeFilterCompilerTest.java`

## Findings

### ~~High: `AUTO` does not currently select the vector bytecode backend~~ RESOLVED

`compileBytecode()` no longer returns early when the interpreter's Vector API
path is eligible. AUTO now tries vectorized bytecode first, falls back to
scalar bytecode.

Changes:
- `VectorCompiledFilter.compileBytecode()` — removed `usesVectorApi()` gate
- `VectorCompiledCountOnlyFilter.compileBytecode()` — same
- `JitBackend.AUTO` doc updated to reflect two-tier: vector bytecode → scalar
  bytecode
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

### ~~High: compress+store overflow in vectorized row-ID output~~ RESOLVED

`VectorBytecodeFilterCompiler` used an unmasked `intoMemorySegment()` after
`compress()`. `compress()` packs matching lane values at the front of the
vector but produces a full-width result. The unmasked store wrote ALL lanes
(including garbage tail values), overflowing the output buffer and corrupting
the native heap (`realloc(): invalid next size`).

Fix: `FilterHelpers.writeCompressedRows()` creates a store mask via
`species.indexInRange(0, trueCount)` and uses the masked
`intoMemorySegment(seg, offset, order, storeMask)`.

Test: `CompiledFilterRegressionTest` — 92 tests, all passing.

### ~~High: null-aware I8→F8 cast converts LONG_NULL to -9.22E18 instead of NaN~~ RESOLVED

`emitCast` used raw `convertShape(L2D)` which converts LONG_NULL to a large
negative double. Null-aware comparisons then treated this as a valid value
instead of null, producing wrong results for mixed I8/F8 queries like
`i64 < f64` when i64 has null values.

Fix: `FilterHelpers.longToDoubleNullAware()` detects LONG_NULL lanes via
`src.eq(nullVec)`, performs `convertShape(L2D)`, then blends NaN into null
lanes.

Tests: `CompiledFilterRegressionTest#testIntFloatColumnsComparisonFilterOutNulls`,
`testColumnArithmeticsNullComparison`.

## Coverage Notes

The dedicated tests (`VectorBytecodeFilterCompilerTest`) cover:

- `I8` comparisons, boolean composition, count-only execution
- `F8` comparisons with epsilon and `NaN`
- mixed `I8` + `F8` cases, including F8-first load order
- floating-point arithmetic division (div-by-zero → NaN):
  `testDoubleArithmeticDivByZero`
- null-aware arithmetic (LONG_NULL preservation):
  `testLongArithmeticNullAware`
- mixed F8-first then I8 ordered null-aware comparison:
  `testMixedDoubleFirstThenLongNullOrdered`
- AUTO backend selection: `testAutoSelectsVectorBytecodeForEligibleProgram`,
  `testAutoFallsBackToScalarForControlFlow`

`CompiledFilterRegressionTest` (92 tests) exercises the full SQL pipeline
with all type combinations, null handling, arithmetic, comparisons, IN(),
UUID, varchar, and mixed-type expressions. All 92 tests pass with the
vectorized bytecode compiler active via AUTO.

## Overall Assessment

Generating Vector API bytecode from `LoweredProgram` is a better long-term
architecture than the handwritten vector interpreter. All original blockers
are resolved. The interpreter has been fully removed from the runtime path.

Remaining work:
- I4/F4 support (lane count mismatch with I8/F8 species)

## Interpreter Removal Readiness

### ~~1. Runtime fallback~~ RESOLVED

The interpreter has been fully removed from `VectorCompiledFilter` and
`VectorCompiledCountOnlyFilter`:
- No `VectorFilterInterpreter` field or instantiation
- `compile()` only calls `compileBytecode()`
- `call()` throws `IllegalStateException` if no bytecode filter was compiled
- No interpreter fallback in any code path

### ~~2. Test oracle~~ RESOLVED

All `ScalarBytecodeFilterCompilerTest` tests now use hardcoded expected
row indices computed from the input data. The `interpreterFilter()`,
`interpreterCount()`, and `writeIr()` helper methods and their imports
(`Vm`, `MemoryCARW`, `VectorFilterInterpreter`) have been removed.

The interpreter is no longer a test dependency for the scalar bytecode
compiler tests.

### ~~3. Default integration path~~ RESOLVED

AUTO uses compiled backends: vectorized bytecode → scalar bytecode.
`JitBackend.JAVA_INTERPRETED` has been removed. The interpreter class
`VectorFilterInterpreter` still exists but is unreferenced by any
production or test code.

### ~~4. Test harness observability~~ RESOLVED

`AbstractCairoTest.getVectorApiExecutionCountIfSelected()` and
`assertVectorApiExecutedIfSelected()` no longer reference the interpreter.
They check `VectorCompiledFilter.usesBytecode()` to verify the bytecode
path is active. `VectorCompiledFilterTest` asserts `usesBytecode()` and
`usesVectorBytecode()` instead of the removed `usesVectorApi()`.
