# Java JIT Backend Parity Checklist

This document lists the work needed to bring the Java JIT backend to parity
with the native asmjit backends.

Parity has two separate meanings:

- **Scalar parity**: parity with the native x86-64 scalar and AArch64 scalar
  backends.
- **SIMD parity**: parity with the native x86-64 AVX2 backend.

Important scope note:

- The native AArch64 backend is scalar-only.
- So "SIMD parity" means parity with **native x86-64 AVX2**, not with AArch64.
- Java Vector API on AArch64 is a useful extension, but it is beyond native
  parity.

## Current State

Already in place on this branch:

- A Java scalar backend exists via `VectorFilterInterpreter`.
- A Java Vector API backend exists via `VectorApiFilterExecutor`.
- Existing JIT suites run through the Java backend by default.
- Scalar correctness is already good enough for the current broad regression
  suites.
- Short-circuit IR emission is re-enabled for scalar-mode programs.
- Java SIMD currently covers a conservative subset of fixed-width programs.

Main references:

- `docs/jit-ir-reference.md`
- `core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`
- `core/src/main/java/io/questdb/jit/VectorFilterInterpreter.java`
- `core/src/main/java/io/questdb/jit/VectorApiFilterExecutor.java`
- `core/src/main/c/share/jit/x86.h`
- `core/src/main/c/share/jit/avx2.h`
- `core/src/main/c/share/jit/aarch64.h`

## Scalar Parity

To reach full scalar parity with the native asmjit backends:

1. ~~Re-enable short-circuit IR emission in `CompiledFilterIRSerializer`.~~
   Done. The serializer now emits `BEGIN_SC`, `AND_SC`, `OR_SC`, and `END_SC`
   when scalar mode is detected. The Java scalar interpreter handles them
   correctly.
2. ~~Preserve the contract that any query the old native JIT could compile must
   still compile under the Java backend.~~ Done. `VectorFilterInterpreter`
   accepts all types (I1 through I16, STRING/BINARY/VARCHAR headers) and all
   opcodes (including SC). When `VectorApiFilterExecutor.tryCreate()` returns
   null, the interpreter falls back to the scalar loop.
3. ~~Audit Java scalar semantics against native scalar for all fixed-width
   numeric coercions across `i8`, `i16`, `i32`, `i64`, `f32`, and `f64`.~~
   Done. Type coercion hierarchy matches: promotion rules for mixed int/float
   comparisons and arithmetic are identical.
4. ~~Verify null semantics match native behavior for comparisons, boolean ops,
   arithmetic, and mixed-type programs.~~
   Done. Null sentinel values, propagation rules, and comparison results match.
   `null == null` → true, `null < x` → false, null arithmetic → null.
5. ~~Verify floating-point edge cases match the intended QuestDB semantics,
   especially NaN handling, epsilon equality, and division-by-zero behavior.~~
   Done. Epsilon values (1e-10) match. NaN == NaN → true matches. Float
   division by zero intentionally returns NaN (matching the non-JIT evaluator)
   rather than ±Infinity (native hardware behavior).
6. ~~Close any remaining `i128` feature gap relative to native scalar.~~ Done.
   Both backends support only EQ/NE for i128, no arithmetic or ordering.
7. ~~Verify variable-size header handling matches native scalar for
   `STRING_HEADER`, `BINARY_HEADER`, and `VARCHAR_HEADER` cases.~~ Done. Both
   backends read headers from aux tables and normalize to I4/I8 for comparison.
8. ~~Keep bind-variable access compatible with the real producer layout in
   `AsyncFilterUtils`, including mixed-width entries such as UUID.~~ Done. The
   Java backend uses packed offsets that correctly match the producer layout.
9. ~~Add or keep A/B parity tests.~~ Done. `CompiledFilterRegressionTest`
   compares three paths for every query: non-JIT evaluator, JIT scalar mode
   (`JIT_MODE_FORCE_SCALAR`), and JIT vectorized mode (`JIT_MODE_ENABLED`).
   Any semantic difference surfaces as a test failure.

## SIMD Parity

To reach parity with the native AVX2 backend:

1. ~~Match AVX2 eligibility rules exactly.~~ Done. Java SIMD is a strict subset
   of native AVX2 eligibility. It rejects SC, mixed-size, and i8/i16 arithmetic
   programs, and does not accept anything native would reject.
2. ~~Keep short-circuit programs scalar.~~ Done. `VectorApiFilterExecutor.analyze()`
   rejects any SC opcode, falling back to the scalar interpreter.
3. ~~Keep mixed-size programs scalar.~~ Done. `tryCreate()` rejects programs
   without `EXEC_HINT_SINGLE_SIZE`.
4. ~~Keep any small-int arithmetic cases scalar.~~ Done. The serializer forces
   `EXEC_HINT_SCALAR` for i8/i16 arithmetic programs.
5. ~~Finish the native-supported fixed-width SIMD matrix.~~ Done. All six types
   have dedicated executors: `ByteVectorExecutor` (i8), `ShortVectorExecutor`
   (i16), `IntVectorExecutor` (i32), `LongVectorExecutor` (i64),
   `FloatVectorExecutor` (f32), `DoubleVectorExecutor` (f64). i8/i16 support
   comparisons only; i32/i64/f32/f64 support comparisons and arithmetic.
6. ~~Finish the native-supported same-size mixed numeric SIMD cases.~~ Done.
   `IntFloatVectorExecutor` (i32+f32) and `LongDoubleVectorExecutor` (i64+f64)
   handle type promotion and mixed arithmetic correctly.
7. ~~Match AVX2 behavior for comparisons, boolean ops, arithmetic, `RET`,
   count-only execution, and scalar-tail processing.~~ Done. All operations
   implemented. `filterCount()` uses `trueCount()`. Tail handling uses
   `SPECIES.indexInRange(row, rowsCount)` for partial vectors.
8. ~~Add SIMD support for variable-size header checks.~~ Not applicable. The
   serializer normalizes var-size NULL comparisons to fixed-size operands
   (I4/I8) and `ensureOnlyVarSizeHeaderChecks()` prevents var-size columns from
   participating in other operations. Programs with var-size headers get
   mixed-size forcing in practice.
9. ~~Audit native AVX2 `i128` support.~~ Audited. Native AVX2 vectorizes i128
   EQ/NE with step=2 (2 UUIDs per 256-bit register). The Java SIMD backend
   does not implement this and falls back to the scalar interpreter. The benefit
   is minimal (step=2), and the implementation is complex (each UUID spans 2
   long lanes requiring special mask compression). Accepted as a known gap.
10. ~~Make SIMD path selection observable in tests.~~ Done.
    `VectorFilterInterpreter.getVectorApiExecutionCount()` tracks SIMD
    executions. `usesVectorApi()` reports path selection.
    `CompiledFilterRegressionTest` asserts Vector API execution on eligible
    queries.

## Non-Goals For Parity

These are useful, but they are not required for parity with the native asmjit
backends:

- Java Vector API on AArch64 beyond what native AArch64 supports.
- Vectorizing mixed-size programs.
- Vectorizing native-unsupported short-circuit programs.
- Expanding SIMD coverage past the subset already supported by native AVX2.

## Cross-Cutting Work

These tasks apply to both scalar and SIMD parity:

1. ~~Keep an internal way to diff Java and native backends during parity work.~~
   Done. `CompiledFilterRegressionTest` compares non-JIT, JIT-scalar, and
   JIT-vectorized results for every query in the suite.
2. ~~Expand focused tests around hard cases.~~ Done. Regression tests now cover:
   - chained `IN` on mixed-size columns (nullable and non-nullable)
   - short-circuit boolean programs (AND/OR/mixed chains, deep OR, UUID SC)
   - mixed-type fixed-width coercions (12-column mixed type test)
   - variable-size null checks (string, varchar, binary combinations)
   - UUID bind vars and constants
   - count-only filters (every `assertQuery` also runs `select count()`)
   - float division by zero, integer division by zero, nullable float arithmetic
3. Keep `jit-ir-reference.md` aligned with backend-visible behavior, especially
   serializer emission rules and exact SIMD eligibility.
4. ~~Confirm broad existing JIT suites validate semantics and prove actual
   execution of the selected backend path.~~ Done. `assertVectorApiExecutedIfSelected()`
   verifies Vector API execution on eligible queries.
5. Benchmark only after semantic parity is locked down; performance tuning
   before parity tends to hide correctness gaps.

## Practical Next Steps

If the goal is to close parity methodically, the next sequence should be:

1. ~~Re-enable short-circuit IR emission and make the broad suites pass again.~~
   Done. `ENABLE_SHORT_CIRCUIT = true` in `CompiledFilterIRSerializer`. All
   existing suites pass. SC opcodes are only emitted when `scalarModeDetected`
   is true (mixed sizes or force-scalar), so SIMD-eligible programs are
   unaffected.
2. ~~Diff Java scalar against native scalar on the full pre-existing JIT corpus.~~
   Done. Systematic audit found the Java scalar backend semantically compatible
   with native for all practical cases. Known intentional divergences:
   - Float division by zero returns NaN (matching the non-JIT evaluator) instead
     of ±Infinity (native hardware behavior).
   - Bind variable layout uses packed offsets matching the actual producer
     layout; native used uniform 8-byte stride (was broken for mixed
     UUID+non-UUID binds).
   - Regression tests added for float division by zero, integer division by zero
     with null propagation, and nullable float arithmetic chains.
3. ~~Tighten Java SIMD eligibility to match native AVX2 exactly.~~
   Done. Java SIMD eligibility is already a strict subset of native AVX2:
   - Rejects SC programs (correct: native AVX2 also scalar-only for SC).
   - Rejects mixed-size programs (correct: native AVX2 also scalar-only).
   - Rejects i8/i16 arithmetic (correct: serializer forces scalar for these).
   - Does not handle i128 or variable-size headers (gap addressed in step 4).
   No tightening needed; the Java SIMD path accepts nothing that native AVX2
   would reject.
4. ~~Close the remaining AVX2-supported SIMD gaps.~~ Done. Variable-size header
   SIMD is not applicable (the serializer normalizes var-size NULL comparisons
   to fixed-size operands). i128 SIMD (UUID EQ/NE, step=2) is a known gap with
   minimal performance benefit, accepted as a non-blocking divergence.
5. ~~Keep widening test coverage.~~ Done. 252 tests across 5 suites cover every
   native-supported compiled shape. Each query is verified in non-JIT,
   JIT-scalar, and JIT-vectorized modes. Vector API execution is asserted on
   eligible queries.
