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
2. Preserve the contract that any query the old native JIT could compile must
   still compile under the Java backend. If Java SIMD is ineligible, the Java
   backend must fall back to scalar instead of rejecting the query.
3. Audit Java scalar semantics against native scalar for all fixed-width numeric
   coercions across `i8`, `i16`, `i32`, `i64`, `f32`, and `f64`.
4. Verify null semantics match native behavior for comparisons, boolean ops,
   arithmetic, and mixed-type programs.
5. Verify floating-point edge cases match the intended QuestDB semantics,
   especially NaN handling, epsilon equality, and division-by-zero behavior.
6. Close any remaining `i128` feature gap relative to native scalar. If native
   only supports a limited subset, Java should match that exact subset.
7. Verify variable-size header handling matches native scalar for
   `STRING_HEADER`, `BINARY_HEADER`, and `VARCHAR_HEADER` cases.
8. Keep bind-variable access compatible with the real producer layout in
   `AsyncFilterUtils`, including mixed-width entries such as UUID.
9. Add or keep A/B parity tests that compare Java scalar results to native
   scalar results while both implementations are still available for diffing.

## SIMD Parity

To reach parity with the native AVX2 backend:

1. Match AVX2 eligibility rules exactly. Java SIMD should only run programs the
   native AVX2 backend would also vectorize.
2. Keep short-circuit programs scalar. Native AVX2 does not support
   short-circuit execution in SIMD mode.
3. Keep mixed-size programs scalar. Native AVX2 does not vectorize mixed-size
   IR programs.
4. Keep any small-int arithmetic cases scalar if native AVX2 also forces them
   scalar for semantic reasons.
5. Finish the native-supported fixed-width SIMD matrix:
   - `i8`
   - `i16`
   - `i32`
   - `i64`
   - `f32`
   - `f64`
6. Finish the native-supported same-size mixed numeric SIMD cases, especially:
   - `i32 <-> f32`
   - `i64 <-> f64`
7. Match AVX2 behavior for comparisons, boolean ops, arithmetic, `RET`,
   count-only execution, and scalar-tail processing.
8. Add SIMD support for variable-size header checks if the native AVX2 backend
   already supports that subset.
9. Audit native AVX2 `i128` support and implement only the subset it actually
   vectorizes in reachable programs.
10. Make SIMD path selection observable in tests so the existing JIT corpus
    proves that vector-eligible queries actually execute the Vector API path.

## Non-Goals For Parity

These are useful, but they are not required for parity with the native asmjit
backends:

- Java Vector API on AArch64 beyond what native AArch64 supports.
- Vectorizing mixed-size programs.
- Vectorizing native-unsupported short-circuit programs.
- Expanding SIMD coverage past the subset already supported by native AVX2.

## Cross-Cutting Work

These tasks apply to both scalar and SIMD parity:

1. Keep an internal way to diff Java and native backends during parity work,
   even if normal test execution keeps routing through Java.
2. Expand focused tests around hard cases:
   - chained `IN`
   - short-circuit boolean programs
   - mixed-type fixed-width coercions
   - variable-size null checks
   - UUID bind vars
   - count-only filters
   - async execution paths
3. Keep `jit-ir-reference.md` aligned with backend-visible behavior, especially
   serializer emission rules and exact SIMD eligibility.
4. Confirm broad existing JIT suites do two things:
   - validate semantics
   - prove actual execution of the selected backend path
5. Benchmark only after semantic parity is locked down; performance tuning
   before parity tends to hide correctness gaps.

## Practical Next Steps

If the goal is to close parity methodically, the next sequence should be:

1. ~~Re-enable short-circuit IR emission and make the broad suites pass again.~~
   Done. `ENABLE_SHORT_CIRCUIT = true` in `CompiledFilterIRSerializer`. All
   existing suites pass. SC opcodes are only emitted when `scalarModeDetected`
   is true (mixed sizes or force-scalar), so SIMD-eligible programs are
   unaffected.
2. Diff Java scalar against native scalar on the full pre-existing JIT corpus.
3. Tighten Java SIMD eligibility to match native AVX2 exactly.
4. Close the remaining AVX2-supported SIMD gaps, with variable-size header
   vectorization as the main likely missing area.
5. Keep widening test coverage until every native-supported compiled shape is
   either:
   - executed by Java SIMD, or
   - executed by Java scalar fallback with matching results.
