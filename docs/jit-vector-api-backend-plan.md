# Plan: Java Vector API JIT Backend for QuestDB

## Context

QuestDB's JIT filter compiler translates SQL WHERE predicates into native machine
code via a C++ backend (asmjit). It supports x86-64 scalar + AVX2 SIMD and
AArch64 scalar only. This branch adds a **pure-Java backend** that consumes the
same IR and uses the Java Vector API for SIMD, eliminating JNI overhead, the
C++ build dependency for the SIMD path, and bringing SIMD to AArch64 for free.

**Current branch target: Java 22 + `jdk.incubator.vector`.** The branch now
builds `core` with source/target 22, adds `requires jdk.incubator.vector` to
`module-info.java`, and uses the Foreign Memory API plus the incubating Vector
API in the main sources.

## Current Status

### Done On This Branch

- Backend-neutral JIT interfaces are in place (`JitFilter`,
  `JitCountOnlyFilter`) and the existing native wrappers implement them.
- `cairo.sql.jit.mode=vector` is parsed, logged, and exposed via
  `SqlJitMode.JIT_MODE_FORCE_VECTOR`.
- `IrDecoder` is implemented and tested against the real serialized IR format.
- A pure-Java Phase 1 backend is implemented via `VectorCompiledFilter`,
  `VectorCompiledCountOnlyFilter`, and `VectorFilterInterpreter`.
- A real Vector API path is implemented via `VectorApiFilterExecutor` for
  fixed-width `i32`, `i64`, `f32`, and `f64` programs.
- The build is bumped to Java 22 and enables `jdk.incubator.vector` in both the
  compiler configuration and `module-info.java`.
- `SqlCodeGenerator` temporarily uses the Java backend for ALL JIT modes
  (the native C++ backend is bypassed). This is the "temporary replacement"
  approach — no conditional dispatch, just `new VectorCompiledFilter()` in
  place of `new CompiledFilter()`.
- The temporary replacement is intentional on this branch: existing JIT suites
  keep exercising the Java backend(s) by default rather than the native path.
- Full scalar semantic parity: all 115 `CompiledFilterTest` +
  `CompiledFilterRegressionTest` cases pass on the Java backend.
- Variable-size column NULL checks (`STRING`, `BINARY`, `VARCHAR`) are
  supported via header reads from aux/data pages.
- Null comparison semantics match QuestDB's `Numbers.lessThan()`: strict
  operators (`<`, `>`) return false on NULL, non-strict (`<=`, `>=`) return
  true when both NULL.
- Float division by zero produces NaN (matching non-JIT evaluator, diverging
  from the C++ backend which produces `±Infinity`).
- Short-circuit label scoping handles chained `IN()` lists correctly.
- Focused backend tests and end-to-end SQL integration tests are in place.
- The broad pre-existing JIT suites now execute the compiled cursor rather than
  only checking that JIT was selected, and they assert real Vector API
  execution whenever a query is compiled onto the Vector API path.

### Still Pending

- Widening Vector API eligibility beyond the current conservative subset
  (single-size, single-exact-type fixed-width programs only).
- SIMD support for additional cases such as mixed-type programs and possibly
  `i8` / `i16` if that turns out to be worthwhile.
- Performance work, benchmarking, and ARM64 validation.

## Feasibility Assessment

### Verdict: Feasible

The approach is architecturally sound and Java 22 already removes the main
friction points needed for this branch:

1. **Memory access** -- `MemorySegment.ofAddress(addr).reinterpret(size)` works
   directly with QuestDB's raw `long` addresses. No copying needed. The Foreign
   Memory API is final by Java 22.
2. **Warm-up latency** -- the native backend is immediately fast; the Vector API
   code needs C2 compilation (~10K invocations). QuestDB processes many page
   frames per query, so the interpreter method should reach C2 quickly.
3. **i128 (UUID/LONG128)** -- no 128-bit integer Vector species exists. Must be
   handled as paired long operations.
4. **Vector API stability** -- 9th incubator in Java 24, targeted for
   finalization in Java 25. API surface has been stable since Java 17. Requires
   `--add-modules jdk.incubator.vector`.

### Expected Performance

| Scenario | vs Native AVX2 | vs Native AArch64 scalar |
|----------|---------------|--------------------------|
| Warm (C2-compiled) | 70-90% | 200-400% (SIMD on ARM!) |
| Cold (C2 not yet compiled) | 5-20% | 50-100% |

The big win is **AArch64**: the native backend is scalar-only on ARM, while the
Vector API automatically uses NEON.

## Recommended Approach: Vectorized IR Interpreter

An interpreter that walks the IR instruction array once per SIMD-width chunk of
rows, mapping each opcode to a Vector API operation. This is chosen over:

- **Bytecode generation:** Enormous complexity for marginal gain. The IR has only
  21 opcodes; switch-dispatch cost is negligible compared to SIMD work per
  instruction.
- **Template dispatch:** Good as a later optimization for the top 5 filter
  patterns, but the interpreter is the correct foundation.

The C++ AVX2 backend already does NOT support short-circuit opcodes in SIMD mode
-- same applies here. On this branch, short-circuit emission is currently
hard-disabled in the serializer, so most boolean chains are emitted as regular
`AND` / `OR` programs instead.

---

## Architecture

### New Classes

```
io.questdb.jit.VectorCompiledFilter          -- Done: Java backend wrapper for row-id filtering
io.questdb.jit.VectorCompiledCountOnlyFilter -- Done: Java backend wrapper for count-only filtering
io.questdb.jit.VectorFilterInterpreter       -- Done: scalar interpreter + Vector API dispatch
io.questdb.jit.VectorApiFilterExecutor       -- Done: Vector API executor for eligible fixed-width programs
io.questdb.jit.IrDecoder                     -- Done: parses IR from native memory into Instruction[]
io.questdb.jit.JitFilter                     -- Done: backend-neutral filter interface
io.questdb.jit.JitCountOnlyFilter            -- Done: backend-neutral count-only filter interface
```

### Modified Classes

```
io.questdb.jit.JitUtil                    -- Done: add isVectorApiAvailable()
io.questdb.cairo.SqlJitMode               -- Done: add JIT_MODE_FORCE_VECTOR = 3
io.questdb.griffin.SqlCodeGenerator       -- Done: backend selection logic
io.questdb.jit.CompiledFilter             -- Done: implements JitFilter
io.questdb.jit.CompiledCountOnlyFilter    -- Done: implements JitCountOnlyFilter
```

### Integration Point

`SqlCodeGenerator.java` (~line 3232) currently does:

```java
if (useJit && canCompile) {
    compiledFilter = new CompiledFilter();
    compiledFilter.compile(jitIRMem, jitOptions);
    ...
}
```

Current state on this branch: `SqlCodeGenerator` temporarily uses the Java
backend unconditionally (the native backend is bypassed):

```java
if (useJit && canCompile) {
    compiledFilter = new VectorCompiledFilter();         // was: new CompiledFilter()
    compiledFilter.compile(jitIRMem, jitOptions);

    compiledCountOnlyFilter = new VectorCompiledCountOnlyFilter();
    compiledCountOnlyFilter.compile(jitIRMem, jitOptions);
}
```

The `JitUtil.isJitSupported()` check was also removed from the `canCompile`
guard so that the Java backend works on any architecture.

`AsyncJitFilteredRecordCursorFactory` and `AsyncJitFilterAtom` hold `JitFilter`
instead of `CompiledFilter`. The `call()` signature is identical.

Current branch policy is to keep this temporary replacement in place so the
pre-existing JIT suites continue exercising the Java backend(s). Reintroducing
native-vs-Java selection can be revisited later if needed, but it is no longer
the immediate goal of this branch.

### Memory Access Strategy

The scalar fallback still uses the existing `Unsafe`-based access path. The
Vector API path uses the Foreign Memory API directly for zero-copy access:

```java
MemorySegment seg = MemorySegment.ofAddress(colAddr).reinterpret(size);
LongVector vec = LongVector.fromMemorySegment(SPECIES, seg, offset, NATIVE_ORDER);
```

For output (writing row IDs to `filteredRows`):
```java
MemorySegment out = MemorySegment.ofAddress(filteredRowsAddr).reinterpret(rowCount * 8);
out.set(ValueLayout.JAVA_LONG, outputIdx * 8, rowId);
```

No `Unsafe` is needed inside the Vector API executor. The existing
`Unsafe`-based code remains for scalar fallback and for the native JIT path.

### Interpreter Core Design

Current implementation note: `VectorFilterInterpreter.compile()` now decodes the
IR once, computes bind-variable byte offsets, and then either:

- keeps the scalar row-by-row interpreter as fallback, or
- installs a `VectorApiFilterExecutor` if the IR is eligible for the current
  Vector API subset.

The pseudocode below describes the current vectorized execution model at a high
level.

The interpreter processes the decoded `Instruction[]` once per SIMD chunk:

```
for (row = 0; row < rowCount; row += step) {
    for (instr : program) {
        switch (instr.opcode) {
            case MEM  -> stack.push(loadColumn(colAddr, row, type))
            case IMM  -> stack.push(broadcast(value, type))
            case VAR  -> stack.push(broadcastBindVar(varsAddr, idx, type))
            case EQ   -> { rhs=pop(); lhs=pop(); push(lhs.eq(rhs)); }
            case LT   -> { rhs=pop(); lhs=pop(); push(lhs.lt(rhs)); }
            case ADD  -> { rhs=pop(); lhs=pop(); push(lhs.add(rhs)); }
            case AND  -> { rhs=pop(); lhs=pop(); push(lhs.and(rhs)); }
            case OR   -> { rhs=pop(); lhs=pop(); push(lhs.or(rhs)); }
            case NOT  -> { v=pop(); push(v.not()); }
            case NEG  -> { v=pop(); push(v.neg()); }
            case RET  -> break
            ...
        }
    }
    mask = stack.pop();
    scatterMatchingRowIds(mask, row, output);
}
scalarTail(remaining rows);
```

Stack entries are tagged with type (`VectorValue` carrying one of
`IntVector`/`LongVector`/`FloatVector`/`DoubleVector`/`VectorMask`). Binary ops
perform implicit widening per the type conversion matrix in the IR spec (Section
5.1 of `jit-ir-reference.md`).

### Key Semantic Details

| Feature | Implementation |
|---------|---------------|
| Float epsilon EQ/NE | `a.sub(b).abs().lt(EPSILON_BROADCAST)` |
| NULL sentinel detection | `vec.eq(NULL_BROADCAST)` producing mask |
| NULL-aware arithmetic | Detect null mask, perform op, blend with `mask.blend(result, nullVal)` |
| Integer div-by-zero | `divisor.eq(ZERO).not()` mask, conditional divide, blend null sentinel |
| i128 equality | Currently handled by scalar fallback (no active Vector API path yet) |
| Count-only mode | Accumulate `mask.trueCount()` instead of scattering row IDs |
| Scalar tail | Process remaining `rowCount % step` rows with scalar reads |

### SIMD Width Strategy

The Java Vector API automatically selects the preferred species for the platform:
- `LongVector.SPECIES_PREFERRED` returns 256-bit on AVX2, 512-bit on AVX-512,
  128-bit on NEON.

The filter's column types determine the effective width:
- `i64`/`f64`: 4 rows per 256-bit vector (AVX2), 2 rows per 128-bit (NEON)
- `i32`/`f32`: 8 rows per 256-bit vector
- `i16`: 16 rows per 256-bit vector
- `i8`: 32 rows per 256-bit vector

The current Java Vector API executor only uses the `i32` / `i64` / `f32` /
`f64` cases above. `i8` / `i16` remain scalar for now.

### Configuration

New config value for the existing property:
```
cairo.sql.jit.mode = on | scalar | off | vector
```
Where `vector` forces the Java backend and currently routes eligible programs to
the Vector API path. On this branch, `on` and `scalar` also route through the
Java backend because the native path is intentionally bypassed to maximize test
coverage of the new backend(s).

---

## Limitations

1. **Vector API eligibility is still conservative.** The current executor only
   accepts fixed-width `i32`, `i64`, `f32`, and `f64` programs where every data
   push in the IR has the same exact type and the final program shape is fully
   vectorizable.
2. **Variable-size columns support NULL checks only.** `STRING`, `BINARY`, and
   `VARCHAR` header reads work for `= null` / `<> null`, but the backend does
   not extract lengths or read payload data (same restriction as the native
   backend — the serializer enforces this via `ensureOnlyVarSizeHeaderChecks()`).
3. **Short-circuit works only in scalar fallback.** SIMD short-circuit remains
   unsupported, same as native AVX2. Also, the serializer currently has
   short-circuit emission hard-disabled to keep more queries on the regular
   boolean path.
4. **i128 is limited to equality/inequality** and is currently scalar-only,
   handled as paired longs.
5. **`jdk.incubator.vector` is now required** for building and running this
   branch's Java Vector API path.
6. **Float division by zero diverges from C++ backend.** The Java backend
   returns NaN (matching QuestDB's non-JIT evaluator), while the C++ backend
   returns `±Infinity` (IEEE 754). See `jit-ir-reference.md` Section 4.10.

---

## Risks

| Risk | Mitigation |
|------|------------|
| Vector API not intrinsified on a JVM | Keep scalar fallback inside the Java backend and measure before widening eligibility |
| C2 fails to scalar-replace VectorValue | Profile; redesign stack to use primitive arrays if needed |
| API changes before finalization | API stable since JDK 17; wrap in thin adapter layer |

---

## Phased Roadmap

### Phase 1: Scalar Interpreter + Wiring
- [x] `JitFilter`/`JitCountOnlyFilter` interfaces
- [x] `IrDecoder` parsing IR from native memory into Java `Instruction[]`
- [x] `VectorCompiledFilter`/`VectorCompiledCountOnlyFilter` with scalar interpreter
- [x] `SqlCodeGenerator` wiring to the Java backend (currently a temporary full replacement for all enabled JIT modes)
- [x] Focused backend tests for the Java interpreter
- [x] End-to-end SQL integration test for `JIT_MODE_FORCE_VECTOR`
- [x] All 115 `CompiledFilterTest` + `CompiledFilterRegressionTest` passing on the Java backend

### Phase 2: SIMD Vectorized Interpreter
- [x] `VectorFilterInterpreter` with Vector API for i32, i64, f32, f64
- [x] Species selection from compilation options
- [x] Vectorized comparison, arithmetic, boolean ops
- [x] NULL sentinel handling via `VectorMask`
- [x] Count-only mode with `trueCount()`
- [x] Scalar tail for remainder rows
- [x] `MemorySegment.ofAddress().reinterpret()` for zero-copy native memory access
- [ ] Widen Vector API eligibility beyond current single-type fixed-width programs
- [ ] Decide whether to add SIMD support for `i8` / `i16`
- [ ] Support mixed-type vectorized programs rather than scalar fallback

### Phase 3: Complete Type Support
- [x] i128/UUID as paired long operations for equality/inequality
- [x] Float epsilon comparisons
- [x] Division-by-zero handling for integer and float arithmetic
- [x] Short-circuit support in the scalar Java backend
- [x] Variable-size column header reads (string, binary, varchar) for NULL checks
- [x] Null comparison semantics matching `Numbers.lessThan()` (strict vs non-strict)
- [x] Float/double NaN comparison matching non-JIT IS NULL semantics
- [x] Null-aware type coercion (INT_NULL→LONG_NULL, INT_NULL/LONG_NULL→NaN)
- [x] Short-circuit label scoping for chained IN() expressions

### Phase 4: Performance Tuning
- [ ] JMH benchmarks against native backend
- [ ] C2 compilation verification (`-XX:+PrintCompilation`)
- [ ] Optional specialized templates for top patterns
- [ ] Full test suite validation

### Phase 5: AArch64 Validation
- [ ] Test on ARM64 (Graviton, Apple Silicon)
- [ ] Verify NEON intrinsic generation
- [ ] Benchmark vs current scalar-only AArch64 native backend

---

## Verification

Completed on this branch:

1. [x] `mvn -pl core -DskipTests test-compile`
2. [x] `mvn -pl core -DforkCount=0 -DreuseForks=false -Dtest=VectorCompiledFilterTest,VectorCompiledFilterIntegrationTest test` (7 tests)
3. [x] `mvn -pl core -DforkCount=0 -DreuseForks=false -Dtest=CompiledFilterTest,CompiledFilterRegressionTest test` (115 tests, all on Java backend)

Still pending:

4. [ ] `CompiledFilterIRSerializerTest` (IR serialization, independent of backend)
5. [ ] JMH micro-benchmarks comparing native vs vector backend throughput
6. [ ] Manual QuestDB runs with `cairo.sql.jit.mode=vector` outside the test harness
7. [ ] ARM64 testing on CI or cloud instance

---

## Key Files Reference

| File | Role |
|------|------|
| `docs/jit-ir-reference.md` | Complete IR specification |
| `core/.../jit/VectorCompiledFilter.java` | Java backend filter wrapper (row-ID mode) |
| `core/.../jit/VectorCompiledCountOnlyFilter.java` | Java backend filter wrapper (count-only mode) |
| `core/.../jit/VectorFilterInterpreter.java` | Java backend driver: scalar fallback + Vector API dispatch |
| `core/.../jit/VectorApiFilterExecutor.java` | Java Vector API executor |
| `core/.../jit/IrDecoder.java` | IR instruction decoder |
| `core/.../jit/JitFilter.java` | Backend-neutral filter interface |
| `core/.../jit/JitCountOnlyFilter.java` | Backend-neutral count-only interface |
| `core/.../jit/CompiledFilter.java` | Native C++ filter wrapper |
| `core/.../jit/CompiledCountOnlyFilter.java` | Native C++ count-only wrapper |
| `core/.../jit/CompiledFilterIRSerializer.java` | IR serializer; short-circuit emission currently hard-disabled on this branch |
| `core/.../jit/FiltersCompiler.java` | JNI bridge (unchanged for native path) |
| `core/.../jit/JitUtil.java` | Architecture/capability detection, including Vector API availability |
| `core/.../cairo/SqlJitMode.java` | JIT mode constants |
| `core/.../griffin/SqlCodeGenerator.java` | Backend selection (~line 3232) |
| `core/.../engine/table/AsyncJitFilteredRecordCursorFactory.java` | Execution orchestration |
| `core/src/test/.../jit/VectorCompiledFilterTest.java` | Java backend unit tests |
| `core/src/test/.../griffin/VectorCompiledFilterIntegrationTest.java` | Java backend integration tests |
| `core/src/test/.../griffin/CompiledFilterRegressionTest.java` | E2E regression tests |
| `core/src/test/.../griffin/CompiledFilterTest.java` | E2E filter tests |
