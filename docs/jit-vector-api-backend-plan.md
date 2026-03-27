# Plan: Java Vector API JIT Backend for QuestDB

## Context

QuestDB's JIT filter compiler translates SQL WHERE predicates into native machine
code via a C++ backend (asmjit). It supports x86-64 scalar + AVX2 SIMD and
AArch64 scalar only. The proposal is to add a **pure-Java backend** that consumes
the same IR and uses the Java Vector API for SIMD, eliminating JNI overhead, the
C++ build dependency for the SIMD path, and bringing SIMD to AArch64 for free.

**Vector API target: Java 24+** (future phase). The current Phase 1 backend is
implemented in pure Java and remains compatible with the project's current Java
17 source/target level. A Java 24+ toolchain is only required once the actual
Vector API path is introduced.

## Current Status

### Done On This Branch

- Backend-neutral JIT interfaces are in place (`JitFilter`,
  `JitCountOnlyFilter`) and the existing native wrappers implement them.
- `cairo.sql.jit.mode=vector` is parsed, logged, and exposed via
  `SqlJitMode.JIT_MODE_FORCE_VECTOR`.
- `IrDecoder` is implemented and tested against the real serialized IR format.
- A pure-Java Phase 1 backend is implemented via `VectorCompiledFilter`,
  `VectorCompiledCountOnlyFilter`, and `VectorFilterInterpreter`.
- `SqlCodeGenerator` now selects the Java backend explicitly when
  `cairo.sql.jit.mode=vector`.
- Focused backend tests and end-to-end SQL integration tests are in place.

### Still Pending

- Actual Vector API SIMD execution.
- Variable-size column support (`STRING`, `BINARY`, `VARCHAR`) in the Java backend.
- Broader semantic parity work beyond the initial fixed-width subset.
- Performance work, benchmarking, and ARM64 validation.

## Feasibility Assessment

### Verdict: Feasible

The approach is architecturally sound and Java 24 removes the main friction
points:

1. **Memory access** -- `MemorySegment.ofAddress(addr).reinterpret(size)` works
   directly with QuestDB's raw `long` addresses. No copying needed. The Foreign
   Memory API is final in Java 24.
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
-- same applies here. The serializer already sets exec_hint=scalar when
short-circuits are present.

---

## Architecture

### New Classes

```
io.questdb.jit.VectorCompiledFilter          -- Done: Java backend wrapper for row-id filtering
io.questdb.jit.VectorCompiledCountOnlyFilter -- Done: Java backend wrapper for count-only filtering
io.questdb.jit.VectorFilterInterpreter       -- Done for Phase 1 as a scalar interpreter; SIMD work remains
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

Done on this branch: `SqlCodeGenerator` now introduces a backend choice and
selects the Java backend when `JIT_MODE_FORCE_VECTOR` is requested:

```java
if (useJit && canCompile) {
    if (useVectorBackend) {
        jitFilter = new VectorCompiledFilter();
        jitFilter.compile(jitIRMem, jitOptions);
    } else {
        jitFilter = new CompiledFilter();  // native path
        jitFilter.compile(jitIRMem, jitOptions);
    }
}
```

`AsyncJitFilteredRecordCursorFactory` and `AsyncJitFilterAtom` hold `JitFilter`
instead of `CompiledFilter`. The `call()` signature is identical.

### Memory Access Strategy

This section describes the planned Phase 2 Vector API path. The current Phase 1
implementation uses the existing `Unsafe`-based memory access primitives and
does not require `MemorySegment`.

Java 24 has the Foreign Memory API finalized. Direct zero-copy access:

```java
MemorySegment seg = MemorySegment.ofAddress(colAddr).reinterpret(size);
LongVector vec = LongVector.fromMemorySegment(SPECIES, seg, offset, NATIVE_ORDER);
```

For output (writing row IDs to `filteredRows`):
```java
MemorySegment out = MemorySegment.ofAddress(filteredRowsAddr).reinterpret(rowCount * 8);
out.set(ValueLayout.JAVA_LONG, outputIdx * 8, rowId);
```

No Unsafe needed for the Vector API path. The existing Unsafe-based code remains
for the native JIT path.

### Interpreter Core Design

Current implementation note: the existing `VectorFilterInterpreter` executes the
decoded IR row-by-row as a scalar interpreter. The pseudocode below describes
the intended Phase 2 vectorized execution model on top of the same decoded IR.

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
| i128 equality | Load as two `LongVector`, compare hi and lo, AND masks |
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

The backend determines `step` from the compilation options (bits 1-3 encode max
type size) and uses the corresponding species, matching the C++ backend strategy.

### Configuration

New config value for the existing property:
```
cairo.sql.jit.mode = on | scalar | off | vector
```
Where `vector` forces the Vector API backend. `on` (default) prefers native when
available, falls back to vector, then to Java scalar.

---

## Limitations

1. **Current backend is scalar only.** The Vector API SIMD path is still pending.
2. **Phase 1 supports fixed-width scalar types first.** Variable-size columns
   (`STRING`, `BINARY`, `VARCHAR`) are not yet supported by the Java backend.
3. **Short-circuit currently works in the scalar Java backend.** SIMD short-circuit
   remains unsupported, same as native AVX2.
4. **i128 is limited to equality/inequality** and is handled as paired longs.
5. **`jdk.incubator.vector` is not needed yet.** It becomes a build/runtime
   requirement only once the actual Vector API path lands.

---

## Risks

| Risk | Mitigation |
|------|------------|
| Vector API not intrinsified on a JVM | Keep native backend as primary; vector is opt-in |
| C2 fails to scalar-replace VectorValue | Profile; redesign stack to use primitive arrays if needed |
| API changes before finalization | API stable since JDK 17; wrap in thin adapter layer |

---

## Phased Roadmap

### Phase 1: Scalar Interpreter + Wiring
- [x] `JitFilter`/`JitCountOnlyFilter` interfaces
- [x] `IrDecoder` parsing IR from native memory into Java `Instruction[]`
- [x] `VectorCompiledFilter`/`VectorCompiledCountOnlyFilter` with scalar interpreter
- [x] `SqlCodeGenerator` backend selection behind `cairo.sql.jit.mode=vector`
- [x] Focused backend tests for the Java interpreter
- [x] End-to-end SQL integration test for `JIT_MODE_FORCE_VECTOR`
- [x] Existing `CompiledFilterRegressionTest` still passing on the native path

### Phase 2: SIMD Vectorized Interpreter
- [ ] `VectorFilterInterpreter` with Vector API for i32, i64, f32, f64
- [ ] Species selection from compilation options
- [ ] Vectorized comparison, arithmetic, boolean ops
- [ ] NULL sentinel handling via `VectorMask`
- [ ] Count-only mode with `trueCount()`
- [ ] Scalar tail for remainder rows
- [ ] `MemorySegment.ofAddress().reinterpret()` for zero-copy native memory access

### Phase 3: Complete Type Support
- [x] i128/UUID as paired long operations for equality/inequality
- [x] Float epsilon comparisons
- [x] Division-by-zero handling for integer arithmetic
- [x] Short-circuit support in the scalar Java backend
- [ ] Variable-size column header reads (string, binary, varchar)
- [ ] Broader type/semantic parity beyond the initial fixed-width subset

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
2. [x] `mvn -pl core -DforkCount=0 -DreuseForks=false -Dtest=VectorCompiledFilterTest,VectorCompiledFilterIntegrationTest test`
3. [x] `mvn -pl core -DforkCount=0 -DreuseForks=false -Dtest=CompiledFilterIRSerializerTest,CompiledFilterRegressionTest,PropServerConfigurationTest#testSqlJitMode test`

Still pending:

4. [ ] JMH micro-benchmarks comparing native vs vector backend throughput
5. [ ] Manual QuestDB runs with `cairo.sql.jit.mode=vector` outside the test harness
6. [ ] ARM64 testing on CI or cloud instance

---

## Key Files Reference

| File | Role |
|------|------|
| `docs/jit-ir-reference.md` | Complete IR specification |
| `core/.../jit/CompiledFilter.java` | Current native filter wrapper |
| `core/.../jit/CompiledCountOnlyFilter.java` | Current native count-only wrapper |
| `core/.../jit/CompiledFilterIRSerializer.java` | IR serializer (unchanged) |
| `core/.../jit/FiltersCompiler.java` | JNI bridge (unchanged for native path) |
| `core/.../jit/JitUtil.java` | Architecture/capability detection |
| `core/.../cairo/SqlJitMode.java` | JIT mode constants |
| `core/.../griffin/SqlCodeGenerator.java` | Backend selection (~line 3232) |
| `core/.../engine/table/AsyncJitFilteredRecordCursorFactory.java` | Execution orchestration |
| `core/src/test/.../griffin/CompiledFilterRegressionTest.java` | E2E tests |
