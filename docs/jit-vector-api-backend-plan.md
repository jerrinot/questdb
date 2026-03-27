# Plan: Java Vector API JIT Backend for QuestDB

## Context

QuestDB's JIT filter compiler translates SQL WHERE predicates into native machine
code via a C++ backend (asmjit). It supports x86-64 scalar + AVX2 SIMD and
AArch64 scalar only. The proposal is to add a **pure-Java backend** that consumes
the same IR and uses the Java Vector API for SIMD, eliminating JNI overhead, the
C++ build dependency for the SIMD path, and bringing SIMD to AArch64 for free.

**Target: Java 24** (prototype). The Foreign Function & Memory API is final
(since Java 22), and the Vector API is the 9th incubator with a stable API
surface.

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
io.questdb.jit.VectorCompiledFilter          -- Replaces CompiledFilter for vector path
io.questdb.jit.VectorCompiledCountOnlyFilter -- Count-only variant
io.questdb.jit.VectorFilterInterpreter       -- Core SIMD interpreter
io.questdb.jit.IrDecoder                     -- Parses IR from native memory into Instruction[]
io.questdb.jit.JitFilter                     -- Interface: call() + close()
io.questdb.jit.JitCountOnlyFilter            -- Interface: call() + close()
```

### Modified Classes

```
io.questdb.jit.JitUtil                    -- Add isVectorApiAvailable()
io.questdb.cairo.SqlJitMode               -- Add JIT_MODE_FORCE_VECTOR = 3
io.questdb.griffin.SqlCodeGenerator        -- Backend selection logic
io.questdb.jit.CompiledFilter             -- Extract JitFilter interface
io.questdb.jit.CompiledCountOnlyFilter    -- Extract JitCountOnlyFilter interface
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

The change introduces a `JitFilter` interface and selects the backend:

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

1. **SIMD mode only for single-type-size filters** (same as native AVX2)
2. **No short-circuit in SIMD** (same as native AVX2; scalar fallback handles it)
3. **Variable-size columns (string/binary/varchar):** Phase 1 delegates to scalar
4. **i128:** Handled as paired longs (no native 128-bit Vector species)
5. **Requires `--add-modules jdk.incubator.vector`** until the API finalizes

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
- `JitFilter`/`JitCountOnlyFilter` interfaces
- `IrDecoder` parsing IR from native memory into Java `Instruction[]`
- `VectorCompiledFilter`/`VectorCompiledCountOnlyFilter` with scalar interpreter
- `SqlCodeGenerator` backend selection behind `cairo.sql.jit.mode=vector`
- Existing `CompiledFilterRegressionTest` passing with vector backend

### Phase 2: SIMD Vectorized Interpreter
- `VectorFilterInterpreter` with Vector API for i32, i64, f32, f64
- Species selection from compilation options
- Vectorized comparison, arithmetic, boolean ops
- NULL sentinel handling via VectorMask
- Count-only mode with `trueCount()`
- Scalar tail for remainder rows
- `MemorySegment.ofAddress().reinterpret()` for zero-copy native memory access

### Phase 3: Complete Type Support
- i128/UUID as paired long operations
- Variable-size column header reads (string, binary, varchar)
- Float epsilon comparisons
- Division-by-zero handling
- Short-circuit support in scalar fallback

### Phase 4: Performance Tuning
- JMH benchmarks against native backend
- C2 compilation verification (`-XX:+PrintCompilation`)
- Optional specialized templates for top patterns
- Full test suite validation

### Phase 5: AArch64 Validation
- Test on ARM64 (Graviton, Apple Silicon)
- Verify NEON intrinsic generation
- Benchmark vs current scalar-only AArch64 native backend

---

## Verification

1. `mvn -Dtest=CompiledFilterRegressionTest test` -- end-to-end correctness
   against Java interpreter baseline
2. `mvn -Dtest=CompiledFilterIRSerializerTest test` -- IR serialization
3. JMH micro-benchmarks comparing native vs vector backend throughput
4. Manual: run QuestDB with `cairo.sql.jit.mode=vector`, execute filter queries,
   verify correct results and no crashes
5. ARM64 testing on CI or cloud instance

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