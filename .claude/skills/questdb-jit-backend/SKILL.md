---
name: questdb-jit-backend
description: >
  Build a JIT filter backend for QuestDB's IR. Use when implementing a new
  code-generation or interpreter backend for QuestDB's compiled WHERE-clause
  filters, targeting any ISA or language. Covers IR format, opcodes, type
  system, NULL semantics, type conversions, bind variables, short-circuit
  evaluation, float epsilon comparison, and loop structure.
allowed-tools: Read, Grep, Glob, Bash, Edit, Write, Agent
---

# QuestDB JIT Backend Implementation Guide

## Quick orientation

QuestDB compiles SQL WHERE-clause predicates into a compact, stack-based
(RPN) IR. A backend consumes this IR and either interprets it or generates
native code to filter rows. This skill contains everything needed to build
a correct backend for **any ISA or language**.

Read `references/ir-spec.md` for the full IR specification — instruction
format, opcodes, type system, NULL handling, type conversions, short-circuit
evaluation, float comparison semantics, bind-variable layout, and loop
structure. That file is the authoritative reference; the sections below
summarize the critical rules and pitfalls.

Read the canonical source document at `docs/jit-ir-reference.md` in the
QuestDB repo for the most up-to-date specification with source file cross-
references.

## Critical rules a backend MUST implement

### 1. Instruction format

Every instruction is exactly 24 bytes: 4 (opcode) + 4 (options) + 8 (lo) +
8 (hi). Stop on `Ret` (0) or `Inv` (-1).

### 2. Stack-based evaluation (RPN)

- `IMM`, `MEM`, `VAR` push values.
- Unary ops pop 1, push 1. Binary ops pop 2, push 1.
- Short-circuit ops (`AND_SC`, `OR_SC`) pop 1, push nothing.
- After all instructions: if stack non-empty, pop final value. Zero = reject
  row, non-zero = accept.

### 3. Operand order for binary operations

The IR serializer visits the **right** child first, then **left**. So the
stack has `[rhs (bottom), lhs (top)]`. The backend pops `lhs` first (top),
then `rhs`. The operation is `lhs OP rhs`. Getting this wrong reverses
`SUB`, `DIV`, `LT`, `GT`, `LE`, `GE`.

### 4. Type conversions

When operands differ in type, widen the narrower one. Key rules:
- `i32` to `i64`: sign-extend.
- `i32`/`i64` to `f32`/`f64`: integer-to-float conversion.
- `i64` mixed with `f32`: BOTH promote to `f64` (avoids precision loss).
- `f32` to `f64`: float widen.
- `i128` and var-size headers: no cross-type conversion.

See `references/ir-spec.md` Section 5.1 for the full conversion matrix.

### 5. NULL handling

Each type has a specific NULL sentinel (Section 6 of the reference):
- `i32`: `0x80000000` (INT_NULL)
- `i64`: `0x8000000000000000` (LONG_NULL)
- `f32`/`f64`: NaN
- `i128`: both halves = LONG_NULL
- String/binary: length = -1
- Varchar: header word = 4

When `null_check` is enabled (bit 6 of options word):

**Arithmetic:** Preserve NULL sentinels. If either operand is NULL, result
is NULL. Integer division by zero also returns NULL sentinel.

**Comparisons — the strict/non-strict distinction:**
- `<`, `>` (strict): return **false** when any operand is NULL.
- `<=`, `>=` (non-strict): return **true** when **both** operands are NULL
  (sentinel equality), false when only one is NULL.
- `=`: true only when both are the same sentinel. `<>`: opposite.

This applies to both integer sentinels and float NaN. IEEE 754 returns false
for `NaN <= NaN`, but QuestDB requires **true**. A backend must override
IEEE 754 behavior for `<=`/`>=` when both operands are NaN.

**NULL-aware type conversions (Section 5.2):**
- `i32` INT_NULL widened to `i64` must become LONG_NULL, not `-2147483648L`.
- `i32`/`i64` NULL widened to float must become NaN.
- `i8` and `i16` skip NULL conversion checks (GeoHash sentinel semantics).

### 6. Float comparison: epsilon-based equality

Float/double `EQ` and `NE` use epsilon comparison, not exact bit equality:
```
EQ: |a - b| <= 0.0000000001
NE: |a - b| >  0.0000000001
```

Ordered comparisons combine epsilon equality with strict ordering:
- `GT`: not-equal-within-epsilon AND strictly greater.
- `GE`: equal-within-epsilon OR strictly greater-or-equal.
- `LT` / `LE`: analogous.

### 7. Division semantics

**Integer division by zero:** return the NULL sentinel (INT_NULL or
LONG_NULL), not a trap or zero.

**Float division by zero:** return NaN to match QuestDB's non-JIT evaluator.
IEEE 754 produces +/-Infinity; a correct backend must override this.

### 8. Short-circuit evaluation

Labels 0 (`next_row`) and 1 (`store_row`/`inc_count`) are pre-created.
Labels 2+ come from `BEGIN_SC`/`END_SC`.

- `AND_SC(label)`: pop; if false, jump to label.
- `OR_SC(label)`: pop; if true, jump to label.

**Label scoping pitfall:** Multiple `IN()` expressions reuse the same label
index. Each `OR_SC(n)`/`AND_SC(n)` must jump to the nearest **following**
`END_SC(n)`, not the last one globally. Pre-compute per-instruction jump
targets; do not use a single per-label target array.

### 9. Flag-based short-circuit optimization

When `EQ`/`NE` immediately precedes `AND_SC`/`OR_SC` on integer types, the
backend can skip materializing a boolean. Instead, emit only a compare and
tag the stack entry with `kFlagsEq` or `kFlagsNe`. The short-circuit opcode
then branches directly on the CPU flags.

Track `data_kind_t` on the stack: `kMemory`, `kConst`, `kFlagsEq`,
`kFlagsNe`. For `kFlags*` entries, use conditional branches. For
`kMemory`/`kConst`, test against zero and branch.

### 10. Bind variable addressing

The native ABI writes all bind variables at 8-byte stride (`vars_ptr + 8 *
index`). UUID (i128) writes 16 bytes, breaking this stride for any
subsequent variable. A new backend should use computed byte offsets from
the IR stream instead of a fixed stride.

### 11. Variable-size column access

String/binary columns use a two-vector layout (aux offsets + data). The
backend computes `length = aux[row+1] - aux[row] - header_size`. If length
is zero, read the actual header from the data vector to distinguish empty
string (header=0) from NULL (header=-1).

Varchar columns use 16-byte aux entries. Load the first 8 bytes; compare
the full 64-bit value against `4` (the NULL flag) for NULL detection.

The JIT only uses variable-size columns for NULL checks; it does not read
payload data.

### 12. i128 (UUID) comparisons

Only `EQ` and `NE` are supported. Use byte-parallel compare (e.g., PCMPEQB
on x86, equivalent on other ISAs). No ordered comparisons or arithmetic.

### 13. Loop structure

```
for input_index in 0..rows_count-1:
    evaluate IR for this row
    if result != 0:
        output[output_count++] = input_index   // row-ID mode
        // or: count++                          // count-only mode
return output_count  // or count
```

Pre-scan the IR to hoist column-address and constant loads out of the loop.
Cache column values within each row iteration (up to 8 columns).

### 14. Compilation options word

```
Bit 0     : debug (log assembly)
Bits 1-3  : log2 of max column type size
Bits 4-5  : execution hint (0=scalar, 1=single-size/SIMD-eligible, 2=mixed)
Bit 6     : null_check enabled
Bits 7-31 : reserved
```

## Existing backends for reference

Read these files to see how the rules above are implemented in practice:

- **x86 scalar:** `core/src/main/c/share/jit/x86.h`
- **x86 AVX2 SIMD:** `core/src/main/c/share/jit/avx2.h`
- **AArch64 scalar:** `core/src/main/c/share/jit/aarch64.h`
- **x86 primitives:** `core/src/main/c/share/jit/impl/x86.h`
- **AArch64 primitives:** `core/src/main/c/share/jit/impl/aarch64.h`
- **Null/epsilon constants:** `core/src/main/c/share/jit/impl/consts.h`
- **Java interpreter:** `core/src/main/java/io/questdb/jit/VectorFilterInterpreter.java`
- **Java Vector API:** `core/src/main/java/io/questdb/jit/VectorApiFilterExecutor.java`
- **IR serializer:** `core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`
- **IR decoder:** `core/src/main/java/io/questdb/jit/IrDecoder.java`
- **Shared C++ types:** `core/src/main/c/share/jit/common.h`

## Testing a new backend

1. Run `CompiledFilterRegressionTest` — compares JIT output against the
   non-JIT evaluator for a wide range of expressions.
2. Run `VectorCompiledFilterTest` — unit tests for the Java backend.
3. Run `VectorCompiledFilterIntegrationTest` — full SQL pipeline tests.
4. Edge cases to stress-test:
   - NULL with strict vs. non-strict operators
   - Division by zero (integer: NULL sentinel; float: NaN)
   - Mixed-type NULL coercion (INT_NULL -> LONG_NULL, INT_NULL -> NaN)
   - Chained IN() with reused label indices
   - Epsilon float equality near boundary values
   - UUID bind variables followed by other bind variables
   - Boolean column expansion (`bool_col` -> `bool_col = true`)
   - Symbol constant resolution to integer keys