# QuestDB JIT IR Reference

This document describes the Intermediate Representation (IR) used by QuestDB's
JIT filter compiler. It is intended to be sufficient for implementing a new
backend that consumes the IR and generates native code.

## Overview

The JIT system compiles SQL WHERE clause predicates into native machine code for
fast row filtering. The pipeline is:

1. **Java frontend** (`CompiledFilterIRSerializer`) traverses the SQL expression
   tree in post-order and emits a flat IR instruction stream into a contiguous
   memory buffer.
2. The serializer returns a 32-bit **options** word encoding type-size, execution
   hint, and flags.
3. A **backend** reads the IR stream plus options and executes/compiles the
   filter.

Four backends exist today: three native C++ backends that use
[asmjit](https://asmjit.com) to JIT-compile the IR into machine code — x86-64
scalar (`x86.h`), x86-64 AVX2 SIMD (`avx2.h`), and AArch64 scalar
(`aarch64.h`) — and a pure-Java scalar interpreter (`VectorFilterInterpreter`)
that evaluates the same IR without native code generation.

### Source Files

| File | Role |
|---|---|
| `core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java` | IR serializer (Java) |
| `core/src/main/java/io/questdb/jit/CompiledFilter.java` | Native JIT-compiled filter wrapper (row-ID mode) |
| `core/src/main/java/io/questdb/jit/CompiledCountOnlyFilter.java` | Native JIT-compiled filter wrapper (count-only mode) |
| `core/src/main/java/io/questdb/jit/VectorCompiledFilter.java` | Java interpreter filter wrapper (row-ID mode) |
| `core/src/main/java/io/questdb/jit/VectorCompiledCountOnlyFilter.java` | Java interpreter filter wrapper (count-only mode) |
| `core/src/main/java/io/questdb/jit/VectorFilterInterpreter.java` | Pure-Java scalar IR interpreter |
| `core/src/main/java/io/questdb/jit/IrDecoder.java` | IR instruction decoder (Java records) |
| `core/src/main/java/io/questdb/jit/FiltersCompiler.java` | JNI bridge to C++ |
| `core/src/main/java/io/questdb/jit/JitUtil.java` | Architecture support check |
| `core/src/main/c/share/jit/common.h` | Shared C++ types: `instruction_t`, `data_type_t`, `opcodes` |
| `core/src/main/c/share/jit/compiler.h` | JNI function declarations |
| `core/src/main/c/share/jit/compiler.cpp` | `Function`/`CountOnlyFunction` structs, JNI implementations |
| `core/src/main/c/share/jit/x86.h` | x86-64 scalar backend |
| `core/src/main/c/share/jit/avx2.h` | x86-64 AVX2 SIMD backend |
| `core/src/main/c/share/jit/aarch64.h` | AArch64 scalar backend |
| `core/src/main/c/share/jit/impl/x86.h` | x86 low-level primitives (arithmetic, comparisons, conversions) |
| `core/src/main/c/share/jit/impl/consts.h` | Null sentinel values and epsilon constants |

---

## 1. Instruction Format

Every instruction occupies exactly **24 bytes** (`INSTRUCTION_SIZE = 4 + 4 + 8 + 8 = 24`,
`CompiledFilterIRSerializer.java:121`). The C++ backend computes the instruction
count as `filterSize / sizeof(instruction_t)` (`compiler.cpp:913`).

```
Offset  Size   Field          C++ type
──────  ────   ─────          ────────
 0       4     opcode         int32_t   (opcodes enum)
 4       4     options        int32_t   (type code or unused, depending on opcode)
 8       8     ipayload.lo    int64_t   ┐ union: either two int64 halves
16       8     ipayload.hi    int64_t   ┘ or one double at offset 8
```

C++ definition (from `common.h`):

```cpp
struct instruction_t {
    opcodes opcode;       // int32_t
    int32_t options;
    union {
        struct {
            int64_t lo;
            int64_t hi;
        } ipayload;
        double dpayload;  // aliases ipayload.lo
    };
};
```

The Java serializer writes instructions as four consecutive writes. For most
instructions: `putInt(opcode)`, `putInt(options)`, `putLong(payload_lo)`,
`putLong(payload_hi)`. For floating-point immediates (`IMM` with `F4_TYPE` or
`F8_TYPE`), the third write is `putDouble(payload)` instead of `putLong`
(`CompiledFilterIRSerializer.java:739`). The byte layout is identical either
way — 8 bytes at offset 8 — but the Java write method differs.

The C++ backend reads the buffer by casting `filterAddress` to
`const instruction_t *` and iterating by `sizeof(instruction_t)` = 24 bytes.

---

## 2. Opcodes

All opcodes are defined identically in both Java (`CompiledFilterIRSerializer`)
and C++ (`common.h`).

| Value | Java name | C++ name | Arity | Description |
|-------|-----------|----------|-------|-------------|
| -1 | `UNDEFINED_CODE` | `Inv` | - | Invalid / stub placeholder |
| 0 | `RET` | `Ret` | 0 | Terminates the instruction stream |
| 1 | `IMM` | `Imm` | 0 | Push immediate constant |
| 2 | `MEM` | `Mem` | 0 | Push column value (memory read) |
| 3 | `VAR` | `Var` | 0 | Push bind variable value |
| 4 | `NEG` | `Neg` | 1 | Arithmetic negation (`-a`) |
| 5 | `NOT` | `Not` | 1 | Logical NOT (`!a`), implemented as XOR with 1 |
| 6 | `AND` | `And` | 2 | Bitwise AND of two boolean results |
| 7 | `OR` | `Or` | 2 | Bitwise OR of two boolean results |
| 8 | `EQ` | `Eq` | 2 | Equality comparison (`a == b`) |
| 9 | `NE` | `Ne` | 2 | Inequality comparison (`a != b`) |
| 10 | `LT` | `Lt` | 2 | Less than (`a < b`) |
| 11 | `LE` | `Le` | 2 | Less or equal (`a <= b`) |
| 12 | `GT` | `Gt` | 2 | Greater than (`a > b`) |
| 13 | `GE` | `Ge` | 2 | Greater or equal (`a >= b`) |
| 14 | `ADD` | `Add` | 2 | Addition (`a + b`) |
| 15 | `SUB` | `Sub` | 2 | Subtraction (`a - b`) |
| 16 | `MUL` | `Mul` | 2 | Multiplication (`a * b`) |
| 17 | `DIV` | `Div` | 2 | Division (`a / b`) |
| 18 | `AND_SC` | `And_Sc` | 1 | Short-circuit AND: pop, if false jump to `label[payload.lo]` |
| 19 | `OR_SC` | `Or_Sc` | 1 | Short-circuit OR: pop, if true jump to `label[payload.lo]` |
| 20 | `BEGIN_SC` | `Begin_Sc` | 0 | Create (allocate) a label at index `payload.lo` |
| 21 | `END_SC` | `End_Sc` | 0 | Bind (define position of) the label at index `payload.lo` |

### Evaluation Model

The IR uses a **stack-based (RPN) evaluation model**. Leaf instructions (`IMM`,
`MEM`, `VAR`) push values onto an implicit operand stack. Unary operators pop
one value and push the result. Binary operators pop two values and push the
result. The short-circuit opcodes pop one value but push nothing.

After all instructions have been processed, if the stack is non-empty, the
backend pops the final value and tests it: zero means "reject row", non-zero
means "accept row". If the stack is empty (all predicates resolved via
short-circuit jumps), the row acceptance/rejection was already handled by the
jump targets.

### `RET` Semantics

`RET` terminates instruction processing. The backend stops iterating the
instruction stream when it encounters `Ret` (or `Inv`). Every valid IR stream
ends with a `RET` instruction.

---

## 3. Type System

### 3.1 IR Type Codes

Type codes are stored in the `options` field of `IMM`, `MEM`, and `VAR`
instructions. They are defined identically in Java and C++.

| Value | Java constant | C++ enum (`data_type_t`) | Size | Description |
|-------|---------------|--------------------------|------|-------------|
| 0 | `I1_TYPE` | `i8` | 1 byte | Signed 8-bit integer |
| 1 | `I2_TYPE` | `i16` | 2 bytes | Signed 16-bit integer |
| 2 | `I4_TYPE` | `i32` | 4 bytes | Signed 32-bit integer |
| 3 | `F4_TYPE` | `f32` | 4 bytes | 32-bit IEEE 754 float |
| 4 | `I8_TYPE` | `i64` | 8 bytes | Signed 64-bit integer |
| 5 | `F8_TYPE` | `f64` | 8 bytes | 64-bit IEEE 754 double |
| 6 | `I16_TYPE` | `i128` | 16 bytes | 128-bit integer (UUID) |
| 7 | `STRING_HEADER_TYPE` | `string_header` | 8 bytes (logical) | String column NULL check |
| 8 | `BINARY_HEADER_TYPE` | `binary_header` | 8 bytes (logical) | Binary column NULL check |
| 9 | `VARCHAR_HEADER_TYPE` | `varchar_header` | 8 bytes (logical) | Varchar column NULL check |

**Important:** The variable-size types (7, 8, 9) can only appear in NULL checks
(equality/inequality with NULL). The serializer validates this via
`ensureOnlyVarSizeHeaderChecks()` and rejects any other use.

### 3.2 QuestDB Column Type to IR Type Mapping

The mapping is defined in `CompiledFilterIRSerializer.columnTypeCode()`:

| QuestDB Column Type | IR Type Code |
|---|---|
| `BOOLEAN`, `BYTE`, `GEOBYTE` | `I1_TYPE` (0) |
| `SHORT`, `GEOSHORT`, `CHAR` | `I2_TYPE` (1) |
| `INT`, `IPv4`, `GEOINT`, `SYMBOL` | `I4_TYPE` (2) |
| `FLOAT` | `F4_TYPE` (3) |
| `LONG`, `GEOLONG`, `DATE`, `TIMESTAMP` | `I8_TYPE` (4) |
| `DOUBLE` | `F8_TYPE` (5) |
| `LONG128`, `UUID` | `I16_TYPE` (6) |
| `STRING` | `STRING_HEADER_TYPE` (7) |
| `BINARY` | `BINARY_HEADER_TYPE` (8) |
| `VARCHAR`, `VARCHAR_SLICE` | `VARCHAR_HEADER_TYPE` (9) |

SYMBOL columns are mapped to `I4_TYPE` because symbol values are stored as
integer keys in the symbol table.

### 3.3 Type Size Calculation

The `type_shift()` function in `common.h` returns log2 of the element size:

| Type | `type_shift()` | Element size |
|---|---|---|
| `i8` | 0 | 1 byte |
| `i16` | 1 | 2 bytes |
| `i32`, `f32` | 2 | 4 bytes |
| `i64`, `f64` | 3 | 8 bytes |
| `i128` | 4 | 16 bytes |

Variable-size types (`string_header`, `binary_header`, `varchar_header`) are not
handled by `type_shift()` because they are accessed through special code paths.

The Java `TypesObserver.typeSizeBytes()` method returns the byte size directly:

| Type code | Byte size |
|---|---|
| `I1_TYPE` | 1 |
| `I2_TYPE` | 2 |
| `I4_TYPE`, `F4_TYPE` | 4 |
| `I8_TYPE`, `F8_TYPE`, `STRING_HEADER_TYPE`, `BINARY_HEADER_TYPE`, `VARCHAR_HEADER_TYPE` | 8 |
| `I16_TYPE` | 16 |

---

## 4. Instruction Details

### 4.1 `IMM` (Immediate Constant)

Pushes a constant value onto the stack.

| Field | Content |
|---|---|
| `opcode` | 1 (`Imm`) |
| `options` | Type code (0-9) |
| `ipayload.lo` / `dpayload` | The constant value |
| `ipayload.hi` | Upper 64 bits for i128; zero for all other types |

**Payload encoding by type:**

- **Integer types** (`i8`, `i16`, `i32`, `i64`): value stored in `ipayload.lo`
  as a sign-extended 64-bit integer.
- **Float types** (`f32`, `f64`): value stored in `dpayload` as a C `double`.
  For `f32`, the backend narrows it to `float` at code generation time.
- **i128** (UUID): lower 64 bits in `ipayload.lo`, upper 64 bits in
  `ipayload.hi`.

### 4.2 `MEM` (Column Reference)

Pushes a value read from a column at the current row index.

| Field | Content |
|---|---|
| `opcode` | 2 (`Mem`) |
| `options` | Type code (0-9) |
| `ipayload.lo` | Column index (0-based position in the column data array) |
| `ipayload.hi` | 0 (unused) |

**Backend behavior by type:**

- **Fixed-size types** (`i8`..`i128`): The backend loads the value from
  `cols[column_idx] + input_index * element_size`. For `i8` and `i16`, the load
  is sign-extended to 32 bits (`movsx`).
- **`string_header`**: See Section 4.6 for the full variable-size column access
  algorithm.
- **`binary_header`**: Same algorithm as string_header, but with an 8-byte
  length header instead of 4-byte.
- **`varchar_header`**: See Section 4.7 for the varchar-specific access path.

### 4.3 `VAR` (Bind Variable)

Pushes a bind variable value.

| Field | Content |
|---|---|
| `opcode` | 3 (`Var`) |
| `options` | Type code |
| `ipayload.lo` | Bind variable index (0-based position in the vars array) |
| `ipayload.hi` | 0 (unused) |

The backend reads the value from `vars_ptr + 8 * index` with a type-appropriate
load size. See Section 10.4 for the full bind variable memory layout.

### 4.4 Operator Instructions

Operator instructions have no meaningful `options` or payload (both zero), with
the exception of short-circuit opcodes.

| Opcode | Stack effect | Notes |
|---|---|---|
| `NEG` | pop 1, push 1 | Preserves NULL (if null-checks enabled) |
| `NOT` | pop 1, push 1 | XOR with 1; operates on 32-bit GP register |
| `AND` | pop 2, push 1 | Bitwise AND of two 32-bit boolean values |
| `OR` | pop 2, push 1 | Bitwise OR of two 32-bit boolean values |
| `EQ` | pop 2, push 1 | Result: 1 if equal, 0 otherwise |
| `NE` | pop 2, push 1 | Result: 1 if not equal, 0 otherwise |
| `LT` | pop 2, push 1 | Result: 1 if lhs < rhs, 0 otherwise |
| `LE` | pop 2, push 1 | Result: 1 if lhs <= rhs, 0 otherwise |
| `GT` | pop 2, push 1 | Result: 1 if lhs > rhs, 0 otherwise |
| `GE` | pop 2, push 1 | Result: 1 if lhs >= rhs, 0 otherwise |
| `ADD` | pop 2, push 1 | Arithmetic addition |
| `SUB` | pop 2, push 1 | Arithmetic subtraction |
| `MUL` | pop 2, push 1 | Arithmetic multiplication |
| `DIV` | pop 2, push 1 | Arithmetic division |

**Note on operand order:** `PostOrderTreeTraversalAlgo` visits the **right**
child first, then the **left** child, then the operator node
(`PostOrderTreeTraversalAlgo.java:95`). For a binary operation `a OP b`
(tree node with `lhs = a`, `rhs = b`), the visitor pushes `b` (rhs) first,
then `a` (lhs) second. When the backend pops, the x86 `get_arguments()`
(`x86.h:913`) does `lhs = pop()` (gets `a`, the last pushed / top of stack),
then `rhs = pop()` (gets `b`, the first pushed). The operation is then
`lhs OP rhs` = `a OP b`.

**This matters for non-commutative operations.** For `a - b`, the stack
contains `[b (bottom), a (top)]`. The backend pops `a` as `lhs` and `b` as
`rhs`, then computes `a - b`. Getting this wrong reverses `SUB`, `DIV`, `LT`,
`GT`, `LE`, and `GE`.

### 4.5 Short-Circuit Instructions

These instructions implement early-exit evaluation for AND/OR chains.

**`AND_SC`** (opcode 18):
- Pops one boolean value from the stack.
- If the value is false (zero), jumps to `labels[ipayload.lo]`.
- If true, falls through to the next instruction.
- Pushes nothing.

**`OR_SC`** (opcode 19):
- Pops one boolean value from the stack.
- If the value is true (non-zero), jumps to `labels[ipayload.lo]`.
- If false, falls through to the next instruction.
- Pushes nothing.

**`BEGIN_SC`** (opcode 20):
- Creates (allocates) a new label at index `ipayload.lo` in the label array.
- Does not affect the value stack.

**`END_SC`** (opcode 21):
- Binds (defines the position of) the label at index `ipayload.lo`.
- The label becomes the target for any prior forward jumps to that index.
- Does not affect the value stack.

#### Label Conventions

The backend pre-creates two labels before processing the IR:

| Index | Name | Purpose |
|---|---|---|
| 0 | `l_next_row` | Skip row storage, advance to next row. Used by `AND_SC` on false. |
| 1 | `l_store_row` / `l_inc_count` | Store row ID (or increment counter), then advance. Used by `OR_SC` on true. |

Labels at index 2+ are user-defined via `BEGIN_SC`/`END_SC`, typically for
`IN()` list evaluation.

Maximum labels: **8** (defined as `MAX_LABELS` in both Java and C++).

#### Label Scoping for Chained IN() Expressions

When multiple `IN()` expressions appear in the same AND chain, the serializer
reuses the **same label index** (typically 2) for each one. For example,
`a IN (1,2) AND b IN (3,4)` produces two `BEGIN_SC(2)`/`END_SC(2)` pairs in
sequence:

```
BEGIN_SC(2)  ; first IN group
...
OR_SC(2)    ; on match, jump to END_SC(2) of THIS group
...
AND_SC(0)
END_SC(2)   ; binds label 2 for the first group
BEGIN_SC(2)  ; second IN group (reuses label index 2)
...
OR_SC(2)    ; must jump to END_SC(2) of THIS group, not the first
...
AND_SC(0)
END_SC(2)   ; binds label 2 for the second group
```

In a native code generator (like asmjit), `BEGIN_SC` creates a new forward
label object and `END_SC` binds it, so each pair naturally scopes correctly.

In an interpreter, a naive approach of storing one target per label index fails
because the second `END_SC(2)` overwrites the first. Each `AND_SC(2)`/`OR_SC(2)`
must resolve to the nearest **following** `END_SC` with the same label index,
not the globally last one. Pre-compute per-instruction jump targets during
compilation rather than using a single per-label target array.

#### Flag-Based Optimization

When an `EQ` or `NE` instruction is immediately followed by `AND_SC` or `OR_SC`,
and the operand type supports it (integer types including i128), both the x86
backend (`x86.h:1000`) and the AArch64 backend (`aarch64.h:1016`) emit only a
compare instruction without materializing a boolean result. The value pushed
onto the stack carries a `data_kind_t` marker:

| `data_kind_t` | Meaning | `AND_SC` uses | `OR_SC` uses |
|---|---|---|---|
| `kFlagsEq` | CMP was for equality | branch-if-not-equal | branch-if-equal |
| `kFlagsNe` | CMP was for inequality | branch-if-equal | branch-if-not-equal |

On x86, this avoids the `SETE`/`SETNE` + `TEST` + `JZ`/`JNZ` sequence. On
AArch64, it avoids the `CSET` + `CBZ`/`CBNZ` sequence.

### 4.6 Variable-Size Column Access: String and Binary

String and binary columns use a two-vector storage layout:

- **Aux (index) vector:** An array of **N+1** 64-bit offsets (8 bytes each),
  where N is the row count. Entry `i` is the byte offset into the data vector
  where row `i`'s entry begins. Entry `N` is the end-of-data sentinel. The
  backend reads **both** `aux[row]` and `aux[row + 1]` to compute the row's
  storage size, so the aux vector must always have at least `row_count + 1`
  entries.
- **Data vector:** A byte stream containing `[length_header][payload_bytes]` for
  each row. The header is 4 bytes (`int32`) for STRING, 8 bytes (`int64`) for
  BINARY.

The JIT backend reads variable-size columns through `read_mem_varsize()`
(`x86.h:119`). The exact generated code sequence:

```
// Inputs: column_idx, input_index (current row), header_size (4 for STRING, 8 for BINARY)
// varsize_aux_ptr and data_ptr are function arguments

1. aux_base     = *(int64_t*)(varsize_aux_ptr + column_idx * 8)
2. next_index   = input_index + 1
3. offset       = *(int64_t*)(aux_base + input_index * 8)     // x86.h:141
4. next_offset  = *(int64_t*)(aux_base + next_index * 8)      // x86.h:142
5. length       = next_offset - offset - header_size           // x86.h:143-144

6. if length != 0:    // JNZ at x86.h:147
       return length  // Non-empty, non-NULL: length is the payload size

7. // length == 0: ambiguous — could be empty string (header=0) or NULL (header=-1)
   // In both cases: next_offset - offset == header_size (header only, no payload)
   data_base = *(int64_t*)(data_ptr + column_idx * 8)         // x86.h:150
   length    = *(header_size bytes)(data_base + offset)        // x86.h:151
   return length  // 0 for empty, -1 for NULL
```

**NULL sentinel:** `TableUtils.NULL_LEN = -1` (`TableUtils.java:129`). For
STRING, this is a 4-byte `int` value of -1 (0xFFFFFFFF). For BINARY, this is
an 8-byte `long` value of -1 (0xFFFFFFFFFFFFFFFF).

The result is pushed onto the value stack as `i32` (for STRING, 4-byte header)
or `i64` (for BINARY, 8-byte header), representing the length value. A
subsequent `EQ` with the NULL sentinel determines whether the column is NULL.

### 4.7 Variable-Size Column Access: Varchar

Varchar columns use a different aux vector format. Each aux entry is **16 bytes**
(`VARCHAR_AUX_WIDTH_BYTES = 2 * Long.BYTES`), structured as:

```
Byte offset   Size    Content
───────────   ────    ───────
 0             4      Header word (flags + length)
 4             6      Inlined UTF-8 prefix
10             6      48-bit data vector offset (for non-inlined values)
```

**Header word bit layout (4 bytes, little-endian):**

```
Bits [3:0]   — Flags:
  Bit 0 (0x1): HEADER_FLAG_INLINED   — value fully inlined in aux entry
  Bit 1 (0x2): HEADER_FLAG_ASCII     — string is ASCII-only
  Bit 2 (0x4): VARCHAR_HEADER_FLAG_NULL — value is NULL
  Bit 3:       reserved

Bits [31:4]  — Size/length (28 bits, max 268 MB)
  For inlined strings (bit 0 set):  only bits [7:4] used (max 9 bytes)
  For non-inlined strings:          bits [31:4] hold the full length
```

The JIT backend reads varchar headers through `read_mem_varchar_header()`, which
loads only the first 8 bytes of the aux entry (the header word + part of the
prefix):

```
1. aux_base     = varsize_aux_ptr[column_idx]
2. header_offset = input_index * 16              // each entry is 16 bytes (shift by 4)
3. header       = *(int64_t*)(aux_base + header_offset)  // load first 8 bytes
```

The result is pushed as `i64`. For NULL detection, the serializer emits
`IMM(I8_TYPE, VARCHAR_HEADER_FLAG_NULL)` — the integer value `4` — and the
backend performs a **full 64-bit equality comparison** (`header == 4`), not a
bitmask test. This works because NULL varchar entries are written as:

```
putInt(0x00000004)   // header word: only NULL flag set, length = 0
putInt(0x00000000)   // zero prefix bytes
putShort(0x0000)     // zero prefix bytes
```

The first 8 bytes of a NULL entry are exactly `0x0000000000000004` (little-endian
int64 = 4). Non-NULL entries always have additional bits set (length in bits 4+,
or non-zero inlined prefix in bytes 4-9), so the 8-byte value is never `4` for
a non-NULL entry.

**Note:** The JIT backend uses varchar headers only for NULL checks. It does not
extract lengths or read varchar payload data.

### 4.8 Operand Stack Metadata: `data_kind_t`

Each value on the operand stack carries a `data_kind_t` tag that tracks the
value's origin and representation. This is defined in `common.h`:

```cpp
enum class data_kind_t : uint8_t {
    kMemory,   // Value loaded from memory (column or bind variable)
    kConst,    // Compile-time constant (immediate)
    kFlagsEq,  // CMP emitted for equality; use JNE to skip if not equal
    kFlagsNe,  // CMP emitted for inequality; use JE to skip if equal
};
```

| Kind | Meaning | When set |
|---|---|---|
| `kMemory` | Value originates from a memory load (column read, bind variable, or result of an operation involving memory values). Used as the default for computed results. | `MEM`, `VAR` instructions; results of operations where at least one operand is `kMemory`. |
| `kConst` | Value is a compile-time constant. Both operands of a binary operation must be `kConst` for the result to be `kConst`. | `IMM` instructions; results of operations where both operands are `kConst`. |
| `kFlagsEq` | No value in the register; CPU flags are set from a CMP instruction for an equality check. The next short-circuit opcode should use `JE`/`JNE` directly. | Set by the `EQ` opcode when a short-circuit opcode follows immediately and the operand type supports flag optimization (integer types including i128). |
| `kFlagsNe` | Same as `kFlagsEq` but for inequality. | Set by the `NE` opcode under the same conditions. |

The `dst_kind()` helper determines the result kind for binary operations:

```cpp
inline data_kind_t dst_kind(const jit_value_t &lhs, const jit_value_t &rhs) {
    return (lhs.dkind() == data_kind_t::kConst && rhs.dkind() == data_kind_t::kConst)
           ? data_kind_t::kConst : data_kind_t::kMemory;
}
```

A new backend must track `data_kind_t` on the stack to correctly handle the
flag-based optimization in short-circuit evaluation. When popping a value for
`AND_SC` or `OR_SC`, check the kind:

- `kFlagsEq`: use a conditional branch that tests the equality flag from the
  preceding CMP (e.g., `JE` for OR_SC, `JNE` for AND_SC).
- `kFlagsNe`: use the inverse branch (e.g., `JNE` for OR_SC, `JE` for AND_SC).
- `kMemory` or `kConst`: materialize the value in a register, test it against
  zero, and branch accordingly.

### 4.9 i128 Comparison Semantics

128-bit values (UUID/LONG128) are stored in XMM/NEON vector registers and
compared using SIMD byte-comparison instructions.

**x86-64 implementation** (from `impl/x86.h`):

```cpp
inline void int128_cmp(Compiler &c, const Vec &lhs, const Vec &rhs) {
    Gp mask = c.new_gp16();
    c.pcmpeqb(lhs, rhs);     // Compare 16 bytes in parallel, set each byte to 0xFF if equal
    c.pmovmskb(mask, lhs);   // Extract MSB of each byte into 16-bit mask
    c.cmp(mask, 0xffff);      // All 16 bytes equal iff mask == 0xFFFF
}

inline Gp int128_eq(Compiler &c, const Vec &lhs, const Vec &rhs) {
    Gp r = c.new_gp64();
    c.xor_(r, r);
    int128_cmp(c, lhs, rhs);
    c.sete(r.r8_lo());        // r = 1 if ZF set (all equal), 0 otherwise
    return r.as<Gp>();
}

inline Gp int128_ne(Compiler &c, const Vec &lhs, const Vec &rhs) {
    Gp r = c.new_gp64();
    c.xor_(r, r);
    int128_cmp(c, lhs, rhs);
    c.setne(r.r8_lo());       // r = 1 if ZF clear (not all equal), 0 otherwise
    return r.as<Gp>();
}
```

Only `EQ` and `NE` are supported for i128. The serializer does not emit `LT`,
`LE`, `GT`, `GE`, or arithmetic operations for i128 values (UUID and LONG128
columns only support equality comparisons in the JIT).

When the flag-based optimization is active (i128 comparison followed by
`AND_SC`/`OR_SC`), the backend emits only `int128_cmp()` (PCMPEQB + PMOVMSKB +
CMP) and pushes a `kFlagsEq`/`kFlagsNe` marker, avoiding the SETE/SETNE
materialization.

### 4.10 Division Semantics

Integer and floating-point division handle zero divisors differently. A new
backend must replicate this behavior exactly.

**Integer division** (`int32_div` at `impl/x86.h:233`, `impl/aarch64.h:229`;
`int64_div` at `impl/x86.h:303`, `impl/aarch64.h:285`):

The backend must check for zero divisor before executing the hardware divide
instruction (x86 `IDIV` traps on zero; AArch64 `SDIV` silently returns zero).
Both backends return the NULL sentinel on zero divisor.

Without `null_check`:

```
if divisor == 0:
    return INT_NULL (or LONG_NULL for 64-bit)
return dividend / divisor    // signed division
```

With `null_check` enabled, the zero and NULL checks are combined. For `int32_div`
(`impl/x86.h:254`):

```
result = INT_NULL                          // optimistic: assume null
if (divisor & 0x7FFFFFFF) == 0: return     // catches both 0 and INT_NULL (0x80000000)
if dividend == INT_NULL: return
result = dividend / divisor                // signed IDIV
```

The `TEST rhs, 0x7FFFFFFF` trick (`impl/x86.h:255`) checks for both zero and
`INT_NULL` (0x80000000) in a single instruction: both have `rhs & 0x7FFFFFFF == 0`.

For `int64_div` with `null_check` (`impl/x86.h:323`): uses `BTR` (bit test and
reset) on bit 63 to clear the sign bit, then tests if the result is zero. This
catches both zero and `LONG_NULL` (0x8000000000000000) since both have all
non-sign bits clear.

The AArch64 backends (`impl/aarch64.h:229`, `impl/aarch64.h:285`) use equivalent
logic with `CBZ`, `AND`, and `CMP` instructions.

**Floating-point division** (`float_div`, `double_div`):

The C++ native backends emit `DIVSS`/`DIVSD` (x86) or `fdiv` (AArch64) without
an explicit zero check, relying on IEEE 754 semantics where `x / 0.0` produces
`±Infinity`.

**Semantic divergence:** QuestDB's non-JIT Function classes (`DivDoubleFunction`,
etc.) return **NaN** for float/double division by zero, not `±Infinity`. A new
backend should produce NaN on zero divisor to match the non-JIT path. The
existing C++ backends produce `±Infinity`, which diverges from the non-JIT
evaluator for queries like `col / 0 > 0` (JIT returns true via Infinity, non-JIT
returns false via NaN). This divergence is tolerated because the regression tests
only compare null-equality results for arithmetic expressions, but a backend
aiming for exact parity should treat float division by zero as producing NaN.

---

## 5. Type Conversions

### 5.1 Implicit Conversions at the Backend

When a binary operation has operands of different types, the `convert()` function
in `x86.h` (and equivalently in `aarch64.h`) widens the narrower operand. The
full conversion matrix:

| Left type | Right type | Left converts to | Right converts to |
|---|---|---|---|
| i8/i16/i32 | i8/i16/i32 | (no conversion) | (no conversion) |
| i8/i16/i32 | i64 | i64 (`movsxd`) | (no conversion) |
| i8/i16/i32 | f32 | f32 (`cvtsi2ss`) | (no conversion) |
| i8/i16/i32 | f64 | f64 (`cvtsi2sd`) | (no conversion) |
| i64 | i8/i16/i32 | (no conversion) | i64 (`movsxd`) |
| i64 | i64 | (no conversion) | (no conversion) |
| i64 | f32 | f64 (`cvtsi2sd`) | f64 (`cvtss2sd`) |
| i64 | f64 | f64 (`cvtsi2sd`) | (no conversion) |
| f32 | i8/i16/i32 | (no conversion) | f32 (`cvtsi2ss`) |
| f32 | i64 | f64 (`cvtss2sd`) | f64 (`cvtsi2sd`) |
| f32 | f32 | (no conversion) | (no conversion) |
| f32 | f64 | f64 (`cvtss2sd`) | (no conversion) |
| f64 | i8/i16/i32 | (no conversion) | f64 (`cvtsi2sd`) |
| f64 | i64 | (no conversion) | f64 (`cvtsi2sd`) |
| f64 | f32 | (no conversion) | f64 (`cvtss2sd`) |
| f64 | f64 | (no conversion) | (no conversion) |
| i128 | i128 | (no conversion) | (no conversion) |
| string/binary/varchar header | same | (no conversion) | (no conversion) |

**Key rules:**
- Integer-to-integer widening: `movsxd` (sign-extend 32-bit to 64-bit).
- Integer-to-float: `cvtsi2ss` (32-bit int to float) or `cvtsi2sd` (to double).
- Long-to-float: both operands promote to f64 (i64 to double + f32 to double).
  This avoids precision loss.
- Float-to-double: `cvtss2sd`.
- i128, string/binary/varchar headers: no cross-type conversion supported.

### 5.2 NULL-Aware Conversions

When `null_check` is enabled, integer-to-wider conversions preserve NULL sentinel
values. The `cvt_null_check()` function determines whether a NULL check is
needed during conversion:

```cpp
inline bool cvt_null_check(data_type_t type) {
    return !(type == data_type_t::i8 || type == data_type_t::i16);
}
```

- `i8` and `i16` columns: NULL check skipped during conversion (their NULL
  sentinels are specific GeoHash values, not the generic INT_NULL pattern).
- `i32` to `i64`: if the 32-bit value equals `INT_NULL` (0x80000000), the result
  is `LONG_NULL` (0x8000000000000000) instead of a sign-extended value. This is
  critical for arithmetic null propagation: without it, `null_i32 + i64` widens
  `INT_NULL` to `-2147483648L` (a valid long), bypassing the i64 null check in
  the addition and producing a garbage result instead of `LONG_NULL`.
- `i32` to `f32`/`f64`: if `INT_NULL`, the result is `NaN`.
- `i64` to `f64`: if `LONG_NULL`, the result is `NaN`.

### 5.3 Constant Type Inference in the Serializer

The serializer defers writing constants until the entire predicate has been
scanned (the "backfill" mechanism). This allows it to determine the correct type
for numeric literals.

`TypesObserver.constantTypeCode()` scans observed column/bind-variable types
from widest to narrowest and returns the widest observed type. Special case: if
both `i64` and `f32` are observed, the constant type is promoted to `f64` to
avoid precision loss.

When the predicate has mixed-size operands (`hasMixedSizes() == true`), the
serializer uses `serializeUntypedNumber()` which tries parsing the constant as
int, then long, then double, then float, in that order.

### 5.4 Immediate-to-Register Conversions

When the backend encounters an immediate value (asmjit `Imm`) that needs to be
in a register for an operation, `imm2reg()` handles the conversion:

- Integer immediate to `f32`: loaded via const pool (`movss`).
- Integer immediate to `f64`: loaded via const pool (`movsd`).
- Integer immediate to `i64` or value exceeds 32-bit range: `movabs` into 64-bit
  GP register.
- Double immediate to `f32` (if fits): loaded via const pool as float.
- Double immediate to `f64`: loaded via const pool as double.

---

## 6. NULL Handling

### 6.1 NULL Sentinel Values

Each type has a specific bit pattern that represents NULL:

| Column Type | IR Type | NULL Value | Java constant |
|---|---|---|---|
| `GEOBYTE` | i8 | `GeoHashes.BYTE_NULL` | - |
| `GEOSHORT` | i16 | `GeoHashes.SHORT_NULL` | - |
| `GEOINT`, GeoHash types | i32 | `GeoHashes.INT_NULL` | - |
| `IPv4` | i32 | `Numbers.IPv4_NULL` | - |
| `INT`, `SYMBOL`, other i32 | i32 | `Numbers.INT_NULL` (0x80000000) | `INT_NULL` |
| `LONG`, `DATE`, `TIMESTAMP` | i64 | `Numbers.LONG_NULL` (0x8000000000000000) | `LONG_NULL` |
| `GEOLONG` | i64 | `GeoHashes.NULL` | - |
| `FLOAT` | f32 | `Float.NaN` (0x7fc00000) | NaN |
| `DOUBLE` | f64 | `Double.NaN` (0x7ff8000000000000) | NaN |
| `UUID` / `LONG128` | i128 | `(LONG_NULL, LONG_NULL)` | Both halves = LONG_NULL |
| `STRING` | string_header | `TableUtils.NULL_LEN` | Serialized as i32 IMM |
| `BINARY` | binary_header | `TableUtils.NULL_LEN` | Serialized as i64 IMM |
| `VARCHAR` | varchar_header | `VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL` | Serialized as i64 IMM |

**Note:** `BOOLEAN`, `BYTE`, `SHORT`, and `CHAR` columns are not considered
nullable in the JIT context. The serializer throws an error if `null` is used
with these non-geo types.

### 6.2 NULL Checks in Arithmetic Operations

When `null_check` is enabled in the options, arithmetic operations preserve NULL
sentinel values:

- **Integer negation**: if the operand is `INT_NULL` or `LONG_NULL`, the result
  is the same NULL value (not negated).
- **Integer arithmetic** (`add`, `sub`, `mul`, `div`): the x86 backend checks
  both operands for NULL and propagates the sentinel if either is NULL (using
  `cmove` conditional moves).
- **Float/double arithmetic**: NaN propagation for `+`, `-`, `*` is handled
  automatically by IEEE 754 (any operation with NaN produces NaN). However,
  float/double **division** requires an explicit zero check: QuestDB's non-JIT
  evaluator returns NaN for `x / 0.0` (not `±Infinity`), so a backend must
  check for zero divisor and produce NaN. See Section 4.10 for details.

### 6.3 NULL in Comparisons

When null checks are enabled, comparisons follow QuestDB's `Numbers.lessThan()`
semantics, which differ between strict and non-strict operators.

**Integer types** (`i32`, `i64`): when either operand is `INT_NULL` /
`LONG_NULL`:

| Operator | Both NULL | One NULL, one non-NULL |
|---|---|---|
| `EQ` (`=`) | true (raw sentinel comparison: `INT_NULL == INT_NULL`) | false |
| `NE` (`<>`) | false (raw sentinel comparison) | true |
| `LT` (`<`) | false | false |
| `GT` (`>`) | false | false |
| `LE` (`<=`) | true (`a == b`, both are the same sentinel) | false |
| `GE` (`>=`) | true (`a == b`, both are the same sentinel) | false |

The key distinction: strict operators (`<`, `>`) always return false when any
NULL is involved. Non-strict operators (`<=`, `>=`) return true when **both**
operands are NULL (because the sentinel values are equal). This matches the
`Numbers.lessThan(a, b, negated)` function used by QuestDB's non-JIT evaluator:

```java
public static boolean lessThan(int a, int b, boolean negated) {
    final boolean eq = a == b;
    return (eq || (a != INT_NULL && b != INT_NULL))
        && (negated ? (eq || a > b) : (!eq && a < b));
}
```

This behavior means `column <= null` and `column >= null` act as IS NULL checks
(returning true only for NULL rows), matching QuestDB's standard evaluation.

**Float/double types**: NaN-based null handling follows an analogous pattern:

| Operator | Both NaN | One NaN, one non-NaN |
|---|---|---|
| `EQ` | true | false |
| `NE` | false | true |
| `LT` | false | false |
| `GT` | false | false |
| `LE` | true | false |
| `GE` | true | false |

IEEE 754 naturally returns false for ordered comparisons involving NaN, but it
also returns false for `NaN <= NaN`. A backend must override this: when **both**
operands are NaN, `<=` and `>=` must return true to match QuestDB's IS NULL
semantics.

**Why this matters:** QuestDB's SQL evaluator treats `column op null` uniformly
— the `null` keyword is serialized as the type's null sentinel, and comparisons
against it follow the rules above. Without the both-NULL exception for `<=`/`>=`,
queries like `f32 <= null` or `i32 >= null` produce zero rows instead of
returning all NULL rows.

---

## 7. Float Comparison Semantics

Float and double equality/inequality comparisons use **epsilon-based comparison**
rather than exact bit-level comparison:

```cpp
static const double DOUBLE_EPSILON = 0.0000000001;
static const float  FLOAT_EPSILON  = 0.0000000001;
```

- `EQ` for floats: `|a - b| <= epsilon` (using `fabs` of the difference).
- `NE` for floats: `|a - b| > epsilon`.

Ordered comparisons (`<`, `>`, `<=`, `>=`) on floats combine epsilon-based
equality with strict ordering:

- `GT` (a > b): `(a != b within epsilon) AND (a > b strictly)`.
- `GE` (a >= b): `(a == b within epsilon) OR (a >= b strictly)`.
- `LT` (a < b): `(a != b within epsilon) AND (a < b strictly)`.
- `LE` (a <= b): `(a == b within epsilon) OR (a <= b strictly)`.

This ensures values that are "equal within epsilon" compare as equal in all
comparison operators, producing consistent results.

---

## 8. Compilation Options

The `serialize()` method returns a 32-bit options word. The C++ backend parses
it in `Function::compile()`.

```
Bit layout:
  Bit 0       : Debug flag (1 = log generated assembly to stdout)
  Bits 1-3    : log2 of max column type size
                 0 = 1 byte, 1 = 2 bytes, 2 = 4 bytes, 3 = 8 bytes, 4 = 16 bytes
  Bits 4-5    : Execution hint
                 0 = scalar
                 1 = single-size (SIMD-eligible)
                 2 = mixed-size (forces scalar)
  Bit 6       : Null checks flag (1 = enabled)
  Bits 7-31   : Reserved (zero)
```

### Execution Hint Determination

The serializer determines the execution hint as follows:

1. If `forceScalar` is requested, or if `forceScalarMode` was set during
   serialization: hint = `EXEC_HINT_SCALAR` (0).
2. Otherwise, if `TypesObserver.hasMixedSizes()` is true (columns of different
   byte sizes appear in the filter): hint = `EXEC_HINT_MIXED_SIZE_TYPE` (2).
3. Otherwise: hint = `EXEC_HINT_SINGLE_SIZE_TYPE` (1).

The `forceScalarMode` flag is set when a predicate contains arithmetic
operations (`+`, `-`, `*`, `/`) on byte or short columns (max type size <= 2
bytes). This is because SIMD mode would use byte/short-width overflow semantics,
whereas the Java `Function` classes implicitly upcast to int for arithmetic.

### Backend Dispatch

The C++ `Function::compile()` method uses the options to choose the execution
path:

```cpp
uint32_t type_size = (options >> 1) & 7;  // log2 of max column type size
uint32_t exec_hint = (options >> 4) & 3;  // execution hint
bool null_check    = (options >> 6) & 1;  // null check flag
```

On x86-64:
- If `exec_hint == 1` (single_size) AND CPU has AVX2: use `avx2_loop()`.
  - SIMD step size: `256 / ((1 << type_size) * 8)`.
  - step=32 for i8, step=16 for i16, step=8 for i32/f32, step=4 for i64/f64.
- Otherwise: use `scalar_loop()`.

On AArch64: always use `scalar_loop()` (no SIMD backend implemented).

---

## 9. Short-Circuit Evaluation

Short-circuit evaluation is used only in **scalar mode** for top-level AND or OR
chains. It allows early termination of predicate evaluation when the result is
already determined.

### 9.1 AND Chains

For a filter like `pred1 AND pred2 AND pred3`:

```
<pred1 IR>     ; evaluate pred1, push result
AND_SC(0)      ; if false, jump to l_next_row (label 0)
<pred2 IR>     ; evaluate pred2, push result
AND_SC(0)      ; if false, jump to l_next_row
<pred3 IR>     ; evaluate pred3, push result (final - tested by loop epilog)
RET
```

Predicates are sorted by priority (ascending) before serialization. The priority
system is designed so that the most selective predicates are evaluated first:

| Priority | Predicate type |
|---|---|
| 0 (highest) | UUID/LONG128 equality |
| 1 | LONG/TIMESTAMP/DATE equality |
| 2 | INT/IPv4 equality |
| 3 | SYMBOL equality |
| 4 | Other type equality |
| 5 | Other comparisons (<, >, <=, >=) |
| 6 | Other type inequality |
| 7 | SYMBOL inequality |
| 8 | INT/IPv4 inequality |
| 9 | LONG/TIMESTAMP/DATE inequality |
| 10 (lowest) | UUID/LONG128 inequality |

### 9.2 OR Chains

For a filter like `pred1 OR pred2 OR pred3`:

```
<pred1 IR>     ; evaluate pred1, push result
OR_SC(1)       ; if true, jump to l_store_row (label 1)
<pred2 IR>     ; evaluate pred2, push result
OR_SC(1)       ; if true, jump to l_store_row
<pred3 IR>     ; evaluate pred3, push result (final - tested by loop epilog)
RET
```

Predicates are sorted by **inverted priority** (descending) - predicates with
the lowest probability of success are evaluated first.

### 9.3 IN() Lists with Short-Circuit

For `col IN (v1, v2, v3)` inside an AND chain (top-level, scalar mode):

```
BEGIN_SC(2)    ; create success label at index 2
<v1>           ; push v1
<col>          ; push col
EQ             ; v1 == col?
OR_SC(2)       ; if true, jump to success (label 2)
<v2>           ; push v2
<col>          ; push col
EQ             ; v2 == col?
OR_SC(2)       ; if true, jump to success
<v3>           ; push v3
<col>          ; push col
EQ             ; v3 == col?
AND_SC(0)      ; if false, jump to l_next_row (label 0) -- no match at all
END_SC(2)      ; bind success label here
```

For IN() lists with fewer than 3 elements, the serializer uses a simpler
unrolled form with direct `AND_SC(0)`.

For IN() lists in non-AND contexts (OR chains or nested expressions), the
serializer falls back to boolean OR chains (no short-circuit).

---

## 10. Compiled Function Signatures

### 10.1 Filter Function (Row-ID Mode)

```c
typedef int64_t (*CompiledFn)(
    int64_t *cols,             // Array of column data pointers
    int64_t  cols_count,       // Number of columns
    int64_t *varsize_indexes,  // Array of variable-size column auxiliary data pointers
    int64_t *vars,             // Array of bind variable values (8 bytes each)
    int64_t  vars_count,       // Number of bind variables
    int64_t *filtered_rows,    // Output: array of matching row indices
    int64_t  rows_count        // Total number of rows to filter
);
// Returns: number of matching rows written to filtered_rows
```

### 10.2 Count-Only Function

```c
typedef int64_t (*CompiledCountOnlyFn)(
    int64_t *cols,             // Array of column data pointers
    int64_t  cols_count,       // Number of columns
    int64_t *varsize_indexes,  // Array of variable-size column auxiliary data pointers
    int64_t *vars,             // Array of bind variable values (8 bytes each)
    int64_t  vars_count,       // Number of bind variables
    int64_t  rows_count        // Total number of rows to filter
);
// Returns: count of matching rows
```

The count-only variant has no `filtered_rows` parameter and no `rows_ptr`
register. Instead of storing row indices, the loop simply increments a counter.

### 10.3 Runtime Data Layout

**`cols` array:** An array of `int64_t` pointers. `cols[i]` points to the base
address of column `i`'s data for the current page frame. Column values are read
at `cols[column_idx] + input_index * element_size`.

**`varsize_indexes` array:** An array of `int64_t` pointers. For variable-size
columns (string, binary, varchar), `varsize_indexes[column_idx]` points to the
auxiliary (index) vector. For string and binary columns, this is an array of
64-bit offsets into the data vector. For varchar columns, this points to the
aux vector containing 16-byte headers per row.

**`vars` array:** Bind variable values written sequentially. See Section 10.4
for per-type sizes and addressing.

**`filtered_rows` array:** Output buffer where matching row indices (64-bit
integers) are written sequentially by the filter function.

### 10.4 Bind Variable Memory Layout

The bind variable memory is populated by `AsyncFilterUtils.writeBindVarFunction()`
which writes each bind variable value sequentially with **type-dependent sizes**.
The backend reads bind variables using `vars_ptr + 8 * index` as the byte offset
(`read_vars_mem()` in `x86.h:112`).

Per-type sizes written by the producer:

| Column Type | Bytes Written | Method |
|---|---|---|
| BOOLEAN, BYTE, GEOBYTE | 8 | `putLong(value)` — value widened to 64 bits |
| SHORT, GEOSHORT, CHAR | 8 | `putLong(value)` — value widened to 64 bits |
| INT, IPv4, GEOINT, SYMBOL | 8 | `putLong(value)` — value widened to 64 bits |
| FLOAT | 8 | `putFloat(value)` + `putFloat(NaN)` — 4 bytes value + 4 bytes padding |
| LONG, GEOLONG, DATE, TIMESTAMP | 8 | `putLong(value)` |
| DOUBLE | 8 | `putDouble(value)` |
| UUID | 16 | `putLong128(lo, hi)` — two consecutive 64-bit values |

The backend addresses each bind variable at `vars_ptr + 8 * index` regardless
of type (`read_vars_mem()` in `x86.h:112` and `aarch64.h:130`). For most types,
each entry is exactly 8 bytes and the `8 * index` stride is correct.

**UUID breaks this contract.** The current producer writes 16 bytes for UUID
(`putLong128` at `AsyncFilterUtils.java:213`). LONG128 is not a separate case
in the producer — unsupported bind-variable types (including bare LONG128) fall
through to an exception at `AsyncFilterUtils.java:216`. So this issue is
specific to UUID bind variables.

The backend still uses `8 * index` addressing for UUID. Concretely: if a UUID
bind variable is at index `i`, it occupies bytes `[8*i, 8*i+16)`. Any bind
variable at index `i+1` will be read from `8*(i+1) = 8*i+8`, which lands in the
middle of the UUID value.

This means **a UUID bind variable followed by any other bind variable produces
incorrect results.** A new backend must either:
- Replicate this limitation (UUID bind variables must be the last or only entry).
- Or adopt a different addressing scheme and coordinate with the producer.

The serializer does not enforce this ordering, but the type-compatibility rules
(Section 13.2) constrain UUID to its own predicate, which limits how often UUID
bind variables co-occur with non-UUID bind variables in the same filter.

---

## 11. Scalar Loop Structure

The scalar loop processes one row at a time:

```
preload_columns_and_constants()    // Hoist column address and constant loads

loop:
    clear value cache              // Reset per-row column value cache
    emit_code(IR)                  // Process all IR instructions for this row

    if value stack not empty:
        mask = pop()
        if mask == 0: goto l_next_row    // Predicate failed

l_store_row:                       // OR_SC true jumps land here
    output[output_index] = input_index   // Store matching row ID
    output_index++

l_next_row:                        // AND_SC false jumps land here
    input_index++
    if input_index < rows_count: goto loop

    return output_index            // Number of matching rows
```

### Caching Optimizations

Three caches reduce redundant loads inside the hot loop:

1. **ColumnAddressCache** (up to 8 columns): Hoists `cols[column_idx]` loads
   out of the loop. Only fixed-size columns are cached (not string/binary/varchar).

2. **ConstantCache** (up to 8 constants): Hoists constant loads (integers into
   GP registers, floats/doubles into XMM registers) out of the loop. On x86,
   float constants are distinguished by both value and type (f32 vs f64).

3. **ColumnValueCache** (up to 8 values): Within a single row iteration, caches
   column values that have already been loaded. This avoids redundant memory
   reads when the same column appears multiple times in the predicate. Cleared
   at the start of each row iteration.

---

## 12. AVX2 SIMD Loop Structure (x86-64 Only)

The AVX2 loop processes multiple rows per iteration using 256-bit YMM registers.

**Step size** (rows per iteration) depends on the element size:
- i8: 32 rows
- i16: 16 rows
- i32/f32: 8 rows
- i64/f64: 4 rows

The SIMD loop does **not** support short-circuit opcodes (`AND_SC`, `OR_SC`,
`BEGIN_SC`, `END_SC`). These are only emitted in scalar mode, which is enforced
by the serializer setting `EXEC_HINT_SCALAR` or `EXEC_HINT_MIXED_SIZE_TYPE`
when short-circuit is used.

### Filter Function (Row-ID Mode) SIMD Loop

For 4-wide (i64) operations on non-Zen1/2 CPUs, uses mask-compress optimization:
- Maintains a YMM register with current row IDs `[i, i+1, i+2, i+3]`.
- Uses `compress_register()` to compact matching row IDs.
- Writes compacted results with `vmovdqu`.
- Uses `popcnt` to count matches and advance the output pointer.

For other step sizes or on AMD Zen1/2:
- Converts the comparison mask to a bitmask via `vpmovmskb`/`vmovmskps`/etc.
- Tests for zero (no matches) and short-circuits the scatter.
- Uses an unrolled scatter loop to write individual matching row IDs.

### Count-Only Function SIMD Loop

Instead of scattering row IDs, the count-only variant accumulates match counts:

- For step=4 (i64): `vpsubq acc, acc, mask` (subtract -1 entries = add 1s).
- For step=8 (i32): `vpsubd acc, acc, mask`.
- For step=16 (i16): `vpmaddwd` to pack pairs, then `vpsubd`.
- For step=32 (i8): `popcnt` of bitmask.

After the SIMD loop, a horizontal reduction sums the accumulator lanes.

### Scalar Tail

Both SIMD variants fall through to a scalar tail loop for the remaining
`rows_count % step` rows that don't fill a complete SIMD batch.

---

## 13. Predicate Context and Type Validation

### 13.1 Predicate Boundaries

The serializer defines a "predicate" as a self-contained comparison or boolean
expression. Predicates are separated by top-level `AND` / `OR` operators. For
example, `a > 1 AND b = 2 OR c < 3` contains three predicates: `a > 1`,
`b = 2`, `c < 3`.

Within each predicate, the `PredicateContext` tracks:
- The `columnType` (set from the first column or bind variable encountered).
- A `localTypesObserver` recording all types seen in this predicate.
- Whether arithmetic operations are present (`hasArithmeticOperations`).
- Symbol table reference and column index (for symbol resolution).

### 13.2 Type Compatibility Rules

The serializer enforces strict type compatibility within predicates:

- Boolean columns can only appear in equality/inequality comparisons or as
  standalone predicates.
- Symbol columns only support `=`, `!=`, and `IN()`. The serializer rejects
  `<`, `>`, `<=`, `>=` on symbols (since comparison would need string ordering,
  not integer key comparison).
- GeoHash types (GEOBYTE, GEOSHORT, GEOINT, GEOLONG) form a compatible group
  but cannot be mixed with other types.
- IPv4, CHAR, TIMESTAMP, DATE columns are type-strict (cannot be mixed
  with other types in the same predicate).
- UUID/LONG128 columns are type-strict and only support `EQ` and `NE` (no
  ordered comparisons, no arithmetic). The backend's `convert()` function
  passes i128 values through without conversion (Section 5.1), and only
  `cmp_eq`/`cmp_ne` handle i128 (Section 4.9).
- Numeric types (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE) can be mixed and are
  implicitly converted.

### 13.3 Boolean Column Expansion

A standalone boolean column `bool_col` in a WHERE clause is automatically
expanded to `bool_col = true`:

```
IMM(I1_TYPE, 1)    ; push constant true
MEM(I1_TYPE, col)  ; push column value
EQ                 ; compare
```

The NOT case (`NOT bool_col`) is handled by the expression tree visitor
naturally producing `MEM` followed by `NOT`.

### 13.4 Symbol Constant Resolution

Symbol string constants in the WHERE clause are resolved to integer keys at
serialization time:

1. If the symbol value is found in the `StaticSymbolTable`: emit
   `IMM(I4_TYPE, key)` where `key` is the integer symbol table key.
2. If the symbol value is not found: create a `CompiledFilterSymbolBindVariable`
   wrapper and emit `VAR(I4_TYPE, index)`. At execution time, the bind variable
   resolves the symbol string to a key using the runtime symbol table.

---

## 14. Supported Architectures

| Architecture | Scalar | SIMD | Notes |
|---|---|---|---|
| x86-64 | Yes | AVX2 | AMD Zen1/Zen1+/Zen2 (family_id=23) uses alternate SIMD path (no mask-compress) |
| AArch64 | Yes | No | Full feature parity with x86 scalar path |

Architecture support is checked by `JitUtil.isJitSupported()`:

```java
public static boolean isJitSupported() {
    return Os.arch == Os.ARCH_X86_64 || Os.arch == Os.ARCH_AARCH64;
}
```

---

## 15. Backend Implementor Checklist

A new backend must implement the following:

1. **IR stream iteration:** Read `instruction_t` structs from a contiguous
   buffer. Stop on `Ret` or `Inv`.

2. **Operand stack:** Maintain a stack of typed values with `data_kind_t`
   metadata (Section 4.8). Handle push (leaf instructions) and pop (operator
   instructions). Track `kMemory`/`kConst`/`kFlagsEq`/`kFlagsNe` kinds.

3. **Column reads (`Mem`):** Load data from `cols[column_idx]` at the current
   row offset. Handle:
   - Fixed-size types: direct indexed load with sign-extension for i8/i16.
   - i128: 16-byte unaligned load (Section 4.9).
   - String headers: offset-difference algorithm (Section 4.6), 4-byte header.
   - Binary headers: same algorithm, 8-byte header (Section 4.6).
   - Varchar headers: load 8 bytes from aux vector at `row * 16` (Section 4.7).

4. **Bind variable reads (`Var`):** Load from `vars_ptr + 8 * index` with
   type-appropriate size (Section 10.4). Note the UUID caveat.

5. **Type conversions:** Implement the conversion matrix from Section 5.1.
   Handle NULL-aware conversions when `null_check` is set (Section 5.2).

6. **Comparison operations:** Integer comparisons use standard signed compare,
   but with null checks enabled, must implement the strict/non-strict NULL
   distinction: `<`/`>` return false when any operand is NULL, while `<=`/`>=`
   return true when **both** are NULL (Section 6.3). Float/double comparisons
   use epsilon-based equality (Section 7) and must handle both-NaN as true for
   `<=`/`>=` (not false as IEEE 754 mandates). i128 comparisons use byte-level
   parallel compare (Section 4.9).

7. **Arithmetic operations:** With `null_check`, preserve NULL sentinels
   (Section 6). Integer division must return NULL on zero divisor. Float
   division should return NaN on zero divisor to match QuestDB's non-JIT
   evaluator; relying on IEEE 754 `±Infinity` diverges from the expected
   behavior (Section 4.10).

8. **Short-circuit opcodes:** Implement label management (create, bind) and
   conditional jumps (AND_SC, OR_SC). Pre-create labels at indices 0 and 1.
   Handle `kFlagsEq`/`kFlagsNe` stack entries by using direct conditional
   branches instead of materializing booleans (Section 4.8). When multiple
   `IN()` expressions reuse the same label index, each `AND_SC`/`OR_SC` must
   jump to the nearest following `END_SC` with the same label, not the last
   one globally (Section 4.5).

9. **Loop structure:** Iterate `input_index` from 0 to `rows_count - 1`. For
   each row, evaluate the IR. On match, store `input_index` in
   `filtered_rows[output_index++]` (or increment counter for count-only).
   Return the output count.

10. **Preloading (optional but recommended):** Pre-scan the IR stream before the
    loop to hoist column address loads and constant materializations out of the
    hot loop. Cache column values within each row iteration (Section 11).

11. **Integration:** Wire the backend into `compiler.cpp` by providing
    `Function` and `CountOnlyFunction` structs (Section 17.2). See Section 17
    for dispatch, code generation library options, and alignment guarantees.

---

## 16. IR Examples

### Simple Equality: `WHERE x = 42`

The traversal visits rhs (`42`) first, then lhs (`x`):

```
IMM(I4_TYPE, 42)     ; push constant 42 (rhs visited first)
MEM(I4_TYPE, 0)      ; push column 0 value (lhs visited second)
EQ                   ; compare, push 1 or 0
RET                  ; end
```

### AND with Short-Circuit: `WHERE x = 1 AND y > 10`

```
IMM(I4_TYPE, 1)      ; push 1 (rhs of '=')
MEM(I4_TYPE, 0)      ; push x (lhs of '=')
EQ                   ; x == 1?
AND_SC(0)            ; if false, jump to l_next_row
IMM(I4_TYPE, 10)     ; push 10 (rhs of '>')
MEM(I4_TYPE, 1)      ; push y (lhs of '>')
GT                   ; y > 10?
RET
```

### OR with Short-Circuit: `WHERE x = 1 OR y = 2`

```
IMM(I4_TYPE, 1)      ; push 1 (rhs of '=')
MEM(I4_TYPE, 0)      ; push x (lhs of '=')
EQ                   ; x == 1?
OR_SC(1)             ; if true, jump to l_store_row
IMM(I4_TYPE, 2)      ; push 2 (rhs of '=')
MEM(I4_TYPE, 1)      ; push y (lhs of '=')
EQ                   ; y == 2?
RET
```

### IN() List: `WHERE x IN (1, 2, 3)` (AND chain context)

```
BEGIN_SC(2)           ; create success label at index 2
IMM(I4_TYPE, 1)      ; push 1
MEM(I4_TYPE, 0)      ; push x
EQ                   ; x == 1?
OR_SC(2)             ; if true, jump to success label
IMM(I4_TYPE, 2)      ; push 2
MEM(I4_TYPE, 0)      ; push x
EQ                   ; x == 2?
OR_SC(2)             ; if true, jump to success label
IMM(I4_TYPE, 3)      ; push 3
MEM(I4_TYPE, 0)      ; push x
EQ                   ; x == 3?
AND_SC(0)            ; if false, jump to l_next_row (no match)
END_SC(2)            ; bind success label here
```

### Mixed Types: `WHERE int_col + 1.5 > 0.0`

```
IMM(F8_TYPE, 0.0)    ; push 0.0 (double)
IMM(F8_TYPE, 1.5)    ; push 1.5 (double)
MEM(I4_TYPE, 0)      ; push int_col
ADD                  ; int_col + 1.5 (int_col converted to double by backend)
GT                   ; (int_col + 1.5) > 0.0?
RET
```

### NULL Check on String Column: `WHERE str_col = null`

```
IMM(I4_TYPE, -1)     ; push NULL_LEN sentinel as i32
MEM(STRING_HEADER_TYPE, 0)  ; push string header (computed length)
EQ                   ; length == NULL_LEN?
RET
```

### Boolean Column: `WHERE bool_col`

Expanded to `bool_col = true`:

```
IMM(I1_TYPE, 1)      ; push true
MEM(I1_TYPE, 0)      ; push bool_col
EQ                   ; bool_col == true?
RET
```

---

## 17. Integrating a New Backend

### 17.1 Where Dispatch Happens

Backend dispatch occurs in `compiler.cpp` via the `Function` and
`CountOnlyFunction` structs, selected at compile time by `#ifdef __aarch64__`.
There is no runtime dispatch between architectures — a single binary contains
either the x86 or AArch64 backend.

Within the x86 backend, runtime dispatch between scalar and AVX2 occurs in
`Function::compile()`:

```cpp
void compile(const instruction_t *istream, size_t size, uint32_t options) {
    // ...
    if (exec_hint == single_size && features.has_avx2()) {
        avx2_loop(istream, size, step, null_check, unroll_factor);
    } else {
        scalar_loop(istream, size, null_check, unroll_factor);
    }
}
```

### 17.2 Required Entry Points

A new backend must provide two struct types, each with these methods:

```cpp
struct Function {
    void begin_fn();     // Set up function signature, allocate argument registers
    void compile(const instruction_t *istream, size_t size, uint32_t options);
    void end_fn();       // Finalize function
};

struct CountOnlyFunction {
    void begin_fn();     // Same but with 6-parameter signature (no rows_ptr)
    void compile(const instruction_t *istream, size_t size, uint32_t options);
    void end_fn();
};
```

`begin_fn()` must set up the function signature matching `CompiledFn` (7 params)
or `CompiledCountOnlyFn` (6 params) from Section 10. `compile()` must process
the IR stream and generate the filtering loop. `end_fn()` finalizes code
generation.

### 17.3 Code Generation Library

The existing backends use [asmjit](https://asmjit.com), which is bundled in the
QuestDB source tree. A new backend is not required to use asmjit — it could use
any code generation approach (LLVM, Cranelift, hand-assembled machine code, etc.)
as long as it produces a callable function pointer with the correct signature.

The integration point is in `compiler.cpp`'s JNI functions: `compileFunction()`
and `compileCountOnlyFunction()` create an asmjit `CodeHolder` and `Compiler`,
pass them to the `Function`/`CountOnlyFunction` struct, then call
`rt.add(&fn, &code)` to get an executable function pointer. A different code
generator would replace this pipeline while keeping the JNI interface unchanged.

### 17.4 The IR is Pre-Sorted

The backend receives the IR stream with predicates already sorted by the
serializer. When short-circuit opcodes are present, the serializer has already
reordered predicates by priority (Section 9.1). The backend does not need to
perform any reordering — it processes instructions sequentially.

### 17.5 Memory Alignment

Column data pointers in the `cols` array point to memory-mapped file regions.
QuestDB's storage layer provides the following alignment guarantees:

- 64-bit types (LONG, DOUBLE, TIMESTAMP, DATE): 8-byte aligned.
- 32-bit types (INT, FLOAT, SYMBOL, IPv4): 4-byte aligned.
- 16-bit types (SHORT, CHAR): 2-byte aligned.
- 8-bit types (BYTE, BOOLEAN): byte-aligned.
- 128-bit types (UUID): loaded with unaligned instructions (`movdqu` on x86,
  not `movdqa`). The backend should not assume 16-byte alignment.

For SIMD backends, column data is naturally aligned to element boundaries (each
row's data is at `base + row_index * element_size`), but the base address itself
may not be aligned to the SIMD register width (256-bit for AVX2). The existing
AVX2 backend uses unaligned loads (`vmovdqu`/`vmovups`).

---

## 18. Testing

### 18.1 IR Serialization Tests

`CompiledFilterIRSerializerTest` (in `core/src/test/java/.../jit/`) tests the
Java serializer in isolation. It verifies that SQL expressions produce the
expected IR instruction sequences, represented as string dumps like:

```
"(i64 -1L)(i64 42L)(i64 along)(/)(<>)(ret)"
```

These tests cover operator mapping, type inference, constant backfilling, symbol
resolution, and IN() list serialization. They do not execute the generated IR.

### 18.2 End-to-End Regression Tests

`CompiledFilterRegressionTest` (in `core/src/test/java/.../griffin/`) tests the
full pipeline: SQL parsing, IR serialization, JIT compilation, and execution.
Each test runs a SQL query with the JIT-compiled filter and compares the result
against the Java-interpreted filter to ensure correctness.

These tests use `N_SIMD = 512` rows to exercise the AVX2 SIMD path, plus
additional rows to cover the scalar tail. They test arithmetic operators,
boolean combinations, NULL handling, mixed types, and multi-column filters.

### 18.3 Java Backend Unit Tests

`VectorCompiledFilterTest` (in `core/src/test/java/.../jit/`) tests the
`VectorFilterInterpreter` in isolation, verifying that the Java backend correctly
decodes and evaluates IR instruction streams for basic comparisons, arithmetic,
and count-only mode.

`VectorCompiledFilterIntegrationTest` (in `core/src/test/java/.../griffin/`)
tests the Java backend through the full SQL pipeline with actual table data.

### 18.4 Verifying a New Backend

To verify a new backend:

1. Run `CompiledFilterRegressionTest` — this compares JIT output against the
   non-JIT Java evaluator for a wide range of filter expressions.
2. Pay special attention to edge cases:
   - **NULL comparisons:** both strict (`<`, `>` → false) and non-strict
     (`<=`, `>=` → true when both NULL) operators. See Section 6.3.
   - **Division by zero:** integer division returns NULL sentinel; float
     division should return NaN (not `±Infinity`). See Section 4.10.
   - **Mixed-type NULL coercion:** `INT_NULL` widened to `LONG_NULL` for i32→i64
     arithmetic, and to `NaN` for i32/i64→f32/f64 comparisons. See Section 5.2.
   - **Short-circuit label scoping:** chained `IN()` lists reuse label indices;
     each jump must resolve to its nearest `END_SC`. See Section 4.5.
   - **Epsilon-based float equality** and **short-circuit evaluation**.
3. Set the debug bit (bit 0) in options to log generated assembly for manual
   inspection.
