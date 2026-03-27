# QuestDB JIT IR Specification

This is the complete IR specification for building a backend that consumes
QuestDB's compiled WHERE-clause filter IR. It is ISA-agnostic and
language-agnostic.

## 1. Instruction Format

Every instruction is exactly **24 bytes**:

```
Offset  Size   Field
------  ----   -----
 0       4     opcode       (int32)
 4       4     options      (int32: type code for leaf instructions, 0 otherwise)
 8       8     payload.lo   (int64, or double via union alias)
16       8     payload.hi   (int64: upper 64 bits for i128; 0 for other types)
```

The instruction count = `buffer_size / 24`.

For floating-point immediates (`IMM` with `F4_TYPE` or `F8_TYPE`), the 8
bytes at offset 8 hold a C `double` value. The byte layout is identical
to `int64` — the difference is how the producer writes it.

## 2. Opcodes

| Value | Name       | Arity | Description |
|-------|------------|-------|-------------|
| -1    | `Inv`      | -     | Invalid / stub placeholder |
| 0     | `Ret`      | 0     | Terminates instruction stream |
| 1     | `Imm`      | 0     | Push immediate constant |
| 2     | `Mem`      | 0     | Push column value (memory read) |
| 3     | `Var`      | 0     | Push bind variable value |
| 4     | `Neg`      | 1     | Arithmetic negation (`-a`) |
| 5     | `Not`      | 1     | Logical NOT (XOR with 1) |
| 6     | `And`      | 2     | Bitwise AND of two booleans |
| 7     | `Or`       | 2     | Bitwise OR of two booleans |
| 8     | `Eq`       | 2     | Equality (`a == b`) |
| 9     | `Ne`       | 2     | Inequality (`a != b`) |
| 10    | `Lt`       | 2     | Less than (`a < b`) |
| 11    | `Le`       | 2     | Less or equal (`a <= b`) |
| 12    | `Gt`       | 2     | Greater than (`a > b`) |
| 13    | `Ge`       | 2     | Greater or equal (`a >= b`) |
| 14    | `Add`      | 2     | Addition |
| 15    | `Sub`      | 2     | Subtraction |
| 16    | `Mul`      | 2     | Multiplication |
| 17    | `Div`      | 2     | Division |
| 18    | `And_Sc`   | 1     | Short-circuit AND: pop, if false jump to label[payload.lo] |
| 19    | `Or_Sc`    | 1     | Short-circuit OR: pop, if true jump to label[payload.lo] |
| 20    | `Begin_Sc` | 0     | Create label at index payload.lo |
| 21    | `End_Sc`   | 0     | Bind label at index payload.lo |

### Evaluation model

Stack-based (RPN). Leaf instructions push. Unary ops pop 1, push 1.
Binary ops pop 2, push 1. Short-circuit ops pop 1, push 0.

After processing all instructions:
- Stack non-empty: pop final value. 0 = reject row, non-zero = accept.
- Stack empty (all resolved via short-circuit jumps): row acceptance was
  already handled by the jump targets.

Stop on `Ret` or `Inv`.

## 3. Type System

### 3.1 IR type codes

| Value | Name              | Size    | Description |
|-------|-------------------|---------|-------------|
| 0     | `i8`              | 1 byte  | Signed 8-bit integer |
| 1     | `i16`             | 2 bytes | Signed 16-bit integer |
| 2     | `i32`             | 4 bytes | Signed 32-bit integer |
| 3     | `f32`             | 4 bytes | 32-bit IEEE 754 float |
| 4     | `i64`             | 8 bytes | Signed 64-bit integer |
| 5     | `f64`             | 8 bytes | 64-bit IEEE 754 double |
| 6     | `i128`            | 16 bytes| 128-bit integer (UUID) |
| 7     | `string_header`   | 8 bytes | String column NULL check only |
| 8     | `binary_header`   | 8 bytes | Binary column NULL check only |
| 9     | `varchar_header`  | 8 bytes | Varchar column NULL check only |

Variable-size types (7, 8, 9) can **only** appear in NULL checks (EQ/NE
with the NULL sentinel). No other operations are valid.

### 3.2 Type size (log2)

| Type       | log2(size) | Byte size |
|------------|------------|-----------|
| i8         | 0          | 1         |
| i16        | 1          | 2         |
| i32, f32   | 2          | 4         |
| i64, f64   | 3          | 8         |
| i128       | 4          | 16        |

## 4. Instruction Details

### 4.1 IMM (Immediate Constant)

| Field       | Content |
|-------------|---------|
| opcode      | 1 |
| options     | Type code (0-9) |
| payload.lo  | Integer value (sign-extended i64), or double via union |
| payload.hi  | Upper 64 bits for i128; 0 otherwise |

For `f32`/`f64`: payload.lo holds the value as a C double (8 bytes).
For `f32`, the backend narrows to float at code-gen time.

For `i128`: lower 64 bits in payload.lo, upper 64 bits in payload.hi.

### 4.2 MEM (Column Reference)

| Field       | Content |
|-------------|---------|
| opcode      | 2 |
| options     | Type code (0-9) |
| payload.lo  | Column index (0-based) |
| payload.hi  | 0 |

Backend behavior:
- Fixed-size types: load from `cols[col_idx] + row_index * element_size`.
  Sign-extend i8 and i16 to 32 bits.
- i128: 16-byte unaligned load.
- string_header: see Section 8 (variable-size access).
- binary_header: same algorithm, 8-byte length header.
- varchar_header: see Section 9.

### 4.3 VAR (Bind Variable)

| Field       | Content |
|-------------|---------|
| opcode      | 3 |
| options     | Type code |
| payload.lo  | Bind variable index (0-based) |
| payload.hi  | 0 |

Read from the bind-variable memory blob. See Section 12 for layout.

### 4.4 Operators

No meaningful options or payload (both zero), except short-circuit opcodes.

| Opcode | Stack effect | Notes |
|--------|-------------|-------|
| Neg    | pop 1, push 1 | Preserves NULL (if null_check enabled) |
| Not    | pop 1, push 1 | XOR with 1; operates on 32-bit value |
| And    | pop 2, push 1 | Bitwise AND of 32-bit booleans |
| Or     | pop 2, push 1 | Bitwise OR of 32-bit booleans |
| Eq..Ge | pop 2, push 1 | Result: 1 or 0 |
| Add..Div | pop 2, push 1 | Arithmetic |

### 4.5 Operand order

The serializer visits **right child first**, then **left child**, then
operator. For `a OP b`:
- Stack after visiting: `[b (bottom), a (top)]`
- Backend pops: `lhs = pop()` gets `a`, `rhs = pop()` gets `b`
- Computes: `lhs OP rhs` = `a OP b`

**This matters for non-commutative operations.** Getting it wrong reverses
SUB, DIV, LT, GT, LE, GE.

## 5. Type Conversions

### 5.1 Implicit conversion matrix

When a binary operation has operands of different types, widen the narrower:

| Left         | Right        | Left becomes | Right becomes |
|--------------|--------------|--------------|---------------|
| i8/i16/i32   | i8/i16/i32   | (none)       | (none)        |
| i8/i16/i32   | i64          | i64          | (none)        |
| i8/i16/i32   | f32          | f32          | (none)        |
| i8/i16/i32   | f64          | f64          | (none)        |
| i64          | i8/i16/i32   | (none)       | i64           |
| i64          | i64          | (none)       | (none)        |
| i64          | f32          | **f64**      | **f64**       |
| i64          | f64          | f64          | (none)        |
| f32          | i8/i16/i32   | (none)       | f32           |
| f32          | i64          | **f64**      | **f64**       |
| f32          | f32          | (none)       | (none)        |
| f32          | f64          | f64          | (none)        |
| f64          | i8/i16/i32   | (none)       | f64           |
| f64          | i64          | (none)       | f64           |
| f64          | f32          | (none)       | f64           |
| f64          | f64          | (none)       | (none)        |
| i128         | i128         | (none)       | (none)        |

**Critical:** `i64 + f32` promotes **both** to `f64`. This avoids precision
loss from converting i64 to f32.

### 5.2 NULL-aware conversions

When `null_check` is enabled, conversions must preserve NULL sentinels:

- `i32` INT_NULL (0x80000000) -> `i64`: must become LONG_NULL
  (0x8000000000000000), NOT sign-extended `-2147483648L`.
- `i32` INT_NULL -> `f32`/`f64`: must become NaN.
- `i64` LONG_NULL -> `f64`: must become NaN.
- `i8` and `i16`: skip NULL conversion checks (GeoHash sentinel semantics).

**Why this matters:** Without NULL-aware i32->i64 conversion, `null_i32 +
i64_value` widens INT_NULL to a valid long, bypassing the i64 null check
in addition, producing garbage instead of LONG_NULL.

## 6. NULL Handling

### 6.1 NULL sentinel values

| IR Type  | NULL value | Notes |
|----------|-----------|-------|
| i8       | GeoHash-specific | Not generic INT_NULL |
| i16      | GeoHash-specific | Not generic INT_NULL |
| i32      | 0x80000000 (INT_NULL) | Also used for IPv4, SYMBOL |
| i64      | 0x8000000000000000 (LONG_NULL) | Also DATE, TIMESTAMP |
| f32      | NaN (0x7fc00000) | IEEE 754 quiet NaN |
| f64      | NaN (0x7ff8000000000000) | IEEE 754 quiet NaN |
| i128     | (LONG_NULL, LONG_NULL) | Both halves |
| string   | length header = -1 (4-byte int) | |
| binary   | length header = -1 (8-byte long) | |
| varchar  | header word = 0x00000004 | NULL flag bit only |

BOOLEAN, BYTE, SHORT, CHAR are not nullable in the JIT context.

### 6.2 NULL in arithmetic

When `null_check` enabled:
- **Integer negation:** if operand is NULL sentinel, result is the same
  sentinel (not negated).
- **Integer +, -, *, /:** if either operand is NULL, result is NULL.
- **Integer /:** if divisor is 0 (even if not NULL), result is NULL.
- **Float +, -, *:** NaN propagation is automatic via IEEE 754.
- **Float /:** must return NaN on zero divisor (NOT +/-Infinity).

### 6.3 NULL in comparisons

When `null_check` enabled:

**Integer types:**

| Operator | Both NULL | One NULL |
|----------|-----------|----------|
| EQ (=)   | true      | false    |
| NE (<>)  | false     | true     |
| LT (<)   | false     | false    |
| GT (>)   | false     | false    |
| LE (<=)  | **true**  | false    |
| GE (>=)  | **true**  | false    |

The key: `<=` and `>=` return **true** when both operands are the same
NULL sentinel. This makes `column <= null` and `column >= null` behave
as IS NULL checks.

**Float/double types:**

Same table applies. IEEE 754 says `NaN <= NaN` is false, but QuestDB
requires **true**. A backend must override IEEE 754 for `<=`/`>=` when
both operands are NaN.

## 7. Float Comparison Semantics

Epsilon-based, not exact bit equality:

```
DOUBLE_EPSILON = 0.0000000001
FLOAT_EPSILON  = 0.0000000001
```

- `EQ`: `|a - b| <= epsilon`
- `NE`: `|a - b| > epsilon`
- `GT`: not-equal-within-epsilon AND a > b strictly
- `GE`: equal-within-epsilon OR a >= b strictly
- `LT`: not-equal-within-epsilon AND a < b strictly
- `LE`: equal-within-epsilon OR a <= b strictly

This ensures values "equal within epsilon" compare as equal across all
comparison operators.

## 8. Variable-Size Column Access: String and Binary

Two-vector layout:
- **Aux vector:** N+1 entries, each 8-byte offset into data vector.
- **Data vector:** `[length_header][payload_bytes]` per row.
  Header: 4 bytes (STRING), 8 bytes (BINARY).

Col-top detection: if `cols[col_idx] == 0`, the data column is absent.
Return -1 (NULL sentinel). This check uses the data column address because
string/binary NULL detection reads the actual data header.

Access algorithm:

```
data_base = cols[col_idx]
if data_base == 0:
    return -1                       // col-top: treat as NULL

aux_base    = varsize_aux_ptr[col_idx]
offset      = aux_base[row]         // 8-byte read
next_offset = aux_base[row + 1]     // 8-byte read
length      = next_offset - offset - header_size

if length != 0:
    return length       // non-empty, non-NULL

// Ambiguous: empty string (header=0) or NULL (header=-1)
header    = read header_size bytes from (data_base + offset)
return header           // 0 = empty, -1 = NULL
```

Result type: `i32` for string (4-byte header), `i64` for binary (8-byte).
A subsequent EQ with the NULL sentinel (-1) determines NULL.

## 9. Variable-Size Column Access: Varchar

Aux entries are 16 bytes each. The layout differs by encoding:

**Fully inlined (size ≤ 9 bytes):**
```
Offset  Size  Content
 0       1    (size << 4) | flags
 1       9    Inline UTF-8 data (zero-padded)
10       6    48-bit data vector offset (little-endian)
```

**Non-inlined (size > 9 bytes):**
```
Offset  Size  Content
 0       4    (size << 4) | flags
 4       6    Inlined UTF-8 prefix
10       6    48-bit data vector offset (little-endian)
```

**NULL:**
```
Offset  Size  Content
 0       4    VARCHAR_HEADER_FLAG_NULL (= 4)
 4       6    Zero padding
10       6    48-bit data vector offset (little-endian)
```

Header flag bits (lowest 4 bits of first byte/int):
- Bit 0: INLINED (value fully in aux entry)
- Bit 1: ASCII-only
- Bit 2: NULL flag
- Bits [31:4]: length (for non-inlined 4-byte header)

JIT access:
```
aux_base = varsize_aux_ptr[col_idx]
if aux_base == 0:
    return 4                        // col-top: treat as NULL
header = load_i64(aux_base + row * 16)
push header as i64
```

**IMPORTANT: Col-top detection for varchar must check `aux_base`, NOT
`cols[col_idx]`.** The data column address (`cols[col_idx]`) can be 0 when
all varchar values are fully inlined (≤ 9 bytes) — this is a normal
condition, not a col-top. Unlike string/binary columns which read from the
data column, varchar NULL detection reads only from the aux column.

NULL detection: compare full 64-bit value against `4` (only NULL flag set,
zero length, zero prefix). Non-NULL entries always have additional bits
(at minimum the INLINED flag or a non-zero length).

## 10. Short-Circuit Evaluation

### Labels

Labels 0 and 1 are pre-created:
- 0 = `next_row`: skip to next row (AND_SC false target)
- 1 = `store_row` / `inc_count`: accept row (OR_SC true target)
- 2-7 = user-defined via BEGIN_SC/END_SC (max 8 labels)

### AND_SC(label)

Pop one boolean. If false (zero), jump to label. If true, fall through.

### OR_SC(label)

Pop one boolean. If true (non-zero), jump to label. If true, fall through.

### BEGIN_SC(label_idx)

Create a new forward label at the given index.

### END_SC(label_idx)

Bind (define position of) the label at the given index. Prior forward jumps
to this index now resolve here.

### Label scoping for chained IN()

Multiple IN() expressions in the same AND chain reuse the same label index
(typically 2). Each pair of BEGIN_SC/END_SC scopes independently:

```
BEGIN_SC(2)    ; first IN group
...OR_SC(2)... ; must jump to THIS group's END_SC(2)
AND_SC(0)
END_SC(2)      ; binds for first group

BEGIN_SC(2)    ; second IN group (reuses index 2)
...OR_SC(2)... ; must jump to THIS group's END_SC(2)
AND_SC(0)
END_SC(2)      ; binds for second group
```

**Implementation requirement:** Pre-compute per-instruction jump targets.
A naive per-label target array fails because the second END_SC(2)
overwrites the first.

### Flag-based optimization

When EQ/NE immediately precedes AND_SC/OR_SC on integer types, the backend
can skip materializing a boolean result. Instead, emit only a compare and
tag the stack entry:

| Stack tag   | AND_SC branches on | OR_SC branches on |
|-------------|-------------------|-------------------|
| kFlagsEq    | not-equal (skip)  | equal (accept)    |
| kFlagsNe    | equal (skip)      | not-equal (accept)|

### Operand stack metadata (data_kind_t)

Each stack value carries a tag:
- `kMemory`: from memory load or operation involving memory values (default)
- `kConst`: compile-time constant (both operands must be kConst)
- `kFlagsEq`: compare emitted for equality; branch on flags directly
- `kFlagsNe`: compare emitted for inequality; branch on flags directly

## 11. i128 (UUID) Comparisons

Only EQ and NE are supported. No ordered comparisons, no arithmetic.

Implementation: byte-parallel compare of all 16 bytes. On x86 this is
PCMPEQB + PMOVMSKB + CMP against 0xFFFF. On other ISAs, use the
equivalent SIMD byte-compare.

When flag-based optimization is active (i128 compare + AND_SC/OR_SC),
emit only the compare and push kFlagsEq/kFlagsNe.

## 12. Bind Variable Memory Layout

Values are written sequentially with type-dependent sizes:

| Column Type            | Bytes | Notes |
|------------------------|-------|-------|
| BOOLEAN, BYTE, GEO*   | 8     | Value widened to 64 bits |
| SHORT, CHAR            | 8     | Value widened to 64 bits |
| INT, IPv4, SYMBOL      | 8     | Value widened to 64 bits |
| FLOAT                  | 8     | 4 bytes value + 4 bytes NaN padding |
| LONG, DATE, TIMESTAMP  | 8     | Native 64-bit |
| DOUBLE                 | 8     | Native 64-bit |
| UUID                   | 16    | Two consecutive 64-bit values (lo, hi) |

**UUID breaks the uniform 8-byte stride.** Native backends use `vars_ptr +
8 * index` which produces wrong offsets when UUID is followed by another
variable. Use computed byte offsets instead of fixed stride.

## 13. Compilation Options Word

32-bit integer returned by the serializer:

```
Bit 0     : debug flag (1 = log generated assembly)
Bits 1-3  : log2 of max column type size (0=1B, 1=2B, 2=4B, 3=8B, 4=16B)
Bits 4-5  : execution hint
              0 = scalar
              1 = single-size (SIMD-eligible)
              2 = mixed-size (forces scalar)
Bit 6     : null_check enabled
Bits 7-31 : reserved (zero)
```

Parsing:
```
type_size  = (options >> 1) & 7
exec_hint  = (options >> 4) & 3
null_check = (options >> 6) & 1
```

SIMD step size (for single-size): `register_width / (element_bytes * 8)`.
For 256-bit (AVX2): step=32 (i8), 16 (i16), 8 (i32/f32), 4 (i64/f64).

The `forceScalar` hint is set when predicates contain arithmetic on
byte/short columns (max type size <= 2 bytes), because SIMD would use
narrow overflow semantics while non-JIT upcasts to int.

## 14. Division Edge Cases

### Integer division

Without null_check:
```
if divisor == 0: return INT_NULL (or LONG_NULL)
return dividend / divisor  (signed)
```

With null_check (i32):
```
result = INT_NULL                              // assume null
if (divisor & 0x7FFFFFFF) == 0: return result  // catches 0 AND INT_NULL
if dividend == INT_NULL: return result
result = dividend / divisor                    // signed division
```

The `& 0x7FFFFFFF` trick catches both zero (0x00000000) and INT_NULL
(0x80000000) in one check — both have all non-sign bits clear.

For i64 with null_check: clear bit 63, test if zero. Catches both 0 and
LONG_NULL (0x8000000000000000).

### Float division

QuestDB's non-JIT evaluator returns NaN for `x / 0.0`, not +/-Infinity.
A backend should explicitly check for zero divisor and produce NaN.

## 15. Loop Structure

### Scalar loop

```
preload columns and constants     // hoist out of loop

for input_index in 0..rows_count-1:
    clear per-row value cache
    evaluate all IR instructions

    if stack not empty:
        result = pop()
        if result == 0: goto next_row

    store_row:                    // OR_SC true target
        output[output_count++] = input_index

    next_row:                     // AND_SC false target
        continue

return output_count
```

### Caching (optional but recommended)

1. **Column address cache** (up to 8): hoist `cols[col_idx]` out of loop.
   Fixed-size columns only.
2. **Constant cache** (up to 8): hoist constant loads. Float constants
   distinguished by value AND type (f32 vs f64).
3. **Column value cache** (up to 8): within one row, cache column values
   to avoid redundant memory reads. Cleared each row.

### SIMD loop (when applicable)

Process `step` rows per iteration using vector registers. Does NOT support
short-circuit opcodes. Falls through to a scalar tail for remaining
`rows_count % step` rows.

Row-ID mode: compress matching row indices and write to output buffer.
Count-only mode: accumulate match counts in vector accumulators, then
horizontal-reduce.

## 16. Memory Alignment

- 64-bit types: 8-byte aligned
- 32-bit types: 4-byte aligned
- 16-bit types: 2-byte aligned
- 8-bit types: byte-aligned
- 128-bit types: NOT guaranteed 16-byte aligned; use unaligned loads
- SIMD base addresses: NOT guaranteed aligned to SIMD width; use
  unaligned vector loads

## 17. Predicate Context and Validation

### Type compatibility rules

- Boolean: only EQ/NE or standalone predicate.
- Symbol: only EQ, NE, IN(). No ordered comparisons.
- GeoHash types: compatible group, cannot mix with other types.
- IPv4, CHAR, TIMESTAMP, DATE: type-strict (no mixing).
- UUID/LONG128: type-strict, only EQ and NE.
- Numeric (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE): freely mixable with
  implicit conversion.

### Boolean column expansion

Standalone `bool_col` in WHERE expands to:
```
IMM(i8, 1)    ; true
MEM(i8, col)  ; bool_col
EQ             ; bool_col == true?
```

### Symbol constant resolution

Symbol strings are resolved to integer keys at serialization time.
If not in the static symbol table, a bind variable is created for
runtime resolution. The IR always works with integer keys, never strings.

## 18. IR Examples

### Simple equality: `WHERE x = 42`
```
IMM(i32, 42)     ; rhs visited first
MEM(i32, 0)      ; lhs visited second
EQ                ; compare
RET
```

### AND with short-circuit: `WHERE x = 1 AND y > 10`
```
IMM(i32, 1)
MEM(i32, 0)       ; x
EQ                 ; x == 1?
AND_SC(0)          ; if false -> next_row
IMM(i32, 10)
MEM(i32, 1)       ; y
GT                 ; y > 10?
RET
```

### IN() list: `WHERE x IN (1, 2, 3)`
```
BEGIN_SC(2)        ; create success label
IMM(i32, 1)
MEM(i32, 0)
EQ                 ; x == 1?
OR_SC(2)           ; if true -> success
IMM(i32, 2)
MEM(i32, 0)
EQ                 ; x == 2?
OR_SC(2)           ; if true -> success
IMM(i32, 3)
MEM(i32, 0)
EQ                 ; x == 3?
AND_SC(0)          ; if false -> next_row (no match)
END_SC(2)          ; bind success label here
```

### Mixed types: `WHERE int_col + 1.5 > 0.0`
```
IMM(f64, 0.0)
IMM(f64, 1.5)
MEM(i32, 0)        ; int_col (backend converts to f64)
ADD                 ; int_col + 1.5
GT                  ; > 0.0?
RET
```

### NULL check on string column: `WHERE str_col = null`
```
IMM(i32, -1)                ; NULL_LEN sentinel
MEM(string_header, 0)       ; computed length
EQ                          ; length == -1?
RET
```

### NULL check on varchar column: `WHERE vc_col = null`
```
IMM(i64, 4)                 ; VARCHAR_HEADER_FLAG_NULL
MEM(varchar_header, 0)      ; 8-byte header
EQ                          ; header == 4?
RET
```