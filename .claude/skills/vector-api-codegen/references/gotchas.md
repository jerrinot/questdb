# Vector API Gotchas for Bytecode Generation

Critical pitfalls discovered during pattern testing. Each caused a test
failure before being understood.

## 1. mask.cast() requires same lane count

`VectorMask<Integer>.cast(LongVector.SPECIES_PREFERRED)` throws
`IllegalArgumentException` because int has 2× the lanes of long.

**Rule:** `mask.cast(targetSpecies)` only works when `sourceSpecies.length()
== targetSpecies.length()`. This means:

| Cast | Works? | Reason |
|------|--------|--------|
| int mask → float mask | Yes | Both 4 bytes, same lanes |
| long mask → double mask | Yes | Both 8 bytes, same lanes |
| int mask → long mask | **No** | int has 2× lanes |
| byte mask → int mask | **No** | byte has 4× lanes |

**Workaround for different-width types:** Don't cast the mask. Instead,
detect the condition after conversion in the target type:

```java
// DON'T: intMask.cast(LONG_SPECIES) — fails
// DO: detect in long land after conversion
LongVector lv = (LongVector) iv.convertShape(I2L, LONG_SPECIES, 0);
VectorMask<Long> isNull = lv.compare(EQ, signExtendedSentinel);
```

## 2. INT_NULL sign-extends to wrong value

`(long) Numbers.INT_NULL` = `0xFFFFFFFF_80000000`, NOT `Numbers.LONG_NULL`
(`0x80000000_00000000`). After `convertShape(I2L)`, the null sentinel
is the sign-extended value, not LONG_NULL.

**Fix:** Detect the sign-extended sentinel and blend the correct one:

```java
long signExtendedIntNull = (long) Numbers.INT_NULL;
VectorMask<Long> isNull = lv.compare(EQ, signExtendedIntNull);
lv = lv.blend(Numbers.LONG_NULL, isNull);
```

This applies to all int→long null coercion paths, including int→double
(via int→long→double or int→float→double).

## 3. intoMemorySegment writes a full vector

`vector.intoMemorySegment(seg, offset, order)` always writes
`SPECIES.length() * elementBytes` regardless of how many lanes are
"meaningful" (e.g., after compress).

**Impact:** The output row-ID buffer must be sized to
`(maxRows + SPECIES.length()) * Long.BYTES` to avoid out-of-bounds on
the last compress+store. The extra slots are garbage and are ignored
because `filteredCount` tracks the actual count.

## 4. convertShape with part index

When converting between types with different lane counts (int → long),
a single source vector produces multiple destination vectors:

```java
// IntVector: 8 lanes, LongVector: 4 lanes
LongVector part0 = (LongVector) iv.convertShape(I2L, LONG_SPECIES, 0); // lanes 0-3
LongVector part1 = (LongVector) iv.convertShape(I2L, LONG_SPECIES, 1); // lanes 4-7
```

Each part processes `LONG_SPECIES.length()` source elements. The number
of parts = `INT_SPECIES.length() / LONG_SPECIES.length()`.

**For the filter loop:** When the loop iterates at LONG_SPECIES stride
(the narrowest species), each iteration loads only `LONG_SPECIES.length()`
ints. No part splitting needed — just use part 0.

## 5. ByteVector / ShortVector have many lanes

On AVX2 (256-bit): ByteVector has 32 lanes, ShortVector has 16. On
AVX-512: 64 and 32 respectively. Test data must be at least
`SPECIES.length()` elements or loads will fail.

**For mixed-type filters:** If comparing a byte column against a long
column, the byte vector has 8× the lanes. The simplest approach: iterate
at long stride and load only `LONG_SPECIES.length()` bytes per iteration
(using a smaller byte species or scalar loads + broadcast).

## 6. Masked compare semantics

`v.compare(op, scalar, mask)` returns a mask that is **always false**
for lanes where the input mask is false. This is NOT the same as
`v.compare(op, scalar).and(mask)` when the data in inactive lanes
would cause issues (e.g., comparing uninitialized memory).

Always prefer the masked variant for safety.

## 7. Module system

The test module must declare `requires jdk.incubator.vector;` in its
`module-info.java`. The main module already has this. The `--add-modules
jdk.incubator.vector` flag is also needed at compile and runtime.

## 8. Species method is instance, not static

`SPECIES.indexInRange(offset, limit)` is an instance method on
`VectorSpecies`, not a static method. The species object must be loaded
first (from a static field) before calling indexInRange.

Similarly, `SPECIES.maskAll(true)` creates an all-true mask and
`SPECIES.maskAll(false)` creates an all-false mask.