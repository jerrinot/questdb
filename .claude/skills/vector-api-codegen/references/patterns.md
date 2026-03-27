# Vector API Patterns for Bytecode Generation

All patterns validated in `VectorApiPatternsTest.java` (38 tests).

## 1. Species selection

Always use `SPECIES_PREFERRED` — adapts to hardware (AVX2 → 256-bit,
AVX-512 → 512-bit, NEON → 128-bit). Declare as `static final`:

```java
static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
```

### Lane count relationships (same vector bit width)

| Type | Element bytes | Lanes (256-bit) | Lanes (512-bit) |
|------|--------------|-----------------|-----------------|
| byte | 1 | 32 | 64 |
| short | 2 | 16 | 32 |
| int | 4 | 8 | 16 |
| float | 4 | 8 | 16 |
| long | 8 | 4 | 8 |
| double | 8 | 4 | 8 |

**Same-width types have identical lane counts:** int == float, long == double.
This is critical — `mask.cast()` only works between species with the same
lane count.

**Wider types have fewer lanes:** byte = 2× short = 4× int = 8× long.

## 2. Loading from raw memory

Wrap a raw address into a `MemorySegment`, then load:

```java
MemorySegment seg = MemorySegment.ofAddress(dataAddress).reinterpret(byteSize);
LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder());
```

**Masked load** for tail handling (safe even when segment is short):

```java
VectorMask<Long> active = LONG_SPECIES.indexInRange(row, rowCount);
LongVector v = LongVector.fromMemorySegment(LONG_SPECIES, seg, row * Long.BYTES, ByteOrder.nativeOrder(), active);
```

Inactive lanes are set to zero. No separate remainder loop needed.

## 3. Comparisons → masks

```java
VectorMask<Long> mask = v.compare(VectorOperators.GT, 42L);           // scalar
VectorMask<Long> mask = v.compare(VectorOperators.GT, otherVector);   // vector
VectorMask<Long> mask = v.compare(VectorOperators.GT, 42L, active);   // masked
```

Supported operators: `EQ`, `NE`, `LT`, `LE`, `GT`, `GE`.

## 4. Mask boolean operations

```java
VectorMask<Long> combined = mask1.and(mask2);
VectorMask<Long> either   = mask1.or(mask2);
VectorMask<Long> inverted = mask1.not();
```

## 5. Count-only path

```java
count += match.and(active).trueCount();
```

`trueCount()` returns `int`.

## 6. Row-ID output — compress + store

The efficient pattern (replaces lane-by-lane extraction):

```java
// Pre-compute iota: [0, 1, 2, ..., SPECIES.length()-1]
long[] iotaArray = new long[LONG_SPECIES.length()];
for (int i = 0; i < iotaArray.length; i++) iotaArray[i] = i;
LongVector iota = LongVector.fromArray(LONG_SPECIES, iotaArray, 0);

// In the loop:
LongVector rowIds = iota.add(row);
LongVector compressed = rowIds.compress(match);
compressed.intoMemorySegment(out, filteredCount * Long.BYTES, ByteOrder.nativeOrder());
filteredCount += match.trueCount();
```

**Warning:** `intoMemorySegment` always writes a full vector width. The
output buffer must have room for `SPECIES.length()` extra longs past the
last expected match.

## 7. Type conversions via convertShape

```java
// int → long (different lane counts → uses part index)
LongVector lv = (LongVector) iv.convertShape(VectorOperators.I2L, LONG_SPECIES, 0);
// Part 0 = first LONG_SPECIES.length() ints, part 1 = next batch

// long → double (same lane count → single part)
DoubleVector dv = (DoubleVector) lv.convertShape(VectorOperators.L2D, DOUBLE_SPECIES, 0);

// int → float (same lane count → single part)
FloatVector fv = (FloatVector) iv.convertShape(VectorOperators.I2F, FLOAT_SPECIES, 0);

// int → double (different lane counts → uses part index)
DoubleVector dv = (DoubleVector) iv.convertShape(VectorOperators.I2D, DOUBLE_SPECIES, 0);
```

Available conversion operators: `I2L`, `I2F`, `I2D`, `L2D`, `L2F`,
`F2D`, `D2F`, `L2I`, `D2L`, etc.

## 8. Mask cast across same-lane-count types

```java
// Long mask → Double mask (both 8 bytes → same lane count)
VectorMask<Double> dm = longMask.cast(DOUBLE_SPECIES);

// Int mask → Float mask (both 4 bytes → same lane count)
VectorMask<Float> fm = intMask.cast(FLOAT_SPECIES);
```

**Only works when both species have the same lane count.** Cannot cast
an int mask (8 lanes) directly to a long mask (4 lanes).

## 9. Null sentinel detection

### Integer/Long null

```java
LongVector nullVec = LongVector.broadcast(LONG_SPECIES, Numbers.LONG_NULL);
VectorMask<Long> isNull = v.compare(VectorOperators.EQ, nullVec);
```

### Float/Double NaN

```java
VectorMask<Double> isNan = v.test(VectorOperators.IS_NAN);
```

### Null-aware comparison (QuestDB LE/GE semantics)

```java
VectorMask<Long> lhsNull = lhs.compare(VectorOperators.EQ, LONG_NULL_VEC);
VectorMask<Long> rhsNull = rhs.compare(VectorOperators.EQ, LONG_NULL_VEC);
VectorMask<Long> anyNull = lhsNull.or(rhsNull);
VectorMask<Long> bothNull = lhsNull.and(rhsNull);
// LE: (lhs <= rhs where neither null) OR (both null)
VectorMask<Long> result = lhs.compare(VectorOperators.LE, rhs, anyNull.not()).or(bothNull);
```

## 10. Epsilon float equality

```java
double epsilon = 1e-10;
VectorMask<Double> eq = v.sub(target).abs().compare(VectorOperators.LE, epsilon);
```

## 11. Arithmetic with null preservation

```java
VectorMask<Long> isNull = lhs.compare(VectorOperators.EQ, LONG_NULL_VEC)
        .or(rhs.compare(VectorOperators.EQ, LONG_NULL_VEC));
VectorMask<Long> valid = isNull.not();

LongVector result = lhs.div(rhs, valid);     // Only divide valid lanes
result = result.blend(LONG_NULL_VEC, isNull); // Restore nulls
```

`blend(scalar, mask)` replaces lanes where mask is true.

## 12. Null coercion during type widening

INT_NULL (0x80000000) sign-extends to 0xFFFFFFFF80000000, NOT LONG_NULL.

```java
LongVector lv = (LongVector) iv.convertShape(VectorOperators.I2L, LONG_SPECIES, 0);
long signExtendedIntNull = (long) Numbers.INT_NULL;
VectorMask<Long> isNull = lv.compare(VectorOperators.EQ, signExtendedIntNull);
lv = lv.blend(Numbers.LONG_NULL, isNull);
```

## 13. Bind variable broadcast

Load scalar from vars address, broadcast to vector:

```java
long value = Unsafe.getUnsafe().getLong(varsAddr + varOffset);
LongVector bcast = LongVector.broadcast(LONG_SPECIES, value);
```

## 14. Multi-column filter combining

Same iteration, different segments, combine masks via cast:

```java
LongVector lv = LongVector.fromMemorySegment(LONG_SPECIES, longSeg, ...);
DoubleVector dv = DoubleVector.fromMemorySegment(DOUBLE_SPECIES, dblSeg, ...);

VectorMask<Long> longMatch = lv.compare(VectorOperators.GT, 25L, active);
VectorMask<Double> dblMatch = dv.compare(VectorOperators.LT, 0.8, active.cast(DOUBLE_SPECIES));
VectorMask<Long> combined = longMatch.and(dblMatch.cast(LONG_SPECIES));
```

## Method signatures for bytecode emission

These are the methods the bytecode generator must call via
`invokestatic` or `invokevirtual`.

### Static factory methods (invokestatic)

| Method | Signature |
|--------|-----------|
| `LongVector.fromMemorySegment` | `(VectorSpecies, MemorySegment, long, ByteOrder) → LongVector` |
| `LongVector.fromMemorySegment` | `(VectorSpecies, MemorySegment, long, ByteOrder, VectorMask) → LongVector` |
| `LongVector.broadcast` | `(VectorSpecies, long) → LongVector` |
| `LongVector.fromArray` | `(VectorSpecies, long[], int) → LongVector` |
| `VectorSpecies.indexInRange` | `(long, long) → VectorMask` |

Same pattern for `IntVector`, `DoubleVector`, `FloatVector`, etc.

### Instance methods (invokevirtual)

| Method | Signature |
|--------|-----------|
| `vector.compare` | `(VectorOperators.Comparison, long) → VectorMask` |
| `vector.compare` | `(VectorOperators.Comparison, Vector) → VectorMask` |
| `vector.compare` | `(VectorOperators.Comparison, long, VectorMask) → VectorMask` |
| `vector.test` | `(VectorOperators.Test) → VectorMask` |
| `vector.add/sub/mul/neg` | `(Vector or scalar) → Vector` |
| `vector.div` | `(Vector, VectorMask) → Vector` |
| `vector.blend` | `(long, VectorMask) → Vector` |
| `vector.compress` | `(VectorMask) → Vector` |
| `vector.convertShape` | `(VectorOperators.Conversion, VectorSpecies, int) → Vector` |
| `vector.abs` | `() → Vector` |
| `vector.intoMemorySegment` | `(MemorySegment, long, ByteOrder) → void` |
| `mask.and/or/not` | `(VectorMask) → VectorMask` / `() → VectorMask` |
| `mask.trueCount` | `() → int` |
| `mask.cast` | `(VectorSpecies) → VectorMask` |
| `mask.laneIsSet` | `(int) → boolean` |