# BytecodeAssembler API and Patterns Reference

## Assembler API

### Lifecycle

| Method | Purpose |
|--------|---------|
| `init(Class<?> host)` | Reset assembler for a new class. `host` provides the class loader context for `defineAnonymousClass`. |
| `setupPool()` | Write CAFEBABE magic, version, placeholder pool count. Pools `Object.<init>` and `Code` attribute. |
| `finishPool()` | Write the final pool count at the reserved offset. |
| `loadClass()` | Extract byte array, call `Unsafe.defineAnonymousClass(host, bytes)`. Returns `Class<T>`. |
| `newInstance()` | `loadClass()` + `getDeclaredConstructor().newInstance()`. Returns the instance. |
| `dump(String path)` | Write class bytes to a file for `javap` inspection. |

### Constant pool methods

| Method | Pool tag | Returns |
|--------|----------|---------|
| `poolUtf8(CharSequence)` | 1 (CONSTANT_Utf8) | pool index |
| `poolClass(Class<?>)` | 7 (CONSTANT_Class) — auto-creates Utf8 entry with `/` separators | pool index |
| `poolClass(int utf8Index)` | 7 (CONSTANT_Class) | pool index |
| `poolMethod(int classIdx, CharSequence name, CharSequence sig)` | 10 (CONSTANT_Methodref) | pool index |
| `poolMethod(Class<?>, CharSequence name, CharSequence sig)` | auto-pools class | pool index |
| `poolInterfaceMethod(Class<?>, String name, String sig)` | 11 (CONSTANT_InterfaceMethodref) | pool index |
| `poolInterfaceMethod(int classIdx, String name, String sig)` | 11 | pool index |
| `poolField(int classIdx, int nameAndTypeIdx)` | 9 (CONSTANT_Fieldref) | pool index |
| `poolNameAndType(int nameIdx, int typeIdx)` | 12 (CONSTANT_NameAndType) | pool index |
| `poolStringConst(int utf8Idx)` | 8 (CONSTANT_String) | pool index |
| `poolIntConst(int value)` | 3 (CONSTANT_Integer) | void (increments count) |
| `poolLongConst(long value)` | 5 (CONSTANT_Long) | pool index (occupies 2 slots) |
| `poolDoubleConst(double value)` | 6 (CONSTANT_Double) | pool index (occupies 2 slots) |

**Note:** `poolLongConst` and `poolDoubleConst` consume 2 pool slots (JVM
spec requirement). `poolCount += 2` internally.

### Class structure methods

| Method | Purpose |
|--------|---------|
| `defineClass(int thisClassIdx)` | Emit access flags (PUBLIC), this class, super class (Object). |
| `defineClass(int thisClassIdx, int superClassIdx)` | Same with explicit superclass. |
| `interfaceCount(int n)` | Emit interface count. Follow with `n` calls to `putShort(ifaceIdx)`. |
| `fieldCount(int n)` | Emit field count. |
| `defineField(int nameIdx, int typeIdx)` | Emit PRIVATE field. |
| `methodCount(int n)` | Emit method count. |
| `defineDefaultConstructor()` | Emit `<init>()V` calling `super.<init>()`. |
| `defineDefaultConstructor(int superMethodIdx)` | Same with explicit super method. |

### Method body methods

| Method | Purpose |
|--------|---------|
| `startMethod(nameIdx, descIdx, maxStack, maxLocals)` | Start PUBLIC method. |
| `startPrivateMethod(nameIdx, descIdx, maxStack, maxLocals)` | Start PRIVATE method (for chunks). |
| `endMethodCode()` | Finalize code length. Must be called before exception/attribute counts. |
| `endMethod()` | Finalize method attribute length. |
| `getMethodCodeSize()` | Returns bytecode size of current method (call after `endMethodCode()`). |
| `getCodeStart()` | Returns the byte offset where code begins. |

### Bytecode instructions

#### Loads and stores
| Method | Opcode | Stack effect | Notes |
|--------|--------|-------------|-------|
| `aload(n)` | aload_0..3 or aload n | -> objectref | Optimized for 0-3 |
| `iload(n)` | iload_0..3 or iload n | -> int | Optimized for 0-3 |
| `lload(n)` | lload_0..3 or lload n | -> long (2 slots) | Optimized for 0-3 |
| `fload(n)` | fload n | -> float | |
| `dload(n)` | dload n | -> double (2 slots) | |
| `istore(n)` | istore_0..3 or istore n | int -> | Optimized for 0-3 |
| `lstore(n)` | lstore_0..3 or lstore n | long -> | Optimized for 0-3 |
| `fstore(n)` | fstore n | float -> | |
| `dstore(n)` | dstore n | double -> | |

#### Constants
| Method | Stack effect | Notes |
|--------|-------------|-------|
| `iconst(v)` | -> int | Optimized: iconst_m1..5, bipush, sipush. Range: Short.MIN_VALUE..Short.MAX_VALUE |
| `lconst_0()` | -> long (2 slots) | Push 0L |
| `lconst_1()` | -> long (2 slots) | Push 1L |
| `fconst_0()` | -> float | Push 0.0f |
| `fconst_1()` | -> float | Push 1.0f |
| `fconst_2()` | -> float | Push 2.0f |
| `dconst_0()` | -> double (2 slots) | Push 0.0d |
| `dconst_1()` | -> double (2 slots) | Push 1.0d |
| `ldc(index)` | -> value | Load from constant pool (int, float, string). Auto-selects ldc vs ldc_w. |
| `ldc2_w(index)` | -> value (2 slots) | Load long or double from constant pool. |

For int constants outside `Short.MIN_VALUE..Short.MAX_VALUE`, use
`poolIntConst(v)` + `ldc(index)`. For long/double constants, use
`poolLongConst(v)` / `poolDoubleConst(v)` + `ldc2_w(index)`.

**Important:** All `pool*` methods must be called before `finishPool()`.
No pool methods can be called after `finishPool()` closes the constant
pool.

#### Invocations
| Method | Stack effect | Notes |
|--------|-------------|-------|
| `invokeInterface(idx, argCount)` | pops receiver + argCount slots, pushes return | argCount = sum of arg slot widths |
| `invokeVirtual(idx)` | pops receiver + args, pushes return | |
| `invokeStatic(idx)` | pops args, pushes return | |
| `invokespecial(idx)` | pops receiver + args, pushes return | For constructors and super calls |

#### Fields
| Method | Stack effect |
|--------|-------------|
| `getfield(idx)` | objectref -> value |
| `putfield(idx)` | objectref, value -> |
| `getStatic(idx)` | -> value |

#### Arithmetic
| Method | Stack | Notes |
|--------|-------|-------|
| `iadd()` | int, int -> int | |
| `isub()` | int, int -> int | |
| `imul()` | int, int -> int | |
| `idiv()` | int, int -> int | |
| `irem()` | int, int -> int | |
| `ineg()` | int -> int | |
| `ladd()` | long, long -> long | |
| `lsub()` | long, long -> long | |
| `lmul()` | long, long -> long | |
| `ldiv()` | long, long -> long | |
| `lneg()` | long -> long | |
| `fadd()` | float, float -> float | |
| `fsub()` | float, float -> float | |
| `fmul()` | float, float -> float | |
| `fdiv()` | float, float -> float | |
| `fneg()` | float -> float | |
| `dadd()` | double, double -> double | |
| `dsub()` | double, double -> double | |
| `dmul()` | double, double -> double | |
| `ddiv()` | double, double -> double | |
| `dneg()` | double -> double | |

#### Comparisons
| Method | Stack | Notes |
|--------|-------|-------|
| `lcmp()` | long, long -> int | -1, 0, or 1 |
| `fcmpg()` | float, float -> int | NaN -> 1 |
| `dcmpg()` | double, double -> int | NaN -> 1 |

#### Type conversions
| Method | Stack | Notes |
|--------|-------|-------|
| `i2l()` | int -> long | 1 slot becomes 2 |
| `i2f()` | int -> float | Same slot count |
| `i2d()` | int -> double | 1 slot becomes 2 |
| `i2b()` | int -> byte (as int) | Truncates |
| `i2s()` | int -> short (as int) | Truncates |
| `l2i()` | long -> int | 2 slots become 1 |
| `l2d()` | long -> double | Same slot count (2) |
| `l2f()` | long -> float | 2 slots become 1 |
| `f2d()` | float -> double | 1 slot becomes 2 |
| `f2i()` | float -> int | Same slot count |
| `f2l()` | float -> long | 1 slot becomes 2 |
| `d2f()` | double -> float | 2 slots become 1 |
| `d2i()` | double -> int | 2 slots become 1 |
| `d2l()` | double -> long | Same slot count (2) |

All arithmetic, comparison, and conversion instructions emit exactly 1
byte each.

#### Stack manipulation
| Method | Stack |
|--------|-------|
| `dup()` | v -> v, v |
| `dup2()` | v1, v2 -> v1, v2, v1, v2 |
| `dup_x2()` | ..., v3, v2, v1 -> ..., v1, v3, v2, v1 | Copies top value below 3rd (category 1 values) |
| `pop()` | v -> |

#### Control flow
| Method | Returns | Notes |
|--------|---------|-------|
| `goto_()` | branch position | Unconditional jump (forward or backward). Patch with `setJmp`. |
| `ifeq()` | branch position | Jump if top int == 0 |
| `ifne()` | branch position | Jump if top int != 0 |
| `iflt()` | branch position | Jump if top int < 0 |
| `ifle()` | branch position | Jump if top int <= 0 |
| `ifge()` | branch position | Jump if top int >= 0 |
| `ifgt()` | branch position | Jump if top int > 0 |
| `if_icmpeq()` | branch position | Jump if int1 == int2 (pops both) |
| `if_icmpne()` | branch position | Jump if int1 != int2 (pops both) |
| `if_icmplt()` | branch position | Jump if int1 < int2 (pops both) |
| `if_icmple()` | branch position | Jump if int1 <= int2 (pops both) |
| `if_icmpge()` | branch position | Jump if int1 >= int2 (pops both) |
| `if_icmpgt()` | branch position | Jump if int1 > int2 (pops both) |
| `setJmp(branch, target)` | void | Patch any jump (forward or backward): writes `target - branch + 1` at branch position. Negative offset = backward jump. |

**`if_icmp*` operand order:** Stack has `[..., val1, val2]`. The JVM pops
`val2` (top), then `val1`, and evaluates `val1 <cond> val2`. So
`if_icmpge` after pushing A then B tests `A >= B`.

#### Returns
| Method | Stack |
|--------|-------|
| `return_()` | (void return) |
| `ireturn()` | int -> (returns int) |
| `lreturn()` | long -> (returns long) |
| `freturn()` | float -> (returns float) |
| `dreturn()` | double -> (returns double) |

#### Other
| Method | Stack | Notes |
|--------|-------|-------|
| `new_(classIdx)` | -> objectref | Allocate object (not initialized) |
| `athrow()` | objectref -> | Throw exception |
| `iinc(idx, inc)` | (no stack effect) | Increment local variable |

### Stack map tables

Required by JVM verifier for methods with branches:

| Method | Purpose |
|--------|---------|
| `startStackMapTables(attrNameIdx, frameCount)` | Begin StackMapTable attribute |
| `same_frame(offset)` | Frame with same locals, empty stack |
| `append_frame(itemCount, offset)` | Frame appending local variables |
| `full_frame(offset)` | Full frame specification |
| `putITEM_Integer()` | Verification type: int |
| `putITEM_Long()` | Verification type: long |
| `putITEM_Object(classIdx)` | Verification type: object |
| `putITEM_Top()` | Verification type: top (second slot of long/double) |
| `endStackMapTables()` | Finalize attribute length |

**Offsets in stack map tables are relative to the previous frame** (or to
method start for the first frame), minus 1. Use `position() - codeStart`
to compute absolute positions, then convert to relative.

## JVM method descriptor syntax

```
(ParameterTypes)ReturnType

Type codes:
  B = byte      S = short     I = int       J = long
  F = float     D = double    C = char      Z = boolean
  V = void      Lclass/Name; = object ref   [T = array of T
```

Examples:
- `()V` — void method()
- `(I)I` — int method(int)
- `(IJ)V` — void method(int, long)
- `(Lio/questdb/cairo/sql/Record;Lio/questdb/cairo/map/MapValue;)V`
  — void method(Record, MapValue)

## Worked example: RecordValueSinkFactory

This is the simplest factory — generates a single `copy(Record, MapValue)`
method with no fields, no branches, no size management.

```java
// Phase 1: Pool
asm.init(RecordSink.class);
asm.setupPool();
int thisClass = asm.poolClass(asm.poolUtf8("io/questdb/cairo/valuesink"));
int ifaceClass = asm.poolClass(RecordValueSink.class);
int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
int wPutInt = asm.poolInterfaceMethod(MapValue.class, "putInt", "(II)V");
// ... more getter/putter pairs for each type
int copyName = asm.poolUtf8("copy");
int copySig = asm.poolUtf8(
    "(Lio/questdb/cairo/sql/Record;Lio/questdb/cairo/map/MapValue;)V");
asm.finishPool();

// Phase 2: Class
asm.defineClass(thisClass);        // extends Object
asm.interfaceCount(1);
asm.putShort(ifaceClass);          // implements RecordValueSink
asm.fieldCount(0);                 // no fields
asm.methodCount(2);                // <init> + copy
asm.defineDefaultConstructor();

// Phase 3: copy() method
// Method params: this=0, record=1, mapValue=2
asm.startMethod(copyName, copySig, /*maxStack=*/4, /*maxLocals=*/3);

for (int i = 0; i < columnCount; i++) {
    int colIdx = columnFilter.getColumnIndexFactored(i);
    asm.aload(2);                  // stack: [MapValue]
    asm.iconst(i);                 // stack: [MapValue, targetSlot]
    asm.aload(1);                  // stack: [MapValue, targetSlot, Record]
    asm.iconst(colIdx);            // stack: [MapValue, targetSlot, Record, colIdx]

    switch (ColumnType.tagOf(type)) {
        case ColumnType.INT:
            asm.invokeInterface(rGetInt, 1);  // Record.getInt(colIdx) -> int
            // stack: [MapValue, targetSlot, intValue]
            asm.invokeInterface(wPutInt, 2);  // MapValue.putInt(slot, val)
            // stack: []
            break;
        case ColumnType.LONG:
            asm.invokeInterface(rGetLong, 1); // -> long (2 slots)
            // stack: [MapValue, targetSlot, longValue(2)]
            asm.invokeInterface(wPutLong, 3); // argCount=3: int(1)+long(2)
            // stack: []
            break;
        // ... all types
    }
}

asm.return_();
asm.endMethodCode();
asm.putShort(0);                   // 0 exceptions
asm.putShort(0);                   // 0 method attributes
asm.endMethod();
asm.putShort(0);                   // 0 class attributes

// Phase 4
return asm.newInstance();
```

## maxStack calculation

`maxStack` must be >= the maximum operand stack depth at any point in the
method. Count manually:

For the RecordValueSink pattern:
- `aload(2)` -> depth 1
- `iconst(i)` -> depth 2
- `aload(1)` -> depth 3
- `iconst(colIdx)` -> depth 4 (maximum for int/float/bool/byte/short/char)
- `invokeInterface(getter, 1)` -> consumes receiver+1 arg (2 slots), pushes result
  - int result: depth = 4 - 2 + 1 = 3
  - long result: depth = 4 - 2 + 2 = 4 (long = 2 slots)
- `invokeInterface(putter, N)` -> consumes receiver+N arg slots, pushes void
  - putInt: consumes 1+2=3 slots: depth = 3 - 3 = 0
  - putLong: consumes 1+3=4 slots: depth = 4 - 4 = 0

So maxStack=4 covers all cases here.

For methods with `long`/`double` parameters or temporaries, the stack can
grow to 5 or 6. Count carefully — a wrong maxStack causes `VerifyError`.

## Common pitfalls

1. **Wrong argCount in invokeInterface.** This is the #1 source of
   VerifyErrors. Long and double arguments count as 2 slots.

2. **Forgetting endMethodCode() before exception/attribute counts.** The
   assembler computes code length from the method start to the current
   position when `endMethodCode()` is called.

3. **Pool index vs. pool slot for long/double constants.** `poolLongConst`
   and `poolDoubleConst` return the first of 2 consecutive pool slots.
   Use the returned index with `ldc2_w()`.

4. **Stack map tables.** Any method with branches (goto, if*) requires a
   StackMapTable attribute or the JVM verifier rejects it. The simple
   factories (RecordValueSinkFactory) avoid this by being branch-free.
   RecordComparatorCompiler handles this explicitly.

5. **Class name collisions.** Each generated class needs a unique pool
   name, but since `defineAnonymousClass` doesn't register names, this
   is mainly for debugging. Use descriptive names like
   `io/questdb/cairo/valuesink`.

6. **Exceeding Short.MAX_VALUE in iconst.** `iconst()` handles values up
   to Short.MAX_VALUE via bipush/sipush. For larger values, use
   `poolIntConst()` + `ldc()`.