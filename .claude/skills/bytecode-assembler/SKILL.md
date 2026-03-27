---
name: bytecode-assembler
description: >
  Work with QuestDB's BytecodeAssembler — the runtime JVM bytecode generator
  that creates anonymous classes for column-specialized data processing. Use
  when adding new column types, writing new bytecode-generated factories,
  modifying RecordSink/RecordComparator/RecordToRowCopier/GroupByFunctionsUpdater
  generation, or debugging bytecode issues. Covers the assembler API, the
  4-phase generation pattern, constant pool setup, stack discipline,
  the 8KB method size limit, and all existing factory consumers.
allowed-tools: Read, Grep, Glob, Bash, Edit, Write, Agent
---

# QuestDB BytecodeAssembler Guide

## What it is

`BytecodeAssembler` (`core/src/main/java/io/questdb/std/BytecodeAssembler.java`)
generates JVM `.class` files at runtime by writing raw bytecode into a
`ByteBuffer`. The generated classes are loaded via
`Unsafe.defineAnonymousClass()` — they are not registered in the system
class loader and cannot be referenced by name.

**Why it exists:** eliminates per-row virtual dispatch. Instead of a loop
with a type switch at runtime, the generated class contains straight-line
bytecode specialized to the exact column types. The JIT compiler inlines
everything.

Consult `references/api-patterns.md` for the complete assembler API,
constant pool encoding, stack discipline rules, and the 4-phase generation
pattern with a worked example.

Consult `references/patterns.md` for 6 validated bytecode patterns:
1. Straight-line (no branches) — invokeInterface with varying slot widths
2. Instance fields + getfield/putfield — caching values across methods
3. Branches + stack map tables — ifne, setJmp, StackMapTable with append_frame
4. Multi-method (private helpers) — chunked dispatch via invokeVirtual
5. Pool constants + arithmetic + type conversions — ldc2_w, i2l, lmul
6. Loops (backward branches) — goto_ backward, if_icmpge forward, iinc,
   2-slot locals (long), StackMapTable with 2 frames (append_frame + same_frame)

Each pattern is tested in `core/src/test/java/io/questdb/test/std/BytecodeAssemblerHandRolledTest.java`.

## Existing factories (consumers)

| Factory | Generates | Interface | Key method |
|---------|-----------|-----------|------------|
| `RecordSinkFactory` | `RecordSink` | `copy(Record, RecordSinkSPI)` | Per-column getter+putter, 3-tier size management |
| `RecordValueSinkFactory` | `RecordValueSink` | `copy(Record, MapValue)` | Per-column getter+putter for map aggregation |
| `RecordComparatorCompiler` | `RecordComparator` | `setLeft(Record)`, `compare(Record)` | Caches left fields in instance fields |
| `RecordToRowCopierUtils` | `RecordToRowCopier` | `copy(SqlExecutionContext, Record, Row)` | Per-column with type conversions |
| `GroupByFunctionsUpdaterFactory` | `GroupByFunctionsUpdater` | `updateNew()`, `updateExisting()`, `updateEmpty()`, `merge()` | Unrolled function calls via fields `f0..fn` |
| `DateFormatCompiler` / `MicrosFormatCompiler` / `NanosFormatCompiler` | `DateFormat` | `format(...)` | Compiled date pattern |

Source locations:
- `core/src/main/java/io/questdb/cairo/RecordSinkFactory.java`
- `core/src/main/java/io/questdb/cairo/map/RecordValueSinkFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/orderby/RecordComparatorCompiler.java`
- `core/src/main/java/io/questdb/griffin/RecordToRowCopierUtils.java`
- `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByFunctionsUpdaterFactory.java`

## The 4-phase generation pattern

Every factory follows the same structure:

### Phase 1: Constant pool
```java
asm.init(HostInterface.class);
asm.setupPool();
int thisClass = asm.poolClass(asm.poolUtf8("io/questdb/generated/name"));
int ifaceClass = asm.poolClass(TargetInterface.class);
int rGetInt = asm.poolInterfaceMethod(Record.class, "getInt", "(I)I");
int wPutInt = asm.poolInterfaceMethod(MapValue.class, "putInt", "(II)V");
// ... pool all methods, fields, strings needed
asm.finishPool();
```

### Phase 2: Class structure
```java
asm.defineClass(thisClass);          // or defineClass(thisClass, superClass)
asm.interfaceCount(1);
asm.putShort(ifaceClass);
asm.fieldCount(N);                   // declare instance fields if needed
for (...) asm.defineField(nameIdx, typeIdx);
asm.methodCount(M);
asm.defineDefaultConstructor();      // always needed
```

### Phase 3: Method body — column-driven emission
```java
asm.startMethod(nameIdx, sigIdx, maxStack, maxLocals);
for (int i = 0; i < columnCount; i++) {
    asm.aload(2);                    // target (MapValue, RecordSinkSPI, etc.)
    asm.iconst(i);                   // target slot
    asm.aload(1);                    // source (Record)
    asm.iconst(columnIndex);         // source column index
    switch (columnType) {
        case INT:
            asm.invokeInterface(rGetInt, 1);   // Record.getInt(col) -> int
            asm.invokeInterface(wPutInt, 2);   // MapValue.putInt(slot, val)
            break;
        case LONG:
            asm.invokeInterface(rGetLong, 1);  // -> long (2 stack slots)
            asm.invokeInterface(wPutLong, 3);  // argCount=3: slot + long(2)
            break;
        // ... all column types
    }
}
asm.return_();
asm.endMethodCode();
asm.putShort(0);                     // exception table count
asm.putShort(0);                     // attribute count
asm.endMethod();
asm.putShort(0);                     // class attribute count
```

### Phase 4: Instantiate
```java
return asm.newInstance();            // defineAnonymousClass + newInstance
// or: Class<T> cls = asm.loadClass();  // for 2-phase create
```

## Critical rules

### Stack discipline

JVM bytecode is stack-based. Every instruction has a defined stack effect.
You must track the stack depth manually.

- `int`, `float`, `boolean`, `byte`, `short`, `char`, object ref: **1 slot**
- `long`, `double`: **2 slots**
- `invokeInterface(index, argCount)`: argCount is the number of **stack
  slots** consumed by arguments (NOT the number of parameters). A method
  taking `(int, long)` has argCount=3 (1 + 2). The receiver (`this`) is
  counted separately by the assembler (+1 internally).
- `maxStack` in `startMethod()` must be >= the maximum stack depth reached.
  Getting this wrong causes a `VerifyError` at class load time.

### invokeInterface argCount

The `argCount` parameter to `asm.invokeInterface(methodIndex, argCount)`
is the sum of argument slot widths, **not** the Java parameter count:

| Java signature | argCount |
|----------------|----------|
| `(I)I` — getInt(int) | 1 |
| `(I)J` — getLong(int) | 1 |
| `(II)V` — putInt(int, int) | 2 |
| `(IJ)V` — putLong(int, long) | 3 |
| `(ID)V` — putDouble(int, double) | 3 |
| `(ILRecord;I)V` — putDecimal128(int, Record, int) | 3 |

### Constant pool encoding

Method signatures use JVM internal format:
- `I` = int, `J` = long, `D` = double, `F` = float
- `B` = byte, `S` = short, `C` = char, `Z` = boolean
- `V` = void (return type only)
- `Lpackage/Class;` = object reference (use `/` not `.`)
- `(params)return` — e.g., `(IJ)V` = void method(int, long)

Class names in the pool use `/` separators: `io/questdb/cairo/sql/Record`.
The `poolClass(Class<?>)` method handles this automatically.

### 8KB method size limit (HugeMethodLimit)

JVM's C2 compiler refuses to inline methods larger than ~8000 bytes of
bytecode. Generated methods that exceed this threshold run as interpreted
bytecode — a massive performance regression.

**Budget:** ~14 bytes per column for simple getter+putter pairs.
`CHUNK_TARGET_SIZE = 6000` bytes per chunk in RecordSinkFactory.

**Three-tier strategy** (RecordSinkFactory / RecordToRowCopierUtils):
1. **Single method** — if total bytecode < limit, emit one method.
2. **Chunked** — split columns into groups, emit a private method per
   chunk, have the main method call each chunk sequentially. Check
   `asm.getMethodCodeSize()` after `endMethodCode()` to verify.
3. **Looping fallback** — if even chunking fails (estimation was wrong),
   return null and use `LoopingRecordSink` / `LoopingRecordToRowCopier`
   which use runtime type dispatch.

### Adding a new column type

When adding a new column type, update ALL factories that switch on
`ColumnType.tagOf()`:

1. `RecordValueSinkFactory` — add getter+putter case
2. `RecordSinkFactory` — add case in `emitColumnCopy()` and update
   `estimateBytecodeSize()` for the new type
3. `RecordToRowCopierUtils` — add case, possibly with type conversions
4. `RecordComparatorCompiler` — add field type, setLeft cache, compare
5. `GroupByFunctionsUpdaterFactory` — usually no change (delegates to
   GroupByFunction implementations)

Also update the corresponding `Record` interface with the getter, and the
target interfaces (`RecordSinkSPI`, `MapValue`, `TableWriter.Row`) with
the putter.

### RecordComparator is the most complex

Unlike the other factories, `RecordComparatorCompiler` generates:
- **Instance fields** for caching the left record's values
- **Constructor** that initializes Decimal128/256 field instances
- **`setLeft(Record)`** that reads columns into cached fields
- **`compare(Record)`** that loads cached fields vs. right record getters
- **`setRankMaps(ObjList)`** for symbol ranking
- **Stack map tables** for branch targets (required by JVM verifier)

If you need to add a column type here, you must add field declarations,
setLeft caching code, AND comparison code, all keeping the stack map
tables consistent.

### Debugging

`asm.dump("/tmp/generated.class")` writes the class file to disk. Then:
```bash
javap -c -p /tmp/generated.class
```

This disassembles the bytecode and is invaluable for debugging VerifyErrors.

## Tests

- `RecordSinkFactoryTest` (not in repo name — test coverage is via
  integration tests that exercise the generated sinks)
- `RecordValueSinkFactoryTest` — `core/src/test/java/.../cairo/map/RecordValueSinkFactoryTest.java`
- `RecordToRowCopierUtilsTest` — `core/src/test/java/.../griffin/RecordToRowCopierUtilsTest.java`
- `GroupByFunctionsUpdaterFactoryTest` — `core/src/test/java/.../griffin/engine/groupby/GroupByFunctionsUpdaterFactoryTest.java`
- `RecordSinkFactoryTest` — `core/src/test/java/.../cairo/RecordSinkFactoryTest.java`