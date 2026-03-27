# Validated Bytecode Patterns

These patterns are tested and validated in
`core/src/test/java/io/questdb/test/std/BytecodeAssemblerHandRolledTest.java`.
Each assembly method demonstrates a specific bytecode generation pattern
used in QuestDB. Read the test file for the full runnable code.

## Pattern 1: Straight-Line (no branches)

**Used by:** RecordValueSinkFactory, simple RecordSinkFactory paths

**What it exercises:** invokeInterface with varying stack slot widths,
basic push/pop discipline.

**Structure:** For each column, push the target (MapValue), push the slot
index, push the source (Record), push the column index, call getter, call
putter.

**Stack discipline per column:**
```
aload(2)                    // [MapValue]                  depth=1
iconst(slot)                // [MapValue, slot]            depth=2
aload(1)                    // [MapValue, slot, Record]    depth=3
iconst(col)                 // [MapValue, slot, Record, col] depth=4
invokeInterface(getter, 1)  // pops 2, pushes result
                            // int:  [MapValue, slot, val]   depth=3
                            // long: [MapValue, slot, val(2)] depth=4
invokeInterface(putter, N)  // pops all                     depth=0
```

**invokeInterface argCount rules:**
- `putInt(int, int)`:    argCount=2 (1+1)
- `putLong(int, long)`:  argCount=3 (1+2)
- `putDouble(int, double)`: argCount=3 (1+2)
- `putFloat(int, float)`: argCount=2 (1+1)
- `putBool(int, boolean)`: argCount=2 (1+1)

**maxStack:** 4 is sufficient for all column types (the widest point is
after the getter returns a long/double: [MapValue, slot, 2-slot-value]).

**No stack map tables needed** because there are no branches.

**Critical rule: host class package.** The class name in the constant pool
must be in the same package as the host class passed to `asm.init()`. The
factory uses `asm.init(RecordSink.class)` (package `io.questdb.cairo`)
and class name `io/questdb/cairo/valuesink`. Using a different package
causes `defineHiddenClass` to return null silently.

## Pattern 2: Instance Fields + getfield/putfield

**Used by:** RecordComparatorCompiler (setLeft caches, compare reads)

**What it exercises:** field declarations, putfield to write fields,
getfield to read them, invokeStatic for utility methods.

**Declaring fields:**
```java
// In the constant pool:
int f0Name = asm.poolUtf8("f0");
int intType = asm.poolUtf8("I");
int f0Field = asm.poolField(thisClass, asm.poolNameAndType(f0Name, intType));

// In the class structure:
asm.fieldCount(1);
asm.defineField(f0Name, intType);   // PRIVATE int f0
```

**Writing a field (setLeft pattern):**
```
aload(0)                    // [this]
aload(1)                    // [this, record]
iconst(col)                 // [this, record, col]
invokeInterface(rGetInt, 1) // [this, intValue]
putfield(f0Field)           // []  — stores intValue into this.f0
```

**Reading a field (compare pattern):**
```
aload(0)                    // [this]
getfield(f0Field)           // [f0Value]
aload(1)                    // [f0Value, record]
iconst(col)                 // [f0Value, record, col]
invokeInterface(rGetInt, 1) // [f0Value, rightValue]
invokeStatic(integerCompare)// [result]
ireturn()                   // returns result
```

**Field type descriptors:**
- `I` = int, `J` = long, `D` = double, `F` = float, `Z` = boolean
- `Ljava/lang/Object;` = Object reference

**No stack map tables** when compare has no branches (single-column case
delegates to Integer.compare and returns directly).

## Pattern 3: Branches + Stack Map Tables

**Used by:** RecordComparatorCompiler for multi-column ORDER BY

**What it exercises:** ifne with forward jumps, setJmp to patch targets,
istore/iload for local variables, StackMapTable attribute with
append_frame.

**The multi-column comparison pattern:**
```
// Column 0 comparison
aload(0); getfield(f0)              // left value
aload(1); iconst(0); invokeInterface(rGetInt, 1)  // right value
invokeStatic(integerCompare)        // cmp result
istore(2)                           // int cmp = result

// Branch: if cmp != 0, skip to return
iload(2)
branch0 = ifne()                    // forward jump (target unknown)

// Column 1 comparison (only reached if col0 tied)
aload(0); getfield(f1)
aload(1); iconst(1); invokeInterface(rGetInt, 1)
invokeStatic(integerCompare)
istore(2)                           // overwrite cmp

// Return label — both paths converge here
returnLabel = asm.position()
iload(2)
ireturn()

// Patch the forward jump
asm.setJmp(branch0, returnLabel)
```

**Stack map table mechanics:**

The JVM verifier requires a StackMapTable for any method with branches.
At each branch target, the verifier must know the types of all local
variables and stack entries.

```java
asm.endMethodCode();
asm.putShort(0);                    // 0 exceptions

// 1 attribute: StackMapTable
asm.putShort(1);
asm.startStackMapTables(stackMapAttrIndex, 1); // 1 frame entry

// At returnLabel, local variable 2 (int cmp) was introduced since
// the method entry frame [this(RecordComparator), record(Record)].
// append_frame adds 1 new local relative to the initial frame.
asm.append_frame(1, returnLabel - asm.getCodeStart());
asm.putITEM_Integer();              // the appended local: int cmp

asm.endStackMapTables();
asm.endMethod();
```

**Rules for stack map table offsets:**
- For the first frame: offset = absolute byte position from code start.
- For subsequent frames: offset = delta from previous frame, minus 1.
- Use `position() - asm.getCodeStart()` to compute absolute positions.

**Frame types:**
- `same_frame(offset)`: same locals as previous frame, empty stack.
  Use when no new locals were introduced since the last frame.
- `append_frame(count, offset)`: adds `count` new locals. Follow with
  `count` calls to `putITEM_*()` to declare their types.
- `full_frame(offset)`: explicit list of all locals and stack entries.
  Use when the frame differs significantly from the previous one.

**Verification type items:**
- `putITEM_Integer()`: int, byte, short, char, boolean
- `putITEM_Long()`: long (followed by implicit Top for second slot)
- `putITEM_Object(classIndex)`: object reference
- `putITEM_Top()`: second slot of long/double, or unused slot

**The N-column generalization** (from RecordComparatorCompiler):
All N-1 intermediate comparisons use the same pattern: compare, istore(2),
iload(2), ifne. All branch targets point to the same return label. Only
one stack map frame is needed (at the return label), because all branches
converge there and the locals are identical at each target.

## Pattern 4: Multi-Method (Private Helpers)

**Used by:** RecordSinkFactory chunking, RecordToRowCopierUtils chunking

**What it exercises:** startPrivateMethod, poolMethod for own-class
methods (not poolInterfaceMethod), invokeVirtual to call private methods.

**Declaring own-class methods in the pool:**
```java
// Pool the method reference for this class's private method
int chunk0Method = asm.poolMethod(thisClass,
    asm.poolNameAndType(chunk0Name, copySig));
```

Note: use `poolMethod` (CONSTANT_Methodref), not `poolInterfaceMethod`
(CONSTANT_InterfaceMethodref). The generated class is a concrete class,
not an interface.

**Public dispatch method:**
```java
asm.startMethod(copyName, copySig, 3, 3);
// this.c0(record, value)
asm.aload(0);               // [this]
asm.aload(1);               // [this, record]
asm.aload(2);               // [this, record, value]
asm.invokeVirtual(chunk0Method);  // calls private c0()
// this.c1(record, value)
asm.aload(0);
asm.aload(1);
asm.aload(2);
asm.invokeVirtual(chunk1Method);
asm.return_();
```

**Private helper methods:**
```java
asm.startPrivateMethod(chunk0Name, copySig, 4, 3);
// ... emit column copy bytecode for this chunk ...
asm.return_();
asm.endMethodCode();
asm.putShort(0); // exceptions
asm.putShort(0); // attributes
asm.endMethod();
```

**methodCount must include all methods:** `<init>` + public copy +
N private chunks. Forgetting to count a method corrupts the class file.

**When to use this pattern:** When the estimated bytecode for all columns
exceeds `CHUNK_TARGET_SIZE` (6000 bytes). Split columns into groups of
~428 columns each (at ~14 bytes/column). The dispatch method itself is
tiny (3 loads + invokeVirtual per chunk).

## Pattern 5: Pool Constants + Arithmetic + Type Conversions

**Used by:** Custom computations, date format compilers

**What it exercises:** poolLongConst, ldc2_w, i2l widening, lmul.

**Placing a long constant in the pool:**
```java
// poolLongConst returns the pool index AND consumes 2 pool slots
int multiplierIndex = asm.poolLongConst(100_000L);
```

**Loading and using it:**
```
aload(1)                    // [Record]
iconst(0)                   // [Record, 0]
invokeInterface(rGetInt, 1) // [intVal]         depth=1
i2l()                       // [longVal(2)]     depth=2 (int widened to long)
ldc2_w(multiplierIndex)     // [longVal(2), 100000L(2)]  depth=4
lmul()                      // [result(2)]      depth=2
```

**maxStack calculation with 2-slot types:**
After i2l, the int (1 slot) becomes a long (2 slots). After ldc2_w,
another long (2 slots) is on the stack. The deepest point in the example
is [MapValue(1), slot(1), longVal(2), 100000L(2)] = 6 slots. Declaring
maxStack=4 causes a VerifyError — must be 6.

**Available conversions:**
| Method | Stack effect | Notes |
|--------|-------------|-------|
| i2l()  | int -> long | Widen: 1 slot becomes 2 |
| i2f()  | int -> float | Same slot count |
| i2d()  | int -> double | 1 slot becomes 2 |
| l2i()  | long -> int | Narrow: 2 slots become 1 |
| l2d()  | long -> double | Same slot count (2) |
| f2d()  | float -> double | 1 slot becomes 2 |
| d2f()  | double -> float | 2 slots become 1 |

**Pool constant methods and their load instructions:**
| Pool method | Pool slots | Load instruction |
|-------------|-----------|------------------|
| poolLongConst(long) | 2 | ldc2_w(index) |
| poolDoubleConst(double) | 2 | ldc2_w(index) |
| poolIntConst(int) | 1 | ldc(index) |
| poolStringConst(utf8Index) | 1 | ldc(index) |

## Pattern 6: Loops (Backward Branches + Loop Counter)

**Used by:** JIT filter loop (the per-row iteration), any generated
counting or accumulation logic.

**What it exercises:** lconst_0, lstore/lload (2-slot locals), istore/iload,
if_icmpge (forward exit branch), goto_ (backward branch to loop header),
iinc, i2l widening, ladd, StackMapTable with TWO frames.

**This is the foundational pattern for a JIT backend.** The filter loop
iterates over rows, evaluates a predicate, and stores matching row IDs.
This pattern proves all the bytecode mechanics needed for that loop.

**Generated bytecode layout:**
```
// locals: this=0, record=1, value=2, sum(long)=3-4, i(int)=5
// maxStack=4, maxLocals=6

  lconst_0           ; push 0L
  lstore 3           ; long sum = 0  (locals 3-4, 2 slots)
  iconst_0           ; push 0
  istore 5           ; int i = 0    (local 5)

LOOP:                ; [StackMap: append_frame +long +int]
  iload 5            ; push i
  iconst_5           ; push 5
  if_icmpge EXIT     ; if i >= 5, forward jump to EXIT

  lload 3            ; push sum (2 slots)          depth=2
  aload 1            ; push record                 depth=3
  iload 5            ; push i                      depth=4
  invokeinterface Record.getInt(I)I              ; depth=3
  i2l                ; int -> long                 depth=4
  ladd               ; sum + val -> new sum        depth=2
  lstore 3           ; store back                  depth=0

  iinc 5, 1          ; i++  (no stack effect)
  goto LOOP          ; BACKWARD jump to LOOP header

EXIT:                ; [StackMap: same_frame]
  aload 2            ; push MapValue
  iconst_0           ; push 0
  lload 3            ; push sum
  invokeinterface MapValue.putLong(IJ)V
  return
```

**Backward jumps with goto_ and setJmp:**
```java
int loopStart = asm.position();     // capture loop header position
// ... loop condition + body ...
int backJmp = asm.goto_();          // emit goto with placeholder offset
asm.setJmp(backJmp, loopStart);     // patch: loopStart < backJmp, so offset is negative
```

`setJmp(branch, target)` writes `target - branch + 1` at the branch
offset position. When target < branch (backward jump), this produces a
negative 16-bit value — correct for JVM branch instructions which use
signed offsets.

**Forward exit branch:**
```java
int exitBranch = asm.if_icmpge();   // emit conditional jump with placeholder
// ... loop body ...
int exitLabel = asm.position();     // after the backward goto
asm.setJmp(exitBranch, exitLabel);  // patch forward jump
```

**Two-slot local variables (long/double):**
`lstore(3)` stores a long in locals 3 AND 4 (long occupies 2 slots).
The next usable local index is 5. `maxLocals` must account for this:
`this(1) + record(1) + value(1) + sum(2) + i(1) = 6`.

**iinc for loop counter:**
`iinc(index, increment)` modifies a local variable in-place without
touching the stack. Emits 3 bytes: opcode + index + increment.

**StackMapTable with two frames:**

A loop creates TWO branch targets that need stack map frames:
1. The loop header (backward jump target from goto)
2. The exit point (forward jump target from if_icmpge)

```java
asm.startStackMapTables(stackMapAttr, 2); // 2 entries

// Frame 1: loop header — new locals since method entry
int loopOffset = loopStart - asm.getCodeStart();
asm.append_frame(2, loopOffset);    // 2 new locals
asm.putITEM_Long();                 // local 3-4: long sum
asm.putITEM_Integer();              // local 5: int i

// Frame 2: exit — same locals as frame 1, empty stack
int exitOffset = exitLabel - asm.getCodeStart();
asm.same_frame(exitOffset - loopOffset - 1);  // delta from previous

asm.endStackMapTables();
```

**Why append_frame at the loop header:**
The method entry frame has locals [this, Record, MapValue]. Before the
loop starts, we introduce `long sum` (2 slots) and `int i` (1 slot).
`append_frame(2, offset)` says "same as previous frame, plus 2 new
locals" — followed by their verification types.

**Why same_frame at exit:**
At the exit label, the locals are identical to the loop header frame.
`same_frame(delta)` says "same locals as previous frame, empty stack."

**Offset rules for multiple frames:**
- First frame: `offset_delta` = absolute byte offset from code start.
- Subsequent frames: `offset_delta = target_offset - previous_offset - 1`.
  The JVM reconstructs the absolute offset as `previous + delta + 1`.

**ladd() was added to BytecodeAssembler** (opcode 0x61) since it was
missing. It follows the same convention as `iadd()` — single byte via
`putByte()`.

## Common Mistakes (discovered during testing)

1. **Wrong maxStack with 2-slot types.** Every long or double on the stack
   counts as 2 slots. After `i2l` + `ldc2_w`, you may need maxStack=6
   even though there are only 3 "values" on the stack. The JVM verifier
   rejects the class at load time (defineHiddenClass returns null).

2. **Host class package mismatch.** `asm.init(hostClass)` determines the
   class loader context. The generated class name must be in the same
   package as the host class. Example: host is `RecordSink.class` in
   `io.questdb.cairo`, so the class name must start with
   `io/questdb/cairo/`. A mismatch causes defineHiddenClass to silently
   return null — no exception, no error message.

3. **Wrong methodCount.** If you declare N methods but emit N+1 (or N-1),
   the class file is corrupt and defineHiddenClass returns null. Count
   carefully: `<init>` + public methods + private methods.

4. **Missing StackMapTable for branches.** Any method with ifne, iflt,
   goto_, if_icmpge, etc. requires a StackMapTable attribute. Without it,
   the JVM verifier rejects the class. Branch-free methods (patterns 1,
   2, 4, 5) don't need it.

5. **invokeInterface argCount off by one.** Long and double parameters
   consume 2 stack slots. `putLong(int, long)` has argCount=3, not 2.
   Getting this wrong causes a VerifyError.

6. **Forgetting putShort(0) for exceptions and attributes.** After
   endMethodCode(), every method needs: putShort(exceptionCount),
   putShort(attributeCount). For methods with a StackMapTable, the
   attribute count is 1, not 0.