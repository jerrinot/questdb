# JIT Filter Prefetching Design Document

## Glossary

| Term                     | Definition                                                                                                                                                                                                                                                                                                 |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Page Frame**           | A contiguous range of rows within a partition, sized between `cairo.sql.page.frame.min.rows` (100K) and `cairo.sql.page.frame.max.rows` (1M). Each frame is processed independently by worker threads.                                                                                                     |
| **Table Column Index**   | The physical column position in the table schema (0-based). Obtained via `metadata.getColumnIndexQuiet(columnName)`. Different from query column index—filter IR uses table indexes, address cache uses query indexes.                                                                                     |
| **Query Column Index**   | The column position in the query's SELECT/WHERE projection (0-based). The address cache is indexed by query column index, NOT table column index. Translation required when prefetching filter columns.                                                                                                    |
| **Aux Vector**           | Secondary storage for variable-size columns (VARCHAR, STRING). Contains fixed-size headers/pointers (16 bytes per row for VARCHAR: 8-byte pointer + 4-byte length + 4-byte flags); actual string data is stored separately in the data vector.                                                             |
| **Variable-Size Column** | Columns with per-row variable length: VARCHAR, STRING, BINARY. These have both a data vector and an aux vector. JIT-compiled filters on these columns access the aux vector for NULL checks and length comparisons, but NOT the data vector for content comparisons (string equality is not JIT-compiled). |
| **Fixed-Size Column**    | Columns with constant byte size per row. See Section 4.1 for complete type-to-size mapping.                                                                                                                                                                                                                |
| **Frame Format**         | Either `NATIVE` (mmap'd column files, value=0) or `PARQUET` (columnar file format, value=1). Defined in `PartitionFormat.java`. Prefetch only applies to NATIVE.                                                                                                                                           |
| **SQE/CQE**              | Submission Queue Entry / Completion Queue Entry in io_uring. SQE describes an operation; CQE contains the result.                                                                                                                                                                                          |
| **Cold Data**            | Data not present in the OS page cache, requiring disk I/O on access.                                                                                                                                                                                                                                       |
| **Hot Data**             | Data already resident in the OS page cache.                                                                                                                                                                                                                                                                |

---

## 1. Overview

### 1.0 Quick Start (Minimal Implementation)

For developers who want to understand the core change before diving into details:

**The minimal viable prefetch requires only 4 changes:**

1. **Track filter columns** in `CompiledFilterIRSerializer.serializeColumn()`:
   ```java
   // Add to existing method - collect table column indexes used in WHERE clause
   filterTableColumnIndexes.add(tableColumnIndex);
   ```

2. **Expose `Files.prefetch()`** - wrap `madvise(MADV_WILLNEED)`:
   ```java
   public static int prefetch(long address, long len) {
       return madvise0(address, len, POSIX_MADV_WILLNEED);
   }
   ```

3. **Translate column indexes** - filter uses table indexes, cache uses query indexes:
   ```java
   int queryIdx = cache.tableToQueryColumnIndex(tableIdx);
   ```

4. **Call prefetch from dispatch loop** in `PageFrameSequence.dispatch()`:
   ```java
   // After dispatching frame N, prefetch frame N+2
   if (prefetchIndex < frameCount) {
       prefetchFrame(prefetchIndex);
   }
   ```

**Start with Phase 1 (sync madvise only)** - skip io_uring until the basics work.

### 1.1 Problem Statement

When executing queries with JIT-compiled filters on cold data (data not present in the OS page cache), performance
degrades significantly due to major page faults. Each page fault blocks the executing thread while the kernel fetches
data from storage, resulting in high latency for analytical queries on large datasets.

Current behavior:

```
Worker Thread                     Kernel
     |                               |
     |-- access cold page ---------> |
     |         [BLOCKED]             |-- read from disk
     |         [BLOCKED]             |
     |<-------- page ready --------- |
     |-- access next cold page ----> |
     |         [BLOCKED]             |-- read from disk
     ...
```

With 1M row page frames and 8-byte columns, a single column requires ~8MB of data. For cold data, this triggers ~2,000
page faults (assuming 4KB pages), each potentially causing disk I/O.

### 1.2 Proposed Solution

Implement asynchronous prefetching of page frame data using `madvise(MADV_WILLNEED)` to hint the kernel to read pages
into memory before they are needed. On Linux, leverage `io_uring` for fully asynchronous prefetch operations.

Key design principle: **Only prefetch columns used in the filter expression**, not all columns in the query. This
minimizes wasted I/O bandwidth and memory pressure.

Target behavior:

```
Dispatcher                    Worker 0              Worker 1              Kernel
     |                            |                     |                    |
     |-- dispatch frame 0 ------->|                     |                    |
     |-- prefetch frame 2 -----------------------------------------> async read
     |-- dispatch frame 1 --------------------------->  |                    |
     |-- prefetch frame 3 -----------------------------------------> async read
     |                            |-- process frame 0   |                    |
     |                            |   (pages arriving)  |-- process frame 1  |
     |                            |                     |   (pages arriving) |
```

### 1.3 Goals

1. Reduce query latency on cold data by overlapping I/O with computation
2. Prefetch only filter-relevant columns to minimize I/O waste
3. Provide configurable lookahead to tune prefetch aggressiveness
4. Support both synchronous (`madvise`) and asynchronous (`io_uring`) prefetch paths
5. Zero overhead when data is already cached (hot path optimization)

### 1.4 Non-Goals

1. Prefetching for non-JIT filter paths (can be added later)
2. Prefetching SELECT columns (they benefit from sequential access patterns after filtering)
3. Cross-partition prefetching (prefetch stays within current query's page frames)
4. Symbol table prefetching (symbol tables are typically small and cached)

---

## 2. Current Architecture Analysis

### 2.1 Page Frame Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Query Execution Flow                               │
└─────────────────────────────────────────────────────────────────────────────┘

SqlCodeGenerator.compileSelect()
        │
        ▼
AsyncJitFilteredRecordCursorFactory
        │
        ▼
PageFrameSequence.of()
        │
        ├──► frameCursor.getColumnIndexes()  ──► columns needed by query
        │
        ▼
PageFrameSequence.dispatch()              ◄─── PREFETCH INTEGRATION POINT
        │
        ├──► for each frame:
        │       reducePubSeq.next()
        │       reduceQueue.get(cursor).of(this, frameIndex)
        │       reducePubSeq.done(cursor)
        │
        ▼
Worker threads pick up tasks
        │
        ▼
PageFrameReduceTask.populateFrameMemory()
        │
        ├──► frameMemoryPool.navigateTo(frameIndex)
        │
        ▼
AsyncFilterUtils.applyCompiledFilter()
        │
        ├──► task.populateJitData()         ──► collect column addresses
        │
        ▼
CompiledFilter.call()                      ◄─── PAGE FAULTS OCCUR HERE
        │
        ├──► JIT code accesses mmap'd memory
        │
        ▼
Filtered row IDs returned
```

### 2.2 Key Data Structures

#### PageFrameAddressCache

Located at: `core/src/main/java/io/questdb/cairo/sql/PageFrameAddressCache.java`

Caches memory addresses and sizes for all page frames. Populated during `buildAddressCache()` before dispatch begins.

```java
public class PageFrameAddressCache {
    // Per-frame, per-column addresses and sizes
    // IMPORTANT: Indexed by QUERY column index, not table column index
    private final ObjList<LongList> pageAddresses;    // [frameIndex][queryColumnIndex] -> address
    private final ObjList<LongList> pageSizes;        // [frameIndex][queryColumnIndex] -> size
    private final ObjList<LongList> auxPageAddresses; // For varchar columns
    private final ObjList<LongList> auxPageSizes;

    // Column metadata
    // Maps query column index -> table column index
    private final IntList columnIndexes;
    private final IntList columnTypes;  // Indexed by query column index
}
```

**Critical**: The address cache uses **query column indexes** internally. Filter columns are identified by **table
column indexes**. Translation is required (see Section 3.1.3).

#### CompiledFilterIRSerializer

Located at: `core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java`

Serializes filter expressions to IR bytecode. Column references use `MEM` opcode with **table column index** as payload.

```java
// IR instruction format (24 bytes total)
|opcode(4B) |

options(4B) |payload.

lo(8B) |payload.

hi(8B) |

// Column reference instruction
opcode  =

MEM(2)

options =

type_code(I1_TYPE, I2_TYPE, I4_TYPE, I8_TYPE, etc .)

payload.lo =table_column_index  // NOTE: This is TABLE column index
```

#### Page Frame Sizing

Configured in: `core/src/main/java/io/questdb/PropServerConfiguration.java`

```java
// Default values
sqlPageFrameMinRows =100_000;   // cairo.sql.page.frame.min.rows
sqlPageFrameMaxRows =1_000_000; // cairo.sql.page.frame.max.rows

// Actual frame size calculation (FwdTableReaderPageFrameCursor.java:311-313)
pageFrameRowLimit =Math.

min(
        pageFrameMaxRows,
        Math.max(pageFrameMinRows, (partitionHi -partitionLo) /workerCount)
        );
```

### 2.3 Existing madvise Infrastructure

Located at: `core/src/main/c/linux/files.c`

```c
JNIEXPORT jint JNICALL Java_io_questdb_std_Files_madvise0
        (JNIEnv *e, jclass cls, jlong address, jlong len, jint advise) {
    void *memAddr = (void *) address;
    return posix_madvise(memAddr, (off_t) len, advise);
}

// Currently exposed constants
POSIX_MADV_RANDOM     // Used for write-only files
POSIX_MADV_SEQUENTIAL // Used for sequential reads
```

`POSIX_MADV_WILLNEED` (value 3) is not currently exposed but is available in the system headers.

**Important**: The Java wrapper `Files.madvise()` has a guard condition:

```java
public static void madvise(long address, long len, int advise) {
    if (Os.isLinux() && mmapCache.isSingleUse(address)) {
        madvise0(address, len, advise);
    }
}
```

This guard prevents madvise on shared memory regions. For prefetching read-only page frame data, we need a separate
method that bypasses this check (see Section 3.2).

### 2.4 Existing io_uring Infrastructure

Located at: `core/src/main/java/io/questdb/std/IOURingImpl.java`

Currently supports:

- `IORING_OP_NOP` (opcode 0) - No operation (testing)
- `IORING_OP_READ` (opcode 22) - Async read

Does not yet support:

- `IORING_OP_MADVISE` (opcode 28) - Async madvise (requires Linux kernel 5.6+)
- `IORING_OP_FADVISE` (opcode 27) - Async fadvise

---

## 3. Detailed Design

### 3.1 Filter Column Tracking

#### 3.1.1 Modification to CompiledFilterIRSerializer

Track which columns are referenced in the filter expression during IR serialization.

```java
// File: core/src/main/java/io/questdb/jit/CompiledFilterIRSerializer.java

public class CompiledFilterIRSerializer implements PostOrderTreeTraversalAlgo.Visitor, Mutable {

    // NEW: Track TABLE column indexes used in the filter
    // Using IntHashSet for O(1) contains checks during serialization
    private final IntHashSet filterTableColumnIndexSet = new IntHashSet();
    // Also maintain an IntList for ordered iteration during prefetch
    private final IntList filterTableColumnIndexes = new IntList();

    @Override
    public void clear() {
        // ... existing clear logic ...
        filterTableColumnIndexSet.clear();
        filterTableColumnIndexes.clear();
    }

    private void serializeColumn(int position, final CharSequence token) throws SqlException {
        if (predicateContext.isActive()) {
            final int tableIndex = metadata.getColumnIndexQuiet(token);
            if (tableIndex == -1) {
                throw SqlException.invalidColumn(position, token);
            }

            // NEW: Track this column as used by filter (O(1) duplicate check)
            if (filterTableColumnIndexSet.add(tableIndex)) {
                // Only added to list if not already present in set
                filterTableColumnIndexes.add(tableIndex);
            }

            // ... existing serialization logic ...
            putOperand(MEM, typeCode, tableIndex);
        }
    }

    // NEW: Getter for filter columns (returns TABLE column indexes)
    // Returns IntList for simpler iteration; order matches first occurrence in filter
    public IntList getFilterTableColumnIndexes() {
        return filterTableColumnIndexes;
    }
}
```

#### 3.1.2 Passing Filter Columns to Execution Layer

Modify `SqlCodeGenerator` to capture and propagate filter column indexes.

```java
// File: core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
// Around line 2723-2767 where JIT compilation happens

// After successful IR serialization:
IntList filterTableColumnIndexes = new IntList();
filterTableColumnIndexes.

addAll(jitIRSerializer.getFilterTableColumnIndexes());

// Pass to AsyncJitFilteredRecordCursorFactory constructor
        return new

AsyncJitFilteredRecordCursorFactory(
        configuration,
        engine,
        messageBus,
        base,
        filter,
        compiledFilter,
        bindVarFunctions,
        perWorkerFilters,
        reduceTaskFactory,
        limitLoFunction,
        limitLoPos,
        workerCount,
        enablePreTouch,
        filterTableColumnIndexes  // NEW parameter
);
```

#### 3.1.3 Column Index Translation

The filter produces **table column indexes**, but `PageFrameAddressCache` uses **query column indexes**. Add a
translation helper with O(1) lookup:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Column Index Translation Flow                             │
└─────────────────────────────────────────────────────────────────────────────┘

Table Schema (physical):          Query Projection (logical):
┌─────────────────────────┐       ┌─────────────────────────┐
│ Table Index │ Column    │       │ Query Index │ Column    │
├─────────────┼───────────┤       ├─────────────┼───────────┤
│ 0           │ ts        │       │ 0           │ sensor_id │ ← tableIdx=1
│ 1           │ sensor_id │  ───► │ 1           │ temp      │ ← tableIdx=2
│ 2           │ temp      │       │ 2           │ a         │ ← tableIdx=3
│ 3           │ a         │       └─────────────┴───────────┘
│ 4           │ b         │
└─────────────┴───────────┘

Filter "WHERE sensor_id = 42 AND temp > 25.0" produces:
  filterTableColumnIndexes = [1, 2]

To prefetch, we need query indexes to access PageFrameAddressCache:
  tableIdx=1 → queryIdx=0  (sensor_id)
  tableIdx=2 → queryIdx=1  (temp)
```

```java
// File: core/src/main/java/io/questdb/cairo/sql/PageFrameAddressCache.java

public class PageFrameAddressCache {
    // ... existing fields ...

    // NEW: Inverse mapping for O(1) table->query column index translation
    // Key: table column index, Value: query column index + 1 (see explanation below)
    // Built once during of() initialization, used many times during prefetch
    private final IntIntHashMap tableToQueryColumnMap = new IntIntHashMap();

    public void of(
            TableReader reader,
            IntList columnIndexes,  // query column index -> table column index
            IntList columnTypes,
            int columnCount,
            int partitionIndex
    ) {
        // ... existing initialization ...

        // NEW: Build inverse map for fast table->query lookups
        tableToQueryColumnMap.clear();
        for (int queryIdx = 0, n = columnIndexes.size(); queryIdx < n; queryIdx++) {
            int tableIdx = columnIndexes.getQuick(queryIdx);
            // WHY +1: IntIntHashMap.get() returns 0 for missing keys.
            // We need to distinguish "queryIdx=0" from "key not found".
            // By storing queryIdx+1, we get:
            //   - Missing key → get() returns 0 → subtract 1 → returns -1 (not found)
            //   - queryIdx=0  → stored as 1 → get() returns 1 → subtract 1 → returns 0 ✓
            //   - queryIdx=5  → stored as 6 → get() returns 6 → subtract 1 → returns 5 ✓
            tableToQueryColumnMap.put(tableIdx, queryIdx + 1);
        }

        // EDGE CASE: If same table column appears twice in query (SELECT a, b, a),
        // the map will contain the LAST occurrence. This is acceptable since both
        // refer to the same underlying data - we just need any valid query index.
    }

    /**
     * Translates a table column index to query column index.
     * O(1) lookup using precomputed inverse map.
     *
     * @param tableColumnIndex the column index in the table schema
     * @return the query column index, or -1 if the column is not in the query
     */
    public int tableToQueryColumnIndex(int tableColumnIndex) {
        int result = tableToQueryColumnMap.get(tableColumnIndex);
        return result - 1;  // See comment in of() for why we subtract 1
    }

    /**
     * Checks if the column at the given query index is a variable-size type.
     * Variable-size columns (VARCHAR, STRING, BINARY) have aux vectors that
     * should be prefetched separately from the data vector.
     *
     * IMPORTANT: Use ColumnType.isVarSize() instead of manual type checks.
     *
     * @param queryColumnIndex the query column index
     * @return true if the column is variable-size
     */
    public boolean isVarSizeColumn(int queryColumnIndex) {
        if (queryColumnIndex < 0 || queryColumnIndex >= columnTypes.size()) {
            return false;
        }
        int columnType = columnTypes.getQuick(queryColumnIndex);
        // Use the built-in helper to future-proof against new var-size types
        return ColumnType.isVarSize(columnType);
    }
}
```

**Memory Overhead Analysis:**

- `IntIntHashMap` base overhead: ~48 bytes (object header + internal arrays)
- Per-entry overhead: ~8 bytes (key + value, assuming no collisions)
- For 10 query columns: ~128 bytes total
- This is negligible compared to the frame data (megabytes) and eliminates O(n) scans during prefetch.

### 3.2 Prefetch Infrastructure

#### 3.2.0 Page Alignment Considerations

**Important**: The `madvise` syscall handles unaligned addresses gracefully:

```
madvise behavior with unaligned addresses:
┌─────────────────────────────────────────────────────────────┐
│ Input:  addr=0x1234, len=5000                               │
│ Kernel: rounds addr DOWN to page boundary (0x1000)          │
│         rounds (addr+len) UP to page boundary (0x3000)      │
│ Effect: pages 0x1000-0x2FFF are advised (2 pages)           │
└─────────────────────────────────────────────────────────────┘
```

Column data addresses from mmap are typically page-aligned at the file level, but within a page frame (a slice of a
column), the starting address may not be page-aligned.

**Recommendation**: Do NOT manually align addresses. Let the kernel handle it:

- Manual alignment adds complexity and potential bugs
- The kernel is optimized for this
- The slight over-prefetch (up to 1 page on each end) is negligible

**System page size**: Typically 4KB on x86-64 Linux. Can be larger (2MB huge pages) but madvise works with base page
size.

#### 3.2.1 New Files Java API

```java
// File: core/src/main/java/io/questdb/std/Files.java

public final class Files {
    // Existing constants
    public static final int POSIX_MADV_RANDOM;
    public static final int POSIX_MADV_SEQUENTIAL;

    // NEW: Prefetch hint constant
    public static final int POSIX_MADV_WILLNEED;

    // NEW: Minimum bytes to prefetch (one page). Below this threshold,
    // the syscall overhead (~200-500ns) exceeds the benefit.
    public static final int MIN_PREFETCH_BYTES = 4096;

    // NEW: Maximum bytes for a single madvise call.
    // Capped at Integer.MAX_VALUE because madvise len is size_t but we pass as int.
    // In practice, default config uses 64MB which is well under this limit.
    public static final long MAX_PREFETCH_CHUNK_BYTES = Integer.MAX_VALUE;

    static {
        // ... existing initialization ...
        POSIX_MADV_WILLNEED = getPosixMadvWillneed();
    }

    private static native int getPosixMadvWillneed();

    /**
     * Issues madvise for prefetching without the isSingleUse guard.
     * Safe to call on read-only memory regions like page frame data.
     *
     * @param address the start address (kernel handles alignment)
     * @param len the length in bytes
     * @return 0 on success, -1 on error (errors are non-fatal for prefetch)
     */
    public static int prefetch(long address, long len) {
        if (POSIX_MADV_WILLNEED < 0) {
            return -1;  // Not supported on this platform
        }
        if (address == 0 || len < MIN_PREFETCH_BYTES) {
            return 0;  // Nothing to prefetch or too small to be worth it
        }
        // Cap at MAX_PREFETCH_CHUNK_BYTES to avoid int overflow in JNI
        int safeLen = (int) Math.min(len, MAX_PREFETCH_CHUNK_BYTES);
        return madvise0(address, safeLen, POSIX_MADV_WILLNEED);
    }
}
```

**Why MIN_PREFETCH_BYTES = 4096?**

- madvise syscall overhead: ~200-500ns on modern Linux
- Reading 4KB from hot page cache: ~50-100ns
- Reading 4KB from NVMe SSD: ~100μs
- For regions smaller than one page, the syscall overhead dominates and prefetch provides no benefit

#### 3.2.2 JNI Implementation

```c
// File: core/src/main/c/linux/files.c

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixMadvWillneed
        (JNIEnv *e, jclass cls) {
    return POSIX_MADV_WILLNEED;  // Value is 3 on Linux
}

// NOTE: madvise0 already exists and returns jint (the return value was
// previously ignored). For prefetch, we check the return but treat errors
// as non-fatal since prefetch is purely advisory.
```

```c
// File: core/src/main/c/osx/files.c (macOS support)
// NOTE: macOS uses MADV_WILLNEED (not POSIX_MADV_WILLNEED), though both have value 3

#include <sys/mman.h>

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixMadvWillneed
        (JNIEnv *e, jclass cls) {
    // macOS defines MADV_WILLNEED, not POSIX_MADV_WILLNEED
    // Both have value 3, but use the correct constant for clarity
    return MADV_WILLNEED;
}

// macOS uses madvise() not posix_madvise(), ensure the existing madvise0
// implementation uses the correct function:
JNIEXPORT jint JNICALL Java_io_questdb_std_Files_madvise0
        (JNIEnv *e, jclass cls, jlong address, jlong len, jint advise) {
    // On macOS, use madvise() which returns 0 on success, -1 on error
    return madvise((void *) address, (size_t) len, advise);
}
```

```c
// File: core/src/main/c/windows/files.c (Windows stub)

JNIEXPORT jint JNICALL Java_io_questdb_std_Files_getPosixMadvWillneed
        (JNIEnv *e, jclass cls) {
    return -1;  // Not supported on Windows; prefetch will be skipped
}
```

### 3.3 Threading Model

**Critical**: Understanding the threading model is essential for correct implementation.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Threading Model                                    │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌──────────────────┐
                    │  Dispatcher      │  (Single thread per query)
                    │  Thread          │
                    └────────┬─────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
    dispatch(0)         dispatch(1)         dispatch(2)
         │                   │                   │
         │              prefetch(3)         prefetch(4)    ◄── Called from dispatcher
         │                   │                   │              thread ONLY
         ▼                   ▼                   ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Worker 0   │      │  Worker 1   │      │  Worker 2   │
│  process(0) │      │  process(1) │      │  process(2) │
└─────────────┘      └─────────────┘      └─────────────┘
         │                   │                   │
         ▼                   ▼                   ▼
    JIT filter          JIT filter          JIT filter
    (pages ready)       (pages ready)       (pages ready)
```

**Key invariants:**

1. **Prefetch is called from the dispatcher thread only** - The `PageFrameSequence.dispatch()` method runs on a single
   thread per query. All prefetch calls originate here.

2. **Workers never call prefetch** - Worker threads only process frames; they don't initiate prefetch. This avoids
   synchronization issues.

3. **Prefetch happens after dispatch, before processing** - The timeline is: dispatch frame N → prefetch frame
   N+lookahead → worker processes frame N.

4. **No shared mutable state between prefetch calls** - Each prefetch call is independent. The `AsyncPrefetchManager` (
   if used) must be per-query, not shared across queries.

5. **io_uring ring (if used) is per-query** - When using async prefetch, each query gets its own io_uring instance. This
   avoids contention and simplifies lifecycle management.

### 3.4 Prefetchable Atom Interface

Rather than using `instanceof` checks, define a proper interface for prefetch-capable atoms:

```java
// File: core/src/main/java/io/questdb/cairo/sql/async/PrefetchableAtom.java (NEW)

package io.questdb.cairo.sql.async;

import io.questdb.cairo.sql.PageFrameAddressCache;

/**
 * Interface for atoms that support prefetching page frame data.
 */
public interface PrefetchableAtom {

    /**
     * Prefetch data for the specified frame.
     *
     * @param frameIndex the frame to prefetch
     * @param cache the address cache containing frame addresses
     */
    void prefetchFrame(int frameIndex, PageFrameAddressCache cache);

    /**
     * @return the number of frames to prefetch ahead of dispatch
     */
    int getPrefetchLookahead();

    /**
     * @return true if prefetching is enabled
     */
    boolean isPrefetchEnabled();
}
```

### 3.4 AsyncJitFilterAtom Modifications

```java
// File: core/src/main/java/io/questdb/griffin/engine/table/AsyncJitFilteredRecordCursorFactory.java

public static class AsyncJitFilterAtom extends AsyncFilterAtom implements PrefetchableAtom {

    // Existing fields...
    final CompiledFilter compiledFilter;
    final MemoryCARW bindVarMemory;
    final ObjList<Function> bindVarFunctions;

    // NEW: Prefetch configuration and state
    private final IntList filterTableColumnIndexes;
    private final boolean prefetchEnabled;
    private final int prefetchLookahead;
    private final long prefetchMaxChunkBytes;

    public AsyncJitFilterAtom(
            CairoConfiguration configuration,
            Function filter,
            ObjList<Function> perWorkerFilters,
            CompiledFilter compiledFilter,
            MemoryCARW bindVarMemory,
            ObjList<Function> bindVarFunctions,
            IntList columnTypes,
            boolean enablePreTouch,
            IntList filterTableColumnIndexes  // NEW
    ) {
        super(configuration, filter, perWorkerFilters, columnTypes, enablePreTouch);
        this.compiledFilter = compiledFilter;
        this.bindVarMemory = bindVarMemory;
        this.bindVarFunctions = bindVarFunctions;

        // NEW:
        this.filterTableColumnIndexes = filterTableColumnIndexes;
        this.prefetchEnabled = configuration.isSqlJitPrefetchEnabled()
                && Files.POSIX_MADV_WILLNEED >= 0;  // Check platform support
        this.prefetchLookahead = configuration.getSqlJitPrefetchLookahead();
        this.prefetchMaxChunkBytes = configuration.getSqlJitPrefetchMaxChunkBytes();
    }

    /**
     * Prefetch data for the specified frame.
     *
     * <p><b>THREADING CONTRACT:</b> This method MUST only be called from the
     * dispatcher thread. Calling from worker threads will cause race conditions
     * and undefined behavior. See Section 3.3 for threading model details.</p>
     *
     * <p><b>Preconditions:</b></p>
     * <ul>
     *   <li>frameIndex must be valid (0 <= frameIndex < frameCount)</li>
     *   <li>cache must be initialized with frame data</li>
     *   <li>Called from dispatcher thread only (see Section 3.3)</li>
     * </ul>
     *
     * <p><b>Postconditions:</b></p>
     * <ul>
     *   <li>madvise(WILLNEED) issued for each filter column's data</li>
     *   <li>Errors are logged but do not throw (prefetch is advisory)</li>
     * </ul>
     *
     * @param frameIndex the frame to prefetch
     * @param cache the address cache containing frame addresses
     */
    @Override
    public void prefetchFrame(int frameIndex, PageFrameAddressCache cache) {
        // IMPORTANT: Verify dispatcher thread in debug builds.
        // In production, this assertion is compiled out but documents the contract.
        assert isDispatcherThread() : "prefetchFrame must be called from dispatcher thread only";

        // Early exit checks
        if (!prefetchEnabled || filterTableColumnIndexes.size() == 0) {
            return;
        }

        // Validate frameIndex bounds
        // Note: cache.getFrameCount() should be checked by caller, but defensive check here
        if (frameIndex < 0) {
            LOG.debug().$("prefetch skipped: invalid frameIndex [frameIndex=").$(frameIndex).I$();
            return;
        }

        // Check frame format - only NATIVE frames benefit from prefetch
        // PartitionFormat.NATIVE = 0 (defined in PartitionFormat.java)
        byte format = cache.getFrameFormat(frameIndex);
        if (format != PartitionFormat.NATIVE) {
            // Parquet frames use different I/O patterns; skip prefetch
            return;
        }

        LongList addresses = cache.getPageAddresses(frameIndex);
        LongList sizes = cache.getPageSizes(frameIndex);

        // Null check for safety (should not happen with valid frameIndex)
        if (addresses == null || sizes == null) {
            LOG.debug().$("prefetch skipped: null addresses/sizes [frameIndex=").$(frameIndex).I$();
            return;
        }

        // Prefetch each filter column
        for (int i = 0, n = filterTableColumnIndexes.size(); i < n; i++) {
            int tableColIdx = filterTableColumnIndexes.getQuick(i);

            // Translate table column index to query column index (O(1) lookup)
            int queryColIdx = cache.tableToQueryColumnIndex(tableColIdx);
            if (queryColIdx < 0) {
                // Column not in query projection - this can happen if filter references
                // a column that was optimized out of the projection
                continue;
            }

            // Bounds check on addresses list
            if (queryColIdx >= addresses.size()) {
                LOG.debug()
                        .$("prefetch skipped: queryColIdx out of bounds [queryColIdx=").$(queryColIdx)
                        .$(", addresses.size=").$(addresses.size())
                        .I$();
                continue;
            }

            long addr = addresses.getQuick(queryColIdx);
            long size = sizes.getQuick(queryColIdx);

            if (addr != 0 && size > 0) {
                prefetchRegion(addr, size);
            }

            // Also prefetch aux vectors for variable-size columns in filter
            if (cache.isVarSizeColumn(queryColIdx)) {
                LongList auxAddresses = cache.getAuxPageAddresses(frameIndex);
                LongList auxSizes = cache.getAuxPageSizes(frameIndex);

                if (auxAddresses != null && auxSizes != null
                        && queryColIdx < auxAddresses.size()) {
                    long auxAddr = auxAddresses.getQuick(queryColIdx);
                    long auxSize = auxSizes.getQuick(queryColIdx);

                    if (auxAddr != 0 && auxSize > 0) {
                        // For VARCHAR/STRING/BINARY, only prefetch the aux vector (headers).
                        // The actual data is accessed via pointers and is typically
                        // scattered; prefetching it provides little benefit.
                        prefetchRegion(auxAddr, auxSize);
                    }
                }
            }
        }
    }

    /**
     * Prefetch a memory region, chunking if necessary.
     * madvise has no hard size limit, but very large requests may:
     * - Block longer than desired
     * - Cause excessive page cache pressure
     * We chunk at a configurable maximum (default 64MB).
     */
    private void prefetchRegion(long addr, long size) {
        long remaining = size;
        long currentAddr = addr;

        while (remaining > 0) {
            int chunkLen = (int) Math.min(remaining, prefetchMaxChunkBytes);
            int result = Files.prefetch(currentAddr, chunkLen);

            if (result != 0) {
                // Prefetch failed; log at debug level and continue
                // This is non-fatal as prefetch is purely advisory
                LOG.debug()
                        .$("prefetch failed [addr=").$(currentAddr)
                        .$(", len=").$(chunkLen)
                        .$(", errno=").$(Os.errno())
                        .I$();
                break;
            }

            currentAddr += chunkLen;
            remaining -= chunkLen;
        }
    }

    @Override
    public int getPrefetchLookahead() {
        return prefetchLookahead;
    }

    @Override
    public boolean isPrefetchEnabled() {
        return prefetchEnabled;
    }
}
```

### 3.5 PageFrameSequence Integration

```java
// File: core/src/main/java/io/questdb/cairo/sql/async/PageFrameSequence.java

public class PageFrameSequence<T extends StatefulAtom> implements Closeable {

    // ... existing fields ...

    // NEW: Cached prefetch parameters (avoid repeated interface checks)
    private boolean prefetchEnabled;
    private int prefetchLookahead;

    public PageFrameSequence<T> of(
            RecordCursorFactory base,
            SqlExecutionContext executionContext,
            SCSequence collectSubSeq,
            int order
    ) throws SqlException {
        // ... existing initialization ...

        // NEW: Cache prefetch settings from atom
        if (atom instanceof PrefetchableAtom) {
            PrefetchableAtom prefetchable = (PrefetchableAtom) atom;
            this.prefetchEnabled = prefetchable.isPrefetchEnabled();
            this.prefetchLookahead = prefetchable.getPrefetchLookahead();
        } else {
            this.prefetchEnabled = false;
            this.prefetchLookahead = 0;
        }

        return this;
    }

    private boolean dispatch(int dispatchLimit) {
        boolean idle = true;
        boolean dispatched = false;

        final MCSequence reduceSubSeq = messageBus.getPageFrameReduceSubSeq(shard);
        final MPSequence reducePubSeq = messageBus.getPageFrameReducePubSeq(shard);
        final int collectedFrameCount = collectedFrameIndex + 1;

        long cursor;
        int i = dispatchStartFrameIndex;
        OUT:
        for (; i < frameCount; i++) {
            while (true) {
                final int totalDispatched = dispatchStartFrameIndex - collectedFrameCount;
                cursor = totalDispatched < dispatchLimit ? reducePubSeq.next() : -1;
                if (cursor > -1) {
                    reduceQueue.get(cursor).of(this, i);
                    LOG.debug()
                            .$("dispatched [shard=").$(shard)
                            .$(", id=").$(getId())
                            .$(", frameIndex=").$(i)
                            .$(", frameCount=").$(frameCount)
                            .$(", cursor=").$(cursor)
                            .I$();
                    reducePubSeq.done(cursor);
                    dispatchStartFrameIndex = i + 1;

                    // NEW: Prefetch future frames after successful dispatch
                    if (prefetchEnabled && prefetchLookahead > 0) {
                        int prefetchIndex = i + prefetchLookahead;
                        // Ensure we don't prefetch beyond available frames
                        if (prefetchIndex < frameCount) {
                            prefetchFrame(prefetchIndex);
                        }
                    }

                    idle = false;
                    dispatched = true;
                    break;
                } else if (cursor == -1) {
                    // ... existing work stealing logic ...
                }
                // ... rest of existing loop ...
            }
        }
        // ... rest of existing method ...
    }

    // NEW: Delegate to atom for prefetch
    private void prefetchFrame(int frameIndex) {
        if (atom instanceof PrefetchableAtom) {
            ((PrefetchableAtom) atom).prefetchFrame(frameIndex, frameAddressCache);
        }
    }

    // ... rest of existing methods ...
}
```

### 3.6 io_uring Async Prefetch (Linux-specific, Phase 2)

#### 3.6.1 Extend IOUringAccessor

The io_uring SQE structure has a `fadvise_advice` field at offset 28 (in a union with other fields). Add the accessor:

```java
// File: core/src/main/java/io/questdb/std/IOUringAccessor.java

public class IOUringAccessor {
    // ... existing fields ...

    // NEW: For IORING_OP_MADVISE
    static final short SQE_FADVISE_ADVICE_OFFSET;
    static final byte IORING_OP_MADVISE = 28;  // Requires kernel 5.6+

    static native short getSqeFadviseAdviceOffset();

    static {
        // ... existing initialization ...
        SQE_FADVISE_ADVICE_OFFSET = getSqeFadviseAdviceOffset();
    }
}
```

```c
// File: core/src/main/c/linux/io_uring.c

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeFadviseAdviceOffset
        (JNIEnv *e, jclass cls) {
    // The fadvise_advice field is at offset 28 in io_uring_sqe
    // It's in a union: struct { __u16 xattr_flags; __u16 cmd_op; }; __u32 fadvise_advice;
    return offsetof(struct io_uring_sqe, fadvise_advice);
}
```

#### 3.6.2 Kernel Version Check

Add a check for IORING_OP_MADVISE support:

```java
// File: core/src/main/java/io/questdb/std/IOURingFacadeImpl.java

public class IOURingFacadeImpl implements IOURingFacade {

    // ... existing methods ...

    /**
     * Check if IORING_OP_MADVISE is supported (requires kernel 5.6+).
     */
    public boolean isMadviseSupported() {
        String version = IOUringAccessor.kernelVersion();
        if (version == null) {
            return false;
        }
        // Parse major.minor from version string (e.g., "5.15.0-generic")
        try {
            int dotIndex = version.indexOf('.');
            if (dotIndex < 0) return false;
            int major = Integer.parseInt(version.substring(0, dotIndex));

            int secondDot = version.indexOf('.', dotIndex + 1);
            int minorEnd = secondDot > 0 ? secondDot : version.length();
            // Handle non-numeric suffixes
            int i = dotIndex + 1;
            while (i < minorEnd && Character.isDigit(version.charAt(i))) i++;
            int minor = Integer.parseInt(version.substring(dotIndex + 1, i));

            return major > 5 || (major == 5 && minor >= 6);
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
```

#### 3.6.3 IOURing Madvise Implementation

```java
// File: core/src/main/java/io/questdb/std/IOURingImpl.java

public class IOURingImpl implements IOURing {

    // ... existing fields and methods ...

    /**
     * Enqueue an async madvise operation.
     *
     * @param addr the start address
     * @param len the length in bytes (32-bit limit)
     * @param advice the advice value (e.g., MADV_WILLNEED = 3)
     * @return the operation ID, or -1 if the queue is full
     */
    public long enqueueMadvise(long addr, int len, int advice) {
        final long sqeAddr = nextSqe();
        if (sqeAddr == 0) {
            return -1;  // Queue full
        }

        // Clear the SQE (important for unused union fields)
        Unsafe.getUnsafe().setMemory(sqeAddr, SIZEOF_SQE, (byte) 0);

        // Set opcode
        Unsafe.getUnsafe().putByte(sqeAddr + SQE_OPCODE_OFFSET, IORING_OP_MADVISE);

        // fd is not used for madvise, but kernel expects -1
        Unsafe.getUnsafe().putInt(sqeAddr + SQE_FD_OFFSET, -1);

        // Address to madvise
        Unsafe.getUnsafe().putLong(sqeAddr + SQE_ADDR_OFFSET, addr);

        // Length (32-bit)
        Unsafe.getUnsafe().putInt(sqeAddr + SQE_LEN_OFFSET, len);

        // Advice value
        Unsafe.getUnsafe().putInt(sqeAddr + SQE_FADVISE_ADVICE_OFFSET, advice);

        // User data for correlation
        final long id = idSeq++;
        Unsafe.getUnsafe().putLong(sqeAddr + SQE_USER_DATA_OFFSET, id);

        return id;
    }
}
```

#### 3.6.4 Async Prefetch Manager

```java
// File: core/src/main/java/io/questdb/cairo/sql/async/AsyncPrefetchManager.java (NEW)

package io.questdb.cairo.sql.async;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;

/**
 * Manages async prefetch operations using io_uring on Linux.
 * Falls back to synchronous madvise on other platforms or when io_uring is unavailable.
 *
 * <p>This class is NOT thread-safe. Each worker thread should have its own instance,
 * or access must be externally synchronized.</p>
 */
public class AsyncPrefetchManager implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(AsyncPrefetchManager.class);
    private static final int MADV_WILLNEED = 3;

    private final IOURing ring;
    private final boolean asyncEnabled;
    private final long maxChunkBytes;

    // Track pending operations for this manager instancec
    private int pendingCount;

    public AsyncPrefetchManager(CairoConfiguration configuration) {
        this.maxChunkBytes = configuration.getSqlJitPrefetchMaxChunkBytes();

        boolean useAsync = configuration.isSqlJitPrefetchAsync()
                && Os.isLinux()
                && IOURingFacadeImpl.INSTANCE.isAvailable()
                && IOURingFacadeImpl.INSTANCE.isMadviseSupported();

        if (useAsync) {
            try {
                this.ring = new IOURingImpl(
                        IOURingFacadeImpl.INSTANCE,
                        configuration.getSqlJitPrefetchRingCapacity()
                );
                this.asyncEnabled = true;
                LOG.info().$("async prefetch enabled via io_uring").$();
            } catch (Exception e) {
                LOG.info().$("io_uring init failed, falling back to sync prefetch [error=").$(e).$();
                this.ring = null;
                this.asyncEnabled = false;
            }
        } else {
            this.ring = null;
            this.asyncEnabled = false;
        }
    }

    /**
     * Prefetch specified columns for the given frame.
     *
     * @param frameIndex          Frame to prefetch
     * @param cache               Page frame address cache
     * @param filterTableColIndexes  Table column indexes to prefetch
     */
    public void prefetch(
            int frameIndex,
            PageFrameAddressCache cache,
            IntList filterTableColIndexes
    ) {
        if (filterTableColIndexes.size() == 0) {
            return;
        }

        LongList addresses = cache.getPageAddresses(frameIndex);
        LongList sizes = cache.getPageSizes(frameIndex);

        if (addresses == null || sizes == null) {
            return;
        }

        for (int i = 0, n = filterTableColIndexes.size(); i < n; i++) {
            int tableColIdx = filterTableColIndexes.getQuick(i);
            int queryColIdx = cache.tableToQueryColumnIndex(tableColIdx);

            if (queryColIdx < 0 || queryColIdx >= addresses.size()) {
                continue;
            }

            long addr = addresses.getQuick(queryColIdx);
            long size = sizes.getQuick(queryColIdx);

            if (addr != 0 && size > 0) {
                if (asyncEnabled) {
                    prefetchAsync(addr, size);
                } else {
                    prefetchSync(addr, size);
                }
            }
        }

        // Submit any pending async operations
        if (asyncEnabled && pendingCount > 0) {
            ring.submit();
            pendingCount = 0;
        }
    }

    private void prefetchAsync(long addr, long size) {
        long remaining = size;
        long currentAddr = addr;

        while (remaining > 0) {
            // io_uring madvise uses 32-bit length
            int chunkLen = (int) Math.min(remaining, Math.min(maxChunkBytes, Integer.MAX_VALUE));

            long id = ring.enqueueMadvise(currentAddr, chunkLen, MADV_WILLNEED);
            if (id >= 0) {
                pendingCount++;
            } else {
                // Queue full, submit current batch and retry once
                if (pendingCount > 0) {
                    ring.submit();
                    drainCompletions();
                    pendingCount = 0;

                    id = ring.enqueueMadvise(currentAddr, chunkLen, MADV_WILLNEED);
                    if (id >= 0) {
                        pendingCount++;
                    } else {
                        // Still full, fall back to sync for this chunk
                        Files.prefetch(currentAddr, chunkLen);
                    }
                } else {
                    Files.prefetch(currentAddr, chunkLen);
                }
            }

            currentAddr += chunkLen;
            remaining -= chunkLen;
        }
    }

    private void prefetchSync(long addr, long size) {
        long remaining = size;
        long currentAddr = addr;

        while (remaining > 0) {
            int chunkLen = (int) Math.min(remaining, maxChunkBytes);
            Files.prefetch(currentAddr, chunkLen);
            currentAddr += chunkLen;
            remaining -= chunkLen;
        }
    }

    /**
     * Drain completion queue. Should be called periodically to prevent CQ overflow.
     * For prefetch operations, we don't care about results - just drain.
     */
    public void drainCompletions() {
        if (asyncEnabled && ring != null) {
            while (ring.nextCqe()) {
                // Discard results; prefetch is advisory
            }
        }
    }

    @Override
    public void close() {
        if (ring != null) {
            Misc.free(ring);
        }
    }
}
```

### 3.7 Configuration

#### 3.7.1 Property Keys

```java
// File: core/src/main/java/io/questdb/PropertyKey.java

public enum PropertyKey {
    // ... existing keys ...

    // NEW: Prefetch configuration
    CAIRO_SQL_JIT_PREFETCH_ENABLED("cairo.sql.jit.prefetch.enabled"),
    CAIRO_SQL_JIT_PREFETCH_LOOKAHEAD("cairo.sql.jit.prefetch.lookahead"),
    CAIRO_SQL_JIT_PREFETCH_ASYNC("cairo.sql.jit.prefetch.async"),
    CAIRO_SQL_JIT_PREFETCH_RING_CAPACITY("cairo.sql.jit.prefetch.ring.capacity"),
    CAIRO_SQL_JIT_PREFETCH_MAX_CHUNK_BYTES("cairo.sql.jit.prefetch.max.chunk.bytes"),
}
```

#### 3.7.2 Configuration Interface

```java
// File: core/src/main/java/io/questdb/cairo/CairoConfiguration.java

public interface CairoConfiguration {
    // ... existing methods ...

    /**
     * Enable JIT filter prefetching.
     * Default: true
     */
    default boolean isSqlJitPrefetchEnabled() {
        return true;
    }

    /**
     * Number of frames to prefetch ahead of dispatch.
     *
     * Tuning guidance:
     * - Higher values help with high-latency storage (HDD, network)
     * - Lower values reduce memory pressure
     * - Value of 0 disables prefetch
     *
     * Default: 2 (balanced for NVMe/SATA SSD)
     */
    default int getSqlJitPrefetchLookahead() {
        return 2;
    }

    /**
     * Use io_uring for async prefetch (Linux 5.6+ only).
     * Falls back to sync madvise if unavailable.
     * Default: true
     */
    default boolean isSqlJitPrefetchAsync() {
        return true;
    }

    /**
     * io_uring submission queue capacity for prefetch operations.
     * Must be a power of 2.
     *
     * Sizing guidance:
     * - Should be >= (prefetchLookahead * max_filter_columns * 2)
     * - The *2 accounts for aux vectors
     *
     * Default: 64
     */
    default int getSqlJitPrefetchRingCapacity() {
        return 64;
    }

    /**
     * Maximum bytes to prefetch in a single madvise call.
     *
     * Trade-offs:
     * - Larger values: fewer syscalls, but madvise may block longer while
     *   kernel initiates I/O for many pages
     * - Smaller values: more syscalls (each ~200-500ns), but more responsive
     *
     * Why 64MB default:
     * - Matches common readahead window sizes
     * - With NVMe (~3GB/s), 64MB takes ~20ms to read sequentially
     * - Larger than typical frame column size (1M rows × 8 bytes = 8MB)
     * - Small enough to avoid excessive blocking
     *
     * io_uring note: The SQE len field is 32-bit, limiting single operations
     * to ~4GB. The 64MB default is well within this limit.
     *
     * Default: 64MB (67108864)
     */
    default long getSqlJitPrefetchMaxChunkBytes() {
        return 64 * 1024 * 1024L;
    }
}
```

#### 3.7.3 Configuration Implementation

```java
// File: core/src/main/java/io/questdb/PropServerConfiguration.java

public class PropServerConfiguration implements ServerConfiguration {

    // NEW fields
    private final boolean sqlJitPrefetchEnabled;
    private final int sqlJitPrefetchLookahead;
    private final boolean sqlJitPrefetchAsync;
    private final int sqlJitPrefetchRingCapacity;
    private final long sqlJitPrefetchMaxChunkBytes;

    // In constructor:
    this.sqlJitPrefetchEnabled =

    getBoolean(
            properties, env,
            PropertyKey.CAIRO_SQL_JIT_PREFETCH_ENABLED,
        true
    );

    this.sqlJitPrefetchLookahead =Math.max(0,

    getInt(
            properties, env,
            PropertyKey.CAIRO_SQL_JIT_PREFETCH_LOOKAHEAD,
        2
    ));

    this.sqlJitPrefetchAsync =

    getBoolean(
            properties, env,
            PropertyKey.CAIRO_SQL_JIT_PREFETCH_ASYNC,
        true
    );

    this.sqlJitPrefetchRingCapacity =Numbers.ceilPow2(Math.max(8,

    getInt(
            properties, env,
            PropertyKey.CAIRO_SQL_JIT_PREFETCH_RING_CAPACITY,
        64
    )));

    this.sqlJitPrefetchMaxChunkBytes =

    getLong(
            properties, env,
            PropertyKey.CAIRO_SQL_JIT_PREFETCH_MAX_CHUNK_BYTES,
        64*1024*1024L
    );

    // Getter implementations
    @Override
    public boolean isSqlJitPrefetchEnabled() {
        return sqlJitPrefetchEnabled;
    }

    @Override
    public int getSqlJitPrefetchLookahead() {
        return sqlJitPrefetchLookahead;
    }

    @Override
    public boolean isSqlJitPrefetchAsync() {
        return sqlJitPrefetchAsync;
    }

    @Override
    public int getSqlJitPrefetchRingCapacity() {
        return sqlJitPrefetchRingCapacity;
    }

    @Override
    public long getSqlJitPrefetchMaxChunkBytes() {
        return sqlJitPrefetchMaxChunkBytes;
    }
}
```

---

## 4. Memory and I/O Analysis

### 4.1 Column Type Reference

Complete mapping of QuestDB column types to their fixed sizes. Variable-size columns have separate aux vector sizes.

#### 4.1.1 Fixed-Size Column Types

**IMPORTANT:** Always use constants from `ColumnType.java`, never hardcode these values.

| Column Type     | Type Code | Constant in Code       | Bytes/Row | Notes                        |
|-----------------|-----------|------------------------|-----------|------------------------------|
| BOOLEAN         | 1         | `ColumnType.BOOLEAN`   | 1         | 0=false, 1=true, -1=null     |
| BYTE            | 2         | `ColumnType.BYTE`      | 1         | Signed 8-bit integer         |
| SHORT           | 3         | `ColumnType.SHORT`     | 2         | Signed 16-bit integer        |
| CHAR            | 4         | `ColumnType.CHAR`      | 2         | UTF-16 character             |
| INT             | 5         | `ColumnType.INT`       | 4         | Signed 32-bit integer        |
| LONG            | 6         | `ColumnType.LONG`      | 8         | Signed 64-bit integer        |
| DATE            | 7         | `ColumnType.DATE`      | 8         | Milliseconds since epoch     |
| TIMESTAMP       | 8         | `ColumnType.TIMESTAMP` | 8         | Microseconds since epoch     |
| FLOAT           | 9         | `ColumnType.FLOAT`     | 4         | 32-bit IEEE 754              |
| DOUBLE          | 10        | `ColumnType.DOUBLE`    | 8         | 64-bit IEEE 754              |
| SYMBOL          | 12        | `ColumnType.SYMBOL`    | 4         | Index into symbol table      |
| LONG256         | 13        | `ColumnType.LONG256`   | 32        | 256-bit integer (4 × 64-bit) |
| GEOHASH (byte)  | 14        | `ColumnType.GEOBYTE`   | 1         | 1-7 bits precision           |
| GEOHASH (short) | 15        | `ColumnType.GEOSHORT`  | 2         | 8-15 bits precision          |
| GEOHASH (int)   | 16        | `ColumnType.GEOINT`    | 4         | 16-31 bits precision         |
| GEOHASH (long)  | 17        | `ColumnType.GEOLONG`   | 8         | 32-60 bits precision         |
| UUID            | 19        | `ColumnType.UUID`      | 16        | 128-bit UUID                 |
| IPv4            | 25        | `ColumnType.IPv4`      | 4         | 32-bit IP address            |

#### 4.1.2 Variable-Size Column Types

**CRITICAL:** Use `ColumnType.isVarSize(columnType)` helper instead of manual checks.

| Column Type | Type Code | Constant in Code     | Aux Bytes/Row | Notes                                         |
|-------------|-----------|----------------------|---------------|-----------------------------------------------|
| STRING      | 11        | `ColumnType.STRING`  | 8             | Legacy type; 8-byte offset per row            |
| BINARY      | 18        | `ColumnType.BINARY`  | 8             | 8-byte offset per row                         |
| VARCHAR     | 26        | `ColumnType.VARCHAR` | 16            | 8-byte pointer + 4-byte length + 4-byte flags |

**Why only prefetch aux vectors for variable-size columns?**

The data vector contains actual string/binary content accessed via pointers from the aux vector. This data is typically:

1. **Scattered**: Strings are stored sequentially by insertion order, not by row order after sorting
2. **Variable access patterns**: Filter may reject row before accessing string content
3. **Large**: String data can be orders of magnitude larger than aux vectors

The aux vector (fixed 8 or 16 bytes per row) is accessed sequentially during filtering and benefits from prefetch. The
data vector does not.

**Important**: For variable-size columns, prefetch only the aux vector. The data vector contains scattered string/binary
data accessed via pointers; sequential prefetch provides little benefit and may waste bandwidth.

#### 4.1.3 Prefetch Size Estimation

| Frame Size | Column Type           | Bytes/Row | Column Size | Prefetch (1 col) | Prefetch (3 cols) |
|------------|-----------------------|-----------|-------------|------------------|-------------------|
| 100K rows  | BYTE/BOOLEAN          | 1         | ~100 KB     | ~100 KB          | ~300 KB           |
| 100K rows  | SHORT/CHAR            | 2         | ~200 KB     | ~200 KB          | ~600 KB           |
| 100K rows  | INT/FLOAT/SYMBOL/IPv4 | 4         | ~400 KB     | ~400 KB          | ~1.2 MB           |
| 100K rows  | LONG/DOUBLE/TIMESTAMP | 8         | ~800 KB     | ~800 KB          | ~2.4 MB           |
| 100K rows  | UUID                  | 16        | ~1.6 MB     | ~1.6 MB          | ~4.8 MB           |
| 100K rows  | LONG256               | 32        | ~3.2 MB     | ~3.2 MB          | ~9.6 MB           |
| 1M rows    | BYTE/BOOLEAN          | 1         | ~1 MB       | ~1 MB            | ~3 MB             |
| 1M rows    | SHORT/CHAR            | 2         | ~2 MB       | ~2 MB            | ~6 MB             |
| 1M rows    | INT/FLOAT/SYMBOL/IPv4 | 4         | ~4 MB       | ~4 MB            | ~12 MB            |
| 1M rows    | LONG/DOUBLE/TIMESTAMP | 8         | ~8 MB       | ~8 MB            | ~24 MB            |
| 1M rows    | UUID                  | 16        | ~16 MB      | ~16 MB           | ~48 MB            |
| 1M rows    | LONG256               | 32        | ~32 MB      | ~32 MB           | ~96 MB            |
| 1M rows    | VARCHAR (aux only)    | 16        | ~16 MB      | ~16 MB           | ~48 MB            |

**Formula**: `prefetch_bytes = frame_rows × bytes_per_row × num_filter_columns`

### 4.2 Lookahead Tuning

The optimal lookahead depends on:

1. **Storage latency**: Higher latency benefits from larger lookahead
2. **Worker count**: More workers consume frames faster
3. **Memory pressure**: Larger lookahead uses more page cache
4. **Filter column count**: More columns = more bytes prefetched per frame

Recommended starting values:

| Storage Type  | Latency | Recommended Lookahead | Rationale                            |
|---------------|---------|-----------------------|--------------------------------------|
| NVMe SSD      | ~100μs  | 1-2 frames            | Fast I/O, prefetch completes quickly |
| SATA SSD      | ~500μs  | 2-3 frames            | Moderate latency needs some buffer   |
| HDD           | ~10ms   | 3-4 frames            | High latency, need more overlap      |
| Network (NFS) | 1-100ms | 4-8 frames            | Highly variable, larger buffer helps |

**Default of 2** is chosen as a balanced value for the common NVMe/SATA SSD case.

### 4.3 Page Cache Impact

With prefetching enabled:

| Scenario        | Behavior                                                              |
|-----------------|-----------------------------------------------------------------------|
| Cold query      | Pages arrive before access, reducing blocking time                    |
| Hot query       | `madvise(MADV_WILLNEED)` on cached pages is a no-op (~100ns overhead) |
| Memory pressure | Prefetched pages may evict other useful data                          |

**Mitigation for memory pressure:**

- Only prefetch filter columns (typically 1-3 columns, not 10-20)
- Limit lookahead to prevent excessive prefetch
- System's LRU will naturally evict prefetched pages if unused
- Consider adding memory pressure detection (future enhancement)

---

## 5. Concrete Example

### 5.1 Example Query

```sql
SELECT a, b, c, d
FROM measurements
WHERE sensor_id = 42
  AND temperature > 25.0
```

**Table schema:**
| Column | Type | Table Index |
|--------|------|-------------|
| ts | TIMESTAMP | 0 |
| sensor_id | INT | 1 |
| temperature | DOUBLE | 2 |
| a | LONG | 3 |
| b | DOUBLE | 4 |
| c | STRING | 5 |
| d | INT | 6 |

**Query projection** (what's in SELECT + WHERE):
| Query Index | Column | Table Index |
|-------------|--------|-------------|
| 0 | sensor_id | 1 |
| 1 | temperature | 2 |
| 2 | a | 3 |
| 3 | b | 4 |
| 4 | c | 5 |
| 5 | d | 6 |

### 5.2 Filter Column Tracking

`CompiledFilterIRSerializer.getFilterTableColumnIndexes()` returns: `[1, 2]`

- Table index 1 = `sensor_id` (used in `sensor_id = 42`)
- Table index 2 = `temperature` (used in `temperature > 25.0`)

**Note:** Columns `a, b, c, d` (table indexes 3, 4, 5, 6) are NOT prefetched because they're only in SELECT, not in
WHERE.

### 5.3 Prefetch Address Resolution

For frame 0 with 1M rows:

```
PageFrameAddressCache contents:
  columnIndexes = [1, 2, 3, 4, 5, 6]  // query idx -> table idx mapping

  pageAddresses[0] = [
    0x7f1234560000,  // query idx 0 (sensor_id), 4 bytes * 1M = 4MB
    0x7f1234960000,  // query idx 1 (temperature), 8 bytes * 1M = 8MB
    0x7f1235160000,  // query idx 2 (a)
    0x7f1235960000,  // query idx 3 (b)
    0x7f1236160000,  // query idx 4 (c) - aux vector
    0x7f1236560000,  // query idx 5 (d)
  ]

  pageSizes[0] = [4194304, 8388608, 8388608, 8388608, 16777216, 4194304]
```

**Prefetch calls:**

1. Filter table index 1 → query index 0:
   ```
   Files.prefetch(0x7f1234560000, 4194304)  // sensor_id: 4MB
   ```

2. Filter table index 2 → query index 1:
   ```
   Files.prefetch(0x7f1234960000, 8388608)  // temperature: 8MB
   ```

**Total prefetched:** 12MB per frame for 2 filter columns.

### 5.4 Dispatch Timeline

With `prefetchLookahead = 2`:

```
Time    Action
─────   ──────────────────────────────────────────────
T0      dispatch(frame 0) → worker queue
T0      prefetch(frame 2) → madvise for sensor_id, temperature
T1      dispatch(frame 1) → worker queue
T1      prefetch(frame 3) → madvise for sensor_id, temperature
T2      worker 0 starts frame 0 (pages arriving from prefetch @ T0-2)
T3      worker 1 starts frame 1 (pages arriving from prefetch @ T1-3)
T4      dispatch(frame 2) → worker queue
T4      prefetch(frame 4) → ...
...
```

---

## 6. Testing Strategy

### 6.1 Unit Tests

```java
// File: core/src/test/java/io/questdb/test/jit/CompiledFilterIRSerializerTest.java

@Test
public void testFilterColumnTrackingSimple() throws Exception {
    // Table: a (LONG), b (INT), c (DOUBLE), d (SHORT), e (BOOLEAN)
    // Filter: WHERE a > 10 AND c < 20.0

    CompiledFilterIRSerializer serializer = new CompiledFilterIRSerializer();
    // ... setup and serialize filter ...

    IntList filterColumns = serializer.getFilterTableColumnIndexes();

    Assert.assertEquals(2, filterColumns.size());
    Assert.assertTrue(filterColumns.contains(0)); // column 'a' at table index 0
    Assert.assertTrue(filterColumns.contains(2)); // column 'c' at table index 2
    Assert.assertFalse(filterColumns.contains(1)); // column 'b' not in filter
    Assert.assertFalse(filterColumns.contains(3)); // column 'd' not in filter
}

@Test
public void testFilterColumnTrackingWithDuplicates() throws Exception {
    // Filter: WHERE a > 10 AND a < 100
    // Column 'a' appears twice but should only be tracked once

    CompiledFilterIRSerializer serializer = new CompiledFilterIRSerializer();
    // ... setup and serialize filter ...

    IntList filterColumns = serializer.getFilterTableColumnIndexes();
    Assert.assertEquals(1, filterColumns.size());
    Assert.assertEquals(0, filterColumns.getQuick(0)); // column 'a'
}

@Test
public void testFilterColumnTrackingWithFunction() throws Exception {
    // Filter: WHERE abs(a) > 10
    // Column 'a' should be tracked even when used in a function
    // (Note: JIT may not support all functions, but column tracking should work)

    CompiledFilterIRSerializer serializer = new CompiledFilterIRSerializer();
    // ... setup with filter "abs(a) > 10" ...

    IntList filterColumns = serializer.getFilterTableColumnIndexes();
    Assert.assertEquals(1, filterColumns.size());
    Assert.assertEquals(0, filterColumns.getQuick(0)); // column 'a'
}

@Test
public void testFilterColumnNotInSelect() throws Exception {
    // Query: SELECT a FROM t WHERE b > 10
    // Filter column 'b' is not in SELECT but IS in the internal projection
    // (QuestDB adds filter columns to projection automatically)

    CompiledFilterIRSerializer serializer = new CompiledFilterIRSerializer();
    // Table schema: a (LONG) at index 0, b (INT) at index 1
    TableReaderMetadata metadata = createMetadata("a", ColumnType.LONG, "b", ColumnType.INT);

    // Serialize filter "b > 10"
    serializer.of(metadata, parseFilter("b > 10"));

    // Filter should track table column index 1 (column 'b')
    IntList filterColumns = serializer.getFilterTableColumnIndexes();
    Assert.assertEquals(1, filterColumns.size());
    Assert.assertEquals(1, filterColumns.getQuick(0));  // table index of 'b'

    // Simulate query projection that includes 'b' (added internally):
    // Query projection: [a, b] -> table indexes [0, 1]
    PageFrameAddressCache cache = new PageFrameAddressCache(configuration);
    IntList queryColumnIndexes = new IntList();
    queryColumnIndexes.add(0);  // query idx 0 -> table idx 0 (a)
    queryColumnIndexes.add(1);  // query idx 1 -> table idx 1 (b)
    cache.of(metadata, queryColumnIndexes, false);

    // Translation should work: table index 1 -> query index 1
    Assert.assertEquals(1, cache.tableToQueryColumnIndex(1));
}

@Test
public void testAllColumnTypesInFilter() throws Exception {
    // Verify each fixed-size column type can be tracked correctly
    String[] types = {
            "BOOLEAN", "BYTE", "SHORT", "CHAR", "INT", "LONG",
            "DATE", "TIMESTAMP", "FLOAT", "DOUBLE", "SYMBOL",
            "LONG256", "UUID", "IPv4"
            // GEOHASH types would need specific precision testing
    };

    for (String type : types) {
        // Create table with column of this type
        // Create filter using that column
        // Verify filter column tracking works
    }
}
```

### 6.2 Column Index Translation Tests

```java
// File: core/src/test/java/io/questdb/test/cairo/PageFrameAddressCacheTest.java

@Test
public void testTableToQueryColumnIndex() throws Exception {
    PageFrameAddressCache cache = new PageFrameAddressCache(configuration);

    // Simulate query that selects columns at table indexes 2, 5, 7
    IntList columnIndexes = new IntList();
    columnIndexes.add(2);  // query idx 0 -> table idx 2
    columnIndexes.add(5);  // query idx 1 -> table idx 5
    columnIndexes.add(7);  // query idx 2 -> table idx 7

    cache.of(metadata, columnIndexes, false);

    Assert.assertEquals(0, cache.tableToQueryColumnIndex(2));
    Assert.assertEquals(1, cache.tableToQueryColumnIndex(5));
    Assert.assertEquals(2, cache.tableToQueryColumnIndex(7));
    Assert.assertEquals(-1, cache.tableToQueryColumnIndex(0));  // not in query
    Assert.assertEquals(-1, cache.tableToQueryColumnIndex(3));  // not in query
}
```

### 6.3 Integration Tests

```java
// File: core/src/test/java/io/questdb/test/griffin/JitPrefetchTest.java

public class JitPrefetchTest extends AbstractCairoTest {

    @Test
    public void testPrefetchEnabledByDefault() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (x LONG, y INT, z DOUBLE)");
            insert("INSERT INTO t SELECT x, x::int, x::double FROM long_sequence(100000)");

            try (RecordCursorFactory factory = select("SELECT * FROM t WHERE x > 50000")) {
                Assert.assertTrue(factory instanceof AsyncJitFilteredRecordCursorFactory);
                // Verify prefetch is enabled (check atom configuration)
            }
        });
    }

    @Test
    public void testOnlyFilterColumnsArePrefetched() throws Exception {
        // This test verifies the column tracking logic
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (a LONG, b INT, c DOUBLE, d SHORT, e BOOLEAN)");
            insert("INSERT INTO t SELECT x, x::int, x::double, x::short, true FROM long_sequence(100000)");

            try (RecordCursorFactory factory = select("SELECT a, b, c FROM t WHERE a > 100 AND c < 50000.0")) {
                Assert.assertTrue(factory instanceof AsyncJitFilteredRecordCursorFactory);
                AsyncJitFilteredRecordCursorFactory jitFactory =
                        (AsyncJitFilteredRecordCursorFactory) factory;

                // Get the atom and verify filter columns
                // (May need to expose filter column list for testing)
            }
        });
    }

    @Test
    public void testPrefetchDisabledOnWindows() throws Exception {
        // Verify graceful fallback when madvise is not available
        Assume.assumeTrue(Os.isWindows());

        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (x LONG)");
            insert("INSERT INTO t SELECT x FROM long_sequence(100000)");

            // Query should still work, just without prefetch benefit
            try (RecordCursor cursor = select("SELECT * FROM t WHERE x > 50000").getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) count++;
                Assert.assertTrue(count > 0);
            }
        });
    }

    @Test
    public void testPrefetchWithMixedColumnTypes() throws Exception {
        // Verify prefetch works with different column sizes
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (a BYTE, b SHORT, c INT, d LONG, e FLOAT, f DOUBLE)");
            insert("INSERT INTO t SELECT " +
                    "x::byte, x::short, x::int, x, x::float, x::double " +
                    "FROM long_sequence(100000)");

            // Filter uses columns of different sizes
            try (RecordCursor cursor = select(
                    "SELECT * FROM t WHERE a > 10 AND c < 50000 AND f > 100.0"
            ).getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    // Just verify no crashes
                }
            }
        });
    }

    @Test
    public void testPrefetchWithVarchar() throws Exception {
        // Verify varchar aux vector prefetch
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (x LONG, s VARCHAR)");
            insert("INSERT INTO t SELECT x, 'str' || x FROM long_sequence(100000)");

            // Filter on varchar column (NULL check only supported in JIT)
            try (RecordCursor cursor = select(
                    "SELECT * FROM t WHERE x > 50000 AND s IS NOT NULL"
            ).getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    // Verify no crashes
                }
            }
        });
    }

    @Test
    public void testPrefetchLookaheadBeyondFrameCount() throws Exception {
        // Edge case: lookahead exceeds available frames
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE t (x LONG)");
            // Small table = few frames
            insert("INSERT INTO t SELECT x FROM long_sequence(1000)");

            // Should not crash when prefetchIndex >= frameCount
            try (RecordCursor cursor = select("SELECT * FROM t WHERE x > 500").getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                }
            }
        });
    }
}
```

### 6.4 Performance Benchmarks

```java
// File: benchmarks/src/main/java/org/questdb/JitPrefetchBenchmark.java

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class JitPrefetchBenchmark {

    @Param({"true", "false"})
    public boolean prefetchEnabled;

    @Param({"0", "1", "2", "4"})
    public int prefetchLookahead;

    @Param({"1", "3"})
    public int filterColumnCount;

    private CairoEngine engine;
    private SqlExecutionContext context;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        // Create 1GB table with various column types
        // Configure prefetch settings based on parameters
        // See benchmark setup guide for details
    }

    @Setup(Level.Invocation)
    public void dropCaches() throws Exception {
        // Linux only: flush page cache to simulate cold data
        if (Os.isLinux()) {
            // sync && echo 3 > /proc/sys/vm/drop_caches
            // Requires root or appropriate capabilities
        }
    }

    @Benchmark
    public long filterQueryCold(Blackhole bh) throws Exception {
        long count = 0;
        try (RecordCursor cursor = executeQuery(getFilterQuery())) {
            while (cursor.hasNext()) {
                bh.consume(cursor.getRecord().getLong(0));
                count++;
            }
        }
        return count;
    }

    private String getFilterQuery() {
        switch (filterColumnCount) {
            case 1:
                return "SELECT count() FROM t WHERE x > 500000000";
            case 3:
                return "SELECT count() FROM t WHERE x > 500000000 AND y < 1000 AND z > 0.5";
            default:
                throw new IllegalArgumentException();
        }
    }

    @TearDown(Level.Trial)
    public void teardown() {
        Misc.free(engine);
    }
}
```

---

## 7. Implementation Plan

### 7.0 Dependency Graph

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Phase 1 Implementation Dependencies                       │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────┐
                    │  1.1 JNI const  │  No dependencies - start here
                    │  MADV_WILLNEED  │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ 1.2 Files.      │  Depends on: 1.1
                    │ prefetch()      │
                    └────────┬────────┘
                             │
    ┌────────────────────────┼────────────────────────┐
    │                        │                        │
┌───▼───┐             ┌──────▼──────┐          ┌──────▼──────┐
│  1.3  │             │    1.4      │          │    1.5      │
│Column │             │ IR Filter   │          │   Config    │
│Index  │             │ Column      │          │ Properties  │
│Trans. │             │ Tracking    │          │             │
└───┬───┘             └──────┬──────┘          └──────┬──────┘
    │                        │                        │
    │                        │                        │
    └────────────────────────┼────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │ 1.6 Prefetch-   │  Depends on: 1.3, 1.4, 1.5
                    │ ableAtom iface  │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ 1.7 Implement   │  Depends on: 1.2, 1.6
                    │ prefetchFrame() │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ 1.8 Integrate   │  Depends on: 1.7
                    │ dispatch loop   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ 1.9 End-to-end  │  Depends on: 1.8
                    │ testing         │
                    └─────────────────┘

RECOMMENDED ORDER: 1.1 → 1.2 → (1.3, 1.4, 1.5 in parallel) → 1.6 → 1.7 → 1.8 → 1.9
```

### 7.1 Phase 1: Synchronous Prefetch (MVP)

**Goal:** Working prefetch with sync madvise, testable end-to-end.

| Step | Task                                       | Files                                                                         | Dependencies  | Test                        |
|------|--------------------------------------------|-------------------------------------------------------------------------------|---------------|-----------------------------|
| 1.1  | Add `POSIX_MADV_WILLNEED` constant         | `Files.java`, `files.c` (all platforms)                                       | None          | Unit test constant value    |
| 1.2  | Add `Files.prefetch()` method              | `Files.java`                                                                  | 1.1           | Unit test with mock         |
| 1.3  | Add `tableToQueryColumnIndex()`            | `PageFrameAddressCache.java`                                                  | None          | Unit test index translation |
| 1.4  | Track filter columns in serializer         | `CompiledFilterIRSerializer.java`                                             | None          | Unit test column tracking   |
| 1.5  | Add configuration properties               | `PropertyKey.java`, `CairoConfiguration.java`, `PropServerConfiguration.java` | None          | Config loading test         |
| 1.6  | Add `PrefetchableAtom` interface           | New file                                                                      | 1.3, 1.4, 1.5 | -                           |
| 1.7  | Implement prefetch in `AsyncJitFilterAtom` | `AsyncJitFilteredRecordCursorFactory.java`                                    | 1.2, 1.6      | Unit test prefetch logic    |
| 1.8  | Integrate with dispatch loop               | `PageFrameSequence.java`                                                      | 1.7           | Integration test            |
| 1.9  | End-to-end testing                         | -                                                                             | 1.8           | `JitPrefetchTest.java`      |

**Suggested working order for a single developer:**

1. Start with 1.1 and 1.2 (JNI layer) - can be tested in isolation
2. Do 1.3, 1.4, 1.5 next (can be done in any order, no dependencies between them)
3. Then 1.6, 1.7 (Java implementation)
4. Finally 1.8, 1.9 (integration)

### 7.2 Phase 2: io_uring Async Prefetch

**Goal:** Fully async prefetch on Linux 5.6+.

| Step | Task                               | Files                                | Test                      |
|------|------------------------------------|--------------------------------------|---------------------------|
| 2.1  | Add `SQE_FADVISE_ADVICE_OFFSET`    | `IOUringAccessor.java`, `io_uring.c` | Unit test offset value    |
| 2.2  | Add kernel version check           | `IOURingFacadeImpl.java`             | Unit test version parsing |
| 2.3  | Implement `enqueueMadvise()`       | `IOURingImpl.java`                   | Unit test SQE setup       |
| 2.4  | Create `AsyncPrefetchManager`      | New file                             | Unit test with mock ring  |
| 2.5  | Integrate async manager (optional) | `AsyncJitFilterAtom.java`            | Integration test          |
| 2.6  | Benchmark sync vs async            | -                                    | Performance comparison    |

### 7.3 Phase 3: Tuning and Optimization

| Step | Task                                                          |
|------|---------------------------------------------------------------|
| 3.1  | Benchmark on various storage types (NVMe, SATA, HDD, NFS)     |
| 3.2  | Tune default lookahead based on benchmarks                    |
| 3.3  | Add metrics (prefetch count, bytes, latency)                  |
| 3.4  | Consider adaptive lookahead based on observed page fault rate |
| 3.5  | Documentation and user guide                                  |

---

## 8. Debugging Guide

### 8.1 Verifying Prefetch is Working

**Check madvise syscalls with strace:**

```bash
strace -e madvise -f -p $(pgrep -f questdb) 2>&1 | grep WILLNEED
```

Expected output when prefetch is active:

```
[pid 12345] madvise(0x7f1234560000, 4194304, MADV_WILLNEED) = 0
[pid 12345] madvise(0x7f1234960000, 8388608, MADV_WILLNEED) = 0
```

**Monitor page faults with perf:**

```bash
perf stat -e page-faults,major-faults,minor-faults \
    curl -s "http://localhost:9000/exec?query=SELECT..."
```

With prefetch: expect fewer major faults.

**Check /proc/vmstat:**

```bash
# Before query
cat /proc/vmstat | grep -E 'pgfault|pgmajfault'
# Run query
# After query
cat /proc/vmstat | grep -E 'pgfault|pgmajfault'
```

### 8.2 Common Failure Modes

| Symptom                    | Likely Cause                              | Resolution                                          |
|----------------------------|-------------------------------------------|-----------------------------------------------------|
| No madvise syscalls seen   | Prefetch disabled or unsupported platform | Check `cairo.sql.jit.prefetch.enabled` and platform |
| madvise returns ENOMEM     | System under memory pressure              | Reduce lookahead or prefetch chunk size             |
| No performance improvement | Data already hot in cache                 | Test with cold data (drop caches)                   |
| Query slower with prefetch | Over-prefetching causing cache thrashing  | Reduce lookahead value                              |
| io_uring errors in log     | Kernel too old or io_uring unavailable    | Check kernel version >= 5.6 for async               |

### 8.3 Debug Logging

Enable debug logging for prefetch:

```properties
# In server.conf or via -D flag
log.level.io.questdb.cairo.sql.async=DEBUG
```

Expected log output:

```
DEBUG i.q.c.s.async.PageFrameSequence - dispatched [shard=0, id=123, frameIndex=0, ...]
DEBUG i.q.c.s.async.PageFrameSequence - prefetch [frameIndex=2, columns=2, bytes=12582912]
```

---

## 9. Observability

### 9.1 Metrics

Add to QuestDB metrics (consider sampling for high-volume):

```java
// Prefetch operations counter (sampled at 1/100)
questdb_jit_prefetch_total {
    type = "sync|async", result = "success|error"
}

// Prefetch bytes counter (sampled at 1/100)
questdb_jit_prefetch_bytes_total

// Prefetch columns per query (histogram with small buckets)
questdb_jit_prefetch_columns_per_query {
    le = "1|2|3|5|10"
}

// Prefetch latency (histogram, async only)
questdb_jit_prefetch_latency_us {
    le = "10|100|1000|10000"
}
```

### 9.2 Logging

```java
// Info level: startup configuration
LOG.info()
    .

$("JIT prefetch configured [enabled=").

$(prefetchEnabled)
    .

$(", lookahead=").

$(prefetchLookahead)
    .

$(", async=").

$(asyncEnabled)
    .

$(", maxChunk=").

$(maxChunkBytes)
    .

I$();

// Debug level: per-frame prefetch
LOG.

debug()
    .

$("prefetch [frameIndex=").

$(frameIndex)
    .

$(", columns=").

$(filterColumnCount)
    .

$(", totalBytes=").

$(totalBytes)
    .

I$();

// Debug level: errors (non-fatal)
LOG.

debug()
    .

$("prefetch failed [addr=").

$(addr)
    .

$(", len=").

$(len)
    .

$(", errno=").

$(Os.errno())
        .

I$();
```

---

## 10. Code Review Checklist

Before submitting a PR for prefetch implementation, verify:

### 10.1 Column Index Handling

- [ ] All column lookups use `tableToQueryColumnIndex()` - never use table index directly with address cache
- [ ] Return value of `tableToQueryColumnIndex()` is checked for -1 before use
- [ ] `ColumnType.isVarSize()` is used instead of manual type code checks
- [ ] No hardcoded column type constants (use `ColumnType.*` constants)

### 10.2 Threading Safety

- [ ] `prefetchFrame()` is only called from dispatcher thread
- [ ] Thread assertion is present in `prefetchFrame()` (even if just a comment in production)
- [ ] No shared mutable state between prefetch calls
- [ ] io_uring ring (if used) is per-query, not shared

### 10.3 Bounds and Null Checks

- [ ] `frameIndex` is validated before use: `0 <= frameIndex < frameCount`
- [ ] Null checks for `getPageAddresses()` and `getPageSizes()` results
- [ ] `queryColIdx` bounds check against `addresses.size()` before array access
- [ ] Aux vector lists are null-checked before access

### 10.4 Prefetch Size Handling

- [ ] Regions smaller than `MIN_PREFETCH_BYTES` (4096) are skipped
- [ ] Large regions are chunked at `prefetchMaxChunkBytes`
- [ ] Chunk size is capped at `Integer.MAX_VALUE` for JNI safety
- [ ] Zero-size and null-address regions are handled

### 10.5 Frame Format

- [ ] Frame format is checked: `getFrameFormat() == PartitionFormat.NATIVE`
- [ ] Parquet frames are skipped (no prefetch benefit)

### 10.6 Configuration

- [ ] All config values have sensible defaults
- [ ] Config validation prevents invalid values (negative lookahead, zero chunk size)
- [ ] Feature can be disabled via `cairo.sql.jit.prefetch.enabled=false`

### 10.7 Error Handling

- [ ] `Files.prefetch()` errors are logged at DEBUG level, not thrown
- [ ] io_uring failures fall back to sync madvise
- [ ] Platform-specific unavailability (Windows) is handled gracefully

### 10.8 Testing

- [ ] Unit tests for column index translation (including edge cases)
- [ ] Unit tests for filter column tracking (including duplicates)
- [ ] Integration test with mixed column types
- [ ] Test with prefetch disabled (verify no regression)
- [ ] Test on Windows/macOS (verify graceful fallback)

---

## 11. Future Enhancements (Out of Scope for Initial Implementation)

1. **Adaptive lookahead**: Dynamically adjust lookahead based on observed page fault rates and I/O latency
2. **Memory pressure awareness**: Reduce prefetch when system memory is constrained (monitor `/proc/meminfo`)
3. **Cross-partition prefetch**: Prefetch next partition's metadata while processing current one
4. **SELECT column prefetch**: After filtering, prefetch projection columns for matching rows (requires row bitmap)
5. **Prefetch for non-JIT paths**: Extend to Java-based filter execution
6. **Prefetch statistics in EXPLAIN**: Show prefetch plan in query explain output

---

## 12. Common Gotchas and Pitfalls

This section summarizes common mistakes to avoid during implementation.

### 12.1 Column Index Confusion

| Mistake                                                | Consequence                                      | Fix                                              |
|--------------------------------------------------------|--------------------------------------------------|--------------------------------------------------|
| Using table column index directly in address cache     | Wrong column prefetched or ArrayIndexOutOfBounds | Always translate via `tableToQueryColumnIndex()` |
| Assuming query column order matches table column order | Silent wrong data access                         | Build inverse map during initialization          |
| Not handling -1 return from translation                | NPE or incorrect array access                    | Check for -1 and skip that column                |

### 12.2 Frame and Format Issues

| Mistake                               | Consequence                 | Fix                                                |
|---------------------------------------|-----------------------------|----------------------------------------------------|
| Prefetching Parquet frames            | Wasted I/O, possibly errors | Check `getFrameFormat() == PartitionFormat.NATIVE` |
| Not checking for null addresses/sizes | NPE                         | Defensive null checks before iteration             |
| Prefetching beyond frameCount         | Array bounds exception      | Guard with `prefetchIndex < frameCount`            |

### 12.3 Variable-Size Column Handling

| Mistake                                    | Consequence                    | Fix                                                  |
|--------------------------------------------|--------------------------------|------------------------------------------------------|
| Prefetching VARCHAR data vector            | Wasted I/O (scattered data)    | Only prefetch aux vector for var-size columns        |
| Forgetting STRING/BINARY are also var-size | Inconsistent prefetch behavior | Use `isVarSizeColumn()` which checks all three types |
| Assuming aux vector always exists          | NPE                            | Null check `auxAddresses` and `auxSizes`             |

### 12.4 Threading and Lifecycle

| Mistake                                     | Consequence                           | Fix                                            |
|---------------------------------------------|---------------------------------------|------------------------------------------------|
| Calling prefetch from worker threads        | Race conditions, corrupted state      | Only call from dispatcher thread (Section 3.3) |
| Sharing AsyncPrefetchManager across queries | io_uring contention, lifecycle issues | One manager per query                          |
| Not draining io_uring CQ                    | CQ overflow, lost completions         | Call `drainCompletions()` periodically         |
| Forgetting to close io_uring ring           | Resource leak                         | Implement `QuietCloseable`, call `Misc.free()` |

### 12.5 Performance Pitfalls

| Mistake                                 | Consequence                          | Fix                                                       |
|-----------------------------------------|--------------------------------------|-----------------------------------------------------------|
| O(n) column index translation per frame | Slow prefetch with many columns      | Use `IntIntHashMap` for O(1) lookup                       |
| O(n²) duplicate check in IR serializer  | Slow compilation for complex filters | Use `IntHashSet.add()` which returns false for duplicates |
| Prefetching all query columns           | Excessive I/O, memory pressure       | Only prefetch filter columns                              |
| Setting lookahead too high              | Page cache thrashing                 | Start with 2, tune based on storage                       |

### 12.6 Platform-Specific Issues

| Platform         | Issue                                  | Mitigation                                        |
|------------------|----------------------------------------|---------------------------------------------------|
| Windows          | No madvise support                     | `Files.prefetch()` returns -1, gracefully skipped |
| macOS            | Uses `madvise()` not `posix_madvise()` | Different JNI implementation in `osx/files.c`     |
| Linux < 5.6      | No io_uring MADVISE support            | Falls back to sync madvise                        |
| Huge pages (2MB) | madvise still works on 4KB boundaries  | No special handling needed                        |

### 12.7 Testing Blind Spots

Ensure your tests cover:

- [ ] Filter column that's not in SELECT (e.g., `SELECT a FROM t WHERE b > 10`)
- [ ] Filter column used multiple times (e.g., `WHERE a > 10 AND a < 100`)
- [ ] Mixed fixed-size and var-size columns in filter
- [ ] Empty result set (all rows filtered out)
- [ ] Single-row frame (edge case for frame sizing)
- [ ] SYMBOL column in filter (verify symbol table not prefetched)
- [ ] Query on Parquet partition (should skip prefetch)
- [ ] Concurrent queries with prefetch enabled
- [ ] Prefetch lookahead > remaining frames
- [ ] All column types from Section 4.1

---

## 13. References

- Linux `madvise(2)` man page: https://man7.org/linux/man-pages/man2/madvise.2.html
- Linux `io_uring(7)` man page: https://man7.org/linux/man-pages/man7/io_uring.7.html
- io_uring and kernel operations: https://kernel.dk/io_uring.pdf
- QuestDB JIT compiler: `core/src/main/java/io/questdb/jit/`
- PageFrameSequence: `core/src/main/java/io/questdb/cairo/sql/async/PageFrameSequence.java`
- PageFrameAddressCache: `core/src/main/java/io/questdb/cairo/sql/PageFrameAddressCache.java`
