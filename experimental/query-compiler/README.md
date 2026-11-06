# Experimental query compiler

This optional module generates native query pipelines inside QuestDB's ordinary
SQL execution. The aim is faster complete queries through specialization and better
execution strategies. ClickBench is a coverage/performance proxy; query names and
benchmark distributions must not determine code generation.
SSB serves the same role for joins. Faster joins, including multiway joins, remain
a goal through reusable contracts and specialization on proven execution properties,
validated on independent inputs beyond SSB's star schema.

The module also contains standalone physical-plan and handwritten controls. Their
algorithms, scheduling and result materialization can differ from integrated SQL.
Keep those comparisons distinct.

## SQL integration

Install the compiler on an execution context before compiling SQL:

```java
context.setQueryCodeGenerator(new ResolvedQueryCompiler(new LlvmBackend(), workDirectory));
try (var factory = engine.select(
        "select category, count(), sum(amount), count_distinct(customer) "
            + "from events where amount >= 0 group by category", context);
     var cursor = factory.getCursor(context)) {
    while (cursor.hasNext()) { /* consume the ordinary result cursor */ }
}
```

Registration is absent by default. Core exposes `QueryCodeGenerator`; it has no
LLVM dependency. The normal planner resolves functions and chooses factories.
`ResolvedQueryCompiler` binds supported implementations, types and storage layouts.
Existing metadata COUNT and vector specializations retain their planner gates.
Eligible static-SYMBOL grouping uses bounded dictionary arrays for integer/DOUBLE
COUNT/SUM/MIN/MAX/AVG, including vector floating-state and AVG(LONG) 128-bit sum contracts.
Unsupported plans keep ordinary execution.

| Responsibility | Code |
| --- | --- |
| SQL binding and capability checks | [ResolvedQueryCompiler](src/main/java/org/questdb/experimental/codegen/ResolvedQueryCompiler.java) |
| Shared expression and input binding | [ResolvedInputs](src/main/java/org/questdb/experimental/codegen/ResolvedInputs.java), [ResolvedVarchar](src/main/java/org/questdb/experimental/codegen/ResolvedVarchar.java) |
| Aggregate initialization, update, merge and finalization | [AggregateUpdates](src/main/java/org/questdb/experimental/codegen/AggregateUpdates.java) |
| Finalized map rows feeding another aggregation | [ResolvedMapAggregate](src/main/java/org/questdb/experimental/codegen/ResolvedMapAggregate.java) |
| Unkeyed reduction and worker-state merge | [ResolvedScalarAggregate](src/main/java/org/questdb/experimental/codegen/ResolvedScalarAggregate.java) |
| Kernel preparation and ownership | [ResolvedAggregateKernel.Prepared](src/main/java/org/questdb/experimental/codegen/ResolvedAggregateKernel.java) |
| Acquired-slot scratch, resumable calls and publication | [AggregateSlot](src/main/java/org/questdb/experimental/codegen/AggregateSlot.java) |
| Map invocation layout and resumable status codes | [MapKernelState](src/main/java/org/questdb/experimental/codegen/MapKernelState.java) |
| Existing map representation and generated probing | [NativeMapView](../../core/src/main/java/io/questdb/cairo/map/NativeMapView.java), [QuestMapProbe](src/main/java/org/questdb/experimental/codegen/QuestMapProbe.java) |
| Typed IR, SSA and backend emission | [Ir](src/main/java/org/questdb/experimental/codegen/Ir.java), [SsaProgram](src/main/java/org/questdb/experimental/codegen/SsaProgram.java), [LlvmEmitter](src/main/java/org/questdb/experimental/codegen/LlvmEmitter.java) |
| Native invocation and allocator/cancellation callbacks | [NativeCalls](src/main/java/org/questdb/experimental/codegen/NativeCalls.java), [KernelMemory](src/main/java/org/questdb/experimental/codegen/KernelMemory.java) |

## Ownership and failure

QuestDB retains page-frame snapshots, acquired worker slots, scheduling, allocators,
query memory limits and result cursors. Eligible kernels operate directly on
Unordered4Map, Unordered8Map, UnorderedVarcharMap or OrderedMap storage. Generated
hashing, probing, updates, redistribution and merge follow those representations.

A growth exit returns before the pending row. Java grows the owned storage,
refreshes borrowed views and resumes. A failed execution is cancelled and drained;
it is never replayed through Java after partial mutation. Unsupported frame
representations decline before mutation. Parquet slices, column tops and lazy
conversions have separate eligibility rules; support for one does not imply all.

`Prepared` owns code; `AggregateSlot` owns reusable descriptors and borrows the
acquired slot's allocator. Optional partition/ranking kernels compile on first use.
Dictionary arrays own their per-slot state and publish through the query owner's
`finishScan` hook after workers drain. They borrow the existing slot allocators and
scratch. Array candidates defer fallback kernels until needed; cursor preparation
compiles the admitted scan without executing a dummy batch.
All worker calls must finish before the code owner closes. Native callbacks record
allocation/cancellation failures for Java to rethrow; Java exceptions do not
unwind through native code.

Eligible numeric aggregate subqueries can feed a second keyed COUNT/SUM/MIN/MAX
without a Java row loop. `ResolvedMapAggregate` evaluates supported projections
and consumes each finalized input map in its existing merge task. Slot-owned
ordinary maps feed a generated final merge and numeric Top-K; the ordinary result
cursor retains ownership. Filters and limits between the two aggregations remain
boundaries. This does not rewrite COUNT DISTINCT into a two-stage plan.

Bounded aggregation retains partial states and can switch to packed input records
when local combining does not reduce work. Equal-width records permit at most 5%
extra bytes in the observed map window. Short initial bypass windows grow only
when repeated map windows confirm low reuse; each frame and reuse change resets
the policy. This limits misleading-prefix costs but cannot eliminate every phase
regression. Existing workers, maps and allocators still merge the partitions.
An eligible unordered positive LIMIT/OFFSET finalizes only enough complete
partitions to satisfy demand, after scanning all input. It merges the first
partition on the owner and uses its group count to size subsequent parallel
batches. Completed counts determine when to stop. Ordinary iteration, exact size,
sorting, shared contexts and deferred DISTINCT merge still complete all partitions;
partial cursors report unknown size and preserve row IDs as demand grows.
Fixed-width key-only aggregation switches after its first full local map to an
exact duplicate filter: remembered keys already have a partition record, and
every miss is appended immediately. Collisions may add records but cannot lose
distinct values. Small sets retain their original map; predicates reuse the
shared scan lowering.
Large numeric DISTINCT state can separate dependent prefetches; the existing
adaptive-kernel policy selects the measured winner after cache-based admission.
Large fixed-key partition maps reuse the staged variable-key merge loop. Both
paths use discovered cache geometry and retain their original generated loops
when it is absent.
Eligible numeric Top-K can subdivide large partitions and defer independent VARCHAR
extrema until after ranking. Retained transient strings are copied into query-owned
storage; stable frame payloads remain borrowed. These are eligibility-gated paths,
not promises that every aggregation uses the same strategy.

Static SYMBOL predicates borrow IDs already resolved by the ordinary functions
at cursor initialization. Typed parameters load once per frame; dictionary lookup
stays outside generated row loops. Reopening refreshes values without compiling
new code. Literal VARCHAR tails likewise share query-owned, memory-tracked bytes;
long literals retain bounded generated comparison loops. Standalone selection/count,
aggregate selection and VARCHAR Top-K share
this binding. Large sets, dynamic symbol sources and equality against runtime
expressions retain ordinary filtering. Parameterized aggregate filters currently
produce selection vectors; the experimental map-fused ABI still accepts only
column pointers. No additional configuration is needed.

Eligible INT grouping expressions over one static SYMBOL also evaluate once per
dictionary entry, using the ordinary resolved function for semantics. Generated
scan/map loops load the result by SYMBOL ID. This supports `length` and literal
`strpos`, with optional STRING casts and `lower`/`upper` composition. The lookup
is query-memory-tracked, immutable while workers run, and rebuilt on cursor reopen.
Preparation checks cancellation and includes the NULL entry. It is admitted only
for known-size unfiltered scans when dictionary entries plus NULL are fewer than
input rows divided by aggregate slots (workers plus owner). Other cases retain
ordinary evaluation before map mutation. This conservative work bound avoids
serial preparation with as much expression work as one parallel scan slot; it is
not a complete cost model. STRING-valued group keys, dynamic dictionaries, runtime
substring arguments and expressions involving multiple inputs remain ordinary.

For a single dictionary-derived INT key and numeric, non-allocating aggregate
state, that same preparation also proves the result's min/max range. If the range
plus a NULL slot fits the existing per-worker cache budget and is smaller than the
rows-per-worker preparation budget above, workers aggregate into
private arrays. The arrays retain the ordinary packed aggregate state and share
initialization/merge generation with direct-SYMBOL vector aggregation. After the
scan drains, generated code merges the arrays directly; observed groups are then
published once into the existing result map and combined with ordinary fallback
frames. Wide ranges and composite keys keep generated hashing. No input-row
statistics scan or additional setting is required. This optimizes bounded INT
results, not general STRING-valued grouping.

## Supported scope

The binders and focused tests are authoritative. Current families include:

- INT/LONG addition, subtraction and multiplication, widening casts, comparisons,
  boolean predicates and constant membership;
  resolved constant BETWEEN/NOT BETWEEN on supported integer expressions and direct
  TIMESTAMP/TIMESTAMP_NS columns, preserving reversed bounds, units and NULL semantics;
  static SYMBOL equality/inequality against literals and small IN/NOT IN sets
  (up to 16 values, including runtime constants), with AND/OR/NOT composition;
  static SYMBOL LIKE/ILIKE/regex through the existing dictionary match sets,
  with generated range/bitmap/sparse-set membership;
  direct VARCHAR null, literal equality, prefix/suffix and short literal substring
  predicates. Supported numeric, SYMBOL and VARCHAR predicates compose under
  AND/OR/NOT with generated short-circuit control flow. Prefix matching binds the VARCHAR
  starts_with overload or resolved LIKE prefix; STRING overloads remain ordinary;
  searched CASE string values.
- Direct and computed fixed-width keys, SYMBOL IDs, VARCHAR keys and composite keys
  using existing map encodings. DOUBLE grouping preserves raw IEEE bits, including
  signed zero and NaN payloads. NULL and empty strings remain distinct. INT keys
  derived from static SYMBOL dictionaries have the eligibility rules above.
- DOUBLE arithmetic (+, -, *, /), integer-to-DOUBLE widening, tolerance-aware
  comparisons and NULL predicates in scan/filter/aggregate expressions
- FLOAT inputs widened into the existing DOUBLE expressions and aggregates,
  including AVG(FLOAT) and explicit FLOAT-to-DOUBLE casts. Captured inputs retain
  four-byte storage. FLOAT arithmetic, grouping and SUM(FLOAT) retain ordinary
  execution; FLOAT accumulation has a separate rounding/state contract.
- VARCHAR code-point `length` in numeric aggregate arguments, fixed/composite
  grouping keys and residual predicates, including supported searched CASE values.
  This reuses ordinary numeric aggregates and borrowed frame descriptors. ASCII
  uses metadata; other values use the ordinary continuation-byte counting algorithm.
  `length(NULL::varchar)` remains the ordinary INT value `-1`. Compact input records
  store the computed integer; numeric-only join/finalized-map consumers retain
  ordinary evaluation when they cannot supply a string descriptor.
- Resolved COUNT (including static SYMBOL, DOUBLE and VARCHAR), integer/DOUBLE SUM/MIN/MAX/AVG,
  INT/LONG/static-SYMBOL DISTINCT and VARCHAR MIN/MAX
  implementations. Supported overloads depend on the selected map/update path.
- Anchored single-capture `regexp_replace` on the ordinary ASCII-byte function,
  composed with supported VARCHAR group keys and aggregate arguments. The admitted
  language has concatenation, ASCII classes, greedy character repeats and optional
  groups that consume at least one byte when taken; other patterns and capture predicates retain ordinary evaluation.
  Generated prefix checks precede a PCRE2 JIT call through typed IR. Each slot owns
  its matcher and a cancellable Java fallback for native work/stack limits. Captures
  borrow the input slice; the existing map copies unstable keys on insertion.
- Unkeyed COUNT and integer/DOUBLE SUM/MIN/MAX/AVG reductions over filtered frames and
  unfiltered expressions whose plan uses row-by-row updates. Existing vector and
  metadata specializations retain priority. Worker-state copying/merging is generated.
  Supported filters feed SIMD masks directly into reduction; Parquet late
  materialization and unsupported inputs retain their selected-row fallback.
- INT/LONG projection, final COUNT reduction, SHORT/INT/LONG/DOUBLE full sort and
  Top-K, and positive LIMIT/OFFSET selection over supported generated results.
  Standalone DOUBLE projection stays ordinary: the tested batch bridge adds
  marshalling work and does not improve the complete query. DOUBLE expressions
  inside admitted aggregate/filter/map-consumer kernels are generated.
- Finalized fixed-key maps feed generated outer aggregation through exact numeric
  readers and static SYMBOL IDs, including finalized DOUBLE AVG and typed projections.
  Existing worker slots execute the consumer; ordinary functions retain symbol
  dictionary ownership. Variable source keys, intervening filters/limits and outer
  aggregates requiring finalization retain ordinary execution.
- Ordered frame-prefix admission for forward filtered scans with finite nonnegative
  LIMIT advice. Native VARCHAR selection stops each frame after K matches; the
  existing ordered collector applies the remaining global budget.
- Single-column VARCHAR Top-K: generated prefix rejection, key encoding, threshold
  admission, worker-result merge, survivor heap packing, compaction/final sorting
  and final LIMIT prefix selection in the existing EncodedTopKBuffer. EXPLAIN uses
  `compiledTopK`; execution counters and result checks establish actual coverage.
- INT, full-width LONG, static-SYMBOL and composite fixed-width inner-join probe pipelines
  with supported COUNT and integer/DOUBLE SUM/MIN/MAX/AVG, post-join
  predicates and supported fixed-width grouping. They borrow the existing frozen join;
  Java retains build and runtime ownership. Before worker publication, core may replace
  bounded INT/SYMBOL or LONG/DATE/TIMESTAMP key tables with packed chain-head arrays,
  preserving the row heap and duplicate order. Known probe work, observed range and
  query-memory bounds govern conversion; displaced hash storage is released. Java and
  generated probes share this lookup. Sparse/composite keys retain hashing.
  An already-admitted fixed-width payload copy may co-locate duplicate-row headers
  and payload, then release the old heap. Logical handles and duplicate order stay
  unchanged. Temporary query-memory headroom must cover the copy; Parquet and
  layouts that need extra padding keep the existing separate storage.
  Large compact copies share the existing Linux advisory huge-page hint with
  UnorderedVarcharMap. Allocation accounting stays unchanged; kernel policy decides
  whether to use large pages. Other platforms keep their ordinary allocation.
  Outer joins and general join-to-DISTINCT
  remain outside this binding.
- The join kernel accepts independent typed builds and dependent inner-join chains
  and branches. Keys can read the scan or earlier joined payloads, including composite
  keys from multiple inputs. Dependent lookups execute inside their producer's duplicate
  loop; dictionary resolution and group-map growth preserve every selected position.
  Build-only child membership can be transferred to a single INT/LONG/SYMBOL producer
  key before scanning, using existing probes and dictionary views. Compact keys use
  a bitmap; sparse keys use QuestDB's split-block Bloom layout. Exact joins retain
  duplicate multiplicity. Preparation requires known amortizable probe work and
  available query memory; mixed-source conditions remain in the exact pipeline.
  When all child key components come from one build, preparation can also cache
  its exact child-chain head per producer row. This supports composite producers
  and children and costs four bytes per producer row, without copying duplicate
  products. Generated consumers retain the ordinary chains and exact fallback.
  The known-work guard does not predict rejection by earlier joins; see
  [measured gains and preparation limits](target/join-dependency-links.md).
  COUNT(*) can consume an unobserved build's multiplicity instead of enumerating
  its duplicate rows. Eligibility follows resolved sources: predicates, grouping
  and later join keys must not read that build. A bounded, query-owned copy of the
  existing heap's chain counts preserves ordinary probes and fallback. Independent
  leaves multiply their counts; observed producer rows still iterate. Unique builds,
  unknown probe work and insufficient memory skip the copy. A frame-level dispatch
  keeps those executions on the ordinary loop. COUNT state and wrapping arithmetic
  use the existing aggregate operation; SUM/AVG and COUNT(value) are not admitted
  by this proof. See [multiplicity evidence](target/join-multiplicities.md).
  Closed COUNT forests can also fold producer duplicates before the fact scan.
  Predicates/grouping must read only the probe, and each dependent key must read
  one producer. Reverse-order sums/products reuse cached heads and the frozen
  heap's backward links; generated consumers then probe only roots. This supports
  deeper trees, branches, independent roots and the same typed/composite keys.
  Query-owned root weights replace intermediate counts/heads only after complete
  success. Unsupported heaps, insufficient memory or subtree overflow preserve
  ordinary execution. Zero weights reject before creating a group; final COUNT
  arithmetic still wraps. See [subtree evidence and costs](target/join-subtrees.md).
  SQL planning retains the existing join order when no common probe fits, admitting
  supported plans into one fused aggregate factory. Each build reuses the existing serial/parallel
  build, payload and probe lifecycle; all share the ordinary frame workers. Scalar
  output and composite numeric/SYMBOL grouping use the existing fixed-key encoder
  and maps; supported numeric expressions share the aggregate input bindings.
  INT/SYMBOL lookup retains the narrow table. LONG/composite lookup borrows existing
  Unordered8Map/OrderedMap views, using the same generated hashing/equality as other
  map consumers. SYMBOL keys translate through the existing per-execution cache;
  unresolved entries resume after ordinary dictionary lookup. Mixed key components
  preserve RecordSink encoding. Range arithmetic never narrows LONG values.
  SYMBOL payloads retain their source dictionaries. Dictionary-derived grouping
  expressions and variable-width grouping remain outside this binding. Unsupported frames
  use an exact Java fallback; unsupported plans retain ordinary execution. No new toggle.
  Completed scalar integral keys feed shared conservative bounds or exact bitmaps,
  including full-width LONG and translated SYMBOL IDs. Unknown map bounds use a
  construction scan only when known probe work can amortize it. Composite keys use
  exact probes. All-composite plans compile only that exact loop. The standalone
  filter-only integration below is still INT-only. An alternative generated loop checks all sets before exact probing;
  the existing adaptive policy measures complete frames, including map-growth retries.
  Exact probing is the initial choice. Universal sets bypass alternative sampling.
  Sets are shared immutable execution state, charged to the query and freed after drain.
  Known unfiltered probe counts can admit larger exact bitmaps within a shared
  distinct-key budget; existing filters retain their allowance. Unknown qualifying
  counts keep the existing representation. The frame chooser retains both existing
  and expanded membership, sharing the same compiled loop through slot-owned
  descriptors. It changes no descriptor while another worker uses it and preserves
  the selected representation across every growth retry. This adds no user switch
  or another compiled loop just for the expanded bitmap. Frame timing remains
  sensitive to scheduling and shared resources; it is not a guarantee of the best
  whole-query strategy.
  After build preparation, the compiler specializes on physical facts: uniqueness,
  direct lookup, payload layout and available dependency caches. Addresses, key
  values and dictionary IDs stay in execution descriptors. Reopen checks these
  facts and retains at most one specialized program set.
  Eligible scalar aggregates can keep their existing update state in IR variables,
  publishing it before cancellation polls and exits. Grouped and allocating state
  retains memory updates. Generated alternatives also compare cached child heads
  with direct re-probing when both exist. Up to eight kernels cover these choices
  and membership filtering; compilation is paid before workers start.
  Workers sample alternatives in coordinated periods without holding a policy lock
  during native execution. Later exploration compares the winner with one rotating
  challenger and occupies under 2% of completed frames. This is measured execution
  adaptation, not a cost-based join-order optimizer.
- Build-derived filters for an uninterrupted chain of INT inner joins sharing a
  frame scan. Generated checks reject rows before ordinary joins fetch payloads;
  duplicate multiplicity remains the joins' responsibility. Completed build keys
  become query-owned ranges/bitmaps before frame dispatch, using the existing
  scheduler. Sparse domains retain conservative bounds. Both sides of the first
  join can supply the common scan without changing its ordinary build choice.
  These are partial generated pipelines. The ordinary filter-only path still lacks
  the fused path's adaptive bypass for filters that reject few rows.

Integer MIN/MAX accepts LONG, INT and SHORT inputs and preserves the ordinary
8-byte LONG or 4-byte INT state. Generated ordering normalizes SHORT/INT/LONG/DOUBLE results into signed 64-bit
keys, preserving ordinary NULL, NaN and signed-zero order. AVG ranks finalized
values; partial sums are never used as a ranking shortcut.
Unkeyed kernels bind the actual 32-byte-spaced SimpleMapValue slots, promote
supported aggregate updates to SSA accumulators, and publish state once per frame.
Column tops, lazy conversions and mixed-coordinate late materialization can fall
back before mutation; native and ordinary updates share the same worker state.
`compiledScalarAggregate` in EXPLAIN identifies this binding; drained native-row
counters distinguish execution from runtime fallback.

Generic string construction, general string ordering and unsupported function
implementations retain ordinary execution. `compiledAggregateBatches` in EXPLAIN
proves aggregate binding, not complete generated execution for the entire query.
The ordinary `Async JIT` label refers to filter compilation.

Filter LIMIT kernels are prepared on eligible cursor open and reused across bind
changes and rewinds. They borrow existing task row IDs; no extra sort, result copy
or worker quota is introduced. Numeric-only selection keeps its existing full-frame
SIMD kernel. Parquet filtering remains full-frame to preserve selectivity statistics;
its existing column-top/type-cast fallback still applies. Negative, reverse and
unbounded scans retain ordinary LIMIT handling. Native admission/input counters
establish execution; an EXPLAIN filter label alone does not.

The VARCHAR Top-K build borrows a pinned frame and the acquired slot's buffer.
Generated code returns at buffer growth/compaction boundaries and resumes with
refreshed addresses. The existing owner keeps heap budgets, threshold copies and
row-ID lifetime. Merge, packing and sorting operate on normalized entries independently
of source types and compile lazily. The sort ports the existing natural-run detection,
in-place MSD radix partitioning and non-branchless pdqsort, including its insertion
and heapsort paths. A bounded explicit stack replaces recursion; pdqsort handles the
smaller partition first to bound its stack. Comparisons retain the 16-byte inline
prefix, bounded heap tails, length and unsigned row-ID ordering. Generated helpers
contain the sort loops; the stock native sorter is not called on this path.

Sorting workspace grows in a buffer-owned DirectLongList charged to the existing
query tracker. Small sorts reserve only their comparison-sort stack. FIRST/LAST
compaction, scratch-heap copying and heap-offset restoration keep the existing
algorithm. Lazy type conversions decline before mutation and retain ordinary
build/compaction. SQL binding remains one physical VARCHAR key; the generic sorter
does not by itself qualify composite ordering or partially ordered plans.

Bounded partitioned `ROW_NUMBER()` can become a generated group-by when its outer
filter is exactly `rn <= K` (or the equivalent strict/reversed comparison), with
constant `1 <= K <= 64`. Each group keeps a sorted prefix of K values; partial
prefixes merge through the existing group-by scheduler and maps. The existing
Unnest cursor expands final prefixes and supplies row numbers. There is no new
runtime toggle. EXPLAIN shows `Grouped Top K`; aggregate native-row counters
establish actual generated execution.

This rewrite accepts one direct INT/LONG/FLOAT/DOUBLE ordering column and direct
INT/LONG/SYMBOL/DOUBLE/VARCHAR partition columns. Output may contain only partition
keys, the ranked value and row number, so ties do not require retaining arbitrary
row payloads. NULL ordering and duplicates follow the ordinary window. Other
window functions, computed keys, multiple ordering columns, payload columns,
nonconstant or larger bounds retain ordinary execution. State is O(groups * K),
not constant total memory; existing query memory limits still apply.

Filter/aggregate fusion is a separate harness experiment exposed by
`ResolvedQueryCompiler.filterFusionExperiment`; ordinary construction does not
turn it on. The standalone adaptive scan alternatives are also distinct from the
integrated aggregation path.

## Backends and generated-code inspection

All backends consume the typed IR. There is no JVM execution backend.

| Backend | Compilation |
| --- | --- |
| `LlvmBackend` | Verified SSA → LLVM text → in-process optimization and ORC LLJIT; O2 by default, levels 0–3 supported |
| `AsmJitBackend` | Optimized IR → register/label stream → in-process AsmJit Compiler; Linux x86-64 |
| `NativeBackend` | IR → C → `cc` shared library; `NativeBackend("clang")` selects another compiler |

LLVM bypasses the prototype scalar/SSA optimizer and uses LLVM's passes. Java
passes LLVM text once during compilation. Execution passes column/state addresses
and row counts through FFM; no text crosses that boundary per row. The native
bridge selects the host CPU, retains an engine per optimization level and releases
query code through ORC resource trackers.

`new LlvmBackend(2, true)` writes input SSA, LLVM text and optimized LLVM text.
`new AsmJitBackend(true)` writes IR and the instruction stream. Both disable dumps
by default. For native profiling, create a writable directory and set
`QUERY_CODEGEN_PROFILE_DIR` before JVM startup: LLVM writes optimized IR and object
files with `questdb_kernel_*` symbols plus a perf map. See
[llvm_backend.cpp](src/main/c/llvm_backend.cpp) for registration and lifetime details.

Existing compiler controls include `questdb.codegen.ssa`,
`questdb.codegen.disableSsaOptimization` and `questdb.codegen.disableOptimization`
for AsmJit/C. They do not select LLVM's optimization pipeline. Library paths can
be overridden with `questdb.codegen.asmjit.library` and
`questdb.codegen.llvm.library`.

## Build and test

Requires JDK 25, Maven, CMake 3.22+, a C++17 compiler, `cc`, LLVM 21.1 development
files/shared library, pkg-config and PCRE2 development files with JIT support (tested with 10.46).
The current build is exercised on Linux x86-64. Native
bridges build through the module's Maven lifecycle; AsmJit uses the pin in
`core/CMakeLists.txt`. LLVM and PCRE2 belong only to this optional module. The capture
bridge lives beside the LLVM bridge; it adds no dependency to ordinary QuestDB.

From the repository root:

```sh
mvn -q -pl core -am -DskipTests -Djacoco.skip=true install
mvn -f experimental/query-compiler/pom.xml test
```

For focused work, select existing tests with `-Dtest=ClassName` on the second
command. The optional reactor profile is:

```sh
mvn -Pquery-compiler-prototype -pl experimental/query-compiler -am \
  '-Dtest=org.questdb.experimental.codegen.*Test' \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true test
```

## Runners and controls

The ASOF experiment runs inside the ordinary adaptive join. Its filtered forward
and ordinary lookup regions can consume bounded master spans and update the
existing unkeyed aggregate state through LLVM. Lookup fuses timestamp positioning,
descending predicate/key search and aggregate updates across multiple probes,
within the current master/RHS frames and the owner's 64-probe observation window.
Frame crossing, indexed/memoized lookup and strategy transitions stay with Java;
an unfinished native probe has no aggregate effect and resumes there. Both regions
reuse the existing scalar aggregate lowering, including SUM/MIN/MAX/COUNT/AVG with
supported INT/LONG/DOUBLE inputs, rather than a separate aggregate implementation.

A single same-type INT/LONG/SYMBOL key admits these regions. Without a supported
aggregate consumer, generated descending lookup still runs after the first Java
candidate misses. The owner lends only the remaining current-frame span, bounded
by memoized history and its work limit.
SYMBOL probes use its existing dictionary translation. Unsupported predicates,
keys and frame representations retain ordinary lookup. `lookup_rows` reports
candidate checks in the smaller backward kernel; it excludes the larger fused
region, whose completed probes are counted by `aggregate_probes`.
`aggregate region` in EXPLAIN indicates eligibility, while `aggregate_probes` in
the runner reports actual use. The optional region compiles on first use, so
inspect warmup/first-use latency as well as steady-state results. The smaller
backward kernel also compiles lazily: immediate matches avoid that kernel, while
eligible aggregate queries can compile the larger region even for immediate
matches. Both retain code across cached factory reuse and release it on eviction.
A tiny query may pay more compilation cost than it can recover. No machine-specific
crossover threshold is assumed.

The existing ASOF runner provides a reproducible 500M-row, six-aggregate control:

```sh
taskset -c 4 experimental/query-compiler/run-asof.sh \
  experimental/query-compiler/target/asof-db 500000000 1024 1000000 \
  filtered int adaptive,linear 5 32 keep 0 arith16 1 jit 0 llvm aggregates
```

Use `stock` instead of `llvm` for the baseline. `long`/`symbol`, `arith256` and
`arith_all` vary key representation and selectivity; no compiler admission depends
on these workload names. The runner checks the complete aggregate result, includes
cursor open/close in elapsed time, and reports compilation, CPU time, I/O and faults.
See [ASOF_SELECTION_PLAN.md](ASOF_SELECTION_PLAN.md) for ownership and remaining
admission boundaries, and [PROGRESS.md](PROGRESS.md) for measured evidence.

Run existing Griffin tests against stock execution or LLVM after building the core
test JAR and the experimental runner:

```sh
mvn -Pquery-compiler-prototype -pl experimental/query-compiler -am \
  -DskipTests -Djacoco.skip=true install
JAVA_TOOL_OPTIONS='-Dfuzz.s0=1 -Dfuzz.s1=2' \
  experimental/query-compiler/run-griffin-tests.sh llvm \
  'io.questdb.test.cairo.fuzz.ParallelGroupByFuzzTest#testParallelKSumMergeFuzz'
JAVA_TOOL_OPTIONS='-Dfuzz.s0=123456789 -Dfuzz.s1=987654321 -Dquestdb.fuzz.queries=1000' \
  experimental/query-compiler/run-griffin-tests.sh llvm \
  'io.questdb.test.griffin.fuzz.QueryFuzzTest#testQueryFuzz'
```

Use `stock` instead of `llvm` for the control. Class names run all their tests;
`class#method` selects one. Finish builds before starting a run: rebuilding classes
while a test JVM reads them invalidates the result. Seeds use the existing
`TestUtils` properties, `fuzz.s0` and `fuzz.s1`.

`QueryCodegenTestSupport` installs the backend only in test contexts. Shared node
and `TestUtils` contexts, the query fuzzer and the hash-join context helpers use it;
other tests with direct context construction need the same explicit wiring.
The runner reports compilations and actual native kernel calls per test, fails if
the entire LLVM run executes none, and checks that LLVM code handles are released.
Kernel calls are not input-row counts or proof that every operator was generated.
Empty input, unsupported functions and ordinary batch paths can legitimately report
zero; direct Java aggregate/map unit tests remain Java tests.

When the query fuzzer has a compiler installed, it also runs an explicit stock arm
with that hook unset, at the same filter-JIT mode. It retains the existing result,
exception, floating comparison and cursor checks. Existing JIT/storage/bind/fused
axes and fault injection remain enabled. The compiler axis reports successful
comparisons that actually invoked native code and fails on zero coverage. Exact
plan assertions in other suites are retained; a changed plan is a failure to review,
not a reason to discard result or lifecycle assertions.

For integrated SQL, `run-scan.sh --resolved-sql` accepts repetitions, cases, rows,
batch size, warmups, implementation, total execution slots, configuration and an
optional sharding threshold. Batch size/threshold zero preserve configuration
defaults. For example, once controlled profiling is available:

```sh
experimental/query-compiler/run-scan.sh --resolved-sql 9 count,mixed,high 500000000 0 5 '' 8 server
```

An empty implementation interleaves Java and LLVM; `java` disables the generator
in the chosen core build. Use an unmodified frozen core through
`QUERY_CODEGEN_CORE_CLASSPATH` when claiming a stock comparison. Server mode uses
`PropServerConfiguration`, adjusts the worker budget and disables HTTP; it retains
other execution defaults unless explicitly overridden. Choose physical CPU
placement for the machine under test rather than copying an affinity mask.

| Entry point | Purpose |
| --- | --- |
| `run-scan.sh --resolved-sql` | Integrated SQL and external controls on the same synthetic fixture |
| `run-scan.sh --resolved-join` | Join pipelines in ordinary SQL execution |
| `run-scan.sh --resolved-batch` | Single-slot aggregate update diagnostic |
| `run-scan.sh --resolved-three-way` | Ordinary, integrated and standalone execution; scheduling/layouts differ |
| `run-scan.sh`, `run-parallel.sh` | Standalone scan strategies and parallel aggregation |
| `run-compiler.sh` | Fresh compilation and first execution of composed physical plans |
| `run-matrix.sh`, `run-demo.sh`, `run-real-data.sh` | Backend/algorithm controls; small fixtures establish correctness, not large-scale performance |

The SQL runner also accepts `native-hash`, `native-auto` and `duckdb` controls.
`query.codegen.handwritten=true` selects the same-algorithm COUNT/SUM C reference
compiled through LLVM. These references support restricted fixture shapes:

- [HandwrittenCountSum](src/main/java/org/questdb/experimental/codegen/HandwrittenCountSum.java)
  retains the integrated slot/runtime boundary and rejects incompatible bindings.
- [CountSumPaths](src/main/java/org/questdb/experimental/codegen/CountSumPaths.java)
  compares Rosti-based hashing with range discovery plus direct-array aggregation.
  Its native threads/allocations lack engine cancellation and query memory tracking;
  discovery is included in execution time. It is not an SQL backend.
- [DuckDbComparison](src/main/java/org/questdb/experimental/codegen/DuckDbComparison.java)
  uses a separately supplied DuckDB JDBC JAR, reports its version/settings and
  validates results against QuestDB. Add the JAR to `QUERY_CODEGEN_CORE_CLASSPATH`.

Existing algorithm controls under `query.codegen` include `bounded`,
`localCapacity`, `sharedVarchar`, `prefetchRows`, `disablePrefetchHints` and
`largeTopK`. `profile` and `probeCounters` add instrumentation. Keep counterfactual
settings separate from default comparisons; these properties are not query-ID
eligibility rules.

## Standalone physical plans

`Pipeline`, `PhysicalPlan`, `PlanCompiler`, `CompiledPlan`, `CompiledGroupBy` and
`ParallelGroupBy` remain controls for composition, backend and algorithm experiments.
`CompilerDemo` shows scan/filter/project/join/aggregate composition. These executors
use prototype state/output representations and can materialize boxed Java rows;
their totals are not equivalent to ordinary SQL frontend or server workloads.
`KernelCache` tests leases/eviction but is not automatically installed in SQL.

## Interpreting results and continuing work

Execution-performance reports use at least 500M input rows. Keep compilation,
execution, validation sorting and result consumption boundaries explicit. Compare
identical work and results; report configuration, concurrency and storage layout.
A generated phase's speedup does not establish an end-to-end query gain. Wall time
alone does not identify a bottleneck: use resource utilization/saturation/errors,
CPU and memory evidence, and a counterfactual with predicted effects.

Runners create temporary databases; several replace their generated-code directories.
Run them individually and inspect each runner's arguments before using existing
data. Keep only meaningful findings in [PROGRESS.md](PROGRESS.md); detailed prior
entries remain in Git history, without a separate result archive.

[FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md#immediate-sequence-maintainability) holds
current work and refactoring guardrails. [TODO.md](TODO.md) holds deferred problems.
[AGGREGATE_DESIGN.md](AGGREGATE_DESIGN.md) and [PIPELINE_DESIGN.md](PIPELINE_DESIGN.md)
record design proposals, contracts and experiments; proposals are not implemented
features. The shared aggregate-definition generator remains outside cleanup.
