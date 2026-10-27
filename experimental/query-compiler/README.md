# Typed query compiler prototype

Composable scan, filter, projection, inner hash join and aggregation operators
lower to fused pipelines in one typed IR. Three backends execute the same IR:

- **AsmJit:** in-process x86-64 machine code, with AsmJit's register allocator.
- **Native C:** C → optimizing source compiler → shared library.
- **LLVM:** typed SSA → LLVM IR → in-process optimization and ORC LLJIT.

All three read QuestDB page frames and generate the same state algorithms.
The physical operator graph is broader than the current opt-in SQL frontend.

[Pipeline design and literature review](PIPELINE_DESIGN.md) explains the staged
compiler: typed scan/filter/project planning, demand-driven input binding,
eager, late scalar and masked alternatives, explicit vector effects, and runtime
selection. It records the code contracts checked, alternatives considered,
CPU hypotheses, validation gates and remaining production integration work.

Meaningful results and discoveries go in [PROGRESS.md](PROGRESS.md). Routine runs
print timings to the console. There is no result archive.

[Aggregate integration design](AGGREGATE_DESIGN.md) describes connecting compilation
to resolved QuestDB functions, lifecycle and ownership contracts, and the staged
experiment for sharing aggregate definitions. It separates current code evidence
from proposed changes.

[Foundation integration plan](FOUNDATIONS_PLAN.md) sequences the remaining work:
resolved operators and compatible state, shared parallel execution, native result
ownership, then compilation admission and tiering. Each gap has code references,
infrastructure to reuse, measurement controls and completion gates.

## Join pipelines in ordinary execution

The opt-in `ResolvedQueryCompiler` binds INT inner joins with COUNT and integer
SUM over either input, scalar aggregation or a direct INT/LONG grouping key, and
resolved integer comparisons after the join (`<`, `>=`, and their reversed
forms). `ResolvedInputs` also binds integer addition/subtraction and widening in
arguments and predicates from the actual function implementations. Aggregate
updates consume produced values and null flags rather than requiring frame loads;
map lookup consumes a produced key. LLVM, AsmJit and C share this lowering.

Generated lookup, partition routing and duplicate iteration borrow QuestDB's frozen
join. Build-side values borrow its native page-address cache or its existing copied
payload. The normal copy decision is unchanged. Parquet without an eligible copy
and unsupported probe layouts decline before mutation. Java retains build,
filter/dispatch, allocation, map growth, merge and cursor ownership.

Keyed joins use the actual Unordered4/8Map or fixed-key OrderedMap and existing
shards. A group miss returns before updating that match, preserving the probe and
duplicate positions. The ordinary key writer inserts/grows the map, borrowed views
are refreshed, and native execution resumes the same match. Runtime failures
cancel and drain; they never replay a partially updated frame through Java.

The ordinary fused planner rejects predicates that read both inputs. With the
compiler explicitly installed, those predicates may reach the optional binder;
if the complete pipeline is rejected, its temporary factory is closed and the
ordinary plan is restored. Compiler-disabled planning keeps the existing gate.
A speedup for this shape therefore includes admission to parallel fused execution,
not just a change from Java instructions to LLVM instructions.

Unsupported aggregate/key/expression families, outer joins and SYMBOL/composite
join keys keep ordinary execution. In particular, the fused join registry excludes
DISTINCT's auxiliary allocation; join-to-DISTINCT needs the separate aggregation
owner in the foundation plan. Existing count-only specializations stay in charge.
This work does not yet migrate every `CompiledPlan` composition or provide bounded
streaming output for non-aggregate queries.

The existing runner accepts samples, cases, input rows, warmups, forced mode,
execution slots and a concurrent comparison flag:

```sh
taskset -c 0-7 experimental/query-compiler/run-scan.sh --resolved-join 3 unique,payload,payload-duplicates 500000000 5 '' 8 false
taskset -c 0-7 experimental/query-compiler/run-scan.sh --resolved-join 3 post-filter,keyed-duplicates,keyed-high 500000000 5 '' 8 false
```

An empty mode interleaves ordinary and LLVM execution; `java` also runs against
unmodified core via `QUERY_CODEGEN_CORE_CLASSPATH`. Server defaults are preserved
except for the worker budget and disabled HTTP listener. Timings include build,
filtering/probing, merge, result consumption and close; compilation and data
creation are separate. Keyed fixtures order results in SQL and compare every
returned value. The benchmark's primitive result collector is not an engine output
container. Query memory sampling reports a lower bound, not an exact peak.
`map-misses` counts native exits serviced by ordinary slot-local map insertion;
it adds no instrumentation to successful probes or aggregate updates.

At least two execution slots are required: defaults disable parallel joins with
zero background workers. Owner-only progress of an already eligible plan is tested
separately. Concurrent comparisons share the ordinary query worker pool.

## Design constraint: generalize beyond the benchmark

Execution-performance reports use at least **500 million input rows per case**.
Small automated fixtures and the legacy demo/matrix remain useful for correctness
and debugging; they do not establish execution performance at the target scale.

ClickBench supplies example workloads. The implementation should specialize from
composable physical operators, types, expressions, null semantics and aggregate
state layouts. Query IDs, table/column names and benchmark-specific distributions
must not select execution strategies. Any future cost decisions should use
available statistics or observed runtime behavior, with safe handling of wrong
estimates and unsupported plans.

Validate operator combinations across row counts, group/pair cardinalities,
skew, null rates, filter selectivity, key widths and aggregate mixes, including
small queries and compilation/reuse costs. Keep some workloads outside tuning
and check real data as well as generated inputs. Correctness, memory behavior
and regressions across that range determine whether a change generalizes.

The demo's handwritten kernels are controls for specific workloads. The shared
IR and lowering remain independent of those fixtures. The matrix starts every
outer group/pair table at 64 slots and grows from observed occupancy, without
using the input cardinality to size the table.

## Computed VARCHAR grouping keys

The variable-key binding accepts direct VARCHAR columns, immutable UTF-8 literals
and nested searched `CASE WHEN` expressions. Conditions use the same resolved
integer predicates as the filter compiler. Only the selected string branch is
decoded; NULL and empty strings remain distinct. Literal bytes live in existing
slot descriptors and the selected key is copied through OrderedMap's tracked
staging buffer. Map growth, worker ownership, redistribution and merge are reused.
All three backends lower the same expression IR. There is no SQL-text matching.

The compiler reads the searched-CASE picker's resolved branches, including casts,
rather than interpreting the lifecycle argument list. Simple CASE/SWITCH, dynamic
string construction and unsupported conditions retain ordinary execution.

Constant positive LIMIT/OFFSET over generated Long Top K now selects the requested
row-ID window in generated code too. The sorted-pair input and output list are
borrowed from their existing owners; a separate capability check keeps older or
unsupported compiler implementations on ordinary execution. Empty windows, bounds,
query memory accounting, cancellation and cursor reopening retain existing owners.

Long Top K automatically specializes on its resolved ordering and retained prefix
length (the upper bound for OFFSET). The shared IR emits only the requested
direction; K<=64 emits only the existing insertion algorithm. Larger limits keep
both capacity paths because large-only specialization regressed isolated execution.
There are no configuration toggles for these compiler decisions.

Finalized aggregate readers can declare read thread safety separately from mutation
safety. LONG DISTINCT counts use the existing parallel Top-K jobs after merge;
ordinary Java getter fallback has the same read guarantee. The existing group-count
threshold, acquired slots, result lists and drain barrier remain in charge. This
scheduling improvement benefits ordinary execution as well as generated kernels.

Radix sorting uses a typed, direction-specific helper. LLVM keeps one optimized
native body per query module; C and AsmJit expand the same helper before lowering.
Calls propagate cancellation and failures before publishing results and share the
query's existing code lifetime. Small K has no radix call. Exact K and value offsets
remain runtime inputs; map collection and worker-list merge reuse their existing
storage and ownership.
Older preparation hooks retain generic code. Specialized slots check destination
order and capacity class before mutation. See PROGRESS.md for compilation and
execution measurements.

## Run

Requires JDK 25, Maven, CMake, a C++17 compiler, `cc` (GCC or Clang), and LLVM 21.1
development headers, CMake configuration and shared library on Linux x86-64.
Maven builds both experimental native bridges, reading the exact AsmJit
repository/revision from `core/CMakeLists.txt` and locating installed LLVM with
CMake. Dependency fetching and C++ bridge compilation happen at build time.
LLVM is a dependency of this optional module only. From the repo root:

```sh
mvn -q -pl core -am -Dmaven.test.skip=true -Djacoco.skip=true install
mvn -f experimental/query-compiler/pom.xml test
experimental/query-compiler/run-matrix.sh 9
# Optional case-name substring, including heldout-correlated / flat-unique
experimental/query-compiler/run-matrix.sh 9 heldout
# Existing Java aggregation algorithms: 1/2/4/8 prototype workers, real SQL query workers
# Default: 500M rows each for mixed, selective, and nested-repeated; five warmups
# Arguments: repetitions, comma-separated case filters, rows, maximum workers, warmups
experimental/query-compiler/run-parallel.sh 9
experimental/query-compiler/run-parallel.sh 9 nested-repeated 500000000 8 5
# Scan strategies: old control, shared eager, late scalar, masks, adaptive, real SQL
# 500M rows/case, 8 workers, 5 warmups, 9 rotated samples by default
experimental/query-compiler/run-scan.sh 9
# Optional process affinity; choose physical CPUs appropriate to your machine
taskset -c 0-7 experimental/query-compiler/run-scan.sh 9 selective,changing 500000000 8 5
# Fresh compilation plus first execution: filter/group, join/group, aggregate/join/aggregate
experimental/query-compiler/run-compiler.sh 100000 9
experimental/query-compiler/run-compiler.sh 128 9
experimental/query-compiler/run-demo.sh 1000000 9
# Q8-shaped aggregation: rows, repetitions, mode, groups, distinct values per group
experimental/query-compiler/run-demo.sh 1000000 15 distinct 1024 128
# Q9-style subset: SUM(AdvEngineID), COUNT(*), COUNT DISTINCT(UserID)
experimental/query-compiler/run-demo.sh 1000000 15 distinct-mixed 1024 128
```

The demo creates and deletes a temporary database. It replaces
`experimental/query-compiler/target/generated/` on each run, keeping only the
current IR, C source, compiled code and compiler diagnostics for inspection.
The matrix/real-data runners similarly replace `target/matrix-code/`, and the
reuse runner replaces `target/reuse-code/`; the fresh-compile runner replaces
`target/compiler-code/`. Run one runner at a time. AsmJit writes no query artifacts
by default; `new AsmJitBackend(true)` enables IR and wire-format dumps.
LLVM also writes no query artifacts by default; `new LlvmBackend(2, true)` dumps
the input SSA, LLVM text and optimized LLVM text.

For a real-data check, download UCI's [Adult data](https://archive.ics.uci.edu/dataset/2/adult)
`adult.data` outside the repository and run:

```sh
experimental/query-compiler/run-real-data.sh /tmp/adult.data 9
```

This uses the original 32,561 training rows and order, selecting age,
education-number, capital-gain and hours-per-week. No duplication or synthetic
replacement; no data is committed. The real-data runner uses 200 warmups for
these small-input comparisons.

The module is excluded from normal builds. An optional reactor build is available:

```sh
mvn -Pquery-compiler-prototype -pl experimental/query-compiler -am \
  '-Dtest=org.questdb.experimental.codegen.*Test' \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true test
```

## Working with it

`Pipeline` is the first frontend: a small Java DSL for physical plans, not an SQL
parser. For example:

```java
var key = new Pipeline.Column(0, "AdvEngineID", ColumnType.SHORT);
var plan = new Pipeline(
        List.of(key),
        new Pipeline.Compare(Ir.Op.NE, key, new Pipeline.Constant(0)),
        List.of(key),
        List.of(new Pipeline.Count())
);
try (var query = CompiledGroupBy.compile(
        engine, context, "hits", plan, new AsmJitBackend(), workDirectory)) {
    var rows = query.execute(context);
}
```

Use `new NativeBackend()` for optimized C, or `new NativeBackend("clang")` to
select a source compiler.
`Pipeline.Add` demonstrates adding a scalar operation once by emitting existing
IR instructions; none of the backends needs to change.

For parallel scan/filter/group-by, use `ParallelGroupBy.compile(engine, context,
"hits", plan, backend, workDirectory, workers)`. It supports COUNT, COUNT(value),
SUM and nested DISTINCT on all three backends. The separate flat DISTINCT
experiment remains serial. Generated scan, shard and merge code is shared across
workers; mutable state is private to an execution slot.

The algorithms come from the current Java implementation:

- `GroupByShardingContext.maybeEnableSharding`: switch when a fragment's group
  count exceeds `getGroupByShardingThreshold()`, or aggregate cardinality exceeds
  ten times that threshold. Retain the sharding hint between executions.
- `GroupByMapFragment.shard`: use the top hash bits to partition into the existing
  `NUM_SHARDS` (256), copying full aggregate states. Subsequent scans update the
  corresponding shard. Equal-numbered shards merge independently; output shards
  have disjoint groups and need no further aggregation.
- `Map.merge`: copy the state for a new key; call aggregate merge for an existing
  key. COUNT/SUM retain the concrete Java functions' null and overflow rules.
- `CountDistinctLongGroupByFunction.merge`: ignore empty/null sources, adopt source
  state into empty destinations, handle singleton values, and merge sets in the
  direction selected by `destSize > (srcSize >>> 1)`. Generated set merges preserve
  Java's slot iteration, insertion and growth order. Source allocations remain
  alive through result consumption; they are not deep-copied on adoption.
- `GroupByAllocatorFactory` and `PerWorkerLocks` are reused directly. Each slot
  gets the regular allocator. All worker allocations are retained until the query
  has joined every task and copied the results. No new allocation lock is imposed.

`NestedMergeTest` compares generated merge with the real Java function for 64
source/destination combinations per backend: result, set headers and slots,
pointer adoption/retention, allocation and cleanup. Parallel tests cover sharding,
mixed aggregates, empty input, nulls, column tops, repeated execution and failures
during both scan and shard merge.

This is still an isolated execution harness. It uses a per-query executor and a
barrier per frame, not QuestDB's production page-frame dispatcher/worker pool.
The outer table/hash remain the prototype's pre-existing layout; Java's map
pre-sizing statistics and eager source-map release are not reproduced. Sharding
uses that table's hash, not a claim of byte-for-byte equivalence with QuestDB maps.
Composed plans/joins, the SQL adapter and kernel cache remain serial. Integrating
the regular runtime is the next gate; adding aggregate/operator coverage waits.

Parallel allocation uses the context's query tracker, or an owned budget tracker
when none is supplied. Its per-chunk accounting has the same concurrent limit
semantics as core. The extra prototype `Options.memoryLimit` is also checked at
joined task barriers; it is not an exact concurrent reservation cap. The parallel
runner reports maximum retained bytes **observed at barriers**, which can miss
transient growth peaks. Descriptor bookkeeping and Java result objects are excluded.

`run-parallel.sh` requires at least **500 million input rows per workload**;
the third argument is an absolute row count, not a multiplier. The default cases
are mixed COUNT/SUM, selective aggregation and repeated nested DISTINCT. It
rotates serial LLVM, parallel LLVM and real parallel SQL after five full-data
warmups, validates every result, and reports actual row/group/pair counts,
scan/merge phases, and per-process major faults/storage reads during measured
executions. Other case filters remain available; high-cardinality workloads can
exceed the explicit native-memory budget and fail. SQL uses N−1
background workers plus the caller; the prototype uses N workers plus a coordinator.
Compilation and comparison sorting are excluded, while allocation, merge, result
copy and cleanup are included. Set `QUERY_CODEGEN_CORE_CLASSPATH` to a stable core
JAR when another build may replace `core/target/classes` during a run.

`PhysicalPlan` composes these expressions into operator trees. For example:

```java
var dk = new Pipeline.Column(0, "k", ColumnType.INT);
var fk = new Pipeline.Column(0, "k", ColumnType.INT);
var amount = new Pipeline.Column(1, "amount", ColumnType.LONG);
var dim = new PhysicalPlan.Scan("dim", List.of(dk));
var facts = new PhysicalPlan.Scan("facts", List.of(fk, amount));
var join = new PhysicalPlan.HashJoin(dim, facts, List.of(dk), List.of(fk));
var grouped = new PhysicalPlan.Aggregate(join, List.of(join.columns().get(0)),
        List.of(new Pipeline.Sum(join.columns().get(2)), new Pipeline.Count()));
try (var query = CompiledPlan.compile(
        engine, context, grouped, new AsmJitBackend(), workDirectory)) {
    var rows = query.execute(context);
}
```

`PlanCompiler` separates stateful operators into dependency-ordered pipelines.
The example generates a join-build pipeline, a fused scan/probe/aggregate pipeline,
and an output pipeline. An aggregate can feed another join or aggregate; an
aggregate → join → aggregate example in `CompilerDemo` generates five pipelines.
Filters and projections fuse on either side of joins. Join build keys may repeat,
keys may be composite, and integer null equality matches QuestDB SQL. Each build
row survives through a generated payload chain. Shared subplans retain one state.

All pipelines share a query allocator and cancellation handling. Join payloads
and output use generated chunked tuple storage; tables grow from observed
occupancy. Results are currently materialized into boxed Java rows. Composed
plans support the nested DISTINCT algorithm, not the experimental flat variant.
The physical DSL is explicit about join order/build side; there is no new cost
optimizer or SQL join lowering yet.

`IrOptimizer` runs before AsmJit and C. It propagates constants and copies,
simplifies arithmetic and comparison combinations, removes known branches and
dead locals, and revalidates the resulting typed program. Facts are merged at
conditional branches and invalidated for loop-carried writes. Loads and allocations
are never propagated, moved or dropped as pure expressions; lazy evaluation and
integer wraparound are preserved. This remains the default structured path.

An experimental shared SSA middle layer is available with
`-Dquestdb.codegen.ssa=true` for both AsmJit and C. `SsaProgram` introduces basic
blocks, explicit edges and phi values for branches and loop backedges.
`SsaOptimizer` verifies dominance and phi inputs, eliminates unreachable paths,
numbers equivalent values and moves non-trapping arithmetic out of loops.
Loads can be reused within a block until a store, allocation, free or poll;
they are never hoisted. There is no alias analysis or memory SSA yet.
`SsaLiveness` coalesces non-interfering phi values; remaining parallel copies are
resolved on their incoming edges. Late lowering rematerializes cheap addresses
and comparisons and preserves fallthrough layout.

For AsmJit and C, SSA is **opt-in because measurements do not yet show a general
execution win** and its analyses add compilation cost. Their default path remains
the control. LLVM always consumes SSA and uses its own optimization passes.
For example:

```sh
JAVA_TOOL_OPTIONS=-Dquestdb.codegen.ssa=true experimental/query-compiler/run-matrix.sh 5
mvn -f experimental/query-compiler/pom.xml -Dquestdb.codegen.ssa=true test
```

Add `-Dquestdb.codegen.disableSsaOptimization=true` to exercise SSA conversion
and backend lowering without its optimization passes. SSA flags are read at
each compilation; set them before a run and use a fresh kernel cache.

For a diagnostic control, start the JVM with
`-Dquestdb.codegen.disableOptimization=true`. This disables structured and SSA
optimization passes for AsmJit and C; the setting is fixed when the optimizer
class is initialized. SSA conversion, verification and backend lowering still
run when SSA is selected.
Compilation timings include optimization. Dumped `query.ir` files show the IR
actually compiled, after the shared stage when enabled.

`MachineCode` lowers typed IR to registers, labels and memory operations, then
the native bridge uses AsmJit's `Compiler` and `JitRuntime`. It selects immediate
operands, constant memory displacements and comparison branches, preserves lazy
expressions, and removes unreachable code before register allocation. The bridge
knows no SQL operators. It uses the dependency version selected by QuestDB with
an isolated runtime and ABI; production filter-JIT entry points are unchanged.
This backend currently supports Linux x86-64 only. The library path can be set
with `-Dquestdb.codegen.asmjit.library=/absolute/path/libquery_codegen.so`.
Generated functions and callbacks are released when their owning kernels close.

`LlvmBackend` lowers the original typed IR to verified SSA, retaining phi values.
It bypasses our scalar/SSA optimizers and out-of-SSA lowering so LLVM owns those
decisions. `new LlvmBackend()` selects O2; `new LlvmBackend(1)` selects O1, with
levels 0–3 supported. The shared optimizer flags above do not change LLVM's path.

Java emits LLVM IR text and passes its UTF-8 bytes through one FFM compilation
call. The C++ bridge parses and verifies the module, runs LLVM's standard pass
pipeline for the selected level, and uses ORC LLJIT to produce a callable native
address. There is no compiler subprocess or query file in the default path.
One engine per optimization level is initialized lazily and retained for the
process; it targets the host CPU. Compilation and release are serialized. Each
kernel owns an ORC resource tracker; closing it releases its code, while cache
leases retain live kernels. Execution uses the existing native ABI and
allocation/free/cancellation callbacks, with no text conversion at execution time.
The bridge path can be overridden with
`-Dquestdb.codegen.llvm.library=/absolute/path/libquery_codegen_llvm.so`.

Text emission plus native parsing/initial verification measured about 0.35–0.9 ms
(3–5% of warm compilation) on three kernels; optimization and code generation
dominate. A binary stream could avoid formatting/parsing, but still needs LLVM IR
construction. Keep text for this prototype until that tradeoff warrants changing.
See the progress log for the measurement boundaries and compilation costs.

The IR has typed integer/address/boolean values, explicit-width memory loads,
locals, arithmetic, branches and loops. Generated execution has no per-row
`Record`, `Function`, `MapValue` or aggregate-updater calls. Integer arithmetic
wraps like Java long; the native compiler uses `-fwrapv`. Nulls and column tops
follow QuestDB's integer representation. `Compare` explicitly rejects null
operands; `SqlCompare` follows QuestDB's integer comparison rules, including
null-equals-null. `Add` is long arithmetic; it does not implement int overflow.

`ScanPlan` preserves a maximal scan/filter/project prefix before lowering. Both
frontends use `ScanLowering`. Its late and masked paths make predicate columns
available first; a compiler-time demand pass binds consumer inputs once after
filtering and before stateful work. Unrequested columns generate no reads. Projection substitution
has a structural budget to bound expression expansion.

`VectorMask` represents a bounded, read-only lane region; `TrailingZeros` enumerates
selected positions in input order. LLVM emits explicit vector predicates and
legalizes their width for the host. C and AsmJit use a portable scalar expansion
of the same operation. Guarded scalar tails and column tops preserve memory
safety and null semantics. Aggregation, hashing and merge remain scalar and use
the existing algorithms. SIMD speedups on every backend are not claimed.

Compiled queries retain eager scalar, late scalar and masked alternatives when
their IR differs. Eager execution binds demanded inputs before the predicate,
preserving the original strategy where it performs well; it still omits unused
columns. Unfiltered scans compile only one implementation.
`AdaptiveKernel` measures serial chunks or joined parallel frames using query-local
feedback. Every worker in a parallel frame gets the same variant, avoiding slow
exploration chunks delaying otherwise fast workers at the existing barrier.
It samples all choices initially, compares median cost per row with hysteresis,
then runs the winner without timing for 128 units before comparing it with one
rotating challenger. Each chunk runs once; failures are never retried. Small units
do not train the policy.
All alternatives share state and preserve row order. This costs extra compilation
and exploration work; it is not guaranteed to match the best forced strategy.
Immutable code leases can be cached, but feedback is never shared across queries.

`run-scan.sh` compares forced strategies with this default and real parallel SQL.
It covers sparse, dense, empty, random, correlated and abruptly changing filters,
integer widths, expressions, multiple aggregates and nested DISTINCT. Each case
has at least 500M actual input rows and every execution is checked against SQL.
It prints compilation, execution ranges, scan/merge phases and process I/O checks,
then removes its temporary database and code. It retains no results archive.

Current support: BYTE/SHORT/INT/LONG columns, filters, multiple grouping keys,
COUNT, SUM, and COUNT DISTINCT, including global aggregates and empty-input
semantics. Two physical DISTINCT algorithms are available:

- `CountDistinct(argument)` deduplicates `(group keys, argument)` pairs, then
  collapses them into group counts in a second kernel. One null-marker pair per
  group preserves all-null groups. Use `execute(context, groupCapacity,
  pairCapacity)` for separate capacities; the two-argument overload uses the
  same capacity for both. This flat variant must be the only aggregate.
- `NestedCountDistinct(argument)` emits QuestDB's serial long DISTINCT algorithm:
  keep the first value inline, then use a growable per-group hash set. Hashing,
  set layout, probing, insertion order and growth thresholds match
  `CountDistinctLongGroupByFunction` / `GroupByLongHashSet`. The defaults are
  initial capacity 3 and load factor 0.7; the three-argument constructor accepts
  other settings, and the demo uses its engine configuration. Both the outer
  group table and inner sets grow from observed values.
  One nested DISTINCT can be mixed with COUNT, COUNT(value), and SUM in any
  aggregate order. Their states are updated in one scan and group lookup.

Nested allocation/free uses QuestDB's `FastGroupByAllocator` on all three backends.
Native code calls it through FFM callbacks on growth; the generated IR handles
initialization and rehashing. Allocation failures return to Java safely, and
query teardown frees all remaining sets. The outer group table remains the
prototype's implementation, so this does not reproduce the entire QuestDB map.

`execute(context)` starts at 64 slots and doubles at 75% occupancy. Rehashing
moves entire aggregate states, including nested-set pointers; it does not replay
rows. `Options(initialCapacity, maxCapacity, memoryLimit)` controls the bounds;
the default native aggregation budget is 512 MiB. The existing integer-capacity
overloads deliberately retain fixed capacities for controlled comparisons.
Exhausting capacity or memory throws before exposing results.

In serial execution, outer tables and DISTINCT sets share `FastGroupByAllocator`, charged to the
context's memory tracker when present. Otherwise an owned tracker enforces the
prototype budget. With a caller tracker, the extra prototype bound conservatively
reserves a whole possible new chunk; it can reject before actual usage reaches
the limit. Peak bytes include retained data chunks and both tables during rehash.
Descriptor/allocator bookkeeping and boxed Java output are outside this budget.
In composed plans the budget also covers join payloads and native output tuples.
This is bounded native query state, not a complete process/heap memory limit.

Cancellation is checked between 65,536-row calls and inside long probes,
zeroing, rehashing and output copying. Native callbacks record failures and
return status codes so Java exceptions never unwind through C. Failure teardown
releases query state. Query objects, caches and execution contexts are serial-use.

## Resolved aggregate batches in ordinary SQL

`ResolvedQueryCompiler` registers an optional implementation on the ordinary
SQL execution context. Normal QuestDB resolution and factory selection run first:

```java
context.setQueryCodeGenerator(new ResolvedQueryCompiler(new LlvmBackend(), workDirectory));
try (var factory = engine.select(
        "select category, count(), sum(amount), count_distinct(customer) "
            + "from events where amount >= 0 group by category", context);
     var cursor = factory.getCursor(context)) {
    while (cursor.hasNext()) { /* ordinary native map cursor */ }
}
```

Registration is absent by default. Core depends only on `QueryCodeGenerator`;
the experimental module supplies LLVM, AsmJit or C. The existing generic keyed
`AsyncGroupByRecordCursorFactory` binds exact resolved COUNT, integer SUM and
LONG DISTINCT implementations with BYTE/SHORT/INT/LONG columns, constants, resolved
integer addition/subtraction and widening. Filters and computed grouping keys use
the same expression binder, including INT overflow and getter-specific NULL rules.
Unsupported implementations keep their existing execution path. Earlier planner
specializations, including metadata COUNT and vector aggregation, stay in charge.
EXPLAIN adds `compiledAggregateBatches` when binding succeeds; the existing
`Async JIT` label still describes filter compilation.

Supported maps generate probing, insertion and aggregate updates together. Other
layouts can retain ordinary probing and native aggregate updates per batch. Existing workers,
slot locks, allocators, memory limits, sharding decisions, merge and result cursors
retain ownership. DISTINCT emits the existing singleton/set algorithm and reports
new-value statistics to the normal sharding decision. Growth and cancellation use
exception-safe callbacks; they are not per-row calls. Unsupported column tops and lazy type
conversion decline a batch before mutation and use ordinary functions once.
Execution failures propagate through cancellation/draining without replay.

Sharded execution generates **hashing, shard selection, map probing and aggregate
updates in a native pipeline** for supported direct/computed fixed-width keys
and VARCHAR-bearing keys.
`QuestMapProbe` emits the actual `Unordered4Map`, `Unordered8Map`, `UnorderedVarcharMap` or fixed/variable-key
`OrderedMap` algorithm, including zero keys, composite key packing and biased
compressed offsets. Native insertion, rehash, redistribution and merge operate on
borrowed map views. Growth returns before the pending row; the existing owner
allocates/resizes memory, refreshes the view and resumes that row. Ordinary result
cursors consume the same state. Bounded LONG aggregate Top-K collection and merge
also compile, including preparation forwarded through projection wrappers.

VARCHAR decoding reads native inline/split storage and Parquet slices, keeping NULL
distinct from empty. UnorderedVarcharMap borrows stable keys and copies new unstable
keys into its arena; OrderedMap copies encoded keys into its append heap.
Hashing, byte equality, insertion, relocation, merge and numeric Top-K use shared IR.
Direct VARCHAR NULL predicates also compile. Mixed integer/VARCHAR keys, multiple
VARCHAR fields and wider aggregate states use the existing OrderedMap encoding,
tracked staging buffer and variable-size heap traversal. Computed string keys,
string ordering and compound string predicates remain unsupported. Parquet late
materialization retains its ordinary fallback because columns use different row
spaces; it is not counted as complete generated execution.

The generated pipeline prepares candidates in batches of 32 rows, prefetches their
key/value entries and the first nested DISTINCT set slots, then validates full keys
and applies updates in input order. Hash collisions use the original probe path.
Slot scratch holds source rows, keys, outer entry addresses and any packed key bytes.
A native return discards the batch, so owner insertion/growth cannot leave
stale addresses; DISTINCT set pointers are always reloaded for actual updates.
`-Dquery.codegen.prefetchRows=0` selects the original scalar loop as a control;
positive values are powers of two up to 1,024. The diagnostic
`-Dquery.codegen.disablePrefetchHints=true` retains batching but removes cache hints.

The ordinary `--resolved-sql` harness accepts
`-Dquery.codegen.sqlFile=/path/query.sql` against its existing fixture; handwritten
reference modes reject this override. General output arithmetic remains separate:
the ordinary optimizer may rewrite `SUM(v+1)` into SUM/COUNT plus a Java output
expression. Current design and measured scope are in [FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md)
and [PROGRESS.md](PROGRESS.md).

The `--resolved-sql` runner also accepts `-Dquery.codegen.handwritten=true` as an
experimental reference. It replaces both native COUNT/SUM kernels with
[`handwritten_count_sum.c`](src/main/c/handwritten_count_sum.c), bypassing the typed
IR. The supported resolved shape is a fixed-width key with `count(), count(v),
sum(v), sum(w), sum(s)`, where `v` is LONG, `w` INT and `s` SHORT, using 32-row
batches. Input values, key distribution and filters remain arbitrary. Shape/layout
validation rejects incompatible bindings; this is a measurement kernel, not a
general backend. Results are labelled `handwritten-c`.

Clang 21 emits frontend IR with LLVM passes disabled; the existing LLVM O2 JIT,
host target, native ABI, slot scratch, Java insertion/growth, workers and merge
remain shared. This requires the source checkout and `clang-21`. It measures
headroom relative to a straightforward handwritten implementation of the same
algorithm, not a hardware lower bound or an alternative table layout.

For a wider execution-headroom experiment, `--resolved-sql` accepts `native-hash`
and `native-auto` in its implementation argument:

```sh
taskset -c 0-7 experimental/query-compiler/run-scan.sh --resolved-sql 3 high 500000000 0 5 native-auto 8 server
```

[`count_sum_paths.cpp`](src/main/c/count_sum_paths.cpp) is a handwritten, fixed-shape
COUNT/SUM reference compiled by `clang++-21 -O3 -march=native`, outside our IR/JIT.
`native-hash` adapts full LONG keys to QuestDB's existing Rosti SIMD probing and
growth, fusing the filter and five updates. `native-auto` measures the accepted key
range on every execution, then uses a direct array if all worker arrays fit within
512 MiB; otherwise it runs the same Rosti reference. No cardinality/range hint is
passed to either path. `query.codegen.keyStride=104729` exercises sparse, wide
domains without changing group count; `query.codegen.permuteKeys=true` changes
key order. Both properties also apply to the Java/LLVM controls.

Timing includes discovery, frame gathering, allocation, native worker creation,
scan, parallel merge, Java result conversion and release. Compilation and SQL
validation sorting are excluded. Every execution is compared exactly with SQL.
This reference supports only the fixture's `k>=0` filter, LONG key and LONG/INT/SHORT
arguments, direct native columns without tops, and an unpartitioned table. Its
native threads and allocations are benchmark-only: no engine cancellation or query
memory accounting, and overflow-sensitive parallel SUM equivalence is unproven.
This is not an alternative SQL backend or a claimed universal optimum. In particular,
the measured Rosti fallback loses to the integrated LLVM hash path; production
admission must retain that path and justify when dense aggregation pays.

The same runner accepts `duckdb` as an external SQL control. Put a released
`duckdb_jdbc` JAR on the core classpath; the prototype has no build dependency on it:

```sh
QUERY_CODEGEN_CORE_CLASSPATH="core/target/classes:/path/to/duckdb_jdbc-2.0.0.0.jar" \
  taskset -c 0-7 experimental/query-compiler/run-scan.sh --resolved-sql 5 high 500000000 0 5 duckdb 8 server
```

This reuses the fixture's data expressions, translates `long_sequence` to DuckDB's
one-based `range` and SHORT to SMALLINT, and checks every grouped value against
QuestDB SQL. DuckDB uses a temporary persistent database, checkpointed before
warmup, with its default storage/compression and optimizer. Only its execution
thread count is set for the default comparison. Runtime version, settings, native
storage layout, plan, loading and preparation times are printed. Query timing
includes complete JDBC result conversion into the same Java result representation
and cursor close; validation sorting stays outside timing. The current fixture's
sums fit LONG, although DuckDB's SUM result type is HUGEINT.

`-Dquery.codegen.duckdbPerfectBits=20` is a separately labelled diagnostic that
changes `perfect_ht_threshold`; it is **not** the DuckDB default. Key-order,
cardinality and stride properties apply identically to both engines.

Other sharded key shapes use the existing Java loop. The unsuccessful address-batch
bridge was removed, including its map resize listeners. Unsharded execution retains
its existing packed batch boundary. Late materialization and unsupported argument
layouts use ordinary accessors. Filter evaluation, non-keyed
aggregation and composed joins are not fused into this integrated kernel yet.
Preparation compiles updater and eligible probe kernels synchronously per factory,
without admission, background tiering or a shared code cache. The factory owns code
and slot scratch. No new executor, allocator, map implementation or result container
is introduced.

The existing harness measures this path with actual SQL and counts native rows:

```sh
# 5 warmups, 9 rotated samples, 500M rows, 7 workers plus caller.
# Batch 0 uses the configured default; server uses actual production configuration.
experimental/query-compiler/run-scan.sh --resolved-sql 9 count,mixed,distinct,selective,skew,high,distinct-high 500000000 0 5 '' 8 server
# Same-map single-slot diagnostic; no normal parallel scheduling in this mode.
experimental/query-compiler/run-scan.sh --resolved-batch 9 count,mixed,high 500000000 1024 5
```

The arguments after warmups force `java`/`llvm` (empty means both), set total
execution slots and choose `server` or `test` configuration. Server mode uses
`PropServerConfiguration`, including its query worker mode, sharding threshold and
page-frame settings; only the worker count and optional explicit batch size are
set. HTTP is disabled because this embedded harness starts no listeners and has
no server-distribution MIME file. Test mode retains the earlier
`DefaultCairoConfiguration` experiment. Preparation, first row, result consumption, close, process CPU,
faults and storage reads are reported separately; first row includes scan and
merge. Every result is compared exactly outside the execution timer. High
cardinality can activate sharding; `native-rows` counts compiled updates and
`map-rows` separately counts rows using the generated map probe. See the progress log for controls
and measured limits.

The three-way mode compares ordinary SQL, the integrated LLVM updater and the
standalone LLVM pipeline in one process, on the same table, SQL semantics, core
binary, configuration and CPU budget:

```sh
taskset -c 0-7 experimental/query-compiler/run-scan.sh --resolved-three-way 9 previous-mixed,mixed,previous-distinct,distinct,count,selective 500000000 0 5 '' 8 server
# Counterfactual: change only the sharding threshold to the earlier test default.
taskset -c 0-7 experimental/query-compiler/run-scan.sh --resolved-three-way 9 previous-mixed 500000000 0 5 '' 8 server 1000
```

`previous-mixed` and `previous-distinct` reuse the original `WorkloadMatrix`
definitions; `mixed` and `distinct` use the newer integration workloads. Every
execution checks the complete output against ordinary SQL. The mode order cycles
through all six permutations. Repeat in fresh processes to expose JVM/profile
variation. Compilation and comparison sorting are excluded from execution time;
allocation, build, merge, result collection and cleanup are included. Preparation
times, actual sharding and native coverage are printed separately. Native input
rows count the full pipeline's scan; native update rows count accepted rows in
the integrated updater. They are different measures. `NaN` marks an uninstrumented
phase or memory measurement, not zero cost.

The standalone path retains its own map, eight workers plus coordinator, and
512 MiB native-state budget; ordinary and integrated SQL use seven shared workers
plus caller. Process affinity bounds both to eight CPUs. This comparison measures
the implementations as a whole; it does not isolate fusion, code generation or
map layout. `QUERY_CODEGEN_CORE_CLASSPATH` can point all modes at one frozen core
JAR. An optional final threshold argument also works in the other resolved modes;
zero keeps the configuration default.

## Compiler boundaries

`ResolvedQueryCompiler` is the SQL integration path. QuestDB resolves functions and
owns frames, maps, worker scheduling and result cursors; `QueryCodeGenerator`
borrows the resolved planning inputs. `GroupByKernel`, `CompiledJoin` and
`CompiledProjection` describe their respective execution lifetimes.

`AggregateUpdates` binds aggregate state and emits initialization, updates, merge,
prefetch and finalization. `ResolvedAggregateKernel.Prepared` owns generated code;
`AggregateSlot` owns reusable scratch under an acquired QuestDB worker slot.
`QuestMapProbe` binds a map variant once. `NativeMapView` defines the borrowed
map descriptor shared by the maps and generated code.

The old model-based SQL adapter and its reuse demo have been removed. The
standalone DSL executors remain controls for backend and algorithm experiments;
they are not another SQL integration path. `KernelCache` still has focused lease
and eviction tests.

Established scan, filter, projection, map, ordered-key, expression, sort, Top-K and
parallel DISTINCT paths no longer have individual on/off properties. Remaining
experimental controls are `bounded`, `localCapacity`,
`sharedVarchar`, `prefetchRows` and `disablePrefetchHints` under `query.codegen`;
the first three select working-set/shared-state counterfactuals, the latter two
isolate batching and hints. `profile` and `probeCounters` enable instrumentation.
These controls are for experiments, not query eligibility policy.

## Reading the timings

`run-compiler.sh` measures uncached preparation plus the first execution of each
newly compiled query. Three unmeasured rounds initialize the compiler infrastructure;
they do not reuse generated query code. Backend order rotates and every result
is checked against SQL. It also prints the first sample per workload; the initial
AsmJit and LLVM samples include their respective bridge/engine initialization.
The runner compares AsmJit, C, LLVM O1 and LLVM O2.
Preparation includes lowering, scan-factory construction and backend compilation.
Execution includes state allocation, pipelines, result materialization and cleanup;
canonical comparison sorting is excluded for every backend and SQL. The ordinary
SQL baseline also parses and optimizes SQL; the physical DSL graph is already
constructed. Compare C, AsmJit and LLVM to isolate backend choice on the same graph;
these totals are not equivalent SQL frontend workloads. No automatic tier switch
is implemented.


Every warmup and measured result is compared with existing QuestDB SQL execution.
The demo rotates execution order and reports compilation separately. It reports
both total time and time excluding canonical result sorting used for validation.
Both include allocation, scan, aggregation, result copying and cleanup. The
second metric subtracts each sample's measured sort phase before taking its
median; SQL ORDER BY is absent from these workloads.

Phase timings separate building groups, copying output, sorting, and cleanup.
For the SQL baselines, cursor creation plus the first `hasNext()` builds the
complete group map. An unmeasured warmup counts adjacent descending output keys
before sorting to expose differences in the result order. Phase medians need
not sum to the total median.

The matrix compares QuestDB's scalar and normal paths with all three generated
backends (LLVM at O2), plus handwritten Java controls where available. It prints actual row,
group and non-null distinct-pair counts, growth counts and generated/control
peak allocation. SQL peak allocation is not instrumented. The separate reuse
runner measures factory creation, first execution and warm execution through
the SQL adapter; its totals include materialization and comparison sorting.
Its amortization figure is an illustrative first-run-excess / warm-saving estimate;
it omits intermediate warmup costs and is not a measured break-even point.

The original demo compares QuestDB's scalar and normal aggregation paths, a
handwritten Java control, generated C and LLVM O2 code. `distinct` runs both
flat and nested variants; `distinct-mixed` uses nested sets. The nested Java control calls the actual
`GroupByLongHashSet` implementation. The normal plan is printed. All run with
one execution thread and no background query workers;
this is not a comparison with a multi-worker QuestDB server.

The handwritten controls use the same hash, probing, state layout and phases as
generated code. Tests compare table bytes, or for nested sets every header field
and hash slot after each insertion (excluding addresses and unused padding).
In the original demo, controls receive the same explicit capacities sized from
the synthetic workload. In the matrix, controls and generated code start at 64
and grow using the same rules. This helps separate code generation from changes
to algorithms and sizing, although it still does not reproduce QuestDB's full
outer map or demonstrate a hardware bottleneck.
The dataset is synthetic; `distinct` exercises Q8's aggregation without its
top-10 sort. `distinct-mixed` adds SUM and COUNT, a subset of Q9 without AVG or
the top-10 sort. Vary the last two arguments to change group and pair cardinality.
Capture useful improvements and failed approaches in the progress log.
Compilation is reported separately; flat and nested kernels compile in the same
process, so their compilation times are not a cold-start comparison.
