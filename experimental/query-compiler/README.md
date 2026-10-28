# Experimental query compiler

This optional module generates native query pipelines inside QuestDB's ordinary
SQL execution. The aim is faster complete queries through specialization and better
execution strategies. ClickBench is a coverage/performance proxy; query names and
benchmark distributions must not determine code generation.

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
Unsupported plans keep ordinary execution.

| Responsibility | Code |
| --- | --- |
| SQL binding and capability checks | [ResolvedQueryCompiler](src/main/java/org/questdb/experimental/codegen/ResolvedQueryCompiler.java) |
| Shared expression and input binding | [ResolvedInputs](src/main/java/org/questdb/experimental/codegen/ResolvedInputs.java), [ResolvedVarchar](src/main/java/org/questdb/experimental/codegen/ResolvedVarchar.java) |
| Aggregate initialization, update, merge and finalization | [AggregateUpdates](src/main/java/org/questdb/experimental/codegen/AggregateUpdates.java) |
| Kernel preparation and ownership | [ResolvedAggregateKernel.Prepared](src/main/java/org/questdb/experimental/codegen/ResolvedAggregateKernel.java) |
| Acquired-slot scratch, resumable calls and publication | [AggregateSlot](src/main/java/org/questdb/experimental/codegen/AggregateSlot.java) |
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
All worker calls must finish before the code owner closes. Native callbacks record
allocation/cancellation failures for Java to rethrow; Java exceptions do not
unwind through native code.

Bounded aggregation retains partial states and can switch to compact input records
when local combining does not reduce work. Existing workers merge partitions.
Eligible LONG Top-K can subdivide large partitions and defer independent VARCHAR
extrema until after ranking. Retained transient strings are copied into query-owned
storage; stable frame payloads remain borrowed. These are eligibility-gated paths,
not promises that every aggregation uses the same strategy.

## Supported scope

The binders and focused tests are authoritative. Current families include:

- Integer expressions, comparisons, boolean predicates and constant membership;
  direct VARCHAR null and literal substring predicates; searched CASE string values.
- Direct and computed fixed-width keys, SYMBOL IDs, VARCHAR keys and composite keys
  using existing map encodings. NULL and empty strings remain distinct.
- Resolved COUNT, integer SUM, LONG DISTINCT, SHORT AVG and VARCHAR MIN/MAX
  implementations. Supported overloads depend on the selected map/update path.
- Numeric projection, final COUNT reduction, LONG full sort and Top-K, and positive
  LIMIT/OFFSET selection over supported generated results.
- INT inner-join probe pipelines with supported COUNT/integer SUM, post-join
  predicates and direct INT/LONG grouping. They borrow the existing frozen join;
  Java retains build and runtime ownership. Outer joins and general join-to-DISTINCT
  remain outside this binding.

Generic string construction, general string ordering and unsupported function
implementations retain ordinary execution. `compiledAggregateBatches` in EXPLAIN
proves aggregate binding, not complete generated execution for the entire query.
The ordinary `Async JIT` label refers to filter compilation.

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

Requires JDK 25, Maven, CMake 3.22+, a C++17 compiler, `cc`, and LLVM 21.1 development
files/shared library. The current build is exercised on Linux x86-64. Native
bridges build through the module's Maven lifecycle; AsmJit uses the pin in
`core/CMakeLists.txt`. LLVM belongs only to this optional module.

From the repository root:

```sh
mvn -q -pl core -am -Dmaven.test.skip=true -Djacoco.skip=true install
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
