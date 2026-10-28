# Query compilation pipeline: explicit stages and delayed lowering

Design written before implementation, 2026-10-25. This is an experimental
architecture for the existing operators, not a claim that the prototype is a
production competitor to Umbra. The north star is to compile QuestDB's proven
physical algorithms with their semantics and lifecycle intact, achieving robust
execution across data distributions while controlling compilation latency.

The original implementation sequence below is historical. The integrated path now
uses core page-frame workers, slot ownership and native map views. The
[2026-10-27 literature reassessment](#literature-reassessment-2026-10-27) separates
the remaining gaps from infrastructure already implemented.

## Evidence and assumptions checked in code

The 500M-row selective case takes 117.1 ms in LLVM P8 versus 26.1 ms in SQL8.
Forcing QuestDB's filter to scalar gives 44.7 ms. The generated machine code loads
key and aggregate columns on rejected rows. `Pipeline.lower` and
`PlanCompiler.emit(Scan)` both make that choice before LLVM sees the program.
The predicate is buried in the same loop as allocation, probing and aggregate
updates. A larger frame reduces dispatch overhead but barely changes execution.
These establish wasteful software work, not a calibrated bandwidth limit.

Current code contracts:

- `Pipeline` owns typed scalar/predicate semantics and aggregate update/merge
  definitions. INT/LONG use different null sentinels; BYTE/SHORT have no nulls.
  `SqlCompare` intentionally differs from null-rejecting `Compare`. `Add` uses
  wrapping LONG arithmetic and recognizes a sentinel result. Preserve all of it.
- `PlanCompiler` already establishes dependencies across aggregate and join
  states. Its scan/filter/project chains lose their structure too early.
- `AsyncGroupByRecordCursorFactory.aggregateFiltered` invokes a compiled filter,
  then passes selected row IDs to aggregation. Core's JIT generates vector masks
  and enumerates matching positions. We will preserve this filtering algorithm;
  changing how row IDs are represented does not change the aggregation algorithm.
- Native frames pin input columns through each call. Address zero denotes a
  column top, not an empty vector. Frame/chunk boundaries bound cancellation.
- `ParallelGroupBy` owns worker-local fragments and follows core sharding/merge
  rules. Input row order within each fragment matters for sentinel-overflow and
  state-layout tests. Selected rows must remain in ascending input order.
- The existing structured IR and SSA carry widths, types and control flow but
  expose only scalar loads. Integer address arithmetic is deliberately wrapping;
  there is no proven global no-alias contract.

## Literature and decisions

| Primary source | Finding relevant here | Decision and limitation |
| --- | --- | --- |
| [Neumann, VLDB 2011](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) | Data-centric compilation removes tuple dispatch and can keep values in registers across operators. | Keep fused consumers and dependency-ordered pipelines; compilation is useful only when the emitted execution strategy is good. |
| [Kersten et al., VLDB 2018](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) | Under matched algorithms, compiled and vectorized execution have different strengths; instruction count and latency hiding both matter. | Treat fusion and batching as physical choices. Compare with identical state algorithms, and do not equate fewer instructions with less elapsed time. |
| [Menon et al., PVLDB 2017 / conference 2018](https://www.vldb.org/pvldb/vol11/p1-menon.pdf) | Relaxed operator fusion introduces bounded staging to expose SIMD and independent memory work. | Separate a pure filter stage from stateful consumers. Do not add speculative prefetching or change hash algorithms without a separate study. |
| [Kersten, Leis, Neumann, VLDBJ 2021](https://link.springer.com/article/10.1007/s00778-020-00643-4) | Tidy Tuples, Umbra IR and Flying Start address code-generation structure and total query latency, with a fast backend plus LLVM. | Keep one semantic source and multiple backends. Retain AsmJit as the fast path; measure compilation as well as execution. |
| [Kohn et al., ICDE 2018](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf) | Execution modes can switch at morsel boundaries when they share state and calling conventions. | Keep the existing kernel ABI and aggregate layouts. Tier switching belongs at a joined runtime boundary, not within an aggregate update. Background compilation is a subsequent runtime change. |
| [Jungmair et al., VLDB 2022](https://www.vldb.org/pvldb/vol15/p2389-jungmair.pdf), [MLIR rationale](https://mlir.llvm.org/docs/Rationale/Rationale/) | Multiple abstraction levels preserve information until the appropriate optimization phase. | Add a small explicit scan-stage representation before scalar IR. Adopting the whole MLIR dependency stack is unnecessary for this prototype. |
| [Leis et al., SIGMOD 2014](https://db.in.tum.de/~leis/papers/morsels.pdf) | Scheduling pipelines over bounded morsels separates physical work from worker count and supports locality. | Keep batch size distinct from scheduling morsel size. Integrate with QuestDB's existing page-frame dispatcher later; do not introduce another scheduler. |
| [Raducanu et al., SIGMOD 2013](https://ir.cwi.nl/pub/21351/21351B.pdf) | Primitive implementations respond differently to selectivity, data and hardware. | Retain forced controls and compare alternative implementations at runtime. Measure adaptation overhead and response to distribution changes. |
| [Leis et al., VLDB 2025 retrospective](https://www.vldb.org/pvldb/vol18/p5531-viktor.pdf) | Cardinality errors, correlations and benchmark overfitting remain practical optimizer problems. | Use properties and observed masks for local decisions. No query-name rules, fabricated selectivities or new global join-order optimizer. |
| [LLVM vectorizers](https://llvm.org/docs/Vectorizers.html), [LLVM language reference](https://llvm.org/docs/LangRef.html) | Vectorization needs legal dependence/control flow and accurate memory semantics; unsupported calls and complex loops can prevent it. | Express a pure vector predicate explicitly. Keep unsafe loads out of tails and column-top paths. Never add `inbounds`, `nsw`, `nuw`, `noalias` or fast-math merely to improve code. |
| [AMD Zen 5 optimization guide, publication 58455](https://docs.amd.com/v/u/en-US/58455_1.00) ([AMD-authored PDF mirror](https://raw.githubusercontent.com/bitRAKE/amd_zen_sogs/main/58455/58455_1.00.pdf)) | Frontend delivery, dependency chains, addressing, cache locality and memory-level parallelism require different remedies; bit operations also have target-specific details. | Inspect actual code and measure fixed work. Let LLVM legalize vector width and select instructions for the host; do not hard-code Ryzen throughput assumptions into planning. |

Research PDFs and diagnostic measurements live temporarily outside the repository.
This document records decisions; `PROGRESS.md` records meaningful outcomes.

## Architecture

```
resolved physical operators (existing Pipeline / PhysicalPlan)
    -> scan-stage plan: source schema, bindings, predicate, dependencies
    -> physical scan strategy: unfiltered, eager scalar, late scalar, or masked
    -> typed control-flow IR with explicit lane-mask and bit-scan operations
    -> verified SSA / portable lowering
    -> LLVM vectors, or C / AsmJit scalar implementation
    -> existing frame runtime and unchanged aggregation state / merge kernels
```

There are three distinct granularities: a frame pins storage, a scheduling morsel
is the unit of worker ownership/cancellation, and a vector batch is the unit of
predicate evaluation. The compiler may change the last without changing the
first two. A batch boundary is not a global barrier.

### Scan-stage planning

Introduce an immutable `ScanPlan` for a maximal scan/filter/project prefix.
Expressions remain typed references to the original columns. A projection changes
bindings; a subsequent filter substitutes those bindings, so a predicate on a
derived expression can execute before unrelated projection values are loaded.
Adjacent filters combine in their existing order. This is legal for the current
pure, non-throwing fixed-width expression family. An extension must declare effects
before it is eligible; volatile functions, exceptions, strings and decoding cannot
silently acquire this treatment.

Projection substitution has a structural budget of 256 expression nodes. Shared
expression DAGs are counted with saturation; exceeding the budget retains a
materialized projection boundary. This bounds duplicated code for expressions
such as repeated `x + x`, independently of table names or estimated cardinality.
It is a compilation-complexity guard, not a tuned selectivity threshold.

The plan records predicate-column dependencies and output bindings. Validation
rejects a foreign column even if the expression is hidden under a projection.
There is no movement through a join or an aggregate, no change to build/probe
choice and no change to the logical query optimizer. Both standalone group-by and
composed plans use the same scan lowering, eliminating duplicate scan generators.

Values are expressions until a consumer requests them. For late and masked
execution, a compiler-time demand
pass emits the consumer into a region while collecting its input bindings into
a preceding prologue. Requested values are loaded exactly once at that stage's
entrance, after eligibility is established and before stateful probing/updating.
Scalar predicate values remain available to the consumer. This avoids reloading
columns across potentially aliasing state writes without inventing LLVM no-alias
facts, and lets independent key/value loads overlap. Unrequested bindings produce
no loads. The distinction between eligibility data and consumer data is explicit.

### Physical strategies and selected rows

An unfiltered scan uses a dense loop. A filtered scan has three legal alternatives:

1. Eager scalar: bind requested inputs in source-column order before testing the
   predicate, retaining the original execution strategy. It can overlap independent
   reads on dense data. A compile-time demand pass still omits unrequested columns.
2. Late fused scalar: evaluate the predicate, then load consumer data for the match.
3. Masked: evaluate a bounded block of predicates independently, then feed
   matching row IDs to the same consumer in ascending order.

The first mask representation is at most 64 rows in one integer. A zero mask
skips the block; set bits identify row positions. Bit scanning and clearing the
lowest set bit enumerate the selected rows. This follows core's filter-then-selected-
rows execution, but uses a different compaction implementation: core's BYTE path
uses branch-free unrolled index stores (`avx2::unrolled_loop2`), and its LONG path
can compress vector registers. Here indices are consumed immediately from the
mask. This difference must be measured rather than described as identical native
code. No payload columns or aggregate states are materialized at the stage boundary.

The representation allows future bounded arrays of row IDs for operators that
benefit from batched hash probing or prefetching. Those alternatives need evidence
before replacing immediate consumption. A full-mask dense-range specialization
was implemented and rejected: it duplicated the consumer, increased compilation
cost, hurt partial masks and failed to recover the eager loop's all-matching win.
Keep the proven eager strategy as an explicit alternative instead of treating
late loading or dense-mask specialization as universally beneficial.

Column-top legality is established before entering the vector loop. With present
predicate columns, full batches may load consecutive lanes. A missing predicate
column uses the scalar path with existing null/default semantics. Consumer column
tops are still guarded at the selected row; presence is not equivalent to
non-null values. The scalar tail handles fewer than a full batch and never reads
past a mapped page. Zero rows must not dereference any column.

### Runtime strategy selection (measurement-driven design addition)

The forced 500M-row matrix establishes that no implementation dominates: masks
win on sparse and random predicates, late scalar fusion wins on some correlated
predicates, and the original eager strategy wins on all-matching input.
Selectivity alone cannot explain the choice. Compile the legal programs when
they differ, and choose using observed cost at existing runtime boundaries.
Unfiltered programs compile only once.

Each compiled query has local feedback; cached machine code never owns query
feedback. Serial execution samples native chunks. Parallel execution chooses one
variant for every worker in a frame, measures the joined frame, and switches only
at that existing barrier. Independent worker exploration amplified its cost in
the initial matrix: a slow sampled chunk delays the whole frame. Coordinating
the choice avoids that amplification without adding a barrier or a scheduler.
All alternatives use the same ABI, row order and state layout. A chunk executes
exactly once, including failures: there is no shadow execution, rollback, replay,
or change to the aggregate algorithm. Native cancellation bounds stay unchanged.

Start with two unscored units per alternative, then five timed units per alternative
in alternating forward/reverse order (ABC/CBA for three). Compare median
nanoseconds per input row. Retain the current choice unless the challenger is
at least 10% faster, to avoid reacting to
small timing differences. Execute 128 units with the chosen implementation before
sampling again; these units need no timer. After initial exploration, compare
the current winner with one rotating challenger using five samples each in
balanced ABBA order. This bounds exposure to expensive losing variants; every
variant remains eligible for later reevaluation. Units below 4,096 rows do not train the
policy. The constants bound exploration and measurement overhead, not estimated
SQL selectivity. There is no query-name, cardinality or CPU-model rule.

This is online exploration, not an oracle: different units may contain different
data, allocations or scheduling interruptions. ABBA order and medians reduce but
do not eliminate those effects. Frequent changes, expensive losing alternatives,
and tiny queries can make adaptation a loss. Validate fixed distributions and
abrupt changes, and retain forced execution controls. Compilation eagerly pays
for all versions; asynchronous tiering and compilation-budget decisions remain
separate work. Code leases must cover every version and close safely on failure.

### Typed lane operations and effects

`VectorMask(lanes, predicate)` returns an I64 whose bit j is predicate(j).
Within this expression, an integer load reads lane j at
`baseAddress + j * elementWidth`; invariant values are broadcast. Arithmetic,
signed comparisons, Boolean combinations and null checks retain existing scalar
semantics. Width is explicit at the load; arithmetic remains wrapping I64 unless
the type system proves a narrower operation equivalent.

The verifier admits only pure arithmetic and consecutive integer reads. It rejects
allocation, callbacks, nested lane masks, pointer loads and dependent addresses.
All arms of vector selects must be safe to evaluate. Callers must establish valid
full-batch bounds and nonzero bases. The expression has a READ effect, so it is
never hoisted through state mutation or treated as a pure common subexpression.

`TrailingZeros(I64)` has defined zero behavior (64), avoiding backend-dependent
undefined results. Enumeration still checks for nonzero masks first.

LLVM lowers predicate arithmetic to vector operations and bitcasts the i1 lanes
to an integer mask. It may narrow and legalize those vectors for the host. This
does not depend on the loop vectorizer understanding the hash-table update loop.
Portable lowering expands the same mask semantics into scalar IR for C and
AsmJit. The latter remains a correctness/fast-compilation alternative; SIMD
performance on every backend is not claimed. No backend receives SQL names,
query IDs, aggregate definitions or assumed data distributions.

SSA preserves the lane-expression region instead of extracting its loads into
the scalar parent. Mapping, liveness, effect analysis, verification, dumps and
cache identity must all understand the new operations. This is essential: adding
only an LLVM special case would recreate the disconnected-operator problem.

## CPU reasoning and falsifiable predictions

For a sparse filter, predicate columns should be read for all rows; key/value
loads should execute only for matches. The mask path should reduce loop/control
instructions per input row relative to the fused scalar control. Expected outcomes
are lower instructions, cycles and wall time in the selective family. Full scans
should not acquire a filter stage. Dense predicates, long arithmetic predicates,
column tops and tiny inputs may lose; all must be measured or covered explicitly.

Do not infer DRAM bytes from logical column widths: cache-line fetches and
prefetching can fetch rejected neighbors. Do not infer bandwidth saturation from
the existing P4/P8 plateau. Relevant follow-up measurements are load latency,
MPKI, branch misses, frontend stalls and calibrated bandwidth, each normalized by
input rows. A new memory- or branch-bottleneck claim requires those measurements.

SIMD is intentionally confined to independent predicates. Concurrent updates to
the same group, pointer-chasing hash probes, allocation and DISTINCT growth retain
their ordered scalar algorithms. Vectorizing them requires conflict handling and
a separate correctness/performance design. Likewise, unrolling/prefetching may
increase register pressure, code size and cache traffic; they are not defaults.

The current host has system-wide I/O pressure, but previous timed query intervals
have no process storage reads or major faults. Treat this work as an exploratory
prototype implementation, with warm-cache fixed-work A/B measurements. No core
production optimization or unproven hardware-resource claim follows from it.

## Alternatives considered

| Alternative | Advantages | Reasons not to choose it as the architecture |
| --- | --- | --- |
| Sink two loads in the current scalar loop | Small change; useful control. | Leaves early lowering, scalar-only IR and duplicate scan generators intact. |
| Rely entirely on LLVM auto-vectorization | Less explicit vector code. | Stateful loops and callbacks obstruct legality; cannot convey SQL staging decisions after they have been erased. |
| Copy every intermediate into column vectors | Straightforward SIMD and operator boundaries. | Adds allocation/materialization and loses existing fused consumer benefits. |
| Call QuestDB's compiled SQL filter from a new parallel executor | Quickly reproduces a good filter implementation. | Adds another frontend/runtime seam; does not improve the common typed compiler or composed-plan lowering. Core's algorithm remains the model. |
| Add AVX-512 code to a particular aggregate/query | Fast local demonstration. | Wrong abstraction; duplicates semantics and ties the planner to a query and CPU. |
| Replace the compiler with MLIR/LingoDB | Mature multi-level infrastructure. | Large integration and lifecycle change before validating the required representations; preserve the architectural lesson first. |
| Choose vector execution from estimated selectivity alone | Cheap, no exploration. | The measured random/correlated cases have different winners. Runtime cost includes predicate, consumer and hardware effects; keep explicit alternatives and bounded feedback. |

## Implementation and validation sequence

1. Preserve the current executable as an A/B control outside the repository.
2. Implement scan-stage planning and shared demand-driven lowering. Exercise
   scan/filter/project composition, type/null behavior and joins/aggregates.
3. Add verified lane-mask/bit-scan IR, portable lowering and LLVM emission. Check
   scalar/vector equivalence, high mask bits, zero masks, signed widths, overflow,
   conjunctions, constants, column tops, tails, read effects and lifecycle.
4. Route both frontends through the common lowering. Keep worker count, sharding,
   allocation, merge, cancellation intervals and output conversion unchanged.
5. Run the existing suite and focused tests across LLVM, C and AsmJit, including
   LLVM optimization levels and the optional shared SSA path.
6. Run balanced 500M-row A/B controls: old eager scan, shared eager scan, late
   scalar scan, mask scan, adaptive execution and actual QuestDB. Cover zero/sparse/medium/dense selectivity, random or
   correlated predicates, multiple widths, expressions, mixed aggregates and
   repeated DISTINCT. Compilation and setup are reported separately; allocation,
   scan, merge, result conversion and cleanup remain timed. Check every result.
7. Inspect generated machine code and collect fixed-work counters for focused
   controls. Keep a simple progress entry with results and every regression.

## Production integration after this implementation

The same stage representation should eventually consume resolved QuestDB
functions and their shared aggregate definitions described in
`AGGREGATE_DESIGN.md`. Replace the experimental executor with core page-frame
tasks and slot ownership before claiming production scheduling/NUMA behavior.
Compilation tiers must share state layout and checkpoint ABI; code leases must
outlive all native calls, and failed optimization must retain the existing kernel.
Runtime measurements may inform tier choice, but must not reorder rows or change
semantic eligibility. These are explicit remaining runtime projects, not hidden
claims of this scan-pipeline implementation.

## Literature reassessment: 2026-10-27

Reviewed the primary papers' design, evaluation and limitations against commit
`4cc95d1e96`. This is a research synthesis and a proposed experiment sequence;
no execution or compilation improvement was measured during this review. Earlier
measurements are identified separately. Paper results describe their versions,
hardware and workloads, not present-day performance guarantees for QuestDB.

### Findings that change the architectural recommendation

The objective is to compile a resolved query into an efficient parallel program,
including its intermediate representations and state access. Generating a larger
fraction of instructions is not itself an objective. The most promising gains
remove repeated work, unnecessary materialization or expensive dependencies.

Umbra provides two distinct lessons that should not be conflated. Tidy Tuples
organizes code generators into layers but emits one compact SSA IR in a single
pass. Its layers are not a stack of materialized intermediate programs. Separately,
Umbra's low-level plan operators expose reusable buffers and their physical
properties. We can adopt a small amount of that planning structure without
replacing our backend, scheduler or SQL optimizer. Multi-level IRs remain an
alternative with their own costs, rather than a prerequisite for a serious compiler.

### Primary sources and their boundaries

| Paper and sections studied | Mechanism relevant to this prototype | Scope and implication |
| --- | --- | --- |
| [Neumann, *Efficiently Compiling Efficient Query Plans for Modern Hardware*, PVLDB 2011, §§3–4](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) | Compile-time producer/consumer calls construct fused runtime loops between materialization points. Generated code interoperates with precompiled C++ for complex engine operations. | A pipeline need not contain the entire query or regenerate every helper. Keep abstraction overhead out of the row loop while retaining runtime infrastructure. |
| [Kersten, Leis, Neumann, *Tidy Tuples and Flying Start*, VLDB Journal 2021, §§2–4](https://mediatum.ub.tum.de/doc/1638236/1638236.pdf) | SQL values, tuple operations and data structures compose through a typed API into compact SSA. Contiguous IR storage, direct SSA construction and a fast native backend reduce preparation cost. Selected host-language calls can be inlined by the optimizing backend. | Logical code-generation layers do not require multiple lowering passes. Helper reuse and specialization coexist. The published backend timings cannot be transferred to our JVM/FFM pipeline. |
| [Neumann, *Evolution of a Compiling Query Engine*, PVLDB 2021, §§2–5](https://www.vldb.org/pvldb/vol14/p3207-neumann.pdf) | Pipeline functions form a resumable dependency/state-machine structure. Value placement considers late loading and register pressure. The fast native backend makes LLVM an optional higher tier. | A single enormous function is not the destination. Preserve scheduling, suspension and code-size boundaries. |
| [Kersten et al., *Everything You Always Wanted to Know About Compiled and Vectorized Queries*, PVLDB 2018, §§3–7,10](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) | A matched implementation holds algorithms, structures and parallelization constant. Compiled execution reduces instructions; vectorized execution can expose more independent memory accesses. | Neither strategy dominates the tested workloads. Lower instruction counts or higher IPC alone do not establish a better execution strategy. Use the same-algorithm control alongside stock QuestDB. |
| [Menon, Mowry, Pavlo, *Relaxed Operator Fusion*, PVLDB 2017, §§3–5](https://www.vldb.org/pvldb/vol11/p1-menon.pdf) | Cache-resident tuple-ID buffers split fused pipelines into stages. Staging enables SIMD predicates and grouped prefetching ahead of dependent accesses. | Their placement heuristics use SIMD eligibility and estimated state size; they discuss runtime variants and code growth. Initial sensitivity studies are single-threaded on Broadwell. Stage placement and size still need validation on our hardware. |
| [Leis et al., *Morsel-Driven Parallelism*, SIGMOD 2014, §§2–4](https://15721.courses.cs.cmu.edu/spring2016/papers/p743-leis.pdf) | Dynamically scheduled pipeline work combines with local state and explicit merge phases. Aggregation uses bounded preaggregation and partition-wise completion; joins use a different state organization. | Morsel scheduling, vector batching and state ownership are separate decisions. Reuse core scheduling; do not derive aggregation ownership from the join algorithm or copy their morsel-size constants. |
| [Kohn, Leis, Neumann, *Adaptive Execution of Compiled Queries*, ICDE 2018, §III](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf) | Identical worker state allows execution modes to change between morsels without replay. Compilation admission estimates remaining work, compile time, speedup and workers consumed by compilation. | Remaining input is known at the considered pipeline boundary. Estimates can be wrong. Our current eager alternative compilation is not this tiering mechanism. |
| [Kohn, Leis, Neumann, *Building Advanced SQL Analytics From Low-Level Plan Operators*, SIGMOD 2021, §§3–4](https://zenodo.org/records/5779772/files/loleops.pdf) | Plans describe both streams and reusable buffers, including partitioning and ordering. Several aggregates can share preparation and select compatible hash or ordered processing. | Studied implementations are non-spilling and focus on complex analytics. Reuse requires matching keys, expressions and semantic properties; it is not permission to change aggregate semantics. |
| [Jungmair, Kohn, Giceva, *Designing an Open Framework for Query Optimization and Compilation*, PVLDB 2022, §§4–7](https://www.vldb.org/pvldb/vol15/p2389-jungmair.pdf) | MLIR dialects retain relational and scalar information at different abstraction levels, allowing staged transformations and cross-domain optimization. | Useful evidence for delayed lowering. Adopting MLIR or moving QuestDB's entire optimizer is a separate engineering decision, unsupported by our current measurements. |
| [Jungmair, Giceva, *Declarative Sub-Operators for Universal Data Processing*, PVLDB 2023, §§3,5–6](https://www.vldb.org/pvldb/vol16/p3461-jungmair.pdf) | Explicit state, references and accesses permit dependency analysis and transformations for local state, merging or synchronization. Navigation is separated from updating an entry. | Automatic parallelization needs a semantic conflict model. Our raw memory effects do not establish those properties; existing aggregate merge contracts remain authoritative. |
| [Wagner et al., *Incremental Fusion*, ICDE 2024, §§IV–VII](https://ir.cwi.nl/pub/34359/34359.pdf) | A finite set of suboperators generates both precompiled vector primitives and fused query code. Shared state permits switching between them. Hash collision resolution resides in runtime structures. | InkFuse's evaluation uses eight manually planned TPC-H queries and no SQL frontend. Enumerability constrains representation; its generic key-packing approach is not automatically the best layout for us. |
| [Gubner, Boncz, *Excalibur*, PVLDB 2023, §§3,5–7](https://www.vldb.org/pvldb/vol16/p829-boncz.pdf) | Cached fragments support changes in fusion, vectorization and other execution choices. Exploration has a budget tied to query progress. | The authors acknowledge limited functionality, high compilation latency and complexity relative to gains. Start with a few measured alternatives rather than importing the whole search system. |
| [Beischl et al., *Profiling Dataflow Systems on Multiple Abstraction Levels*, EuroSys 2021, §4 and evaluation](https://db.in.tum.de/~beischl/papers/Profiling_Dataflow_Systems_on_Multiple_Abstraction_Levels.pdf) | Code-generation lineage plus debug information maps native samples to IR, operators and pipelines. Shared helpers require caller attribution, addressed through stacks or register tagging. | This is more than symbol naming. The paper reports material sampling overhead, including 38% for its highlighted configuration; profiling machinery needs its own perturbation check. |
| [Neumann, Freitag, *Umbra: A Disk-Based System with In-Memory Performance*, CIDR 2020, §3](https://vldb.org/cidrdb/papers/2020/p29-neumann-cidr20.pdf) | String representations encode lifetime distinctions; pipeline setup, work, merge and cleanup are explicit execution steps. | Borrowed values cannot outlive storage ownership. Keep QuestDB page-frame and allocator contracts; this paper does not justify replacing its storage manager. |
| [Kuiper, Boncz, Mühleisen, *Robust External Hash Aggregation in the Solid State Age*, ICDE 2024, §§V–VI](https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf) | Bounded preaggregation uses compact salted pointer slots and places new entries directly in partition storage. Temporary pages share buffer management with persistent data. | A bounded lookup table can still produce intermediates proportional to input size. The memory budget must cover all live pages and concurrent partition completion, not just hash slots. |
| [Xue, Marcus, *Global Hash Tables Strike Back!*, PVLDB 19(3), 2025, §§2–4](https://www.vldb.org/pvldb/vol19/p523-marcus.pdf) | Specialized concurrent ticket assignment separates group identification from aggregate updates, offering alternatives to partitioning and replicated state. | Main experiments use 100M synthetic pairs, SUM and perfect presizing, with resizing studied separately. Neither ownership model wins universally. Our 500M-row, dynamic-growth, mixed-state controls are still required. |

### What is already present, and what the code does not yet express

The compiler is not starting from scratch. These are current code observations,
not inferred deficiencies from comparing feature lists:

| Current implementation | Consequence for the next design |
| --- | --- |
| [`PlanCompiler`](src/main/java/org/questdb/experimental/codegen/PlanCompiler.java), `CompiledPlan` and [`ScanPlan`](src/main/java/org/questdb/experimental/codegen/ScanPlan.java) already describe dependency-ordered phases and scan/filter/project bindings in the standalone path. | Reuse this work. The missing step is a coherent integrated contract, not another standalone executor. |
| [`ResolvedQueryCompiler`](src/main/java/org/questdb/experimental/codegen/ResolvedQueryCompiler.java) binds resolved core operators through separate hooks. [`ResolvedAggregateKernel.prepareFilter`](src/main/java/org/questdb/experimental/codegen/ResolvedAggregateKernel.java) adds a fused variant explicitly for the experiment. | LLVM sees the emitted kernel, not the whole set of runtime boundaries. Cross-kernel materialization decisions must happen before lowering. |
| `AggregateSlot.updateFiltered` now consumes numeric selections in 16K-row stages for bounded fixed-width aggregates, using the executor's existing selection buffer. `ScanLowering.selectionRange` retains original frame row IDs. | Storage/task frames and cache working sets are separate. Adaptive compact-record mode persists until frame completion; a stage boundary must not restart the aggregation policy. Compacted Parquet coordinates still decline. |
| [`ResolvedAggregateKernel.emitPrefetched`](src/main/java/org/questdb/experimental/codegen/ResolvedAggregateKernel.java) already stages keys and candidate entries. `QuestPartitions` already supplies bounded-state flush/merge kernels. | Staging and bounded aggregation are implemented. Improve their composition and admission; do not present them as new missing algorithms. |
| [`AggregateUpdates`](src/main/java/org/questdb/experimental/codegen/AggregateUpdates.java) binds exact core function implementations, state offsets and lifecycle operations. | Preserve these as the semantic source for compilation. General sharing/reordering needs additional proven contracts, especially for nulls, overflow, DISTINCT and floating reductions. |
| [`AggregateSlot.canUpdate`](src/main/java/org/questdb/experimental/codegen/AggregateSlot.java) declines `PageFrameFilteredMemoryRecord`; ordinary filtering can use original row coordinates while decoded payloads are compacted. | A batch contract must identify each column's row coordinates and lifetime. Removing the guard without that contract is incorrect. |
| [`AdaptiveKernel.compile`](src/main/java/org/questdb/experimental/codegen/AdaptiveKernel.java) eagerly compiles distinct alternatives before its execution-time policy selects them. | This explores execution strategies but does not hide compilation or budget it against remaining work. |
| [`LlvmBackend.compile`](src/main/java/org/questdb/experimental/codegen/LlvmBackend.java) lowers structured IR to SSA, emits text, copies it through FFM, and the [native bridge](src/main/c/llvm_backend.cpp) parses, verifies and optimizes under a shared mutex. | Measure time and allocation by compilation stage. Text parsing and Java IR allocation are candidates, not established bottlenecks; backend optimization may dominate. |
| [`LlvmEmitter`](src/main/java/org/questdb/experimental/codegen/LlvmEmitter.java) marks every IR helper `noinline`. Ordinary memory accesses carry `align 1` and integer-derived addresses. | Current helpers deliberately control code growth. Any richer inlining or memory policy must preserve that win and prove its facts; neither blanket inlining nor speculative `noalias` is justified. |
| The [LLVM bridge](src/main/c/llvm_backend.cpp) dumps objects, optimized IR and numbered native symbols. | Instruction-level inspection exists. Operator lineage and time-aware code-lifetime registration remain missing from this path. |

### Proposed compiler boundary

The smallest useful common representation should retain:

- Typed values and their null/overflow behavior, including which resolved
  function supplied the semantics.
- Sources, consumers and dependencies, with dense ranges, lane masks or selected
  row IDs at their boundaries. A materialized projection may change row coordinates.
- State schema, physical properties, ownership, lifetime and legal update/merge
  operations. A borrowed pointer and a query-owned value are different contracts.
- Explicit places where execution may suspend or switch code, without replaying
  an update or retaining pointers invalidated by map growth or page release.

Initially this can be an extension of the existing planning/binding objects.
Introduce a new IR only when a concrete transformation needs a representation
that those objects cannot express cleanly. Keep the resolved SQL frontend and
core worker, map, allocator and cancellation owners.

Separate mechanism from policy. The representation makes fusion, bounded staging
and state reuse legal to express. A later measured policy chooses among a small
set of alternatives. The papers do not supply a universally correct cost model.

### First-principles checks before selecting an experiment

Our completed [filter-fusion experiment](FILTER_FUSION_EXPERIMENT.md) provides a
specific example. With 500M rows and 50% accepted rows, an eight-byte selection
vector accounts for 2 GB of writes and 2 GB of subsequent reads at the logical
level. Eight workers each holding selections for a 1M-row frame have about 32 MB
of live row IDs, competing with other data in a 32 MiB shared LLC. Reducing frame
size removed most of the measured fusion advantage. Logical bytes are not DRAM
bytes, and that experiment did not establish DRAM saturation.

For a future staged implementation, budget the full simultaneously live set:

`live bytes ~= workers * (batch rows * scratch bytes per row + local state bytes)`

Account for which workers actually share a cache. Include keys, hashes, candidate
pointers, payloads and any nested DISTINCT state. A batch size alone is not a
working-set budget. Small intermediates may improve independent memory access;
large ones may evict useful input and state.

Likewise, replicated aggregation consumes at least `workers * groups * state
bytes` when each worker sees every group, before keys, spare capacity and merge
buffers. A shared table trades that replication for synchronization and coherence.
Neither side is free. The old raw-partition counterfactual already lost through
extra traffic; literature does not override that result.

Compilation has a separate break-even test. For repeated execution, a simplified
necessary comparison is `reuse count * execution saving > extra compile cost`.
For illustration only, 200 ms of extra compilation and 20 ms saved per execution
require more than ten executions to improve total latency. For background tiering,
include work completed during compilation, the worker it consumes, remaining
input, and any cache/bandwidth interference. Report both first-execution latency
and warmed reuse.

### Sequence of falsifiable experiments

The following was the initial research shortlist. The active order and detailed
experiment cards now live in [FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md), which
prioritizes compiler-owned state layout and ownership after a matched headroom
comparison. Use that plan for execution; the list below records the narrower
questions raised by this literature review. Each execution comparison uses at
least 500M input rows, stock and same-checkout Java controls, exact semantic checks,
and held-out inputs.

1. **Make the existing evidence easier to attribute.** Add compilation-stage
   timings and carry pipeline/operation identity through existing IR/object dumps.
   Extend native registration to preserve code lifetimes before using long-lived
   processes with address reuse. Use debug information for instruction provenance
   where possible. Verify profiling perturbation; do not build a new profiler or
   reserve a tagging register merely because a paper did so.
2. **Unify one integrated source-to-sink region.** Start with the existing
   scan/filter/group-by family and its current map algorithm. Represent its row
   coordinates and bounded intermediates explicitly. Compare full-frame selection,
   bounded selection and fused masks while holding state ownership and scheduling
   constant. Prediction: cache-pressure cases reduce controller traffic and
   cycles; cache-resident controls should have much less benefit. A gain explained
   only by changing global frame configuration does not validate the compiler.
3. **Improve load scheduling inside that representation.** The retained IBS data
   points to aggregate-input and key loads, not primarily the small group map.
   Test issuing aggregate-input prefetches in the existing preparation pass before
   proposing another hash table. Prediction: lower demand-load stall cost at
   comparable useful work. Unchanged stalls, extra traffic or sparse-input losses
   falsify the proposed benefit. Keep this separate from the boundary experiment.
4. **Make reusable state a planning property.** Identify a real query family where
   multiple consumers repeat partitioning, ordering or deduplication. Compare
   independent existing algorithms with a shared intermediate using the same
   semantics. Prediction: remove a complete pass or structure, measurable in
   input visits, allocations and bytes. Longer state lifetimes and lost parallelism
   can erase the saving. Do not add operators solely to manufacture a demonstration.
5. **Make AsmJit-to-LLVM tiering economical.** Use shared state and existing safe
   checkpoints, with code leases covering running workers. First measure the
   current fast-backend quality and per-phase compilation costs. Compile only when
   remaining work or reuse can repay it. Compare short, medium, long and concurrent
   workloads; the success metric is complete latency, including losing exploration.

Bounded versus shared aggregation ownership remains a separate subsequent study,
using the implementations and controls already present. It should follow evidence
of replication, merge traffic or coherence costs in the selected workload.
Similarly, compact IR storage, direct SSA construction and stronger proven memory
facts are compiler experiments to prioritize from profiles, not a blanket rewrite.

The moonshot acceptance criterion is substantial end-to-end improvement across
operator families and distributions, including compilation and cleanup. Supporting
LLVM for at least half of ClickBench and producing major gains for at least half
are different targets. Preserve both coverage and time-weighted reporting; neither
a higher compilation coverage count nor one large synthetic win satisfies that goal.

### Resolved short-literal substring filtering (2026-10-28)

The stock LIKE implementation already resolves case-sensitive `%literal%` patterns
of 1–8 UTF-8 bytes to `ConstContainsSwarVarcharFunction`. Bind that exact resolved
function and copy its literal bytes; do not parse SQL, reinterpret escaping, accept
ILIKE, or substitute semantics for arbitrary functions. Share conjunction binding
and string lowering between standalone selection/count and aggregate filtering.
Numeric leaves remain eligible for existing masks before pure string residuals.

Hardware evidence: roughly 2.7 branch misses and 344 instructions/input row, with
24–26% bad-speculation slots and 37% frontend underdelivery. Most samples are in
the existing row filter with inlined SWAR search. Specializing SWAR alone saves
instructions but scarcely changes branch misses. A 32-byte candidate mask followed
by exact bounded verification reaches about 195ms instead of 330ms, at 57–58GB/s
versus a measured 60.4GB/s read ceiling. The detailed gate is
`/tmp/query-moonshot-actual-20261028/string-study.json`.

Use the first byte and the last byte differing from it, following the same
non-degenerate probe principle in [Rust's substring implementation](https://doc.rust-lang.org/stable/src/core/str/pattern.rs.html#1833-1850)
and [Muła's SIMD search](https://github.com/WojciechMula/sse4-strstr).
Blind first/last probing regressed repeated-text controls and is rejected.
One-byte literals need only one mask. Repeated-byte literals intersect shifted
byte-match masks to detect complete runs; the block stride overlaps by the literal
length minus one. Their SWAR fallback regressed a 500M-row control and is rejected
for long slices. Short slices still use specialized SWAR. The final vector overlaps the
previous vector within the slice; exact 8/4/2/1-byte verification never overreads.
A 64-byte policy loses on shorter strings; no hardware or SQL-name tuning surface
is introduced. Guard-page checks cover all three backends.

The existing standalone JIT ABI carries data and auxiliary pointers but omits the
native/Parquet frame representation. Add a Java overload carrying the borrowed
`PageFrameMemory`; existing JIT implementations delegate to their unchanged native
ABI. All engine call sites supply their already-pinned frame, including latest-by
slices. The LLVM adapter builds the same bounded VARCHAR descriptors used by
aggregate filtering, using the caller's shifted auxiliary pointers and row count.
A small per-frame allocation initially owns this adapter storage and is freed in
`finally`; it is included in timing. Do not add thread-local ownership or a second
workspace pool without evidence that this allocation matters.

Try resolved compilation before the legacy serializer. Only a declined resolved
predicate needs legacy serialization and bind-variable link functions. Preserve
all existing factory adoption, failure cleanup, Java fallback and frame-level
cancellation. Count-only residuals must be tested before incrementing the count;
selection preserves source row order. Native coverage and exact results are
required for both paths, mixed numeric/string conjunctions, NULLs, column tops,
Parquet slices and reopening.


### String-state ownership and representation experiments (2026-10-28)

[Umbra's storage paper, section 3.1](https://www.cidrdb.org/cidr2020/papers/p29-neumann-cidr20.pdf)
distinguishes persistent, transient and temporary strings; materializing transient
strings requires copying. This matters more than merely adopting its 16-byte
header. QuestDB already exposes the needed lifetime fact:
`Utf8Sequence.isStable()` promises immutable bytes for the query lifetime, and
`PageFrameMemoryRecord.writeNativeVarcharView()` exports the native/Parquet
stability distinction. `Pipeline.Utf8Value` carries it. Preserve that contract.

A future compact MIN/MAX state may borrow stable payloads and own unstable ones,
but today's LONG value slot names a GroupByUtf8Sink. Ordinary Java readers and
fallback updates require that exact layout. A new physical state therefore needs
an explicit reader/fallback or final-materialization boundary; replacing the
pointer silently is invalid. The 500M control currently spends ~238GB/query on
memory traffic in the original sink-per-partial implementation and creates ~67M
worker-local groups. Typed input partitions subsequently reduce traffic to ~153GB;
representation and replication deserve a larger experiment than another
instruction-only reduction.

[Schmidt's Umbra thesis, section 4.5.3](https://db.in.tum.de/~schmidt/papers/master-thesis.pdf)
describes a small lookup table referencing partitioned output streams, followed
by a partition-local merge. This supports testing compact materialization and
cache-sized ownership; it does not prove that unconditional partitioning wins.
Our prior raw-partition regression remains a counterexample. Extend existing
QuestPartitionInputs/QuestPartitions if the measured payload/lifetime model fits,
rather than introducing a separate scheduler or buffering framework.

[Optimistically Compressed Hash Tables and Strings, ICDE 2020](https://ir.cwi.nl/pub/30644/30644.pdf)
separates frequently accessed state from exceptions and describes a bounded
dynamic string dictionary. The relevant lesson here is to distinguish hot bytes
from total allocation size and account for the work needed to achieve locality.
Its domain compression requires valid bounds; its dictionary benefits depend on
repetition. Neither is an unconditional policy for our inputs.

The isolated 24-byte borrowed-view experiment was rejected: it replaces copies
with dependent reads of scattered winning payloads and increases complete-query
latency. Keeping a longer prefix recovers only part of the loss. More aggressive
subdivision also loses because it copies more records. Ordinary sink layouts and
the existing cache policy remain unchanged; see PROGRESS for measured outcomes.

### Rank before independent output aggregates, 2026-10-28

The retained experiment changes the schedule at an existing materialization
boundary. For bounded fixed-width group keys with a direct, known small LONG
Top-K consumer, compute numeric aggregates exactly for every complete group.
Initialize string MIN/MAX slots to NULL without updating them. Existing generated
Top-K selects each partition's exact candidates into ordinary destination maps.
Replay the same partial-state and compact-input pages, using read-only probes of
those selected keys, and update MIN/MAX only on matches. Final output uses the
normal GroupByUtf8Sink layout. The union of local Top-K sets contains the global
Top-K; no approximate rank, sign assumption or inferred key bound is involved.

The consumer contract matters: a post-aggregation filter boundary does not forward this
Top-K preparation, and finalized/deferred rank fields are excluded. Only bound
VARCHAR MIN/MAX updates are delayed. Input expressions were already evaluated
into page records or partial states, so replay does not re-evaluate expressions
or rescan the base table. Source allocators, retained transient payloads and
snapshot-stable input bytes remain alive. Subdivision replays only each child's
own records. Existing cancellation, memory accounting and cleanup still apply.

Each occupied register in the existing sampled cardinality sketch establishes a
lower bound of one distinct sampled group. Enable replay only when this lower
bound exceeds K; otherwise use ordinary merge kernels. This conservative guard
avoids an extra pass for small partitions without a new setting or hardware
constant. It is not a guarantee of row selectivity under skew. Optional kernels
compile only when used, and their cost remains inside first-result latency.

[Selective Late Materialization, PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4616-liu.pdf)
motivates choosing materialization points from attribute dependencies and access
costs. [Cache-Efficient Top-k Aggregation, PVLDB 2023](https://www.vldb.org/pvldb/vol17/p644-siddiqui.pdf)
studies candidate reduction with safe bounds. This experiment retains exact
ranking of all groups and delays only independent output work; it does not import
that paper's sampling or pruning assumptions. Local 500M-row results support the
schedule, but skew sensitivity and other CPU architectures remain open.

### Optional code ownership, 2026-10-28

Partition operators retain pure IR builders until their first invocation. A
factory-owned `CompiledKernel` publishes compiled code once under synchronization;
its backend remains reachable until that factory closes. The existing worker join
still precedes resource release. This preserves the same typed IR, state ABI and
operator semantics while omitting unused compiler work. Factory preparation time
alone is no longer total compilation cost: first-result latency includes lazy
compilation inside execution. This is demand-driven preparation, not background
tiering or a prediction of future query benefit. The measured tradeoffs are in
[PROGRESS.md](PROGRESS.md).
