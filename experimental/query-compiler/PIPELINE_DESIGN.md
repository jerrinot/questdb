# Query compilation pipeline: explicit stages and delayed lowering

Design written before implementation, 2026-10-25. This is an experimental
architecture for the existing operators, not a claim that the prototype is a
production competitor to Umbra. The north star is to compile QuestDB's proven
physical algorithms with their semantics and lifecycle intact, achieving robust
execution across data distributions while controlling compilation latency.

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
| [Menon et al., VLDB 2018](https://www.vldb.org/pvldb/vol11/p1-menon.pdf) | Relaxed operator fusion introduces bounded staging to expose SIMD and independent memory work. | Separate a pure filter stage from stateful consumers. Do not add speculative prefetching or change hash algorithms without a separate study. |
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
