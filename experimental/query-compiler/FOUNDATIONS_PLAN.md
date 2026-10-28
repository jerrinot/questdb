# Compiler experiment plan: own the parallel program

Updated 2026-10-28 against committed checkpoint `9df4de643d`. The immediate work
is the maintainability sequence below, before further operator expansion. E0 has
500M-row measurements, and E1's first adaptive partial-record representation and
E3's first bounded filter stage are implemented; see [PROGRESS.md](PROGRESS.md).
Older timings below still require a fresh matched control before another
optimization claim. Research and its limitations are in
[PIPELINE_DESIGN.md](PIPELINE_DESIGN.md#literature-reassessment-2026-10-27).

## North star and success criteria

Compile a resolved SQL query into a specialized parallel program whose state
representation, intermediate buffers and memory-access schedule fit the work and
hardware. LLVM should optimize code after those decisions have made the work
small. Generating instructions around a fixed general-purpose state representation
leaves a large part of this opportunity unexplored.

The architectural bet is **compiler-owned data structures and data movement**.
Java retains storage snapshots, allocation accounting, worker scheduling,
cancellation and lifetime management. Existing map layouts remain useful controls
and fallbacks; they need not be the permanent ABI of every generated operator.

Hardware generality is an acceptance requirement. Do not turn this Ryzen host's
cache capacity, prefetch distance or core count into a universal policy. Prefer
reducing bytes, dependent accesses and redundant work; obtain resource budgets
from the execution environment where a budget is needed. Test retained mechanisms
across core counts, cache-domain placements, state sizes and input distributions,
with portable backend correctness checks. These local sensitivity tests can
falsify overfitting, but cannot establish performance on other CPU families,
NUMA servers or architectures. Actual cross-machine validation remains required.

Our stretch target is another **2x over today's LLVM execution in substantial
families with measured headroom**, including complete build/merge/output/cleanup.
This is an experiment target, not a forecast or a requirement for bandwidth-floor
scans. For generated kernels using the same algorithm and layout, target within
10% of the best measured handwritten control. That control is an attainable
reference, not proof of an optimal algorithm or a hardware lower bound.

Keep the existing acceptance targets separately visible:

- At least 22/43 local ClickBench queries have every data-processing operator
  generated through LLVM, including merge, sorting, projection and LIMIT.
- At least 22/43 have repeatable warm speedups of at least 1.5x versus clean stock
  under the repository protocol. Report first-use latency and regressions too.
- Report time-weighted savings and performance across operator families and
  held-out distributions. Crossing 22 by tuning a borderline short query is not
  the architectural breakthrough. Compilation must eventually pay for itself
  on ordinary first use, as well as repeated execution.

The latest clean 2026-10-28 stock/LLVM/LLVM/stock refresh meets the original
combined gate: **22/43** queries have every data-processing operator generated
and at least 1.5x warm speedup across both process ranges. There are 30/43 complete
generated pipelines and 23/43 robust major wins overall. Generated final COUNT
reduction closes Q1/Q4/Q5/Q20; Q19 and Q40 exceed 1.5x at medians only.
All 516 responses validate, including independent stock checks of tied output
groups. Summed warm times are 22.533/10.437s; first-request sums, including
compilation once, are 34.076/27.106s under the repository's lukewarm protocol.
Compiler-timer sums are 0.168/3.959s; optional operator compilation now occurs
inside execution when needed, so use the complete first-request totals.
Q2/Q7/Q24/Q25/Q42 regress over 3%, by 0.264/0.709/0.039/2.607/0.180ms.
Q32 supplies 22.9% of saved time. All four processes pass the host gate. An entire
preceding refresh was excluded because unrelated JetBrains Toolbox CPU activity
overlapped its final stock pass. Stock settings remain unchanged. These local
eight-core results do not establish performance on the deleted EC2 machine.

The independent 500M-row string-state experiment reduces 8.39M-group execution
from 4.67 to 3.14s and memory traffic from 237 to 153GB by changing intermediate
representation and ownership. Optional-kernel deferral then removes ~90–100ms
from new small-state query latency without changing its warm loop. This clears
the initial coverage gate, not the stretch target of another 2x over LLVM.
The cache-footprint experiment confirms substantial state behind aggregate
pointers, but rejects more aggressive subdivision: its extra copying increases
traffic and complete latency. Compact winning-string references also lose: fewer
allocations introduce scattered dependent payload reads. Exact ranking before
independent string MIN/MAX is retained instead. Earlier accepted eight-core
profiles reduce this 500M-row family from ~3.1 to ~1.7s and controller traffic
150 to 86GB/query; guarded one/four-core checks show 1.66x/1.77x gains.
This schedule adds no ClickBench coverage and is not a cross-machine result.

This checkpoint completes the current experiment for the requested commit.
Wall-clock profiling is paused at the user's request while the desktop is in use. Later contaminated
and interrupted timing runs are excluded. Before making a broader claim, resume
controlled final-build measurements, including cross-cache placement, skew and
other architectures. The checkpoint is committed; the following maintainability
work is planned and does not resume performance experiments or profiling.
See [PROGRESS.md](PROGRESS.md).

## Immediate sequence: maintainability

### Success criteria

A cleanup must remove a demonstrated maintenance burden while preserving execution.
Passing tests is necessary, but does not establish that the design became simpler.
For each patch, identify the concrete before/after benefit in at least one of:

- **Fewer independently maintained facts:** a layout, capability decision or
  lifecycle rule has one authoritative definition and its old copies disappear.
- **Fewer states to reason about:** coupled fields or flags cannot disagree, and
  the change removes corresponding checks or coordination from callers. Moving
  the same nullable fields into a record is insufficient.
- **A smaller change surface:** name a realistic follow-up edit and show that it
  requires fewer independently coordinated edits or less unrelated code reading.
- **A clearer execution/lifetime path:** a responsibility has an identifiable
  owner, without adding forwarding layers or spreading its lifecycle across files.

Check the benefit against any new types, mutable state, dispatch, dependencies and
public API. Net line count and file size are supporting evidence, not targets.
If the tradeoff cannot be explained concretely, keep the current implementation.

### Guardrails

1. **One concern per reversible patch.** Preserve the existing dirty work. Capture
   the current implementation before editing; compare against that baseline.
   Do not combine cleanup with algorithm changes, heuristics, eligibility changes,
   new operators, bug fixes or unrelated formatting. Record discovered bugs separately.
2. **Prefer deletion, direct code and existing infrastructure.** A new helper or
   type must remove a real responsibility elsewhere or enforce a current invariant.
   No new configuration switches, forwarding-only wrappers, single-implementation
   strategy interfaces, registries, generic phase graphs, layout frameworks or speculative extension
   points. A single cohesive owner can justify a helper; hypothetical reuse cannot.
3. **Preserve contracts.** Keep SQL/null/overflow semantics, native layouts,
   allocation ownership, reset/close order, publication, cancellation, lazy
   compilation, fallback and worker scheduling. Trace producers and consumers
   before changing their boundary. Remove defensive checks only after proving
   that they are redundant under the actual callers.
4. **Protect execution cost.** No new per-row/per-batch allocation, dispatch or
   synchronization for abstraction convenience. Keep generated loops and data
   movement unchanged in cleanup. Query/slot metadata additions need an explicit
   benefit. Core changes must be narrow and retain no experimental dependency.
5. **Keep verification proportional.** Use existing focused tests for affected
   behavior and lifetimes; add a test only for an uncovered failure mode. When
   generation changes, compare representative LLVM IR, AsmJit streams and C output.
   Account for every difference. Matching generated code does not prove that Java
   orchestration or compilation costs stayed constant. No permanent golden-output
   archive or test suite that merely mirrors the refactored structure.
6. **Respect the profiling pause.** Correctness work runs at reduced priority.
   If accepting a patch depends on measuring a performance tradeoff, defer that
   patch until controlled profiling resumes. Performance experiments remain
   separate, with hardware evidence and the existing 500M-row workload requirement.
7. **Keep the process light.** Before each patch, state the code evidence, the
   smallest change and how it will be checked. Afterwards, report what became
   simpler, what new machinery was introduced and the validation limit. If the
   benefit fails to materialize, undo only that patch and choose another candidate.
   Finish when no concrete improvement meets these rules; do not manufacture work.

### Candidate work, subject to those criteria

- Finish consolidating duplicated operator control words where readers and writers
  can share the existing owner. Avoid inventing a descriptor system.
- Inspect `ResolvedAggregateKernel.Prepared`'s partition/ranking/replay combinations.
  Introduce a description only if it eliminates invalid combinations and caller
  coordination. Preserve late `prepareLongTopK` binding and `CompiledKernel.defer`.
- Consider extracting partition orchestration from `AggregateSlot` only if it
  produces a cohesive owner with fewer cross-boundary dependencies. Keep
  `runMutable`, allocation and scheduling in their existing owners. A smaller
  `AggregateSlot` alone is not success.
- Factor genuinely identical IR traversal using existing `SsaProgram.map` where
  appropriate. Preserve explicit effects, scope, evaluation order and exhaustive
  switches. Leave intentional backend-specific duplication alone.
- Shorten architecture documentation and the progress log while preserving current
  entrypoints, controls and evidence limits. Distinguish the standalone plan harness
  from SQL integration; audit callers before removing either.

The shared aggregate-definition/Java-emitter pilot in
[AGGREGATE_DESIGN.md](AGGREGATE_DESIGN.md#shared-definition-experiment) is compiler
research, outside this cleanup sequence. It requires its own experiment and
acceptance criteria; refactoring does not authorize a generator framework.

Layout cleanup completed so far (2026-10-28): partition fields belong to
`QuestPartitions`/`QuestRepartition`; VARCHAR extrema use `GroupByUtf8Sink` constants;
each slot calculates its state, scratch and key addresses once. Native layouts,
allocation sizes and page reuse are unchanged. Thirteen representative kernels
emitted identical LLVM IR, AsmJit streams and C bodies (39 comparisons). The latest
slot/sink checks passed 48 SQL, 18 kernel and 3 fused-filter tests. The earlier
partition slice passed page reuse checks. No performance claim follows from this.

Sort/Top-K/LIMIT control fields now belong to their existing operator classes.
Their Java writers and generated readers share those definitions: 51 generated
outputs and normalized `AggregateSlot` bytecode are unchanged; 39 focused tests
passed. README now separates current SQL integration from standalone controls in
223 lines instead of 785. A record wrapping the nullable partition kernels was
rejected: their construction already has one owner, and wrapping alone would not
remove runtime choices or caller coordination. No new types or runtime branches
were needed for this pass.

Variable-use collection now lives beside the IR definitions. Structured dead-local
analysis, SSA expression reads and liveness reads share it; the latter two no longer
rebuild expression trees just to discard them. Rewriting, scope validation and
effect classification remain separate. Thirty representative kernels match across
all backends with default and SSA lowering; 99 focused checks pass in each mode.
This removes unnecessary compiler allocations without a measured timing claim.

## What the evidence makes worth trying

| Retained observation | Implication for the experiment order |
| --- | --- |
| A 500M-row, 8.39M-group experiment created roughly 66M worker-local groups. For COUNT+SUM alone, 16 bytes of values per group means 128 MiB per worker, or 1 GiB across eight, before keys and capacity overhead. | Measure current replication and partial-state traffic, then attack representation and ownership. Bounded aggregation has since been added; do not assume the old amplification is unchanged. |
| On 500M dispersed VARCHAR rows with 20M groups, private LLVM took 5305ms and the adaptive shared path 2486ms. But shared execution remained about 10% slower on tiny state. Uncombined shared atomics were disastrous on skew. | Ownership can matter much more than instruction count. Existing shared and bounded paths are controls, not missing inventions. |
| A raw-partition experiment increased traffic from 43.0 to 51.8GB and execution from 696 to 848ms on the actual URL grouping workload. | Partitioning every row is not automatically an improvement. Compare bytes of partial states, lookup metadata and key ownership, not algorithm names. |
| Filter fusion improved 175.95 to 148.20ms on 500M rows, but the advantage largely vanished with smaller frames or a second LLC domain. | Buffer placement and cache residency are compiler decisions. Full fusion is one schedule, not the universal destination. |
| After the scheduling fix, actual grouped DISTINCT spent roughly 260ms in scan and 92ms in merge; parallel Top-K was about 10ms. | Another small-K sorting optimization cannot deliver a large whole-query gain there. Scan/state/merge deserve the experiment budget. |
| The backend has object dumps and native symbols; several paths eagerly compile alternatives, helpers are uniformly noinline, and the LLVM bridge serializes compilation. | We can inspect generated code now. First-use latency needs a separate measured compilation strategy, not an assumption that text parsing dominates. |

## Minimum compiler contract, introduced by experiments

Extend existing [PlanCompiler](src/main/java/org/questdb/experimental/codegen/PlanCompiler.java)
and [ScanPlan](src/main/java/org/questdb/experimental/codegen/ScanPlan.java) concepts
through the integrated [ResolvedQueryCompiler](src/main/java/org/questdb/experimental/codegen/ResolvedQueryCompiler.java)
hooks. The standalone phase planner already exists; a second executor or a wholesale
MLIR migration is not a prerequisite. Add only facts used by the active experiment:

| Contract | Why a transformation needs it |
| --- | --- |
| Logical aggregate fields and init/update/merge/finalize semantics, independent of physical offsets | Compare layouts without inventing different SUM/COUNT/null behavior. Start from AggregateUpdates and the resolved core functions. |
| State schema, owner, lifetime and physical properties | Choose private/shared/partitioned storage; preserve allocator ownership, stable references and final-reader semantics. |
| Batch columns with explicit row coordinates, selection and lifetime | Compose filters with original or compacted payload columns without reading the wrong row. |
| Memory regions with proven alignment, alias and mutation facts | Allow legal load hoisting/vectorization. Never manufacture noalias, inbounds, overflow or floating-point guarantees. |
| Phase dependencies and safe resume points | Switch code only after committed work; map growth, page release and cancellation cannot invalidate borrowed pointers silently. |

[Tidy Tuples](https://mediatum.ub.tum.de/doc/1638236/1638236.pdf) motivates a typed
code-generation API and compact SSA. [Low-Level Plan Operators](https://zenodo.org/records/5779772/files/loleops.pdf)
motivates explicit streams, reusable state and physical properties. These are
design mechanisms to test, not a requirement to reproduce either engine.

## Experiment sequence

Run one numbered experiment at a time. Within it, change one mechanism per arm.
The default sequence is E0 → E1 → E2 → E3 → E4 → E5 → E6. Reorder only when the
measured headroom or a failed experiment justifies it; record that reason briefly.
A failed bet should save implementation work, not generate another configuration
flag. E0's matched probe/update comparison found no material C-versus-LLVM gap.
Generated-PC profiling instead put about 35% of samples in partition flushing,
so the first E1 representation experiment targets intermediate records before
replacing the final map. Adaptive input partitioning now retains the dispersed-case
gain and local combining on the tiny-domain control. Next establish its sensitivity
to record widths, short frames and compilation/page-allocation costs before the
final-map representation experiments.

### E0 — Establish where the compiler leaves performance on the table

**Question.** Is the current gap caused by generated machine code, the state
algorithm/layout, or surrounding engine work?

Start with the existing 500M-row fixed-width fixture and its five aggregates:
`count(), count(v), sum(v), sum(w), sum(s)`, grouped by a LONG key with `k >= 0`.
Use a dispersed large group domain, initially 8.39M groups, without giving its
cardinality to the implementation. Consume every result. Use a tiny-domain control
only after the principal case is understood.

Compare clean stock, same-checkout Java, current LLVM and the independent
[HandwrittenCountSum](src/main/java/org/questdb/experimental/codegen/HandwrittenCountSum.java)
loop through the same integrated execution boundary where supported. Verify that
map type, growth policy, batching, null semantics and worker placement match;
otherwise report a phase comparison, not a compiler-quality ratio. Reuse
[ResolvedBatchMatrix](src/main/java/org/questdb/experimental/codegen/ResolvedBatchMatrix.java)
and its profile handshake.

The existing [CountSumPaths](src/main/java/org/questdb/experimental/codegen/CountSumPaths.java)
Rosti/direct-array reference is a second, algorithm-level control. It has a separate
native scheduler and restricted input support. Its timing cannot establish an
integrated engine win. Include discovery, initialization and output in its own
accounting; do not mistake its best result for an unconditional floor.

Measure complete latency plus scan/update, flush/redistribution, merge, final read
and cleanup; physical/partial groups created, bytes allocated/zeroed/copied,
controller bytes, cycles per input row and code size. Inspect the exact generated
objects. Use separate diagnostics for intrusive counters.

**Decision.** If matched C is materially faster, inspect its disassembly and data
flow before replacing the map. If both are close but a different state representation
wins, proceed to E1. If engine work dominates, target that measured phase instead.
No new compiler framework is required to answer this question.

### E1 — Generate a data structure for the query

**Hypothesis.** A query-specific state layout and probe loop can eliminate metadata,
padding, pointer chasing or unnecessary state loads that general maps retain.

**Initial experiment.** On 500M dispersed rows, the bounded local map creates
500M partial states: it combines nothing. Storing fixed-width aggregate inputs
instead of expanded per-row aggregate states reduces the tested record from
48 to 24 bytes. With the existing LLVM emitter, final maps, workers and allocators,
complete latency improves from 2.52–2.56s to 1.77s. With 1,024 groups, however,
the same change regresses 0.474s to 1.151s. This is an exploratory implementation,
not a default policy or a general breakthrough.

**Adaptive slice implemented.** The existing full-map boundary now compares bytes
of aggregate states with bytes of inputs for the observed aggregation window.
Unproductive combining switches to compact records for the rest of the frame;
the next frame starts locally again. A fixed-prefix sampler was rejected after a
misleading-prefix test exposed its blind spot. Both formats retain separate page
chains in the existing slot allocator and merge through the existing shard owner.
No new configuration switch or core scheduling infrastructure is involved.

Fresh 500M-row comparisons: dispersed 2.553 -> 1.752s; alternating low/high domains
1.675 -> 1.319s; tiny-domain reversed-order medians 0.480 / 0.479s. All results
match clean-stock oracles. Fourteen focused checks cover multiple key widths,
selective filters, mixed formats, misleading prefixes, cancellation, reuse and
both worker modes across the three backends. Details are in [PROGRESS.md](PROGRESS.md).

The automatic policy is now part of the experimental compiler for eligible
bounded fixed-width aggregates. Narrow COUNT/SUM controls also pass: with the
ordinary Unordered8Map choice, 24-byte states become 16-byte inputs and execution
improves from 1.89–1.92s to 1.38s on 500M dispersed rows. Smaller frames retain the
gain. An empty-filter control exposes about 10ms of extra preparation with no
execution benefit; partly filled pages add only about 1.3% to compact payload in
the large-domain case. These controls do not close every distribution or workload
case. Resampling occurs at frame boundaries after compact mode starts. This
intermediate-layout slice precedes the final-map layout/probe comparisons below
because of the measured phase budget; it does not establish an optimal hash table
or finish E1.

Moving active page counts into dense metadata removed the dependent header load
but did not improve complete or scan-phase latency; discard that experiment.
The smaller-frame control reduces traffic in both arms and motivates E3 next:
bound filter/consumer intermediates inside the existing frame rather than change
server frame settings. Keep the winning state representation fixed for that test.

Start with fixed-width COUNT/SUM and unchanged ownership/scheduling. First compare
current LLVM with matched handwritten code on the existing map. Then prototype a
compiler-owned layout with the same hash/probe algorithm and load policy. Only
then compare a tagged/control-byte probe algorithm, using the existing
[Rosti primitives](../../core/src/main/c/share/rosti.h) as the semantic/algorithm
reference. Keeping these steps separate distinguishes layout gains from SIMD/probe
gains. Key width, null representation and aggregate fields specialize the layout;
SQL text, known fixture ranges and query numbers never do.

[AggregateUpdates](src/main/java/org/questdb/experimental/codegen/AggregateUpdates.java)
currently binds concrete map offsets. Separate its logical field requirements from
field placement for this slice. Allocation, memory limits and cancellation stay
with existing owners. A generated final reader must expose the same SQL values.
If the prototype first converts to ordinary final maps, charge that entire pass;
a kernel-only gain that disappears at this boundary fails integration. Preserve
pointer validity on growth and keep unsupported states on their current path.

Next, and separately, compare current conservative LLVM memory operations with
proven region/alignment facts. Inspect optimized IR and assembly for the expected
removed reloads, vectorization or spills. Blanket alias promises or inlining do
not count as an optimization experiment.

**Prediction.** Fewer bytes per live group and dependent loads should reduce
allocation/zeroing, cache misses or demand-load stalls, with lower complete cycles
per row. A cache-resident small-domain control should show a different mechanism
from the large-domain case. SIMD control-byte probes must reduce examined payload
cache lines enough to pay for their extra instructions.

**Reject or narrow** if the best integrated result is within noise, new layout
conversion erases it, or lower instruction counts leave the limiting resource
unchanged. The stretch target is 2x on a state-dominated family; if the measured
phase budget makes that impossible, explicitly lower the local target and move
the architectural bet to E2 rather than polishing instructions indefinitely.

### E2 — Stop storing and merging the same groups repeatedly

**Hypothesis.** High-cardinality execution can approach one durable state per group,
without paying a full raw-row partition pass or a global atomic update per row.

Start from the winning E1 layout, or unchanged LLVM if E1 loses. Compare the
existing private/bounded algorithms before changing ownership. There are two
sequential alternatives, not a combined rewrite:

1. **Partition-resident entries with a bounded lookup directory.** Allocate new
   partial states directly in the existing partition pages, so directory reset
   does not copy every state out of a local map. This changes the copy boundary in
   [QuestPartitions](src/main/java/org/questdb/experimental/codegen/QuestPartitions.java).
   Preserve local combining; measure duplicate partial groups and final merge.
   A bounded directory does not bound all pages or prevent O(input rows) state.
2. **Shared group identification with local combining.** Generalize the existing
   [SharedVarcharMaps](src/main/java/org/questdb/experimental/codegen/SharedVarcharMaps.java)
   / QuestSharedVarchar experiment to fixed-width COUNT/SUM, only after matching its
   narrow COUNT control. Compare ticket assignment separately from counter storage
   and combining. Reuse worker slots, shards and allocators; include dynamic growth.

The first follows the placement mechanism in
[DuckDB's aggregation paper, §§V–VI](https://duckdb.org/pdf/ICDE2024-kuiper-boncz-muehleisen-out-of-core.pdf).
The second tests the specialized ownership argument in
[Global Hash Tables Strike Back!](https://www.vldb.org/pvldb/vol19/p523-marcus.pdf).
Neither paper establishes a universally winning ownership model.

**Prediction.** The partition variant removes local-map-to-page copying; the shared
variant reduces physical group replication, initialization and merge traffic.
Both may lose through extra indirection or synchronization. Collect bytes/input
row, partial groups/final group, merge work, coherence traffic, retries and worker
imbalance. Compare one CCD with the same number of cores split across CCDs; vary
uniform/skewed and clustered/dispersed data before widening key/state support.

**Reject or narrow** if raw/partial-state traffic grows without a compensating
critical-path reduction, or shared coherence costs erase the savings. Exact group
counts are not free presizing inputs. No unlimited-memory or skew-free winner
qualifies for automatic selection.

### E3 — Compile the memory-access schedule across operators

**Hypothesis.** Keeping intermediates within cache and overlapping independent loads
can beat both whole-frame materialization and an entirely scalar fused loop.

**First slice retained, 2026-10-28.** Numeric filtering of eligible bounded
fixed-width aggregates now feeds 16K-row stages using the executor's existing
selection buffer. Frames, worker scheduling and final maps are unchanged.
Selection IDs remain in frame coordinates, compact mode persists across stages
until the frame ends, and the existing failure/ownership contracts remain in use.
On 500M dispersed rows, COUNT/SUM improves from 1.38–1.40s to 1.16–1.18s with
roughly unchanged instructions; controller traffic falls about 54 -> 39GB/query.
Sparse/empty controls do not show a meaningful regression, and wider states and
changing distributions also improve. Twenty-three focused checks pass across the
three backends and both worker modes. This closes the first bounded-intermediate
slice; it does not complete cross-operator load scheduling or Parquet coordinates.

**Compact-record merge retained, 2026-10-28.** Actual-data profiling exposed a
serial probe loop when replaying compact inputs into final maps. Reusing the
existing staged probe operations and slot scratch reduces Q32 warm execution
from 1.22–1.25s to 1.09–1.11s, with lower directory-load latency and cycles despite
more instructions. The 500M-row single-key control is neutral; twelve focused
checks pass. This extends the access-schedule experiment to a pipeline breaker,
without new ownership, map algorithms or tuning switches. See the measured scope
and rejected first-row initialization experiment in [PROGRESS.md](PROGRESS.md).

Use the retained [FilterFusionExperiment](src/main/java/org/questdb/experimental/codegen/FilterFusionExperiment.java)
as the starting point. Hold the state implementation fixed. Compare whole-frame
selection, existing fused masks and bounded staged selection. Then, separately,
issue aggregate-input loads/prefetches during the existing key-preparation stage
in ResolvedAggregateKernel; sweep only a few justified distances/batch sizes.
A 64-row mask is not proof of 64 hardware SIMD lanes or vector aggregate updates.

Extend the integrated batch contract to support per-column row coordinates before
admitting compacted Parquet payloads. AggregateSlot currently rejects
PageFrameFilteredMemoryRecord, and the core fused route excludes late
materialization. Removing those guards or disabling late materialization would
change the work and can be incorrect. Native pages are the first performance case;
compacted/mixed pages are a separate integration case with decode costs included.

[Relaxed Operator Fusion](https://www.vldb.org/pvldb/vol11/p1-menon.pdf) motivates
bounded staging. Budget scratch and state together per cache-sharing domain.
Predict lower cache-eviction traffic for boundary changes, or shorter exposed
load stalls at similar bytes for load scheduling. Inspect register spills and code
size; lane conflicts in aggregate updates still require correct serialization.

**Reject or narrow** if prefetches merely add traffic, sparse inputs slow down,
or a gain comes only from changing server frame settings. Include sparse/half/dense
selection and one-/two-CCD placement as mechanism controls. The retained small-map
profile implicated input loads; it does not justify changing that map.

### E4 — Remove hashing when the domain permits it, safely

**Hypothesis.** Generated guarded direct addressing can make dense-domain aggregation
close to its input/update cost without requiring a wasteful unconditional range scan.

Reuse the existing direct-array reference and its discovery-inclusive measurements.
Compare hashing, full range-discovery plus arrays, and an online guarded array.
Use trusted domain metadata only where the code actually proves its meaning; a
sample cannot prove a min/max bound. A speculative domain checks every key before
indexing. An out-of-range key triggers migration of already-committed state and
resumes at the uncommitted row; it cannot replay the frame or lose prior updates.
Migration and initialization are part of query latency and the memory budget.

Start with integer keys and COUNT/SUM. Exercise late outliers, a domain that grows
throughout execution, sparse extremes, nulls and skew. A failed speculation should
fall back once rather than oscillate. Never allocate `max-min+1` without overflow
and total-worker memory checks. A range of 8.39M values is already 128 MiB of
COUNT+SUM values per worker, before occupancy and merge state.

**Prediction.** Removing hashes/probes reduces dependent loads and instructions;
state initialization and random updates may still dominate. At 500M LONG keys,
a preliminary key scan reads at least 4GB logically: about 67ms at an illustrative
60GB/s before other work. Online admission must repay its guards and migration.

**Reject or narrow** if sparse/outlier costs consume the dense-case saving under
honest admission, or the direct-array control is already at the measured resource
floor. No special knowledge of ClickBench key ranges enters planning.

### E5 — Remove complete passes through shared intermediate state

**Measured reprioritization, 2026-10-28: partition-local Top-K.** Q32's remaining
cost includes roughly 1.75M minor faults and 2.3 CPU-seconds in the kernel per
warm query. Retaining every final map creates this cost even though only ten rows
escape. An isolated fused prototype reaches 0.65–0.68s versus 1.09–1.11s; kernel
CPU falls to 0.2–0.3s and controller traffic from about 48 to 36GB/query. User-mode
cycles do not improve. This evidence moves state lifetime/consumer fusion ahead
of further instruction tuning. A 500M-row/8.39M-group control gains only 2–3%.

The concrete design uses existing owners and algorithms:

1. `LongTopKRecordCursorFactory` is the sole origin of the static ordering/prefix
   preparation, forwarded through column-only projections. Bind that demand to
   the aggregate compiler. Admit bounded fixed-width states, a small known K,
   and a ranking field that needs no deferred finalization. Variable/allocating
   states and unknown/large K keep their existing paths. HAVING filters do not
   forward the Top-K capability, so pruning cannot move before a HAVING predicate.
2. `GroupByShardingContext.mergeShard` already executes under a worker-slot lock.
   After `shardAll` flushes and closes the local maps, reopen that slot's existing
   fragment map as its merge workspace. Merge every source for this shard in the
   current order. Do not prune incomplete groups or assume a skewed distribution.
3. Run the existing generated Top-K against the completed workspace, then copy
   surviving keys and raw aggregate fields into the ordinary destination shard.
   Generate the copy/probe loop too. Borrow the existing slot allocator for the
   small pair buffer; its address is forgotten at clear before allocator release.
4. Finalize only the copied destination states, then clear the workspace while
   retaining its capacity for the next shard. The normal global Top-K consumes
   at most `NUM_SHARDS * K` rows without subdivision, or that bound multiplied by
   the chosen subpartition count. Its row addresses refer to destination maps that
   remain owned until cursor close, never to the workspace that will be reused.

Correctness follows from the existing disjoint-key partitioning: a group outside
its partition's top K cannot be required for a valid global top K under the same
ordering. Equal ranks permit any tied group, as SQL already does. No monotonic
aggregate or skew assumption is needed because each group is complete first.
Copy AVG's raw sum/count before finalizing once. Preserve map growth/resume,
query memory accounting, failure draining and factory/cursor reuse. Destination
presizing must describe retained rows, not pre-pruning cardinality.

**Retained after integration.** Actual-data ranking/payload checks, 500M unique
and repeated-group controls, a tiny-domain control, ascending/descending ordering,
null/zero keys and payloads, cancellation after copying and ordinary Top-K fallback
all pass. The final integrated Q32 comparison is 1,089–1,099 -> 664–671ms. The
500M unique composite-group control is 6.25 -> 4.40s; tiny-state execution stays
neutral. Twenty-four focused parameterized checks pass. Evidence and the validated
hardware study are under `/tmp/query-moonshot-actual-20261028`; see PROGRESS.md.
The matched full suite subsequently validated all 516 responses and 22 median
1.5x wins, with 21 robust across process ranges; Q40 remains borderline.

**Cache-sized merge follow-up.** The retained workspace still exceeds cache on
large partitions. Blanket global fan-out and an unread-row-based subdivision
policy both fail repeated-key controls. The replacement estimates distinct groups
with a 32KiB slot-local sampled HLL, merged after all scans join. It reuses
QuestDB's estimator; sampled hashes need a stronger finalizer because the map hash
alone biases dense domains. Estimates control execution only, never results.

For eligible partition-local Top-K, compare estimated state footprint with a
conservative per-worker share of the detected last-level data cache. Subdivide
only beyond a fourfold margin, into at most 64 suffix-hash buckets. Every source
contributes before selecting complete groups. Page reservation, query allocators,
map algorithms and merge workers remain shared with the existing path; page
chains are reused within the query and consumed only through their current tails.
Missing cache metadata keeps the unsplit path. The Linux cache topology and CPU
allow-list are sampled once; live affinity changes are not tracked by this prototype.

Normalized L3 misses fall 4.82 -> 2.59-2.61/input row and controller traffic
244 -> 120GB per 500M unique rows. The integrated actual-data comparison is
653-677 -> 328-334ms. Held-out 67.1M groups improve 2.25-2.28 -> 1.73-1.75s;
8.39M/16.77M groups remain within about 1%. Four-core unique-group execution
improves about 5.36 -> 3.76-3.78s. Eighteen focused generated-helper and SQL
lifecycle cases pass across LLVM, AsmJit and C. The fresh suite validates 516
responses: 28 generated shapes, 22 median 1.5x wins, 21 across process ranges.
The next measured family is VARCHAR substring filtering. The resolved compiler
now precedes the legacy serializer and shares bounded literal-search lowering
between standalone filtering and aggregate residuals. A 32-byte discriminating
pair reduces branch misses; equal-byte literals use exact run masks. First/last
probing and the equal-byte SWAR fallback both failed 500M-row adversarial controls
and were replaced. Protected-page, NULL, Parquet and lifecycle checks pass.
Actual count/filter execution reaches 192–193ms versus 326–337ms stock, with
57–58GB/s controller traffic against a 60.4GB/s read ceiling. The final full suite
must pass before expanding coverage claims; see PROGRESS.md.

**Hypothesis.** A compiler can save more by sharing an expensive preparation across
compatible consumers than by making each independent operator slightly faster.

Begin with an actual grouped-DISTINCT family already supported by
[DeferredDistinct](src/main/java/org/questdb/experimental/codegen/DeferredDistinct.java)
and AggregateUpdates. Audit what expression/hash/state sharing already happens.
Then prototype one compatible partition/dedup stream shared by outer grouping and
distinct-state processing, or by multiple DISTINCT consumers with proven identical
keys and equality semantics. Non-distinct COUNT/SUM must retain row multiplicity;
deduplicating their input changes the answer. Existing observations that flat
DISTINCT can lose near unique-pair cardinality are required counterexamples.

Use existing group/merge tasks and query-owned pages. Compare pass counts, bytes,
set allocations and group visits with the independent algorithms. Include the
longer intermediate lifetime and any lost parallelism. Keep states with incompatible
null/equality semantics separate. After finalization, test direct consumption by
Top-K only if its avoided result scan has material measured cost; do not select
unfinalized partial aggregates or assume LIMIT permits early scan termination.

**Prediction.** A successful transformation removes a measured pass or structure,
not merely a function call. Require a real admitted SQL example and a held-out
combination. If current bindings cannot express a useful legal shared consumer,
report that result and defer this bet; do not add unrelated operators solely to
manufacture a demonstration. General reusable buffers are justified by a winning
transformation, not by a feature checklist.

### E6 — Make specialization economical on first use

**Hypothesis.** Fast initial native code plus selective optimizing compilation can
retain warm execution gains without charging every query for all LLVM alternatives.

**First experiments completed, 2026-10-28.** Instrumented fresh preparation shows
LLVM optimization/emission dominate, not text parsing or serial mutex wait. A
smaller DISTINCT merge representation is retained; the production path remains
synchronous LLVM O2. Temporary balanced fresh-compilation experiments give:

| Pipeline family | Mode | Preparation, ms | Execution, ms | Complete request, ms |
| --- | --- | ---: | ---: | ---: |
| Filtered grouped DISTINCT | LLVM O2 | 232–235 | 67–70 | ~302 |
| Filtered grouped DISTINCT | LLVM O0 | 33–35 | 90–93 | ~125 |
| Filtered grouped DISTINCT | AsmJit | 12–13 | 80–81 | 92–93 |
| Short-literal substring/count | LLVM O2 | ~29 | 194–196 | 223–226 |
| Short-literal substring/count | LLVM O0 | ~10 | 278–281 | 288–291 |
| Short-literal substring/count | AsmJit | ~4 | 716–723 | 719–727 |

These are warmed-service, warm-data requests with fresh code each time on the
actual 99,997,497 rows. Complete request includes compilation once. They are
diagnostic compiler comparisons, not a new stock suite or a general backend policy.
LLVM O1 saves little on these two families. A global fast-backend switch fails.

A forced handoff experiment lets one existing worker compile O2 while others use
the initial kernel. Publication happens between native calls; old code remains
owned until all workers join. No new pool or asynchronous code cache is needed for
this experiment. LLVM O0→O2 gives substring/count ~214–216ms versus ~225ms O2, but
grouped DISTINCT ~266–270ms versus ~125ms O0 alone. AsmJit→O2 is ~240–253ms for
grouped DISTINCT versus ~93ms AsmJit alone. Promoting every used phase wastes work.
This forced policy is **not retained**. Mixed AsmJit/LLVM input counters do not
establish strict LLVM coverage. The LLVM-only comparison remains mandatory.

**Fast compiler correction, retained.** The initial AsmJit string result above
included scalar lane-mask expansion. Native SSE2 lowering reduces that execution
to 199–204ms and fresh complete requests to 198–202ms in a later balanced run;
preparation stays ~3.8ms. Eight 500M string controls improve, with exact results.
Forced AsmJit→O2 promotion then takes 205–208ms and loses to staying in AsmJit.
The remaining LLVM execution advantage is too small at the bandwidth floor.
This changes the admission evidence, not the selected LLVM production backend.
Do not add a tiering framework merely because fast publication works.

**Next admission experiment.** Expose explicit work progress at the existing
operator boundaries before introducing a policy. `Kernel.run`'s second argument
is not uniformly a row count: merge, Top-K and union kernels also use it for
control. A generic backend must not infer remaining work from that argument.
`PageFrameSequence` already caches frame row counts before dispatch; use this
snapshot for scan/filter work. A merge phase must supply its own entries/pages,
and a tiny finalization phase must not inherit the original table's row count.

First use measured initial-tier throughput and remaining work to reject promotion
whose compilation cost cannot plausibly be repaid. Include the lost worker during
compilation in that bound. Retain both tiers through query close, publish once,
preserve resumable status/state on map growth, and reset execution progress on
reopen. Measure compiler interference and early cancellation, not just function
pointer publication. Validate the mechanism with a 500M-row fixture before adding
automatic admission to the engine. Avoid a global executor, concurrent cache or
new user setting until an experiment demonstrates the need.

Extend the measured compilation stages to further families: binding/IR, SSA, text emission/copy,
parse/verify, optimization, code emission/linking, and mutex wait. Record both CPU
and elapsed time, instruction/block counts and emitted bytes. Current
[AdaptiveKernel](src/main/java/org/questdb/experimental/codegen/AdaptiveKernel.java)
compiles alternatives eagerly; [KernelCache](src/main/java/org/questdb/experimental/codegen/KernelCache.java)
is explicitly serial-use. They are not an existing concurrent tiering system.

Run the same IR/state through AsmJit-only, LLVM-only and AsmJit→LLVM at safe joined
frame boundaries. Initially switch code without changing state representation;
layout migration belongs to a separate experiment. The optimized tier is admitted
only when remaining work/reuse can repay compilation and resource interference.
Benchmark the compiler worker competing with query workers, queueing and cancellation.
Concurrent code leases and publication must cover every executing worker before
retirement; do not just call the serial cache from another thread.
Report rows and phases executed by each tier. An AsmJit-only or mixed-tier run is
not evidence that LLVM processed the entire query; retain the LLVM-only arm for
the strict coverage comparison.

Profile-directed follow-ups are separate arms: compact/direct SSA if frontend
construction dominates; selective helper inlining/reuse if duplicated optimizer
work dominates; compiler concurrency if mutex wait dominates. Preserve the retained
radix-helper benefit. Do not assume that bypassing LLVM text parsing fixes hundreds
of milliseconds of optimization work. [Adaptive Execution of Compiled Queries](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf)
provides the remaining-work model, not a ready-made threshold.

**Prediction.** First-use latency and wasted compiler CPU fall while long-running
execution approaches LLVM-only. Include cases too short to repay compilation,
cancellation before tier completion, repeated runs and competing queries. A
500M-row table with different selected work can test admission; smaller latency
controls are useful but do not satisfy the 500M-row execution-performance gate.
Reject tiering if its interference or state/lease machinery costs more than it saves.

## Napkin math before implementation

For each active query, write down its measured work and an optimistic bound:

- **Amdahl:** if a candidate affects fraction `f` of baseline critical-path time,
  infinite speedup there gives at most `1/(1-f)` overall. Eliminating a 10ms phase
  from a 366ms query gives only about 1.028x. Overlapping phase CPU times cannot
  simply be added as elapsed time.
- **Traffic:** `T >= controller_bytes / attainable_bandwidth` for the same core
  placement and read/write mix. Subtract/qualify system-wide background traffic.
  Reducing 43GB to 28GB at an illustrative 63GB/s removes about 238ms of traffic
  service, not a guaranteed 238ms of elapsed time. Bus activity and a counterfactual
  must establish saturation; matching a streaming rate alone did not do so in E3.
- **Memory-level parallelism:** `bandwidth <= outstanding_lines * 64 / latency`.
  For illustration, 60GB/s at 100ns requires roughly 94 outstanding cache lines
  system-wide, about 12 per core at eight cores. Measure latency/concurrency rather
  than assuming DRAM latency from every sampled cache miss.
- **Cache/state budget:** sum simultaneously live scratch, keys, lookup metadata,
  aggregates and partial pages for workers sharing each LLC. Worker count times
  state size is a lower bound only when all workers see all groups.
- **Compilation:** compare `C + N*E`, then account for overlap, remaining work and
  compiler contention in tiering. A 200ms compile premium for 20ms/run savings
  needs more than ten uses. Compilation that finishes after the query does not help.

## Measurement, correctness and generalization

Use the current Ryzen 9950X first: 16 physical cores, two 32MiB LLC domains. Start
with eight physical cores on one CCD, then use the same core count spread across
both CCDs to distinguish cache/coherence effects. Confirm topology, SMT siblings,
frequency and thermal behavior live. Scale finalists to one and sixteen physical
cores. The deleted EC2 instance is unavailable; many-core/high-bandwidth validation
remains a later portability check, not evidence already established locally.

Reuse the existing harnesses, native object/IR dumps, symbol maps and perf handshake.
Those symbols support fixed-lifetime captures now. Long-lived captures with address
reuse need timed load/unload registration; detailed operator lineage can be added
when attribution requires it. Full DWARF instrumentation is not a prerequisite for
E0. Measure profiling perturbation and report counters from separate matched runs.

Before a production-path optimization, complete the hardware-first skill's USE
inventory and validated bottleneck study: CPU, memory capacity, storage, network,
interconnect and imposed limits, with utilization/saturation/errors or explicit
irrelevance/unknowns. Require independent signals and a falsifying counterfactual
for the limiting resource. An exploratory prototype may test a hypothesis before
that diagnosis is complete; it must remain labelled as exploratory.

Execution-performance fixtures have at least 500M input rows. Start with one query
and one causal change; screen the handful of controls needed to discriminate it.
Then test changed seeds/order, skew, cardinality, key/aggregate widths and a different
query shape. Freeze a proposed selection rule before held-out validation. Reserve
expensive full-suite runs for accepted architectural changes, not every code edit.
Detailed hardware studies must eventually cover at least 80% of baseline runtime
and a representative of every family before a broad engine claim.

Keep clean stock, candidate Java and LLVM as distinct arms. Use identical affinity,
worker budget, data, server configuration and result consumption. Diagnostic frame,
layout or algorithm overrides must appear in both matched arms and be labelled;
stock defaults remain unchanged in acceptance runs. Include initialization,
discovery, growth, redistribution, merge, output and cleanup. Report compilation
separately and in first-use end-to-end latency. Distinguish client/network overhead
when showing HTTP timings. Faster candidate Java is a shared-core gain, not an
LLVM instruction-selection result.

Use at least three comparable baseline executions; normally three warmups and five
balanced paired samples for the active experiment, across fresh process blocks.
Extend only ambiguous comparisons. Normalize counters by input rows and relevant
work (groups, probes, selected rows); ensure event groups actually schedule and
report their running fraction. Inspect distributions, not just minima or IPC.

For ClickBench acceptance, keep its actual 99,997,497-row dataset and repository
protocol: one cache drop followed by three requests per query, server left running,
with separate complete process passes. Do not call the first request true-cold.
Run correctness/coverage counters separately if they perturb that protocol. The
500M-row mechanism experiments supplement this standard dataset; they do not
redefine it or turn a small synthetic test into a scale claim.

Correctness work stays proportional: exact complete aggregate comparison, with
SQL-aware tie/null/overflow/floating rules; focused tests for the changed ownership
or boundary contract, especially growth, cancellation, reopen and partially
committed state. Small correctness fixtures are appropriate. Do not build a giant
test matrix for every exploratory arm. Native counters verify all relevant data
processing; a plan marker alone does not establish complete LLVM coverage.

## Retention and stopping rules

Each experiment ends with one decision: retain, reject, or a precisely bounded
inconclusive result. A default candidate needs reproducible end-to-end benefit and
a mechanism consistent with the counters. A repeatable regression above 3% in an
affected family blocks unconditional adoption; smaller regressions remain reported.
A proven property-based admission rule can scope a win, but must survive held-out
inputs and include its decision cost. No query-name exceptions or new user toggles.

The moonshot bar concerns architecture. A small isolated win can remain if simple,
but it does not justify expanding a framework. If a mechanism has no substantial
headroom, stop polishing it and move to the next bet. Implement the minimum
integration needed to find out; delete failed experimental code when its finding
has been recorded.

Keep a short breakthrough/rejection entry in [PROGRESS.md](PROGRESS.md) and update
this plan's next action. Temporary profiles, object files and raw counters remain
outside the checkout, with reproduction commands and an evidence path in the log.
No new result archive, generic experiment platform or permanent tuning surface.
