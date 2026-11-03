# Whole-query compiler experiments

2026-11-03. Active targets and experiments; the measured checkpoint is below.
This sequence supersedes the remaining local-loop sequence in TODO.md and the dated
execution order in FOUNDATIONS_PLAN.md. Retain their evidence and semantic contracts.
Run one mechanism at a time. Keep targets separate from achieved results.

## Destination and goals

Compile resolved SQL into an efficient parallel program: choose the work, intermediate
representation and state lifetime before LLVM optimizes instructions. QuestDB retains
SQL semantics, snapshots, workers, cancellation, memory accounting and result ownership.
Existing algorithms and layouts are the starting point and controls; they are not a
permanent restriction on a measured physical-strategy experiment.

Join acceleration, including multiway joins, remains a goal. SSB is a control and
a source of optimization opportunities. Its star shape, key distributions and query
count must not define the architecture or the implementation backlog. A mechanism
may specialize for proven properties such as bounded key ranges or independent join
inputs; admission must establish those properties and preserve an alternative when
they do not hold. Generality does not require every optimization to win on every input.

Targets for this cycle, measured against a fresh, frozen current LLVM baseline B0:

| Goal | Acceptance evidence |
| --- | --- |
| Broad new performance | Set an ambitious complete-query target for the selected join mechanism before implementation, based on measured recoverable work. Demonstrate gains on at least two independent 500M-row controls, varying the properties that explain the gain. Report SSB totals and per-family results as controls, without an SSB speedup quota. |
| Complete generated execution | Make build, probe, filtering, aggregation, merge and ordering compose through reusable contracts where measurements justify the work. Report strict all-operator coverage separately from speedup; do not add operators solely to reach an SSB query-count target. Java scheduling/allocation is allowed; Java data-processing loops do not count as generated. |
| Generality | Validate claimed support beyond SSB: independent and dependent join inputs, key types/dictionaries, sparse and wide domains, duplicates, skew and selectivity. Aggregate placement must also cover SUM, AVG, MIN/MAX, COUNT(value), mixed aggregate lists and grouping, with NULL/overflow/rounding controls. MIN and MAX are primary aggregate families: validate each independently as well as in mixed lists. Type coverage alone does not establish aggregate generality; generated expansion and successful fallback do not establish work elimination. Use relevant controls for each mechanism, with explicit rejection/fallback for unsupported properties. SYMBOL/LONG and dependent-chain composition remain architectural candidates; a multiway improvement need not also improve a single-table query. |
| Preserve previous gains | Refresh the original ClickBench combined gate: at least 22/43 queries both fully generated and robustly at least 1.5x faster than stock. Preserve the existing aggregation, dictionary and ranking controls. |
| Economical execution | Measure complete first requests, compilation CPU, generated code size, peak live query memory and RSS separately. A repeatable control regression exceeding both 5% and 1ms requires correction, narrower semantic/property-based admission, or rejection. First-request tradeoffs are reported even when warm execution wins. |

The former 2x/seven-query SSB targets are historical experiment targets, superseded
by this scope clarification. Preserve their results and unmet status. Do not switch
back to an old, slower baseline or drop difficult controls. Strict generation coverage
and performance remain separate ledgers; a fast partial pipeline is still partial.

Use DuckDB as an external control for remaining headroom, with its version and storage
settings pinned. Report geometric-mean ratio, absolute gaps and per-family losses;
beating a historical stock plan is not sufficient evidence of competitive execution.

## Current facts that constrain the design

Latest SSB control, 2026-11-03: installed LLVM sums **2.690s** against freshly rerun
uncompressed DuckDB **2.252s** across all 13 queries, 600M rows/eight physical cores,
six warm executions each. LLVM takes **19.4%** more summed time (geomean time ratio
**1.213x**) and wins three queries. Repeating four DuckDB cases with warm I/O gives
2.244s; Q4.3 retains small reads, and the ~20% gap persists. All 210 outputs/order
checks pass. Largest absolute gaps: Q4.1, Q2.1, Q3.4. Q4.1 spends ~437ms in probe/
aggregation, ~7ms building and <1ms merging. All SSB queries use SUM, so the recent
COUNT-forest folding does not apply. Strict full generation remains **0/13**.
Stock was not rerun; the historical checkpoints below retain their original scope.
See [current SSB comparison](target/ssb-current.md).

Execution checkpoint, 2026-11-02: **W0 complete; first W1 predicate boundary closed.**
Fresh four-way SSB sums: 240.925s stock, 197.933s current LLVM-off, 4.829s frozen LLVM
B0 and 2.262s DuckDB. LLVM-off is not a matched physical plan. Hardware profiles cover
84% of B0 runtime and pass the resource/hardware gate. Resolved constant ranges now
use the existing typed predicate compiler: paired all-13-query sums improve
**4.817 -> 3.330s**, geometric mean **1.43x**. Only three queries improve 1.5x;
strict full SSB generation remains **0/13**. The original SSB cycle targets were not achieved.
Independent selective 500M-row controls improve conservatively **2.1x / 3.9x**;
all-match improves **1.4x**. Five focused tests pass.

ClickBench retention is **27/43 both fully generated and >=1.5x stock in both passes**;
coverage remains 34 full / 4 partial / 5 ordinary. Five original 500M-row H2O controls
also pass complete output validation. Q1.2 gains track instructions **51.2 -> 6.4**
and branch misses **0.97 -> 0.13** per input row with nearly unchanged memory traffic.
LLVM compile cost grows about 31ms; first-request latency still improves.
See [whole-workload evidence and limits](target/whole-query.md).

Initial W2 experiment: **static shared bitmap policy rejected**. Pooling distinct-key
storage and guarding construction with known probe counts improves the all-13 SSB sum
**3.325 -> 2.866s**, geomean **1.15x**, but Q3.4 regresses **8%** at eight cores and
**17%** at one; it improves **1.29x** at sixteen. The guarded policy fixes the thin/empty
construction regression, but application cost still depends on execution resources.
The existing chooser loses its old membership option when that representation changes.
No production source or bytecode was changed; these gains do not advance retained goals.
See [runtime-filter evidence](target/runtime-filter-economics.md).

Retained W2 successor: **preserve both membership representations** in the existing
frame chooser, sharing its two compiled loops. The full B1/current pair improves
**3.327 -> 2.867s**, geomean **1.154x**, and Q3.4 returns to baseline. Two balanced
500M-row control passes show selective gains without repeatable regressions above the
retention threshold. Installed classes pass 52 focused checks. No additional operator
coverage or new policy constants. See [representation-choice evidence](target/runtime-filter-choice.md).

Fresh cumulative cycle score: **4.764 -> 2.823s**, geomean **1.640x** over frozen B0,
with **six/13** queries above 1.5x. Strict generation remains **0/13**. The former
2x/seven-query and complete-generation targets were not met; they no longer determine
the next implementation. ClickBench's previous 27/43 combined gate was not remeasured
in this join-only slice.

The chooser still misses Q3.4's forced winner at sixteen cores by about 17-25%, so W2's
within-10%-of-best aim is not achieved. Next selection experiment: distinguish noisy
frame wall timing/hysteresis from shared resource coupling. Compare forced whole-query
strategies with sampled costs while varying worker/owner concurrency at fixed affinity;
only then test coordinated strategy windows using existing dispatch/drain boundaries.
Include transition, sampling and first-request costs; do not add a CPU-count rule.
Reproduce the selection problem on independent join controls before retaining a new
coordination policy; a Q3.4-specific winner or threshold is insufficient evidence.
W1 generated build/merge/order remain structural goals. Dependent SYMBOL/LONG
composition is now implemented. Build-only membership transfer is retained: selective
500M-row typed chains gain another 2.72-4.08x with preparation included. This removes
repeated failed lookups; bounded exact dependency links now share successful lookup
results too. All-match INT/LONG/SYMBOL gain another 1.49/1.73/1.59x, including
preparation, with mixed LONG+SYMBOL composite controls gaining 1.46-1.57x.
No-match input still pays ~1.8ms of unnecessary preparation (+2% across six pairs).
See [membership transfer](target/join-dependency-filter.md) for the previous
all-match variance and [cached dependency evidence](target/join-dependency-links.md)
for that checkpoint. COUNT now folds unobserved duplicate leaves using the existing
chain-count algorithm without destroying ordinary links. Two-leaf typed branches
gain 1.64-2.11x; three-leaf controls gain 2.91-3.52x, with grouped COUNT gaining 1.70x.
The next slice folds closed COUNT forests, including producer duplicates, through
cached heads and the existing heap recurrence. Typed branches gain another 2.76-2.99x;
a deeper mixed-type forest gains 3.83x. Observed build values and mixed-source keys
retain ordinary processing. Compilation and no-match first requests still have costs.
See [leaf evidence](target/join-multiplicities.md) and [subtree evidence](target/join-subtrees.md).
Query-tracker peaks omit filter row-ID lists; complete memory accounting is open.

Checked against the checkout, not inferred from historical feature lists:

| Fact | Consequence / existing code to extend |
| --- | --- |
| The integrated compiler enters through separate core hooks. Standalone dependency and scan planning already exists. | Extend [QueryCodeGenerator](../../core/src/main/java/io/questdb/griffin/engine/QueryCodeGenerator.java), [ResolvedQueryCompiler](src/main/java/org/questdb/experimental/codegen/ResolvedQueryCompiler.java), existing candidate/metadata and ScanPlan as needed. Do not build another SQL resolver or executor. |
| `analyseMultiway` accepts typed/composite independent joins and dependent inner joins in the existing optimizer order. | [HashJoinGroupByCandidate](../../core/src/main/java/io/questdb/griffin/HashJoinGroupByCandidate.java) retains source identity and later key dependencies before pruning payloads. This does not introduce a new join-order optimizer. |
| Compiled key indexes address the scan followed by earlier build payloads; typed lookup borrows existing tables/map views. | [CompiledJoin](../../core/src/main/java/io/questdb/griffin/engine/join/CompiledJoin.java) and [ResolvedJoinKernel](src/main/java/org/questdb/experimental/codegen/ResolvedJoinKernel.java) lower dependent probes inside producer duplicate loops. LONG remains full-width; SYMBOL retains its source dictionary. |
| Generated joins now reuse ordinary SYMBOL translation and staged-key storage. | Reuse [SymbolKeyTranslator](../../core/src/main/java/io/questdb/griffin/engine/join/SymbolKeyTranslator.java), [MapHashJoinBuild](../../core/src/main/java/io/questdb/griffin/engine/join/MapHashJoinBuild.java) and HashJoinBuildContext. SYMBOL IDs from different dictionaries are not directly comparable. |
| Builds expose exact row/key counts and size after freeze; INT bounds are published, wider bounds can be obtained by a bounded key scan. | [FrozenHashJoinBuild](../../core/src/main/java/io/questdb/griffin/engine/join/FrozenHashJoinBuild.java) supplies execution facts. They cannot retrospectively justify an earlier build-orientation decision. |
| Single-join interval orientation already has an execution-time choice; its counts ignore residual row filters. | Extend or reuse [HashJoinGroupByBuildChoiceRecordCursorFactory](../../core/src/main/java/io/questdb/griffin/engine/table/HashJoinGroupByBuildChoiceRecordCursorFactory.java). Unknown is not zero; interval counts are upper bounds after filtering. |
| Join atoms own existing build/probe/merge phases and phase timers. | [AsyncHashJoinGroupByAtom](../../core/src/main/java/io/questdb/griffin/engine/table/AsyncHashJoinGroupByAtom.java) is the existing owner/barrier, not grounds for a new scheduler. Its CompiledJoin updater does not expose the aggregate compiler's generated merge/ranking interface. |
| Finalized-map aggregation, generated aggregate merge and numeric ranking already exist. | Reuse [ResolvedMapAggregate](src/main/java/org/questdb/experimental/codegen/ResolvedMapAggregate.java), [AggregateUpdates](src/main/java/org/questdb/experimental/codegen/AggregateUpdates.java), GroupByKernel and ResolvedAggregateKernel. Their state/read contracts constrain reuse after joins. |
| Single-join COUNT replaces duplicate chains with multiplicities. Generated multiway COUNT can fold unobserved leaves and closed build forests. | `FrozenHashJoinBuild.copyKeyRowCounts` / `sumKeyRowWeights` reuse HashJoinRowHeap's backward-link recurrence; `ResolvedJoinKernel` proves source eligibility, `JoinDependencyFilter` prepares weights transactionally, and `AggregateUpdates` uses the existing COUNT state. Observed build values still require ordinary processing. |
| Alternatives are eagerly compiled; feedback is thread-local within a compiled kernel. LLVM compilation is serialized in the native bridge. | [AdaptiveKernel](src/main/java/org/questdb/experimental/codegen/AdaptiveKernel.java) and [llvm_backend.cpp](src/main/c/llvm_backend.cpp) require measured compile economics. No assumption that background tiering is cheap or immediately safe. |

The November 2 checkpoints above supersede the October 30 ClickBench snapshot and
the pre-fusion SSB suite for their respective workloads. Preserve frozen B0 when
assessing cumulative gains; do not replace it with historical stock.

## Query-wide planning facts, introduced only when used

Keep two kinds of information distinct:

1. **Semantic facts:** typed source bindings, dictionary identity, join dependencies,
   expression effects, ordering requirements, and legal aggregate state operations.
   These prove that a transformation preserves results.
2. **Execution facts:** snapshot rows or upper bounds, completed build cardinality,
   observed uniqueness/ranges, state width/allocated bytes, and measured work reduction.
   These help choose among already legal physical alternatives.

Reuse HashJoinGroupByCandidate/Metadata and execution-owned build/slot state. Add only
fields read by the current transformation; do not copy every fact into a second catalog.
Every fact needs an origin, validity scope and an explicit unknown state. Data observed
in one execution cannot become a semantic guarantee for the next. Reopen invalidates
snapshot-dependent dictionaries, pointers and counts. Per-query budgets include all
concurrent states, not independent per-operator allowances.

The owner chooses at existing preparation or drained-task boundaries and publishes
immutable descriptors to workers. No process-global mutable optimizer state, new worker
pool, per-row policy callbacks or shared hot counters. Changing a key dependency must
also change legal lowering: a later join depending on an earlier match cannot be
hoisted into common-probe membership filtering.

Active implementation slice: [general generated joins](JOIN_GENERALIZATION_PLAN.md).
Typed probing now covers INT, full-width LONG, translated SYMBOL and fixed composites
through the existing builds/maps/workers. Independent and dependent joins share the
typed contract. Dependent 500M-row controls improve 12.2-24.2x versus the previous
ordinary plans, including parallel admission; generated execution is 1.49-3.18x faster
than the same fused Java plan. See [dependency evidence](target/join-dependencies.md)
and [typed controls](target/join-generalization.md). These are not new SSB/ClickBench scores.
Generated build/merge/order and broader work elimination remain open. Selective
prejoining wins with construction included, but all-match materialization loses;
next test build-side pruning and bounded dependency reuse before adding operators.

## W0 — Refresh the whole-workload baseline (completed)

**Question:** where is the remaining time, and which boundary can change enough of it?

Freeze source, classes, native bridge, configuration and data identity for B0. Reuse
the existing SSB fixture, run-case harness, phase timers, native-row counters, object
dumps and result oracles. The retained entry points are documented in README; the SSB
runner is currently `/run/media/jara/test/query-ssb-control-20261031/run-case.py`.
The current harness must walk all relevant factories, not infer execution from EXPLAIN.

Compare frozen stock, current core with no optional compiler, B0 LLVM, and pinned
uncompressed DuckDB. Current Java separates some core/planner effects from compiler
effects, but is not automatically a matched-plan control: multiway admission requires
compiled support. For an optimization claim use B0 versus candidate with the mechanism
isolated; add a matched handwritten control only where backend quality is in question.

Start with all 13 original SSB queries at eight physical cores, first plus three warm
executions with complete output. Rotate process order. Repeat decisive pairs in reverse.
Refresh all 43 ClickBench queries with their repository cache-drop/three-execution
protocol; retain its best-hot convention separately from SSB medians. Reuse a compact
H2O control set spanning arrays, low/high cardinality, VARCHAR and ranking. Avoid
repeating the 500M-output storage stress case for unrelated changes.

Produce one current table: actual operator execution/admission, first/warm distributions,
CPU-seconds, phase times, live memory/RSS, bytes moved, physical I/O and compilation.
Strict time-weighted coverage is the fraction of baseline query time attributable to
queries meeting the all-operators criterion; it is not the same as sampled native CPU
share in partial queries. Show both with their denominators. Aggregate worker times
must not be added to wall time as though phases were serial.

**Gate:** USE/resource controls, result validation and fixed-work hardware studies
cover at least 80% of SSB baseline runtime and a representative of each major execution
family. Use one-core diagnostics to separate work removal from parallelism. Rank
opportunities by recoverable complete-query cost, not fallback count. If a phase is
5% of execution, deleting it entirely can save at most 5%; this bounds its priority.
W0 changes diagnostics only, not production execution.

## W1 — Make connected operators compose

**Hypothesis:** loss of type/dependency/state information at factory boundaries is
forcing ordinary row processing or repeated materialization that dwarfs local-loop costs.

Take the highest-cost affected family from W0. Run one of the following slices at a
time; report any reordering of this sequence from the measured opportunity table:

- Extend the existing common-probe region through its admitted expressions and into
  generated finalization/merge/order, reusing current aggregate/ranking implementations.
  A necessary blocking boundary stays explicit; compilation does not imply one giant loop.
- Express an acyclic dependent chain (`F -> D1 -> D2`) in existing source metadata.
  Lower a later lookup from the correct earlier match, preserving duplicate nesting,
  payload lifetime and every resume position. Do not apply independent-key filtering to it.
- Admit SYMBOL, then LONG join keys through their actual engine contracts. SYMBOL
  preparation/translation is charged to the query and refreshed on reopen. Sparse,
  disjoint and differently numbered dictionaries are required controls. LONG hashing
  follows the existing full-width/staged map representation.
- Close measured build/merge/sort boundaries through reusable contracts. Generated
  build insertion must use existing serial/partitioned ownership, duplicate chains,
  growth exits and publication. Frozen lookup views alone are not a mutable-build ABI.

**Arms:** B0; candidate with exactly one boundary removed; forced ordinary boundary
with otherwise comparable planning where available. Keep the data structure and worker
budget fixed for the first comparison. A small-group sort may improve coverage without
material speedup; label it as such, and keep it separate from the performance arm.

**Predictions:** fewer row-access calls/materialized bytes or repeated hash probes,
lower cycles/input, and often better parallel occupancy. Compilation/code size may rise.
Measure the relevant counters rather than requiring IPC to increase. Upper bound the
byte saving as removed rows times intermediate width times necessary write/read passes;
write allocation and cache residency need measured controller traffic.

**Keep/kill:** require a material complete-query benefit for a performance slice and
the same mechanism in independent data. A coverage-only slice must actually close an
operator boundary without material regression. If it merely moves unchanged work
between Java and native code, stop expanding it and use the result to select W2/W3.
An SSB coverage count alone does not justify a coverage-only slice.

## W2 — Choose physical strategies from consistent execution facts

**Hypothesis:** globally expensive choices arise because local policies see different
or incomplete facts about the same execution. Changing work should beat tuning its loop.

First test a bounded set of forced whole-query alternatives. Candidate decisions are
build orientation for filtered inputs, runtime-filter construction, and array/hash or
partitioned aggregation after joins. Select one decision with demonstrated headroom;
do not implement all combinations or a new join-order search engine.

Use existing prepared-build facts and snapshot interval counts where available. When
filtered cardinality is unknown, compare conservative planning with a bounded sample
before introducing it. Include sampling, discarded preparation and all scans in the
timer. Do not fully build both orientations and then report only the winner's probe time.

For a filter, estimate profitability as remaining downstream work saved minus its
construction and probing cost. For state selection, include all workers' state and
retained partition/output pages. For orientation, include payload width and translation
cost as well as rows. A global minimum over fixed alternatives is a hindsight reference,
not an achievable free optimizer.

**Controls:** selectivity/order reversals between partitions and executions; correlated
keys; unknown sizes; duplicate fanout; wide payload; small/all-match probes; tight query
budgets. Same-query reopen must discard invalid facts. No shared hot policy counter.

**Gate:** only add an automatic choice after forced alternatives show substantial
headroom. Aim for execution within 10% of the best measured legal alternative across
the controls, including decision cost. If the measurements cannot select reliably,
retain the existing strategy rather than adding a permanent toggle. Exact shared facts
are useful even when a learned policy is rejected.

## W3 — Eliminate joined work through aggregate placement

**Architectural bet:** many joins feed an aggregate that does not need every expanded
joined tuple. The compiler should exploit that algebra where semantics prove it legal.
This is a separate algorithm experiment after the relevant composition contracts exist.

Start with independent inner joins and scalar COUNT(*), no post-join predicate.
The current single-join path already stores per-key multiplicity. Extend that proven
algorithm across builds: each probe row contributes the product of its match counts,
instead of visiting every combination. Reuse countKeyRows and its partition task path.
Admission must verify the aggregate and predicate shape, not infer uniqueness from data
names or benchmark conventions. Prove zero-match, NULL and count-overflow behavior first.
In particular, the current `addPairCount` positive-count guard is not proof that an
overflowed multiplicity product can be passed through it unchanged.

With three fanouts of eight, expansion visits 512 combinations per surviving probe;
counting needs three lookups and a product after building counts. This is an operation
count hypothesis, not a 170x query-speed prediction: scanning, filtering, preparation
and output remain. Measure their share before choosing the large control.

Then, only as separate experiments, consider aggregate pushdown/groupjoin for COUNT
and other states whose actual init/update/merge/finalize contracts permit it. Reuse
AggregateUpdates and existing finalized-map consumers. SUM/AVG, DISTINCT, floating
rounding, sentinel overflow, cross-input predicates and outer joins are not licensed
by the COUNT proof. Assert functional dependencies from exact execution facts or valid
metadata; no assumed key constraints from SSB.

**Arms:** unchanged expansion; forced legal reduction; automatic admission. Vary fanout,
survival, group count and skew, including unique/all-match cases where reduction adds
work. Validate small complete SQL results and 500M-row controls with independent counts;
cap pathological baseline runs rather than hiding failures or projecting speedups.

**Success:** a reduction in expanded tuples/hash work, CPU and intermediate traffic
that produces at least 2x whole-query improvement on a qualifying family and survives
two independent controls. This can establish a general mechanism without improving
SSB, whose dimensions may be unique. Report that contribution on its own controls.

First W3 slice is implemented: resolved unobserved leaves can contribute counts to
scalar or grouped COUNT, including dependent branches and typed composite lookups.
The original two-leaf target missed 2x on two representations; a third-leaf sensitivity
control reaches 2.91-3.52x without a code change. Do not report those as the same target.
The first dormant-check regression was fixed by frame-level dispatch, at an explicit
compilation cost. Closed build forests are now retained too: producer multiplicities
are folded once and generated workers probe only roots. The six primary typed cases
gain 1.84-2.99x over leaf folding; a deeper mixed-type forest gains 3.83x. Memory,
overflow, unsupported heaps, cancellation/reopen and zero-group semantics are checked.
Repeated-input consumption now closes the COUNT-only aggregate gate: COUNT(value)
uses weights, extrema update once, and SUM/AVG replay actual ordered updates inside
a generated loop, including mixed lists and grouping. Initialization is peeled from
AVG's subsequent updates. Non-COUNT folding preserves the observed input order and
requires representable run lengths; it does not substitute multiplication for SUM.
Three 500M-row process pairs on each INT/LONG/SYMBOL branch give SUM 2.86-3.21x and
AVG 2.52-2.67x over the frozen COUNT-capable compiler. Exact-state/SQL controls cover
NULLs, sentinel overflow, signed zero, nonfinite values and rounding-sensitive order.
Small no-match/ordinary-AVG costs and larger compilation/code size remain explicit;
see [evidence](target/join-repeated-inputs.md).

W3 is not complete. Observed build inputs retain ordinary expansion except for legal
unobserved suffixes; DISTINCT, outer joins and functional-dependency rewrites still
need separate proofs. Java-owned build preparation does not increase strict whole-
query generated coverage. The next experiment must separate residual root lookup
from ordered aggregate arithmetic rather than assume every remaining cost is hashing.

## W4 — Make whole-query specialization economical

**Question:** can the same physical strategy reach good first-request latency without
paying optimized compilation for code it barely executes?

Measure Java IR/SSA work, text emission/parsing, LLVM optimization/code generation,
bridge lock wait, and first execution separately; complete request latency is decisive.
Compare current LLVM with supported AsmJit and deferred compilation on the same admitted
plan and state ABI. Unsupported backend programs do not become equivalent controls.

For extra compile cost C and repeated execution saving d, the simple reuse break-even
is C/d executions. For one long query, remaining work must amortize compilation before
it finishes. Include compiler CPU contention, alternate code size and unused variants.

Begin with forced/deferred modes and code reuse on reopen. A background compiler or
tier switch is justified only after these controls establish payoff. If attempted,
use existing drain/safe-resume boundaries and code ownership; one batch executes once,
and no machine-code lease ends while workers can call it. Recheck SYMBOL dictionaries
and execution-local constants independently of reusable code.

**Success:** lower complete first-request latency without materially losing long-query
throughput; report compilation CPU and code bytes. No interpreter, compiler pool or
general cache framework is introduced merely to make the architecture look complete.

## Validation and retention

- Use existing tests for the changed contract, adding sensitive failure/reopen cases
  where needed. Small semantic tests remain small; full performance controls use at
  least 500M rows. Official datasets/SQL retain their protocol rather than being padded.
- Refresh USE and validate the hardware study before production optimization. Collect
  source-matched symbols, fixed-work PMU groups and memory-controller evidence where
  appropriate. Independent signals establish a bottleneck; source samples locate it.
- All full outputs are consumed and checked, with documented floating/tie semantics.
  Counters establish actual operator execution. Failure after mutation never replays
  through Java. Cover cancellation, empty inputs, column tops, growth, dictionaries and
  supported/fallback frame representations through the affected existing suites.
- Preserve completion at the same query-memory budget when B0 completes. Report both
  live allocations and process RSS; faster execution cannot conceal a new budget failure.
- Start at one/eight physical cores; test another cache/worker placement at the phase
  acceptance gate. A second microarchitecture is required for cross-machine claims.
  The deleted EC2 instance is not available evidence for this candidate.
- Keep an active candidate and B0 reference, one loaded fixture per workload where
  possible, and a compact current report. Delete owned temporary data/class copies
  after validation. PROGRESS records accepted or rejected conclusions, not every run.
- Every retained slice must remove work, close a real composition gap, or remove
  independently maintained facts. No query-name rules, new user switches, factorial
  variants, speculative alias/overflow guarantees, second scheduler or broad refactor.

Run W0 first, then the largest W1 boundary. W2 follows evidence of costly choices; W3
is the larger algebraic bet. W4 moves earlier if first-request measurements show
compilation is the dominant limit. Small loop optimizations become subordinate to
this sequence. No overall goal is declared complete from a microbenchmark alone.

## Research basis and limits

| Primary source | What we take from it |
| --- | --- |
| [Neumann, data-centric compilation, 2011](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) | Preserve useful work across operator boundaries. Generated instructions alone do not choose a good physical plan. |
| [Jungmair et al., layered query compilation, 2022](https://www-db.cs.tum.edu/~jungmair/papers/p2485-jungmair.pdf) | Retain relational/state information until transformations no longer need it. This motivates the small integrated contract, not adopting MLIR or replacing QuestDB's planner. |
| [Fent and Neumann, groupjoin/nested aggregates, 2021](https://www.vldb.org/pvldb/vol14/p2383-fent.pdf) | Join and aggregation can share work/state under explicit key and functional-dependency conditions; parallel contention and empty-input semantics matter. W3's multiway COUNT extension is our proposed experiment, not a reported result from this paper. |
| [Qiao et al., RPT+ dynamic execution, 2026](https://yimingqiao.github.io/files/rpt_plus.pdf) | Filter construction and scanning can outweigh filtering benefits. Include these costs when choosing a whole-query execution strategy. |
| [Kohn et al., adaptive compiled execution, 2018](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf) | Compilation economics and safe execution-mode switching are part of query execution. Their interpreter is not a requirement to add one here. |

These papers motivate experiments. Their reported speedups are not predictions for
QuestDB. Existing experiments already rejected several plausible fusion/prefetch
variants; the same falsification standard applies to this larger plan.
