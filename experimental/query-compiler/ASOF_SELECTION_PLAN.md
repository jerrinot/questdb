# Automatic ASOF selection and generated execution

2026-11-04. Code baseline: `baf86ae4fc`. Status: bounded unfiltered selection, filter-aware scan/lookup, LLVM forward scan/aggregate fusion and bounded INT/LONG/SYMBOL backward lookup through ordinary SQL are implemented. First-use lookup compilation admission, broader consumer admission and parallel temporal execution remain open. Execute one milestone at a time and revise the next milestone when
measurements contradict its assumptions.

The goal is for ordinary ASOF SQL to approach the performance of the best eligible
existing algorithm without requiring hints. The improvement belongs in QuestDB's
ordinary planner and execution path and must work without the experimental LLVM
compiler. Existing hints remain explicit overrides and experimental controls.

The initial milestones isolate selection. With the filter-placement controls now
complete, extend the experimental compiler to ASOF and measure fusion separately
from algorithm selection and parallelism. Ordinary selection remains usable without
LLVM; the generated path uses the existing query-code-generator integration.

## Code facts at the initial baseline

The selection entry point is
[`SqlCodeGenerator.generateJoinAsof()`](../../core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java).
The existing adaptive lookup is
[`HorizonJoinTimeFrameHelper`](../../core/src/main/java/io/questdb/griffin/engine/table/HorizonJoinTimeFrameHelper.java).

| Code | Verified behavior | Consequence |
| --- | --- | --- |
| `SqlCodeGenerator.generateJoinAsof()` | Dense, Memoized and Indexed require hints after capability checks; Fast is the normal time-frame path. | Introduce automatic selection in the unhinted branch. Preserve hint precedence and existing fallbacks. |
| `AsOfJoinFastRecordCursorFactory.performKeyMatching()` | Scans backward from a timestamp position until a key matches or the search ends. | Measure search distance and repeated work, not just input cardinality. |
| `AsOfJoinDenseRecordCursorFactoryBase.hasNext()` | Initializes at the first useful timestamp, scans forward, and lazily fills historical matches by scanning backward. | Changing to a forward scan does not require eagerly reading the entire historical prefix. |
| `HorizonJoinTimeFrameHelper.findKeyedAsOfMatch()` | Already switches between backward lookup and forward scanning using observed work. | Evaluate reuse before implementing another algorithm or chooser. |
| `HorizonJoinTimeFrameHelper` constructor and `dropKeyCache()` | Row-producing and aggregate HORIZON paths use different adaptation policies; cache clearing can itself be expensive. Its API has no tolerance parameter. | Reuse needs a contract audit, including phase changes, state reset costs and tolerance. |
| `TimeFrameCursor.getIndexReaderForCurrentFrame()` | Provides indexed access on ordinary time-frame cursors; the concurrent implementation does not support it. | Keep the selection experiment serial and capability-driven. Parallel/index integration is separate work. |
| `SqlCodeGenerator.generateJoinAsof()` filtered branches | Can free a compiled filter and move its predicate into serial lookup. | Filter placement is a separate physical choice made before filter ownership transfers. |
| `AbstractAsOfJoinFastRecordCursor.size()/calculateSize()` and `CountRecordCursorFactory` | A plain count can be answered from the left cursor without evaluating RHS matches. | Performance queries must consume matched RHS values and prove that matching ran. |

Reuse `RecordSink`, typed maps, `SymbolTranslatingRecord`/`SymbolJoinKeyMapping`,
`TimeFrameCursor`, memory accounting and circuit breakers. Do not construct all
existing factories around the same child cursors and switch owners at runtime.

## Research and its practical implications

- [Smooth Scan](https://stratos.seas.harvard.edu/sites/g/files/omnuum4611/files/stratos/files/smoothscan.pdf)
  adapts an access path as observed density changes. Its region-density experiment
  demonstrates the importance of reducing sequential work after an initially dense
  region. Apply that principle to temporal lookup versus forward scanning. Its
  PostgreSQL access costs and thresholds are not QuestDB calibration data.
- [Progressive query optimization, SIGMOD 2004](https://research.ibm.com/publications/robust-query-processing-through-progressive-optimization)
  checks assumptions during execution and reuses completed work when changing plans.
  Apply explicit validity checks and safe transition points within ASOF; a global
  query reoptimizer is unnecessary for this experiment.
- [Evaluating and Optimizing ASOF Joins, TUM guided research, 2025](https://talks.db.in.tum.de/uploads/a2/26cb5f915e49cea2be97996a9e8d15/slides.pdf)
  compares merge and left/right partition strategies and finds no universal winner.
  Treat it as algorithmic evidence, not an integrated QuestDB benchmark. Validate
  skew, relative input sizes and predicate placement independently.
- [DuckDB's ASOF planning study, 2025](https://duckdb.org/2025/02/19/asof-plans)
  shows that tiny probe inputs can justify a different plan and that memory matters.
  Preserve cheap startup and early LIMIT behavior. QuestDB already has timestamp
  order, so importing its sort costs or numerical crossover would be unjustified.

Proposed architecture: planning determines which operations are legal; execution
observes locality and repetition and selects among the legal operations. Observe
work already required by the query. A preliminary full scan and a first-N-rows
decision fixed for the whole execution are unsuitable starting points.

## Milestone 1 — Establish the available gains

Build a small ASOF workload driver alongside the existing experimental runners.
Reuse their engine/configuration setup and Griffin correctness runner where useful;
avoid building another benchmark framework. Run one case at a time against the
unchanged default and every applicable hinted algorithm. Check the actual factory
or EXPLAIN plan: an ignored hint is not an algorithm measurement.

Use a reusable RHS with at least 500 million rows for performance experiments.
Vary the left input from a handful of probes through a dense stream. Keep small
correctness fixtures separate. Generate and retain only the data needed for the
active experiment; do not accumulate dataset copies or result archives.

Start with one sparse/local case and one dense/repeated-search case. Extend the
controls to cover:

| Dimension | Required contrast |
| --- | --- |
| Temporal locality | Local matches, long predecessor gaps, dense bursts followed by sparse stretches, and the reverse |
| Keys | INT, full-width LONG, SYMBOL with unrelated dictionaries, and composite keys |
| Distribution | Uniform keys, hot/rare keys, absent keys, and keys present only in the future |
| Access | Indexed/unindexed SYMBOL, narrow/wide tolerance, filtered/unfiltered RHS |
| Storage and lifetime | Frame/partition boundaries, native/Parquet data, first/repeated execution, small LIMIT |

Use a payload-consuming checksum driver and ordinary SQL consumers such as
SUM/MIN/MAX over RHS values. Verify results independently on small data and against
existing algorithms at scale. Include output consumption, cursor opening, state
allocation, decoding and any selector exploration in measured request time.

Record backward and forward rows visited, timestamp-search work, frames opened,
index probes, predicate evaluations, map operations, state bytes, cache clearing
and mode transitions. Keep rich diagnostics outside retained hot loops; measure
instrumentation overhead separately.

Collect at least three balanced repetitions. Fix workers, affinity, cache protocol,
JIT mode and input snapshot across controls. Perform USE preflight and normalize
hardware metrics per completed left row and per examined RHS row. Classify the
resource constraint using independent signals before changing production code;
retain a validated bottleneck study for the active case. A software hot path or
high cache-miss count alone does not establish the limiting resource.

Compare a zero-offset, row-producing HORIZON query with equivalent ASOF output,
initially serial and without tolerance. Inspect its plan. This is a reuse experiment,
not a pure lookup microbenchmark: account for HORIZON's additional orchestration.

Exit: identify a repeatable selection failure, the best existing control, the
operation counts explaining its advantage, and the cost of HORIZON's adaptation.
If no substantial selection loss appears, broaden the workload evidence before
implementing a selector.

## Milestone 2 — Automatic backward versus forward selection

Start with keyed, unfiltered time-frame inputs. Support INT, LONG, SYMBOL and
composite keys in this milestone using the existing typed key/state contracts.
Unkeyed and non-time-frame fallbacks keep their existing behavior.

Compare the HORIZON helper with Fast and Dense at equivalent boundaries. Reuse the
smallest proven lookup/state component. Keep ASOF ownership, output ordering and
tolerance in its cursor; add only the lower-search-bound contract needed for safe
reuse. Avoid a broad temporal-join class hierarchy refactor. If adapting the helper
requires duplicating ownership or importing unrelated HORIZON execution, re-plan
around a narrower shared lookup component.

Use two initial modes: timestamp-directed backward lookup and forward scanning
with keyed latest-row state. Start cheaply and allocate keyed state lazily. Revisit
the choice at coarse work boundaries using counters accumulated by the scans.
Measure actual rows between positions across frames: encoded row-ID subtraction
is not a row count. Do not open all historical partitions just to estimate a gap;
use existing metadata or bounded traversal when the decision requires it.

For a recent interval, reason about the alternatives as:

```
lookup cost  ~= backward rows * comparison cost + timestamp positioning
forward cost ~= intervening rows * state-update cost + key probes + transition cost
```

Comparison and state-update costs differ. Start with a simple work-ratio policy,
test its sensitivity across key widths and storage formats, and add coarse timing
only if operation counts cannot discriminate. Never time every row or bake in one
CPU's cache size or nanosecond threshold. Count map growth/reset costs as well as
scan work. Require a margin before switching, and a bounded reevaluation interval,
to limit oscillation while recovering from a change in locality.

State transitions must satisfy these invariants:

- Retain the timestamp frontier separately from the row found by keyed lookup.
- On entering forward mode, an absent cached key remains unresolved until history
  has been searched; an incomplete map is not proof of no match.
- On returning to lookup mode or skipping a gap, old entries are usable only when
  their validity is proven. Reuse the helper's watermark/reset rules where valid.
- Emit each left row once. Preserve the last eligible RHS row at duplicate
  timestamps, NULL-key semantics, inclusive tolerance and timestamp scaling.
- Reset execution-specific evidence and snapshot-bound row IDs on reopen/toTop as
  required. Cached factories must not retain decisions based on old data or binds.
- Charge state to existing memory tracking. Use conservative admission before
  growth; `getUsed()` is not a reservation. Preserve genuine allocation and
  cancellation errors rather than catching them and continuing with partial state.

Exit: the unhinted path captures the dense-case gain across required key types,
keeps sparse lookup cheap, and recovers in both directions on phase-changing data.
Confirm that the predicted scan-work and hardware metrics move with elapsed time.

## Milestone 3 — Close gaps against Indexed and Memoized

Measure the remaining distance to every eligible hinted winner. The automatic
implementation must cover their performance regimes; it need not expose a mode
for every existing factory name.

For a single key mapped to an indexed static RHS SYMBOL, evaluate an indexed
lookup alternative using the existing index reader and symbol translation. A
short local scan before indexed lookup is a candidate policy, not a fixed rule.
Test absent symbols, symbols absent from many recent partitions, column tops,
duplicate timestamps and tolerance. Charge index opening and exploratory probes
to the query. Bound any trial work, restore the timestamp position correctly, and
avoid timing comparisons on unrelated regions or silently warmed alternatives.

Compare Memoized and drive-by memoization on repeated rare-key searches. If the
shared adaptive cache covers their advantage, keep the simpler policy. If they
win a distinct regime, reuse validity-range/negative-result caching and generalize
its key representation to INT/LONG/composite keys where applicable. Persistent
index availability is a legitimate SYMBOL-specific capability; memoization itself
does not require SYMBOL semantics.

Exit: explain and resolve every substantial held-out loss to an eligible Indexed
or Memoized control. Do not call selection complete with those cases omitted.

## Milestone 4 — Select filtered-input execution safely

Compare lookup-local filtering with the existing Light path consuming a filtered
RHS, preserving its compiled/parallel filtering where supported. Measure match
search distance, predicate evaluations and complete predicate execution cost.

Place the decision before `generateJoinAsof()` frees compiled-filter resources or
calls `halfClose()`. Audit filter ownership, bind initialization, volatility and
replayability before using any pilot execution. A pilot must be bounded and its
work included; it must not double-evaluate stateful expressions.

First test whether cheap input facts and a legal bounded startup observation can
select well enough. Changing between a filtered stream and random-access lookup
mid-query requires a separate state-transfer proof; do not hide it inside the
two-mode scanner. If a low-cost safe choice is not established, preserve the current
path and explicitly leave this milestone incomplete.

Exit: automatic selection removes measured filtered-input cliffs without changing
predicate placement semantics or paying for both complete RHS executions.

## Milestone 5 — Validate and enable the measured default

Preserve existing hints and capability fallbacks. Add automatic behavior directly
to the eligible unhinted path after each completed milestone passes its gates;
avoid another production enable flag. EXPLAIN should identify adaptive execution
and its eligible operations. Runtime mode counts belong in bounded diagnostics,
not a fabricated compile-time prediction of the eventual winning mode.

Reuse `AsOfJoinTest`, `AsOfJoinFuzzTest`, `AsOfJoinFastOomTest` and the HORIZON
equivalence tests. Extend the existing fuzzer's key coverage and add focused forced
transition cases for first/last rows, duplicate timestamps, NULLs, missing/future
keys, mixed timestamp precision, tolerance boundaries, frame boundaries, self-joins,
projected keys, cancellation and reopen. Compare ordered output, not only aggregates. Include mixed native/Parquet
and composite indexed storage through existing fixtures. Keep tests targeted at
state transitions and semantic contracts rather than mirroring implementation.

The following are proposed acceptance targets, not measured results:

- In at least two independent workload families where a forced algorithm is at
  least 2x faster than today's default, automatic execution captures at least 80%
  of the available elapsed-time saving. Demonstrate the main scan-selection gain
  on INT, LONG and SYMBOL, with composite controls.
- On stationary bulk controls, aim within 20% of the fastest eligible forced
  complete-query execution. Report each miss and the worst ratio, not just a mean.
- Where today's default already wins, require no repeatable regression above 5%
  outside measurement uncertainty. Report first-row latency and small LIMIT costs
  separately, as well as memory growth and phase-change behavior.
- Freeze the policy before testing held-out distributions, seeds, partition sizes
  and key ranges. Validate on a second machine with a materially different cache/
  memory profile before claiming hardware generality; reduced core counts on the
  same host are not that validation. PMU counters inform development, not admission.

The forced winner is an offline benchmark reference, including setup costs. It is
not an online source of free knowledge, and comparisons against it do not establish
a theoretical regret bound. Account for every trial, transition and state rebuild.
Retain one concise progress log with the current decision, decisive measurements
and rejected hypotheses; clean disposable data after each experiment.

The first executable step is Milestone 1's sparse/local versus dense/repeated
search comparison. Its outcome determines the first production change.


## Execution decision — 2026-11-03

The 500M-row / 1,024-key control has opposite winners: at 100K local probes,
Fast is about 0.13s and Dense 3.13s; at 5M independent-key probes, Fast is 15.35s
and Dense 3.24s. HORIZON's current adaptive path is 18.80s on the dense case.
Forcing its existing forward mode reduces this to 4.55s versus Dense's 3.80s in
the same process (different JIT profiles from the first run). All consume RHS
payloads and match the independent count/sum/order-sensitive hash oracle.

Re-plan milestone 2 around the existing Fast timestamp navigation and Dense
forward/backward maps. HORIZON establishes useful state-validity and bounded-work
principles, but its extra timestamp positioning and hashing on backward searches
are not free. Do not transplant its full execution path or change its policy for
unrelated HORIZON consumers.

The proposed selector compares work over short windows, not table cardinality:
backward comparisons versus actual forward rows. Dense already knows the matched
row and timestamp frontier, so their distance supplies a backward-work estimate
without executing a trial lookup. Only use exact row distances; unknown distances
across nonadjacent frames do not count as gigantic gaps. Bound forward exploration
before crossing a suddenly sparse region, invalidate map state on a mode change,
and restart from the timestamp frontier rather than the previously matched key.
Keep maps unopened during cheap backward lookups. Hints keep their fixed algorithms.

The hardware gate and focused transition tests passed; these mechanisms now run
in the ordinary unhinted keyed time-frame path. Current measurements and rejected
experiments are summarized in PROGRESS.md. This is still a one-machine prototype.

## Implementation and remaining gates

- Milestone 1: established the opposite sparse/dense winners on 500M-row inputs;
  the payload-consuming runner rejects ignored hints and incorrect ordered hashes.
- Milestone 2: Fast timestamp navigation plus Dense's two maps, shared across INT,
  LONG, SYMBOL and composite keys. Unknown cross-frame row distances stay unknown.
  Both transitions, native/Parquet boundaries, tolerance, duplicate timestamps,
  NULLs, missing/future keys, reopen and rewind have focused coverage.
- Optional scan/memo state is now bounded, including under finite query budgets.
  Each map may own at most one existing configured hash-join value page; with a
  finite query limit it is additionally capped at one eighth of that limit.
  The two maps together therefore consume at most a quarter of a finite limit.
  Existing OrderedMap/Unordered4Map/Unordered8Map check allocation and growth,
  including explicit old/new rehash overlap, variable-key storage and map-owned
  staging. Bounded VARCHAR state uses OrderedMap because UnorderedVarcharMap's
  separate arena does not yet implement this contract. No per-row limit check
  or wider aggregate/map value was introduced.
- Before growth, published query headroom provides a conservative admission hint,
  not a reservation. Real tracked-allocation and cancellation errors propagate.
  Only the dedicated optional-storage refusal discards maps and retries the same
  left row via lookup. A full history map can give way to a smaller per-probe
  memo cache; oversized keys or rejected initial allocations retain plain lookup.
  After refusal, forward scans stay disabled until rewind/reopen to avoid repeated
  allocation attempts. This bounded policy can sacrifice performance when locality
  improves later; it does not guarantee the best algorithm under memory pressure.
  Explicit hints retain their existing allocation/error contracts.
- Milestone 3: the automatic single-SYMBOL path uses the existing indexed lookup
  after a failed latest-row comparison. Indexed/scan crossover uses sparse timing
  samples of complete operator calls (consumer excluded), row-work admission,
  hysteresis and a retry delay. Every trial is charged to the query. The initial
  adjacency-only index policy lost 24% to Dense at a wider probe spacing and was
  rejected; no hardware-specific nanosecond cutoff replaced it.
- Returning from scan to nonindexed lookup now memoizes positive and negative
  results with a per-key validity frontier. It checks all newer rows before reuse,
  and works through the existing typed key sinks/maps. In lookup mode the two maps
  hold the result and its validity boundary. They are invalidated before a new
  contiguous scan interval. Widening scan values changed MapFactory's concrete map
  choice and caused a measured dense regression; retaining the original layout
  removed it. No additional map implementation or production flag was added.
- Milestone 4 is split after the ownership audit. PageFrameRecordCursorFactory
  reuses its page-frame cursor: opening a second access path is not an independent
  snapshot or a harmless pilot. A filtered-stream/lookup handoff still needs an
  explicit snapshot and resource-transfer contract before `halfClose()`.
- The first filtered step now reuses the existing bounded ASOF scan/lookup state
  inside FilteredAsOfJoinFastRecordCursorFactory. The predicate sees the raw record
  before projection, and only qualifying rows enter scan/memo state. Its existing
  `Function.isStableWithinExecution()` contract controls cache eligibility;
  unstable predicates remain lookup-only. No left-row pilot, replay, second RHS
  cursor, new map, chooser or enable flag was introduced. Key serialization and
  SYMBOL translation remain the existing typed contracts. Reopen and rewind reset
  state; predicate initialization precedes projected access.
- Sequential loops use the existing `recordAtRowIndex()` inside a bound frame and
  full `recordAt()` at scan entry and frame boundaries. The entry rebind is required:
  emitting a historical match may have moved record B away from the scan frame.
  The same implementation serves filtered and unfiltered keys, native and Parquet.
- Mixed-query JVM compilation remains an acceptance gap: the filtered INT control
  rose from 2.36s to 3.17s after warming dense INT and SYMBOL. Sharing one concrete
  cursor helped that phase only modestly while increasing isolated instructions
  and elapsed time; the refactor was rejected. Do not report isolated-query results
  as a server-wide guarantee. Details and counters are in PROGRESS.md.
- This does not preserve compiled/parallel filtering as an alternative runtime
  path: it improves the scalar predicate already stolen by the old default.
  `asof_linear` still retains the original filtered-stream path and is the control.
  Keep the full filtered-stream selection milestone open until expensive predicates,
  compiled filtering and workers demonstrate where the additional ownership work
  pays. HORIZON's filtered automatic chooser was ~15.9s while its forced forward
  path was ~2.46s against Light ~2.10s; do not import its chooser wholesale.
- Milestone 5: focused correctness and warm single-core controls run without LLVM.
  Full admission sensitivity, first-row/LIMIT performance, wider distributions,
  and a materially different second machine are still acceptance work. Sparse
  local-query timings vary across JVM processes; the budget change's follow-up
  ABBA counters show effectively unchanged instructions/cycles per probe. Do not
  claim a few-percent improvement or a universal no-regression result. Existing
  hints remain explicit controls. Do not claim universal best-plan selection or
  hardware generality from this host.

The bounded-state and scalar filtered-scan mechanisms are implemented. Extend the
acceptance controls before building a filtered-stream handoff: vary predicate cost
and selectivity, include compiled filtering and multiple workers, and measure small
LIMIT/first-row cost. A single scalar predicate on one core cannot establish that
retaining parallel/compiled filtering has no value. Keep the explicit Light control.



## Filter placement controls and revised next increment

The follow-up controls are complete on the same 500M-row native dataset. The old
`v%3=0` control was an Async Filter, not an Async JIT Filter. The runner now asserts
both join and filter plans, varies predicate/selectivity, starts real query jobs,
checks an independent payload oracle, and includes close/drain in request time.
It reports query-thread CPU, process CPU and first-row time separately.

At 6.24% selectivity (`v-v/1009*1009<63`), four slots favor forced Light's compiled
stream by 5.77x: 0.456s versus scalar adaptive's 2.629s. LONG, SYMBOL and composite
controls reproduce the gap. At 0.40% selectivity the ratio grows to 6.79x.
A scalar SIN predicate also benefits from workers (35.81s versus 11.92s), so JIT
retention alone cannot close this family. Conversely, local matches still favor
lookup (0.698s versus 0.899s); LIMIT 100 is 2.50ms versus 29.46ms in this embedded
configuration. Do not choose a stream solely because a compiled filter exists.
The embedded dispatch limit is unlimited; the server default is worker-dependent.
Small-LIMIT admission must also be checked with server configuration.

These are controls against an existing forced algorithm, not new production wins.
The detailed hardware study covers the 6.24%-selective INT case only. Current
measurements, the instruction/cycle decomposition and scope limits are in PROGRESS.

Re-plan the next implementation around a bounded batch of qualifying RHS row IDs
inside the current snapshot, rather than opening a second complete RHS execution:

1. Preserve the compiled filter and bind resources before the two keyed
   `generateJoinAsof()` branches call `halfClose()`. Keep one explicit owner. Reuse
   the bounded slicing and column-top/type-cast fallback in `LatestByCompiledFilter`
   instead of adding another expression compiler or address-offset implementation.
   Generalizing that helper must retain LATEST BY behavior. Its existing interface
   takes a pool/address cache; ASOF currently has no production borrowed-frame API
   (`TimeFrameCursorImpl.getFrameMemoryPool()` is test-only). Define that narrow
   lifetime contract first, on the raw pre-projection column layout.
2. Use bounded batches after observed scan work justifies them. Consume qualifying
   row IDs through the existing ASOF maps, avoiding ASOF bookkeeping on rejected
   rows. Preserve the actual timestamp frontier separately from the next qualifying
   row: the latter is not a valid substitute when a long interval has no matches.
   Bound speculative rows and memory independently of table size. Keep sparse and
   unstable-predicate lookup behavior. Reset cached row IDs on bind changes,
   rewind/reopen and mode changes; charge buffers using existing memory tracking.
3. Honor the existing CompiledFilter lifecycle (`prepareExecution`,
   `prepareForDispatch`, `clearExecution`) and its frame/aux-vector contract.
   The experimental LLVM implementations use these hooks for execution bindings;
   assuming the base AsmJIT implementation's no-op hooks would not generalize.
   Test fixed-width, SYMBOL and VARCHAR predicates, projection, NULLs, column tops,
   lazy casts, Parquet and current floating-point boundary semantics.
4. Only after the single-owner batch path is correct, reuse existing query jobs
   for a bounded forward window. `ConcurrentTimeFrameState` already shares a reader
   snapshot, lazy partition opening and frame addresses. `PageFrameSequence.of()`
   currently opens/owns its base cursor, so it cannot simply be layered over an
   active TimeFrameCursor. Prove reader ownership, frame-ID identity, worker drain
   and historical lookup validity before wiring the two. Non-thread-safe predicates
   need the existing per-worker clone contract. Never dispatch a whole RHS merely
   to decide whether lookup would have been cheaper.

First acceptance target: narrow the one-core gap to the 1.64s compiled Light
control while preserving local/limited lookup costs. Then measure whether a bounded
parallel window approaches the four-slot control. Changes must reduce measured
instructions/cycles or distribute useful work, not just hide time in preparation.
Repeat the current key matrix and held-out predicates before changing admission.
No CPU-model checks, query-text recognition or additional production toggle.

This direction is an inference from the measurements and existing contracts.
[Kersten et al., PVLDB 2018](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf)
compare compiled and vectorized execution while holding algorithms and structures
constant; use that experimental discipline here. The paper does not supply a
QuestDB crossover threshold or prove that batching wins for sparse ASOF lookup.

## Next experiment: fused LLVM temporal execution

User direction after the placement controls: extend the generated pipeline itself,
as with INNER JOIN. Bounded filter batches are a useful comparison and fallback,
not the final execution boundary. This section supersedes the batch-only next
increment above; its snapshot, ownership and lifecycle requirements still apply.
The first region below is implemented and measured. It is optional acceleration of
forward mode, not a generated replacement for every ASOF strategy or its consumer.

Architecture constraint: compilation does not select a universal ASOF algorithm.
Preserve distinct temporal lookup, indexed predecessor, memoized lookup and forward
scan paths. Generate their hot regions independently and fuse their eligible
consumers. The existing chooser remains responsible for admission and transitions;
native availability alone must never force a scan. A query can use generated code
for one strategy and existing Java code for another.

Verified reuse points:

- `QueryCodeGenerator.compileJoin()` and `ResolvedJoinKernel` describe INNER joins
  over pinned `FrozenHashJoinBuild` objects. ASOF needs mutable temporal state;
  passing that state as a frozen build would violate the existing contract.
- Reuse `ResolvedInputs`/`ResolvedFilter`, typed IR and LLVM lowering, `QuestMapProbe`
  and `NativeMapView`, dictionary translation, and `AggregateUpdates`. The existing
  join emitter already resumes around symbol misses and ordinary map insertion;
  map growth invalidates native descriptors and requires rebinding.
- `AsOfJoinDenseRecordCursorFactoryBase.scanForward()` already defines timestamp
  cutoff, tolerance, scan-work accounting and latest-row map updates. Compile this
  established operation rather than inventing different temporal matching rules.
  Its historical backward search and adaptive transitions remain authoritative.
- `TimeFrameCursor` exposes records and navigation, not an explicit borrowed native
  slice contract. The first region instead borrows the already-positioned raw
  `PageFrameMemoryRecord`: TimeFrameCursor pins record B, no navigation occurs
  during native execution or map insertion, and no input pointer survives the call.
  This reuses the existing record/pool lifetime, including Parquet buffers. Future
  cross-frame payload or parallel work needs its own explicit borrowing contract.

The generated regions follow the selected strategy:

| Strategy | Work to specialize and fuse | Behavior to preserve |
| --- | --- | --- |
| Timestamp seek and local/backward lookup | Predicate, typed key comparison and tolerance checks over candidate rows, then the consumer | Stop at the first valid predecessor; do not scan skipped time ranges |
| Indexed predecessor | Existing index navigation with generated candidate checks and consumer where worthwhile | Open/use the index only when admitted; no index reimplementation merely to obtain fusion |
| Memoized lookup | Validate the cached-through boundary, inspect newer candidates, reuse the result, then consume it | Cached results prove nothing about unexamined newer rows; preserve per-key validity |
| Forward scan with historical lookup | Predicate, typed key, latest-row updates and probe/consumer | Maintain the contiguous scan frontier and lazy historical search; obey the scan-work budget |

Only the forward strategy advances RHS rows through each left timestamp. Sparse
lookup and indexed access keep their own control flow. All paths must select the
same matched or null-extended row before downstream filtering/projection/aggregation.
RHS filtering happens before winner selection; a failing post-join predicate must
not cause an older match to win. Equal-timestamp tie order, timestamp scaling and
tolerance retain their existing semantics.

Keep strategy state distinct. The current maps hold row IDs in forward mode but a
cached result and its validity boundary in memoized mode. `switchToForward()` and
`switchToLookup()` explicitly discard incompatible state; a shared native layout
must not silently reinterpret it. Native regions report examined work and exact
resume positions so a budget, phase change or memory refusal can return control to
the existing chooser without advancing/emitting the left row twice. Revalidate
selection costs after compilation: faster kernels change crossover points, and
fusing a consumer must not contaminate the chooser's operator-cost samples.

Java retains snapshot ownership, frame opening/decoding, allocation/growth, errors
and worker scheduling. Amortize native calls over useful regions/probe batches,
without making sparse lookups or LIMIT wait for a full batch. Keep a cheap Java
path when compilation or transition overhead would dominate. A resumable kernel retains exact
left/RHS positions and committed state across frame exhaustion, bounded output,
symbol resolution and map growth. Resuming cannot repeat an aggregate update or
skip a row. Unsupported input may decline before mutation; bounded-state refusal
uses the existing adaptive fallback with an explicit consistent resume point.

Implement and falsify in this order:

1. Add the narrow borrowed-slice and strategy-specific kernel contracts. Start with
   the measured forward region: fuse predicate, typed key and map update. Keep
   row IDs in the map initially, preserving historical payload ownership. This is
   an intermediate region, not full ASOF fusion or a scan-only planner. Test INT,
   full-width LONG, translated SYMBOL and composite keys; no INT-only SQL admission.
   Lookup/index/memoized paths and transitions stay available even if only the
   forward region initially has native code.
2. Extend generated coverage to lookup/cached candidate checks based on their own
   profiles. Batch probes within a strategy while honoring chooser checkpoints;
   fuse eligible downstream expressions and aggregates through the existing typed
   declarations. Supply bounded row-ID output for ordinary cursors. Reuse consumer
   emission without forcing strategies into one universal per-row dispatch loop.
   Do not promise native historical payload access before proving its lifetime.
3. Compare Java and generated regions within each identical algorithm, with the
   same maps, inputs and one worker. Retain compiled-filter-only batching as an
   ablation for the forward case. Then test adaptive execution against the best
   eligible forced strategy for sparse, dense, rare and phase-changing inputs;
   verify both generated-to-Java and Java-to-generated transitions. Independently
   measure parallelism. The existing four-slot Light result remains a full-request
   competitor, not a claimed LLVM result or hardware floor.
4. Extend parallel execution only after ordering is explicit. A shared latest-row
   map cannot be updated by arbitrary time morsels: future rows would overwrite
   state needed by earlier probes. Evaluate bounded parallel predicate preparation
   with ordered consumption first, reusing existing jobs. Independent key ranges
   or time ranges with predecessor seeding require separate cost/correctness proof.

Acceptance uses the current 500M RHS controls plus held-out distributions. Record
examined RHS rows as well as completed probes, native rows/calls, cycles, instructions,
cache/branch counters and matched payload hashes. Compare first-use time including
compilation with reused execution separately. For the selective forward-scan case,
the first target is to beat the 1.64s one-core compiled Light control, not merely
the 2.70s Java adaptive path. Other strategies have their own controls. No speedup
factor is predicted before implementation. Reject a win caused by extra exploration
outside timing, changed work, lost LIMIT responsiveness or an INT/COUNT-only shortcut.

Correctness controls include unrelated symbol dictionaries, NULL/unmatched/future
keys, duplicate timestamps, mixed timestamp units, tolerance edges, frame/partition
boundaries, Parquet/column tops, binds and rewind, cancellation and bounded memory.
Use existing ASOF tests/fuzzing and a few focused new cases for the borrowing/resume
contract. Full consumer fusion must include SUM/MIN/MAX/mixed lists and observable
RHS payloads; COUNT(*) alone can bypass ASOF matching entirely.

The hypothesis follows [HyPer's data-centric compilation design](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf):
remove per-row operator boundaries and retain values through the consumer. It is
supported here by the measured Java scalar instruction cost, not by assuming INNER
JOIN speedups transfer. Our largest INNER JOIN gains also include parallelism and
multiplicity reduction; ASOF has at most one RHS match per left row.

## First generated-region result and revised next experiment

`CompiledAsOfScan` / `ResolvedAsOfScan` now lower the existing forward operation:
timestamp bounds, stable predicate, typed key encoding, native map probe and latest
row-ID update. Ordinary map insertion/growth handles misses. Java retains strategy
selection, historical lookup, probe/output and ownership. Unsupported predicates,
key encodings, column tops and casts keep their ordinary path. The region does not
open another reader or retain Parquet pointers across navigation. It is selected
automatically when the existing experimental query compiler can bind it.

The 500M-row control improves about fourfold over Java adaptive, but the ablation
changes the next experiment. At 33% predicate survival, fusion beats Light with an
LLVM filter by 1.7–2x; at 6.24% they are close; at 0.40% the filter-first path wins.
The compiler previously lacked positive-constant integer division binding, so part
of the overall gain is shared expression lowering, not temporal fusion. Full
numbers, correctness and startup limitations are in the current PROGRESS entry.

Query-only cycle samples located 82.1% in the generated scalar forward kernel.
The bounded-vector experiment is now complete and **rejected as a default ASOF
lowering**. With unrolled ordered updates, 6.24% survival improved 0.663 -> 0.460s
and 0.40% improved 0.569 -> 0.384s, but a held-out multiplication predicate at
roughly 50% survival regressed 1.011 -> 1.514s. AVX2-only code generation also lost
the 6.24% benefit (0.644 -> 0.653s). Lower instruction counts did not ensure lower
cycles. No selectivity threshold, new switch or unconditional batching remains.

One independent compiler improvement survives: positive constant integer division
inside an LLVM vector predicate uses 32-bit partial products instead of extracting
and repacking scalar LONG lanes. Matched filter-first ASOF controls improve 9–47%
on this host's normal target and only 4–8% with AVX2-only code generation. This is
shared expression lowering, not new ASOF fusion. See PROGRESS for the fixed-work
counters and proof boundaries.

Keep the existing scalar forward region and multiple ASOF strategies. The next
region to investigate is lookup/cached-candidate checking and probe/consumer work,
using their own profiles. Before revisiting vector forward execution, account for
the consumer's control flow and target costs, and retain the mixed multiplication
predicate as a falsification control. Compilation/startup, sparse probes and LIMIT
remain acceptance criteria; unused forward code still has a planning cost today.

## Reproduce the current controls

Build with `mvn -Pquery-compiler-prototype -pl experimental/query-compiler -am
-DskipTests -Djacoco.skip=true install` (one shell line). From the repository root:

```sh
experimental/query-compiler/run-asof.sh experimental/query-compiler/target/asof-db 500000000 1024 5000000 dense int adaptive,dense 3 3
experimental/query-compiler/run-asof.sh experimental/query-compiler/target/asof-db 500000000 1024 1000 rare symbol adaptive,index 5 5 indexed
experimental/query-compiler/run-asof.sh experimental/query-compiler/target/asof-db 500000000 1024 1000 rare symbol adaptive,memoized 3 3 unindexed
```

Arguments are root, RHS rows, key domain, probe rows, shape, key type, algorithms,
measured repetitions, warmups, then optional index state and optional query-memory
limit in bytes (zero means unlimited). Further optional arguments are predicate
(`mod3`, `arith3`, `arith16`, `arith256`, `arith_all`, `arith_mixed`, `sin`), execution slots (caller included),
filter mode (`jit` or `java`), output LIMIT (zero means all probes), and optional
query compiler (`stock` or `llvm`; default `stock` means no experimental compiler),
then consumer (`rows` or `aggregates`, default `rows`). The aggregate consumer runs
SQL SUM/MIN/MAX/COUNT plus a left-dependent SUM and checks the complete result;
it requires an unlimited input. `arith256_spread` retains four residues per 1009
values, spaced apart instead of adjacent. Its expression does additional arithmetic,
so it is a distribution falsifier, not a pure selectivity ablation.
`arith3` has the same truth values as `mod3` but uses JIT-supported arithmetic;
`arith16`/`arith256` select 63/1009 and 4/1009 of RHS rows independently of key type.
`arith_all` accepts all positive payloads; `arith_mixed` uses wrapping LONG
multiplication and accepts roughly half, without division or clustered survivors.
The index argument changes only the
experiment's RHS index, before timing. Without it, the existing index is kept
(requesting `index` adds it). To test a finite limit while preserving indexes,
append `keep 8388608` for 8 MiB. `ASOF_CLASSES` can prefix classes from a separate
baseline checkout; use `fast` rather than `adaptive` to assert its original plan.
Keep the same schema/index state for stock/candidate comparisons. Compilation,
data creation and index creation are outside timing; cursor opening, matching,
payload consumption and closing are inside. The root must be empty or carry this
runner's dataset marker. Never point it at the canonical database.

Shapes include local/dense, both phase orders, adjacent probes, rare keys, and
filtered/filtered_local; key types are int/long/symbol/composite. For the filtered
controls use `adaptive,linear`: stable predicates use Filtered AsOf Join Adaptive;
stateful predicates retain lookup-only Filtered AsOf Join Fast.
Pin the process using a CPU selected from the current machine's topology; CPU 4
was used for this host's recorded controls, not as a required setting.

Example compiled-filter placement control (use an appropriate CPU set on the host):

```sh
taskset -c 4-7 experimental/query-compiler/run-asof.sh experimental/query-compiler/target/asof-db 500000000 1024 1000000 filtered int adaptive,linear 3 3 keep 0 arith16 4 jit
```

These controls use embedded DefaultCairoConfiguration with the requested worker
count; they do not start a server. The optional final `llvm` argument installs the
existing experimental query compiler. For example, append `0 llvm` to the command
above to use LLVM with no output limit. Compilation is separately reported; the
first execution and reused executions are both printed. Native forward-row counts
prove which ASOF region actually ran; compilation alone does not establish coverage.

## Architecture comparison and revised implementation order

The committee's two-regime experiment now uses real SQL consumers and 500M RHS
rows: 1M probes with selective filtering, and 25M local probes with a less selective
filter. Both adaptive LLVM and Light with LLVM filtering run with one or four
physical cores. Compilation and first execution remain separate from repeated
execution. The harness also reports JVM compilation during each execution: a fixed
number of warmups did not reliably settle short queries.

The denser control exposed a shared cursor cost before consumer fusion: timestamp
lookahead repeatedly resolved the same frame and positioned the output record for
every intermediate timestamp. A validated hardware study attributed dominant
retiring work to that navigation. The implementation now pins record A once per
lookahead region, advances with the existing `recordAtRowIndex`, and positions
record B only at the final predecessor. No input navigation or allocation occurs
inside that borrowed region. This preserves all matching strategies and applies
to ordinary execution as well as LLVM-enabled queries.

Matched one-core INT queries improve 4.613 -> 1.775s, LONG 5.254 -> 2.539s, and
sparse SYMBOL 0.736 -> 0.485s. The full control table, PMUs and tests are in PROGRESS.
This closes the dense INT gap against Light, which takes about 1.77s. It does not
add LLVM coverage or establish a win over the best eligible execution strategy.

After the fix, the selective case still takes approximately 0.604s adaptive versus
0.217s Light on one core, and 0.589s versus 0.169s on four cores. Prioritize retaining
the existing filtered-stream alternative before adding another compiled helper.
The planner still destroys that alternative while extracting the predicate.
Selection must account for complete execution and startup, not a fixed threshold
derived from these predicates. Establish shared-snapshot ownership and a safe
commit point before runtime switching; opening the ordinary time-frame cursor
twice is not a valid pilot.

For later native fusion, the updated local-probe profile puts roughly 36% in
matching (including 29% in the scalar predicate), 24% in timestamp lookahead and
13% in aggregate updates. These are overlapping CPU locations, not hardware stall
classifications. Compile a strategy-specific matching-to-consumer region with
amortized calls and shared aggregate emission; a per-probe predicate helper has
limited headroom. Parallel matching and its independent temporal state remain open.

## Retained-filter milestone (2026-11-03)

The snapshot/ownership milestone above is implemented. The planner transfers the
compiled predicate and its bindings into the shared `CompiledFilterBatch` helper.
A time-frame cursor can lend its open frame in its own column layout; ASOF retains
the raw cursor before applying projection, and caches row offsets rather than
addresses. There is no second RHS reader or asynchronous cursor to reconcile.
The shared helper remains responsible for scalar initialization and query-scoped
compiled bindings; ASOF's scalar hot path calls the underlying function directly.

Forward mode keeps both compiled-filter batches and fused LLVM map updates.
Bounded real-work trials compare their cost per input row, with hysteresis and
periodic reevaluation. Empty/non-progressing probes cap trial calls. Unsupported
frames decline before mutation. The outer ASOF chooser still owns whether forward
work is appropriate, its scan budget and fallback. Stable rejected rows may be
skipped regardless of timestamp; qualifying future rows stop progress. The
retained region does not fuse the left consumer or add parallel matching.

Selective controls improve about 1.5–1.6x across INT/LONG/SYMBOL, while high-survival
controls pay 2–3% for trials. The full fixed-work evidence and rejected experiments
are in PROGRESS. FLOAT predicates retain the prior scalar/fused path because
legacy JIT and scalar comparisons disagree at epsilon boundaries; a regression
checks both stock JIT and LLVM against scalar execution. DOUBLE, variable-key,
projection, lifecycle and Parquet controls pass. No new configuration switches.

The next goal is still to beat the best eligible architecture, not just the
previous adaptive path. Light with LLVM filtering remains materially ahead on
the selective control. Profile the remaining per-probe map/record/consumer work
and compare amortized strategy-specific fusion with the existing filtered-stream
execution. Do not infer its hardware bottleneck from the new Unsafe/map samples
alone. Parallel matching remains a separate, explicitly unclosed milestone.

## Bounded matching/consumer experiment — initial result

The following experiment now exists outside SQL admission, with full measurements
in PROGRESS. Bounded native filtering, map updates, left-key probing and shared
aggregate emission improve the 6.24% and 33.3% survival controls by 1.29x/1.24x
against their fastest eligible strategy. Selective INT/LONG still lose to Light;
SYMBOL ties it. The ambitious 1.5x best-control threshold was not reached.

Two architecture constraints were validated. A copied projection of fixed-width
RHS payloads can survive frame changes and feed existing aggregate IR; a raw-row
budget can replace frequent native cancellation upcalls with bounded returns,
including across entirely rejected prefixes. Neither result authorizes always
scanning the RHS or bypassing adaptive history/memory/work accounting.

Before widening production plumbing, test the shared vector-predicate scheduling
and the selected RHS input accesses: the predicate alone consumes about 138ms
of the selective control, and fewer retired instructions did not predict the
winner. Use varied arithmetic, signed/null/extreme values, key/payload types and
selectivity; retain full-query best-strategy comparisons. Hardware stall cause
is still unproven. Keep this as an exploratory experiment until both the
performance and exact adaptive-state integration gates are satisfied.

## Shared predicate follow-up (2026-11-04)

Completed the predicate-throughput experiment. Wider selection masks and guarded
LONG narrowing did not earn their complexity. A small type-proven INT division
lowering is retained in the existing vector emitter: roughly 8–14% isolated filter
improvement, about 3–4% complete-query improvement in the final control. Fifteen
focused compiler/SQL tests pass. Measurements and rejected hypotheses are in
PROGRESS. This does not improve the original LONG-predicate selective control or
change the admission decision for the scratch native matching region.

The next experiment should target selected-row access and matching, with varied
key and payload types and survival rates. Retain Light and adaptive execution as
controls, log actual strategy work and JVM compilation, and distinguish runtime
strategy variation from compiler effects. Do not add vector-width tuning or a
runtime value-range framework based on these results.

## Consumer scheduling follow-up (2026-11-04)

Completed that experiment. Coarser raw filter batches inside the bounded native
region improve the selective control from about 242 to 187ms. Moderate survival
and scattered selection now beat their best eligible SQL controls by 1.64x/1.59x;
other INT/LONG/SYMBOL controls improve about 1.14–1.25x. The performance threshold
is therefore reached on two regimes. It is not a general ASOF breakthrough or an
SQL admission decision. All 49 scratch correctness/lifetime comparisons pass.
PROGRESS records the hardware evidence, warmup sensitivity and rejected variants.

The native result does not justify enlarging every compiled filter batch. A
scratch change to the existing SQL helper failed to reproduce the gain after
longer warmup and did not reduce load-wait cycles. Keep production batching and
the existing adaptive strategy choices unchanged in this milestone.

The next milestone is a narrow integration contract for matching-to-consumer
fusion within forward mode, before widening operator coverage:

1. Keep the existing row-ID map authoritative. Trace every Java/native writer,
   historical lookup, map refusal and strategy transition before deciding whether
   a copied payload projection can coexist with that state. The scratch payload
   map cannot replace it directly.
2. Define bounded input consumption and consumer updates against borrowed frames.
   Preserve first-qualifying-future-row semantics, yield/resume, timestamp scaling,
   memory accounting and cancellation. Charge speculative filter lookahead; a
   larger batch can cross a budget check and must have an explicit bound.
3. Demonstrate a complete SQL consumer using the shared aggregate IR and the
   existing strategy owner. Validate INT/LONG/SYMBOL keys and multiple aggregates,
   empty/rejected prefixes, partition changes and fallback after partial progress.
   Avoid a separate execution framework or forcing a full-prefix scan on lookup
   workloads. Reuse current fallback for unsupported storage/type combinations.
4. Recompare complete execution against the fastest eligible strategy, including
   sparse probes and high survival. The current 32K setting is an experimental
   point on one host, not a hardware-independent sizing policy. Keep startup and
   steady-state results separate and include actual strategy-work counters.

Parallel matching remains an independent open milestone. Do not conceal its
absence by describing the current single-core native region as a complete engine.

## Row-ID consumer and integration contract (2026-11-04)

The state audit rules out replacing the adaptive maps with the scratch payload
map. `AsOfJoinDenseRecordCursorFactoryBase` uses the same two maps in two ways:

| Mode | Forward map | Backward map |
| --- | --- | --- |
| Forward | latest qualifying row ID in the scanned interval | latest qualifying row ID before that interval; indexed history may cache LONG_NULL |
| Memoized lookup | per-key validity boundary | per-key result, including LONG_NULL for a known miss |

`switchToLookup()` closes both maps. `switchToForward()` discards memoized state
before establishing a contiguous frontier. `retryWithoutScanState()` retries the
current master row after optional-map refusal; it must not replay a completed
consumer update. A forward-map miss alone never proves an unmatched result.
`setupSlaveRec()` still checks tolerance and supplies the chooser with alternative
backward distance when the matched row and frontier share a frame. Native fusion
must preserve that evidence, including cases where no observation is made.

The revised experiment now stores exactly one LONG row ID per map value. It loads
aggregate inputs from the current RHS frame or returns before consuming the probe
to ask the existing `TimeFrameCursor.recordAt(recordB, rowId)` for history. It
retains offsets/IDs across navigation and rebuilds borrowed addresses. Known-miss
sentinels produce NULL inputs without attempting frame access. No payload side
cache or widened map layout is required. The one-core moderate-selectivity control
still clears the performance target; 85 comparisons include alternating historical
partitions and INT/LONG/DOUBLE payloads with INT/LONG/SYMBOL keys. See PROGRESS for
the cost and pathological-case measurements. This remains a standalone experiment,
not execution through the adaptive cursor.

The smallest next integration should use these boundaries:

1. **Borrow and commit the master span.** `PageFrameRecordCursorImpl.hasNext()`
   advances its private row cursor and charges `rowsProducedSinceSkip`.
   `recordAt()` only positions a record; it cannot commit native consumption.
   `skipRows()` also changes the post-skip LIMIT/decode budget and may reshape
   frames. Neither operation is a substitute for a bounded consume operation.
   Add an optional span operation on the existing page-frame cursor, initially for
   a forward entity scan with no row filter. It must bound the span by the open
   frame and remaining decode/LIMIT budget, preserve the current record when
   nothing is consumed, and charge only committed rows. Reuse the same cursor and
   snapshot; do not open a second master reader or invent a staging cursor.
2. **Let the current ASOF owner admit the call.** Enter only with initialized
   forward state and matching input/frame layouts. Bound probes by the remaining
   work window, and RHS evaluation by the caller's remaining scan budget. Publish
   the first unconsumed probe, RHS frontier and per-probe work observations on every
   resumable exit. The existing owner applies `observeWork()` and switches modes;
   it must be able to stop at its normal decision boundary. Keep filtered lookahead
   evaluation distinct from consumed RHS rows. Indexed timing/sampling requires
   its own preserved boundary before admitting that strategy.
3. **Resolve history before consumer mutation.** Consult forward then backward
   maps in their established roles. An unresolved history lookup returns to the
   existing resolver on the same master row. Historical-frame access, symbol
   translation misses, map growth and unsupported frames are explicit returns.
   A map-limit retry retains completed aggregate contributions, retries only the
   unfinished probe, and discards stale map/frame views. Fatal memory errors and
   cancellation continue to abort; they are not eligibility declines.
4. **Use the existing aggregate owner.** Bind ordinary functions through
   `AggregateUpdates`; keep initialization and result state with the existing
   unkeyed group-by owner. A Java fallback after a native prefix must use existing
   updates, not initialize the aggregate again. Start with supported fixed-width,
   non-allocating states; reject unsupported arguments before any mutation.
   Map projected RHS indexes back to raw-frame columns, preserve timestamp scaling
   and tolerance, and reuse ASOF's symbol-translation owner. The scratch
   `SymbolKeyTranslator` is not justification for a second translation cache next
   to `SymbolTranslatingRecord`.

Validate span handoff/partial completion before expanding the compiler API. The
key risk is now exact cursor/chooser ownership, not whether copied payloads are
necessary for speed. Do not add unused production hooks ahead of that check.

## SQL integration milestone (2026-11-04)

The four boundaries above now have an implementation. This supersedes the
standalone-only status of the row-ID experiment:

- `PageFrameRecordCursor` exposes optional bounded borrow/commit operations.
  `PageFrameRecordCursorImpl` admits its ordinary unfiltered forward row cursor,
  checks that record A agrees with the row cursor, and respects the current
  frame and post-skip LIMIT budget. Other cursors decline. No second reader is opened.
- `GroupByNotKeyedRecordCursorFactory` binds through ordinary selected-column
  projections to `FilteredAsOfJoinFastRecordCursorFactory`. `AggregateUpdates`
  supplies the existing function semantics and state offsets. The Java owner
  initializes the aggregate, handles empty input, retains cached rewind behavior,
  and performs ordinary updates for probes outside the generated region.
- The ASOF owner lends at most the remaining 64-probe work window. The native
  region returns completed-prefix and RHS-frontier progress, including on optional
  map refusal. Only the unfinished probe is retried. Work observations return to
  the existing chooser before another window; indexed, lookup, backward-history
  resolution and strategy transitions stay with that owner.
- `ResolvedAsOfJoin` uses the actual latest-row-ID map, existing symbol translation,
  and raw RHS record A for the current frame. Historical payload access borrows
  record B between native invocations. `recordAt()` preserves the TimeFrame;
  Parquet buffer pins preserve A while B changes. A missing forward-map key returns
  to ordinary history resolution. No payload map or second symbol cache is added.

Admission currently covers one matching INT, LONG or SYMBOL key; stable supported
RHS predicates; and fixed, non-allocating scalar aggregate state with INT, LONG or
DOUBLE numeric inputs (including supported shared argument expressions). It is a
forward-region optimization inside filtered ASOF, not a replacement for all ASOF
algorithms. Unsupported consumers/frames use ordinary execution. Composite keys,
variable-width aggregate inputs, grouped consumers and parallel match production
remain separate milestones. Filter residuals use a scalar generated loop when the
existing vector selection lowering cannot represent them.

### Corrected frontier assumption

Reading `scanFilteredBatch()` invalidated the experiment's proposed requirement
to locate the first *raw* future timestamp on every probe. For a stable predicate,
rejected rows can never become matches, including rows later than the current
probe. The existing Java algorithm stops before the first **qualifying** future
row, bounded by its raw-work budget. The integrated region now follows that
algorithm. Wide binary search, narrowed search, interleaved search and a SIMD raw
frontier pass were measured and removed: they added work absent from the existing
algorithm. This correction matters more than selecting a faster search kernel.

The retained vector path uses the shared selection lowering, remembers selected
row offsets within the current RHS frame, and updates the ordinary map in timestamp
order. Key/timestamp prefetching after selection is a measured experiment; see the
current PROGRESS entry for retained results and controls. Addresses are not retained
across frame navigation. The optional region uses the existing deferred-kernel
owner, so a query that stays in lookup mode need not compile it.

### Evidence gates completed and remaining

Real-SQL sparse/moderate/dense, key-type, probe-spacing and aging-history holdouts
now pass, with prior LLVM adaptive, LLVM Light and stock controls. A 5M-probe
phase reversal also preserves results: about 2.5M probes run natively before lookup
takes over, with 1280ms versus 1303ms prior LLVM. The retained
region improves the selective controls; dense execution is effectively tied with
prior LLVM. The permanent runner reproduces the moderate result and reports native
aggregate-probe counts. The normal reactor build and 24 focused tests pass, plus
9 existing adaptive/fuzz controls. See PROGRESS for measurements and proof limits.

The next milestone is strategy cost, before widening generated lookup. The
all-match, 1M-probe case stays entirely in lookup and costs about 3.35s. A scratch
counterfactual sets the existing owner's `adaptive` field false before cursor open
and runs its forced-forward strategy: about 1.03s, same six aggregate outputs.
There is no production hint/threshold/configuration change. The existing filtered
planner branch ignores the dense hint, so EXPLAIN alone would not establish this
counterfactual. The scratch harness prints the override explicitly.

The fixture gives an exact napkin check: 511,998,464 backward key comparisons
versus about 499M forward rows. `observeWork()` admits an unindexed forward trial
only above a 2:1 backward/forward row ratio, so these comparable row counts never
qualify. Across five complete queries the counterfactual reduces cycles from
93.57 to 28.92 billion and load-not-complete cycles from 24.59 to 4.60 billion.
That is a measured difference between configurations, not an admission-only
comparison or proof of a single microarchitectural resource bottleneck. Disabling
`adaptive` also removes optional-map caps, starts with an unlimited forward budget,
changes native filter refill size and bypasses normal work observations/exits.
The object was constructed as adaptive before that reflective change, so the old
control is a hybrid rather than the ordinary forced-Dense configuration.
LONG reproduces the configuration difference at 3617 -> 1099ms, and SYMBOL at
3117 -> 1038ms; all six outputs agree. These remain single-host warm measurements.
Conversely, local matches take 473ms with automatic lookup and 1025ms with forced
forward. Blindly forcing the new region would regress them.

The previous LLVM forward implementation takes 978ms (935–1015ms),
slightly faster than fusion at this probe spacing. This disproves the stronger
hypothesis that the chooser failure was introduced by consumer fusion. Existing compilation already changes per-row costs;
row counts remain useful safety bounds but do not fully predict execution cost.

### Next experiments: isolate admission, then compare lookup implementations

First replace exactly one unindexed admission decision in a scratch overlay,
keeping `adaptive=true`, map caps, computed scan limits and subsequent policy.
Compare it with an equally instrumented baseline in balanced fresh JVM processes.
Count actual lookup candidates, backward history work and consumed forward rows;
`getNativeForwardRows()` excludes fused aggregate RHS work. A forward budget alone
does not bound cold history construction or deferred compilation, so do not yet
describe an online trial as cheap or fully abortable.

Next compare sink-based Java equality, typed Java equality and a scalar generated
lookup under the same policy. Preserve the first-candidate Java check. Generated
work may borrow only the remaining current-frame span, retaining Java's memo,
tolerance, maximum-work, frame-navigation and cancellation boundaries. INT/LONG
and translated SYMBOL are required from the outset. Reuse existing predicate IR
and symbol translation. Keeping candidate counts and results equal while reducing
instructions/cycles per probe would support removing per-candidate execution
overhead; it would not alone identify an exclusive hardware limiter. Include local
matches as the native-call/setup regression control.

Only after those controls establish the remaining crossover, evaluate the
cost-based policy below. Do not calibrate it to the old Java implementation's cost.

The November 4 controls are complete; details are in PROGRESS. Isolating admission
under normal caps/budgets gives 2889 -> 1139ms. Generating the existing scalar lookup
instead gives 3087 -> 1003ms in a separate balanced comparison, versus 3171ms for
typed Java equality. LONG and translated SYMBOL corroborate the generated-loop
gain, and replicated local/selective controls remain near parity. The best prior
forward control is 1004ms: the motivating admission gap largely disappears once
lookup is compiled. This changes the next milestone from a new chooser to a
production-quality bounded-lookup integration.

Keep that integration inside the existing ASOF owner and optional compiler
contract. Preserve the first-candidate check and frame/memo/work boundaries;
retain ordinary fallback for unsupported keys, predicates and frame layouts.
Use the existing saturating timestamp semantics (a scratch regression caught this
edge), shared predicate lowering and existing symbol translation. Add a tiny
nonlocal-query startup control before default admission: deferred compilation on
the first miss still adds 10–13ms in the prototype. Local queries already avoid it.
No production policy change is justified by the one-shot forced admission alone.

The next performance experiment after integration should target the measured
remaining work, not assume another chooser will provide the old 3x gain. The
generated fragment accounts for about 43% of user-cycle samples, and its expensive
load samples are at predicate/key reads. Larger lookup/navigation/consumer fusion
and bounded vector search are competing hypotheses; preserve both forward and
lookup algorithms and compare complete-query costs. A kernel-only improvement
cannot by itself remove the substantial work still outside that kernel.

1. Reuse the current `AsOfJoinDenseRecordCursorFactoryBase` owner, map transitions,
   optional-memory refusal and raw-work limits. The indexed branch already samples
   complete calls using `sampleCountdown`, `indexedNanos`, `forwardNanos` and a
   trial cooldown. Audit whether its sampling can cover unindexed execution before
   creating another chooser. Also retain the existing filtered forward selector
   (`scanForwardBatch`/`observeForwardTrial`): it already compares batch and
   generated scan work. Do not add an independent policy that measures one path
   while executing another. A sample must include the implementation actually
   used; timing only `hasNext()` omits the new native aggregate spans.
2. First instrument a scratch version at completed master-window boundaries.
   Compare equal progress (completed probes) and retain raw RHS work, transition
   cost, compilation cost and actual native-probe count. Never time every RHS row
   or treat empty/non-progressing native calls as cheap work. Separate one-time
   compilation from steady cost estimation while charging it to query latency.
3. Let row-work bounds admit a small, abortable trial when uncertainty is material;
   measured cost decides retention. Do not lower the existing multiplier based on
   this fixture or bake in an LLVM/CPU speed ratio. Preserve tiny-query startup,
   hysteresis/cooldown, LIMIT/cancellation responsiveness and exact result/history
   semantics. A rejected trial may not repeatedly allocate/clear large maps.
4. Falsify against nearby matches, sparse probes, phase reversals, aging/missing
   keys, expensive/scalar predicates, INT/LONG/SYMBOL keys, memory refusal and
   indexed SYMBOL. Include prior LLVM scan and fused execution separately: fusion
   itself is not guaranteed to win at every probe-to-RHS ratio. Only then decide
   whether generated backward matching is the next bottleneck to attack.

This is consistent with the runtime-measurement approach in
[Efficiently Compiling Dynamic Code for Adaptive Query Processing, ADMS 2022](https://db.in.tum.de/people/sites/schmidt/papers/dynamic-blocks.pdf),
but does not require its dynamic-code framework. Section 6.3 reports an especially
relevant failure: the preferred variant changes after LLVM optimization. Its
one-time exploration also cannot react to later distribution changes. Our inference
is to sample the actual executing implementation and retain phase-change evidence;
not to import its thresholds or introduce a general bandit/reoptimization layer.

Keep a competing hypothesis explicit: generating the existing bounded backward
lookup may change the ranking again. For the all-match INT fixture, approximately
512M key comparisons represent about 2GB of useful key bytes, before timestamp,
predicate and navigation traffic. A vector key/predicate mask could reduce the
instruction work substantially; that is a napkin hypothesis, not a measured floor.
Prototype only the current frame's bounded predecessor search, with INT/LONG/SYMBOL
keys and ordinary history/tolerance/cancellation boundaries, and compare with the
best forward control. Do not let a successful chooser experiment freeze today's
implementation costs into permanent policy.

### November 4 integration outcome and revised next milestone

Bounded lookup is integrated through `CompiledAsOfLookup` and the ordinary optional
compiler. A first attempt added overhead to Java's candidate loop; the accepted
implementation instead gives both Java and LLVM a bounded frame span, leaving
memo reuse, work limits and mode changes with the existing owner. No separate
chooser, cursor hierarchy or configuration switch was added. Normal SQL, seeded
fuzz, memory-failure and timestamp-boundary checks pass. See the latest PROGRESS
entry for replicated controls and the material Java process variation.

The next milestone is first-use compilation admission. A fresh one-probe query
pays about 4.4ms in the backend even on a warm JVM/backend; immediate matches avoid
lookup compilation altogether. Compare the existing backends/optimization tiers
on the same typed search and include planning, compilation, execution and factory
reuse. Choose an automatic policy only from measured preparation cost and useful
search work, retaining the current Java span as the baseline. Do not hard-code a
probe-count crossover, add a new toggle, or tune algorithm selection against the
superseded Java candidate loop. Broader consumer fusion and parallel temporal
production remain subsequent milestones.
