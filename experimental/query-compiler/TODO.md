# Deferred aggregation performance work

Type expansion checkpoint, 2026-10-30: retained changes and final protocol results
are committed; see README for current coverage and PROGRESS for measured gains,
regressions and rejected experiments. Next coverage gap: shared VARCHAR-to-numeric
expression binding, followed by capture-aware regex evaluation. Streaming partition
writes remain an experiment pending ownership/completion and adaptive-selection work.
Continue one gap at a time, preserving ordinary function/state contracts and exact
results before measuring 500M-row controls.

- [x] Static SYMBOL literal equality and bounded IN, including initialized runtime values.
- [x] Static SYMBOL COUNT in keyed and unkeyed generated aggregates.
- [x] Keyed SYMBOL/INT DISTINCT using the ordinary integer-set ABI; retain static-dictionary scope.
  Scalar DISTINCT and parallel deferred INT-set union remain separate gaps.
- [x] Ordinary INT/LONG AVG and scalar SHORT/INT/LONG AVG, including join consumers.
- [x] Add SUM(LONG), AVG(INT) and 128-bit AVG(LONG) to the existing SYMBOL array path.
- [x] Direct DOUBLE ordinary COUNT/SUM/MIN/MAX/AVG, scalar and join consumers.
- [x] DOUBLE vector/SYMBOL-array COUNT/SUM/AVG/MIN/MAX contracts.
- [x] DOUBLE binary arithmetic, integer widening and SQL comparisons/NULL predicates.
- [x] DOUBLE raw-bit grouping keys, including computed/composite keys and column tops.
  Standalone DOUBLE projection remains a gap. Raw-bit batch encoding was correct
  but added marshalling work and regressed 4/16-core ordering by 1–3%; the experiment
  was rejected. Fuse typed expressions into existing map/order consumers next.
- [x] Finalized-map consumers read DOUBLE values/keys and static SYMBOL IDs using
  existing worker slots and dictionary owners. On 500M rows/8.4M inner groups,
  the SYMBOL/DOUBLE two-stage case improves 1.41 to 1.20–1.25s at eight cores.
  Outer AVG and variable source keys remain separate contracts.
- [x] VARCHAR COUNT, including searched CASE, scalar consumers and compact input partitions.
- [x] Mixed batch/scalar aggregates now try the existing generated reduction
  before ordinary batch/scalar phases. Fully batched plans retain their current path.
- [x] Correct ordinary Rosti `keyedIntSumIntMerge`: retain the LONG accumulator
  instead of narrowing the source to INT before merge. Native regression reproduces
  the wrong answer; rebuilt native library and generated-array comparisons pass.
- [x] Correct ordinary Rosti integer AVG missing-key wrap-up: combine floating
  partial sums with the actual 64/128-bit state, then publish the final DOUBLE.
  Refresh map views after NULL-key insertion can grow the map. Native tests cover
  existing/new NULL groups, 128-bit sums and growth; mixed generated floating/
  integer plans reuse this corrected ordinary missing-key boundary.
- [x] Fuse supported scalar filters into the existing register reduction, retaining
  SIMD predicates and Parquet late materialization. On 500M rows, integer AVG
  drops 260–271 to 146–147 ms and controller traffic 16.2 to 8.2 GB. The 6.3%
  selectivity control pays about 3–4% more; no selectivity-specific policy added.
- [x] Numeric result ranking: INT/SHORT/DOUBLE full sort and Top-K, including
  finalized AVG, signed zero/NaN ordering and ordinary fallback. On 500M rows,
  selected K=64 cases gain 4–5%; aggregation still dominates.
- [x] Resolved VARCHAR literal equality, prefix and suffix predicates with shared
  query-tracked constants and bounded comparison loops. Equality/short prefixes
  reach the8.2GB header-scan floor on500M rows; mixed filtered AVG saves9GB reads.
- [ ] Extend remaining VARCHAR expressions, runtime literal predicates, long
  substring and grouped VARCHAR DISTINCT.
- [x] Dictionary-domain SYMBOL predicates: reuse resolved LIKE/ILIKE/regex match sets,
  compile exact range/bitmap/sparse membership, and share immutable query-owned state.
  Official 1BRC filtered COUNT falls about 3.1 s to 78 ms; 16-core execution reaches
  the measured input-bandwidth floor. Sparse/skewed domains, 4/8/16 cores and
  lifecycle failures are covered; other CPU families remain unmeasured.
- [x] Mixed SYMBOL/VARCHAR/numeric AND/OR/NOT reuse the existing residual/filter
  lifecycle. The 500M-row SYMBOL OR control improves 669 to72ms at eight cores;
  numeric OR and conjunction controls remain unchanged.
- [x] Remove per-bucket occupancy branches from generated inline-map scans.
  Existing cursor order and zero-key placement are preserved;500M-row MIN/MAX
  aggregation improves14–18% at eight cores with branch misses1.57 to0.93/input.
- [ ] Continue the moonshot by removing measured materialization or dependent-access
  costs. Keep discovered hardware budgets, snapshot/query ownership and complete
  compilation/build/merge/output accounting; do not assume this host's optimum is universal.

Recorded 2026-10-26. Continue compiler coverage first; this is a later experiment,
not a change to execution defaults.

2026-10-27 actual-data diagnosis changes the immediate priority:

- [x] Correct generated scan/allocation checkpoint policy through the existing time-throttled
  breaker. Q13 makes 4.42M socket polls/query versus ~70 for stock; a temporary
  scan-only counterfactual lowers execution from 309 to 279 ms. Preserve timeout,
  explicit cancellation, disconnect detection and phase/worker ownership. Audit
  allocation/zeroing and merge callbacks as well as the main row loop. Scan and
  standalone allocator bindings are fixed and tested. Owner-provided phase
  callbacks remain unchanged; audit their cadence separately.
- [ ] Reduce aggregation and merge memory traffic. Q33's busy phases reach the
  measured ~63 GB/s controller ceiling; generating fewer instructions cannot
  materially beat its ~0.78s floor while moving ~49 GB/query. Investigate bounded
  local state and existing shard ownership; retain complete build/merge/cleanup
  timing. This result is specific to this actual-data shape.

- [ ] Complete redistribution-specific attribution after the actual-data
  scan/update, final merge and Top-K breakdown (now measured inside full queries).
  Scan timing currently includes redistribution. The old wider-state profiles
  remain separate evidence; see the 2026-10-27 diagnosis in PROGRESS.md.
- [ ] Investigate replicated state: the 500M-row / 8.39M-group run creates roughly
  66M worker-local groups before merging. Determine how much time and memory this
  replication costs before selecting a replacement strategy.
- [ ] Compare guarded direct arrays, bounded local pre-aggregation followed by
  partition-wise aggregation, and a specialized shared aggregation table. Reuse
  QuestDB's existing owners, shards and scheduling. Account for discovery,
  materialization, synchronization, skew, merge and cleanup in complete timing.
- [ ] Vary cardinality, range density, key order, skew, aggregate width and worker
  count independently; use at least 500M rows and unchanged stock defaults.
  Require exact results and a repeatable full-query improvement.
  The 2026-10-28 controls now cover unique/repeated keys, skew, wider state, K=64/65
  and same/split L3 placement; see PROGRESS.md. Other machines remain unmeasured.
- [x] Reduce retained variable-key state when local combining is ineffective.
  Existing compact-input partitions now accept variable keys after a full window
  with zero combining, when numeric inputs are narrower than aggregate state.
  The 500M-row / 16.78M-group case retains 16.65 rather than 24.09 GB of pages;
  complete hot execution falls 2.65 to 2.38 s. Reassess each frame and retain the
  existing maps, allocators, budgets and retry paths. See PROGRESS.md.
- [ ] Bound total retained variable-key memory. Compact records still grow with
  input rows; peak RSS is now about 27.7 GB, above the older unbounded-map path's
  20.4 GB. Evaluate admission or draining through existing owners and query
  budgets. A small local map alone does not bound query memory.
  The disposable spill experiment completes 500M unique groups under 4 GiB and
  2 GiB query limits; the resident pipeline fails under 8 GiB. It relocates the
  existing packed records and reuses generated merge/subdivision/Top-K. This is
  feasibility evidence, not an integrated fix: cached spill reads, soft per-slot
  watermarks and whole-partition reloads leave strict admission and skew open.
  See the bounded-retention design in FOUNDATIONS_PLAN.md and PROGRESS.md.
- [x] Measure variable-key bytes before extending cache subdivision to that path.
  The existing HLL now carries deterministic distinct-key width samples. Planning
  counts aligned used entries and the actual power-of-two offset table at the
  map's load factor; unused OrderedMap heap capacity is not cache working data.
  Variable state/input records share existing subdivision, page reuse and owners.
  The 500M-unique-key control improves 4.47 to 3.30 s. Small-map cases can pay
  roughly 1–3% for estimation; this is advisory sizing, not a total-memory bound.

Evidence and limits:

- Generated Top-K takes 7.58 ms versus 8.81 ms for Java Top-K over 8.39M groups;
  the complete query is about 2.4 s. These phase numbers are repeated scans of
  already-built maps, not an exact in-query time breakdown. A different Top-K
  algorithm is unlikely to materially improve this LIMIT 10 workload. Larger K
  merits a separate sorted-buffer/heap/selection comparison.
- An older compact-range, wider COUNT/SUM experiment improved 1.24 s to 0.41 s
  with direct arrays, including range discovery. Its sparse-key native hash
  fallback regressed. Do not extrapolate that speedup to the current workload.
- At 8.39M groups, COUNT+SUM values alone occupy 128 MiB per worker, or 1 GiB
  across eight workers, before any additional metadata. Direct addressing still
  requires a memory budget and a measured admission policy.
- The older synthetic run has no established precise hardware bottleneck. The
  actual-data Q33 diagnosis establishes bandwidth saturation in its busy phases.
  Existing sharding does not by itself bound the working set while every worker
  updates groups across all shards.

References: [experiment log](PROGRESS.md), [current coverage plan](FOUNDATIONS_PLAN.md),
[DuckDB bounded pre-aggregation](https://duckdb.org/2024%2F03%2F29/external-aggregation),
[shared versus partitioned aggregation study](https://www.vldb.org/pvldb/vol19/p523-marcus.pdf).
Temporary evidence: `/tmp/query-topk`, `/tmp/query-optimal`, `/tmp/query-map-profile`.

## Coverage and semantic boundaries confirmed 2026-10-28

- [x] Bind ordinary aggregation without GROUP BY for existing scalar COUNT and
  integer SUM/MIN/MAX emitters. The async factory retains vector/metadata
  specializations and uses generated reduction for its row-update paths. Existing
  worker state, merge order and empty-input semantics remain; generated copying
  and merging consume native or Java partials. Broader aggregate types and the
  mixed-coordinate late-materialization boundary remain separate gaps. See the
  2026-10-28 scalar-reduction entry in PROGRESS.md; performance is not measured.
- [ ] Investigate LONG SUM overflow into the reserved NULL sentinel before making
  broader equivalence claims. Both `SumLongGroupByFunction` and `Pipeline.Sum`
  interpret `Long.MIN_VALUE` as empty state. For example, the non-null sequence
  `[Long.MAX_VALUE, 1, 5]` produces 5 when updated serially, whereas merging
  partials `[Long.MAX_VALUE]` and `[1, 5]` produces `Long.MIN_VALUE + 5`.
  The integer-extrema SQL fixture exposed differing sums under different merge
  orders; all extrema matched. Its companion SUM now uses INT input whose total
  fits LONG. This existing semantic issue is separate from adding MIN/MAX.

## Closed scheduling gap: finalized aggregate read parallelism

- [x] `GroupByFunction.isReadThreadSafe()` now describes concurrent finalized
  output reads. Its default retains the existing thread-safety requirement;
  `CountDistinctLongGroupByFunction` declares its plain count getter safe while
  keeping mutation thread safety false. Primary/shared Top-K cursor paths use the
  read contract after build/merge drains. This also covers Java fallback; there is
  no compiler-only promise, new executor, threshold change or configuration toggle.
- Temporary current-source diagnostic on actual Q13, eight physical cores:
  6,019,102 groups; serial Top-K49.6ms versus parallel9.6ms. Complete warmed query
  409.5ms versus368.0/370.4ms in parallel/serial/parallel processes,12 measured
  requests after12 warmups each. Exact ordered results and generated row counters
  match. Scan remains259-261ms, merge92-93ms, cleanup7ms. This is a scheduling
  counterfactual; integrated results are recorded in PROGRESS.md.
- [x] Three500M-row fixtures exercise8.39M uniform/skewed groups and16 groups,
  both orders/capacity classes, exact selected aggregates and rank multisets.
  The unchanged default threshold is5M groups. Focused SQL checks cover forced
  ordinary getter fallback, cancellation/draining and reopening on all backends
  and both worker modes. Candidate Java is a separate performance control.
  Q13's scan/merge remain the larger opportunity; even removing its serial Top-K
  entirely only approaches the1.5x stock target. Do not tune thresholds to this query.
- Whole-query controller intervals peak around41GB/s in this eight-core diagnostic;
  this is below the earlier whole-machine streaming ceiling but is not a calibrated
  eight-core bandwidth diagnosis. A four-event PMU group failed to schedule;
  a two-event retry counts correctly. No hardware-stall mechanism is established.
  Phase CPU accounting subsequently shows57ms process CPU across50ms elapsed
  (about14% of eight-core capacity), versus75ms across9.6ms (about98%). This
  establishes the serialized scheduling restriction; it does not identify a
  memory-stall mechanism or imply less total CPU work. The scoped study passes
  the USE/hardware gate. Evidence: `/tmp/query-read-safety`, with the earlier
  temporary controls under `/tmp/query-current-clickbench/q13-*`.

## Remaining VARCHAR boundaries

- [x] Generate single-VARCHAR encoded Top-K build: prefix rejection, normalization,
  key-heap writes and threshold admission use typed IR against existing buffers.
  Compaction/growth refreshes the borrowed view before resumption.
- [x] Generate worker-result merge and compaction's survivor selection/heap packing
  against the existing normalized entries. Preserve thresholds, scratch memory and
  heap offsets; source buffers remain unchanged. Threshold publication remains
  with the buffer owner.
- [x] Generate encoded Top-K sorting, including the sorts inside compaction and
  final LIMIT selection. Port `vergesort_entries<encoded_var>`, MSD radix and pdqsort
  from existing sources. Preserve prefix/tail ordering, heap rebasing and small/large
  partition behavior; use bounded stacks and tracked, reusable workspace.
- [x] Qualify Q22–Q25 on actual data. The current standard HTML records 34/43 fully
  generated queries. Q25 is fully generated but slightly slower than stock; operator
  coverage does not imply a speedup. See the 2026-10-28 profiling entry in PROGRESS.md.
- [ ] Extend to composite ordering for Q26 without losing its existing partially
  timestamp-sorted execution. Do not replace that plan with a full-table Top-K scan.

- [x] Mixed/composite VARCHAR and wider-state keys now use the existing OrderedMap
  encoding, tracked staging buffer and heap ownership (2026-10-27). Direct SYMBOL
  fields and searched-CASE VARCHAR keys are now bound too. General string
  construction and simple CASE/SWITCH remain outside this binding.
- Extend the borrowed batch boundary to describe each column's row space before
  generating updates over `PageFrameFilteredMemoryRecord`. Its filter columns are
  full-frame and its payload columns may be compacted. The current safe fallback
  remains necessary; disabling late materialization would not close this gap.
- General string expressions, compound NULL/integer predicates and string sorting
  need their own resolved bindings and generated consumers. Current direct NULL
  predicates and LONG aggregate Top-K do not establish that coverage.
- Measure the unstable-key copy/resume path before claiming Parquet performance:
  it currently yields once per new group and rebuilds the prepared window. Preserve
  destination-arena ownership if batching these allocations later.

## Remaining measured performance gaps (2026-10-27)

- [x] Fuse admitted filters into generated VARCHAR Top-K's row consumer (2026-10-29).
  Q25 hot execution falls from 54.6 to 31.8 ms, versus 51.5 ms stock; controller
  traffic falls from 2.90 to 1.69 GB/query on eight physical cores. Independent
  500M-row controls improve 1.09–2.39x; sparse numeric filtering is near parity.
  Reuse predicate bindings, buffer ownership and scheduling, with no new toggle.
  Preserve eight-row numeric predicate windows across resumptions: wider or shifted
  windows regressed sparse conjunctions. Parquet, column tops, casts and unsupported
  predicates retain the existing path. First-request latency still loses to stock;
  the standard protocol measured 633 ms versus 127 ms. See PROGRESS.md for scope.
- [ ] Bind VARCHAR code-point length and generated post-aggregate predicates.
  Integer AVG state and DOUBLE ordering are now supported; Q27/Q28 still fall
  back because their expression/consumer chain is incomplete. Length already has
  an ASCII metadata fast path; preserve the non-ASCII code-point count. Profile
  expression work separately from grouping.
- [ ] Investigate general constant-regex lowering after the shared expression/state
  prerequisites. Q28 spends 53% of CPU samples in regex but stock already borrows
  captured bytes; do not build a query-specific URL parser or assume allocation is
  the cause. Hash/probe and string MIN remain significant even if regex improves.

- Bounded overlapping equality closes much of the measured branch waste: EC2
  eight-core Q33 improves 1147->984ms. The remaining hash loops/tails and serial
  multiply/add chain are the next isolated experiment; keep exact hash semantics
  and measure branches, cycles and complete query time. Equal 256-1023-byte
  primitive comparisons regress 4.1%, despite other length/mismatch distributions
  improving. Also retain the unexplained full-machine Q32 difference: about 3%
  slower in repeated controls even though its generated native objects are
  byte-identical. Query order/process state strongly affects its absolute latency;
  do not use a single pass to claim a suite-wide gain or a clean regression screen.
  Evidence: `/tmp/query-ec2/tail-experiment` and `remote-results/tail-*`.
- Shared final-map VARCHAR COUNT now demonstrates a general hot-query gain with a
  bounded counter cache and observed small-state fallback. Keep the cold-start
  regression visible: Q33's full-protocol first execution is1.62s versus1.41s stock,
  including0.41s compilation. Small-state execution also remains about10% slower than
  private LLVM, despite beating ordinary Java. The opt-in compiler path needs a
  small-query/admission and memory-footprint study before default enablement;
  per-slot cache scratch adds128KiB and the shared result uses256 final maps.
- Generated inner-set union now distributes ranges by estimated input work rather
  than pending-state count, using existing workers and parallel initialization.
  Q8's union phase falls about 115->46 ms and complete hot execution 480->405 ms;
  the standard protocol also improves Q8/Q9. The unchanged scan is about 277 ms,
  already above DuckDB's complete 171 ms. Next isolate scan/state traffic and a
  generated intermediate aggregation consumer before adopting a flat DISTINCT
  strategy: a SQL rewrite with a Java outer aggregate measured about 470 ms.
  Remaining risk: presizing from summed cardinalities wastes memory for overlapping
  sets, and additional ranges can increase traffic. Preserve the serial fallback
  and 256 MiB per-target bound when studying a better capacity policy. See the
  2026-10-29 PROGRESS entry and `/tmp/query-q8-20261029` for controls and limits.
- Scan morsel scheduling is already provided by page frames, the shared reduce
  queue and owner work stealing. Q8's 1M->100K maximum-frame experiment regresses
  412->446 ms and increases controller traffic 13.91->15.80 GB despite a smaller
  final scan tail. Larger-frame controls do not show a repeatable gain. Keep the
  default; investigate aggregation work before another scheduler. Explicit NUMA
  placement remains a separate, unvalidated gap on this one-node machine. Evidence:
  `/tmp/query-morsel-20261029` and the 2026-10-29 PROGRESS entry.
- The generated finalized-map consumer, fixed-key word packing and exact key-only
  duplicate filter are integrated. A flat pair-DISTINCT plan now beats nested sets
  on the independent 500M-row medium (0.96 -> 0.81 s), many-group (3.90 -> 0.84 s)
  and large-state (5.45 -> 3.22 s) controls, but loses on singleton sets
  (0.147 -> 0.190 s). Original COUNT DISTINCT SQL still uses nested sets. The next
  physical-strategy experiment needs observed reuse/state size and a cheap fallback;
  do not add a query-specific rewrite or silently replace every DISTINCT plan.
  Preserve all-NULL groups, finalized-state semantics, mixed aggregates, memory
  budgets and fallback compatibility with ordinary aggregate functions. A mixed
  COUNT DISTINCT + SUM control rules out blanket flattening: medium-domain nested
  sets take 1.03 s versus 1.47 s flat, whereas large-state flat wins 5.50 -> 3.26 s.
  Flat emits 474M partial SUM states on the medium control; its key-only duplicate
  filter cannot apply. A same-snapshot scratch experiment now separates regular
  group state from key-only DISTINCT pairs: 500M-row medium execution improves
  nested/flat/separate 1.16/1.36/0.91 s, large state 5.66/3.20/2.45 s, but singleton
  sets regress 0.229 -> 0.318 s. Four/sixteen-core controls retain the larger-state
  gains. This is two generated loops per selected chunk, not integrated routing;
  see the 2026-10-29 PROGRESS entry and `/tmp/query-fanout-20261029/study.json`.
  State reconciliation is now proven in isolation: the generated exporter appends
  ordinary inline/set values into existing pair partitions before exact deduplication,
  preserving all-NULL groups with a NULL pair. Against membership checks, the 500M-row
  large-state mixed fallback control improves 5.68 -> 4.09 s, but medium regresses
  1.48 -> 1.69 s. See `/tmp/query-reconcile-20261029/study.json`. Next integrate real
  fallback routing and side-state query budgets; scratch whole-frame routing with
  an always-true predicate is not that integration. Only then test adaptive admission
  and phase changes. Serial export preparation remains a measured-strategy concern.
  `CompiledMapAggregate` supplies a finalized-map consumer, not this fan-out or
  runtime strategy choice. Keep the existing snapshot, slot ownership and budgets;
  two independently opened SQL factories would not establish snapshot equivalence.
  The hard compatibility boundary is ordinary-frame fallback: Java's
  CountDistinctLongGroupByFunction still carries an inline singleton or set pointer,
  while finalized native readers consume a count. A side table must incorporate fallback sets before
  counting, not add their cardinalities to already-deduplicated native pairs.
  Reuse the existing task infrastructure without a second worker pool. Export must
  finish after scan joins and before pair merge; existing post-merge dependency
  stages run too late for that preparation step.
- [x] Emit exact 8/4/2/1-byte chunks for known-size copies. Hardware counters and
  500M-row controls support retaining this small change; dynamic copies stay unchanged.
- [x] Reuse staged partition merge for large fixed-key maps. Cache admission avoids
  the measured medium-state regression; 500M-row wider-key and 4/8/16-core controls
  retain the gain. Compact-input merging already stages probes and stays unchanged.
  See the 2026-10-29 PROGRESS entry and `merge-study.json` for scope and counters.
- [x] Hide dependent loads for large numeric nested DISTINCT state. The existing
  adaptive-kernel policy chooses ordinary or staged prefetch loops at invocation
  boundaries, after hardware-derived cache admission. Static cache-size admission
  alone fails when a large map's working set becomes hot. Independent 500M-row
  uniform/skew/phase-change controls and the full protocol validate the retained
  policy; actual Q8 is neutral. Missing cache metadata keeps the ordinary path.
  No new scheduler or setting. See the 2026-10-29 PROGRESS entry and staged-distinct
  hardware study. Cross-machine validation is still open.

- An isolated generated raw-partition experiment now bypasses local preaggregation,
  using the existing pages/allocator/merge scheduler. On Q33 it is worse:696.2->848.0ms,
  43.0->51.8GB/query, with exact results and both near61GB/s. Do not adopt it or assume
  an adaptive bypass alone solves VARCHAR grouping. Additional partition traffic
  outweighs the avoided local probes; a different layout/ownership mechanism needs
  a new measured counterfactual. Temporary code/evidence:`/tmp/query-clickbench-next/raw-*`.
- Generated projection now skips unnecessary record positioning for fully materialized
  output; simple/deep500M-row checks retain exact values and cursor behavior.
- Keep protocol and warmed timings separate. Empty selection-buffer copying explained
  an important part of the gap; the shared fix also benefits ordinary execution.
  Stock's short-query allocation variability still requires repeated full passes.
- EC2 shared-map counterfactual: immutable base/mask metadata and the mutable group
  counter currently share one64-byte shard view. Two loads of this metadata account
  for about26% of sampled instruction locations in the shared kernel diagnostic;
  this is a hotspot, not proof of false sharing. Shared aggregation has substantially
  more REMOTE_HITM events than private aggregation while using only164GB/s of the
  host's525GB/s mixed-streaming capability. Test separating the fields, measuring
  fixed-work latency and coherence events together, at physical-core and SMT counts.
  Keep private maps as a control: on96 physical EC2 cores they beat shared maps
  (137ms versus182ms), while shared wins with SMT. Evidence:`/tmp/query-ec2`.


## String-state footprint and scheduling

- [x] `AggregateSlot.mergeTopKPartition()` currently estimates group bytes from
  `partitionRowBytes`, covering keys and inline aggregate slots. VARCHAR MIN/MAX
  slots contain pointers; their allocator-owned sink capacity/header is additional
  live state. Measure this footprint and test subdivision sensitivity before
  changing the policy. The retained 500M-row / 8.39M-group string case still spends
  ~66% of dispatch slots in backend-memory stalls and moves ~153GB/query.
  Use the existing second-level partition implementation as the counterfactual;
  compare its extra copying against fewer state misses, with a fixed-width
  control and identical final output. Do not infer a cache-capacity bottleneck
  merely from the estimated footprint.
  The 2026-10-28 experiment rejects more aggressive subdivision: 500M-row
  execution rises ~3.1 -> 3.56s, with traffic 153 -> 172GB/query. The missing
  allocation bytes are real, but this counterfactual does not establish a
  profitable cache-capacity policy. Keep the current policy. Borrowed winning
  string representations were also tested and rejected; dependent payload reads
  offset the allocation saving.
- [x] Schedule independent VARCHAR MIN/MAX after exact partition ranking, replaying
  existing records only into selected keys. Preserve ordinary state layouts and
  use the existing sampled cardinality sketch to decline small partitions.
- [ ] When profiling is resumed, complete final-build balanced measurements for
  this schedule, including cross-cache placement, skew and other architectures.
  Earlier accepted profiles and one/four-core checks are encouraging; subsequent
  contaminated/interrupted desktop runs are excluded. No new coverage claim.

## 2026-10-29: vector MAX(SHORT) with all-negative groups

- [x] Fix `kIntMaxShort` in `core/src/main/c/share/vec_int_key_agg.cpp`:
  when another aggregate already created the group, the initial LONG_NULL
  accumulator is narrowed to SHORT zero before MAX. An all-negative group
  can therefore return zero. `kIntMinShort` already checks the sentinel.
  Reproduced with SYMBOL grouping and MIN/AVG/MAX over negative SHORT values;
  scalar SQL and the generated array agree on the negative maximum. The native
  comparison now widens the input instead of narrowing the LONG accumulator.
  A direct regression covers pre-existing/new groups, growth, NULL keys and
  both SHORT extremes; all four Rosti tests pass with the rebuilt native library.

## 2026-10-29: unordered group admission

- [x] Replace the successful constant-key experiment with a runtime key set;
  reuse generated predicate/selection and aggregate-update lowering. Measure
  discovery, parameter publication, scan, merge, output and cleanup together.
  No recompilation when the selected values change. See PROGRESS.md for the
  measured upper bound and the independent 500M-row controls.
- [x] Discover keys from the existing prepared page-frame sequence and reader
  snapshot before dispatch, with bounded work and cancellation. Publish one
  immutable selection to every worker. If discovery cannot supply enough keys,
  use the existing complete aggregation without discarding contributions.
  A numeric grouping-key subset is a cheap prefilter, not a bound on the number
  of composite groups; skew needs exact full-key membership or a measured decline.
- [x] Admit only an explicit nonnegative SQL LIMIT/OFFSET consuming the aggregate
  exclusively, through cardinality-preserving projections. Reject ORDER BY,
  HAVING, output DISTINCT, windows, shared consumers and negative limits. Existing
  `prepareRowLimit`/`selectRows` only promises selection: callers can subsequently
  request the full cursor or exact size. Do not silently weaken that contract.
- [x] Validate cross-worker contributions, NULL/composite keys, selective input
  filters, changing snapshots/parameters, insufficient discovery, hot prefixes,
  offsets and cancellation/reopen. Add only focused contract tests; extend the
  existing maps, allocator and scheduler rather than introducing parallel owners.

The runtime implementation and 500M-row controls are in the 2026-10-29 PROGRESS
entry below the upper-bound experiment. It samples 64 small blocks across the
same snapshot, caps required groups at 16, and declines unselective samples and
mixed/Parquet input. No new configuration switch.

- [ ] Exact full-key admission for high composite fanout, if measurements justify
  it. A selected numeric prefix can still contain arbitrarily many full groups;
  the existing partition allocator remains responsible for those states.
  Finite sampling can also miss a hostile distribution. Current admission stays
  correct but can add an unprofitable membership pass. Do not claim a hard state
  bound or a universal no-regression guarantee.

## 2026-10-29: SYMBOL predicate coverage

- [x] Bind static SYMBOL literal equality/inequality and bounded IN/NOT IN through
  the existing functions' initialized IDs. Keep dictionary lookup and bind-value
  type checking in ordinary cursor initialization; refresh on reopen. Reuse
  selection/count, aggregate selection and VARCHAR Top-K descriptors.
- [ ] Preserve narrow comparisons through vector broadcast of runtime parameters.
  The new equality selection retires 2.99 versus 2.07 instructions/input for the
  previous path on the 500M-row control. A representative parameterized count
  kernel still widens I32 to I64 before vector comparison (optimized LLVM and
  llc output inspected). This is a code-generation lead, not proof that widening
  explains the whole instruction delta. Validate it independently across widths,
  backends and CPUs before changing vector lowering; keep full-query comparisons.
- [x] Bind COUNT(symbol) and keyed COUNT DISTINCT(symbol) through the ordinary aggregate
  contracts, preserving NULL semantics, dictionary scope and worker merge. Reuse
  bounded symbol-domain arrays where admitted; do not create a separate symbol
  aggregation framework.

The predicate extension improves coverage. The measured gain for 16-value sets
comes from avoiding the default Java fallback; enabling the existing stock JIT
for those sets closes the timing gap. See PROGRESS for that diagnostic and the
near-parity small-set cases.


## 2026-10-30: streaming partition writes

- [ ] Integrate full-line streaming writes only after defining their IR memory
  contract and completing a full-query experiment. A matched 500M-row C stage
  gains **401 to297ms** at eight cores: controller reads **16.29 to8.25GB**, writes
  **8.09 to8.10GB**. Ordinary buffered stores take528ms. Four cores gain480 to383ms,
  sixteen387 to256ms; every output byte matches. These are stage results.
  Against the current730ms full query, the104ms stage saving suggests an upper
  first estimate around14%, not26% for the whole pipeline.
- One-core falsification: streaming stores regress 1.375 to 1.472s despite traffic
  falling 25.05 to 17.80GB. Instructions rise 7.4%; two cores gain only 4%.
  Retain ordinary stores and compare complete paths through existing adaptive
  machinery, with safe buffer/fence transitions. Do not encode this host's core
  crossover as an admission rule. Lower traffic alone is not acceptance evidence.
- Let the backend lower aligned full-line copies to its supported vector width.
  A same-Clang counterfactual removes 1.5 instructions/input and gains 4% on one
  core; eight-core time/traffic stay unchanged. The best ordinary one-core loop
  still wins. SSE and ARM64 copy-IR legalization is verified only by compilation;
  neither store completion nor cross-architecture performance is established.
- Reuse `QuestPartitions` and slot/query allocation ownership. Current pages have
  a16-byte header and no64-byte alignment guarantee. Do not add a second allocator.
  Make payload alignment, partial tails and store completion explicit; fence
  before publication and before failures can return to query-memory cleanup.
- Keep partial line buffers across adaptive `partitionInputs` windows. Draining
  each short window destroys next-call alignment and adds tail traffic. Flush
  before slot completion/merge; cover rewind, cancellation and budget failures.
  Preserve packed12/24-byte rows; padding merely to qualify wastes bandwidth.
- Require full-query counters/results, small/skewed-state controls, short frames,
  partition splits,4/8/16-core scaling and another CPU family when available.
  Detailed code-grounded design and diagnostics:
  `/tmp/query-type-coverage-20261029/partition-writes/integration-design.md`.


## 2026-10-30: next VARCHAR expression experiment

- Q28's refreshed profile attributes 53% of sampled stacks to regex matching or
  its wrapper, 29% to maps, and 11% to VARCHAR MIN (inclusive, overlapping groups).
  Traffic is 27.8GB/query at about 13.5GB/s, with 20ms bursts below 47GB/s against
  a 63.4GB/s ceiling. Prioritize fewer executed operations and dependent accesses.
- Read `RegexpReplaceVarcharFunctionFactory.SingleGroupAsciiFunc`: its existing
  result borrows source bytes and preserves unmatched input. Keep that ownership,
  NULL, newline, capture-priority and UTF-8 behavior. No URL-specific parser.
- Share frame-aware numeric/string expression binding before adding code-point
  length. `AggregateUpdates` already receives `ResolvedVarchar.Frame`; reuse its
  descriptors and ordinary AVG state instead of creating a length-only aggregator.
  ASCII length can use metadata; non-ASCII length must count code points.
- Compare a bounded, capture-aware regex compiler with a native JIT helper on the
  same strings and full pipeline. Establish Java semantic compatibility before
  admission; unsupported patterns retain the ordinary function. Measure compile
  cost, helper-call cost, instructions, branch behavior and all grouped output.
  Add 500M-row controls for failing/long prefixes, Unicode, empty captures,
  repeated alternatives, NULL and skew before defaulting any new path.
- [Hyperscan's paper](https://www.usenix.org/conference/nsdi19/presentation/wang-xiang)
  motivates decomposing literals and automata and using SIMD, but its matching
  interface does not supply the capture semantics required here. It is not a
  drop-in replacement. [Cox's VM treatment](https://swtch.com/~rsc/regexp/regexp2.html)
  explains ordered submatch tracking. [PCRE2 JIT](https://pcre.org/current/doc/html/pcre2jit.html)
  supplies a native compiled control, including ARM64 and x86; Java compatibility
  and per-worker scratch ownership remain obligations, not assumed equivalence.
