# Deferred aggregation performance work

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
- Adaptive generated inner-set union now closes the indivisible DISTINCT merge tail,
  using existing workers and parallel initialization. Remaining risk: presizing from
  summed cardinalities wastes memory for overlapping sets. Preserve the serial
  fallback and256MiB per-target bound when studying a better capacity policy.
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
