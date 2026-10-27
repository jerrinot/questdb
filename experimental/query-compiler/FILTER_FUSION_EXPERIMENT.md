# Filter and aggregation fusion experiment

Exploratory, explicitly selected by the harness; ordinary compiler policy stays
unchanged. The experiment uses resolved types/functions, never query names or
fixture constants for eligibility. Existing map algorithms, allocation, bounded
state, sharding and merge remain the controls.

Hypothesis: keeping a predicate mask inside the generated aggregation kernel can
remove the frame-sized selection-vector boundary while retaining vector predicates
and staged map prefetches. First isolate that boundary; do not claim that this
also eliminates every repeated predicate/aggregate column load.

Candidate: evaluate a mask for each input batch, stage only admitted keys and map
candidates, then consume them in source order. Use the existing mask lowering,
null semantics and tail guards. Resume at the first unprocessed physical input row
on growth/copy/flush exits. Publish the accepted-row count at resumable exits, not
through a new store in every row update. Unsupported predicates and frames decline
before aggregate mutation. No per-row Java calls or new production setting.

Control: current LLVM selection kernel followed by current LLVM map/update kernel.
Also retain ordinary Java execution on the same checkout as a correctness/control
reference. It includes shared-core prototype changes and is not a clean stock build.

Napkin model: materializing and reading N accepted 64-bit row IDs adds 16N logical
bytes. At 500M input rows and 50% acceptance that is 4 GB. Much may stay in cache;
this is not a prediction of 4 GB less DRAM traffic. A sparse input batch admits
fewer independent map probes, so fusion can lose memory-level parallelism. Predicate
vector width, branches, register pressure and code size can also offset the saving.

Run one grouped COUNT/SUM query over 500M rows first, on eight physical cores of
one Ryzen 9950X CCD. Warm data, identical worker/configuration/input/state paths,
compilation outside timed execution, full SQL result validation, and exact native
input/accepted/update coverage. Repeat in balanced process order. Then vary only
selectivity (1%, 50%, 99%) and state cardinality (1024 vs 1048576 keys).

Inspect cycles/instructions and dispatched loads/stores per input row, branch
misses, incomplete-load stalls, UMC traffic, elapsed distributions, query faults,
I/O and GC. Record counter availability/multiplexing and host interference. Reduction
in logical traffic alone is insufficient: elapsed and relevant resource metrics
must agree. The hypothesis is falsified by unchanged/worse execution or by losses
from sparse-batch prefetching/vectorization. Results do not establish a new default
or workload-wide gain without broader evidence.

## Result, 2026-10-27

Fusion helps this workload, but this is not a general breakthrough or a new default.
The compiler experiment is selected by `ResolvedQueryCompiler.filterFusionExperiment`;
there is no new system property or server setting. It accepts existing resolved
numeric predicates and mutable map implementations. Residual VARCHAR NULL checks,
column tops/casts, late-materialized frames and shared VARCHAR maps retain the
existing path. Keys and aggregates use their existing lowering and map algorithms.
The executor receives the accepted count so Parquet selectivity statistics remain
correct when fully decoded frames use fusion.

Retained implementation, 500M input rows per execution, eight physical cores
(CPUs 0-7, one 9950X CCD), seven query workers plus the caller:

| Accepted | Groups | Selection vector, ms | Fused, ms | Time reduction |
| --- | ---: | ---: | ---: | ---: |
| 1% | 1,024 | 52.24 | 51.75 | No clear difference |
| 50% | 1,024 | 175.95 | 148.20 | 15.8% |
| 99% | 1,024 | 287.51 | 178.14 | 38.0% |
| 50% | 1,048,576 | 923.68 | 868.84 | 5.9% |

The 50% case has three fresh processes per arm, five measured executions after
three warmups per process, in A/B/B/A/B/A order. Other cases have two processes per
arm in A/B/B/A order. Entries are pooled sample medians. Full sample ranges for
selection vector / fusion respectively: 1% **50.27-55.05 / 48.25-55.85 ms**;
50% **174.30-181.89 / 145.92-150.46 ms**; 99% **284.29-292.35 / 173.77-183.39 ms**;
large state **914.34-940.21 / 857.01-903.42 ms**. Sparse-process medians vary, so do
not turn the small pooled difference into a speedup claim.

Timing includes cursor allocation, execution, merge, result copying and cursor
close. Compilation, oracle execution and canonical comparison sorting are outside.
Every output is compared in full with ordinary SQL, and every native execution
accounts for exactly 500M inputs and the expected accepted/updated rows. All
measured samples above have zero major faults and zero storage reads. Small-state
samples have zero measured GC time. Large-state samples do include GC/output
allocation; build-phase medians alone are **789.13 -> 741.92 ms**.

Ordinary Java on this same dirty checkout measures **177.85 ms** at 50% acceptance
(one fresh process, five samples). This is not a pristine stock comparison. SQL
preparation medians in the main A/B are approximately **134 -> 152 ms**; fusion
compiles one additional kernel while retaining the existing fallback kernels.
The execution saving is therefore larger for a reused plan than a one-shot query.

## Hardware evidence and interpretation

Separate execution-only PMU passes count five complete queries each (2.5B input
rows). Two hardware events per pass run at 100%, with no multiplex scaling. The
initial four-event pass ran at 66% and is excluded from this interpretation.

| Per input row, 50% acceptance | Selection vector | Fused |
| --- | ---: | ---: |
| Cycles | 14.92 | 12.27 |
| Retired instructions | 35.08 | 33.47 |
| Dispatched loads | 11.93 | 12.26 |
| Dispatched stores | 2.69 | 3.61 |
| Retired branches | 6.41 | 5.55 |
| Branch misses | 0.0833 | 0.1019 |
| No-retirement cycles waiting for oldest load | 7.35 | 5.65 |
| Memory-controller read CAS commands | 0.3010 | 0.2625 |
| Memory-controller write CAS commands | 0.02349 | 0.00505 |

Cycles fall 17.7%, instructions 4.6%, load-wait cycles 23.1%, and combined
memory-controller CAS commands 17.5%. UMC measurements are system-wide and counts
are reported as commands. The local perf metric definitions subsequently confirmed
64 bytes per CAS command (`umc-metrics.json`); this converts the totals to
**10.38 -> 8.56 GB/query**, including system-wide background traffic. This supports
reduced off-chip traffic and waiting; it does not establish DRAM saturation.
More dispatched memory operations do not imply more DRAM bytes: these counts
include repeated scratch/stack accesses and vector stores.

Actual JIT objects show the old eight-lane predicate plus `vpcompressq` row-ID
stores replaced by two 16-lane predicate comparisons per 32-row batch, mask
iteration and the existing map-prefetch/update stages. The aggregate kernel grows
from 1,253 to 1,986 bytes, replacing a separate 377-byte filter during execution.
The fused accepted-row counter spills in the consume loop. Thus this is not an
isolated test of selection-vector bytes: predicate width, loop shape, cancellation
frequency and register allocation also change. Duplicate input loads and the small
map-prefetch scratch are still present. Generality is tested through selectivity,
state cardinality and correctness cases, not inferred from one benchmark query.

An attempted mask-popcount progress counter reduced dispatched stores by 21%
relative to the initial fused code, without a reliable 50%-case latency gain. It
made sparse execution worse: fresh A/B/B/A process medians were **56.02, 50.82,
48.90, 56.46 ms** (popcount, increment, increment, popcount). That rewrite was
reverted. Lowering a counter is not sufficient evidence of an improvement.

The host has ample available memory, no CPU quota/throttling or cgroup memory
limit, and no measured query storage I/O. High background I/O PSI remains
unexplained despite an idle sampled NVMe device; these are warm-data results only.
The governor/boost policy was unchanged. They do not establish cold-query behavior,
whole-suite performance or performance on the former EC2 host.

## First-principles model and attempts to falsify it

At 50% acceptance, 500M input rows produce 250M 64-bit row IDs: **2 GB written and
2 GB read logically**. With default 1M-row frames, each active selection vector
holds about **4 MB**; eight acquired slots therefore hold about **32 MB**, almost
all of the CCD's **32 MiB L3**, before input streams, maps and other state compete
for it. Reusing the vectors only avoids DRAM traffic if their dirty lines remain
in cache. Removing the frame vectors saves 1.82 GB of measured controller traffic;
the 4 GB logical estimate was never a prediction of equal DRAM-byte savings.

Fresh streaming calibration uses 536,870,912 doubles, eight threads pinned to
CPUs 0-7, two warmups and ten measured passes. Read-only arrays occupy 4 GiB;
ordinary-store triad arrays total 12 GiB. Read controller throughput is
**60.48-60.98 GB/s** (useful reads 57.17-58.59); triad controller throughput is
**64.11-64.15 GB/s** (useful traffic 46.27-46.30). Initialization and warmup are
outside the counters. An idle measurement observes **1.96 GB/s** background traffic;
UMC results include that traffic. The clean read pass's 1.89 GB/s difference
between controller and useful rates is consistent with that background and the
64-byte conversion. These are same-affinity reference rates, not universal roofs.

The initial traffic model is plausible: at unchanged throughput,
`175.95 ms * 8.56 / 10.38 = 145.1 ms`, close to 148.2 ms measured. At the measured
read ceiling, 8.56 GB takes approximately 141 ms. **That agreement alone does not
prove saturation.** Dedicated data-bus counters report only **56.3% / 54.3%** busy
for materialized/fused execution. Eight cores split across both CCDs achieve
**70.60-70.69 GB/s** streaming reads, yet fusion improves only slightly. The
one-CCD streaming reference is not a hard DRAM limit.

Two matched counterfactuals isolate cache pressure. Neither changes repository or
server defaults. Both arms use the same worker count and frame settings within
each comparison; the smaller-frame control is a temporary harness under `/tmp`.

| Placement / maximum frame rows | Selection vector, ms | Fused, ms | Controller GB/query, selection / fused |
| --- | ---: | ---: | ---: |
| Eight cores, one CCD / 1,000,000 | 175.95 | 148.20 | 10.38 / 8.56 |
| Eight cores, two CCDs / 1,000,000 | 147.03 | 142.87 | 8.77 / 8.69 |
| Eight cores, one CCD / 125,000 | 154.75 | 150.18 | 8.44 / 8.39 |

Counterfactual timings use two fresh processes per arm, five measured executions
per process, in A/B/B/A order; traffic comes from separate gated five-query runs.
Spread-core samples are more variable (materialized 140.67-167.95 ms, fused
133.57-147.13). Small-frame samples range 152.79-167.08 / 147.13-151.14 ms. Do not
claim their approximately 3% residual differences as robust new wins. Halving the
frame size to 500K rows gave an intermediate 163.38 / 150.82 ms and 9.09 / 8.51 GB
in one process per arm, consistent with the footprint explanation.

The 125K-row control reduces live row IDs to about **4 MB** across all slots. It
removes approximately **84% of the timing gap** and **98% of the traffic gap**.
Spreading the same eight cores also gives the vectors more aggregate LLC capacity
and removes most of their additional traffic. Taken together, these support **LLC
capacity pressure and resulting traffic as the principal source of the default
configuration's fusion advantage**. They falsify a blanket explanation based on
DRAM saturation or fusion being intrinsically much faster with cache-resident
materialization. Some instruction/loop/dispatch differences remain inseparable.

Fresh cycle sampling puts materialized execution at **14.66% filter / 80.83% map
kernel** and fused execution at **95.45% fused kernel**; merge samples are negligible.
IBS captures 89,983 / 71,646 total samples over 15 fixed-work executions per arm.
After excluding exact software-prefetch instruction PCs, the dominant sampled
native demand-load miss weights are:

| Load | Materialized share | Fused share |
| --- | ---: | ---: |
| SUM input `v` | 56.0% | 74.9% |
| Predicate input `p` | 30.5% | 0.4% |
| Key input `k` | 9.6% | 23.7% |

The remaining fused map-key/value accesses each contribute about 0.25%; its spilled
counter load contributes about 0.05%. The SUM input's sampled mean miss cost is
47.3 -> 49.7 cycles, so fusion has not made that load latency cheaper. These are
latency-weighted sample shares, **not wall-time shares**, and samples/weights must
not be summed as non-overlapping stall time. Nevertheless, they locate the remaining
long-latency demand accesses in the input columns, not in this small hash map.
Source attribution uses actual JIT object PCs, not Java method names. No profiler
lost-sample warning was reported.

## Checks and next experiment

Three focused tests cover mask tails, empty/sparse/dense predicates, nulls,
COUNT/SUM/SHORT AVG/DISTINCT, computed/composite/VARCHAR keys, growth and bounded-state
flushes, repeated cursors, unsupported predicate/column-top fallback, and recovery
after an injected failure following native progress. Native scratch ownership is
checked. All pass. The 58 previously captured default LLVM kernels remain
byte-identical, and the retained fused object's text is checked against the
measured version.

The design implication is to keep intermediates within a deliberate cache budget,
not to fuse everything. Before changing default admission, test real mixed
predicates and wider aggregate state. The next performance experiment should test
prefetching aggregate input columns during the existing map-preparation stage:
measure input-load miss cost, incomplete-load cycles and whole-query latency, with
sparse/dense controls. The profile provides no reason to replace the small map's
algorithm. If admitted-row batching is revisited, use the existing bounded scratch
rather than a new frame-sized vector or query-specific recognition.

Harness: `org.questdb.experimental.codegen.FilterFusionExperiment` with arguments
`SCRATCH_ROOT create|java|materialized|fused THRESHOLD SLOTS SAMPLES WARMUPS k|h`.
`create` builds the isolated 500M-row fixture; thresholds 10/500/990 select the
three acceptance rates. Use the ordinary prototype JVM flags and compiled classes,
with external CPU affinity. The canonical database is untouched. Run and profiling
commands, raw samples, PMU counters, preflight, native objects and test output are
under `/tmp/query-filter-fusion`; only this concise experiment record and the
progress entry are kept in the repository. Main retained-result inputs are
`balanced`, `controls`, `pmu-*`, `actual-*` and `counter-control`; `final-balanced`
contains the rejected popcount variant and must not be quoted as the retained code.
`ibs-*` and `cpu-*` contain sampling and exact object attribution; `ceiling-*`,
`spread-ceiling-*`, `spread-balanced`, `frame-*` and `small-frame-*` contain the
hardware/capacity counterfactuals. `ceiling-0-read` is an unsuccessful control-pipe
attempt, contains no complete measurement and is excluded.
