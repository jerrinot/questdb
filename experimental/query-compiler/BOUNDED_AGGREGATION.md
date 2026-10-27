# Bounded local aggregation experiment

The measured large VARCHAR grouping case moves about 49 GB/query and reaches the
same ~63 GB/s controller ceiling as STREAM. Generated code removes instructions,
but unbounded worker shard maps still create large random working sets. The experiment changes that data flow. The default admits nonallocating fixed-width
unordered maps and fixed composite OrderedMap keys. VARCHAR remains
opt-in (`query.codegen.bounded=true`); `false` disables the experiment for A/B.

Each existing reducer slot updates a bounded ordinary QuestDB map. At a generated
batch boundary it publishes the map, emits dense partial states into 256 append-only
partitions, clears the map and resumes before the next unconsumed row. Partition
selection uses the existing full hash and shard bits. No SQL text or observed
ClickBench constant participates. The initial bound budgets roughly 256 KiB of
hash slots; `query.codegen.localCapacity` permits controlled sensitivity tests.

After the existing frame sequence drains, small maps with no flushed state use
the ordinary merge. Otherwise remaining states are flushed and the existing
GroupByMergeShardTask scheduler merges each partition into an ordinary destination
map. Hashing, partition append, state merge and Top-K are generated from shared IR.
The final map/cursor and its presizing statistics retain QuestDB ownership.

COUNT/SUM/AVG with fixed-width unordered or fixed composite OrderedMap keys
qualify by default. Single VARCHAR remains opt-in. DISTINCT and variable composite
state retain their existing path. `query.codegen.boundedOrdered=false` disables
the fixed composite case for counterfactual measurements. Pages
use the existing query-tracked GroupByAllocator. Generated allocation requires an
explicit worker-allocator capability: Java COUNT/SUM previously shared an allocator
because their scan did not allocate. Stable table key pointers remain borrowed;
map-owned VARCHAR keys are copied into query-owned pages before map clear. All
buffers outlive merging and result access, and are forgotten before allocator clear.

Acceptance requires exact ordinary-SQL comparison, reuse/Parquet/null/fallback and
failure checks, then equal-work controller traffic and end-to-end A/B measurements.
A local cache improvement can still lose through extra hashing, weak reduction,
partition-buffer traffic or merge overhead. Do not enable by default without that
evidence. Hardware gate: `/tmp/query-clickbench-next/bandwidth-study.json`.

Related primary work: DuckDB's bounded preaggregation and partition processing
(https://duckdb.org/2024/03/29/external-aggregation), and the counterpoint that shared
specialized tables can compete with partitioning (Xue and Marcus, *Global Hash
Tables Strike Back!*, https://www.vldb.org/pvldb/vol19/p523-marcus.pdf).
