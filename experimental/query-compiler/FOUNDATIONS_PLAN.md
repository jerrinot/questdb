# Query compiler foundations and complete LLVM coverage

Updated 2026-10-27. The target is at least 22 of the 43 local QuestDB ClickBench
queries, numbered Q0–Q42, with **every data-processing operator generated**, including
merge, sorting, projection and LIMIT. Java continues to own storage, allocation,
scheduling, cancellation and cursor lifetimes. A compiled aggregate update under a
Java map/filter/sort does not count as a compiled query.

ClickBench is the acceptance workload, not the compiler's programming model.
Binding uses resolved functions, physical layouts and operator contracts; no query
names, SQL text recognition, fixed data ranges or assumed cardinalities. Maintain
a small breakthrough log in [PROGRESS.md](PROGRESS.md), with temporary raw evidence
outside the repository. Keep LLVM and AsmJit and the C reference; no JVM backend.

## Current checkpoint

Generated-operator coverage now reaches 28/43 shapes: Q1,Q4,Q5,Q7-Q19,
Q30-Q41. The latest slice adds searched-CASE VARCHAR keys and explicitly generated
LIMIT/OFFSET over the sorted Top-K buffer. The stricter audit found that earlier
coverage counts had included native sorting while overlooking Java OFFSET
iteration; the new sorted-window bridge closes that boundary for Q38-Q41.
Actual-data group equality and generated row counters validate the new paths.

Coverage remains separate from performance. A fresh local eight-core comparison
with three complete repository-protocol passes per mode gives21 generated queries
at least1.5x faster by process-median warm results;20 clear that threshold across
all measured process ranges. The43-query warm ClickBench score is1.455x; the first
execution score is1.040x. The28-shape coverage target is met, but the22-query major
performance-gain target is not. These are local results, not a refresh of the
deleted EC2 instance. The retained radix helper reduces compilation. The finalized
reader scheduling gap is now closed through the aggregate's read-safety contract,
including ordinary getter fallback. Three refreshed LLVM suites retain21 major
wins against the earlier full-suite stock baseline; fresh matched-prefix Q13
medians are546ms clean stock,505ms current Java and366ms LLVM. The shared scheduling
improvement is distinct from generated-code gains.
See PROGRESS.md and TODO.md for measured scope and limits.

## Initial coverage target and order

Source: `/home/jara/devel/oss/ClickBench/questdb/{create,queries}.sql`. The refreshed
ordinary-SQL audit compiled/drained all 43 queries against the actual schema with
no errors; 21 reached generated aggregate updates. After the variable OrderedMap
slice, **nine complete shapes qualify: Q8, Q12, Q13, Q14, Q15, Q16, Q33, Q36 and Q37**. Every
selected input row reaches generated map/update code, supported predicates are
generated, and native bounded ordering visits every result group. The prior five
and Q14 match the ordinary engine's result hashes on the fixture.
Q16's tied cutoff can return different valid keys; exact full-group comparison and
validation of the ordered Top-K count multiset establish its correctness separately.
Q17 still has an ordinary unordered LIMIT, and Q34 has a Java constant VirtualRecord
projection, so both are excluded. The target remains 22/43.

The original seven also pass exact ordered-result comparison against clean stock on the
actual local ClickBench database (99,997,497 rows, 22 partitions), using three HTTP
executions per query under the local runner's cache protocol. Native counters
confirm all admitted map rows and result groups. Their hot geometric-mean speedup
is 1.13×, with Q13 measuring 2.5% slower; see the actual-data milestone in PROGRESS.md.

That small fixture establishes binding/plans, not performance; the shared phase
paths are exercised separately at 500M rows. Q41's two-SHORT-key aggregation is
native, but its OFFSET/encoded sort remains ordinary. Remaining queries must not
be counted from an update marker. Order-sensitive fixture hashes for other
queries are not a full correctness oracle. Exact byte/result comparisons and
ownership/merge cases are covered separately by the focused tests.

A first target of 23 queries leaves one query of margin above 50%:

| Shared capability family | Zero-based queries |
| --- | --- |
| Filtered scalar COUNT, key-only DISTINCT, filtered projection | Q1, Q4, Q5, Q19, Q24 |
| Fixed-width/SYMBOL grouping and numeric ordering | Q7, Q8, Q10, Q11, Q15, Q35, Q40, Q41 |
| VARCHAR/composite grouping with numeric ordering/LIMIT | Q12, Q13, Q14, Q16, Q17, Q33, Q34, Q36, Q37, Q38 |

Execute one slice at a time:

1. **Generated fixed-key map mutation, redistribution and merge** over existing
   Unordered4Map/Unordered8Map. Implemented and tested; the experiment shows modest, workload-dependent changes,
   not a broad performance breakthrough. Details below and in PROGRESS.md.
2. **First complete grouping → numeric Top-K → output composition.** Bind ordinary
   Long Top K's sorted-buffer algorithm, including local collection and final merge,
   to actual aggregate values and stable map row IDs. The bounded numeric Top-K
   collection and merge are now generated for the existing unordered INT/LONG maps;
   see the second slice below. Reuse current worker dispatch and cursor ownership.
   Q15 is a useful vertical acceptance shape, but vary keys,
   aggregates, K, ordering direction and cardinality independently.
3. **General fixed-width keys and expressions.** Add SHORT/BYTE/TIMESTAMP/IPv4 and
   SYMBOL representations with their actual null/translation semantics; support
   composite key encoding and OrderedMap mutation/growth/merge. Bind resolved
   filter/projection operations to the shared IR; reuse the existing filter compiler's
   semantics rather than building another SQL front end.
4. **VARCHAR values and keys.** Borrow QuestDB's UTF-8 representations with explicit
   lifetime; reuse native map hashing, equality and ownership rules. Support both
   UnorderedVarcharMap and composite OrderedMap encoding. Symbol IDs alone are not
   a general VARCHAR representation.
5. **Key-only grouping, scalar consumers and bounded output.** Accept resolved
   GROUP BY with no aggregate functions; support COUNT over that phase, filtered
   projection and LIMIT/OFFSET. Preserve storage order where it already satisfies
   ORDER BY. Borrow or reserve bounded output storage through existing owners;
   don't return an unbounded boxed result list.
6. **Finish the ordering family and verify the coverage count.** Q7 needs complete
   numeric sorting without LIMIT, and OFFSET cases need correct retained ranks.
   Existing encoded sorting/Top-K provide algorithms and storage; calling their
   current C++ implementation is not generated LLVM sorting. Count a query only
   after every actual data-processing phase passes the generated-path check.

Floating AVG, LIKE/regex, string ordering, CASE and timestamp bucketing can follow
this first coverage target. Existing joined-value work remains usable but joins
are absent from this workload; do not expand joins at the expense of these gaps.

## First slice: actual unordered-map mutation

### Verified contracts

- `MapFactory.createUnorderedMap` chooses the implementation from key and value
  widths. The server's maximum unordered entry size defaults to 32 bytes; the
  test configuration defaults to 16. Wider states use OrderedMap. Do not alter
  these defaults to claim coverage or speed.
- Unordered4Map aligns entries to **four** bytes; Unordered8Map to **eight**.
  Both keep zero keys in an extra end-of-buffer entry. `size` counts nonzero keys,
  `hasZero` is separate, and `free` controls growth.
- Their hash functions, linear probing, load factor, capacity limits and memory
  tracking already define the algorithm. Reuse them. No new map or executor.
- `GroupByShardingContext` already acquires slots, dispatches/drains parallel merge
  work and retains source allocators until destination state is finished. This is
  required when DISTINCT merges adopt or mutate source set pointers.

### Design and implementation

`Map.writeNativeMutationView` borrows an exclusive 64-byte snapshot: the existing
48-byte probe view followed by free slots and nonzero-key count. Generated code
inserts and updates values in the map's existing allocation. The slot publishes
counters in `finally` before returning control to Java. A borrow expires on return
or growth; no other map operation may overlap it.

`QuestMapProbe` shares hashing/probing with the previous read-only path and emits
insertion for mutable unordered views. New values initialize from the same resolved
COUNT/SUM/DISTINCT descriptions used for updates. No callback on a new group.
Before an insertion would consume the final permitted free slot, save the current
position/shard and return without committing that row. The owner allocates growth,
invokes the generated rehash, publishes new backing, and resumes the same row once.
Prepared candidate pointers are rebuilt after every growth return.

The unsharded loop also preserves Java's `reserveCapacity(batchRows)` policy at
**every configured batch boundary**. It checks free slots in generated code and
returns `NEED_CAPACITY` only when reservation needs growth; the existing owner
calculates capacity and invokes generated relocation through `reserveNative`.
A prefetch window ends at a reservation boundary, including non-power-of-two batch
sizes. Omitting this policy produced denser maps and a measurable DISTINCT
regression in the first experiment; retaining the nominal load factor alone did
not preserve the existing execution algorithm.

`Map.growNative` reuses capacity checks, query accounting, allocation, zeroing and
publication. The rehash callback receives the **actual entry stride from the map**.
Failure frees the new allocation and leaves the old backing valid. Primitive
allocation/zeroing and zero-entry copying remain owner operations.

`QuestMapMutation` emits relocation and physical-entry iteration for merge/sharding.
New groups copy state; existing groups invoke the shared aggregate merge definitions.
Redistribution uses the same high hash bits and preserves the current 256-shard
policy. The normal SQL factory attempts a generated whole-frame update before
Java's packed-batch path. The existing fragments and sharding context invoke the
same slot updater for redistribution and merge; queues, worker budget, presizing
and planner decisions are unchanged.

Unsupported layouts retain their existing path before mutation. Native errors
propagate; they never request replay or Java fallback after partial mutation.
The read-only probe ABI remains unchanged for joined aggregation and the handwritten
reference. Joined group insertion is not migrated by this slice.

### Checks and experiment

Focused checks cover both key widths, NULL/negative/zero keys, collisions and growth
inside prepared batches, filtered tails, COUNT/SUM/DISTINCT, source-pointer lifetimes,
cancellation, failed rehash allocation cleanup, Java readers, and reopen. Test-only
layout settings exercise wider states on unordered maps; performance runs preserve
server defaults and their real OrderedMap fallbacks.

Measure at least 500M rows per workload, eight total execution slots, five warmups,
three measured executions, exact result comparisons and full result consumption.
Keep clean stock, disabled candidate and generated paths separate. Freeze the prior
prototype for before/after comparison. Vary aggregate width, group cardinality and
skew independently; report compilation separately and preserve within-process
ranges. Native map row/call counts prove the executed path. PMU collection excludes
setup and warmup using the existing harness handshake.

This is an exploratory capability experiment. Removing crossings predicts fewer
calls and instructions; it does not prove a CPU bottleneck or a performance gain.
No breakthrough claim without elapsed-time improvement, correct results and
counterfactual evidence. The earlier tight 3% disabled-path neutrality target was
unresolved across JVMs and must not be silently treated as passed.

## Second slice: generated bounded numeric Top-K

The ordinary planner still selects `LongTopKRecordCursorFactory`. Its planning hook
asks the aggregate factory to prepare the optional kernel **before dispatch**. Queries
without this operator do not compile it. Binding admits only the exact aggregate
implementations already resolved by the update compiler and their assigned value
indices/offsets; their `getLong` methods expose the stored result directly. No SQL
spelling, fixed K, domain assumption or query identifier participates.

`QuestLongTopK` emits the actual `DirectLongLongAscList`/`DescList` algorithm: full-buffer
cutoff, binary search down to the existing 65-entry threshold, linear search and tie
handling, backward overlapping pair copy, insertion and bounded size. Direction and
source kind dispatch before their scan loops. It reads the existing map probe view,
visits the separate zero entry first, then physical occupied slots, and stores the
same raw map row IDs as the ordinary map Top-K cursor. LONG_NULL is ordered by the
same signed comparison; SUM needs no separate finalization. The same insertion
emitter merges worker lists. Both selection and movement compile through the shared
IR to LLVM/AsmJit/C; there is no call to Java `add` or an external sorting algorithm.

The existing lists expose an exclusive borrow of their pair allocation, plus a size
publication operation. Existing slot scratch holds the map view and list descriptor;
no new result allocation, map, queue or executor is introduced. Completed insertions
publish size into scratch, and `finally` publishes it to the owning list on success
or cancellation. The existing callback checks cancellation every 1024 visited slots
or source pairs. Failure propagates without fallback/replay.

`AsyncGroupByRecordCursor` retains the configured choice between sequential and
parallel collection. `GroupByLongTopKJob` acquires the same worker slots and writes
the same worker buffers; the owner merges them only after the existing drain.
The ordinary output cursor reads the resulting bounded row-ID list and exposes
stored map fields. It owns rewind, random access, close and reopen. Unsupported
layouts/readers retain ordinary collection before mutation.

This closes **bounded, single-column LONG aggregate ordering** on Unordered4Map and
Unordered8Map. It does not claim general generated projection, filters, key ordering,
OrderedMap/string/composite keys, multi-column ordering, unlimited sorting or OFFSET.
The strict complete-LLVM ClickBench coverage count remains unaudited after this slice;
do not increment the previous zero merely because the sort is now generated.

Focused checks compare values **and exact row IDs/ties** with the engine lists for
both key widths, directions, K=1/10/65/66/128/5000, NULL keys/results and extremes.
They cover merge, cancellation, reuse and Java cursor consumption. Ordinary SQL checks
exercise both dispatcher modes, sharded/unsharded and parallel/sequential collection,
ordered output, rewind, reopen and OrderedMap fallback. Performance uses unchanged
server defaults, 500M input rows and independently varied cardinality/K/direction.
`TopKPhaseExperiment` can additionally keep the fully built maps alive and time only
collection/merge, explicitly excluding aggregation and cleanup; report those phase
times separately from complete queries. Raw evidence stays in `/tmp/query-topk`.

## Third slice: resolved integer filters through shared IR

`AsyncGroupByAtom` now offers its resolved filter to the same optional compiler
before creating worker slots. `ResolvedInputs` binds exact integer comparison,
NULL-test and Boolean implementations to the existing `Pipeline` predicates.
It admits BYTE/SHORT/INT/LONG columns, widening reads, integer constants,
equality/inequality, ordered comparisons and AND/OR/NOT. It reads the actual
resolved constant from `EqIntStrCFunctionFactory.Func`; NULL equality may resolve
through that factory or `EqSymLongFunctionFactory`, despite their names.
There is no SQL-text matching. Runtime binds, arithmetic and other unsupported
functions retain the ordinary filter; mutable functions are never frozen.

`ScanLowering.selection` generates the existing ordered row-ID selection contract
through the shared vector-mask IR, with bounded eight-row reads and a scalar tail.
The `SelectRows` IR operation compacts row IDs in ascending lane order and returns
the count. It is a memory-write effect even when its result is unused. LLVM lowers
it to masked vector compress-stores and population counts, following the same
selection-vector compaction principle as QuestDB's existing AVX2 filter. C and
AsmJit use the existing portable lowering pass for a scalar implementation. This
avoids enumerating every selected bit in the LLVM path without a dense-data guard
or query-specific strategy.
Each acquired slot borrows the engine's `DirectLongList`, reserves its capacity
once per frame, and publishes its count only after successful native execution.
The slot owns only reusable input-address scratch. Cancellation is polled before
execution and at bounded intervals; failure propagates without replay.

This preserves the engine's filter-then-aggregate phases, full configured map
batches, Parquet selectivity tracking and late materialization. It is two generated
kernels per supported frame, not single-loop filter/aggregate fusion. Column tops
and lazy type casts retain ordinary filtering before any native mutation. The
existing aggregate compiler and fallback rules still govern the second phase.
No scheduler, map, selection allocation or SQL resolver is duplicated.

Planning exposes `compiledAggregateFilter`; diagnostics report input and selected
rows after workers drain. `query.codegen.filter=false` isolates the old filtering
path while keeping generated aggregation and Top-K identical. This is an optional
prototype switch, not a stock QuestDB configuration change.

Focused checks compare native row IDs with the resolved Java functions, including
NULLs, widening, extremes, Boolean combinations, vector tails, cancellation and
slot reuse. Ordinary SQL checks run all three backends and both dispatcher modes,
including unsupported arithmetic/bind fallback and existing Parquet/cast cases.
Performance varies selectivity independently at 500M rows under unchanged server
defaults; temporary evidence is in `/tmp/query-llvm-filter`.

This closes the integer-filter binding gap for eligible keyed aggregations.
Scalar aggregation, broader expressions/types, general projection and ordering
still need work. Complete ClickBench coverage needs a fresh operator-by-operator
audit; this slice alone does not establish the 50% target. Deferred aggregation
algorithm experiments are tracked in [TODO.md](TODO.md).

## Fourth slice: fixed-width OrderedMap execution

The compiler now borrows the resolved direct key column list, in the order of the
existing `RecordSink`. `QuestFixedKey` admits BYTE/SHORT/INT/LONG columns only when
their types match input metadata and their total packed width matches the actual
OrderedMap key size. Computed keys, SYMBOL translation and variable-width keys
retain the ordinary path. The existing single INT/LONG unordered path is unchanged.

Keys are staged once per prepared row into slot-owned scratch, without field padding.
Hashing reproduces `Hash.hashMem64`: eight-byte words, a signed four-byte tail,
signed byte tails, the existing multiplier and avalanche. Equality compares exactly
those key bytes. Prepared batches reuse the existing prefetch schedule; they retain
both the staged key and candidate address. Every return for growth rebuilds the
remaining prepared rows, since heap relocation invalidates candidate pointers.

OrderedMap retains both allocations and its insertion-order heap. Its 64-byte
mutation view holds heap base, offset table, mask, aligned entry stride, append
pointer, heap limit, free slots and size. Generated probing reads the packed
unsigned biased offset plus cached low hash. Misses initialize/copy keys and values
in the existing heap and publish the packed slot and counters. Growth returns
before committing the pending row. The owner applies existing memory accounting,
heap limits and resize policy, then resumes that same row. Unsharded batch boundaries
reserve worst-case heap bytes exactly as the ordinary fixed-key batch probe does;
offset-table growth remains insertion-driven.

Offset-table rehash copies occupied packed slots with their cached hashes into the
owner's new allocation. Failure frees that allocation and retains the old table.
The owner may have successfully relocated the heap before rehash fails, so its view
is refreshed in `finally` before outer publication. No exception requests fallback
or replay after mutation.

Generated merge scans source hash slots in the ordinary merge order and reuses
cached low hashes. Redistribution scans the source heap in cursor insertion order
and computes the full hash for the existing shard selection. Aggregate initialization,
copy and merge reuse the existing resolved definitions, including DISTINCT allocator
ownership. Existing worker slots, sharding jobs, cancellation and drain remain owners.
Generated Top-K scans the heap in insertion order and stores the same raw row IDs
as the Java cursor, preserving tie behavior and the existing bounded-list algorithm.

Checks exercise odd packed key widths, negative/zero/NULL keys, growth, Java reads,
native merge and redistribution, exact Top-K row IDs/ties, cancellation/reuse,
high-bit compressed offsets and failed relocation ownership. Experiments retain
stock defaults and compare `query.codegen.ordered=false` against the new path on
500M rows, varying cardinality, skew and value width. Raw evidence stays in
`/tmp/query-ordered`; the experiment log records the outcome. This does not by itself
complete additional ClickBench queries: their remaining type, expression, VARCHAR
or ordering gaps still apply.

## Fifth slice: shared resolved integer expressions

`ResolvedInputs.scalar` now binds exact resolved integer addition/subtraction and
BYTE/SHORT/INT widening implementations, alongside columns and constants. Filters,
aggregate arguments and grouping keys use this same compiler-time binding. No
mutable Function survives compilation. Unsupported children decline the whole
binding before execution. Core changes expose the admitted implementation classes;
the ordinary functions' evaluation is unchanged.

Each arithmetic node retains its resolved INT or LONG result type. INT operations
wrap at 32 bits and sign-extend before a wider parent reads them; NULL propagation
includes a computed result landing on the type's sentinel. The existing I64 IR
can express this without new backend operations. BYTE/SHORT NULL getters return
zero; widening follows the resolved cast's actual source getter width. SUM, COUNT and LONG DISTINCT
consume scalar expressions. DISTINCT's prepared lookup hint reads only the source
columns needed by its expression. Ordinary query optimization still applies:
`SUM(v + 1)` can become SUM/COUNT plus a Java output expression, so generated
argument tests and measurements use column-plus-column expressions as well.

The optional compiler borrows key functions in RecordSink order: direct columns,
then computed keys. The same fixed-key descriptor lowers expressions into either
the existing INT/LONG scalar key or packed OrderedMap bytes, including each key's
storage NULL sentinel. It reads required frame columns only after validating all
pages; column tops/conversions retain ordinary access. Existing map selection,
growth, sharding and merge algorithms remain unchanged. Projection wrappers now
forward Top-K preparation through their existing column-index translations, just
as they already forward its capability check and execution.

Filtering still fills the existing selection vector. This slice does not fuse the
filter and aggregation loops or add general output-expression lowering. It also
does not add VARCHAR, SYMBOL, timestamp or IPv4 semantics. The diagnostic switch
`query.codegen.expressions=false` retains earlier arithmetic/cast admission and
computed-key fallback. The ordinary SQL benchmark can read SQL from
`query.codegen.sqlFile`; handwritten-reference modes reject that override so their
fixed physical plans cannot silently benchmark a different query.

## Sixth slice: single VARCHAR keys in the existing unordered map

The first string boundary is `Pipeline.Utf8Value`: address, byte length, NULL,
ASCII and stability. It is separate from integer `Value`; there is no boxed string
or implicit integer-to-string conversion in generated execution. `QuestVarchar`
lowers that view into the shared IR, so LLVM, AsmJit and the C reference use the
same storage and key semantics.

`PageFrameMemoryRecord.writeNativeVarcharView` borrows the existing pinned frame's
aux/data vectors, bounds, split/slice format and stability. Inline values (up to
nine bytes) live in aux; longer native values are contiguous in data, with a
redundant prefix in aux. Parquet slice entries supply a pointer and length. The
emitted decoder checks bounds, retains column-top NULLs and does not overread a
hash/equality tail. Lazy type conversions decline before any update. Filtered
Parquet records explicitly decline this descriptor: their filter and payload
columns use different row spaces, which this boundary cannot yet express.

The planner still selects the map. Only a single direct VARCHAR key in
`UnorderedVarcharMap` qualifies; mixed keys and wider states that select OrderedMap
retain the ordinary path. The generated implementation follows `Hash.hashMem64`
(signed 8/4/1-byte chunks and its avalanche), cached low-hash/size/flags comparison,
byte equality and linear probing. ASCII is ignored for equality, but occupancy
must be tested first because an empty string's comparable packed header is zero.
NULL and empty remain distinct. Prepared candidates keep their full hash for
shard selection; merge and rehash use cached low bits where the Java algorithm does.

Native table pointers retain the same borrowed lifetime as the ordinary map.
For a new unstable key, generated code yields **before insertion or aggregate
update**. The owner copies into that destination map's existing tracked arena,
then resumes the pending row with the copied pointer. Only new unstable groups
cross this slow path. Redistribution/merge copy owned keys into the destination
arena just as `UnorderedVarcharMap.merge` does. This avoids introducing another
allocator or transferring a pointer whose arena can close first. Growth uses the
existing native relocation callback; failure frees the replacement allocation and
keeps the old map valid. A VARCHAR-specific control extension leaves the existing
integer and handwritten kernel ABI unchanged.

Generated code covers insertion, updates, rehash, redistribution, merge and LONG
aggregate Top-K over physical VARCHAR map slots. Existing workers, allocation,
slot ownership, sharding decisions and output cursors remain in charge. Exact
resolved VARCHAR `IS NULL` / `IS NOT NULL` checks produce the existing selection
vector from aux headers; compound string predicates are still outside this slice.

The ordinary SQL fixture option `query.codegen.varcharKeys=inline|prefix|mixed`
varies byte length, shared prefixes and UTF-8 content independently of cardinality
and skew, includes NULL/empty keys, and uses server defaults. It is rejected by
fixed-shape handwritten/legacy benchmark paths. Result validation compares exact
UTF-8 bytes, not only a hash. Performance evidence and remaining boundaries are
recorded in the simple progress log.

## Seventh slice: VARCHAR-bearing OrderedMap keys

`QuestVariableKey` binds direct VARCHAR fields alongside supported integer fields
and resolved integer expressions in RecordSink order. It emits the actual
`OrderedMap.VarSizeKey` format: a four-byte total key length, then packed fields;
each VARCHAR contributes a four-byte length (-1 for NULL) and its UTF-8 bytes.
ASCII is deliberately erased, as `appendPlainValue(..., true)` does. Hashing
excludes the total-length header and uses the existing `Hash.hashMem64` algorithm.
Equality first checks total length, then compares the encoded bytes. This preserves
field boundaries, NULL versus empty, integer widths and arithmetic NULL sentinels.

Prepared keys occupy the existing `OrderedMap.ProbeView` staging buffer. An optional
map-owned view lends its tracked buffer for the generated window and closes with
the map, before the query tracker is released. Each window checks required bytes
before writing. Growth yields the first unconsumed row, enlarges the buffer through
its existing capacity mechanism, and rebuilds that window. No truncated key or
invalidated candidate survives growth; no committed aggregate update is replayed.

The generated OrderedMap path now obtains the value address from each entry's
length rather than a constant key width. Insertion copies the encoded key into
the owner's append heap and advances by align8(header + key + values). Growth
reserves the pending entry through the existing heap limit/resize path and reuses
the existing generated cached-hash offset-table relocation. Failure refreshes the
borrowed map view even when the heap moved before offset-table allocation failed.

Final merge preserves hash-slot traversal. Redistribution and LONG aggregate
Top-K preserve the variable-size cursor's insertion order and aligned dynamic
stride. Redistribution saves its source byte offset across a destination growth
return. Existing aggregate initialization/merge, including LONG DISTINCT, is reused.
Java still owns maps, query memory, workers, slot acquisition, cancellation and
result cursors; it does not serialize/probe keys or merge values on admitted paths.
Parquet keys are copied into the map's heap, matching ordinary OrderedMap ownership.

A diagnostic `query.codegen.variableKeys=false` retains the preceding compiler's
eligibility without altering stock configuration. The SQL fixture's
`query.codegen.varcharLayout=wide|composite|strings` varies state width and key
composition independently of its length distribution and logical cardinality.
These are fixture/compiler diagnostics, not SQL recognition or execution defaults.
Computed VARCHAR expressions, SYMBOL fields, string ordering, general projection
and Parquet late-materialized row spaces remain separate gaps. The existing safe
fallbacks continue to apply before mutation.

## Later foundation work remains

The existing joined-value compiler supports integer payloads, predicates and keyed
COUNT/SUM; join → DISTINCT still needs an allocator-owning aggregation boundary.
General phase composition should use existing `UnorderedPageFrameSequence`
non-frame dispatch and explicit borrowed batches, not another DAG scheduler.
`RecordArray` currently reserves one row at a time and clear releases its backing;
it needs a verified bulk reservation/reuse contract before serving as output storage.
Compilation admission, concurrent code leasing, broader numeric IR and sustained
shared-worker fairness remain separate gaps, not prerequisites for declaring the
current map experiment correct and narrowly useful.

## Next slice: resolved VARCHAR expressions in grouping keys

Code inspection identifies the boundary in `QuestVariableKey.bind`: direct VARCHAR
columns bind, but a VARCHAR field from `keyFunctions` immediately declines. The
existing variable-key path already owns staging, exact key encoding, map growth,
redistribution, merge and generated numeric ordering. Extend that binding rather
than adding a second map or execution path.

A searched CASE needs explicit resolved branches. `CaseFunctionFactory` currently
captures casted values in its picker, while `VarcharCaseFunction.args()` retains
original arguments for lifecycle management. `SwitchFunctionFactory` also returns
VarcharCaseFunction. Reading `args()` as a searched CASE would therefore confuse
resolved casts and switch semantics. Replace only the searched-CASE picker lambda
with a small inspectable picker; keep its first-true evaluation and lifecycle list.

Bind VARCHAR columns, immutable UTF-8 constants and nested searched CASE into a
shared expression lowering. Conditions reuse ResolvedInputs' resolved integer
predicates. Each result remains a borrowed address/length/null/ASCII/stability view.
Generate branch-local string decoding; only the selected branch produces a view.
Copy literal bytes into existing slot descriptors and selected values into the
existing tracked key staging buffer. No per-row JNI call, Java callback, allocation,
new scheduler or map. Preserve the distinction between NULL and empty strings.
Unsupported predicates, dynamic strings and simple CASE/SWITCH keep fallback.

Acceptance: ordinary SQL equality plus actual generated map/merge/sort counters;
multiple/nested branches, casts, Unicode, empty/NULL values, long literals, column
tops and Parquet; repeat cursor opens. Use all three backends for focused checks.
Then measure a general 500M-row fixture locally and verify actual-data Q39 coverage.
This is a capability experiment; the retired EC2 cannot support new EC2 performance
claims, and adding coverage alone does not establish the performance target.

The composition audit also found that LongTopKRecordCursor inherited Java OFFSET
iteration. Its factory now requests an explicit sorted-window capability, separate
from unordered map selection. The existing QuestRowLimit kernel consumes the
borrowed (value, row ID) buffer and copies only the requested window into LIMIT's
tracked output. It uses existing slot scratch, polling and lifecycle. Ordinary
execution and older compiler implementations do not advertise this capability.

## Next experiments: compilation cost, specialization and generated helpers

Design recorded 2026-10-27. Direction specialization (B) and small-capacity
specialization (the retained part of C, K<=64) are automatic compiler decisions;
their experiment toggles have been removed. Large-capacity specialization failed
the isolated execution regression gate and is not selected. Measurements are in
PROGRESS.md. D/E are complete: the always-inline helper reproduces B's compiled
shape and timing, while the outlined helper cuts warm native Top-K compilation
from157 to56ms and improves isolated collection/merge. Whole-query execution is
essentially unchanged. The typed helper is now automatic; there is no experiment
toggle. The design below records the controlled comparisons and acceptance gates.
Objective: substantially reduce preparation while preserving or improving warmed
execution. Run one comparison at a time; keep algorithms, memory layout, polling,
worker scheduling, O2 optimization and CPU target identical.

### Source contracts and available controls

- `LongTopKRecordCursorFactory` passes constant `lo` and `ascending` through the
  existing Selected/Virtual/Async factory and atom/compiler hooks. Column
  translations and generic callers are preserved. `lo` is the retained prefix
  length, including OFFSET, rather than the number of returned rows.
- Generic `QuestLongTopK.program` emits both directions and small/large algorithms,
  with the existing capacity threshold of 64. B removes the unused direction;
  retained C removes the large algorithm for small K. `QuestLargeTopK.emit` emits
  three source modes and a final radix sort per direction. `QuestRadixSort.emit`
  originally expanded each body directly into the caller: eight radix bodies in A,
  four in B, and none in the retained small-capacity C. The retained implementation
  emits calls to one direction-specific radix body instead.
- Source kind is genuinely dynamic: `AsyncGroupByRecordCursor` can collect from
  a map array, individual shards, and then merge worker pair lists. Preserve all
  required routes. Actual cardinality/sharding is not a planning constant.
- `Ir.Program` retains its single entry body and now supports typed helper calls.
  Helpers declare their inputs and return a failure status; LLVM emits one native
  body per reachable helper in the existing query module. C/AsmJit expand helpers
  before their existing lowering. There is no separate cache, runtime owner or
  public backend-selection policy for helpers.
- Reuse `TopKPhaseExperiment` for collection/merge on already-built maps and
  `ResolvedBatchMatrix` for full 500M-row SQL runs. Separate parallel map builds
  can change traversal even when aggregate values match. C's temporary phase
  harness switches compiled kernels only after workers drain, holding maps,
  destination addresses and scratch fixed, and validates exact value/row-ID
  sequences. Preserve this control for D/E. Existing focused Top-K tests cover
  ordering, ties, merge, cancellation and reopen; add only new boundary cases.

### Sequential comparisons

| Variant | Change from its control | Question and expected evidence |
| --- | --- | --- |
| A | Current inline, multi-mode O2 kernel | Reproduce compilation and execution baseline. |
| B vs A | Emit only the resolved sort direction; retain inline bodies and both capacity paths | Does eliminating unused structure reduce IR, native code, compilation instructions and time without an execution penalty? |
| C vs B | Select small/large algorithm using resolved capacity; retain runtime capacity arithmetic | Does removing the unreachable algorithm help independently of direction? Check 64/65 and generic preparation fallback. |
| D vs retained inline control | Express radix sorting as a same-module helper marked `alwaysinline` | Control for helper representation, parameterization and compiler pass ordering. Large K uses B because C was rejected there. Confirm calls disappear before interpreting E. |
| E vs D | Change only that helper to `noinline` | Measure the actual call-boundary tradeoff: lower duplication/compile work versus call overhead, spills and lost cross-boundary optimization. |

Compile D/E with the same helper body and signature, constant sort direction,
memory-effect information, calling convention, O2 passes and target. Use a direct
internal native call, explicit buffer/count arguments, and the existing polling
callback. Keep the helper in the query's existing ORC resource tracker; no separate
symbol lifetime, process cache, Java transition or extra allocation. Return and
propagate cancellation status before callers publish size or resume processing.
Only assert aliasing/alignment attributes proved by the existing scratch contract,
and apply them identically to both controls. `noinline` must not imply `optnone`:
the helper's own loops remain optimized. Inspect final IR/disassembly to establish
the experiment actually retained or removed calls. Attribute reference:
https://llvm.org/docs/LangRef.html#function-attributes

D need not compile to exactly the same instructions as its inline control: the preparation schedule
can change. If their execution differs materially, report that representation
effect separately and compare E with both D and that control. If E is promising, a later
variant can remove the inline attribute and measure LLVM's ordinary heuristic.
Do not change radix width, unrolling, buffer capacity, algorithm thresholds, or
cross-query caching during these comparisons. These are separate experiments.

### Three measurement layers

1. **Preparation only.** Reuse the temporary compile-cost harness and timing
   breakdown. Three fresh processes per variant; in each, record first compile
   separately, then five forced recompilations after initialization. Do not let
   the SQL factory cache bypass compilation. Close factories between samples.
   Record whole SQL preparation, Java lowering/emission, native parse/verify,
   optimization, machine-code materialization, compiler-thread CPU time/cycles/
   instructions, and pre/post-optimization IR and native code sizes. Kernel code
   sizes must include all helpers. Compare default uninstrumented elapsed time
   too; dump/profiling output belongs outside the timed run. Current Q39 native
   Top-K is about308ms; whole warm preparation about442ms. Treat these as reference
   figures to reproduce on the current host, not immutable thresholds.
2. **Execution isolation.** Reuse live, finalized engine maps and existing slots,
   then measure Top-K collection plus worker-list merge. Clear destination/scratch
   state consistently. Compare identical map traversal and candidate values. Also
   exercise the radix body at final-buffer sizes 0/1/16/64/512 and full-buffer sizes
   4096/16384 to expose call overhead and cache-footprint sensitivity. These small
   kernel diagnostics do not represent 500M-row query performance. Keep input
   preparation/reset outside kernel timings and report it separately; include all
   normal reset/allocation work in full queries. Count helper calls, pairs sorted,
   compactions, polls and scanned/merged groups in a separate diagnostic run so
   instrumentation does not alter the primary performance comparison.
3. **Complete SQL.** Run Q39 on the actual 99,997,497-row ClickBench dataset as the
   motivating case, plus general fixtures with at least500M source rows. Measure
   first-use latency, warm execution with cached factory, and prepare-plus-execute
   totals. Include ordinary current-core Java as a control; clean stock remains a
   separately labelled binary. Preserve default server execution settings. All
   variants must perform the same native operators and drain equivalent results.

### Small, staged workload selection

Start B/C on Q39 and one fixed-width SUM grouping with 500M source rows. Then screen
D/E on phase-isolated outputs before promoting the winner to complete-query runs.
Use three representative full-query shapes: fixed-width key/SUM with many distinct
scores; VARCHAR or composite-key/COUNT with many cutoff ties; selective grouping
with a small final buffer. Vary input order (random, already favorable, improving
scores) in the phase diagnostics, since threshold rejection changes compaction
frequency. These distributions are controlled fixture data, never planner rules.

Use a compact boundary sweep rather than a Cartesian product: K=1/64/65/1024/2048/
8192, both directions on the main case, and group counts below K, near4096 and
large enough for repeated compaction and the normal parallel Top-K route. Record
actual groups and pair counts; 500M scanned rows does not mean 500M sort entries.
Exercise single-map, map-array and worker-list merge paths explicitly. Repeat
finalist whole-query runs at one and eight physical cores, without SMT siblings.
Retain only cases needed to discriminate mechanisms or expose regressions.

For identical map/pair traversal compare exact `(value,row ID)` sequences, including
cutoff ties, signed extremes and LONG_NULL, against the current implementation and
engine list oracle. Across independent parallel SQL executions, compare complete
aggregate results and legal ordered results with tie-aware validation; raw row IDs
and unspecified tie order need not match. Focused cancellation/reuse checks must
cancel inside the helper and verify subsequent cleanup/reopen behavior. Use the
existing memory/lifecycle checks; no duplicate infrastructure.

### Hardware evidence and run discipline

The available host is a Ryzen9950X (16 cores/32 threads, two L3 domains). Record
actual topology/affinity, boost/frequency, thermal/throttling state, background
load, quotas, page faults/I/O, reclaim, GC and compiler-lock wait. Use the same
eight-physical-core JVM affinity for both variants and measure the native compiler
thread separately: pinning the whole JVM to one core caused compiler-thread
scheduling delays in B. Keep query worker counts and placement equal. Complete
the USE preflight and distinguish compiler resource pressure from query resource
pressure before attributing a mechanism.

For execution use at least three warmups and five balanced paired samples, with
A/B/B/A ordering across process blocks. Extend only noisy or close comparisons.
Report individual cases, medians and paired uncertainty, not just an aggregate.
Collect counters in matched separate runs with small non-multiplexed groups:
cycles/instructions per compile, per scanned group and per sorted pair; branches/
misses; thread CPU versus wall time; code bytes; disassembly/stack accesses around
calls. Local perf lists `ic_tag_hit_miss.instruction_cache_miss`,
`ic_cache_fill_l2`, `ic_cache_fill_sys`,
`de_no_dispatch_per_slot.no_ops_from_frontend`, and instruction-TLB events.
Validate scheduling and counts before use. Event availability alone is not proof
of reliable attribution. Separate compiler-thread counters from execution workers.

Predictions: B/C should remove IR/native blocks and compiler work while leaving
sorting work/traffic unchanged. E should reduce duplicated code and preparation;
it adds native calls and may change spills or frontend pressure. If claiming an
instruction-cache benefit, require reduced instruction-fetch cost/stalls together
with fewer execution cycles per fixed unit. Smaller text alone proves no speedup.
If slower, inspect call counts, spills and lost specialization before proposing a
policy. Unknown hardware evidence permits a qualified timing result, not an
invented microarchitectural explanation. Run the skill's study validator before
promoting an implementation optimization beyond the exploratory prototype.

### Decision rules and stopping point

- Every candidate must preserve results, cancellation/lifetime contracts and
  generated coverage. A failed correctness check ends its performance evaluation.
- Experimental target: reduce native Top-K preparation from about308ms to100ms
  or less, and whole warm preparation to about250ms or less. This is a target,
  not a prediction. Unchanged surrounding work already costs roughly134ms;
  eliminating Top-K compilation cannot eliminate all SQL preparation.
- Compare helper gains to the best fully-inline specialization, not only A.
  Treat a repeatable execution slowdown above3% in either the isolated phase or
  an affected complete-query family as a failure for default adoption. Report
  smaller slowdowns too; an uncertainty band crossing that margin is inconclusive.
  Require a material preparation saving (initial threshold25% for the affected
  kernel) to justify new helper machinery. Thresholds are experiment decisions,
  not production admission policies.
- Publish `C + N * E` for N=1/10/100/1000, where C is measured preparation and E
  measured warm execution; report measured first-use latency separately. When
  the candidate executes slower, calculate where its preparation saving is
  exhausted. Do not hide a long-lived-query regression behind amortization.
- If specialization wins and outlining loses, keep specialization. If outlining
  only wins for large batches, scope any subsequent policy to measured batch/work
  properties. Do not introduce query IDs, data-specific constants or caching to
  rescue the result. Broaden to affected generated-ordering queries only after a
  candidate passes, then record a concise finding in PROGRESS.md. Temporary
  diagnostic artifacts stay outside the checkout; no results archive is needed.
