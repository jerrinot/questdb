# Generated string negation and ordered filter LIMIT

Status: milestones 1–3 implemented and correctness-checked, 2026-10-28.
Milestone 4 (full-data qualification and controlled performance) remains pending.
Query numbers use the local zero-based Q0–Q42 convention.
Wall-clock profiling remains paused. Small correctness and code-generation checks
can proceed; performance experiments wait until the user resumes profiling.
Preserve the existing uncommitted multiplication work.

## Implementation checkpoint

Negation binds the existing VARCHAR leaves. A lazy QuestRowLimit span kernel
admits task prefixes in collection order; the cursor consumes them without charging
the budget again. Descriptor-based native selection receives an immutable K and
stops after K accepted rows. Numeric-only filters retain their existing full-frame
SIMD path, avoiding a new descriptor allocation on that path. Parquet retains
full-frame filtering so truncated counts cannot bias its selectivity estimator.
The existing late-materialization column-top fallback remains unchanged.

The original Q22/Q23/Q24 SQL matches stock on a fresh 4,096-row fixture using the
ClickBench schema. Ordered comparisons cover all 105 Q23 columns. Drained counters:
Q22 filters 4,096 inputs, selects/updates 2,253 rows and ranks five group entries;
Q23 evaluates 24 rows across dispatched frames, admits ten in one collected frame;
Q24 evaluates/admits ten rows. The full-frame LLVM control evaluates 262 rows for
each, with matching output and unchanged worker/frame/dispatch settings. Work
counts include scheduling slack; this is not full-data qualification or a latency
measurement. The admission-only control and hardware study remain in milestone 4.

42 SQL checks pass across LLVM/AsmJit/C and legacy/fiber workers, plus nine scan
checks and four existing core LIMIT/cancellation checks. Coverage includes
deterministic later-frame-first completion, ordered output, count/skip/rewind,
bind changes, zero-dispatch LIMIT 0, Parquet, column tops, protected tails, mask
trimming, and failures preparing either optional kernel. A dense 4,096-row fixture
evaluates 10–20 rows with two in-flight frames and LIMIT 10. No timing claim.
Evidence: `/tmp/query-filter-limit-20261028`.

## Outcome and evidence

Qualify the original SQL with every executed data-processing operator generated
through LLVM. Keep QuestDB's planner, snapshots, worker scheduling, map layouts,
record ownership and output serialization. Coverage and performance are separate
acceptance criteria; native-call counters must substantiate plan markers.
Eligibility uses resolved implementations, ordering and LIMIT semantics; query IDs
are test labels only.

The planning baseline was:

| Query family | Execution before this change | Missing work |
| --- | --- | --- |
| Q22: string predicates, grouped extrema/DISTINCT, Top-K | Native map updates and Top-K; Java WHERE | Negation of supported string predicates |
| Q23: filtered full rows, timestamp order, LIMIT | LLVM filtering; ordered scan; Java early LIMIT | Generated admission of the ordered prefix |
| Q24: filtered VARCHAR output, timestamp order, LIMIT | Same filter/LIMIT path, wrapped in SelectedRecord | Same admission capability |

The latest full-data audit recorded 30/43 complete pipelines. A subsequent
4,096-row replica using the original ClickBench schema confirms Q22 performs
2,253 native map updates and one native Top-K visit, but zero native filter rows.
Changing only NOT LIKE to LIKE admits all 4,096 filter inputs to LLVM; both results
match stock. Q23 was inspected without execution; Q24's values match stock, but the
helper sorts results, so that check does not establish ordered LIMIT correctness.
Evidence: `/tmp/query-clickbench-coverage-20261028`. This is neither a performance
result nor full-data requalification.

## 1. Close Q22: negate existing string predicates

**Code boundary.** [ResolvedFilter.bind](src/main/java/org/questdb/experimental/codegen/ResolvedFilter.java)
flattens top-level AND, emits VARCHAR NULL/short literal-contains leaves, and sends
other leaves to ResolvedInputs. That numeric binder handles NOT recursively but
cannot bind its VARCHAR child. [NotFunctionFactory](../../core/src/main/java/io/questdb/griffin/engine/functions/bool/NotFunctionFactory.java)
returns `!arg.getBool(record)`; the admitted contains function returns false on
NULL. Therefore its negation returns true on NULL. Preserve the actual resolved
functions, rather than importing a different SQL NULL convention.

Implementation:

1. Within the existing leaf-binding path, recognize chains of the exact
   NotFunctionFactory.Func implementation around supported VARCHAR leaves.
   Track negation parity and retain the existing depth bound. Emit the existing
   leaf once, then invert its Boolean result when required. Numeric predicates
   continue through their existing binder.
2. Keep current top-level conjunction ordering, numeric rejection before string
   decoding, descriptor ownership and bounded ByteSequences loads. Do not add a
   new predicate hierarchy, regex engine, function registry or setting. Unsupported
   composite expressions and patterns continue to decline the complete binding.
3. Reuse this binding for standalone selection, count-only filtering and aggregate
   filtering. Existing generated map mutation, DISTINCT, merge and Top-K stay intact.

Validation: extend existing substring SQL tests with negation, double negation,
NULL/empty values, inline/external strings, multibyte literals and boundary matches.
Include one grouped case with two VARCHAR extrema and LONG DISTINCT, plus an
unsupported-pattern fallback. Use LLVM/AsmJit/C and both existing worker modes;
reuse current Parquet, column-top and ownership checks. Require exact native input
and selected-row counts, matching groups, and exercised generated merge/Top-K.
An all-null/empty result alone cannot prove those downstream paths executed.

Performance hypothesis: remove per-row Java predicate dispatch and expose string
tests to native optimization. The row set and logical string work must remain the
same. Measure filter share `f` before predicting total gain: a filter speedup `s`
can improve the query by at most `1 / ((1-f) + f/s)`. A reduced Java profile alone
does not establish lower cycles or a hardware bottleneck. Do not combine this
patch with filter/aggregate fusion or a different substring algorithm.

**Exit:** original Q22 matches stock; LLVM accounts for filtering, grouping,
aggregate update/merge and ranking. Verify on actual data before adding it to the
full-data coverage count. Preserve tied-Top-K validation from the existing audit.

## 2. Close Q23/Q24: generated frame admission in the ordered collector

**Existing algorithm to retain.**
[AsyncJitFilteredRecordCursorFactory](../../core/src/main/java/io/questdb/griffin/engine/table/AsyncJitFilteredRecordCursorFactory.java)
evaluates LIMIT on cursor open and dispatches frame filters through PageFrameSequence.
Workers produce ordered frame-local row IDs; the owner consumes frames in scan
order. [AsyncFilteredRecordCursor](../../core/src/main/java/io/questdb/griffin/engine/table/AsyncFilteredRecordCursor.java)
currently enforces LIMIT through rowsRemaining in hasNext/checkLimit, skipRows and
calculateSize. The planner already obtains EventTime order from the scan: neither
query requires sorting. SelectedRecord only maps Q24's output columns.

The existing [QuestRowLimit](src/main/java/org/questdb/experimental/codegen/QuestRowLimit.java)
enumerates map entries or sorted row-ID pairs. It is not a borrowed frame-prefix
operator. Do not route these scans through map descriptors, a materializing LIMIT
wrapper, a new scheduler or a copied list of complete query results.

Implementation:

1. Add a narrow optional frame-admission capability to the existing CompiledFilter
   boundary, implemented by ResolvedStandaloneFilter.Rows. The stock implementation
   declines. Prefer this existing owner over a new operator/strategy hierarchy.
   Lazily prepare a small typed-IR span kernel when an eligible cursor opens;
   the filter wrapper owns its code and fixed-size owner-only budget state.
2. Initially admit only forward scans with an already-advised finite nonnegative
   LIMIT. Evaluate bound functions on each open, as today; never bake a runtime
   bind value into code. Negative, NULL/unbounded, reverse scans and outer OFFSET
   retain existing paths. Preserve getLimitLoFunctionOnly's LATEST ON exclusion.
3. For each collected frame, execute `take = min(available, remaining)` and update
   remaining in generated code. Expose only that prefix of the existing task row-ID
   list; do not copy IDs or payloads. Java enumerates admitted records and owns
   scheduling, but no longer implements per-row LIMIT selection on this path.
4. Apply admission exactly once per frame, including count-only tasks. Already
   admitted rows consumed by hasNext, skipRows or calculateSize must never debit
   the budget twice. Check for buffered admitted rows before treating a zero
   remaining budget as end-of-stream. Zero LIMIT must not dispatch filter work.
5. Signal normal completion when the admitted prefix is consumed; retain its
   borrowed frame until collection. Preserve cancellation reasons and existing
   collect/await/reset cleanup. Reset the budget on reopen and after toTop has
   drained old work. Free code/state only after worker calls finish, including
   partial preparation failures and existing factory half-close/close paths.

Only the ordered owner mutates the global budget. A shared atomic quota in workers
is incorrect: a later frame could consume matches before an earlier frame finishes.
One native admission call per collected frame is acceptable to experiment with;
one native call per returned row is not. This milestone alone may be neutral or
slower for LIMIT 10: it closes coverage, not the performance argument.

Validation: add a focused ordered filter/LIMIT case to existing parameterized SQL
tests. Compare cursor order directly, not Demo.readSql's sorted results. Cover
empty/no-match, dense/sparse matches, a terminal frame containing more matches than
needed, limits spanning frames/partitions, zero, reopen/toTop, interleaved skipRows
and calculateSize, and fallback for changed bind values. Exercise out-of-order
worker completion with deterministic coordination, not sleeps. Retain random
access and record lifetime, especially SELECT * and Parquet payloads.
Reuse existing positive/negative LIMIT and failure tests rather than building a
new test harness. Run the affected cases in both worker modes.

**Exit:** original Q23/Q24 return the correct ordered prefix; counters show native
filtering and native admission. No added sort, result materialization, shared quota
or full-table scan. Do not count Q24's column mapping or ordinary output record
getters as an unimplemented transformation.

## 3. Experiment: stop filtering a frame after enough matches

The current generated filter processes the entire supplied frame even when LIMIT
needs ten rows. The opportunity is to avoid predicate loads and row-ID writes,
not merely to compile the final decrement.

After milestone 2 is correct, extend the existing ScanLowering selection emission
and CompiledFilter call boundary with a bounded variant. Each worker receives the
same immutable per-execution cap K, the original positive LIMIT. It emits the first
K matching IDs in its frame and then stops. The collector still admits only its
remaining global budget. Proof: no frame can contribute more than K rows to a
global prefix of length K, so dropping that frame's later matches is safe.
No worker reads or updates the owner's changing remaining budget.
Publish the immutable cap through existing task publication before dispatch, and
change it only after old work drains on reopen/toTop.

Keep the normal unlimited kernels unchanged. Retain mask-based selection and
bounded tails; trim the final mask in source order, and test the cap after complete
string-residual acceptance. Preserve original frame coordinates. Do not shorten
the source frame's row count or treat scanned-row count as matched-row count.
Use the existing output buffer, task publication and late-materialization APIs.
If bounded selection biases Parquet selectivity statistics, do not feed its
truncated count into the full-frame estimator. Preserve the current fallback
until that contract is handled; do not disable late materialization globally.

Expected work, for frame size F, selectivity p and dispatch window D: ordinary
filtering may evaluate about D*F rows around early termination; bounded filtering
should approach D*min(F,K/p), with vector-block and scheduling slack. This model
assumes reasonably distributed matches. Clustering and zero-match frames can
erase the saving. It excludes frame enumeration, mapping, decoding and output,
which can dominate elapsed time. Keep the configured dispatch window unchanged.
PropServerConfiguration defaults it to min(queryWorkers,32), while
DefaultCairoConfiguration uses Integer.MAX_VALUE: exercise both in correctness
checks, and use identical server settings for stock/candidate measurements.

Audit supplied rows, actually evaluated rows, emitted IDs, admitted IDs, dispatched
and drained frames separately. The current getNativeInputRows increments by the
supplied frame size; it cannot prove bounded work without revised instrumentation.
Keep diagnostics out of timed runs when they perturb execution.

**Exit:** fewer evaluated rows and selection writes on dense early-match inputs,
correct sparse/clustered behavior, and no extra frame dispatch or result copying.
An admission-only versus bounded-filter control isolates this mechanism. Do not
retain a performance claim if lower work fails to reduce complete latency.

## 4. Qualify and measure, one query at a time

After profiling resumes, use unchanged original SQL and the existing isolated
99,997,497-row ClickBench snapshot; protect `$HOME/.questdb`. First validate Q22,
then Q23, then Q24. Check Q23's full typed output, not just row IDs. Verify Q24's
timestamp ordering through ordered cursor checks and timestamp ties. Count-only
or grouped plan labels are insufficient; record calls for every required stage.

Run stock and candidate with identical workers, frame sizes and dispatch settings.
Follow the repository protocol: one cache drop per query, then three executions
with the server alive. Report first-request latency including compilation and both
warm requests; use at least three matched process pairs in balanced order.
Perform the USE preflight and hardware bottleneck study before performance tuning.
Choose counters supported by the detected machine and normalize to fixed query
work: cycles/instructions, branches, cache/TLB behavior, faults/I/O and bytes read
or written. Distinguish reduced total work from improved cost per evaluated row.

Use existing large-control infrastructure for independent 500M-row dense, sparse
and clustered inputs, changed string lengths/null rates, several LIMIT values,
and a wide-output case. Keep correctness fixtures small. Once targeted checks pass,
run the full 43-query comparison to detect effects on other filters. Cross-machine
claims require another architecture; do not revive the deleted EC2 target.

Record only retained conclusions and important failures in PROGRESS.md. No new
toggles or benchmark-result archive. Report new full-data qualification only after
the execution audit passes (potentially 30 -> 31 -> 33); report >=1.5x repeatable
major gains separately. Completing coverage does not establish a breakthrough.
