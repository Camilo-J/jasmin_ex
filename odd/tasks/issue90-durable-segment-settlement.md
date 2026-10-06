# Issue 90 — Durable atomic segment settlement

## Objective and problem
Persist opt-in segment outcomes and account deltas together so concurrent or restarted duplicate results cannot refund a segment twice. HTTP multipart remains inactive.

## Authorized scope and decisions
User authorized implementation, including provisional uncertain treatment for missing responses/timeouts. Pending or uncertain segments must not automatically refund, retransmit, or be bypassed by whole-bill settlement/expiry. Final timeout billing and reconciliation policy are deferred. Accepted segments retain charge/quota; confirmed rejected segments refund full unit price including precharge plus one quota.
Mark the new mode at admission, not at the first result. Preserve legacy reservation and snapshot semantics. No HTTP, SMPP dispatch, DLR, retry, remote operation, push, PR, or merge changes.
Branch: `feat/durable-segment-settlement-90`; local base: `960237ff18ba1f382a73dcc08d4324879009149e` containing the previously merged PR106 implementation. No remote baseline refresh performed.

## Delivery strategy
Stacked-to-main, three functional work units chosen after one honest slicing pass. Tracked source/test/doc diff was 1078 insertions plus 61 deletions (**1139 authored lines**) before U1 commit. Do not trim tests to fit the ~400-line budget. User accepted the U1 size exception only (506 actual authored lines). Adding this evidence does not widen that exception to later units. This document must not be read as the whole feature being done.

## Tasks
- [x] T1A In-memory opt-in reservation, segment refunds, and expiry-overflow protection.
  - Route: delegated direct; triggers: preparation for writing and multiple non-trivial files.
  - Commit: `34eff975df47156d1f77bf72d485fdc70897f1a0` (`feat(billing): add opt-in in-memory segment settlement`).
  - Outcome: ordinary verified locally (self-proof + independent technical + parent focused 72). Checkbox closed locally; not native-approved.
  - Size exception: 506 actual authored lines. Parent 72; worker `mix test` 833 passed, 29 excluded.
  - Native review: provider review-reliability relay `binding_mismatch` failed before execution. User continued without that report. Exact candidate decline confirmed (`action=declined`, `consent=declined_this_candidate`, target matched). Bound STATUS remains reviewing; preserve it; no review-authority recovery. Do not resume that failed lineage; keep it as diagnostic only.
  - Last reviewed boundary remains `960237ff18ba1f382a73dcc08d4324879009149e` because no receipt was acknowledged. Parent medium reassessment of U1.
- [ ] T1B Snapshot v5 codec and backward reads of versions 1–4.
  - Route: delegated direct.
  - Persist ledger with reservation bindings, count, money, and outcomes; extra keys ignored; unsupported version 6 fails closed.
  - Source is implemented and independently verifiable in the U2 aux worktree. Checkbox stays pending parent commit and native choice.
- [ ] T1C Router public APIs, restart/concurrency proof, and capability docs.
  - Route: delegated direct.
  - `Routing.admit_segments/2` and `Routing.settle_segment/5`, snapshot transaction, kill-restart, concurrent same-index and distinct-index, docs.
  - Implemented in the live worktree; unstaged pending.

## Verification
Default applicable deterministic test-first policy, source: orchestrator default; global strict TDD unknown. Runner: `mix test`; observe RED, GREEN and refactor. Normalize before final proof and review freeze.
- `mix test test/jasmin_ex/billing/segment_ledger_test.exs test/jasmin_ex/routing/billing_test.exs test/jasmin_ex/routing/billing_snapshot_test.exs test/jasmin_ex/routing/snapshot_test.exs`
- `mix format --check-formatted`
- `mix credo --strict`
- `mix test`
- `mix dialyzer`
Runtime proof for U2: snapshot restore/write of ledger identity, v1–v4 backward reads, v4 extra-key ignore, unsupported v6. Restart/concurrency Router proofs belong to T1C. No external runtime proof claimed. No known baseline failures.
RDD read-only status: on, global. Native assessment and candidate consent remain pending for U2. Prior U1 native decline does not approve this candidate.

## Progress and next step
U1 is committed at `34eff97` and locally checkbox-closed as ordinary verified, not native-approved. Full-feature writer proof remains 83 focused / 844 `mix test` (parent 83) for the unsliced tree. U2 snapshot codec is staged; U3 stays unstaged in the live worktree. U2 aux `/tmp/opencode/jasmin-ex-snapshot-90` at HEAD `34eff97` plus staged bytes (hash `b8ca43ab76ed248b30afa167a6ea0563a4177141162f0283090c0e3dbb76f5ba`): focused quartet 75 passed; format exit 0; credo 184 files, no issues; `mix test` 836 passed, 29 excluded; dialyzer 0 errors. T1B checkbox pending parent commit and native choice. No remote operations. No commit in this unit.
