# Issue 90 — Durable atomic segment settlement

## Objective and problem
Persist opt-in segment outcomes and account deltas together so concurrent or restarted duplicate results cannot refund a segment twice. HTTP multipart remains inactive.

## Authorized scope and decisions
User authorized implementation, including provisional uncertain treatment for missing responses/timeouts. Pending or uncertain segments must not automatically refund, retransmit, or be bypassed by whole-bill settlement/expiry. Final timeout billing and reconciliation policy are deferred. Accepted segments retain charge/quota; confirmed rejected segments refund full unit price including precharge plus one quota.
Mark the new mode at admission, not at the first result. Preserve legacy reservation and snapshot semantics. No HTTP, SMPP dispatch, DLR, retry, remote operation, push, PR, or merge changes.
Branch: `feat/durable-segment-settlement-90`; local base: `960237ff18ba1f382a73dcc08d4324879009149e` containing the previously merged PR106 implementation. No remote baseline refresh performed.

## Delivery strategy
Stacked-to-main, three functional work units chosen after one honest slicing pass. User accepted the U1 size exception only (506 actual authored lines). That exception does not widen to U2 or U3. This document must not be read as the whole feature being native-approved.

## Tasks
- [x] T1A In-memory opt-in reservation, segment refunds, and expiry-overflow protection.
  - Commit: `34eff975df47156d1f77bf72d485fdc70897f1a0`. Ordinary verified locally; not native-approved.
  - Size exception: 506 authored lines. Parent 72; worker 833 passed, 29 excluded.
  - Native lineage: `binding_mismatch` then exact decline. Bound STATUS remains reviewing as diagnostic only. Do not resume or synthesize that lineage.
- [x] T1B Snapshot v5 codec and backward reads of versions 1–4.
  - Commit: `1321e86c693a8d6c1b622a18cd5f421590e09f04` (`feat(billing): persist segment ledger in versioned snapshots`), 342 authored lines.
  - Outcome: ordinary verified (parent aux 75 focused; writer 836 passed, 29 excluded; format, Credo, Dialyzer pass). Checkbox closed locally; not native-approved.
  - Native committed assessment: medium, 820 lines since last reviewed boundary `960237f`. User `Omiti resta vez`; exact decline confirmed for target `d0e733a7b2a683f617c68810e974be024c9037849897296ff23ea06477f553e8`; no active U2 review created. Parent medium reassessment after decline.
- [ ] T1C Router public APIs, restart/concurrency proof, and capability docs.
  - Route: delegated direct.
  - `Routing.admit_segments/2` and `Routing.settle_segment/5`, snapshot transaction, kill-restart, concurrent same-index and distinct-index, docs.
  - Staged with runtime proof; checkbox pending parent commit and native choice.

## Verification
Default applicable deterministic test-first policy, source: orchestrator default; global strict TDD unknown. Runner: `mix test`.
- `mix test test/jasmin_ex/billing/segment_ledger_test.exs test/jasmin_ex/routing/billing_test.exs test/jasmin_ex/routing/billing_snapshot_test.exs test/jasmin_ex/routing/snapshot_test.exs`
- `mix format --check-formatted`
- `mix credo --strict`
- `mix test`
- `mix dialyzer`
Runtime proof for T1C: Router persist-before-publish, kill-restart of empty opted-in ledger, concurrent same-index one credit, distinct-index both refunds, snapshot_failed rollback-then-retry, expire empty/uncertain uncredited, terminal then late ignored. Guarantee remains a single local Router and atomic rename process-restart proof, not power-loss/fsync, multi-router, or SMS delivery exactly-once. No known baseline failures.
RDD read-only status: on, global. Last native reviewed boundary remains `960237ff18ba1f382a73dcc08d4324879009149e` because no receipt was acknowledged. Full unsliced writer proof 83 focused / 844 `mix test` (parent 83) is retained.

## Progress and next step
U1 and U2 are committed and locally checkbox-closed as ordinary verified, not native-approved. U3 Router APIs, runtime proofs, and docs are staged on HEAD `1321e86`. Main-worktree proof of HEAD plus staged U3: focused quartet 83 passed; format exit 0; credo 184 files, no issues; `mix test` 844 passed, 29 excluded; dialyzer 0 errors. Staged U3 365+/27- (392 authored). T1C checkbox pending parent commit and native choice. No remote operations. No commit in this unit.
