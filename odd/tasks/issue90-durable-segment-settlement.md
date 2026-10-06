# Issue 90 — Durable atomic segment settlement

## Objective and problem
Persist opt-in segment outcomes and account deltas together so concurrent or restarted duplicate results cannot refund a segment twice. HTTP multipart remains inactive.

## Authorized scope and decisions
User authorized implementation, including provisional uncertain treatment for missing responses/timeouts. Pending or uncertain segments must not automatically refund, retransmit, or be bypassed by whole-bill settlement/expiry. Final timeout billing and reconciliation policy are deferred. Accepted segments retain charge/quota; confirmed rejected segments refund full unit price including precharge plus one quota.
Mark the new mode at admission, not at the first result. Preserve legacy reservation and snapshot semantics. No HTTP, SMPP dispatch, DLR, retry, remote operation, push, PR, or merge changes.
Branch: `feat/durable-segment-settlement-90`; local base: `960237ff18ba1f382a73dcc08d4324879009149e` containing the previously merged PR106 implementation. No remote baseline refresh performed.

## Delivery strategy
Stacked-to-main, three functional work units chosen after one honest slicing pass. Tracked source/test/doc diff is 1078 insertions plus 61 deletions (**1139 authored lines**), excluding this tracking file. Do not trim tests to fit the ~400-line budget. U1 is the first native candidate and remains pending assessment. U2 snapshot v5 codec and U3 Router public APIs plus restart/concurrency docs are implemented in the live worktree and stay unstaged until parent commits those units. This document must not be read as the whole feature being done.
User explicitly accepted the 505-line first-unit size exception only; adding this evidence line does not widen that exception to later work. Parent independently reran the first unit focused quartet: 72 passed. No remote publication is authorized.

## Tasks
- [ ] T1A In-memory opt-in reservation, segment refunds, and expiry-overflow protection.
  - Route: delegated direct; triggers: preparation for writing and multiple non-trivial files.
  - Mark segment mode at admission on Reservation/State. Apply ledger identity, outcomes, remainder, and account deltas in one State transition.
  - Duplicate results are no-ops; terminal conflicts, malformed identity/index/outcome, and overflow leave state unchanged.
  - Protect pending/uncertain reservations against whole-bill settle/expiry, including before the first result.
  - Typed expiry overflow leaves the entire due batch unchanged; Router expire_change must surface that error so legacy expire does not crash.
  - Rollback boundary: optional reservation ledger field, State segment APIs, in-memory tests, and the Router expire error adapter.
- [ ] T1B Snapshot v5 codec and backward reads of versions 1–4.
  - Route: delegated direct.
  - Persist ledger with reservation bindings, count, money, and outcomes; extra keys ignored; unsupported version 6 fails closed.
  - Implemented in the live worktree; not part of the U1 index.
- [ ] T1C Router public APIs, restart/concurrency proof, and capability docs.
  - Route: delegated direct.
  - `Routing.admit_segments/2` and `Routing.settle_segment/5`, snapshot transaction, kill-restart, concurrent same-index and distinct-index, docs.
  - Implemented in the live worktree; not part of the U1 index.

## Verification
Default applicable deterministic test-first policy, source: orchestrator default; global strict TDD unknown. Runner: `mix test`; observe RED, GREEN and refactor. Normalize before final proof and review freeze.
- `mix test test/jasmin_ex/billing/segment_ledger_test.exs test/jasmin_ex/routing/billing_test.exs test/jasmin_ex/routing/billing_snapshot_test.exs test/jasmin_ex/routing/snapshot_test.exs`
- `mix format --check-formatted`
- `mix credo --strict`
- `mix test`
- `mix dialyzer`
Runtime proof for U1: in-memory State transitions plus Router expire overflow adapter. Restart/concurrency and snapshot codec proofs belong to later units. No external runtime proof claimed. No known baseline failures.
RDD read-only status: on, global. Native assessment and candidate consent remain pending; prior declines do not apply to this candidate. First candidate is staged U1 only.

## Progress and next step
Live worktree holds the full implemented feature; only U1 is staged. U2/U3 remain unstaged. Checkboxes stay pending parent commit and native closure. Prior implementation RED/GREEN evidence is preserved; this unit stages no new behavior.
U1 independently observed in `/tmp/opencode/jasmin-ex-domain-90` from HEAD plus staged U1 bytes (hash `a2eac35c1bee5cc382ad4340cd4836e72a27d89976565fc9b2a931077a6040d0` of the pre-observation patch; worktree retained). Focused quartet 72 passed; `mix format --check-formatted` exit 0; `mix credo --strict` 184 files, no issues; `mix test` 833 passed, 29 excluded; `mix dialyzer` 0 errors. First candidate pending native assessment. No remote operations. No commit.
