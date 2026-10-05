# Issue 90 — Segment-aware billing

## Objective
Make the Bill constructor represent the full cost and quota of a planned multipart message while preserving all default single-segment callers. This is a prerequisite, not HTTP multipart activation.

## Authorized scope and constraints
Local implementation authorized; origin/main fetched with explicit authorization and confirmed at PR #104 merge `896a02099e075f17aad2c14535f3593cb8a037ae`. Feature branch: `feat/multipart-billing-90`. No push, PR, or merge authorized.
Price and precharge rounding are per segment: total rate is unit rate times N; total precharge is floor(unit rate times percentage / 100) times N; remainder is exact. N segments consume N quota units. Admission reserves the full total before enqueue.
Accepted segments retain charge/quota; definitive nonacceptance releases reservations after applicable safe retries. Uncertain outcomes remain reserved provisionally without blind retries; recovery, visibility and retention policy are future work. None of that settlement behavior is activated by this constructor unit.
Do not change HTTP responses, `/rate`, dispatch, DLR, settlement, persisted schemas, or fingerprint format. Default segment count is 1. Validate an explicit integer count in 1..255 and reject total monetary overflow before constructing a Bill.

## Delivery strategy
Independent reviewable unit, stacked-to-main. Forecast 180–260 authored behavior/test lines plus concise documentation and this task file. Strategy: ask-on-risk; ask before a delivery grows beyond approximately 400 authored lines. The size heuristic never permits omitting tests or compressing code.

## Tasks
- [x] T1 Add optional segment_count to Bill with per-segment rounding, total-rate overflow protection and N quota; prove compatibility and total-cost admission.
  - Route: delegated direct. Trigger: preparation for writing and multiple non-trivial source/test files.
  - Edit surfaces: Bill, its unit/contract tests, routing billing tests, HTTP long-message documentation, and this task file.
  - Acceptance: absent count preserves existing bills and fingerprints; invalid counts rejected; rounding-before-multiplication proven with a distinguishing example; insufficient total balance or N quota rejected without mutation; admitted totals reflected correctly.
  - Rollback boundary: segment-aware Bill constructor, corresponding tests and documentation only.

## Verification
Default applicable test-first policy; no verified project-wide strict-TDD setting. Runner: mix test. Observe deterministic RED before implementation, then GREEN; normalize before final checks.
- `mix test test/jasmin_ex/billing/bill_test.exs test/jasmin_ex/billing/contracts_test.exs test/jasmin_ex/routing/billing_test.exs` — RED 59/70 passed, 11 failed (constructor ignored `segment_count`); GREEN 70 passed after `Bill.new/1` implementation
- `mix format --check-formatted` — pass (no output)
- `mix credo --strict` — pass, 182 files, no issues
- `mix test` — 812 passed (1 doctest, 811 tests), 29 excluded (`:compatibility`, `:integration`)
- `mix dialyzer` — pass, Total errors: 0
Runtime harness: N/A, this unit exercises the Bill constructor and deterministic in-process admission without activating HTTP or external broker work.
RDD: on (global). Committed-only native assessment: medium, `review_due: false`, reason `under_budget`; no reviewer run or native approval exists. No known baseline check failures.

## Progress
T1 implemented locally; parent independently reran the focused suite: 70 passed. HTTP multipart remains inactive.
Constructor: optional `segment_count` 1..255 (default 1 when omitted); explicit nil and other invalid values return `:invalid_segment_count`; `rate_minor` input is per-segment; stored `rate_minor` is `unit * N`; `quota_debit` is N; split unit first then multiply by N (unit 3 / 50% / N 3 -> total 9, precharge 3, remainder 6). Admission unchanged; existing `State.admit/3` already debits stored totals.
Git diff (tracked sources, excluding this task file): 5 files, 253 insertions, 19 deletions. No commit, review, or Engram-mirror success claimed here.
Work-unit commit: `1842a0e` (`feat(billing): account for multipart segments`), 293 additions and 19 deletions, 312 authored lines including this document. Functional checks complete; the medium slice stays pending below the native review budget.
First reviewed boundary: `896a02099e075f17aad2c14535f3593cb8a037ae`.
Initial worktree assessment was unassessable because this task file was untracked; no low-risk inference was made. The committed-only assessment will cover the complete work unit, including this document.

## Next step
Publish this independent billing prerequisite only after explicit user authorization, or continue with a separately scoped multipart transport/settlement unit. HTTP multipart remains inactive; DLR identity/callback behavior and uncertainty recovery need their own contract before activation.
