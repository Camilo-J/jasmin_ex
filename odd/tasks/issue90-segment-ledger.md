# Issue 90 — Idempotent segment settlement ledger

## Objective
Define the new per-segment settlement behavior as a pure, opt-in ledger before integrating it with persistence and account mutations. HTTP multipart remains inactive.

## Authorized scope and decisions
Local implementation authorized after PR #105 merged. Branch: `feat/segment-ledger-90`; base: `19346c6b468354ff47eb7048549aeb7821423224`. No push, PR or merge for this unit.
The user approved full-price refund (precaptured amount plus remainder) and one quota credit for a definitively nonaccepted segment after applicable safe retries. Accepted segments retain charges/quota. Uncertain outcomes stay pending provisionally without blind retransmission; recovery and retention policy are later work.
This unit calculates deltas only. It does not credit accounts, persist recovery state, expire reservations, change legacy whole-bill settlement, activate HTTP, change DLR, or implement retries.

## Delivery strategy
Independent units toward main, using the already selected stacked-to-main strategy. Forecast 260–340 authored source/test/doc lines plus this task document. Approximately 400 lines remains a planning heuristic, never a reason to shrink tests or code-golf.
Observed source/test/doc size is 495 lines, plus this task document. The user explicitly accepted a single-unit size exception: the validation, binding, uncertainty resolution and refund-idempotency proof belong together. This exception does not authorize publication or labels and does not cover later units.

## Tasks
- [x] T1 Add a pure SegmentLedger bound to bill identity and fingerprint with independent segment outcomes and exactly-once refund deltas.
  - Route: delegated direct; trigger: preparation for writing and multiple non-trivial source/test files.
  - Allowed changes: new ledger module and tests, a concise capability paragraph in HTTP long-message docs, and this task document.
  - Accept a valid Bill with count from quota_debit; preserve total/unit arithmetic and validate hand-built or malformed input without raising.
  - Reject invalid indexes, outcomes or binding; repeated same outcomes return zero deltas; terminal outcome conflicts cannot change state.
  - Uncertain segments may resolve to accepted or rejected; the first definitive rejection returns full unit price and one quota credit, including zero-price bills.
  - No timeout/refund operation for pending or uncertain outcomes; retry attempts are not segment identities.
  - Rollback boundary: new pure module and tests plus its documentation, without account or production behavior changes.

## Verification
Default applicable test-first policy; no verified global strict-TDD setting. Runner: mix test. Observe deterministic RED before implementation, then GREEN and refactor. Normalize before final checks.
- `mix test test/jasmin_ex/billing/segment_ledger_test.exs test/jasmin_ex/billing/bill_test.exs test/jasmin_ex/billing/contracts_test.exs`
- `mix format --check-formatted`
- `mix credo --strict`
- `mix test`
- `mix dialyzer`
Runtime harness: N/A; pure immutable state transitions and refund calculations, with no broker or HTTP activation. No known baseline check failures.
RDD: globally on. Native assessment: medium, `review_due: true`, reason `slice_budget_reached`. Candidate consent and any reviewer result remain pending. The previous billing review refusal and candidate decline do not consent to, approve, or disable this new candidate.

## Progress
T1 source implemented locally; no work-unit commit in this unit. Observed RED: `mix test test/jasmin_ex/billing/segment_ledger_test.exs` → 0/10 passed, `UndefinedFunctionError` `SegmentLedger.open/1` (module not available). Observed GREEN after implementation: 10/10 passed. Required checks: focused billing trio 41 passed; `mix format --check-formatted` exit 0; `mix credo --strict` 184 files, no issues; `mix test` 822 passed, 29 excluded (`:compatibility`, `:integration`); `mix dialyzer` 0 errors. Runtime harness N/A. First reviewed boundary remains the base commit above.
Parent structurally read back the implementation and independently reran the exact required focused command: 41 passed. Local work-unit commit and native assessment follow; no account credit or durable settlement has been claimed.
Work-unit commit: `f534dc8` (`feat(billing): track idempotent segment refund deltas`), 4 files and 535 additions. The accepted size exception preserves all tests. Native review is due for this independent unit against the recorded base.

## Next step
Parent records the local work-unit commit and native assessment. Later integrate per-segment state and deltas atomically with durable reservations before HTTP dispatch; do not claim durable exactly-once behavior from this pure unit alone.
