# Issue 90 — SAR envelope transport

## Objective and rationale
Preserve encoded SMPP optional parameters through queue serialization and retries so the merged SAR codec can support later HTTP multipart activation. HTTP remains single-message in this work unit.

## Authorized scope and constraints
User authorized continuing issue 90 locally and fetching origin. Branch: `feat/sar-envelope-90`; base and first reviewed boundary: `032b091743bea88705833f165d435ad34cbb4527`.
No push, PR, merge, billing/quota changes, HTTP multipart activation, DLR aggregation, northbound or idempotency work. Two retries means three attempts; retain existing retry behavior. Full-cost reservation was previously approved; quota and uncertain settlement require later decisions.

## Delivery strategy
`ask-on-risk`; user selected `stacked-to-main` (independent deliveries). Forecast 280–380 authored lines plus this tracking document, generated files excluded. Actual 425 lines retain one coherent transport behavior with its regression tests; no cosmetic reductions. This unit remains independent of later HTTP activation. No remote publication.

## Tasks
- [x] T1 Preserve binary optional parameters through envelope v2, retry and quarantine paths, with tests and documentation.
  - Route: delegated direct; trigger: preparation for writing and multiple non-trivial implementation/test files.
  - Keep version 2 and canonical padded Base64 for wire binary values; absent fields remain backward compatible.
  - Preserve SAR/unknown TLV bytes without duplicating PDU semantic validation.
  - Prove retry preservation and actual SubmitSM encoding with SAR parameters.
  - Do not change worker/supervisor behavior unless a failing test identifies a necessary gap.

## Verification
Default applicable deterministic test-first policy; no verified global strict-TDD setting. Observe RED before implementation, then GREEN and check-only verification after final normalization.
- `mix test test/jasmin_ex/messaging/envelope_test.exs test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs test/jasmin_ex/messaging/rabbit_mq/work_queue_test.exs`
- `mix format --check-formatted`
- `mix credo --strict`
- `mix test`
- `mix dialyzer`
Runtime harness: deterministic queue/PDU tests, no external broker needed. RDD is on (global); native assessment and candidate consent apply before any native review.

## Progress and evidence
Fetch completed; origin/main contains merged planner #101 and SAR codec #102. Branch created from origin/main. Implementation and functional verification complete.
Observed RED: 9 failing new tests. GREEN: 69 focused tests; full suite 792 passed, 29 excluded; format and strict Credo passed; Dialyzer 0 errors. Parent reran focused tests: 69 passed.
Behavior authored line count: 425 (377 additions, 48 deletions); work-unit commit including this task document: 464 authored lines. Commit: `7b5a508d390f9caf6cc47877231e6e5c06e2c843`, tree `f9a37d5afb06ec5e71e4abd64ac246e500ea2578`. Chain strategy: user-selected independent deliveries.
Native assessment after declaring the task file: medium. Review consent granted, but the exact native reviewer task was refused with `binding_mismatch`. No reviewer verdict or receipt exists; the refused slot remains pending and no further reviewer launch is authorized by this closure.
User selected report and continue. One privacy-scrubbed occurrence comment was confirmed on the existing canonical issue: https://github.com/Gentleman-Programming/gentle-ai/issues/4966#issuecomment-5985705633. No issue-state or label changes. A relevant published fix was not verifiable.
The exact captured decline invocation succeeded: `action: declined`, `consent: declined_this_candidate`, target matched. Native bound STATUS was re-entered; it still showed the pending slot. Ordinary medium-tier writer verification and parent spot check are the proof of record. No native approval is claimed. RDD remains globally enabled.
Post-commit assessment against `032b091` reports medium / `slice_budget_reached`; this unchanged candidate was explicitly left unreviewed via the validated native decline. Do not reopen review automatically for delivery.
Rollback boundary: envelope optional-parameter transport, accompanying tests and docs only.
Engram mirror: initial write/readback confirmed; current progress mirrored by parent.

## Next step
This transport unit is locally committed; no push or PR is authorized. HTTP activation and billing remain pending future units. Resolve remaining quota/uncertain-settlement decisions before those units. Any future PR size exception remains a separate publication decision. Resume native review only through a published fix or documented maintainer-authorized recovery, never unpublished code.
