# Issue 90 — UDHI envelope transport

## Objective and rationale
Preserve SubmitSM `esm_class` through envelope serialization, queue retries and quarantine so the approved UDH planner output can reach PDU encoding without losing the UDHI bit. This unit does not activate HTTP multipart submission.

## Authorized scope and constraints
User authorized local continuation and a fresh origin fetch using configured Git credentials. Branch: `feat/udh-envelope-90`; base and first reviewed boundary: `da79a9d20552a3b9e90128a4a3ef7829675ae119` (merged PR #103).
No push, PR, merge, HTTP activation, billing/quota changes, DLR aggregation, northbound or idempotency work. Preserve single-message compatibility and SAR transport. UDH is the previously approved default; SAR remains configurable. Full-cost reservation and segment-first price/rounding are approved; quota and partial/uncertain settlement remain unresolved future decisions.

## Delivery strategy
Independent deliveries (`stacked-to-main`), inherited from issue 90. Forecast 150–250 authored behavior/test/doc lines plus this task document. The previous 467-line publication exception applied only to PR #103. Ask before a new oversized delivery; never shrink tests or documentation cosmetically.

## Tasks
- [x] T1 Preserve valid one-octet `esm_class`, including UDHI `0x40`, through envelope v2 and existing retry/quarantine paths.
  - Route: delegated direct; trigger: preparation for writing and multiple non-trivial implementation/test files.
  - Preserve backward compatibility when the field is absent or default zero; v1 text bytes and existing v2 binary encodings stay unchanged.
  - Reject invalid field types/ranges; leave UDH/SAR semantic validation in the PDU codec.
  - Prove binary UDH payload and UDHI reach SubmitSM encoding after queue round trips.
  - Keep worker/supervisor implementation unchanged unless an observed failing test establishes a necessary gap; request scope expansion first.

## Verification
Default applicable deterministic test-first policy; no verified global strict-TDD setting. Observe RED, implement GREEN, normalize edited source files before final check-only verification.
- `mix test test/jasmin_ex/messaging/envelope_test.exs test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs test/jasmin_ex/messaging/rabbit_mq/work_queue_test.exs`
- `mix format --check-formatted`
- `mix credo --strict`
- `mix test`
- `mix dialyzer`
Runtime boundary: deterministic queue/PDU tests without an external broker. Native review mode is globally on; this candidate needs its own assessment and consent. Previous native transport refusal on 4.0.0 is not a functional-test baseline failure and does not authorize bypassing a new consent prompt.

## Progress and evidence
Fresh origin/main fetch confirmed merged PR #103 (`da79a9d`); branch created from that commit. Implementation and functional verification are complete. RDD: on (global), installed build 4.0.0.
Observed RED: 7 new tests failed with `KeyError` because `esm_class` was dropped. GREEN: 76 focused tests. Writer reported full suite 799 passed, 29 excluded; format and strict Credo passed; Dialyzer 0 errors. Parent independently reran the focused tests: 76 passed, and read back `envelope.ex`.
Behavior diff before this task-document update: 338 additions, 12 deletions. Absent, v2 JSON-null, and `0` stay off the in-memory map and ordinary v2 wire. Envelope does not validate UDHI/SAR coexistence; PDU encode still rejects that combination. Worker and supervisor were not changed.
Work-unit commit: `7b19e4921272e5c31f5a739d7b794368bb7291ac` (377 additions, 12 deletions; 389 authored lines including this document). Native assessment against `da79a9d`: medium, `review_due` false, reason `under_budget`. No review was started and no approval is claimed. The reviewed boundary remains `da79a9d` until a later commit reaches the delivery budget or is high risk.
Rollback boundary: `esm_class` envelope transport and its tests/docs only.
Engram mirror: closing evidence mirrored and read back by the parent.

## Next step
UDHI transport is locally committed and under the review budget. No push or PR is authorized. HTTP multipart and billing remain later units; quota and uncertain settlement still need decisions before those units.
