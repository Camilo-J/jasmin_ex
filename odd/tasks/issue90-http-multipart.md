# Issue 90 — HTTP multipart integration

## Objective and authorized scope
Connect HTTP long-message planning to segment-aware admission, sequential queue publication and SMSC settlement, with HTTP-to-PDU proof. Existing single-message behavior stays compatible. Full multipart DLR processing, client idempotency and northbound SMPP are excluded.
User selected server-owned UDH/SAR configuration (UDH default, maximum 5 segments) and stopping publication on the first rejection or uncertain outcome. Unattempted segments recover full unit price including precharge plus one quota; queued segments await SMSC; uncertain segments never auto-refund or retransmit. HTTP full success requires every broker confirmation, not SMSC or handset delivery. Final timeout charge/reconciliation policy remains deferred.
Branch: `feat/http-multipart-90`; local base `f89b74a6b45ca89574da394d8812677bb3f0a12e` contains all prerequisites merged through PR109. No remote refresh performed. No push, PR or merge is authorized for this feature yet.

## Delivery and safety
Default ask-on-risk, cached stacked-to-main topology. Forecast about 1590 authored lines across five independent prerequisite/activation units; T1 identity codec is an accepted first-unit size exception, not cover for later units. Later unit forecasts are unchanged. The 400-line budget is advisory for implementation, never code-golf or omitted tests; report any coherent overage before commit. Commit each verified unit with its tests/docs.
Identity precedes settlement, compensation and activation. Unique child gateway ids preserve existing journal keys; segment indexes never become retry attempts. Legacy envelopes keep version 2 and identical bytes; metadata-bearing envelopes use a new version so old readers fail closed rather than silently discard billing bindings. Segment metadata is unused until retry transport lands. Broker queue acceptance must not become terminal ledger acceptance. No distributed or power-loss exactly-once claim.

## Tasks
- [ ] T1A Add inert segment envelope identity (v3 codec, strict bindings, exact v2 bytes).
  - Route: delegated direct; triggers: preparation for writing and multiple non-trivial files.
  - Allowed source: envelope codec, existing envelope tests, concatenation docs, this task doc.
  - Prove strict bindings, legacy byte compatibility, new-version downgrade rejection, and no atom creation from wire input.
  - Segment metadata remains unused until retry transport lands.
- [ ] T1B Preserve segment identity through retries and quarantine with stable child ids.
  - Route: delegated direct; triggers: retry helper field-list replacement.
  - Allowed source: retry transport helper, work queue and worker increment delegates, retry/quarantine tests.
  - Prove same child identity with attempt increment. Not part of this identity unit.
- [ ] T2 Settle SMSC outcomes after durable journal evidence; preserve replay without a second SMPP send and implement bounded delayed safe retries.
  - Route: delegated direct; triggers: worker/journal lifecycle mapping and multi-file implementation. Exact surfaces to derive before launch.
- [ ] T3 Implement stop-first queue publication and per-segment compensation without activating HTTP.
  - Route: delegated direct; triggers: new dispatch/compensation behavior and failure/race tests. Exact surfaces to derive before launch.
- [ ] T4 Pass validated server multipart configuration through HTTP/pipeline options, without client overrides.
  - Route: delegated direct; triggers: config propagation across files. Exact surfaces to derive before launch.
- [ ] T5 Activate shared HTTP multipart path and prove UDH/SAR end-to-end PDUs, partial failures and unchanged single messages.
  - Route: delegated direct; triggers: pipeline integration and runtime proof. Exact surfaces to derive before launch.

## Verification
Default applicable deterministic test-first policy, source: orchestrator default; global strict TDD unknown. Runner: `mix test`. Observe RED, GREEN and refactor; source-mutating normalization precedes final checks/review freeze.
T1A: `mix test test/jasmin_ex/messaging/envelope_test.exs test/jasmin_ex/messaging/rabbit_mq/work_queue_test.exs test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs`.
Each unit: `mix format --check-formatted`, `mix credo --strict`, `mix test`, `mix dialyzer`; parent reruns one reported focused command. No known baseline failures. Compatibility/integration tests excluded by default must be disclosed, not treated as external provider proof.
RDD is on (global), read-only status confirmed. Each candidate needs its own native assessment/consent; prior omissions do not apply. Native approval is not publication authorization.

## Progress and next step
T1A identity codec is staged for parent commit and native review choice; checkbox stays open. T1B retry transport is follow-up and is not claimed here. T2–T5 are untouched. HTTP multipart remains inactive. Segment metadata is unused until retry transport lands. Full mirror is parent-owned.

Full T1 observed (kept as the complete T1 evidence, not an intermediate proof):
- RED: `mix test test/jasmin_ex/messaging/envelope_test.exs test/jasmin_ex/messaging/rabbit_mq/work_queue_test.exs test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs` → 79/96 passed, 17 failed (seed 241261). New segment bind, v3 wire, fail-closed v1/v2 `segment` keys, retry preservation, and worker/queue metadata preservation were missing.
- GREEN: same focused command → 96 passed in 0.6s (seed 429171).
- Artifacts for full T1: 750 insertions / 42 deletions across 7 tracked files, plus this task doc. One honest slicing pass: identity codec about 517 authored lines (accepted first-unit exception) and retry transport about 313.
- Final writer checks for full T1: focused 96 passed; formatting passed; Credo no issues; full suite 864 passed, 29 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.

T1A staged and aux-verified (checkbox still open; no commit/native consent):
- Actual identity diff: 504 insertions / 19 deletions (523 authored) for envelope codec, tests, concatenation docs, and this task doc. Accepted first-unit exception; later-unit forecasts unchanged.
- Aux `/tmp/opencode/jasmin-ex-envelope-v3-90` detached at `f89b74a`. Focused 89 passed (seed 353720); format passed; Credo no issues; full suite 857 passed, 29 excluded; Dialyzer 0 errors.
- Segment metadata is unused until retry transport lands. Native assessment/consent pending; previous native failures are not this candidate's consent.
