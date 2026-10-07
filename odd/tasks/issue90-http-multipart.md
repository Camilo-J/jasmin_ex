# Issue 90 — HTTP multipart integration

## Objective and authorized scope
Connect HTTP long-message planning to segment-aware admission, sequential queue publication and SMSC settlement, with HTTP-to-PDU proof. Existing single-message behavior stays compatible. Full multipart DLR processing, client idempotency and northbound SMPP are excluded.
User selected server-owned UDH/SAR configuration (UDH default, maximum 5 segments) and stopping publication on the first rejection or uncertain outcome. Unattempted segments recover full unit price including precharge plus one quota; queued segments await SMSC; uncertain segments never auto-refund or retransmit. HTTP full success requires every broker confirmation, not SMSC or handset delivery. Final timeout charge/reconciliation policy remains deferred.
Branch: `feat/http-multipart-90`; local HEAD `b65e476` includes the identity and retry prerequisites. PR110 and PR111 were published under explicit authorization and human-merged as `db28f1605ae4f5d7106f8f0476656be941548907` and `8f20f592edfeb787a4ed9400e34b5d72926c2af4`; CI, Redis, Dragonfly and RabbitMQ passed for both. No future remote publication is authorized.

## Delivery and safety
Default ask-on-risk, cached stacked-to-main topology. Forecast about 1590 authored lines across five independent prerequisite/activation units; T1 identity codec is an accepted first-unit size exception, not cover for later units. Later unit forecasts are unchanged. The 400-line budget is advisory for implementation, never code-golf or omitted tests; report any coherent overage before commit. Commit each verified unit with its tests/docs.
Identity precedes settlement, compensation and activation. Unique child gateway ids preserve existing journal keys; segment indexes never become retry attempts. Legacy envelopes keep version 2 and identical bytes; metadata-bearing envelopes use a new version so old readers fail closed rather than silently discard billing bindings. Broker queue acceptance must not become terminal ledger acceptance. No distributed or power-loss exactly-once claim.

## Tasks
- [x] T1A Add inert segment envelope identity (v3 codec, strict bindings, exact v2 bytes).
  - Route: delegated direct; triggers: preparation for writing and multiple non-trivial files.
  - Allowed source: envelope codec, existing envelope tests, concatenation docs, this task doc.
  - Ordinary-verified commit `a8c0576` (528 authored). Not native-approved. Last native-reviewed boundary remains `f89b74a`.
- [x] T1B Preserve segment identity through retries and quarantine with stable child ids.
  - Route: delegated direct; triggers: retry helper field-list replacement.
  - Allowed source: `Envelope.retry/1`, work queue and worker increment delegates, retry/quarantine tests.
  - Ordinary-verified commit `b65e476`, 352 authored lines; PR111 merged. Parent reran 96 focused tests; writer 864 full tests passed, 29 excluded. Native review omitted by the user, not approved; native omission closure remains unavailable.
- [x] T2A1 Extract the existing known-response checkpoint without changing legacy behavior.
  - Route: delegated direct; trigger: hunk staging and intermediate-state verification while preserving the full implementation.
  - Stage only `persist_known_response/3` and its use from `checkpoint_and_publish/5`; no segment settlement or new behavior. Existing worker checkpoint tests prove the refactor.
  - Ordinary-verified commit `00d8b94`, 9 insertions / 3 deletions. Parent focused 35 passed; auxiliary full suite 864 passed, 29 excluded; format, Credo and Dialyzer passed. Native candidate omission confirmed (`declined_this_candidate`), not an approval.
- [x] T2A2 Settle optional segment outcomes after durable journal evidence; replay settlement without another SMPP send.
  - Route: delegated direct; triggers: worker lifecycle mapping and non-trivial implementation/tests.
  - Allowed source: connector worker, its existing tests, one settlement paragraph in HTTP long-message docs.
  - Inject the settlement dependency; missing dependency fails closed. Keep production segment publication inactive until dependency wiring is proven.
  - Prove known-response persistence before settlement, settlement failure replay without resubmit, transient retries without refund, exhausted definitive nonacceptance refund, and uncertain holds.
  - User selected two deliveries and explicitly accepted a size exception only for this second delivery (about 790 authored lines). Keep all settlement outcomes with their tests; no temporary partial-settlement behavior or omitted tests. Publication and merge remain separate decisions.
  - Ordinary-verified commit `e1f9ad2`, 815 authored lines including tracking. Parent and independent verifier confirmed 48 worker tests; full suite 877 passed, 29 excluded; format, Credo and Dialyzer passed. Native candidate omission confirmed, not an approval. No publication performed.
- [ ] T2B Wire production segment settlement and prove the Router integration before HTTP activation.
  - Route: delegated direct; triggers: dependency wiring and integration tests. Exact surfaces to derive before launch.
- [ ] T2C Implement bounded delayed safe retries with the existing attempt budget.
  - Route: delegated direct; triggers: timer/queue mapping and behavior tests. Exact surfaces to derive before launch.
  - Current retries remain immediate; delay is explicitly pending. Do not block workers with sleep or automatically retransmit uncertain outcomes.
- [ ] T3 Implement stop-first queue publication and per-segment compensation without activating HTTP.
  - Route: delegated direct; triggers: new dispatch/compensation behavior and failure/race tests. Exact surfaces to derive before launch.
- [ ] T4 Pass validated server multipart configuration through HTTP/pipeline options, without client overrides.
  - Route: delegated direct; triggers: config propagation across files. Exact surfaces to derive before launch.
- [ ] T5 Activate shared HTTP multipart path and prove UDH/SAR end-to-end PDUs, partial failures and unchanged single messages.
  - Route: delegated direct; triggers: pipeline integration and runtime proof. Exact surfaces to derive before launch.

## Verification
Default applicable deterministic test-first policy, source: orchestrator default; global strict TDD unknown. Runner: `mix test`. Observe RED, GREEN and refactor; source-mutating normalization precedes final checks/review freeze.
T1A/T1B: `mix test test/jasmin_ex/messaging/envelope_test.exs test/jasmin_ex/messaging/rabbit_mq/work_queue_test.exs test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs`.
Each unit: `mix format --check-formatted`, `mix credo --strict`, `mix test`, `mix dialyzer`; parent reruns one reported focused command. No known baseline failures. Compatibility/integration tests excluded by default must be disclosed, not treated as external provider proof.
RDD is on (global), read-only status confirmed. Each candidate needs its own native assessment/consent; prior omissions do not apply. Native approval is not publication authorization.

## Progress and next step
T1A and T1B are ordinary-verified and merged upstream in PR110/111. T2A1 checkpoint extraction is committed as `00d8b94`; T2A2 settlement is committed as `e1f9ad2`. Both new deliveries are local only. Native review was omitted, not approved; last native-reviewed boundary remains `f89b74a`. T2B production wiring and T2C delayed retry remain required follow-ups. T3–T5 are untouched. HTTP multipart remains inactive. Full mirror is parent-owned.

Full T1 observed (kept as the complete T1 evidence, not an intermediate proof):
- RED: focused three-file command → 79/96 passed, 17 failed (seed 241261).
- GREEN: same command → 96 passed in 0.6s (seed 429171).
- Full writer checks: focused 96 passed; formatting passed; Credo no issues; full suite 864 passed, 29 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.

T1B historical staged proof before commit/publication:
- Staged retry transport plus tracking; expected about 313 plus tracking, actual code 323 authored (under 400, not golfed). No extra size exception.
- HEAD+staged focused 96 passed (seed 12265); format passed; Credo no issues; full suite 864 passed, 29 excluded; Dialyzer 0 errors.

T2A observed functional proof (both deliveries committed; HTTP multipart still inactive):
- RED: `mix test test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs` → 36/48 passed, 12 failed (seed 215969).
- GREEN: same command → 48 passed in 0.9s (seed 777198).
- Final after allowed-source format/credo normalization: focused 48 passed in 0.8s (seed 706420); `mix format --check-formatted` passed; `mix credo --strict` no issues; full suite 877 passed, 29 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.
- Authored 762 insertions / 17 deletions (worker 144/17, tests 616, docs 2), excluding parent-owned task-doc rebase. Accepted delivery split: about 18 lines of existing checkpoint extraction first; about 790 lines of complete settlement and proofs second, with an explicit second-delivery size exception. Counts include tracking and will be updated from actual commits.
- Production Router wiring (T2B) and delayed retry (T2C) remain inactive. No exactly-once claim.

T2A1 historical first proof (committed `00d8b94`; native omitted `declined_this_candidate`, not approved):
- Index path only: `lib/jasmin_ex/messaging/rabbit_mq/connector_worker.ex`, 9 insertions / 3 deletions, blob `7cbe9ee119691eb5bf8069030c929acfe59902fc`.
- Patch: `/tmp/opencode/jasmin-ex-checkpoint-extract-90.patch`. Aux: `/tmp/opencode/jasmin-ex-checkpoint-extract-90` detached at `b65e476` with the same hunk.
- `mix test test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs` → 35 passed in 0.6s (seed 950833).
- `mix format --check-formatted` passed; `mix credo --strict` no issues; full suite 864 passed, 29 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.
- Message: `refactor(messaging): extract durable response checkpoint`. T2A2 size exception remains explicit for the second delivery.

T2A2 historical staged proof (committed `e1f9ad2`; native omitted, not approved):
- Four paths only: connector worker, its tests, HTTP long-message docs, this task doc. Production Router wiring is inactive; HTTP multipart is inactive.
- HEAD+staged focused `mix test test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs` → 48 passed in 0.9s (seed 370529).
- `mix format --check-formatted` passed; `mix credo --strict` no issues; full suite 877 passed, 29 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.
- Explicit second-delivery size exception. Suggested message: `feat(messaging): settle segment outcomes from durable evidence`.
- Staged 794 insertions / 21 deletions (worker 135/14, tests 616, docs 2, this task doc 41/7). Observed work-unit authored so far: T1A 528, T1B 352, T2A1 12 (9/3), T2A2 815 (794/21).
