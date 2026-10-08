# Issue 90 — HTTP multipart integration

## Objective and authorized scope
Connect HTTP long-message planning to segment-aware admission, sequential queue publication and SMSC settlement, with HTTP-to-PDU proof. Existing single-message behavior stays compatible. Full multipart DLR processing, client idempotency and northbound SMPP are excluded.
User selected server-owned UDH/SAR configuration (UDH default, maximum 5 segments) and stopping publication on the first rejection or uncertain outcome. Unattempted segments recover full unit price including precharge plus one quota; queued segments await SMSC; uncertain segments never auto-refund or retransmit. HTTP full success requires every broker confirmation, not SMSC or handset delivery. Final timeout charge/reconciliation policy remains deferred.
Branch: `feat/http-multipart-90`; T2B starting HEAD `d884117` includes identity, retry and worker settlement prerequisites. PR110/111 and PR112/113 were merged under prior explicit publication authorization; PR112 merge `93df5ec` and PR113 merge `9bead825` were verified in the previous session. This session did not fetch or reverify remote state. Local production source matches publication head `430576b`; histories are parallel, so no reset/rebase/merge is needed to implement T2B locally. No future remote publication is authorized.

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
- [x] T2B Wire production segment settlement and prove the Router integration before HTTP activation.
  - Route: delegated direct; triggers: preparation for writing, dependency wiring and non-trivial integration tests.
  - Authorized surfaces: `lib/jasmin_ex/smpp/connector_supervisor.ex`, `test/jasmin_ex/smpp/connector_supervisor_test.exs`, `test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs`, `docs/http-long-messages.md`, this task document.
  - Forecast: 280–360 authored lines, one behavior work unit on the existing feature branch; cached stacked-to-main strategy remains, with no remote publication authorized.
  - Reuse `Routing.settle_segment/5`; prove production callback wiring, accepted/rejected/uncertain ledger effects, one full-unit-plus-quota refund, and durable replay after snapshot failure/restart without another SMPP send. Preserve legacy behavior and keep HTTP multipart inactive.
  - TDD: default applicable deterministic test-first policy; global strict mode unknown. RED/GREEN runner: `mix test test/jasmin_ex/smpp/connector_supervisor_test.exs test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs`. Closure: format check, Credo, full tests and Dialyzer. RDD effective global on; this candidate needs its own assessment/consent.
  - Status: ordinary-verified work-unit commit `0f221e9e83d5e1d7bbe60e6ea281a95f310fbccc` (366 additions / 13 deletions, 379 authored). Parent repeated 71 focused tests (seed 927679), independent technical spot verification repeated 71 (seed 428383). Native assessment: medium; committed range from retained native boundary `f89b74a` was due (`slice_budget_reached`), including historical omitted units. User omitted this candidate; exact provider decline returned `action: declined`, `consent: declined_this_candidate` and matching target `sha256:a0d824ff4332be1cfe1ca0d97fb316e2cb3457134e16ac8b6cec38d47d46ac82`. No native approval; review remains globally enabled. Post-decline T2B-only committed assessment from `d884117`: medium, 379 lines, ordinary self-verification plus independent spot verification complete. HTTP multipart remains inactive. T2C–T5 untouched.
- [x] T2C-1 Add inert Client/Publisher retry wait transport (direct API unused by workers).
  - Depends on merged PR114 (`bd4f188050f9f82d983d483c2ce9e186b18131d6`). Non-closing `Refs #90`.
  - Route: delegated direct; triggers: broker topology helpers and publisher regression tests.
  - User approved a fixed 5000 ms TTL; retain the existing three-attempt budget. Explicit `publish_retry/3` and `<work-prefix>-retry.<id>.wait`; no WorkQueue, worker, or production retry-delay wiring.
  - Verification: publisher/work-queue/worker unit tests; direct-API e2e wait TTL; format, Credo, full tests, Dialyzer. Production MT retries remain immediate.
  - Published: PR115 merged at `2026-10-07T23:55:14Z` as `4cfd29866729d8c4d821e94d4a2f06ee4b12e873`; required `ci`, Redis, Dragonfly and RabbitMQ all succeeded.
- [x] T2C-2 Wire WorkQueue delayed retries onto the wait primitive.
  - Depends on merged PR115 (`4cfd29866729d8c4d821e94d4a2f06ee4b12e873`). Non-closing `Refs #90`.
  - Route: delegated direct; triggers: WorkQueue retry publish/ACK and worker e2e wait activation.
  - Safe retries now delay 5000 ms via explicit `publish_retry/3` onto `<work-prefix>-retry.<id>.wait` and publisher confirm before original ACK. Max 3 unchanged. Enqueue/quarantine stay classic. Uncertain outcomes still quarantine without delay; no sleep, auto-retransmit, or refund.
  - Client/Publisher/publisher-test bytes unchanged from T2C-1. HTTP multipart remains inactive.
  - Status: ordinary-verified on aux `feat/mt-retry-delay-wire-90`; this work-unit commit; native assessment pending from retained boundary `bd4f188`; publication pending current authorization; no future pushes.
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
T1A and T1B are ordinary-verified and merged upstream in PR110/111. T2A1 checkpoint extraction is committed locally as `00d8b94` and published in merged PR112; T2A2 settlement is committed locally as `e1f9ad2` and published in merged PR113. Merge evidence is retained in the previous session handoff. Native review was omitted, not approved; last native-reviewed boundary remains `f89b74a`. T2B production wiring is ordinary-verified and committed as `0f221e9`; native review was omitted for this candidate with validated decline, not approval. PR114 merged at `bd4f188`. T2C-1 inert wait transport is published as PR115 merged at `4cfd298`. T2C-2 WorkQueue wiring is ordinary-verified on this stacked-to-main slice; native assessment and publication pending. T3–T5 are untouched. HTTP multipart remains inactive. Full mirror is parent-owned. No future pushes.

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
- Production Router wiring (T2B) is implemented locally pending parent commit; delayed retry (T2C) remains inactive. No exactly-once claim.

T2B observed functional proof (committed `0f221e9`; native review omitted for this candidate; HTTP multipart still inactive):
- RED: `mix test test/jasmin_ex/smpp/connector_supervisor_test.exs test/jasmin_ex/messaging/rabbit_mq/connector_worker_test.exs` → 64/71 passed, 7 failed (seed 772245). Failures were missing `settle_segment` / default Router on the worker child spec, Client option leak of `:router`, live `start_worker` callback nil, and production-callback helper nil; one follow-on `already_started` was leftover StubConnection from the live assertion. Not compile errors.
- GREEN: same command → 71 passed in 1.6s (seed 432490).
- Final after allowed-source format: `mix format --check-formatted` passed; `mix credo --strict` no issues; full suite 881 passed (1 doctest, 880 tests), 29 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.
- Pre-closure snapshot: 364 insertions / 12 deletions including tracking (supervisor 21/7, supervisor tests 20, worker tests 305, docs 1/1, task doc 17/4); final count comes from the work-unit commit. Forecast 280–360; under the 400 advisory, not golfed.
- Runtime proof scope: real isolated Router (`name: nil`, `tmp_dir`) plus fake broker/SMPP transport. Not external SMPP or RabbitMQ proof.
- Proof limitation: ledger tests invoke the production child-spec callback factory; the live worker test checks callback presence but does not invoke its closure against the Router. Source readback confirms both use the same factory. Uncertain snapshot-failure replay is not separately tested.
- Rollback boundary: `lib/jasmin_ex/smpp/connector_supervisor.ex` production callback wiring; supervisor and worker tests; one settlement paragraph in `docs/http-long-messages.md`; this task document. No T2C/T3/T4/T5 or HTTP activation.

T2C-1 observed (inert direct API; production MT retries remain immediate):
- RED: focused publisher/work-queue/worker tests → 71/77 passed, 6 failed (seed 113578). Failures were missing `Publisher.publish_retry/3`, not compile errors.
- GREEN: same command → 77 passed in 1.4s (seed 72733). Final focused 77 passed (seed 701188).
- Closure: `mix format --check-formatted` passed; `mix credo --strict` no issues; full suite 889 passed (1 doctest, 888 tests), 30 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.
- Runtime: local pinned `rabbitmq:4.3.4` already present; `mix test --include integration test/jasmin_ex/messaging/rabbit_mq/e2e_test.exs` → 10 passed in 96.6s (seed 827909). Direct `publish_retry` wait declare accepted, classic redeclare 406, work empty before TTL, same attempt 2 after ~5s. Exhausted worker retries stay immediate at the default timeout. HTTP remains inactive.

T2C-2 observed (WorkQueue wires wait primitive; production safe MT retries delay 5s):
- RED: focused publisher/work-queue/worker tests → 73/83 passed, 10 failed (seed 295232). Failures were immediate classic `publish` versus expected `publish_retry`, not compile errors.
- GREEN: same command → 83 passed in 1.4s (seed 819368). Final focused 83 passed (seed 956695).
- Closure: `mix format --check-formatted` passed; `mix credo --strict` no issues; full suite 895 passed (1 doctest, 894 tests), 30 excluded (`:compatibility`, `:integration`); Dialyzer 0 errors.
- Runtime: local pinned `rabbitmq:4.3.4@sha256:4b336f82e93749f1ebf8d6283b4d5a98bf1efac8412ec56015a6ab5aae0f57a2` already present; `mix test --include integration test/jasmin_ex/messaging/rabbit_mq/e2e_test.exs` → 10 passed in 107.0s (seed 908397). Exhausted worker retries wait `2 * Client.wait_queue_ttl_ms() + 5000`. Direct `publish_retry` wait API from T2C-1 remains. Independent probe (not this e2e): absent work target can leave RabbitMQ total messages 1 with ready 0; destination recreation is not proven as an immediate or eventual handoff. HTTP remains inactive. Not exactly-once.

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
