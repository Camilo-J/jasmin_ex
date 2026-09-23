# Make DLR Handling Production-Reachable and Restart-Safe

## Delivery decision

The maintainer chose **two stacked PR slices** using `stacked-to-main`:

1. WU7-A is Slice 1 and targets `main`.
2. WU7-A merged as PR #82 at `origin/main@da4c3a6631a6785269b66d8b488ab3b789c7dbb3`.
   WU7-B proceeds on local `feat/dlr-handling-wu7b` from that boundary.

The forecast is **710–1,120 authored lines**, so the 400-line review heuristic is
advisory and MUST NOT cause code-golf, compressed tests, reduced documentation, or
weakened runtime evidence.

## Feature identity

| Field | Value |
|---|---|
| Feature | DLR handling — Work Unit 7 (WU7) |
| Issue | GitHub issue #10 |
| Objective | Make the completed DLR pipeline reachable from the production application, resilient to broker unavailability and restart, and operable through accurate documentation. |
| Problem | WU1–WU6 provide DLR contracts, durable mappings and plans, receipt parsing, topic transport, lookup processing, and secure HTTP callbacks, but the application does not supervise or inject the complete production path. |
| Why now | WU6 is merged. Production wiring and restart recovery are the remaining boundary before issue #10 can claim an operational DLR path. |
| Branch | `feat/dlr-handling-wu7b` (WU7-B); WU7-A was delivered separately |
| Base | `origin/main@da4c3a6631a6785269b66d8b488ab3b789c7dbb3` (WU7-B) |
| Development route | Organic Driven Development (ODD), delegated direct |
| Tracker role | Authoritative ODD feature document for WU7 |

## Scope and constraints

### Authorized scope

WU7 may implement only the production reachability and proof boundary for existing
DLR behavior:

- An optional `JasminEx.Dlr.Supervisor` and application child wiring.
- Ordered RabbitMQ topology readiness before DLR consumers start.
- Retrying recovery when RabbitMQ is unavailable at startup or after disconnect.
- Production injection of the existing broker publisher, StateStore, connector
  receipt publication, lookup processor, HTTP thrower, Mint adapter, and callback
  destination policy.
- Production-level end-to-end tests using RabbitMQ, StateStore/Valkey, FakeSMSC,
  and the fake DLR endpoint.
- Restart proof covering durable correlation/plan state and broker recovery.
- `docs/dlr-handling.md` covering enablement, configuration, topology, retry,
  security, observability, failure handling, rollback, and verification.
- Minimal configuration or support-harness changes required by those outcomes.
- This tracker, updated only with honest implementation evidence.

### Explicitly out of scope

- New DLR domain semantics, callback fields, callback methods, or lookup levels.
- SMPP `data_sm` receipt support or an SMPP callback thrower.
- GET `/send` DLR support.
- Per-request DLR expiry overrides.
- Changes to MT routing, billing, queue semantics, or non-DLR retry policy.
- Coupling DLR receipt queues to MT work queues.
- Relaxing destination policy, TLS verification, bounded response handling, or the
  one-network-attempt HTTP client contract.
- Classic-queue fallback when quorum delayed retry is unavailable.
- Destructive migration of existing DLR queues, mappings, plans, or jobs.
- Deployment automation, release packaging, remote operations, PR creation, or
  issue closure in this work unit unless separately authorized.
- Native review during this preparation step.

## Route declaration

| Trigger | Declaration |
|---|---|
| Route | `delegated direct` |
| Mapping trigger | Active: the feature is expected to span 4+ files. |
| Writer trigger | Active: each implementation slice is expected to span 2+ non-trivial files. |
| Preparation | Delegated and complete; repository mapping established the task boundaries and risks recorded here. |
| Implementation | A single writer must preserve the stable WU7-A/WU7-B boundaries unless the maintainer authorizes a different delivery shape. |

## Strict TDD contract

| Field | Requirement |
|---|---|
| `strict_tdd` | `true` |
| Source | Engram `sdd-init/jasmin_ex` observation **#995** |
| Runner | `mix test` (ExUnit) |
| Required sequence | Observed **RED → GREEN → REFACTOR** for every new behavior |
| Evidence quality | Record exact command, exit status, test count, and concise observed cause/result. Do not reconstruct a missing RED after implementation. |

Tests and implementation for one behavior remain in the same work unit. A broad
failure caused by an unrelated environment problem is not valid RED evidence.
Runtime proof supplements TDD; it does not replace focused application or policy
tests.

## Delivery and review strategy

| Field | Decision |
|---|---|
| Delivery strategy | Two stacked PR slices |
| Chain strategy | `stacked-to-main` |
| Forecast | **710–1,120 authored additions plus deletions** |
| Review heuristic | About 400 authored changed lines per review slice, advisory |
| Maintainer choice | **WU7-A and WU7-B are separate stacked PR slices** |
| Current delivery state | WU7-A merged via PR #82; WU7-B implemented and verified locally. Its candidate at `938edad` was approved/acknowledged; the accepted reliability follow-up is locally verified, awaiting the parent session's reassessment before any PR. |
| Counting rule | Count authored additions plus deletions; exclude generated artifacts only when identified explicitly, while retaining them in complete snapshot evidence. |
| Guardrail | Do not code-golf, remove tests/docs/comments, or weaken evidence to approach the heuristic. |

### Delivery slices

1. Slice 1 contains WU7-A and targets `main`.
2. Slice 2 contains WU7-B on local `feat/dlr-handling-wu7b`, based on the merged
   Slice 1 boundary `da4c3a6631a6785269b66d8b488ab3b789c7dbb3`.

## Mapped repository facts

These facts constrain implementation and must be reverified against the current
branch before changing behavior:

1. **WU6 is not production-reachable yet.** `JasminEx.Application.children/1`
   starts StateStore, routing, messaging, SMPP connectors/server, and HTTP API,
   but no DLR supervisor or DLR consumers.
2. **Worker startup is one-shot.** `JasminEx.Dlr.Worker.init/1` calls `consume/1`
   once. If connection resolution, channel creation, QoS, or consume fails, the
   worker remains alive with `channel: nil` and receives no retry message; it can
   therefore stay idle indefinitely after an unavailable broker.
3. **Topology readiness must precede consumers.** Lookup and HTTP consumers must
   not start until the DLR topic exchange, DLX, quorum delayed-retry queues,
   bindings, and dead queue are declared successfully.
4. **Topology failure must retry without classic fallback.**
   `TopicTopology.declare/2` deliberately maps retry-queue declaration failure to
   `{:error, :delayed_retry_unsupported}`. WU7 must retry readiness or remain
   unavailable; it must not silently downgrade to classic queues.
5. **HTTP API injection is incomplete.** The HTTP API accepts `dlr_store` and
   `dlr_config`, but top-level application wiring currently passes neither.
6. **Connector receipt injection is incomplete.** SMPP connector configuration
   supports `dlr_enabled`, `dlr_expiry`, and `dlr_publisher`, but production
   application assembly does not provide the complete DLR publisher context.
7. **Known submit-response publication injection is incomplete.** The connector
   worker can publish `dlr.submit_sm_resp` through an injected DLR publisher, but
   production assembly does not establish that dependency end to end.
8. **Receipt publication injection is incomplete.** `deliver_sm` can parse and
   publish `dlr.deliver_sm` only when its connector receives a DLR publisher
   context; production assembly must supply it without altering non-DLR delivery.
9. **Configured retry values are currently inert.** `Dlr.Config` exposes lookup
   and HTTP additional attempts and delays, while `TopicTopology` hardcodes
   `10_000/3` and `30_000/4`. WU7 must define and test the production mapping so
   configured values actually govern declared topology.
10. **Existing production adapters are reusable.** StateStore uses Redix, broker
    transport uses RabbitMQ, SMPP tests have FakeSMSC, and WU6 provides a fake DLR
    endpoint plus the Mint HTTP adapter. WU7 should assemble these contracts, not
    replace them.

## Feature acceptance criteria

WU7 is acceptable only when all of the following are demonstrated:

1. DLR disabled leaves the application child graph and existing non-DLR behavior
   unchanged.
2. DLR enabled starts one optional DLR supervision boundary with deterministic,
   inspectable children and no duplicate registered processes.
3. The DLR topology becomes ready before lookup or HTTP consumers subscribe.
4. Broker unavailability at startup does not crash the whole application and does
   not leave the DLR path permanently idle; readiness/consumption retries with
   bounded backoff and recovers when the broker returns.
5. Delayed-retry quorum topology failure remains explicit and never falls back to
   classic queues.
6. `Dlr.Config` retry delay and attempt values govern the declared lookup and HTTP
   queue arguments rather than remaining inert.
7. HTTP intake persists DLR correlation through the production StateStore and
   publishes MT work only through existing boundaries.
8. A known `submit_sm_resp` and a parsed `deliver_sm` receipt can enter the DLR
   topic path through production connector dependencies.
9. Lookup processing uses the production StateStore and publisher; HTTP callback
   processing uses the approved destination policy and Mint adapter.
10. The broker/StateStore/FakeSMSC/fake-endpoint scenario completes an actual DLR
    callback with the expected compatible payload and terminal settlement.
11. Restart proof demonstrates recovery without losing required durable state,
    duplicating avoidable work, or leaving consumers permanently absent.
12. Readiness, retry, duplicate-delivery, publication uncertainty, and external
    dependency risks are documented honestly.
13. `docs/dlr-handling.md` gives operators an actionable enablement, verification,
    rollback, and troubleshooting path.
14. Focused tests, integration tests, full tests, formatter, Credo, and Dialyzer
    pass with exact evidence recorded here.

## Stable top-level ODD tasks

- [x] **WU7-A — Add the optional DLR supervision and application contract (estimated 180–280 authored lines).**
- [x] **WU7-B — Prove production E2E/restart recovery and document operations (estimated 530–840 authored lines).**

The two identifiers above are stable. Do not split them into additional top-level
ODD tasks merely to improve line-count optics.

## WU7-A — Optional DLR supervision/application contract

### Objective

Create the smallest production assembly boundary that makes DLR enablement
explicit, optional, correctly ordered, and testable without changing WU1–WU6
domain behavior.

### Required RED application tests

Before source implementation, add focused application tests that fail because the
contract does not yet exist. They must cover at least:

1. Disabled DLR contributes no DLR supervisor child.
2. Enabled DLR contributes exactly one `JasminEx.Dlr.Supervisor` child.
3. Application options pass validated `Dlr.Config`, StateStore, broker connection
   and publisher dependencies, and only explicit test overrides.
4. Child ordering or an equivalent readiness gate prevents worker consumption
   before topology readiness.
5. Invalid enabled configuration fails deterministically rather than partially
   starting consumers.

### GREEN implementation boundary

- Add `JasminEx.Dlr.Supervisor` with a narrow child contract.
- Wire it through `JasminEx.Application.children/1` and `start/2` only when DLR is
  enabled.
- Reuse existing StateStore and RabbitMQ process identities instead of creating
  accidental duplicate connection pools.
- Keep processor/client/publisher overrides injectable for deterministic tests,
  while production defaults use existing concrete adapters.
- Establish topology readiness before consumers without implementing silent
  classic fallback.
- Keep reconnect/retry mechanics that require runtime proof scoped to WU7-B.

### Acceptance criteria

1. Focused tests observe an honest missing-contract RED.
2. Disabled and enabled child graphs are deterministic.
3. `Dlr.Supervisor` is optional and has an independent rollback boundary.
4. Required production dependencies are explicit and do not rely on test-only
   process dictionary or ambient global state.
5. Consumer startup cannot race ahead of successful topology declaration.
6. Existing application tests and non-DLR child wiring remain green.

### Authorized likely files

- `lib/jasmin_ex/application.ex`
- `lib/jasmin_ex/dlr/supervisor.ex`
- `lib/jasmin_ex/dlr/config.ex` only if application assembly requires a narrowly
  validated option or conversion
- `test/jasmin_ex/dlr/application_test.exs`
- Existing `test/jasmin_ex/*/application_test.exs` only for compatibility
  assertions directly affected by the child graph
- `odd/tasks/dlr-handling-wu7.md` for evidence only

Any additional source or test file requires a recorded scope reason before edit.

### Exact focused check

```bash
mix test test/jasmin_ex/dlr/application_test.exs
```

### Runtime evidence

`N/A` for WU7-A because this slice defines the optional child graph and dependency
contract under injected boundaries. Real RabbitMQ, StateStore, SMPP, HTTP, broker
loss, and restart behavior belong to WU7-B. WU7-A must not claim runtime readiness.

### Rollback boundary

Remove `JasminEx.Dlr.Supervisor`, its focused application tests, and only the DLR
child/configuration branch in `JasminEx.Application`. Preserve every WU1–WU6 DLR
module, persisted value, broker topology definition, HTTP API behavior, connector
behavior, and non-DLR application child.

### Route

`delegated direct`; mapping and writer triggers are active, and delegated
preparation is complete.

### WU7-A progress and evidence

| Evidence | Status | Exact observation |
|---|---|---|
| Progress | Complete | Optional DLR root supervision/config assembly implemented; WU6 workers and runtime adapters remain dormant for WU7-B. |
| RED | Observed | `mix test test/jasmin_ex/dlr/application_test.exs test/jasmin_ex/http_api/application_test.exs` exited 2 with 3/8 passing and 5 failures: no DLR child, no enabled dependency validation, and no HTTP DLR injection. A compatibility strengthening RED exited 2 with 7/8 passing because disabled DLR added nil HTTP options. |
| GREEN | Passed | The same focused command exited 0 with 8/8 passing after optional child assembly, dependency validation, ordering, and conditional HTTP injection were implemented. |
| REFACTOR | Passed | `mix format` followed by the same focused command exited 0 with 8/8 passing; after the Credo alias-order correction, the focused command again exited 0 with 8/8 passing. |
| Compatibility | Passed | `mix test` exited 0 with 684 passing (1 doctest, 683 tests) and 26 excluded. |
| Formatter | Passed | `mix format --check-formatted` exited 0 with no output. |
| Credo | Passed after correction | The first `mix credo --strict` found one alias-order readability issue and exited 4; after reordering aliases, the required command exited 0 across 177 files with no issues. |
| Dialyzer | Passed | `mix dialyzer` exited 0 with 0 errors, 0 skipped, and 0 unnecessary skips. |
| Runtime harness | N/A | WU7-A proves child-spec/config assembly only. Real topology readiness, workers, adapters, connection retry, and restart behavior belong to WU7-B. |
| Route/trigger evidence | Complete | `delegated direct`; mapping and writer triggers remained active across four implementation/test files plus this tracker. |
| Rollback boundary | Reviewed | Remove `lib/jasmin_ex/dlr/supervisor.ex`, `test/jasmin_ex/dlr/application_test.exs`, the WU7-A assertions in `test/jasmin_ex/http_api/application_test.exs`, and only the DLR branches in `lib/jasmin_ex/application.ex`; preserve WU1–WU6 and all non-DLR children. |
| Authored lines | 306 | 268 additions plus 38 deletions in this implementation commit, including tracker evidence; no generated artifacts. The estimate was exceeded without code-golf and remains below the 400-line review heuristic. |
| Work-unit commit | Complete | `da4c3a6631a6785269b66d8b488ab3b789c7dbb3` — `feat(dlr): add optional application supervision (#82)` (squash merge) |

## WU7-B — Production E2E/restart recovery and operator documentation

### Objective

Complete and prove the production DLR path across broker, StateStore, connector,
lookup worker, secure HTTP thrower, application restart, and operator workflow.

### Required RED integration boundary

Before implementation, add a production-shaped integration scenario that fails at
the first missing WU7 behavior rather than bypassing application assembly. The path
must include:

1. RabbitMQ through the repository broker harness.
2. StateStore/Valkey through the existing StateStore harness or an equivalent
   production Redix boundary.
3. FakeSMSC for `submit_sm_resp` and/or `deliver_sm` receipt traffic.
4. The fake DLR endpoint for actual HTTP callback observation.
5. The production application/DLR supervision path rather than directly starting
   isolated WU6 workers as the final proof.

### GREEN implementation boundary

- Make DLR topology declaration consume validated `Dlr.Config` retry values.
- Add a readiness/retry owner that declares topology before consumers and retries
  after broker startup failure or disconnect.
- Ensure workers that initially cannot consume do not remain silently idle.
- Inject the production StateStore and RabbitMQ publisher into HTTP intake,
  connector known-response publication, receipt publication, lookup processing,
  and HTTP callback processing.
- Use the existing `LookupPlan`, `HttpThrower`, destination policy, and Mint adapter
  contracts; do not duplicate their logic in the supervisor.
- Add only the readiness telemetry or inspection surface needed to prove and
  operate recovery.
- Keep retries broker-owned and preserve at-least-once behavior.

### Restart proof

The integration evidence must stop and restore the relevant process/application
and at least one external dependency boundary. It must prove:

1. Durable request/reverse-map/lookup-plan state survives the selected restart.
2. Topology is safely redeclared and consumers return after RabbitMQ recovery.
3. A worker that saw the broker unavailable at startup eventually consumes.
4. The expected callback reaches the fake endpoint after recovery.
5. Terminal settlement occurs and no permanently idle or duplicate consumer set
   remains.
6. Any unavoidable at-least-once duplicate window is measured and documented,
   never disguised as exactly-once delivery.

### Production risks that require explicit evidence

| Risk | Required treatment |
|---|---|
| Production adapters | Prove Redix, RabbitMQ client/publisher, Mint, and connector paths are used; test doubles may observe but must not replace the final boundary. |
| Readiness race | Show zero DLR consumers before successful topology readiness and expected consumers afterward. |
| Startup broker outage | Show retry and eventual recovery without restarting the entire release manually. |
| Mid-run broker restart | Show topology/consumer recovery and terminal processing after restoration. |
| Delayed retry support | Show unsupported quorum delayed retry stays explicit; no classic fallback. |
| Retry configuration | Assert queue arguments reflect `Dlr.Config` lookup/HTTP delays and attempts. |
| Publication uncertainty | Preserve at-least-once semantics and document duplicate-safe phases. |
| StateStore outage/recovery | Record whether processing retries, remains unavailable, or terminalizes according to existing contracts; do not silently lose correlation. |
| Endpoint failure | Prove broker-owned HTTP retry and terminal budget with actual request counts. |

### Documentation requirement

Create `docs/dlr-handling.md` in the same work unit. It must cover:

- What the DLR pipeline does and which receipt paths are supported.
- Prerequisites and explicit enablement.
- Every production configuration field and default.
- RabbitMQ exchange, DLX, queues, bindings, quorum delayed retry, and dead-letter
  ownership.
- StateStore durability and expiry responsibilities.
- HTTP callback methods, compatible fields, ACK contract, destination security,
  TLS identity, timeouts, body/header bounds, and non-followed redirects.
- Startup readiness, dependency recovery, retry budgets, and at-least-once caveats.
- Telemetry/logging/inspection signals available to operators.
- Verification commands and expected healthy observations.
- Failure symptoms and troubleshooting for broker, StateStore, SMPP receipt, and
  callback endpoint problems.
- Safe disablement and rollback without deleting durable work blindly.
- Known limitations and the explicit out-of-scope list.

### Acceptance criteria

1. A production-shaped path accepts a DLR-enabled request, persists correlation,
   receives SMSC evidence, publishes and consumes the appropriate DLR event,
   resolves durable state, and sends the compatible callback.
2. Actual broker queues and actual HTTP requests are observed; a pure fake-only
   test is insufficient.
3. Startup with RabbitMQ unavailable recovers after RabbitMQ becomes available.
4. Broker restart reestablishes topology and exactly the expected consumer set.
5. State required for replay survives the selected process/application restart.
6. Configured lookup and HTTP retry values appear in declared queue arguments.
7. Classic fallback is absent and unsupported delayed retry is observable.
8. Existing MT, routing, HTTP API, SMPP, and DLR focused tests remain green.
9. Operator documentation matches verified production behavior and commands.
10. Runtime evidence records dependency versions, scenario, counters, dispositions,
    restart points, and exit status.

### Authorized likely file scopes

Production and topology:

- `lib/jasmin_ex/dlr/supervisor.ex`
- `lib/jasmin_ex/dlr/worker.ex`
- `lib/jasmin_ex/dlr/config.ex`
- `lib/jasmin_ex/messaging/rabbit_mq/topic_topology.ex`
- `lib/jasmin_ex/application.ex`
- `lib/jasmin_ex/http_api/supervisor.ex` and/or its application wiring point
- `lib/jasmin_ex/smpp/connector_supervisor.ex` and only the connector configuration
  path needed for DLR publication injection

Tests and support:

- `test/jasmin_ex/dlr/e2e_test.exs`
- `test/jasmin_ex/dlr/application_test.exs`
- `test/jasmin_ex/messaging/rabbit_mq/topic_topology_test.exs`
- Existing RabbitMQ, StateStore, FakeSMSC, and fake DLR endpoint harness files only
  when a production-shaped proof cannot be expressed through their current APIs
- Focused compatibility tests for HTTP API and connector injection

Documentation and evidence:

- `docs/dlr-handling.md`
- `odd/tasks/dlr-handling-wu7.md`

Before editing any unlisted file, record why the existing boundary is insufficient
and how the extra file remains inside WU7 scope.

WU7-B scope extensions recorded before edits: `test/jasmin_ex/dlr/worker_test.exs`
is needed to isolate the currently one-shot startup-consume retry without treating
a Docker outage as a unit-test RED. `lib/jasmin_ex/dlr/retry_policy.ex` and its focused
test file may be needed because the current fixed runtime settlement budget would
disagree with newly configured durable queue delivery limits; this is DLR-only retry
consistency, not a change to MT queue behavior. No other unlisted file is authorized
by this note.

Additional WU7-B harness scope reason: `test/support/rabbit_mq_harness.ex` needs
non-destructive service stop/start helpers to prove startup unavailability while
preserving the same published port, durable queue volume, and existing Compose
project. `restart!/1` cannot leave the broker unavailable while starting the
application, and `stop!/1` destroys volumes; the extension remains test-only.

Additional production scope reason before edit: `lib/jasmin_ex/messaging/rabbit_mq/topic_publisher.ex`
owns the DLR topic publish channel. OTP supervisor shutdown without trapping exits
leaves that AMQP channel alive after DLR subtree restart, as the worker-channel
RED showed for the same ownership pattern. Closing only this DLR-owned publisher
channel is necessary to prevent orphan channels/consumer ambiguity; MT publisher
and MT work-queue semantics are unchanged.

### WU7-B accepted reliability follow-up (locally verified)

The candidate at `938edad` was approved and acknowledged under native review
lineage `review-e1989d51d986e786`. The maintainer accepted only these three
nonblocking warnings as a local correction before PR; the separate known-response
publisher test suggestion is deferred. This does not reopen WU7-A or invalidate
the original WU7-B implementation evidence. Do not bind or change the old review.

- [x] Close a readiness AMQP channel if topology declaration exits after opening it;
      retries must not accumulate orphan channels.
- [x] Classify transient declaration exits as broker/transient errors, while
      preserving explicit inequivalent-argument and unsupported quorum delayed-retry
      errors, with no classic fallback or queue deletion.
- [x] Verify DLR topic publisher shutdown when its channel is nil. The existing
      `close(%{channel: nil})` clause already avoids `ch.pid`; the regression test
      passed before source changes, so no publisher source change was justified.
- [x] Observe focused RED for changed behavior, then GREEN and REFACTOR; run the exact
      application, topology, publisher, E2E, full suite, formatter, Credo, Dialyzer,
      and `git diff --check` gates before committing the correction.

The focused publisher test file is necessary to reproduce shutdown with no channel;
it is within the previously recorded DLR publisher ownership extension. Rollback
only this correction's DLR readiness, classification, publisher-close changes,
focused tests, and tracker evidence; preserve all existing `.v1` queues and state.

Follow-up TDD evidence: topology test RED `mix test
test/jasmin_ex/messaging/rabbit_mq/topic_topology_test.exs`, exit 2, 7/8 passed,
4 excluded: `:disconnected` was incorrectly classified as
`:delayed_retry_unsupported`. Readiness test RED `mix test
test/jasmin_ex/dlr/application_test.exs`, exit 2, 7/8 passed: the new isolated
failure-path test could not use the injected client (production hardcoded
`Client.open_channel/1`), before the channel-close behavior could be observed.
After narrow client injection and closing on the post-open worker-startup exit,
both declaration-exit and worker-startup-exit tests confirmed channel closure.
Publisher baseline GREEN `mix test
test/jasmin_ex/messaging/rabbit_mq/topic_publisher_test.exs`, exit 0, 7 passed,
including shutdown with a nil channel after connection loss. No RED is claimed
for that pre-existing safe behavior. `mix format` ran before final checks.

| Follow-up gate | Observed result |
|---|---|
| `mix test test/jasmin_ex/dlr/application_test.exs` | Exit 0; 9 passed. |
| `mix test test/jasmin_ex/messaging/rabbit_mq/topic_topology_test.exs` | Exit 0; 8 passed, 4 integration excluded. |
| `mix test test/jasmin_ex/messaging/rabbit_mq/topic_publisher_test.exs` | Exit 0; 7 passed. |
| `mix test --only integration test/jasmin_ex/dlr/e2e_test.exs` | Exit 0; 2 passed, broker/StateStore/FakeSMSC/endpoint recovery scenario. |
| `mix test` | Exit 0; 695 passed (1 doctest, 694 tests), 29 excluded. |
| `mix format --check-formatted` | Exit 0; no output. |
| `mix credo --strict` | Exit 0; 178 files, 2515 mods/funs, no issues. |
| `mix dialyzer` | Exit 0; 0 errors, 0 skipped, 0 unnecessary skips. |
| `git diff --check` | Exit 0; no output. |

### Focused integration command

```bash
mix test --only integration test/jasmin_ex/dlr/e2e_test.exs
```

### Full verification gates

```bash
mix test test/jasmin_ex/dlr/application_test.exs
mix test test/jasmin_ex/messaging/rabbit_mq/topic_topology_test.exs
mix test --only integration test/jasmin_ex/dlr/e2e_test.exs
mix test
mix format --check-formatted
mix credo --strict
mix dialyzer
```

### Runtime evidence requirements

Record all of the following, with no placeholder left at task closure:

- Exact integration command and exit status.
- RabbitMQ and StateStore/Valkey versions.
- DLR configuration values used by the scenario.
- Queue names, queue types, retry arguments, and consumer counts before readiness,
  after readiness, during outage, and after recovery.
- FakeSMSC script and observed PDU sequence.
- Correlation, lookup-plan, and callback phase evidence without exposing secrets.
- Fake endpoint script, exact HTTP request count, callback method/fields, status,
  ACK body, and terminal disposition.
- Process/application and dependency restart points.
- Retry/redelivery counters and any observed duplicate window.
- Confirmation that classic fallback did not occur.
- Concise proof that no worker remained permanently idle.

### Rollback boundary

Disable DLR first, stop DLR consumers, and preserve queued jobs and StateStore data
until they are drained, quarantined, expired, or handled by explicit operator
policy. Revert WU7 production injection, readiness/retry ownership, topology config
mapping, E2E tests, and `docs/dlr-handling.md` together. Do not remove WU1–WU6
domain/transport modules, delete durable data, weaken destination policy, or alter
MT queues as part of WU7 rollback.

### Route

`delegated direct`; mapping and writer triggers are active, and delegated
preparation is complete.

### Progress and evidence placeholders

| Evidence | Status | Exact observation |
|---|---|---|
| Progress | WU7-B complete locally; remote delivery not requested | Production path, startup outage, broker and DLR-subtree restart, Valkey outage, broker HTTP retry/exhaustion, queue mismatch preservation, and operator documentation are locally exercised. All full verification gates passed before work-unit commit `aabe81330f55b029c2a720a265d6b0f5dd527f5c`. No push, PR, or native review performed. |
| RED | Observed for production path | `mix test --only integration test/jasmin_ex/dlr/e2e_test.exs` exited 2, 0/1 passing (seed 824925): application-assembled root started real RabbitMQ and Valkey, POST `/send` returned 200, `DlrMap.fetch_request` found production Redix correlation, and FakeSMSC emitted a `submit_sm` PDU after MT broker delivery. The 8-second assertion for actual fake-endpoint request plus settled HTTP broker queue returned false; current `Dlr.Supervisor` still has no children, so no DLR event consumer or callback can progress. The test-only loopback allow rule is explicit in the test; production injection of it remains pending. This supersedes the earlier scaffold-only 0/1 failure (not valid RED) and the intermediate fixture failures (incorrect Application alias, then undeclared MT test queue). Separately `mix test test/jasmin_ex/messaging/rabbit_mq/topic_topology_test.exs` exited 2, 6/7 passing, 3 excluded: configured 125ms delay was declared as 10000ms. |
| GREEN | Observed | Callback integration first passed 1/1 after production wiring. Focused application/worker/topology tests passed 22/22, 3 excluded. Configured worker retry RED was 7/8; GREEN 8/8. Orphan-channel shutdown RED was 8/9; GREEN 9/9. Failed QoS channel-close RED was 9/10; GREEN 10/10. HTTP timeout injection RED was 1/2; GREEN 2/2. |
| REFACTOR | Observed | `mix format` after changes; `mix credo --strict` passed (178 files, no issues); application 7/7, topology 8/8 (4 integration excluded), worker 10/10. |
| Restart and outage proof | Observed | Integration 2/2 passed (seed 270012): broker stopped before application start → readiness `:disconnected`, zero DLR worker children → same port restored → ready and one consumer per DLR work queue. Full path restarted broker then DLR subtree, checked old channels dead, persisted request and reverse maps still present, fresh consumers and receipt callback. During Valkey stop, an actual lookup `:retry` settlement telemetry event preceded Valkey restart and callback; reverse map survived. |
| Documentation validation | Complete | `docs/dlr-handling.md` covers enablement, all DLR config defaults, broker inspection, security, failure handling, duplicate caveat, StateStore persistence caveat and non-destructive rollback; reviewed against implementation and the successful 2/2 E2E run. |
| Full suite | Passed after runtime changes | `mix test` exited 0: 692 passed (1 doctest, 691 tests), 29 excluded; integration is excluded and must be run separately. |
| Formatter | Passed after final assertion adjustment | `mix format && ... && mix format --check-formatted` exited 0; final tracker edits are Markdown only. |
| Credo | Passed after runtime changes | `mix credo --strict` exited 0: 178 files, 2509 modules/functions, no issues. |
| Dialyzer | Passed after runtime changes | `mix dialyzer` exited 0: 0 errors, 0 skipped, 0 unnecessary skips. |
| Authored lines | Over advisory 400-line heuristic | Immediately before this tracker edit: 763 tracked additions/deletions plus 686 untracked E2E/docs lines = 1449. Runtime test and docs retained; no code-golf or automatic scope split. Recount before commit. |
| Work-unit commit | Complete | `aabe81330f55b029c2a720a265d6b0f5dd527f5c` — `feat(dlr): prove production callback and recovery`; 14 files, 1368 additions and 81 deletions, including tests and docs. This tracker identity is recorded in a separate local evidence-only commit because a commit cannot include its own hash. |

Runtime profile: pinned `rabbitmq:4.3.4` and `valkey/valkey:9.1.1` (environment overrides unset); E2E DLR options `queue_prefix=jasmin_ex.dlr.e2e.<unique>`, `http_delay_ms=125`, `http_timeout_ms=1234`, lookup delay 10000, lookup additional attempts 2, HTTP additional attempts 3, expiry 86400 seconds. Queues were `<prefix>.lookup.v1`, `<prefix>.http.v1`, `<prefix>.dead.v1`, all quorum with no classic fallback. Lookup/HTTP arguments include `x-delayed-retry-type=all`, delay min=max 10000/125 ms, delivery limits 3/4, dead-letter strategy `at-least-once`; focused declaration assertions cover configurable values. A separate actual-broker mismatch test published one durable message, attempted a conflicting delay declaration, received an inequivalent-argument error, and found the original queue with that one message intact (`mix test --only integration test/jasmin_ex/messaging/rabbit_mq/topic_topology_test.exs`, exit 0, 4 passed, 8 excluded; seed 886737).

FakeSMSC observed `bind_transceiver`, three `submit_sm` requests for level-1 success/retry/exhaustion, a fourth `submit_sm` for level 2 before broker restart, and `deliver_sm_resp` after the receipt; scripted receipt `id:fake-msg-id ... stat:DELIVRD` resolved the stored reverse map. The fake endpoint observed eight POST `/dlr` requests: one `ACK/Jasmin` success, two for one 500→ACK broker retry with identical encoded body, four HTTP 500 responses for the exhausted job (one dead-letter, no HTTP queue messages), and one level-2 `DELIVRD` callback with `ACK/Jasmin` after broker and Valkey recovery. Request/reverse maps and `LookupPlan` completion were read through Redix. The repeated retry callback proves at-least-once behavior; publish-before-plan-checkpoint remains a possible duplicate window, not an exactly-once claim. Consumers were absent before startup readiness and one per queue after ready and after broker recovery; queue inspection was unavailable while the broker was stopped. Valkey outage produced lookup retry telemetry while readiness remained true. The DLR subtree was also terminated and restarted twice; old publisher, topology and lookup channels were verified closed. No permanently idle worker or second consumer set was observed. A callback can arrive before lookup cleanup, so the final correlation deletion is polled rather than assumed instantaneous.

## Verification evidence ledger

| Gate | WU7-A | WU7-B | Final feature |
|---|---|---|---|
| Focused RED | Observed: exit 2, 3/8 passed and 5 failed; compatibility strengthening RED exit 2, 7/8 passed | Production E2E, topology arguments, injection, failed consume, configured budget, orphan channel and QoS setup failures observed RED | N/A; evidence belongs to each task |
| Focused GREEN | Passed: exit 0, 8/8 | Focused application 7, topology 8, worker 10 passed; actual broker topology 4 passed | Passed for both work units |
| Focused REFACTOR | Passed: exit 0, 8/8 | Formatter/Credo passed and focused tests rerun | Passed |
| Runtime harness | N/A; application contract only | Real RabbitMQ/Valkey/FakeSMSC/endpoint E2E exit 0, 2 passed (seed 418641), broker topology exit 0, 4 passed (seed 886737) | Passed |
| Full `mix test` | Passed: 684 tests, 26 excluded | Exit 0, 692 passed, 29 excluded (seed 418392) | Passed |
| Formatter | Passed | `mix format` then `mix format --check-formatted`, exit 0 | Passed |
| Credo | Passed after correcting one WU7-A alias-order issue | Exit 0, 178 files, no issues | Passed |
| Dialyzer | Passed: 0 errors | Exit 0, 0 errors | Passed |
| Rollback reviewed | Complete | Non-destructive DLR-only rollback boundary above and in docs | Complete |

## Work-unit commit placeholders

No commit is authorized during tracker preparation. Implementation must preserve
tests and docs with the behavior they verify.

| Work unit | Intended outcome | Commit placeholder | Review slice |
|---|---|---|---|
| WU7-A | Optional DLR supervisor and application contract | `da4c3a6`: `feat(dlr): add optional application supervision (#82)` | Slice 1 merged into `main` |
| WU7-B | Production recovery proof and operator documentation | `aabe81330f55b029c2a720a265d6b0f5dd527f5c`: `feat(dlr): prove production callback and recovery` | Slice 2 on local `feat/dlr-handling-wu7b`; no PR authorized |

Commit hashes, exact subjects, focused checks, runtime evidence or N/A rationale,
and rollback boundaries must be recorded before a task is marked complete.

## Authored-line running count

| Boundary | Forecast | Current authored additions + deletions | Notes |
|---|---:|---:|---|
| WU7-A | 180–280 | 306 | 268 additions plus 38 deletions, including tracker evidence; coherent scope retained without code-golf. |
| WU7-B | 530–840 | 1449 in work-unit commit `aabe813` | Real outage/restart scenarios and operator docs exceeded forecast; 400-line heuristic is advisory, do not code-golf. |
| Total WU7 | **710–1,120** | **306 merged + 1449 WU7-B work-unit** | Evidence-only tracker closure commit does not change runtime scope. |

## Review assessment placeholders

| Assessment | Current value |
|---|---|
| Review-load risk | WU7-A is 306 authored lines and below the 400-line heuristic; broader WU7 remains high by forecast |
| Review due | WU7-A Slice 1 merged after native Claude review; WU7-B candidate `938edad` approved/acknowledged under `review-e1989d51d986e786`. Accepted local reliability follow-up verified; later reassessment belongs to the parent. |
| Proposed review order | WU7-A child/config contract → readiness/retry ownership → production injection → E2E/restart proof → operator docs |
| Smallest honest boundary | WU7-A child/config assembly is merged; WU7-B runtime/recovery/docs is one coherent but over-heuristic unit, accepted without splitting tests/docs away from behavior |
| Native review lineage | WU7-A approved/acknowledged; WU7-B `review-e1989d51d986e786` approved/acknowledged at candidate `938edad`. Do not reuse old authority for the correction. |
| Findings/corrections | Three nonblocking warnings accepted for local correction: readiness channel leak on exit, overbroad topology exit classification, and publisher nil-channel shutdown. Known-response publisher test suggestion deferred. |
| Final reviewer disposition | WU7-B reviewed candidate approved/acknowledged; correction pending fresh parent reassessment. |
| Delivery exception | No delivery exception granted or requested; WU7-B exceeds the advisory 400-line review heuristic (1449 authored lines) and remains local. Any future PR sizing decision needs separate authorization. |

## Tracker progress

| Item | Status | Evidence |
|---|---|---|
| Branch synchronization | Complete | Local `feat/dlr-handling-wu7b` at merged `origin/main@da4c3a6631a6785269b66d8b488ab3b789c7dbb3`; published WU7-A branch untouched |
| Delegated mapping/preparation | Complete | Route and mapped facts recorded above |
| Stable top-level tasks | 2 implemented locally, 1 merged | WU7-A merged as PR #82; WU7-B committed locally and not delivered remotely |
| Delivery choice | Complete | Two stacked PR slices with `stacked-to-main`; WU7-A targets `main` |
| Source changes | WU7-B implemented and committed locally | Readiness/topology retry, channel cleanup, connector/HTTP injection and broker-owned settlement in `aabe813`; tracker evidence in `914e5d3` |
| Tests | WU7-B final gates passed | E2E 2 passed with broker/Valkey outages and terminal budgets, actual broker topology 4 passed, regular suite 692 passed / 29 excluded; formatter/Credo/Dialyzer/`git diff --check` exit 0 |
| Documentation | Complete and committed locally | `docs/dlr-handling.md` verified against implementation and integration observations in `aabe813` |
| Commits | WU7-A merged; WU7-B local work-unit committed | WU7-A included in `origin/main@da4c3a6`; WU7-B `aabe81330f55b029c2a720a265d6b0f5dd527f5c`, plus local tracker-evidence closure commit |
| Remote delivery | WU7-A PR #82 merged | Four checks succeeded; no WU7-B push or PR authorized |
| Native review | WU7-A and WU7-B candidate approved/acknowledged | WU7-B review lineage `review-e1989d51d986e786` reported three nonblocking warnings; accepted local correction verified, no new review in this task |

## Next step

Local WU7-B is implemented and verified at the reviewed boundary. The accepted
reliability follow-up is locally verified; the publisher nil-channel path was
already safe and is now regression-tested. Commit the correction locally before
parent reassessment.
Preserve existing `.v1` queues. Push, PR creation, native review, and any
delivery-strategy exception require separate authorization; none is included here.
