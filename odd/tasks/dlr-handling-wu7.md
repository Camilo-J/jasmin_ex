# Make DLR Handling Production-Reachable and Restart-Safe

## Delivery decision

The maintainer chose **two stacked PR slices** using `stacked-to-main`:

1. WU7-A is Slice 1 and targets `main`.
2. WU7-B waits until WU7-A is delivered, then follows after WU7-A merges and its
   branch is rebased onto the updated `main` boundary.

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
| Branch | `feat/dlr-handling-wu7` |
| Base | `main@b980d4ebd7e5f004f2e3cebc657f87e9d9812d78` |
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
| Current delivery state | WU7-A authorized; WU7-B blocked until WU7-A delivery |
| Counting rule | Count authored additions plus deletions; exclude generated artifacts only when identified explicitly, while retaining them in complete snapshot evidence. |
| Guardrail | Do not code-golf, remove tests/docs/comments, or weaken evidence to approach the heuristic. |

### Delivery slices

1. Slice 1 contains WU7-A and targets `main`.
2. Slice 2 contains WU7-B. Work on it remains blocked until Slice 1 is delivered;
   after Slice 1 merges, WU7-B follows from the updated `main` boundary without
   force or hidden history rewriting.

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

- [ ] **WU7-A — Add the optional DLR supervision and application contract (estimated 180–280 authored lines).**
- [ ] **WU7-B — Prove production E2E/restart recovery and document operations (estimated 530–840 authored lines).**

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

### Progress and evidence placeholders

| Evidence | Status | Exact observation |
|---|---|---|
| Progress | Ready | Delivery choice recorded; WU7-A implementation authorized |
| RED | Pending | Command, exit status, failing tests, and missing behavior |
| GREEN | Pending | Command, exit status, passing tests, and implemented contract |
| REFACTOR | Pending | Format/refactor command and unchanged focused result |
| Compatibility | Pending | Existing application suites and result |
| Authored lines | 0 | Update with additions plus deletions |
| Work-unit commit | Pending | Conventional commit hash and subject |

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
| Progress | Pending | Not started; blocked on delivery choice |
| RED | Pending | Focused integration command, exit status, and first missing production behavior |
| GREEN | Pending | Focused integration command, versions, counters, and completed path |
| REFACTOR | Pending | Format/refactor command and unchanged integration result |
| Restart proof | Pending | Outage/restart points, recovery observations, and terminal result |
| Documentation validation | Pending | Review/check command and verified sections |
| Full suite | Pending | `mix test` result |
| Formatter | Pending | `mix format --check-formatted` result |
| Credo | Pending | `mix credo --strict` result |
| Dialyzer | Pending | `mix dialyzer` result |
| Authored lines | 0 | Update with additions plus deletions |
| Work-unit commit | Pending | Conventional commit hash and subject |

## Verification evidence ledger

| Gate | WU7-A | WU7-B | Final feature |
|---|---|---|---|
| Focused RED | Pending | Pending | N/A; evidence belongs to each task |
| Focused GREEN | Pending | Pending | Pending |
| Focused REFACTOR | Pending | Pending | Pending |
| Runtime harness | N/A; application contract only | Pending | Pending |
| Full `mix test` | Pending compatibility result | Pending | Pending |
| Formatter | Pending | Pending | Pending |
| Credo | Pending if run for slice | Pending | Pending |
| Dialyzer | Pending if run for slice | Pending | Pending |
| Rollback reviewed | Pending | Pending | Pending |

## Work-unit commit placeholders

No commit is authorized during tracker preparation. Implementation must preserve
tests and docs with the behavior they verify.

| Work unit | Intended outcome | Commit placeholder | Review slice |
|---|---|---|---|
| WU7-A | Optional DLR supervisor and application contract | Pending: `feat(dlr): add optional application supervision` | Slice 1 targeting `main` |
| WU7-B | Production recovery proof and operator documentation | Pending: `feat(dlr): prove production recovery and operations` | Slice 2, blocked until WU7-A delivery |

Commit hashes, exact subjects, focused checks, runtime evidence or N/A rationale,
and rollback boundaries must be recorded before a task is marked complete.

## Authored-line running count

| Boundary | Forecast | Current authored additions + deletions | Notes |
|---|---:|---:|---|
| WU7-A | 180–280 | 0 | Tracker preparation is not implementation scope. |
| WU7-B | 530–840 | 0 | Includes E2E/restart proof and `docs/dlr-handling.md`. |
| Total WU7 | **710–1,120** | **0** | Update after each work-unit commit; do not optimize for the heuristic. |

## Review assessment placeholders

| Assessment | Current value |
|---|---|
| Review-load risk | High by forecast; exact assessment pending implementation |
| Review due | Pending authored-line count for WU7-A Slice 1 |
| Proposed review order | WU7-A child/config contract → readiness/retry ownership → production injection → E2E/restart proof → operator docs |
| Smallest honest boundary | WU7-A and WU7-B are the current candidates; validate after implementation |
| Native review lineage | None started; native review is explicitly out of scope for setup |
| Findings/corrections | Pending |
| Final reviewer disposition | Pending |
| Delivery exception | None; maintainer selected two stacked PR slices with `stacked-to-main` |

## Tracker progress

| Item | Status | Evidence |
|---|---|---|
| Branch synchronization | Complete | `feat/dlr-handling-wu7` created from `main@b980d4ebd7e5f004f2e3cebc657f87e9d9812d78` after fast-forward-only update from `origin/main` |
| Delegated mapping/preparation | Complete | Route and mapped facts recorded above |
| Stable top-level tasks | 2 pending | WU7-A authorized; WU7-B blocked until WU7-A delivery |
| Delivery choice | Complete | Two stacked PR slices with `stacked-to-main`; WU7-A targets `main` |
| Source changes | Not started | Explicitly excluded from setup |
| Tests | Not run | No behavior implementation authorized |
| Documentation | Not started | `docs/dlr-handling.md` belongs to WU7-B |
| Commits | None | Tracker intentionally remains uncommitted pending delivery choice |
| Remote delivery | None | No push or PR authorized |
| Native review | None | Explicitly excluded |

## Next step

Implement and deliver WU7-A as Slice 1 targeting `main`. Keep WU7-B blocked until
WU7-A merges, then rebase the WU7-B branch onto the updated `main` boundary.
