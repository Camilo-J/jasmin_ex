# Deliver DLR Lookup and Secure HTTP Callbacks

## Feature identity

| Field | Value |
|---|---|
| Feature | DLR handling — Work Unit 6 (WU6) |
| Objective | Convert durable DLR events into replayable lookup plans and deliver compatible HTTP callbacks through a bounded, SSRF-resistant client. |
| Problem | WU1–WU5 can register DLR correlation, publish known submit responses, parse receipts, and dispatch durable events, but no lookup processor or HTTP thrower currently completes the callback path. |
| Why now | WU6 is the next cohesive slice on top of merged WU5 and is required before final application wiring and end-to-end rollout work. |
| Branch | `feat/dlr-handling-wu6` |
| Base | `main@49f49f1` |
| Development route | Organic Driven Development (ODD); prior Engram SDD artifacts are historical requirements references only. |

## Scope and constraints

### Authorized scope

Implement WU6 locally on `feat/dlr-handling-wu6` only:

- Pure lookup policy for submit-response and receipt events.
- Durable, replayable `LookupPlan` processing integrated with the existing DLR worker boundary.
- Versioned `HttpJob` payloads and HTTP thrower ACK, terminal, and retry semantics.
- Destination validation, DNS/IP policy, original-host TLS verification with a pinned peer, and a bounded direct Mint HTTP/1 adapter. The completed OTP `:httpc` adapter remains historical WU6 evidence until the authorized migration below replaces it.
- A local fake DLR endpoint and a broker-backed retry harness sufficient to verify WU6.
- A narrow production assembly change only if it is required to make WU6 reachable and testable. Any such change must be recorded as a WU6 reachability decision; do not silently absorb Phase 7's full application wiring.

No push, pull request, merge, or remote access is authorized. Do not add an SMPP thrower, inbound `data_sm` handling, GET `/send` DLR support, per-request expiry, or any coupling between DLR receipts and MT work queues.

### Route declaration

| Trigger | Declaration |
|---|---|
| Route | `delegated direct` |
| Mapping trigger | Active: expected change spans 4+ files. |
| Writer trigger | Active: expected change spans 2+ non-trivial files. |
| Preparation | Delegated to explore; the mapper result and historical tasks 6.1–6.12 define the implementation boundary below. |
| Implementation | Remains in this single writer thread; do not delegate implementation. |

### TDD mode

- `strict_tdd: true`
- Source: Engram observation `sdd-init/jasmin_ex` **#995**.
- Runner: `mix test` (ExUnit).
- Every behavior must have observed **RED → GREEN → REFACTOR** evidence. A test written after implementation, an unobserved failure, or a permanently broad red suite does not satisfy this contract.
- Record the exact focused command and outcome for each behavior before closing its task.

### Delivery and review strategy

| Field | Decision |
|---|---|
| Delivery strategy | `ask-on-risk` |
| Existing chain strategy | `stacked-to-main` |
| Base boundary | `main@49f49f1` |
| Review heuristic | 400 authored changed lines, additions plus deletions, advisory rather than a code-size target |
| Forecast | WU6 is expected to be oversized but cohesive because lookup durability, callback semantics, and outbound security form one end-to-end safety boundary. |
| Guardrail | Do not shrink lines artificially by compressing code, deleting tests, removing comments, or weakening evidence. Ask before changing the delivery boundary when review risk becomes material. |

### Authorized Mint transport migration

The maintainer authorized replacing the OTP `:httpc` adapter with a direct Mint HTTP/1 adapter. This is required because `:httpc` cannot expose the exact response status while also guaranteeing streamed, bounded handling for every response body. The migration must preserve all destination-policy, TLS identity, one-attempt, redirect, timeout, header, and body bounds already established by WU6.

Verified planning rationale:

- Mint exposes exact `:status`, `:headers`, `:data`, and `:done` response events, so the adapter can retain the received status while bounding every body incrementally.
- Mint supports tuple-address pinning with an explicit `hostname`, allowing connection to the approved peer while preserving the original host for HTTP Host, TLS SNI, and certificate hostname verification.
- The adapter will force HTTP/1 and configure `max_header_list_size`; body size and total-attempt time remain explicitly enforced by the adapter.
- No Mint, Finch, or Req dependency currently exists in `mix.exs` or `mix.lock`.
- `hpax` already exists transitively in `mix.lock`; this observation does not replace declaring Mint as a direct dependency.
- The current native lineage remains blocked. Migration completion requires a fresh or explicitly rebound review candidate over the complete corrected WU6 boundary; no existing terminal or blocked lineage may be presented as approval.

Migration planning route and delivery decision:

| Field | Decision |
|---|---|
| Route | `delegated` |
| Trigger evidence | Active: the migration changes 2+ non-trivial files. Preparation read covered `mix.exs`, `mix.lock`, `lib/jasmin_ex/dlr/http_client.ex`, `lib/jasmin_ex/dlr/http_client/httpc.ex`, and `test/jasmin_ex/dlr/httpc_test.exs`. |
| Delivery strategy | `ask-on-risk`, resolved by the maintainer to chained delivery |
| Chain strategy | `stacked-to-main` |
| Estimated authored change | 500–600 additions plus deletions across the implementation slices; the budget guides review slicing and must not drive code compression. |
| Slice 1 | Task 6.14, direct Mint HTTP/1 transport and focused transport proof; targets `main`. Expected approximately 350–400 authored changed lines. |
| Slice 2 | Task 6.15, runtime/integration cutover and `:httpc` removal; targets `main` only after Slice 1 lands or is rebased onto its merged boundary. Expected approximately 150–200 authored changed lines. |
| Review boundary | Each slice receives its own focused checks and rollback boundary. After both slices, create a fresh or explicitly rebound native review candidate for complete WU6 from `main@49f49f1`. |

## Acceptance criteria

WU6 is acceptable only when all of the following are demonstrated:

1. The lookup processor persists a validated replayable plan before reverse-map mutation, callback publication, correlation cleanup, or source settlement.
2. Reverse-map creation is idempotent, callback-job publication receives a positive routed confirmation, and only then may cleanup and source ACK occur.
3. A replayed plan resumes from its durable phase. Cleanup retry does not deliberately publish the callback job again; publication uncertainty remains explicitly at-least-once.
4. HTTP GET encodes callback fields into the query string; HTTP POST sends `application/x-www-form-urlencoded` with the exact compatible fields.
5. HTTP success requires a final status below 400 and a bounded body whose surrounding whitespace strips to the exact, case-sensitive value `ACK/Jasmin`.
6. HTTP 404 is terminal regardless of body. Wrong ACK, other non-success statuses, network errors, and timeouts use broker-owned retry semantics until the existing HTTP budget is exhausted.
7. Only `http` and `https` callback schemes are accepted. Userinfo, fragments, control characters, invalid ports/hosts, and reserved callback-field query collisions are rejected.
8. DNS and literal-address checks reject loopback, private, link-local, multicast, unspecified, reserved, and metadata destinations, including IPv4-mapped IPv6 forms, unless an explicit operator-owned test exception applies.
9. DNS is resolved and policy-checked at dispatch time. The adapter connects to the validated pinned peer without a second unchecked hostname resolution.
10. HTTPS preserves the original hostname for Host, SNI, and certificate hostname verification while connecting to the pinned peer. Inability to prove this is a blocker; TLS verification must never be disabled.
11. Redirects are not followed. A redirect response is evaluated only as the received status/body pair, and its Location target is never contacted.
12. Connect, total-attempt, header, and response-body bounds are enforced. Each `HttpClient` call performs at most one network attempt; only the broker/worker owns retries.

## Actionable checklist

Historical task numbers 6.1–6.12 remain stable for traceability. Each task closes only after its progress and evidence placeholders are replaced with exact observations.

### Unit A — Pure lookup policy

- [x] **6.1 — Observe RED for lookup policy.** Add table-driven `test/jasmin_ex/dlr/lookup_test.exs` coverage for requested levels 1/2/3, successful and failed submit responses, final/non-final receipts, event-specific missing-map behavior, malformed/expired data, reverse-map actions, cleanup actions, and compatible level-1/level-2 callback fields.
  - Progress: `complete`
  - RED evidence: `mix test test/jasmin_ex/dlr/lookup_test.exs` → exit 2, 0/8 passed; all eight tests failed because `JasminEx.Dlr.Lookup.plan/2` was undefined.
  - GREEN evidence: `mix test test/jasmin_ex/dlr/lookup_test.exs` → exit 0, 8 passed.
  - REFACTOR evidence: `mix format lib/jasmin_ex/dlr/lookup.ex test/jasmin_ex/dlr/lookup_test.exs && mix test test/jasmin_ex/dlr/lookup_test.exs` → exit 0, 8 passed.
- [x] **6.2 — Implement the pure lookup planner.** Add `lib/jasmin_ex/dlr/lookup.ex` with deterministic, side-effect-free `plan/2` output. Preserve the compatibility quirk: level-1 `connector` is the routed connector ID; level-2 `connector` is the raw receipt ID and `id_smsc` is the normalized SMSC ID.
  - Progress: `complete`; closed by work-unit commit `6220ba0`.
  - RED evidence: shared with task 6.1: undefined `Lookup.plan/2`, exit 2.
  - GREEN evidence: focused lookup suite exit 0, 8 passed.
  - REFACTOR evidence: focused suite remained 8/8 after formatting; runtime harness `N/A` because Unit A is pure policy with no runtime boundary.

### Unit B — Durable plan and worker integration

- [x] **6.3 — Observe RED for durable plan replay.** Add `test/jasmin_ex/dlr/lookup_plan_test.exs` for versioned encode/decode, stable event identity, bounded expiry, persist-before-mutation ordering, reverse-map idempotency, positive routed-confirm gating, phase advancement, cleanup replay, crash recovery, and typed retry/terminal outcomes.
  - Progress: `complete`
  - RED evidence: `mix test test/jasmin_ex/dlr/lookup_plan_test.exs` → exit 2, 0/5 passed; `LookupPlan` APIs and context-bearing worker processor invocation were undefined.
  - GREEN evidence: `mix test test/jasmin_ex/dlr/lookup_plan_test.exs test/jasmin_ex/dlr/worker_test.exs` → exit 0, 11 passed.
  - REFACTOR evidence: grouped processor phase flow and removed compiler warnings; formatted rerun exit 0, 11 passed.
- [x] **6.4 — Implement `LookupPlan` and the lookup processor.** Add `lib/jasmin_ex/dlr/lookup_plan.ex`; integrate the processor through `lib/jasmin_ex/dlr/worker.ex` without weakening the worker's existing broker-owned retry and settlement contract. Persist the plan first, then apply idempotent mutations, obtain positive routed publication confirmation, mark forwarded, clean up, and ACK.
  - Progress: `complete`; closed by work-unit commit `1a9a137`.
  - RED evidence: shared with task 6.3: missing durable plan/processor APIs, exit 2.
  - GREEN evidence: plan and existing worker suites exit 0, 11 passed; tests prove plan-first ordering and forwarded cleanup replay without deliberate republish.
  - REFACTOR evidence: `mix format lib/jasmin_ex/dlr/lookup_plan.ex lib/jasmin_ex/dlr/worker.ex test/jasmin_ex/dlr/lookup_plan_test.exs` followed by focused suites remained green; external runtime harness `N/A` because Unit B uses injected store/publisher boundaries and does not add transport topology.

### Unit C — HTTP job and thrower semantics

- [x] **6.5 — Implement the versioned HTTP job codec.** Add `lib/jasmin_ex/dlr/http_job.ex` with bounded validated fields, fixed deadline, actual callback level, stable identities, and atom-safe decoding. Reject unsupported versions, kinds, methods, and malformed or expired jobs.
  - Progress: `complete`; closed by work-unit commit `5702f39`.
  - RED evidence: `mix test test/jasmin_ex/dlr/http_thrower_test.exs` → exit 2, 0/7 passed because `HttpJob.encode/1` and related codec APIs were undefined.
  - GREEN evidence: grouped focused suites exit 0, 20 passed, including version/kind/method/bounds and atom-count assertions.
  - REFACTOR evidence: codec fields were shared with persisted lookup plans; formatted grouped rerun remained 20/20.
- [x] **6.6 — Observe RED for callback payload and classification.** Add `test/jasmin_ex/dlr/http_thrower_test.exs` for exact GET query and POST form fields, stripped exact `ACK/Jasmin`, status below 400, unfollowed redirects, terminal 404, wrong ACK, other status/network/timeout retry outcomes, deadline handling, and settlement ordering.
  - Progress: `complete`; closed by work-unit commit `5702f39`.
  - RED evidence: same focused command → exit 2, 0/7; `HttpThrower.process/3` and `HttpJob` were unavailable.
  - GREEN evidence: `mix test test/jasmin_ex/dlr/http_thrower_test.exs test/jasmin_ex/dlr/lookup_test.exs test/jasmin_ex/dlr/lookup_plan_test.exs` → exit 0, 20 passed.
  - REFACTOR evidence: formatted grouped rerun exit 0, 20 passed.
- [x] **6.7 — Implement the HTTP thrower.** Add `lib/jasmin_ex/dlr/http_thrower.ex` to encode callback requests, invoke one client attempt, classify success/terminal/retry outcomes, and return settlement directives compatible with the worker and broker retry budget. Never retry inside the thrower or client.
  - Progress: `complete`; closed by work-unit commit `5702f39`.
  - RED evidence: shared with task 6.6: missing thrower API, exit 2.
  - GREEN evidence: exact GET query/POST form, `<400` plus stripped exact ACK, terminal 404, retry classifications, expiry, and one-call/one-client-attempt all pass in the 20-test grouped suite.
  - REFACTOR evidence: formatting plus grouped suite remained green; external runtime harness is deferred to Unit D where the real client and broker-backed endpoint exist.

### Unit D — Destination policy, HTTP adapter, and fake endpoint

- [x] **6.8 — Observe RED for outbound callback routing threats.** Add `test/jasmin_ex/dlr/destination_policy_test.exs` for scheme/authority/query controls, forbidden IPv4/IPv6 and mapped forms, DNS rebinding, multi-address rejection, exact test exceptions, redirect non-following, TLS hostname mismatch, pinned-peer identity, and zero outbound attempts on policy rejection.
  - Progress: `complete`; closed by work-unit commit `102c6be`.
  - RED evidence: `mix test test/jasmin_ex/dlr/destination_policy_test.exs test/jasmin_ex/dlr/httpc_test.exs` → exit 2 because `JasminEx.Dlr.DestinationPolicy.approve/2` was undefined.
  - GREEN evidence: the same command → exit 0, 10 passed and 1 broker integration test excluded by tag.
  - REFACTOR evidence: after formatting and bounded streaming changes, the same command remained green with 10 passed and 1 excluded.
- [x] **6.9 — Implement destination policy.** Add `lib/jasmin_ex/dlr/destination_policy.ex` with injectable resolution, HTTP/HTTPS-only URI validation, IP classification, optional exact origin/CIDR allowlisting, per-attempt resolution, and an immutable approved destination carrying original-host and pinned-peer data.
  - Progress: `complete`; closed by work-unit commit `102c6be`.
  - RED evidence: shared with task 6.8: `DestinationPolicy.approve/2` was undefined, exit 2.
  - GREEN evidence: focused destination/adapter command exit 0, 10 passed and 1 excluded.
  - REFACTOR evidence: URI validation, address classification, exact host/address exceptions, and approved immutable routing data remained green after formatting.
- [x] **6.10 — Observe RED for the OTP adapter gate.** Add `test/jasmin_ex/dlr/httpc_test.exs` proving no redirects or hidden retries, one network attempt per call, original Host/SNI/certificate verification with a pinned peer, explicit timeouts, bounded headers/body, cancellation, exact GET/POST transport, and typed failures.
  - Progress: `complete`; closed by work-unit commit `102c6be`.
  - RED evidence: shared focused command with task 6.8 → exit 2 at the missing destination-policy boundary required by the adapter.
  - GREEN evidence: focused destination/adapter command exit 0, 10 passed and 1 excluded; the mismatch case emitted `hostname_check_failed` while the valid certificate/SNI case returned the exact ACK.
  - REFACTOR evidence: switched successful response reception to asynchronous `stream: {:self, :once}` flow control, bounded each chunk, and cancelled timed-out or oversized requests; focused rerun remained green.
- [x] **6.11 — Implement the HTTP client port and `:httpc` adapter.** Add `lib/jasmin_ex/dlr/http_client.ex` and `lib/jasmin_ex/dlr/http_client/httpc.ex`; add only the required `:inets`/`:ssl` runtime assembly. Use a dedicated profile with no ambient proxy/cookie state. If OTP cannot preserve original-host verification while pinning the peer, stop and report the blocker rather than weakening TLS or destination policy.
  - Progress: `complete`; closed by work-unit commit `102c6be`.
  - RED evidence: shared with task 6.10: the policy/client gate was unavailable, exit 2.
  - GREEN evidence: focused command exit 0, 10 passed and 1 excluded; the adapter connected to `127.0.0.1` while preserving `callback.test` for Host, SNI, and certificate hostname verification.
  - REFACTOR evidence: empirical inspection found this OTP runtime ignores documented `max_header_size`/`max_body_size` request options, so the adapter now uses one-at-a-time async streaming and explicit cancellation rather than relying on ignored options; focused and broker-backed reruns passed.
- [x] **6.12 — Add the fake endpoint and broker-backed retry harness.** Add `test/support/fake_dlr_endpoint.ex` with deterministic HTTP/TLS scripts, request counters, bounded responses, redirect traps, slow/oversized cases, and explicit loopback-only policy exceptions. Prove the thrower retry/terminal budget against the real broker while the fake endpoint records actual network attempts.
  - Progress: `complete`; closed by work-unit commit `102c6be`. Implementation and runtime behavior are verified. No independent broker-only RED command was captured before implementation, so that strict-TDD evidence gap is recorded rather than reconstructed retroactively.
  - RED evidence: the Unit D focused RED established that the real adapter path could not execute because `DestinationPolicy.approve/2` was undefined; the separately tagged broker test was excluded by that command, so there is no honest broker-only RED result to report.
  - GREEN evidence: `mix test --only integration test/jasmin_ex/dlr/httpc_test.exs` → exit 0, 1 passed and 5 excluded. RabbitMQ 4.3.4 delivered counts 0 then 1; the endpoint script returned HTTP 500 then HTTP 200 with `ACK/Jasmin`; exactly two HTTP requests were recorded before terminal ACK.
  - REFACTOR evidence: the same broker-backed command remained green after bounded async reception and cancellation replaced synchronous full-body buffering.
- [x] **6.13 — Replace permissive IPv6 fallback with explicit global-unicast eligibility.** Require IPv6 destinations to be in `2000::/3` and outside the IANA special-purpose ranges that overlap it, while preserving exact operator-owned host/address exceptions and existing IPv4 behavior.
  - Progress: `complete`; fresh follow-up candidate after terminal lineage `review-45e3beccb7ea46f5`; parent review pending.
  - RED evidence: `mix test test/jasmin_ex/dlr/destination_policy_test.exs` → exit 2, 5/6 passed; IPv4-IPv6 translation address `64:ff9b::c000:201` was approved.
  - GREEN evidence: the same focused command → exit 0, 6 passed; the table rejects translation, discard-only, Teredo, benchmarking, ORCHID, documentation, 6to4, local, multicast, unspecified, loopback, and mapped-private representatives while accepting an ordinary global-unicast address.
  - REFACTOR evidence: `mix format lib/jasmin_ex/dlr/destination_policy.ex test/jasmin_ex/dlr/destination_policy_test.exs && mix test test/jasmin_ex/dlr/destination_policy_test.exs` → exit 0, 6 passed.

### Unit E — Direct Mint HTTP/1 migration

- [x] **6.14 — Add the bounded direct Mint HTTP/1 transport.** Introduce Mint as a direct dependency and add a transport adapter that consumes exact `:status`, `:headers`, `:data`, and `:done` events. Connect to the approved tuple address with the original hostname, force HTTP/1, set `max_header_list_size`, preserve Host/SNI/certificate hostname verification, enforce connect/total/body bounds, avoid redirects and retries, and close the connection on every terminal path. Keep the existing `:httpc` adapter available until the next slice so this work unit can be reviewed and rolled back independently.
  - Progress: `complete`; closed by `this commit`. Direct Mint 1.10.0 transport proof is implemented with the existing `:httpc` adapter and tests unchanged; the resulting hash is reported out-of-band because a commit cannot contain its own final identity.
  - RED evidence: `mix test test/jasmin_ex/dlr/mint_test.exs` → exit 2, 0/10 passed; all ten focused tests failed because `JasminEx.Dlr.HttpClient.Mint.request/2` was undefined. Earlier syntax-only test-authoring failures were corrected before this behavioral RED and are not presented as task evidence.
  - GREEN evidence: after adding the direct dependency and adapter, `mix test test/jasmin_ex/dlr/mint_test.exs` → exit 0, 10 passed; the final expanded focused suite passes 11/11.
  - REFACTOR evidence: `mix format lib/jasmin_ex/dlr/http_client/mint.ex test/jasmin_ex/dlr/mint_test.exs && mix test test/jasmin_ex/dlr/mint_test.exs` → exit 0, 11 passed; subsequent Credo-driven extraction retained 11/11.
  - Check evidence: focused suite exit 0, 11 passed; destination-policy/thrower/Mint suite exit 0, 24 passed; formatter exit 0; Credo exit 0 with no issues; Dialyzer exit 0 with 0 errors, 0 skipped, and 0 unnecessary skips.
  - Runtime harness: `N/A` for this proof slice because no production or broker consumer is cut over to Mint; task 6.15 owns the existing broker-backed harness migration. The focused suite exercises real loopback HTTP/TLS sockets directly.
  - Authored changed lines: 573 additions plus deletions, excluding the generated one-line `mix.lock` update and including this tracker evidence.
  - Risks: Mint is not yet the runtime/broker adapter, so the established `:httpc` path remains authoritative until task 6.15; the slice intentionally proves one-shot transport behavior only and adds no pooling, redirects, retries, or runtime wiring.
  - Objective: prove the replacement transport's exact-status and bounded-streaming contract before removing the established adapter.
  - Acceptance criteria: focused tests observe RED before implementation; streamed 2xx/3xx/4xx/5xx responses retain their exact status; headers and body are bounded incrementally; tuple-address pinning retains the original hostname for Host and verified TLS identity; GET/POST payloads are unchanged; one call creates at most one attempt; redirects are not followed; timeout, protocol, oversized, and TLS failures are typed; every success or failure path closes the Mint connection.
  - Authorized file scope: `mix.exs`, `mix.lock`, `lib/jasmin_ex/dlr/http_client/mint.ex`, `test/jasmin_ex/dlr/mint_test.exs`, and this tracker for exact evidence only. `lib/jasmin_ex/dlr/http_client.ex` may change only if the existing port type cannot represent the verified Mint result without loss. Do not modify the destination-policy contract, thrower classification, fake endpoint, broker harness, or existing `:httpc` files in this slice.
  - Exact checks: `mix test test/jasmin_ex/dlr/mint_test.exs`; `mix test test/jasmin_ex/dlr/destination_policy_test.exs test/jasmin_ex/dlr/http_thrower_test.exs test/jasmin_ex/dlr/mint_test.exs`; `mix format --check-formatted`; `mix credo --strict`; `mix dialyzer`.
  - Rollback boundary: remove the Mint adapter and its focused tests, remove the direct Mint dependency and resulting lock changes, and revert only any necessary `HttpClient` type adjustment. The existing `:httpc` path remains intact and no broker/runtime consumer is cut over by this slice.
  - Route and trigger: `delegated`; active because `mix.exs`, the new adapter, and the focused test are 2+ non-trivial files, with the preparation read recorded in the migration table.
  - Delivery: `ask-on-risk` resolved to `stacked-to-main`; Slice 1 targets `main`, carries only task 6.14, and is expected to remain near the 400-line heuristic without reducing safety evidence.
- [x] **6.15 — Cut over WU6 integration and remove `:httpc`.** Move the broker-backed retry harness and complete WU6 transport gate to the Mint adapter, remove the OTP adapter and dedicated profile configuration, remove `:inets` if no longer required elsewhere, and retain `:ssl`. Preserve the fake endpoint's deterministic status, TLS, redirect, timeout, oversized-response, and request-count evidence.
  - Progress: `complete`; closed by `this commit` on top of E1 commit `b45a733`. The broker-backed harness now exercises Mint, the legacy adapter and test are deleted, and `:inets` is removed while `:ssl` remains.
  - RED evidence: `mix test --only integration test/jasmin_ex/dlr/mint_test.exs` → exit 2, 0/1 passed and 11 excluded. The new Mint broker harness completed HTTP 500 then HTTP 200/`ACK/Jasmin`, delivery counts 0 then 1, and two network attempts, then failed the migration-boundary assertion because the legacy adapter file still existed.
  - GREEN evidence: after deleting the legacy adapter/test and removing `:inets`, the same command → exit 0, 1 passed and 11 excluded. RabbitMQ 4.3.4 delivered counts 0 then 1; exactly two endpoint requests occurred before terminal ACK.
  - REFACTOR evidence: removed the temporary source-layout assertion, formatted the final test/runtime changes, and reran `mix test --only integration test/jasmin_ex/dlr/mint_test.exs` → exit 0, 1 passed and 11 excluded.
  - Check evidence: five-suite command exit 0, 39 passed and 1 excluded; full `mix test` exit 0, 677 passed including 1 doctest and 26 excluded; formatter exit 0; Credo exit 0 with no issues across 175 source files; Dialyzer exit 0 with 0 errors, 0 skipped, and 0 unnecessary skips; residue grep exit 1 with no output as expected.
  - Runtime harness: RabbitMQ 4.3.4; scripted HTTP 500 then HTTP 200/`ACK/Jasmin`; delivery counts 0 then 1; exactly two actual HTTP requests; terminal ACK; exit 0, 1 passed and 11 excluded.
  - Authored changed lines: 660 additions plus deletions, including adapter/test deletion, Mint harness cutover, runtime cleanup, and this tracker evidence.
  - Risks: current native review lineage remains blocked; no native review lifecycle command was run. The cutover intentionally changes no callback classification, broker retry ownership, destination policy, or production assembly beyond removing unused `:inets`.
  - Objective: complete the replacement without changing callback semantics, retry ownership, destination approval, or the established runtime evidence boundary.
  - Acceptance criteria: no runtime or test reference to `JasminEx.Dlr.HttpClient.Httpc`, `:httpc`, or adapter profiles remains; Mint is the exercised client in the broker-backed retry harness; HTTP 500 then HTTP 200/`ACK/Jasmin` still produces delivery counts 0 then 1, exactly two network attempts, and terminal ACK; all five WU6 focused suites, full tests, formatter, Credo, and Dialyzer pass; the final authored slice count is recorded; current native lineage is still reported blocked pending a fresh or explicitly rebound complete-WU6 candidate.
  - Authorized file scope: `lib/jasmin_ex/dlr/http_client/httpc.ex` (deletion), `test/jasmin_ex/dlr/httpc_test.exs` (deletion after equivalent Mint coverage exists), `test/jasmin_ex/dlr/mint_test.exs`, `test/support/fake_dlr_endpoint.ex` only if Mint exposes a missing deterministic transport case, `mix.exs` for runtime application cleanup, and this tracker for exact evidence. No lookup, job, thrower, worker, messaging, or destination-policy behavior changes are authorized.
  - Exact checks: `mix test --only integration test/jasmin_ex/dlr/mint_test.exs`; `mix test test/jasmin_ex/dlr/lookup_test.exs test/jasmin_ex/dlr/lookup_plan_test.exs test/jasmin_ex/dlr/http_thrower_test.exs test/jasmin_ex/dlr/destination_policy_test.exs test/jasmin_ex/dlr/mint_test.exs`; `mix test`; `mix format --check-formatted`; `mix credo --strict`; `mix dialyzer`; `git grep -n -E 'HttpClient\\.Httpc|:httpc|dlr_.*httpc|profile:' -- '*.ex' '*.exs'` must return no migration residue.
  - Rollback boundary: restore the deleted `:httpc` adapter/test and `:inets` runtime declaration, restore the broker harness client/profile configuration, and remove only the Mint integration additions from this slice. Do not roll back task 6.14's already-reviewed transport proof or any WU1–WU6 domain behavior.
  - Route and trigger: `delegated`; active because adapter deletion, integration-test cutover, and runtime cleanup span 2+ non-trivial files, with the preparation read recorded in the migration table.
  - Delivery: `ask-on-risk` resolved to `stacked-to-main`; Slice 2 follows Slice 1, targets `main` after Slice 1 lands or is rebased onto that merged boundary, and carries only task 6.15.

**Feature task count: 15; pending Mint migration tasks: 0.**

## Verification gates

Run and record exact command, exit status, and concise observed result. Passing results without the required prior RED evidence do not close a behavior.

```bash
mix test test/jasmin_ex/dlr/lookup_test.exs test/jasmin_ex/dlr/lookup_plan_test.exs test/jasmin_ex/dlr/http_thrower_test.exs test/jasmin_ex/dlr/destination_policy_test.exs test/jasmin_ex/dlr/mint_test.exs
mix test
mix format --check-formatted
mix credo --strict
mix dialyzer
```

The runtime gate must also run the broker-backed HTTP thrower retry harness with `test/support/fake_dlr_endpoint.ex`. Record the exact command selected by the implementation, RabbitMQ version, endpoint script, broker redelivery/attempt counters, actual HTTP request count, terminal disposition, and exit status. A fake-only retry assertion is insufficient.

The command above is the authoritative post-migration Mint gate. Task 6.15 also requires the tagged broker-backed command and the no-residue grep recorded in its evidence.

## Work-unit commits

This planning invocation authorizes only the tracker commit; no source or test commit is authorized. During implementation, close cohesive behavior with tests in the same work-unit commit and use Conventional Commits. Proposed review story:

| Work unit | Outcome | Candidate rollback boundary | Commit evidence |
|---|---|---|---|
| A | Pure lookup policy | `lookup.ex` and `lookup_test.exs` | `6220ba0`; 513 authored lines including the initial authoritative tracker |
| B | Durable lookup plan and processor | `lookup_plan.ex`, its tests, and bounded worker integration | `1a9a137`; 596 authored changed lines |
| C | HTTP job and thrower semantics | `http_job.ex`, `http_thrower.ex`, and focused tests | `5702f39`; 358 authored changed lines |
| D | Destination policy and one-attempt adapter | destination/client modules, endpoint support, security/adapter tests, and only required runtime applications | `102c6be`; 949 authored changed lines |
| E1 | Direct Mint HTTP/1 transport proof | direct dependency, Mint adapter, and focused tests; existing `:httpc` path remains intact | `b45a733`; 573 authored changed lines excluding generated `mix.lock` |
| E2 | Runtime/integration cutover and OTP adapter removal | broker-backed Mint gate, legacy adapter/test deletion, and `:inets` cleanup | Task 6.15 closed by `this commit`; 660 authored changed lines; resulting hash reported out-of-band |

If these units cannot stand independently because the safety contract requires a cohesive WU6 candidate, preserve the cohesive boundary and record the honest authored line count. Do not invent artificial splits or rewrite for line-count optics.

## Review assessment

- Current assessment: **high review-load risk; expected oversized cohesive WU6**.
- The 400-line threshold is an advisory signal to ask, not an instruction to reduce test/security coverage.
- Review order: pure lookup decisions → replay/settlement ordering → payload classification → destination/TLS/transport proof → broker-backed retry evidence.
- Before any delivery action, record authored additions plus deletions, changed files, smallest honest review boundary, and whether `ask-on-risk` needs a fresh user decision.
- Phase 7 remains separate. Any production assembly change in WU6 must be justified solely by WU6 reachability and must not include full supervisor rollout, end-to-end application enablement, or documentation rollout.

## Rollback boundaries

1. Disable or remove WU6 processor assembly before deleting modules; do not disturb WU1–WU5 intake, map, event, receipt, or MT settlement behavior.
2. Preserve pending lookup plans and HTTP jobs until they are drained, quarantined, or expired by explicit operator policy. Never delete them merely to simplify rollback.
3. Unit A can roll back as pure policy/tests before reachability exists.
4. Unit B rolls back lookup-plan storage and worker processor integration together so no event can mutate maps without its persisted plan.
5. Unit C rolls back the job/thrower contract together; do not leave a published job without a compatible consumer.
6. Unit D rolls back destination policy, client port/adapter, fake endpoint support, and any WU6-only `:inets`/`:ssl` assembly together. Never retain an adapter path that bypasses destination approval.
7. Unit E1 rolls back the direct Mint dependency, adapter, and focused tests while leaving the existing `:httpc` transport operational.
8. Unit E2 rolls back the integration cutover, `:httpc` deletions, and `:inets` cleanup together; do not leave tests or runtime assembly pointing at a removed adapter.
9. Do not roll back or modify WU1–WU5 correlation, SMPP receipt, topic transport, or MT queue behavior as part of WU6 recovery.

## Progress and evidence summary

| Item | Status | Evidence |
|---|---|---|
| Tasks 6.1–6.12 | 12/12 implemented | Units A–D behavior verified; task 6.12 retains the explicit broker-only RED evidence gap recorded above |
| Focused five-test gate | Passed | Exact post-migration command from the verification gate → exit 0, 39 passed and 1 integration test excluded |
| Full test suite | Passed | `mix test` → exit 0, 677 passed including 1 doctest, 26 excluded |
| Formatter | Passed | `mix format --check-formatted` → exit 0 |
| Credo | Passed | `mix credo --strict` → exit 0, no issues |
| Dialyzer | Passed | `mix dialyzer` → exit 0, 0 errors and 0 skipped warnings |
| Broker-backed thrower retry harness | Passed with Mint | RabbitMQ 4.3.4; HTTP 500 then 200/`ACK/Jasmin`; delivery counts 0 then 1; two actual HTTP requests; terminal ACK; exit 0, 1 passed and 11 excluded |
| Review correction | Implemented and locally verified | Preserved streamed HTTP status and terminalized expired plan/event replay; 91/200 authored correction lines; focused gate 13 passed/1 excluded, five-suite gate 33 passed/1 excluded, full suite 671 passed/26 excluded, Credo and Dialyzer passed |
| Native review `R3-ipv6-reserved-bypass` | Terminal `escalated` for lineage `review-45e3beccb7ea46f5`; superseded locally by task 6.13 | The narrow correction rejected site-local and documentation space but retained a permissive IPv6 fallback; the fresh follow-up now uses positive global-unicast eligibility plus special-purpose exclusions. |
| Mint transport task 6.14 | Complete; `b45a733` | Direct Mint 1.10.0 HTTP/1 proof preserves exact final status after 1xx, pins the approved tuple while retaining Host/SNI/certificate identity, bounds streamed headers/body and one total deadline, returns typed failures, avoids redirects/retries/second DNS, and closes every established connection. Focused: 11 passed; grouped: 24 passed; formatter, Credo, and Dialyzer passed. |
| Mint runtime cutover task 6.15 | Complete; closed by `this commit` | Mint now owns the broker-backed retry gate; the legacy adapter/test and unused `:inets` runtime application are removed. All required checks passed and the residue grep returned no output. |
| Task 6.14 authored changed lines | 573 | Additions plus deletions for authored source, tests, dependency declaration, and tracker evidence; excludes the generated one-line `mix.lock` update. |
| Task 6.15 authored changed lines | 660 | Additions plus deletions for legacy adapter/test removal, Mint broker harness cutover, runtime cleanup, and tracker evidence. |
| Authored changed lines | 2,416 through Unit D before tracker updates | Unit A: 513; Unit B: 596; Unit C: 358; Unit D implementation: 949 additions plus deletions |
| Review decision | Current native lineage blocked; fresh/rebound review required after migration | `review-579dcd39287035c5` was quarantined after its provider continuation was lost, and `review-45e3beccb7ea46f5` is terminal escalated; after tasks 6.14–6.15, review the complete corrected WU6 candidate from the original base boundary. |
| Work-unit commits | Units A–E2 committed in separate slices | `6220ba0` (A), `1a9a137` (B), `5702f39` (C), `102c6be` (D), `b45a733` (E1), and E2 `this commit` with its resulting hash reported out-of-band |

## Next step

Tasks 6.14 and 6.15 are complete as separate local commits. Retain the documented task 6.12 broker-only RED evidence gap. A future authorized step may create a fresh or explicitly rebound native review candidate over the complete corrected WU6 candidate from `main@49f49f1`; the current lineage remains blocked, and no native review or remote delivery action was run here.
