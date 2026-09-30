# JasminEx

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `jasmin_ex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:jasmin_ex, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/jasmin_ex>.

## State-store integration evidence

The state-store contract uses one ephemeral, authenticated RESP service for
live integration tests. Ordinary tests do not start Docker:

```bash
mix test
mix test --include integration test/jasmin_ex/state_store/integration_test.exs
```

The harness creates a unique Compose project, selects a stable local port for
its run, waits for service health, and removes only that project's resources.
Run a specific pinned compatibility target by setting its image:

```bash
STATE_STORE_TEST_IMAGE='redis:8.0.3-bookworm@sha256:be0a135f1955140436b9114da96dd22fbedb874469400b6ef458cc0d42155de0' \
  mix test --include integration test/jasmin_ex/state_store/integration_test.exs

STATE_STORE_TEST_IMAGE='docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.3@sha256:29d44a25a9e6937672f1c12e28c9f481f3d3c0441001ee56ed274a72f50593b7' \
  mix test --include integration test/jasmin_ex/state_store/integration_test.exs
```

| Backend | Exact image | Evidence semantics |
|---|---|---|
| Valkey | `valkey/valkey:9.1.1@sha256:2f4a4b0a42a72569b40567fae9016dc54aa76736250be28120b5fced8050c0f0` | Blocking reference integration evidence. |
| Redis | `redis:8.0.3-bookworm@sha256:be0a135f1955140436b9114da96dd22fbedb874469400b6ef458cc0d42155de0` | Non-blocking compatibility evidence; CI uploads its result artifact even when the test fails. |
| Dragonfly | `docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.3@sha256:29d44a25a9e6937672f1c12e28c9f481f3d3c0441001ee56ed274a72f50593b7` | Non-blocking compatibility evidence; CI uploads its result artifact even when the test fails. |

These pinned Redis and Dragonfly runs evidence only the tested portable binary
fetch, expiring put, delete, authentication, outage/reconnect, and TTL
scenarios. They do not claim universal compatibility. TLS integration remains
deferred. This change does not add pools, Cluster, Sentinel, provider adapters,
DLR/schema/workflow changes, or non-expiring writes. RabbitMQ messaging uses a
separate compose file and must not start Valkey, Redis, or Dragonfly.

## RabbitMQ messaging operator runbook

Messaging defaults to disabled. Do not claim environment fitness until the
pinned durable/restart harness retains a complete metric baseline.

### Quick path

1. Leave `:messaging` at `enabled: false` until the baseline is complete.
2. Run the pinned RabbitMQ durable/restart harness.
3. Confirm every required metric was retained with that run. A missing metric
   is incomplete validation — never invent t2.micro fitness or thresholds.

### Enable / disable

| Setting | Effect |
|---|---|
| `enabled: false` (default) | No publisher or consumer starts. |
| `enabled: true` plus validated AMQP options | Supervises the shared connection and publisher. |

### Topology names

| Queue | Name |
|---|---|
| Work | `jasmin.work.<connector_id>` |
| Quarantine | `jasmin.work.<connector_id>.quarantine` |

### Quarantine ownership

Operations owns disposition. Retain with no TTL, replay, or automatic purge.

### Envelope v2 (binary-safe queue payload)

Queue encode now writes integer `version` 2. Wire `submit_sm` uses
`short_message_base64` (standard padded canonical Base64). In-memory
`submit_sm.short_message` stays a binary. v2 absent or JSON-null
`data_coding` / `registered_delivery` default to 0. v1 JSON-null and
`Envelope.new/1` `:null` for those fields stay invalid. This is queue
serialization only.

A v1 reader rejects v2 with `:unsupported_version` and the worker rejects
without requeue. Application work-queue declarations have no owned
dead-letter exchange. Rejection does **not** automatically quarantine the
message. Consumers that retry or quarantine also produce v2.

#### Quick path

1. Pause ingress and all consumers that read this envelope.
2. Upgrade every reader before any process publishes v2.
3. Resume. Queued v1 remains readable; new work, retries, and quarantine
   evidence are v2.

#### Upgrade and rollback

| Step | Action |
|---|---|
| Upgrade | Pause ingress and consumers, deploy readers first, then resume. |
| Mixed fleet | Do not publish v2 while any v1 reader is still consuming. |
| Rollback | Isolate and drain v2 first. Never convert arbitrary binary back to v1 text. |

#### Checklist

- [ ] Every consumer understands v2 before the first v2 publish.
- [ ] Work and quarantine queues are not assumed to dead-letter rejected v2.
- [ ] Rollback drains v2 instead of downgrading bytes to version-1 strings.

### Rollback

1. Disable publish and consume (`enabled: false`).
2. Drain or quarantine in-flight work.
3. Keep queues and the evidence journal.

### Evidence

| Kind | Command | What it proves |
|---|---|---|
| Fake / unit | `mix test` | Contract and adapter logic. Ordinary tests are not broker proof. |
| Integration | `mix test --include integration test/jasmin_ex/messaging/rabbit_mq/integration_test.exs` | Pinned RabbitMQ durable/restart path. |
| CI | GitHub job `rabbitmq-4-3-4` | Pinned durable/restart evidence in a separate job; must not start Valkey, Redis, or Dragonfly. Not an environment fitness claim. |

Required baseline metrics: connector count, rate, payload, backlog, latency,
CPU, memory, alarms, confirms, redeliveries, and recovery. The measurement
helper records those values with the run. Missing metrics remain incomplete
validation and forbid any fitness claim.

## HTTP send encoding

`POST /send` and `POST /rate` encode or validate the short message **before** routing,
billing, DLR registration, or queue publish. Invalid payloads never
reserve balance, decrement quota, register a DLR, or enqueue.

Supported HTTP `coding` values are `0`, `1`, `2`, `3`, and `8`:

| coding | Scheme | Text | `hex-content` |
|---|---|---|---|
| 0 | GSM 03.38 unpacked | Default alphabet plus `0x1B` escapes | Structural GSM decode only |
| 1 | IA5 ASCII | Strict 7-bit ASCII | Structural ASCII decode only |
| 2 | Octet | Raw bytes, including NUL and high bytes | Original bytes, no transform |
| 3 | Latin-1 | ISO-8859-1 conversion | Structural Latin-1 decode only |
| 8 | UCS2 | UTF-16BE, including valid supplementary-plane pairs | Structural UTF-16BE decode only |

Text is encoded **once** with `Coding.encode_short_message/2`. Hex is
decoded once with `Base.decode16/2`; `Coding.decode_short_message/2` is
structural validation only and never re-encodes or transcodes. The
original `content` (UTF-8 text, or hex-decoded bytes) is what
`Filter.Content` sees. The submit envelope carries a separate encoded
wire field.

The 254-octet bound is the SMPP `submit_sm` `sm_length` u8 PDU limit, not
cellular SMS capacity. This change does not segment messages.

Typed HTTP 400 bodies keep the existing `error:<reason>\n` contract:

| Reason | When |
|---|---|
| `missing_content` | Neither `content` nor `hex-content` is present |
| `malformed_hex` | `hex-content` is not valid hex |
| `invalid_coding` | `coding` is not 0, 1, 2, 3, or 8 |
| `invalid_content` | Text cannot be represented, or hex bytes fail the coding's structure |
| `message_too_long` | Encoded wire octets exceed 254 |
