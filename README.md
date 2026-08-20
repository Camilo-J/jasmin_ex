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
