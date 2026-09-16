defmodule JasminEx.Messaging.RabbitMQ.TopicTopologyTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.RabbitMQ.TopicTopology

  defmodule Fake do
    def start(script) do
      {:ok, agent} = Agent.start_link(fn -> %{script: script, events: []} end)
      agent
    end

    def events(agent), do: Agent.get(agent, &Enum.reverse(&1.events))
    def channel(agent), do: %{agent: agent, pid: self()}

    def declare_exchange(%{agent: agent}, name, type, opts) do
      track(agent, {:declare_exchange, name, type, opts})
      :ok
    end

    def declare_queue(%{agent: agent}, name, opts) do
      track(agent, {:declare_queue, name, opts})
      script(agent, :declare_queue, {:ok, %{queue: name}})
    end

    def bind_queue(%{agent: agent}, queue, exchange, opts) do
      track(agent, {:bind_queue, queue, exchange, opts})
      :ok
    end

    defp script(agent, key, default),
      do: Agent.get(agent, fn state -> Map.get(state.script, key, default) end)

    defp track(agent, event),
      do: Agent.update(agent, fn state -> %{state | events: [event | state.events]} end)
  end

  test "stable queue names use the DLR prefix and v1 suffix" do
    names = TopicTopology.names("jasmin_ex.dlr")

    assert names.exchange == "messaging"
    assert names.dlx == "jasmin_ex.dlr.dlx"
    assert names.lookup == "jasmin_ex.dlr.lookup.v1"
    assert names.http == "jasmin_ex.dlr.http.v1"
    assert names.dead == "jasmin_ex.dlr.dead.v1"
  end

  test "lookup queue arguments are quorum delayed-retry with 10s min=max" do
    args = TopicTopology.lookup_arguments("jasmin_ex.dlr.dlx")

    assert arg(args, "x-queue-type") == "quorum"
    assert arg(args, "x-single-active-consumer") == true
    assert arg(args, "x-delayed-retry-type") == "all"
    assert arg(args, "x-delayed-retry-min") == 10_000
    assert arg(args, "x-delayed-retry-max") == 10_000
    assert arg(args, "x-overflow") == "reject-publish"
    assert arg(args, "x-dead-letter-exchange") == "jasmin_ex.dlr.dlx"
    assert arg(args, "x-dead-letter-strategy") == "at-least-once"
    assert arg(args, "x-delivery-limit") == 3
    refute arg(args, "x-queue-type") == "classic"
  end

  test "HTTP queue arguments are quorum delayed-retry with 30s min=max" do
    args = TopicTopology.http_arguments("jasmin_ex.dlr.dlx")

    assert arg(args, "x-queue-type") == "quorum"
    assert arg(args, "x-single-active-consumer") == true
    assert arg(args, "x-delayed-retry-type") == "all"
    assert arg(args, "x-delayed-retry-min") == 30_000
    assert arg(args, "x-delayed-retry-max") == 30_000
    assert arg(args, "x-overflow") == "reject-publish"
    assert arg(args, "x-dead-letter-exchange") == "jasmin_ex.dlr.dlx"
    assert arg(args, "x-dead-letter-strategy") == "at-least-once"
    assert arg(args, "x-delivery-limit") == 4
    refute arg(args, "x-queue-type") == "classic"
  end

  test "dead queue is durable quorum with reject-publish overflow and no delayed retry" do
    args = TopicTopology.dead_arguments()

    assert arg(args, "x-queue-type") == "quorum"
    assert arg(args, "x-overflow") == "reject-publish"
    assert arg(args, "x-delayed-retry-type") == nil
  end

  test "declare fails closed when delayed-retry arguments are rejected" do
    agent = Fake.start(%{declare_queue: {:error, :precondition_failed}})
    channel = Fake.channel(agent)

    assert {:error, :delayed_retry_unsupported} =
             TopicTopology.declare(channel, prefix: "jasmin_ex.dlr", client: Fake)

    events = Fake.events(agent)
    refute Enum.any?(events, fn event -> classic_declare?(event) end)
  end

  test "declare does not fall back to classic queues on topology failure" do
    agent = Fake.start(%{declare_queue: {:error, :channel_closed}})
    channel = Fake.channel(agent)

    assert {:error, :delayed_retry_unsupported} =
             TopicTopology.declare(channel, prefix: "jasmin_ex.dlr", client: Fake)

    refute Enum.any?(Fake.events(agent), &classic_declare?/1)
  end

  defp arg(args, name) do
    case List.keyfind(args, name, 0) do
      {^name, _type, value} -> value
      {^name, value} -> value
      nil -> nil
    end
  end

  defp classic_declare?({:declare_queue, _name, opts}) do
    args = Keyword.get(opts, :arguments, [])
    arg(args, "x-queue-type") == "classic"
  end

  defp classic_declare?(_event), do: false
end

defmodule JasminEx.Messaging.RabbitMQ.TopicTopologyIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias JasminEx.Messaging.RabbitMQ.{Client, Config, TopicTopology}
  alias JasminEx.RabbitMQHarness

  setup do
    harness = RabbitMQHarness.new()
    :ok = RabbitMQHarness.start!(harness)
    on_exit(fn -> safe_stop(harness) end)

    {:ok, conn} = Client.open_connection(connection_opts(harness))
    {:ok, ch} = Client.open_channel(conn)
    on_exit(fn -> _ = Client.close_connection(conn) end)

    prefix = "jasmin_ex.dlr.#{System.unique_integer([:positive])}"
    {:ok, harness: harness, conn: conn, ch: ch, prefix: prefix}
  end

  test "pinned broker accepts quorum delayed-retry topology", %{ch: ch, prefix: prefix} do
    assert :ok = TopicTopology.declare(ch, prefix: prefix, client: Client)

    names = TopicTopology.names(prefix)
    assert {:ok, _} = Client.declare_queue(ch, names.lookup, passive: true)
    assert {:ok, _} = Client.declare_queue(ch, names.http, passive: true)
    assert {:ok, _} = Client.declare_queue(ch, names.dead, passive: true)
  end

  test "classic redeclare of a DLR lookup queue is rejected", %{ch: ch, prefix: prefix} do
    assert :ok = TopicTopology.declare(ch, prefix: prefix, client: Client)
    names = TopicTopology.names(prefix)

    result =
      try do
        Client.declare_queue(ch, names.lookup,
          durable: true,
          arguments: [{"x-queue-type", :longstr, "classic"}]
        )
      catch
        :exit, reason -> {:error, reason}
      end

    assert match?({:error, _}, result)
  end

  test "lookup binds dlr.* and HTTP binds dlr_thrower.http", %{ch: ch, prefix: prefix} do
    assert :ok = TopicTopology.declare(ch, prefix: prefix, client: Client)
    names = TopicTopology.names(prefix)

    lookup = start_collector()
    http = start_collector()
    assert :ok = Client.qos(ch, prefetch_count: 1)
    assert {:ok, _} = Client.consume(ch, names.lookup, lookup, no_ack: false)
    assert {:ok, _} = Client.consume(ch, names.http, http, no_ack: false)

    assert :ok = Client.select_confirms(ch)

    assert :ok =
             Client.publish(ch, names.exchange, "dlr.submit_sm_resp", "lookup-body",
               mandatory: true
             )

    assert true = Client.wait_for_confirms(ch, 2_000)
    assert {:basic_deliver, "lookup-body", meta} = await_deliver(lookup)
    assert meta.routing_key == "dlr.submit_sm_resp"

    assert :ok =
             Client.publish(ch, names.exchange, "dlr_thrower.http", "http-body", mandatory: true)

    assert true = Client.wait_for_confirms(ch, 2_000)
    assert {:basic_deliver, "http-body", http_meta} = await_deliver(http)
    assert http_meta.routing_key == "dlr_thrower.http"
    refute_deliver(lookup)
  end

  defp connection_opts(harness) do
    env = Map.new(RabbitMQHarness.compose_environment(harness))

    Config.to_connection_options(
      Config.new!(
        host: "127.0.0.1",
        port: RabbitMQHarness.port(harness),
        username: env["RABBITMQ_TEST_USER"],
        password: env["RABBITMQ_TEST_PASSWORD"],
        confirm_timeout_ms: 2_000
      )
    )
  end

  defp start_collector do
    parent = self()

    spawn_link(fn ->
      Enum.each(Stream.repeatedly(fn -> receive do: (msg -> msg) end), fn
        {:basic_deliver, _, _} = msg -> send(parent, {self(), msg})
        _other -> :ok
      end)
    end)
  end

  defp await_deliver(collector) do
    receive do
      {^collector, {:basic_deliver, _, _} = msg} -> msg
    after
      5_000 -> flunk("missing broker delivery")
    end
  end

  defp refute_deliver(collector) do
    receive do
      {^collector, {:basic_deliver, payload, _}} ->
        flunk("unexpected extra delivery #{inspect(payload)}")
    after
      300 -> :ok
    end
  end

  defp safe_stop(harness) do
    RabbitMQHarness.stop!(harness)
  rescue
    _error -> :ok
  end
end
