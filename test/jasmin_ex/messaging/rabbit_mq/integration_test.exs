defmodule JasminEx.Messaging.RabbitMQ.IntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias JasminEx.Messaging.RabbitMQ.{Client, Config, Connection, Publisher}
  alias JasminEx.RabbitMQHarness

  setup do
    harness = RabbitMQHarness.new()
    :ok = RabbitMQHarness.start!(harness)

    on_exit(fn -> safe_stop(harness) end)
    {:ok, harness: harness, config: config(harness)}
  end

  test "confirmed persistent publish survives broker restart", %{
    harness: harness,
    config: config
  } do
    connector = unique("survive")
    payload = "durable-#{connector}"
    {connection, publisher} = start_stack(config)

    assert :ok = Publisher.publish(publisher, connector, payload)
    stop_stack(connection, publisher)

    assert :ok = RabbitMQHarness.restart!(harness)
    assert :ok = RabbitMQHarness.wait!(harness)

    {:ok, conn} = open(harness)
    {:ok, ch} = Client.open_channel(conn)
    queue = "jasmin.work.#{connector}"
    assert {:ok, _} = Client.declare_queue(ch, queue, durable: true)
    collector = start_collector()
    assert :ok = Client.qos(ch, prefetch_count: 1)
    assert {:ok, _} = Client.consume(ch, queue, collector, no_ack: false)
    assert {:basic_deliver, ^payload, meta} = await_deliver(collector)
    assert meta.persistent == true
    _ = Client.close_connection(conn)
  end

  test "connection or channel loss does not report enqueue success", %{
    harness: harness,
    config: config
  } do
    connector = unique("loss")
    {:ok, conn} = open(harness)
    {:ok, publisher} = Publisher.start_link(config: config, connection: conn, name: nil)

    assert :ok = Publisher.publish(publisher, connector, "before-loss")
    assert :ok = Client.close_connection(conn)

    result = Publisher.publish(publisher, connector, "after-loss")
    refute result == :ok
    assert {:error, reason} = result
    assert reason in [:channel_closed, :disconnected, :timeout, :nack]
    GenServer.stop(publisher)
  end

  test "prefetch 1 delivers at most one unacked message", %{config: config} do
    connector = unique("prefetch")
    queue = "jasmin.work.#{connector}"
    {connection, publisher} = start_stack(config)
    {:ok, conn} = Connection.get(connection)
    {:ok, ch} = Client.open_channel(conn)
    assert {:ok, _} = Client.declare_queue(ch, queue, durable: true)
    collector = start_collector()
    assert :ok = Client.qos(ch, prefetch_count: 1)
    assert {:ok, _} = Client.consume(ch, queue, collector, no_ack: false)

    assert :ok = Publisher.publish(publisher, connector, "first")
    assert :ok = Publisher.publish(publisher, connector, "second")
    assert {:basic_deliver, "first", meta} = await_deliver(collector)
    refute_deliver(collector)
    assert :ok = Client.ack(ch, meta.delivery_tag)
    assert {:basic_deliver, "second", _} = await_deliver(collector)
    stop_stack(connection, publisher)
  end

  test "connector A channel loss leaves B consuming and the shared connection usable", %{
    config: config
  } do
    connector_a = unique("a")
    connector_b = unique("b")
    {connection, publisher} = start_stack(config)
    {:ok, conn} = Connection.get(connection)
    {:ok, ch_a} = Client.open_channel(conn)
    {:ok, ch_b} = Client.open_channel(conn)
    collector_a = start_collector()
    collector_b = start_collector()

    for {ch, id, collector} <- [
          {ch_a, connector_a, collector_a},
          {ch_b, connector_b, collector_b}
        ] do
      assert {:ok, _} = Client.declare_queue(ch, "jasmin.work.#{id}", durable: true)
      assert :ok = Client.qos(ch, prefetch_count: 1)
      assert {:ok, _} = Client.consume(ch, "jasmin.work.#{id}", collector, no_ack: false)
    end

    assert :ok = Publisher.publish(publisher, connector_a, "a-before")
    assert {:basic_deliver, "a-before", _} = await_deliver(collector_a)
    assert :ok = Client.close_channel(ch_a)
    assert :ok = Publisher.publish(publisher, connector_b, "b-after")
    assert {:basic_deliver, "b-after", _} = await_deliver(collector_b)
    refute_deliver(collector_a)
    assert {:ok, ch_c} = Client.open_channel(conn)
    assert :ok = Client.close_channel(ch_c)
    stop_stack(connection, publisher)
  end

  defp config(harness) do
    env = Map.new(RabbitMQHarness.compose_environment(harness))

    Config.new!(
      host: "127.0.0.1",
      port: RabbitMQHarness.port(harness),
      username: env["RABBITMQ_TEST_USER"],
      password: env["RABBITMQ_TEST_PASSWORD"],
      confirm_timeout_ms: 2_000
    )
  end

  defp open(harness), do: Client.open_connection(Config.to_connection_options(config(harness)))

  defp start_stack(config) do
    {:ok, connection} = Connection.start_link(config: config, name: nil)

    {:ok, publisher} =
      Publisher.start_link(config: config, connection_server: connection, name: nil)

    {connection, publisher}
  end

  defp stop_stack(connection, publisher) do
    GenServer.stop(publisher)
    GenServer.stop(connection)
  end

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

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
