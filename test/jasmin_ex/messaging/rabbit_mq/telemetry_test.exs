defmodule JasminEx.Messaging.RabbitMQ.TelemetryTest do
  use ExUnit.Case, async: false

  alias JasminEx.Messaging.Envelope
  alias JasminEx.Messaging.RabbitMQ.{Config, Connection, ConnectorWorker, Publisher, Telemetry}

  @secret "s3cret-pass"
  @events for s <- [
                [:connection, :up],
                [:connection, :down],
                [:channel, :up],
                [:channel, :down],
                [:confirm],
                [:consumer, :started],
                [:consumer, :stopped],
                [:redelivery],
                [:settlement],
                [:retry],
                [:quarantine],
                [:recovery]
              ],
              do: [:jasmin_ex, :messaging | s]
  @cfg [host: "broker.example", username: "guest", password: @secret, confirm_timeout_ms: 50]
  @base %{
    gateway_id: "g",
    connector_id: "alpha",
    attempt: 1,
    max_attempts: 3,
    expires_at: "2099-01-01T00:00:00Z",
    submit_sm: %{source_addr: "a", destination_addr: "b", short_message: "h"}
  }

  defmodule JournalStore do
    def put(t, k, v, ttl), do: :ets.insert(t, {k, v, ttl}) && :ok

    def fetch(t, k) do
      case :ets.lookup(t, k) do
        [{^k, v, _}] -> {:ok, v}
        [] -> :missing
      end
    end
  end

  defmodule Fake do
    def start(script \\ %{}) do
      {:ok, a} = Agent.start_link(fn -> %{script: script, n: 1} end)
      a
    end

    def connection(a), do: %{pid: self(), agent: a}

    def open_connection(_) do
      {:ok, pid} = Agent.start_link(fn -> :open end)
      {:ok, %{pid: pid}}
    end

    def close_connection(%{pid: pid}) do
      if Process.alive?(pid), do: Agent.stop(pid)
      :ok
    catch
      :exit, _ -> :ok
    end

    def connection_pid(%{pid: pid}), do: pid

    def open_channel(%{agent: a} = conn) do
      id = Agent.get_and_update(a, &{&1.n, %{&1 | n: &1.n + 1}})
      {:ok, Map.merge(conn, %{channel_id: id, pid: spawn(fn -> Process.sleep(:infinity) end)})}
    end

    def close_channel(%{pid: pid}), do: Process.exit(pid, :shutdown)
    def select_confirms(_), do: :ok

    def declare_queue(%{agent: a}, name, _),
      do: {:ok, %{queue: name, message_count: Agent.get(a, &Map.get(&1, :q, 0))}}

    def publish(%{agent: a}, _, key, _, _) do
      if String.ends_with?(to_string(key), ".quarantine") do
        Agent.update(a, &bump_queue_count/1)
      end

      :ok
    end

    defp bump_queue_count(state), do: Map.update(state, :q, 1, &(&1 + 1))

    def wait_for_confirms(%{agent: a}, _),
      do: Agent.get(a, &Map.get(&1.script, :wait_for_confirms, true))

    def qos(_, _), do: :ok
    def consume(%{channel_id: id}, _, _, _), do: {:ok, "ctag-#{id}"}
    def cancel(_, tag), do: {:ok, tag}
    def ack(_, _), do: :ok
    def reject(_, _, _), do: :ok
  end

  setup do
    id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(id, @events, &__MODULE__.handle_telemetry/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
    :ok
  end

  def handle_telemetry(event, m, meta, pid), do: send(pid, {:telemetry, event, m, meta})

  test "connection and recovery events stay bounded and secret-free" do
    {:ok, server} =
      Connection.start_link(
        config: config(),
        client: Fake,
        client_opts: [test_owner: self()],
        name: nil,
        reconnect_backoff_ms: 5
      )

    {_, up} = event([:connection, :up])
    assert %{host: "broker.example", port: 5672} = up
    refute_secrets(up)
    assert {:ok, %{pid: first}} = Connection.get(server)
    ref = Process.monitor(first)
    Agent.stop(first)
    assert_receive {:DOWN, ^ref, :process, ^first, _}
    {_, down} = event([:connection, :down])
    refute_secrets(down)
    assert {:ok, %{pid: second}} = wait_reconnect(server, first)
    assert second != first
    {_, recovered} = event([:recovery])
    refute_secrets(recovered)
    _ = event([:connection, :up])
    GenServer.stop(server)
  end

  test "publisher emits channel state and confirm latency/result" do
    for {confirm, expected} <- [{true, :ok}, {false, :nack}] do
      agent = Fake.start(%{wait_for_confirms: confirm})

      {:ok, pub} =
        Publisher.start_link(
          config: config(),
          client: Fake,
          connection: Fake.connection(agent),
          name: nil
        )

      {_, ch} = event([:channel, :up])
      assert ch.role == :publisher
      refute_secrets(ch)
      _ = Publisher.publish(pub, "alpha", "payload")
      {%{latency_ms: latency}, meta} = event([:confirm])
      assert meta.result == expected and meta.connector_id == "alpha"
      assert is_integer(latency) and latency >= 0
      refute_secrets(meta)
      GenServer.stop(pub)
      Agent.stop(agent)
    end
  end

  test "worker emits consumer, settlement, retry, quarantine, redelivery, and alarms" do
    {:ok, outcome} = Agent.start_link(fn -> {:ok, "ok"} end)
    {worker, agent} = start_bound(fn _ -> Agent.get(outcome, & &1) end)
    {_, ch} = event([:channel, :up])
    assert ch.role == :consumer
    {_, started} = event([:consumer, :started])
    assert started.connector_id == "alpha"
    deliver(worker, %{gateway_id: "gw-ok"})
    {_, ack} = event([:settlement])
    assert ack.decision == :ack and ack.gateway_id == "gw-ok"
    Agent.update(outcome, fn _ -> {:error, {:submit_rejected, :ESME_RINVDESTADR}} end)
    deliver(worker, %{gateway_id: "gw-rej"})
    {_, reject} = event([:settlement])
    assert reject.decision == :reject
    Agent.update(outcome, fn _ -> {:error, :disconnected} end)
    deliver(worker, %{gateway_id: "gw-retry"})
    {_, retry} = event([:retry])
    assert retry.gateway_id == "gw-retry" and retry.attempt == 1
    deliver(worker, %{gateway_id: "gw-q", attempt: 3, max_attempts: 3})
    {%{depth: depth, age_ms: age}, quiet} = event([:quarantine])
    assert depth >= 1 and age >= 0
    refute quiet.alarm
    refute_secrets(quiet)
    deliver(worker, %{gateway_id: "gw-al", attempt: 3, max_attempts: 3})
    {%{depth: observed}, alarmed} = event([:quarantine])
    assert observed > 1 and alarmed.alarm and :depth in alarmed.alarms
    refute_secrets(alarmed)
    deliver(worker, %{gateway_id: "gw-r"}, %{redelivered: true})
    {_, redelivered} = event([:redelivery])
    assert redelivered.redelivered
    send(worker, {:smpp_bind_lost, "alpha", :tcp_closed})
    _ = ConnectorWorker.inflight(worker)
    _ = event([:consumer, :stopped])
    {_, down} = event([:channel, :down])
    assert down.role == :consumer
    Agent.stop(outcome)
    stop(worker, agent)
  end

  test "quarantine_meta flags only the crossed depth or age threshold" do
    depth = Telemetry.quarantine_meta(config(quarantine_depth_alarm: 1), 1, 0)
    assert depth.alarm and depth.alarms == [:depth]
    age = Telemetry.quarantine_meta(config(quarantine_age_ms_alarm: 1), 1, 5)
    assert age.alarm and age.alarms == [:age]
    none = Telemetry.quarantine_meta(config(), 1, 0)
    refute none.alarm
    assert none.alarms == []
  end

  defp event(suffix) do
    assert_receive {:telemetry, [:jasmin_ex, :messaging | ^suffix], meas, meta}
    {meas, meta}
  end

  defp deliver(worker, overrides, extra \\ %{}) do
    send(worker, {:basic_deliver, payload(overrides), Map.merge(%{delivery_tag: 1}, extra)})
    assert ConnectorWorker.inflight(worker) == nil
  end

  defp config(overrides \\ []), do: Config.new!(Keyword.merge(@cfg, overrides))

  defp start_bound(submit) do
    agent = Fake.start()
    table = :ets.new(:pr4e_journal, [:set, :public])

    {:ok, worker} =
      ConnectorWorker.start_link(
        config: config(quarantine_depth_alarm: 2),
        client: Fake,
        connection: Fake.connection(agent),
        connector_id: "alpha",
        store: {JournalStore, table},
        submit: submit,
        republish: &republish(agent, &1),
        name: nil
      )

    send(worker, {:smpp_bound, "alpha", :initial})
    _ = ConnectorWorker.inflight(worker)
    {worker, agent}
  end

  defp republish(agent, {:quarantine, %{connector_id: id}, _}),
    do: Fake.publish(%{agent: agent}, "", id <> ".quarantine", "", [])

  defp republish(_, _), do: :ok

  defp payload(overrides) do
    now = DateTime.to_iso8601(DateTime.utc_now())
    {:ok, envelope} = Envelope.new(Map.merge(Map.put(@base, :enqueued_at, now), overrides))
    elem(Envelope.encode(envelope), 1)
  end

  defp wait_reconnect(server, old) do
    Enum.find_value(1..50, fn _ ->
      case Connection.get(server) do
        {:ok, %{pid: pid}} when pid != old -> {:ok, %{pid: pid}}
        _ -> Process.sleep(5) && nil
      end
    end) || flunk("no reconnect")
  end

  defp refute_secrets(map) do
    refute inspect(map) =~ @secret
    refute Enum.any?([:password, :userinfo, :username], &Map.has_key?(map, &1))
  end

  defp stop(worker, agent) do
    if Process.alive?(worker), do: GenServer.stop(worker)
    Agent.stop(agent)
  end
end
