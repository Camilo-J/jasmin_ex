defmodule JasminEx.Messaging.RabbitMQ.ConnectorWorkerTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.RabbitMQ.{Config, Connection, ConnectorWorker}

  defmodule Fake do
    def start(fail_at \\ nil) do
      {:ok, agent} =
        Agent.start_link(fn ->
          %{events: [], next: 1, prefetch: nil, consumers: %{}, pids: %{}, fail_at: fail_at}
        end)

      agent
    end

    def events(agent), do: Agent.get(agent, &Enum.reverse(&1.events))
    def connection(agent), do: %{pid: self(), agent: agent}

    def open_connection(opts) do
      {:ok, pid} = Agent.start_link(fn -> :open end)
      {:ok, %{pid: pid, agent: Keyword.fetch!(opts, :channel_agent)}}
    end

    def close_connection(%{pid: pid}) do
      if Process.alive?(pid), do: Agent.stop(pid)
      :ok
    catch
      :exit, _ -> :ok
    end

    def connection_pid(%{pid: pid}), do: pid
    def channel_pid(agent, id), do: Agent.get(agent, & &1.pids[id])

    def open_channel(%{agent: agent}) do
      id = next_id(agent)
      pid = spawn(fn -> Process.sleep(:infinity) end)
      track(agent, {:open_channel, id})
      Agent.update(agent, &%{&1 | pids: Map.put(&1.pids, id, pid)})
      {:ok, %{agent: agent, channel_id: id, pid: pid}}
    end

    def close_channel(%{agent: agent, channel_id: id, pid: pid}) do
      if Process.alive?(pid), do: Process.exit(pid, :shutdown)
      track(agent, {:close_channel, id})
      :ok
    end

    def qos(%{agent: agent, channel_id: id}, opts) do
      track(agent, {:qos, id, opts})

      case Agent.get(agent, & &1.fail_at) do
        :qos ->
          {:error, :qos_failed}

        _ ->
          Agent.update(agent, &%{&1 | prefetch: Keyword.get(opts, :prefetch_count)})
          :ok
      end
    end

    def consume(%{agent: agent, channel_id: id}, queue, consumer, opts) do
      tag = "ctag-#{id}"
      track(agent, {:consume, id, queue, consumer, opts})

      case Agent.get(agent, & &1.fail_at) do
        :consume ->
          {:error, :consume_failed}

        _ ->
          Agent.update(agent, &%{&1 | consumers: Map.put(&1.consumers, id, {consumer, tag})})
          {:ok, tag}
      end
    end

    def cancel(%{agent: agent, channel_id: id}, tag) do
      track(agent, {:cancel, id, tag})
      Agent.update(agent, &%{&1 | consumers: Map.delete(&1.consumers, id)})
      {:ok, tag}
    end

    def deliver(agent, payload) do
      Agent.get_and_update(agent, fn state ->
        unacked = Map.get(state, :unacked, 0)

        case {(state.prefetch || 0) > 0 and unacked < state.prefetch, Map.values(state.consumers)} do
          {true, [{pid, _} | _]} ->
            send(pid, {:basic_deliver, payload, %{delivery_tag: unacked + 1}})
            {:ok, Map.put(state, :unacked, unacked + 1)}

          _ ->
            {:rejected, state}
        end
      end)
    end

    defp next_id(agent),
      do:
        Agent.get_and_update(agent, fn state -> {state.next, %{state | next: state.next + 1}} end)

    defp track(agent, event),
      do: Agent.update(agent, fn state -> %{state | events: [event | state.events]} end)
  end

  setup do
    {:ok,
     config:
       Config.new!(
         host: "b",
         username: "u",
         password: "p",
         queue_prefix: "jasmin.work",
         confirm_timeout_ms: 50
       )}
  end

  test "does not consume before a matching smpp_bound", %{config: config} do
    {worker, agent} = start(config, "alpha")
    assert Fake.events(agent) == []
    send(worker, {:smpp_bound, "beta", :initial})
    assert ConnectorWorker.inflight(worker) == nil
    assert Fake.events(agent) == []
    stop(worker, agent)
  end

  test "opens a dedicated prefetch-1 manual-ack consumer after bind", %{config: config} do
    {alpha, beta, agent} = start_pair(config)
    send(alpha, {:smpp_bound, "alpha", :initial})
    _ = ConnectorWorker.inflight(alpha)
    send(beta, {:smpp_bound, "beta", :reconnect})
    _ = ConnectorWorker.inflight(beta)
    events = Fake.events(agent)
    assert {:qos, 1, [prefetch_count: 1]} = find(events, :qos)
    assert {:consume, 1, "jasmin.work.alpha", ^alpha, aopts} = find(events, :consume)
    assert aopts[:no_ack] == false
    assert {:qos, 2, [prefetch_count: 1]} = find(events, :qos, 1)
    assert {:consume, 2, "jasmin.work.beta", ^beta, bopts} = find(events, :consume, 1)
    assert bopts[:no_ack] == false
    assert {:open_channel, 1} in events and {:open_channel, 2} in events
    stop_pair(alpha, beta, agent)
  end

  test "cancels and closes only this channel on matching bind_lost", %{config: config} do
    {alpha, beta, agent} = start_pair(config)
    bind_both(alpha, beta)
    send(alpha, {:smpp_bind_lost, "alpha", :tcp_closed})
    _ = ConnectorWorker.inflight(alpha)
    events = Fake.events(agent)
    assert {:cancel, 1, "ctag-1"} in events
    assert {:close_channel, 1} in events
    refute {:cancel, 2, "ctag-2"} in events
    refute {:close_channel, 2} in events
    assert {:consume, 2, "jasmin.work.beta", ^beta, _} = find(events, :consume, 1)
    stop_pair(alpha, beta, agent)
  end

  test "ignores irrelevant lifecycle messages and repeated events", %{config: config} do
    {worker, agent} = start(config, "alpha")
    send(worker, {:smpp_bound, "alpha", :initial})
    _ = ConnectorWorker.inflight(worker)
    send(worker, {:smpp_bound, "alpha", :reconnect})
    send(worker, {:smpp_bind_lost, "gamma", :tcp_closed})
    send(worker, {:smpp_bound, "beta", :initial})
    _ = ConnectorWorker.inflight(worker)
    events = Fake.events(agent)
    assert Enum.count(events, &match?({:open_channel, _}, &1)) == 1
    assert Enum.count(events, &match?({:consume, _, _, _, _}, &1)) == 1
    send(worker, {:smpp_bind_lost, "alpha", :tcp_closed})
    send(worker, {:smpp_bind_lost, "alpha", :tcp_closed})
    _ = ConnectorWorker.inflight(worker)
    events = Fake.events(agent)
    assert Enum.count(events, &match?({:cancel, _, _}, &1)) == 1
    assert Enum.count(events, &match?({:close_channel, _}, &1)) == 1
    stop(worker, agent)
  end

  test "retains at most one opaque delivery without SMPP dispatch", %{config: config} do
    {worker, agent} = start(config, "alpha")
    send(worker, {:smpp_bound, "alpha", :initial})
    _ = ConnectorWorker.inflight(worker)
    assert :ok = Fake.deliver(agent, "one")
    assert {:basic_deliver, "one", %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    assert :rejected = Fake.deliver(agent, "two")
    assert {:basic_deliver, "one", %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    refute_received {:submit_sm, _}
    events = Fake.events(agent)
    refute Enum.any?(events, &match?({:ack, _, _}, &1))
    refute Enum.any?(events, &match?({:reject, _, _}, &1))
    stop(worker, agent)
  end

  test "closes the opened channel when qos or consume setup fails", %{config: config} do
    for fail_at <- [:qos, :consume] do
      {worker, agent} = start(config, "alpha", fail_at)
      send(worker, {:smpp_bound, "alpha", :initial})
      assert ConnectorWorker.inflight(worker) == nil
      assert Process.alive?(worker)
      pid = Fake.channel_pid(agent, 1)
      events = Fake.events(agent)
      assert {:open_channel, 1} in events
      assert {:qos, 1, [prefetch_count: 1]} = find(events, :qos)
      assert {:close_channel, 1} in events
      refute Process.alive?(pid)
      refute Enum.any?(events, &match?({:cancel, _, _}, &1))

      assert Enum.count(events, &match?({:consume, _, _, _, _}, &1)) ==
               if(fail_at == :consume, do: 1, else: 0)

      stop(worker, agent)
    end
  end

  test "channel failure of A leaves B and Connection alive", %{config: config} do
    agent = Fake.start()

    {:ok, conn} =
      Connection.start_link(
        config: config,
        client: Fake,
        client_opts: [channel_agent: agent],
        name: nil
      )

    {:ok, alpha} = start_one(config, "alpha", nil, connection_server: conn)
    {:ok, beta} = start_one(config, "beta", nil, connection_server: conn)
    bind_both(alpha, beta)
    pid = Fake.channel_pid(agent, 1)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    _ = ConnectorWorker.inflight(alpha)
    assert Process.alive?(beta) and Process.alive?(conn)
    assert {:ok, _} = Connection.get(conn)
    events = Fake.events(agent)
    refute {:cancel, 2, "ctag-2"} in events
    refute {:close_channel, 2} in events
    assert {:consume, 2, "jasmin.work.beta", ^beta, _} = find(events, :consume, 1)
    stop_pair(alpha, beta, agent)
    GenServer.stop(conn)
  end

  defp start(config, id, fail_at \\ nil) do
    agent = Fake.start(fail_at)
    {:ok, worker} = start_one(config, id, Fake.connection(agent))
    {worker, agent}
  end

  defp start_pair(config) do
    agent = Fake.start()
    conn = Fake.connection(agent)
    {:ok, alpha} = start_one(config, "alpha", conn)
    {:ok, beta} = start_one(config, "beta", conn)
    {alpha, beta, agent}
  end

  defp start_one(config, connector_id, connection, extra \\ []) do
    ConnectorWorker.start_link(
      Keyword.merge(
        [
          config: config,
          client: Fake,
          connection: connection,
          connector_id: connector_id,
          name: nil
        ],
        extra
      )
    )
  end

  defp bind_both(alpha, beta) do
    send(alpha, {:smpp_bound, "alpha", :initial})
    _ = ConnectorWorker.inflight(alpha)
    send(beta, {:smpp_bound, "beta", :initial})
    _ = ConnectorWorker.inflight(beta)
  end

  defp find(events, kind, skip \\ 0) do
    events
    |> Enum.filter(&(is_tuple(&1) and elem(&1, 0) == kind))
    |> Enum.drop(skip)
    |> List.first() || flunk("missing #{kind}")
  end

  defp stop_pair(alpha, beta, agent) do
    if Process.alive?(alpha), do: GenServer.stop(alpha)
    if Process.alive?(beta), do: GenServer.stop(beta)
    Agent.stop(agent)
  end

  defp stop(worker, agent) do
    if Process.alive?(worker), do: GenServer.stop(worker)
    Agent.stop(agent)
  end
end
