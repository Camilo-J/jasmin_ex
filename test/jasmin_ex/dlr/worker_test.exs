defmodule JasminEx.Dlr.WorkerTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Worker

  defmodule Fake do
    def start do
      {:ok, agent} =
        Agent.start_link(fn ->
          %{
            events: [],
            next: 1,
            pids: %{},
            consumers: %{},
            fail_first: false,
            fail_qos_once: false
          }
        end)

      agent
    end

    def events(agent), do: Agent.get(agent, &Enum.reverse(&1.events))
    def connection(agent), do: %{pid: self(), agent: agent}
    def channel_pid(agent, id), do: Agent.get(agent, & &1.pids[id])

    def open_channel(%{agent: agent}) do
      id = Agent.get_and_update(agent, &{&1.next, %{&1 | next: &1.next + 1}})
      fail? = Agent.get(agent, & &1.fail_first)

      if fail? and id == 1 do
        track(agent, {:open_failed, id})
        {:error, :disconnected}
      else
        pid = spawn(fn -> Process.sleep(:infinity) end)
        track(agent, {:open_channel, id})
        Agent.update(agent, &%{&1 | pids: Map.put(&1.pids, id, pid)})
        {:ok, %{agent: agent, channel_id: id, pid: pid}}
      end
    end

    def close_channel(%{agent: agent, channel_id: id, pid: pid}) do
      if Process.alive?(pid), do: Process.exit(pid, :shutdown), else: exit(:noproc)
      track(agent, {:close_channel, id})
      :ok
    end

    def qos(%{agent: agent, channel_id: id}, opts) do
      track(agent, {:qos, id, opts})
      if Agent.get(agent, & &1.fail_qos_once) and id == 1, do: {:error, :closed}, else: :ok
    end

    def consume(%{agent: agent, channel_id: id}, queue, consumer, opts) do
      track(agent, {:consume, id, queue, opts})
      Agent.update(agent, &%{&1 | consumers: Map.put(&1.consumers, id, consumer)})
      {:ok, "ctag-#{id}"}
    end

    def ack(%{agent: agent, channel_id: id}, tag) do
      track(agent, {:ack, id, tag})
      :ok
    end

    def reject(%{agent: agent, channel_id: id}, tag, opts) do
      track(agent, {:reject, id, tag, opts})
      :ok
    end

    def nack(%{agent: agent, channel_id: id}, tag, opts) do
      track(agent, {:nack, id, tag, opts})
      :ok
    end

    def publish(%{agent: agent}, exchange, key, payload, opts) do
      track(agent, {:publish, exchange, key, payload, opts})
      :ok
    end

    defp track(agent, event),
      do: Agent.update(agent, fn state -> %{state | events: [event | state.events]} end)
  end

  test "unknown routing key is rejected without requeue" do
    {worker, agent} = start_worker(:lookup, fn _payload, _meta -> :ok end)
    send_deliver(worker, "dlr.not-a-kind", 1)

    assert {:reject, 1, 1, requeue: false} = find(Fake.events(agent), :reject)
    refute find(Fake.events(agent), :ack)
    refute find(Fake.events(agent), :nack)
    stop(worker, agent)
  end

  test "HTTP worker rejects dlr_thrower.smpps without requeue and does not publish MT work" do
    {worker, agent} = start_worker(:http, fn _payload, _meta -> :ok end)
    send_deliver(worker, "dlr_thrower.smpps", 7, queue_kind: :http)

    events = Fake.events(agent)
    assert {:reject, 1, 7, requeue: false} = find(events, :reject)
    refute Enum.any?(events, &match?({:publish, "", _queue, _, _}, &1))
    refute Enum.any?(events, &match?({:nack, _, _, _}, &1))
    stop(worker, agent)
  end

  test "channel loss discards the in-flight tag and never settles it on a replacement channel" do
    parent = self()

    processor = fn _payload, _meta ->
      send(parent, :processing)
      Process.sleep(50)
      :ok
    end

    {worker, agent} = start_worker(:lookup, processor)
    send_deliver(worker, "dlr.submit_sm_resp", 11)
    assert_receive :processing, 200
    Process.exit(Fake.channel_pid(agent, 1), :kill)
    Process.sleep(80)

    events = Fake.events(agent)
    refute Enum.any?(events, &match?({:ack, 2, 11}, &1))
    refute Enum.any?(events, &match?({:reject, 2, 11, _}, &1))
    assert {:open_channel, 2} in events
    stop(worker, agent)
  end

  test "redelivered exhausted budget is terminal and is not reset by the worker" do
    {worker, agent} =
      start_worker(:lookup, fn _payload, _meta ->
        flunk("processor must not run when exhausted")
      end)

    send_deliver(worker, "dlr.deliver_sm", 3,
      redelivered: true,
      headers: [{"x-delivery-count", :long, 3}]
    )

    assert {:reject, 1, 3, requeue: false} = find(Fake.events(agent), :reject)
    stop(worker, agent)
  end

  test "worker has no local retry counter that would reset a delayed redelivery" do
    calls = :counters.new(1, [])

    {worker, agent} =
      start_worker(:lookup, fn _payload, _meta ->
        :counters.add(calls, 1, 1)
        :retry
      end)

    send_deliver(worker, "dlr.deliver_sm", 4,
      redelivered: true,
      headers: [{"x-delivery-count", :long, 2}]
    )

    assert :counters.get(calls, 1) == 1
    assert {:reject, 1, 4, requeue: false} = find(Fake.events(agent), :reject)
    stop(worker, agent)
  end

  test "prefetch is 1 and lookup processor success is acked" do
    {worker, agent} =
      start_worker(:lookup, fn payload, meta ->
        assert payload == "ok-body"
        assert meta.routing_key == "dlr.submit_sm_resp"
        :ok
      end)

    send_deliver(worker, "dlr.submit_sm_resp", 5)
    events = Fake.events(agent)
    assert {:qos, 1, [prefetch_count: 1]} = find(events, :qos)
    assert {:ack, 1, 5} = find(events, :ack)
    stop(worker, agent)
  end

  test "initial connection failure retries and eventually subscribes without restarting the worker" do
    agent = Fake.start()
    Agent.update(agent, &%{&1 | fail_first: true})

    {:ok, worker} =
      Worker.start_link(
        queue_kind: :lookup,
        queue: "jasmin_ex.dlr.lookup.v1",
        processor: fn _, _ -> :ok end,
        client: Fake,
        connection: Fake.connection(agent),
        reconnect_backoff_ms: 25,
        name: nil
      )

    assert {:open_failed, 1} in Fake.events(agent)
    assert eventually(fn -> Enum.any?(Fake.events(agent), &match?({:consume, 2, _, _}, &1)) end)
    stop(worker, agent)
  end

  test "a QoS setup failure closes its channel before retrying subscription" do
    agent = Fake.start()
    Agent.update(agent, &%{&1 | fail_qos_once: true})

    {:ok, worker} =
      Worker.start_link(
        queue_kind: :lookup,
        queue: "jasmin_ex.dlr.lookup.v1",
        processor: fn _, _ -> :ok end,
        client: Fake,
        connection: Fake.connection(agent),
        reconnect_backoff_ms: 25,
        name: nil
      )

    assert eventually(fn -> Enum.any?(Fake.events(agent), &match?({:consume, 2, _, _}, &1)) end)
    assert {:close_channel, 1} in Fake.events(agent)
    refute Process.alive?(Fake.channel_pid(agent, 1))
    stop(worker, agent)
  end

  test "configured additional attempts apply to redelivered lookup work" do
    agent = Fake.start()

    {:ok, worker} =
      Worker.start_link(
        queue_kind: :lookup,
        queue: "jasmin_ex.dlr.lookup.v1",
        processor: fn _, _ -> :retry end,
        client: Fake,
        connection: Fake.connection(agent),
        additional_attempts: 5,
        name: nil
      )

    send_deliver(worker, "dlr.deliver_sm", 8,
      redelivered: true,
      headers: [{"x-delivery-count", :long, 3}]
    )

    assert {:reject, 1, 8, requeue: true} = find(Fake.events(agent), :reject)
    stop(worker, agent)
  end

  test "supervisor shutdown closes the worker channel instead of leaving an orphan consumer" do
    agent = Fake.start()
    connection = Fake.connection(agent)

    {:ok, supervisor} =
      Supervisor.start_link(
        [
          {Worker,
           [
             queue_kind: :lookup,
             queue: "jasmin_ex.dlr.lookup.v1",
             processor: fn _, _ -> :ok end,
             client: Fake,
             connection: connection,
             name: nil
           ]}
        ],
        strategy: :one_for_one
      )

    channel_pid = Fake.channel_pid(agent, 1)
    assert Process.alive?(channel_pid)
    :ok = Supervisor.stop(supervisor)
    refute Process.alive?(channel_pid)
    Agent.stop(agent)
  end

  defp eventually(predicate) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    await(predicate, deadline)
  end

  defp await(predicate, deadline) do
    cond do
      predicate.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(10)
        await(predicate, deadline)
    end
  end

  defp start_worker(kind, processor) do
    agent = Fake.start()
    queue = if kind == :lookup, do: "jasmin_ex.dlr.lookup.v1", else: "jasmin_ex.dlr.http.v1"

    {:ok, worker} =
      Worker.start_link(
        queue_kind: kind,
        queue: queue,
        processor: processor,
        client: Fake,
        connection: Fake.connection(agent),
        name: nil
      )

    {worker, agent}
  end

  defp send_deliver(worker, routing_key, tag, opts \\ []) do
    meta = %{
      delivery_tag: tag,
      redelivered: Keyword.get(opts, :redelivered, false),
      routing_key: routing_key,
      headers: Keyword.get(opts, :headers, :undefined)
    }

    send(worker, {:basic_deliver, "ok-body", meta})
    _state = :sys.get_state(worker)
  end

  defp find(events, kind) do
    Enum.find(events, fn
      tuple when is_tuple(tuple) -> elem(tuple, 0) == kind
      _ -> false
    end)
  end

  defp stop(worker, agent) do
    if Process.alive?(worker), do: GenServer.stop(worker)
    if Process.alive?(agent), do: Agent.stop(agent)
  end
end
