defmodule JasminEx.Dlr.WorkerTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Worker

  defmodule Fake do
    def start do
      {:ok, agent} =
        Agent.start_link(fn ->
          %{events: [], next: 1, pids: %{}, consumers: %{}}
        end)

      agent
    end

    def events(agent), do: Agent.get(agent, &Enum.reverse(&1.events))
    def connection(agent), do: %{pid: self(), agent: agent}
    def channel_pid(agent, id), do: Agent.get(agent, & &1.pids[id])

    def open_channel(%{agent: agent}) do
      id = Agent.get_and_update(agent, &{&1.next, %{&1 | next: &1.next + 1}})
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
      :ok
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
    Process.sleep(20)
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
