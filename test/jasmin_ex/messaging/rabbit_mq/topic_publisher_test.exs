defmodule JasminEx.Messaging.RabbitMQ.TopicPublisherTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.RabbitMQ.{Config, TopicPublisher}

  defmodule Fake do
    def start(script \\ %{}) do
      {:ok, agent} =
        Agent.start_link(fn ->
          %{script: script, events: [], next: 1, inflight: 0, max_inflight: 0, pids: %{}}
        end)

      agent
    end

    def events(agent), do: Agent.get(agent, &Enum.reverse(&1.events))
    def max_inflight(agent), do: Agent.get(agent, & &1.max_inflight)
    def connection(agent), do: %{pid: self(), agent: agent}

    def open_channel(%{agent: agent} = conn) do
      id = Agent.get_and_update(agent, &{&1.next, %{&1 | next: &1.next + 1}})
      pid = spawn(fn -> Process.sleep(:infinity) end)
      track(agent, {:open_channel, id})
      Agent.update(agent, &%{&1 | pids: Map.put(&1.pids, id, pid)})
      {:ok, Map.merge(conn, %{channel_id: id, pid: pid})}
    end

    def close_channel(%{agent: agent, channel_id: id, pid: pid}) do
      if Process.alive?(pid), do: Process.exit(pid, :shutdown)
      track(agent, {:close_channel, id})
      :ok
    end

    def select_confirms(%{agent: agent, channel_id: id}) do
      track(agent, {:select_confirms, id})
      :ok
    end

    def return(%{agent: agent} = ch, handler) do
      track(agent, {:return, ch.channel_id, handler})
      Agent.update(agent, &put_in(&1, [:script, :return_handler], handler))
      :ok
    end

    def publish(%{agent: agent} = ch, exchange, key, payload, opts) do
      Agent.update(agent, fn state ->
        inflight = state.inflight + 1
        %{state | inflight: inflight, max_inflight: max(state.max_inflight, inflight)}
      end)

      track(agent, {:publish, ch.channel_id, exchange, key, payload, opts})
      maybe_return(agent, payload)
      script = Agent.get(agent, & &1.script)

      result =
        case Map.get(script, :publish, :ok) do
          :channel_down -> {:error, :channel_closed}
          other -> other
        end

      Agent.update(agent, &%{&1 | inflight: &1.inflight - 1})
      result
    end

    def wait_for_confirms(%{agent: agent}, ms) do
      track(agent, {:wait_for_confirms, ms})

      value =
        Agent.get_and_update(agent, fn state ->
          case Map.get(state.script, :wait_for_confirms, true) do
            [next | rest] -> {next, put_in(state, [:script, :wait_for_confirms], rest)}
            other -> {other, state}
          end
        end)

      case value do
        :channel_down -> {:error, :channel_closed}
        other -> other
      end
    end

    defp maybe_return(agent, payload) do
      script = Agent.get(agent, & &1.script)

      if script[:basic_return] do
        handler = script[:return_handler] || self()
        send(handler, {:basic_return, payload, %{reply_text: "NO_ROUTE"}})
      end
    end

    defp track(agent, event),
      do: Agent.update(agent, fn state -> %{state | events: [event | state.events]} end)
  end

  setup do
    config =
      Config.new!(
        host: "b",
        username: "u",
        password: "p",
        queue_prefix: "jasmin.work",
        confirm_timeout_ms: 50
      )

    {:ok, config: config}
  end

  test "mandatory unroutable basic.return is failure even when a confirm arrives", %{
    config: config
  } do
    agent = Fake.start(%{wait_for_confirms: true, basic_return: true})
    {:ok, pub} = start(config, agent)

    assert {:error, :unroutable} =
             TopicPublisher.publish(pub, "dlr.missing", "payload")

    events = Fake.events(agent)
    assert {:publish, _, "messaging", "dlr.missing", "payload", opts} = find(events, :publish)
    assert opts[:mandatory] == true
    assert opts[:persistent] == true
    stop(pub, agent)
  end

  test "confirm timeout replaces the publishing channel before reuse", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: [:timeout, true]})
    {:ok, pub} = start(config, agent)

    assert {:ambiguous, :timeout} = TopicPublisher.publish(pub, "dlr.submit_sm_resp", "a")
    assert {:close_channel, 1} in Fake.events(agent)
    assert :ok = TopicPublisher.publish(pub, "dlr.submit_sm_resp", "b")
    assert {:open_channel, 2} in Fake.events(agent)
    stop(pub, agent)
  end

  test "connection loss is ambiguous and the channel is replaced", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: :channel_down})
    {:ok, pub} = start(config, agent)

    assert {:ambiguous, :channel_closed} =
             TopicPublisher.publish(pub, "dlr.submit_sm_resp", "body")

    assert {:close_channel, 1} in Fake.events(agent)
    stop(pub, agent)
  end

  test "one outstanding publish per channel", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = TopicPublisher.publish(pub, "dlr.submit_sm_resp", "one")
    assert :ok = TopicPublisher.publish(pub, "dlr.deliver_sm", "two")
    assert Fake.max_inflight(agent) == 1
    stop(pub, agent)
  end

  test "unknown routing keys are published and are not inspected by the publisher", %{
    config: config
  } do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = TopicPublisher.publish(pub, "dlr.not-a-known-kind", "body")

    events = Fake.events(agent)

    assert {:publish, _, "messaging", "dlr.not-a-known-kind", "body", _opts} =
             find(events, :publish)

    stop(pub, agent)
  end

  test "positive confirm without return is success", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = TopicPublisher.publish(pub, "dlr_thrower.http", "job")
    stop(pub, agent)
  end

  defp start(config, agent) do
    TopicPublisher.start_link(
      config: config,
      client: Fake,
      connection: Fake.connection(agent),
      name: nil
    )
  end

  defp stop(pub, agent) do
    GenServer.stop(pub)
    Agent.stop(agent)
  end

  defp find(events, kind) do
    Enum.find(events, fn
      tuple when is_tuple(tuple) -> elem(tuple, 0) == kind
      _ -> false
    end)
  end
end
