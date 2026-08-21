defmodule JasminEx.Messaging.RabbitMQ.PublisherTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.RabbitMQ.{Config, Publisher}

  defmodule Fake do
    def open_channel(%{agent: agent} = conn) do
      track(agent, :open_channel)
      {:ok, Map.put(conn, :channel_id, 1)}
    end

    def close_channel(%{agent: agent, channel_id: channel_id}) do
      track(agent, {:close_channel, channel_id})
      :ok
    end

    def select_confirms(%{agent: agent} = ch) do
      track(agent, {:select_confirms, ch.channel_id})
      script(agent, :select_confirms, :ok)
    end

    def declare_queue(%{agent: agent}, name, opts) do
      track(agent, {:declare_queue, name, opts})
      {:ok, %{queue: name}}
    end

    def publish(%{agent: agent}, exchange, key, payload, opts) do
      track(agent, {:publish, exchange, key, payload, opts})
      script(agent, :publish, :ok)
    end

    def wait_for_confirms(%{agent: agent}, ms) do
      track(agent, {:wait_for_confirms, ms})

      case script(agent, :wait_for_confirms, true) do
        :channel_down -> {:error, :channel_closed}
        other -> other
      end
    end

    def start(script) do
      {:ok, agent} = Agent.start_link(fn -> %{script: script, events: []} end)
      agent
    end

    def events(agent), do: Agent.get(agent, &Enum.reverse(&1.events))
    def connection(agent), do: %{pid: self(), agent: agent}

    defp script(agent, key, default),
      do: Agent.get(agent, fn state -> Map.get(state.script, key, default) end)

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

  test "durable confirmed publish succeeds only after positive confirm", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = Publisher.publish(pub, "connector-a", "payload-1")

    events = Fake.events(agent)
    assert {:declare_queue, "jasmin.work.connector-a", opts} = find(events, :declare_queue)
    assert_classic_queue(opts)
    assert {:publish, "", "jasmin.work.connector-a", "payload-1", popts} = find(events, :publish)
    assert popts[:persistent] == true
    assert find(events, :select_confirms)
    assert {:wait_for_confirms, 50} = find(events, :wait_for_confirms)
    stop(pub, agent)
  end

  test "quarantine publication declares a durable classic queue", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = Publisher.publish(pub, "connector-a.quarantine", "payload-q")

    events = Fake.events(agent)

    assert {:declare_queue, "jasmin.work.connector-a.quarantine", opts} =
             find(events, :declare_queue)

    assert_classic_queue(opts)
    stop(pub, agent)
  end

  test "nack, timeout, and channel loss never report success", %{config: config} do
    for {script, reason} <- [
          {%{wait_for_confirms: false}, :nack},
          {%{wait_for_confirms: :timeout}, :timeout},
          {%{wait_for_confirms: :channel_down}, :channel_closed}
        ] do
      agent = Fake.start(script)
      {:ok, pub} = start(config, agent)
      assert {:error, ^reason} = Publisher.publish(pub, "c", "body")
      stop(pub, agent)
    end
  end

  test "confirm selection failure closes the newly opened channel", %{config: config} do
    agent = Fake.start(%{select_confirms: {:error, :channel_closed}})
    {:ok, pub} = start(config, agent)

    assert {:close_channel, 1} in Fake.events(agent)
    stop(pub, agent)
  end

  defp start(config, agent) do
    Publisher.start_link(
      config: config,
      client: Fake,
      connection: Fake.connection(agent),
      name: nil
    )
  end

  defp assert_classic_queue(opts) do
    assert opts[:durable] == true
    assert {"x-queue-type", :longstr, "classic"} in Keyword.get(opts, :arguments, [])
  end

  defp find(events, kind),
    do:
      Enum.find(events, fn term -> is_tuple(term) and elem(term, 0) == kind end) ||
        flunk("missing #{kind}")

  defp stop(pub, agent) do
    GenServer.stop(pub)
    Agent.stop(agent)
  end
end
