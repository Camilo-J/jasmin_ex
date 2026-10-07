defmodule JasminEx.Messaging.RabbitMQ.PublisherTest do
  use ExUnit.Case, async: true

  alias JasminEx.Billing.{Admission, Bill, Reservation}
  alias JasminEx.Messaging.RabbitMQ.{Client, Config, Publisher}
  alias JasminEx.Routing
  alias JasminEx.Routing.Config, as: RoutingConfig
  alias JasminEx.Routing.ConnectorRef
  alias JasminEx.Routing.Router

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
      script(agent, :declare_queue, {:ok, %{queue: name}})
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
    for {script, expected} <- [
          {%{wait_for_confirms: false}, {:error, :non_ok}},
          {%{wait_for_confirms: :timeout}, {:ambiguous, :timeout}},
          {%{wait_for_confirms: :channel_down}, {:ambiguous, :channel_closed}}
        ] do
      agent = Fake.start(script)
      {:ok, pub} = start(config, agent)
      assert ^expected = Publisher.publish(pub, "c", "body")
      stop(pub, agent)
    end
  end

  test "ambiguous confirmation is not success and is not definite non_ok", %{config: config} do
    for {script, reason} <- [
          {%{wait_for_confirms: :timeout}, :timeout},
          {%{wait_for_confirms: :channel_down}, :channel_closed}
        ] do
      agent = Fake.start(script)
      {:ok, pub} = start(config, agent)
      result = Publisher.publish(pub, "c", "body")
      assert {:ambiguous, ^reason} = result
      refute result == :ok
      refute result == {:error, :non_ok}
      stop(pub, agent)
    end
  end

  test "definite nack is non_ok enqueue failure", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: false})
    {:ok, pub} = start(config, agent)
    assert {:error, :non_ok} = Publisher.publish(pub, "c", "body")
    stop(pub, agent)
  end

  describe "ambiguous confirmation leaves reservation open" do
    @describetag :tmp_dir

    test "timeout confirm keeps the admitted reservation open", %{
      config: config,
      tmp_dir: tmp_dir
    } do
      {router, reservation} = admit_open_reservation(tmp_dir)
      agent = Fake.start(%{wait_for_confirms: :timeout})
      {:ok, pub} = start(config, agent)

      result = Publisher.publish(pub, "c", "body")
      assert {:ambiguous, :timeout} = result
      assert_reservation_left_open(router, reservation, result)

      stop(pub, agent)
    end

    test "channel closure confirm keeps the admitted reservation open", %{
      config: config,
      tmp_dir: tmp_dir
    } do
      {router, reservation} = admit_open_reservation(tmp_dir)
      agent = Fake.start(%{wait_for_confirms: :channel_down})
      {:ok, pub} = start(config, agent)

      result = Publisher.publish(pub, "c", "body")
      assert {:ambiguous, :channel_closed} = result
      assert_reservation_left_open(router, reservation, result)

      stop(pub, agent)
    end

    test "definite nack classifies settle_non_ok rather than leave_open", %{
      config: config,
      tmp_dir: tmp_dir
    } do
      {_router, reservation} = admit_open_reservation(tmp_dir)
      assert reservation.state == :open
      agent = Fake.start(%{wait_for_confirms: false})
      {:ok, pub} = start(config, agent)

      result = Publisher.publish(pub, "c", "body")
      assert {:error, :non_ok} = result
      assert Publisher.reservation_action(result) == :settle_non_ok
      refute Publisher.reservation_action(result) == :leave_open

      stop(pub, agent)
    end
  end

  test "confirm selection failure closes the newly opened channel", %{config: config} do
    agent = Fake.start(%{select_confirms: {:error, :channel_closed}})
    {:ok, pub} = start(config, agent)

    assert {:close_channel, 1} in Fake.events(agent)
    stop(pub, agent)
  end

  test "wait publication declares a durable quorum delay queue to the work queue", %{
    config: config
  } do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = Publisher.publish_retry(pub, "connector-a", "payload-wait")

    events = Fake.events(agent)

    assert {:declare_queue, "jasmin.work-retry.connector-a.wait", opts} =
             find(events, :declare_queue)

    assert_wait_queue(opts, "jasmin.work.connector-a")

    assert {:publish, "", "jasmin.work-retry.connector-a.wait", "payload-wait", popts} =
             find(events, :publish)

    assert popts[:persistent] == true
    assert find(events, :select_confirms)
    assert {:wait_for_confirms, 50} = find(events, :wait_for_confirms)
    stop(pub, agent)
  end

  test "wait publication uses the configured prefix for queue name and dead-letter key" do
    config =
      Config.new!(
        host: "b",
        username: "u",
        password: "p",
        queue_prefix: "custom.mt",
        confirm_timeout_ms: 50
      )

    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = Publisher.publish_retry(pub, "alpha", "payload-wait")

    events = Fake.events(agent)
    assert {:declare_queue, "custom.mt-retry.alpha.wait", opts} = find(events, :declare_queue)
    assert_wait_queue(opts, "custom.mt.alpha")

    assert {:publish, "", "custom.mt-retry.alpha.wait", "payload-wait", popts} =
             find(events, :publish)

    assert popts[:persistent] == true
    stop(pub, agent)
  end

  test "wait declaration failure does not publish or report success", %{config: config} do
    agent = Fake.start(%{declare_queue: {:error, :incompatible_queue_arguments}})
    {:ok, pub} = start(config, agent)

    assert {:error, :incompatible_queue_arguments} =
             Publisher.publish_retry(pub, "connector-a", "payload-wait")

    events = Fake.events(agent)

    assert {:declare_queue, "jasmin.work-retry.connector-a.wait", opts} =
             find(events, :declare_queue)

    assert_wait_queue(opts, "jasmin.work.connector-a")
    refute Enum.any?(events, &match?({:publish, _, _, _, _}, &1))
    refute Enum.any?(events, &match?({:wait_for_confirms, _}, &1))
    stop(pub, agent)
  end

  test "ordinary publish of ids ending in .wait stays classic", %{config: config} do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = Publisher.publish(pub, "foo.wait", "payload")

    events = Fake.events(agent)
    assert {:declare_queue, "jasmin.work.foo.wait", opts} = find(events, :declare_queue)
    assert_classic_queue(opts)
    assert {:publish, "", "jasmin.work.foo.wait", "payload", popts} = find(events, :publish)
    assert popts[:persistent] == true

    refute Enum.any?(events, fn
             {:declare_queue, "jasmin.work-retry." <> _, _} -> true
             _ -> false
           end)

    stop(pub, agent)
  end

  test "ordinary publish of retry.foo and foo.quarantine stays classic", %{config: config} do
    for connector_id <- ["retry.foo", "foo.quarantine"] do
      agent = Fake.start(%{wait_for_confirms: true})
      {:ok, pub} = start(config, agent)
      assert :ok = Publisher.publish(pub, connector_id, "payload")
      events = Fake.events(agent)

      assert {:declare_queue, "jasmin.work." <> ^connector_id, opts} =
               find(events, :declare_queue)

      assert_classic_queue(opts)
      stop(pub, agent)
    end
  end

  test "explicit retry publication uses a non-overlapping work-retry namespace", %{
    config: config
  } do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = Publisher.publish_retry(pub, "foo", "payload-wait")

    events = Fake.events(agent)
    assert {:declare_queue, "jasmin.work-retry.foo.wait", opts} = find(events, :declare_queue)
    assert_wait_queue(opts, "jasmin.work.foo")

    assert {:publish, "", "jasmin.work-retry.foo.wait", "payload-wait", popts} =
             find(events, :publish)

    assert popts[:persistent] == true

    refute Enum.any?(events, fn
             {:declare_queue, "jasmin.work.foo.wait", _} -> true
             _ -> false
           end)

    stop(pub, agent)
  end

  test "retry of a .wait connector id does not collide with that connector work queue", %{
    config: config
  } do
    agent = Fake.start(%{wait_for_confirms: true})
    {:ok, pub} = start(config, agent)
    assert :ok = Publisher.publish_retry(pub, "foo.wait", "payload-wait")

    events = Fake.events(agent)

    assert {:declare_queue, "jasmin.work-retry.foo.wait.wait", opts} =
             find(events, :declare_queue)

    assert_wait_queue(opts, "jasmin.work.foo.wait")

    refute Enum.any?(events, fn
             {:declare_queue, "jasmin.work.foo.wait", _} -> true
             _ -> false
           end)

    stop(pub, agent)
  end

  test "wait nack, timeout, and channel loss never report success", %{config: config} do
    for {script, expected} <- [
          {%{wait_for_confirms: false}, {:error, :non_ok}},
          {%{wait_for_confirms: :timeout}, {:ambiguous, :timeout}},
          {%{wait_for_confirms: :channel_down}, {:ambiguous, :channel_closed}}
        ] do
      agent = Fake.start(script)
      {:ok, pub} = start(config, agent)
      assert ^expected = Publisher.publish_retry(pub, "connector-a", "payload-wait")
      stop(pub, agent)
    end
  end

  defp assert_reservation_left_open(router, reservation, result) do
    assert Publisher.reservation_action(result) == :leave_open
    snapshot = Routing.snapshot(router)
    open = snapshot.reservations[reservation.bill_id]
    assert open.state == :open
    assert open == reservation
    assert snapshot.tombstones == %{}
  end

  defp admit_open_reservation(tmp_dir) do
    routing_config = RoutingConfig.new(snapshot_path: Path.join(tmp_dir, "routing.json"))
    router = start_supervised!({Router, name: nil, config: routing_config})
    {:ok, group} = Routing.put_group(router, gid: "ops")

    {:ok, _user} =
      Routing.put_user(router,
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: 500,
        submit_quota: 3
      )

    {:ok, connector} = ConnectorRef.new("smpp-t")

    {:ok, _route} =
      Routing.put_route(router,
        kind: :static,
        order: 10,
        connector: connector,
        filters: [],
        rate_minor: 100,
        precharge_percent: 10
      )

    {:ok, bill} =
      Bill.new(
        bill_id: "bill-1",
        uid: "u1",
        route_order: 10,
        rate_minor: 100,
        precharge_percent: 10
      )

    {:ok, admission} = Admission.new(bill: bill, ttl_ms: 1_000)
    assert {:ok, %Reservation{state: :open} = reservation} = Routing.admit(router, admission)
    {router, reservation}
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

  defp assert_wait_queue(opts, work_queue) do
    assert opts[:durable] == true
    args = Keyword.get(opts, :arguments, [])
    assert {"x-queue-type", :longstr, "quorum"} in args
    assert {"x-overflow", :longstr, "reject-publish"} in args
    assert {"x-dead-letter-exchange", :longstr, ""} in args
    assert {"x-dead-letter-routing-key", :longstr, work_queue} in args
    assert {"x-dead-letter-strategy", :longstr, "at-least-once"} in args
    assert {"x-message-ttl", :long, Client.wait_queue_ttl_ms()} in args
    refute {"x-queue-type", :longstr, "classic"} in args
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
