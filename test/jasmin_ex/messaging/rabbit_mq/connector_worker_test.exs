defmodule JasminEx.Messaging.RabbitMQ.ConnectorWorkerTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.{Envelope, StateStoreJournal}
  alias JasminEx.Messaging.RabbitMQ.{Config, Connection, ConnectorWorker}

  @future "2099-01-01T00:00:00Z"
  @past "2020-01-01T00:00:00Z"

  defmodule JournalStore do
    def put(table, key, value, ttl_ms) do
      true = :ets.insert(table, {key, value, ttl_ms})
      :ok
    end

    def fetch(table, key) do
      case :ets.lookup(table, key) do
        [{^key, value, _ttl_ms}] -> {:ok, value}
        [] -> :missing
      end
    end
  end

  defmodule FailingStore do
    def put(a, _k, _v, _t) when is_pid(a), do: Agent.get_and_update(a, &List.pop_at(&1, 0))
    def put(_table, _key, _value, _ttl_ms), do: {:error, :unavailable}
    def fetch(_table, _key), do: :missing
  end

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
        next_tag = Map.get(state, :next_tag, 1)

        case {(state.prefetch || 0) > 0 and unacked < state.prefetch, Map.values(state.consumers)} do
          {true, [{pid, _} | _]} ->
            send(pid, {:basic_deliver, payload, %{delivery_tag: next_tag}})
            {:ok, Map.merge(state, %{unacked: unacked + 1, next_tag: next_tag + 1})}

          _ ->
            {:rejected, state}
        end
      end)
    end

    def ack(%{agent: agent, channel_id: id}, tag) do
      track(agent, {:ack, id, tag})

      case Agent.get(agent, & &1.fail_at) do
        :ack ->
          {:error, :channel_closed}

        _ ->
          Agent.update(agent, &%{&1 | unacked: max(Map.get(&1, :unacked, 0) - 1, 0)})
          :ok
      end
    end

    def reject(%{agent: agent, channel_id: id}, tag, opts) do
      track(agent, {:reject, id, tag, opts})

      case Agent.get(agent, & &1.fail_at) do
        :reject ->
          {:error, :channel_closed}

        _ ->
          Agent.update(agent, &%{&1 | unacked: max(Map.get(&1, :unacked, 0) - 1, 0)})
          :ok
      end
    end

    def record(agent, event), do: track(agent, event)

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

  test "retains at most one unsubmitted valid delivery without store or submit", %{
    config: config
  } do
    {worker, agent} = start(config, "alpha")
    send(worker, {:smpp_bound, "alpha", :initial})
    _ = ConnectorWorker.inflight(worker)
    {_envelope, payload} = valid_payload(%{gateway_id: "gw-hold"})
    assert :ok = Fake.deliver(agent, payload)
    assert {:basic_deliver, ^payload, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    assert :rejected = Fake.deliver(agent, "two")
    assert {:basic_deliver, ^payload, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    refute_received {:submit_sm, _}
    events = Fake.events(agent)
    refute Enum.any?(events, &match?({:ack, _, _}, &1))
    refute Enum.any?(events, &match?({:reject, _, _, _}, &1))
    stop(worker, agent)
  end

  test "leaves opaque bodies unsubmitted when a submit stub is injected", %{config: config} do
    {store, _} = journal_store()
    test = self()
    submit = fn env -> send(test, {:submitted, env}) && {:ok, "nope"} end
    {worker, agent} = start_bound(config, "alpha", store: store, submit: submit)
    assert :ok = Fake.deliver(agent, "one")
    assert ConnectorWorker.inflight(worker) == nil
    refute_received {:submitted, _}
    assert StateStoreJournal.read(store, "gateway-1", 1) == :missing
    events = Fake.events(agent)
    assert {:reject, 1, 1, [requeue: false]} in events
    refute Enum.any?(events, &match?({:ack, _, _}, &1))
    stop(worker, agent)
  end

  test "journals a valid v1 envelope before submit and acks known success", %{config: config} do
    {store, _} = journal_store()
    test = self()

    submit = fn env ->
      send(
        test,
        {:submit_seen, env.gateway_id, StateStoreJournal.read(store, env.gateway_id, env.attempt)}
      )

      {:ok, "smsc-ok"}
    end

    {worker, agent} = start_bound(config, "alpha", store: store, submit: submit)
    {_envelope, payload} = valid_payload(%{gateway_id: "gw-ok", attempt: 1})
    assert :ok = Fake.deliver(agent, payload)
    assert ConnectorWorker.inflight(worker) == nil

    assert_received {:submit_seen, "gw-ok",
                     {:ok, %{state: :dispatching, gateway_id: "gw-ok", attempt: 1}}}

    assert {:ok, %{state: :dispatching}} = StateStoreJournal.read(store, "gw-ok", 1)
    assert {:ack, 1, 1} in Fake.events(agent)
    {_second, payload2} = valid_payload(%{gateway_id: "gw-ok-2", attempt: 1})
    assert :ok = Fake.deliver(agent, payload2)
    assert ConnectorWorker.inflight(worker) == nil
    assert_received {:submit_seen, "gw-ok-2", {:ok, %{state: :dispatching}}}
    assert {:ack, 1, 2} in Fake.events(agent)
    stop(worker, agent)
  end

  test "rejects explicit submit rejection without requeue", %{config: config} do
    {store, _} = journal_store()
    submit = fn _env -> {:error, {:submit_rejected, :ESME_RINVDESTADR}} end
    {worker, agent} = start_bound(config, "alpha", store: store, submit: submit)
    {_envelope, payload} = valid_payload(%{gateway_id: "gw-rej"})
    assert :ok = Fake.deliver(agent, payload)
    assert ConnectorWorker.inflight(worker) == nil
    events = Fake.events(agent)
    assert {:reject, 1, 1, [requeue: false]} in events
    refute Enum.any?(events, &match?({:ack, _, _}, &1))
    stop(worker, agent)
  end

  test "settles classified inflight before cancel and close", %{config: config} do
    for {result, expected} <- [
          {{:ok, "smsc-td"}, {:ack, 1, 1}},
          {{:error, {:submit_rejected, :ESME_RINVDESTADR}}, {:reject, 1, 1, [requeue: false]}}
        ] do
      {store, _} = journal_store()
      {worker, agent} = start_bound(config, "alpha", store: store, submit: fn _ -> result end)
      {_envelope, payload} = valid_payload(%{gateway_id: "gw-td"})
      assert :ok = Fake.deliver(agent, payload)
      _ = ConnectorWorker.inflight(worker)
      send(worker, {:smpp_bind_lost, "alpha", :tcp_closed})
      assert ConnectorWorker.inflight(worker) == nil
      events = Fake.events(agent)
      settle_at = Enum.find_index(events, &(&1 == expected))
      cancel_at = Enum.find_index(events, &match?({:cancel, 1, _}, &1))
      close_at = Enum.find_index(events, &match?({:close_channel, 1}, &1))
      assert is_integer(settle_at)
      assert is_integer(cancel_at) and settle_at < cancel_at
      assert is_integer(close_at) and settle_at < close_at
      stop(worker, agent)
    end
  end

  test "does not clear inflight or complete teardown when broker settlement fails", %{
    config: config
  } do
    for {fail_at, result, expected_event, decision} <- [
          {:ack, {:ok, "smsc-ok"}, {:ack, 1, 1}, :ack},
          {:reject, {:error, {:submit_rejected, :ESME_RINVDESTADR}},
           {:reject, 1, 1, [requeue: false]}, :reject}
        ] do
      {store, _} = journal_store()

      {worker, agent} =
        start_bound(config, "alpha", [store: store, submit: fn _ -> result end], fail_at)

      {_envelope, payload} = valid_payload(%{gateway_id: "gw-settle-fail"})
      assert :ok = Fake.deliver(agent, payload)
      assert {:classified, ^decision, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
      {_next, payload2} = valid_payload(%{gateway_id: "gw-next"})
      assert :rejected = Fake.deliver(agent, payload2)
      events = Fake.events(agent)
      assert expected_event in events
      refute {:cancel, 1, "ctag-1"} in events
      refute {:close_channel, 1} in events
      send(worker, {:smpp_bind_lost, "alpha", :tcp_closed})
      assert {:classified, ^decision, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
      events = Fake.events(agent)
      refute {:cancel, 1, "ctag-1"} in events
      refute {:close_channel, 1} in events
      pid = Fake.channel_pid(agent, 1)
      if is_pid(pid) and Process.alive?(pid), do: Process.exit(pid, :kill)
      stop(worker, agent)
    end
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

  test "retries proven never-reached work only after confirmed republish", %{config: config} do
    for {result, republish_result, expect_ack?} <- [
          {{:error, :disconnected}, :ok, true},
          {{:error, :unbinding}, {:error, :timeout}, false}
        ] do
      {store, _} = journal_store()

      {worker, agent} =
        start_bound(config, "alpha", fn agent ->
          [
            store: store,
            submit: fn _ -> result end,
            republish: fn action ->
              Fake.record(agent, {:republish, action})
              republish_result
            end
          ]
        end)

      {_envelope, payload} =
        valid_payload(%{
          gateway_id: "gw-retry",
          attempt: 1,
          max_attempts: 3,
          expires_at: @future
        })

      assert :ok = Fake.deliver(agent, payload)
      _ = ConnectorWorker.inflight(worker)
      events = Fake.events(agent)
      assert {:republish, {:retry, next}} = find(events, :republish)
      assert next.attempt == 2
      assert next.gateway_id == "gw-retry"
      assert next.max_attempts == 3
      assert next.expires_at == @future
      refute Enum.any?(events, &match?({:republish, {:quarantine, _, _}}, &1))

      if expect_ack? do
        assert ConnectorWorker.inflight(worker) == nil
        republish_at = Enum.find_index(events, &match?({:republish, {:retry, _}}, &1))
        ack_at = Enum.find_index(events, &(&1 == {:ack, 1, 1}))
        assert is_integer(republish_at) and is_integer(ack_at) and republish_at < ack_at
        assert {:ok, %{state: :not_sent}} = StateStoreJournal.read(store, "gw-retry", 1)
      else
        assert {:basic_deliver, ^payload, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
        refute Enum.any?(events, &match?({:ack, _, _}, &1))
      end

      stop(worker, agent)
    end
  end

  test "does not retry exhausted attempt or expired envelopes", %{config: config} do
    for {overrides, result} <- [
          {%{gateway_id: "gw-max", attempt: 3, max_attempts: 3, expires_at: @future},
           {:error, :disconnected}},
          {%{gateway_id: "gw-exp", attempt: 1, max_attempts: 3, expires_at: @past},
           {:error, :unbinding}}
        ] do
      {store, _} = journal_store()

      {worker, agent} =
        start_bound(config, "alpha", fn agent ->
          [store: store, submit: fn _ -> result end, republish: republish_ok(agent)]
        end)

      {_envelope, payload} = valid_payload(overrides)
      assert :ok = Fake.deliver(agent, payload)
      assert ConnectorWorker.inflight(worker) == nil
      events = Fake.events(agent)
      assert {:republish, {:quarantine, env, evidence}} = find(events, :republish)
      assert env.gateway_id == overrides.gateway_id
      assert env.connector_id == "alpha"
      assert evidence.stage == :pre_write
      assert evidence.gateway_id == env.gateway_id
      assert evidence.connector_id == env.connector_id
      assert evidence.source_addr == env.submit_sm.source_addr
      assert evidence.destination_addr == env.submit_sm.destination_addr
      refute Enum.any?(events, &match?({:republish, {:retry, _}}, &1))
      republish_at = Enum.find_index(events, &match?({:republish, {:quarantine, _, _}}, &1))
      ack_at = Enum.find_index(events, &(&1 == {:ack, 1, 1}))
      assert is_integer(republish_at) and is_integer(ack_at) and republish_at < ack_at
      stop(worker, agent)
    end
  end

  test "quarantines timeout and uncertain outcomes after confirmed publish", %{config: config} do
    for {result, reason} <- [
          {{:unknown, :response_timeout}, :response_timeout},
          {{:unknown, :disconnected}, :disconnected}
        ] do
      {store, _} = journal_store()

      {worker, agent} =
        start_bound(config, "alpha", fn agent ->
          [store: store, submit: fn _ -> result end, republish: republish_ok(agent)]
        end)

      {_envelope, payload} =
        valid_payload(%{gateway_id: "gw-q", expires_at: @future})

      assert :ok = Fake.deliver(agent, payload)
      assert ConnectorWorker.inflight(worker) == nil
      events = Fake.events(agent)
      assert {:republish, {:quarantine, env, evidence}} = find(events, :republish)
      assert env.gateway_id == "gw-q"
      assert evidence.stage == :post_write
      assert evidence.reason == reason
      assert evidence.gateway_id == "gw-q"
      assert evidence.connector_id == "alpha"
      assert evidence.source_addr == env.submit_sm.source_addr
      assert evidence.destination_addr == env.submit_sm.destination_addr
      refute Enum.any?(events, &match?({:republish, {:retry, _}}, &1))
      republish_at = Enum.find_index(events, &match?({:republish, {:quarantine, _, _}}, &1))
      ack_at = Enum.find_index(events, &(&1 == {:ack, 1, 1}))
      assert is_integer(republish_at) and is_integer(ack_at) and republish_at < ack_at
      assert {:ok, %{state: :sent, evidence: stored}} = StateStoreJournal.read(store, "gw-q", 1)
      assert stored["stage"] == "post_write"
      assert stored["reason"] == Atom.to_string(reason)
      stop(worker, agent)
    end
  end

  test "rejects unsupported and malformed envelopes without silent drop", %{config: config} do
    test = self()

    for payload <- [
          ~s({"version":2,"connector_id":"alpha"}),
          "one"
        ] do
      {store, _} = journal_store()

      {worker, agent} =
        start_bound(config, "alpha", fn agent ->
          [
            store: store,
            submit: fn env -> send(test, {:submitted, env}) && {:ok, "nope"} end,
            republish: republish_ok(agent)
          ]
        end)

      assert :ok = Fake.deliver(agent, payload)
      assert ConnectorWorker.inflight(worker) == nil
      refute_received {:submitted, _}
      events = Fake.events(agent)
      assert {:reject, 1, 1, [requeue: false]} in events
      refute Enum.any?(events, &match?({:ack, _, _}, &1))
      refute Enum.any?(events, &match?({:republish, _}, &1))
      stop(worker, agent)
    end
  end

  test "journal write failure stalls without submit or settlement", %{config: config} do
    test = self()

    {worker, agent} =
      start_bound(config, "alpha", fn agent ->
        [
          store: {FailingStore, :unused},
          submit: fn env -> send(test, {:submitted, env}) && {:ok, "nope"} end,
          republish: republish_ok(agent)
        ]
      end)

    {_envelope, payload} = valid_payload(%{gateway_id: "gw-stall", expires_at: @future})
    assert :ok = Fake.deliver(agent, payload)
    assert {:basic_deliver, ^payload, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    refute_received {:submitted, _}
    events = Fake.events(agent)
    refute Enum.any?(events, &match?({:ack, _, _}, &1))
    refute Enum.any?(events, &match?({:reject, _, _, _}, &1))
    refute Enum.any?(events, &match?({:republish, _}, &1))
    stop(worker, agent)
  end

  test "preserves stalled inflight on bind-lost instead of silent close", %{config: config} do
    test = self()
    unavailable = {:error, :unavailable}
    {:ok, store} = Agent.start_link(fn -> [unavailable, :ok, :ok, unavailable, unavailable] end)

    {worker, agent} =
      start_bound(config, "alpha", fn agent ->
        [
          store: {FailingStore, store},
          submit: fn
            %{gateway_id: "gw-stall-lost"} = env ->
              send(test, {:submitted, env}) && {:ok, "smsc-ok"}

            _env ->
              :unsupported
          end,
          republish: republish_ok(agent)
        ]
      end)

    {_envelope, payload} = valid_payload(%{gateway_id: "gw-stall-lost", expires_at: @future})
    assert :ok = Fake.deliver(agent, payload)
    assert {:basic_deliver, ^payload, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    send(worker, {:smpp_bind_lost, "alpha", :tcp_closed})
    assert {:basic_deliver, ^payload, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    refute_received {:submitted, _}
    events = Fake.events(agent)
    refute {:cancel, 1, "ctag-1"} in events
    refute {:close_channel, 1} in events
    refute Enum.any?(events, &match?({:republish, _}, &1))
    refute Enum.any?(events, &match?({:ack, _, _}, &1))
    send(worker, {:smpp_bound, "alpha", :reconnect})
    assert ConnectorWorker.inflight(worker) == nil
    assert_received {:submitted, %{gateway_id: "gw-stall-lost"}}
    events = Fake.events(agent)
    assert {:ack, 1, 1} in events
    assert {:cancel, 1, "ctag-1"} in events
    assert {:close_channel, 1} in events
    assert Enum.count(events, &match?({:consume, _, "jasmin.work.alpha", ^worker, _}, &1)) == 2
    {_envelope, risky} = valid_payload(%{gateway_id: "gw-maybe-sent"})
    assert :ok = Fake.deliver(agent, risky)
    send(worker, {:smpp_bind_lost, "alpha", :tcp_closed})
    assert {:basic_deliver, ^risky, %{delivery_tag: 2}} = ConnectorWorker.inflight(worker)
    events = Fake.events(agent)
    refute {:ack, 2, 2} in events
    refute {:cancel, 2, "ctag-2"} in events
    refute {:close_channel, 2} in events
    stop(worker, agent)
  end

  test "quarantines recoverable unclassified inflight then closes only C", %{config: config} do
    agent = Fake.start()
    conn = Fake.connection(agent)
    {store, _} = journal_store()
    extra = [store: store, submit: fn _ -> :unsupported end, republish: republish_ok(agent)]
    {:ok, alpha} = start_one(config, "alpha", conn, extra)
    {:ok, beta} = start_one(config, "beta", conn)
    bind_both(alpha, beta)
    {_envelope, payload} = valid_payload(%{gateway_id: "gw-unclassified", expires_at: @future})
    assert :ok = Fake.deliver(agent, payload)
    assert {:basic_deliver, ^payload, %{delivery_tag: 1}} = ConnectorWorker.inflight(alpha)
    send(alpha, {:smpp_bind_lost, "alpha", :tcp_closed})
    assert ConnectorWorker.inflight(alpha) == nil
    events = Fake.events(agent)
    assert {:republish, {:quarantine, env, evidence}} = find(events, :republish)
    assert env.gateway_id == "gw-unclassified"
    assert env.connector_id == "alpha"
    assert evidence.stage == :post_write
    assert evidence.reason == :bind_lost
    assert evidence.gateway_id == "gw-unclassified"
    assert evidence.connector_id == "alpha"
    republish_at = Enum.find_index(events, &match?({:republish, {:quarantine, _, _}}, &1))
    ack_at = Enum.find_index(events, &(&1 == {:ack, 1, 1}))
    cancel_at = Enum.find_index(events, &match?({:cancel, 1, _}, &1))
    close_at = Enum.find_index(events, &match?({:close_channel, 1}, &1))
    assert is_integer(republish_at) and is_integer(ack_at) and republish_at < ack_at
    assert is_integer(cancel_at) and ack_at < cancel_at
    assert is_integer(close_at) and ack_at < close_at
    refute {:cancel, 2, "ctag-2"} in events
    refute {:close_channel, 2} in events
    assert {:consume, 2, "jasmin.work.beta", ^beta, _} = find(events, :consume, 1)
    stop_pair(alpha, beta, agent)
  end

  test "channel down replaces stale classified work and quarantines unresolved redelivery", %{
    config: config
  } do
    {store, _} = journal_store()
    test = self()

    {worker, agent} =
      start_bound(
        config,
        "alpha",
        fn agent ->
          [
            store: store,
            submit: fn env -> send(test, {:submitted, env}) && {:ok, "smsc-ok"} end,
            republish: republish_ok(agent)
          ]
        end,
        :ack
      )

    {_envelope, payload} = valid_payload(%{gateway_id: "gw-down-classified"})
    assert :ok = Fake.deliver(agent, payload)
    assert {:classified, :ack, %{delivery_tag: 1}} = ConnectorWorker.inflight(worker)
    assert_received {:submitted, %{gateway_id: "gw-down-classified"}}
    pid = Fake.channel_pid(agent, 1)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    assert ConnectorWorker.inflight(worker) == nil
    events = Fake.events(agent)
    assert Enum.count(events, &match?({:consume, _, "jasmin.work.alpha", ^worker, _}, &1)) == 2
    refute {:cancel, 1, "ctag-1"} in events
    send(worker, {:basic_deliver, payload, %{delivery_tag: 2, redelivered: true}})

    assert {:classified, :ack, %{delivery_tag: 2, redelivered: true}} =
             ConnectorWorker.inflight(worker)

    refute_received {:submitted, _}
    events = Fake.events(agent)
    assert {:republish, {:quarantine, env, evidence}} = find(events, :republish)
    assert env.gateway_id == "gw-down-classified"
    assert evidence.reason == :unresolved_dispatch
    stop(worker, agent)
  end

  test "restores exactly one consumer after same-connector down while bound", %{config: config} do
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
    assert Enum.count(events, &match?({:open_channel, _}, &1)) == 3
    assert Enum.count(events, &match?({:consume, _, "jasmin.work.alpha", ^alpha, _}, &1)) == 2
    assert Enum.count(events, &match?({:consume, _, "jasmin.work.beta", ^beta, _}, &1)) == 1
    refute {:cancel, 2, "ctag-2"} in events
    refute {:close_channel, 2} in events
    stop_pair(alpha, beta, agent)
    GenServer.stop(conn)
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

  defp start_bound(config, id, extra, fail_at \\ nil) do
    agent = Fake.start(fail_at)
    extra = if is_function(extra, 1), do: extra.(agent), else: extra
    {:ok, worker} = start_one(config, id, Fake.connection(agent), extra)
    send(worker, {:smpp_bound, id, :initial})
    _ = ConnectorWorker.inflight(worker)
    {worker, agent}
  end

  defp republish_ok(agent) do
    fn action ->
      Fake.record(agent, {:republish, action})
      :ok
    end
  end

  defp journal_store do
    table = :ets.new(:pr3d_journal, [:set, :public])
    {{JournalStore, table}, table}
  end

  defp valid_payload(overrides) do
    attributes =
      Map.merge(
        %{
          gateway_id: "gateway-1",
          connector_id: "alpha",
          attempt: 1,
          max_attempts: 3,
          enqueued_at: "2026-08-01T15:00:00Z",
          expires_at: "2026-08-02T15:00:00Z",
          submit_sm: %{
            source_addr: "+12025550100",
            destination_addr: "+12025550101",
            short_message: "hello"
          }
        },
        overrides
      )

    {:ok, envelope} = Envelope.new(attributes)
    {:ok, payload} = Envelope.encode(envelope)
    {envelope, payload}
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
