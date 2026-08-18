defmodule JasminEx.Messaging.RabbitMQ.E2ETest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 120_000

  alias JasminEx.Messaging.{Envelope, StateStoreJournal}

  alias JasminEx.Messaging.RabbitMQ.{
    Client,
    Config,
    Connection,
    ConnectorWorker,
    Publisher,
    WorkQueue
  }

  alias JasminEx.RabbitMQHarness
  alias JasminEx.Smpp.Client, as: SmppClient
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU.Body

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

  setup do
    harness = RabbitMQHarness.new()
    :ok = RabbitMQHarness.start!(harness)
    on_exit(fn -> safe_stop(harness) end)
    {:ok, harness: harness, config: config(harness)}
  end

  @tag :ack_reject
  test "consume does not start before bind and known success acks without redelivery", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config)
    publish_envelope!(stack, "gw-ok")
    assert queue_info(stack) == %{message_count: 1, consumer_count: 0}
    refute_submit(stack)

    stack = bind_client!(stack)

    assert_settled_empty(stack)
    assert submit_count(stack) == 1
    stop_pid(stack.worker)
    assert leftover(stack) == :none
    stop_stack(%{stack | worker: nil})
  end

  @tag :ack_reject
  test "explicit SMPP reject is terminal without requeue", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config, script: %{submit_sm: {:reply_status, :ESME_RINVDSTADR}})
    publish_envelope!(stack, "gw-rej")
    stack = bind_client!(stack)

    assert_settled_empty(stack)
    assert submit_count(stack) == 1
    stop_pid(stack.worker)
    assert leftover(stack) == :none
    stop_stack(%{stack | worker: nil})
  end

  @tag :retry_quarantine
  test "proven never-reached work retries then is terminal when attempts are exhausted", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config, submit: &never_reached_submit/1)
    publish_envelope!(stack, "gw-retry")
    stack = bind_client!(stack)

    assert {:basic_deliver, payload, _} = await_deliver(stack, quarantine_name(stack))
    assert {:ok, quarantined} = Envelope.decode(payload)
    assert quarantined.gateway_id == "gw-retry"
    assert quarantined.attempt == 3
    assert :json.decode(payload)["evidence"]["stage"] == "pre_write"
    assert_settled_empty(stack)
    stop_pid(stack.worker)
    assert leftover(stack) == :none
    assert journal_state(stack, "gw-retry", 1) == :not_sent
    assert journal_state(stack, "gw-retry", 2) == :not_sent
    assert journal_state(stack, "gw-retry", 3) == :not_sent
    stop_stack(%{stack | worker: nil})
  end

  @tag :retry_quarantine
  test "expired never-reached work is terminal and does not requeue", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config, submit: &never_reached_submit/1)
    publish_envelope!(stack, "gw-exp", %{expires_at: "2000-01-01T00:00:00Z"})
    stack = bind_client!(stack)

    assert {:basic_deliver, payload, _} = await_deliver(stack, quarantine_name(stack))
    assert {:ok, quarantined} = Envelope.decode(payload)
    assert quarantined.gateway_id == "gw-exp"
    assert quarantined.attempt == 1
    assert_settled_empty(stack)
    stop_pid(stack.worker)
    assert leftover(stack) == :none
    stop_stack(%{stack | worker: nil})
  end

  @tag :retry_quarantine
  test "post-write timeout quarantines with evidence and is not auto-requeued", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config, script: %{submit_sm: :withhold})
    publish_envelope!(stack, "gw-timeout")
    stack = bind_client!(stack, response_timeout_ms: 200)

    assert {:basic_deliver, payload, _} = await_deliver(stack, quarantine_name(stack), 8_000)
    decoded = :json.decode(payload)
    assert decoded["gateway_id"] == "gw-timeout"
    assert decoded["connector_id"] == stack.connector_id
    assert decoded["evidence"]["stage"] == "post_write"
    assert decoded["evidence"]["reason"] == "response_timeout"
    assert decoded["evidence"]["gateway_id"] == "gw-timeout"
    assert decoded["evidence"]["connector_id"] == stack.connector_id
    assert decoded["evidence"]["source_addr"] == "+12025550100"
    assert decoded["evidence"]["destination_addr"] == "+12025550101"
    assert submit_count(stack) == 1
    assert_settled_empty(stack)
    stop_pid(stack.worker)
    assert leftover(stack) == :none
    stop_stack(%{stack | worker: nil})
  end

  @tag :retry_quarantine
  test "uncertain disconnect quarantines and is not auto-requeued", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config, script: %{submit_sm: :close})
    publish_envelope!(stack, "gw-disc")
    stack = bind_client!(stack)

    assert {:basic_deliver, payload, _} = await_deliver(stack, quarantine_name(stack))
    decoded = :json.decode(payload)
    assert decoded["gateway_id"] == "gw-disc"
    assert decoded["evidence"]["stage"] == "post_write"
    assert decoded["evidence"]["reason"] == "disconnected"
    assert decoded["evidence"]["connector_id"] == stack.connector_id
    assert submit_count(stack) == 1
    assert_settled_empty(stack)
    stop_pid(stack.worker)
    assert leftover(stack) == :none
    stop_stack(%{stack | worker: nil})
  end

  @tag :retry_quarantine
  test "inflight work is quarantined before channel close on bind loss", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config, submit: fn _ -> :unsupported end)
    publish_envelope!(stack, "gw-inflight")
    stack = bind_client!(stack)

    assert :ok =
             wait_until(fn ->
               match?({:basic_deliver, _, _}, ConnectorWorker.inflight(stack.worker))
             end)

    send(stack.worker, {:smpp_bind_lost, stack.connector_id, :tcp_closed})
    assert {:basic_deliver, payload, _} = await_deliver(stack, quarantine_name(stack))
    decoded = :json.decode(payload)
    assert decoded["gateway_id"] == "gw-inflight"
    assert decoded["evidence"]["reason"] == "bind_lost"
    assert decoded["evidence"]["stage"] == "post_write"

    assert :ok =
             wait_until(fn ->
               ConnectorWorker.inflight(stack.worker) == nil and
                 queue_info(stack).consumer_count == 0
             end)

    stop_pid(stack.worker)
    assert leftover(stack) == :none
    stop_stack(%{stack | worker: nil})
  end

  @tag :retry_quarantine
  test "repeated bind-loss and restore leaves exactly one consumer", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config)
    assert queue_info(stack).consumer_count == 0
    stack = bind_client!(stack)
    assert :ok = wait_until(fn -> queue_info(stack).consumer_count == 1 end)

    Enum.each(1..2, fn _ ->
      send(stack.worker, {:smpp_bind_lost, stack.connector_id, :tcp_closed})
      assert :ok = wait_until(fn -> queue_info(stack).consumer_count == 0 end)
      send(stack.worker, {:smpp_bound, stack.connector_id, :reconnect})
      assert :ok = wait_until(fn -> queue_info(stack).consumer_count == 1 end)
    end)

    assert queue_info(stack) == %{message_count: 0, consumer_count: 1}
    stop_stack(stack)
  end

  @tag :retry_quarantine
  test "broker restart does not SMS-resend journalled or quarantined work", %{
    harness: harness,
    config: config
  } do
    stack = start_stack(harness, config, script: %{submit_sm: :withhold})
    publish_envelope!(stack, "gw-restart")
    stack = bind_client!(stack, response_timeout_ms: 1_500)
    ref = stack.pdu_ref
    assert_receive {:fake_smsc_pdu, ^ref, %{command: :submit_sm}}, 5_000
    assert leftover(stack, quarantine_name(stack)) == :none
    true = Process.unlink(stack.worker) && Process.exit(stack.worker, :kill)
    assert :ok = RabbitMQHarness.restart!(harness)

    stack = refresh_amqp(stack)
    send(stack.worker, {:smpp_bound, stack.connector_id, :reconnect})
    assert {:basic_deliver, payload, _} = await_deliver(stack, quarantine_name(stack))
    assert :json.decode(payload)["gateway_id"] == "gw-restart"
    assert :json.decode(payload)["evidence"]["reason"] == "unresolved_dispatch"
    assert_settled_empty(stack)
    refute_receive {:fake_smsc_pdu, ^ref, %{command: :submit_sm}}, 2_000
    stop_stack(stack)
  end

  defp bind_client!(stack, opts \\ []) do
    {:ok, client} =
      SmppClient.start_link(
        connector_id: stack.connector_id,
        host: ~c"localhost",
        port: stack.smsc_port,
        system_id: "user",
        password: "pw",
        system_type: "type",
        bind_as: :transmitter,
        heartbeat_ms: 10_000,
        response_timeout_ms: Keyword.get(opts, :response_timeout_ms, 1_000),
        reconnect_base_ms: 50,
        reconnect_cap_ms: 50,
        reconnect_jitter: false,
        lifecycle_notify: stack.worker
      )

    Agent.update(stack.client_slot, fn _ -> client end)
    assert :ok = wait_until(fn -> SmppClient.status(client) == :bound end)
    %{stack | client: client}
  end

  defp start_stack(harness, config, opts \\ []) do
    connector_id = unique("e2e")
    table = :ets.new(:e2e_journal, [:set, :public])
    {:ok, client_slot} = Agent.start_link(fn -> nil end)
    {:ok, port, smsc} = FakeSMSC.start_link(Keyword.take(opts, [:script]))
    pdu_ref = FakeSMSC.subscribe_pdus(smsc)
    {:ok, connection} = Connection.start_link(config: config, name: nil)

    {:ok, publisher} =
      Publisher.start_link(config: config, connection_server: connection, name: nil)

    submit = Keyword.get(opts, :submit, fn envelope -> submit_via_slot(client_slot, envelope) end)

    republish = fn action -> WorkQueue.republish(%{publisher: publisher}, action) end

    {:ok, worker} =
      ConnectorWorker.start_link(
        config: config,
        connector_id: connector_id,
        connection_server: connection,
        store: {JournalStore, table},
        submit: submit,
        republish: republish,
        name: nil
      )

    %{
      harness: harness,
      config: config,
      connector_id: connector_id,
      connection: connection,
      publisher: publisher,
      worker: worker,
      smsc: smsc,
      smsc_port: port,
      pdu_ref: pdu_ref,
      client_slot: client_slot,
      table: table,
      client: nil
    }
  end

  defp publish_envelope!(stack, gateway_id, overrides \\ %{}) do
    {_envelope, payload} = valid_payload(stack.connector_id, gateway_id, overrides)
    assert :ok = Publisher.publish(stack.publisher, stack.connector_id, payload)
  end

  defp queue_info(stack, name \\ nil) do
    name = name || queue_name(stack)
    {:ok, conn} = Client.open_connection(Config.to_connection_options(stack.config))
    {:ok, ch} = Client.open_channel(conn)

    assert {:ok, info} = Client.declare_queue(ch, name, durable: true)

    _ = Client.close_connection(conn)
    Map.take(info, [:message_count, :consumer_count])
  end

  defp leftover(stack, name \\ nil) do
    name = name || queue_name(stack)
    {:ok, conn} = Client.open_connection(Config.to_connection_options(stack.config))
    {:ok, ch} = Client.open_channel(conn)
    collector = start_collector()
    assert {:ok, _} = Client.declare_queue(ch, name, durable: true)
    assert :ok = Client.qos(ch, prefetch_count: 1)
    assert {:ok, _} = Client.consume(ch, name, collector, no_ack: false)

    result =
      receive do
        {^collector, {:basic_deliver, payload, meta}} ->
          {:basic_deliver, payload, meta}
      after
        300 -> :none
      end

    _ = Client.close_connection(conn)
    result
  end

  defp assert_settled_empty(stack) do
    assert :ok =
             wait_until(fn ->
               ConnectorWorker.inflight(stack.worker) == nil and
                 queue_info(stack) == %{message_count: 0, consumer_count: 1}
             end)
  end

  defp refute_submit(stack) do
    ref = stack.pdu_ref
    refute_receive {:fake_smsc_pdu, ^ref, %{command: :submit_sm}}, 300
  end

  defp submit_count(stack) do
    count_submits(stack.pdu_ref, 0)
  end

  defp count_submits(ref, count) do
    receive do
      {:fake_smsc_pdu, ^ref, %{command: :submit_sm}} -> count_submits(ref, count + 1)
    after
      0 -> count
    end
  end

  defp submit_via_slot(slot, envelope) do
    client = Agent.get(slot, & &1)
    SmppClient.send_submit_sm(client, struct(Body.SubmitSM, envelope.submit_sm))
  end

  defp never_reached_submit(envelope) do
    {:ok, client} =
      SmppClient.start_link(
        connector_id: envelope.connector_id,
        host: ~c"127.0.0.1",
        port: 1,
        system_id: "user",
        password: "pw",
        system_type: "type",
        bind_as: :transmitter,
        heartbeat_ms: 10_000,
        response_timeout_ms: 200,
        reconnect_base_ms: 50,
        reconnect_cap_ms: 50,
        reconnect_jitter: false
      )

    try do
      SmppClient.send_submit_sm(client, struct(Body.SubmitSM, envelope.submit_sm))
    after
      stop_pid(client)
    end
  end

  defp await_deliver(stack, name, timeout \\ 5_000) do
    assert :ok =
             wait_until(
               fn -> match?({:basic_deliver, _, _}, leftover(stack, name)) end,
               timeout
             )

    leftover(stack, name)
  end

  defp refresh_amqp(stack) do
    Enum.each([stack.worker, stack.publisher, stack.connection], &stop_pid/1)
    config = config(stack.harness)
    {:ok, connection} = Connection.start_link(config: config, name: nil)
    assert :ok = wait_until(fn -> match?({:ok, _}, Connection.get(connection)) end, 15_000)

    {:ok, publisher} =
      Publisher.start_link(config: config, connection_server: connection, name: nil)

    republish = fn action -> WorkQueue.republish(%{publisher: publisher}, action) end

    {:ok, worker} =
      ConnectorWorker.start_link(
        config: config,
        connector_id: stack.connector_id,
        connection_server: connection,
        store: {JournalStore, stack.table},
        submit: fn envelope -> submit_via_slot(stack.client_slot, envelope) end,
        republish: republish,
        name: nil
      )

    %{stack | config: config, connection: connection, publisher: publisher, worker: worker}
  end

  defp journal_state(stack, gateway_id, attempt) do
    assert {:ok, record} =
             StateStoreJournal.read({JournalStore, stack.table}, gateway_id, attempt)

    record.state
  end

  defp valid_payload(connector_id, gateway_id, overrides) do
    attributes =
      Map.merge(
        %{
          gateway_id: gateway_id,
          connector_id: connector_id,
          attempt: 1,
          max_attempts: 3,
          enqueued_at: "2026-08-01T15:00:00Z",
          expires_at: "2099-01-01T00:00:00Z",
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

  defp queue_name(stack), do: "jasmin.work.#{stack.connector_id}"

  defp quarantine_name(stack), do: queue_name(stack) <> ".quarantine"

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

  defp wait_until(predicate, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(predicate, deadline)
  end

  defp do_wait(predicate, deadline) do
    if predicate.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        {:error, :timeout}
      else
        Process.sleep(25)
        do_wait(predicate, deadline)
      end
    end
  end

  defp stop_stack(stack) do
    Enum.each(
      [
        stack.client,
        stack.worker,
        stack.publisher,
        stack.connection,
        stack.smsc,
        stack.client_slot
      ],
      &stop_pid/1
    )
  end

  defp stop_pid(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp stop_pid(_pid), do: :ok

  defp safe_stop(harness) do
    RabbitMQHarness.stop!(harness)
  rescue
    _error -> :ok
  end
end
