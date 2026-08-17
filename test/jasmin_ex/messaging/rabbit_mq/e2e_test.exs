defmodule JasminEx.Messaging.RabbitMQ.E2ETest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 120_000

  alias JasminEx.Messaging.Envelope
  alias JasminEx.Messaging.RabbitMQ.{Client, Config, Connection, ConnectorWorker, Publisher}
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

  defp bind_client!(stack) do
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
        response_timeout_ms: 1_000,
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
    table = :ets.new(:pr4c_journal, [:set, :public])
    {:ok, client_slot} = Agent.start_link(fn -> nil end)
    {:ok, port, smsc} = FakeSMSC.start_link(Keyword.take(opts, [:script]))
    pdu_ref = FakeSMSC.subscribe_pdus(smsc)
    {:ok, connection} = Connection.start_link(config: config, name: nil)

    {:ok, publisher} =
      Publisher.start_link(config: config, connection_server: connection, name: nil)

    {:ok, worker} =
      ConnectorWorker.start_link(
        config: config,
        connector_id: connector_id,
        connection_server: connection,
        store: {JournalStore, table},
        submit: &submit_via_slot(client_slot, &1),
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
      client: nil
    }
  end

  defp publish_envelope!(stack, gateway_id) do
    {_envelope, payload} = valid_payload(stack.connector_id, gateway_id)
    assert :ok = Publisher.publish(stack.publisher, stack.connector_id, payload)
  end

  defp queue_info(stack) do
    {:ok, conn} = Client.open_connection(Config.to_connection_options(stack.config))
    {:ok, ch} = Client.open_channel(conn)

    assert {:ok, info} =
             Client.declare_queue(ch, queue_name(stack), durable: true)

    _ = Client.close_connection(conn)
    Map.take(info, [:message_count, :consumer_count])
  end

  defp leftover(stack) do
    {:ok, conn} = Client.open_connection(Config.to_connection_options(stack.config))
    {:ok, ch} = Client.open_channel(conn)
    collector = start_collector()
    assert :ok = Client.qos(ch, prefetch_count: 1)
    assert {:ok, _} = Client.consume(ch, queue_name(stack), collector, no_ack: false)

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

  defp valid_payload(connector_id, gateway_id) do
    attributes = %{
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
    }

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
