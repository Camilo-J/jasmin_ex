defmodule JasminEx.Dlr.E2ETest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 120_000

  alias JasminEx.Application, as: JasminApp
  alias JasminEx.Dlr.{Event, LookupPlan, Readiness}
  alias JasminEx.Dlr.Map, as: DlrMap
  alias JasminEx.Dlr.Supervisor, as: DlrSupervisor
  alias JasminEx.FakeDlrEndpoint
  alias JasminEx.HttpApi.Supervisor, as: HttpSupervisor

  alias JasminEx.Messaging.RabbitMQ.{
    Client,
    Config,
    Connection,
    ConnectorWorker,
    Publisher,
    TopicPublisher,
    TopicTopology,
    WorkQueue
  }

  alias JasminEx.RabbitMQHarness
  alias JasminEx.Routing
  alias JasminEx.Routing.{ConnectorRef, Filter, Router}
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.StateStore.Config, as: StoreConfig
  alias JasminEx.StateStore.Redix, as: Store
  alias JasminEx.StateStoreHarness

  @tag :tmp_dir
  test "application intake reaches FakeSMSC and delivers the callback over the broker", %{
    tmp_dir: tmp_dir
  } do
    broker = RabbitMQHarness.new(port: available_port!())
    :ok = RabbitMQHarness.start!(broker)
    on_exit(fn -> RabbitMQHarness.stop!(broker) end)

    state_store = StateStoreHarness.start!()
    on_exit(fn -> StateStoreHarness.stop!(state_store) end)

    {:ok, smsc_port, smsc} = FakeSMSC.start_link()
    on_exit(fn -> safe_stop(smsc, &GenServer.stop/1) end)
    smsc_ref = FakeSMSC.subscribe_pdus(smsc)

    {:ok, endpoint} = FakeDlrEndpoint.start_link(script: [{:reply, 200, "ACK/Jasmin"}])
    on_exit(fn -> FakeDlrEndpoint.stop(endpoint) end)
    env = Map.new(RabbitMQHarness.compose_environment(broker))
    prefix = "jasmin_ex.dlr.e2e.#{System.unique_integer([:positive])}"

    messaging = [
      enabled: true,
      host: "127.0.0.1",
      port: RabbitMQHarness.port(broker),
      username: env["RABBITMQ_TEST_USER"],
      password: env["RABBITMQ_TEST_PASSWORD"]
    ]

    store_config = [
      host: "127.0.0.1",
      port: state_store.port,
      password:
        Map.fetch!(
          Map.new(StateStoreHarness.compose_environment(state_store)),
          "STATE_STORE_TEST_PASSWORD"
        ),
      prefix: prefix
    ]

    connector_id = "dlr-e2e"

    connector = [
      connector_id: connector_id,
      host: ~c"127.0.0.1",
      port: smsc_port,
      system_id: "user",
      password: "pw",
      system_type: "type",
      bind_as: :transceiver,
      reconnect_base_ms: 50,
      reconnect_cap_ms: 50,
      reconnect_jitter: false,
      messaging: messaging
    ]

    declare_mt_queue!(messaging, connector_id)

    dlr = [
      enabled: true,
      queue_prefix: prefix,
      http_delay_ms: 125,
      http_timeout_ms: 1_234,
      http_client:
        {JasminEx.Dlr.HttpClient.Mint,
         [
           resolver: {FakeDlrEndpoint.Resolver, %{"callback.test" => [{127, 0, 0, 1}]}},
           allow: [{"callback.test", {127, 0, 0, 1}}]
         ]}
    ]

    original_env =
      for key <- [:messaging, :state_store],
          into: %{},
          do: {key, Application.get_env(:jasmin_ex, key, :unset)}

    assert :ok = Application.stop(:jasmin_ex)
    Application.put_env(:jasmin_ex, :messaging, messaging)
    Application.put_env(:jasmin_ex, :state_store, store_config)

    on_exit(fn ->
      Enum.each(original_env, fn
        {key, :unset} -> Application.delete_env(:jasmin_ex, key)
        {key, value} -> Application.put_env(:jasmin_ex, key, value)
      end)

      {:ok, _} = Application.ensure_all_started(:jasmin_ex)
    end)

    children =
      JasminApp.children(
        state_store: store_config,
        routing: [snapshot_path: Path.join(tmp_dir, "routing.json")],
        messaging: messaging,
        dlr: dlr,
        smpp_connectors: [connector],
        http_api: [enabled: true, port: 0, queue: {WorkQueue, %{publisher: Publisher}}]
      )

    assert Enum.any?(children, fn
             {module, _} -> module == DlrSupervisor
             _ -> false
           end)

    {:ok, app} = Supervisor.start_link(children, strategy: :one_for_one)
    on_exit(fn -> safe_stop(app, &Supervisor.stop/1) end)

    assert eventually(fn -> Readiness.status().ready end, 8_000)

    {_, http_worker, _, _} =
      Enum.find(Supervisor.which_children(DlrSupervisor), fn {id, _, _, _} ->
        id == :dlr_http_worker
      end)

    {_, _, http_context} = :sys.get_state(http_worker).processor
    assert {JasminEx.Dlr.HttpClient.Mint, client_options} = http_context[:client]
    assert client_options[:timeout] == 1_234

    configure_route(connector_id)
    assert :ok = FakeSMSC.wait_connected(smsc, 5_000)

    {HttpSupervisor, http_pid, :supervisor, _} =
      Enum.find(Supervisor.which_children(app), fn {id, _, _, _} -> id == HttpSupervisor end)

    {:ok, _} = Application.ensure_all_started(:inets)
    callback_url = FakeDlrEndpoint.url(endpoint, "callback.test")

    body =
      URI.encode_query(%{
        "username" => "alice",
        "password" => "s3cret",
        "to" => "21200000",
        "from" => "1616",
        "content" => "hello",
        "dlr" => "yes",
        "dlr-url" => callback_url,
        "dlr-level" => "1",
        "dlr-method" => "POST"
      })

    url = ~c"http://127.0.0.1:#{HttpSupervisor.port(http_pid)}/send"

    assert {:ok, {{_, 200, _}, _, gateway_body}} =
             :httpc.request(:post, {url, [], ~c"application/x-www-form-urlencoded", body}, [],
               body_format: :binary
             )

    gateway_id = String.trim(gateway_body)

    store = {Store, Store.context(StoreConfig.new!(store_config))}

    assert {:ok, %{gateway_id: ^gateway_id, connector_id: ^connector_id}} =
             DlrMap.fetch_request(
               store,
               gateway_id,
               {ConnectorWorker, :system}
             )

    assert_receive {:fake_smsc_pdu, ^smsc_ref, %{command: :submit_sm}}, 5_000

    names = TopicTopology.names(prefix)

    assert eventually(
             fn ->
               FakeDlrEndpoint.requests(endpoint) != [] and
                 queue_has_no_messages?(messaging, names.http)
             end,
             8_000
           )

    assert [%{method: "POST", path: "/dlr", body: callback_body}] =
             FakeDlrEndpoint.requests(endpoint)

    assert URI.decode_query(callback_body)["id"] == gateway_id
    assert URI.decode_query(callback_body)["level"] == "1"

    clock = {ConnectorWorker, :system}
    event_id = Event.submit_event_id(connector_id, gateway_id, 1)
    assert {:ok, %{phase: :complete}} = LookupPlan.fetch(store, event_id, clock)
    assert :missing = DlrMap.fetch_request(store, gateway_id, clock)
    assert queue_has_no_messages?(messaging, names.lookup)

    FakeDlrEndpoint.script(endpoint, [
      {:reply, 500, "not ready"},
      {:reply, 200, "ACK/Jasmin"}
    ])

    assert {:ok, {{_, 200, _}, _, retry_gateway_body}} =
             :httpc.request(:post, {url, [], ~c"application/x-www-form-urlencoded", body}, [],
               body_format: :binary
             )

    retry_gateway_id = String.trim(retry_gateway_body)
    assert_receive {:fake_smsc_pdu, ^smsc_ref, %{command: :submit_sm}}, 5_000
    assert eventually(fn -> length(FakeDlrEndpoint.requests(endpoint)) == 3 end, 8_000)
    assert [_, first_retry, second_retry] = FakeDlrEndpoint.requests(endpoint)
    assert URI.decode_query(first_retry.body)["id"] == retry_gateway_id
    assert first_retry.body == second_retry.body
    assert queue_has_no_messages?(messaging, names.http)

    FakeDlrEndpoint.script(endpoint, List.duplicate({:reply, 500, "unavailable"}, 4))

    assert {:ok, {{_, 200, _}, _, exhausted_body}} =
             :httpc.request(:post, {url, [], ~c"application/x-www-form-urlencoded", body}, [],
               body_format: :binary
             )

    exhausted_id = String.trim(exhausted_body)
    assert_receive {:fake_smsc_pdu, ^smsc_ref, %{command: :submit_sm}}, 5_000
    assert eventually(fn -> length(FakeDlrEndpoint.requests(endpoint)) == 7 end, 8_000)

    assert Enum.all?(Enum.drop(FakeDlrEndpoint.requests(endpoint), 3), fn request ->
             URI.decode_query(request.body)["id"] == exhausted_id
           end)

    assert eventually(fn -> queue_counts(messaging, names.dead) == {1, 0} end, 8_000)
    assert queue_has_no_messages?(messaging, names.http)

    old_topology = :sys.get_state(Readiness).channel.pid
    old_publisher = :sys.get_state(TopicPublisher).channel.pid
    assert :ok = Supervisor.terminate_child(app, DlrSupervisor)
    refute Process.alive?(old_topology)
    refute Process.alive?(old_publisher)
    assert {:ok, _} = Supervisor.restart_child(app, DlrSupervisor)
    assert eventually(fn -> Readiness.status().ready end, 8_000)

    FakeDlrEndpoint.script(endpoint, [{:reply, 200, "ACK/Jasmin"}])
    receipt_form = body |> URI.decode_query() |> Map.put("dlr-level", "2") |> URI.encode_query()

    assert {:ok, {{_, 200, _}, _, receipt_gateway_body}} =
             :httpc.request(
               :post,
               {url, [], ~c"application/x-www-form-urlencoded", receipt_form},
               [],
               body_format: :binary
             )

    receipt_gateway_id = String.trim(receipt_gateway_body)
    assert_receive {:fake_smsc_pdu, ^smsc_ref, %{command: :submit_sm}}, 5_000

    assert eventually(
             fn ->
               match?(
                 {:ok, %{gateway_id: ^receipt_gateway_id}},
                 DlrMap.fetch_reverse(store, connector_id, "fake-msg-id", clock)
               )
             end,
             8_000
           )

    :ok = RabbitMQHarness.restart!(broker)
    assert RabbitMQHarness.port(broker) == messaging[:port]

    assert eventually(
             fn ->
               match?({:ok, _}, Connection.get())
             end,
             15_000
           )

    old_lookup =
      DlrSupervisor
      |> Supervisor.which_children()
      |> Enum.find(fn {id, _, _, _} -> id == :dlr_lookup_worker end)
      |> elem(1)

    old_channel = :sys.get_state(old_lookup).channel.pid
    assert :ok = Supervisor.terminate_child(app, DlrSupervisor)
    refute Process.alive?(old_channel)
    assert {:ok, _} = Supervisor.restart_child(app, DlrSupervisor)

    assert {:ok, %{gateway_id: ^receipt_gateway_id}} =
             DlrMap.fetch_request(store, receipt_gateway_id, clock)

    assert {:ok, %{gateway_id: ^receipt_gateway_id}} =
             DlrMap.fetch_reverse(store, connector_id, "fake-msg-id", clock)

    assert eventually(
             fn ->
               queue_has_no_messages?(messaging, names.lookup) and
                 queue_has_no_messages?(messaging, names.http)
             end,
             15_000
           )

    assert %{ready: true, error: nil} = Readiness.status()

    assert Enum.count(Supervisor.which_children(DlrSupervisor), fn
             {id, pid, _, _} -> id in [:dlr_lookup_worker, :dlr_http_worker] and is_pid(pid)
           end) == 2

    telemetry_id = "dlr-store-outage-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        telemetry_id,
        [:jasmin_ex, :dlr, :settlement],
        &__MODULE__.handle_settlement/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)
    :ok = StateStoreHarness.stop_valkey!(state_store)
    assert :ok = FakeSMSC.send_bytes(smsc, receipt_bytes())

    assert_receive {:fake_smsc_pdu, ^smsc_ref, %{command: :deliver_sm_resp, status: :ESME_ROK}},
                   5_000

    assert_receive {:dlr_settlement, %{phase: :lookup, reason_class: :retry}}, 10_000
    assert length(FakeDlrEndpoint.requests(endpoint)) == 7
    :ok = StateStoreHarness.start_valkey!(state_store)

    assert {:ok, %{gateway_id: ^receipt_gateway_id}} =
             DlrMap.fetch_reverse(store, connector_id, "fake-msg-id", clock)

    assert eventually(fn -> length(FakeDlrEndpoint.requests(endpoint)) == 8 end, 20_000)
    assert %{body: receipt_callback} = List.last(FakeDlrEndpoint.requests(endpoint))
    assert URI.decode_query(receipt_callback)["id"] == receipt_gateway_id
    assert URI.decode_query(receipt_callback)["message_status"] == "DELIVRD"

    assert eventually(
             fn -> DlrMap.fetch_request(store, receipt_gateway_id, clock) == :missing end,
             8_000
           )
  end

  @tag :tmp_dir
  test "broker absence during application startup delays consumers until topology is ready", %{
    tmp_dir: tmp_dir
  } do
    broker = RabbitMQHarness.new(port: available_port!())
    :ok = RabbitMQHarness.start!(broker)
    on_exit(fn -> RabbitMQHarness.stop!(broker) end)
    state_store = StateStoreHarness.start!()
    on_exit(fn -> StateStoreHarness.stop!(state_store) end)

    env = Map.new(RabbitMQHarness.compose_environment(broker))

    messaging = [
      enabled: true,
      host: "127.0.0.1",
      port: broker.port,
      username: env["RABBITMQ_TEST_USER"],
      password: env["RABBITMQ_TEST_PASSWORD"]
    ]

    store_config = [
      host: "127.0.0.1",
      port: state_store.port,
      password:
        Map.fetch!(
          Map.new(StateStoreHarness.compose_environment(state_store)),
          "STATE_STORE_TEST_PASSWORD"
        )
    ]

    prefix = "jasmin_ex.dlr.startup.#{System.unique_integer([:positive])}"

    assert :ok = Application.stop(:jasmin_ex)
    on_exit(fn -> {:ok, _} = Application.ensure_all_started(:jasmin_ex) end)
    :ok = RabbitMQHarness.stop_broker!(broker)

    {:ok, app} =
      Supervisor.start_link(
        JasminApp.children(
          state_store: store_config,
          routing: [snapshot_path: Path.join(tmp_dir, "routing.json")],
          messaging: messaging,
          dlr: [enabled: true, queue_prefix: prefix]
        ),
        strategy: :one_for_one
      )

    on_exit(fn -> safe_stop(app, &Supervisor.stop/1) end)

    assert eventually(fn -> Readiness.status().error == :disconnected end, 5_000)

    refute Enum.any?(Supervisor.which_children(DlrSupervisor), fn {id, _, _, _} ->
             id in [:dlr_lookup_worker, :dlr_http_worker]
           end)

    :ok = RabbitMQHarness.start_broker!(broker)
    assert eventually(fn -> Readiness.status().ready end, 15_000)
    names = TopicTopology.names(prefix)
    assert queue_has_no_messages?(messaging, names.lookup)
    assert queue_has_no_messages?(messaging, names.http)
  end

  def handle_settlement(_event, _measurements, metadata, recipient) do
    send(recipient, {:dlr_settlement, metadata})
  end

  defp receipt_bytes do
    text =
      "id:fake-msg-id sub:001 dlvrd:001 submit date:2609231100 done date:2609231101 stat:DELIVRD err:000 Text:hello"

    receipt = %Body.DeliverSM{source_addr: "src", destination_addr: "1616", short_message: text}
    {:ok, body} = Body.encode(:deliver_sm, receipt)

    PDU.build(
      command: :deliver_sm,
      status: :ESME_ROK,
      sequence_number: 77,
      body: IO.iodata_to_binary(body)
    )
    |> PDU.encode()
    |> IO.iodata_to_binary()
  end

  defp available_port! do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp configure_route(connector_id) do
    {:ok, group} = Routing.put_group(Router, gid: "ops")

    {:ok, _user} =
      Routing.put_user(Router,
        uid: "u1",
        username: "alice",
        secret: "s3cret",
        group: group,
        balance_minor: 500,
        submit_quota: 4
      )

    {:ok, ref} = ConnectorRef.new(connector_id)
    {:ok, filter} = Filter.Destination.new(address: "21200000")

    {:ok, _route} =
      Routing.put_route(Router,
        kind: :static,
        order: 10,
        connector: ref,
        filters: [filter],
        rate_minor: 100,
        precharge_percent: 10
      )
  end

  defp queue_has_no_messages?(messaging, name) do
    config = Config.new!(messaging)

    with {:ok, connection} <- Client.open_connection(Config.to_connection_options(config)),
         {:ok, channel} <- Client.open_channel(connection) do
      result = Client.declare_queue(channel, name, passive: true)
      _ = Client.close_connection(connection)
      match?({:ok, %{message_count: 0, consumer_count: 1}}, result)
    else
      _ -> false
    end
  end

  defp queue_counts(messaging, name) do
    config = Config.new!(messaging)

    with {:ok, connection} <- Client.open_connection(Config.to_connection_options(config)),
         {:ok, channel} <- Client.open_channel(connection) do
      result = Client.declare_queue(channel, name, passive: true)
      :ok = Client.close_connection(connection)

      case result do
        {:ok, %{message_count: count, consumer_count: consumers}} -> {count, consumers}
        _ -> :unavailable
      end
    else
      _ -> :unavailable
    end
  end

  defp declare_mt_queue!(messaging, connector_id) do
    config = Config.new!(messaging)
    {:ok, connection} = Client.open_connection(Config.to_connection_options(config))
    {:ok, channel} = Client.open_channel(connection)

    assert {:ok, _} =
             Client.declare_queue(
               channel,
               "#{config.queue_prefix}.#{connector_id}",
               Client.queue_declare_opts()
             )

    :ok = Client.close_connection(connection)
  end

  defp eventually(predicate, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll(predicate, deadline)
  end

  defp safe_stop(pid, stop) do
    if Process.alive?(pid), do: stop.(pid)
  catch
    :exit, _reason -> :ok
  end

  defp poll(predicate, deadline) do
    cond do
      predicate.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(25)
        poll(predicate, deadline)
    end
  end
end
