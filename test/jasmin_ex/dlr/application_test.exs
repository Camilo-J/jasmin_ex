defmodule JasminEx.Dlr.ApplicationTest do
  use ExUnit.Case, async: true

  alias JasminEx.Application
  alias JasminEx.Dlr.Config
  alias JasminEx.Dlr.Supervisor, as: DlrSupervisor
  alias JasminEx.Messaging.RabbitMQ.Connection
  alias JasminEx.Messaging.RabbitMQ.Supervisor, as: MessagingSupervisor
  alias JasminEx.Messaging.RabbitMQ.TopicPublisher
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.Server.Supervisor, as: ServerSupervisor
  alias JasminEx.StateStore.Redix, as: StateStore

  @messaging [
    enabled: true,
    host: "broker.example",
    username: "app",
    password: "secret"
  ]

  test "omits DLR supervision when DLR config is absent or disabled" do
    refute Enum.any?(Application.children([]), &dlr_child?/1)

    refute Enum.any?(
             Application.children(dlr: [enabled: false]),
             &dlr_child?/1
           )
  end

  test "places one enabled DLR supervisor after messaging and before public interfaces" do
    children =
      Application.children(
        messaging: @messaging,
        dlr: [enabled: true],
        smpp_connectors: [%{name: :connector}],
        smpp_server: [enabled: true, port: 2775],
        http_api: [enabled: true, port: 0]
      )

    assert [
             _state_store,
             _router,
             {MessagingSupervisor, _messaging_opts},
             {DlrSupervisor, dlr_opts},
             {ConnectorSupervisor, _connector_opts},
             {ServerSupervisor, _server_opts},
             {JasminEx.HttpApi.Supervisor, _http_opts}
           ] = children

    assert %Config{enabled: true} = dlr_opts[:config]
    assert {StateStore, %{connection: JasminEx.StateStore.Connection}} = dlr_opts[:store]
    assert dlr_opts[:connection_server] == Connection
    assert dlr_opts[:publisher] == {TopicPublisher, TopicPublisher}
    assert dlr_opts[:messaging_config].host == "broker.example"
    assert dlr_opts[:http_client] == {JasminEx.Dlr.HttpClient.Mint, []}
    assert Enum.count(children, &dlr_child?/1) == 1
  end

  test "explicit test-only callback approval reaches the DLR boundary without changing defaults" do
    client = {JasminEx.Dlr.HttpClient.Mint, [allow: [{"callback.test", {127, 0, 0, 1}}]]}

    assert {DlrSupervisor, opts} =
             Application.children(
               messaging: @messaging,
               dlr: [enabled: true, http_client: client]
             )
             |> Enum.find(&dlr_child?/1)

    assert opts[:http_client] == client
  end

  test "enabled DLR injects both connector receipt and known-response publishers" do
    connector = [
      connector_id: "c1",
      host: ~c"127.0.0.1",
      port: 2775,
      system_id: "u",
      password: "p",
      system_type: "t",
      bind_as: :transceiver
    ]

    assert {ConnectorSupervisor, [configured]} =
             Application.children(
               messaging: @messaging,
               dlr: [enabled: true],
               smpp_connectors: [connector]
             )
             |> Enum.find(fn
               {module, _opts} -> module == ConnectorSupervisor
               _ -> false
             end)

    assert configured[:dlr_enabled] == true
    assert configured[:dlr_publisher] == {TopicPublisher, TopicPublisher}
    assert is_function(configured[:dlr_known_publisher], 2)
  end

  test "passes only explicit DLR dependency overrides" do
    store = {TestStore, :store_context}
    publisher = {TestPublisher, :publisher_context}

    assert {_module, opts} =
             Application.children(
               messaging: @messaging,
               dlr: [
                 enabled: true,
                 store: store,
                 connection_server: :test_connection,
                 publisher: publisher,
                 ignored_dependency: :not_propagated
               ]
             )
             |> Enum.find(&dlr_child?/1)

    assert opts[:store] == store
    assert opts[:connection_server] == :test_connection
    assert opts[:publisher] == publisher
    refute Keyword.has_key?(opts, :ignored_dependency)
  end

  test "rejects enabled DLR when messaging is not enabled" do
    assert_raise ArgumentError, ~r/enabled DLR requires enabled RabbitMQ messaging/, fn ->
      Application.children(dlr: [enabled: true])
    end

    assert_raise ArgumentError, ~r/enabled DLR requires enabled RabbitMQ messaging/, fn ->
      Application.children(
        messaging: [enabled: false],
        dlr: [enabled: true]
      )
    end
  end

  test "rejects invalid enabled DLR configuration before assembling children" do
    assert_raise ArgumentError, ~r/invalid DLR configuration/, fn ->
      Application.children(
        messaging: @messaging,
        dlr: [enabled: true, queue_prefix: ""]
      )
    end
  end

  defp dlr_child?({DlrSupervisor, _opts}), do: true
  defp dlr_child?(_child), do: false
end
