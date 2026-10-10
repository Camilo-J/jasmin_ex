defmodule JasminEx.HttpApi.ApplicationTest do
  use ExUnit.Case, async: true

  alias JasminEx.Application
  alias JasminEx.Dlr.Config, as: DlrConfig
  alias JasminEx.HttpApi.Supervisor, as: HttpSupervisor
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.Server.Supervisor, as: ServerSupervisor
  alias JasminEx.StateStore.Connection

  test "omits HTTP API when config is absent or disabled" do
    refute Enum.any?(Application.children([]), &http_child?/1)
    refute Enum.any?(Application.children(http_api: [enabled: false]), &http_child?/1)
    refute Enum.any?(Application.children(http_api: []), &http_child?/1)
    assert [%{id: Connection}, {JasminEx.Routing.Router, _}] = Application.children([])
  end

  test "places enabled HTTP API after existing children" do
    children =
      Application.children(
        smpp_connectors: [%{name: :connector}],
        smpp_server: [enabled: true, port: 2775],
        http_api: [enabled: true, port: 0, concat: :sar, max_segments: 3]
      )

    assert [
             %{id: Connection},
             {JasminEx.Routing.Router, _},
             {ConnectorSupervisor, [%{name: :connector}]},
             {ServerSupervisor, _server_opts},
             {HttpSupervisor, opts}
           ] = children

    assert opts[:config].enabled
    assert opts[:config].port == 0
    assert opts[:config].host == {127, 0, 0, 1}
    assert opts[:config].concat == :sar
    assert opts[:config].max_segments == 3
    assert opts[:router] == JasminEx.Routing.Router

    assert {:ok,
            {_flags,
             [
               _metrics,
               _concat_reference,
               %{start: {Bandit, :start_link, [[{:plug, {_router, plug_opts}} | _]]}}
             ]}} = HttpSupervisor.init(opts)

    assert plug_opts.concat == :sar
    assert plug_opts.max_segments == 3
    assert plug_opts.pipeline == JasminEx.MtSubmitPipeline
    assert plug_opts.concat_reference == JasminEx.MtSubmitPipeline.ConcatReference

    assert [
             %{id: Connection},
             {JasminEx.Routing.Router, _},
             {HttpSupervisor, no_smpp}
           ] = Application.children(http_api: [enabled: true, port: 1401])

    assert no_smpp[:config].port == 1401
    refute Keyword.has_key?(no_smpp, :dlr_store)
    refute Keyword.has_key?(no_smpp, :dlr_config)
  end

  test "rejects malformed HTTP multipart configuration before assembling children" do
    assert_raise ArgumentError, "HTTP API concat must be :udh or :sar", fn ->
      Application.children(http_api: [enabled: true, concat: :invalid])
    end

    assert_raise ArgumentError, "HTTP API max_segments must be an integer from 1 to 5", fn ->
      Application.children(http_api: [enabled: true, max_segments: 6])
    end
  end

  test "passes enabled DLR store and config dependencies to HTTP API" do
    store = {TestStore, :store_context}

    children =
      Application.children(
        messaging: [
          enabled: true,
          host: "broker.example",
          username: "app",
          password: "secret"
        ],
        dlr: [enabled: true, store: store],
        http_api: [enabled: true, port: 0]
      )

    assert {HttpSupervisor, opts} = List.last(children)
    assert opts[:dlr_store] == store
    assert %DlrConfig{enabled: true} = opts[:dlr_config]
  end

  defp http_child?({HttpSupervisor, _opts}), do: true
  defp http_child?(_child), do: false
end
