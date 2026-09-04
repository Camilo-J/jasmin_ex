defmodule JasminEx.HttpApi.ApplicationTest do
  use ExUnit.Case, async: true

  alias JasminEx.Application
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
        http_api: [enabled: true, port: 0]
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
    assert opts[:router] == JasminEx.Routing.Router

    assert [
             %{id: Connection},
             {JasminEx.Routing.Router, _},
             {HttpSupervisor, no_smpp}
           ] = Application.children(http_api: [enabled: true, port: 1401])

    assert no_smpp[:config].port == 1401
  end

  defp http_child?({HttpSupervisor, _opts}), do: true
  defp http_child?(_child), do: false
end
