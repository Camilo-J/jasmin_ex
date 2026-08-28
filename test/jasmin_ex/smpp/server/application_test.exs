defmodule JasminEx.Smpp.Server.ApplicationTest do
  use ExUnit.Case, async: true

  alias JasminEx.Application
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.Server.Supervisor, as: ServerSupervisor
  alias JasminEx.StateStore.Connection

  test "omits SMPP server when config is absent or disabled" do
    refute Enum.any?(Application.children([]), &server_child?/1)
    refute Enum.any?(Application.children(smpp_server: [enabled: false]), &server_child?/1)
    assert [%{id: Connection}, {JasminEx.Routing.Router, _}] = Application.children([])
  end

  test "places enabled SMPP server after outbound connectors" do
    children =
      Application.children(
        smpp_connectors: [%{name: :connector}],
        smpp_server: [enabled: true, port: 0]
      )

    assert [
             %{id: Connection},
             {JasminEx.Routing.Router, _},
             {ConnectorSupervisor, [%{name: :connector}]},
             {ServerSupervisor, opts}
           ] = children

    assert opts[:config].enabled
    assert opts[:config].port == 0
    assert opts[:router] == JasminEx.Routing.Router

    assert [
             %{id: Connection},
             {JasminEx.Routing.Router, _},
             {ServerSupervisor, no_connectors}
           ] = Application.children(smpp_server: [enabled: true, port: 2775])

    assert no_connectors[:config].port == 2775
  end

  defp server_child?({ServerSupervisor, _opts}), do: true
  defp server_child?(_child), do: false
end
