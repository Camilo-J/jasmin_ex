defmodule JasminEx.Messaging.ApplicationTest do
  use ExUnit.Case, async: true

  alias JasminEx.Application
  alias JasminEx.Messaging.RabbitMQ.Supervisor, as: MessagingSupervisor
  alias JasminEx.StateStore.Connection

  test "omits messaging supervision when messaging config is absent" do
    children = Application.children(smpp_connectors: [%{name: :connector}])

    refute Enum.any?(children, &messaging_child?/1)
    assert match?([%{id: Connection}, {JasminEx.Smpp.ConnectorSupervisor, _}], children)
  end

  test "omits messaging supervision when messaging is explicitly disabled" do
    children =
      Application.children(
        messaging: [enabled: false, host: "b", username: "u", password: "p"],
        smpp_connectors: [%{name: :connector}]
      )

    refute Enum.any?(children, &messaging_child?/1)
    assert length(children) == 2
  end

  test "places messaging supervisor after state store and before SMPP when enabled" do
    children =
      Application.children(
        messaging: [
          enabled: true,
          host: "broker.example",
          username: "app",
          password: "secret",
          queue_prefix: "jasmin.work"
        ],
        smpp_connectors: [%{name: :connector}]
      )

    assert [state_store, messaging, smpp] = children
    assert state_store.id == Connection
    assert {MessagingSupervisor, opts} = messaging
    assert opts[:config].host == "broker.example"
    assert opts[:config].username == "app"
    assert opts[:config].queue_prefix == "jasmin.work"
    assert opts[:config].password == "secret"
    assert {JasminEx.Smpp.ConnectorSupervisor, [%{name: :connector}]} = smpp
  end

  test "defaults messaging to disabled when only empty messaging keyword list is provided" do
    children = Application.children(messaging: [], smpp_connectors: [])

    assert [%{id: Connection}] = children
    refute Enum.any?(children, &messaging_child?/1)
  end

  defp messaging_child?({MessagingSupervisor, _opts}), do: true
  defp messaging_child?(_), do: false
end
