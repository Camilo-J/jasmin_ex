defmodule JasminEx.Routing.ApplicationTest do
  use ExUnit.Case, async: false

  alias JasminEx.Application
  alias JasminEx.Messaging.RabbitMQ.Supervisor, as: MessagingSupervisor
  alias JasminEx.Routing.Router
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.StateStore.Connection

  test "supervises router after state store and before messaging and SMPP" do
    children = enabled_children()
    assert [state_store, router, messaging, smpp] = children
    assert state_store.id == Connection
    assert {Router, opts} = router
    assert %JasminEx.Routing.Config{} = opts[:config]
    assert {MessagingSupervisor, _} = messaging
    assert {ConnectorSupervisor, [%{name: :connector}]} = smpp
  end

  test "restore failure blocks later children" do
    dir = Path.join(System.tmp_dir!(), "jr-app-#{System.unique_integer([:positive])}")
    path = Path.join(dir, "routing-v1.json")
    File.mkdir_p!(dir)
    File.write!(path, "not-json")

    [_state_store, router | later] =
      Application.children(
        routing: [snapshot_path: path, name: :"rt-#{System.unique_integer([:positive])}"],
        messaging: messaging_opts(),
        smpp_connectors: [%{name: :connector}]
      )

    assert {Router, _} = router
    assert Enum.any?(later, &match?({MessagingSupervisor, _}, &1))
    assert Enum.any?(later, &match?({ConnectorSupervisor, _}, &1))

    probe = :"probe-#{System.unique_integer([:positive])}"
    probe_child = %{id: :probe, start: {Agent, :start_link, [fn -> :ok end, [name: probe]]}}
    Process.flag(:trap_exit, true)

    assert {:error, {:shutdown, {:failed_to_start_child, Router, _reason}}} =
             Supervisor.start_link([router, probe_child], strategy: :one_for_one)

    refute Process.whereis(probe)
  end

  defp enabled_children do
    Application.children(
      routing: [snapshot_path: "var/jasmin_ex/routing-v1.json"],
      messaging: messaging_opts(),
      smpp_connectors: [%{name: :connector}]
    )
  end

  defp messaging_opts do
    [
      enabled: true,
      host: "broker.example",
      username: "app",
      password: "secret",
      queue_prefix: "jasmin.work"
    ]
  end
end
