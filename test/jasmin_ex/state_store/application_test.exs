defmodule JasminEx.StateStore.ApplicationTest do
  use ExUnit.Case, async: false

  alias JasminEx.Application
  alias JasminEx.StateStore.Connection

  test "places one named availability-tolerant Redix child before SMPP supervision" do
    [state_store, smpp] = Application.children(smpp_connectors: [%{name: :connector}])

    assert state_store.id == Connection

    assert {Redix, :start_link, [options]} = state_store.start
    assert options[:name] == Connection
    assert options[:sync_connect] == false
    assert options[:exit_on_disconnection] == false
    assert options[:database] == 0
    assert options[:backoff_initial] == 100
    assert options[:backoff_max] == 5_000

    assert {JasminEx.Smpp.ConnectorSupervisor, [%{name: :connector}]} = smpp
  end

  test "restarts the named Redix child after an unexpected termination" do
    original_pid = Process.whereis(Connection)
    monitor = Process.monitor(original_pid)

    Process.exit(original_pid, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^original_pid, :killed}
    replacement_pid = wait_for_restarted_child(original_pid)
    assert Process.whereis(Connection) == replacement_pid
  end

  test "passes configured authentication, TLS, and finite health checks to the connection child" do
    [state_store] =
      Application.children(
        state_store: [
          endpoint: "rediss://cache.example:6380",
          username: "app",
          password: "secret",
          connect_timeout_ms: 10,
          command_timeout_ms: 20,
          health_check_timeout_ms: 30,
          backoff_initial_ms: 10,
          backoff_max_ms: 20
        ]
      )

    assert {Redix, :start_link, [options]} = state_store.start
    assert options[:host] == "cache.example"
    assert options[:port] == 6380
    assert options[:username] == "app"
    assert options[:password] == "secret"
    assert options[:ssl] == true
    assert options[:backoff_initial] == 10
    assert options[:backoff_max] == 20
    assert options[:timeout] == 10
    assert options[:health_check_interval] == 30
    refute Keyword.has_key?(options, :health_check_timeout)
  end

  defp wait_for_restarted_child(original_pid, attempts \\ 50)

  defp wait_for_restarted_child(_original_pid, 0), do: flunk("Redix child was not restarted")

  defp wait_for_restarted_child(original_pid, attempts) do
    case List.keyfind(Supervisor.which_children(JasminEx.Supervisor), Connection, 0) do
      {Connection, replacement_pid, :worker, [Redix]} when replacement_pid != original_pid ->
        replacement_pid

      _other ->
        Process.sleep(10)
        wait_for_restarted_child(original_pid, attempts - 1)
    end
  end
end
