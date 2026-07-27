defmodule JasminEx.Smpp.ConnectorSupervisorTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias JasminEx.Application
  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.FakeSMSC

  @invalid_connectors_message "invalid SMPP connector configuration: expected [], one non-empty keyword connector configuration, or a list of non-empty keyword connector configurations"

  test "uses an explicit three-restarts-in-five-seconds intensity" do
    assert {:ok, {flags, []}} = ConnectorSupervisor.init([])
    assert %{intensity: 3, period: 5, strategy: :one_for_one} = flags
  end

  test "normalizes zero, one, and multiple connector configurations" do
    first = connector_config(1111)
    second = connector_config(2222)

    assert {:ok, {_flags, []}} = ConnectorSupervisor.init([])

    assert {:ok, {_flags, [single]}} = ConnectorSupervisor.init(first)
    assert single.id == {:smpp_client, 0}
    assert single.start == {Client, :start_link, [first]}

    assert {:ok, {_flags, [first_child, second_child]}} =
             ConnectorSupervisor.init([first, second])

    assert first_child.start == {Client, :start_link, [first]}
    assert second_child.start == {Client, :start_link, [second]}
  end

  test "rejects invalid top-level and malformed list shapes" do
    valid = connector_config(1111)

    for invalid <- [%{}, :invalid, [valid, :invalid], [valid, []], [[:host]]] do
      assert_raise ArgumentError, @invalid_connectors_message, fn ->
        ConnectorSupervisor.init(invalid)
      end
    end
  end

  test "restarts a transient client with a new PID after abnormal exit" do
    {:ok, port, smsc} = FakeSMSC.start_link()
    {:ok, supervisor} = ConnectorSupervisor.start_link(connector_config(port))

    [{_id, first_client, :worker, [Client]}] = Supervisor.which_children(supervisor)
    assert :ok = wait_until(fn -> Client.status(first_client) == :bound end)

    Process.exit(first_client, :kill)

    assert :ok =
             wait_until(fn ->
               case Supervisor.which_children(supervisor) do
                 [{_id, client, :worker, [Client]}] when is_pid(client) ->
                   client != first_client and Client.status(client) == :bound

                 _children ->
                   false
               end
             end)

    [{_id, restarted_client, :worker, [Client]}] = Supervisor.which_children(supervisor)
    refute restarted_client == first_client

    GenServer.stop(supervisor)
    GenServer.stop(smsc)
  end

  test "supervises a transient client that is not restarted after graceful unbind" do
    {:ok, port, smsc} = FakeSMSC.start_link()

    {:ok, supervisor} =
      ConnectorSupervisor.start_link(connector_config(port))

    [{_id, client, :worker, [Client]}] = Supervisor.which_children(supervisor)
    assert :ok = wait_until(fn -> Client.status(client) == :bound end)
    monitor = Process.monitor(client)
    assert :ok = Client.unbind(client)
    assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 500

    assert :ok =
             wait_until(fn ->
               Enum.all?(Supervisor.which_children(supervisor), fn {_id, pid, _type, _modules} ->
                 pid == :undefined
               end)
             end)

    GenServer.stop(supervisor)
    GenServer.stop(smsc)
  end

  test "application only wires a connector supervisor when connector config is present" do
    assert Application.children([]) == []
    assert [{ConnectorSupervisor, [[]]}] = Application.children(smpp_connectors: [[]])
  end

  defp connector_config(port) do
    [
      host: ~c"localhost",
      port: port,
      system_id: "user",
      password: "pw",
      system_type: "type",
      bind_as: :transmitter,
      heartbeat_ms: 10_000,
      response_timeout_ms: 100,
      reconnect_base_ms: 5,
      reconnect_cap_ms: 5,
      reconnect_jitter: false
    ]
  end

  defp wait_until(predicate, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_until_deadline(predicate, deadline)
  end

  defp wait_until_deadline(predicate, deadline) do
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(5)
        wait_until_deadline(predicate, deadline)
    end
  end
end
