defmodule JasminEx.Smpp.ConnectorSupervisorTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias JasminEx.Application
  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.ConnectorSupervisor
  alias JasminEx.Smpp.FakeSMSC

  test "supervises a transient client that is not restarted after graceful unbind" do
    {:ok, port, smsc} = FakeSMSC.start_link()

    {:ok, supervisor} =
      ConnectorSupervisor.start_link(
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
      )

    [{_id, client, :worker, [Client]}] = Supervisor.which_children(supervisor)
    assert :ok = wait_until(fn -> Client.status(client) == :bound end)
    assert :ok = Client.unbind(client)

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
