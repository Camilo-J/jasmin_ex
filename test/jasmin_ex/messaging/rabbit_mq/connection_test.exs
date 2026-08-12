defmodule JasminEx.Messaging.RabbitMQ.ConnectionTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.RabbitMQ.{Config, Connection}

  defmodule Fake do
    def open_connection(opts) do
      owner = Keyword.fetch!(opts, :test_owner)
      send(owner, {:open_connection, opts})
      {:ok, agent} = Agent.start_link(fn -> :open end)
      {:ok, %{pid: agent}}
    end

    def close_connection(%{pid: pid}) do
      if Process.alive?(pid), do: Agent.stop(pid)
      :ok
    catch
      :exit, _ -> :ok
    end

    def connection_pid(%{pid: pid}), do: pid
  end

  setup do
    config = Config.new!(host: "b", username: "u", password: "p", confirm_timeout_ms: 50)
    {:ok, config: config}
  end

  test "opens a monitored connection and returns it", %{config: config} do
    {:ok, server} = start(config)
    assert {:ok, %{pid: pid}} = Connection.get(server)
    assert Process.alive?(pid)
    assert_received {:open_connection, _}
    GenServer.stop(server)
  end

  test "reconnects after the broker connection dies", %{config: config} do
    {:ok, server} = start(config, reconnect_backoff_ms: 5)
    assert {:ok, %{pid: first}} = Connection.get(server)
    ref = Process.monitor(first)
    Agent.stop(first)
    assert_receive {:DOWN, ^ref, :process, ^first, _}
    assert {:ok, %{pid: second}} = wait(server, first)
    assert second != first
    GenServer.stop(server)
  end

  test "keeps a connection restored before a scheduled reconnect", %{config: config} do
    {:ok, server} = start(config, reconnect_backoff_ms: 1_000)
    assert {:ok, %{pid: first}} = Connection.get(server)
    ref = Process.monitor(first)
    Agent.stop(first)
    assert_receive {:DOWN, ^ref, :process, ^first, _}
    assert {:ok, %{pid: restored}} = wait(server, first)

    send(server, :reconnect)

    assert Process.alive?(restored)
    assert {:ok, %{pid: ^restored}} = Connection.get(server)
    GenServer.stop(server)
  end

  defp start(config, extra \\ []) do
    Connection.start_link(
      Keyword.merge(
        [config: config, client: Fake, client_opts: [test_owner: self()], name: nil],
        extra
      )
    )
  end

  defp wait(server, old, n \\ 50)
  defp wait(_, _, 0), do: flunk("no reconnect")

  defp wait(server, old, n) do
    case Connection.get(server) do
      {:ok, %{pid: pid}} when pid != old -> {:ok, %{pid: pid}}
      _ -> Process.sleep(5) && wait(server, old, n - 1)
    end
  end
end
