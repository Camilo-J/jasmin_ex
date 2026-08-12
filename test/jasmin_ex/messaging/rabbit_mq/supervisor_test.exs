defmodule JasminEx.Messaging.RabbitMQ.SupervisorTest do
  use ExUnit.Case, async: true

  alias JasminEx.Messaging.RabbitMQ.{Config, Connection, Publisher}
  alias JasminEx.Messaging.RabbitMQ.Supervisor, as: MessagingSupervisor

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

    def open_channel(conn) do
      {:ok, Map.put(conn, :channel_id, 1)}
    end

    def close_channel(_ch), do: :ok
    def select_confirms(_ch), do: :ok
  end

  setup do
    config =
      Config.new!(
        host: "broker.example",
        username: "u",
        password: "p",
        confirm_timeout_ms: 50
      )

    {:ok, config: config}
  end

  test "starts connection before publisher under rest_for_one", %{config: config} do
    connection_name = unique_name("conn")
    publisher_name = unique_name("pub")

    opts = [
      config: config,
      client: Fake,
      client_opts: [test_owner: self()],
      connection_name: connection_name,
      publisher_name: publisher_name,
      name: nil
    ]

    assert {:ok, {flags, child_specs}} = MessagingSupervisor.init(opts)
    assert flags.strategy == :rest_for_one
    assert Enum.map(child_specs, & &1.id) == [Connection, Publisher]

    assert {:ok, sup} = MessagingSupervisor.start_link(opts)

    children = Supervisor.which_children(sup)
    ids = MapSet.new(Enum.map(children, fn {id, _pid, _type, _modules} -> id end))
    assert ids == MapSet.new([Connection, Publisher])
    assert_received {:open_connection, _}

    connection_pid = child_pid(children, Connection)
    publisher_pid = child_pid(children, Publisher)
    assert Process.alive?(connection_pid)
    assert Process.alive?(publisher_pid)
    assert {:ok, _} = Connection.get(connection_name)

    Process.exit(sup, :shutdown)
  end

  test "publisher child receives the shared connection server name", %{config: config} do
    connection_name = unique_name("conn")
    publisher_name = unique_name("pub")

    assert {:ok, sup} =
             MessagingSupervisor.start_link(
               config: config,
               client: Fake,
               client_opts: [test_owner: self()],
               connection_name: connection_name,
               publisher_name: publisher_name,
               name: nil
             )

    assert Process.whereis(connection_name)
    assert Process.whereis(publisher_name)
    assert {:ok, _} = Connection.get(connection_name)

    Process.exit(sup, :shutdown)
  end

  defp unique_name(prefix), do: :"mq-#{prefix}-#{System.unique_integer([:positive])}"

  defp child_pid(children, id) do
    case List.keyfind(children, id, 0) do
      {^id, pid, :worker, _} when is_pid(pid) -> pid
      other -> flunk("expected worker #{inspect(id)}, got: #{inspect(other)}")
    end
  end
end
