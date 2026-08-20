defmodule JasminEx.Messaging.RabbitMQ.Connection do
  @moduledoc false
  use GenServer

  alias JasminEx.Messaging.RabbitMQ.{Config, Telemetry}

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name_opts(Keyword.get(opts, :name, __MODULE__)))

  def child_spec(opts), do: opts |> super() |> Map.put(:id, Keyword.get(opts, :id, __MODULE__))
  def get(server \\ __MODULE__), do: GenServer.call(server, :get)

  @impl true
  def init(opts) do
    state = %{
      config: Keyword.fetch!(opts, :config),
      client: Keyword.get(opts, :client, JasminEx.Messaging.RabbitMQ.Client),
      client_opts: Keyword.get(opts, :client_opts, []),
      backoff_ms: Keyword.get(opts, :reconnect_backoff_ms, 250),
      connection: nil,
      monitor: nil,
      recovering: false
    }

    {:ok, connect(state)}
  end

  @impl true
  def handle_call(:get, _from, %{connection: nil} = state) do
    state = connect(state)
    reply = if state.connection, do: {:ok, state.connection}, else: {:error, :disconnected}
    {:reply, reply, state}
  end

  def handle_call(:get, _from, state), do: {:reply, {:ok, state.connection}, state}

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, %{monitor: ref} = state) do
    Telemetry.emit([:connection, :down], %{}, Telemetry.connection_meta(state.config))
    Process.send_after(self(), :reconnect, state.backoff_ms)
    {:noreply, %{state | connection: nil, monitor: nil, recovering: true}}
  end

  def handle_info(:reconnect, %{connection: nil} = state), do: {:noreply, connect(state)}
  def handle_info(:reconnect, state), do: {:noreply, state}
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state), do: close(state)

  defp connect(state) do
    close(state)
    opts = Config.to_connection_options(state.config) ++ state.client_opts

    case state.client.open_connection(opts) do
      {:ok, conn} ->
        meta = Telemetry.connection_meta(state.config)
        if state.recovering, do: Telemetry.emit([:recovery], %{}, meta)
        Telemetry.emit([:connection, :up], %{}, meta)
        mon = Process.monitor(state.client.connection_pid(conn))
        %{state | connection: conn, monitor: mon, recovering: false}

      {:error, _} ->
        Process.send_after(self(), :reconnect, state.backoff_ms)
        %{state | connection: nil, monitor: nil}
    end
  end

  defp close(%{connection: nil}), do: :ok

  defp close(%{client: client, connection: conn, monitor: mon}) do
    if is_reference(mon), do: Process.demonitor(mon, [:flush])
    _ = client.close_connection(conn)
    :ok
  end

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
