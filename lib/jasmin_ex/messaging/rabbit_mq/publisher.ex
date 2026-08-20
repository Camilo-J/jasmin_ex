defmodule JasminEx.Messaging.RabbitMQ.Publisher do
  @moduledoc false
  use GenServer

  alias JasminEx.Messaging.RabbitMQ.{Connection, Telemetry}

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name_opts(Keyword.get(opts, :name, __MODULE__)))

  def child_spec(opts), do: opts |> super() |> Map.put(:id, Keyword.get(opts, :id, __MODULE__))

  def publish(server \\ __MODULE__, connector_id, payload)
      when is_binary(connector_id) and is_binary(payload),
      do: GenServer.call(server, {:publish, connector_id, payload})

  @impl true
  def init(opts) do
    {:ok,
     ensure(%{
       config: Keyword.fetch!(opts, :config),
       client: Keyword.get(opts, :client, JasminEx.Messaging.RabbitMQ.Client),
       connection: Keyword.get(opts, :connection),
       connection_server: Keyword.get(opts, :connection_server),
       channel: nil,
       mon: nil
     })}
  end

  @impl true
  def handle_call({:publish, connector_id, payload}, _from, state) do
    state = ensure(state)

    if state.channel do
      {reply, state} = publish_once(state, connector_id, payload)
      {:reply, reply, state}
    else
      {:reply, {:error, :channel_closed}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, %{mon: ref} = state),
    do: {:noreply, %{state | channel: nil, mon: nil}}

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state), do: close(state)

  defp publish_once(state, connector_id, payload) do
    queue = state.config.queue_prefix <> "." <> connector_id
    ch = state.channel
    client = state.client

    with {:ok, _} <- client.declare_queue(ch, queue, durable: true),
         :ok <- client.publish(ch, "", queue, payload, persistent: true) do
      started = System.monotonic_time(:millisecond)
      confirm = client.wait_for_confirms(ch, state.config.confirm_timeout_ms)
      latency_ms = max(System.monotonic_time(:millisecond) - started, 0)
      result = Telemetry.confirm_result(confirm)
      meta = %{result: result, connector_id: connector_id}
      Telemetry.emit([:confirm], %{latency_ms: latency_ms}, meta)

      case confirm do
        true -> {:ok, state}
        false -> {{:error, :nack}, state}
        :timeout -> {{:error, :timeout}, state}
        {:error, :channel_closed} = error -> {error, reset(state)}
        {:error, reason} -> {{:error, reason}, state}
      end
    else
      {:error, :channel_closed} = error -> {error, reset(state)}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp ensure(%{channel: ch} = state) when not is_nil(ch), do: state

  defp ensure(state) do
    close(state)

    with {:ok, conn} <- resolve(state),
         {:ok, ch} <- state.client.open_channel(conn) do
      case state.client.select_confirms(ch) do
        :ok ->
          Telemetry.emit([:channel, :up], %{}, %{role: :publisher})
          %{state | channel: ch, mon: Process.monitor(ch.pid)}

        _ ->
          _ = state.client.close_channel(ch)
          %{state | channel: nil, mon: nil}
      end
    else
      _ -> %{state | channel: nil, mon: nil}
    end
  end

  defp resolve(%{connection: conn}) when not is_nil(conn), do: {:ok, conn}

  defp resolve(%{connection_server: server}) when not is_nil(server),
    do: Connection.get(server)

  defp resolve(_), do: {:error, :disconnected}

  defp reset(state) do
    close(state)
    %{state | channel: nil, mon: nil}
  end

  defp close(%{channel: nil}), do: :ok

  defp close(%{client: client, channel: ch, mon: mon}) do
    if is_reference(mon), do: Process.demonitor(mon, [:flush])
    _ = client.close_channel(ch)
    :ok
  end

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
