defmodule JasminEx.Messaging.RabbitMQ.ConnectorWorker do
  @moduledoc false
  use GenServer

  alias JasminEx.Messaging.RabbitMQ.Connection

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name_opts(Keyword.get(opts, :name, __MODULE__)))

  def child_spec(opts), do: opts |> super() |> Map.put(:id, Keyword.get(opts, :id, __MODULE__))

  def inflight(server \\ __MODULE__), do: GenServer.call(server, :inflight)

  @impl true
  def init(opts) do
    {:ok,
     %{
       config: Keyword.fetch!(opts, :config),
       client: Keyword.get(opts, :client, JasminEx.Messaging.RabbitMQ.Client),
       connection: Keyword.get(opts, :connection),
       connection_server: Keyword.get(opts, :connection_server),
       connector_id: Keyword.fetch!(opts, :connector_id),
       channel: nil,
       mon: nil,
       consumer_tag: nil,
       inflight: nil
     }}
  end

  @impl true
  def handle_call(:inflight, _from, state), do: {:reply, state.inflight, state}

  @impl true
  def handle_info({:smpp_bound, id, _kind}, %{connector_id: id} = state),
    do: {:noreply, consume(state)}

  def handle_info({:smpp_bind_lost, id, _reason}, %{connector_id: id} = state),
    do: {:noreply, teardown(state)}

  def handle_info({:DOWN, ref, :process, _, _}, %{mon: ref} = state),
    do: {:noreply, reset(state)}

  def handle_info({:basic_deliver, _payload, _meta} = delivery, %{inflight: nil} = state),
    do: {:noreply, %{state | inflight: delivery}}

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state), do: teardown(state)

  defp consume(%{channel: ch} = state) when not is_nil(ch), do: state

  defp consume(state) do
    with {:ok, conn} <- resolve(state),
         {:ok, ch} <- state.client.open_channel(conn) do
      consume_opened(state, ch)
    else
      _ -> state
    end
  end

  defp consume_opened(state, ch) do
    with :ok <- state.client.qos(ch, prefetch_count: 1),
         {:ok, tag} <- state.client.consume(ch, queue(state), self(), no_ack: false) do
      %{state | channel: ch, mon: Process.monitor(ch.pid), consumer_tag: tag}
    else
      _ ->
        _ = state.client.close_channel(ch)
        state
    end
  end

  defp teardown(%{channel: nil} = state), do: state

  defp teardown(%{client: client, channel: ch, mon: mon, consumer_tag: tag} = state) do
    if is_reference(mon), do: Process.demonitor(mon, [:flush])
    if is_binary(tag), do: client.cancel(ch, tag)
    _ = client.close_channel(ch)
    reset(state)
  end

  defp reset(state), do: %{state | channel: nil, mon: nil, consumer_tag: nil, inflight: nil}

  defp queue(state), do: state.config.queue_prefix <> "." <> state.connector_id

  defp resolve(%{connection: conn}) when not is_nil(conn), do: {:ok, conn}

  defp resolve(%{connection_server: server}) when not is_nil(server),
    do: Connection.get(server)

  defp resolve(_), do: {:error, :disconnected}

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
