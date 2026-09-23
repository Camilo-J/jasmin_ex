defmodule JasminEx.Messaging.RabbitMQ.TopicPublisher do
  @moduledoc false
  use GenServer

  alias JasminEx.Messaging.RabbitMQ.Connection

  @exchange "messaging"

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name_opts(Keyword.get(opts, :name, __MODULE__)))

  def child_spec(opts), do: opts |> super() |> Map.put(:id, Keyword.get(opts, :id, __MODULE__))

  def publish(server \\ __MODULE__, routing_key, payload, properties \\ [])
      when is_binary(routing_key) and is_binary(payload) and is_list(properties) do
    GenServer.call(server, {:publish, routing_key, payload, properties})
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

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
  def handle_call({:publish, routing_key, payload, properties}, _from, state) do
    state = ensure(state)

    if state.channel do
      {reply, state} = publish_once(state, routing_key, payload, properties)
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

  defp publish_once(state, routing_key, payload, properties) do
    client = state.client
    ch = state.channel
    {:ok, box} = Agent.start(fn -> false end)
    collector = spawn_return_collector(box)

    result =
      with :ok <- client.return(ch, collector),
           :ok <-
             client.publish(ch, @exchange, routing_key, payload, publish_opts(properties)) do
        confirm = client.wait_for_confirms(ch, state.config.confirm_timeout_ms)
        classify(confirm, returned?(box), state)
      else
        {:error, :channel_closed} = error -> {error, reset(state)}
        {:error, reason} -> {{:error, reason}, state}
      end

    if Process.alive?(collector), do: Process.exit(collector, :kill)
    if Process.alive?(box), do: Agent.stop(box)
    result
  end

  defp classify(true, true, state), do: {{:error, :unroutable}, state}
  defp classify(false, true, state), do: {{:error, :unroutable}, state}
  defp classify(true, false, state), do: {:ok, state}
  defp classify(false, false, state), do: {{:error, :non_ok}, state}
  defp classify(:timeout, _returned, state), do: {{:ambiguous, :timeout}, reset(state)}

  defp classify({:error, :channel_closed}, _returned, state),
    do: {{:ambiguous, :channel_closed}, reset(state)}

  defp classify({:error, reason}, _returned, state), do: {{:error, reason}, state}

  defp publish_opts(properties) do
    properties
    |> Keyword.put_new(:mandatory, true)
    |> Keyword.put_new(:persistent, true)
  end

  defp spawn_return_collector(box) do
    spawn(fn ->
      receive do
        {:basic_return, _payload, _meta} -> Agent.update(box, fn _ -> true end)
      after
        5_000 -> :ok
      end
    end)
  end

  defp returned?(box) do
    if Agent.get(box, & &1) do
      true
    else
      Process.sleep(5)
      Agent.get(box, & &1)
    end
  end

  defp ensure(%{channel: ch} = state) when not is_nil(ch), do: state

  defp ensure(state) do
    close(state)

    with {:ok, conn} <- resolve(state),
         {:ok, ch} <- state.client.open_channel(conn) do
      case state.client.select_confirms(ch) do
        :ok ->
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
    if Process.alive?(ch.pid), do: client.close_channel(ch)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
