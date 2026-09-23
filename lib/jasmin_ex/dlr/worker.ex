defmodule JasminEx.Dlr.Worker do
  @moduledoc false
  use GenServer

  alias JasminEx.Dlr.{RetryPolicy, Telemetry}
  alias JasminEx.Messaging.RabbitMQ.Connection

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name_opts(Keyword.get(opts, :name, __MODULE__)))

  def child_spec(opts), do: opts |> super() |> Map.put(:id, Keyword.get(opts, :id, __MODULE__))

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok,
     consume(%{
       queue_kind: Keyword.fetch!(opts, :queue_kind),
       additional_attempts: Keyword.get(opts, :additional_attempts),
       queue: Keyword.fetch!(opts, :queue),
       processor: Keyword.fetch!(opts, :processor),
       client: Keyword.get(opts, :client, JasminEx.Messaging.RabbitMQ.Client),
       connection: Keyword.get(opts, :connection),
       connection_server: Keyword.get(opts, :connection_server),
       reconnect_backoff_ms: min(max(Keyword.get(opts, :reconnect_backoff_ms, 250), 25), 5_000),
       channel: nil,
       mon: nil,
       inflight: nil
     })}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, state),
    do: {:noreply, handle_delivery(state, payload, meta)}

  def handle_info({:DOWN, ref, :process, _, _}, %{mon: ref} = state),
    do: {:noreply, recover(state)}

  def handle_info({:basic_cancel, _meta}, state), do: {:noreply, recover(state)}
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}
  def handle_info(:retry_consume, %{channel: nil} = state), do: {:noreply, consume(state)}
  def handle_info(:retry_consume, state), do: {:noreply, state}
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state), do: close(state)

  defp handle_delivery(%{channel: nil} = state, _payload, _meta), do: state

  defp handle_delivery(state, payload, meta) do
    channel = state.channel
    tag = meta.delivery_tag
    state = %{state | inflight: {channel, tag}}
    outcome = dispatch(state, payload, meta)
    settle(state, channel, tag, meta, outcome)
  end

  defp dispatch(state, payload, meta) do
    cond do
      not allowed?(state.queue_kind, meta.routing_key) ->
        :terminal

      not processable?(state, meta) ->
        :terminal

      true ->
        invoke_processor(state.processor, payload, meta)
    end
  end

  defp processable?(state, meta) do
    additional = state.additional_attempts || RetryPolicy.additional_attempts(state.queue_kind)

    case RetryPolicy.failures(state.queue_kind, meta, additional) do
      {:ok, count} -> count <= additional
      {:error, _reason} -> false
    end
  end

  def invoke_processor(processor, payload, meta) when is_function(processor, 2),
    do: processor.(payload, meta)

  def invoke_processor({module, function}, payload, meta),
    do: apply(module, function, [payload, meta])

  def invoke_processor({module, function, context}, payload, meta),
    do: apply(module, function, [payload, meta, context])

  defp settle(%{inflight: {channel, tag}} = state, channel, tag, meta, outcome) do
    if live_channel?(channel) do
      apply_settle(state, channel, tag, meta, outcome)
      emit(outcome, state)
    end

    %{state | inflight: nil}
  end

  defp settle(state, _channel, _tag, _meta, _outcome), do: %{state | inflight: nil}

  defp apply_settle(state, channel, tag, meta, outcome) do
    case RetryPolicy.settle(
           state.queue_kind,
           meta,
           outcome_for_policy(outcome),
           state.additional_attempts
         ) do
      :ack -> state.client.ack(channel, tag)
      {:reject, opts} -> state.client.reject(channel, tag, opts)
    end
  end

  defp live_channel?(%{pid: pid}), do: Process.alive?(pid)
  defp live_channel?(_channel), do: true

  defp outcome_for_policy(:ok), do: :ok
  defp outcome_for_policy(:retry), do: :retry
  defp outcome_for_policy(_outcome), do: :terminal

  defp allowed?(:lookup, "dlr.submit_sm_resp"), do: true
  defp allowed?(:lookup, "dlr.deliver_sm"), do: true
  defp allowed?(:http, "dlr_thrower.http"), do: true
  defp allowed?(_kind, _key), do: false

  defp consume(state) do
    with {:ok, conn} <- resolve(state),
         {:ok, ch} <- state.client.open_channel(conn) do
      consume_opened(state, ch)
    else
      _ -> retry_later(state)
    end
  catch
    :exit, _reason -> retry_later(state)
  end

  defp consume_opened(state, ch) do
    with :ok <- state.client.qos(ch, prefetch_count: 1),
         {:ok, _tag} <- state.client.consume(ch, state.queue, self(), no_ack: false) do
      %{state | channel: ch, mon: Process.monitor(ch.pid), inflight: nil}
    else
      _ -> close_and_retry(state, ch)
    end
  catch
    :exit, _reason -> close_and_retry(state, ch)
  end

  defp close_and_retry(state, ch) do
    close(%{state | channel: ch})
    retry_later(state)
  end

  defp retry_later(state) do
    Process.send_after(self(), :retry_consume, state.reconnect_backoff_ms)
    %{state | channel: nil, mon: nil, inflight: nil}
  end

  defp recover(state) do
    close(%{state | inflight: nil})
    consume(%{state | channel: nil, mon: nil, inflight: nil})
  end

  defp resolve(%{connection: conn}) when not is_nil(conn), do: {:ok, conn}

  defp resolve(%{connection_server: server}) when not is_nil(server),
    do: Connection.get(server)

  defp resolve(_), do: {:error, :disconnected}

  defp close(%{channel: nil}), do: :ok

  defp close(%{client: client, channel: ch, mon: mon}) do
    if is_reference(mon), do: Process.demonitor(mon, [:flush])
    if live_channel?(ch), do: client.close_channel(ch)
    :ok
  catch
    :exit, _reason -> :ok
  end

  defp emit(outcome, state) do
    reason = if outcome in [:ok, :retry], do: outcome, else: :terminal

    Telemetry.emit([:settlement], %{}, %{
      phase: state.queue_kind,
      reason_class: reason
    })
  end

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
