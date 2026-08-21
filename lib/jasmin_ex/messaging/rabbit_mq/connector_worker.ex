defmodule JasminEx.Messaging.RabbitMQ.ConnectorWorker do
  @moduledoc false
  use GenServer

  alias JasminEx.Messaging.{Envelope, SettlementJournal, StateStoreJournal}
  alias JasminEx.Messaging.RabbitMQ.{Client, Connection, Telemetry}

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
       store: Keyword.get(opts, :store),
       submit: Keyword.get(opts, :submit),
       republish: Keyword.get(opts, :republish),
       bound: false,
       phase: :idle,
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
    do: {:noreply, resume(%{state | bound: true})}

  def handle_info({:smpp_bind_lost, id, _reason}, %{connector_id: id} = state),
    do: {:noreply, teardown(%{state | bound: false})}

  def handle_info({:DOWN, ref, :process, _, _}, %{mon: ref} = state),
    do: {:noreply, recover_down(state)}

  def handle_info({:basic_deliver, _payload, meta} = delivery, %{inflight: nil} = state) do
    if meta[:redelivered] do
      Telemetry.emit([:redelivery], %{}, %{connector_id: state.connector_id, redelivered: true})
    end

    {:noreply, dispatch(%{state | inflight: delivery, phase: :pre_submit})}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state), do: state |> settle_before_close() |> close_if_open()

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
      Telemetry.emit([:channel, :up], %{}, %{role: :consumer, connector_id: state.connector_id})
      Telemetry.emit([:consumer, :started], %{}, %{connector_id: state.connector_id})
      %{state | channel: ch, mon: Process.monitor(ch.pid), consumer_tag: tag}
    else
      _ ->
        _ = state.client.close_channel(ch)
        state
    end
  end

  defp teardown(%{channel: nil} = state), do: state

  defp teardown(state) do
    state = settle_before_close(state)

    case state.inflight do
      nil -> close_if_open(state)
      _ -> state
    end
  end

  defp resume(%{phase: :pre_submit} = state) do
    state = dispatch(state)
    if is_nil(state.inflight), do: state |> close_if_open() |> consume(), else: state
  end

  defp resume(state), do: consume(state)

  defp settle_before_close(%{phase: :pre_submit} = state), do: state

  defp settle_before_close(%{inflight: {:basic_deliver, payload, meta}} = state) do
    case Envelope.decode(payload) do
      {:ok, envelope} -> recover_unclassified(state, envelope, meta)
      {:error, _} -> finish(state, :reject, meta)
    end
  end

  defp settle_before_close(state), do: settle_classified(state)

  defp recover_unclassified(state, envelope, meta) do
    case persist_dispatching(state, envelope) do
      :ok ->
        settle_quarantine(state, envelope, meta, evidence(envelope, :post_write, :bind_lost))

      _ ->
        state
    end
  end

  defp recover_down(state) do
    state = %{state | channel: nil, mon: nil, consumer_tag: nil, inflight: nil, phase: :idle}
    if state.bound, do: consume(state), else: state
  end

  defp close_if_open(%{channel: nil} = state), do: state

  defp close_if_open(%{client: client, channel: ch, mon: mon, consumer_tag: tag} = state) do
    if is_binary(tag) do
      Telemetry.emit([:consumer, :stopped], %{}, %{connector_id: state.connector_id})
      Telemetry.emit([:channel, :down], %{}, %{role: :consumer, connector_id: state.connector_id})
    end

    if is_reference(mon), do: Process.demonitor(mon, [:flush])
    if is_binary(tag), do: client.cancel(ch, tag)
    _ = client.close_channel(ch)
    %{state | channel: nil, mon: nil, consumer_tag: nil}
  end

  defp dispatch(%{inflight: {:basic_deliver, payload, meta}} = state) do
    case Envelope.decode(payload) do
      {:ok, envelope} -> dispatch_valid(state, envelope, meta)
      {:error, _reason} -> finish(state, :reject, meta)
    end
  end

  defp dispatch_valid(state, envelope, meta) do
    if dispatchable?(state) do
      case StateStoreJournal.read(state.store, envelope.gateway_id, envelope.attempt) do
        :missing -> submit_fresh(state, envelope, meta)
        {:ok, record} -> settle_recorded(state, envelope, meta, record)
        _ -> state
      end
    else
      state
    end
  end

  defp submit_fresh(state, envelope, meta) do
    case persist_dispatching(state, envelope) do
      :ok ->
        classify(%{state | phase: :submit_attempted}, envelope, meta, state.submit.(envelope))

      _ ->
        state
    end
  end

  defp settle_recorded(state, envelope, meta, record) do
    case SettlementJournal.redelivery_directive(record) do
      {:retry, evidence} ->
        settle_retry(state, envelope, meta, evidence_from(evidence, envelope))

      {:quarantine, evidence} ->
        settle_quarantine(state, envelope, meta, evidence_from(evidence, envelope))
    end
  end

  defp dispatchable?(%{store: {module, _}, submit: submit})
       when is_atom(module) and is_function(submit, 1),
       do: true

  defp dispatchable?(_state), do: false

  defp persist_dispatching(%{store: {module, _} = store, config: config}, envelope)
       when is_atom(module) do
    record = SettlementJournal.dispatching(envelope.gateway_id, envelope.attempt)
    StateStoreJournal.write(store, record, config.default_expiry_ms)
  end

  defp persist_dispatching(_state, _envelope), do: {:error, :unavailable}

  defp classify(state, envelope, meta, {:ok, _id}), do: finish(state, :ack, meta, envelope)

  defp classify(state, envelope, meta, {:error, {:submit_rejected, _reason}}),
    do: finish(state, :reject, meta, envelope)

  defp classify(state, envelope, meta, {:error, reason})
       when reason in [:disconnected, :unbinding],
       do: settle_retry(state, envelope, meta, evidence(envelope, :pre_write, reason))

  defp classify(state, envelope, meta, {:unknown, reason}),
    do: settle_quarantine(state, envelope, meta, evidence(envelope, :post_write, reason))

  defp classify(state, _envelope, _meta, _other), do: state

  defp settle_retry(state, envelope, meta, evidence) do
    if retryable?(envelope) do
      confirm_action(state, envelope, meta, :not_sent, evidence, &retry_envelope/2)
    else
      settle_quarantine(state, envelope, meta, evidence)
    end
  end

  defp settle_quarantine(state, envelope, meta, evidence) do
    journal_state = if evidence.stage == :pre_write, do: :not_sent, else: :sent
    confirm_action(state, envelope, meta, journal_state, evidence, &quarantine_action/2)
  end

  defp confirm_action(
         %{republish: republish} = state,
         envelope,
         meta,
         journal_state,
         evidence,
         fun
       )
       when is_function(republish, 1) do
    with :ok <- persist_outcome(state, envelope, journal_state, evidence),
         {:ok, action} <- fun.(envelope, evidence),
         :ok <- republish.(action) do
      emit_action(state, action, envelope)
      finish(state, :ack, meta, envelope)
    else
      _ -> state
    end
  end

  defp confirm_action(state, _envelope, _meta, _journal_state, _evidence, _fun), do: state

  defp retry_envelope(envelope, _evidence) do
    with {:ok, next} <- increment(envelope), do: {:ok, {:retry, next}}
  end

  defp quarantine_action(envelope, evidence), do: {:ok, {:quarantine, envelope, evidence}}

  defp persist_outcome(%{store: store, config: config}, envelope, journal_state, evidence) do
    with {:ok, record} <- StateStoreJournal.read(store, envelope.gateway_id, envelope.attempt),
         {:ok, recorded} <- keep_or_record(record, {journal_state, evidence}) do
      StateStoreJournal.write(store, recorded, config.default_expiry_ms)
    end
  end

  defp keep_or_record(record, outcome) do
    case SettlementJournal.record_outcome(record, outcome) do
      {:ok, recorded} -> {:ok, recorded}
      {:error, :outcome_already_recorded} -> {:ok, record}
      other -> other
    end
  end

  defp increment(envelope) do
    Envelope.new(%{
      gateway_id: envelope.gateway_id,
      connector_id: envelope.connector_id,
      attempt: envelope.attempt + 1,
      max_attempts: envelope.max_attempts,
      enqueued_at: envelope.enqueued_at,
      expires_at: envelope.expires_at,
      submit_sm: envelope.submit_sm
    })
  end

  defp retryable?(envelope),
    do: envelope.attempt < envelope.max_attempts and not expired?(envelope)

  defp expired?(%{expires_at: expires_at}) do
    case DateTime.from_iso8601(expires_at) do
      {:ok, datetime, _offset} -> DateTime.compare(datetime, DateTime.utc_now()) != :gt
      _ -> true
    end
  end

  defp evidence(envelope, stage, reason) do
    evidence_from(%{stage: stage, reason: reason}, envelope)
  end

  defp evidence_from(evidence, envelope) when is_map(evidence) do
    %{
      stage: normalize_stage(evidence[:stage] || evidence["stage"]),
      reason: normalize_reason(evidence[:reason] || evidence["reason"]),
      gateway_id: envelope.gateway_id,
      connector_id: envelope.connector_id,
      source_addr: envelope.submit_sm.source_addr,
      destination_addr: envelope.submit_sm.destination_addr
    }
  end

  defp normalize_stage(stage) when stage in [:pre_write, "pre_write"], do: :pre_write
  defp normalize_stage(_stage), do: :post_write

  @reasons %{
    "bind_lost" => :bind_lost,
    "disconnected" => :disconnected,
    "outcome_already_recorded" => :outcome_already_recorded,
    "response_timeout" => :response_timeout,
    "unbinding" => :unbinding,
    "uncertain" => :uncertain,
    "unresolved_dispatch" => :unresolved_dispatch
  }

  defp normalize_reason(reason) when is_atom(reason), do: reason
  defp normalize_reason(reason) when is_binary(reason), do: Map.get(@reasons, reason, :uncertain)
  defp normalize_reason(_reason), do: :uncertain

  defp emit_action(state, {:retry, _next}, envelope) do
    Telemetry.emit([:retry], %{}, %{
      connector_id: state.connector_id,
      gateway_id: envelope.gateway_id,
      attempt: envelope.attempt
    })
  end

  defp emit_action(state, {:quarantine, _envelope, _evidence}, envelope) do
    age_ms = Telemetry.age_ms(envelope)
    depth = observed_depth(state)
    ids = %{connector_id: state.connector_id, gateway_id: envelope.gateway_id}
    meta = Telemetry.quarantine_meta(state.config, depth, age_ms, ids)
    Telemetry.emit([:quarantine], %{depth: depth, age_ms: age_ms}, meta)
  end

  defp observed_depth(%{channel: nil}), do: 0

  defp observed_depth(state) do
    case state.client.declare_queue(
           state.channel,
           queue(state) <> ".quarantine",
           Client.queue_declare_opts()
         ) do
      {:ok, %{message_count: n}} when is_integer(n) -> n
      _ -> 0
    end
  end

  defp finish(state, decision, meta, envelope \\ nil) do
    extras = %{decision: decision, connector_id: state.connector_id}
    extras = if envelope, do: Map.put(extras, :gateway_id, envelope.gateway_id), else: extras
    Telemetry.emit([:settlement], %{}, extras)
    settle_classified(%{state | inflight: {:classified, decision, meta}})
  end

  defp settle_classified(%{inflight: {:classified, decision, meta}} = state) do
    case apply_settle(state, decision, meta) do
      :ok -> %{state | inflight: nil, phase: :idle}
      _ -> state
    end
  end

  defp settle_classified(state), do: state

  defp apply_settle(%{channel: nil}, _decision, _meta), do: {:error, :channel_closed}

  defp apply_settle(%{client: client, channel: ch}, :ack, %{delivery_tag: tag}),
    do: client.ack(ch, tag)

  defp apply_settle(%{client: client, channel: ch}, :reject, %{delivery_tag: tag}),
    do: client.reject(ch, tag, requeue: false)

  defp queue(state), do: state.config.queue_prefix <> "." <> state.connector_id

  defp resolve(%{connection: conn}) when not is_nil(conn), do: {:ok, conn}

  defp resolve(%{connection_server: server}) when not is_nil(server),
    do: Connection.get(server)

  defp resolve(_), do: {:error, :disconnected}

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: name]
end
