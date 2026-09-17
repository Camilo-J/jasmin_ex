defmodule JasminEx.MtSubmitPipeline.Production do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Settlement
  alias JasminEx.Dlr.Config, as: DlrConfig
  alias JasminEx.Dlr.Map, as: DlrMap
  alias JasminEx.Dlr.Request, as: DlrRequest
  alias JasminEx.Messaging.Envelope
  alias JasminEx.Messaging.RabbitMQ.Publisher
  alias JasminEx.Messaging.WorkQueue
  alias JasminEx.Routing
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.RouteTable

  @allowed_keys MapSet.new([:uid, :to, :from, :content, :hex_content, :coding])
  @allowed_coding [0, 1, 2, 3, 8]
  @default_ttl_ms 60_000
  @default_max_attempts 3

  def stages(opts) when is_map(opts) do
    router = Map.fetch!(opts, :router)
    queue = Map.fetch!(opts, :queue)

    %{
      validate: &validate/1,
      route: &route(&1, router),
      bill: &admit(&1, router, opts),
      dispatch: &dispatch(&1, router, queue, opts)
    }
  end

  def validate(input) when is_map(input) do
    with :ok <- reject_unknown(input),
         {:ok, uid} <- require_binary(input, :uid, :missing_uid),
         {:ok, to} <- require_binary(input, :to, :missing_to),
         {:ok, from} <- require_binary(input, :from, :missing_from),
         {:ok, content} <- resolve_content(input),
         {:ok, coding} <- resolve_coding(input) do
      {:ok, %{uid: uid, to: to, from: from, content: content, coding: coding}}
    end
  end

  def validate(_input), do: {:error, :unknown_field}

  def route(message, router) do
    snapshot = Routing.snapshot(router)

    with {:ok, user} <- fetch_user(snapshot, message.uid),
         {:ok, group} <- fetch_group(snapshot, user.gid),
         {:ok, routable} <-
           Routable.new(
             user: user,
             group: group,
             source: message.from,
             destination: message.to,
             content: message.content,
             tags: []
           ),
         {:ok, route} <- fetch_route(snapshot, routable) do
      {:ok, Map.merge(message, %{route: route, connector_id: route.connector.id})}
    end
  end

  def admit(message, router, opts) do
    route = message.route
    bill_id = message_id(opts)

    with {:ok, bill} <-
           Bill.new(
             bill_id: bill_id,
             uid: message.uid,
             route_order: route.order,
             rate_minor: route.rate_minor,
             precharge_percent: route.precharge_percent
           ),
         {:ok, admission} <-
           Admission.new(bill: bill, ttl_ms: Map.get(opts, :ttl_ms, @default_ttl_ms)),
         {:ok, reservation} <- Routing.admit(router, admission) do
      {:ok, Map.merge(message, %{bill_id: bill_id, reservation: reservation})}
    end
  end

  def compensate(router, message, outcome) do
    {:ok, settlement} =
      Settlement.new(
        bill_id: message.bill_id,
        fingerprint: message.reservation.fingerprint,
        outcome: outcome
      )

    Routing.settle(router, settlement)
  end

  def envelope(message, opts \\ %{}) do
    ttl_ms = Map.get(opts, :ttl_ms, @default_ttl_ms)
    now = DateTime.utc_now()

    Envelope.new(%{
      gateway_id: message.bill_id,
      connector_id: message.connector_id,
      attempt: 1,
      max_attempts: Map.get(opts, :max_attempts, @default_max_attempts),
      enqueued_at: DateTime.to_iso8601(now),
      expires_at: DateTime.to_iso8601(DateTime.add(now, ttl_ms, :millisecond)),
      submit_sm: submit_sm(message, opts)
    })
  end

  def dispatch(message, router, queue, opts) do
    case envelope(message, opts) do
      {:ok, envelope} ->
        dispatch_registered(message, router, queue, opts, envelope)

      {:error, reason} ->
        _ = compensate(router, message, :non_ok)
        {:error, reason}
    end
  end

  defp dispatch_registered(message, router, queue, opts, envelope) do
    case register_dlr(message, opts) do
      :ok ->
        after_enqueue(WorkQueue.enqueue(queue, envelope), router, message, opts)

      {:error, reason} ->
        _ = compensate(router, message, :non_ok)
        {:error, reason}

      {:ambiguous, reason} ->
        _ = compensate(router, message, :non_ok)
        {:error, {:ambiguous, reason}}
    end
  end

  defp after_enqueue(:ok, _router, message, _opts), do: {:ok, message.bill_id}

  defp after_enqueue(result, router, message, opts) do
    case enqueue_action(result) do
      :settle_non_ok ->
        _ = compensate(router, message, :non_ok)
        _ = delete_dlr(message, opts)
        {:error, :non_ok}

      :leave_open ->
        {:error, result}
    end
  end

  defp enqueue_action({:ambiguous, _} = result), do: Publisher.reservation_action(result)
  defp enqueue_action({:error, :non_ok} = result), do: Publisher.reservation_action(result)
  defp enqueue_action({:error, _reason}), do: :settle_non_ok

  defp submit_sm(message, opts) do
    submit = %{
      source_addr: message.from,
      destination_addr: message.to,
      short_message: message.content,
      data_coding: message.coding
    }

    if request_receipt?(opts), do: Map.put(submit, :registered_delivery, 1), else: submit
  end

  defp request_receipt?(%{dlr_request: %DlrRequest{request_receipt: true}}), do: true
  defp request_receipt?(_opts), do: false

  defp register_dlr(message, opts) do
    case Map.get(opts, :dlr_request) do
      %DlrRequest{register_callback: true} = request ->
        persist_dlr(message, request, opts)

      _other ->
        :ok
    end
  end

  defp persist_dlr(message, request, opts) do
    case Map.get(opts, :dlr_store) do
      nil ->
        {:error, :dlr_unavailable}

      store ->
        DlrMap.register(
          store,
          %{
            gateway_id: message.bill_id,
            connector_id: message.connector_id,
            url: request.url,
            level: request.level,
            method: request.method,
            expiry_s: dlr_expiry_s(opts, message.connector_id)
          },
          Map.get(opts, :dlr_clock, {__MODULE__, :system})
        )
    end
  end

  defp delete_dlr(message, opts) do
    case {Map.get(opts, :dlr_request), Map.get(opts, :dlr_store)} do
      {%DlrRequest{register_callback: true}, store} when not is_nil(store) ->
        DlrMap.delete_request(store, message.bill_id)

      _other ->
        :ok
    end
  end

  defp dlr_expiry_s(opts, connector_id) do
    case Map.get(opts, :dlr_expiry_fun) do
      fun when is_function(fun, 1) ->
        fun.(connector_id)

      _missing ->
        DlrConfig.connector_expiry(Map.get(opts, :dlr_config) || DlrConfig.new())
    end
  end

  def now_ms(:system), do: System.system_time(:millisecond)

  defp reject_unknown(input) do
    keys = MapSet.new(Map.keys(input))

    if MapSet.subset?(keys, @allowed_keys) do
      :ok
    else
      {:error, :unknown_field}
    end
  end

  defp require_binary(input, key, reason) do
    case Map.get(input, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, reason}
    end
  end

  defp resolve_content(input) do
    content = Map.get(input, :content)
    hex = Map.get(input, :hex_content)

    cond do
      filled?(content) and filled?(hex) -> {:error, :ambiguous_content}
      filled?(content) -> {:ok, content}
      filled?(hex) -> decode_hex(hex)
      true -> {:error, :missing_content}
    end
  end

  defp filled?(value) when is_binary(value) and value != "", do: true
  defp filled?(_value), do: false

  defp decode_hex(hex) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, binary} -> {:ok, binary}
      :error -> {:error, :malformed_hex}
    end
  end

  defp resolve_coding(input) do
    case Map.get(input, :coding, 0) do
      coding when coding in @allowed_coding -> {:ok, coding}
      _invalid -> {:error, :invalid_coding}
    end
  end

  defp fetch_user(%{users: users}, uid) do
    case Map.fetch(users, uid) do
      {:ok, user} -> {:ok, user}
      :error -> {:error, :unknown_user}
    end
  end

  defp fetch_group(%{groups: groups}, gid) do
    case Map.fetch(groups, gid) do
      {:ok, group} -> {:ok, group}
      :error -> {:error, :unknown_group}
    end
  end

  defp fetch_route(snapshot, routable) do
    case RouteTable.winning_route(snapshot.routes, routable) do
      nil -> {:error, :no_route}
      route -> {:ok, route}
    end
  end

  defp message_id(%{id_fun: fun}) when is_function(fun, 0), do: fun.()

  defp message_id(_opts) do
    Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end
end
