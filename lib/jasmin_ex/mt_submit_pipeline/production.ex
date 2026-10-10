defmodule JasminEx.MtSubmitPipeline.Production do
  @moduledoc false

  alias JasminEx.Billing.Admission
  alias JasminEx.Billing.Bill
  alias JasminEx.Billing.Fingerprint
  alias JasminEx.Billing.Settlement
  alias JasminEx.Dlr.Config, as: DlrConfig
  alias JasminEx.Dlr.Map, as: DlrMap
  alias JasminEx.Dlr.Request, as: DlrRequest
  alias JasminEx.Messaging.Envelope
  alias JasminEx.Messaging.RabbitMQ.Publisher
  alias JasminEx.Messaging.WorkQueue
  alias JasminEx.MtSubmitPipeline.ConcatReference
  alias JasminEx.MtSubmitPipeline.Segmentation
  alias JasminEx.MtSubmitPipeline.SegmentDispatch
  alias JasminEx.Routing
  alias JasminEx.Routing.Routable
  alias JasminEx.Routing.RouteTable
  alias JasminEx.Smpp.PDU.Coding
  alias JasminEx.Smpp.PDU.Tlv

  @allowed_keys MapSet.new([:uid, :to, :from, :content, :hex_content, :coding])
  @allowed_coding [0, 1, 2, 3, 8]
  @max_encoded_octets 254
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
         {:ok, payload} <- validate_payload(input) do
      {:ok, Map.merge(payload, %{uid: uid, to: to, from: from})}
    end
  end

  def validate(_input), do: {:error, :unknown_field}

  def validate_payload(input) when is_map(input) do
    payload = Map.take(input, [:content, :hex_content, :coding])

    with {:ok, content} <- resolve_content(payload),
         {:ok, coding} <- resolve_coding(payload),
         {:ok, encoded} <- encoded_short_message(payload, content, coding) do
      {:ok,
       %{
         content: content,
         coding: coding,
         encoded_short_message: encoded
       }}
    end
  end

  def validate_payload(_input), do: {:error, :unknown_field}

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

    with {:ok, message} <- prepare_multipart(message, opts),
         {:ok, bill} <-
           Bill.new(
             bill_id: bill_id,
             uid: message.uid,
             route_order: route.order,
             rate_minor: route.rate_minor,
             precharge_percent: route.precharge_percent,
             segment_count: segment_count(message)
           ),
         {:ok, admission} <-
           Admission.new(bill: bill, ttl_ms: Map.get(opts, :ttl_ms, @default_ttl_ms)) do
      admit_message(message, router, bill_id, admission)
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
    case Map.get(message, :multipart) do
      nil -> dispatch_legacy(message, router, queue, opts)
      multipart -> dispatch_multipart(message, router, queue, opts, multipart)
    end
  end

  defp dispatch_legacy(message, router, queue, opts) do
    case envelope(message, opts) do
      {:ok, envelope} ->
        dispatch_registered(message, router, queue, opts, envelope)

      {:error, reason} ->
        _ = compensate(router, message, :non_ok)
        {:error, reason}
    end
  end

  defp dispatch_multipart(message, router, queue, opts, multipart) do
    case child_envelopes(message, opts, multipart) do
      {:ok, envelopes} -> SegmentDispatch.run(router, queue, message.admission, envelopes)
      error -> error
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
      short_message: message.encoded_short_message,
      data_coding: message.coding
    }

    if request_receipt?(opts), do: Map.put(submit, :registered_delivery, 1), else: submit
  end

  defp prepare_multipart(%{encoded_short_message: encoded} = message, _opts)
       when byte_size(encoded) <= @max_encoded_octets,
       do: {:ok, message}

  defp prepare_multipart(message, opts) do
    options = [
      concat: Map.get(opts, :concat, :udh),
      max_segments: Map.get(opts, :max_segments, 5)
    ]

    with {:ok, _template} <- plan(message, options ++ [reference: 1]),
         :ok <- reject_multipart_dlr(opts),
         {:ok, reference} <- allocate_reference(opts),
         {:ok, plan} <- plan(message, options ++ [reference: reference]) do
      {:ok, Map.put(message, :multipart, %{plan: plan, reference: reference})}
    end
  end

  defp plan(message, options) do
    case Segmentation.plan(message.encoded_short_message, message.coding, options) do
      {:error, {:too_many_segments, _actual, _maximum}} -> {:error, :message_too_long}
      other -> other
    end
  end

  defp reject_multipart_dlr(%{dlr_request: %DlrRequest{enabled: true}}),
    do: {:error, :multipart_dlr_not_supported}

  defp reject_multipart_dlr(_opts), do: :ok

  defp allocate_reference(%{reference_allocator: allocator}) when is_function(allocator, 0) do
    case allocator.() do
      {:ok, reference} when reference in 1..255 -> {:ok, reference}
      _other -> {:error, :concat_reference_unavailable}
    end
  rescue
    _error -> {:error, :concat_reference_unavailable}
  catch
    :exit, _reason -> {:error, :concat_reference_unavailable}
    :throw, _value -> {:error, :concat_reference_unavailable}
  end

  defp allocate_reference(opts) do
    opts
    |> Map.get(:concat_reference, ConcatReference)
    |> ConcatReference.next()
    |> case do
      {:ok, reference} when reference in 1..255 -> {:ok, reference}
      _other -> {:error, :concat_reference_unavailable}
    end
  end

  defp admit_message(%{multipart: %{}} = message, _router, bill_id, admission),
    do: {:ok, Map.merge(message, %{bill_id: bill_id, admission: admission})}

  defp admit_message(message, router, bill_id, admission) do
    with {:ok, reservation} <- Routing.admit(router, admission) do
      {:ok, Map.merge(message, %{bill_id: bill_id, reservation: reservation})}
    end
  end

  defp segment_count(%{multipart: %{plan: %{count: count}}}), do: count
  defp segment_count(_message), do: 1

  defp child_envelopes(message, opts, %{plan: %{segments: segments}}) do
    {:ok, fingerprint} = Fingerprint.compute(message.admission.bill)
    now = DateTime.utc_now()
    ttl_ms = Map.get(opts, :ttl_ms, @default_ttl_ms)

    Enum.reduce_while(segments, {:ok, []}, fn segment, {:ok, envelopes} ->
      case child_envelope(message, opts, fingerprint, now, ttl_ms, segment) do
        {:ok, envelope} -> {:cont, {:ok, [envelope | envelopes]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, envelopes} -> {:ok, Enum.reverse(envelopes)}
      error -> error
    end
  end

  defp child_envelope(message, opts, fingerprint, now, ttl_ms, segment) do
    case segment_submit_sm(message, segment) do
      {:ok, submit_sm} ->
        Envelope.new(%{
          gateway_id: "#{message.bill_id}:#{segment.index}",
          connector_id: message.connector_id,
          attempt: 1,
          max_attempts: Map.get(opts, :max_attempts, @default_max_attempts),
          enqueued_at: DateTime.to_iso8601(now),
          expires_at: DateTime.to_iso8601(DateTime.add(now, ttl_ms, :millisecond)),
          submit_sm: submit_sm,
          segment: %{
            bill_id: message.bill_id,
            index: segment.index,
            count: segment.count,
            fingerprint_version: fingerprint.version,
            fingerprint_digest_base64: Base.encode64(fingerprint.digest)
          }
        })

      error ->
        error
    end
  end

  defp segment_submit_sm(message, segment) do
    submit = %{
      source_addr: message.from,
      destination_addr: message.to,
      short_message: segment.short_message,
      data_coding: message.coding,
      esm_class: segment.esm_class
    }

    case sar_optional_parameters(segment) do
      {:ok, <<>>} ->
        {:ok, submit}

      {:ok, optional_parameters} ->
        {:ok, Map.put(submit, :optional_parameters, optional_parameters)}

      error ->
        error
    end
  end

  defp sar_optional_parameters(%{sar_msg_ref_num: reference} = segment) do
    Tlv.encode(
      sar_msg_ref_num: reference,
      sar_total_segments: segment.sar_total_segments,
      sar_segment_seqnum: segment.sar_segment_seqnum
    )
  end

  defp sar_optional_parameters(_segment), do: {:ok, <<>>}

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

  defp encoded_short_message(input, content, coding) do
    wire_bytes(input, content, coding)
  end

  defp wire_bytes(input, content, coding) do
    if filled?(Map.get(input, :hex_content)) do
      case Coding.decode_short_message(coding, content) do
        {:ok, _decoded} -> {:ok, content}
        :error -> {:error, :invalid_content}
      end
    else
      case Coding.encode_short_message(coding, content) do
        {:ok, encoded} -> {:ok, encoded}
        :error -> {:error, :invalid_content}
      end
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
