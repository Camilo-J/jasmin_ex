defmodule JasminEx.Dlr.Lookup do
  @moduledoc false

  alias JasminEx.Dlr.Payload

  @spec plan(map(), keyword()) :: {:ok, map()} | {:retry, term()} | {:terminal, term()}
  def plan(event, inputs) when is_map(event) and is_list(inputs) do
    with :ok <- fresh(event, Keyword.get(inputs, :now_ms)),
         do: plan_kind(event, inputs)
  end

  defp plan_kind(%{kind: :submit_sm_resp} = event, inputs) do
    with {:ok, request} <- map_result(Keyword.get(inputs, :request), :submit),
         :ok <- match_submit(event, request) do
      submit_plan(event, request)
    end
  end

  defp plan_kind(%{kind: :deliver_sm} = event, inputs) do
    with {:ok, reverse} <- map_result(Keyword.get(inputs, :reverse), :reverse),
         :ok <- match_receipt(event, reverse),
         {:ok, request} <- map_result(Keyword.get(inputs, :request), :request),
         :ok <- match_receipt(event, request) do
      receipt_plan(event, request)
    end
  end

  defp plan_kind(_event, _inputs), do: {:terminal, :unknown_event}

  defp submit_plan(event, request) do
    success? = event.status == "ESME_ROK" and is_binary(event.raw_smsc_id)

    case {request.level, success?} do
      {1, _} ->
        {:ok,
         base_plan(event, acceptance_job(event, request), nil, {:request, request.gateway_id})}

      {2, true} ->
        {:ok, base_plan(event, nil, reverse(event, request), nil)}

      {2, false} ->
        {:terminal, :submit_failed_without_callback}

      {3, true} ->
        {:ok, base_plan(event, acceptance_job(event, request), reverse(event, request), nil)}

      {3, false} ->
        {:ok,
         base_plan(event, acceptance_job(event, request), nil, {:request, request.gateway_id})}

      _unsupported_level ->
        {:terminal, {:malformed_map, :invalid_state}}
    end
  end

  defp receipt_plan(_event, %{level: 1}), do: {:terminal, :receipt_not_requested}

  defp receipt_plan(event, request) do
    case Payload.classify(event.status) do
      :intermediate ->
        {:ok, base_plan(event, nil, nil, nil)}

      outcome ->
        status = if outcome == :delivered, do: "DELIVERED", else: "FAILED"
        payload = Payload.delivery(request, event, status)
        {:ok, base_plan(event, job(event, request, payload), nil, {:request, request.gateway_id})}
    end
  end

  defp base_plan(event, job, reverse, cleanup) do
    %{
      event_id: event.event_id,
      expires_at_ms: event.deadline_ms,
      job: job,
      reverse: reverse,
      cleanup: cleanup
    }
  end

  defp acceptance_job(event, request),
    do: job(event, request, Payload.acceptance(request, event))

  defp job(event, request, payload) do
    %{
      job_id: event.event_id,
      event_id: event.event_id,
      gateway_id: request.gateway_id,
      url: request.url,
      level: payload["level"],
      created_at_ms: event.observed_at_ms,
      deadline_ms: event.deadline_ms,
      payload: payload
    }
  end

  defp reverse(event, request) do
    %{
      connector_id: request.connector_id,
      correlation_group: request.correlation_group,
      raw_smsc_id: event.raw_smsc_id,
      gateway_id: request.gateway_id,
      expiry_s: request.expiry_s
    }
  end

  defp map_result({:ok, record}, _kind) when is_map(record), do: {:ok, record}
  defp map_result(:missing, :submit), do: {:terminal, :submit_map_missing}
  defp map_result(:missing, :reverse), do: {:retry, :reverse_map_missing}
  defp map_result(:missing, :request), do: {:retry, :request_map_missing}

  defp map_result({:error, {:malformed_map, _reason} = reason}, _kind),
    do: {:terminal, reason}

  defp map_result({:error, reason}, _kind), do: {:retry, {:store, reason}}
  defp map_result({:ambiguous, reason}, _kind), do: {:retry, {:store, reason}}
  defp map_result(_result, _kind), do: {:terminal, :invalid_map_result}

  defp match_submit(event, request) do
    if event.gateway_id == request.gateway_id and event.connector_id == request.connector_id,
      do: :ok,
      else: {:terminal, :connector_mismatch}
  end

  # Any connector of the submitting connector's correlation group may deliver
  # the receipt.
  defp match_receipt(event, record) do
    if event.correlation_group == record.correlation_group,
      do: :ok,
      else: {:terminal, :connector_mismatch}
  end

  defp fresh(_event, nil), do: :ok
  defp fresh(%{deadline_ms: deadline}, now) when deadline > now, do: :ok
  defp fresh(_event, _now), do: {:terminal, :expired}
end
