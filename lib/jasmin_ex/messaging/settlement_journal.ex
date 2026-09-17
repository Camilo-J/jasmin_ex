defmodule JasminEx.Messaging.SettlementJournal do
  @moduledoc "Tracks delivery settlement outcomes and redelivery decisions."

  defmodule Record do
    @moduledoc false

    @enforce_keys [:gateway_id, :attempt, :state]
    defstruct [:gateway_id, :attempt, :state, evidence: %{}, known_response: nil]
  end

  def dispatching(gateway_id, attempt)
      when is_binary(gateway_id) and is_integer(attempt) and attempt > 0 do
    %Record{gateway_id: gateway_id, attempt: attempt, state: :dispatching}
  end

  def record_outcome(%Record{state: :dispatching} = record, {state, evidence})
      when state in [:not_sent, :sent] and is_map(evidence) do
    case canonicalize_evidence(evidence) do
      {:ok, evidence} -> {:ok, %{record | state: state, evidence: evidence}}
      {:error, reason} -> {:error, reason}
    end
  end

  def record_outcome(%Record{state: state, evidence: evidence} = record, {state, incoming})
      when is_map(incoming) do
    case canonicalize_evidence(incoming) do
      {:ok, ^evidence} -> {:ok, record}
      {:ok, _other} -> {:error, :outcome_already_recorded}
      {:error, reason} -> {:error, reason}
    end
  end

  def record_outcome(%Record{}, {_state, _evidence}), do: {:error, :outcome_already_recorded}
  def redelivery_directive(%Record{state: :not_sent, evidence: evidence}), do: {:retry, evidence}

  def redelivery_directive(%Record{state: :dispatching}),
    do: {:quarantine, %{"reason" => "unresolved_dispatch"}}

  def redelivery_directive(%Record{}),
    do: {:quarantine, %{"reason" => "outcome_already_recorded"}}

  def redelivery_directive(:missing),
    do: {:quarantine, %{"reason" => "unresolved_dispatch"}}

  @default_handoff_ms 120_000

  def record_known_response(%Record{state: :dispatching} = record, known) when is_map(known) do
    with {:ok, known_response} <- build_known_response(known) do
      {:ok, %{record | state: :sent, known_response: known_response}}
    end
  end

  def record_known_response(%Record{}, _known), do: {:error, :outcome_already_recorded}

  def known_response(%Record{known_response: known}) when is_map(known), do: {:ok, known}
  def known_response(%Record{}), do: :none

  def event_id(connector_id, gateway_id, attempt)
      when is_binary(connector_id) and is_binary(gateway_id) and is_integer(attempt) do
    "#{connector_id}:#{gateway_id}:#{attempt}:submit_sm_resp"
  end

  def outcome_retention_ms(expiry_s, handoff_ms \\ @default_handoff_ms)
      when is_integer(expiry_s) and expiry_s > 0 and is_integer(handoff_ms) and handoff_ms > 0 do
    expiry_s * 1000 + handoff_ms
  end

  defp build_known_response(known) do
    with {:ok, gateway_id} <- required_binary(known, :gateway_id),
         {:ok, connector_id} <- required_binary(known, :connector_id),
         {:ok, attempt} <- required_attempt(known),
         {:ok, observed_at_ms} <- required_timestamp(known),
         {:ok, status} <- known_status(field(known, :status)),
         {:ok, smsc_id} <- optional_smsc_id(field(known, :smsc_id)) do
      {:ok,
       %{
         "version" => 1,
         "gateway_id" => gateway_id,
         "connector_id" => connector_id,
         "attempt" => attempt,
         "smsc_id" => smsc_id,
         "status" => status,
         "observed_at_ms" => observed_at_ms,
         "event_id" => event_id(connector_id, gateway_id, attempt)
       }}
    else
      _ -> {:error, :invalid_known_response}
    end
  end

  defp field(known, key), do: known[key] || known[Atom.to_string(key)]

  defp required_binary(known, key) do
    case field(known, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> :error
    end
  end

  defp required_attempt(known) do
    case field(known, :attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> {:ok, attempt}
      _ -> :error
    end
  end

  defp required_timestamp(known) do
    case field(known, :observed_at_ms) do
      ms when is_integer(ms) -> {:ok, ms}
      _ -> :error
    end
  end

  defp known_status(status) when is_atom(status), do: {:ok, Atom.to_string(status)}
  defp known_status(status) when is_binary(status) and status != "", do: {:ok, status}
  defp known_status(_status), do: :error

  defp optional_smsc_id(nil), do: {:ok, nil}
  defp optional_smsc_id(id) when is_binary(id) and id != "", do: {:ok, id}
  defp optional_smsc_id(_id), do: :error

  @doc false
  def canonicalize_evidence(evidence) when is_map(evidence) do
    {:ok, canonicalize_value(evidence)}
  rescue
    _error -> {:error, :invalid_evidence}
  end

  def canonicalize_evidence(_evidence), do: {:error, :invalid_evidence}

  defp canonicalize_value(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: value

  defp canonicalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp canonicalize_value(value) when is_list(value), do: Enum.map(value, &canonicalize_value/1)

  defp canonicalize_value(value) when is_map(value) do
    Map.new(value, fn
      {key, nested} when is_atom(key) or is_binary(key) ->
        {to_string(key), canonicalize_value(nested)}

      {_key, _nested} ->
        raise ArgumentError, "evidence keys must be atoms or binaries"
    end)
  end

  defp canonicalize_value(_value), do: raise(ArgumentError, "unsupported evidence value")
end
