defmodule JasminEx.Messaging.SettlementJournal do
  @moduledoc "Tracks delivery settlement outcomes and redelivery decisions."

  defmodule Record do
    @moduledoc false

    @enforce_keys [:gateway_id, :attempt, :state]
    defstruct [:gateway_id, :attempt, :state, evidence: %{}]
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
