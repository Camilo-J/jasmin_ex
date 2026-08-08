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
      when state in [:not_sent, :sent] and is_map(evidence),
      do: {:ok, %{record | state: state, evidence: evidence}}

  def record_outcome(%Record{state: state, evidence: evidence} = record, {state, evidence}),
    do: {:ok, record}

  def record_outcome(%Record{}, {_state, _evidence}), do: {:error, :outcome_already_recorded}
  def redelivery_directive(%Record{state: :not_sent, evidence: evidence}), do: {:retry, evidence}

  def redelivery_directive(%Record{state: :dispatching}),
    do: {:quarantine, %{reason: :unresolved_dispatch}}

  def redelivery_directive(%Record{}), do: {:quarantine, %{reason: :outcome_already_recorded}}
  def redelivery_directive(:missing), do: {:quarantine, %{reason: :unresolved_dispatch}}
end
