defmodule JasminEx.Messaging.StateStoreJournal do
  @moduledoc "Persists settlement journal records in a state store."

  alias JasminEx.Messaging.SettlementJournal
  alias JasminEx.Messaging.SettlementJournal.Record
  alias JasminEx.StateStore

  def key(gateway_id, attempt)
      when is_binary(gateway_id) and is_integer(attempt) and attempt > 0 do
    <<byte_size(gateway_id)::32, gateway_id::binary, attempt::64>>
  end

  def write(store, %Record{} = record, ttl_ms) do
    with {:ok, payload} <- encode(record) do
      StateStore.put(store, key(record.gateway_id, record.attempt), payload, ttl_ms)
    end
  end

  def read(store, gateway_id, attempt) do
    with {:ok, payload} <- StateStore.fetch(store, key(gateway_id, attempt)) do
      decode(payload)
    end
  end

  defp encode(%Record{} = record) do
    case SettlementJournal.canonicalize_evidence(record.evidence) do
      {:ok, evidence} ->
        payload = %{
          "gateway_id" => record.gateway_id,
          "attempt" => record.attempt,
          "state" => Atom.to_string(record.state),
          "evidence" => evidence
        }

        {:ok, payload |> :json.encode() |> IO.iodata_to_binary()}

      {:error, _reason} ->
        {:error, :invalid_journal_record}
    end
  rescue
    _error -> {:error, :invalid_journal_record}
  end

  defp decode(payload) do
    with %{
           "gateway_id" => gateway_id,
           "attempt" => attempt,
           "state" => state,
           "evidence" => evidence
         } <- :json.decode(payload),
         true <-
           is_binary(gateway_id) and is_integer(attempt) and attempt > 0 and is_map(evidence),
         {:ok, state} <- known_state(state),
         {:ok, evidence} <- SettlementJournal.canonicalize_evidence(evidence) do
      {:ok, %Record{gateway_id: gateway_id, attempt: attempt, state: state, evidence: evidence}}
    else
      _ -> {:error, :invalid_journal_record}
    end
  rescue
    _error -> {:error, :invalid_journal_record}
  end

  defp known_state(state),
    do:
      Map.fetch(%{"dispatching" => :dispatching, "not_sent" => :not_sent, "sent" => :sent}, state)
end
