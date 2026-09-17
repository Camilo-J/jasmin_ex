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
        payload =
          %{
            "gateway_id" => record.gateway_id,
            "attempt" => record.attempt,
            "state" => Atom.to_string(record.state),
            "evidence" => evidence
          }
          |> maybe_put_known_response(record.known_response)

        {:ok, payload |> :json.encode() |> IO.iodata_to_binary()}

      {:error, _reason} ->
        {:error, :invalid_journal_record}
    end
  rescue
    _error -> {:error, :invalid_journal_record}
  end

  defp decode(payload) do
    with map when is_map(map) <- :json.decode(payload),
         %{
           "gateway_id" => gateway_id,
           "attempt" => attempt,
           "state" => state,
           "evidence" => evidence
         } <-
           map,
         true <-
           is_binary(gateway_id) and is_integer(attempt) and attempt > 0 and is_map(evidence),
         {:ok, state} <- known_state(state),
         {:ok, evidence} <- SettlementJournal.canonicalize_evidence(evidence),
         {:ok, known_response} <- decode_known_response(map) do
      {:ok,
       %Record{
         gateway_id: gateway_id,
         attempt: attempt,
         state: state,
         evidence: evidence,
         known_response: known_response
       }}
    else
      _ -> {:error, :invalid_journal_record}
    end
  rescue
    _error -> {:error, :invalid_journal_record}
  end

  defp maybe_put_known_response(payload, nil), do: payload

  defp maybe_put_known_response(payload, known) when is_map(known),
    do: Map.put(payload, "known_response", known)

  defp decode_known_response(map) when is_map(map) do
    case Map.get(map, "known_response") do
      nil -> {:ok, nil}
      known when is_map(known) -> {:ok, known}
      _other -> {:error, :invalid_journal_record}
    end
  end

  defp known_state(state),
    do:
      Map.fetch(%{"dispatching" => :dispatching, "not_sent" => :not_sent, "sent" => :sent}, state)
end
