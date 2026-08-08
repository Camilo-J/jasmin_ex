defmodule JasminEx.Messaging.StateStoreJournal do
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
    with {:ok, payload} <- StateStore.fetch(store, key(gateway_id, attempt)),
         {:ok, record} <- decode(payload) do
      {:ok, record}
    end
  end

  defp encode(%Record{} = record) do
    payload = %{
      "gateway_id" => record.gateway_id,
      "attempt" => record.attempt,
      "state" => Atom.to_string(record.state),
      "evidence" => stringify(record.evidence)
    }

    {:ok, payload |> :json.encode() |> IO.iodata_to_binary()}
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
         {:ok, state} <- known_state(state) do
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

  defp stringify(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value), do: value

  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp stringify(value) when is_list(value), do: Enum.map(value, &stringify/1)

  defp stringify(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} -> {to_string(key), stringify(nested_value)} end)
  end
end
