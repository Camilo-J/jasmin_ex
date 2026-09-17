defmodule JasminEx.Dlr.Event do
  @moduledoc false

  alias JasminEx.Dlr.Map, as: DlrMap
  alias JasminEx.Dlr.Receipt

  @spec submit_event_id(binary(), binary(), pos_integer()) :: binary()
  def submit_event_id(connector_id, gateway_id, attempt)
      when is_binary(connector_id) and is_binary(gateway_id) and is_integer(attempt) do
    "#{connector_id}:#{gateway_id}:#{attempt}:submit_sm_resp"
  end

  @spec receipt_event_id(binary(), Receipt.t()) :: binary()
  def receipt_event_id(connector_id, %Receipt{} = receipt) when is_binary(connector_id) do
    material =
      Enum.join(
        [
          connector_id,
          receipt.id || "",
          receipt.stat || "",
          receipt.sdate || "",
          receipt.ddate || "",
          receipt.sub || "",
          receipt.dlvrd || "",
          receipt.err || "",
          receipt.text || ""
        ],
        "\0"
      )

    Base.encode16(:crypto.hash(:sha256, material), case: :lower)
  end

  @spec encode(map()) :: {:ok, binary()} | {:error, atom()}
  def encode(%{kind: :submit_sm_resp} = event), do: encode_submit(event)
  def encode(%{kind: :deliver_sm} = event), do: encode_receipt(event)
  def encode(_event), do: {:error, :unknown_kind}

  @spec decode(binary()) :: {:ok, map()} | {:error, atom()}
  def decode(binary) when is_binary(binary) do
    case :json.decode(binary) do
      %{"version" => 1, "kind" => kind} = map -> decode_kind(kind, map)
      %{"version" => _other} -> {:error, :unsupported_version}
      _other -> {:error, :invalid_event}
    end
  rescue
    _error -> {:error, :invalid_event}
  end

  defp encode_submit(event) do
    with {:ok, gateway_id} <- required(event, :gateway_id),
         {:ok, connector_id} <- required(event, :connector_id),
         {:ok, attempt} <- required_attempt(event),
         {:ok, status} <- required(event, :status),
         {:ok, observed_at_ms} <- required_int(event, :observed_at_ms),
         {:ok, deadline_ms} <- required_int(event, :deadline_ms) do
      payload =
        %{
          "version" => 1,
          "kind" => "submit_sm_resp",
          "event_id" => submit_event_id(connector_id, gateway_id, attempt),
          "gateway_id" => gateway_id,
          "connector_id" => connector_id,
          "attempt" => attempt,
          "status" => status,
          "observed_at_ms" => observed_at_ms,
          "deadline_ms" => deadline_ms
        }
        |> maybe_put("raw_smsc_id", Map.get(event, :raw_smsc_id))

      {:ok, json(payload)}
    end
  end

  defp encode_receipt(%{receipt: %Receipt{} = receipt} = event) do
    with {:ok, connector_id} <- required(event, :connector_id),
         {:ok, observed_at_ms} <- required_int(event, :observed_at_ms),
         {:ok, deadline_ms} <- required_int(event, :deadline_ms),
         {:ok, raw_smsc_id} <- required_receipt_id(receipt),
         {:ok, normalized} <- DlrMap.normalize_smsc_id(raw_smsc_id) do
      {:ok,
       json(%{
         "version" => 1,
         "kind" => "deliver_sm",
         "event_id" => receipt_event_id(connector_id, receipt),
         "connector_id" => connector_id,
         "raw_smsc_id" => raw_smsc_id,
         "normalized_smsc_id" => normalized,
         "status" => receipt.stat,
         "sub" => receipt.sub,
         "dlvrd" => receipt.dlvrd,
         "subdate" => receipt.sdate,
         "donedate" => receipt.ddate,
         "err" => receipt.err,
         "text" => receipt.text,
         "observed_at_ms" => observed_at_ms,
         "deadline_ms" => deadline_ms
       })}
    end
  end

  defp encode_receipt(_event), do: {:error, :invalid_event}

  defp decode_kind("submit_sm_resp", map) do
    with {:ok, event_id} <- required(map, "event_id"),
         {:ok, gateway_id} <- required(map, "gateway_id"),
         {:ok, connector_id} <- required(map, "connector_id"),
         {:ok, attempt} <- required_attempt(map),
         {:ok, status} <- required(map, "status"),
         {:ok, observed_at_ms} <- required_int(map, "observed_at_ms"),
         {:ok, deadline_ms} <- required_int(map, "deadline_ms") do
      {:ok,
       %{
         kind: :submit_sm_resp,
         event_id: event_id,
         gateway_id: gateway_id,
         connector_id: connector_id,
         attempt: attempt,
         status: status,
         raw_smsc_id: map["raw_smsc_id"],
         observed_at_ms: observed_at_ms,
         deadline_ms: deadline_ms
       }}
    end
  end

  defp decode_kind("deliver_sm", map) do
    with {:ok, event_id} <- required(map, "event_id"),
         {:ok, connector_id} <- required(map, "connector_id"),
         {:ok, raw_smsc_id} <- required(map, "raw_smsc_id"),
         {:ok, normalized} <- required(map, "normalized_smsc_id"),
         {:ok, status} <- required(map, "status"),
         {:ok, sub} <- required(map, "sub"),
         {:ok, dlvrd} <- required(map, "dlvrd"),
         {:ok, subdate} <- required(map, "subdate"),
         {:ok, donedate} <- required(map, "donedate"),
         {:ok, err} <- required(map, "err"),
         {:ok, text} <- optional_text(map),
         {:ok, observed_at_ms} <- required_int(map, "observed_at_ms"),
         {:ok, deadline_ms} <- required_int(map, "deadline_ms") do
      {:ok,
       %{
         kind: :deliver_sm,
         event_id: event_id,
         connector_id: connector_id,
         raw_smsc_id: raw_smsc_id,
         normalized_smsc_id: normalized,
         status: status,
         sub: sub,
         dlvrd: dlvrd,
         subdate: subdate,
         donedate: donedate,
         err: err,
         text: text,
         observed_at_ms: observed_at_ms,
         deadline_ms: deadline_ms
       }}
    end
  end

  defp decode_kind(_kind, _map), do: {:error, :unknown_kind}

  defp required(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, :invalid_event}
    end
  end

  defp required_receipt_id(%Receipt{id: id}) when is_binary(id) and id != "", do: {:ok, id}
  defp required_receipt_id(_receipt), do: {:error, :invalid_event}

  defp required_attempt(map) do
    case Map.get(map, :attempt) || Map.get(map, "attempt") do
      attempt when is_integer(attempt) and attempt > 0 -> {:ok, attempt}
      _other -> {:error, :invalid_event}
    end
  end

  defp required_int(map, key) do
    case Map.get(map, key) do
      value when is_integer(value) -> {:ok, value}
      _other -> {:error, :invalid_event}
    end
  end

  defp optional_text(%{"text" => text}) when is_binary(text), do: {:ok, text}
  defp optional_text(_map), do: {:ok, ""}

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp json(map), do: map |> :json.encode() |> IO.iodata_to_binary()
end
