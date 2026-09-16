defmodule JasminEx.Dlr.Map do
  @moduledoc false

  alias JasminEx.StateStore

  @request_kind "request"
  @reverse_kind "reverse"
  @source "httpapi"
  @default_expiry_s 86_400

  @spec request_key(binary()) :: binary()
  def request_key(gateway_id) when is_binary(gateway_id) do
    <<"DLR", 1, "req", byte_size(gateway_id)::32, gateway_id::binary>>
  end

  @spec reverse_key(binary(), binary()) :: binary()
  def reverse_key(connector_id, normalized)
      when is_binary(connector_id) and is_binary(normalized) do
    <<"DLR", 1, "rev", byte_size(connector_id)::32, connector_id::binary,
      byte_size(normalized)::32, normalized::binary>>
  end

  @spec normalize_smsc_id(binary()) :: {:ok, binary()} | {:error, :invalid_smsc_id}
  def normalize_smsc_id(raw) when is_binary(raw) and raw != "" do
    {:ok, raw |> ascii_upcase() |> strip_leading_zeros()}
  end

  def normalize_smsc_id(_raw), do: {:error, :invalid_smsc_id}

  @spec register(StateStore.store(), map(), {module(), term()}) ::
          :ok | {:error | :ambiguous, term()}
  def register(store, attrs, clock) when is_map(attrs) do
    with {:ok, record} <- build_request(attrs, now_ms(clock)),
         {:ok, payload} <- encode(record) do
      StateStore.put(
        store,
        request_key(record.gateway_id),
        payload,
        ttl_ms(record, now_ms(clock))
      )
    end
  end

  @spec fetch_request(StateStore.store(), binary(), {module(), term()}) ::
          {:ok, map()} | :missing | {:error, term()}
  def fetch_request(store, gateway_id, clock) when is_binary(gateway_id) do
    fetch_typed(store, request_key(gateway_id), @request_kind, clock)
  end

  @spec delete_request(StateStore.store(), binary()) ::
          :deleted | :missing | {:error | :ambiguous, term()}
  def delete_request(store, gateway_id) when is_binary(gateway_id) do
    StateStore.delete(store, request_key(gateway_id))
  end

  @spec put_reverse(StateStore.store(), map(), {module(), term()}) ::
          :ok | {:error | :ambiguous, term()}
  def put_reverse(store, attrs, clock) when is_map(attrs) do
    with {:ok, record} <- build_reverse(attrs, now_ms(clock)) do
      write_reverse(store, record, clock)
    end
  end

  @spec fetch_reverse(StateStore.store(), binary(), binary(), {module(), term()}) ::
          {:ok, map()} | :missing | {:error, term()}
  def fetch_reverse(store, connector_id, raw_smsc_id, clock)
      when is_binary(connector_id) and is_binary(raw_smsc_id) do
    with {:ok, normalized} <- normalize_smsc_id(raw_smsc_id) do
      fetch_typed(store, reverse_key(connector_id, normalized), @reverse_kind, clock)
    end
  end

  defp write_reverse(store, record, clock) do
    key = reverse_key(record.connector_id, record.normalized_smsc_id)

    case fetch_typed(store, key, @reverse_kind, clock) do
      :missing -> persist_reverse(store, key, record, clock)
      {:ok, existing} -> compare_reverse(existing, record)
      {:error, _reason} = error -> error
    end
  end

  defp persist_reverse(store, key, record, clock) do
    with {:ok, payload} <- encode(record) do
      case StateStore.put(store, key, payload, ttl_ms(record, now_ms(clock))) do
        :ok ->
          :ok

        {:ambiguous, reason} ->
          resolve_ambiguous(store, key, record, clock, reason)

        other ->
          other
      end
    end
  end

  defp resolve_ambiguous(store, key, record, clock, reason) do
    case fetch_typed(store, key, @reverse_kind, clock) do
      {:ok, existing} -> compare_reverse(existing, record)
      :missing -> {:ambiguous, reason}
      {:error, _reason} = error -> error
    end
  end

  defp compare_reverse(existing, record) do
    if existing.gateway_id == record.gateway_id and existing.source == record.source do
      :ok
    else
      {:error, :reverse_collision}
    end
  end

  defp fetch_typed(store, key, kind, clock) do
    case StateStore.fetch(store, key) do
      :missing -> :missing
      {:error, _reason} = error -> error
      {:ok, payload} -> decode_fresh(payload, kind, now_ms(clock))
    end
  end

  defp decode_fresh(payload, kind, now_ms) do
    case decode(payload, kind) do
      {:ok, record} ->
        if record.expires_at_ms <= now_ms, do: :missing, else: {:ok, record}

      {:error, _reason} = error ->
        error
    end
  end

  defp build_request(attrs, now_ms) do
    with {:ok, gateway_id} <- required_binary(attrs, :gateway_id),
         {:ok, connector_id} <- required_binary(attrs, :connector_id),
         {:ok, url} <- required_binary(attrs, :url),
         {:ok, level} <- required_level(attrs),
         {:ok, method} <- required_method(attrs),
         {:ok, expiry_s} <- expiry_s(attrs) do
      {:ok,
       %{
         version: 1,
         kind: @request_kind,
         gateway_id: gateway_id,
         source: @source,
         connector_id: connector_id,
         url: url,
         level: level,
         method: method,
         expiry_s: expiry_s,
         created_at_ms: now_ms,
         expires_at_ms: now_ms + expiry_s * 1000
       }}
    end
  end

  defp build_reverse(attrs, now_ms) do
    with {:ok, connector_id} <- required_binary(attrs, :connector_id),
         {:ok, raw_smsc_id} <- required_binary(attrs, :raw_smsc_id),
         {:ok, gateway_id} <- required_binary(attrs, :gateway_id),
         {:ok, normalized} <- normalize_smsc_id(raw_smsc_id),
         {:ok, expiry_s} <- expiry_s(attrs) do
      {:ok,
       %{
         version: 1,
         kind: @reverse_kind,
         connector_id: connector_id,
         raw_smsc_id: raw_smsc_id,
         normalized_smsc_id: normalized,
         gateway_id: gateway_id,
         source: @source,
         created_at_ms: now_ms,
         expires_at_ms: now_ms + expiry_s * 1000
       }}
    end
  end

  defp encode(record) do
    payload =
      record
      |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
      |> :json.encode()
      |> IO.iodata_to_binary()

    {:ok, payload}
  rescue
    _error -> {:error, {:malformed_map, :invalid_state}}
  end

  defp decode(payload, expected_kind) do
    map = :json.decode(payload)

    with :ok <- version(map),
         :ok <- kind(map, expected_kind),
         :ok <- source(map) do
      record_from(map, expected_kind)
    end
  rescue
    _error -> {:error, {:malformed_map, :invalid_state}}
  end

  defp version(%{"version" => 1}), do: :ok
  defp version(%{"version" => _version}), do: {:error, {:malformed_map, :unsupported_version}}
  defp version(_map), do: {:error, {:malformed_map, :invalid_state}}

  defp kind(%{"kind" => kind}, kind), do: :ok
  defp kind(_map, _kind), do: {:error, {:malformed_map, :invalid_state}}

  defp source(%{"source" => @source}), do: :ok
  defp source(%{"source" => _source}), do: {:error, {:malformed_map, :invalid_source}}
  defp source(_map), do: {:error, {:malformed_map, :invalid_source}}

  defp record_from(map, @request_kind) do
    with gateway_id when is_binary(gateway_id) <- map["gateway_id"],
         connector_id when is_binary(connector_id) <- map["connector_id"],
         url when is_binary(url) <- map["url"],
         level when level in [1, 2, 3] <- map["level"],
         method when method in ["GET", "POST"] <- map["method"],
         expiry_s when is_integer(expiry_s) and expiry_s > 0 <- map["expiry_s"],
         created_at_ms when is_integer(created_at_ms) <- map["created_at_ms"],
         expires_at_ms when is_integer(expires_at_ms) <- map["expires_at_ms"] do
      {:ok,
       %{
         gateway_id: gateway_id,
         source: @source,
         connector_id: connector_id,
         url: url,
         level: level,
         method: method,
         expiry_s: expiry_s,
         created_at_ms: created_at_ms,
         expires_at_ms: expires_at_ms
       }}
    else
      _ -> {:error, {:malformed_map, :invalid_state}}
    end
  end

  defp record_from(map, @reverse_kind) do
    with connector_id when is_binary(connector_id) <- map["connector_id"],
         raw_smsc_id when is_binary(raw_smsc_id) <- map["raw_smsc_id"],
         normalized when is_binary(normalized) <- map["normalized_smsc_id"],
         gateway_id when is_binary(gateway_id) <- map["gateway_id"],
         created_at_ms when is_integer(created_at_ms) <- map["created_at_ms"],
         expires_at_ms when is_integer(expires_at_ms) <- map["expires_at_ms"] do
      {:ok,
       %{
         connector_id: connector_id,
         raw_smsc_id: raw_smsc_id,
         normalized_smsc_id: normalized,
         gateway_id: gateway_id,
         source: @source,
         created_at_ms: created_at_ms,
         expires_at_ms: expires_at_ms
       }}
    else
      _ -> {:error, {:malformed_map, :invalid_state}}
    end
  end

  defp required_binary(attrs, :raw_smsc_id) do
    case Map.get(attrs, :raw_smsc_id) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, :invalid_smsc_id}
    end
  end

  defp required_binary(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, {:malformed_map, :invalid_state}}
    end
  end

  defp required_level(attrs) do
    case Map.get(attrs, :level) do
      level when level in [1, 2, 3] -> {:ok, level}
      _ -> {:error, {:malformed_map, :invalid_state}}
    end
  end

  defp required_method(attrs) do
    case Map.get(attrs, :method) do
      method when method in ["GET", "POST"] -> {:ok, method}
      _ -> {:error, {:malformed_map, :invalid_state}}
    end
  end

  defp expiry_s(attrs) do
    case Map.get(attrs, :expiry_s, @default_expiry_s) do
      expiry when is_integer(expiry) and expiry > 0 -> {:ok, expiry}
      _ -> {:error, {:malformed_map, :invalid_state}}
    end
  end

  defp ttl_ms(record, now_ms) do
    remaining = record.expires_at_ms - now_ms
    if remaining > 0, do: remaining, else: 1
  end

  defp now_ms({module, context}), do: module.now_ms(context)

  defp ascii_upcase(<<byte, rest::binary>>) when byte in ?a..?z do
    <<byte - 32, ascii_upcase(rest)::binary>>
  end

  defp ascii_upcase(<<byte, rest::binary>>), do: <<byte, ascii_upcase(rest)::binary>>
  defp ascii_upcase(<<>>), do: <<>>

  defp strip_leading_zeros(<<"0", rest::binary>>), do: strip_leading_zeros(rest)
  defp strip_leading_zeros(rest), do: rest
end
