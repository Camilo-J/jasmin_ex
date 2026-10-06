defmodule JasminEx.Messaging.Envelope do
  @moduledoc """
  Represents and serializes a queued messaging request.

  Encode writes integer version 2 when segment metadata is absent and omits
  the `segment` field, keeping those bytes identical to the previous v2 wire.
  Present, valid segment metadata encodes as version 3. Wire `submit_sm` uses
  `short_message_base64` and, when non-empty, `optional_parameters_base64`
  (standard padded canonical Base64). In-memory `submit_sm.short_message`
  stays a binary; `submit_sm.optional_parameters` is present only for
  non-empty bytes. One-octet `esm_class` (0–255, including UDHI `0x40`) is
  copied from atom and string keys; absent, v2 JSON-null, and `0` stay off
  in-memory `submit_sm` and ordinary v2 wire. Version 1 JSON is decoded as
  original text bytes and is never Base64-decoded or transcoded. Envelope
  transport does not validate SAR, UDHI, or other PDU semantics.

  Segment `bill_id` is the parent Router/HTTP identity. `gateway_id` is the
  child journal key; callers must allocate a unique child id. Segment metadata
  is unused until retry transport lands. A segment index is never an attempt.
  """

  @v1 1
  @v2 2
  @v3 3
  @fields [
    :gateway_id,
    :connector_id,
    :attempt,
    :max_attempts,
    :enqueued_at,
    :expires_at,
    :submit_sm
  ]
  @segment_fields [
    :bill_id,
    :index,
    :count,
    :fingerprint_version,
    :fingerprint_digest_base64
  ]
  @segment_names Enum.map(@segment_fields, &Atom.to_string/1)
  @known_keys @fields ++ [:segment]
  @enforce_keys @fields
  defstruct @fields ++ [segment: nil]
  @allowed_data_coding [0, 1, 2, 3, 8]

  def new(attributes) when is_map(attributes) do
    with :ok <- reject_unknown_keys(attributes),
         {:ok, submit_sm} <- validate_submit_sm(Map.get(attributes, :submit_sm)),
         true <- valid_attributes?(attributes),
         {:ok, attributes} <- put_segment(attributes) do
      {:ok, struct!(__MODULE__, Map.put(attributes, :submit_sm, submit_sm))}
    else
      _ -> {:error, :invalid_envelope}
    end
  end

  def new(_attributes), do: {:error, :invalid_envelope}

  def encode(%__MODULE__{} = envelope) do
    case envelope.segment do
      nil -> {:ok, encode_legacy(envelope)}
      segment -> encode_with_segment(envelope, segment)
    end
  end

  def decode(payload) when is_binary(payload) do
    with {:ok, attributes} <- decode_json(payload),
         :ok <- validate_version(attributes),
         :ok <- reject_legacy_segment(attributes),
         {:ok, attributes} <- materialize_submit_sm(attributes) do
      attributes
      |> Map.drop(["version"])
      |> atomize_known_keys()
      |> new()
    else
      {:error, :unsupported_version} -> {:error, :unsupported_version}
      _ -> {:error, :invalid_envelope}
    end
  end

  def decode(_payload), do: {:error, :invalid_envelope}

  defp decode_json(payload) do
    {:ok, :json.decode(payload)}
  rescue
    _error -> {:error, :invalid_json}
  end

  defp validate_version(%{"version" => version}) when version in [@v1, @v2, @v3], do: :ok
  defp validate_version(%{"version" => _version}), do: {:error, :unsupported_version}
  defp validate_version(_attributes), do: {:error, :invalid_envelope}

  defp reject_legacy_segment(%{"version" => version} = attributes) when version in [@v1, @v2] do
    if Map.has_key?(attributes, "segment"), do: :error, else: :ok
  end

  defp reject_legacy_segment(%{"version" => @v3, "segment" => segment}) when is_map(segment),
    do: :ok

  defp reject_legacy_segment(_attributes), do: :error

  defp materialize_submit_sm(%{"version" => version, "submit_sm" => submit} = attributes)
       when version in [@v2, @v3] and is_map(submit) do
    with :ok <- reject_v2_raw_fields(submit),
         {:ok, submit} <- decode_v2_binary_fields(submit) do
      {:ok, Map.put(attributes, "submit_sm", submit)}
    end
  end

  defp materialize_submit_sm(%{"version" => version}) when version in [@v2, @v3],
    do: {:error, :invalid_envelope}

  defp materialize_submit_sm(attributes), do: {:ok, attributes}

  defp reject_v2_raw_fields(submit) do
    if Map.has_key?(submit, "short_message") or Map.has_key?(submit, "optional_parameters") do
      {:error, :invalid_envelope}
    else
      :ok
    end
  end

  defp decode_v2_binary_fields(submit) do
    with {:ok, submit} <- decode_required_base64(submit, "short_message_base64", "short_message"),
         {:ok, submit} <-
           decode_optional_base64(submit, "optional_parameters_base64", "optional_parameters") do
      {:ok, absents_v2_json_null(submit)}
    end
  end

  defp decode_required_base64(submit, from, to) do
    case Map.fetch(submit, from) do
      {:ok, encoded} when is_binary(encoded) -> put_decoded_base64(submit, from, to, encoded)
      _ -> {:error, :invalid_envelope}
    end
  end

  defp decode_optional_base64(submit, from, to) do
    case Map.fetch(submit, from) do
      :error ->
        {:ok, submit}

      {:ok, encoded} when is_binary(encoded) ->
        case canonical_base64(encoded) do
          {:ok, <<>>} -> {:ok, Map.delete(submit, from)}
          {:ok, bytes} -> {:ok, submit |> Map.delete(from) |> Map.put(to, bytes)}
          :error -> {:error, :invalid_envelope}
        end

      _ ->
        {:error, :invalid_envelope}
    end
  end

  defp put_decoded_base64(submit, from, to, encoded) do
    case canonical_base64(encoded) do
      {:ok, bytes} -> {:ok, submit |> Map.delete(from) |> Map.put(to, bytes)}
      :error -> {:error, :invalid_envelope}
    end
  end

  defp canonical_base64(encoded) do
    case Base.decode64(encoded) do
      {:ok, bytes} ->
        if Base.encode64(bytes) == encoded, do: {:ok, bytes}, else: :error

      :error ->
        :error
    end
  end

  defp reject_unknown_keys(attributes) do
    if Map.keys(attributes) -- @known_keys == [], do: :ok, else: :error
  end

  defp put_segment(attributes) do
    bind_segment(Map.get(attributes, :segment), attributes)
  end

  defp bind_segment(nil, attributes), do: {:ok, Map.delete(attributes, :segment)}

  defp bind_segment(segment, attributes) do
    case normalize_segment(segment, Map.get(attributes, :gateway_id)) do
      {:ok, segment} -> {:ok, Map.put(attributes, :segment, segment)}
      :error -> :error
    end
  end

  defp normalize_segment(segment, gateway_id) when is_map(segment) and is_binary(gateway_id) do
    with :ok <- reject_unknown_segment_keys(segment),
         {:ok, bill_id} <- nonempty_binary(segment_value(segment, :bill_id)),
         true <- bill_id != gateway_id,
         {:ok, index} <- segment_index(segment_value(segment, :index)),
         {:ok, count} <- segment_index(segment_value(segment, :count)),
         true <- index <= count,
         {:ok, 1} <- fingerprint_version(segment_value(segment, :fingerprint_version)),
         {:ok, digest} <- fingerprint_digest(segment_value(segment, :fingerprint_digest_base64)) do
      {:ok,
       %{
         bill_id: bill_id,
         index: index,
         count: count,
         fingerprint_version: 1,
         fingerprint_digest_base64: digest
       }}
    else
      _ -> :error
    end
  end

  defp normalize_segment(_segment, _gateway_id), do: :error

  defp reject_unknown_segment_keys(segment) do
    keys = Map.keys(segment)

    if Enum.any?(keys, &unknown_segment_key?/1) or length(keys) != length(@segment_fields) do
      :error
    else
      :ok
    end
  end

  defp unknown_segment_key?(key) when is_atom(key), do: Atom.to_string(key) not in @segment_names
  defp unknown_segment_key?(key) when is_binary(key), do: key not in @segment_names
  defp unknown_segment_key?(_key), do: true

  defp segment_value(segment, key) do
    case Map.fetch(segment, key) do
      {:ok, value} -> value
      :error -> Map.get(segment, Atom.to_string(key))
    end
  end

  defp nonempty_binary(value) when is_binary(value) and byte_size(value) > 0, do: {:ok, value}
  defp nonempty_binary(_value), do: :error

  defp segment_index(value) when is_integer(value) and value >= 1 and value <= 255,
    do: {:ok, value}

  defp segment_index(_value), do: :error

  defp fingerprint_version(1), do: {:ok, 1}
  defp fingerprint_version(_value), do: :error

  defp fingerprint_digest(encoded) when is_binary(encoded) do
    case canonical_base64(encoded) do
      {:ok, bytes} when byte_size(bytes) == 32 -> {:ok, encoded}
      _ -> :error
    end
  end

  defp fingerprint_digest(_encoded), do: :error

  defp encode_legacy(envelope) do
    envelope
    |> Map.from_struct()
    |> Map.delete(:segment)
    |> Map.put(:version, @v2)
    |> stringify_envelope()
    |> :json.encode()
    |> IO.iodata_to_binary()
  end

  defp encode_with_segment(envelope, segment) do
    case normalize_segment(segment, envelope.gateway_id) do
      {:ok, segment} ->
        payload =
          envelope
          |> Map.from_struct()
          |> Map.put(:segment, segment)
          |> Map.put(:version, @v3)
          |> stringify_envelope()
          |> :json.encode()
          |> IO.iodata_to_binary()

        {:ok, payload}

      :error ->
        {:error, :invalid_envelope}
    end
  end

  defp valid_attributes?(attributes) do
    Enum.all?(
      [:gateway_id, :connector_id, :enqueued_at, :expires_at],
      &is_binary(Map.get(attributes, &1))
    ) and
      Enum.all?(
        [:attempt, :max_attempts],
        &(is_integer(Map.get(attributes, &1)) and Map.get(attributes, &1) > 0)
      ) and
      Map.get(attributes, :attempt) <= Map.get(attributes, :max_attempts)
  end

  defp validate_submit_sm(
         %{
           "source_addr" => source,
           "destination_addr" => destination,
           "short_message" => message
         } = submit_sm
       ),
       do:
         validate_submit_sm(%{
           source_addr: source,
           destination_addr: destination,
           short_message: message,
           data_coding: Map.get(submit_sm, "data_coding"),
           registered_delivery: Map.get(submit_sm, "registered_delivery"),
           esm_class: Map.get(submit_sm, "esm_class"),
           optional_parameters: Map.get(submit_sm, "optional_parameters")
         })

  defp validate_submit_sm(
         %{
           source_addr: source,
           destination_addr: destination,
           short_message: message
         } = submit_sm
       )
       when is_binary(source) and is_binary(destination) and is_binary(message) do
    with {:ok, data_coding} <- normalize_data_coding(Map.get(submit_sm, :data_coding)),
         {:ok, registered_delivery} <-
           normalize_registered_delivery(Map.get(submit_sm, :registered_delivery)),
         {:ok, esm_class} <- normalize_esm_class(Map.get(submit_sm, :esm_class)),
         {:ok, optional} <-
           normalize_optional_parameters(Map.get(submit_sm, :optional_parameters)) do
      submit = %{
        source_addr: source,
        destination_addr: destination,
        short_message: message,
        data_coding: data_coding,
        registered_delivery: registered_delivery
      }

      {:ok, submit |> put_esm_class(esm_class) |> put_optional_parameters(optional)}
    else
      :error -> {:error, :invalid_submit_sm}
    end
  end

  defp validate_submit_sm(_submit_sm), do: {:error, :invalid_submit_sm}

  defp absents_v2_json_null(submit) do
    submit
    |> replace_v2_json_null("data_coding")
    |> replace_v2_json_null("registered_delivery")
    |> replace_v2_json_null("esm_class")
  end

  defp replace_v2_json_null(submit, key) do
    case Map.fetch(submit, key) do
      {:ok, :null} -> Map.put(submit, key, nil)
      _ -> submit
    end
  end

  defp normalize_data_coding(nil), do: {:ok, 0}

  defp normalize_data_coding(data_coding) when data_coding in @allowed_data_coding,
    do: {:ok, data_coding}

  defp normalize_data_coding(_data_coding), do: :error

  defp normalize_registered_delivery(nil), do: {:ok, 0}
  defp normalize_registered_delivery(0), do: {:ok, 0}
  defp normalize_registered_delivery(1), do: {:ok, 1}
  defp normalize_registered_delivery(_value), do: :error

  defp normalize_esm_class(nil), do: {:ok, 0}

  defp normalize_esm_class(esm_class)
       when is_integer(esm_class) and esm_class >= 0 and esm_class <= 255,
       do: {:ok, esm_class}

  defp normalize_esm_class(_esm_class), do: :error

  defp normalize_optional_parameters(nil), do: {:ok, :absent}
  defp normalize_optional_parameters(<<>>), do: {:ok, :absent}
  defp normalize_optional_parameters(bytes) when is_binary(bytes), do: {:ok, bytes}
  defp normalize_optional_parameters(_bytes), do: :error

  defp put_esm_class(submit, 0), do: submit
  defp put_esm_class(submit, esm_class), do: Map.put(submit, :esm_class, esm_class)

  defp put_optional_parameters(submit, :absent), do: submit
  defp put_optional_parameters(submit, bytes), do: Map.put(submit, :optional_parameters, bytes)

  defp stringify_envelope(attributes) do
    attributes
    |> Map.update!(:submit_sm, &stringify_submit_sm/1)
    |> stringify_segment()
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp stringify_segment(%{segment: segment} = attributes) when is_map(segment) do
    Map.put(attributes, :segment, Map.new(@segment_fields, &string_field(segment, &1)))
  end

  defp stringify_segment(attributes), do: attributes

  defp string_field(map, key), do: {Atom.to_string(key), Map.fetch!(map, key)}

  defp stringify_submit_sm(submit_sm) do
    message = Map.fetch!(submit_sm, :short_message)

    submit_sm
    |> Map.delete(:short_message)
    |> encode_optional_parameters()
    |> encode_esm_class()
    |> Map.put(:short_message_base64, Base.encode64(message))
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp encode_esm_class(%{esm_class: 0} = submit_sm), do: Map.delete(submit_sm, :esm_class)
  defp encode_esm_class(submit_sm), do: submit_sm

  defp encode_optional_parameters(submit_sm) do
    case Map.pop(submit_sm, :optional_parameters) do
      {bytes, rest} when is_binary(bytes) and byte_size(bytes) > 0 ->
        Map.put(rest, :optional_parameters_base64, Base.encode64(bytes))

      {_absent, rest} ->
        rest
    end
  end

  defp atomize_known_keys(attributes) do
    envelope = Map.new(@fields, fn key -> {key, Map.get(attributes, Atom.to_string(key))} end)

    case Map.fetch(attributes, "segment") do
      :error -> envelope
      {:ok, segment} -> Map.put(envelope, :segment, segment)
    end
  end
end
