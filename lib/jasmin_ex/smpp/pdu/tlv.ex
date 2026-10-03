defmodule JasminEx.Smpp.PDU.Tlv do
  @moduledoc false

  @receipted_message_id 0x001E
  @message_state 0x0427
  @sar [sar_msg_ref_num: 0x020C, sar_total_segments: 0x020E, sar_segment_seqnum: 0x020F]
  @singletons [@receipted_message_id, @message_state] ++ Keyword.values(@sar)

  @type tlv ::
          {:receipted_message_id, binary()}
          | {:message_state, non_neg_integer()}
          | {:sar_msg_ref_num | :sar_total_segments | :sar_segment_seqnum, non_neg_integer()}
          | {non_neg_integer(), binary()}

  @type error ::
          :truncated
          | :duplicate_tag
          | :invalid_sar_length
          | :invalid_tlv
          | :invalid_length
          | :invalid_optional_parameters
          | :incomplete_sar
          | :inconsistent_sar
          | :sar_with_udhi
          | :invalid_sar_msg_ref_num
          | :invalid_sar_total_segments
          | :invalid_sar_segment_seqnum

  @spec decode(binary()) :: {:ok, [tlv()]} | {:error, error()}
  def decode(binary) when is_binary(binary), do: decode(binary, [], [])

  @doc "Encodes tuples, validating a complete SAR trio when present."
  @spec encode([tlv()]) :: {:ok, binary()} | {:error, error()}
  def encode(params) when is_list(params) do
    with {:ok, parts} <- encode_params(params, []),
         wire = IO.iodata_to_binary(parts),
         :ok <- validate_sar(wire) do
      {:ok, wire}
    end
  end

  def encode(_params), do: {:error, :invalid_tlv}

  @doc "Validates SAR semantics separately from structural decoding; unknown tags are allowed."
  @spec validate_sar(binary(), non_neg_integer()) :: :ok | {:error, error()}
  def validate_sar(bytes, esm_class \\ 0)

  def validate_sar(bytes, esm_class) when is_binary(bytes) do
    with {:ok, params} <- decode(bytes) do
      sar = for {key, value} <- params, key in Keyword.keys(@sar), into: %{}, do: {key, value}
      validate_trio(sar, esm_class)
    end
  end

  def validate_sar(_bytes, _esm_class), do: {:error, :invalid_optional_parameters}

  defp validate_trio(sar, _esm_class) when map_size(sar) == 0, do: :ok
  defp validate_trio(sar, _esm_class) when map_size(sar) != 3, do: {:error, :incomplete_sar}

  defp validate_trio(sar, esm_class) do
    cond do
      sar.sar_total_segments == 0 -> {:error, :invalid_sar_total_segments}
      sar.sar_segment_seqnum == 0 -> {:error, :invalid_sar_segment_seqnum}
      sar.sar_segment_seqnum > sar.sar_total_segments -> {:error, :inconsistent_sar}
      Bitwise.band(esm_class, 0x40) != 0 -> {:error, :sar_with_udhi}
      true -> :ok
    end
  end

  defp encode_params([], acc), do: {:ok, Enum.reverse(acc)}

  defp encode_params([param | rest], acc) do
    with {:ok, bytes} <- encode_param(param), do: encode_params(rest, [bytes | acc])
  end

  for {key, tag} <- @sar do
    width = if key == :sar_msg_ref_num, do: 16, else: 8
    min = if key == :sar_msg_ref_num, do: 0, else: 1
    max = if key == :sar_msg_ref_num, do: 65_535, else: 255
    reason = String.to_atom("invalid_#{key}")

    defp encode_param({unquote(key), value})
         when is_integer(value) and value >= unquote(min) and value <= unquote(max),
         do: {:ok, <<unquote(tag)::16, unquote(div(width, 8))::16, value::unquote(width)>>}

    defp encode_param({unquote(key), _value}), do: {:error, unquote(reason)}
  end

  defp encode_param({:receipted_message_id, value}) when is_binary(value),
    do: encode_param({@receipted_message_id, value <> <<0>>})

  defp encode_param({:message_state, value}) when is_integer(value) and value in 0..255,
    do: encode_param({@message_state, <<value>>})

  defp encode_param({tag, value})
       when is_integer(tag) and tag in 0..65_535 and is_binary(value) do
    if byte_size(value) <= 65_535,
      do: {:ok, <<tag::16, byte_size(value)::16, value::binary>>},
      else: {:error, :invalid_length}
  end

  defp encode_param(_param), do: {:error, :invalid_tlv}

  defp decode(<<>>, acc, _seen), do: {:ok, Enum.reverse(acc)}

  defp decode(<<_tag::16, _len::16, _rest::binary>> = bin, acc, seen) do
    <<tag::16, length::16, rest::binary>> = bin

    case rest do
      <<value::binary-size(^length), next::binary>> ->
        with :ok <- reject_duplicate(tag, seen),
             {:ok, tlv} <- classify(tag, value) do
          decode(next, [tlv | acc], remember(seen, tag))
        end

      _ ->
        {:error, :truncated}
    end
  end

  defp decode(_truncated_header, _acc, _seen), do: {:error, :truncated}

  defp reject_duplicate(tag, seen) when tag in @singletons do
    if tag in seen, do: {:error, :duplicate_tag}, else: :ok
  end

  defp reject_duplicate(_tag, _seen), do: :ok

  defp remember(seen, tag) when tag in @singletons, do: [tag | seen]
  defp remember(seen, _tag), do: seen

  for {key, tag} <- @sar do
    width = if key == :sar_msg_ref_num, do: 16, else: 8
    defp classify(unquote(tag), <<value::unquote(width)>>), do: {:ok, {unquote(key), value}}
    defp classify(unquote(tag), _value), do: {:error, :invalid_sar_length}
  end

  defp classify(@receipted_message_id, value), do: {:ok, {:receipted_message_id, cstring(value)}}
  defp classify(@message_state, <<state>>), do: {:ok, {:message_state, state}}
  defp classify(@message_state, value), do: {:ok, {:message_state, value}}
  defp classify(tag, value), do: {:ok, {tag, value}}

  defp cstring(value) do
    case :binary.split(value, <<0>>) do
      [id, _rest] -> id
      [id] -> id
    end
  end
end
