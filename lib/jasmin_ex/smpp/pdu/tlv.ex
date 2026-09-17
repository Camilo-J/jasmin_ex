defmodule JasminEx.Smpp.PDU.Tlv do
  @moduledoc false

  @receipted_message_id 0x001E
  @message_state 0x0427
  @singletons [@receipted_message_id, @message_state]

  @type tlv ::
          {:receipted_message_id, binary()}
          | {:message_state, non_neg_integer()}
          | {non_neg_integer(), binary()}

  @spec decode(binary()) :: {:ok, [tlv()]} | {:error, :truncated | :duplicate_tag}
  def decode(binary) when is_binary(binary), do: decode(binary, [], MapSet.new())

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
    if MapSet.member?(seen, tag), do: {:error, :duplicate_tag}, else: :ok
  end

  defp reject_duplicate(_tag, _seen), do: :ok

  defp remember(seen, tag) when tag in @singletons, do: MapSet.put(seen, tag)
  defp remember(seen, _tag), do: seen

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
