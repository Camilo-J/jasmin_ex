defmodule JasminEx.MtSubmitPipeline.Segmentation do
  @moduledoc """
  Pure SMS segment planning for already-encoded GSM, octet, Latin-1 and UTF-16BE bytes.

  `plan/3` defaults to eight-bit UDH concatenation and at most five segments.
  Multipart plans require a caller-owned reference; no reference is generated here.
  SAR plans reserve a seven-octet handset UDH for a sixteen-bit reference.
  See `docs/http-long-messages.md` for provider assumptions and result fields.
  The production HTTP pipeline uses this planner for encoded multipart payloads before
  segment-aware admission and queue dispatch. This module remains a pure planner; HTTP
  policy, reference allocation, billing and transport are owned by their respective layers.
  """

  alias JasminEx.Smpp.PDU.Coding

  @type segment :: %{
          required(:index) => pos_integer(),
          required(:count) => pos_integer(),
          required(:short_message) => binary(),
          required(:esm_class) => 0 | 64,
          optional(:sar_msg_ref_num) => non_neg_integer(),
          optional(:sar_total_segments) => pos_integer(),
          optional(:sar_segment_seqnum) => pos_integer()
        }
  @type error ::
          :unsupported_coding
          | :invalid_payload
          | :invalid_options
          | :invalid_reference
          | {:too_many_segments, pos_integer(), pos_integer()}

  @spec plan(binary(), non_neg_integer(), keyword()) ::
          {:ok, %{count: pos_integer(), segments: [segment()]}} | {:error, error()}
  def plan(payload, coding, options \\ []) do
    with {:ok, config} <- configuration(options),
         :ok <- supported(coding),
         :ok <- valid_payload(payload, coding) do
      if byte_size(payload) <= single_capacity(coding) do
        {:ok, %{count: 1, segments: [segment(payload, 1, 1)]}}
      else
        parts = split(payload, coding, capacity(coding, config.concat), [], 0, [])
        multipart(parts, config)
      end
    end
  end

  defp configuration(options) when is_list(options) do
    if known_options?(options) do
      validate_config(%{
        concat: Keyword.get(options, :concat, :udh),
        max_segments: Keyword.get(options, :max_segments, 5),
        reference: Keyword.get(options, :reference)
      })
    else
      {:error, :invalid_options}
    end
  end

  defp configuration(_options), do: {:error, :invalid_options}

  defp known_options?(options) do
    Keyword.keyword?(options) and
      Enum.all?(Keyword.keys(options), &(&1 in [:concat, :max_segments, :reference])) and
      length(Keyword.keys(options)) == length(Enum.uniq(Keyword.keys(options)))
  end

  defp validate_config(config) do
    cond do
      config.concat not in [:udh, :sar] ->
        {:error, :invalid_options}

      not valid_max_segments?(config.max_segments) ->
        {:error, :invalid_options}

      not is_nil(config.reference) and not valid_reference?(config) ->
        {:error, :invalid_reference}

      true ->
        {:ok, config}
    end
  end

  defp valid_max_segments?(max_segments),
    do: is_integer(max_segments) and max_segments in 1..255

  defp valid_reference?(%{concat: method, reference: reference}) do
    limit = if method == :udh, do: 255, else: 65_535
    is_integer(reference) and reference >= 0 and reference <= limit
  end

  defp supported(coding) when coding in [0, 2, 3, 8], do: :ok
  defp supported(_coding), do: {:error, :unsupported_coding}

  defp valid_payload(payload, coding) when is_binary(payload) do
    case Coding.decode_short_message(coding, payload) do
      {:ok, _decoded} -> :ok
      :error -> {:error, :invalid_payload}
    end
  end

  defp valid_payload(_payload, _coding), do: {:error, :invalid_payload}

  defp single_capacity(0), do: 160
  defp single_capacity(_coding), do: 140

  # UDH8 occupies six octets; SAR16 reserves seven for SMSC-generated UDH.
  # GSM includes septet alignment, UTF-16 must use an even octet capacity.
  defp capacity(0, :udh), do: 153
  defp capacity(0, :sar), do: 152
  defp capacity(8, :sar), do: 132
  defp capacity(_coding, :udh), do: 134
  defp capacity(_coding, :sar), do: 133

  defp split(<<>>, _coding, _capacity, current, _size, parts) do
    Enum.reverse([part_binary(current) | parts])
  end

  defp split(payload, coding, capacity, current, size, parts) do
    {token, rest} = token(payload, coding)

    if size + byte_size(token) <= capacity do
      split(rest, coding, capacity, [token | current], size + byte_size(token), parts)
    else
      split(rest, coding, capacity, [token], byte_size(token), [part_binary(current) | parts])
    end
  end

  defp part_binary(tokens), do: tokens |> Enum.reverse() |> IO.iodata_to_binary()

  # Payload validation precedes tokenization, so escapes and surrogates are valid.
  defp token(<<27, extension, rest::binary>>, 0), do: {<<27, extension>>, rest}

  defp token(<<high::16, low::16, rest::binary>>, 8) when high in 0xD800..0xDBFF,
    do: {<<high::16, low::16>>, rest}

  defp token(<<unit::16, rest::binary>>, 8), do: {<<unit::16>>, rest}
  defp token(<<byte, rest::binary>>, _coding), do: {<<byte>>, rest}

  defp multipart(parts, config) do
    count = length(parts)

    cond do
      count > config.max_segments ->
        {:error, {:too_many_segments, count, config.max_segments}}

      not valid_reference?(config) ->
        {:error, :invalid_reference}

      true ->
        segments =
          parts
          |> Enum.with_index(1)
          |> Enum.map(fn {payload, index} ->
            concatenate(segment(payload, index, count), config)
          end)

        {:ok, %{count: count, segments: segments}}
    end
  end

  defp segment(payload, index, count),
    do: %{short_message: payload, index: index, count: count, esm_class: 0}

  defp concatenate(segment, %{concat: :udh, reference: reference}) do
    header = <<5, 0, 3, reference, segment.count, segment.index>>
    %{segment | short_message: header <> segment.short_message, esm_class: 0x40}
  end

  defp concatenate(segment, %{concat: :sar, reference: reference}) do
    Map.merge(segment, %{
      sar_msg_ref_num: reference,
      sar_total_segments: segment.count,
      sar_segment_seqnum: segment.index
    })
  end
end
