defmodule JasminEx.MtSubmitPipeline.SegmentationTest do
  use ExUnit.Case, async: true

  alias JasminEx.MtSubmitPipeline.Segmentation
  alias JasminEx.Smpp.PDU.Coding

  test "single messages retain their exact bytes and have no concatenation metadata" do
    for {coding, payload} <- [
          {0, repeat("a", 160)},
          {2, repeat(<<255>>, 140)},
          {3, repeat(<<233>>, 140)},
          {8, repeat(<<0, 65>>, 70)},
          {0, <<>>}
        ] do
      for options <- [[], [concat: :sar, reference: 65_535]] do
        assert {:ok, %{count: 1, segments: [segment]}} =
                 Segmentation.plan(payload, coding, options)

        assert segment == %{index: 1, count: 1, short_message: payload, esm_class: 0}
      end
    end
  end

  test "UDH defaults to an eight-bit reference and real SMS capacities" do
    for {coding, payload, sizes} <- [
          {0, repeat("a", 161), [153, 8]},
          {2, repeat(<<255>>, 141), [134, 7]},
          {3, repeat(<<233>>, 141), [134, 7]},
          {8, repeat(<<0, 65>>, 71), [134, 8]}
        ] do
      assert {:ok, %{count: 2, segments: segments}} =
               Segmentation.plan(payload, coding, reference: 42)

      assert Enum.map(segments, &(byte_size(&1.short_message) - 6)) == sizes

      for {segment, index} <- Enum.with_index(segments, 1) do
        assert segment.index == index and segment.count == 2
        assert segment.esm_class == 0x40
        assert <<5, 0, 3, 42, 2, ^index, _::binary>> = segment.short_message
        refute Map.has_key?(segment, :sar_msg_ref_num)
      end

      assert reassemble(segments, :udh) == payload
    end
  end

  test "GSM extension escapes stay attached at segment boundaries" do
    payload = repeat("a", 152) <> <<27, 101>> <> repeat("b", 7)
    assert {:ok, %{segments: segments}} = Segmentation.plan(payload, 0, reference: 0)
    assert Enum.map(segments, &(byte_size(&1.short_message) - 6)) == [152, 9]
    assert <<5, 0, 3, 0, 2, 2, 27, 101, _::binary>> = Enum.at(segments, 1).short_message
    assert reassemble(segments, :udh) == payload
    assert_valid_parts(segments, 0, :udh)
  end

  test "UTF-16 surrogate pairs are indivisible rather than counted as one code unit" do
    payload = repeat(<<0, 65>>, 66) <> <<0xD8, 0x3D, 0xDE, 0x80>> <> repeat(<<0, 66>>, 3)
    assert {:ok, %{segments: segments}} = Segmentation.plan(payload, 8, reference: 255)
    assert Enum.map(segments, &(byte_size(&1.short_message) - 6)) == [132, 10]
    assert reassemble(segments, :udh) == payload
    assert_valid_parts(segments, 8, :udh)
  end

  test "SAR reserves seven handset UDH octets even though its payload has no UDH" do
    for {coding, payload, sizes} <- [
          {0, repeat("a", 161), [152, 9]},
          {2, repeat(<<255>>, 141), [133, 8]},
          {3, repeat(<<233>>, 141), [133, 8]},
          {8, repeat(<<0, 65>>, 71), [132, 10]}
        ] do
      assert {:ok, %{count: 2, segments: segments}} =
               Segmentation.plan(payload, coding, concat: :sar, reference: 65_535)

      assert Enum.map(segments, &byte_size(&1.short_message)) == sizes

      for {segment, index} <- Enum.with_index(segments, 1) do
        assert segment.esm_class == 0
        assert segment.sar_msg_ref_num == 65_535
        assert segment.sar_total_segments == 2 and segment.sar_segment_seqnum == index
        assert segment.index == index and segment.count == 2
      end

      assert reassemble(segments, :sar) == payload
    end
  end

  test "exact multipart limits are accepted and overflow is rejected without truncation" do
    for {coding, unit, capacity} <- [
          {0, "a", 153},
          {2, <<255>>, 134},
          {3, <<233>>, 134},
          {8, <<0, 65>>, 67}
        ] do
      payload = repeat(unit, capacity * 5)

      assert {:ok, %{count: 5, segments: segments}} =
               Segmentation.plan(payload, coding, reference: 1)

      assert reassemble(segments, :udh) == payload

      assert Segmentation.plan(payload <> unit, coding, reference: 1) ==
               {:error, {:too_many_segments, 6, 5}}

      assert {:ok, %{count: 6}} =
               Segmentation.plan(payload <> unit, coding, reference: 1, max_segments: 6)
    end

    assert Segmentation.plan(repeat("a", 161), 0, reference: 1, max_segments: 1) ==
             {:error, {:too_many_segments, 2, 1}}
  end

  test "SAR exact limits and protocol segment-count ceiling never truncate" do
    for {coding, unit, capacity} <- [
          {0, "a", 152},
          {2, <<255>>, 133},
          {3, <<233>>, 133},
          {8, <<0, 65>>, 66}
        ] do
      payload = repeat(unit, capacity * 5)
      options = [concat: :sar, reference: 0]
      assert {:ok, %{count: 5, segments: segments}} = Segmentation.plan(payload, coding, options)
      assert reassemble(segments, :sar) == payload

      assert Segmentation.plan(payload <> unit, coding, options) ==
               {:error, {:too_many_segments, 6, 5}}
    end

    for {method, capacity} <- [{:udh, 153}, {:sar, 152}] do
      payload = repeat("a", capacity * 255)
      options = [concat: method, reference: 1, max_segments: 255]
      assert {:ok, %{count: 255, segments: segments}} = Segmentation.plan(payload, 0, options)
      assert reassemble(segments, method) == payload
      assert List.last(segments).index == 255

      assert Segmentation.plan(payload <> "a", 0, options) ==
               {:error, {:too_many_segments, 256, 255}}
    end
  end

  test "SAR also preserves escape pairs and surrogate pairs at its own boundaries" do
    for {coding, payload, sizes} <- [
          {0, repeat("a", 151) <> <<27, 101>> <> repeat("b", 8), [151, 10]},
          {8,
           repeat(<<0, 65>>, 65) <>
             <<0xD8, 0x3D, 0xDE, 0x80>> <>
             repeat(<<0, 66>>, 4), [130, 12]}
        ] do
      assert {:ok, %{segments: segments}} =
               Segmentation.plan(payload, coding, concat: :sar, reference: 256)

      assert Enum.map(segments, &byte_size(&1.short_message)) == sizes
      assert reassemble(segments, :sar) == payload
      assert_valid_parts(segments, coding, :sar)
    end
  end

  test "validation rejects malformed encoded payloads even below the single limit" do
    for {coding, payload} <- [
          {0, <<27>>},
          {0, <<27, 0>>},
          {0, <<128>>},
          {8, <<0>>},
          {8, <<0xD8, 0>>},
          {8, <<0xDC, 0>>},
          {8, <<0xD8, 0, 0, 65>>},
          {0, :not_binary}
        ] do
      assert Segmentation.plan(payload, coding) == {:error, :invalid_payload}
    end

    for coding <- [1, 4, 5, 6, 7, 9, 10, 13, 14, 255, :UCS2, nil] do
      assert Segmentation.plan("a", coding) == {:error, :unsupported_coding}
    end
  end

  test "configuration is validated without raising or silently ignoring options" do
    for options <- [
          %{},
          nil,
          [:bad],
          [concat: :other],
          [max_segments: 0],
          [max_segments: 256],
          [max_segments: 1.5],
          [unknown: true],
          [max_segments: 2, max_segments: 3]
        ] do
      assert Segmentation.plan("a", 0, options) == {:error, :invalid_options}
    end

    for options <- [
          [reference: -1],
          [reference: 256],
          [reference: "1"],
          [concat: :sar, reference: 65_536]
        ] do
      assert Segmentation.plan("a", 0, options) == {:error, :invalid_reference}
    end

    assert Segmentation.plan(repeat("a", 161), 0) == {:error, :invalid_reference}

    assert Segmentation.plan(repeat("a", 161), 0, concat: :sar) ==
             {:error, :invalid_reference}
  end

  test "plans are deterministic and round-trip varied boundary-adjacent payloads" do
    for {coding, text} <- [{0, "a€[]"}, {8, "A🚀日"}],
        length <- [0, 1, 25, 26, 27, 66, 67, 68, 70, 71, 76, 77],
        method <- [:udh, :sar] do
      {:ok, payload} = Coding.encode_short_message(coding, repeat(text, length))
      options = [concat: method, reference: 7, max_segments: 10]
      assert {:ok, %{segments: segments}} = result = Segmentation.plan(payload, coding, options)
      assert Segmentation.plan(payload, coding, options) == result
      assert reassemble(segments, method) == payload
      assert_valid_parts(segments, coding, method)
    end
  end

  defp repeat(binary, count), do: :binary.copy(binary, count)

  defp content(%{count: 1, short_message: payload}, _method), do: payload
  defp content(%{short_message: <<_header::binary-size(6), payload::binary>>}, :udh), do: payload
  defp content(%{short_message: payload}, :sar), do: payload

  defp reassemble(segments, method),
    do: segments |> Enum.map(&content(&1, method)) |> IO.iodata_to_binary()

  defp assert_valid_parts(segments, coding, method) do
    for segment <- segments do
      assert {:ok, _} = Coding.decode_short_message(coding, content(segment, method))
    end
  end
end
