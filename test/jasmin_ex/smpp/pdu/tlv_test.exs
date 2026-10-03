defmodule JasminEx.Smpp.PDU.TlvTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.PDU.Tlv

  @receipted_message_id 0x001E
  @message_state 0x0427
  @unknown_tag 0x1403

  describe "decode/1" do
    test "decodes receipted_message_id and message_state" do
      binary =
        tlv(@receipted_message_id, <<"00ab12", 0>>) <>
          tlv(@message_state, <<2>>)

      assert {:ok, tlvs} = Tlv.decode(binary)
      assert {:receipted_message_id, "00ab12"} in tlvs
      assert {:message_state, 2} in tlvs
    end

    test "preserves unknown well-formed TLVs as numeric-tag/binary pairs" do
      binary = tlv(@unknown_tag, <<"xyz">>)

      assert {:ok, [{@unknown_tag, "xyz"}]} = Tlv.decode(binary)
    end

    test "rejects a truncated TLV header" do
      assert {:error, :truncated} = Tlv.decode(<<0x00, 0x1E, 0x00>>)
    end

    test "rejects a truncated TLV value" do
      assert {:error, :truncated} = Tlv.decode(<<@receipted_message_id::16, 4::16, "ab">>)
    end

    test "rejects duplicate known singleton tags" do
      binary =
        tlv(@receipted_message_id, <<"one", 0>>) <>
          tlv(@receipted_message_id, <<"two", 0>>)

      assert {:error, :duplicate_tag} = Tlv.decode(binary)
    end

    test "rejects duplicate message_state tags" do
      binary = tlv(@message_state, <<2>>) <> tlv(@message_state, <<6>>)
      assert {:error, :duplicate_tag} = Tlv.decode(binary)
    end

    test "decodes an empty optional section" do
      assert {:ok, []} = Tlv.decode(<<>>)
    end
  end

  describe "SAR codec" do
    test "encodes exact wire bytes and decodes ordered tuples with unknown tags" do
      params = [sar_msg_ref_num: 65_535, sar_total_segments: 255, sar_segment_seqnum: 255]
      wire = <<0x020C::16, 2::16, 65_535::16, 0x020E::16, 1::16, 255, 0x020F::16, 1::16, 255>>
      assert {:ok, ^wire} = Tlv.encode(params)
      assert {:ok, ^params} = Tlv.decode(wire)
      unknown = tlv(@unknown_tag, <<0, 255>>)
      assert {:ok, decoded} = Tlv.decode(unknown <> wire <> unknown)
      assert {:ok, encoded} = Tlv.encode(decoded)
      assert encoded == unknown <> wire <> unknown
      assert :ok = Tlv.validate_sar(wire)
      assert :ok = Tlv.validate_sar(<<>>)
    end

    test "structural decode accepts partial or zero SAR but rejects bad widths and duplicates" do
      assert {:ok, [sar_total_segments: 0]} = Tlv.decode(tlv(0x020E, <<0>>))

      for {tag, value} <- [{0x020C, <<1>>}, {0x020E, <<1, 2>>}, {0x020F, <<>>}] do
        assert {:error, :invalid_sar_length} = Tlv.decode(tlv(tag, value))
      end

      for {tag, value} <- [{0x020C, <<0, 1>>}, {0x020E, <<2>>}, {0x020F, <<1>>}] do
        assert {:error, :duplicate_tag} = Tlv.decode(tlv(tag, value) <> tlv(tag, value))
      end

      assert {:error, :truncated} = Tlv.decode(<<0x020C::16, 2::16, 1>>)
    end

    test "semantic validation rejects incomplete, zero, inconsistent and UDHI SAR" do
      assert {:error, :incomplete_sar} = Tlv.validate_sar(tlv(0x020C, <<0, 0>>))

      for {total, seq, reason} <- [
            {0, 1, :invalid_sar_total_segments},
            {2, 0, :invalid_sar_segment_seqnum},
            {2, 3, :inconsistent_sar}
          ] do
        wire = tlv(0x020C, <<0, 0>>) <> tlv(0x020E, <<total>>) <> tlv(0x020F, <<seq>>)
        assert {:error, ^reason} = Tlv.validate_sar(wire)
      end

      assert {:ok, wire} =
               Tlv.encode(sar_msg_ref_num: 0, sar_total_segments: 1, sar_segment_seqnum: 1)

      assert :ok = Tlv.validate_sar(wire)
      assert {:error, :sar_with_udhi} = Tlv.validate_sar(wire, 0x40)
      assert :ok = Tlv.validate_sar(tlv(@unknown_tag, "x"), 0x40)
    end

    test "encoding validates integer ranges and raw TLV lengths without wrapping" do
      for value <- [-1, 65_536, "1", nil] do
        assert {:error, :invalid_sar_msg_ref_num} = Tlv.encode(sar_msg_ref_num: value)
      end

      for {key, reason} <- [
            {:sar_total_segments, :invalid_sar_total_segments},
            {:sar_segment_seqnum, :invalid_sar_segment_seqnum}
          ],
          value <- [0, 256, -1, nil] do
        assert {:error, ^reason} = Tlv.encode([{key, value}])
      end

      assert {:error, :incomplete_sar} = Tlv.encode(sar_msg_ref_num: 1)
      assert {:error, :invalid_length} = Tlv.encode([{@unknown_tag, :binary.copy("x", 65_536)}])
      assert {:error, :invalid_tlv} = Tlv.encode([{65_536, "x"}])
      assert {:error, :invalid_tlv} = Tlv.encode(nil)
      assert {:error, :invalid_optional_parameters} = Tlv.validate_sar(nil)
    end

    test "numeric SAR tags cannot bypass width, duplicate or semantic checks" do
      assert {:error, :invalid_sar_length} = Tlv.encode([{0x020C, <<1>>}])
      assert {:error, :incomplete_sar} = Tlv.encode([{0x020C, <<0, 1>>}])
      params = [sar_msg_ref_num: 1, sar_total_segments: 2, sar_segment_seqnum: 3]
      assert {:error, :inconsistent_sar} = Tlv.encode(params)
      assert {:error, :duplicate_tag} = Tlv.encode(params ++ [{0x020C, <<0, 1>>}])
      assert {:ok, <<>>} = Tlv.encode([])
    end

    test "existing receipt classification remains compatible" do
      params = [receipted_message_id: "abc", message_state: 2]
      assert {:ok, wire} = Tlv.encode(params)
      assert wire == tlv(@receipted_message_id, <<"abc", 0>>) <> tlv(@message_state, <<2>>)
      assert {:ok, ^params} = Tlv.decode(wire)
      assert {:ok, [message_state: <<1, 2>>]} = Tlv.decode(tlv(@message_state, <<1, 2>>))
    end
  end

  defp tlv(tag, value) when is_integer(tag) and is_binary(value) do
    <<tag::16, byte_size(value)::16, value::binary>>
  end
end
