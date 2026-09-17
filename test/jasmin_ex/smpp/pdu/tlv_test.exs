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

  defp tlv(tag, value) when is_integer(tag) and is_binary(value) do
    <<tag::16, byte_size(value)::16, value::binary>>
  end
end
