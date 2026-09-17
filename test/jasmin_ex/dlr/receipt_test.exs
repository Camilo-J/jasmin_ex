defmodule JasminEx.Dlr.ReceiptTest do
  use ExUnit.Case, async: true

  alias JasminEx.Dlr.Receipt
  alias JasminEx.Smpp.PDU.Body

  describe "parse/1" do
    test "classifies a text-only receipt and pads short numeric fields" do
      pdu =
        deliver_sm(
          "id:00ab12 sub:1 dlvrd:1 submit date:2601011200 done date:2601011201 stat:DELIVRD err:0 text:hello"
        )

      assert {:ok, receipt} = Receipt.parse(pdu)
      assert receipt.id == "00ab12"
      assert receipt.stat == "DELIVRD"
      assert receipt.sub == "001"
      assert receipt.dlvrd == "001"
      assert receipt.err == "000"
      assert receipt.sdate == "2601011200"
      assert receipt.ddate == "2601011201"
      assert receipt.text == "hello"
    end

    test "classifies a TLV-only receipt and defaults missing optional fields" do
      pdu =
        deliver_sm("",
          optional_parameters: tlv_id_state("00ab12", 2)
        )

      assert {:ok, receipt} = Receipt.parse(pdu)
      assert receipt.id == "00ab12"
      assert receipt.stat == "DELIVRD"
      assert receipt.sub == "ND"
      assert receipt.dlvrd == "ND"
      assert receipt.sdate == "ND"
      assert receipt.ddate == "ND"
      assert receipt.err == "ND"
      assert receipt.text == ""
    end

    test "TLV id and state win over conflicting text while text fills optional details" do
      pdu =
        deliver_sm(
          "id:other sub:001 dlvrd:001 submit date:2601011200 done date:2601011201 stat:UNDELIV err:000 text:hi",
          optional_parameters: tlv_id_state("AB12", 2)
        )

      assert {:ok, receipt} = Receipt.parse(pdu)
      assert receipt.id == "AB12"
      assert receipt.stat == "DELIVRD"
      assert receipt.sub == "001"
      assert receipt.dlvrd == "001"
      assert receipt.sdate == "2601011200"
      assert receipt.ddate == "2601011201"
      assert receipt.err == "000"
      assert receipt.text == "hi"
    end

    test "TLV ENROUTE and SCHEDULED map to ENROUTE" do
      enroute = deliver_sm("", optional_parameters: tlv_id_state("AB12", 1))
      scheduled = deliver_sm("", optional_parameters: tlv_id_state("AB12", 0))

      assert {:ok, %Receipt{stat: "ENROUTE"}} = Receipt.parse(enroute)
      assert {:ok, %Receipt{stat: "ENROUTE"}} = Receipt.parse(scheduled)
    end

    test "unknown TLV state maps to UNKNOWN" do
      pdu = deliver_sm("", optional_parameters: tlv_id_state("AB12", 9))
      assert {:ok, receipt} = Receipt.parse(pdu)
      assert receipt.stat == "UNKNOWN"
    end

    test "missing both identifying fields is not a DLR" do
      assert :not_dlr = Receipt.parse(deliver_sm("ordinary MO text"))
    end

    test "TLV id without state and without text stat is not a DLR" do
      value = <<"AB12", 0>>
      optional = <<0x001E::16, byte_size(value)::16, value::binary>>
      assert :not_dlr = Receipt.parse(deliver_sm("hello", optional_parameters: optional))
    end

    test "text without id and stat is not a receipt" do
      assert :not_dlr = Receipt.parse(deliver_sm("just a mobile originated message"))
    end

    test "recognizable but invalid receipt is an error" do
      optional = <<0x001E::16, 4::16, "ab">>
      assert {:error, :truncated} = Receipt.parse(deliver_sm("", optional_parameters: optional))
    end

    test "non-UTF8 body does not crash" do
      pdu =
        deliver_sm(
          <<"id:00ab12 sub:1 dlvrd:1 submit date:1 done date:1 stat:DELIVRD err:0 text:", 0xFF,
            0xFE>>
        )

      assert {:ok, receipt} = Receipt.parse(pdu)
      assert receipt.id == "00ab12"
      assert receipt.stat == "DELIVRD"
    end

    test "esm_class receipt bit is not required" do
      pdu =
        deliver_sm(
          "id:00ab12 sub:001 dlvrd:001 submit date:2601011200 done date:2601011201 stat:DELIVRD err:000 text:x",
          esm_class: 0
        )

      assert {:ok, receipt} = Receipt.parse(pdu)
      assert receipt.stat == "DELIVRD"
    end

    test "data_sm is not classified as a DLR" do
      assert :not_dlr = Receipt.parse(%{command: :data_sm, short_message: "id:1 stat:DELIVRD"})
    end
  end

  defp deliver_sm(short_message, opts \\ []) do
    struct!(%Body.DeliverSM{short_message: short_message}, opts)
  end

  defp tlv_id_state(id, state) do
    value = <<id::binary, 0>>
    <<0x001E::16, byte_size(value)::16, value::binary, 0x0427::16, 1::16, state>>
  end
end
