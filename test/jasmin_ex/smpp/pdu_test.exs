defmodule JasminEx.Smpp.PDUTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.PDU

  describe "struct shape" do
    test "build/1 accepts keyword args and stores them verbatim" do
      pdu = PDU.build(command: :submit_sm, status: :ESME_ROK, sequence_number: 42, body: <<>>)

      assert pdu.command == :submit_sm
      assert pdu.status == :ESME_ROK
      assert pdu.sequence_number == 42
      assert pdu.body == <<>>
    end
  end

  describe "header" do
    # From spec: command_length(command_id, command_status, sequence_number) plus
    # command_length must equal 16 + byte_size(body). Plain `<<>>` body here
    # because body shape is exercised in body_test.exs.

    test "encode/1 of a header-only PDU yields exactly 16 bytes" do
      pdu = PDU.build(command: :submit_sm, status: :ESME_ROK, sequence_number: 1, body: <<>>)

      assert IO.iodata_to_binary(PDU.encode(pdu)) ==
               <<0x0000_0010::32, 0x0000_0004::32, 0::32, 0x0000_0001::32>>
    end

    test "encode/1 prepends body bytes and length reflects total" do
      body = <<1, 2, 3>>
      pdu = PDU.build(command: :submit_sm, status: :ESME_ROK, sequence_number: 7, body: body)
      bin = IO.iodata_to_binary(PDU.encode(pdu))
      assert byte_size(bin) == 16 + byte_size(body)
      assert bin == <<0x0000_0013::32, 0x0000_0004::32, 0::32, 0x0000_0007::32, 1, 2, 3>>
    end

    test "decode/1 of a header-only PDU recovers all header fields" do
      bin =
        PDU.build(command: :enquire_link_resp, status: :ESME_ROK, sequence_number: 99, body: <<>>)
        |> PDU.encode()
        |> IO.iodata_to_binary()

      assert {:ok, pdu} = PDU.decode(bin)
      assert pdu.command == :enquire_link_resp
      assert pdu.status == :ESME_ROK
      assert pdu.sequence_number == 99
      assert pdu.body == <<>>
    end

    test "decode/1 of a PUD with body bytes still recovers the header" do
      body = <<0xAA, 0xBB, 0xCC, 0xDD>>

      bin =
        PDU.build(command: :deliver_sm, status: :ESME_ROK, sequence_number: 12_345, body: body)
        |> PDU.encode()
        |> IO.iodata_to_binary()

      assert {:ok, pdu} = PDU.decode(bin)
      assert pdu.command == :deliver_sm
      assert pdu.sequence_number == 12_345
      assert pdu.body == body
    end

    test "header is big-endian (network byte order)" do
      # 16-byte header, no body. Wire integers chosen so the MSB/first byte of
      # each field is the high byte of the int — confirming big-endian layout.
      # 16 = 0x10, generic_nack = 0x8000_0000, ESME_RTHROTTLED = 0x58, seq = 0x7FFF_FFFF.
      bin = <<0x0000_0010::32, 0x8000_0000::32, 0x0000_0058::32, 0x7FFF_FFFF::32>>
      assert {:ok, pdu} = PDU.decode(bin)
      assert pdu.command == :generic_nack
      assert pdu.status == :ESME_RTHROTTLED
      assert pdu.sequence_number == 0x7FFF_FFFF
      assert pdu.body == <<>>
    end

    test "encode -> decode is the identity for arbitrary header fields" do
      for {command, seq} <- [
            {:bind_receiver, 1},
            {:bind_transmitter, 2},
            {:submit_sm, 1024},
            {:deliver_sm, 0xFFFF_FFFF |> Kernel.-(7)},
            {:enquire_link, 5},
            {:generic_nack, 0}
          ] do
        pdu = PDU.build(command: command, status: :ESME_ROK, sequence_number: seq, body: <<>>)
        bin = pdu |> PDU.encode() |> IO.iodata_to_binary()
        assert {:ok, %PDU{} = decoded} = PDU.decode(bin)
        assert decoded.command == command
        assert decoded.status == :ESME_ROK
        assert decoded.sequence_number == seq
        # header length in the wire equals 16 since body is <<>>
        assert <<length::32, _rest::binary>> = bin
        assert length == 16
      end
    end
  end
end
