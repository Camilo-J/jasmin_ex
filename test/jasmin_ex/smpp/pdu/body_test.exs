defmodule JasminEx.Smpp.PDU.BodyTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.Smpp.PDU.Constants

  # ------------------------------------------------------------------
  # encode -> decode round-trip for every in-scope command
  # ------------------------------------------------------------------
  describe "bind_transmitter round-trip" do
    test "encodes then decodes to the same struct" do
      body =
        %Body.Bind{
          system_id: "jCli",
          password: "pwd",
          system_type: "im",
          interface_version: 0x34,
          addr_ton: :UNKNOWN,
          addr_npi: :UNKNOWN,
          address_range: ""
        }

      {:ok, bin} = Body.encode(:bind_transmitter, body)
      assert {:ok, decoded} = Body.decode(:bind_transmitter, bin)
      assert decoded == body
    end

    test "bind_transmitter wire layout matches SMPP spec (all fields null-terminated + 3 u8s)" do
      body = %Body.Bind{
        system_id: "s",
        password: "p",
        system_type: "t",
        interface_version: 0x34,
        addr_ton: :UNKNOWN,
        addr_npi: :ISDN,
        address_range: "r"
      }

      {:ok, bin} = Body.encode(:bind_transmitter, body)

      # "s\0" "p\0" "t\0" +3 u8 + "r\0"
      assert bin == <<"s", 0, "p", 0, "t", 0, 0x34, 0x00, 0x01, "r", 0>>
    end
  end

  describe "bind_receiver round-trip" do
    test "encodes and decodes the same bind_receiver body" do
      body = %Body.Bind{
        system_id: "rcv",
        password: "r",
        system_type: "ty",
        interface_version: 0x34,
        addr_ton: :INTERNATIONAL,
        addr_npi: :ISDN,
        address_range: "+1"
      }

      {:ok, bin} = Body.encode(:bind_receiver, body)
      assert {:ok, decoded} = Body.decode(:bind_receiver, bin)
      assert decoded == body
    end
  end

  describe "bind_transceiver round-trip" do
    test "encodes and decodes the same bind_transceiver body" do
      body = %Body.Bind{
        system_id: "trx",
        password: "p",
        system_type: "st",
        interface_version: 0x34,
        addr_ton: :NATIONAL,
        addr_npi: :ISDN,
        address_range: ""
      }

      {:ok, bin} = Body.encode(:bind_transceiver, body)
      assert {:ok, decoded} = Body.decode(:bind_transceiver, bin)
      assert decoded == body
    end
  end

  describe "bind_*_resp round-trip" do
    test "bind_receiver_resp: body is system_id only" do
      body = %Body.BindResp{system_id: "smsc"}
      bin = Body.encode(:bind_receiver_resp, body) |> elem(1) |> IO.iodata_to_binary()
      assert bin == <<"smsc", 0>>
      assert {:ok, decoded} = Body.decode(:bind_receiver_resp, bin)
      assert decoded.system_id == "smsc"
    end

    test "bind_transmitter_resp: body is system_id only" do
      body = %Body.BindResp{system_id: "smsc-tx"}
      bin = Body.encode(:bind_transmitter_resp, body) |> elem(1) |> IO.iodata_to_binary()
      assert {:ok, decoded} = Body.decode(:bind_transmitter_resp, bin)
      assert decoded.system_id == "smsc-tx"
    end

    test "bind_transceiver_resp: empty system_id is just the terminator" do
      body = %Body.BindResp{system_id: ""}
      bin = Body.encode(:bind_transceiver_resp, body) |> elem(1) |> IO.iodata_to_binary()
      assert bin == <<0>>
      assert {:ok, decoded} = Body.decode(:bind_transceiver_resp, bin)
      assert decoded.system_id == ""
    end
  end

  describe "unbind round-trip" do
    test "client unbind body is empty" do
      {:ok, bin} = Body.encode(:unbind, %Body.Unbind{})
      assert bin == <<>>
      assert {:ok, decoded} = Body.decode(:unbind, bin)
      assert decoded == %Body.Unbind{}
    end

    test "unbind_resp body is empty" do
      {:ok, bin} = Body.encode(:unbind_resp, %Body.Unbind{})
      assert bin == <<>>
      assert {:ok, _} = Body.decode(:unbind_resp, bin)
    end
  end

  describe "enquire_link round-trip" do
    test "client enquire_link body is empty" do
      assert {:ok, <<>>} = Body.encode(:enquire_link, %Body.EnquireLink{})
      assert {:ok, decoded} = Body.decode(:enquire_link, <<>>)
      assert decoded == %Body.EnquireLink{}
    end

    test "enquire_link_resp body is empty" do
      assert {:ok, <<>>} = Body.encode(:enquire_link_resp, %Body.EnquireLink{})
      assert {:ok, decoded} = Body.decode(:enquire_link_resp, <<>>)
      assert decoded == %Body.EnquireLink{}
    end
  end

  describe "submit_sm round-trip" do
    test "encodes a small ASCII submit_sm and round-trips it" do
      body = %Body.SubmitSM{
        service_type: "",
        source_addr_ton: :UNKNOWN,
        source_addr_npi: :UNKNOWN,
        source_addr: "src",
        dest_addr_ton: :INTERNATIONAL,
        dest_addr_npi: :ISDN,
        destination_addr: "+1234567890",
        esm_class: 0,
        protocol_id: 0,
        priority_flag: 0,
        schedule_delivery_time: "",
        validity_period: "",
        registered_delivery: 0,
        replace_if_present_flag: 0,
        data_coding: :SMSC_DEFAULT_ALPHABET,
        sm_default_msg_id: 0,
        short_message: "Hello"
      }

      {:ok, iodata} = Body.encode(:submit_sm, body)
      bin = IO.iodata_to_binary(iodata)
      {:ok, decoded} = Body.decode(:submit_sm, bin)

      assert decoded.__struct__ == Body.SubmitSM
      assert decoded == body
    end

    test "submit_sm wire layout matches SMPP spec (16 fields + sm_length + sm bytes)" do
      body = %Body.SubmitSM{
        service_type: "",
        source_addr_ton: :UNKNOWN,
        source_addr_npi: :UNKNOWN,
        source_addr: "A",
        dest_addr_ton: :INTERNATIONAL,
        dest_addr_npi: :ISDN,
        destination_addr: "B",
        esm_class: 0,
        protocol_id: 0,
        priority_flag: 0,
        schedule_delivery_time: "",
        validity_period: "",
        registered_delivery: 0,
        replace_if_present_flag: 0,
        data_coding: :SMSC_DEFAULT_ALPHABET,
        sm_default_msg_id: 0,
        short_message: "Hi"
      }

      {:ok, iodata} = Body.encode(:submit_sm, body)
      bin = IO.iodata_to_binary(iodata)

      # service_type ("\0")
      # source_addr_ton u8, source_addr_npi u8, source_addr c-octet ("A\0")
      # dest_addr_ton u8, dest_addr_npi u8, destination_addr c-octet ("B\0")
      # esm_class u8, protocol_id u8, priority_flag u8
      # schedule_delivery_time c-octet ("\0"), validity_period c-octet ("\0")
      # registered_delivery u8, replace_if_present_flag u8
      # data_coding u8, sm_default_msg_id u8, sm_length u8, short_message bytes
      expected =
        <<0>> <>
          <<0x00, 0x00, "A", 0>> <>
          <<0x01, 0x01, "B", 0>> <>
          <<0, 0, 0>> <>
          <<0, 0>> <>
          <<0, 0>> <>
          <<0x00, 0, 2, "Hi">>

      assert bin == expected
      assert {:ok, %Body.SubmitSM{} = d} = Body.decode(:submit_sm, bin)
      assert d.short_message == "Hi"
    end

    test "submit_sm_resp body is message_id only" do
      body = %Body.SubmitSMResp{message_id: "abc123"}
      bin = Body.encode(:submit_sm_resp, body) |> elem(1) |> IO.iodata_to_binary()
      assert bin == <<"abc123", 0>>
      assert {:ok, decoded} = Body.decode(:submit_sm_resp, bin)
      assert decoded.message_id == "abc123"
    end
  end

  describe "deliver_sm round-trip" do
    test "encodes and decodes a deliver_sm body" do
      body = %Body.DeliverSM{
        service_type: "",
        source_addr_ton: :INTERNATIONAL,
        source_addr_npi: :ISDN,
        source_addr: "+447700900123",
        dest_addr_ton: :UNKNOWN,
        dest_addr_npi: :UNKNOWN,
        destination_addr: "user",
        esm_class: 0,
        protocol_id: 0,
        priority_flag: 0,
        schedule_delivery_time: "",
        validity_period: "",
        registered_delivery: 0,
        replace_if_present_flag: 0,
        data_coding: :SMSC_DEFAULT_ALPHABET,
        sm_default_msg_id: 0,
        short_message: "Inbound"
      }

      {:ok, iodata} = Body.encode(:deliver_sm, body)
      bin = IO.iodata_to_binary(iodata)
      {:ok, decoded} = Body.decode(:deliver_sm, bin)

      # Critical: deliver_sm round-trip must produce %Body.DeliverSM{}, not %Body.SubmitSM{}
      # Spec scenario "decoded struct equals the original struct" is only satisfied when
      # BOTH the field values AND the struct type match the original.
      assert decoded.__struct__ == Body.DeliverSM
      assert decoded == body
    end

    test "deliver_sm_resp body is message_id only" do
      body = %Body.DeliverSMResp{message_id: "id-1"}
      bin = Body.encode(:deliver_sm_resp, body) |> elem(1) |> IO.iodata_to_binary()
      assert bin == <<"id-1", 0>>
      assert {:ok, decoded} = Body.decode(:deliver_sm_resp, bin)
      assert decoded.message_id == "id-1"
    end
  end

  describe "generic_nack round-trip" do
    test "body is empty and decodes to %GenericNack{}" do
      assert {:ok, <<>>} = Body.encode(:generic_nack, %Body.GenericNack{})
      assert {:ok, decoded} = Body.decode(:generic_nack, <<>>)
      assert decoded == %Body.GenericNack{}
    end
  end

  # ------------------------------------------------------------------
  # error paths
  # ------------------------------------------------------------------
  describe "invalid body decoding" do
    test "bind body missing null terminator returns {:error, :bad_cstring}" do
      # body bytes that never have a terminating null
      incomplete = <<"s", "p", "t", 0x34, 0x00, 0x01, "r">>
      assert Body.decode(:bind_transmitter, incomplete) == {:error, {:decode, :bad_cstring}}
    end

    test "submit_sm body truncated before sm_length returns {:error, :truncated}" do
      # enough for the 16 mandatory fields' headers but missing sm_length itself
      insufficient = <<0, 0, 0, "A", 0, 0, 0, "B", 0, 0, 0, 0, 0, 0, 0, 0, 0>>
      assert Body.decode(:submit_sm, insufficient) == {:error, {:decode, :truncated}}
    end

    test "submit_sm body truncated within short_message returns {:error, :truncated}" do
      # Full 18 mandatory-field bytes, then sm_length=5 but only 3 short_message bytes follow
      truncated = <<0, 0, 0, "A", 0, 0, 0, "B", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, "abc">>
      assert Body.decode(:submit_sm, truncated) == {:error, {:decode, :truncated}}
    end
  end

  describe "atom helpers for body types" do
    test "Body struct namespaces are accessible" do
      assert %Body.Bind{system_id: nil} = %Body.Bind{}
      assert %Body.SubmitSM{} = %Body.SubmitSM{}
      assert %Body.DeliverSM{} = %Body.DeliverSM{}
      assert %Body.BindResp{} = %Body.BindResp{}
      assert %Body.SubmitSMResp{} = %Body.SubmitSMResp{}
      assert %Body.DeliverSMResp{} = %Body.DeliverSMResp{}
      assert %Body.Unbind{} = %Body.Unbind{}
      assert %Body.EnquireLink{} = %Body.EnquireLink{}
      assert %Body.GenericNack{} = %Body.GenericNack{}
      assert Constants.command_id_to_int(:bind_transmitter) == {:ok, 0x0000_0002}
    end
  end
end
