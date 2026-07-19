defmodule JasminEx.Smpp.PDU.ConstantsTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.PDU.Constants, as: C

  describe "command_id atom <-> integer round-trip" do
    test "encodes request atoms to their SMPP wire integer" do
      assert C.command_id_to_int(:generic_nack) == {:ok, 0x8000_0000}
      assert C.command_id_to_int(:bind_receiver) == {:ok, 0x0000_0001}
      assert C.command_id_to_int(:bind_transmitter) == {:ok, 0x0000_0002}
      assert C.command_id_to_int(:submit_sm) == {:ok, 0x0000_0004}
      assert C.command_id_to_int(:deliver_sm) == {:ok, 0x0000_0005}
      assert C.command_id_to_int(:unbind) == {:ok, 0x0000_0006}
      assert C.command_id_to_int(:bind_transceiver) == {:ok, 0x0000_0009}
      assert C.command_id_to_int(:enquire_link) == {:ok, 0x0000_0015}
    end

    test "encodes response atoms (msb set) to their SMPP wire integer" do
      assert C.command_id_to_int(:bind_receiver_resp) == {:ok, 0x8000_0001}
      assert C.command_id_to_int(:bind_transmitter_resp) == {:ok, 0x8000_0002}
      assert C.command_id_to_int(:submit_sm_resp) == {:ok, 0x8000_0004}
      assert C.command_id_to_int(:deliver_sm_resp) == {:ok, 0x8000_0005}
      assert C.command_id_to_int(:unbind_resp) == {:ok, 0x8000_0006}
      assert C.command_id_to_int(:bind_transceiver_resp) == {:ok, 0x8000_0009}
      assert C.command_id_to_int(:enquire_link_resp) == {:ok, 0x8000_0015}
    end

    test "decodes wire integer back to atom" do
      assert C.command_id_to_atom(0x0000_0002) == {:ok, :bind_transmitter}
      assert C.command_id_to_atom(0x8000_0004) == {:ok, :submit_sm_resp}
      assert C.command_id_to_atom(0x0000_0006) == {:ok, :unbind}
    end

    test "round-trip every known command_id" do
      for atom <- C.command_id_atoms() do
        assert {:ok, int} = C.command_id_to_int(atom)
        assert {:ok, ^atom} = C.command_id_to_atom(int)
      end
    end

    test "unknown atom returns :error, unknown int returns :error" do
      assert C.command_id_to_int(:not_a_command) == :error
      assert C.command_id_to_atom(0xDEAD_BEEF) == :error
    end
  end

  describe "command_status atom <-> integer round-trip" do
    test "encodes canonical ESME_* atoms" do
      assert C.command_status_to_int(:ESME_ROK) == {:ok, 0x0000_0000}
      assert C.command_status_to_int(:ESME_RINVMSGLEN) == {:ok, 0x0000_0001}
      assert C.command_status_to_int(:ESME_RINVCMDID) == {:ok, 0x0000_0003}
      assert C.command_status_to_int(:ESME_RSYSERR) == {:ok, 0x0000_0008}
      assert C.command_status_to_int(:ESME_RTHROTTLED) == {:ok, 0x0000_0058}
      assert C.command_status_to_int(:ESME_RUNKNOWNERR) == {:ok, 0x0000_00FF}
    end

    test "decodes known status integer back to atom" do
      assert C.command_status_to_atom(0) == {:ok, :ESME_ROK}
      assert C.command_status_to_atom(0x0000_0058) == {:ok, :ESME_RTHROTTLED}
    end

    test "unknown status returns :error" do
      assert C.command_status_to_int(:nope) == :error
      # 0x9FFF is a reserved-for-vendor range; not in our map
      assert C.command_status_to_atom(0x0000_0099) == :error
    end
  end

  describe "TON (Type of Number) atom <-> integer round-trip" do
    test "round-trip every known TON atom" do
      for atom <- C.ton_atoms() do
        {:ok, int} = C.ton_to_int(atom)
        assert C.ton_to_atom(int) == {:ok, atom}
      end
    end

    test "encodes known TON values" do
      assert C.ton_to_int(:UNKNOWN) == {:ok, 0x00}
      assert C.ton_to_int(:INTERNATIONAL) == {:ok, 0x01}
      assert C.ton_to_int(:NATIONAL) == {:ok, 0x02}
      assert C.ton_to_int(:NETWORK_SPECIFIC) == {:ok, 0x03}
      assert C.ton_to_int(:SUBSCRIBER_NUMBER) == {:ok, 0x04}
      assert C.ton_to_int(:ALPHANUMERIC) == {:ok, 0x05}
      assert C.ton_to_int(:ABBREVIATED) == {:ok, 0x06}
    end

    test "unknown TON returns :error" do
      assert C.ton_to_int(:nope) == :error
      assert C.ton_to_atom(0xFF) == :error
    end
  end

  describe "NPI (Numbering Plan Indicator) atom <-> integer round-trip" do
    test "round-trip every known NPI atom" do
      for atom <- C.npi_atoms() do
        {:ok, int} = C.npi_to_int(atom)
        assert C.npi_to_atom(int) == {:ok, atom}
      end
    end

    test "encodes known NPI values" do
      assert C.npi_to_int(:UNKNOWN) == {:ok, 0x00}
      assert C.npi_to_int(:ISDN) == {:ok, 0x01}
      assert C.npi_to_int(:DATA) == {:ok, 0x03}
      assert C.npi_to_int(:TELEX) == {:ok, 0x04}
      assert C.npi_to_int(:LAND_MOBILE) == {:ok, 0x06}
      assert C.npi_to_int(:NATIONAL) == {:ok, 0x08}
      assert C.npi_to_int(:PRIVATE) == {:ok, 0x09}
      assert C.npi_to_int(:ERMES) == {:ok, 0x0A}
    end

    test "unknown NPI returns :error" do
      assert C.npi_to_int(:nope) == :error
      assert C.npi_to_atom(0xFE) == :error
    end
  end

  describe "data_coding atom <-> integer round-trip" do
    test "round-trip every known data_coding atom" do
      for atom <- C.data_coding_atoms() do
        {:ok, int} = C.data_coding_to_int(atom)
        assert C.data_coding_to_atom(int) == {:ok, atom}
      end
    end

    test "encodes known data_coding values" do
      assert C.data_coding_to_int(:SMSC_DEFAULT_ALPHABET) == {:ok, 0x00}
      assert C.data_coding_to_int(:IA5_ASCII) == {:ok, 0x01}
      assert C.data_coding_to_int(:OCTET_UNSPECIFIED) == {:ok, 0x02}
      assert C.data_coding_to_int(:LATIN_1) == {:ok, 0x03}
      assert C.data_coding_to_int(:UCS2) == {:ok, 0x08}
    end

    test "unknown data_coding returns :error" do
      assert C.data_coding_to_int(:nope) == :error
      assert C.data_coding_to_atom(0xFF) == :error
    end
  end
end
