defmodule JasminEx.Smpp.PDU.Constants do
  @moduledoc """
  SMPP 3.4 protocol constants as atom <-> integer mappings.

  Pure data; no side effects. Helpers return `{:ok, value}` for known
  mappings and `:error` (single atom) for unknown lookups, so callers can
  branch without having to remember which sentinel means "missing".

  Categories:
    * `command_id`     — SMPP operation names (request + response forms)
    * `command_status` — ESME_R* error codes returned in `command_status`
    * `ton`            — Type of Number
    * `npi`            — Numbering Plan Indicator
    * `data_coding`    — data_coding scheme byte
  """

  # ── SMPP 3.4 command_id table (request names + their _resp counterparts)
  # Response forms have the high bit (0x80000000) OR'd in.
  @command_ids %{
    generic_nack: 0x8000_0000,
    bind_receiver: 0x0000_0001,
    bind_receiver_resp: 0x8000_0001,
    bind_transmitter: 0x0000_0002,
    bind_transmitter_resp: 0x8000_0002,
    submit_sm: 0x0000_0004,
    submit_sm_resp: 0x8000_0004,
    deliver_sm: 0x0000_0005,
    deliver_sm_resp: 0x8000_0005,
    unbind: 0x0000_0006,
    unbind_resp: 0x8000_0006,
    bind_transceiver: 0x0000_0009,
    bind_transceiver_resp: 0x8000_0009,
    enquire_link: 0x0000_0015,
    enquire_link_resp: 0x8000_0015
  }

  @command_id_atoms Map.new(@command_ids, fn {atom, int} -> {int, atom} end)

  # ── command_status table (ESME_R*)
  # Only the canonical subset needed for in-scope commands. Reserved/vendor
  # ranges are intentionally absent so lookup returns :error for them.
  @command_status %{
    ESME_ROK: 0x0000_0000,
    ESME_RINVMSGLEN: 0x0000_0001,
    ESME_RINVCMDLEN: 0x0000_0002,
    ESME_RINVCMDID: 0x0000_0003,
    ESME_RINVBNDSTS: 0x0000_0004,
    ESME_RALYBND: 0x0000_0005,
    ESME_RSYSERR: 0x0000_0008,
    ESME_RINVSRCADR: 0x0000_000A,
    ESME_RINVDSTADR: 0x0000_000B,
    ESME_RINVMSGID: 0x0000_000C,
    ESME_RBINDFAIL: 0x0000_000D,
    ESME_RINVPASWD: 0x0000_000E,
    ESME_RINVSYSID: 0x0000_000F,
    ESME_RMSGQFUL: 0x0000_0014,
    ESME_RINVSERTYP: 0x0000_0015,
    ESME_RINVESMCLASS: 0x0000_0043,
    ESME_RSUBMITFAIL: 0x0000_0045,
    ESME_RINVSRCTON: 0x0000_0048,
    ESME_RINVSRCNPI: 0x0000_0049,
    ESME_RINVDSTTON: 0x0000_0050,
    ESME_RINVDSTNPI: 0x0000_0051,
    ESME_RINVSYSTYP: 0x0000_0053,
    ESME_RTHROTTLED: 0x0000_0058,
    ESME_RINVSCHED: 0x0000_0061,
    ESME_RINVEXPIRY: 0x0000_0062,
    ESME_RINVOPTPARSTREAM: 0x0000_00C0,
    ESME_ROPTPARNOTALLWD: 0x0000_00C1,
    ESME_RINVPARLEN: 0x0000_00C2,
    ESME_RMISSINGOPTPARAM: 0x0000_00C3,
    ESME_RINVOPTPARAMVAL: 0x0000_00C4,
    ESME_RUNKNOWNERR: 0x0000_00FF
  }

  @command_status_atoms Map.new(@command_status, fn {atom, int} -> {int, atom} end)

  # ── TON (Type of Number)
  @ton %{
    UNKNOWN: 0x00,
    INTERNATIONAL: 0x01,
    NATIONAL: 0x02,
    NETWORK_SPECIFIC: 0x03,
    SUBSCRIBER_NUMBER: 0x04,
    ALPHANUMERIC: 0x05,
    ABBREVIATED: 0x06
  }

  @ton_atoms Map.new(@ton, fn {atom, int} -> {int, atom} end)

  # ── NPI (Numbering Plan Indicator)
  @npi %{
    UNKNOWN: 0x00,
    ISDN: 0x01,
    DATA: 0x03,
    TELEX: 0x04,
    LAND_MOBILE: 0x06,
    NATIONAL: 0x08,
    PRIVATE: 0x09,
    ERMES: 0x0A,
    INTERNET: 0x0E,
    WAP_CLIENT_ID: 0x12
  }

  @npi_atoms Map.new(@npi, fn {atom, int} -> {int, atom} end)

  # ── data_coding
  @data_coding %{
    SMSC_DEFAULT_ALPHABET: 0x00,
    IA5_ASCII: 0x01,
    OCTET_UNSPECIFIED: 0x02,
    LATIN_1: 0x03,
    OCTET_UNSPECIFIED_COMMON: 0x04,
    JIS: 0x05,
    CYRILLIC: 0x06,
    ISO_8859_8: 0x07,
    UCS2: 0x08,
    PICTOGRAM: 0x09,
    ISO_2022_JP: 0x0A,
    EXTENDED_KANJI_JIS: 0x0D,
    KS_C_5601: 0x0E
  }

  @data_coding_atoms Map.new(@data_coding, fn {atom, int} -> {int, atom} end)

  # ── command_id ─────────────────────────────────────────────────────────────

  @spec command_id_to_int(atom()) :: {:ok, non_neg_integer()} | :error
  def command_id_to_int(atom), do: Map.fetch(@command_ids, atom)

  @spec command_id_to_atom(non_neg_integer()) :: {:ok, atom()} | :error
  def command_id_to_atom(int), do: Map.fetch(@command_id_atoms, int)

  @spec command_id_atoms() :: [atom()]
  def command_id_atoms, do: Map.keys(@command_ids)

  # ── command_status ─────────────────────────────────────────────────────────

  @spec command_status_to_int(atom()) :: {:ok, non_neg_integer()} | :error
  def command_status_to_int(atom), do: Map.fetch(@command_status, atom)

  @spec command_status_to_atom(non_neg_integer()) :: {:ok, atom()} | :error
  def command_status_to_atom(int), do: Map.fetch(@command_status_atoms, int)

  # ── TON ────────────────────────────────────────────────────────────────────

  @spec ton_to_int(atom()) :: {:ok, non_neg_integer()} | :error
  def ton_to_int(atom), do: Map.fetch(@ton, atom)

  @spec ton_to_atom(non_neg_integer()) :: {:ok, atom()} | :error
  def ton_to_atom(int), do: Map.fetch(@ton_atoms, int)

  @spec ton_atoms() :: [atom()]
  def ton_atoms, do: Map.keys(@ton)

  # ── NPI ────────────────────────────────────────────────────────────────────

  @spec npi_to_int(atom()) :: {:ok, non_neg_integer()} | :error
  def npi_to_int(atom), do: Map.fetch(@npi, atom)

  @spec npi_to_atom(non_neg_integer()) :: {:ok, atom()} | :error
  def npi_to_atom(int), do: Map.fetch(@npi_atoms, int)

  @spec npi_atoms() :: [atom()]
  def npi_atoms, do: Map.keys(@npi)

  # ── data_coding ────────────────────────────────────────────────────────────

  @spec data_coding_to_int(atom()) :: {:ok, non_neg_integer()} | :error
  def data_coding_to_int(atom), do: Map.fetch(@data_coding, atom)

  @spec data_coding_to_atom(non_neg_integer()) :: {:ok, atom()} | :error
  def data_coding_to_atom(int), do: Map.fetch(@data_coding_atoms, int)

  @spec data_coding_atoms() :: [atom()]
  def data_coding_atoms, do: Map.keys(@data_coding)

  # ── Helpers used elsewhere in the codec ────────────────────────────────────

  @doc """
  Returns the request-form command_id atom for a given response-form atom.

  e.g. `resp_to_request(:submit_sm_resp) == {:ok, :submit_sm}`.
  Useful for matching an inbound response against an in-flight submit_sm.
  """
  @spec resp_to_request(atom()) :: {:ok, atom()} | :error
  def resp_to_request(resp) when is_atom(resp) do
    str = Atom.to_string(resp)

    case String.split(str, "_resp") do
      [base] when base != "" ->
        base_atom = String.to_atom(base)

        if Map.has_key?(@command_ids, base_atom) do
          {:ok, base_atom}
        else
          :error
        end

      _ ->
        :error
    end
  end

  def resp_to_request(_), do: :error
end
