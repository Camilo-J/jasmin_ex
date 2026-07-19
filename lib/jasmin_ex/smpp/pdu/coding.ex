defmodule JasminEx.Smpp.PDU.Coding do
  @moduledoc """
  Short-message encoding/decoding per SMPP `data_coding` byte.

  Supports the schemes PR1 cares about:

    * `0x00` SMSC_DEFAULT_ALPHABET — GSM 03.38 (ASCII passthrough for the
      core Latin chars we expose; 0x1B-extension and 7-bit packed
      septet forms are deliberately out of PR1 scope).
    * `0x01` IA5_ASCII             — same as 0x00; raw ASCII bytes.
    * `0x02` OCTET_UNSPECIFIED     — raw 8-bit bytes; no transformation.
    * `0x03` LATIN_1               — ISO-8859-1 (via Erlang's :unicode).
    * `0x04..0x07` → 0x0E          — raw 8-bit bytes (treated the same
      as OCTET_UNSPECIFIED by the python-smpp/jasmin convention).
    * `0x08` UCS2                  — UTF-16BE.

  The out-of-PR1 schemes (UDH-bearing concat_payload, JIS variants) are
  not handled yet. We return `:error` rather than crash so the body
  decoder can skip an unsupported PDU instead of dying.

  This module is data_coding-only — it does NOT touch the surrounding
  PDU shape. The body layer mixes it with `sm_length`, addresses, etc.
  """

  @spec encode_short_message(non_neg_integer(), String.t()) ::
          {:ok, binary()} | :error
  def encode_short_message(0x00, str), do: encode_ascii(str)
  def encode_short_message(0x01, str), do: encode_ascii(str)
  def encode_short_message(0x02, str), do: encode_octet(str)
  def encode_short_message(0x03, str), do: encode_latin1(str)
  def encode_short_message(0x04, str), do: encode_octet(str)
  def encode_short_message(0x05, str), do: encode_octet(str)
  def encode_short_message(0x06, str), do: encode_octet(str)
  def encode_short_message(0x07, str), do: encode_octet(str)
  def encode_short_message(0x08, str), do: encode_ucs2(str)
  def encode_short_message(0x09, str), do: encode_octet(str)
  def encode_short_message(0x0A, str), do: encode_octet(str)
  def encode_short_message(0x0D, str), do: encode_octet(str)
  def encode_short_message(0x0E, str), do: encode_octet(str)
  def encode_short_message(_other, _str), do: :error

  @spec decode_short_message(non_neg_integer(), binary()) ::
          {:ok, String.t()} | :error
  def decode_short_message(0x00, bin), do: decode_ascii(bin)
  def decode_short_message(0x01, bin), do: decode_ascii(bin)
  def decode_short_message(0x02, bin), do: {:ok, bin}
  def decode_short_message(0x03, bin), do: decode_latin1(bin)
  def decode_short_message(0x04, bin), do: {:ok, bin}
  def decode_short_message(0x05, bin), do: {:ok, bin}
  def decode_short_message(0x06, bin), do: {:ok, bin}
  def decode_short_message(0x07, bin), do: {:ok, bin}
  def decode_short_message(0x08, bin), do: decode_ucs2(bin)
  def decode_short_message(0x09, bin), do: {:ok, bin}
  def decode_short_message(0x0A, bin), do: {:ok, bin}
  def decode_short_message(0x0D, bin), do: {:ok, bin}
  def decode_short_message(0x0E, bin), do: {:ok, bin}
  def decode_short_message(_other, _bin), do: :error

  # ── encoders ──────────────────────────────────────────────────────────────

  defp encode_ascii(str) do
    if ascii_safe?(str) do
      {:ok, str}
    else
      :error
    end
  end

  defp encode_octet(str), do: {:ok, str}

  defp encode_latin1(str) do
    # Erlang either raises ArgumentError or returns an {:error, _, _} tuple
    # for codepoints that don't fit in latin1.
    case :unicode.characters_to_binary(str, :utf8, :latin1) do
      bin when is_binary(bin) -> {:ok, bin}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp encode_ucs2(str) do
    bytes = for <<codepoint::utf8 <- str>>, into: <<>>, do: <<codepoint::utf16-big>>
    {:ok, IO.iodata_to_binary(bytes)}
  end

  # ── decoders ──────────────────────────────────────────────────────────────

  defp decode_ascii(bin), do: {:ok, bin}

  defp decode_latin1(bin) do
    # No target encoding → Erlang emits UTF-8 (the BEAM-native form).
    {:ok, :unicode.characters_to_binary(bin, :latin1)}
  rescue
    ArgumentError -> :error
  end

  defp decode_ucs2(bin) when rem(byte_size(bin), 2) == 0 do
    pairs = for <<codepoint::utf16-big <- bin>>, into: "", do: <<codepoint::utf8>>
    {:ok, pairs}
  end

  defp decode_ucs2(_other), do: :error

  # ── helpers ────────────────────────────────────────────────────────────────

  defp ascii_safe?(str) do
    String.to_charlist(str)
    |> Enum.all?(&(&1 <= 0x7F))
  end
end
