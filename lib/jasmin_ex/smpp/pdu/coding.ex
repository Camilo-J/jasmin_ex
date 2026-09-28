defmodule JasminEx.Smpp.PDU.Coding do
  @moduledoc """
  Short-message encoding/decoding per SMPP `data_coding` byte.

  `encode_short_message/2` and `decode_short_message/2` return `{:ok, binary}`
  or `:error`. They never raise on malformed input.

  Supported schemes:

    * `0x00` SMSC_DEFAULT_ALPHABET — GSM 03.38 unpacked default alphabet
      plus the extension escape table (`0x1B` + extension octet). Packed
      septets are not produced or consumed.
    * `0x01` IA5_ASCII — strict 7-bit ASCII. Bytes above `0x7F` are rejected.
    * `0x02` OCTET_UNSPECIFIED — raw 8-bit bytes; no transformation.
    * `0x03` LATIN_1 — ISO-8859-1 via Erlang `:unicode`.
    * `0x04..0x07`, `0x09`, `0x0A`, `0x0D`, `0x0E` — raw 8-bit bytes
      (python-smpp/jasmin octet convention).
    * `0x08` UCS2 — strict UTF-16BE, including valid supplementary-plane
      (surrogate-pair) round-trips. Invalid UTF-8, odd-length UTF-16, and
      unpaired surrogates return `:error` instead of dropping bytes.

  Unknown `data_coding` values return `:error` so the body decoder can skip
  an unsupported PDU instead of dying.

  This module is data_coding-only — it does NOT touch the surrounding PDU
  shape. Body encoding consumes already-encoded wire bytes and must not
  call these helpers a second time.
  """

  # GSM 03.38 default alphabet: Unicode codepoint -> unpacked octet.
  # 0x1B is the extension escape and is not a standalone character.
  @gsm_default [
    {?@, 0x00},
    {?£, 0x01},
    {?$, 0x02},
    {?¥, 0x03},
    {?è, 0x04},
    {?é, 0x05},
    {?ù, 0x06},
    {?ì, 0x07},
    {?ò, 0x08},
    {?Ç, 0x09},
    {?\n, 0x0A},
    {?Ø, 0x0B},
    {?ø, 0x0C},
    {?\r, 0x0D},
    {?Å, 0x0E},
    {?å, 0x0F},
    {?Δ, 0x10},
    {?_, 0x11},
    {?Φ, 0x12},
    {?Γ, 0x13},
    {?Λ, 0x14},
    {?Ω, 0x15},
    {?Π, 0x16},
    {?Ψ, 0x17},
    {?Σ, 0x18},
    {?Θ, 0x19},
    {?Ξ, 0x1A},
    {?Æ, 0x1C},
    {?æ, 0x1D},
    {?ß, 0x1E},
    {?É, 0x1F},
    {?\s, 0x20},
    {?!, 0x21},
    {?", 0x22},
    {?#, 0x23},
    {?¤, 0x24},
    {?%, 0x25},
    {?&, 0x26},
    {?', 0x27},
    {?(, 0x28},
    {?), 0x29},
    {?*, 0x2A},
    {?+, 0x2B},
    {?,, 0x2C},
    {?-, 0x2D},
    {?., 0x2E},
    {?/, 0x2F},
    {?0, 0x30},
    {?1, 0x31},
    {?2, 0x32},
    {?3, 0x33},
    {?4, 0x34},
    {?5, 0x35},
    {?6, 0x36},
    {?7, 0x37},
    {?8, 0x38},
    {?9, 0x39},
    {?:, 0x3A},
    {?;, 0x3B},
    {?<, 0x3C},
    {?=, 0x3D},
    {?>, 0x3E},
    {??, 0x3F},
    {?¡, 0x40},
    {?A, 0x41},
    {?B, 0x42},
    {?C, 0x43},
    {?D, 0x44},
    {?E, 0x45},
    {?F, 0x46},
    {?G, 0x47},
    {?H, 0x48},
    {?I, 0x49},
    {?J, 0x4A},
    {?K, 0x4B},
    {?L, 0x4C},
    {?M, 0x4D},
    {?N, 0x4E},
    {?O, 0x4F},
    {?P, 0x50},
    {?Q, 0x51},
    {?R, 0x52},
    {?S, 0x53},
    {?T, 0x54},
    {?U, 0x55},
    {?V, 0x56},
    {?W, 0x57},
    {?X, 0x58},
    {?Y, 0x59},
    {?Z, 0x5A},
    {?Ä, 0x5B},
    {?Ö, 0x5C},
    {?Ñ, 0x5D},
    {?Ü, 0x5E},
    {?§, 0x5F},
    {?¿, 0x60},
    {?a, 0x61},
    {?b, 0x62},
    {?c, 0x63},
    {?d, 0x64},
    {?e, 0x65},
    {?f, 0x66},
    {?g, 0x67},
    {?h, 0x68},
    {?i, 0x69},
    {?j, 0x6A},
    {?k, 0x6B},
    {?l, 0x6C},
    {?m, 0x6D},
    {?n, 0x6E},
    {?o, 0x6F},
    {?p, 0x70},
    {?q, 0x71},
    {?r, 0x72},
    {?s, 0x73},
    {?t, 0x74},
    {?u, 0x75},
    {?v, 0x76},
    {?w, 0x77},
    {?x, 0x78},
    {?y, 0x79},
    {?z, 0x7A},
    {?ä, 0x7B},
    {?ö, 0x7C},
    {?ñ, 0x7D},
    {?ü, 0x7E},
    {?à, 0x7F}
  ]

  @gsm_extension [
    {?\f, 0x0A},
    {?^, 0x14},
    {?{, 0x28},
    {?}, 0x29},
    {?\\, 0x2F},
    {?[, 0x3C},
    {?~, 0x3D},
    {?], 0x3E},
    {?|, 0x40},
    {?€, 0x65}
  ]

  @gsm_default_encode Map.new(@gsm_default)
  @gsm_default_decode Map.new(@gsm_default, fn {codepoint, byte} -> {byte, codepoint} end)
  @gsm_extension_encode Map.new(@gsm_extension)
  @gsm_extension_decode Map.new(@gsm_extension, fn {codepoint, byte} -> {byte, codepoint} end)

  @spec encode_short_message(non_neg_integer(), String.t()) ::
          {:ok, binary()} | :error
  def encode_short_message(0x00, str), do: encode_gsm(str)
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
  def decode_short_message(0x00, bin), do: decode_gsm(bin)
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

  defp encode_gsm(str) when is_binary(str) do
    if String.valid?(str) do
      encode_gsm_codepoints(String.to_charlist(str), [])
    else
      :error
    end
  end

  defp encode_gsm(_str), do: :error

  defp encode_gsm_codepoints([], acc), do: {:ok, IO.iodata_to_binary(Enum.reverse(acc))}

  defp encode_gsm_codepoints([codepoint | rest], acc) do
    cond do
      byte = Map.get(@gsm_default_encode, codepoint) ->
        encode_gsm_codepoints(rest, [byte | acc])

      byte = Map.get(@gsm_extension_encode, codepoint) ->
        encode_gsm_codepoints(rest, [<<0x1B, byte>> | acc])

      true ->
        :error
    end
  end

  defp encode_ascii(str) when is_binary(str) do
    if String.valid?(str) and ascii_bytes?(str) do
      {:ok, str}
    else
      :error
    end
  end

  defp encode_ascii(_str), do: :error

  defp encode_octet(str) when is_binary(str), do: {:ok, str}
  defp encode_octet(_str), do: :error

  defp encode_latin1(str) when is_binary(str) do
    # Erlang either raises ArgumentError or returns an {:error, _, _} tuple
    # for codepoints that don't fit in latin1.
    case :unicode.characters_to_binary(str, :utf8, :latin1) do
      bin when is_binary(bin) -> {:ok, bin}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp encode_latin1(_str), do: :error

  defp encode_ucs2(str) when is_binary(str) do
    case :unicode.characters_to_binary(str, :utf8, {:utf16, :big}) do
      bin when is_binary(bin) -> {:ok, bin}
      _ -> :error
    end
  end

  defp encode_ucs2(_str), do: :error

  # ── decoders ──────────────────────────────────────────────────────────────

  defp decode_gsm(bin) when is_binary(bin), do: decode_gsm_bytes(bin, [])
  defp decode_gsm(_bin), do: :error

  defp decode_gsm_bytes(<<>>, acc), do: {:ok, List.to_string(Enum.reverse(acc))}
  defp decode_gsm_bytes(<<0x1B>>, _acc), do: :error

  defp decode_gsm_bytes(<<0x1B, ext, rest::binary>>, acc) do
    case Map.fetch(@gsm_extension_decode, ext) do
      {:ok, codepoint} -> decode_gsm_bytes(rest, [codepoint | acc])
      :error -> :error
    end
  end

  defp decode_gsm_bytes(<<byte, rest::binary>>, acc) when byte <= 0x7F do
    case Map.fetch(@gsm_default_decode, byte) do
      {:ok, codepoint} -> decode_gsm_bytes(rest, [codepoint | acc])
      :error -> :error
    end
  end

  defp decode_gsm_bytes(_other, _acc), do: :error

  defp decode_ascii(bin) when is_binary(bin) do
    if ascii_bytes?(bin), do: {:ok, bin}, else: :error
  end

  defp decode_ascii(_bin), do: :error

  defp decode_latin1(bin) when is_binary(bin) do
    # No target encoding → Erlang emits UTF-8 (the BEAM-native form).
    case :unicode.characters_to_binary(bin, :latin1) do
      utf8 when is_binary(utf8) -> {:ok, utf8}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp decode_latin1(_bin), do: :error

  defp decode_ucs2(bin) when is_binary(bin) and rem(byte_size(bin), 2) == 0 do
    case :unicode.characters_to_binary(bin, {:utf16, :big}, :utf8) do
      utf8 when is_binary(utf8) -> {:ok, utf8}
      _ -> :error
    end
  end

  defp decode_ucs2(_other), do: :error

  # ── helpers ────────────────────────────────────────────────────────────────

  defp ascii_bytes?(<<>>), do: true
  defp ascii_bytes?(<<byte, rest::binary>>) when byte <= 0x7F, do: ascii_bytes?(rest)
  defp ascii_bytes?(_bin), do: false
end
