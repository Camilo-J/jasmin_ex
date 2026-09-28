defmodule JasminEx.Smpp.PDU.CodingTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.PDU.Coding

  describe "GSM 03.38 default alphabet (data_coding 0x00)" do
    test "encodes GSM default-alphabet characters to unpacked octets" do
      assert {:ok, <<0x48, 0x65, 0x6C, 0x6C, 0x6F>>} = Coding.encode_short_message(0x00, "Hello")
      # '@' is 0x00, '£' is 0x01, '$' is 0x02 — not ASCII byte values.
      assert {:ok, <<0x00, 0x01, 0x02>>} = Coding.encode_short_message(0x00, "@£$")
    end

    test "encodes é to GSM default alphabet byte 0x05" do
      assert {:ok, <<0x05>>} = Coding.encode_short_message(0x00, "é")
    end

    test "encodes extension-table characters with ESC 0x1B" do
      assert {:ok, <<0x1B, 0x65>>} = Coding.encode_short_message(0x00, "€")
      assert {:ok, <<0x1B, 0x3C, 0x1B, 0x3E>>} = Coding.encode_short_message(0x00, "[]")
      assert {:ok, <<0x1B, 0x28, 0x1B, 0x29>>} = Coding.encode_short_message(0x00, "{}")
      assert {:ok, <<0x1B, 0x2F>>} = Coding.encode_short_message(0x00, "\\")
      assert {:ok, <<0x1B, 0x14>>} = Coding.encode_short_message(0x00, "^")
      assert {:ok, <<0x1B, 0x3D>>} = Coding.encode_short_message(0x00, "~")
      assert {:ok, <<0x1B, 0x40>>} = Coding.encode_short_message(0x00, "|")
    end

    test "decodes unpacked GSM octets back to Unicode text" do
      assert {:ok, "Hello"} = Coding.decode_short_message(0x00, <<0x48, 0x65, 0x6C, 0x6C, 0x6F>>)
      assert {:ok, "@£$"} = Coding.decode_short_message(0x00, <<0x00, 0x01, 0x02>>)
      assert {:ok, "é"} = Coding.decode_short_message(0x00, <<0x05>>)
      assert {:ok, "€"} = Coding.decode_short_message(0x00, <<0x1B, 0x65>>)
    end

    test "round-trips GSM default and extension text" do
      for str <- ["Hello", "ABC123", "", "a", "@£$", "é", "[]{}\\^~|€"] do
        {:ok, encoded} = Coding.encode_short_message(0x00, str)
        assert {:ok, ^str} = Coding.decode_short_message(0x00, encoded)
      end
    end

    test "rejects characters outside the GSM default and extension tables" do
      assert Coding.encode_short_message(0x00, "á") == :error
      assert Coding.encode_short_message(0x00, "🚀") == :error
    end

    test "decode rejects dangling escape, unknown escape, and out-of-range bytes" do
      assert Coding.decode_short_message(0x00, <<0x1B>>) == :error
      assert Coding.decode_short_message(0x00, <<0x1B, 0x00>>) == :error
      assert Coding.decode_short_message(0x00, <<0x80>>) == :error
      assert Coding.decode_short_message(0x00, <<"Hello", 0xFF>>) == :error
    end
  end

  describe "IA5 ASCII (data_coding 0x01)" do
    test "encodes and decodes strict ASCII without GSM remapping" do
      assert {:ok, <<"abc">>} = Coding.encode_short_message(0x01, "abc")
      assert {:ok, "@"} = Coding.encode_short_message(0x01, "@")
      assert {:ok, "@"} = Coding.decode_short_message(0x01, <<0x40>>)
      refute Coding.encode_short_message(0x01, "@") == Coding.encode_short_message(0x00, "@")
    end

    test "rejects non-ASCII text and high bytes" do
      assert Coding.encode_short_message(0x01, "é") == :error
      assert Coding.decode_short_message(0x01, <<0x80>>) == :error
    end
  end

  describe "octet / unspecified (data_coding 0x02 and other octet codings)" do
    test "preserves raw binary including NUL and high bytes" do
      raw = <<0x00, 0x1B, 0xFF, "Hi">>

      for coding <- [0x02, 0x04, 0x05, 0x06, 0x07, 0x09, 0x0A, 0x0D, 0x0E] do
        assert {:ok, ^raw} = Coding.encode_short_message(coding, raw)
        assert {:ok, ^raw} = Coding.decode_short_message(coding, raw)
      end
    end
  end

  describe "Latin-1 (data_coding 0x03)" do
    test "encodes Latin-1 chars to single-byte codepoints" do
      # 'H'=0x48, 'é'=0xE9, 'l'=0x6C, 'l'=0x6C, 'o'=0x6F in ISO-8859-1
      assert {:ok, <<0x48, 0xE9, 0x6C, 0x6C, 0x6F>>} = Coding.encode_short_message(0x03, "Héllo")
    end

    test "decodes Latin-1 bytes back to chars" do
      assert {:ok, "Héllo"} = Coding.decode_short_message(0x03, <<0x48, 0xE9, 0x6C, 0x6C, 0x6F>>)
    end

    test "round-trips Latin-1 strings" do
      for str <- ["Héllo", "naïve", "àÁâÃäÅæçèéêëìíîïðñòóôõöùúûüý", "abc"] do
        {:ok, encoded} = Coding.encode_short_message(0x03, str)
        assert {:ok, ^str} = Coding.decode_short_message(0x03, encoded)
        assert byte_size(encoded) == String.length(str)
      end
    end
  end

  describe "UCS2 / UTF-16BE (data_coding 0x08)" do
    test "encodes to big-endian UTF-16 (2 bytes per BMP char)" do
      # 'H'=0x0048 -> <<0x00, 0x48>>, 'i'=0x0069 -> <<0x00, 0x69>>
      assert {:ok, <<0x00, 0x48, 0x00, 0x69>>} = Coding.encode_short_message(0x08, "Hi")
    end

    test "decodes UTF-16BE bytes back to chars" do
      assert {:ok, "Hi"} = Coding.decode_short_message(0x08, <<0x00, 0x48, 0x00, 0x69>>)
    end

    test "round-trips extended Unicode through UCS2 including supplementary-plane emoji" do
      for str <- ["日本語", "Hello", "🚀", "mix한글"] do
        {:ok, encoded} = Coding.encode_short_message(0x08, str)
        assert {:ok, ^str} = Coding.decode_short_message(0x08, encoded)
        # Length must be even (2-byte code units); surrogate-pair codepoints
        # (emoji etc.) use 2 code units so byte_size != len(str)*2 is fine.
        assert rem(byte_size(encoded), 2) == 0
      end
    end

    test "encode rejects invalid UTF-8 instead of dropping bytes" do
      assert Coding.encode_short_message(0x08, <<0xFF>>) == :error
    end

    test "decode rejects odd-length UTF-16 and unpaired surrogates" do
      assert Coding.decode_short_message(0x08, <<0x00>>) == :error
      assert Coding.decode_short_message(0x08, <<0xD8, 0x00>>) == :error
      assert Coding.decode_short_message(0x08, <<0xD8, 0x00, 0x00, 0x41>>) == :error
    end
  end

  describe "unsupported / invalid data_coding" do
    test "encoding with unknown data_coding returns :error" do
      # 0x20 is the RFC 822 EMAIL_HEADER IE id, not a data_coding the segment
      # codec supports for short_message handling here.
      assert Coding.encode_short_message(0x20, "x") == :error
    end

    test "decoding with unknown data_coding returns :error" do
      assert Coding.decode_short_message(0x20, <<"x">>) == :error
    end

    test "Latin-1 encoding rejects non-encodable codepoints" do
      # 0x1F11D (CJK) cannot fit in Latin-1 (0..255).
      assert Coding.encode_short_message(0x03, "𛈝") == :error
    end
  end
end
