defmodule JasminEx.Smpp.PDU.CodingTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.PDU.Coding

  describe "GSM 03.38 default alphabet (data_coding 0x00)" do
    test "encodes plain ASCII text to raw 7-bit-passthrough bytes" do
      assert {:ok, "Hello"} = Coding.encode_short_message(0x00, "Hello")
    end

    test "decodes raw bytes back to ASCII string" do
      assert {:ok, "Hello"} = Coding.decode_short_message(0x00, <<"Hello">>)
    end

    test "round-trips ASCII text" do
      for str <- ["Hello", "ABC123", "", "a"] do
        {:ok, encoded} = Coding.encode_short_message(0x00, str)
        assert {:ok, ^str} = Coding.decode_short_message(0x00, encoded)
        assert byte_size(encoded) == byte_size(str)
      end
    end

    test "treats IA5_ASCII (data_coding 0x01) the same as default alphabet" do
      assert {:ok, "abc"} = Coding.decode_short_message(0x01, "abc")
      assert {:ok, <<"abc">>} = Coding.encode_short_message(0x01, "abc")
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
    test "encodes to big-endian UTF-16 (2 bytes per char)" do
      # 'H'=0x0048 -> <<0x00, 0x48>>, 'i'=0x0069 -> <<0x00, 0x69>>
      assert {:ok, <<0x00, 0x48, 0x00, 0x69>>} = Coding.encode_short_message(0x08, "Hi")
    end

    test "decodes UTF-16BE bytes back to chars" do
      assert {:ok, "Hi"} = Coding.decode_short_message(0x08, <<0x00, 0x48, 0x00, 0x69>>)
    end

    test "round-trips extended Unicode through UCS2" do
      for str <- ["日本語", "Hello", "🚀", "mix한글"] do
        {:ok, encoded} = Coding.encode_short_message(0x08, str)
        assert {:ok, ^str} = Coding.decode_short_message(0x08, encoded)
        # Length must be even (2-byte code units); surrogate-pair codepoints
        # (emoji etc.) use 2 code units so byte_size != len(str)*2 is fine.
        assert rem(byte_size(encoded), 2) == 0
      end
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

    test "default alphabet rejects non-ASCII codepoints" do
      assert Coding.encode_short_message(0x00, "é") == :error
    end
  end
end
