defmodule JasminEx.Smpp.FramingTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.Framing
  alias JasminEx.Smpp.PDU

  # Helper to encode a minimal PDU so we can talk byte-for-byte about frames.
  defp pdu_bytes(command, seq, body) do
    PDU.build(command: command, status: :ESME_ROK, sequence_number: seq, body: body)
    |> PDU.encode()
    |> IO.iodata_to_binary()
  end

  describe "partial PDU across two TCP reads" do
    test "first read contains only part of the declared length, second read completes it" do
      full = pdu_bytes(:enquire_link, 1, <<>>)
      assert byte_size(full) == 16

      cut = 8
      <<prefix::binary-size(cut), suffix::binary>> = full

      assert {[], ^prefix} = Framing.feed(<<>>, prefix)
      # After joining prefix + suffix the whole PDU is complete
      full = prefix <> suffix
      assert {[^full], <<>>} = Framing.feed(prefix, suffix)
    end

    test "first read contains only the header and zero bytes of body — second read adds body" do
      full = pdu_bytes(:submit_sm_resp, 7, "abc123")
      <<header::binary-size(16), body::binary>> = full

      assert {[], ^header} = Framing.feed(<<>>, header)
      assert {[^full], <<>>} = Framing.feed(header, body)
    end

    test "first read is exactly the full PDU and second read adds zero more" do
      full = pdu_bytes(:bind_transmitter, 99, <<>>)
      assert {[^full], <<>>} = Framing.feed(<<>>, full)
      assert {[], <<>>} = Framing.feed(<<>>, <<>>)
    end
  end

  describe "multiple PDUs in one buffer" do
    test "two complete PDUs emitted back-to-back in order" do
      pdu_a = pdu_bytes(:submit_sm, 1, <<>>)
      pdu_b = pdu_bytes(:deliver_sm, 2, <<>>)
      combined = pdu_a <> pdu_b

      assert {[a, b], <<>>} = Framing.feed(<<>>, combined)
      assert a == pdu_a
      assert b == pdu_b
    end

    test "three complete PDUs emitted in order with no bytes lost" do
      pa = pdu_bytes(:enquire_link, 1, <<>>)
      pb = pdu_bytes(:enquire_link, 2, <<>>)
      pc = pdu_bytes(:enquire_link, 3, <<>>)
      buf = pa <> pb <> pc

      assert {[a, b, c], <<>>} = Framing.feed(<<>>, buf)
      assert a == pa
      assert b == pb
      assert c == pc
    end

    test "multiple PDUs followed by a partial PDU — partial stays in the buffer" do
      pa = pdu_bytes(:enquire_link, 1, <<>>)
      pb_full = pdu_bytes(:deliver_sm, 2, <<>>)
      # deliver_sm is 16 bytes (no body); a partial header is just 10 bytes.
      <<pb_partial::binary-size(10), _rest::binary>> = pb_full

      buf = pa <> pb_partial

      assert {[^pa], ^pb_partial} = Framing.feed(<<>>, buf)
    end
  end

  describe "buffered state across multiple feeds" do
    test "accumulated buffer eventually drains when the missing bytes arrive" do
      full = pdu_bytes(:submit_sm_resp, 5, "msg-id")
      cut = 12
      <<prefix::binary-size(cut), suffix::binary>> = full

      {[], ^prefix} = Framing.feed(<<>>, prefix)
      # no-op empty chunk
      {[], ^prefix} = Framing.feed(prev_prefix = prefix, "")
      full = prefix <> suffix
      assert {[^full], <<>>} = Framing.feed(prev_prefix, suffix)
    end

    test "feed accepts the buffered state returned by previous feed" do
      full = pdu_bytes(:submit_sm_resp, 6, "hello")
      <<prefix::binary-size(20), suffix::binary>> = full

      assert {[], ^prefix} =
               Framing.feed(<<>>, prefix)

      assert {[^full], <<>>} =
               Framing.feed(prefix, suffix)
    end
  end

  describe "boundary cases" do
    test "empty initial buffer + empty chunk returns no PDUs and no leftover" do
      assert {[], <<>>} = Framing.feed(<<>>, <<>>)
    end

    test "head-only stream with declared length smaller than header is dropped (graceful)" do
      # command_length = 12, smaller than the 16-byte header — malformed but
      # the framer MUST NOT crash. It should discard enough bytes to recover
      # or surface the malformed bytes for the caller to decide.
      bad = <<12::32, 0x0000_0004::32, 0::32, 0::32>>
      assert {[], _rest} = Framing.feed(<<>>, bad)
    end

    test "successful framing of three PDUs fits the spec scenario in one go" do
      # alignment check: each PDU's wire length is what the next header expects
      pa = pdu_bytes(:bind_transmitter, 1, <<>>)
      pb = pdu_bytes(:bind_transmitter_resp, 1, "smsc")
      pc = pdu_bytes(:enquire_link_resp, 2, <<>>)

      buf = pa <> pb <> pc
      assert {[da, db, dc], <<>>} = Framing.feed(<<>>, buf)

      {:ok, dda} = PDU.decode(da)
      {:ok, ddb} = PDU.decode(db)
      {:ok, ddc} = PDU.decode(dc)
      assert dda.command == :bind_transmitter
      assert ddb.command == :bind_transmitter_resp
      assert ddc.command == :enquire_link_resp
    end
  end
end
