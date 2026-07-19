defmodule JasminEx.Smpp.Framing do
  @moduledoc """
  Length-prefixed TCP frame reassembly for SMPP PDUs.

  SMPP is a byte stream protocol where each PDU is preceded by its own
  declared 32-bit `command_length` (see `JasminEx.Smpp.PDU`). The
  framer consumes raw bytes from one or more `feed/2` calls and emits
  one or more complete PDU binaries in the order they arrived, leaving
  any unconsumed bytes in the returned buffer for the next call.

  This module is intentionally pure — it owns no socket, no process,
  no timer. The session (`JasminEx.Smpp.Client`, PR2) will own those
  concerns and call `feed/2` with the accumulated buffer between TCP
  reads.

  ## Spec scenarios covered

    * Partial PDU across two TCP reads  → exactly one complete PDU,
      no error.
    * Multiple PDUs in one buffer       → both emitted in order, no
      bytes lost or duplicated.
    * Accumulated buffer across feeds   → caller pipes the returned
      buffer back into the next `feed/2` call.

  ## Malformed streams

  When the buffer is shorter than the 16-byte header, nothing is
  emitted (waiting on more bytes). If the buffer is at least 16 bytes
  but the declared `command_length` is smaller than 16 — a wire
  protocol violation — we cannot trust the rest of the bytes and the
  framer currently drops the head of the buffer to seek resync. Real
  recovery beyond that would need a :gen_statem-level recovery hook
  (out of PR1 scope).
  """

  @header_size 16

  @type pdu_binary :: binary()
  @type buffer :: binary()

  @doc """
  Consume `chunk` bytes appended to the buffered statefulness, extract
  every complete PDU, and return `{complete_pdus, leftover_buffer}`.

  Calling with `chunk == <<>>` is a no-op that reports the buffer state
  back as-is, which is convenient in tests and session loops.
  """
  @spec feed(buffer(), binary()) :: {[pdu_binary()], buffer()}
  def feed(buffer, chunk) when is_binary(buffer) and is_binary(chunk) do
    state = buffer <> chunk
    extract(state, [])
  end

  # ── private helpers ──────────────────────────────────────────────────────

  defp extract(<<>>), do: {[], <<>>}

  defp extract(state) when byte_size(state) < @header_size, do: {[], state}

  defp extract(<<command_length::32, _rest::binary>> = state)
       when command_length < @header_size do
    # Wire-level bad length — drop the head bytes so the next header can be
    # examined. This is a coarse recovery; treat the discards as
    # "frames rejected" the caller can log.
    if byte_size(state) <= command_length do
      {[], <<>>}
    else
      <<_::binary-size(^command_length), rest::binary>> = state
      extract(rest, [])
    end
  end

  defp extract(<<command_length::32, _::binary>> = state) do
    if byte_size(state) < command_length do
      # partial PDU at the head — wait for more bytes
      {[], state}
    else
      <<pdu::binary-size(^command_length), rest::binary>> = state
      extract(rest, [pdu])
    end
  end

  defp extract(state, acc) do
    extract(state)
    |> case do
      {[], leftover} ->
        {Enum.reverse(acc), leftover}

      {pdus, leftover} ->
        new_acc = Enum.reduce(pdus, acc, &[&1 | &2])
        # We've emitted at least one new PDU; continue scanning `leftover`.
        extract(leftover, new_acc)
    end
  end
end
