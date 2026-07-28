defmodule JasminEx.Smpp.Client.RequestWindowTest do
  use ExUnit.Case, async: true

  alias JasminEx.Smpp.Client.RequestWindow
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body

  describe "sequence allocation" do
    test "starts at one, is request-type independent, and wraps after the maximum" do
      window = RequestWindow.new()

      {bind_sequence, window} = RequestWindow.take_sequence(window)
      window = RequestWindow.insert(window, bind_sequence, :bind_transmitter, nil)
      {submit_sequence, window} = RequestWindow.take_sequence(window)
      window = RequestWindow.insert(window, submit_sequence, :submit_sm, :submit_caller)

      assert bind_sequence == 1
      assert submit_sequence == 2

      window = %{window | next_sequence: 0x7FFF_FFFF}
      {maximum, window} = RequestWindow.take_sequence(window)
      {wrapped, _window} = RequestWindow.take_sequence(window)

      assert maximum == 0x7FFF_FFFF
      assert wrapped == 1
    end
  end

  describe "correlation" do
    test "matching requires both sequence and expected command" do
      window = RequestWindow.new() |> RequestWindow.insert(7, :submit_sm, :caller)

      assert {:ok, %{from: :caller, command_id: :submit_sm}} =
               RequestWindow.match(window, 7, :submit_sm)

      assert :error = RequestWindow.match(window, 8, :submit_sm)
      assert :error = RequestWindow.match(window, 7, :enquire_link)
    end

    test "an unmatched resolution is a no-op" do
      window = RequestWindow.new() |> RequestWindow.insert(7, :submit_sm, :caller)

      assert {:error, ^window} = RequestWindow.resolve(window, 7, :enquire_link)
      assert {:error, ^window} = RequestWindow.resolve(window, 8, :submit_sm)
    end

    test "resolution removes the entry and queues exactly one cancellation" do
      window = RequestWindow.new() |> RequestWindow.insert(7, :submit_sm, :caller)

      assert {:ok, %{from: :caller}, resolved} =
               RequestWindow.resolve(window, 7, :submit_sm)

      assert resolved.pending == %{}
      assert resolved.cancellations == [7]
    end

    test "expiration removes without cancellation and a stale timeout is a no-op" do
      window = RequestWindow.new() |> RequestWindow.insert(7, :submit_sm, :caller)

      assert {%{from: :caller}, expired} = RequestWindow.expire(window, 7)
      assert expired.pending == %{}
      assert expired.cancellations == []
      assert {nil, ^expired} = RequestWindow.expire(expired, 7)
    end

    test "bind, heartbeat, submit, and unbind coexist and correlate independently" do
      window =
        RequestWindow.new()
        |> RequestWindow.insert(1, :bind_transmitter, nil)
        |> RequestWindow.insert(2, :enquire_link, nil)
        |> RequestWindow.insert(3, :submit_sm, :caller)
        |> RequestWindow.insert(4, :unbind, nil)

      assert {:ok, %{command_id: :submit_sm}, window} =
               RequestWindow.resolve(window, 3, :submit_sm)

      assert :error = RequestWindow.match(window, 3, :submit_sm)

      assert {:ok, %{command_id: :bind_transmitter}} =
               RequestWindow.match(window, 1, :bind_transmitter)

      assert {:ok, %{command_id: :enquire_link}} =
               RequestWindow.match(window, 2, :enquire_link)

      assert {:ok, %{command_id: :unbind}} = RequestWindow.match(window, 4, :unbind)
    end
  end

  describe "flushes and cancellations" do
    test "full flush preserves map reply order and prepends original keys to cancellations" do
      window =
        RequestWindow.new()
        |> RequestWindow.insert(3, :submit_sm, :third)
        |> RequestWindow.insert(1, :bind_transmitter, nil)
        |> RequestWindow.insert(2, :submit_sm, :second)

      window = %{window | cancellations: [9, 8]}

      expected_directives =
        Enum.map(window.pending, fn {_sequence, entry} ->
          {entry.from, {:unknown, :disconnected}}
        end)

      expected_cancellations = Map.keys(window.pending) ++ [9, 8]

      assert {flushed, ^expected_directives} =
               RequestWindow.flush(window, {:unknown, :disconnected})

      assert flushed.pending == %{}
      assert flushed.cancellations == expected_cancellations
    end

    test "submit-only flush preserves internal entries" do
      window =
        RequestWindow.new()
        |> RequestWindow.insert(1, :enquire_link, nil)
        |> RequestWindow.insert(2, :submit_sm, :second)
        |> RequestWindow.insert(3, :unbind, nil)

      expected_froms =
        for {_sequence, %{command_id: :submit_sm, from: from}} <- window.pending, do: from

      assert {flushed, directives} =
               RequestWindow.flush_submits(window, {:unknown, :unbind_deadline})

      assert Enum.map(directives, &elem(&1, 0)) == expected_froms
      assert Enum.all?(directives, &(elem(&1, 1) == {:unknown, :unbind_deadline}))
      assert Map.keys(flushed.pending) |> Enum.sort() == [1, 3]
      assert flushed.cancellations == [2]
    end

    test "internal drop preserves submit callers" do
      window =
        RequestWindow.new()
        |> RequestWindow.insert(1, :bind_transmitter, nil)
        |> RequestWindow.insert(2, :submit_sm, :caller)
        |> RequestWindow.insert(3, :enquire_link, nil)

      dropped = RequestWindow.drop_internal(window)

      assert dropped.pending == %{2 => %{from: :caller, command_id: :submit_sm}}
      assert dropped.cancellations == [3, 1]
      assert RequestWindow.pending_submits?(dropped)
    end

    test "draining returns cancellations in order and clears them exactly once" do
      window = %{RequestWindow.new() | cancellations: [3, 1, 2]}

      assert {actions, drained} = RequestWindow.drain_cancellations(window)

      assert actions == [
               {{:timeout, {:pending, 3}}, :cancel},
               {{:timeout, {:pending, 1}}, :cancel},
               {{:timeout, {:pending, 2}}, :cancel}
             ]

      assert drained.cancellations == []
      assert {[], ^drained} = RequestWindow.drain_cancellations(drained)
    end
  end

  describe "submit response classification" do
    test "classifies accepted, rejected, empty, and malformed responses" do
      {:ok, accepted_body} =
        Body.encode(:submit_sm_resp, %Body.SubmitSMResp{message_id: "message-id"})

      accepted = submit_response(:ESME_ROK, IO.iodata_to_binary(accepted_body))
      rejected = submit_response(:ESME_RTHROTTLED, <<0xFF>>)
      empty = submit_response(:ESME_ROK, <<0>>)
      malformed = submit_response(:ESME_ROK, <<0xFF>>)

      assert RequestWindow.classify_submit_response(accepted) == {:ok, "message-id"}

      assert RequestWindow.classify_submit_response(rejected) ==
               {:error, {:submit_rejected, :ESME_RTHROTTLED}}

      assert RequestWindow.classify_submit_response(empty) == {:unknown, :invalid_response}
      assert RequestWindow.classify_submit_response(malformed) == {:unknown, :invalid_response}
    end
  end

  defp submit_response(status, body) do
    PDU.build(command: :submit_sm_resp, status: status, sequence_number: 1, body: body)
  end
end
