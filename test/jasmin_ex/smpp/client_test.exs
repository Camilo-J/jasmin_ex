defmodule JasminEx.Smpp.ClientTest do
  @moduledoc false
  # Integration scenarios for the SMPP client session lifecycle (PR2).
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body

  # Short timers so heartbeat tests don't run for 30s by default.
  @heartbeat_ms 50
  @response_timeout_ms 200

  defp start_smsc(opts \\ []) do
    {:ok, port, pid} = FakeSMSC.start_link(opts)
    {:ok, port, pid}
  end

  defp start_client(port, opts \\ []) do
    base = [
      host: ~c"localhost",
      port: port,
      system_id: "user",
      password: "pw",
      system_type: "type",
      bind_as: :transmitter,
      heartbeat_ms: @heartbeat_ms,
      response_timeout_ms: @response_timeout_ms,
      reconnect_base_ms: 5,
      reconnect_cap_ms: 5,
      reconnect_jitter: false
    ]

    Client.start_link(Keyword.merge(base, opts))
  end

  defp wait_until(predicate, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(predicate, deadline)
  end

  defp do_wait(predicate, deadline) do
    if predicate.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        {:error, :timeout}
      else
        Process.sleep(5)
        do_wait(predicate, deadline)
      end
    end
  end

  defp stop_pair(smsc, client) do
    if is_pid(client) and Process.alive?(client),
      do:
        (try do
           GenServer.stop(client, :normal, 200)
         catch
           _, _ -> :ok
         end)

    if is_pid(smsc) and Process.alive?(smsc),
      do:
        (try do
           GenServer.stop(smsc, :normal, 200)
         catch
           _, _ -> :ok
         end)
  end

  describe "bind lifecycle" do
    for {bind_as, request_command, response_command} <- [
          {:transmitter, :bind_transmitter, :bind_transmitter_resp},
          {:receiver, :bind_receiver, :bind_receiver_resp},
          {:transceiver, :bind_transceiver, :bind_transceiver_resp}
        ] do
      test "#{bind_as} bind reaches :bound after its matching response" do
        bind_as = unquote(bind_as)
        request_command = unquote(request_command)
        response_command = unquote(response_command)

        {:ok, port, smsc} = start_smsc()
        :ok = FakeSMSC.withhold_response(smsc, request_command)
        ref = FakeSMSC.subscribe(smsc)
        {:ok, client} = start_client(port, bind_as: bind_as)

        assert :ok = FakeSMSC.wait_connected(smsc, 500)

        assert_receive {:fake_smsc_bytes, ^ref, payload}, 500

        assert {:ok, %PDU{command: ^request_command, sequence_number: sequence_number}} =
                 PDU.decode(payload)

        assert Client.status(client) == :bind_pending

        assert :ok =
                 FakeSMSC.send_bytes(
                   smsc,
                   pdu_bytes(response_command, :ESME_ROK, sequence_number, "")
                 )

        assert :ok = wait_until(fn -> Client.status(client) == :bound end)
        stop_pair(smsc, client)
      end
    end

    test "FakeSMSC echoes a request sequence_number in its response" do
      {:ok, port, smsc} = start_smsc()

      {:ok, socket} =
        :gen_tcp.connect(~c"localhost", port, [:binary, packet: :raw, active: false], 500)

      request = pdu_bytes(:enquire_link, :ESME_ROK, 42, "")

      assert :ok = :gen_tcp.send(socket, request)
      assert {:ok, response} = :gen_tcp.recv(socket, 0, 500)
      assert {:ok, %PDU{command: :enquire_link_resp, sequence_number: 42}} = PDU.decode(response)

      :gen_tcp.close(socket)
      stop_pair(smsc, nil)
    end

    test "uses the design-required :state_functions callback mode" do
      assert Client.callback_mode() == :state_functions
    end

    test "successful bind (status 0) transitions the client to :bound" do
      {:ok, port, smsc} = start_smsc()
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)
      assert Client.status(client) == :bound
      stop_pair(smsc, client)
    end

    test "bind rejected (non-zero status) leaves the client out of :bound" do
      {:ok, port, smsc} = start_smsc()
      :ok = FakeSMSC.inject_bind_resp(smsc, :ESME_RSYSERR)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) != :connecting end)
      refute Client.status(client) == :bound
      stop_pair(smsc, client)
    end
  end

  describe "socket drop transitions out of mid-connect / mid-bind" do
    test "closing the harness socket during :connecting forces :disconnected" do
      {:ok, port, smsc} = start_smsc()
      :ok = FakeSMSC.close_on_command(smsc, :bind_transmitter)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) != :connecting end)
      assert Client.status(client) in [:disconnected, :bind_pending]
      refute Client.status(client) == :bound
      stop_pair(smsc, client)
    end
  end

  describe "heartbeat (client-driven enquire_link)" do
    test "client sends enquire_link periodically while :bound, harness responds" do
      {:ok, port, smsc} = start_smsc()
      ref = FakeSMSC.subscribe(smsc)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      # Subscribe to FakeSMSC bytes; we expect an enquire_link to land
      # there once the heartbeat interval elapses.
      enquire_seen =
        wait_until(
          fn ->
            draining =
              receive do
                {:fake_smsc_bytes, ^ref, payload} ->
                  case PDU.decode(payload) do
                    {:ok, %PDU{command: :enquire_link}} -> true
                    _ -> false
                  end

                _ ->
                  false
              after
                0 -> false
              end

            draining
          end,
          @heartbeat_ms * 5
        )

      assert enquire_seen == :ok, "expected an enquire_link PDU from the client"
      assert Client.status(client) == :bound
      stop_pair(smsc, client)
    end

    test "an unanswered heartbeat times out and forces :bound -> :disconnected" do
      {:ok, port, smsc} = start_smsc()
      # Drop ALL enquire_link_resp so the heartbeat times out.
      :ok = FakeSMSC.withhold_response(smsc, :enquire_link)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)
      assert :ok = wait_until(fn -> Client.status(client) == :disconnected end)
      refute Client.status(client) == :bound
      stop_pair(smsc, client)
    end
  end

  describe "reconnect with exponential backoff" do
    test "rebinds after a TCP drop once heartbeat traffic has advanced the sequence" do
      {:ok, port, smsc} = start_smsc()
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)
      assert :ok = wait_until(fn -> heartbeat_has_advanced_sequence?(client) end)

      :ok = FakeSMSC.close_on_command(smsc, :submit_sm)

      assert {:error, :disconnected} = Client.send_submit_sm(client, submit_sm("drop"))
      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      stop_pair(smsc, client)
    end

    test "deterministic backoff: with jitter disabled and base==cap the client must not reach :bound" do
      {:ok, port, smsc} = start_smsc()
      :ok = FakeSMSC.inject_bind_resp(smsc, :ESME_RSYSERR)
      {:ok, client} = start_client(port)

      # Pinning base to 5ms with no jitter means the backoff_delay is
      # always 5ms regardless of attempt count. Verify by subscription:
      # we expect MULTIPLE distinct bind requests to arrive at the
      # harness (proving backoff is reconnecting). The :bound invariant
      # — never reaching :bound — is what the backoff defends.
      ref = FakeSMSC.subscribe(smsc)

      Process.sleep(500)

      # Drain the subscriber mailbox: count bind_transmitter PDUs.
      bind_count =
        for _ <- 1..20, reduce: 0 do
          acc ->
            receive do
              {:fake_smsc_bytes, ^ref, payload} ->
                case PDU.decode(payload) do
                  {:ok, %PDU{command: :bind_transmitter}} -> acc + 1
                  _ -> acc
                end

              _ ->
                acc
            after
              0 -> acc
            end
        end

      # With base=5ms and the test running for 500ms, we expect >= 2
      # bind attempts (proving the backoff reconnect loop is alive).
      assert bind_count >= 2,
             "expected the client to retry bind at least twice; got #{bind_count}"

      # And we must never have reached :bound.
      refute Client.status(client) == :bound

      stop_pair(smsc, client)
    end
  end

  describe "inbound enquire_link (SMSC-driven)" do
    test "an unsolicited enquire_link from the SMSC is auto-answered" do
      {:ok, port, smsc} = start_smsc()
      ref = FakeSMSC.subscribe(smsc)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      # Send an unsolicited enquire_link from the harness -> the client
      # must respond with an enquire_link_resp, observed by our subscriber.
      _ = FakeSMSC.send_bytes(smsc, enquire_link_bytes(:enquire_link, 99))

      # Drain the subscriber mailbox.
      seen_resp =
        wait_until(
          fn ->
            receive do
              {:fake_smsc_bytes, ^ref, payload} ->
                case PDU.decode(payload) do
                  {:ok, %PDU{command: :enquire_link_resp}} -> true
                  _ -> false
                end

              _ ->
                false
            after
              0 -> false
            end
          end,
          500
        )

      assert seen_resp == :ok, "expected an enquire_link_resp from the client"
      assert Client.status(client) == :bound
      stop_pair(smsc, client)
    end
  end

  describe "pending-window cleanup on involuntary :bound exit" do
    test "wraps the sequence_number after 0x7FFFFFFF without emitting the high bit" do
      {:ok, port, smsc} = start_smsc()
      ref = FakeSMSC.subscribe(smsc)
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)
      :sys.replace_state(client, fn {state, data} -> {state, %{data | seq: 0x7FFF_FFFF}} end)

      test_pid = self()

      for message <- ["wrap", "wrap-next"] do
        spawn_link(fn ->
          send(
            test_pid,
            {String.to_atom(message), Client.send_submit_sm(client, submit_sm(message))}
          )
        end)
      end

      assert %{wrap: 0x7FFF_FFFF, "wrap-next": 1} = await_submit_sequences(ref, %{}, 2)

      :ok = FakeSMSC.close_on_command(smsc, :enquire_link)
      assert_receive {:wrap, {:error, :disconnected}}, 500
      assert_receive {:"wrap-next", {:error, :disconnected}}, 500
      stop_pair(smsc, client)
    end

    test "routes out-of-order submit_sm responses to their originating callers" do
      {:ok, port, smsc} = start_smsc()
      ref = FakeSMSC.subscribe(smsc)
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      test_pid = self()

      for {label, message} <- [{:first, "first"}, {:second, "second"}] do
        spawn_link(fn ->
          send(test_pid, {label, Client.send_submit_sm(client, submit_sm(message))})
        end)
      end

      sequences = await_submit_sequences(ref, %{}, 2)
      assert map_size(sequences) == 2

      Enum.each(Enum.reverse(Map.values(sequences)), fn sequence_number ->
        assert :ok =
                 FakeSMSC.send_bytes(
                   smsc,
                   submit_sm_resp_bytes(sequence_number, "msg-#{sequence_number}")
                 )
      end)

      assert_receive {:first, {:ok, first_message_id}}, 500
      assert_receive {:second, {:ok, second_message_id}}, 500
      assert first_message_id == "msg-#{sequences.first}"
      assert second_message_id == "msg-#{sequences.second}"

      stop_pair(smsc, client)
    end

    test "every N pending submit_sm caller gets {:error, :disconnected} when the SMSC drops the socket" do
      {:ok, port, smsc} = start_smsc()
      # Withhold submit_sm_resp so callers stay pending until the TCP drop.
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      # Spawn N concurrent senders; each fires `Client.send_submit_sm/2`
      # which goes through `:gen_statem.call` (so the reply lands back at
      # the caller via the from-tag pinpoint). Forward the result to
      # the test process.
      n = 3
      test_pid = self()

      for i <- 1..n do
        spawn_link(fn ->
          body = %JasminEx.Smpp.PDU.Body.SubmitSM{
            source_addr_ton: :INTERNATIONAL,
            source_addr_npi: :ISDN,
            source_addr: "src",
            dest_addr_ton: :NATIONAL,
            dest_addr_npi: :ISDN,
            destination_addr: "555#{1000 + i}",
            short_message: "msg#{i}"
          }

          reply =
            try do
              Client.send_submit_sm(client, body)
            catch
              _kind, reason -> {:caught, reason}
            end

          send(test_pid, {:reply_for, i, reply})
        end)
      end

      # Wait for each caller to have parked a pending entry.
      :ok =
        wait_until(
          fn ->
            {_state, data} = :sys.get_state(client)
            map_size(data.pending) >= n
          end,
          500
        )

      # Drop the harness side of the TCP conn.
      :ok = GenServer.stop(smsc)

      Enum.each(1..n, fn i ->
        receive do
          {:reply_for, ^i, reply} ->
            assert reply == {:error, :disconnected}, "caller #{i} got #{inspect(reply)}"
        after
          1_000 ->
            flunk("caller #{i} did not receive {:error, :disconnected} within 1s")
        end
      end)

      stop_pair(nil, client)
    end
  end

  describe "unmatched-sequence guard" do
    test "a *_resp with an unknown sequence_number is logged and discarded, session stays :bound" do
      {:ok, port, smsc} = start_smsc()
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      # Harness sends a submit_sm_resp for sequence_number 999 (no
      # matching pending entry); the client must NOT crash and must
      # stay in :bound.
      log =
        capture_log(fn ->
          _ = FakeSMSC.send_bytes(smsc, pdu_bytes(:submit_sm_resp, :ESME_ROK, 999, "no-such-msg"))
          Process.sleep(50)
        end)

      assert log =~ "unknown sequence_number 999"
      assert Client.status(client) == :bound

      stop_pair(smsc, client)
    end

    test "a generic_nack with sequence_number 0 is logged and discarded, session stays :bound" do
      {:ok, port, smsc} = start_smsc()
      {:ok, client} = start_client(port)

      assert :ok = wait_until(fn -> Client.status(client) == :bound end)

      log =
        capture_log(fn ->
          _ = FakeSMSC.send_bytes(smsc, pdu_bytes(:generic_nack, :ESME_RINVCMDID, 0, ""))
          Process.sleep(50)
        end)

      assert log =~ "unknown sequence_number 0"
      assert Client.status(client) == :bound

      stop_pair(smsc, client)
    end
  end

  defp pdu_bytes(command, status, seq, body) do
    PDU.build(command: command, status: status, sequence_number: seq, body: body)
    |> PDU.encode()
    |> IO.iodata_to_binary()
  end

  defp submit_sm_resp_bytes(sequence_number, message_id) do
    {:ok, body} =
      Body.encode(
        :submit_sm_resp,
        %Body.SubmitSMResp{message_id: message_id}
      )

    pdu_bytes(:submit_sm_resp, :ESME_ROK, sequence_number, IO.iodata_to_binary(body))
  end

  defp submit_sm(message) do
    %JasminEx.Smpp.PDU.Body.SubmitSM{
      source_addr_ton: :INTERNATIONAL,
      source_addr_npi: :ISDN,
      source_addr: "src",
      dest_addr_ton: :NATIONAL,
      dest_addr_npi: :ISDN,
      destination_addr: "5551000",
      short_message: message
    }
  end

  defp heartbeat_has_advanced_sequence?(client) do
    {_state, data} = :sys.get_state(client)
    data.seq > 1
  end

  defp await_submit_sequences(_ref, sequences, expected) when map_size(sequences) == expected,
    do: sequences

  defp await_submit_sequences(ref, sequences, expected) do
    receive do
      {:fake_smsc_bytes, ^ref, payload} ->
        case PDU.decode(payload) do
          {:ok, %PDU{command: :submit_sm, sequence_number: sequence_number, body: body}} ->
            {:ok, %Body.SubmitSM{short_message: message}} = Body.decode(:submit_sm, body)

            await_submit_sequences(
              ref,
              Map.put(sequences, String.to_atom(message), sequence_number),
              expected
            )

          _ ->
            await_submit_sequences(ref, sequences, expected)
        end
    after
      500 ->
        flunk("did not receive #{expected} submit_sm requests")
    end
  end

  ## helpers

  defp enquire_link_bytes(command, seq) do
    PDU.build(command: command, status: :ESME_ROK, sequence_number: seq, body: <<>>)
    |> PDU.encode()
    |> IO.iodata_to_binary()
  end
end
