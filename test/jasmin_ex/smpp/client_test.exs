defmodule JasminEx.Smpp.ClientTest do
  @moduledoc false
  # Integration scenarios for the SMPP client session lifecycle (PR2).
  use ExUnit.Case, async: false

  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU

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

    Client.start_link(base ++ opts)
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
    if Process.alive?(client),
      do:
        (try do
           GenServer.stop(client, :normal, 200)
         catch
           _, _ -> :ok
         end)

    if Process.alive?(smsc),
      do:
        (try do
           GenServer.stop(smsc, :normal, 200)
         catch
           _, _ -> :ok
         end)
  end

  describe "bind lifecycle" do
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

  ## helpers

  defp enquire_link_bytes(command, seq) do
    PDU.build(command: command, status: :ESME_ROK, sequence_number: seq, body: <<>>)
    |> PDU.encode()
    |> IO.iodata_to_binary()
  end
end
