defmodule JasminEx.Smpp.ClientPR3Test do
  @moduledoc false
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias JasminEx.Smpp.Client
  alias JasminEx.Smpp.DeliverHandler.SendToPid
  alias JasminEx.Smpp.FakeSMSC
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body

  @response_timeout_ms 100

  defp start_smsc(opts \\ []) do
    {:ok, port, smsc} = FakeSMSC.start_link(opts)
    {port, smsc}
  end

  # Keyword.merge/2, not ++: build_config/1 reads options with Keyword.fetch!/2
  # and Keyword.get/3, which both return the FIRST match, so appending would
  # silently discard every per-test override.
  defp start_client(port, opts \\ []) do
    defaults = [
      host: ~c"localhost",
      port: port,
      system_id: "user",
      password: "pw",
      system_type: "type",
      bind_as: :transmitter,
      heartbeat_ms: 10_000,
      response_timeout_ms: @response_timeout_ms,
      reconnect_base_ms: 5,
      reconnect_cap_ms: 5,
      reconnect_jitter: false
    ]

    Client.start_link(Keyword.merge(defaults, opts))
  end

  defp wait_until(predicate, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_until_deadline(predicate, deadline)
  end

  defp wait_until_deadline(predicate, deadline) do
    if predicate.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        {:error, :timeout}
      else
        Process.sleep(5)
        wait_until_deadline(predicate, deadline)
      end
    end
  end

  defp await_bound(client), do: wait_until(fn -> Client.status(client) == :bound end)

  # Reaching :bind_pending only proves the client's connect/2 returned. The
  # harness accepts asynchronously, so send_bytes/2 can still answer
  # {:error, :no_connection} until it has handled its own :accepted message.
  # Any test that injects bytes while the client is still binding must wait for
  # both sides, not just the client.
  defp await_bind_pending(client, smsc) do
    with :ok <- wait_until(fn -> Client.status(client) == :bind_pending end) do
      FakeSMSC.wait_connected(smsc)
    end
  end

  defp stop(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 200)
      catch
        _, _ -> :ok
      end
    end
  end

  defp stop(nil), do: :ok

  defp submit_sm(message) do
    %Body.SubmitSM{
      source_addr_ton: :INTERNATIONAL,
      source_addr_npi: :ISDN,
      source_addr: "src",
      dest_addr_ton: :NATIONAL,
      dest_addr_npi: :ISDN,
      destination_addr: "5551000",
      short_message: message
    }
  end

  defp pdu_bytes(command, sequence_number, body) do
    PDU.build(command: command, status: :ESME_ROK, sequence_number: sequence_number, body: body)
    |> PDU.encode()
    |> IO.iodata_to_binary()
  end

  defp deliver_sm_bytes(sequence_number, message) do
    deliver_sm = struct(Body.DeliverSM, Map.from_struct(submit_sm(message)))
    {:ok, body} = Body.encode(:deliver_sm, deliver_sm)
    pdu_bytes(:deliver_sm, sequence_number, IO.iodata_to_binary(body))
  end

  describe "windowing and response correlation" do
    test "ignores a bind response whose sequence does not match the pending bind" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :bind_transmitter)
      {:ok, client} = start_client(port, response_timeout_ms: 500)
      assert :ok = await_bind_pending(client, smsc)

      assert :ok = FakeSMSC.send_bytes(smsc, pdu_bytes(:bind_transmitter_resp, 999, <<0>>))
      Process.sleep(20)

      assert Client.status(client) == :bind_pending
      stop(client)
      stop(smsc)
    end

    test "emits disconnect and reconnect telemetry when a bound connection drops" do
      {port, smsc} = start_smsc()
      handler_id = {__MODULE__, make_ref()}
      test_pid = self()

      :ok =
        :telemetry.attach_many(
          handler_id,
          [[:jasmin_ex, :smpp, :disconnected], [:jasmin_ex, :smpp, :reconnect_scheduled]],
          fn event, _measurements, metadata, _config ->
            send(test_pid, {:telemetry, event, metadata})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)

      :ok = FakeSMSC.close_on_command(smsc, :submit_sm)
      assert {:error, :disconnected} = Client.send_submit_sm(client, submit_sm("drop"))

      assert_receive {:telemetry, [:jasmin_ex, :smpp, :disconnected], %{}}, 500
      assert_receive {:telemetry, [:jasmin_ex, :smpp, :reconnect_scheduled], %{attempt: _}}, 500
      stop(client)
      stop(smsc)
    end

    test "the reorder harness releases queued submit responses in reverse request order" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.reorder_responses(smsc, :submit_sm)
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe(smsc)

      test_pid = self()

      spawn_link(fn ->
        send(test_pid, {:first, Client.send_submit_sm(client, submit_sm("first"))})
      end)

      spawn_link(fn ->
        send(test_pid, {:second, Client.send_submit_sm(client, submit_sm("second"))})
      end)

      assert :ok = wait_until(fn -> pending_count(client) == 2 end)
      assert :ok = await_submits(ref, 2)
      assert :ok = FakeSMSC.release_reordered(smsc)

      assert_receive {:first, {:ok, "fake-msg-id"}}, 500
      assert_receive {:second, {:ok, "fake-msg-id"}}, 500
      stop(client)
      stop(smsc)
    end

    test "a resolved submit response cancels its pending timeout" do
      {port, smsc} = start_smsc()
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)

      assert {:ok, "fake-msg-id"} = Client.send_submit_sm(client, submit_sm("resolved"))
      Process.sleep(@response_timeout_ms * 2)

      assert Client.status(client) == :bound
      assert pending_count(client) == 0
      stop(client)
      stop(smsc)
    end

    test "submit_sm fast-fails without crashing before the client is bound" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :bind_transmitter)
      {:ok, client} = start_client(port)

      assert Client.status(client) in [:disconnected, :connecting, :bind_pending]
      assert {:error, :disconnected} = Client.send_submit_sm(client, submit_sm("early"))
      assert Process.alive?(client)
      stop(client)
      stop(smsc)
    end
  end

  describe "deliver_sm dispatch" do
    test "forwards inbound deliver_sm to SendToPid and acknowledges it" do
      {port, smsc} = start_smsc()
      {:ok, handler} = SendToPid.start_link(owner: self())
      {:ok, client} = start_client(port, deliver_handler: {SendToPid, handler})
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe(smsc)

      assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(77, "hello"))

      assert_receive {:smpp_deliver_sm, %Body.DeliverSM{short_message: "hello"},
                      %{client: ^client}},
                     500

      bytes = await_pdu(ref, :deliver_sm_resp)

      assert {:ok, %PDU{command: :deliver_sm_resp, sequence_number: 77, body: body}} =
               PDU.decode(bytes)

      assert {:ok, %Body.DeliverSMResp{message_id: ""}} = Body.decode(:deliver_sm_resp, body)
      stop(client)
      stop(handler)
      stop(smsc)
    end

    test "a dead SendToPid owner negative-acknowledges deliver_sm so the SMSC retries" do
      {port, smsc} = start_smsc()

      owner =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      {:ok, handler} = SendToPid.start_link(owner: owner)
      {:ok, client} = start_client(port, deliver_handler: {SendToPid, handler})
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe(smsc)
      Process.exit(owner, :kill)

      log =
        capture_log(fn ->
          assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(78, "after-owner"))
          Process.sleep(30)
        end)

      assert log =~ "no live consumer"
      bytes = await_pdu(ref, :deliver_sm_resp)

      assert {:ok, %PDU{command: :deliver_sm_resp, sequence_number: 78, status: :ESME_RX_T_APPN}} =
               PDU.decode(bytes)

      assert Client.status(client) == :bound
      stop(client)
      stop(handler)
      stop(smsc)
    end

    test "an unencodable handler status responds ESME_RSYSERR instead of killing the session" do
      {port, smsc} = start_smsc()
      ref = FakeSMSC.subscribe(smsc)

      {:ok, client} =
        start_client(port,
          deliver_handler: {JasminEx.Smpp.UnmappedStatusDeliverHandler, :ignored}
        )

      assert :ok = await_bound(client)

      log =
        capture_log(fn ->
          assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(81, "unmapped"))
          Process.sleep(30)
        end)

      assert log =~ "unencodable status"
      bytes = await_pdu(ref, :deliver_sm_resp)
      assert {:ok, %PDU{status: :ESME_RSYSERR, sequence_number: 81}} = PDU.decode(bytes)
      assert Client.status(client) == :bound
      stop(client)
      stop(smsc)
    end

    test "acknowledges a deliver_sm coalesced into the bind response packet" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :bind_transmitter)
      {:ok, handler} = SendToPid.start_link(owner: self())

      {:ok, client} =
        start_client(port, deliver_handler: {SendToPid, handler}, response_timeout_ms: 500)

      assert :ok = await_bind_pending(client, smsc)
      ref = FakeSMSC.subscribe(smsc)
      bind_seq = pending_bind_seq(client)

      coalesced =
        pdu_bytes(:bind_transmitter_resp, bind_seq, <<0>>) <> deliver_sm_bytes(80, "coalesced")

      assert :ok = FakeSMSC.send_bytes(smsc, coalesced)

      assert_receive {:smpp_deliver_sm, %Body.DeliverSM{short_message: "coalesced"}, _context},
                     500

      bytes = await_pdu(ref, :deliver_sm_resp)
      assert {:ok, %PDU{status: :ESME_ROK, sequence_number: 80}} = PDU.decode(bytes)
      assert :ok = await_bound(client)
      stop(client)
      stop(handler)
      stop(smsc)
    end

    test "a crashing handler asks the SMSC to retry without crashing the session" do
      {port, smsc} = start_smsc()
      ref = FakeSMSC.subscribe(smsc)

      {:ok, client} =
        start_client(port, deliver_handler: {JasminEx.Smpp.RaisingDeliverHandler, :ignored})

      assert :ok = await_bound(client)

      assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(79, "boom"))

      bytes = await_pdu(ref, :deliver_sm_resp)
      assert {:ok, %PDU{status: :ESME_RX_T_APPN, sequence_number: 79}} = PDU.decode(bytes)
      assert Client.status(client) == :bound
      stop(client)
      stop(smsc)
    end

    test "a malformed deliver_sm body responds with a system error, not a retry" do
      {port, smsc} = start_smsc()
      {:ok, handler} = SendToPid.start_link(owner: self())
      {:ok, client} = start_client(port, deliver_handler: {SendToPid, handler})
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe(smsc)

      assert :ok = FakeSMSC.send_bytes(smsc, pdu_bytes(:deliver_sm, 82, <<0xFF, 0xFE>>))

      bytes = await_pdu(ref, :deliver_sm_resp)
      assert {:ok, %PDU{status: :ESME_RSYSERR, sequence_number: 82}} = PDU.decode(bytes)
      refute_receive {:smpp_deliver_sm, _pdu, _context}, 100
      assert Client.status(client) == :bound
      stop(client)
      stop(handler)
      stop(smsc)
    end
  end

  describe "deliver_sm without a configured handler" do
    test "asks the SMSC to retry instead of acknowledging an unconsumed delivery" do
      {port, smsc} = start_smsc()
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe(smsc)

      log =
        capture_log(fn ->
          assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(83, "no-handler"))
          Process.sleep(30)
        end)

      assert log =~ "no deliver_handler is configured"
      bytes = await_pdu(ref, :deliver_sm_resp)
      assert {:ok, %PDU{status: :ESME_RX_T_APPN, sequence_number: 83}} = PDU.decode(bytes)
      assert Client.status(client) == :bound
      stop(client)
      stop(smsc)
    end
  end

  describe "graceful unbind" do
    test "unbind replies instead of blocking when the session is not bound" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :bind_transmitter)
      {:ok, client} = start_client(port, response_timeout_ms: 500)
      assert :ok = wait_until(fn -> Client.status(client) == :bind_pending end)

      assert {:error, :disconnected} = Client.unbind(client)
      assert Process.alive?(client)
      assert Client.status(client) == :bind_pending
      stop(client)
      stop(smsc)
    end

    test "a second unbind while already unbinding replies instead of blocking" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :unbind)
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)

      assert :ok = Client.unbind(client)
      assert :ok = wait_until(fn -> Client.status(client) == :unbinding end)
      assert {:error, :unbinding} = Client.unbind(client)
      assert Process.alive?(client)
      stop(client)
      stop(smsc)
    end

    test "unbind cancels the pending timers of the requests it flushes" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      :ok = FakeSMSC.withhold_response(smsc, :unbind)
      {:ok, client} = start_client(port, response_timeout_ms: 400)
      assert :ok = await_bound(client)

      test_pid = self()

      spawn_link(fn ->
        send(test_pid, {:flushed, Client.send_submit_sm(client, submit_sm("flushed"))})
      end)

      assert :ok = wait_until(fn -> pending_count(client) == 1 end)

      # Let the flushed request's timer come close to firing before unbinding.
      # If unbind leaves it armed it expires almost immediately inside
      # :unbinding, whose catch-all stops the session long before the unbind
      # timer would have. Both timers share response_timeout_ms, so only this
      # head start makes the two outcomes distinguishable in time.
      Process.sleep(320)
      assert :ok = Client.unbind(client)
      assert_receive {:flushed, {:error, :unbinding}}, 500

      Process.sleep(150)
      assert Process.alive?(client), "a stale flushed timer stopped the session during unbind"
      assert Client.status(client) == :unbinding
      stop(client)
      stop(smsc)
    end

    test "unbind resolves pending requests with :unbinding and stops normally after unbind_resp" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)

      test_pid = self()

      spawn_link(fn ->
        send(test_pid, {:pending, Client.send_submit_sm(client, submit_sm("pending"))})
      end)

      assert :ok = wait_until(fn -> pending_count(client) == 1 end)

      assert :ok = Client.unbind(client)
      assert_receive {:pending, {:error, :unbinding}}, 500
      assert :ok = wait_until(fn -> not Process.alive?(client) end)
      stop(smsc)
    end

    test "unbind force-closes and stops when the SMSC withholds unbind_resp" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :unbind)
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)

      assert :ok = Client.unbind(client)
      assert :ok = wait_until(fn -> not Process.alive?(client) end)
      stop(smsc)
    end
  end

  defp pending_count(client) do
    {_state, data} = :sys.get_state(client)
    map_size(data.pending)
  end

  defp pending_bind_seq(client) do
    {_state, data} = :sys.get_state(client)
    [seq] = Map.keys(data.pending)
    seq
  end

  defp await_pdu(ref, command) do
    receive do
      {:fake_smsc_bytes, ^ref, bytes} ->
        case PDU.decode(bytes) do
          {:ok, %PDU{command: ^command}} -> bytes
          _ -> await_pdu(ref, command)
        end
    after
      500 -> flunk("did not receive #{command} from client")
    end
  end

  defp await_submits(_ref, 0), do: :ok

  defp await_submits(ref, remaining) do
    receive do
      {:fake_smsc_bytes, ^ref, bytes} ->
        case PDU.decode(bytes) do
          {:ok, %PDU{command: :submit_sm}} -> await_submits(ref, remaining - 1)
          _ -> await_submits(ref, remaining)
        end
    after
      500 -> {:error, :timeout}
    end
  end
end

defmodule JasminEx.Smpp.RaisingDeliverHandler do
  @moduledoc false
  @behaviour JasminEx.Smpp.DeliverHandler

  @impl true
  def handle_deliver_sm(_pdu, _context), do: raise("handler failure")
end

defmodule JasminEx.Smpp.UnmappedStatusDeliverHandler do
  @moduledoc false
  @behaviour JasminEx.Smpp.DeliverHandler

  # Deliberately outside the encodable command_status set: the client must
  # reject it rather than pass it into PDU encoding.
  @impl true
  def handle_deliver_sm(_pdu, _context), do: {:error, :custom_failure}
end
