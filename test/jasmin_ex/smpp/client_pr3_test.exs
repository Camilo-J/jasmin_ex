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
  @lifecycle_events [
    [:jasmin_ex, :smpp, :bound],
    [:jasmin_ex, :smpp, :disconnected],
    [:jasmin_ex, :smpp, :reconnect_scheduled],
    [:jasmin_ex, :smpp, :rebound]
  ]
  @deliver_failed_event [:jasmin_ex, :smpp, :deliver_sm, :failed]

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

  def handle_telemetry(event, measurements, metadata, %{test_pid: test_pid, blocking: false}) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  def handle_telemetry(event, measurements, metadata, %{test_pid: test_pid, blocking: true}) do
    send(test_pid, {:telemetry_blocked, self(), event, measurements, metadata})

    receive do
      :continue -> :ok
    end
  end

  defp attach_telemetry(events, blocking \\ false) do
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        &__MODULE__.handle_telemetry/4,
        %{test_pid: self(), blocking: blocking}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
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
    pdu_bytes(command, :ESME_ROK, sequence_number, body)
  end

  defp pdu_bytes(command, status, sequence_number, body) do
    PDU.build(command: command, status: status, sequence_number: sequence_number, body: body)
    |> PDU.encode()
    |> IO.iodata_to_binary()
  end

  defp submit_sm_resp_bytes(sequence_number, message_id) do
    {:ok, body} = Body.encode(:submit_sm_resp, %Body.SubmitSMResp{message_id: message_id})
    pdu_bytes(:submit_sm_resp, sequence_number, IO.iodata_to_binary(body))
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

    test "emits initial bound only after the matching response and never emits rebound" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :bind_transmitter)
      attach_telemetry(@lifecycle_events)
      {:ok, client} = start_client(port, response_timeout_ms: 500)
      assert :ok = await_bind_pending(client, smsc)
      refute_receive {:telemetry, [:jasmin_ex, :smpp, :bound], _, _}, 30

      assert :ok =
               FakeSMSC.send_bytes(
                 smsc,
                 pdu_bytes(:bind_transmitter_resp, pending_bind_seq(client), <<0>>)
               )

      assert :ok = await_bound(client)

      assert_receive {:telemetry, [:jasmin_ex, :smpp, :bound], %{},
                      %{client: ^client, bind_as: :transmitter, kind: :initial}},
                     500

      refute_receive {:telemetry, [:jasmin_ex, :smpp, :bound], _, _}, 30
      refute_receive {:telemetry, [:jasmin_ex, :smpp, :rebound], _, _}, 30
      stop(client)
      stop(smsc)
    end

    test "startup retries keep initial bind semantics and expose deterministic scheduling" do
      {port, smsc} = start_smsc(script: %{bind_transmitter: {:reply_status, :ESME_RSYSERR}})
      attach_telemetry(@lifecycle_events)

      {:ok, client} =
        start_client(port,
          reconnect_base_ms: 20,
          reconnect_factor: 2,
          reconnect_cap_ms: 100
        )

      for {attempt, delay_ms} <- [{1, 20}, {2, 40}] do
        assert_receive {:telemetry, [:jasmin_ex, :smpp, :disconnected], %{},
                        %{
                          client: ^client,
                          bind_as: :transmitter,
                          state: :bind_pending,
                          reason: :bind_rejected
                        }},
                       500

        assert_receive {:telemetry, [:jasmin_ex, :smpp, :reconnect_scheduled],
                        %{delay_ms: ^delay_ms},
                        %{
                          client: ^client,
                          bind_as: :transmitter,
                          attempt: ^attempt,
                          reason: :bind_rejected
                        }},
                       500
      end

      :ok = FakeSMSC.auto_reply(smsc, :bind_transmitter)
      assert :ok = await_bound(client)

      assert_receive {:telemetry, [:jasmin_ex, :smpp, :bound], %{},
                      %{client: ^client, bind_as: :transmitter, kind: :initial}},
                     500

      stop(client)
      stop(smsc)
    end

    test "orders disconnect, retry scheduling, and reconnect bound telemetry" do
      {port, smsc} = start_smsc()
      attach_telemetry(@lifecycle_events)
      {:ok, client} = start_client(port, reconnect_base_ms: 30, reconnect_cap_ms: 30)
      assert :ok = await_bound(client)

      assert_receive {:telemetry, [:jasmin_ex, :smpp, :bound], %{}, %{kind: :initial}}, 500

      :ok = FakeSMSC.close_on_command(smsc, :submit_sm)
      assert {:unknown, :disconnected} = Client.send_submit_sm(client, submit_sm("drop"))

      assert_receive {:telemetry, [:jasmin_ex, :smpp, :disconnected], %{},
                      %{
                        client: ^client,
                        bind_as: :transmitter,
                        state: :bound,
                        reason: :tcp_closed
                      }},
                     500

      assert_receive {:telemetry, [:jasmin_ex, :smpp, :reconnect_scheduled], %{delay_ms: 30},
                      %{
                        client: ^client,
                        bind_as: :transmitter,
                        attempt: 1,
                        reason: :tcp_closed
                      }},
                     500

      assert_receive {:telemetry, [:jasmin_ex, :smpp, :bound], %{},
                      %{client: ^client, bind_as: :transmitter, kind: :reconnect}},
                     500

      {_state, data} = :sys.get_state(client)
      assert data.backoff_attempt == 0
      stop(client)
      stop(smsc)
    end

    test "voluntary unbind emits neither disconnect nor retry telemetry" do
      {port, smsc} = start_smsc()

      attach_telemetry([
        [:jasmin_ex, :smpp, :disconnected],
        [:jasmin_ex, :smpp, :reconnect_scheduled]
      ])

      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)
      monitor = Process.monitor(client)
      assert :ok = Client.unbind(client)
      assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 500
      refute_receive {:telemetry, _, _, _}, 50
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

    test "a successful submit response returns its message ID" do
      {port, smsc} = start_smsc()
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)

      assert {:ok, "fake-msg-id"} = Client.send_submit_sm(client, submit_sm("accepted"))
      stop(client)
      stop(smsc)
    end

    test "a local submit write failure returns an unknown send failure exactly once" do
      {port, smsc} = start_smsc()
      {:ok, client} = start_client(port, reconnect_base_ms: 500, reconnect_cap_ms: 500)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)
      closed_socket = closed_tcp_socket()

      :sys.replace_state(client, fn {state, data} ->
        {state, %{data | socket: closed_socket}}
      end)

      caller = spawn_submit_caller(client, submit_sm("send-failed"), 100)

      assert_receive {:submit_result, ^caller, {:unknown, {:send_failed, :closed}}}, 500
      refute_receive {:fake_smsc_pdu, ^ref, %PDU{command: :submit_sm}}, 50
      assert_receive {:no_late_submit_reply, ^caller}, 200
      stop(client)
      stop(smsc)
    end

    test "successful responses with empty or malformed message IDs are unknown exactly once" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port, response_timeout_ms: 80)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      for response_body <- [<<0>>, <<0xFF>>] do
        caller = spawn_submit_caller(client, submit_sm("invalid-response"), 120)
        %PDU{sequence_number: seq} = await_pdu_event(ref, :submit_sm)

        assert :ok =
                 FakeSMSC.send_bytes(
                   smsc,
                   pdu_bytes(:submit_sm_resp, :ESME_ROK, seq, response_body)
                 )

        assert_receive {:submit_result, ^caller, {:unknown, :invalid_response}}, 500
        assert_receive {:no_late_submit_reply, ^caller}, 200
        assert pending_count(client) == 0
      end

      assert Client.status(client) == :bound
      stop(client)
      stop(smsc)
    end

    for status <- [:ESME_RTHROTTLED, :ESME_RINVDSTADR] do
      test "submit rejection #{status} is a definite protocol error" do
        status = unquote(status)
        {port, smsc} = start_smsc()
        :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
        {:ok, client} = start_client(port)
        assert :ok = await_bound(client)
        ref = FakeSMSC.subscribe_pdus(smsc)

        task = Task.async(fn -> Client.send_submit_sm(client, submit_sm("rejected")) end)
        %PDU{sequence_number: seq} = await_pdu_event(ref, :submit_sm)

        assert :ok = FakeSMSC.send_bytes(smsc, pdu_bytes(:submit_sm_resp, status, seq, <<0xFF>>))
        assert {:error, {:submit_rejected, ^status}} = Task.await(task, 500)
        stop(client)
        stop(smsc)
      end
    end

    test "a withheld submit response returns an explicit unknown timeout" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port, response_timeout_ms: 40)
      assert :ok = await_bound(client)

      assert {:unknown, :response_timeout} =
               Client.send_submit_sm(client, submit_sm("withheld"))

      stop(client)
      stop(smsc)
    end

    test "the public call waits for protocol timeouts longer than five seconds" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port, response_timeout_ms: 6_000)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      task =
        Task.async(fn ->
          receive do: (:send -> Client.send_submit_sm(client, submit_sm("long")))
        end)

      :erlang.trace_pattern({:gen_statem, :call, 3}, true, [])
      :erlang.trace(task.pid, true, [:call])

      on_exit(fn ->
        :erlang.trace_pattern({:gen_statem, :call, 3}, false, [])
      end)

      send(task.pid, :send)

      assert_receive {:trace, caller, :call,
                      {:gen_statem, :call, [^client, {:send_submit_sm, _body}, :infinity]}},
                     500

      assert caller == task.pid
      %PDU{sequence_number: seq} = await_pdu_event(ref, :submit_sm)
      assert :ok = FakeSMSC.send_bytes(smsc, submit_sm_resp_bytes(seq, "long-id"))
      assert {:ok, "long-id"} = Task.await(task, 500)
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

    test "SendToPid uses the canonical context key and preserves its owner message" do
      {:ok, handler} = SendToPid.start_link(owner: self())
      client = self()
      pdu = struct(Body.DeliverSM, Map.from_struct(submit_sm("canonical")))

      assert :ok =
               SendToPid.handle_deliver_sm(pdu, %{
                 client: client,
                 handler_context: handler,
                 handler: :deprecated_alias_not_used
               })

      assert_receive {:smpp_deliver_sm, ^pdu, %{client: ^client}}, 500
      stop(handler)
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

      :ok = :sys.suspend(handler)
      assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(78, "after-owner"))

      assert :ok =
               wait_until(fn ->
                 Process.info(handler, :message_queue_len) == {:message_queue_len, 1}
               end)

      owner_ref = Process.monitor(owner)
      Process.exit(owner, :kill)
      assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}, 500

      log =
        capture_log(fn ->
          :ok = :sys.resume(handler)

          assert {:ok,
                  %PDU{
                    command: :deliver_sm_resp,
                    sequence_number: 78,
                    status: :ESME_RX_T_APPN
                  }} = ref |> await_pdu(:deliver_sm_resp) |> PDU.decode()
        end)

      assert log =~ "no live consumer"
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

    test "passes canonical and compatibility handler context keys with the same value" do
      {port, smsc} = start_smsc()

      {:ok, client} =
        start_client(port,
          deliver_handler: {JasminEx.Smpp.CapturingDeliverHandler, self()}
        )

      assert :ok = await_bound(client)
      assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(84, "context"))

      assert_receive {:handler_context,
                      %{
                        client: ^client,
                        handler_context: handler_context,
                        handler: compatibility_context
                      }},
                     500

      assert handler_context == self()
      assert compatibility_context == handler_context
      stop(client)
      stop(smsc)
    end

    test "supports a legacy external handler that pattern-matches the handler alias" do
      {port, smsc} = start_smsc()

      {:ok, client} =
        start_client(port, deliver_handler: {JasminEx.Smpp.LegacyDeliverHandler, self()})

      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe(smsc)

      assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(89, "legacy"))
      assert_receive {:legacy_handler, %Body.DeliverSM{short_message: "legacy"}}, 500

      assert {:ok, %PDU{status: :ESME_ROK, sequence_number: 89}} =
               ref |> await_pdu(:deliver_sm_resp) |> PDU.decode()

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

  describe "deliver_sm failure telemetry" do
    test "emits bounded no-handler failure before responding" do
      assert_delivery_failure(nil, nil, :handler_not_configured, :ESME_RX_T_APPN, 85)
    end

    test "emits bounded unavailable failures without leaking handler details" do
      for handler <- [
            JasminEx.Smpp.RaisingDeliverHandler,
            JasminEx.Smpp.InvalidDeliverHandler
          ] do
        log =
          capture_log(fn ->
            assert_delivery_failure(
              handler,
              :sensitive_handler_context,
              :handler_unavailable,
              :ESME_RX_T_APPN,
              86
            )
          end)

        refute log =~ "sensitive"
        refute log =~ "handler failure sentinel"
      end
    end

    test "emits bounded unencodable-status failure without leaking the status" do
      log =
        capture_log(fn ->
          assert_delivery_failure(
            JasminEx.Smpp.UnmappedStatusDeliverHandler,
            :sensitive_handler_context,
            :unencodable_status,
            :ESME_RSYSERR,
            87
          )
        end)

      refute log =~ "sensitive"
      refute log =~ "custom_failure"
    end

    test "does not report a valid handler negative acknowledgement as a client failure" do
      {port, smsc} = start_smsc()
      attach_telemetry([@deliver_failed_event])

      {:ok, client} =
        start_client(port,
          deliver_handler: {JasminEx.Smpp.NegativeAckDeliverHandler, :opaque}
        )

      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe(smsc)
      assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(88, "negative-ack"))

      assert {:ok, %PDU{status: :ESME_RTHROTTLED}} =
               ref |> await_pdu(:deliver_sm_resp) |> PDU.decode()

      refute_receive {:telemetry, @deliver_failed_event, _, _}, 50
      stop(client)
      stop(smsc)
    end
  end

  describe "graceful unbind" do
    test "rejects invalid effective unbind drain timeouts before starting" do
      for timeout <- [-1, 1.5] do
        message =
          ":unbind_drain_timeout_ms must be a non-negative integer, got: #{inspect(timeout)}"

        assert_raise ArgumentError, message, fn ->
          start_client(1, unbind_drain_timeout_ms: timeout)
        end
      end
    end

    test "zero drain timeout immediately releases unresolved submits" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port, unbind_drain_timeout_ms: 0)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      task = Task.async(fn -> Client.send_submit_sm(client, submit_sm("immediate-deadline")) end)
      assert %PDU{command: :submit_sm} = await_pdu_event(ref, :submit_sm)
      assert :ok = Client.unbind(client)

      assert {:unknown, :unbind_deadline} = Task.await(task, 500)
      assert %PDU{command: :unbind} = await_pdu_event(ref, :unbind)
      stop(smsc)
    end

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

    test "new submits are rejected before bytes are written while existing submits drain" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port, response_timeout_ms: 500, unbind_drain_timeout_ms: 50)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      task = Task.async(fn -> Client.send_submit_sm(client, submit_sm("existing")) end)
      assert %PDU{command: :submit_sm} = await_pdu_event(ref, :submit_sm)

      assert :ok = Client.unbind(client)
      assert {:error, :unbinding} = Client.send_submit_sm(client, submit_sm("new"))
      refute_receive {:fake_smsc_pdu, ^ref, %PDU{command: :submit_sm}}, 30
      assert {:unknown, :unbind_deadline} = Task.await(task, 500)
      assert %PDU{command: :unbind} = await_pdu_event(ref, :unbind)
      stop(smsc)
    end

    test "an existing submit response settles before the wire unbind is sent" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)

      {:ok, client} =
        start_client(port, response_timeout_ms: 500, unbind_drain_timeout_ms: 300)

      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      task = Task.async(fn -> Client.send_submit_sm(client, submit_sm("draining")) end)
      %PDU{sequence_number: seq} = await_pdu_event(ref, :submit_sm)
      assert :ok = Client.unbind(client)
      refute_receive {:fake_smsc_pdu, ^ref, %PDU{command: :unbind}}, 30

      assert :ok = FakeSMSC.send_bytes(smsc, submit_sm_resp_bytes(seq, "drained-id"))
      assert {:ok, "drained-id"} = Task.await(task, 500)
      assert %PDU{command: :unbind} = await_pdu_event(ref, :unbind)
      stop(smsc)
    end

    test "out-of-order submit responses all drain before exactly one wire unbind" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.reorder_responses(smsc, :submit_sm)

      {:ok, client} =
        start_client(port, response_timeout_ms: 500, unbind_drain_timeout_ms: 300)

      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      tasks =
        for message <- ["first", "second"] do
          Task.async(fn -> Client.send_submit_sm(client, submit_sm(message)) end)
        end

      assert %PDU{command: :submit_sm} = await_pdu_event(ref, :submit_sm)
      assert %PDU{command: :submit_sm} = await_pdu_event(ref, :submit_sm)
      assert :ok = Client.unbind(client)
      refute_receive {:fake_smsc_pdu, ^ref, %PDU{command: :unbind}}, 30

      assert :ok = FakeSMSC.release_reordered(smsc)
      assert Enum.map(tasks, &Task.await(&1, 500)) == [{:ok, "fake-msg-id"}, {:ok, "fake-msg-id"}]
      assert %PDU{command: :unbind} = await_pdu_event(ref, :unbind)
      refute_receive {:fake_smsc_pdu, ^ref, %PDU{command: :unbind}}, 50
      stop(smsc)
    end

    test "the drain deadline releases unresolved submits before sending wire unbind" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)

      {:ok, client} =
        start_client(port, response_timeout_ms: 500, unbind_drain_timeout_ms: 40)

      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      task = Task.async(fn -> Client.send_submit_sm(client, submit_sm("deadline")) end)
      assert %PDU{command: :submit_sm} = await_pdu_event(ref, :submit_sm)
      assert :ok = Client.unbind(client)

      assert {:unknown, :unbind_deadline} = Task.await(task, 500)
      assert %PDU{command: :unbind} = await_pdu_event(ref, :unbind)
      stop(smsc)
    end

    test "disconnect while draining releases submits and does not reconnect" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      {:ok, client} = start_client(port, response_timeout_ms: 500)
      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)
      monitor = Process.monitor(client)

      task = Task.async(fn -> Client.send_submit_sm(client, submit_sm("disconnect")) end)
      assert %PDU{command: :submit_sm} = await_pdu_event(ref, :submit_sm)
      assert :ok = Client.unbind(client)
      assert :ok = GenServer.stop(smsc)

      assert {:unknown, :disconnected} = Task.await(task, 500)
      assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 500
      refute Process.alive?(client)
    end

    test "a cancelled submit timer cannot terminate a later drain" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)

      {:ok, client} =
        start_client(port, response_timeout_ms: 120, unbind_drain_timeout_ms: 500)

      assert :ok = await_bound(client)
      ref = FakeSMSC.subscribe_pdus(smsc)

      first = Task.async(fn -> Client.send_submit_sm(client, submit_sm("first")) end)
      %PDU{sequence_number: first_seq} = await_pdu_event(ref, :submit_sm)
      assert :ok = FakeSMSC.send_bytes(smsc, submit_sm_resp_bytes(first_seq, "first-id"))
      assert {:ok, "first-id"} = Task.await(first, 500)

      Process.sleep(90)
      second = Task.async(fn -> Client.send_submit_sm(client, submit_sm("second")) end)
      %PDU{sequence_number: second_seq} = await_pdu_event(ref, :submit_sm)
      assert :ok = Client.unbind(client)
      Process.sleep(40)

      assert Process.alive?(client)
      assert Client.status(client) == :unbinding
      refute_receive {:fake_smsc_pdu, ^ref, %PDU{command: :unbind}}, 20

      assert :ok = FakeSMSC.send_bytes(smsc, submit_sm_resp_bytes(second_seq, "second-id"))
      assert {:ok, "second-id"} = Task.await(second, 500)
      assert %PDU{command: :unbind} = await_pdu_event(ref, :unbind)
      stop(smsc)
    end

    test "wire unbind send failure stops instead of remaining bound" do
      {port, smsc} = start_smsc()
      {:ok, client} = start_client(port)
      assert :ok = await_bound(client)
      monitor = Process.monitor(client)

      :sys.replace_state(client, fn {state, data} -> {state, %{data | socket: nil}} end)

      assert {:error, {:send_failed, _reason}} = Client.unbind(client)
      assert_receive {:DOWN, ^monitor, :process, ^client, :normal}, 500
      refute Process.alive?(client)
      stop(smsc)
    end

    test "a submit timeout during draining cannot be mistaken for the unbind timeout" do
      {port, smsc} = start_smsc()
      :ok = FakeSMSC.withhold_response(smsc, :submit_sm)
      :ok = FakeSMSC.withhold_response(smsc, :unbind)
      {:ok, client} = start_client(port, response_timeout_ms: 100, unbind_drain_timeout_ms: 500)
      assert :ok = await_bound(client)

      test_pid = self()

      spawn_link(fn ->
        send(test_pid, {:flushed, Client.send_submit_sm(client, submit_sm("flushed"))})
      end)

      assert :ok = wait_until(fn -> pending_count(client) == 1 end)
      assert :ok = Client.unbind(client)
      assert_receive {:flushed, {:unknown, :response_timeout}}, 500

      assert Process.alive?(client)
      assert Client.status(client) == :unbinding
      stop(client)
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
    map_size(data.request_window.pending)
  end

  defp assert_delivery_failure(handler, context, reason, response_status, sequence_number) do
    {port, smsc} = start_smsc()
    handler_id = attach_telemetry([@deliver_failed_event], true)

    opts = if handler, do: [deliver_handler: {handler, context}], else: []
    {:ok, client} = start_client(port, opts)
    assert :ok = await_bound(client)
    ref = FakeSMSC.subscribe(smsc)
    assert :ok = FakeSMSC.send_bytes(smsc, deliver_sm_bytes(sequence_number, "sensitive-body"))

    assert_receive {:telemetry_blocked, telemetry_caller, @deliver_failed_event, %{},
                    %{
                      client: ^client,
                      handler: ^handler,
                      reason: ^reason,
                      response_status: ^response_status
                    } = metadata},
                   500

    assert telemetry_caller == client
    assert map_size(metadata) == 4
    refute_receive {:fake_smsc_bytes, ^ref, _response}, 30
    send(telemetry_caller, :continue)

    assert {:ok, %PDU{status: ^response_status, sequence_number: ^sequence_number}} =
             ref |> await_pdu(:deliver_sm_resp) |> PDU.decode()

    :ok = :telemetry.detach(handler_id)
    stop(client)
    stop(smsc)
  end

  defp pending_bind_seq(client) do
    {_state, data} = :sys.get_state(client)
    [seq] = Map.keys(data.request_window.pending)
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

  defp await_pdu_event(ref, command) do
    receive do
      {:fake_smsc_pdu, ^ref, %PDU{command: ^command} = pdu} -> pdu
      {:fake_smsc_pdu, ^ref, _other} -> await_pdu_event(ref, command)
    after
      500 -> flunk("did not receive decoded #{command} from client")
    end
  end

  defp spawn_submit_caller(client, body, late_reply_wait_ms) do
    parent = self()

    spawn(fn ->
      result = Client.send_submit_sm(client, body)
      send(parent, {:submit_result, self(), result})

      receive do
        message -> send(parent, {:late_submit_reply, self(), message})
      after
        late_reply_wait_ms -> send(parent, {:no_late_submit_reply, self()})
      end
    end)
  end

  defp closed_tcp_socket do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, {_address, port}} = :inet.sockname(listener)
    accept = Task.async(fn -> :gen_tcp.accept(listener) end)
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])
    {:ok, peer} = Task.await(accept)

    :ok = :gen_tcp.close(peer)
    :ok = :gen_tcp.close(socket)
    :ok = :gen_tcp.close(listener)
    socket
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
  def handle_deliver_sm(_pdu, _context), do: raise("handler failure sentinel")
end

defmodule JasminEx.Smpp.UnmappedStatusDeliverHandler do
  @moduledoc false
  @behaviour JasminEx.Smpp.DeliverHandler

  # Deliberately outside the encodable command_status set: the client must
  # reject it rather than pass it into PDU encoding.
  @impl true
  def handle_deliver_sm(_pdu, _context), do: {:error, :custom_failure}
end

defmodule JasminEx.Smpp.InvalidDeliverHandler do
  @moduledoc false
  @behaviour JasminEx.Smpp.DeliverHandler

  @impl true
  def handle_deliver_sm(_pdu, _context), do: {:unexpected, :sensitive_return}
end

defmodule JasminEx.Smpp.NegativeAckDeliverHandler do
  @moduledoc false
  @behaviour JasminEx.Smpp.DeliverHandler

  @impl true
  def handle_deliver_sm(_pdu, _context), do: {:error, :ESME_RTHROTTLED}
end

defmodule JasminEx.Smpp.CapturingDeliverHandler do
  @moduledoc false
  @behaviour JasminEx.Smpp.DeliverHandler

  @impl true
  def handle_deliver_sm(_pdu, %{handler_context: test_pid} = context) do
    send(test_pid, {:handler_context, context})
    :ok
  end
end

defmodule JasminEx.Smpp.LegacyDeliverHandler do
  @moduledoc false
  @behaviour JasminEx.Smpp.DeliverHandler

  @impl true
  def handle_deliver_sm(pdu, %{handler: test_pid}) do
    send(test_pid, {:legacy_handler, pdu})
    :ok
  end
end
