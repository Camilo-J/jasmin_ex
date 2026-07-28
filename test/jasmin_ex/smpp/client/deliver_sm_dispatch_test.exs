defmodule JasminEx.Smpp.Client.DeliverSMDispatchTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias __MODULE__.ExitingHandler
  alias __MODULE__.InvalidHandler
  alias __MODULE__.RaisingHandler
  alias __MODULE__.RecordingHandler
  alias __MODULE__.StatusHandler
  alias __MODULE__.ThrowingHandler
  alias JasminEx.Smpp.Client.DeliverSMDispatch
  alias JasminEx.Smpp.PDU.Body

  @failed_event [:jasmin_ex, :smpp, :deliver_sm, :failed]

  test "decodes and invokes the handler with canonical and compatibility context" do
    body = deliver_sm_body("hello")

    assert :ESME_ROK =
             DeliverSMDispatch.dispatch(body, {RecordingHandler, self()}, self())

    assert_receive {:handled, %Body.DeliverSM{short_message: "hello"}, context}
    assert context.client == self()
    assert context.handler_context == self()
    assert context.handler == context.handler_context
  end

  test "malformed bodies return a system error without invoking the handler" do
    attach_telemetry()

    assert :ESME_RSYSERR =
             DeliverSMDispatch.dispatch(<<0xFF, 0xFE>>, {RecordingHandler, self()}, self())

    refute_receive {:handled, _pdu, _context}
    refute_receive {:delivery_failed, _metadata}
  end

  test "missing, failed, and invalid handlers ask for redelivery with bounded failures" do
    attach_telemetry()
    client = self()

    log =
      capture_log(fn ->
        assert :ESME_RX_T_APPN = DeliverSMDispatch.dispatch(deliver_sm_body(), {nil, nil}, client)

        assert_receive {:delivery_failed,
                        %{
                          client: ^client,
                          handler: nil,
                          reason: :handler_not_configured,
                          response_status: :ESME_RX_T_APPN
                        }}

        for handler <- [RaisingHandler, ThrowingHandler, ExitingHandler, InvalidHandler] do
          assert :ESME_RX_T_APPN =
                   DeliverSMDispatch.dispatch(deliver_sm_body(), {handler, :sensitive}, client)

          assert_receive {:delivery_failed,
                          %{
                            client: ^client,
                            handler: ^handler,
                            reason: :handler_unavailable,
                            response_status: :ESME_RX_T_APPN
                          }}
        end
      end)

    refute log =~ "sensitive"
    refute log =~ "failure sentinel"
  end

  test "valid negative acknowledgements pass through while unencodable statuses degrade safely" do
    attach_telemetry()
    client = self()

    assert :ESME_RTHROTTLED =
             DeliverSMDispatch.dispatch(
               deliver_sm_body(),
               {StatusHandler, :ESME_RTHROTTLED},
               client
             )

    refute_receive {:delivery_failed, _metadata}

    for status <- [:custom_failure, {:not, :an_atom}] do
      assert :ESME_RSYSERR =
               DeliverSMDispatch.dispatch(deliver_sm_body(), {StatusHandler, status}, client)

      assert_receive {:delivery_failed,
                      %{
                        client: ^client,
                        handler: StatusHandler,
                        reason: :unencodable_status,
                        response_status: :ESME_RSYSERR
                      }}
    end
  end

  def handle_telemetry(@failed_event, %{}, metadata, test_pid) do
    send(test_pid, {:delivery_failed, metadata})
  end

  defp attach_telemetry do
    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach(handler_id, @failed_event, &__MODULE__.handle_telemetry/4, self())
    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp deliver_sm_body(message \\ "message") do
    body = %Body.DeliverSM{short_message: message}
    {:ok, encoded} = Body.encode(:deliver_sm, body)
    IO.iodata_to_binary(encoded)
  end

  defmodule RecordingHandler do
    def handle_deliver_sm(pdu, %{handler_context: test_pid} = context) do
      send(test_pid, {:handled, pdu, context})
      :ok
    end
  end

  defmodule RaisingHandler do
    def handle_deliver_sm(_pdu, _context), do: raise("failure sentinel")
  end

  defmodule ThrowingHandler do
    def handle_deliver_sm(_pdu, _context), do: throw(:failure_sentinel)
  end

  defmodule ExitingHandler do
    def handle_deliver_sm(_pdu, _context), do: exit(:failure_sentinel)
  end

  defmodule InvalidHandler do
    def handle_deliver_sm(_pdu, _context), do: {:unexpected, :failure_sentinel}
  end

  defmodule StatusHandler do
    def handle_deliver_sm(_pdu, %{handler_context: status}), do: {:error, status}
  end
end
