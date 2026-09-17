defmodule JasminEx.Smpp.Client.DeliverSMDispatch do
  @moduledoc false

  require Logger

  alias JasminEx.Dlr.Event
  alias JasminEx.Dlr.Receipt
  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.Smpp.PDU.Constants

  @type handler_config :: {module() | nil, term()}
  @type dlr_context :: %{
          required(:connector_id) => String.t(),
          required(:publisher) => {module(), term()},
          optional(:dlr_expiry) => pos_integer(),
          optional(:clock) => {module(), term()}
        }

  @spec dispatch(binary(), handler_config(), pid()) :: Constants.command_status()
  @spec dispatch(binary(), handler_config(), pid(), dlr_context() | nil) ::
          Constants.command_status()
  def dispatch(body, handler_config, client, dlr_context \\ nil) do
    case decode_deliver_sm(body) do
      {:ok, pdu} -> dispatch_decoded(pdu, handler_config, client, dlr_context)
      :error -> :ESME_RSYSERR
    end
  end

  defp dispatch_decoded(pdu, handler_config, client, nil) do
    invoke_deliver_handler(handler_config, pdu, client)
  end

  defp dispatch_decoded(pdu, handler_config, client, dlr_context) do
    case Receipt.parse(pdu) do
      {:ok, receipt} -> publish_receipt(receipt, dlr_context)
      :not_dlr -> invoke_deliver_handler(handler_config, pdu, client)
      {:error, _reason} -> :ESME_RINVOPTPARSTREAM
    end
  end

  defp publish_receipt(receipt, context) do
    now = now_ms(context)
    expiry = Map.get(context, :dlr_expiry, 86_400)

    event = %{
      kind: :deliver_sm,
      connector_id: context.connector_id,
      receipt: receipt,
      observed_at_ms: now,
      deadline_ms: now + expiry * 1000
    }

    with {:ok, payload} <- Event.encode(event),
         :ok <- publish(context.publisher, payload) do
      :ESME_ROK
    else
      _other -> :ESME_RX_T_APPN
    end
  end

  defp publish({module, publisher_context}, payload) do
    module.publish(publisher_context, "dlr.deliver_sm", payload)
  end

  defp now_ms(%{clock: {module, clock_context}}), do: module.now_ms(clock_context)
  defp now_ms(_context), do: System.system_time(:millisecond)

  defp decode_deliver_sm(body) do
    case Body.decode(:deliver_sm, body) do
      {:ok, %Body.DeliverSM{} = pdu} -> {:ok, pdu}
      _other -> :error
    end
  rescue
    _error ->
      Logger.error("deliver_sm body failed to decode")
      :error
  end

  # No handler means nothing consumed the message, so acknowledging it would
  # silently discard real MO traffic. Let the SMSC redeliver once one is wired.
  defp invoke_deliver_handler({nil, _context}, _pdu, client) do
    Logger.error(
      "deliver_sm received but no deliver_handler is configured; asking the SMSC to retry"
    )

    deliver_failure(client, nil, :handler_not_configured, :ESME_RX_T_APPN)
  end

  defp invoke_deliver_handler({handler, context}, pdu, client) do
    case handler.handle_deliver_sm(pdu, %{
           client: client,
           handler_context: context,
           handler: context
         }) do
      :ok -> :ESME_ROK
      {:error, status} -> encodable_status(client, handler, status)
      _other -> handler_unavailable(client, handler)
    end
  rescue
    _error -> handler_unavailable(client, handler)
  catch
    _kind, _reason -> handler_unavailable(client, handler)
  end

  defp handler_unavailable(client, handler) do
    Logger.error("deliver_sm handler unavailable; asking the SMSC to retry")
    deliver_failure(client, handler, :handler_unavailable, :ESME_RX_T_APPN)
  end

  # An unmapped status would raise later during response encoding and kill the
  # client, so reject it here and preserve session liveness.
  defp encodable_status(client, handler, status) when is_atom(status) do
    case Constants.command_status_to_int(status) do
      {:ok, _int} -> status
      :error -> unencodable_status(client, handler)
    end
  end

  defp encodable_status(client, handler, _status), do: unencodable_status(client, handler)

  defp unencodable_status(client, handler) do
    Logger.warning("deliver_sm handler returned an unencodable status; responding :ESME_RSYSERR")
    deliver_failure(client, handler, :unencodable_status, :ESME_RSYSERR)
  end

  defp deliver_failure(client, handler, reason, response_status) do
    :telemetry.execute(
      [:jasmin_ex, :smpp, :deliver_sm, :failed],
      %{},
      %{
        client: client,
        handler: handler,
        reason: reason,
        response_status: response_status
      }
    )

    response_status
  end
end
