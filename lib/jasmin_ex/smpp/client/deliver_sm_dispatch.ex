defmodule JasminEx.Smpp.Client.DeliverSMDispatch do
  @moduledoc false

  require Logger

  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.Smpp.PDU.Constants

  @type handler_config :: {module() | nil, term()}

  @spec dispatch(binary(), handler_config(), pid()) :: Constants.command_status()
  def dispatch(body, handler_config, client) do
    case decode_deliver_sm(body) do
      {:ok, pdu} -> invoke_deliver_handler(handler_config, pdu, client)
      :error -> :ESME_RSYSERR
    end
  end

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
