defmodule JasminEx.Smpp.DeliverHandler do
  @moduledoc """
  Port for handling inbound SMPP `deliver_sm` PDUs.

  Handlers run in the client session process, so they must return quickly. Move
  slow work to another process; `SendToPid` is the built-in asynchronous handoff.

  Configure a client with `{handler_module, handler_context}`. The context is
  opaque to the client. The callback receives `:handler_context` as the
  canonical key and the deprecated `:handler` compatibility alias with the
  exact same value: `%{client: client_pid, handler_context: context, handler: context}`.

  A handler returns `:ok` to acknowledge the delivery with `:ESME_ROK`, or
  `{:error, status}` to negative-acknowledge it. The status must be an
  encodable SMPP command status; the client rejects anything else and responds
  `:ESME_RSYSERR` rather than letting PDU encoding kill the session.
  """

  alias JasminEx.Smpp.PDU.Body.DeliverSM
  alias JasminEx.Smpp.PDU.Constants

  @type handler_context :: term()
  @type config :: {module(), handler_context()}
  @type client_context :: %{
          required(:client) => pid(),
          required(:handler_context) => handler_context(),
          required(:handler) => handler_context()
        }

  @callback handle_deliver_sm(DeliverSM.t(), client_context()) ::
              :ok | {:error, Constants.command_status()}
end

defmodule JasminEx.Smpp.DeliverHandler.SendToPid do
  @moduledoc """
  A `DeliverHandler` adapter that forwards deliveries to an owner process.

  Configure the client with `{SendToPid, server_pid}`. The callback receives
  `server_pid` through the canonical `:handler_context` key and does not use the
  deprecated `:handler` alias. The owner receives exactly
  `{:smpp_deliver_sm, pdu, %{client: client_pid}}`. The adapter monitors the
  owner; if it dies, each subsequent delivery is warning-logged and
  negative-acknowledged with `:ESME_RX_T_APPN` so the SMSC retries it later.
  Acknowledging an undelivered PDU would drop the message permanently. Use
  `set_owner/2` to attach a replacement owner. Deliveries while no owner is
  live are not queued.
  """
  use GenServer

  require Logger

  @behaviour JasminEx.Smpp.DeliverHandler

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, Keyword.get(opts, :owner))

  @spec set_owner(pid(), pid() | nil) :: :ok
  def set_owner(server, owner), do: GenServer.call(server, {:set_owner, owner})

  @impl JasminEx.Smpp.DeliverHandler
  @spec handle_deliver_sm(
          JasminEx.Smpp.PDU.Body.DeliverSM.t(),
          JasminEx.Smpp.DeliverHandler.client_context()
        ) ::
          :ok | {:error, :ESME_RX_T_APPN}
  def handle_deliver_sm(pdu, %{client: client, handler_context: server}) do
    GenServer.call(server, {:deliver, pdu, %{client: client}})
  end

  @impl true
  def init(owner), do: {:ok, monitor_owner(owner)}

  @impl true
  def handle_call({:set_owner, owner}, _from, state) do
    _ = clear_owner(state)
    {:reply, :ok, monitor_owner(owner)}
  end

  def handle_call({:deliver, pdu, context}, _from, %{owner: owner} = state) when is_pid(owner) do
    send(owner, {:smpp_deliver_sm, pdu, context})
    {:reply, :ok, state}
  end

  def handle_call({:deliver, _pdu, _context}, _from, state) do
    Logger.warning("deliver_sm received with no live consumer; asking the SMSC to retry")
    {:reply, {:error, :ESME_RX_T_APPN}, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor_ref: ref} = state) do
    Logger.warning("deliver_sm owner went down; no live consumer remains")
    {:noreply, %{state | owner: nil, monitor_ref: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp monitor_owner(owner) when is_pid(owner) do
    %{owner: owner, monitor_ref: Process.monitor(owner)}
  end

  defp monitor_owner(_owner), do: %{owner: nil, monitor_ref: nil}

  defp clear_owner(%{monitor_ref: nil} = state), do: state

  defp clear_owner(%{monitor_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    %{state | owner: nil, monitor_ref: nil}
  end
end
