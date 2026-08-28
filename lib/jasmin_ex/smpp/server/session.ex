defmodule JasminEx.Smpp.Server.Session do
  @moduledoc false
  @behaviour :gen_statem

  alias JasminEx.Routing
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.Smpp.Server.{BindingManager, Transport}

  @roles %{bind_transmitter: :tx, bind_receiver: :rx, bind_transceiver: :trx}
  @resps %{
    bind_transmitter: :bind_transmitter_resp,
    bind_receiver: :bind_receiver_resp,
    bind_transceiver: :bind_transceiver_resp
  }

  def start_link(opts), do: :gen_statem.start_link(__MODULE__, opts, [])
  def handoff(pid, socket), do: :gen_statem.call(pid, {:handoff, socket})
  def callback_mode, do: :handle_event_function

  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok, :awaiting_socket,
     %{
       socket: nil,
       buffer: <<>>,
       router: Keyword.fetch!(opts, :router),
       manager: Keyword.fetch!(opts, :binding_manager),
       max: Keyword.get(opts, :max_pdu_length, 65_536)
     }}
  end

  def handle_event({:call, from}, {:handoff, socket}, :awaiting_socket, data) do
    :ok = Transport.own(socket)
    {:next_state, :awaiting_bind, %{data | socket: socket}, [{:reply, from, :ok}]}
  end

  def handle_event(:info, {:tcp, sock, chunk}, state, %{socket: sock} = data)
      when state in [:awaiting_bind, :bound] do
    case Transport.feed(data.buffer, chunk, data.max) do
      {:error, _} ->
        Transport.close(sock)
        {:stop, :normal, data}

      {:ok, frames, buffer} ->
        :ok = Transport.activate(sock)
        dispatch(%{data | buffer: buffer}, frames, state)
    end
  end

  def handle_event(:info, {:tcp_closed, _}, _state, data), do: {:stop, :normal, data}
  def handle_event(:info, {:tcp_error, _, _}, _state, data), do: {:stop, :normal, data}
  def handle_event(:internal, :stop, :unbinding, data), do: {:stop, :normal, data}

  def terminate(_reason, _state, data) do
    try do
      BindingManager.release(data.manager, self())
    catch
      :exit, _ -> :ok
    end

    if data.socket, do: Transport.close(data.socket)
    :ok
  end

  defp dispatch(data, [], state), do: {:next_state, state, data}

  defp dispatch(data, [frame | rest], state) do
    case PDU.decode(frame) do
      {:error, _} ->
        Transport.close(data.socket)
        {:stop, :normal, data}

      {:ok, pdu} ->
        case handle_pdu(data, pdu, state) do
          {:stop, _, _} = stop -> stop
          {:next_state, new_state, data, actions} -> {:next_state, new_state, data, actions}
          {:next_state, new_state, data} -> dispatch(data, rest, new_state)
          {:keep_state, data} -> dispatch(data, rest, state)
        end
    end
  end

  defp handle_pdu(data, %{command: cmd} = pdu, :awaiting_bind) when is_map_key(@roles, cmd) do
    bind(data, pdu, @roles[cmd], @resps[cmd])
  end

  defp handle_pdu(data, %{command: :submit_sm, sequence_number: seq}, _state) do
    reply(data, :submit_sm_resp, :ESME_RSUBMITFAIL, seq, <<0>>)
    {:keep_state, data}
  end

  defp handle_pdu(data, %{command: :enquire_link, sequence_number: seq}, :bound) do
    reply(data, :enquire_link_resp, :ESME_ROK, seq, <<>>)
    {:keep_state, data}
  end

  defp handle_pdu(data, %{command: :unbind, sequence_number: seq}, :bound) do
    reply(data, :unbind_resp, :ESME_ROK, seq, <<>>)
    {:next_state, :unbinding, data, [{:next_event, :internal, :stop}]}
  end

  defp handle_pdu(data, _pdu, _state), do: {:keep_state, data}

  defp bind(data, pdu, role, resp_cmd) do
    with {:ok, body} <- Body.decode(pdu.command, pdu.body),
         {:ok, user} <- Routing.authenticate_smpp(data.router, body.system_id, body.password),
         :ok <-
           BindingManager.register(data.manager, body.system_id, role, self(), user.max_bindings) do
      {:ok, resp_body} = Body.encode(resp_cmd, %Body.BindResp{system_id: body.system_id})
      reply(data, resp_cmd, :ESME_ROK, pdu.sequence_number, resp_body)
      {:next_state, :bound, data}
    else
      _ ->
        reply(data, resp_cmd, :ESME_RBINDFAIL, pdu.sequence_number, <<0>>)
        Transport.close(data.socket)
        {:stop, :normal, data}
    end
  end

  defp reply(data, command, status, seq, body) do
    Transport.send_pdu(
      data.socket,
      PDU.build(command: command, status: status, sequence_number: seq, body: body)
    )
  end
end
