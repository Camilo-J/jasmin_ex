defmodule JasminEx.Smpp.Client do
  @moduledoc """
  SMPP 3.4 client session backed by a `:gen_statem` and one TCP socket.

  ## Public API

    * `start_link/1` starts a session and begins connection attempts.
    * `status/1` reports the lifecycle state.
    * `send_submit_sm/2` sends a request while bound and returns an accepted,
      rejected, known-not-sent, or unknown outcome from its sequence-matched
      response lifecycle. Acceptance by the SMSC does not mean handset delivery.
    * `unbind/1` closes the session deliberately after an SMPP unbind exchange.

  Optional `lifecycle_notify` is a pid or registered atom name that receives
  ordered session hooks after a successful bind and after a later bind loss.
  Failed startup and voluntary unbind do not emit these messages.

  ## Lifecycle states

  The session progresses through `:disconnected`, `:connecting`,
  `:bind_pending`, and `:bound`. A voluntary `unbind/1` enters `:unbinding`
  and terminates normally after an unbind response, socket close, or bounded
  timeout; it does not schedule reconnect. Involuntary TCP loss returns to
  `:disconnected` and schedules reconnect with backoff.

  ## Telemetry

  Telemetry handlers execute synchronously in the client process. The client
  emits these events:

    * `[:jasmin_ex, :smpp, :bound]` has measurements `%{}` and metadata
      `%{client: pid, bind_as: atom, connector_id: String.t(), kind: :initial |
      :reconnect}`. Bound means the SMSC session reached `:bound`; it does not
      mean a message was delivered. Failed startup attempts do not change `kind`
      from `:initial`.
    * `[:jasmin_ex, :smpp, :disconnected]` has measurements `%{}` and metadata
      `%{client: pid, bind_as: atom, connector_id: String.t(), state: atom,
      reason: atom}`. `state` is the involuntarily exited lifecycle state and
      `reason` is a bounded category.
    * `[:jasmin_ex, :smpp, :reconnect_scheduled]` has measurements
      `%{delay_ms: non_neg_integer}` and metadata
      `%{client: pid, bind_as: atom, connector_id: String.t(), attempt: pos_integer,
      reason: atom}`. It is emitted when the retry timer is scheduled; a
      successful bind resets the next attempt to 1.
    * `[:jasmin_ex, :smpp, :deliver_sm, :failed]` has measurements `%{}` and
      metadata
      `%{client: pid, handler: module | nil, reason: :handler_not_configured |
      :handler_unavailable | :unencodable_status, response_status: atom}`. It
      reports a local client failure before `deliver_sm_resp` is sent. A valid
      handler negative acknowledgement is not a client failure.

  Disconnect and retry reasons are bounded categories such as
  `:connect_failed`, `:bind_send_failed`, `:bind_timeout`, `:bind_rejected`,
  `:heartbeat_send_failed`, `:heartbeat_timeout`, `:tcp_closed`, `:tcp_error`,
  and `:submit_send_failed`; raw socket and exception terms are never included.
  Initial state and voluntary unbind emit neither disconnect nor retry events.

  When `lifecycle_notify` is a pid or registered atom name, a successful bind
  sends `{:smpp_bound, connector_id, kind}` with `kind` `:initial` or
  `:reconnect`. Involuntary loss of a bound session first replies to any
  in-flight submit, then sends `{:smpp_bind_lost, connector_id, reason}`
  before reconnect scheduling.
  """

  require Logger

  alias JasminEx.Smpp.Client.Config
  alias JasminEx.Smpp.Client.DeliverSMDispatch
  alias JasminEx.Smpp.Client.ReconnectPolicy
  alias JasminEx.Smpp.Client.RequestWindow
  alias JasminEx.Smpp.Client.Transport
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.Smpp.PDU.Constants

  @behaviour :gen_statem

  @connect_timeout_ms 5_000
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts), do: :gen_statem.start_link(__MODULE__, Config.new!(opts), [])

  @spec status(pid()) :: :disconnected | :connecting | :bind_pending | :bound | :unbinding
  def status(pid), do: :gen_statem.call(pid, :status, 5_000)

  @type submit_unknown_reason ::
          :response_timeout
          | :disconnected
          | :unbind_deadline
          | :invalid_response
          | {:send_failed, term()}

  @type submit_result ::
          {:ok, String.t()}
          | {:error, {:submit_rejected, Constants.command_status()} | :disconnected | :unbinding}
          | {:unknown, submit_unknown_reason()}

  @spec send_submit_sm(pid(), Body.SubmitSM.t()) :: submit_result()
  def send_submit_sm(pid, %Body.SubmitSM{} = body),
    do: :gen_statem.call(pid, {:send_submit_sm, body}, :infinity)

  @spec unbind(pid()) :: :ok | {:error, term()}
  def unbind(pid), do: :gen_statem.call(pid, :unbind, 5_000)

  @impl true
  def callback_mode, do: :state_functions

  @impl true
  def init(config) do
    data = %{
      config: config,
      socket: nil,
      buffer: <<>>,
      request_window: RequestWindow.new(),
      backoff_attempt: 0,
      ever_bound: false,
      target_state: nil,
      exit_reason: nil,
      unbind_phase: nil
    }

    {:ok, :disconnected, data, [reconnect_action(0)]}
  end

  # :disconnected -> :connecting is explicit so lifecycle state reflects the
  # design even though the TCP connect operation itself is synchronous.
  def disconnected({:call, from}, :status, data), do: reply_state(from, :disconnected, data)

  def disconnected({:call, from}, {:send_submit_sm, _body}, data),
    do: reply_disconnected(from, data)

  def disconnected({:call, from}, :unbind, data), do: reply_disconnected(from, data)

  def disconnected({:timeout, :reconnect}, :try_connect, data) do
    {:next_state, :connecting, data, [{:state_timeout, 0, :connect}]}
  end

  def disconnected(_event_type, _event, data), do: {:keep_state, data}

  def connecting({:call, from}, :status, data), do: reply_state(from, :connecting, data)

  def connecting({:call, from}, {:send_submit_sm, _body}, data),
    do: reply_disconnected(from, data)

  def connecting({:call, from}, :unbind, data), do: reply_disconnected(from, data)

  def connecting(:state_timeout, :connect, data) do
    case Transport.connect(data.config.host, data.config.port, @connect_timeout_ms) do
      {:ok, socket} -> send_bind(%{data | socket: socket, buffer: <<>>})
      {:error, _reason} -> data |> close_socket() |> arm_reconnect(:connecting, :connect_failed)
    end
  end

  def connecting(:info, {:tcp_closed, socket}, %{socket: socket} = data),
    do: data |> close_socket() |> arm_reconnect(:connecting, :tcp_closed)

  def connecting(:info, {:tcp_error, socket, reason}, %{socket: socket} = data) do
    log_tcp_error(reason)
    data |> close_socket() |> arm_reconnect(:connecting, :tcp_error)
  end

  def connecting(_event_type, _event, data), do: {:keep_state, data}

  def bind_pending({:call, from}, :status, data), do: reply_state(from, :bind_pending, data)

  def bind_pending({:call, from}, {:send_submit_sm, _body}, data),
    do: reply_disconnected(from, data)

  def bind_pending({:call, from}, :unbind, data), do: reply_disconnected(from, data)

  def bind_pending({:timeout, {:pending, seq}}, :pending_timeout, data) do
    data
    |> expire_pending(seq)
    |> close_socket()
    |> arm_reconnect(:bind_pending, :bind_timeout)
  end

  def bind_pending(:info, {:tcp, socket, bytes}, %{socket: socket} = data),
    do: receive_tcp(:bind_pending, data, socket, bytes)

  def bind_pending(:info, {:tcp_closed, socket}, %{socket: socket} = data),
    do:
      data
      |> close_socket()
      |> flush_pending({:error, :disconnected})
      |> arm_reconnect(:bind_pending, :tcp_closed)

  def bind_pending(:info, {:tcp_error, socket, reason}, %{socket: socket} = data) do
    log_tcp_error(reason)

    data
    |> close_socket()
    |> flush_pending({:error, :disconnected})
    |> arm_reconnect(:bind_pending, :tcp_error)
  end

  def bind_pending(_event_type, _event, data), do: {:keep_state, data}

  def bound({:call, from}, :status, data), do: reply_state(from, :bound, data)

  def bound({:call, from}, :unbind, data) do
    data = drop_internal_pending(data)
    {cancels, data} = drain_cancellations(data)

    if pending_submits?(data) do
      {:next_state, :unbinding, %{data | unbind_phase: :draining},
       [{:reply, from, :ok}] ++ cancels ++ [unbind_drain_action(data)]}
    else
      case send_unbind(data) do
        {:ok, data, action} ->
          {:next_state, :unbinding, data, [{:reply, from, :ok}] ++ cancels ++ [action]}

        {:error, reason, data} ->
          {:stop_and_reply, :normal, [{:reply, from, {:error, {:send_failed, reason}}}],
           close_socket(data)}
      end
    end
  end

  def bound({:call, from}, {:send_submit_sm, body}, data) do
    {seq, data} = take_sequence(data)
    pdu = build_submit_pdu(body, seq)

    case Transport.send(data.socket, pdu) do
      :ok ->
        data = insert_pending(data, seq, :submit_sm, from)
        {:keep_state, data, [pending_timeout_action(seq, data.config.response_timeout_ms)]}

      {:error, reason} ->
        :gen_statem.reply(from, {:unknown, {:send_failed, reason}})

        data
        |> close_socket()
        |> flush_pending({:unknown, :disconnected})
        |> arm_reconnect(:bound, :submit_send_failed)
    end
  end

  def bound({:timeout, {:pending, seq}}, :pending_timeout, data) do
    case expire_pending_entry(data, seq) do
      {nil, data} ->
        {:keep_state, data}

      {%{from: nil}, data} ->
        data
        |> close_socket()
        |> flush_pending({:unknown, :disconnected})
        |> arm_reconnect(:bound, :heartbeat_timeout)

      {%{command_id: :submit_sm, from: from}, data} ->
        :gen_statem.reply(from, {:unknown, :response_timeout})
        {:keep_state, data}
    end
  end

  def bound(:state_timeout, :heartbeat, data) do
    case send_heartbeat(data) do
      {:ok, data, action} ->
        {:keep_state, data, [action, {:state_timeout, data.config.heartbeat_ms, :heartbeat}]}

      :error ->
        data
        |> close_socket()
        |> flush_pending({:unknown, :disconnected})
        |> arm_reconnect(:bound, :heartbeat_send_failed)
    end
  end

  def bound(:info, {:tcp, socket, bytes}, %{socket: socket} = data),
    do: receive_tcp(:bound, data, socket, bytes)

  def bound(:info, {:tcp_closed, socket}, %{socket: socket} = data),
    do:
      data
      |> close_socket()
      |> flush_pending({:unknown, :disconnected})
      |> arm_reconnect(:bound, :tcp_closed)

  def bound(:info, {:tcp_error, socket, reason}, %{socket: socket} = data) do
    log_tcp_error(reason)

    data
    |> close_socket()
    |> flush_pending({:unknown, :disconnected})
    |> arm_reconnect(:bound, :tcp_error)
  end

  def bound(_event_type, _event, data), do: {:keep_state, data}

  def unbinding({:call, from}, :status, data), do: reply_state(from, :unbinding, data)

  def unbinding({:call, from}, {:send_submit_sm, _body}, data),
    do: {:keep_state, data, [{:reply, from, {:error, :unbinding}}]}

  def unbinding({:call, from}, :unbind, data),
    do: {:keep_state, data, [{:reply, from, {:error, :unbinding}}]}

  def unbinding({:timeout, {:pending, seq}}, :pending_timeout, data) do
    case expire_pending_entry(data, seq) do
      {%{command_id: :submit_sm, from: from}, data} ->
        :gen_statem.reply(from, {:unknown, :response_timeout})
        continue_unbinding(data)

      {%{command_id: :unbind}, data} when data.unbind_phase == :waiting_for_response ->
        {:stop, :normal, close_socket(data)}

      {_stale_or_unrelated, data} ->
        {:keep_state, data}
    end
  end

  def unbinding({:timeout, :unbind_drain}, :deadline, %{unbind_phase: :draining} = data) do
    data = flush_submit_pending(data, {:unknown, :unbind_deadline})
    {actions, data} = drain_cancellations(data)

    case send_unbind(data) do
      {:ok, data, action} -> {:keep_state, data, actions ++ [action]}
      {:error, _reason, data} -> {:stop, :normal, close_socket(data)}
    end
  end

  def unbinding({:timeout, :unbind_drain}, :deadline, data), do: {:keep_state, data}

  def unbinding(:info, {:tcp, socket, bytes}, %{socket: socket} = data),
    do: receive_tcp(:unbinding, data, socket, bytes)

  def unbinding(:info, {:tcp_closed, socket}, %{socket: socket} = data),
    do:
      {:stop, :normal, data |> flush_submit_pending({:unknown, :disconnected}) |> close_socket()}

  def unbinding(:info, {:tcp_error, socket, reason}, %{socket: socket} = data) do
    log_tcp_error(reason)
    {:stop, :normal, data |> flush_submit_pending({:unknown, :disconnected}) |> close_socket()}
  end

  def unbinding(_event_type, _event, data), do: {:keep_state, data}

  defp send_bind(data) do
    {seq, data} = take_sequence(data)
    pdu = build_bind_pdu(data.config, seq)

    case Transport.send(data.socket, pdu) do
      :ok ->
        data = insert_pending(data, seq, bind_as_command(data.config.bind_as), nil)

        {:next_state, :bind_pending, data,
         [pending_timeout_action(seq, data.config.response_timeout_ms)]}

      {:error, _reason} ->
        data |> close_socket() |> arm_reconnect(:connecting, :bind_send_failed)
    end
  end

  defp receive_tcp(state, data, socket, bytes) do
    {pdus, leftover} = Transport.decode(data.buffer, bytes)

    {data, _last_state} =
      Enum.reduce(pdus, {%{data | buffer: leftover}, state}, fn pdu, {acc, acc_state} ->
        acc = apply_pdu(acc, pdu, acc_state)
        {acc, effective_state(acc_state, acc)}
      end)

    safe_setopts_once(socket)
    settle_inbound(state, data)
  end

  # The lifecycle state transition settles only once the whole TCP read is
  # processed, so PDUs the SMSC coalesced behind a successful bind response
  # must still be dispatched against the state that response established.
  # Otherwise a deliver_sm sharing the bind_resp packet is silently dropped.
  defp effective_state(:bind_pending, %{target_state: :bound}), do: :bound
  defp effective_state(state, _data), do: state

  defp apply_pdu(
         data,
         %PDU{command: command, sequence_number: seq, status: status},
         :bind_pending
       ) do
    bind_command = bind_request_for(command)

    case RequestWindow.resolve(data.request_window, seq, bind_command) do
      {:ok, _entry, request_window} when not is_nil(bind_command) ->
        data = %{data | request_window: request_window}

        case status do
          :ESME_ROK -> %{data | target_state: :bound}
          _ -> %{data | target_state: :disconnected, exit_reason: :bind_rejected}
        end

      {:error, _request_window} ->
        log_unknown_response(command, seq)
        data
    end
  end

  defp apply_pdu(data, %PDU{command: :enquire_link_resp, sequence_number: seq}, :bound),
    do: resolve_internal_response(data, :enquire_link, seq)

  defp apply_pdu(
         data,
         %PDU{command: :submit_sm_resp, sequence_number: seq} = pdu,
         state
       )
       when state in [:bound, :unbinding] do
    case RequestWindow.resolve(data.request_window, seq, :submit_sm) do
      {:ok, %{from: from}, request_window} ->
        :gen_statem.reply(from, RequestWindow.classify_submit_response(pdu))
        %{data | request_window: request_window}

      {:error, _request_window} ->
        log_unknown_response(:submit_sm_resp, seq)
        data
    end
  end

  defp apply_pdu(data, %PDU{command: :enquire_link, sequence_number: seq}, :bound) do
    send_wire(data, enquire_link_resp(seq))
  end

  defp apply_pdu(data, %PDU{command: :deliver_sm, sequence_number: seq, body: body}, state)
       when state in [:bound, :unbinding] do
    status = DeliverSMDispatch.dispatch(body, data.config.deliver_handler, self())

    send_wire(
      data,
      PDU.build(command: :deliver_sm_resp, status: status, sequence_number: seq, body: <<0>>)
    )
  end

  defp apply_pdu(data, %PDU{command: :enquire_link, sequence_number: seq}, :unbinding),
    do: send_wire(data, enquire_link_resp(seq))

  defp apply_pdu(data, %PDU{command: :unbind_resp, sequence_number: seq}, :unbinding) do
    case RequestWindow.resolve(data.request_window, seq, :unbind) do
      {:ok, _entry, request_window} ->
        %{data | request_window: request_window, target_state: :unbound}

      {:error, _request_window} ->
        log_unknown_response(:unbind_resp, seq)
        data
    end
  end

  defp apply_pdu(data, %PDU{command: command, sequence_number: seq}, _state)
       when command in [
              :bind_transmitter_resp,
              :bind_receiver_resp,
              :bind_transceiver_resp,
              :submit_sm_resp,
              :enquire_link_resp,
              :generic_nack
            ] do
    log_unknown_response(command, seq)
    data
  end

  defp apply_pdu(data, _pdu, _state), do: data

  defp settle_inbound(:bind_pending, %{target_state: :bound} = data) do
    kind = if data.ever_bound, do: :reconnect, else: :initial
    emit_lifecycle(:bound, data, %{kind: kind})
    notify_lifecycle(data, {:smpp_bound, data.config.connector_id, kind})
    data = %{data | target_state: nil, backoff_attempt: 0, ever_bound: true}

    {cancellations, data} = drain_cancellations(data)

    {:next_state, :bound, data,
     cancellations ++ [{:state_timeout, data.config.heartbeat_ms, :heartbeat}]}
  end

  defp settle_inbound(:bind_pending, %{target_state: :disconnected} = data) do
    %{data | target_state: nil}
    |> close_socket()
    |> arm_reconnect(:bind_pending, :bind_rejected)
  end

  defp settle_inbound(:unbinding, %{target_state: :unbound} = data),
    do: {:stop, :normal, close_socket(data)}

  defp settle_inbound(:unbinding, data), do: continue_unbinding(data)

  defp settle_inbound(_state, data) do
    {actions, data} = drain_cancellations(data)
    {:keep_state, data, actions}
  end

  defp resolve_internal_response(data, command_id, seq) do
    case RequestWindow.resolve(data.request_window, seq, command_id) do
      {:ok, %{from: nil}, request_window} ->
        %{data | request_window: request_window}

      _unmatched ->
        log_unknown_response(:enquire_link_resp, seq)
        data
    end
  end

  defp send_heartbeat(%{socket: nil}), do: :error

  defp send_heartbeat(data) do
    {seq, data} = take_sequence(data)
    pdu = PDU.build(command: :enquire_link, status: :ESME_ROK, sequence_number: seq, body: <<>>)

    case Transport.send(data.socket, pdu) do
      :ok ->
        data = insert_pending(data, seq, :enquire_link, nil)

        {:ok, data, pending_timeout_action(seq, data.config.response_timeout_ms)}

      {:error, _reason} ->
        :error
    end
  end

  defp build_bind_pdu(config, seq) do
    command = bind_as_command(config.bind_as)

    body = %Body.Bind{
      system_id: config.system_id,
      password: config.password,
      system_type: config.system_type,
      interface_version: 0x34,
      addr_ton: :UNKNOWN,
      addr_npi: :UNKNOWN,
      address_range: ""
    }

    {:ok, body_bin} = Body.encode(command, body)
    PDU.build(command: command, status: :ESME_ROK, sequence_number: seq, body: body_bin)
  end

  defp build_submit_pdu(body, seq) do
    {:ok, body_bin} = Body.encode(:submit_sm, body)
    PDU.build(command: :submit_sm, status: :ESME_ROK, sequence_number: seq, body: body_bin)
  end

  defp take_sequence(data) do
    {sequence, request_window} = RequestWindow.take_sequence(data.request_window)
    {sequence, %{data | request_window: request_window}}
  end

  defp pending_timeout_action(seq, timeout),
    do: RequestWindow.pending_timeout_action(seq, timeout)

  defp unbind_drain_action(data),
    do: {{:timeout, :unbind_drain}, data.config.unbind_drain_timeout_ms, :deadline}

  defp flush_pending(data, reply) do
    {request_window, directives} = RequestWindow.flush(data.request_window, reply)
    Enum.each(directives, fn {from, pending_reply} -> reply_to_pending(from, pending_reply) end)
    %{data | request_window: request_window}
  end

  defp flush_submit_pending(data, reply) do
    {request_window, directives} = RequestWindow.flush_submits(data.request_window, reply)
    Enum.each(directives, fn {from, pending_reply} -> reply_to_pending(from, pending_reply) end)
    %{data | request_window: request_window}
  end

  defp drop_internal_pending(data),
    do: %{data | request_window: RequestWindow.drop_internal(data.request_window)}

  defp pending_submits?(data),
    do: RequestWindow.pending_submits?(data.request_window)

  defp insert_pending(data, seq, command_id, from),
    do: %{data | request_window: RequestWindow.insert(data.request_window, seq, command_id, from)}

  defp expire_pending(data, seq) do
    {_entry, data} = expire_pending_entry(data, seq)
    data
  end

  defp expire_pending_entry(data, seq) do
    {entry, request_window} = RequestWindow.expire(data.request_window, seq)
    {entry, %{data | request_window: request_window}}
  end

  defp drain_cancellations(data) do
    {actions, request_window} = RequestWindow.drain_cancellations(data.request_window)
    {actions, %{data | request_window: request_window}}
  end

  defp continue_unbinding(%{unbind_phase: :draining} = data) do
    {actions, data} = drain_cancellations(data)

    if pending_submits?(data) do
      {:keep_state, data, actions}
    else
      case send_unbind(data) do
        {:ok, data, action} ->
          {:keep_state, data, actions ++ [{{:timeout, :unbind_drain}, :cancel}, action]}

        {:error, _reason, data} ->
          {:stop, :normal, close_socket(data)}
      end
    end
  end

  defp continue_unbinding(data) do
    {actions, data} = drain_cancellations(data)
    {:keep_state, data, actions}
  end

  defp send_unbind(data) do
    {seq, data} = take_sequence(data)
    pdu = PDU.build(command: :unbind, status: :ESME_ROK, sequence_number: seq, body: <<>>)

    case safe_tcp_send(data.socket, pdu) do
      :ok ->
        data = %{
          insert_pending(data, seq, :unbind, nil)
          | unbind_phase: :waiting_for_response
        }

        {:ok, data, pending_timeout_action(seq, data.config.response_timeout_ms)}

      {:error, reason} ->
        {:error, reason, data}
    end
  end

  defp arm_reconnect(data, state, reason) do
    if state == :bound do
      notify_lifecycle(data, {:smpp_bind_lost, data.config.connector_id, reason})
    end

    attempt = data.backoff_attempt + 1
    delay = backoff_delay(data)
    emit_lifecycle(:disconnected, data, %{state: state, reason: reason})

    :telemetry.execute(
      [:jasmin_ex, :smpp, :reconnect_scheduled],
      %{delay_ms: delay},
      lifecycle_metadata(data, %{attempt: attempt, reason: reason})
    )

    {cancellations, data} = drain_cancellations(data)
    actions = cancellations ++ [reconnect_action(delay)]

    {:next_state, :disconnected, %{data | backoff_attempt: attempt}, actions}
  end

  defp reconnect_action(delay), do: {{:timeout, :reconnect}, delay, :try_connect}
  defp reply_state(from, state, data), do: {:keep_state, data, [{:reply, from, state}]}

  defp reply_disconnected(from, data),
    do: {:keep_state, data, [{:reply, from, {:error, :disconnected}}]}

  defp reply_to_pending(nil, _reply), do: :ok
  defp reply_to_pending(from, reply), do: :gen_statem.reply(from, reply)
  defp close_socket(%{socket: nil} = data), do: %{data | buffer: <<>>}

  defp close_socket(%{socket: socket} = data) do
    Transport.close(socket)
    %{data | socket: nil, buffer: <<>>}
  rescue
    _ -> %{data | socket: nil, buffer: <<>>}
  end

  defp send_wire(%{socket: nil} = data, _pdu), do: data

  defp send_wire(data, pdu),
    do:
      (
        Transport.send(data.socket, pdu)
        data
      )

  defp safe_setopts_once(socket) do
    Transport.activate_once(socket)
  rescue
    _ -> :ok
  end

  defp bind_as_command(:transmitter), do: :bind_transmitter
  defp bind_as_command(:receiver), do: :bind_receiver
  defp bind_as_command(:transceiver), do: :bind_transceiver
  defp bind_request_for(:bind_transmitter_resp), do: :bind_transmitter
  defp bind_request_for(:bind_receiver_resp), do: :bind_receiver
  defp bind_request_for(:bind_transceiver_resp), do: :bind_transceiver
  defp bind_request_for(_command), do: nil

  defp log_unknown_response(command, seq) do
    Logger.warning(%{
      message: "late or unmatched SMPP response discarded",
      command: command,
      sequence_number: seq
    })
  end

  defp log_tcp_error(reason), do: Logger.warning("SMPP TCP error: #{inspect(reason)}")

  defp notify_lifecycle(%{config: %{lifecycle_notify: pid}}, message) when is_pid(pid),
    do: send(pid, message)

  defp notify_lifecycle(%{config: %{lifecycle_notify: name}}, message)
       when is_atom(name) and not is_nil(name) do
    case Process.whereis(name) do
      pid when is_pid(pid) -> send(pid, message)
      nil -> :ok
    end
  end

  defp notify_lifecycle(_data, _message), do: :ok

  defp emit_lifecycle(event, data, metadata),
    do:
      :telemetry.execute(
        [:jasmin_ex, :smpp, event],
        %{},
        lifecycle_metadata(data, metadata)
      )

  defp lifecycle_metadata(data, metadata),
    do:
      Map.merge(
        %{client: self(), bind_as: data.config.bind_as, connector_id: data.config.connector_id},
        metadata
      )

  defp enquire_link_resp(seq),
    do:
      PDU.build(command: :enquire_link_resp, status: :ESME_ROK, sequence_number: seq, body: <<>>)

  defp safe_tcp_send(socket, pdu) do
    Transport.send(socket, pdu)
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp backoff_delay(%{config: config, backoff_attempt: attempt}),
    do: ReconnectPolicy.delay(config.reconnect, attempt, &:rand.uniform/1)
end
