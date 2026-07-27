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
      `%{client: pid, bind_as: atom, kind: :initial | :reconnect}`. Bound means
      the SMSC session reached `:bound`; it does not mean a message was
      delivered. Failed startup attempts do not change `kind` from `:initial`.
    * `[:jasmin_ex, :smpp, :disconnected]` has measurements `%{}` and metadata
      `%{client: pid, bind_as: atom, state: atom, reason: atom}`. `state` is the
      involuntarily exited lifecycle state and `reason` is a bounded category.
    * `[:jasmin_ex, :smpp, :reconnect_scheduled]` has measurements
      `%{delay_ms: non_neg_integer}` and metadata
      `%{client: pid, bind_as: atom, attempt: pos_integer, reason: atom}`. It is
      emitted when the retry timer is scheduled; a successful bind resets the
      next attempt to 1.
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
  """

  require Logger

  alias JasminEx.Smpp.Framing
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body
  alias JasminEx.Smpp.PDU.Constants

  @behaviour :gen_statem

  @default_heartbeat_ms 30_000
  @default_response_timeout_ms 5_000
  @default_reconnect_base_ms 1_000
  @default_reconnect_factor 2
  @default_reconnect_cap_ms 30_000
  @default_jitter true
  @connect_timeout_ms 5_000
  @seq_wrap 0x7FFF_FFFF

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts), do: :gen_statem.start_link(__MODULE__, build_config(opts), [])

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
      seq: 1,
      pending: %{},
      cancelled: [],
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
    case :gen_tcp.connect(
           data.config.host,
           data.config.port,
           [:binary, packet: :raw, active: :once],
           @connect_timeout_ms
         ) do
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
    |> remove_pending(seq)
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
    cancels = cancellation_actions(data)
    data = clear_cancellations(data)

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

    case :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu))) do
      :ok ->
        pending = Map.put(data.pending, seq, %{from: from, command_id: :submit_sm})
        data = %{data | pending: pending}
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
    case Map.pop(data.pending, seq) do
      {nil, _pending} ->
        {:keep_state, data}

      {%{from: nil}, pending} ->
        %{data | pending: pending}
        |> close_socket()
        |> flush_pending({:unknown, :disconnected})
        |> arm_reconnect(:bound, :heartbeat_timeout)

      {%{command_id: :submit_sm, from: from}, pending} ->
        :gen_statem.reply(from, {:unknown, :response_timeout})
        {:keep_state, %{data | pending: pending}}
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
    case Map.pop(data.pending, seq) do
      {%{command_id: :submit_sm, from: from}, pending} ->
        :gen_statem.reply(from, {:unknown, :response_timeout})
        continue_unbinding(%{data | pending: pending})

      {%{command_id: :unbind}, _pending} when data.unbind_phase == :waiting_for_response ->
        {:stop, :normal, close_socket(data)}

      _stale_or_unrelated ->
        {:keep_state, data}
    end
  end

  def unbinding({:timeout, :unbind_drain}, :deadline, %{unbind_phase: :draining} = data) do
    data = flush_submit_pending(data, {:unknown, :unbind_deadline})
    actions = cancellation_actions(data)

    case send_unbind(clear_cancellations(data)) do
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

    case :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu))) do
      :ok ->
        pending =
          Map.put(data.pending, seq, %{
            from: nil,
            command_id: bind_as_command(data.config.bind_as)
          })

        {:next_state, :bind_pending, %{data | pending: pending},
         [pending_timeout_action(seq, data.config.response_timeout_ms)]}

      {:error, _reason} ->
        data |> close_socket() |> arm_reconnect(:connecting, :bind_send_failed)
    end
  end

  defp receive_tcp(state, data, socket, bytes) do
    {pdus, leftover} = Framing.feed(<<>>, data.buffer <> bytes)

    {data, _last_state} =
      Enum.reduce(pdus, {%{data | buffer: leftover}, state}, fn pdu_bin, {acc, acc_state} ->
        acc = dispatch_pdu(acc, pdu_bin, acc_state)
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

  defp dispatch_pdu(data, pdu_bin, state) do
    case PDU.decode(pdu_bin) do
      {:ok, pdu} -> apply_pdu(data, pdu, state)
      {:error, _reason} -> data
    end
  end

  defp apply_pdu(
         data,
         %PDU{command: command, sequence_number: seq, status: status},
         :bind_pending
       ) do
    case {bind_request_for(command), Map.get(data.pending, seq)} do
      {bind_command, %{command_id: bind_command}} when not is_nil(bind_command) ->
        data = cancel_pending(data, seq)

        case status do
          :ESME_ROK -> %{data | target_state: :bound}
          _ -> %{data | target_state: :disconnected, exit_reason: :bind_rejected}
        end

      _ ->
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
    case Map.get(data.pending, seq) do
      %{command_id: :submit_sm, from: from} ->
        :gen_statem.reply(from, submit_response(pdu))
        cancel_pending(data, seq)

      _unmatched ->
        log_unknown_response(:submit_sm_resp, seq)
        data
    end
  end

  defp apply_pdu(data, %PDU{command: :enquire_link, sequence_number: seq}, :bound) do
    send_wire(data, enquire_link_resp(seq))
  end

  defp apply_pdu(data, %PDU{command: :deliver_sm, sequence_number: seq, body: body}, state)
       when state in [:bound, :unbinding] do
    status = dispatch_deliver_sm(data, body)

    send_wire(
      data,
      PDU.build(command: :deliver_sm_resp, status: status, sequence_number: seq, body: <<0>>)
    )
  end

  defp apply_pdu(data, %PDU{command: :enquire_link, sequence_number: seq}, :unbinding),
    do: send_wire(data, enquire_link_resp(seq))

  defp apply_pdu(data, %PDU{command: :unbind_resp, sequence_number: seq}, :unbinding) do
    case Map.get(data.pending, seq) do
      %{command_id: :unbind} ->
        %{data | target_state: :unbound}

      _ ->
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
    data = %{data | target_state: nil, backoff_attempt: 0, ever_bound: true}

    {:next_state, :bound, data,
     cancellation_actions(data) ++ [{:state_timeout, data.config.heartbeat_ms, :heartbeat}]}
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
    actions = cancellation_actions(data)
    {:keep_state, clear_cancellations(data), actions}
  end

  defp resolve_internal_response(data, command_id, seq) do
    case Map.get(data.pending, seq) do
      %{command_id: ^command_id, from: nil} ->
        cancel_pending(data, seq)

      _ ->
        log_unknown_response(:enquire_link_resp, seq)
        data
    end
  end

  defp send_heartbeat(%{socket: nil}), do: :error

  defp send_heartbeat(data) do
    {seq, data} = take_sequence(data)
    pdu = PDU.build(command: :enquire_link, status: :ESME_ROK, sequence_number: seq, body: <<>>)

    case :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu))) do
      :ok ->
        pending = Map.put(data.pending, seq, %{from: nil, command_id: :enquire_link})

        {:ok, %{data | pending: pending},
         pending_timeout_action(seq, data.config.response_timeout_ms)}

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

  defp take_sequence(data), do: {data.seq, %{data | seq: next_seq(data.seq)}}
  defp next_seq(seq) when seq >= @seq_wrap, do: 1
  defp next_seq(seq), do: seq + 1

  defp pending_timeout_action(seq, timeout),
    do: {{:timeout, {:pending, seq}}, timeout, :pending_timeout}

  defp unbind_drain_action(data),
    do: {{:timeout, :unbind_drain}, data.config.unbind_drain_timeout_ms, :deadline}

  defp cancellation_actions(data),
    do: Enum.map(data.cancelled, &{{:timeout, {:pending, &1}}, :cancel})

  defp clear_cancellations(data), do: %{data | cancelled: []}

  defp cancel_pending(data, seq),
    do: %{data | pending: Map.delete(data.pending, seq), cancelled: [seq | data.cancelled]}

  defp remove_pending(data, seq), do: %{data | pending: Map.delete(data.pending, seq)}

  defp flush_pending(data, reply) do
    Enum.each(data.pending, fn {_seq, %{from: from}} -> reply_to_pending(from, reply) end)
    %{data | pending: %{}, cancelled: Map.keys(data.pending) ++ data.cancelled}
  end

  defp flush_submit_pending(data, reply) do
    Enum.reduce(data.pending, data, fn
      {seq, %{command_id: :submit_sm, from: from}}, acc ->
        reply_to_pending(from, reply)
        cancel_pending(acc, seq)

      {_seq, _entry}, acc ->
        acc
    end)
  end

  defp drop_internal_pending(data) do
    Enum.reduce(data.pending, data, fn
      {_seq, %{command_id: :submit_sm}}, acc -> acc
      {seq, _entry}, acc -> cancel_pending(acc, seq)
    end)
  end

  defp pending_submits?(data),
    do: Enum.any?(data.pending, fn {_seq, entry} -> entry.command_id == :submit_sm end)

  defp continue_unbinding(%{unbind_phase: :draining} = data) do
    actions = cancellation_actions(data)
    data = clear_cancellations(data)

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
    actions = cancellation_actions(data)
    {:keep_state, clear_cancellations(data), actions}
  end

  defp send_unbind(data) do
    {seq, data} = take_sequence(data)
    pdu = PDU.build(command: :unbind, status: :ESME_ROK, sequence_number: seq, body: <<>>)

    case safe_tcp_send(data.socket, IO.iodata_to_binary(PDU.encode(pdu))) do
      :ok ->
        pending = Map.put(data.pending, seq, %{from: nil, command_id: :unbind})
        data = %{data | pending: pending, unbind_phase: :waiting_for_response}
        {:ok, data, pending_timeout_action(seq, data.config.response_timeout_ms)}

      {:error, reason} ->
        {:error, reason, data}
    end
  end

  defp arm_reconnect(data, state, reason) do
    attempt = data.backoff_attempt + 1
    delay = backoff_delay(data)
    emit_lifecycle(:disconnected, data, %{state: state, reason: reason})

    :telemetry.execute(
      [:jasmin_ex, :smpp, :reconnect_scheduled],
      %{delay_ms: delay},
      lifecycle_metadata(data, %{attempt: attempt, reason: reason})
    )

    actions = cancellation_actions(data) ++ [reconnect_action(delay)]

    {:next_state, :disconnected, %{clear_cancellations(data) | backoff_attempt: attempt}, actions}
  end

  defp reconnect_action(delay), do: {{:timeout, :reconnect}, delay, :try_connect}
  defp reply_state(from, state, data), do: {:keep_state, data, [{:reply, from, state}]}

  defp reply_disconnected(from, data),
    do: {:keep_state, data, [{:reply, from, {:error, :disconnected}}]}

  defp reply_to_pending(nil, _reply), do: :ok
  defp reply_to_pending(from, reply), do: :gen_statem.reply(from, reply)
  defp close_socket(%{socket: nil} = data), do: %{data | buffer: <<>>}

  defp close_socket(%{socket: socket} = data) do
    :gen_tcp.close(socket)
    %{data | socket: nil, buffer: <<>>}
  rescue
    _ -> %{data | socket: nil, buffer: <<>>}
  end

  defp send_wire(%{socket: nil} = data, _pdu), do: data

  defp send_wire(data, pdu),
    do:
      (
        :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu)))
        data
      )

  defp safe_setopts_once(socket) do
    :inet.setopts(socket, active: :once)
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

  # The two failure causes answer differently on purpose. A body that does not
  # decode is a malformed PDU and will not decode on redelivery either, so it
  # gets a system error. A handler that fails is our side being momentarily
  # unable to process a well-formed message, so it asks for redelivery instead
  # of dropping it.
  defp dispatch_deliver_sm(data, body) do
    case decode_deliver_sm(body) do
      {:ok, pdu} -> invoke_deliver_handler(data, pdu)
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
  # silently discard real MO traffic. Treat it as the local misconfiguration it
  # is and let the SMSC redeliver once a handler is wired up.
  defp invoke_deliver_handler(%{config: %{deliver_handler: {nil, _context}}}, _pdu) do
    Logger.error(
      "deliver_sm received but no deliver_handler is configured; asking the SMSC to retry"
    )

    deliver_failure(nil, :handler_not_configured, :ESME_RX_T_APPN)
  end

  defp invoke_deliver_handler(%{config: %{deliver_handler: {handler, context}}}, pdu) do
    case handler.handle_deliver_sm(pdu, %{
           client: self(),
           handler_context: context,
           handler: context
         }) do
      :ok -> :ESME_ROK
      {:error, status} -> encodable_status(handler, status)
      _other -> handler_unavailable(handler)
    end
  rescue
    _error -> handler_unavailable(handler)
  catch
    _kind, _reason -> handler_unavailable(handler)
  end

  defp handler_unavailable(handler) do
    Logger.error("deliver_sm handler unavailable; asking the SMSC to retry")
    deliver_failure(handler, :handler_unavailable, :ESME_RX_T_APPN)
  end

  # A handler may return any atom. Encoding an unmapped one raises inside
  # PDU.build/1 — outside this dispatch path's rescue — which would kill the
  # session instead of answering the SMSC, so it degrades to a system error.
  defp encodable_status(handler, status) when is_atom(status) do
    case Constants.command_status_to_int(status) do
      {:ok, _int} -> status
      :error -> unencodable_status(handler)
    end
  end

  defp encodable_status(handler, _status), do: unencodable_status(handler)

  defp unencodable_status(handler) do
    Logger.warning("deliver_sm handler returned an unencodable status; responding :ESME_RSYSERR")
    deliver_failure(handler, :unencodable_status, :ESME_RSYSERR)
  end

  defp deliver_failure(handler, reason, response_status) do
    :telemetry.execute(
      [:jasmin_ex, :smpp, :deliver_sm, :failed],
      %{},
      %{
        client: self(),
        handler: handler,
        reason: reason,
        response_status: response_status
      }
    )

    response_status
  end

  defp emit_lifecycle(event, data, metadata),
    do:
      :telemetry.execute(
        [:jasmin_ex, :smpp, event],
        %{},
        lifecycle_metadata(data, metadata)
      )

  defp lifecycle_metadata(data, metadata),
    do: Map.merge(%{client: self(), bind_as: data.config.bind_as}, metadata)

  defp enquire_link_resp(seq),
    do:
      PDU.build(command: :enquire_link_resp, status: :ESME_ROK, sequence_number: seq, body: <<>>)

  defp submit_response(%PDU{status: status}) when status != :ESME_ROK,
    do: {:error, {:submit_rejected, status}}

  defp submit_response(%PDU{body: body}) do
    case Body.decode(:submit_sm_resp, body) do
      {:ok, %Body.SubmitSMResp{message_id: id}} when is_binary(id) and byte_size(id) > 0 ->
        {:ok, id}

      _invalid ->
        {:unknown, :invalid_response}
    end
  end

  defp safe_tcp_send(socket, bytes) do
    :gen_tcp.send(socket, bytes)
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp backoff_delay(%{
         config: %{reconnect: %{base_ms: base, factor: factor, cap_ms: cap, jitter: jitter}},
         backoff_attempt: attempt
       }) do
    delay = min(base * Integer.pow(factor, attempt), cap)
    if jitter, do: :rand.uniform(delay), else: delay
  end

  defp build_config(opts) do
    response_timeout_ms = Keyword.get(opts, :response_timeout_ms, @default_response_timeout_ms)

    unbind_drain_timeout_ms =
      opts
      |> Keyword.get(:unbind_drain_timeout_ms, response_timeout_ms)
      |> validate_unbind_drain_timeout!()

    %{
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port),
      system_id: Keyword.fetch!(opts, :system_id),
      password: Keyword.fetch!(opts, :password),
      system_type: Keyword.fetch!(opts, :system_type),
      bind_as: Keyword.fetch!(opts, :bind_as),
      heartbeat_ms: Keyword.get(opts, :heartbeat_ms, @default_heartbeat_ms),
      response_timeout_ms: response_timeout_ms,
      unbind_drain_timeout_ms: unbind_drain_timeout_ms,
      reconnect: %{
        base_ms: Keyword.get(opts, :reconnect_base_ms, @default_reconnect_base_ms),
        factor: Keyword.get(opts, :reconnect_factor, @default_reconnect_factor),
        cap_ms: Keyword.get(opts, :reconnect_cap_ms, @default_reconnect_cap_ms),
        jitter: Keyword.get(opts, :reconnect_jitter, @default_jitter)
      },
      deliver_handler: normalize_deliver_handler(Keyword.get(opts, :deliver_handler))
    }
  end

  defp validate_unbind_drain_timeout!(timeout) when is_integer(timeout) and timeout >= 0,
    do: timeout

  defp validate_unbind_drain_timeout!(timeout) do
    raise ArgumentError,
          ":unbind_drain_timeout_ms must be a non-negative integer, got: #{inspect(timeout)}"
  end

  defp normalize_deliver_handler({handler, context}) when is_atom(handler), do: {handler, context}

  defp normalize_deliver_handler(nil), do: {nil, nil}
end
