defmodule JasminEx.Smpp.Client do
  @moduledoc """
  SMPP 3.4 client session backed by a `:gen_statem` and one TCP socket.
  """

  require Logger

  alias JasminEx.Smpp.Framing
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body

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

  @spec status(pid()) :: :disconnected | :connecting | :bind_pending | :bound
  def status(pid), do: :gen_statem.call(pid, :status, 5_000)

  @spec send_submit_sm(pid(), Body.SubmitSM.t()) :: {:ok, String.t()} | {:error, term()}
  def send_submit_sm(pid, %Body.SubmitSM{} = body),
    do: :gen_statem.call(pid, {:send_submit_sm, body}, 5_000)

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
      target_state: nil,
      exit_reason: nil
    }

    {:ok, :disconnected, data, [reconnect_action(0)]}
  end

  # :disconnected -> :connecting is explicit so lifecycle state reflects the
  # design even though the TCP connect operation itself is synchronous.
  def disconnected({:call, from}, :status, data), do: reply_state(from, :disconnected, data)

  def disconnected({:timeout, :reconnect}, :try_connect, data) do
    {:next_state, :connecting, data, [{:state_timeout, 0, :connect}]}
  end

  def disconnected(_event_type, _event, data), do: {:keep_state, data}

  def connecting({:call, from}, :status, data), do: reply_state(from, :connecting, data)

  def connecting(:state_timeout, :connect, data) do
    case :gen_tcp.connect(
           data.config.host,
           data.config.port,
           [:binary, packet: :raw, active: :once],
           @connect_timeout_ms
         ) do
      {:ok, socket} -> send_bind(%{data | socket: socket, buffer: <<>>})
      {:error, _reason} -> data |> close_socket() |> arm_reconnect()
    end
  end

  def connecting(:info, {:tcp_closed, socket}, %{socket: socket} = data),
    do: data |> close_socket() |> arm_reconnect()

  def connecting(:info, {:tcp_error, socket, _reason}, %{socket: socket} = data),
    do: data |> close_socket() |> arm_reconnect()

  def connecting(_event_type, _event, data), do: {:keep_state, data}

  def bind_pending({:call, from}, :status, data), do: reply_state(from, :bind_pending, data)

  def bind_pending({:timeout, {:pending, seq}}, :pending_timeout, data) do
    data
    |> remove_pending(seq)
    |> close_socket()
    |> arm_reconnect()
  end

  def bind_pending(:info, {:tcp, socket, bytes}, %{socket: socket} = data),
    do: receive_tcp(:bind_pending, data, socket, bytes)

  def bind_pending(:info, {:tcp_closed, socket}, %{socket: socket} = data),
    do: data |> close_socket() |> flush_pending({:error, :disconnected}) |> arm_reconnect()

  def bind_pending(:info, {:tcp_error, socket, _reason}, %{socket: socket} = data),
    do: data |> close_socket() |> flush_pending({:error, :disconnected}) |> arm_reconnect()

  def bind_pending(_event_type, _event, data), do: {:keep_state, data}

  def bound({:call, from}, :status, data), do: reply_state(from, :bound, data)

  def bound({:call, from}, {:send_submit_sm, body}, data) do
    {seq, data} = take_sequence(data)
    pdu = build_submit_pdu(body, seq)

    case :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu))) do
      :ok ->
        pending = Map.put(data.pending, seq, %{from: from, command_id: :submit_sm})
        data = %{data | pending: pending}
        {:keep_state, data, [pending_timeout_action(seq, data.config.response_timeout_ms)]}

      {:error, _reason} ->
        {:keep_state, data, [{:reply, from, {:error, :disconnected}}]}
    end
  end

  def bound({:timeout, {:pending, seq}}, :pending_timeout, data) do
    case Map.pop(data.pending, seq) do
      {nil, _pending} ->
        {:keep_state, data}

      {%{from: nil}, pending} ->
        %{data | pending: pending}
        |> close_socket()
        |> flush_pending({:error, :disconnected})
        |> arm_reconnect()

      {%{from: from}, pending} ->
        :gen_statem.reply(from, {:error, :timeout})
        {:keep_state, %{data | pending: pending}}
    end
  end

  def bound(:state_timeout, :heartbeat, data) do
    case send_heartbeat(data) do
      {:ok, data, action} ->
        {:keep_state, data, [action, {:state_timeout, data.config.heartbeat_ms, :heartbeat}]}

      :error ->
        data |> close_socket() |> flush_pending({:error, :disconnected}) |> arm_reconnect()
    end
  end

  def bound(:info, {:tcp, socket, bytes}, %{socket: socket} = data),
    do: receive_tcp(:bound, data, socket, bytes)

  def bound(:info, {:tcp_closed, socket}, %{socket: socket} = data),
    do: data |> close_socket() |> flush_pending({:error, :disconnected}) |> arm_reconnect()

  def bound(:info, {:tcp_error, socket, _reason}, %{socket: socket} = data),
    do: data |> close_socket() |> flush_pending({:error, :disconnected}) |> arm_reconnect()

  def bound(_event_type, _event, data), do: {:keep_state, data}

  defp send_bind(data) do
    {seq, data} = take_sequence(data)
    pdu = build_bind_pdu(data.config, seq)

    case :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu))) do
      :ok ->
        pending = Map.put(data.pending, seq, %{from: nil, command_id: :bind_transmitter})

        {:next_state, :bind_pending, %{data | pending: pending},
         [pending_timeout_action(seq, data.config.response_timeout_ms)]}

      {:error, _reason} ->
        data |> close_socket() |> arm_reconnect()
    end
  end

  defp receive_tcp(state, data, socket, bytes) do
    {pdus, leftover} = Framing.feed(<<>>, data.buffer <> bytes)

    data =
      Enum.reduce(pdus, %{data | buffer: leftover}, fn pdu_bin, acc ->
        dispatch_pdu(acc, pdu_bin, state)
      end)

    safe_setopts_once(socket)
    settle_inbound(state, data)
  end

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

  defp apply_pdu(data, %PDU{command: :submit_sm_resp, sequence_number: seq, body: body}, :bound) do
    case Map.pop(data.pending, seq) do
      {nil, _pending} ->
        log_unknown_response(:submit_sm_resp, seq)
        data

      {%{command_id: :submit_sm, from: from}, pending} ->
        :gen_statem.reply(from, {:ok, extract_message_id(body)})
        %{data | pending: pending, cancelled: [seq | data.cancelled]}

      {_entry, _pending} ->
        log_unknown_response(:submit_sm_resp, seq)
        data
    end
  end

  defp apply_pdu(data, %PDU{command: :enquire_link, sequence_number: seq}, :bound) do
    send_wire(data, enquire_link_resp(seq))
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
    data = %{data | target_state: nil, backoff_attempt: 0}

    {:next_state, :bound, data,
     cancellation_actions(data) ++ [{:state_timeout, data.config.heartbeat_ms, :heartbeat}]}
  end

  defp settle_inbound(:bind_pending, %{target_state: :disconnected} = data) do
    %{data | target_state: nil} |> close_socket() |> arm_reconnect()
  end

  defp settle_inbound(_state, data),
    do: {:keep_state, clear_cancellations(data), cancellation_actions(data)}

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

  defp arm_reconnect(data) do
    actions = cancellation_actions(data) ++ [reconnect_action(backoff_delay(data))]

    {:next_state, :disconnected,
     %{clear_cancellations(data) | backoff_attempt: data.backoff_attempt + 1}, actions}
  end

  defp reconnect_action(delay), do: {{:timeout, :reconnect}, delay, :try_connect}
  defp reply_state(from, state, data), do: {:keep_state, data, [{:reply, from, state}]}
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

  defp log_unknown_response(command, seq),
    do: Logger.warning("unknown sequence_number #{seq} for #{inspect(command)}, discarding")

  defp enquire_link_resp(seq),
    do:
      PDU.build(command: :enquire_link_resp, status: :ESME_ROK, sequence_number: seq, body: <<>>)

  defp extract_message_id(body) do
    case Body.decode(:submit_sm_resp, body) do
      {:ok, %Body.SubmitSMResp{message_id: id}} -> id
      _ -> ""
    end
  end

  defp backoff_delay(%{
         config: %{reconnect: %{base_ms: base, factor: factor, cap_ms: cap, jitter: jitter}},
         backoff_attempt: attempt
       }) do
    delay = min(base * Integer.pow(factor, attempt), cap)
    if jitter, do: :rand.uniform(delay), else: delay
  end

  defp build_config(opts) do
    %{
      host: Keyword.fetch!(opts, :host),
      port: Keyword.fetch!(opts, :port),
      system_id: Keyword.fetch!(opts, :system_id),
      password: Keyword.fetch!(opts, :password),
      system_type: Keyword.fetch!(opts, :system_type),
      bind_as: Keyword.fetch!(opts, :bind_as),
      heartbeat_ms: Keyword.get(opts, :heartbeat_ms, @default_heartbeat_ms),
      response_timeout_ms: Keyword.get(opts, :response_timeout_ms, @default_response_timeout_ms),
      reconnect: %{
        base_ms: Keyword.get(opts, :reconnect_base_ms, @default_reconnect_base_ms),
        factor: Keyword.get(opts, :reconnect_factor, @default_reconnect_factor),
        cap_ms: Keyword.get(opts, :reconnect_cap_ms, @default_reconnect_cap_ms),
        jitter: Keyword.get(opts, :reconnect_jitter, @default_jitter)
      }
    }
  end
end
