defmodule JasminEx.Smpp.Client do
  @moduledoc """
  SMPP 3.4 client session — a `:gen_statem` owning a single `:gen_tcp`
  socket to an SMSC.

  ## Public API (PR2 surface)

    * `start_link/1`  — boot the session.
    * `status/1`      — current lifecycle state (`:disconnected`,
      `:connecting`, `:bind_pending`, or `:bound`).

  Submit/recv semantics, windowing, `deliver_sm` dispatch, and graceful
  unbind land in PR3. This module ships enough for the bind lifecycle,
  mid-flow socket drops, heartbeat (enquire_link), reconnect with
  exponential backoff, and the pending-window cleanup invariant.
  """
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

  @bind_timeout_ms 5_000
  @connect_timeout_ms 5_000
  @bind_seq 1
  @seq_wrap 0x8000_0000

  ## ── public API ────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, build_config(opts), [])
  end

  @spec status(pid()) :: :disconnected | :connecting | :bind_pending | :bound
  def status(pid), do: :gen_statem.call(pid, :status, 5_000)

  ## ── gen_statem callbacks ──────────────────────────────────────────────────

  @impl true
  def callback_mode, do: :handle_event_function

  @impl true
  def init(config) do
    {:ok, :disconnected,
     %{
       config: config,
       socket: nil,
       buffer: <<>>,
       seq: 1,
       pending: %{},
       backoff_attempt: 0,
       target_state: nil,
       exit_reason: nil
     }, [{{:timeout, :reconnect}, 0, :try_connect}]}
  end

  @impl true
  def handle_event({:call, from}, :status, state, data) do
    {:keep_state, data, [{:reply, from, state}]}
  end

  @impl true
  def handle_event({:call, _from}, _msg, _state, data), do: {:keep_state, data, []}

  @impl true
  def handle_event({:timeout, :reconnect}, :try_connect, :disconnected, data) do
    try_connect(data)
  end

  @impl true
  def handle_event({:timeout, :reconnect}, :try_connect, :connecting, data) do
    close_socket(data) |> arm_reconnect()
  end

  @impl true
  def handle_event({:timeout, :bind_response}, _tick, :bind_pending, data) do
    close_socket(data) |> arm_reconnect()
  end

  @impl true
  def handle_event(:state_timeout, :heartbeat, :bound, data) do
    case send_heartbeat(data) do
      {:ok, data2} ->
        actions = [{:state_timeout, data2.config.heartbeat_ms, :heartbeat}]
        {:keep_state, data2, actions}

      :error ->
        close_socket(data) |> arm_reconnect()
    end
  end

  @impl true
  def handle_event(:info, {:heartbeat_timeout, seq}, :bound, data) do
    close_socket(%{data | pending: Map.delete(data.pending, seq)}) |> arm_reconnect()
  end

  @impl true
  def handle_event(:info, {:tcp, sock, data_in}, state, %{socket: sock} = data) do
    %{data | buffer: data.buffer <> data_in} |> process_inbound(state, sock)
  end

  @impl true
  def handle_event(:info, {:tcp_closed, sock}, _state, %{socket: sock} = data) do
    data |> close_socket() |> flush_pending_for_disconnect() |> arm_reconnect()
  end

  @impl true
  def handle_event(:info, {:tcp_error, sock, _reason}, _state, %{socket: sock} = data) do
    data |> close_socket() |> flush_pending_for_disconnect() |> arm_reconnect()
  end

  @impl true
  def handle_event(:info, _other, _state, data), do: {:keep_state, data, []}

  @impl true
  def handle_event(_type, _event, _state, data), do: {:keep_state, data, []}

  ## ── private helpers ────────────────────────────────────────────────────────

  defp try_connect(data) do
    %{host: host, port: port} = data.config

    case :gen_tcp.connect(host, port, [:binary, packet: :raw, active: :once], @connect_timeout_ms) do
      {:ok, sock} ->
        fresh = %{data | socket: sock, buffer: <<>>, backoff_attempt: 0}
        :ok = :gen_tcp.send(sock, IO.iodata_to_binary(PDU.encode(build_bind_pdu(fresh))))

        tref = :erlang.start_timer(@bind_timeout_ms, self(), :bind_response)

        pending =
          Map.put(fresh.pending, @bind_seq, %{
            from: nil,
            command_id: :bind_transmitter,
            tref: tref
          })

        {:next_state, :bind_pending, %{fresh | pending: pending}}

      {:error, _reason} ->
        data |> close_socket() |> arm_reconnect()
    end
  end

  defp build_bind_pdu(%{config: cfg, seq: seq}) do
    command = bind_as_command(cfg.bind_as)

    body_struct = %Body.Bind{
      system_id: cfg.system_id,
      password: cfg.password,
      system_type: cfg.system_type,
      interface_version: 0x34,
      addr_ton: :UNKNOWN,
      addr_npi: :UNKNOWN,
      address_range: ""
    }

    {:ok, body_bin} = Body.encode(command, body_struct)
    PDU.build(command: command, status: :ESME_ROK, sequence_number: seq, body: body_bin)
  end

  # Allocate next sequence_number (post-bind). 0x7FFFFFFF+1 -> 1.
  defp next_seq(seq) when seq >= @seq_wrap, do: 1
  defp next_seq(seq), do: seq + 1

  defp send_heartbeat(%{socket: nil}), do: :error

  defp send_heartbeat(data) do
    seq = next_seq(data.seq)
    pdu = PDU.build(command: :enquire_link, status: :ESME_ROK, sequence_number: seq, body: <<>>)

    case :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu))) do
      :ok ->
        Process.send_after(self(), {:heartbeat_timeout, seq}, data.config.response_timeout_ms)
        pending = Map.put(data.pending, seq, %{from: nil, command_id: :enquire_link})
        {:ok, %{data | seq: seq, pending: pending}}

      {:error, _} ->
        :error
    end
  end

  defp send_wire(%{socket: nil} = data, _pdu), do: data

  defp send_wire(data, pdu) do
    :gen_tcp.send(data.socket, IO.iodata_to_binary(PDU.encode(pdu)))
    data
  end

  # Reserved cancel helper (currently the heartbeat path uses Process.send_after
  # so no tref needs cancelling; this exists for the pending-window cleanup path
  # that lands with PR3).
  def __seq_wrap__, do: @seq_wrap

  defp bind_as_command(:transmitter), do: :bind_transmitter
  defp bind_as_command(:receiver), do: :bind_receiver
  defp bind_as_command(:transceiver), do: :bind_transceiver

  # Decode chained bytes, dispatch complete PDUs by mutating `data` and
  # recording a target_state / exit_reason. The post-loop step applies
  # the queuing transition and re-arms active mode.
  defp process_inbound(%{socket: nil} = data, _state, _sock), do: {:keep_state, data}

  defp process_inbound(%{buffer: buffer} = data, state, sock) do
    {pdus, leftover} = Framing.feed(<<>>, buffer)

    settled =
      Enum.reduce(pdus, data, fn pdu_bin, acc -> dispatch_pdu(acc, pdu_bin, state) end)
      |> Map.put(:buffer, leftover)
      |> cancel_pending_for_target()
      |> settle_transition()

    safe_setopts_once(sock)
    settled
  end

  defp dispatch_pdu(data, pdu_bin, state) do
    case PDU.decode(pdu_bin) do
      {:ok, pdu} -> apply_pdu(data, pdu, state)
      {:error, _} -> data
    end
  end

  defp apply_pdu(
         data,
         %PDU{command: :bind_transmitter_resp, sequence_number: @bind_seq, status: :ESME_ROK},
         :bind_pending
       ) do
    %{data | target_state: :bound}
  end

  defp apply_pdu(
         data,
         %PDU{command: :bind_transmitter_resp, sequence_number: @bind_seq, status: _other},
         :bind_pending
       ) do
    %{data | target_state: :disconnected, exit_reason: :bind_rejected}
  end

  # Heartbeat round-trip: when the SMSC acks our enquire_link, drop the
  # pending entry (it has `from: nil`).
  defp apply_pdu(data, %PDU{command: :enquire_link_resp, sequence_number: seq}, :bound)
       when is_map_key(data.pending, seq) do
    %{data | pending: Map.delete(data.pending, seq)}
  end

  # Inbound enquire_link from SMSC while bound: auto-reply with our own
  # enquire_link_resp using the same sequence_number.
  defp apply_pdu(data, %PDU{command: :enquire_link, sequence_number: seq}, :bound) do
    send_wire(data, enquire_link_resp(seq))
    data
  end

  defp apply_pdu(data, _pdu, _state), do: data

  defp enquire_link_resp(seq) do
    PDU.build(command: :enquire_link_resp, status: :ESME_ROK, sequence_number: seq, body: <<>>)
  end

  defp cancel_pending_for_target(%{target_state: nil} = data), do: data

  defp cancel_pending_for_target(%{target_state: _} = data), do: cancel_pending(data, @bind_seq)

  defp cancel_pending(data, seq) do
    case Map.pop(data.pending, seq) do
      {nil, _} ->
        data

      {%{tref: tref}, pending} ->
        safe_cancel(tref)
        %{data | pending: pending}
    end
  end

  defp safe_cancel(tref) do
    :erlang.cancel_timer(tref)
  rescue
    _ -> :ok
  end

  defp settle_transition(%{target_state: target} = data) when target != nil do
    %{data | target_state: nil} |> transition(target, data.exit_reason)
  end

  defp settle_transition(data), do: {:keep_state, data}

  defp transition(data, :bound, _reason) do
    # On entry to :bound, arm the heartbeat :state_timeout. The fire path
    # sends an enquire_link with `from: nil` tracking.
    actions = [{:state_timeout, data.config.heartbeat_ms, :heartbeat}]
    {:next_state, :bound, data, actions}
  end

  defp transition(data, :disconnected, :bind_rejected), do: arm_reconnect(data)
  defp transition(data, :disconnected, _reason), do: {:next_state, :disconnected, data, []}

  defp flush_pending(data, reply) do
    Enum.each(data.pending, fn {_seq, %{from: from}} -> reply_to(from, reply) end)
    data = %{data | pending: %{}}
    data
  end

  defp flush_pending_for_disconnect(data), do: flush_pending(data, {:error, :disconnected})

  defp reply_to(nil, _reply), do: :ok
  defp reply_to(from, reply) when is_pid(from), do: send(from, reply)

  defp close_socket(%{socket: nil} = data), do: %{data | socket: nil, buffer: <<>>}

  defp close_socket(%{socket: sock} = data) do
    :gen_tcp.close(sock)
    %{data | socket: nil, buffer: <<>>}
  rescue
    _ -> %{data | socket: nil, buffer: <<>>}
  end

  defp safe_setopts_once(sock) do
    :inet.setopts(sock, active: :once)
  rescue
    _ -> :ok
  end

  defp arm_reconnect(data) do
    backoff = backoff_delay(data)

    {:next_state, :disconnected, %{data | backoff_attempt: data.backoff_attempt + 1},
     [{{:timeout, :reconnect}, backoff, :try_connect}]}
  end

  defp backoff_delay(%{
         config: %{reconnect: %{base_ms: base, factor: f, cap_ms: cap, jitter: j}},
         backoff_attempt: n
       }) do
    exp = base * Integer.pow(f, n)
    capped = min(exp, cap)
    if j, do: :rand.uniform(capped), else: capped
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
