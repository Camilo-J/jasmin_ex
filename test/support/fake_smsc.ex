defmodule JasminEx.Smpp.FakeSMSC do
  @moduledoc """
  Scriptable TCP peer used by `JasminEx.Smpp.Client` tests.

  Listens on a local port, accepts exactly one peer connection, decodes
  every complete PDU that arrives using `JasminEx.Smpp.Framing`, and
  applies a per-command "script" action. The default action for any
  unscripted command is `:auto_reply` — the harness decodes the inbound
  request, identifies its matching `_resp` form, and writes the response
  onto the wire (status `:ESME_ROK` unless a test overrode it).

  ## PR2 scripting surface

    * `:auto_reply`           — default; respond with status `:ESME_ROK`
    * `{:reply_status, s}`    — respond with a custom status (used by
      `inject_bind_resp/2` to simulate SMSC auth failures)
    * `:withhold`             — drop all responses for that command
    * `:close`                — close the socket on arrival

  ## Limitations

    * Single connection per harness. PR3 may revisit for multi-conn.
    * No automatic response delay in PR2 — `:withhold` + timeout scenarios
      are enough for the heartbeat suite.
  """
  use GenServer

  alias JasminEx.Smpp.Framing
  alias JasminEx.Smpp.PDU
  alias JasminEx.Smpp.PDU.Body, as: Body

  @default_action :auto_reply

  @req_resp_map %{
    bind_receiver: :bind_receiver_resp,
    bind_transmitter: :bind_transmitter_resp,
    bind_transceiver: :bind_transceiver_resp,
    submit_sm: :submit_sm_resp,
    deliver_sm: :deliver_sm_resp,
    unbind: :unbind_resp,
    enquire_link: :enquire_link_resp
  }

  # ── client API ────────────────────────────────────────────────────────────

  @spec start_link(keyword()) :: {:ok, pos_integer(), pid()}
  def start_link(opts \\ []) do
    port = Keyword.get(opts, :port, 0)
    script = Keyword.get(opts, :script, %{})
    {:ok, parent_pid} = GenServer.start_link(__MODULE__, %{port: port, script: script})
    bound_port = GenServer.call(parent_pid, :port)
    {:ok, bound_port, parent_pid}
  end

  @spec port(pid()) :: pos_integer()
  def port(pid), do: GenServer.call(pid, :port)

  @doc """
  Write raw bytes onto the harness socket toward the connected client.
  Returns `:ok`; falls back to `{:error, :no_connection}` if the harness
  has not yet accepted a peer.
  """
  @spec send_bytes(pid(), iodata()) :: :ok | {:error, :no_connection}
  def send_bytes(pid, bytes) when is_binary(bytes) or is_list(bytes) do
    GenServer.call(pid, {:send, IO.iodata_to_binary(bytes)})
  end

  @doc """
  Block until the harness has accepted a peer connection. Tests call
  this after `:gen_tcp.connect/3` so subsequent `send_bytes/2` lands on
  the live socket. Returns `:ok` or `{:error, :timeout}`.
  """
  @spec wait_connected(pid(), pos_integer()) :: :ok | {:error, :timeout}
  def wait_connected(pid, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(pid, deadline)
  end

  @doc """
  Subscribe to inbound byte notifications. Each PDU the harness receives
  triggers one `{:fake_smsc_bytes, ref, bin}` message to the subscriber.
  """
  @spec subscribe(pid()) :: reference()
  def subscribe(pid), do: GenServer.call(pid, {:subscribe, self()})

  @doc """
  Override the response status sent for the next `:bind_transmitter` /
  `:bind_receiver` / `:bind_transceiver` request — i.e. the inbound
  bind command gets a non-:ESME_ROK reply. Subsequent bind attempts
  (e.g. on reconnect) reset to default `:auto_reply` if you
  re-inject; there is no automatic per-message handshake, just a single
  script entry keyed by the request form.
  """
  @spec inject_bind_resp(pid(), atom()) :: :ok
  def inject_bind_resp(pid, status) do
    GenServer.call(pid, {:set_script, :bind_transmitter, {:reply_status, status}})
  end

  @doc """
  Drop all responses to the given inbound command.
  """
  @spec withhold_response(pid(), atom()) :: :ok
  def withhold_response(pid, command_id) do
    GenServer.call(pid, {:set_script, command_id, :withhold})
  end

  @doc """
  Close the socket the moment a given command arrives. Simulates SMSC
  vanishing mid-session (heartbeat-induced disconnect, mid-bind drop, etc.).
  """
  @spec close_on_command(pid(), atom()) :: :ok
  def close_on_command(pid, command_id) do
    GenServer.call(pid, {:set_script, command_id, :close})
  end

  # ── GenServer callbacks ───────────────────────────────────────────────────

  @impl true
  def init(%{port: port, script: script}) do
    {:ok, listener} =
      :gen_tcp.listen(port, [:binary, packet: :raw, active: false, reuseaddr: true])

    {:ok, bound_port} = :inet.port(listener)
    parent = self()
    _ = spawn_link(fn -> accept_loop(parent, listener) end)

    {:ok,
     %{
       listener: listener,
       port: bound_port,
       script: script,
       subscribers: %{},
       conn: nil,
       buffer: <<>>
     }}
  end

  @impl true
  def handle_call(:port, _from, %{port: port} = state), do: {:reply, port, state}

  def handle_call(:is_connected, _from, %{conn: c} = state),
    do: {:reply, c != nil, state}

  def handle_call({:send, bytes}, _from, %{conn: conn} = state) when conn != nil do
    :ok = :gen_tcp.send(conn, bytes)
    {:reply, :ok, state}
  end

  def handle_call({:send, _bytes}, _from, state), do: {:reply, {:error, :no_connection}, state}

  def handle_call({:subscribe, pid}, _from, %{subscribers: subs} = state) do
    ref = make_ref()
    {:reply, ref, %{state | subscribers: Map.put(subs, ref, pid)}}
  end

  def handle_call({:set_script, command_id, action}, _from, %{script: script} = state) do
    {:reply, :ok, %{state | script: Map.put(script, command_id, action)}}
  end

  @impl true
  def handle_info({:tcp, sock, data}, state) when state.conn == sock do
    new_state = consume_bytes(state, data)

    case new_state.conn do
      nil ->
        :ok

      _ ->
        try do
          :ok = :inet.setopts(sock, active: :once)
        catch
          _, _ -> :ok
        end
    end

    {:noreply, new_state}
  end

  def handle_info({:tcp_closed, sock}, state) when state.conn == sock do
    {:noreply, %{state | conn: nil, buffer: <<>>}}
  end

  def handle_info({:tcp_closed, _other}, state), do: {:noreply, state}

  def handle_info({:tcp_error, sock, _reason}, state) when state.conn == sock do
    {:noreply, %{state | conn: nil, buffer: <<>>}}
  end

  def handle_info({:tcp_error, _other, _reason}, state), do: {:noreply, state}

  def handle_info({:accepted, sock}, state) do
    :ok = :inet.setopts(sock, active: :once)
    # Re-arm the accept loop for subsequent reconnect attempts. Listener
    # port remains alive for the lifetime of the harness.
    parent = self()
    _ = spawn_link(fn -> accept_loop(parent, state.listener) end)
    {:noreply, %{state | conn: sock, buffer: <<>>}}
  end

  # keep listener reference even when conn is set; it's the OS port that
  # re-accepts on subsequent connect attempts.
  @impl true
  def terminate(_reason, state) do
    if state.conn,
      do:
        (try do
           :gen_tcp.close(state.conn)
         catch
           _, _ -> :ok
         end)

    if state.listener,
      do:
        (try do
           :gen_tcp.close(state.listener)
         catch
           _, _ -> :ok
         end)

    :ok
  end

  # ── private helpers ───────────────────────────────────────────────────────

  defp accept_loop(parent, listener) do
    case :gen_tcp.accept(listener) do
      {:ok, sock} ->
        # Take ownership before transferring, so that whoever calls
        # `controlling_process` next has clean handoff. Here we
        # transfer to the parent — the parent's own init runs in its
        # own process and the parent gen_server IS the eventual
        # connection handler.
        :ok = :gen_tcp.controlling_process(sock, parent)
        send(parent, {:accepted, sock})

      _ ->
        :ok
    end
  end

  defp do_wait(pid, deadline) do
    case GenServer.call(pid, :is_connected, 200) do
      true ->
        :ok

      false ->
        now = System.monotonic_time(:millisecond)

        if now >= deadline do
          {:error, :timeout}
        else
          do_wait(pid, deadline)
        end
    end
  end

  defp consume_bytes(%{buffer: buf} = state, data) do
    state = notify_subscribers(state, data)
    {pdus, new_buf} = Framing.feed(buf, data)
    state = %{state | buffer: new_buf}

    Enum.reduce(pdus, state, fn bin, acc -> handle_pdu(acc, bin) end)
  end

  defp handle_pdu(state, bin) do
    case PDU.decode(bin) do
      {:ok, pdu} -> apply_action(state, pdu, action_for(state, pdu.command))
      {:error, _} -> state
    end
  end

  defp notify_subscribers(state, bin) do
    for {ref, pid} <- state.subscribers, do: send(pid, {:fake_smsc_bytes, ref, bin})
    state
  end

  defp action_for(state, command_id) do
    Map.get(state.script, command_id, Map.get(state.script, :default, @default_action))
  end

  defp apply_action(state, %PDU{} = pdu, :auto_reply), do: reply_to(state, pdu, :ESME_ROK)

  defp apply_action(state, %PDU{} = pdu, {:reply_status, status}),
    do: reply_to(state, pdu, status)

  defp apply_action(state, _pdu, :withhold), do: state

  defp apply_action(state, _pdu, :close) do
    if state.conn, do: :gen_tcp.close(state.conn)
    %{state | conn: nil, buffer: <<>>}
  end

  defp reply_to(%{conn: nil} = state, _pdu, _status), do: state

  defp reply_to(state, %PDU{command: request_cmd, sequence_number: sequence_number}, status) do
    case Map.fetch(@req_resp_map, request_cmd) do
      {:ok, resp_cmd} ->
        body = encode_resp_body(resp_cmd)

        wire =
          PDU.build(
            command: resp_cmd,
            status: status,
            sequence_number: sequence_number,
            body: body
          )
          |> PDU.encode()
          |> IO.iodata_to_binary()

        :ok = :gen_tcp.send(state.conn, wire)
        state

      :error ->
        state
    end
  end

  defp encode_resp_body(:bind_transmitter_resp),
    do: encode_struct_body(:bind_transmitter_resp, %Body.BindResp{system_id: "fake-smsc"})

  defp encode_resp_body(:bind_receiver_resp),
    do: encode_struct_body(:bind_receiver_resp, %Body.BindResp{system_id: "fake-smsc"})

  defp encode_resp_body(:bind_transceiver_resp),
    do: encode_struct_body(:bind_transceiver_resp, %Body.BindResp{system_id: "fake-smsc"})

  defp encode_resp_body(:submit_sm_resp),
    do: encode_struct_body(:submit_sm_resp, %Body.SubmitSMResp{message_id: "fake-msg-id"})

  defp encode_resp_body(:deliver_sm_resp),
    do: encode_struct_body(:deliver_sm_resp, %Body.DeliverSMResp{message_id: "fake-msg-id"})

  defp encode_resp_body(:unbind_resp), do: ""
  defp encode_resp_body(:enquire_link_resp), do: ""

  defp encode_struct_body(cmd, struct) do
    {:ok, iodata} = Body.encode(cmd, struct)
    IO.iodata_to_binary(iodata)
  end
end
