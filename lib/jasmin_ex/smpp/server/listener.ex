defmodule JasminEx.Smpp.Server.Listener do
  @moduledoc false
  use GenServer

  alias JasminEx.Smpp.Server.{Session, SessionSupervisor, Transport}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name_opts(opts))
  def port(server), do: GenServer.call(server, :port)

  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    {:ok, listen} = Transport.listen(config.port, config.host)
    {:ok, port} = :inet.port(listen)

    state = %{
      listen: listen,
      port: port,
      router: Keyword.fetch!(opts, :router),
      binding_manager: Keyword.fetch!(opts, :binding_manager),
      sessions: Keyword.fetch!(opts, :session_supervisor),
      max: config.max_pdu_length
    }

    {:ok, Map.put(state, :acceptor, spawn_link(fn -> accept_loop(state) end))}
  end

  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  def terminate(_reason, state) do
    Transport.close(state.listen)
    :ok
  end

  defp accept_loop(state) do
    case Transport.accept(state.listen) do
      {:ok, socket} ->
        handoff(socket, state)
        accept_loop(state)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        accept_loop(state)
    end
  end

  defp handoff(socket, state) do
    opts = [
      router: state.router,
      binding_manager: state.binding_manager,
      max_pdu_length: state.max
    ]

    with {:ok, session} <- SessionSupervisor.start_session(state.sessions, opts),
         :ok <- :gen_tcp.controlling_process(socket, session),
         :ok <- Session.handoff(session, socket) do
      :ok
    else
      _ -> Transport.close(socket)
    end
  end

  defp name_opts(opts) do
    case Keyword.get(opts, :name) do
      nil -> []
      name -> [name: name]
    end
  end
end
